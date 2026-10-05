#include <folly/Benchmark.h>
#ifdef VELOX_BENCH_CHM_BOOST
#include <boost/unordered/concurrent_flat_map.hpp>
#endif
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/init/Init.h>
#include <array>
#include <barrier>
#include <exception>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <thread>
#include <vector>

DEFINE_bool(
    self_test,
    false,
    "Check workload and conditional identity semantics");
DEFINE_string(threads, "1,4", "Comma-separated worker counts dividing 262144");
namespace {
#ifdef VELOX_BENCH_CHM_BOOST
// Visitors protect references only until the callback returns.
template <class K, class V>
class Map {
 public:
  explicit Map(size_t capacity = 0) : map_(capacity) {}
  bool contains(const K& k) const { return map_.cvisit(k, [](const auto&) {}) != 0; }
  std::optional<V> read(const K& k) const {
    std::optional<V> result;
    map_.cvisit(k, [&](const auto& entry) { result = entry.second; });
    return result;
  }
  auto insert(const K& k, const V& v) { return std::pair{0, map_.emplace(k, v)}; }
  void insert_or_assign(const K& k, const V& v) { map_.insert_or_assign(k, v); }
  bool assign_if_equal(const K& k, const V& old, const V& desired) {
    bool changed = false;
    map_.visit(k, [&](auto& entry) {
      if (entry.second == old) { entry.second = desired; changed = true; }
    });
    return changed;
  }
  size_t erase_if_equal(const K& k, const V& old) {
    return map_.erase_if(k, [&](const auto& entry) { return entry.second == old; });
  }
  size_t size() const { return map_.size(); }
 private:
  boost::concurrent_flat_map<K, V> map_;
};
#else
template <class K, class V>
using Map =
#ifdef VELOX_BENCH_CHM_SIMD
    folly::ConcurrentHashMapSIMD<K, V>;
#else
    folly::ConcurrentHashMap<K, V>;
#endif

#endif

template <class K, class V>
bool contains(const Map<K,V>& map, const K& k) {
#ifdef VELOX_BENCH_CHM_BOOST
  return map.contains(k);
#else
  return map.find(k) != map.cend();
#endif
}
template <class K, class V>
std::optional<V> read(const Map<K,V>& map, const K& k) {
#ifdef VELOX_BENCH_CHM_BOOST
  return map.read(k);
#else
  auto it = map.find(k);
  if (it != map.cend()) return it->second;
  return std::nullopt;
#endif
}

constexpr unsigned kBatch = 262144;
struct Entry {
  int64_t id;
};
using Ptr = std::shared_ptr<const Entry>;
enum Op { Hit, Miss, Mixed95, Mixed50, ConditionalUpdate, Churn };
const char* names[] =
    {"Hit", "Miss", "Mixed95", "Mixed50", "ConditionalUpdate", "Churn"};
void check(bool ok) {
  if (!ok)
    throw std::runtime_error("self-test failed");
}
template <class K>
K key(unsigned id) {
  if constexpr (std::is_same_v<K, int64_t>)
    return id;
  else {
    auto s = std::to_string(id);
    return std::string(16 - s.size(), '0') + s;
  }
}
template <class V>
V value(unsigned id) {
  if constexpr (std::is_same_v<V, int64_t>)
    return id;
  else
    return std::make_shared<const Entry>(Entry{static_cast<int64_t>(id)});
}
uint32_t next(uint32_t& x) {
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  return x;
}

template <class K, class V>
class Workload {
 public:
  Map<K, V> map;
  Workload(unsigned count, unsigned threads, bool hot)
      : start_(threads + 1),
        finish_(threads + 1),
        keys_(threads),
        misses_(threads),
        successes_(threads),
        errors_(threads) {
    for (unsigned i = 0; i < 256; ++i)
      values_[i] = value<V>(i);
    for (unsigned i = 0; i < count; ++i)
      map.insert(key<K>(i), values_[i % 256]);
    for (unsigned w = 0; w < threads; ++w) {
      uint32_t state = 0x9e3779b9u ^ (w + 1);
      for (unsigned i = 0; i < kBatch / threads; ++i) {
        auto r = next(state);
        unsigned range =
            hot && r % 100 < 90 ? std::max(1u, count / 100) : count;
        unsigned id = next(state) % range;
        keys_[w].push_back(key<K>(id));
        misses_[w].push_back(key<K>(count + id));
      }
    }
    // All allocation of workload data precedes starting workers.
    workers_.reserve(threads);
    try {
      for (unsigned w = 0; w < threads; ++w)
        workers_.emplace_back([this, w] {
          for (;;) {
            start_.arrive_and_wait();
            if (stop_)
              return;
            try {
              successes_[w] = execute(w);
            } catch (...) {
              errors_[w] = std::current_exception();
            }
            finish_.arrive_and_wait();
          }
        });
    } catch (...) {
      for (unsigned w = workers_.size(); w < threads; ++w)
        start_.arrive_and_drop();
      stop_ = true;
      start_.arrive_and_wait();
      for (auto& t : workers_)
        t.join();
      throw;
    }
  }
  ~Workload() {
    stop_ = true;
    start_.arrive_and_wait();
    for (auto& t : workers_)
      t.join();
  }
  unsigned run(Op op) {
    op_ = op;
    start_.arrive_and_wait();
    finish_.arrive_and_wait();
    for (auto& e : errors_)
      if (e)
        std::rethrow_exception(e);
    return std::accumulate(successes_.begin(), successes_.end(), 0u);
  }

 private:
  unsigned execute(unsigned w) {
    const auto& trace = op_ == Miss ? misses_[w] : keys_[w];
    unsigned success = 0;
    switch (op_) {
      case Hit:
      case Miss:
        for (const auto& k : trace)
          if (contains(map, k))
            ++success;
        break;
      case Mixed95:
      case Mixed50:
        for (unsigned i = 0; i < trace.size(); ++i) {
          if ((i + w) % (op_ == Mixed95 ? 20 : 2) == 0) {
            map.insert_or_assign(trace[i], values_[(i + w) % 256]);
            ++success;
          } else if (contains(map, trace[i]))
            ++success;
        }
        break;
      case ConditionalUpdate:
      case Churn:
        for (unsigned i = 0; i < trace.size(); ++i) {
          // Copy the value and release the hazard-protected iterator before
          // mutation.
          auto old = read(map, trace[i]);
          if (old) {
            if (op_ == ConditionalUpdate) {
              if (map.assign_if_equal(trace[i], *old, values_[(i + w) % 256]))
                ++success;
            } else if (map.erase_if_equal(trace[i], *old))
              ++success;
          }
          if (op_ == Churn)
            map.insert(trace[i], values_[(i + w) % 256]);
        }
        break;
    }
    return success;
  }
  std::barrier<> start_, finish_;
  std::vector<std::vector<K>> keys_, misses_;
  std::array<V, 256> values_;
  std::vector<unsigned> successes_;
  std::vector<std::exception_ptr> errors_;
  std::vector<std::thread> workers_;
  bool stop_ = false;
  Op op_ = Hit;
};

template <class K, class V>
void registerCases(
    const std::string& type,
    const std::vector<unsigned>& threads) {
  for (unsigned count : {1024u, 65536u})
    for (bool hot : {false, true})
      for (unsigned n : threads)
        for (int o = 0; o < 6; ++o) {
          auto name = type + "/" + names[o] + "/" + std::to_string(count) +
              "/" + (hot ? "Hot/" : "Uniform/") + std::to_string(n);
          folly::addBenchmark(__FILE__, name, [=](unsigned requested) {
            folly::BenchmarkSuspender suspended;
            Workload<K, V> work(count, n, hot);
            work.run(
                static_cast<Op>(o)); // Warm worker-local hazard pointer state.
            suspended.dismiss();
            // Folly asks for logical operations; report actual complete
            // batches.
            unsigned batches = std::max(1u, requested / kBatch);
            for (unsigned i = 0; i < batches; ++i)
              folly::doNotOptimizeAway(work.run(static_cast<Op>(o)));
            suspended.rehire();
            return batches * kBatch;
          });
        }
  for (unsigned count : {1024u, 65536u})
    for (bool presized : {false, true})
      for (bool fresh : {false, true}) {
        auto name = type + "/" + (fresh ? "InsertNewValues/" : "InsertAll/") +
            std::to_string(count) + "/" + (presized ? "Presized" : "Grow");
        folly::addBenchmark(__FILE__, name, [=](unsigned times) {
          folly::BenchmarkSuspender suspended;
          std::vector<K> keys;
          std::vector<V> values;
          for (unsigned i = 0; i < count; ++i) {
            keys.push_back(key<K>(i));
            values.push_back(value<V>(i));
          }
          suspended.dismiss();
          for (unsigned t = 0; t < times; ++t) {
            Map<K, V> map(presized ? count : 0);
            for (unsigned i = 0; i < count; ++i)
              map.insert(keys[i], fresh ? value<V>(i) : values[i]);
            folly::doNotOptimizeAway(map.size());
          }
          suspended.rehire();
          return times; // Whole table, including destruction, unlike C# GC
                        // scheduling.
        });
      }
}
template <class K, class V>
void verify() {
  for (unsigned n : {1u, 4u})
    for (bool hot : {false, true}) {
      Workload<K, V> w(1024, n, hot);
      for (int o = 0; o < 6; ++o) {
        unsigned s = w.run(static_cast<Op>(o));
        check(s <= kBatch);
        if (o == Miss)
          check(s == 0);
        else if (o <= Mixed50 || n == 1)
          check(s == kBatch);
        check(w.map.size() == 1024);
      }
      check(w.run(Hit) == kBatch);
    }
}
} // namespace
int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  if (FLAGS_self_test) {
    Map<int64_t, Ptr> map;
    auto old = value<Ptr>(1), current = value<Ptr>(1);
    check(map.insert(1, old).second);
    check(bool(map.assign_if_equal(1, old, current)));
    check(!map.assign_if_equal(1, old, old));
    check(!map.erase_if_equal(1, old));
    check(map.erase_if_equal(1, current) == 1);
    check(old->id == 1 && current->id == 1);
    verify<int64_t, int64_t>();
    verify<std::string, int64_t>();
    verify<int64_t, Ptr>();
    verify<std::string, Ptr>();
    std::cout
        << "PASS: identity and all steady-state workloads/types, 1/4 workers, uniform/hot\n";
    return 0;
  }
  std::vector<unsigned> threads;
  std::istringstream input(FLAGS_threads);
  std::string part;
  while (std::getline(input, part, ',')) {
    size_t used;
    unsigned long n = std::stoul(part, &used);
    if (used != part.size() || n == 0 || n > kBatch || kBatch % n)
      throw std::invalid_argument("Invalid --threads");
    threads.push_back(n);
  }
  if (threads.empty())
    throw std::invalid_argument("Empty --threads");
  registerCases<int64_t, int64_t>("Int64_Int64", threads);
  registerCases<std::string, int64_t>("String_Int64", threads);
  registerCases<int64_t, Ptr>("Int64_Entry", threads);
  registerCases<std::string, Ptr>("String_Entry", threads);
  folly::runBenchmarks();
  return 0;
}
