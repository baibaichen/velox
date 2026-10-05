#define main microbenchmarkMain
#include "ConcurrentHashMapBench.cpp"
#undef main
#include <pthread.h>
#include <sys/resource.h>
#include <chrono>
#ifdef VELOX_BENCH_MEMORY_CACHE
#include "velox/common/caching/MemoryCache.h"

// Preserve the dictionary workload and ownership semantics for an A/B
// comparison.
class CacheWorkload {
 public:
  using Value = std::shared_ptr<const std::string>;
  CacheWorkload() : cache_(options()) {}
  void insert(const std::string& key, Value value) {
    Cache::EntryOptions entry;
    entry.size = 1;
    if (scenario_ == "expiry")
      entry.absoluteExpirationRelativeToNow = std::chrono::milliseconds(10);
    cache_.set(key, std::move(value), entry);
  }
  void insert_or_assign(const std::string& key, Value value) {
    insert(key, std::move(value));
  }
  Value get(const std::string& key) {
    Value result;
    cache_.tryGetValue(key, result);
    return result;
  }
  size_t size() const {
    return cache_.count();
  }

 private:
  using Cache = facebook::velox::
      MemoryCache<std::string, const std::string, boost::concurrent_flat_map>;
  std::string scenario_{
      std::getenv("CACHE_SCENARIO") ? std::getenv("CACHE_SCENARIO") : "steady"};
  Cache::Options options() {
    Cache::Options o;
    o.sizeLimit = scenario_ == "capacity" ? 10000 : 100000;
    o.expirationScanFrequency = std::chrono::milliseconds(10);
    return o;
  }
  Cache cache_;
};

std::optional<CacheWorkload::Value> read(
    CacheWorkload& cache,
    const std::string& key) {
  auto value = cache.get(key);
  if (!value)
    return std::nullopt;
  return value;
}
#endif
int main(int argc, char** argv) {
  if (argc != 7)
    return 2;
  unsigned n = std::stoul(argv[1]), writes = std::stoul(argv[2]);
  bool hot = std::stoi(argv[3]);
  double warm = std::stod(argv[4]), seconds = std::stod(argv[5]);
  std::vector<int> cpus;
  std::istringstream input(argv[6]);
  std::string part;
  while (std::getline(input, part, ','))
    cpus.push_back(std::stoi(part));
  if (!n || n > 64 || writes > 100 || warm <= 0 || seconds <= 0 ||
      cpus.size() != n)
    return 2;
  using Value = std::shared_ptr<const std::string>;
#ifdef VELOX_BENCH_MEMORY_CACHE
  CacheWorkload map;
#else
  Map<std::string, Value> map;
#endif
  std::vector<std::string> keys;
  for (unsigned i = 0; i < 100000; ++i) {
    auto s = std::to_string(i);
    keys.push_back(std::string(32 - s.size(), '0') + s);
    map.insert(keys.back(), std::make_shared<const std::string>(128, 'a'));
  }
  std::vector<std::vector<unsigned>> traces(n);
  for (unsigned w = 0; w < n; ++w) {
    uint32_t state = 0x9e3779b9u ^ (w + 1);
    for (unsigned i = 0; i < 65536; ++i) {
      auto r = next(state);
      unsigned range = hot && r % 100 < 90 ? 1000 : 100000;
      traces[w].push_back(next(state) % range);
    }
  }
  using Clock = std::chrono::steady_clock;
  std::barrier gate(n + 1);
  Clock::time_point deadline;
  struct Result {
    uint64_t reads = 0, writes = 0, sum = 0, misses = 0;
    std::vector<double> latency;
  };
  std::vector<Result> results(n);
  std::vector<std::thread> workers;
  for (unsigned w = 0; w < n; ++w)
    workers.emplace_back([&, w] {
      cpu_set_t set;
      CPU_ZERO(&set);
      CPU_SET(cpus[w], &set);
      if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set))
        std::abort();
      for (int phase = 0; phase < 2; ++phase) {
        gate.arrive_and_wait();
        Result result;
        uint64_t index = 0;
        do {
          for (unsigned b = 0; b < 256; ++b, ++index) {
            auto& k = keys[traces[w][index % 65536]];
            auto sampleStart =
                index % 1024 == 0 ? Clock::now() : Clock::time_point{};
            if ((index + w) % 100 < writes) {
              map.insert_or_assign(
                  k,
                  std::make_shared<const std::string>(
                      128, char('a' + index % 26)));
              ++result.writes;
            } else {
              auto v = read(map, k);
              if (!v)
                ++result.misses;
              else {
                if ((*v)->size() != 128)
                  std::abort();
                result.sum += static_cast<unsigned char>((**v)[0]);
              }
              ++result.reads;
            }
            if (index % 1024 == 0)
              result.latency.push_back(
                  std::chrono::duration<double, std::nano>(
                      Clock::now() - sampleStart)
                      .count());
          }
        } while (Clock::now() < deadline);
        results[w] = result;
        gate.arrive_and_wait();
      }
    });
  double elapsed = 0;
  for (double duration : {warm, seconds}) {
    auto begin = Clock::now();
    deadline = begin +
        std::chrono::duration_cast<Clock::duration>(
                   std::chrono::duration<double>(duration));
    gate.arrive_and_wait();
    gate.arrive_and_wait();
    elapsed = std::chrono::duration<double>(Clock::now() - begin).count();
  }
  for (auto& t : workers)
    t.join();
  uint64_t reads = 0, updates = 0, sum = 0, misses = 0;
  std::vector<double> latency;
  for (auto& r : results) {
    latency.insert(latency.end(), r.latency.begin(), r.latency.end());
    reads += r.reads;
    updates += r.writes;
    sum += r.sum;
    misses += r.misses;
  }
#ifndef VELOX_BENCH_MEMORY_CACHE
  if (map.size() != 100000)
    return 3;
#endif
  std::sort(latency.begin(), latency.end());
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  double cpu = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
      usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
  std::cout << "{\"reads\":" << reads << ",\"writes\":" << updates
            << ",\"misses\":" << misses << ",\"entries\":" << map.size()
            << ",\"checksum\":" << sum << ",\"seconds\":" << elapsed
            << ",\"ops_per_second\":" << (reads + updates) / elapsed
            << ",\"process_cpu_seconds\":" << cpu
            << ",\"sample_p50_ns\":" << latency[latency.size() / 2]
            << ",\"sample_p99_ns\":" << latency[latency.size() * 99 / 100]
            << ",\"peak_rss_kb\":" << usage.ru_maxrss << "}\n";
}
