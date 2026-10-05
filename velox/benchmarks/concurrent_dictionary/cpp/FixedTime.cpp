#define main microbenchmarkMain
#include "ConcurrentHashMapBench.cpp"
#undef main
#include <pthread.h>
#include <sys/resource.h>
#include <chrono>
#ifdef VELOX_BENCH_MEMORY_CACHE
#include "velox/common/caching/MemoryCache.h"

// Preserve the dictionary workload and ownership semantics for an A/B comparison.
class CacheWorkload {
 public:
  using Value = std::shared_ptr<const std::string>;
  void insert(const std::string& key, Value value) {
    if (!cache_.put(key, std::move(value))) {
      throw std::runtime_error("Unexpected cache admission failure");
    }
  }
  void insert_or_assign(const std::string& key, Value value) {
    insert(key, std::move(value));
  }
  Value get(const std::string& key) { return cache_.get(key); }
  size_t size() const { return cache_.size(); }
 private:
  facebook::velox::MemoryCache<std::string, std::string> cache_{100000};
};

std::optional<CacheWorkload::Value> read(CacheWorkload& cache, const std::string& key) {
  auto value = cache.get(key);
  if (!value) return std::nullopt;
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
    uint64_t reads = 0, writes = 0, sum = 0;
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
            if ((index + w) % 100 < writes) {
              map.insert_or_assign(
                  k,
                  std::make_shared<const std::string>(
                      128, char('a' + index % 26)));
              ++result.writes;
            } else {
              auto v = read(map, k);
              if (!v || (*v)->size() != 128)
                std::abort();
              result.sum += static_cast<unsigned char>((**v)[0]);
              ++result.reads;
            }
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
  uint64_t reads = 0, updates = 0, sum = 0;
  for (auto& r : results) {
    reads += r.reads;
    updates += r.writes;
    sum += r.sum;
  }
  if (map.size() != 100000)
    return 3;
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  double cpu = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
      usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
  std::cout << "{\"reads\":" << reads << ",\"writes\":" << updates
            << ",\"checksum\":" << sum << ",\"seconds\":" << elapsed
            << ",\"ops_per_second\":" << (reads + updates) / elapsed
            << ",\"process_cpu_seconds\":" << cpu
            << ",\"peak_rss_kb\":" << usage.ru_maxrss << "}\n";
}
