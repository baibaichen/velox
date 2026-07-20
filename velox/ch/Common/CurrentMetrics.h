#pragma once

#include <atomic>
#include <cstdint>

namespace facebook::velox::ch {

namespace CurrentMetrics {

enum Metric {
  CacheFileSegments,
  FilesystemCacheHoldFileSegments,
  FilesystemCacheDownloadQueueElements,
  FilesystemCacheDelayedCleanupElements,
  FilesystemCacheReserveThreads,
  FilesystemCacheSizeLimit,
  // B2: hard-blocker enumerator names referenced via add/sub on the in-scope
  // center-SCC path (Tasks 011/012).
  FilesystemCacheElements,
  FilesystemCacheInvalidatedElements,
  FilesystemCachePriorityQueueElements,
  FilesystemCacheSize,
  FilesystemCacheKeys,
  // Sentinel: number of metrics. Keep last.
  kMetricCount,
};

namespace detail {
// Task 017: real process-wide gauge storage. One relaxed atomic per metric.
// Reset on process exit is acceptable (no persistence contract).
extern std::atomic<int64_t> values[static_cast<int>(kMetricCount)];
} // namespace detail

inline int64_t get(Metric m) {
  return detail::values[static_cast<int>(m)].load(std::memory_order_relaxed);
}

inline void add(Metric m, int64_t delta = 1) {
  detail::values[static_cast<int>(m)].fetch_add(delta, std::memory_order_relaxed);
}

inline void sub(Metric m, int64_t delta = 1) {
  detail::values[static_cast<int>(m)].fetch_sub(delta, std::memory_order_relaxed);
}

/// RAII gauge guard: adds on construction, subtracts the same delta on
/// destruction. Preserves the CH exception-safe increment/decrement structure.
class Increment {
 public:
  explicit Increment(Metric m, int64_t delta = 1) : metric_(m), delta_(delta) {
    add(metric_, delta_);
  }

  ~Increment() {
    sub(metric_, delta_);
  }

  Increment(const Increment&) = delete;
  Increment& operator=(const Increment&) = delete;

 private:
  Metric metric_;
  int64_t delta_;
};

} // namespace CurrentMetrics

} // namespace facebook::velox::ch
