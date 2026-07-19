#pragma once

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
  // center-SCC path (Tasks 011/012). Real counters remain Task 017; add/sub
  // stay no-op.
  FilesystemCacheElements,
  FilesystemCacheInvalidatedElements,
  FilesystemCachePriorityQueueElements,
  FilesystemCacheSize,
  FilesystemCacheKeys,
};

inline void add(Metric, int64_t = 1) {}

inline void sub(Metric, int64_t = 1) {}

class Increment {
 public:
  explicit Increment(Metric, int64_t = 1) {}
};

} // namespace CurrentMetrics

} // namespace facebook::velox::ch
