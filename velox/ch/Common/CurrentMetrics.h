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
};

inline void add(Metric, int64_t = 1) {}

inline void sub(Metric, int64_t = 1) {}

class Increment {
 public:
  explicit Increment(Metric, int64_t = 1) {}
};

} // namespace CurrentMetrics

} // namespace facebook::velox::ch
