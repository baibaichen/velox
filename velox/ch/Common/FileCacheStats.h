#pragma once

#include <cstdint>

namespace facebook::velox::ch
{

/// RuntimeMetric key for bytes written to the FileCache. Used in IoStats
/// free-form counters; flows through FileDataSource -> RuntimeMetric ->
/// OperatorStats -> TaskStats -> Gluten JNI -> Spark SQLMetric.
inline constexpr const char * kFileCacheWriteBytes = "fileCacheWriteBytes";

/// Point-in-time snapshot of FileCache gauges + cumulative counters.
struct FileCacheStatsSnapshot
{
    // Gauges (from CurrentMetrics)
    int64_t cacheSize = 0;
    int64_t cacheSizeLimit = 0;
    int64_t cacheKeys = 0;
    int64_t cacheElements = 0;
    int64_t cacheFileSegments = 0;
    int64_t holdFileSegments = 0;
    int64_t invalidatedElements = 0;
    int64_t priorityQueueElements = 0;
    int64_t downloadQueueElements = 0;
    int64_t delayedCleanupElements = 0;
    int64_t reserveThreads = 0;

    // Cumulative counters (from ProfileEvents)
    uint64_t cacheReadBytes = 0;
    uint64_t sourceReadBytes = 0;
    uint64_t cacheWriteBytes = 0;
    uint64_t cacheHitCount = 0;
    uint64_t cacheMissCount = 0;
    uint64_t predownloadedFromSourceBytes = 0;
    uint64_t predownloadedBytes = 0;
    uint64_t reserveAttempts = 0;
    uint64_t reserveFailures = 0;
    uint64_t evictedBytes = 0;
    uint64_t evictedSegments = 0;
    uint64_t evictionTries = 0;
    uint64_t waitReadBufferMicroseconds = 0;
    uint64_t readFromSourceMicroseconds = 0;
    uint64_t predownloadedFromSourceMicroseconds = 0;
    uint64_t readFromCacheMicroseconds = 0;
    uint64_t cacheWriteMicroseconds = 0;
    uint64_t createBufferMicroseconds = 0;

    /// Subtract a previous snapshot to get deltas for cumulative counters.
    /// Gauge fields are taken from `*this` (the newer snapshot).
    FileCacheStatsSnapshot operator-(const FileCacheStatsSnapshot & prev) const;
};

/// Loads a point-in-time snapshot from the global metrics storage.
FileCacheStatsSnapshot takeFileCacheStatsSnapshot();

} // namespace facebook::velox::ch
