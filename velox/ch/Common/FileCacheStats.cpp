#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Common/CurrentMetrics.h"
#include "velox/ch/Common/ProfileEvents.h"

namespace facebook::velox::ch
{

FileCacheStatsSnapshot FileCacheStatsSnapshot::operator-(
    const FileCacheStatsSnapshot & prev) const
{
    FileCacheStatsSnapshot delta;
    // Gauges: take current (this) values
    delta.cacheSize = cacheSize;
    delta.cacheSizeLimit = cacheSizeLimit;
    delta.cacheKeys = cacheKeys;
    delta.cacheElements = cacheElements;
    delta.cacheFileSegments = cacheFileSegments;
    delta.holdFileSegments = holdFileSegments;
    delta.invalidatedElements = invalidatedElements;
    delta.priorityQueueElements = priorityQueueElements;
    delta.downloadQueueElements = downloadQueueElements;
    delta.delayedCleanupElements = delayedCleanupElements;
    delta.reserveThreads = reserveThreads;
    // Cumulative: subtract
    delta.cacheReadBytes = cacheReadBytes - prev.cacheReadBytes;
    delta.sourceReadBytes = sourceReadBytes - prev.sourceReadBytes;
    delta.cacheWriteBytes = cacheWriteBytes - prev.cacheWriteBytes;
    delta.cacheHitCount = cacheHitCount - prev.cacheHitCount;
    delta.cacheMissCount = cacheMissCount - prev.cacheMissCount;
    delta.predownloadedFromSourceBytes = predownloadedFromSourceBytes - prev.predownloadedFromSourceBytes;
    delta.predownloadedBytes = predownloadedBytes - prev.predownloadedBytes;
    delta.reserveAttempts = reserveAttempts - prev.reserveAttempts;
    delta.reserveFailures = reserveFailures - prev.reserveFailures;
    delta.evictedBytes = evictedBytes - prev.evictedBytes;
    delta.evictedSegments = evictedSegments - prev.evictedSegments;
    delta.evictionTries = evictionTries - prev.evictionTries;
    delta.waitReadBufferMicroseconds = waitReadBufferMicroseconds - prev.waitReadBufferMicroseconds;
    delta.readFromSourceMicroseconds = readFromSourceMicroseconds - prev.readFromSourceMicroseconds;
    delta.predownloadedFromSourceMicroseconds = predownloadedFromSourceMicroseconds - prev.predownloadedFromSourceMicroseconds;
    delta.readFromCacheMicroseconds = readFromCacheMicroseconds - prev.readFromCacheMicroseconds;
    delta.cacheWriteMicroseconds = cacheWriteMicroseconds - prev.cacheWriteMicroseconds;
    delta.createBufferMicroseconds = createBufferMicroseconds - prev.createBufferMicroseconds;
    return delta;
}

FileCacheStatsSnapshot takeFileCacheStatsSnapshot()
{
    FileCacheStatsSnapshot s;
    // Gauges
    s.cacheSize = CurrentMetrics::get(CurrentMetrics::FilesystemCacheSize);
    s.cacheSizeLimit = CurrentMetrics::get(CurrentMetrics::FilesystemCacheSizeLimit);
    s.cacheKeys = CurrentMetrics::get(CurrentMetrics::FilesystemCacheKeys);
    s.cacheElements = CurrentMetrics::get(CurrentMetrics::FilesystemCacheElements);
    s.cacheFileSegments = CurrentMetrics::get(CurrentMetrics::CacheFileSegments);
    s.holdFileSegments = CurrentMetrics::get(CurrentMetrics::FilesystemCacheHoldFileSegments);
    s.invalidatedElements = CurrentMetrics::get(CurrentMetrics::FilesystemCacheInvalidatedElements);
    s.priorityQueueElements = CurrentMetrics::get(CurrentMetrics::FilesystemCachePriorityQueueElements);
    s.downloadQueueElements = CurrentMetrics::get(CurrentMetrics::FilesystemCacheDownloadQueueElements);
    s.delayedCleanupElements = CurrentMetrics::get(CurrentMetrics::FilesystemCacheDelayedCleanupElements);
    s.reserveThreads = CurrentMetrics::get(CurrentMetrics::FilesystemCacheReserveThreads);
    // Cumulative
    s.cacheReadBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheBytes);
    s.sourceReadBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    s.cacheWriteBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes);
    s.cacheHitCount = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheHits);
    s.cacheMissCount = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheMisses);
    s.predownloadedFromSourceBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceBytes);
    s.predownloadedBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedBytes);
    s.reserveAttempts = ProfileEvents::get(ProfileEvents::FilesystemCacheReserveAttempts);
    s.reserveFailures = ProfileEvents::get(ProfileEvents::FilesystemCacheFailedReserveAttempts);
    s.evictedBytes = ProfileEvents::get(ProfileEvents::FilesystemCacheEvictedBytes);
    s.evictedSegments = ProfileEvents::get(ProfileEvents::FilesystemCacheEvictedFileSegments);
    s.evictionTries = ProfileEvents::get(ProfileEvents::FilesystemCacheEvictionTries);
    s.waitReadBufferMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferWaitReadBufferMicroseconds);
    s.readFromSourceMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceMicroseconds);
    s.predownloadedFromSourceMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds);
    s.readFromCacheMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheMicroseconds);
    s.cacheWriteMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteMicroseconds);
    s.createBufferMicroseconds = ProfileEvents::get(ProfileEvents::CachedReadBufferCreateBufferMicroseconds);
    return s;
}

} // namespace facebook::velox::ch
