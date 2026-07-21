#pragma once

#include <cstddef>
#include <cstdint>

namespace facebook::velox::ch
{

namespace CurrentMetrics
{

enum Metric
{
    CacheFileSegments,
    FilesystemCacheHoldFileSegments,
    FilesystemCacheDownloadQueueElements,
    FilesystemCacheDelayedCleanupElements,
    FilesystemCacheReserveThreads,
    FilesystemCacheSizeLimit,
    FilesystemCacheElements,
    FilesystemCacheInvalidatedElements,
    FilesystemCachePriorityQueueElements,
    FilesystemCacheSize,
    FilesystemCacheKeys,
    END
};

inline constexpr size_t kNumMetrics = static_cast<size_t>(END);

void add(Metric m, int64_t delta = 1);
void sub(Metric m, int64_t delta = 1);
int64_t get(Metric m);
void set(Metric m, int64_t v);

class Increment
{
public:
    explicit Increment(Metric m, int64_t delta = 1);
    ~Increment();
    Increment(const Increment &) = delete;
    Increment & operator=(const Increment &) = delete;

private:
    Metric metric_;
    int64_t delta_;
};

} // namespace CurrentMetrics

} // namespace facebook::velox::ch
