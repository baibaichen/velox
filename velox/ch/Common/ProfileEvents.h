#pragma once

#include <cstddef>
#include <cstdint>

namespace facebook::velox::ch
{

namespace ProfileEvents
{

enum Event
{
    // 50 existing events (unchanged order)
    FilesystemCacheGetOrSetMicroseconds,
    FilesystemCacheGetMicroseconds,
    FilesystemCacheReserveAttempts,
    FilesystemCacheFailedReserveAttempts,
    FilesystemCacheReserveMicroseconds,
    CachedReadBufferReadFromCacheBytes,
    CachedReadBufferReadFromSourceBytes,
    CachedReadBufferCacheWriteBytes,
    FileSegmentWaitMicroseconds,
    FileSegmentWriteMicroseconds,
    FileSegmentCompleteMicroseconds,
    FilesystemCacheCheckCorrectness,
    FilesystemCacheCheckCorrectnessMicroseconds,
    FilesystemCacheStateLockMicroseconds,
    FilesystemCachePriorityWriteLockMicroseconds,
    FilesystemCachePriorityReadLockMicroseconds,
    FileSegmentFailToIncreasePriority,
    FileSegmentHolderCompleteMicroseconds,
    FileSegmentIncreasePriorityMicroseconds,
    FileSegmentLockMicroseconds,
    FilesystemCacheBackgroundDownloadQueuePush,
    FilesystemCacheBackgroundEvictedBytes,
    FilesystemCacheBackgroundEvictedFileSegments,
    FilesystemCacheBackgroundRemovedInvalidatedEntries,
    FilesystemCacheCreatedKeyDirectories,
    FilesystemCacheDowngradedFileSegments,
    FilesystemCacheEvictMicroseconds,
    FilesystemCacheEvictedBytes,
    FilesystemCacheEvictedFileSegments,
    FilesystemCacheEvictionReusedIterator,
    FilesystemCacheEvictionSkippedEvictingFileSegments,
    FilesystemCacheEvictionSkippedFileSegments,
    FilesystemCacheEvictionSkippedMovingFileSegments,
    FilesystemCacheEvictionTries,
    FilesystemCacheFailToReserveSpaceBecauseOfCacheResize,
    FilesystemCacheFailedEvictionCandidates,
    FilesystemCacheFreeSpaceKeepingThreadErrors,
    FilesystemCacheFreeSpaceKeepingThreadRun,
    FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds,
    FilesystemCacheHoldFileSegments,
    FilesystemCacheIdleClientEvictions,
    FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds,
    FilesystemCacheLoadMetadataMicroseconds,
    FilesystemCacheLockKeyMicroseconds,
    FilesystemCacheLockMetadataMicroseconds,
    FilesystemCacheLockOriginPoolMicroseconds,
    FilesystemCacheUnusedHoldFileSegments,
    OpenedFileCacheHits,
    OpenedFileCacheMisses,
    OpenedFileCacheMicroseconds,
    // 10 new CH CachedOnDiskReadBufferFromFile reader events
    CachedReadBufferWaitReadBufferMicroseconds,
    CachedReadBufferReadFromSourceMicroseconds,
    CachedReadBufferPredownloadedFromSourceMicroseconds,
    CachedReadBufferReadFromCacheMicroseconds,
    CachedReadBufferCacheWriteMicroseconds,
    CachedReadBufferPredownloadedFromSourceBytes,
    CachedReadBufferPredownloadedBytes,
    CachedReadBufferCreateBufferMicroseconds,
    CachedReadBufferReadFromCacheHits,
    CachedReadBufferReadFromCacheMisses,
    END
};

inline constexpr size_t kNumEvents = static_cast<size_t>(END);

void increment(Event e, uint64_t delta = 1);
uint64_t get(Event e);

} // namespace ProfileEvents

struct Microseconds {};

template <typename Unit>
class ProfileEventTimeIncrement
{
public:
    explicit ProfileEventTimeIncrement(ProfileEvents::Event e);
    ~ProfileEventTimeIncrement();
    ProfileEventTimeIncrement(const ProfileEventTimeIncrement &) = delete;
    ProfileEventTimeIncrement & operator=(const ProfileEventTimeIncrement &) = delete;

    uint64_t elapsed() const;

private:
    ProfileEvents::Event event_;
    uint64_t startNs_;
};

} // namespace facebook::velox::ch
