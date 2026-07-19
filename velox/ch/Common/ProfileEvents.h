#pragma once

#include <cstdint>

namespace facebook::velox::ch {

namespace ProfileEvents {

enum Event {
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
};

inline void increment(Event, uint64_t = 1) {}

} // namespace ProfileEvents

template <typename Unit>
class ProfileEventTimeIncrement {
 public:
  explicit ProfileEventTimeIncrement(ProfileEvents::Event) {}
};

struct Microseconds {};

} // namespace facebook::velox::ch
