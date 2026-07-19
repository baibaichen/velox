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
  // B1: enumerator name surface required by the center SCC (Tasks 011/012).
  // Real counters remain Task 017; increment stays no-op.
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
  FilesystemCacheEvictedBytes,
  FilesystemCacheEvictedFileSegments,
  FilesystemCacheEvictionReusedIterator,
  FilesystemCacheEvictionSkippedEvictingFileSegments,
  FilesystemCacheEvictionSkippedFileSegments,
  FilesystemCacheEvictionSkippedMovingFileSegments,
  FilesystemCacheEvictionTries,
  FilesystemCacheEvictMicroseconds,
  FilesystemCacheFailedEvictionCandidates,
  FilesystemCacheFailToReserveSpaceBecauseOfCacheResize,
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
