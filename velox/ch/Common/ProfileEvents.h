#pragma once

#include <atomic>
#include <chrono>
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
  // Sentinel: number of events. Keep last.
  kEventCount,
};

namespace detail {
// Task 017: real process-wide event counters. One relaxed atomic per event.
// Reset on process exit is acceptable (no persistence contract).
extern std::atomic<uint64_t> values[static_cast<int>(kEventCount)];
} // namespace detail

inline uint64_t get(Event e) {
  return detail::values[static_cast<int>(e)].load(std::memory_order_relaxed);
}

inline void increment(Event e, uint64_t delta = 1) {
  detail::values[static_cast<int>(e)].fetch_add(delta, std::memory_order_relaxed);
}

} // namespace ProfileEvents

/// RAII timer that records elapsed microseconds into `event` on destruction.
///
/// The `Unit` template parameter is retained for CH call-shape compatibility
/// (`ProfileEventTimeIncrement<Microseconds>` in `Guards.h` and `FileSegment`).
/// All current call sites use `Microseconds`, so the primary template records in
/// microseconds; the `Microseconds` specialisation makes the unit explicit.
template <typename Unit>
class ProfileEventTimeIncrement {
 public:
  explicit ProfileEventTimeIncrement(ProfileEvents::Event event)
      : event_(event), start_(std::chrono::steady_clock::now()) {}

  ~ProfileEventTimeIncrement() {
    using namespace std::chrono;
    const auto us = duration_cast<microseconds>(steady_clock::now() - start_);
    ProfileEvents::increment(event_, static_cast<uint64_t>(us.count()));
  }

  ProfileEventTimeIncrement(const ProfileEventTimeIncrement&) = delete;
  ProfileEventTimeIncrement& operator=(const ProfileEventTimeIncrement&) = delete;

 private:
  ProfileEvents::Event event_;
  std::chrono::steady_clock::time_point start_;
};

struct Microseconds {};

template <>
class ProfileEventTimeIncrement<Microseconds> {
 public:
  explicit ProfileEventTimeIncrement(ProfileEvents::Event event)
      : event_(event), start_(std::chrono::steady_clock::now()) {}

  ~ProfileEventTimeIncrement() {
    using namespace std::chrono;
    const auto us = duration_cast<microseconds>(steady_clock::now() - start_);
    ProfileEvents::increment(event_, static_cast<uint64_t>(us.count()));
  }

  ProfileEventTimeIncrement(const ProfileEventTimeIncrement&) = delete;
  ProfileEventTimeIncrement& operator=(const ProfileEventTimeIncrement&) = delete;

 private:
  ProfileEvents::Event event_;
  std::chrono::steady_clock::time_point start_;
};

} // namespace facebook::velox::ch
