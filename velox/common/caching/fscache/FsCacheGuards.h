/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <mutex>

namespace facebook::velox::cache::fs {

/// Lock hierarchy. Acquire in this top-to-bottom order to avoid deadlock:
///
///   CachePriorityGuard > CacheStateGuard > CacheMetadataGuard
///     > KeyGuard > FileSegmentGuard
///
/// Phase 1: each guard type wraps its own std::mutex; FsCache holds exactly
/// one mutex of each priority/state/metadata type and one FileSegmentMutex
/// per FileSegment. Per-key / per-bucket subdivision (where KeyMutex /
/// FileSegmentMutex live alongside the bucket they protect) is a phase 2
/// concern — the type names and rank ordering stay stable so callers do not
/// change.

/// Order rank used by LockOrderChecker. Lower values must be acquired before
/// higher values on the same thread.
enum class LockRank : int {
  kCachePriority = 0,
  kCacheState = 1,
  kCacheMetadata = 2,
  kKey = 3,
  kFileSegment = 4,
};

#ifndef NDEBUG
/// Records the highest lock rank held on the current thread and fires a CHECK
/// if a lower-ranked lock is then acquired. Debug builds only.
class LockOrderChecker {
 public:
  /// Pushes rank onto the thread-local stack and asserts it is strictly
  /// greater than the current top. Call after the underlying mutex is locked.
  static void onAcquire(LockRank rank);

  /// Pops the thread-local stack and asserts the released rank matches the
  /// top. Call before unlocking the underlying mutex.
  static void onRelease(LockRank rank);
};
#endif

namespace detail {

/// Mutex tagged with a compile-time rank. Behaves like std::mutex; in debug
/// builds, each lock/unlock notifies LockOrderChecker so out-of-order
/// acquisitions on the same thread fire a CHECK.
template <LockRank kRank>
class RankedMutex {
 public:
  /// Locks the underlying mutex, then records the rank in debug builds.
  void lock() {
    mutex_.lock();
#ifndef NDEBUG
    LockOrderChecker::onAcquire(kRank);
#endif
  }

  /// Releases the rank in debug builds, then unlocks the underlying mutex.
  void unlock() {
#ifndef NDEBUG
    LockOrderChecker::onRelease(kRank);
#endif
    mutex_.unlock();
  }

  /// Attempts to acquire the mutex without blocking; records the rank only on
  /// success. Returns true if the lock was acquired.
  bool try_lock() {
    if (!mutex_.try_lock()) {
      return false;
    }
#ifndef NDEBUG
    LockOrderChecker::onAcquire(kRank);
#endif
    return true;
  }

 private:
  std::mutex mutex_;
};

/// RAII scoped lock for a RankedMutex. Locks on construction and unlocks on
/// destruction; non-copyable, non-movable.
template <LockRank kRank>
class RankedGuard {
 public:
  /// Locks the given mutex; blocks until acquired.
  explicit RankedGuard(RankedMutex<kRank>& mutex) : mutex_{&mutex} {
    mutex_->lock();
  }

  /// Unlocks the mutex.
  ~RankedGuard() {
    mutex_->unlock();
  }

  RankedGuard(const RankedGuard&) = delete;
  RankedGuard& operator=(const RankedGuard&) = delete;

 private:
  RankedMutex<kRank>* mutex_;
};

} // namespace detail

using CachePriorityMutex = detail::RankedMutex<LockRank::kCachePriority>;
using CacheStateMutex = detail::RankedMutex<LockRank::kCacheState>;
using CacheMetadataMutex = detail::RankedMutex<LockRank::kCacheMetadata>;
using KeyMutex = detail::RankedMutex<LockRank::kKey>;
using FileSegmentMutex = detail::RankedMutex<LockRank::kFileSegment>;

using CachePriorityGuard = detail::RankedGuard<LockRank::kCachePriority>;
using CacheStateGuard = detail::RankedGuard<LockRank::kCacheState>;
using CacheMetadataGuard = detail::RankedGuard<LockRank::kCacheMetadata>;
using KeyGuard = detail::RankedGuard<LockRank::kKey>;
using FileSegmentGuard = detail::RankedGuard<LockRank::kFileSegment>;

} // namespace facebook::velox::cache::fs
