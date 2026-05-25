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

#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

namespace facebook::velox {
class ReadFile;
} // namespace facebook::velox

namespace facebook::velox::cache::fs {

/// Single cache segment with a download state machine.
///
/// Lifecycle:
///   kEmpty --beginDownload()--> kDownloading --download() ok--> kDownloaded
///                                            --download() throws--> kEmpty
///   kDownloaded --evict() while readers active--> kDetached
///
/// Phase 1: downloads are synchronous and all-or-nothing — the segment is
/// either kEmpty or kDownloaded on disk; .tmp + rename keeps the published
/// file atomic. Background / partial downloads return in phase 2.
class FileSegment {
 public:
  /// Download lifecycle state.
  enum class State : uint8_t {
    kEmpty = 0,
    kDownloading = 1,
    kDownloaded = 2,
    kDetached = 3,
  };

  /// Constructs a fresh kEmpty segment for the given key. remotePath is the
  /// original (string) path used to fetch missing bytes from remote; it is
  /// stored alongside the (path-only-hashed) key because FsCacheKey itself no
  /// longer carries the human-readable path.
  FileSegment(FsCacheKey key, std::string remotePath)
      : key_{std::move(key)}, remotePath_{std::move(remotePath)} {}

  /// Returns the cache key (path/offset/size) that identifies this segment.
  const FsCacheKey& key() const {
    return key_;
  }

  /// Returns the remote file path string for download() and diagnostics.
  const std::string& remotePath() const {
    return remotePath_;
  }

  /// Returns the segment size in bytes; equal to key().size.
  uint64_t size() const {
    return key_.size;
  }

  /// Returns the current lifecycle state.
  State state() const {
    return state_.load(std::memory_order_acquire);
  }

  /// Returns the number of bytes downloaded so far. Equals size() once the
  /// segment reaches kDownloaded; 0 after a failed download.
  uint64_t downloadedSize() const {
    return downloadedSize_.load(std::memory_order_acquire);
  }

  /// Returns the absolute local path under cacheRoot for this segment. Does
  /// not touch the filesystem. Layout is "<cacheRoot>/<hex[0:2]>/<hex[2:4]>/
  /// <fileName>" where <hex> is the 16-hex-digit hash from key().fileName().
  std::string localPath(const std::string& cacheRoot) const;

  /// Atomically transitions kEmpty -> kDownloading. Returns true if this
  /// caller won the CAS race and must follow up with download(); returns
  /// false otherwise. FsCache calls this under mutex_ during coordination;
  /// standalone callers (and tests) must call it before download().
  bool beginDownload();

  /// Downloads the segment from remote into cacheRoot. State must be
  /// kDownloading on entry (i.e. beginDownload() returned true). On success
  /// the segment transitions to kDownloaded and a sub-aligned file is
  /// published atomically via .tmp + rename. On failure the .tmp / final
  /// path are removed, downloadedSize_ is reset, state reverts to kEmpty,
  /// and the exception is rethrown.
  void download(
      ::facebook::velox::ReadFile& remote,
      const std::string& cacheRoot);

  /// Reads bytes [offsetInSegment, offsetInSegment + length) from the local
  /// file into outBuf. State must be kDownloaded or kDetached.
  void read(
      uint64_t offsetInSegment,
      uint64_t length,
      char* outBuf,
      const std::string& cacheRoot) const;

 public:
  /// Held by FsCache::lookupOrCreate while CAS-ing state_ and waiting on cv_.
  /// Exposed publicly (not via friend) because FileSegment is a coordination
  /// object whose synchronization is orchestrated by FsCache; hiding
  /// mutex_/cv_ would just push FsCache logic into FileSegment.
  ///
  /// TODO: phase 2 will move per-key serialization into a KeyMutex on the
  /// metadata side; at that point FileSegment can expose waitForDownload() /
  /// notifyAll() helpers and drop the public mutex_/cv_.
  mutable FileSegmentMutex mutex_;

  /// Collapses concurrent LRU bumps on the same segment. FsCache::recordHit()
  /// callers take this mutex with try_to_lock; losers skip the bump (LRU bump is
  /// best-effort, and one bump per burst of concurrent hits is enough to
  /// move the segment toward MRU). Plain std::mutex (not RankedMutex)
  /// because it is acquired strictly outside any cache-level mutex chain
  /// and held only across a single onHit() call.
  mutable std::mutex increasePriorityMutex_;

  /// Notifies waiters when state_ leaves kDownloading. Paired with mutex_.
  mutable std::condition_variable_any cv_;

 private:
  FsCacheKey key_;
  std::string remotePath_;
  std::atomic<State> state_{State::kEmpty};
  std::atomic<uint64_t> downloadedSize_{0};

  // Phase 1: hits_ is recorded but not consumed; SLRU promotion lands in
  // phase 2.
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> refCount_{0};
};

} // namespace facebook::velox::cache::fs
