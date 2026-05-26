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
#include <thread>
#include <utility>

namespace facebook::velox {
class ReadFile;
} // namespace facebook::velox

namespace facebook::velox::cache::fs {

/// Single cache segment with a download state machine.
///
/// Lifecycle (per spec §5.4):
///   kEmpty --beginDownload()--> kDownloading --complete()--> kDownloaded
///                                            --abandon() w/ bytes--> kPartiallyDownloaded
///   kPartiallyDownloaded --beginDownload() resume--> kDownloading
///                        --metadata refuses resume--> kPartiallyDownloadedNoContinuation
///   {kDownloaded, kDownloading, kPartiallyDownloaded, kPartiallyDownloadedNoContinuation}
///       --evict() while readers active--> kDetached
///
/// Phase 1: downloads are synchronous and all-or-nothing — the segment is
/// either kEmpty or kDownloaded on disk; .tmp + rename keeps the published
/// file atomic. The 2 partial states (kPartiallyDownloaded,
/// kPartiallyDownloadedNoContinuation) are dead code at this commit; task 2-3
/// wire the reserve/write/complete/abandon paths that drive transitions into
/// them. Background / partial downloads return in phase 2.
class FileSegment {
 public:
  /// Download lifecycle state.
  enum class State : uint8_t {
    /// Metadata exists, no writer yet.
    kEmpty = 0,
    /// Single writer active; downloadedSize_ advancing.
    kDownloading = 1,
    /// Entire segment on disk.
    kDownloaded = 2,
    /// Writer abandoned with downloadedSize_ > 0; resume allowed.
    kPartiallyDownloaded = 3,
    /// Partial bytes on disk but metadata refused resume (phase-3 only).
    kPartiallyDownloadedNoContinuation = 4,
    /// Metadata removed; segment kept alive by active readers.
    kDetached = 5,
  };

  /// Constructs a fresh kEmpty segment for the given key. remotePath is the
  /// original (string) path used to fetch missing bytes from remote; it is
  /// stored alongside the (path-only-hashed) key because FsCacheKey itself no
  /// longer carries the human-readable path.
  FileSegment(FsCacheKey key, std::string remotePath)
      : key_{std::move(key)}, remotePath_{std::move(remotePath)} {}

  /// Closes a leaked writer fd if a kDownloading segment is dropped without
  /// complete() or abandon(). Normal paths close fd_ explicitly; this is a
  /// safety net for exception-throwing tests and future error paths.
  ~FileSegment();

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
  ///
  /// TODO: migrate EvictionPolicyTest off `beginDownload` / `download` to
  /// the reserve/write/complete path and remove. Post-Task 8 production
  /// callers no longer use this method; it stays for the LRU policy unit
  /// test that exercises this state machine directly.
  bool beginDownload();

  /// Downloads the segment from remote into cacheRoot. State must be
  /// kDownloading on entry (i.e. beginDownload() returned true). On success
  /// the segment transitions to kDownloaded and a sub-aligned file is
  /// published atomically via .tmp + rename. On failure the .tmp / final
  /// path are removed, downloadedSize_ is reset, state reverts to kEmpty,
  /// and the exception is rethrown.
  ///
  /// TODO: migrate EvictionPolicyTest off `beginDownload` / `download` and
  /// remove. Same rationale as `beginDownload` above.
  void download(
      ::facebook::velox::ReadFile& remote,
      const std::string& cacheRoot);

  /// Atomically transitions kEmpty -> kDownloading, opens the local cache
  /// file (O_CREAT|O_WRONLY, NO ftruncate), and records the calling thread
  /// as the writer. Returns false if the segment was not kEmpty (another
  /// writer already won or segment already kDownloaded).
  /// reservedBytes is the declared size; complete() will ftruncate to this.
  bool reserve(uint64_t reservedBytes, const std::string& cacheRoot);

  /// Appends `len` bytes at current downloadedSize_ via pwrite, advances
  /// downloadedSize_ release-store, notify_all on cv_. Must be called by
  /// the thread that won reserve(). Throws if downloadedSize_ + len would
  /// exceed reservedBytes.
  void write(const char* buf, uint64_t len);

  /// CAS kDownloading -> kDownloaded, ftruncate(fd_, key().size),
  /// fsync, close(fd_), notify_all. Must be called by the writer thread.
  void complete();

  /// CAS kDownloading -> kPartiallyDownloaded; close(fd_) without
  /// ftruncate (partial stat_size lets loadFromDisk delete the file on
  /// next restart per spec §5.5).
  void abandon();

  /// Returns the thread id of the current writer or a default-constructed
  /// id if no writer is active. Used by FileSegmentsHolder dtor to detect
  /// "this thread owns a leaked DOWNLOADING segment".
  std::thread::id getDownloader() const noexcept;

  /// Blocks the calling reader until downloadedSize_ >= needed OR the
  /// segment transitions out of kDownloading. If the segment ends in
  /// kPartiallyDownloaded / kPartiallyDownloadedNoContinuation / kDetached
  /// AND downloadedSize_ < needed, throws VeloxRuntimeError so the reader
  /// surfaces the writer's abandon instead of reading past the partial
  /// boundary. kDownloaded is always a clean wake.
  void waitForDownloadedSize(uint64_t needed);

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

  // Thread that won reserve(). Default-constructed when no writer is active.
  std::atomic<std::thread::id> downloader_{};

  // Open file descriptor for the writer path; -1 when no writer is active.
  int fd_{-1};

  // Bytes promised by reserve(); complete() ftruncates to key().size and
  // write() checks downloadedSize_ + len <= reservedBytes_.
  uint64_t reservedBytes_{0};

  // Phase 1: hits_ is recorded but not consumed; SLRU promotion lands in
  // phase 2.
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> refCount_{0};
};

} // namespace facebook::velox::cache::fs
