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

#include "velox/common/caching/fscache/EvictionPolicy.h"
#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/caching/fscache/FsCacheMetadata.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace facebook::velox {
class ReadFile;
} // namespace facebook::velox

namespace facebook::velox::cache::fs {

/// Counters exposed to tests and observability. All fields are sampled atomic
/// totals; differences between two snapshots give per-interval rates.
struct FsCacheStats {
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t evictions{0};
  uint64_t bytesOnDisk{0};
};

/// Top-level entry point of the FsCache module. Owns the metadata index and
/// eviction policy; downloads missing segments synchronously through a
/// FileSegment state machine.
class FsCache {
 public:
  explicit FsCache(FsCacheConfig config);
  ~FsCache();

  /// Returns segments covering [offset, min(offset + size, remote.size()))
  /// for the given path, downloading any missing segments synchronously from
  /// remote. The requested range is clamped to remote.size() so reads near
  /// EOF (or on files smaller than config.alignment) do not overshoot the
  /// file --- the last returned segment's key.size therefore reflects the
  /// true byte count, not the splitRange outward-alignment overshoot.
  /// Returns an empty vector when offset == remote.size() or size == 0.
  /// VELOX_USER_CHECKs that offset <= remote.size(). Segment boundaries
  /// otherwise follow splitRange(offset, clampedSize, config); each returned
  /// segment is in state kDownloaded on return.
  std::vector<FileSegmentPtr> getOrSet(
      const std::string& path,
      uint64_t offset,
      uint64_t size,
      ::facebook::velox::ReadFile& remote);

  /// Snapshot of counters. Cheap; intended for tests and observability.
  FsCacheStats stats() const;

  /// Returns the immutable config (cacheRoot, maxBytes, segmentSize,
  /// numBuckets). Public so callers like FsCacheBufferedInput can resolve
  /// cacheRoot when constructing input streams over downloaded segments.
  const FsCacheConfig& config() const {
    return config_;
  }

  /// Process-global singleton. Mirrors AsyncDataCache::getInstance(). The
  /// caller owns the FsCache lifetime; this only stores a raw pointer. Used
  /// by the default-init in QueryCtx so bench / test setups that want FsCache
  /// plumbing can install one without touching every QueryCtx construction
  /// site.
  static FsCache* getInstance();

  /// Installs the singleton. Pass nullptr on teardown to reset.
  static void setInstance(FsCache* instance);

  /// Splits an arbitrary [offset, offset + size) range into aligned cache
  /// segments. The outer boundaries are snapped to config.alignment; internal
  /// cuts produced by the maxSegmentSize chunk loop preserve aligned starts
  /// but may have sub-alignment tail size. Each returned (offset, size) is
  /// at most config.maxSegmentSize bytes. Returns an empty vector if size
  /// is zero.
  static std::vector<std::pair<uint64_t, uint64_t>> splitRange(
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& config);

  /// Scans cacheRoot for cache files left over from a previous run. Removes
  /// .tmp files and size-mismatched files, then rmdirs any subdirectory left
  /// empty afterwards. Does NOT repopulate metadata_ (the on-disk filename
  /// encodes only the hash, not the original remote path, so a
  /// FsCacheKey{path, offset, size} cannot be reconstructed) and does NOT
  /// credit surviving files to counters_.bytesOnDisk: orphan files that are
  /// never re-requested would then be untracked by the LruPolicy and could
  /// not be evicted, eventually filling the disk. Instead, surviving files
  /// are picked up on demand --- the next getOrSet() for the same key hashes
  /// to the same on-disk filename, FileSegment::download() short-circuits
  /// when it finds the file already present with the expected size, and
  /// lookupOrCreate()'s writer path then performs the normal onInsert +
  /// bytesOnDisk accounting so the segment participates in eviction.
  /// Idempotent.
  ///
  /// CAVEAT: between loadFromDisk() and the demand-pickup of all survivors,
  /// the cache will tolerate up to `survivor_bytes` of additional downloads
  /// before evict() catches up, so on-disk usage may transiently reach
  /// roughly `maxBytes + survivor_bytes` (worst case ~2x maxBytes if the
  /// previous run filled the cache). Phase 2 will track an orphan-bytes
  /// counter so evict() sees the true on-disk total during the warmup
  /// window.
  ///
  /// NOT called from the FsCache constructor; the caller (typically the
  /// Velox process startup hook that constructs the singleton FsCache) must
  /// invoke it explicitly before serving traffic if persistence across
  /// restarts is desired, AND must invoke it BEFORE any concurrent thread
  /// calls getOrSet() --- calling it concurrently with active downloads
  /// would race against in-flight .tmp files. This also keeps construction
  /// side-effect-free and keeps unit tests from paying directory-scan cost.
  void loadFromDisk();

 private:
  // Looks up an existing segment or coordinates a fresh download. Single
  // writer per key via FileSegment::beginDownload(); concurrent callers wait
  // on FileSegment::cv_ for the writer's outcome. path is the original remote
  // path string, passed through to FileSegment so it can be used by download()
  // and diagnostics — the (path-only-hashed) FsCacheKey does not carry it.
  FileSegmentPtr lookupOrCreate(
      const FsCacheKey& key,
      const std::string& path,
      ::facebook::velox::ReadFile& remote);

  // Evicts until bytesOnDisk + bytesNeeded <= maxBytes. Selects victims via
  // policy_, removes the on-disk file first, then drops the entry from
  // policy_/metadata_ to avoid orphan files on filesystem-remove failure.
  //
  // CAVEAT: this invariant is only single-writer tight. evict() reads
  // counters_.bytesOnDisk to decide whether to drain, but recordMiss() does
  // not credit the in-flight reservation until AFTER download() finishes,
  // so N concurrent miss-path writers can each independently observe
  // headroom and skip eviction. The worst-case transient over-shoot is
  // maxBytes + N * segmentSize before the next miss path's evict() drains
  // it back; in production this is a tiny fraction of maxBytes (e.g. 64
  // writers x 8 MiB segment = 512 MiB on top of a 100 GiB cache). Phase 2
  // will fold this into the in-flight reservation accounting tracked
  // alongside the warm-restart orphan-bytes counter (see loadFromDisk).
  void evict(uint64_t bytesNeeded);

  // Records a cache hit: touches LRU and bumps counters_.hits.
  void recordHit(FileSegment* segment);

  // Records a fresh miss: inserts into LRU and bumps counters_.misses /
  // counters_.bytesOnDisk by segmentSize.
  void recordMiss(FileSegment* segment, uint64_t segmentSize);

  // Live atomic counters. stats() composes an FsCacheStats POD snapshot from
  // per-field relaxed loads. Per-field atomicity is enough for the
  // observability use case; cross-field consistency between hits/misses/bytes
  // is best-effort.
  struct AtomicCounters {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> evictions{0};
    std::atomic<uint64_t> bytesOnDisk{0};
  };

  const FsCacheConfig config_;
  std::unique_ptr<FsCacheMetadata> metadata_;
  std::unique_ptr<EvictionPolicy> policy_;

  // Phase 1 lock instances. CacheMetadataMutex lives inside FsCacheMetadata;
  // FileSegmentMutex lives inside each FileSegment; KeyMutex is reserved
  // for phase 2 (per-key serialization of concurrent downloads).
  mutable CachePriorityMutex priorityMutex_;
  mutable CacheStateMutex stateMutex_;
  // Serializes evict(). Two concurrent writers calling evict() could otherwise
  // each receive the same victim from selectVictims() (which does not detach
  // entries from the LRU list), causing the second thread to dereference a
  // FileSegment whose owning shared_ptr has already been dropped by the first
  // thread's metadata_->erase(). Plain std::mutex (not RankedMutex) because
  // it is held strictly outside the priority/state/metadata acquisitions
  // inside the eviction loop, so no rank ordering with those locks applies.
  mutable std::mutex evictionMutex_;
  AtomicCounters counters_;
};

} // namespace facebook::velox::cache::fs
