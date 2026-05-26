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

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FileSegmentsHolder.h"
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

/// Selects which pair of stats counters a `FsCache::getOrSet` call bumps.
/// Task 8 wires the parameter through the signature; Task 14 wires the
/// actual 4-counter accounting. The prefetch callsite in FsCacheBufferedInput
/// flips to `kPrefetch` in Task 11 step 4.
enum class IsPrefetch : uint8_t { kPrefetch, kDemand };

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

  /// CH-aligned entry point. Returns a holder of segments covering
  /// [offset, min(offset + size, remote.size())) — contiguous, offset-
  /// ascending, and (in this commit) all in state kDownloaded on return
  /// thanks to the transitional shim documented in the implementation. The
  /// requested range is clamped to remote.size() so reads near EOF do not
  /// overshoot the file; the last returned segment's key.size therefore
  /// reflects the true byte count, not the splitRange outward-alignment
  /// overshoot. Returns a holder over an empty vector when
  /// offset == remote.size() or size == 0.
  ///
  /// `settings` controls hole slicing (alignment / maxSegmentSize) per spec
  /// §5.3 fillHoles. Phase-2 callers pass `&config_` so all callers share
  /// the cache-level defaults; the parameter is kept separate so a future
  /// caller can tune per-call (e.g. larger alignment for cold scans)
  /// without touching FsCacheConfig. In this commit `settings` is accepted
  /// for signature stability but not yet propagated (the implementation
  /// uses `config_` directly); Task 14 forwards it through.
  ///
  /// `isPrefetch` selects which pair of stats counters to bump. Task 8
  /// only forwards the value; Task 14 wires the actual prefetch/demand
  /// counter pairs. There is intentionally no default — every caller
  /// decides.
  ///
  /// VELOX_USER_CHECKs that offset <= remote.size().
  FileSegmentsHolderPtr getOrSet(
      const std::string& path,
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& settings,
      ::facebook::velox::ReadFile& remote,
      IsPrefetch isPrefetch);

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

  /// Given `found` (from FsCacheMetadata::lookupRange) and the requested range
  /// [lo, hi), returns a continuous list covering [lo, hi) where every gap is
  /// filled with newly inserted kEmpty segments sliced by cfg.maxSegmentSize.
  /// Caller MUST already hold `lockedKey` for `path`; new segments are
  /// inserted directly into `lockedKey->segments` (not via
  /// FsCacheMetadata::insert, which would re-acquire the same per-key mutex
  /// and deadlock). The `metadata` parameter is reserved for future per-bucket
  /// accounting and currently only documents the contract.
  static std::vector<FileSegmentPtr> fillHolesWithEmptyFileSegments(
      std::vector<FileSegmentPtr> found,
      uint64_t lo,
      uint64_t hi,
      const PathKey& path,
      const std::string& remotePath,
      LockedKey& lockedKey,
      FsCacheMetadata& metadata,
      const FsCacheConfig& cfg);

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
  /// the new getOrSet writer path then performs the normal onInsert +
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

  /// Records a cache hit: touches LRU and bumps counters_.hits.
  void recordHit(FileSegment* segment);

  /// Records a fresh miss: inserts into LRU and bumps counters_.misses /
  /// counters_.bytesOnDisk by segmentSize.
  void recordMiss(FileSegment* segment, uint64_t segmentSize);

  /// Evicts until bytesOnDisk + bytesNeeded <= maxBytes. Selects victims from
  /// each bucket's per-bucket EvictionPolicy under that bucket's
  /// CachePriorityMutex, removes the on-disk file first, then drops the entry
  /// from the bucket policy and metadata_ to avoid orphan files on
  /// filesystem-remove failure. Public so the caller
  /// (FsCacheBufferedInput::load / test driveSegments) can drain capacity
  /// before reserving a fresh segment.
  ///
  /// CAVEAT: this invariant is only single-writer tight. evict() reads
  /// counters_.bytesOnDisk to decide whether to drain, but recordMiss() does
  /// not credit the in-flight reservation until AFTER download() finishes,
  /// so N concurrent miss-path writers can each independently observe
  /// headroom and skip eviction. The worst-case transient over-shoot is
  /// maxBytes + N * segmentSize before the next miss path's evict() drains
  /// it back; in production this is a tiny fraction of maxBytes (e.g. 64
  /// writers x 8 MiB segment = 512 MiB on top of a 100 GiB cache). Phase 2
  /// will fold this into the in-flight reservation accounting tracked
  /// alongside the warm-restart orphan-bytes counter (see loadFromDisk).
  void evict(uint64_t bytesNeeded);

 private:
  // Live atomic counters. stats() composes an FsCacheStats POD snapshot from
  // per-field relaxed loads. Per-field atomicity is enough for the
  // observability use case. Snapshots taken concurrently with writers can
  // observe arbitrary cross-field skew (the four loads are independent);
  // callers needing a consistent snapshot (e.g. tests asserting hits+misses
  // == total) must first quiesce writers (e.g. thread.join()).
  struct AtomicCounters {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> evictions{0};
    std::atomic<uint64_t> bytesOnDisk{0};
  };

  const FsCacheConfig config_;
  std::unique_ptr<FsCacheMetadata> metadata_;

  // Per-bucket CachePriorityMutex and EvictionPolicy live inside
  // FsCacheMetadata::Bucket; per-key download serialization lives inside
  // KeyMetadata. FsCache only retains a global evictionMutex_ that serializes
  // concurrent evict() calls so two writers cannot pick the same victim
  // from a bucket's policy.
  //
  // Plain std::mutex (not RankedMutex) because evictionMutex_ is held
  // strictly outside the priority/state/metadata acquisitions inside the
  // eviction loop, so no rank ordering with those locks applies.
  mutable std::mutex evictionMutex_;
  AtomicCounters counters_;

  // Rotating start offset for evict() round-robin across buckets.
  // fetch_add(1, relaxed) per evict() call so a bucket that is "first" in
  // one call is "last" in the next; distributes the eviction load and
  // prevents always-evict-from-bucket-0 starvation.
  std::atomic<size_t> evictStart_{0};
};

} // namespace facebook::velox::cache::fs
