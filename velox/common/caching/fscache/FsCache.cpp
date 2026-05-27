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

#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/LruPolicy.h"
#include "velox/common/caching/fscache/SlruPolicy.h"
#include "velox/common/file/File.h"

#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <string_view>

namespace facebook::velox::cache::fs {

FsCache::FsCache(FsCacheConfig config)
    : config_{std::move(config)},
      metadata_{std::make_unique<FsCacheMetadata>(
          config_.numBuckets,
          [&config = std::as_const(config_)]()
              -> std::unique_ptr<EvictionPolicy> {
            if (!config.enableSlru) {
              return std::make_unique<LruPolicy>();
            }
            // Per-bucket SLRU mirrors the per-bucket LruPolicy layout: each
            // bucket owns its own protected/probationary pair sized to the
            // average per-bucket budget. CH does not shard, but Velox's 1024
            // buckets share the global maxBytes evenly, so the natural
            // per-bucket capacity is maxBytes / numBuckets. CH semantics
            // (promote-on-first-hit, demote-on-protected-overflow) are honored
            // within each bucket — see SLRUFileCachePriority::tryIncreasePriority
            // (SLRUFileCachePriority.cpp:586, move-to-protected at :705).
            const uint64_t perBucketCapacity =
                config.maxBytes / config.numBuckets;
            return std::make_unique<SlruPolicy>(
                perBucketCapacity, config.slruProtectedRatio);
          })},
      downloadPool_{
          std::make_unique<DownloadThreadPool>(config_.downloadThreads)} {
  // splitRange relies on maxSegmentSize being a multiple of alignment so
  // chunk starts remain aligned. Validate at construction since the
  // FsCacheConfig struct itself has no constructor to enforce it.
  VELOX_CHECK_GT(config_.alignment, 0, "FsCacheConfig.alignment must be > 0");
  VELOX_CHECK_GT(
      config_.maxSegmentSize, 0, "FsCacheConfig.maxSegmentSize must be > 0");
  VELOX_CHECK_EQ(
      config_.maxSegmentSize % config_.alignment,
      0,
      "FsCacheConfig.maxSegmentSize must be a multiple of alignment, got maxSegmentSize={} alignment={}",
      config_.maxSegmentSize,
      config_.alignment);
}

FsCache::~FsCache() = default;

namespace {
// Local-static-pointer idiom (mirrors AsyncDataCache::getInstance): avoids
// needing a class-scope static definition while still giving the singleton a
// single canonical storage cell.
FsCache** instancePtr() {
  static FsCache* instance{nullptr};
  return &instance;
}
} // namespace

// static
FsCache* FsCache::getInstance() {
  return *instancePtr();
}

// static
void FsCache::setInstance(FsCache* instance) {
  *instancePtr() = instance;
}

std::vector<std::pair<uint64_t, uint64_t>> FsCache::splitRange(
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& config) {
  std::vector<std::pair<uint64_t, uint64_t>> result;
  if (size == 0) {
    return result;
  }
  // Only the outer boundaries [alignedStart, alignedEnd) are snapped to
  // alignment. The internal cuts produced by the maxSegmentSize chunk loop
  // are NOT re-aligned, so e.g. alignment=4MiB, maxSegmentSize=32MiB,
  // size=40MiB yields [0, 32MiB), [32MiB, 40MiB) -- the second chunk's size
  // is unaligned (its start is still aligned because 32MiB is a multiple of
  // 4MiB, guaranteed by the contract that maxSegmentSize % alignment == 0).
  // ClickHouse behaves the same way: alignment is a sharing key for adjacent
  // reads, not a strict invariant on every segment.
  const uint64_t alignedStart = (offset / config.alignment) * config.alignment;
  const uint64_t end = offset + size;
  const uint64_t alignedEnd =
      ((end + config.alignment - 1) / config.alignment) * config.alignment;
  // The size>0 early-return plus outward alignment guarantees
  // alignedStart < alignedEnd, so at least one chunk is produced; the
  // VELOX_CHECK_GT guards against a future refactor accidentally producing
  // a zero-size chunk (which would create a "<hash>.<offset>.0" cache file
  // that crash recovery would silently accept).
  uint64_t cursor = alignedStart;
  while (cursor < alignedEnd) {
    const uint64_t chunkSize =
        std::min(config.maxSegmentSize, alignedEnd - cursor);
    VELOX_CHECK_GT(chunkSize, 0);
    result.emplace_back(cursor, chunkSize);
    cursor += chunkSize;
  }
  return result;
}

namespace {

// Splits [lo, hi) into kEmpty segments of size <= maxSegmentSize and
// emplaces them into lockedKey.segments(). Pushes the new shared_ptrs onto
// `out` in offset-ascending order. Caller must hold `lockedKey`; this is
// what makes the segments-map mutation race-free.
void sliceHoleAndInsert(
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,
    const FsCacheConfig& cfg,
    std::vector<FileSegmentPtr>& out) {
  auto* keyMeta = lockedKey.get();
  VELOX_CHECK_NOT_NULL(
      keyMeta, "sliceHoleAndInsert requires a non-empty LockedKey");
  uint64_t cursor{lo};
  while (cursor < hi) {
    const uint64_t size = std::min(cfg.maxSegmentSize, hi - cursor);
    auto seg = std::make_shared<FileSegment>(
        FsCacheKey{path, cursor, size}, remotePath);
    const auto inserted = keyMeta->segments.emplace(cursor, seg).second;
    // Caller holds the per-key lock and lookupRange already showed no segment
    // at `cursor`; a duplicate here is therefore a programming error.
    VELOX_CHECK(
        inserted,
        "fillHolesWithEmptyFileSegments: duplicate insert at offset={}",
        cursor);
    ++keyMeta->numSegments;
    out.push_back(std::move(seg));
    cursor += size;
  }
}

// Like FsCacheMetadata::lookupRange but operates directly on an already-locked
// KeyMetadata's segments map. This avoids a deadlock: lookupRange would try to
// re-acquire the KeyMutex that the caller already holds via LockedKey. The
// algorithm is identical: lower_bound + prev intersection check + forward scan.
std::vector<FileSegmentPtr> lookupRangeUnlocked(
    const KeyMetadata& keyMeta,
    uint64_t lo,
    uint64_t hi) {
  if (lo >= hi) {
    return {};
  }
  const auto& segs = keyMeta.segments;
  std::vector<FileSegmentPtr> result;
  auto it = segs.lower_bound(lo);
  if (it != segs.begin()) {
    auto prev = std::prev(it);
    const auto prevEnd = prev->second->key().offset + prev->second->key().size;
    if (prevEnd > lo) {
      it = prev;
    }
  }
  while (it != segs.end() && it->first < hi) {
    result.push_back(it->second);
    ++it;
  }
  return result;
}

} // namespace

std::vector<FileSegmentPtr> FsCache::fillHolesWithEmptyFileSegments(
    std::vector<FileSegmentPtr> found,
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,
    FsCacheMetadata& /* metadata */,
    const FsCacheConfig& cfg) {
  std::vector<FileSegmentPtr> result;
  if (found.empty()) {
    sliceHoleAndInsert(lo, hi, path, remotePath, lockedKey, cfg, result);
    return result;
  }

  // Leading hole.
  if (lo < found.front()->key().offset) {
    sliceHoleAndInsert(
        lo,
        found.front()->key().offset,
        path,
        remotePath,
        lockedKey,
        cfg,
        result);
  }

  for (size_t i{0}; i < found.size(); ++i) {
    result.push_back(found[i]);
    if (i + 1 < found.size()) {
      const uint64_t gapStart = found[i]->key().offset + found[i]->key().size;
      const uint64_t gapEnd = found[i + 1]->key().offset;
      if (gapStart < gapEnd) {
        sliceHoleAndInsert(
            gapStart, gapEnd, path, remotePath, lockedKey, cfg, result);
      }
    }
  }

  // Trailing hole.
  const uint64_t lastEnd =
      found.back()->key().offset + found.back()->key().size;
  if (lastEnd < hi) {
    sliceHoleAndInsert(
        lastEnd, hi, path, remotePath, lockedKey, cfg, result);
  }
  return result;
}

FileSegmentsHolderPtr FsCache::getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& /* settings */,
    ::facebook::velox::ReadFile& remote,
    IsPrefetch /* isPrefetch */) {
  // settings is unused in this commit (the in-process cache always uses
  // config_); Task 14 wires it through to fillHoles. Keeping the parameter in
  // the signature here avoids a second ABI break later. isPrefetch is also
  // unused until Task 14 wires it to the stats counters.
  //
  // bypass_cache_threshold (spec §8.3): an empty holder tells the caller to
  // read the full region directly from `remote`. Done before any lock so a
  // misconfigured huge scan does not even touch the bucket hierarchy.
  if (shouldBypass(size)) {
    return std::make_unique<FileSegmentsHolder>(std::vector<FileSegmentPtr>{});
  }
  const uint64_t fileSize = remote.size();
  VELOX_USER_CHECK_LE(
      offset,
      fileSize,
      "FsCache::getOrSet offset past EOF, fileSize={}",
      fileSize);
  if (offset == fileSize || size == 0) {
    return std::make_unique<FileSegmentsHolder>(std::vector<FileSegmentPtr>{});
  }
  const uint64_t clampedSize = std::min(size, fileSize - offset);
  // Outward-aligned range matches splitRange's outer boundary contract.
  const uint64_t alignedLo =
      (offset / config_.alignment) * config_.alignment;
  const uint64_t end = offset + clampedSize;
  const uint64_t alignedHi =
      ((end + config_.alignment - 1) / config_.alignment) * config_.alignment;
  // Re-clamp alignedHi to file size so we don't allocate post-EOF kEmpty
  // segments that would then refuse to download.
  const uint64_t clampedHi = std::min(alignedHi, fileSize);

  const PathKey pathKey = PathKey::fromPath(path);
  std::vector<FileSegmentPtr> slots;
  {
    auto lockedKey =
        metadata_->lockKeyMetadata(pathKey, KeyNotFoundPolicy::kCreateEmpty);
    // Use lookupRangeUnlocked to avoid deadlock: we already hold the KeyMutex
    // via lockedKey, and metadata_->lookupRange would try to re-acquire it.
    auto found = lookupRangeUnlocked(*lockedKey.get(), alignedLo, clampedHi);
    slots = fillHolesWithEmptyFileSegments(
        std::move(found),
        alignedLo,
        clampedHi,
        pathKey,
        path,
        lockedKey,
        *metadata_,
        config_);
  } // lockedKey released here before download

  return std::make_unique<FileSegmentsHolder>(std::move(slots));
}

void FsCache::evict(uint64_t bytesNeeded) {
  // Serialize concurrent eviction. Without this, two writers that both miss
  // the cache could each receive the same victim from a bucket's
  // selectVictims() (which does not detach entries from the LRU list --
  // detachment happens later via onRemove). The first thread's
  // metadata_->erase() then drops the only shared_ptr to the segment, and
  // the second thread dereferences a freed FileSegment. Holding
  // evictionMutex_ across the whole pass guarantees at most one in-flight
  // select+remove cycle at a time.
  std::lock_guard<std::mutex> evictGuard{evictionMutex_};
  const uint64_t current =
      counters_.bytesOnDisk.load(std::memory_order_relaxed);
  if (current + bytesNeeded <= config_.maxBytes) {
    return;
  }
  const uint64_t toFree = current + bytesNeeded - config_.maxBytes;

  // Round-robin candidate selection across all buckets. Each bucket is
  // visited AT MOST ONCE per evict() call (regardless of how many round-
  // robin attempts it takes to acquire its priorityMutex), which is what
  // makes the round-robin scheme correct: LruPolicy::selectVictims is a
  // non-mutating peek of the LRU tail --- detachment happens later in
  // onRemove --- so calling it twice on the same bucket would return the
  // same head-LRU FileSegment* twice and double-count it as a victim.
  // evictStart_ rotates the start offset so the same buckets are not
  // always queried first; distributes eviction load across the bucket
  // array.
  //
  // Phase A: up to N rounds of try_lock across all buckets. A bucket
  // whose priorityMutex is contended on round r may succeed on round
  // r+1; once it succeeds (or yields no victims even on success),
  // visited[idx] flips true and that bucket is excluded from later
  // attempts.
  //
  // Phase B: if Phase A still hasn't freed enough, fall back to a single
  // blocking pass over any bucket not yet visited. This guarantees
  // forward progress even when one bucket is permanently contended
  // (e.g., a long-running recordHit chain).
  //
  // Victims are only collected here -- onRemove is deferred until after
  // filesystem removal succeeds, so an LRU-evicted-but-still-on-disk
  // file cannot leak as an orphan.
  struct VictimEntry {
    FileSegment* victim;
    FsCacheMetadata::Bucket* bucket;
  };
  std::vector<VictimEntry> candidates;
  uint64_t accumulated{0};

  const auto& buckets = metadata_->buckets();
  const size_t numBuckets = buckets.size();
  const size_t start =
      evictStart_.fetch_add(1, std::memory_order_relaxed) % numBuckets;
  std::vector<bool> visited(numBuckets, false);

  // Collects victims from a bucket whose priorityMutex is already held by
  // the caller. Flips visited[idx] so the bucket is excluded from any
  // subsequent attempt in this evict() call -- this is what enforces the
  // at-most-once invariant described above.
  auto collectFrom = [&](size_t idx) {
    auto& bucket = *buckets[idx];
    visited[idx] = true;
    auto picked = bucket.priority->selectVictims(toFree - accumulated);
    for (auto* v : picked) {
      candidates.push_back({v, &bucket});
      accumulated += v->size();
      if (accumulated >= toFree) {
        break;
      }
    }
  };

  // Phase A: try_lock rounds. A bucket may need multiple rounds before
  // its priorityMutex is acquirable.
  for (size_t round = 0; round < numBuckets && accumulated < toFree;
       ++round) {
    bool madeProgress{false};
    for (size_t i = 0; i < numBuckets && accumulated < toFree; ++i) {
      const size_t idx = (start + i) % numBuckets;
      if (visited[idx]) {
        continue;
      }
      std::unique_lock<CachePriorityMutex> lk{
          buckets[idx]->priorityMutex, std::try_to_lock};
      if (!lk.owns_lock()) {
        continue;
      }
      madeProgress = true;
      collectFrom(idx);
    }
    if (!madeProgress) {
      // No bucket was acquirable this entire round. Re-trying with the
      // same lock state would loop forever; break out and let Phase B
      // block on the remaining buckets.
      break;
    }
  }

  // Phase B: blocking fallback for buckets Phase A could not acquire.
  // Worst case numBuckets * per-bucket lock acquisitions; spec §4.6
  // accepts this as evict is off the hot path.
  for (size_t i = 0; i < numBuckets && accumulated < toFree; ++i) {
    const size_t idx = (start + i) % numBuckets;
    if (visited[idx]) {
      continue;
    }
    CachePriorityGuard guard{buckets[idx]->priorityMutex};
    collectFrom(idx);
  }

  uint64_t freed{0};
  uint32_t evictedCount{0};
  for (auto& entry : candidates) {
    const auto key = entry.victim->key();
    std::error_code ec;
    std::filesystem::remove(entry.victim->localPath(config_.cacheRoot), ec);
    if (ec) {
      // Filesystem removal failed (disk error, race with manual cleanup,
      // permission change). Leave the victim in its bucket's policy and in
      // metadata_ so a later evict() can retry; better a temporary
      // over-capacity than losing the entry and leaking the file.
      LOG(WARNING) << "FsCache evict: failed to remove "
                   << entry.victim->remotePath() << " [" << key.offset << ".."
                   << key.offset + key.size << "): " << ec.message();
      continue;
    }
    {
      CachePriorityGuard guard{entry.bucket->priorityMutex};
      entry.bucket->priority->onRemove(entry.victim);
    }
    metadata_->erase(key);
    freed += key.size;
    ++evictedCount;
  }
  counters_.evictions.fetch_add(evictedCount, std::memory_order_relaxed);
  if (freed > 0) {
    // fetch_sub does not clamp to zero. Underflow cannot occur in practice:
    // every segment in a bucket's policy reached kDownloaded via
    // recordMiss, which credited segmentSize == key.size to bytesOnDisk;
    // freed sums key.size of segments actually removed from the policy, so
    // freed <= sum of outstanding credits == bytesOnDisk at all times. The
    // DCHECK surfaces accounting bugs in debug builds.
    const uint64_t prev =
        counters_.bytesOnDisk.fetch_sub(freed, std::memory_order_relaxed);
    VELOX_DCHECK_GE(
        prev, freed, "bytesOnDisk underflow: prev={} freed={}", prev, freed);
  }
}

size_t FsCache::ShardedAtomic::shardIndex() {
  // Per-thread shard id assigned on first call. Threads are spread across
  // kShards via modulo, so two threads can still collide once
  // thread-count > kShards; at that point cross-core invalidation falls
  // back to the (still cheap) intra-shard atomic RMW. The global counter
  // increments only once per thread, so its contention is negligible.
  static std::atomic<size_t> nextId{0};
  thread_local const size_t id =
      nextId.fetch_add(1, std::memory_order_relaxed);
  return id % kShards;
}

FsCacheStats FsCache::stats() const {
  FsCacheStats snapshot;
  snapshot.prefetchHits = counters_.prefetchHits.load();
  snapshot.prefetchMisses =
      counters_.prefetchMisses.load(std::memory_order_relaxed);
  snapshot.demandHits = counters_.demandHits.load();
  snapshot.demandMisses =
      counters_.demandMisses.load(std::memory_order_relaxed);
  snapshot.evictions = counters_.evictions.load(std::memory_order_relaxed);
  snapshot.bytesOnDisk = counters_.bytesOnDisk.load(std::memory_order_relaxed);
  return snapshot;
}

uint64_t FsCache::totalSize() const {
  return counters_.bytesOnDisk.load(std::memory_order_relaxed);
}

bool FsCache::shouldBypass(uint64_t size) const {
  return config_.bypassThresholdBytes > 0 &&
      size >= config_.bypassThresholdBytes;
}

void FsCache::recordHit(FileSegment* segment, IsPrefetch isPrefetch) {
  auto& counter = isPrefetch == IsPrefetch::kPrefetch
      ? counters_.prefetchHits
      : counters_.demandHits;
  counter.increment();
  // R3 (profile doc 2026-05-27): sequence-windowed LRU bump dedup. Every
  // recordHit() relaxed-increments segment->hits_; only every Nth hit
  // forwards to LruPolicy::onHit (which would otherwise take the bucket
  // priorityMutex). A hot segment hit thousands of times in a burst
  // collapses to ~1/N splice-to-MRU calls; LRU order stays correct within
  // an order of magnitude. kLruBumpEveryNHits must be a power of two so
  // the gate compiles to a single AND.
  constexpr uint64_t kLruBumpEveryNHits = 16;
  static_assert(
      (kLruBumpEveryNHits & (kLruBumpEveryNHits - 1)) == 0,
      "kLruBumpEveryNHits must be a power of two");
  const uint64_t prevHits =
      segment->hits_.fetch_add(1, std::memory_order_relaxed);
  if ((prevHits & (kLruBumpEveryNHits - 1)) != 0) {
    return;
  }
  auto& bucket = metadata_->bucketOf(segment->key().path);
  if (CachePriorityGuard guard{bucket.priorityMutex, std::try_to_lock};
      guard.owns_lock()) {
    bucket.priority->onHit(segment);
  }
  // Contention -> drop the bump; the next hit on this segment (or the next
  // burst) will land within the window and try again.
}

void FsCache::recordMiss(
    FileSegment* segment,
    uint64_t segmentSize,
    IsPrefetch isPrefetch) {
  {
    auto& bucket = metadata_->bucketOf(segment->key().path);
    CachePriorityGuard guard{bucket.priorityMutex};
    bucket.priority->onInsert(segment);
  }
  auto& counter = isPrefetch == IsPrefetch::kPrefetch
      ? counters_.prefetchMisses
      : counters_.demandMisses;
  counter.fetch_add(1, std::memory_order_relaxed);
  counters_.bytesOnDisk.fetch_add(segmentSize, std::memory_order_relaxed);
}

namespace {

std::filesystem::path sentinelPath(std::string_view cacheRoot) {
  return std::filesystem::path{cacheRoot} / kFsCacheVersionSentinelName;
}

bool sentinelMatches(std::string_view cacheRoot) {
  std::ifstream in{sentinelPath(cacheRoot)};
  if (!in.is_open()) {
    return false;
  }
  std::string content;
  std::getline(in, content);
  return content == kFsCacheCurrentVersion;
}

bool writeSentinel(std::string_view cacheRoot) {
  std::ofstream out{sentinelPath(cacheRoot), std::ios::trunc};
  out << kFsCacheCurrentVersion;
  out.close();
  return out.good();
}

// Recursively removes every direct child of cacheRoot (including
// subdirectories) but leaves cacheRoot itself in place. Returns true only
// when the directory could be opened AND every removal succeeded; callers
// must NOT write the version sentinel on a false return, otherwise a
// partially-cleared phase-1 layout would silently be promoted to
// "v2 verified" on the next restart.
bool blindClearCacheRoot(std::string_view cacheRoot) {
  std::error_code ec;
  std::filesystem::directory_iterator it{cacheRoot, ec};
  if (ec) {
    LOG(ERROR) << "FsCache blind-clear: failed to open " << cacheRoot << ": "
               << ec.message();
    return false;
  }
  bool ok = true;
  for (const auto& entry : it) {
    std::error_code rmEc;
    std::filesystem::remove_all(entry.path(), rmEc);
    if (rmEc) {
      LOG(WARNING) << "FsCache blind-clear: failed to remove "
                   << entry.path().string() << ": " << rmEc.message();
      ok = false;
    }
  }
  return ok;
}

struct ParsedName {
  uint64_t offset;
  uint64_t size;
};

// Parses "<16-hex>.<offset>.<size>" produced by FsCacheKey::fileName().
// Returns nullopt for anything that does not match, so loadFromDisk leaves
// unrelated files in cacheRoot untouched rather than deleting them.
std::optional<ParsedName> parseFileName(const std::string& name) {
  // Hash prefix is fmt::format("{:016x}", ...): exactly 16 lowercase hex
  // chars followed by a dot.
  if (name.size() < 18 || name[16] != '.') {
    return std::nullopt;
  }
  for (size_t i = 0; i < 16; ++i) {
    const char c = name[i];
    const bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!isHex) {
      return std::nullopt;
    }
  }
  const auto secondDot = name.find('.', 17);
  if (secondDot == std::string::npos || secondDot == 17 ||
      secondDot + 1 >= name.size()) {
    return std::nullopt;
  }
  // Strict digit-only check on both numeric tokens. std::stoull would
  // otherwise accept leading whitespace and '+'/'-' signs, neither of which
  // FsCacheKey::fileName() ever produces.
  auto isAllDigits = [](const std::string& s) {
    if (s.empty()) {
      return false;
    }
    for (const char c : s) {
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  };
  const std::string offsetStr = name.substr(17, secondDot - 17);
  const std::string sizeStr = name.substr(secondDot + 1);
  if (!isAllDigits(offsetStr) || !isAllDigits(sizeStr)) {
    return std::nullopt;
  }
  try {
    const uint64_t offset = std::stoull(offsetStr);
    const uint64_t size = std::stoull(sizeStr);
    return ParsedName{offset, size};
  } catch (const std::exception&) {
    // stoull only throws here on overflow; treat as unrecognised name.
    return std::nullopt;
  }
}

} // namespace

void FsCache::loadFromDisk() {
  if (!std::filesystem::exists(config_.cacheRoot)) {
    std::filesystem::create_directories(config_.cacheRoot);
    VELOX_CHECK(
        writeSentinel(config_.cacheRoot),
        "FsCache: failed to write version sentinel under cacheRoot: {}",
        config_.cacheRoot);
    return;
  }
  if (!sentinelMatches(config_.cacheRoot)) {
    // Either a fresh phase-2 install on top of phase-1 files, or an
    // unrelated foreign cacheRoot. Disk layout from phase-1 is not
    // re-keyable (filename only encodes hash, not original path), so spec
    // §10 R7 commits to blind-clear and re-fetch on demand. Refuse to
    // stamp the sentinel if the clear was incomplete --- otherwise the
    // next restart would skip this branch and operate on a half-cleared
    // tree of phase-1 files.
    VELOX_CHECK(
        blindClearCacheRoot(config_.cacheRoot),
        "FsCache: blind-clear of cacheRoot failed; refusing to advance "
        "version sentinel. cacheRoot: {}",
        config_.cacheRoot);
    VELOX_CHECK(
        writeSentinel(config_.cacheRoot),
        "FsCache: failed to write version sentinel after blind-clear "
        "under cacheRoot: {}",
        config_.cacheRoot);
    return;
  }

  // Collect victims during the scan, then delete after iteration finishes.
  // std::filesystem::recursive_directory_iterator does not guarantee safe
  // increment after the current entry is unlinked, and even less so when the
  // unlink empties the parent directory. Two-phase deletion side-steps both.
  std::vector<std::filesystem::path> toRemove;
  for (auto& entry :
       std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
    if (!entry.is_regular_file()) {
      continue;
    }
    if (entry.path().filename() == kFsCacheVersionSentinelName) {
      continue;
    }
    if (entry.path().extension() == ".tmp") {
      toRemove.push_back(entry.path());
      continue;
    }
    const auto parsed = parseFileName(entry.path().filename().string());
    if (!parsed.has_value()) {
      // Unrecognised file. Leave it untouched --- admin tooling owns
      // cleanup of foreign content under cacheRoot.
      continue;
    }
    if (entry.file_size() != parsed->size) {
      toRemove.push_back(entry.path());
    }
    // Survivor: the next getOrSet() will hash to the same filename,
    // the warm-restart short-circuit detects the file exists with the
    // expected size, and getOrSet credits recordMiss + bytesOnDisk so
    // the segment participates in eviction.
  }
  std::error_code ignore;
  for (const auto& path : toRemove) {
    std::filesystem::remove(path, ignore);
  }
  // Second pass: rmdir any subdirectory left empty after victim removal.
  // Repeated until no further dirs are removed so cleanup also reaches the
  // intermediate two-char buckets ("/aa/bb/") in any iteration order.
  bool removedAny = true;
  while (removedAny) {
    removedAny = false;
    std::vector<std::filesystem::path> emptyDirs;
    for (auto& entry :
         std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
      if (entry.is_directory() && std::filesystem::is_empty(entry.path())) {
        emptyDirs.push_back(entry.path());
      }
    }
    for (const auto& dir : emptyDirs) {
      if (std::filesystem::remove(dir, ignore)) {
        removedAny = true;
      }
    }
  }
}

} // namespace facebook::velox::cache::fs
