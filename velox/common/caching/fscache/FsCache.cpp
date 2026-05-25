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
#include "velox/common/file/File.h"

#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <mutex>
#include <optional>

namespace facebook::velox::cache::fs {

FsCache::FsCache(FsCacheConfig config)
    : config_{std::move(config)},
      metadata_{std::make_unique<FsCacheMetadata>(config_.numBuckets)},
      policy_{std::make_unique<LruPolicy>()} {
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

std::vector<FileSegmentPtr> FsCache::getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    ::facebook::velox::ReadFile& remote) {
  // Clamp the requested range to the remote file's actual size. splitRange
  // aligns the outer end outward to config_.alignment, which would otherwise
  // overshoot EOF for files smaller than alignment (or for reads near the
  // tail of any file) and cause LocalReadFile::preadInternal to reject the
  // short read inside FileSegment::download. See spec
  // 2026-05-23-fscache-vs-cbi-tpcds §7 OQ #2.
  const uint64_t fileSize = remote.size();
  VELOX_USER_CHECK_LE(
      offset,
      fileSize,
      "FsCache::getOrSet offset past EOF, fileSize={}",
      fileSize);
  if (offset == fileSize || size == 0) {
    return {};
  }
  const uint64_t clampedSize = std::min(size, fileSize - offset);
  const auto ranges = splitRange(offset, clampedSize, config_);
  std::vector<FileSegmentPtr> result;
  result.reserve(ranges.size());
  const PathKey pathKey = PathKey::fromPath(path);
  for (const auto& [segOffset, segSize] : ranges) {
    // splitRange rounds alignedEnd outward to config_.alignment, so the LAST
    // emitted segment's (segOffset + segSize) can exceed fileSize even though
    // the outer clampedSize already fits. Re-clamp per segment so key.size
    // (which names the on-disk file and is what FileSegment::download reads
    // from remote) matches the true byte count; a mismatched key.size would
    // otherwise re-trigger the EOF overshoot inside download. Underflow is
    // impossible: every cursor emitted by splitRange is alignment-aligned and
    // < alignedEnd, and the only segment whose end can exceed fileSize is the
    // last one whose start is still < fileSize (alignedStart <= offset <
    // fileSize, every subsequent cursor is offset + k*alignment <= alignedEnd
    // - alignment < fileSize until the loop exits).
    const uint64_t effectiveSize = std::min(segSize, fileSize - segOffset);
    FsCacheKey key{pathKey, segOffset, effectiveSize};
    result.push_back(lookupOrCreate(key, path, remote));
  }
  return result;
}

FileSegmentPtr FsCache::lookupOrCreate(
    const FsCacheKey& key,
    const std::string& path,
    ::facebook::velox::ReadFile& remote) {
  // 1. Fast path: existing kDownloaded segment.
  if (auto existing = metadata_->lookup(key); existing != nullptr &&
      existing->state() == FileSegment::State::kDownloaded) {
    recordHit(existing.get());
    return existing;
  }

  // 2. Insert (or pick up existing) segment under the metadata lock. If the
  //    insert races with a concurrent inserter, lookup() returns the winning
  //    entry so writer/waiter coordination on the SAME FileSegment instance
  //    is preserved.
  auto segment = std::make_shared<FileSegment>(key, path);
  if (!metadata_->insert(segment)) {
    segment = metadata_->lookup(key);
    VELOX_CHECK_NOT_NULL(segment);
  }

  // 3. Coordinate download. Only the thread that wins beginDownload() does
  //    the actual fetch; concurrent waiters block on cv_ until the writer
  //    either completes (kDownloaded) or fails (kEmpty). On failure each
  //    waiter throws so the caller can retry or surface the error; the
  //    segment metadata entry stays so a subsequent caller can race for
  //    beginDownload() again.
  std::unique_lock<FileSegmentMutex> lock{segment->mutex_};
  if (segment->state() == FileSegment::State::kDownloaded) {
    // Another thread completed between fast-path lookup and metadata insert.
    // Treat as a hit; the slow-path fall-through is not a miss.
    lock.unlock();
    recordHit(segment.get());
    return segment;
  }
  if (segment->beginDownload()) {
    // 3a. Writer path. Release the FileSegment mutex before evict() and
    // download() so waiters can register on cv_ while we work.
    lock.unlock();
    // Reserve capacity by evicting kDownloaded victims; never touches
    // kDownloading segments because LruPolicy only contains segments that
    // reached kDownloaded (onInsert runs after a successful download below).
    evict(key.size);
    try {
      segment->download(remote, config_.cacheRoot);
    } catch (...) {
      // download() already reset state_ to kEmpty and removed the .tmp.
      // Notify waiters so they can throw rather than wait forever.
      std::lock_guard<FileSegmentMutex> resetLock{segment->mutex_};
      segment->cv_.notify_all();
      throw;
    }
    recordMiss(segment.get(), key.size);
    std::lock_guard<FileSegmentMutex> notifyLock{segment->mutex_};
    segment->cv_.notify_all();
    return segment;
  }
  // 3b. Waiter path: another thread is downloading; wait for completion or
  //     failure. cv_ is notified on both outcomes (see writer path above).
  segment->cv_.wait(lock, [&] {
    return segment->state() != FileSegment::State::kDownloading;
  });
  const auto finalState = segment->state();
  lock.unlock();
  if (finalState != FileSegment::State::kDownloaded) {
    // Writer threw. Surface as a user-level error; caller may retry by
    // calling getOrSet again, at which point a fresh race for
    // beginDownload() happens. Do not VELOX_CHECK here: that would turn
    // another thread's IO failure into a CHECK-failure crash on the waiter.
    VELOX_USER_FAIL(
        "FsCache concurrent download failed for path={} offset={} size={}",
        segment->remotePath(),
        key.offset,
        key.size);
  }
  // Successful concurrent download counts as a hit for this thread.
  recordHit(segment.get());
  return segment;
}

void FsCache::evict(uint64_t bytesNeeded) {
  // Serialize concurrent eviction. Without this, two writers that both miss
  // the cache could each receive the same victim from selectVictims() (which
  // does not detach entries from the LRU list -- detachment happens later via
  // onRemove). The first thread's metadata_->erase() then drops the only
  // shared_ptr to the segment, and the second thread dereferences a freed
  // FileSegment. Holding evictionMutex_ across the whole pass guarantees at
  // most one in-flight selectVictims+remove cycle at a time.
  std::lock_guard<std::mutex> evictGuard{evictionMutex_};
  uint64_t current;
  {
    CacheStateGuard guard{stateMutex_};
    current = stats_.bytesOnDisk;
  }
  if (current + bytesNeeded <= config_.maxBytes) {
    return;
  }
  const uint64_t toFree = current + bytesNeeded - config_.maxBytes;

  // selectVictims is read-only: pick candidates under priorityMutex_ but do
  // NOT yet call policy_->onRemove. Filesystem removal must succeed first;
  // otherwise an LRU-evicted-but-still-on-disk file becomes an orphan that
  // no future eviction pass can find (gone from policy but still consuming
  // bytes), and metadata becomes inconsistent with the on-disk state.
  std::vector<FileSegment*> candidates;
  {
    CachePriorityGuard guard{priorityMutex_};
    candidates = policy_->selectVictims(toFree);
  }

  uint64_t freed = 0;
  uint32_t evictedCount = 0;
  for (auto* victim : candidates) {
    const auto key = victim->key();
    std::error_code ec;
    std::filesystem::remove(victim->localPath(config_.cacheRoot), ec);
    if (ec) {
      // Filesystem removal failed (disk error, race with manual cleanup,
      // permission change). Leave the victim in policy_ and metadata_ so a
      // later evict() can retry; better a temporary over-capacity than
      // losing the entry and leaking the file.
      LOG(WARNING) << "FsCache evict: failed to remove " << victim->remotePath()
                   << " [" << key.offset << ".." << key.offset + key.size
                   << "): " << ec.message();
      continue;
    }
    {
      CachePriorityGuard guard{priorityMutex_};
      policy_->onRemove(victim);
    }
    metadata_->erase(key);
    freed += key.size;
    ++evictedCount;
  }
  {
    CacheStateGuard guard{stateMutex_};
    stats_.evictions += evictedCount;
    stats_.bytesOnDisk -= std::min(stats_.bytesOnDisk, freed);
  }
}

FsCacheStats FsCache::stats() const {
  CacheStateGuard guard{stateMutex_};
  return stats_;
}

void FsCache::recordHit(FileSegment* segment) {
  {
    CachePriorityGuard guard{priorityMutex_};
    policy_->onHit(segment);
  }
  CacheStateGuard guard{stateMutex_};
  ++stats_.hits;
}

void FsCache::recordMiss(FileSegment* segment, uint64_t segmentSize) {
  {
    CachePriorityGuard guard{priorityMutex_};
    policy_->onInsert(segment);
  }
  CacheStateGuard guard{stateMutex_};
  ++stats_.misses;
  stats_.bytesOnDisk += segmentSize;
}

namespace {

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
    // FileSegment::download() short-circuits on the existence + size check,
    // and lookupOrCreate() credits onInsert + bytesOnDisk so the segment
    // participates in eviction.
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
