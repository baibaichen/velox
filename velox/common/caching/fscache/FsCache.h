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

  /// Returns segments covering [offset, offset + size) for the given path,
  /// downloading any missing segments synchronously from remote. Segment
  /// boundaries follow splitRange(offset, size, config); each returned
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

 private:
  // Looks up an existing segment or coordinates a fresh download. Single
  // writer per key via FileSegment::beginDownload(); concurrent callers wait
  // on FileSegment::cv_ for the writer's outcome.
  FileSegmentPtr lookupOrCreate(
      const FsCacheKey& key,
      ::facebook::velox::ReadFile& remote);

  // Evicts until bytesOnDisk + bytesNeeded <= maxBytes. Selects victims via
  // policy_, removes the on-disk file first, then drops the entry from
  // policy_/metadata_ to avoid orphan files on filesystem-remove failure.
  void evict(uint64_t bytesNeeded);

  // Records a cache hit: touches LRU and bumps stats_.hits.
  void recordHit(FileSegment* segment);

  // Records a fresh miss: inserts into LRU and bumps stats_.misses /
  // stats_.bytesOnDisk by segmentSize.
  void recordMiss(FileSegment* segment, uint64_t segmentSize);

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
  FsCacheStats stats_;
};

} // namespace facebook::velox::cache::fs
