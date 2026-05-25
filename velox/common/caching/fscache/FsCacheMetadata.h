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
#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/caching/fscache/KeyMetadata.h"

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

namespace facebook::velox::cache::fs {

using FileSegmentPtr = std::shared_ptr<FileSegment>;

/// Two-level index: numBuckets buckets each carrying its own
/// CacheMetadataMutex and a PathKey -> KeyMetadataPtr map. Each KeyMetadata
/// holds the file's per-offset FileSegments under its own KeyMutex. Phase 2
/// keeps the public surface (insert/lookup/erase/snapshot) of phase 1 but
/// replaces the single global metadata mutex with per-bucket guards and the
/// per-bucket flat map with the KeyMetadata indirection so that segments of
/// the same path co-locate and serialize on their own per-key lock.
class FsCacheMetadata {
 public:
  /// Constructs with numBuckets buckets. numBuckets must be a power of two so
  /// the hash->bucket modulo folds to a single bitwise AND in bucketIndex().
  explicit FsCacheMetadata(size_t numBuckets);

  /// Inserts the segment. Returns false if a segment with the same
  /// (path, offset) is already present at that offset under the existing
  /// KeyMetadata; in that case the caller's segment is dropped and the
  /// existing entry is preserved.
  bool insert(FileSegmentPtr segment);

  /// Returns the segment for key or nullptr if not present.
  FileSegmentPtr lookup(const FsCacheKey& key) const;

  /// Removes the segment for key. Returns true if it existed. When the last
  /// segment of a KeyMetadata is removed, the KeyMetadata entry is dropped
  /// from the bucket map.
  bool erase(const FsCacheKey& key);

  /// Returns all segments currently in the index. Order is unspecified.
  /// Used by recovery and tests; not on the hot path.
  std::vector<FileSegmentPtr> snapshot() const;

  /// Returns the number of buckets configured at construction time.
  size_t numBuckets() const {
    return buckets_.size();
  }

 private:
  // One bucket of the two-level index. Owns the bucket-level guard and the
  // PathKey -> KeyMetadataPtr map; the map and the KeyMetadata segments are
  // only legal to read or write under their respective locks.
  struct Bucket {
    mutable CacheMetadataMutex guard;
    std::unordered_map<PathKey, KeyMetadataPtr> keys;
  };

  // Computes the bucket index for a PathKey. numBuckets is power-of-two
  // (CHECKed in ctor) so the modulo folds to a bitmask: hash & (numBuckets -
  // 1). All segments of the same file share the same PathKey and therefore
  // land in the same bucket.
  size_t bucketIndex(const PathKey& path) const {
    return std::hash<PathKey>{}(path) & bucketMask_;
  }

  const size_t bucketMask_;
  // Bucket lives behind unique_ptr because it carries a non-movable mutex.
  std::vector<std::unique_ptr<Bucket>> buckets_;
};

} // namespace facebook::velox::cache::fs
