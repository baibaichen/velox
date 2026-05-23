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

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

namespace facebook::velox::cache::fs {

using FileSegmentPtr = std::shared_ptr<FileSegment>;

/// Two-level index: numBuckets buckets each holding an unordered_map keyed by
/// FsCacheKey. Phase 1 protects the whole structure with a single
/// CacheMetadataMutex; phase 2 subdivides without changing the public surface.
class FsCacheMetadata {
 public:
  /// Constructs with numBuckets buckets. numBuckets must be a power of two so
  /// the hash->bucket modulo folds to a single bitwise AND in bucketIndex().
  explicit FsCacheMetadata(size_t numBuckets);

  /// Inserts the segment. Returns false if a segment with the same key
  /// already exists; in that case the caller's segment is dropped and the
  /// existing entry is preserved.
  bool insert(FileSegmentPtr segment);

  /// Returns the segment for key or nullptr if not present.
  FileSegmentPtr lookup(const FsCacheKey& key) const;

  /// Removes the segment for key. Returns true if it existed.
  bool erase(const FsCacheKey& key);

  /// Returns all segments currently in the index. Order is unspecified.
  /// Used by recovery and tests; not on the hot path.
  std::vector<FileSegmentPtr> snapshot() const;

 private:
  size_t bucketIndex(const FsCacheKey& key) const {
    // numBuckets is power-of-two (CHECKed in ctor) so the modulo folds to a
    // bitmask: hash & (numBuckets - 1). Cheaper than `%` and equivalent.
    return static_cast<size_t>(key.hash() & bucketMask_);
  }

  mutable CacheMetadataMutex mutex_;
  const size_t bucketMask_;
  std::vector<std::unordered_map<FsCacheKey, FileSegmentPtr, FsCacheKeyHash>>
      buckets_;
};

} // namespace facebook::velox::cache::fs
