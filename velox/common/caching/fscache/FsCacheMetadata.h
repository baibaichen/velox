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
#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/caching/fscache/KeyMetadata.h"
#include "velox/common/caching/fscache/LruPolicy.h"

#include <cstddef>
#include <functional>
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
  /// One bucket of the two-level index. Owns the bucket-level metadata guard,
  /// the PathKey -> KeyMetadataPtr map, the per-bucket priority mutex, and
  /// the per-bucket EvictionPolicy instance. The map and KeyMetadata segments
  /// are only legal to read or write under their respective locks.
  struct Bucket {
    mutable CacheMetadataMutex guard;
    std::unordered_map<PathKey, KeyMetadataPtr> keys;
    mutable CachePriorityMutex priorityMutex;
    std::unique_ptr<EvictionPolicy> priority;
  };

  /// Factory invoked once per bucket at construction to build that bucket's
  /// EvictionPolicy. Used by tests and by future SLRU plumbing to inject
  /// non-default policies.
  using PolicyFactory = std::function<std::unique_ptr<EvictionPolicy>()>;

  /// Constructs with numBuckets buckets, each holding a freshly-built
  /// EvictionPolicy from policyFactory. numBuckets must be a power of two so
  /// the hash->bucket modulo folds to a single bitwise AND in bucketIndex().
  FsCacheMetadata(size_t numBuckets, PolicyFactory policyFactory);

  /// Convenience overload that defaults policyFactory to LruPolicy.
  explicit FsCacheMetadata(size_t numBuckets)
      : FsCacheMetadata(
            numBuckets,
            [] { return std::make_unique<LruPolicy>(); }) {}

  /// Inserts the segment. Returns false if a segment with the same
  /// (path, offset) is already present at that offset under the existing
  /// KeyMetadata; in that case the caller's segment is dropped and the
  /// existing entry is preserved.
  bool insert(FileSegmentPtr segment);

  /// Returns the segment for key, or nullptr if not present. Best-effort
  /// under concurrency: the bucket guard is released before the per-key
  /// mutex is taken, so a concurrent erase racing against this lookup may
  /// cause it to return nullptr even if a fresh insert at the same path
  /// (under a new KeyMetadata) succeeded. The caller treats nullptr as a
  /// cache miss and proceeds to download, which is correct.
  FileSegmentPtr lookup(const FsCacheKey& key) const;

  /// Returns a LockedKey for `path`. Behaviour on absence depends on policy
  /// (see KeyNotFoundPolicy). When the policy is kCreateEmpty, an empty
  /// KeyMetadata is inserted under the bucket guard, then the per-key lock is
  /// acquired before return. The returned LockedKey owns the per-key mutex;
  /// the bucket guard is released before return so concurrent lookups under
  /// other paths in the same bucket are not blocked while the caller holds
  /// the LockedKey.
  LockedKey lockKeyMetadata(const PathKey& path, KeyNotFoundPolicy policy);

  /// Returns the segments under `path` whose ranges intersect [lo, hi),
  /// ordered by ascending offset. Empty result on missing key or no
  /// intersection. Uses CH-style lower_bound + prev: the segment just
  /// before lower_bound(lo) may straddle `lo`, so it is checked and
  /// included if its end > lo. The per-key lock is acquired via
  /// lockKeyMetadata(kReturnNull) and released on return; the returned
  /// shared_ptrs keep the segments alive independent of subsequent erases.
  std::vector<FileSegmentPtr> lookupRange(
      const PathKey& path,
      uint64_t lo,
      uint64_t hi) const;

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

  /// Returns the Bucket housing the given path. Used by FsCache to access
  /// per-bucket priority. The returned reference is stable for the lifetime
  /// of this FsCacheMetadata.
  Bucket& bucketOf(const PathKey& path) {
    return *buckets_[bucketIndex(path)];
  }
  const Bucket& bucketOf(const PathKey& path) const {
    return *buckets_[bucketIndex(path)];
  }

  /// Returns all buckets in order. Used by evict() to round-robin and by
  /// snapshot(). Reference is stable for the FsCacheMetadata's lifetime.
  const std::vector<std::unique_ptr<Bucket>>& buckets() const {
    return buckets_;
  }

 private:
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
