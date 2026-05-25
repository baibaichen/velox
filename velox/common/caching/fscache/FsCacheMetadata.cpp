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

#include "velox/common/caching/fscache/FsCacheMetadata.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::cache::fs {

FsCacheMetadata::FsCacheMetadata(size_t numBuckets)
    : bucketMask_{numBuckets - 1} {
  VELOX_CHECK_GT(numBuckets, 0, "FsCacheMetadata requires numBuckets > 0");
  VELOX_CHECK_EQ(
      numBuckets & (numBuckets - 1),
      0,
      "FsCacheMetadata numBuckets must be a power of two, got {}",
      numBuckets);
  buckets_.reserve(numBuckets);
  for (size_t i{0}; i < numBuckets; ++i) {
    buckets_.push_back(std::make_unique<Bucket>());
  }
}

bool FsCacheMetadata::insert(FileSegmentPtr segment) {
  VELOX_CHECK_NOT_NULL(segment);
  const auto key = segment->key();
  auto& bucket = *buckets_[bucketIndex(key.path)];
  // Hold the bucket guard while acquiring the per-key mutex. Rank 2 -> rank 3
  // is the legal forward direction in the lock hierarchy. This eliminates the
  // window where a concurrent erase could drop the bucket's mapping for this
  // path while we hold a stale KeyMetadataPtr, which would otherwise let us
  // emplace the segment into an orphan KeyMetadata.
  CacheMetadataGuard bucketGuard{bucket.guard};
  KeyMetadataPtr keyMeta;
  auto it = bucket.keys.find(key.path);
  if (it == bucket.keys.end()) {
    keyMeta = std::make_shared<KeyMetadata>();
    bucket.keys.emplace(key.path, keyMeta);
  } else {
    keyMeta = it->second;
  }
  auto locked = keyMeta->lock();
  const auto inserted =
      locked->segments.emplace(key.offset, std::move(segment)).second;
  if (!inserted) {
    return false;
  }
  ++locked->numSegments;
  return true;
}

FileSegmentPtr FsCacheMetadata::lookup(const FsCacheKey& key) const {
  auto& bucket = *buckets_[bucketIndex(key.path)];
  KeyMetadataPtr keyMeta;
  {
    CacheMetadataGuard bucketGuard{bucket.guard};
    auto it = bucket.keys.find(key.path);
    if (it == bucket.keys.end()) {
      return nullptr;
    }
    keyMeta = it->second;
  }
  auto locked = keyMeta->lock();
  auto segIt = locked->segments.find(key.offset);
  if (segIt == locked->segments.end()) {
    return nullptr;
  }
  return segIt->second;
}

bool FsCacheMetadata::erase(const FsCacheKey& key) {
  auto& bucket = *buckets_[bucketIndex(key.path)];
  // Hold the bucket guard while acquiring the per-key mutex. Holding both
  // makes the empty-drop atomic with the segment erase, so no concurrent
  // insert can have observed (path -> keyMeta) and be waiting outside the
  // bucket on a KeyMetadata that we are about to orphan.
  CacheMetadataGuard bucketGuard{bucket.guard};
  auto it = bucket.keys.find(key.path);
  if (it == bucket.keys.end()) {
    return false;
  }
  auto keyMeta = it->second;
  auto locked = keyMeta->lock();
  const auto erased = locked->segments.erase(key.offset);
  if (erased == 0) {
    return false;
  }
  --locked->numSegments;
  if (locked->segments.empty()) {
    bucket.keys.erase(it);
  }
  return true;
}

std::vector<FileSegmentPtr> FsCacheMetadata::snapshot() const {
  std::vector<FileSegmentPtr> result;
  for (const auto& bucketPtr : buckets_) {
    auto& bucket = *bucketPtr;
    std::vector<KeyMetadataPtr> bucketKeys;
    {
      CacheMetadataGuard bucketGuard{bucket.guard};
      bucketKeys.reserve(bucket.keys.size());
      for (const auto& [_, keyMeta] : bucket.keys) {
        bucketKeys.push_back(keyMeta);
      }
    }
    for (const auto& keyMeta : bucketKeys) {
      auto locked = keyMeta->lock();
      for (const auto& [_, segment] : locked->segments) {
        result.push_back(segment);
      }
    }
  }
  return result;
}

} // namespace facebook::velox::cache::fs
