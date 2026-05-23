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
    : bucketMask_{numBuckets - 1}, buckets_(numBuckets) {
  VELOX_CHECK_GT(numBuckets, 0, "FsCacheMetadata requires numBuckets > 0");
  VELOX_CHECK_EQ(
      numBuckets & (numBuckets - 1),
      0,
      "FsCacheMetadata numBuckets must be a power of two, got {}",
      numBuckets);
}

bool FsCacheMetadata::insert(FileSegmentPtr segment) {
  CacheMetadataGuard guard{mutex_};
  const auto key = segment->key();
  auto& bucket = buckets_[bucketIndex(key)];
  return bucket.emplace(key, std::move(segment)).second;
}

FileSegmentPtr FsCacheMetadata::lookup(const FsCacheKey& key) const {
  CacheMetadataGuard guard{mutex_};
  const auto& bucket = buckets_[bucketIndex(key)];
  auto it = bucket.find(key);
  if (it == bucket.end()) {
    return nullptr;
  }
  return it->second;
}

bool FsCacheMetadata::erase(const FsCacheKey& key) {
  CacheMetadataGuard guard{mutex_};
  auto& bucket = buckets_[bucketIndex(key)];
  return bucket.erase(key) > 0;
}

std::vector<FileSegmentPtr> FsCacheMetadata::snapshot() const {
  CacheMetadataGuard guard{mutex_};
  std::vector<FileSegmentPtr> result;
  for (const auto& bucket : buckets_) {
    for (const auto& [_, segment] : bucket) {
      result.push_back(segment);
    }
  }
  return result;
}

} // namespace facebook::velox::cache::fs
