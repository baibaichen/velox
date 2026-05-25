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

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

namespace facebook::velox::cache::fs {

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;

class KeyMetadata;

/// RAII handle that owns a KeyGuard on a specific KeyMetadata. While alive,
/// the holder may read and write the KeyMetadata's segments / numSegments.
/// Non-copyable; movable. An empty (default-constructed) LockedKey holds no
/// mutex and dereferences to nullptr — used as a sentinel when transferring
/// ownership.
class LockedKey {
 public:
  LockedKey() = default;

  /// Internal: use KeyMetadata::lock() instead of calling this directly.
  /// Locks the given mutex on construction; the pairing of meta and mutex
  /// is assumed to be correct (mutex must belong to meta).
  LockedKey(KeyMetadata* meta, KeyMutex& mutex);

  LockedKey(LockedKey&& other) noexcept;
  LockedKey& operator=(LockedKey&& other) noexcept;
  ~LockedKey();

  LockedKey(const LockedKey&) = delete;
  LockedKey& operator=(const LockedKey&) = delete;

  /// Returns the KeyMetadata pointer or nullptr if this LockedKey is empty.
  KeyMetadata* get() const noexcept {
    return meta_;
  }

  /// Dereferences to the held KeyMetadata; nullptr if empty.
  KeyMetadata* operator->() const noexcept {
    return meta_;
  }

 private:
  KeyMetadata* meta_{nullptr};
  KeyMutex* mutex_{nullptr};
};

/// Per-PathKey container: all FileSegments of one remote file, indexed by
/// offset. Phase-2 introduces this indirection so the per-bucket
/// CacheMetadataGuard can be released as soon as we obtain a KeyMetadataPtr,
/// and finer-grained operations on a single file's segments serialize on the
/// per-key KeyGuard instead of contending on the bucket.
class KeyMetadata {
 public:
  KeyMetadata() = default;

  /// Acquires the per-key mutex and returns a LockedKey RAII for accessing
  /// segments / numSegments. Blocks if another thread holds the mutex.
  LockedKey lock();

  /// Segments belonging to this PathKey, keyed by FsCacheKey::offset. Size is
  /// stored on each FileSegment; KeyMetadata does not duplicate it. Access is
  /// only legal while a LockedKey for this object is alive.
  std::map<uint64_t, FileSegmentPtr> segments;

  /// Cached count of segments (== segments.size()). Maintained by callers
  /// under the LockedKey. Useful to read without iterating segments.
  size_t numSegments{0};

 private:
  mutable KeyMutex mutex_;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

} // namespace facebook::velox::cache::fs
