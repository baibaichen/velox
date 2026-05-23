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

#include <cstdint>
#include <string>

namespace facebook::velox::cache::fs {

/// Identifies a single cache segment by remote file path, byte offset, and
/// segment size. Phase 1 carries no file version — the design assumes remote
/// files are immutable.
struct FsCacheKey {
  std::string path;
  uint64_t offset{0};
  uint64_t size{0};

  bool operator==(const FsCacheKey& other) const noexcept {
    return offset == other.offset && size == other.size && path == other.path;
  }

  bool operator!=(const FsCacheKey& other) const noexcept {
    return !(*this == other);
  }

  /// Returns a 64-bit hash derived from all three fields. Stable across
  /// processes on the same architecture (folly SpookyHashV2 is fixed seed).
  uint64_t hash() const noexcept;

  /// Returns the on-disk file name "<16-hex-hash>.<offset>.<size>". The hash
  /// is hex (only [0-9a-f]) so the "." separator is unambiguous and the
  /// numeric fields cannot collide with the hash. Used both for file
  /// placement and for parsing during recovery.
  std::string fileName() const;
};

/// Hash functor for std unordered containers.
struct FsCacheKeyHash {
  size_t operator()(const FsCacheKey& key) const noexcept {
    return static_cast<size_t>(key.hash());
  }
};

} // namespace facebook::velox::cache::fs
