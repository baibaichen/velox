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

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>

namespace facebook::velox::cache::fs {

/// Path-only hash key. Holds the 16 lowercase hex chars of SpookyHashV2 over
/// the remote file path. Used to bucket and index per-key metadata so that all
/// segments of the same file land in the same bucket and KeyMetadata entry.
struct PathKey {
  /// Lowercase hex characters; no NUL terminator. Always 16 bytes.
  std::array<char, 16> chars;

  /// Computes PathKey from a remote file path.
  static PathKey fromPath(std::string_view path);

  /// Returns the 16 hex chars as a string_view (non-owning, valid as long as
  /// the PathKey is alive).
  std::string_view hex() const noexcept {
    return std::string_view(chars.data(), chars.size());
  }

  bool operator==(const PathKey& other) const noexcept {
    return std::memcmp(chars.data(), other.chars.data(), chars.size()) == 0;
  }

  bool operator!=(const PathKey& other) const noexcept {
    return !(*this == other);
  }
};

/// Identifies a single cache segment by PathKey (path-only hash) + offset +
/// size. Phase-2 splits the phase-1 (path, offset, size) composite key so that
/// all segments of the same file share the same hash bucket, enabling
/// per-key metadata indirection.
struct FsCacheKey {
  PathKey path;
  uint64_t offset{0};
  uint64_t size{0};

  bool operator==(const FsCacheKey& other) const noexcept {
    return offset == other.offset && size == other.size && path == other.path;
  }

  bool operator!=(const FsCacheKey& other) const noexcept {
    return !(*this == other);
  }

  /// Returns a 64-bit hash derived from PathKey only (NOT offset/size).
  /// Two FsCacheKeys with the same path but different offsets hash equally.
  uint64_t hash() const noexcept;

  /// Returns "<pathkey-hex>.<offset>.<size>". Schema matches phase-1 but the
  /// hash value differs — phase-1 cache files cannot be re-keyed by phase-2.
  std::string fileName() const;
};

/// Hash functor for std unordered containers.
struct FsCacheKeyHash {
  size_t operator()(const FsCacheKey& key) const noexcept {
    return static_cast<size_t>(key.hash());
  }
};

} // namespace facebook::velox::cache::fs

namespace std {
template <>
struct hash<::facebook::velox::cache::fs::PathKey> {
  size_t operator()(
      const ::facebook::velox::cache::fs::PathKey& key) const noexcept {
    uint64_t first8{0};
    std::memcpy(&first8, key.chars.data(), sizeof(first8));
    return static_cast<size_t>(first8);
  }
};
} // namespace std
