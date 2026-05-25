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

/// Path-only hash key. Holds the folded 64-bit SpookyHashV2 of the remote
/// path as raw bytes; kept as a POD so the hot path (FsCache::getOrSet ->
/// PathKey::fromPath) is two memcpys with no formatting. Hex stringification
/// is moved into hex() / FsCacheKey::fileName(), which only run on the slow
/// paths (logging, disk I/O). Used to bucket and index per-key metadata so
/// all segments of the same file land in the same bucket and KeyMetadata
/// entry.
struct PathKey {
  /// Raw bytes of the folded SpookyHashV2 64-bit hash. Not hex-encoded.
  std::array<uint8_t, 8> bytes{};

  /// Computes PathKey from a remote file path.
  static PathKey fromPath(std::string_view path);

  /// Returns the 8 hash bytes hex-encoded as 16 lowercase chars. Allocates;
  /// intended for fileName(), logging, and tests, not for the hot path.
  std::string hex() const;

  bool operator==(const PathKey& other) const noexcept {
    return std::memcmp(bytes.data(), other.bytes.data(), bytes.size()) == 0;
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
    std::memcpy(&first8, key.bytes.data(), sizeof(first8));
    return static_cast<size_t>(first8);
  }
};
} // namespace std
