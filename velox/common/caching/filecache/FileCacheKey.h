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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include <fmt/format.h>

namespace facebook::velox::ch {

/// 128-bit logical cache key. Ported from ClickHouse FileCacheKey.
/// CH uses its own UInt128 (wide::integer); the Velox port uses the builtin
/// __uint128_t per the spec's data-structure substitution rules.
using UInt128 = __uint128_t;

struct FileCacheKey {
  using KeyHash = UInt128;
  KeyHash key;

  std::string toString() const;

  FileCacheKey() = default;

  static FileCacheKey random();
  static FileCacheKey fromPath(const std::string& path);
  static FileCacheKey fromKey(const UInt128& key);
  static FileCacheKey fromKeyString(const std::string& keyStr);

  bool operator==(const FileCacheKey& other) const {
    return key == other.key;
  }
  bool operator<(const FileCacheKey& other) const {
    return key < other.key;
  }

 private:
  explicit FileCacheKey(const UInt128& key_);
};

using FileCacheKeyAndOffset = std::pair<FileCacheKey, size_t>;

struct FileCacheKeyAndOffsetHash {
  std::size_t operator()(const FileCacheKeyAndOffset& key) const;
};

} // namespace facebook::velox::ch

namespace std {
template <>
struct hash<facebook::velox::ch::FileCacheKey> {
  std::size_t operator()(const facebook::velox::ch::FileCacheKey& k) const;
};
} // namespace std

template <>
struct fmt::formatter<facebook::velox::ch::FileCacheKey>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const facebook::velox::ch::FileCacheKey& key, FormatCtx& ctx)
      const {
    return fmt::formatter<std::string>::format(key.toString(), ctx);
  }
};
