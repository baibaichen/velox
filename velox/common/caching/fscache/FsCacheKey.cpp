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

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <cstring>

#include <fmt/format.h>
#include <folly/hash/SpookyHashV2.h>

namespace facebook::velox::cache::fs {

PathKey PathKey::fromPath(std::string_view path) {
  uint64_t hash1{0};
  uint64_t hash2{0};
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &hash1, &hash2);
  const uint64_t combined = hash1 ^ hash2;
  PathKey key{};
  std::memcpy(key.bytes.data(), &combined, sizeof(combined));
  return key;
}

std::string PathKey::hex() const {
  uint64_t combined{0};
  std::memcpy(&combined, bytes.data(), sizeof(combined));
  return fmt::format("{:016x}", combined);
}

uint64_t FsCacheKey::hash() const noexcept {
  uint64_t first8{0};
  std::memcpy(&first8, path.bytes.data(), sizeof(first8));
  return first8;
}

std::string FsCacheKey::fileName() const {
  uint64_t combined{0};
  std::memcpy(&combined, path.bytes.data(), sizeof(combined));
  return fmt::format("{:016x}.{}.{}", combined, offset, size);
}

} // namespace facebook::velox::cache::fs
