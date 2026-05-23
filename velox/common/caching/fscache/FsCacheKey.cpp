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

#include <string_view>

#include <fmt/format.h>
#include <folly/hash/SpookyHashV2.h>

namespace facebook::velox::cache::fs {

namespace {
// SpookyHashV2 64-bit hash mixes path bytes with offset and size.
uint64_t combinedHash(std::string_view path, uint64_t offset, uint64_t size) {
  uint64_t hash1 = offset;
  uint64_t hash2 = size;
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &hash1, &hash2);
  return hash1 ^ hash2;
}
} // namespace

uint64_t FsCacheKey::hash() const noexcept {
  return combinedHash(path, offset, size);
}

std::string FsCacheKey::fileName() const {
  return fmt::format("{:016x}.{}.{}", hash(), offset, size);
}

} // namespace facebook::velox::cache::fs
