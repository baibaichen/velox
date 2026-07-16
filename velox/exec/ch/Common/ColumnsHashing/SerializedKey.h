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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::exec::ch {

struct StringRef {
  const char* data{nullptr};
  uint32_t size{0};

  bool operator==(const StringRef& other) const {
    if (size != other.size) {
      return false;
    }
    if (size == 0) {
      return true;
    }
    if (data == nullptr || other.data == nullptr) {
      return false;
    }
    return std::memcmp(data, other.data, size) == 0;
  }
};

// Hashes the key bytes with hardware CRC32 (simd::crc32U64), the same
// primitive the fixed-width keys use (HashCRC32/HashWide). Feeds the bytes
// eight at a time, then folds any tail of under eight bytes through a
// zero-initialized word so no read runs past the key. Mirrors ClickHouse's
// CRC32-based key_string hashing, whose length-dispatch-free loop keeps
// branch prediction stable for variable-length short keys where
// bits::hashBytes suffers heavy branch misses.
struct StringRefHash {
  size_t operator()(const StringRef& key) const {
    uint64_t hash = 0;
    const char* data = key.data;
    uint32_t remaining = key.size;
    while (remaining >= sizeof(uint64_t)) {
      uint64_t word;
      std::memcpy(&word, data, sizeof(word));
      hash = simd::crc32U64(hash, word);
      data += sizeof(uint64_t);
      remaining -= sizeof(uint64_t);
    }
    if (remaining > 0) {
      uint64_t tail = 0;
      std::memcpy(&tail, data, remaining);
      hash = simd::crc32U64(hash, tail);
    }
    return hash;
  }
};

inline bool useSerializedKey(const std::vector<TypePtr>& types) {
  VELOX_USER_CHECK(!types.empty(), "serialized key requires at least one key");
  size_t fixedBytes = 0;
  for (const auto& type : types) {
    VELOX_USER_CHECK_NOT_NULL(type);
    switch (type->kind()) {
      case TypeKind::TINYINT:
        fixedBytes += sizeof(int8_t);
        break;
      case TypeKind::SMALLINT:
        fixedBytes += sizeof(int16_t);
        break;
      case TypeKind::INTEGER:
        fixedBytes += sizeof(int32_t);
        break;
      case TypeKind::BIGINT:
        fixedBytes += sizeof(int64_t);
        break;
      case TypeKind::VARCHAR:
        return true;
      default:
        VELOX_USER_FAIL(
            "serialized key supports integer fixed-width and VARCHAR keys");
    }
  }
  return fixedBytes > 32;
}

} // namespace facebook::velox::exec::ch
