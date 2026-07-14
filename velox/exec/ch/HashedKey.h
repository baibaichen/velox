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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/ch/FixedKey.h"
#include "velox/exec/ch/SerializedKey.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

#include <xxhash.h>

#include <memory>
#include <utility>
#include <vector>

namespace facebook::velox::exec::ch {

class HashedKeyDecoder {
 public:
  // Digesting again for prefetch would scan every variable-width key twice.
  static constexpr bool hasCheapKeyCalculation = false;

  HashedKeyDecoder(
      const RowVectorPtr& input,
      std::vector<column_index_t> channels,
      const std::vector<TypePtr>& keyTypes,
      const SelectivityVector& rows)
      : channels_(std::move(channels)), keyTypes_(keyTypes) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_USER_CHECK(!channels_.empty(), "hashed key requires channels");
    VELOX_USER_CHECK_EQ(channels_.size(), keyTypes_.size());
    decodedKeys_.reserve(channels_.size());
    for (size_t i = 0; i < channels_.size(); ++i) {
      const auto channel = channels_[i];
      VELOX_USER_CHECK_LT(channel, input->childrenSize());
      auto key = input->childAt(channel)->loadedVector();
      VELOX_USER_CHECK_EQ(key->type(), keyTypes_[i]);
      decodedKeys_.push_back(std::make_unique<DecodedVector>(*key, rows));
    }
    VELOX_USER_CHECK(
        useSerializedKey(keyTypes_),
        "HashedKeyDecoder requires a string or an over-32-byte key");
  }

  bool hash(vector_size_t row, UInt128& digest) const {
    uint64_t low = 0x243f6a8885a308d3ULL;
    uint64_t high = 0x13198a2e03707344ULL;
    for (size_t i = 0; i < decodedKeys_.size(); ++i) {
      const auto& decoded = *decodedKeys_[i];
      if (decoded.isNullAt(row)) {
        digest = {};
        return false;
      }
      hashValue(decoded, row, i, low, high);
    }
    digest.words = {low, high};
    return true;
  }

 private:
  static uint64_t mix(uint64_t value) {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  }

  static void update(
      const void* data,
      size_t size,
      TypeKind kind,
      size_t column,
      uint64_t& low,
      uint64_t& high) {
    const uint64_t domain =
        (static_cast<uint64_t>(kind) << 56) ^
        (static_cast<uint64_t>(column) << 32) ^ size;
    const auto valueHash = XXH3_128bits_withSeed(
        data, size, low ^ mix(domain ^ 0x9e3779b97f4a7c15ULL));
    low = mix(low ^ valueHash.low64 ^ domain);
    high = mix(
        high ^ valueHash.high64 ^ bits::rotateLeft64(domain, 23));
  }

  template <typename T>
  static void updateFixed(
      const DecodedVector& decoded,
      vector_size_t row,
      size_t column,
      uint64_t& low,
      uint64_t& high) {
    const auto value = decoded.valueAt<T>(row);
    update(&value, sizeof(value), decoded.base()->typeKind(), column, low, high);
  }

  static void hashValue(
      const DecodedVector& decoded,
      vector_size_t row,
      size_t column,
      uint64_t& low,
      uint64_t& high) {
    switch (decoded.base()->typeKind()) {
      case TypeKind::TINYINT:
        updateFixed<int8_t>(decoded, row, column, low, high);
        return;
      case TypeKind::SMALLINT:
        updateFixed<int16_t>(decoded, row, column, low, high);
        return;
      case TypeKind::INTEGER:
        updateFixed<int32_t>(decoded, row, column, low, high);
        return;
      case TypeKind::BIGINT:
        updateFixed<int64_t>(decoded, row, column, low, high);
        return;
      case TypeKind::VARCHAR: {
        const auto value = decoded.valueAt<StringView>(row);
        update(
            value.data(),
            value.size(),
            TypeKind::VARCHAR,
            column,
            low,
            high);
        return;
      }
      default:
        VELOX_UNREACHABLE();
    }
  }

  std::vector<column_index_t> channels_;
  std::vector<TypePtr> keyTypes_;
  std::vector<std::unique_ptr<DecodedVector>> decodedKeys_;
};

} // namespace facebook::velox::exec::ch
