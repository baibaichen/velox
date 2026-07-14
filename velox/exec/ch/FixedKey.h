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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

#include <array>
#include <cstring>
#include <memory>
#include <vector>

namespace facebook::velox::exec::ch {

struct UInt128 {
  std::array<uint64_t, 2> words{};
  bool operator==(const UInt128&) const = default;
};

struct UInt256 {
  std::array<uint64_t, 4> words{};
  bool operator==(const UInt256&) const = default;
};

static_assert(std::is_trivially_copyable_v<UInt128>);
static_assert(std::is_trivially_copyable_v<UInt256>);
static_assert(sizeof(UInt128) == 16);
static_assert(sizeof(UInt256) == 32);

enum class FixedKeyWidth : uint8_t { k64 = 8, k128 = 16, k256 = 32 };

inline size_t fixedKeyTypeSize(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
      return sizeof(int8_t);
    case TypeKind::SMALLINT:
      return sizeof(int16_t);
    case TypeKind::INTEGER:
      return sizeof(int32_t);
    case TypeKind::BIGINT:
      return sizeof(int64_t);
    default:
      VELOX_USER_FAIL(
          "packFixed supports TINYINT, SMALLINT, INTEGER, and BIGINT");
  }
}

inline FixedKeyWidth fixedKeyWidth(const std::vector<TypePtr>& types) {
  VELOX_USER_CHECK(!types.empty(), "packFixed requires at least one key");
  size_t bytes = 0;
  for (const auto& type : types) {
    VELOX_USER_CHECK_NOT_NULL(type);
    bytes += fixedKeyTypeSize(type->kind());
  }
  VELOX_USER_CHECK_LE(bytes, sizeof(UInt256), "packFixed key exceeds 32 bytes");
  return bytes <= sizeof(uint64_t)
      ? FixedKeyWidth::k64
      : bytes <= sizeof(UInt128) ? FixedKeyWidth::k128 : FixedKeyWidth::k256;
}

class FixedKeyDecoder {
 public:
  static constexpr bool hasCheapKeyCalculation = true;

  FixedKeyDecoder(
      const RowVectorPtr& input,
      std::vector<column_index_t> channels,
      const SelectivityVector& rows)
      : channels_(std::move(channels)) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_USER_CHECK(!channels_.empty(), "packFixed requires key channels");
    std::vector<TypePtr> types;
    types.reserve(channels_.size());
    decodedKeys_.reserve(channels_.size());
    keySizes_.reserve(channels_.size());
    for (const auto channel : channels_) {
      VELOX_USER_CHECK_LT(channel, input->childrenSize());
      auto key = input->childAt(channel)->loadedVector();
      types.push_back(key->type());
      keySizes_.push_back(fixedKeyTypeSize(key->typeKind()));
      decodedKeys_.push_back(
          std::make_unique<DecodedVector>(*key, rows));
    }
    width_ = fixedKeyWidth(types);
  }

  FixedKeyWidth width() const {
    return width_;
  }

  template <typename Key>
  bool pack(vector_size_t row, Key& key) const {
    VELOX_CHECK_GE(sizeof(Key), static_cast<size_t>(width_));
    key = Key{};
    size_t offset = 0;
    for (size_t i = 0; i < decodedKeys_.size(); ++i) {
      const auto& decoded = *decodedKeys_[i];
      if (decoded.isNullAt(row)) {
        return false;
      }
      copyValue(decoded, row, reinterpret_cast<char*>(&key) + offset);
      offset += keySizes_[i];
    }
    return true;
  }

 private:
  static void copyValue(
      const DecodedVector& decoded,
      vector_size_t row,
      char* destination) {
    switch (decoded.base()->typeKind()) {
      case TypeKind::TINYINT: {
        const auto value = decoded.valueAt<int8_t>(row);
        std::memcpy(destination, &value, sizeof(value));
        return;
      }
      case TypeKind::SMALLINT: {
        const auto value = decoded.valueAt<int16_t>(row);
        std::memcpy(destination, &value, sizeof(value));
        return;
      }
      case TypeKind::INTEGER: {
        const auto value = decoded.valueAt<int32_t>(row);
        std::memcpy(destination, &value, sizeof(value));
        return;
      }
      case TypeKind::BIGINT: {
        const auto value = decoded.valueAt<int64_t>(row);
        std::memcpy(destination, &value, sizeof(value));
        return;
      }
      default:
        VELOX_UNREACHABLE();
    }
  }

  std::vector<column_index_t> channels_;
  std::vector<std::unique_ptr<DecodedVector>> decodedKeys_;
  std::vector<size_t> keySizes_;
  FixedKeyWidth width_{FixedKeyWidth::k64};
};

} // namespace facebook::velox::exec::ch
