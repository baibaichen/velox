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
#include <variant>
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

enum class FixedKeyWidth : uint8_t {
  k8 = 1,
  k16 = 2,
  k32 = 4,
  k64 = 8,
  k128 = 16,
  k256 = 32,
};

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
  // A single 1- or 2-byte integer key routes to the direct-address map, so its
  // packed width is the narrow k8/k16. Multi-column packs never narrow below
  // k32 even when their total is 1 or 2 bytes, matching FixedKeyMap::chooseType
  // which reserves key8/key16 for single-column keys.
  if (types.size() == 1) {
    if (bytes == sizeof(uint8_t)) {
      return FixedKeyWidth::k8;
    }
    if (bytes == sizeof(uint16_t)) {
      return FixedKeyWidth::k16;
    }
  }
  if (bytes <= sizeof(uint32_t)) {
    return FixedKeyWidth::k32;
  }
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
      : channels_(std::move(channels)),
        selectedRows_(rows.size(), false),
        packedKeyValid_(rows.size(), false),
        allRowsSelected_(rows.isAllSelected()) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_USER_CHECK(!channels_.empty(), "packFixed requires key channels");
    rows.applyToSelected(
        [&](vector_size_t row) { selectedRows_[row] = true; });
    std::vector<TypePtr> types;
    types.reserve(channels_.size());
    decodedKeys_.reserve(channels_.size());
    keySizes_.reserve(channels_.size());
    for (const auto channel : channels_) {
      VELOX_USER_CHECK_LT(channel, input->childrenSize());
      auto key = input->childAt(channel)->loadedVector();
      types.push_back(key->type());
      keySizes_.push_back(fixedKeyTypeSize(key->typeKind()));
      auto decoded = std::make_unique<DecodedVector>(*key, rows);
      mayHaveNulls_ |= decoded->mayHaveNulls();
      decodedKeys_.push_back(std::move(decoded));
    }
    width_ = fixedKeyWidth(types);
  }

  FixedKeyWidth width() const {
    return width_;
  }

  bool mayHaveNulls() const {
    return mayHaveNulls_;
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

  template <typename Key>
  void packAll() {
    VELOX_CHECK_EQ(sizeof(Key), static_cast<size_t>(width_));
    preparedKeys_ = std::vector<Key>(selectedRows_.size());
    auto& packedKeys = std::get<std::vector<Key>>(preparedKeys_);
    preparedKeysData_ = packedKeys.data();
    packedKeyValid_ = selectedRows_;

    size_t offset = 0;
    for (size_t i = 0; i < decodedKeys_.size(); ++i) {
      copyColumn(*decodedKeys_[i], offset, packedKeys);
      offset += keySizes_[i];
    }
  }

  bool hasPackedKeyAt(vector_size_t row) const {
    return packedKeyValid_[row];
  }

  template <typename Key>
  const Key& packedAt(vector_size_t row) const {
    return static_cast<const Key*>(preparedKeysData_)[row];
  }

 private:
  template <typename Value, typename Key>
  void copyColumnValues(
      const DecodedVector& decoded,
      size_t offset,
      std::vector<Key>& packedKeys) {
    const auto* values = decoded.data<Value>();
    if (allRowsSelected_ && !decoded.mayHaveNulls() &&
        decoded.isIdentityMapping()) {
      auto* destination =
          reinterpret_cast<char*>(packedKeys.data()) + offset;
      for (vector_size_t row = 0; row < packedKeys.size(); ++row) {
        std::memcpy(
            destination + row * sizeof(Key),
            values + row,
            sizeof(Value));
      }
      return;
    }

    for (vector_size_t row = 0; row < packedKeys.size(); ++row) {
      if (!packedKeyValid_[row]) {
        continue;
      }
      if (decoded.isNullAt(row)) {
        packedKeyValid_[row] = false;
        continue;
      }
      const auto value = decoded.valueAt<Value>(row);
      std::memcpy(
          reinterpret_cast<char*>(&packedKeys[row]) + offset,
          &value,
          sizeof(value));
    }
  }

  template <typename Key>
  void copyColumn(
      const DecodedVector& decoded,
      size_t offset,
      std::vector<Key>& packedKeys) {
    switch (decoded.base()->typeKind()) {
      case TypeKind::TINYINT:
        return copyColumnValues<int8_t>(decoded, offset, packedKeys);
      case TypeKind::SMALLINT:
        return copyColumnValues<int16_t>(decoded, offset, packedKeys);
      case TypeKind::INTEGER:
        return copyColumnValues<int32_t>(decoded, offset, packedKeys);
      case TypeKind::BIGINT:
        return copyColumnValues<int64_t>(decoded, offset, packedKeys);
      default:
        VELOX_UNREACHABLE();
    }
  }

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
  std::vector<bool> selectedRows_;
  std::vector<bool> packedKeyValid_;
  std::variant<
      std::monostate,
      std::vector<uint8_t>,
      std::vector<uint16_t>,
      std::vector<uint32_t>,
      std::vector<uint64_t>,
      std::vector<UInt128>,
      std::vector<UInt256>>
      preparedKeys_;
  const void* preparedKeysData_{nullptr};
  bool allRowsSelected_{false};
  bool mayHaveNulls_{false};
  FixedKeyWidth width_{FixedKeyWidth::k64};
};

} // namespace facebook::velox::exec::ch
