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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

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

struct StringRefHash {
  size_t operator()(const StringRef& key) const {
    return bits::hashBytes(1, key.data, key.size);
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

class SerializedKeyDecoder {
 public:
  SerializedKeyDecoder(
      const RowVectorPtr& input,
      std::vector<column_index_t> channels,
      const std::vector<TypePtr>& keyTypes,
      const SelectivityVector& rows)
      : channels_(std::move(channels)), keyTypes_(keyTypes) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_USER_CHECK(!channels_.empty(), "serialized key requires channels");
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
        "SerializedKeyDecoder requires a string or an over-32-byte key");
  }

  bool serialize(vector_size_t row, std::string& output) const {
    output.clear();
    for (const auto& decodedPtr : decodedKeys_) {
      const auto& decoded = *decodedPtr;
      if (decoded.isNullAt(row)) {
        output.clear();
        return false;
      }
      switch (decoded.base()->typeKind()) {
        case TypeKind::TINYINT:
          appendFixed(decoded.valueAt<int8_t>(row), output);
          break;
        case TypeKind::SMALLINT:
          appendFixed(decoded.valueAt<int16_t>(row), output);
          break;
        case TypeKind::INTEGER:
          appendFixed(decoded.valueAt<int32_t>(row), output);
          break;
        case TypeKind::BIGINT:
          appendFixed(decoded.valueAt<int64_t>(row), output);
          break;
        case TypeKind::VARCHAR: {
          const auto value = decoded.valueAt<StringView>(row);
          VELOX_USER_CHECK_LE(
              value.size(), std::numeric_limits<uint32_t>::max());
          const auto size = static_cast<uint32_t>(value.size());
          appendFixed(size, output);
          output.append(value.data(), value.size());
          break;
        }
        default:
          VELOX_UNREACHABLE();
      }
    }
    return true;
  }

 private:
  template <typename T>
  static void appendFixed(T value, std::string& output) {
    output.append(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  std::vector<column_index_t> channels_;
  std::vector<TypePtr> keyTypes_;
  std::vector<std::unique_ptr<DecodedVector>> decodedKeys_;
};

} // namespace facebook::velox::exec::ch
