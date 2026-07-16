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

class SerializedKeyDecoder {
 public:
  static constexpr bool hasCheapKeyCalculation = false;

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

/// Zero-copy key decoder for a single VARCHAR key, mirroring ClickHouse's
/// HashMethodString. Instead of serializing each row into a std::string, it
/// exposes the column's own char buffer as a StringRef, avoiding a per-row
/// copy. Only valid for the key_string map, which chooseType selects solely for
/// a single VARCHAR key.
class StringViewKeyDecoder {
 public:
  StringViewKeyDecoder(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& channels,
      const std::vector<TypePtr>& keyTypes,
      const SelectivityVector& rows) {
    VELOX_CHECK_NOT_NULL(input);
    VELOX_USER_CHECK_EQ(
        channels.size(), 1, "StringViewKeyDecoder requires a single key");
    VELOX_USER_CHECK_EQ(keyTypes.size(), 1);
    VELOX_USER_CHECK_EQ(
        keyTypes[0]->kind(),
        TypeKind::VARCHAR,
        "StringViewKeyDecoder requires a VARCHAR key");
    const auto channel = channels[0];
    VELOX_USER_CHECK_LT(channel, input->childrenSize());
    auto key = input->childAt(channel)->loadedVector();
    VELOX_USER_CHECK_EQ(key->type(), keyTypes[0]);
    decoded_ = std::make_unique<DecodedVector>(*key, rows);
  }

  /// Reads the key at the given row as a StringRef. Returns false for null
  /// keys, matching the skip semantics of SerializedKeyDecoder::serialize. For
  /// keys under 13 bytes StringView stores the bytes inline, so the returned
  /// StringRef points into inlineStorage_; it stays valid until the next at()
  /// call. Longer keys reference the column buffer directly (zero-copy).
  bool at(vector_size_t row, StringRef& key) const {
    if (decoded_->isNullAt(row)) {
      return false;
    }
    const auto value = decoded_->valueAt<StringView>(row);
    VELOX_USER_CHECK_LE(value.size(), std::numeric_limits<uint32_t>::max());
    const auto size = static_cast<uint32_t>(value.size());
    if (value.isInline()) {
      // Inlined bytes live inside the temporary StringView; copy them into
      // stable storage so the StringRef does not dangle across map operations.
      std::memcpy(inlineStorage_.data(), value.data(), size);
      key = StringRef{inlineStorage_.data(), size};
    } else {
      key = StringRef{value.data(), size};
    }
    return true;
  }

  /// Reads a look-ahead key for software prefetching. Behaves like at() but uses
  /// a separate inline buffer, so it never clobbers the StringRef returned by
  /// at() for the current row. The build path consumes the current row's key
  /// (find/emplace/arena) after issuing the prefetch, so the two must not share
  /// storage. The returned StringRef stays valid until the next atForPrefetch()
  /// call.
  bool atForPrefetch(vector_size_t row, StringRef& key) const {
    if (decoded_->isNullAt(row)) {
      return false;
    }
    const auto value = decoded_->valueAt<StringView>(row);
    VELOX_USER_CHECK_LE(value.size(), std::numeric_limits<uint32_t>::max());
    const auto size = static_cast<uint32_t>(value.size());
    if (value.isInline()) {
      std::memcpy(prefetchStorage_.data(), value.data(), size);
      key = StringRef{prefetchStorage_.data(), size};
    } else {
      key = StringRef{value.data(), size};
    }
    return true;
  }

 private:
  std::unique_ptr<DecodedVector> decoded_;
  // Backing storage for inlined keys read by at(); valid until the next at().
  mutable std::array<char, StringView::kInlineSize> inlineStorage_{};
  // Separate backing storage for atForPrefetch(), so look-ahead reads do not
  // clobber the current row's key held in inlineStorage_.
  mutable std::array<char, StringView::kInlineSize> prefetchStorage_{};
};

} // namespace facebook::velox::exec::ch
