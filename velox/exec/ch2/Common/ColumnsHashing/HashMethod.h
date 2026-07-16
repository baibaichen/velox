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
#include "velox/exec/ch2/Common/ColumnsHashing/ColumnsHashingImpl.h"
#include "velox/exec/ch2/Common/SipHash.h"
#include "velox/vector/FlatVector.h"

#include <folly/Portability.h>

#include <cstddef>
#include <cstring>
#include <string_view>
#include <vector>

namespace facebook::velox::exec::ch2 {

using Sizes = std::vector<size_t>;
using ColumnRawPtrs = std::vector<VectorPtr>;

template <typename T>
FOLLY_ALWAYS_INLINE T unalignedLoad(const void* address) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value;
  std::memcpy(&value, address, sizeof(value));
  return value;
}

template <
    typename Value,
    typename Mapped,
    typename FieldType,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodOneNumber : public columns_hashing_impl::HashMethodBase<
                                 HashMethodOneNumber<
                                     Value,
                                     Mapped,
                                     FieldType,
                                     use_cache,
                                     need_offset,
                                     nullable>,
                                 Value,
                                 Mapped,
                                 use_cache,
                                 need_offset,
                                 nullable> {
  using Self = HashMethodOneNumber<
      Value,
      Mapped,
      FieldType,
      use_cache,
      need_offset,
      nullable>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

  static constexpr bool has_cheap_key_calculation = true;
  static constexpr bool has_pre_computed_hashes = false;

  const char* vec;

  HashMethodOneNumber(
      const ColumnRawPtrs& key_columns,
      const Sizes&,
      const HashMethodContextPtr&)
      : Base(key_columns.empty() ? nullptr : key_columns[0]) {
    VELOX_CHECK_EQ(key_columns.size(), 1);
    const auto column = key_columns[0]->loadedVector();
    VELOX_CHECK(
        column->isFlatEncoding(),
        "HashMethodOneNumber task1 requires a flat key vector");
    VELOX_CHECK(
        !column->mayHaveNulls(),
        "HashMethodOneNumber task1 requires non-null keys");
    const auto* flat = column->template asFlatVector<FieldType>();
    VELOX_CHECK_NOT_NULL(
        flat, "HashMethodOneNumber key type does not match FieldType");
    vec = reinterpret_cast<const char*>(flat->rawValues());
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  FieldType getKeyHolder(size_t row, ch::Arena&) const {
    return unalignedLoad<FieldType>(vec + row * sizeof(FieldType));
  }

  const FieldType* getKeyData() const {
    return reinterpret_cast<const FieldType*>(vec);
  }
};


// ============================================================================
// HashMethodString — exactly搬自 CH ColumnsHashing/HashMethod.h:159-218
// (变长字符串 key)。铁律:算法逐字搬 CH,只换 infra 边界。
//
// infra 边界(本 task 主坎):
//   CH `ColumnString` 用 `offsets`(IColumn::Offset 数组) + `chars`(连续
//   UInt8 buffer),第 row 行 key = chars[offsets[row-1] .. offsets[row]]。
//   Velox flat VARCHAR 是 `FlatVector<StringView>`,每行一个自包含的
//   StringView(ptr+size,短串内联/长串指 buffer)。所以取值承载从
//   "offsets/chars 差值定位字节区间" 换成 "从 rawValues()[row] 取 StringView"。
//   取到 view 之后 —— 包 ArenaKeyHolder{key, pool} 的算法逐字不变。
// ============================================================================
template <
    typename Value,
    typename Mapped,
    bool place_string_to_arena = true,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodString : public columns_hashing_impl::HashMethodBase<
                              HashMethodString<
                                  Value,
                                  Mapped,
                                  place_string_to_arena,
                                  use_cache,
                                  need_offset,
                                  nullable>,
                              Value,
                              Mapped,
                              use_cache,
                              need_offset,
                              nullable> {
  using Self = HashMethodString<
      Value,
      Mapped,
      place_string_to_arena,
      use_cache,
      need_offset,
      nullable>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

  static constexpr bool has_cheap_key_calculation = false;
  static constexpr bool has_pre_computed_hashes = false;

  // infra 边界: CH `const IColumn::Offset* offsets; const UInt8* chars;`
  // 换成 Velox 每行自包含的 StringView 数组指针。
  const StringView* values;

  HashMethodString(
      const ColumnRawPtrs& key_columns,
      const Sizes& /*key_sizes*/,
      const HashMethodContextPtr&)
      : Base(key_columns.empty() ? nullptr : key_columns[0]) {
    // CH: if constexpr (nullable) 取 ColumnNullable 的 nested;else 直接列。
    // ch2 task2: nullable 留 VELOX_CHECK + TODO(同 task1)。照抄保留结构。
    VELOX_CHECK_EQ(key_columns.size(), 1);
    const auto column = key_columns[0]->loadedVector();
    // flat + non-null 限制(同 task1)。非-flat/nullable 留 TODO。
    VELOX_CHECK(
        column->isFlatEncoding(),
        "HashMethodString task2 requires a flat key vector (non-flat TODO)");
    VELOX_CHECK(
        !column->mayHaveNulls(),
        "HashMethodString task2 requires non-null keys (nullable TODO)");
    if constexpr (nullable) {
      VELOX_NYI("nullable HashMethodString is not supported in task2");
    }
    const auto* flat = column->template asFlatVector<StringView>();
    VELOX_CHECK_NOT_NULL(
        flat, "HashMethodString key column is not FlatVector<StringView>");
    // infra 边界: CH `offsets = column_string.getOffsets().data();
    //                  chars   = column_string.getChars().data();`
    // 换成 Velox 的 StringView 数组基址。
    values = flat->rawValues();
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  // CH 原文 (HashMethod.h:198-211):
  //   auto getKeyHolder(ssize_t row, Arena & pool) const {
  //     std::string_view key(reinterpret_cast<const char *>(chars)
  //         + offsets[row - 1], offsets[row] - offsets[row - 1]);
  //     if constexpr (place_string_to_arena)
  //         return ArenaKeyHolder{key, pool};
  //     else
  //         return key;
  //   }
  //
  // ch2: 算法(取 string_view → 包 ArenaKeyHolder)逐字不变;只有
  //   "怎么从列拿到这行字节 view" 这个 infra 边界换成 Velox StringView。
  auto getKeyHolder(size_t row, ch::Arena& pool) const {
    // infra 边界: CH 用 chars+offsets 差值算出这行字节区间;Velox 直接
    // 取第 row 行的 StringView(自带 ptr+size)。取到的 std::string_view
    // 语义与 CH 完全一致。
    const StringView& sv = values[row];
    std::string_view key(sv.data(), sv.size());

    if constexpr (place_string_to_arena) {
      return ArenaKeyHolder{key, pool};
    } else {
      return key;
    }
  }

 protected:
  friend class columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;
};

} // namespace facebook::velox::exec::ch2
