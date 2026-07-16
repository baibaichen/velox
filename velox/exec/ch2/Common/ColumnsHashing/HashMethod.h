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
#include "velox/exec/ch2/Interpreters/AggregationCommon.h"
#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h" // ch::UInt128 / ch::UInt256
#include "velox/exec/ch2/Common/SipHash.h"
#include "velox/vector/FlatVector.h"
#include "velox/exec/ch2/DataTypes/FixedStringType.h"

#include <folly/Portability.h>

#include <cstddef>
#include <optional>
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
// HashMethodOneNumberInRange — exactly 搬自 CH ColumnsHashing/HashMethod.h:
// 99-149 (O2 hash join fixed-range 优化)。像 HashMethodOneNumber,但每个 key
// 减 min_key 平移到 [0, range_size),并做范围校验。铁律 + O2 头号红线:
// range 平移+范围校验定位算法逐字搬 CH,**绝不接 Velox kArray/VectorHasher
// 的 range 模式顶替**。Velox 只允许出现在"扫列算 min/max 喂进
// min_key/range_size"这个值域接入 infra 边界(见 computeKeyRange)。
//
// infra 边界:
//   CH `vec = column->getRawData().data()`(IColumn 裸指针)→ Velox flat
//     rawValues()(同 HashMethodOneNumber)。
//   CH min_key/range_size 是成员,由 hash join build 侧扫 key 列算好 min/max
//     后 set(Interpreters/HashJoin/HashJoinMethodsImpl.h:275-276
//     `getter.min_key = key_range.min_key; getter.range_size = key_range.size`,
//     key_range 由 HashJoin.cpp:2216-2262 扫 build key 列算 min/max、
//     range = max-min+1 得到)。ch2 值域接入用 Velox flat 列扫 min/max 拿
//     两个数(computeKeyRange),再 set 进成员——**只是拿到两个数,不接 kArray
//     寻址**。
//
// range map 承载:CH range 场景下 hash 表存的是平移后 key ∈ [0, range_size)
//   (HashJoin.cpp:2257 `range_map->emplace(getKey() - min_key, ...)`),走的
//   仍是普通定长 map 寻址(平移后 key 当普通 key)。"直接当索引"的直查数组
//   fastpath(probeFixedHashMap)是另一条独立优化路径,不在 HashMethod 这条
//   路上。所以 range map 承载 = 复用已有定长 map,平移算法在 HashMethod 侧。
// ============================================================================
template <
    typename Value,
    typename Mapped,
    typename FieldType,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodOneNumberInRange
    : public columns_hashing_impl::HashMethodBase<
          HashMethodOneNumberInRange<
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
  using Self = HashMethodOneNumberInRange<
      Value,
      Mapped,
      FieldType,
      use_cache,
      need_offset,
      nullable>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

  // CH: static constexpr bool has_range_check = true;(本 task 启用
  // HashMethodBase 的 has_range_check 分支:findKey 走 getKeyHolderInRange、
  // 范围外 miss)。
  static constexpr bool has_range_check = true;
  static constexpr bool has_cheap_key_calculation = true;
  static constexpr bool has_pre_computed_hashes = false;

  // CH: const char * vec; FieldType min_key{}; FieldType range_size{};
  const char* vec;
  FieldType min_key{};
  FieldType range_size{};

  HashMethodOneNumberInRange(
      const ColumnRawPtrs& key_columns,
      const Sizes&,
      const HashMethodContextPtr&)
      : Base(key_columns.empty() ? nullptr : key_columns[0]) {
    // CH: explicit HashMethodOneNumberInRange(const IColumn * column):Base(column)
    //     vec = column->getRawData().data();(nullable 取 nested,照抄保留 TODO)
    VELOX_CHECK_EQ(key_columns.size(), 1);
    const auto column = key_columns[0]->loadedVector();
    // flat + non-null 限制(同 task1)。非-flat/nullable 留 TODO。
    VELOX_CHECK(
        column->isFlatEncoding(),
        "HashMethodOneNumberInRange task5 requires a flat key vector (non-flat TODO)");
    VELOX_CHECK(
        !column->mayHaveNulls(),
        "HashMethodOneNumberInRange task5 requires non-null keys (nullable TODO)");
    if constexpr (nullable) {
      VELOX_NYI("nullable HashMethodOneNumberInRange is not supported in task5");
    }
    const auto* flat = column->template asFlatVector<FieldType>();
    VELOX_CHECK_NOT_NULL(
        flat, "HashMethodOneNumberInRange key type does not match FieldType");
    // infra 边界:CH IColumn 裸指针 → Velox flat rawValues()(同 OneNumber)。
    vec = reinterpret_cast<const char*>(flat->rawValues());
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  // CH 原文 (HashMethod.h:138-141) 逐字:直读 + 减 min_key 平移。
  //   FieldType getKeyHolder(size_t row, Arena &) const {
  //     return unalignedLoad<FieldType>(vec + row*sizeof(FieldType)) - min_key;
  //   }
  FieldType getKeyHolder(size_t row, ch::Arena&) const {
    return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)) - min_key;
  }

  // CH 原文 (HashMethod.h:143-147) 逐字:平移 + 范围校验 (shifted < range_size)。
  //   std::pair<FieldType,bool> getKeyHolderInRange(size_t row, Arena &) const {
  //     FieldType shifted_key =
  //         unalignedLoad<FieldType>(vec + row*sizeof(FieldType)) - min_key;
  //     return {shifted_key, shifted_key < range_size};
  //   }
  std::pair<FieldType, bool> getKeyHolderInRange(size_t row, ch::Arena&) const {
    FieldType shifted_key =
        unalignedLoad<FieldType>(vec + row * sizeof(FieldType)) - min_key;
    return {shifted_key, shifted_key < range_size};
  }
};

// ============================================================================
// computeKeyRange — 值域接入 infra 边界(唯一允许 Velox 参与的地方,且只是
// "拿到两个数")。对应 CH HashJoin.cpp:2216-2262 扫 build key 列算 min/max、
// range = max-min+1。ch2 用 Velox flat 列扫 min/max 拿两个数,填 min_key/
// range_size 成员。**不接 kArray 寻址,只算两个数。**
//
// 返回 {min_key, range_size},range_size = max - min + 1(与 CH 一致)。
//
// ---- 两层溢出防护,逐字对齐 CH HashJoin.cpp:2198/2242/2246 ----
// CH 有两层防护,ch2 都补上:
//   1. static constexpr size_t MAX_RANGE = (1ULL << 18); (HashJoin.cpp:2198)。
//      扫描时 if (static_cast<size_t>(max_key - min_key) >= MAX_RANGE) return;
//      (HashJoin.cpp:2242)——超限**不启用 range 优化**,CH 直接 return 不建
//      range_map、回退普通 key32/key64 map。ch2 computeKeyRange 返回
//      std::optional<KeyRange>,空 optional = "这批 key 不适合 range 优化",
//      调用方据此回退普通定长 map(对齐 CH 的 return)。
//   2. size_t range = static_cast<size_t>(max_key - min_key) + 1;
//      (HashJoin.cpp:2246)——差值**提升到 size_t 无符号域**再 +1,wraparound
//      defined,不是有符号 FieldType 溢出 UB。ch2 同样在无符号域算。
//
// range_size 成员类型仍是 FieldType(与 CH HashMethodOneNumberInRange 一致):
// 因为只有 max-min < MAX_RANGE = 2^18 才会启用,range = max-min+1 <= 2^18 必然
// 落在 FieldType(>=int32)可表示范围内,收窄回 FieldType 无损失,与
// getKeyHolderInRange 里 shifted_key < range_size(FieldType 比较)语义不变。
// ============================================================================
template <typename FieldType>
struct KeyRange {
  FieldType min_key{};
  FieldType range_size{};
};

// CH: static constexpr size_t MAX_RANGE = (1ULL << 18); (HashJoin.cpp:2198)。
static constexpr size_t kComputeKeyRangeMaxRange = (1ULL << 18);

template <typename FieldType>
std::optional<KeyRange<FieldType>> computeKeyRange(const VectorPtr& keyColumn) {
  const auto column = keyColumn->loadedVector();
  VELOX_CHECK(
      column->isFlatEncoding(),
      "computeKeyRange task5 requires a flat key vector");
  VELOX_CHECK(
      !column->mayHaveNulls(),
      "computeKeyRange task5 requires non-null keys");
  const auto* flat = column->template asFlatVector<FieldType>();
  VELOX_CHECK_NOT_NULL(flat, "computeKeyRange key type does not match FieldType");
  const auto rows = flat->size();
  VELOX_CHECK_GT(rows, 0, "computeKeyRange requires >=1 row");
  const FieldType* data = flat->rawValues();
  using UnsignedField = std::make_unsigned_t<FieldType>;
  // CH: Key min_key = it->getKey(); ... 扫全部 key 取 min/max。
  FieldType minKey = data[0];
  FieldType maxKey = data[0];
  for (vector_size_t i = 1; i < rows; ++i) {
    if (data[i] < minKey) {
      minKey = data[i];
    }
    if (data[i] > maxKey) {
      maxKey = data[i];
    }
    // CH HashJoin.cpp:2242 逐字:超 MAX_RANGE 不启用 range 优化(CH return)。
    // 差值在 size_t 无符号域比较,避免有符号 FieldType 溢出 UB。
    if (static_cast<size_t>(
            static_cast<UnsignedField>(maxKey) -
            static_cast<UnsignedField>(minKey)) >= kComputeKeyRangeMaxRange) {
      return std::nullopt;
    }
  }
  // CH HashJoin.cpp:2246 逐字:size_t range = static_cast<size_t>(max - min) + 1;
  // 无符号域算(wraparound defined,非有符号 UB)。此处 range <= MAX_RANGE = 2^18,
  // 收窄回 FieldType 无损失。
  const size_t range = static_cast<size_t>(
                           static_cast<UnsignedField>(maxKey) -
                           static_cast<UnsignedField>(minKey)) +
      1;
  KeyRange<FieldType> r;
  r.min_key = minKey;
  r.range_size = static_cast<FieldType>(range);
  return r;
}


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


// ============================================================================
// HashMethodFixedString — exactly搬自 CH ColumnsHashing/HashMethod.h:219-276
// (定长 N 字节字符串 key)。铁律 + O3:取 n 字节 slice + 包 ArenaKeyHolder 的
// persist 算法逐字搬 CH;只把 infra 边界(数据怎么装、N 从哪拿)换成 Velox。
//
// infra 边界(O3 主坎):
//   CH `ColumnFixedString`:`getN()` 拿定长 N,`getChars()` 是连续 UInt8
//     buffer,第 row 行 key = chars[row*n .. row*n+n](CH getKeyHolder:
//     `string_view(&(*chars)[row*n], n)`)。
//   Velox 无 FixedString 物理类型 → 用 ch2::FixedStringType(N) 逻辑类型承载:
//     * N 从 key 列的 FixedStringType 拿(= CH column_string.getN() 的等价)。
//     * 数据装在 FlatVector<StringView>(物理 VARBINARY),每行 StringView 约定
//       正好 N 字节 → 直接取第 row 行 StringView(sv.data(), sv.size()==n)。
//   取到 view 之后 —— 包 ArenaKeyHolder{key, pool} 的算法(persist 协议 task2
//   StringHashMapAdapter 已建,复用)逐字不变,与 HashMethodString 完全一致,
//   唯一区别是「怎么定位这行字节」:定长 n 字节 vs 变长 offsets 差值。
// ============================================================================
template <
    typename Value,
    typename Mapped,
    bool place_string_to_arena = true,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodFixedString : public columns_hashing_impl::HashMethodBase<
                                   HashMethodFixedString<
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
  using Self = HashMethodFixedString<
      Value,
      Mapped,
      place_string_to_arena,
      use_cache,
      need_offset,
      nullable>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

  // CH 原文 (HashMethod.h:237-238):
  //   static constexpr bool has_cheap_key_calculation = false;
  //   static constexpr bool has_pre_computed_hashes = false;
  static constexpr bool has_cheap_key_calculation = false;
  static constexpr bool has_pre_computed_hashes = false;

  // CH 原文 (HashMethod.h:240-241):
  //   size_t n;
  //   const ColumnFixedString::Chars * chars;
  // infra 边界:CH `n` 从 ColumnFixedString::getN() 拿;`chars` 是连续 UInt8
  // buffer。ch2:`n` 从 key 列的 FixedStringType 逻辑类型拿;数据承载换成 Velox
  // 每行自包含的 StringView 数组指针(约定每行正好 n 字节)。
  size_t n;
  const StringView* values;

  HashMethodFixedString(
      const ColumnRawPtrs& key_columns,
      const Sizes& /*key_sizes*/,
      const HashMethodContextPtr&)
      : Base(key_columns.empty() ? nullptr : key_columns[0]) {
    // CH 原文 (HashMethod.h:243-256):if constexpr (nullable) 取 ColumnNullable
    // 的 nested;else 直接列;assert_cast<ColumnFixedString>;n = getN();
    // chars = &getChars();
    // ch2 task6: nullable 留 VELOX_NYI + TODO(同 task2)。照抄保留结构。
    VELOX_CHECK_EQ(key_columns.size(), 1);
    const auto column = key_columns[0]->loadedVector();
    // flat + non-null 限制(同 task2)。非-flat/nullable 留 TODO。
    VELOX_CHECK(
        column->isFlatEncoding(),
        "HashMethodFixedString task6 requires a flat key vector (non-flat TODO)");
    VELOX_CHECK(
        !column->mayHaveNulls(),
        "HashMethodFixedString task6 requires non-null keys (nullable TODO)");
    if constexpr (nullable) {
      VELOX_NYI("nullable HashMethodFixedString is not supported in task6");
    }
    // infra 边界:CH `n = column_string.getN();` → ch2 从 key 列的
    // FixedStringType(N) 逻辑类型拿 N(= CH ColumnFixedString::getN() 等价)。
    const auto* fixedType =
        dynamic_cast<const FixedStringType*>(column->type().get());
    VELOX_CHECK_NOT_NULL(
        fixedType,
        "HashMethodFixedString key column must carry ch2::FixedStringType(N)");
    n = fixedType->fixedLength();
    // infra 边界:CH `chars = &column_string.getChars();`(连续 UInt8 buffer)→
    // Velox StringView 数组基址(每行自带 ptr+size,约定 size == n)。
    const auto* flat = column->template asFlatVector<StringView>();
    VELOX_CHECK_NOT_NULL(
        flat, "HashMethodFixedString key column is not FlatVector<StringView>");
    values = flat->rawValues();
    // 越界守卫:FixedStringType(N) 承载约定「每行 StringView 正好 n 字节」在
    // Velox 变长 VARBINARY 上无物理保证(不像 CH ColumnFixedString 天生定长)。
    // 若某行 size != n,getKeyHolder 的 std::string_view(sv.data(), n) 会越界
    // (< n:external 读越界;inline≤12:读进 StringView 结构体尾部垃圾)。
    // 构造时一次性扫全列校验(而非 per-row check),把约定显式化又不拖热路径
    // getKeyHolder。CH 无此 check(ColumnFixedString 天生 N 字节)。
    const vector_size_t numRows = flat->size();
    for (vector_size_t i = 0; i < numRows; ++i) {
      VELOX_CHECK_EQ(
          values[i].size(),
          n,
          "HashMethodFixedString: row {} StringView size {} != FixedStringType({}); "
          "FixedStringType 承载约定每行正好 N 字节",
          i,
          values[i].size(),
          n);
    }
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  // CH 原文 (HashMethod.h:259-270) 逐字:
  //   auto getKeyHolder(size_t row, Arena & pool) const {
  //     std::string_view key(
  //         reinterpret_cast<const char *>(&(*chars)[row * n]), n);
  //     if constexpr (place_string_to_arena)
  //         return ArenaKeyHolder{key, pool};
  //     else
  //         return key;
  //   }
  //
  // ch2: 算法(取 n 字节 slice → 包 ArenaKeyHolder)逐字不变;只有「怎么从列
  //   拿到这行 n 字节 view」这个 infra 边界换成 Velox StringView。CH 用
  //   chars+row*n 定位定长 n 字节;Velox 直接取第 row 行 StringView(约定正好
  //   n 字节),两者取到的 std::string_view 语义一致(&chars[row*n], n)。
  auto getKeyHolder(size_t row, [[maybe_unused]] ch::Arena& pool) const {
    // infra 边界:CH `&(*chars)[row*n]` 定长定位 → Velox 第 row 行 StringView。
    // StringView 约定每行正好 n 字节(FixedStringType(N) 承载),等价于
    // CH string_view(&chars[row*n], n)。
    const StringView& sv = values[row];
    std::string_view key(sv.data(), n);

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


// ============================================================================
// LowCardinalityKeys - CH HashMethod.h:272-287。照抄保留(task8 low_cardinality
// 用),has_low_cardinality=false 时是空壳。
// ============================================================================
template <bool has_low_cardinality>
struct LowCardinalityKeys {
  ColumnRawData nested_columns;
  ColumnRawData positions;
  Sizes position_sizes;
};

template <>
struct LowCardinalityKeys<false> {};

// ============================================================================
// HashMethodKeysFixed - exactly 搬自 CH src/Common/ColumnsHashing/HashMethod.h:
// 288-472。多列定长 key pack 进宽 Key(UInt128/UInt256)。
// 铁律:pack 分派算法(usePreparedKeys / prepared vs 逐行 packFixed)逐字搬 CH,
// 只换 infra 边界(列裸数据承载 + prepared_keys 承载)。
//
// 本 task 模板实例:has_nullable_keys_=false, has_low_cardinality_=false。
//   - SSSE3 shuffle 分支(packFixedShuffle)= task7:照抄保留、编译期折走(用
//     一个恒 false 的 constexpr 开关,不引入 <immintrin.h>)。
//   - low_cardinality 分支 = task8:照抄保留、has_low_cardinality=false 折走。
//   - nullable 分支:照抄保留、has_nullable_keys=false 折走。
//
// infra 边界:
//   CH 构造 `Base(key_columns)` + `getActualColumns()[i]->getRawData().data()`
//     取列裸基址;ch2 从 Velox flat 列取 rawValues() 装成 ColumnRawData 喂给
//     Base(BaseStateKeysFixed)与 pack。
//   CH `PaddedPODArray<Key> prepared_keys` -> `std::vector<Key>`。
//   getKeyHolder / usePreparedKeys / packFixedBatch 分派逐字。
// ============================================================================

// task7 SSSE3 开关:照抄保留 shuffle 分支的位置,本 task 恒关(不引入 SSSE3)。
static constexpr bool kKeysFixedUseSsse3 = false;

template <
    typename Value,
    typename Key,
    typename Mapped,
    bool has_nullable_keys_ = false,
    bool has_low_cardinality_ = false,
    bool use_cache = true,
    bool need_offset = false>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>,
      public columns_hashing_impl::HashMethodBase<
          HashMethodKeysFixed<
              Value,
              Key,
              Mapped,
              has_nullable_keys_,
              has_low_cardinality_,
              use_cache,
              need_offset>,
          Value,
          Mapped,
          use_cache,
          need_offset> {
  using Self = HashMethodKeysFixed<
      Value,
      Key,
      Mapped,
      has_nullable_keys_,
      has_low_cardinality_,
      use_cache,
      need_offset>;
  using BaseHashed = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;
  using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

  static constexpr bool has_nullable_keys = has_nullable_keys_;
  static constexpr bool has_low_cardinality = has_low_cardinality_;

  static constexpr bool has_cheap_key_calculation = true;
  static constexpr bool has_pre_computed_hashes = false;

  LowCardinalityKeys<has_low_cardinality> low_cardinality_keys;
  Sizes key_sizes;
  size_t keys_size;

  // CH: PaddedPODArray<Key> prepared_keys; -> std::vector<Key>(infra 边界)。
  std::vector<Key> prepared_keys;

  // CH 原文 (HashMethod.h:322-334) 逐字。
  static bool usePreparedKeys(const Sizes& key_sizes) {
    if (has_low_cardinality || has_nullable_keys || sizeof(Key) > 16) {
      return false;
    }

    for (auto size : key_sizes) {
      if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16) {
        return false;
      }
    }

    return true;
  }

  // CH 原文构造 (HashMethod.h:336-421)。infra 边界:key_columns 从
  // ColumnRawPtrs(Velox VectorPtr)取 flat rawValues() 装成 ColumnRawData 喂
  // Base;num_rows 显式传给 packFixedBatch。
  HashMethodKeysFixed(
      const ColumnRawPtrs& key_columns,
      const Sizes& key_sizes_,
      const HashMethodContextPtr&)
      : Base(extractColumnData(key_columns)),
        key_sizes(key_sizes_),
        keys_size(key_columns.size()),
        num_rows_(key_columns.empty() ? 0 : key_columns[0]->size()) {
    if constexpr (has_low_cardinality) {
      // CH low_cardinality 初始化(HashMethod.h:338-355)。照抄保留、task8 启用。
      VELOX_NYI("has_low_cardinality HashMethodKeysFixed is task8");
    }

    if (usePreparedKeys(key_sizes)) {
      // CH: packFixedBatch(keys_size, Base::getActualColumns(), key_sizes,
      //                    prepared_keys);
      // infra 边界:多传 num_rows(整块行数)。
      packFixedBatch<Key>(
          keys_size,
          Base::getActualColumns(),
          key_sizes,
          num_rows_,
          prepared_keys);
    } else if constexpr (kKeysFixedUseSsse3) {
      // CH SSSE3 masks / columns_data 初始化 (HashMethod.h:365-405)。task7 启用。
      // 照抄保留:恒 false 折走,不引入 <immintrin.h>。
      VELOX_UNREACHABLE("SSSE3 packFixedShuffle init is task7");
    }
  }

  // CH 原文 getKeyHolder (HashMethod.h:410-437)。pack 分派逐字。
  Key getKeyHolder(size_t row, ch::Arena&) const {
    if constexpr (has_nullable_keys) {
      // CH: auto bitmap = Base::createBitmap(row);
      //     return packFixed<Key>(row, keys_size, Base::getActualColumns(),
      //                           key_sizes, bitmap);
      // nullable 照抄保留、本 task 折走(has_nullable_keys=false)。
      VELOX_NYI("nullable getKeyHolder is not supported in task3");
    } else {
      if constexpr (has_low_cardinality) {
        // CH: return packFixed<Key, true>(row, keys_size,
        //         low_cardinality_keys.nested_columns, key_sizes,
        //         &low_cardinality_keys.positions,
        //         &low_cardinality_keys.position_sizes);
        // 照抄保留、task8 启用。
        VELOX_NYI("has_low_cardinality getKeyHolder is task8");
      }

      if (!prepared_keys.empty()) {
        return prepared_keys[row];
      }

      if constexpr (kKeysFixedUseSsse3) {
        // CH: if constexpr (sizeof(Key) <= 16)
        //       return packFixedShuffle<Key>(columns_data.get(), keys_size,
        //                                    key_sizes.data(), row, masks.get());
        // task7 照抄保留、恒 false 折走。
        VELOX_UNREACHABLE("packFixedShuffle is task7");
      }

      return packFixed<Key>(
          row, keys_size, Base::getActualColumns(), key_sizes);
    }
  }

 private:
  // infra 边界的唯一实现:把 Velox flat 定长列的 rawValues() 基址取成
  // ColumnRawData(const char* 列表),对应 CH 的
  // `getActualColumns()[i]->getRawData().data()`。flat / non-null 限制
  // VELOX_CHECK + TODO(同 task1/2)。
  static ColumnRawData extractColumnData(const ColumnRawPtrs& key_columns) {
    VELOX_CHECK(!key_columns.empty(), "HashMethodKeysFixed requires >=1 key");
    ColumnRawData data;
    data.reserve(key_columns.size());
    for (const auto& vp : key_columns) {
      const auto column = vp->loadedVector();
      VELOX_CHECK(
          column->isFlatEncoding(),
          "HashMethodKeysFixed task3 requires flat key vectors (non-flat TODO)");
      VELOX_CHECK(
          !column->mayHaveNulls(),
          "HashMethodKeysFixed task3 requires non-null keys (nullable TODO)");
      // infra 边界:取该列定长值区基址(= CH getRawDataBegin<N>() 的等价承载)。
      const char* base = rawValuesOf(column);
      data.push_back(base);
    }
    return data;
  }

  static const char* rawValuesOf(const BaseVector* column) {
    switch (column->typeKind()) {
      case TypeKind::TINYINT:
        return reinterpret_cast<const char*>(
            column->asFlatVector<int8_t>()->rawValues());
      case TypeKind::SMALLINT:
        return reinterpret_cast<const char*>(
            column->asFlatVector<int16_t>()->rawValues());
      case TypeKind::INTEGER:
        return reinterpret_cast<const char*>(
            column->asFlatVector<int32_t>()->rawValues());
      case TypeKind::BIGINT:
        return reinterpret_cast<const char*>(
            column->asFlatVector<int64_t>()->rawValues());
      default:
        VELOX_UNSUPPORTED(
            "HashMethodKeysFixed task3 supports only fixed-width integer keys");
    }
  }

  size_t num_rows_{0};
};


// ============================================================================
// hash128 + HashMethodHashed — exactly 搬自 CH ColumnsHashing/HashMethod.h:
//   hash128         : CH HashMethod.h:19-29
//   HashMethodHashed: CH HashMethod.h:474-495
// 宽/多列 key → 128 位 SipHash digest。铁律 + O6:digest 算法用 CH SipHash
// (task1 已搬进 ch2、逐字节对拍过 CH),绝不用 Velox XXH3 顶替。只有"从列取
// 第 i 行值喂进 hash"这个承载边界换 Velox。
//
// ---- O6 关键:updateHashWithValue 的字节喂法必须逐类对齐 CH ----
// CH `hash128` 靠 `IColumn::updateHashWithValue(i, hash)`(IColumn 虚方法,按
// 列类型把第 i 行值喂进 SipHash)。Velox 无此虚方法,ch2 写等价 dispatch:
//   数值列 (ColumnVector<T>::updateHashWithValue, ColumnVector.cpp:70):
//     `hash.update(data[n])` —— 喂第 n 行值的 sizeof(T) 字节。
//     ch2: flat->rawValues()[row] 取值,hash.update(value)(SipHash 的
//          `update(const T&)` 同样喂 sizeof(T) 字节,与 CH 逐字节一致)。
//   字符串列 (ColumnString::updateHashWithValue, ColumnString.cpp:834):
//     size_t size_used_in_hash = string_size + 1;
//     hash.update(&size_used_in_hash, sizeof(size_used_in_hash)); // 8 字节 size
//     hash.update(&chars[offset], string_size);                   // 原始字节
//     hash.update(UInt8(0));                                      // 尾部兼容 0
//     ch2: 从 FlatVector<StringView> 取第 row 行 sv(ptr+size),按同样三段喂:
//          size+1(size_t 8 字节)→ sv 原始字节 → UInt8(0)。逐字节对齐 CH。
// ============================================================================

// ch2 版 IColumn::updateHashWithValue 等价:按 Velox 列类型把第 row 行值喂进
// ch2::SipHash,字节喂法逐类对齐 CH(见上)。infra 边界 = 从列取值;算法(喂哪
// 些字节、喂进 SipHash)搬 CH。
inline void updateHashWithValue(
    const BaseVector* column,
    size_t row,
    SipHash& hash) {
  switch (column->typeKind()) {
    // ---- 数值列:CH ColumnVector<T>::updateHashWithValue = hash.update(data[n]) ----
    case TypeKind::TINYINT:
      hash.update(column->asFlatVector<int8_t>()->rawValues()[row]);
      return;
    case TypeKind::SMALLINT:
      hash.update(column->asFlatVector<int16_t>()->rawValues()[row]);
      return;
    case TypeKind::INTEGER:
      hash.update(column->asFlatVector<int32_t>()->rawValues()[row]);
      return;
    case TypeKind::BIGINT:
      hash.update(column->asFlatVector<int64_t>()->rawValues()[row]);
      return;
    case TypeKind::REAL:
      hash.update(column->asFlatVector<float>()->rawValues()[row]);
      return;
    case TypeKind::DOUBLE:
      hash.update(column->asFlatVector<double>()->rawValues()[row]);
      return;
    // ---- 字符串列:CH ColumnString::updateHashWithValue(size+1, bytes, 0) ----
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const StringView& sv = column->asFlatVector<StringView>()->rawValues()[row];
      const size_t string_size = sv.size();
      // CH: size_used_in_hash = string_size + 1(兼容聚合状态),喂 size_t 字节。
      const size_t size_used_in_hash = string_size + 1;
      hash.update(
          reinterpret_cast<const char*>(&size_used_in_hash),
          sizeof(size_used_in_hash));
      hash.update(sv.data(), string_size);
      // CH: 尾部兼容 0。
      hash.update(UInt8(0));
      return;
    }
    default:
      VELOX_UNSUPPORTED(
          "hash128 updateHashWithValue supports only fixed-width numeric and string keys");
  }
}

// CH 原文 (HashMethod.h:19-29) 逐字:SipHash hash; 逐列 updateHashWithValue;
// get128()。algorithm 逐字搬(SipHash + get128),只有 updateHashWithValue 的
// 取值承载换 Velox(见上)。
static inline UInt128 hash128(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs& key_columns) {
  SipHash hash;
  for (size_t j = 0; j < keys_size; ++j)
    updateHashWithValue(key_columns[j]->loadedVector(), i, hash);

  return hash.get128();
}

// ============================================================================
// HashMethodHashed — exactly 搬自 CH HashMethod.h:474-495。Key=UInt128,
// getKeyHolder = hash128(row, key_columns.size(), key_columns)。算法逐字;
// infra 边界 = key_columns 承载换 Velox VectorPtr(构造存下,getKeyHolder 逐列
// 取值喂 hash128)。
// ============================================================================
template <
    typename Value,
    typename Mapped,
    bool use_cache = true,
    bool need_offset = false>
struct HashMethodHashed : public columns_hashing_impl::HashMethodBase<
                              HashMethodHashed<Value, Mapped, use_cache, need_offset>,
                              Value,
                              Mapped,
                              use_cache,
                              need_offset> {
  using Key = UInt128;
  using Self = HashMethodHashed<Value, Mapped, use_cache, need_offset>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

  static constexpr bool has_cheap_key_calculation = false;
  static constexpr bool has_pre_computed_hashes = false;

  // CH: ColumnRawPtrs key_columns; infra 边界 = Velox VectorPtr 承载。
  ColumnRawPtrs key_columns;

  // CH 原文构造 (HashMethod.h:489-490):key_columns(std::move(key_columns_))。
  HashMethodHashed(
      ColumnRawPtrs key_columns_,
      const Sizes&,
      const HashMethodContextPtr&)
      : Base(key_columns_.empty() ? nullptr : key_columns_[0]),
        key_columns(std::move(key_columns_)) {
    // flat/non-null 限制(同 task1-3):非-flat / nullable 留 TODO。
    for (const auto& vp : key_columns) {
      const auto column = vp->loadedVector();
      VELOX_CHECK(
          column->isFlatEncoding(),
          "HashMethodHashed task4 requires flat key vectors (non-flat TODO)");
      VELOX_CHECK(
          !column->mayHaveNulls(),
          "HashMethodHashed task4 requires non-null keys (nullable TODO)");
    }
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  // CH 原文 getKeyHolder (HashMethod.h:492-495) 逐字。
  FOLLY_ALWAYS_INLINE Key getKeyHolder(size_t row, ch::Arena&) const {
    return hash128(row, key_columns.size(), key_columns);
  }
};


} // namespace facebook::velox::exec::ch2
