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
#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h" // ch::UInt128

#include <folly/Portability.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

// ============================================================================
// AggregationCommon.h — exactly 搬自 CH src/Interpreters/AggregationCommon.h 的
// packFixed / packFixedBatch **标量版**。铁律:byte 拼接算法逐字搬 CH,只换
// infra 边界。
//
// infra 边界(唯一换点):
//   CH pack 从列拿裸数据用
//     `static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<N>()`
//   —— 返回该定长列数据区首字节 `const char *`。ch2 这里换成:调用方(HashMethod)
//   预先从 Velox flat 列取好的 `rawValues()` 基址(`const char*`),按列装进
//   `std::vector<const char*>`(承载 = CH 的 ColumnRawPtrs 里"裸数据指针"那一维)。
//   packFixedBatch 的 `PaddedPODArray<Key>` out → `std::vector<Key>`。
//   拿到基址之后 —— 逐列 offset + memcpy 的 byte 拼接算法与 CH 完全一致。
//
// **只搬标量**:packFixedShuffle(SSSE3,task7)不搬;packFixed 的
//   low_cardinality 重载参数照抄保留但本 task 不走(has_low_cardinality=false)。
// ============================================================================
namespace facebook::velox::exec::ch2 {

// UInt8/16/32/64 + ColumnRawData + Sizes 定义在 ColumnsHashingImpl.h。
// UInt128(宽 16B 首级 pack)——CH 原文 packFixedBatch 首级用 UInt128,
// port 侧复用 ch::UInt128(trivially copyable,sizeof==16)。
using UInt128 = ch::UInt128;

// ----------------------------------------------------------------------------
// fillFixedBatch<T, step> / fillFixedBatch<T>(整批预 pack)——搬 CH
// AggregationCommon.h:40-75。
// ----------------------------------------------------------------------------

// CH 原文 (AggregationCommon.h:40-49) 逐字。
template <typename T, size_t step>
void fillFixedBatch(size_t num_rows, const T* source, T* dest) {
  for (size_t i = 0; i < num_rows; ++i) {
    *dest = *source;
    ++source;
    dest += step;
  }
}

/// Move keys of size T into binary blob, starting from offset.
/// It is assumed that offset is aligned to sizeof(T).
/// Example: sizeof(key) = 16, sizeof(T) = 4, offset = 8
/// out[0] : [--------****----]
/// out[1] : [--------****----]
/// ...
//
// CH 原文 (AggregationCommon.h:57-75)。infra 边界:CH 的
//   `key_columns[i]` + `getRawDataBegin<sizeof(T)>()` + `column->size()`
// 换成 ch2 预取的 `column_data[i]`(裸基址)+ 显式 `num_rows`。byte 搬运
// (fillFixedBatch<T, sizeof(Key)/sizeof(T)>)与 CH 逐字一致。
template <typename T, typename Key>
void fillFixedBatch(
    size_t keys_size,
    const ColumnRawData& column_data,
    const Sizes& key_sizes,
    size_t num_rows,
    std::vector<Key>& out,
    size_t& offset) {
  for (size_t i = 0; i < keys_size; ++i) {
    if (key_sizes[i] == sizeof(T)) {
      // CH: const auto * column = key_columns[i];
      //     size_t num_rows = column->size();
      //     out.resize_fill(num_rows);
      // infra 边界: num_rows 由调用方给(整块行数);out 是 std::vector。
      out.resize(num_rows);

      /// Note: here we violate strict aliasing.
      /// It should be ok as long as we do not refer to any value from `out`
      /// before filling.
      // infra 边界: CH 用 getRawDataBegin<sizeof(T)>() 取列裸基址;ch2 用
      // 预取的 column_data[i]。
      const char* source = column_data[i];
      T* dest = reinterpret_cast<T*>(
          reinterpret_cast<char*>(out.data()) + offset);
      fillFixedBatch<T, sizeof(Key) / sizeof(T)>(
          num_rows, reinterpret_cast<const T*>(source), dest);
      offset += sizeof(T);
    }
  }
}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all
/// the keys fit into the binary blob. Keys are placed starting from the longest
/// one.
//
// CH 原文 (AggregationCommon.h:79-88)。infra 边界:多传 num_rows,ColumnRawData
// 承载;分派顺序(128→64→32→16→8)与 CH 逐字一致。
template <typename T>
void packFixedBatch(
    size_t keys_size,
    const ColumnRawData& column_data,
    const Sizes& key_sizes,
    size_t num_rows,
    std::vector<T>& out) {
  size_t offset = 0;
  fillFixedBatch<UInt128>(keys_size, column_data, key_sizes, num_rows, out, offset);
  fillFixedBatch<UInt64>(keys_size, column_data, key_sizes, num_rows, out, offset);
  fillFixedBatch<UInt32>(keys_size, column_data, key_sizes, num_rows, out, offset);
  fillFixedBatch<UInt16>(keys_size, column_data, key_sizes, num_rows, out, offset);
  fillFixedBatch<UInt8>(keys_size, column_data, key_sizes, num_rows, out, offset);
}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all
/// the keys fit into the binary blob, they are disposed in it consecutively.
//
// CH 原文 (AggregationCommon.h:92-159)。infra 边界:CH 的
//   `static_cast<const ColumnFixedSizeHelper *>(column)->getRawDataBegin<N>() + index * N`
// 换成 ch2 预取基址 `column_data[j] + index * N`。switch/offset/memcpy 的 byte
// 拼接算法逐字一致。
//
// low_cardinality 重载参数(low_cardinality_positions / _sizes)照抄保留,本
// task has_low_cardinality=false → if constexpr 折走,不走(task8 启用)。
template <typename T, bool has_low_cardinality = false>
static T FOLLY_ALWAYS_INLINE packFixed(
    size_t i,
    size_t keys_size,
    const ColumnRawData& column_data,
    const Sizes& key_sizes,
    const ColumnRawData* low_cardinality_positions [[maybe_unused]] = nullptr,
    const Sizes* low_cardinality_sizes [[maybe_unused]] = nullptr) {
  T key{};
  char* bytes = reinterpret_cast<char*>(&key);
  size_t offset = 0;

  for (size_t j = 0; j < keys_size; ++j) {
    size_t index = i;
    // CH: const IColumn * column = key_columns[j];
    // infra 边界: 预取裸基址。
    const char* column = column_data[j];
    if constexpr (has_low_cardinality) {
      // 照抄保留(task8):low_cardinality index 解引用。本 task 不走。
      // ch2 承载待 task8 补;此处保留结构、false 折走。
      VELOX_UNREACHABLE(
          "packFixed low_cardinality path is not enabled in task3 (task8)");
      (void)low_cardinality_positions;
      (void)low_cardinality_sizes;
    }

    switch (key_sizes[j]) {
      case 1: {
        // CH: getRawDataBegin<1>() + index
        memcpy(bytes + offset, column + index, 1);
        offset += 1;
      } break;
      case 2:
        if constexpr (sizeof(T) >= 2) /// To avoid warning about memcpy exceeding object size.
        {
          memcpy(bytes + offset, column + index * 2, 2);
          offset += 2;
        }
        break;
      case 4:
        if constexpr (sizeof(T) >= 4) {
          memcpy(bytes + offset, column + index * 4, 4);
          offset += 4;
        }
        break;
      case 8:
        if constexpr (sizeof(T) >= 8) {
          memcpy(bytes + offset, column + index * 8, 8);
          offset += 8;
        }
        break;
      default:
        memcpy(bytes + offset, column + index * key_sizes[j], key_sizes[j]);
        offset += key_sizes[j];
    }
  }

  return key;
}

// 注:CH 的 nullable packFixed 重载(带 KeysNullMap bitmap)+ packFixedShuffle
// (SSSE3)本 task 不搬 —— nullable 是 aggregation/task 后续,SSSE3 是 task7。
// has_nullable_keys=false 下 HashMethodKeysFixed 不引用它们。

} // namespace facebook::velox::exec::ch2
