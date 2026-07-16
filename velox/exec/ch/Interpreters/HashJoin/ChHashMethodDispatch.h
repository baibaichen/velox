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

#include "velox/exec/ch/Common/Arena.h"

#include <cstddef>

namespace facebook::velox::exec::ch {

// ============================================================================
// ChHashMethodDispatch — task3-8 复用的最小 build/probe 驱动壳。
//
// 这是 CH `Inserter::insertOne/insertAll`(HashJoinMethods.h:25-48)与
// `HashJoinMethodsImpl.h` probe 循环(:640+ 的 `key_getter.findKey`)驱动形状
// 的 ch 最小版(路 B):对一批 row,用 `hashMethod.emplaceKey(map,row,pool)` /
// `hashMethod.findKey(map,row,pool)` 逐行驱动。设计成模板挂任意 HashMethod
// (task3-8 换 HashMethod / Map 即可复用),本 task 先让 HashMethodString 走通。
//
// 算法搬 CH:emplaceKey/findKey 遍历 + isInserted()/placement-new mapped 的
// build 逻辑逐字对着 CH insertOne/insertAll;infra(map/arena)是 Velox 承载。
// ============================================================================
struct ChHashMethodDispatch {
  // Build 驱动 —— 搬 CH Inserter::insertOne(HashJoinMethods.h:25-33)。
  // 每个 key 首次插入时 placement-new mapped_type(stored_block_no, row);
  // 重复 key 命中既有 cell(此处最小版不再追加 RowRef,留 task3-8 扩)。
  // 返回 map 里 distinct key 数(= inserted 次数)。
  template <typename HashMethod, typename Map>
  static size_t
  build(HashMethod& hashMethod, Map& map, size_t rows, ch::Arena& pool) {
    size_t inserted = 0;
    for (size_t row = 0; row < rows; ++row) {
      // CH: auto emplace_result = key_getter.emplaceKey(map, i, pool);
      auto emplaceResult = hashMethod.emplaceKey(map, row, pool);
      if (emplaceResult.isInserted()) {
        // CH: new (&emplace_result.getMapped())
        //         typename HashMap::mapped_type(stored_block_no, i);
        new (&emplaceResult.getMapped())
            typename Map::mapped_type(/*block_no=*/0, static_cast<uint32_t>(row));
        ++inserted;
      }
    }
    return inserted;
  }

  // Probe 驱动 —— 搬 CH HashJoinMethodsImpl.h 的 probe 循环(:640+):
  // 逐行 findKey,命中则回调 onFound(row, mapped)。最小 inner-all 骨架。
  template <typename HashMethod, typename Map, typename OnFound>
  static size_t probe(
      HashMethod& hashMethod,
      Map& map,
      size_t rows,
      ch::Arena& pool,
      OnFound&& onFound) {
    size_t matched = 0;
    for (size_t row = 0; row < rows; ++row) {
      // CH: auto find_result = key_getter.findKey(*map, ind, pool);
      auto findResult = hashMethod.findKey(map, row, pool);
      if (findResult.isFound()) {
        onFound(row, findResult.getMapped());
        ++matched;
      }
    }
    return matched;
  }
};

} // namespace facebook::velox::exec::ch
