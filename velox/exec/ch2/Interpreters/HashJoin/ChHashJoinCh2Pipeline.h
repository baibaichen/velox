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

// ============================================================================
// ch2-task9a: ch2 HashMethod 首次接坐标模型 + pipeline (验证 spike)。
//
// 这条是 task9 的 "ch2 路径"，与现有 ch decoder 路径 (ChHashBuild/ChHashProbe)
// 并存、独立、opt-in——ch 库一个字不改，默认永远走旧路径，只有显式调用本文件
// 的函数才走 ch2 HashMethod 驱动。9a 只加不删不切不改名。
//
// 咬合点 (task9a 真难点):让 ch2 的 emplaceKey/findKey 驱动**在坐标模型上跑**。
//   - mapped 承载:复用现有 ch 坐标 map 类型 `ch::HashMapAll_*` (cell mapped =
//     ch::RowRefList)。ch2 HashMethod 的 `Data` 模板挂 ch::HashMapAll_key64 /
//     _key_string / _hashed，`Mapped` = ch::RowRefList——ch2 base 的
//     emplaceImpl 在插入时 `new (&it->getMapped()) RowRefList()` 默认构造空
//     RowRefList (word=0)，正是坐标字的空列表。ch2 换 mapped 类型即可，
//     无需改 ch2 HashMethod 的 key 处理算法 (铁律:算法不动)。
//   - build 插坐标:ch2 emplaceKey 拿到 cell 后，对 cell 的 RowRefList 做
//     `getMapped().insert(RowRef(blockNo,rowNo).encode(), arena)` (对齐现有
//     ch build 的坐标插入 ChHashBuild.cpp:108+)。**注意不再 placement-new
//     mapped** (ChHashMethodDispatch::build 的普通-mapped 版会 placement-new，
//     那是 aggregation 语义；坐标模型下 base 已默认构造好 RowRefList，直接
//     insert)。
//   - probe 出坐标:ch2 findKey 命中后拿 `getMapped()` (RowRefList&)，收成
//     ch::ProbeHit{probeRow, &rowRefList}，喂现有 ch::listJoinResults +
//     ch::EmitGather (坐标输出，与旧路径同一套)。
//
// use_cache=false 硬约束 (坐标模型正确性关键):
//   ch2 base 的 consecutive_keys_optimization (use_cache=true) 在 emplace 时
//   `cache.value.second = it->getMapped()` **按值拷贝** mapped，且 cache 命中
//   时返回 `cache.value.second` (拷贝) 而非 cell 引用。若对该拷贝 insert 坐标，
//   真 cell 的 RowRefList 不会收到 RowRef → 丢坐标。故坐标模型下 ch2 HashMethod
//   必须实例化为 use_cache=false，让 getMapped() 恒返回真 cell 引用。
// ============================================================================

#include "velox/exec/ch/EmitGather.h"
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/exec/ch/Interpreters/RowRefList.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"
#include "velox/exec/ch2/Common/ColumnsHashing/HashMethod.h"

#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/exec/ch/ChHashProbe.h"

#include <memory>
#include <vector>

namespace facebook::velox::exec::ch2 {

// ch2 坐标 build:用 ch2 HashMethod 的 emplaceKey 驱动，往 ch 坐标 map
// (mapped=ch::RowRefList) 插坐标 RowRef。coordinateMap/retained/arena 由调用方
// 持有 (对齐 ch ChHashBuild 的 storage_)。
//
// 逐字对齐现有 ch build 坐标插入 (ChHashBuild.cpp addInput):每个 build batch
//   retained.add(input) 拿 batchNo → packBlockNo(driverNo, batchNo) → 逐行
//   emplaceKey 拿 cell → cell.getMapped().insert(RowRef(blockNo,row).encode()).
template <typename HashMethod, typename CoordinateMap>
void ch2BuildCoordinates(
    HashMethod& hashMethod,
    CoordinateMap& coordinateMap,
    ch::RetainedVectorsIndex& retained,
    ch::Arena& arena,
    uint32_t driverNo,
    const RowVectorPtr& input) {
  static_assert(
      std::is_same_v<typename CoordinateMap::mapped_type, ch::RowRefList>,
      "ch2 coordinate build requires a ch::RowRefList-mapped map");
  const size_t rows = input->size();
  const uint32_t batchNo = retained.add(input);
  const uint32_t blockNo = ch::packBlockNo(driverNo, batchNo);
  for (size_t row = 0; row < rows; ++row) {
    // CH: auto emplace_result = key_getter.emplaceKey(map, i, pool);
    // ch2 base 插入时已 `new (&getMapped()) RowRefList()` 默认构造空列表。
    auto emplaceResult = hashMethod.emplaceKey(coordinateMap, row, arena);
    // 坐标插入 (对齐 ch ChHashBuild):不 placement-new，直接往 cell 的
    // RowRefList 追加坐标 RowRef。重复 key 命中既有 cell → RowRefList 追加。
    emplaceResult.getMapped().insert(
        ch::RowRef(blockNo, static_cast<uint32_t>(row)).encode(), arena);
  }
}

// ch2 坐标 probe:用 ch2 HashMethod 的 findKey 驱动，命中后拿 RowRefList& 收成
// ch::ProbeHit (对齐 ch joinProbe 的 hit 收集)。返回的 ProbeHit 直接喂
// ch::listJoinResults + ch::EmitGather。
template <typename HashMethod, typename CoordinateMap>
std::vector<ch::ProbeHit> ch2ProbeCoordinates(
    HashMethod& hashMethod,
    CoordinateMap& coordinateMap,
    ch::Arena& arena,
    const RowVectorPtr& probe) {
  std::vector<ch::ProbeHit> hits;
  const size_t rows = probe->size();
  hits.reserve(rows);
  for (size_t row = 0; row < rows; ++row) {
    // CH: auto find_result = key_getter.findKey(*map, ind, pool);
    auto findResult = hashMethod.findKey(coordinateMap, row, arena);
    if (findResult.isFound()) {
      hits.push_back(
          {static_cast<vector_size_t>(row), &findResult.getMapped()});
    }
  }
  return hits;
}

} // namespace facebook::velox::exec::ch2
