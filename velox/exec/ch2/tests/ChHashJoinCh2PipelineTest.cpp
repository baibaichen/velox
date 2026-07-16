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

// ============================================================================
// ch2-task9a 端到端验证:走 ch2 路径 (ch2 HashMethod 驱动坐标模型) 的 inner
// join 结果 == Velox 原生 hashJoin。覆盖 fixed(int64) / string(varchar) /
// hashed(多列 SipHash) 三类 key。
//
// 与旧路径的关系:本测试**只调 ch2 pipeline glue** (ChHashJoinCh2Pipeline.h) +
// 复用 ch 的 RowRefList/RetainedVectorsIndex/EmitGather (坐标输出，与旧路径同
// 一套)。ch 库源码一个字没改，旧 decoder 路径与 113 gtest 不受影响。
//
// 比对方式:ch2 pipeline 产出 output RowVector (build 投影 + probe 投影)，与
// Velox 原生 PlanBuilder().hashJoin(...) 的结果用 assertEqualResults 无序比对。
// ============================================================================

#include "velox/exec/ch2/Interpreters/HashJoin/ChHashJoinCh2Pipeline.h"
#include "velox/exec/ch2/Common/HashTable/StringHashMapAdapter.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/exec/ch/EmitGather.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using exec::test::AssertQueryBuilder;
using exec::test::PlanBuilder;

class ChHashJoinCh2PipelineTest : public testing::Test,
                                  public velox::test::VectorTestBase {
 protected:
  static constexpr uint32_t kDriverNo = 0;

  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  // Runs the ch2 coordinate pipeline (build+probe+EmitGather) over one build
  // batch and one probe batch, returning the concatenated output. Output
  // columns are [buildProjections..., probeProjections...].
  template <typename HashMethod, typename CoordinateMap>
  RowVectorPtr runCh2(
      HashMethod& buildMethod,
      HashMethod& probeMethod,
      CoordinateMap& map,
      const RowVectorPtr& buildInput,
      const RowVectorPtr& probeInput,
      const std::vector<column_index_t>& buildProjections,
      const std::vector<column_index_t>& probeProjections,
      const RowTypePtr& outputType) {
    ch::Arena arena(pool());
    ch::RetainedVectorsIndex retained(kDriverNo);

    ch2BuildCoordinates(
        buildMethod, map, retained, arena, kDriverNo, buildInput);

    auto hits = ch2ProbeCoordinates(probeMethod, map, arena, probeInput);
    auto matches = ch::listJoinResults(hits, retained);

    ch::EmitGather gather(
        retained, buildProjections, probeProjections, outputType, pool());
    auto batches = gather.emit(matches, probeInput);

    // Concatenate the per-build-block batches into one RowVector, copying to
    // flat so the dictionary views over retained vectors are self-contained.
    std::vector<RowVectorPtr> flatBatches;
    for (const auto& b : batches) {
      flatBatches.push_back(
          std::dynamic_pointer_cast<RowVector>(BaseVector::copy(*b, pool())));
    }
    if (flatBatches.empty()) {
      return std::dynamic_pointer_cast<RowVector>(
          BaseVector::create(outputType, 0, pool()));
    }
    // Simple vertical concat.
    vector_size_t total = 0;
    for (const auto& b : flatBatches) {
      total += b->size();
    }
    auto out = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(outputType, total, pool()));
    vector_size_t offset = 0;
    for (const auto& b : flatBatches) {
      for (column_index_t c = 0; c < outputType->size(); ++c) {
        out->childAt(c)->copy(b->childAt(c).get(), offset, 0, b->size());
      }
      offset += b->size();
    }
    return out;
  }
};

// ---- fixed key: single int64 key via HashMethodOneNumber + HashMapAll_key64 --
TEST_F(ChHashJoinCh2PipelineTest, fixedInt64KeyMatchesNative) {
  auto buildInput = makeRowVector(
      {"b_key", "b_val"},
      {makeFlatVector<int64_t>({1, 2, 2, 3, 5}),
       makeFlatVector<int64_t>({100, 200, 201, 300, 500})});
  auto probeInput = makeRowVector(
      {"p_key", "p_val"},
      {makeFlatVector<int64_t>({2, 3, 4, 1, 2}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  using Cell = ch::HashMapAll_key64::cell_type;
  (void)sizeof(Cell);
  ch::HashMapAll_key64 map(pool());
  // use_cache=false (坐标模型正确性硬约束，见 glue 头注释)。
  using Method = HashMethodOneNumber<
      ch::HashMapAll_key64::value_type,
      ch::RowRefList,
      int64_t,
      /*use_cache=*/false>;
  auto buildKeyCol = buildInput->childAt(0);
  auto probeKeyCol = probeInput->childAt(0);
  Method buildMethod({buildKeyCol}, {}, nullptr);
  Method probeMethod({probeKeyCol}, {}, nullptr);

  auto outputType = ROW(
      {"b_key", "b_val", "p_key", "p_val"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto ch2Out = runCh2(
      buildMethod, probeMethod, map, buildInput, probeInput, {0, 1}, {0, 1},
      outputType);

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto nativePlan =
      PlanBuilder(idGen)
          .values({probeInput})
          .hashJoin(
              {"p_key"},
              {"b_key"},
              PlanBuilder(idGen).values({buildInput}).planNode(),
              "",
              {"b_key", "b_val", "p_key", "p_val"})
          .planNode();
  auto expected = AssertQueryBuilder(nativePlan).copyResults(pool());

  EXPECT_TRUE(exec::test::assertEqualResults({ch2Out}, {expected}));
}

// ---- string key: varchar key via HashMethodString + HashMapAll_key_string ----
TEST_F(ChHashJoinCh2PipelineTest, stringKeyMatchesNative) {
  auto buildInput = makeRowVector(
      {"b_key", "b_val"},
      {makeFlatVector<std::string>({"apple", "banana", "banana", "cherry"}),
       makeFlatVector<int64_t>({1, 2, 3, 4})});
  auto probeInput = makeRowVector(
      {"p_key", "p_val"},
      {makeFlatVector<std::string>({"banana", "cherry", "date", "apple"}),
       makeFlatVector<int64_t>({10, 20, 30, 40})});

  StringHashMapAdapter map(pool());
  using Method = HashMethodString<
      StringHashMapAdapter::value_type,
      ch::RowRefList,
      /*place_string_to_arena=*/true,
      /*use_cache=*/false>;
  auto buildKeyCol = buildInput->childAt(0);
  auto probeKeyCol = probeInput->childAt(0);
  Method buildMethod({buildKeyCol}, {}, nullptr);
  Method probeMethod({probeKeyCol}, {}, nullptr);

  auto outputType = ROW(
      {"b_key", "b_val", "p_key", "p_val"},
      {VARCHAR(), BIGINT(), VARCHAR(), BIGINT()});
  auto ch2Out = runCh2(
      buildMethod, probeMethod, map, buildInput, probeInput, {0, 1}, {0, 1},
      outputType);

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto nativePlan =
      PlanBuilder(idGen)
          .values({probeInput})
          .hashJoin(
              {"p_key"},
              {"b_key"},
              PlanBuilder(idGen).values({buildInput}).planNode(),
              "",
              {"b_key", "b_val", "p_key", "p_val"})
          .planNode();
  auto expected = AssertQueryBuilder(nativePlan).copyResults(pool());

  EXPECT_TRUE(exec::test::assertEqualResults({ch2Out}, {expected}));
}

// ---- hashed key: two int64 keys via HashMethodHashed + HashMapAll_hashed ------
TEST_F(ChHashJoinCh2PipelineTest, hashedMultiKeyMatchesNative) {
  auto buildInput = makeRowVector(
      {"b_k1", "b_k2", "b_val"},
      {makeFlatVector<int64_t>({1, 1, 2, 2, 3}),
       makeFlatVector<int64_t>({7, 8, 7, 7, 9}),
       makeFlatVector<int64_t>({100, 101, 200, 201, 300})});
  auto probeInput = makeRowVector(
      {"p_k1", "p_k2", "p_val"},
      {makeFlatVector<int64_t>({2, 1, 1, 3, 4}),
       makeFlatVector<int64_t>({7, 8, 9, 9, 4}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  ch::HashMapAll_hashed map(pool());
  using Method = HashMethodHashed<
      ch::HashMapAll_hashed::value_type,
      ch::RowRefList,
      /*use_cache=*/false>;
  ColumnRawPtrs buildKeys{buildInput->childAt(0), buildInput->childAt(1)};
  ColumnRawPtrs probeKeys{probeInput->childAt(0), probeInput->childAt(1)};
  Method buildMethod(buildKeys, {}, nullptr);
  Method probeMethod(probeKeys, {}, nullptr);

  auto outputType = ROW(
      {"b_k1", "b_k2", "b_val", "p_k1", "p_k2", "p_val"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto ch2Out = runCh2(
      buildMethod, probeMethod, map, buildInput, probeInput, {0, 1, 2},
      {0, 1, 2}, outputType);

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto nativePlan =
      PlanBuilder(idGen)
          .values({probeInput})
          .hashJoin(
              {"p_k1", "p_k2"},
              {"b_k1", "b_k2"},
              PlanBuilder(idGen).values({buildInput}).planNode(),
              "",
              {"b_k1", "b_k2", "b_val", "p_k1", "p_k2", "p_val"})
          .planNode();
  auto expected = AssertQueryBuilder(nativePlan).copyResults(pool());

  EXPECT_TRUE(exec::test::assertEqualResults({ch2Out}, {expected}));
}

} // namespace
} // namespace facebook::velox::exec::ch2
