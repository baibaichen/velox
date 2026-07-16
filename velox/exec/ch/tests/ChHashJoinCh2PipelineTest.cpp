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
// ch-task9a 端到端验证:走 ch 路径 (ch HashMethod 驱动坐标模型) 的 inner
// join 结果 == Velox 原生 hashJoin。覆盖 fixed(int64) / string(varchar) /
// hashed(多列 SipHash) 三类 key。
//
// 与旧路径的关系:本测试**只调 ch pipeline glue** (ChHashJoinCh2Pipeline.h) +
// 复用 ch 的 RowRefList/RetainedVectorsIndex/EmitGather (坐标输出，与旧路径同
// 一套)。ch 库源码一个字没改，旧 decoder 路径与 113 gtest 不受影响。
//
// 比对方式:ch pipeline 产出 output RowVector (build 投影 + probe 投影)，与
// Velox 原生 PlanBuilder().hashJoin(...) 的结果用 assertEqualResults 无序比对。
// ============================================================================

#include "velox/exec/ch/Interpreters/HashJoin/ChHashJoinCh2Pipeline.h"
#include "velox/exec/ch/Common/HashTable/StringHashMapAdapter.h"
#include "velox/exec/ch/DataTypes/FixedStringType.h"
#include "velox/exec/ch/Interpreters/HashJoin/LowCardinalityKeyGetterForJoin.h"
#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h"

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

namespace facebook::velox::exec::ch {
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

  // Runs the ch coordinate pipeline (build+probe+EmitGather) over one build
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

    chBuildCoordinates(
        buildMethod, map, retained, arena, kDriverNo, buildInput);

    auto hits = chProbeCoordinates(probeMethod, map, arena, probeInput);
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
  auto chOut = runCh2(
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

  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
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
  auto chOut = runCh2(
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

  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
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
  auto chOut = runCh2(
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

  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}


// ===========================================================================
// ch-task9b: 扩端到端覆盖剩余 key 类型 (ch 路径 == Velox 原生 hashJoin)。
// 每类型含重复 build key (RowRefList 链)。use_cache=false 硬约束沿用。
// ===========================================================================

// ---- 多宽度 fixed: keys128 (2x bigint) via HashMethodKeysFixed + Map128 ------
TEST_F(ChHashJoinCh2PipelineTest, keys128TwoBigintMatchesNative) {
  auto buildInput = makeRowVector(
      {"b_k1", "b_k2", "b_val"},
      {makeFlatVector<int64_t>({10, 20, 20, 30, 10}),
       makeFlatVector<int64_t>({100, 200, 200, 300, 100}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  auto probeInput = makeRowVector(
      {"p_k1", "p_k2", "p_val"},
      {makeFlatVector<int64_t>({20, 10, 30, 40, 10}),
       makeFlatVector<int64_t>({200, 100, 300, 400, 100}),
       makeFlatVector<int64_t>({11, 22, 33, 44, 55})});

  using Map128 = ch::HashMapAll_keys128;
  Map128 map(pool());
  using Method = HashMethodKeysFixed<
      Map128::value_type,
      ch::UInt128,
      ch::RowRefList,
      /*has_nullable_keys_=*/false,
      /*has_low_cardinality_=*/false,
      /*use_cache=*/false>;
  Sizes sizes{8, 8};
  ColumnRawPtrs buildKeys{buildInput->childAt(0), buildInput->childAt(1)};
  ColumnRawPtrs probeKeys{probeInput->childAt(0), probeInput->childAt(1)};
  Method buildMethod(buildKeys, sizes, nullptr);
  Method probeMethod(probeKeys, sizes, nullptr);

  auto outputType = ROW(
      {"b_k1", "b_k2", "b_val", "p_k1", "p_k2", "p_val"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto chOut = runCh2(
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
  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}

// ---- 多宽度 fixed: keys256 (3x bigint, 逐行 packFixed) via Map256 -----------
TEST_F(ChHashJoinCh2PipelineTest, keys256ThreeBigintMatchesNative) {
  auto buildInput = makeRowVector(
      {"b_k1", "b_k2", "b_k3", "b_val"},
      {makeFlatVector<int64_t>({1, 2, 1, 3, 2}),
       makeFlatVector<int64_t>({11, 22, 11, 33, 22}),
       makeFlatVector<int64_t>({111, 222, 111, 333, 222}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  auto probeInput = makeRowVector(
      {"p_k1", "p_k2", "p_k3", "p_val"},
      {makeFlatVector<int64_t>({2, 1, 3, 9, 2}),
       makeFlatVector<int64_t>({22, 11, 33, 99, 22}),
       makeFlatVector<int64_t>({222, 111, 333, 999, 222}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  using Map256 = ch::HashMapAll_keys256;
  Map256 map(pool());
  using Method = HashMethodKeysFixed<
      Map256::value_type,
      ch::UInt256,
      ch::RowRefList,
      /*has_nullable_keys_=*/false,
      /*has_low_cardinality_=*/false,
      /*use_cache=*/false>;
  Sizes sizes{8, 8, 8};
  ColumnRawPtrs buildKeys{
      buildInput->childAt(0), buildInput->childAt(1), buildInput->childAt(2)};
  ColumnRawPtrs probeKeys{
      probeInput->childAt(0), probeInput->childAt(1), probeInput->childAt(2)};
  Method buildMethod(buildKeys, sizes, nullptr);
  Method probeMethod(probeKeys, sizes, nullptr);

  auto outputType = ROW(
      {"b_k1", "b_k2", "b_k3", "b_val", "p_k1", "p_k2", "p_k3", "p_val"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
       BIGINT()});
  auto chOut = runCh2(
      buildMethod, probeMethod, map, buildInput, probeInput, {0, 1, 2, 3},
      {0, 1, 2, 3}, outputType);

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto nativePlan =
      PlanBuilder(idGen)
          .values({probeInput})
          .hashJoin(
              {"p_k1", "p_k2", "p_k3"},
              {"b_k1", "b_k2", "b_k3"},
              PlanBuilder(idGen).values({buildInput}).planNode(),
              "",
              {"b_k1", "b_k2", "b_k3", "b_val", "p_k1", "p_k2", "p_k3",
               "p_val"})
          .planNode();
  auto expected = AssertQueryBuilder(nativePlan).copyResults(pool());
  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}

// ---- O2 InRange: 密集 int64 key 走 range 优化 (平移 key 存 range map) --------
// range map 坐标承载:平移后 key ∈ [0,range_size) 存进 HashMapAll_key64
// (mapped=RowRefList),与普通 fixed 同一套坐标插入。范围外 probe key miss。
TEST_F(ChHashJoinCh2PipelineTest, inRangeMatchesNative) {
  // build 密集 key 1000..1006 + 重复 (1002 出现两次 -> RowRefList 链)。
  auto buildInput = makeRowVector(
      {"b_key", "b_val"},
      {makeFlatVector<int64_t>({1000, 1001, 1002, 1002, 1004, 1006}),
       makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})});
  // probe: 命中 (1002,1004,1006,1000) + 范围外 (999 = min-1, 1007 = max+1)。
  auto probeInput = makeRowVector(
      {"p_key", "p_val"},
      {makeFlatVector<int64_t>({1002, 1004, 999, 1006, 1007, 1000}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60})});

  using Map = ch::HashMapAll_key64;
  Map map(pool());
  using Method = HashMethodOneNumberInRange<
      Map::value_type,
      ch::RowRefList,
      int64_t,
      /*use_cache=*/false>;
  auto buildKeyCol = buildInput->childAt(0);
  auto probeKeyCol = probeInput->childAt(0);
  Method buildMethod({buildKeyCol}, {}, nullptr);
  Method probeMethod({probeKeyCol}, {}, nullptr);

  // 值域接入 infra 边界:扫 build key 列算 min/max -> min_key/range_size。
  auto range = computeKeyRange<int64_t>(buildKeyCol);
  ASSERT_TRUE(range.has_value());
  buildMethod.min_key = range->min_key;
  buildMethod.range_size = range->range_size;
  // probe 用同一 range (CH: build 侧算好的 min_key/range_size 复用到 probe)。
  probeMethod.min_key = range->min_key;
  probeMethod.range_size = range->range_size;

  auto outputType = ROW(
      {"b_key", "b_val", "p_key", "p_val"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto chOut = runCh2(
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
  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}

// ---- O3 FixedString: FixedStringType(N) 定长 key 端到端 == 原生 -------------
// build/probe key 列是 FixedStringType(16) 承载 (每行正好 N 字节)。map 存
// StringRef key。输出投影 key 列用 VARBINARY (FixedStringType 物理承载),原生
// 侧同样喂 FixedStringType 列 -> 值相等。
TEST_F(ChHashJoinCh2PipelineTest, fixedStringMatchesNative) {
  constexpr uint32_t kN = 16;
  auto pad = [&](const std::vector<std::string>& in) {
    std::vector<std::string> out;
    for (auto k : in) {
      k.resize(kN, char(0));
      out.push_back(std::move(k));
    }
    return out;
  };
  auto makeFixed = [&](const std::vector<std::string>& padded) {
    return makeFlatVector<StringView>(
        padded.size(),
        [&, padded](auto row) { return StringView(padded[row]); },
        nullptr,
        FIXED_STRING(kN));
  };
  auto bPad = pad({"alpha", "beta", "beta", "gamma", "alpha"});
  auto pPad = pad({"beta", "gamma", "delta", "alpha", "beta"});
  auto buildInput = makeRowVector(
      {"b_key", "b_val"},
      {makeFixed(bPad), makeFlatVector<int64_t>({1, 2, 3, 4, 5})});
  auto probeInput = makeRowVector(
      {"p_key", "p_val"},
      {makeFixed(pPad), makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  StringHashMapAdapter map(pool());
  using Method = HashMethodFixedString<
      StringHashMapAdapter::value_type,
      ch::RowRefList,
      /*place_string_to_arena=*/true,
      /*use_cache=*/false>;
  Method buildMethod({buildInput->childAt(0)}, {}, nullptr);
  Method probeMethod({probeInput->childAt(0)}, {}, nullptr);

  auto outputType = ROW(
      {"b_key", "b_val", "p_key", "p_val"},
      {FIXED_STRING(kN), BIGINT(), FIXED_STRING(kN), BIGINT()});
  auto chOut = runCh2(
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
  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}

// ---- O4 低基数: DictionaryVector probe 走 LowCardinalityKeyGetter -----------
// build/probe key 列都是 DictionaryVector<StringView> (base distinct 值 +
// indices)。getter isLowCardinality -> visit_cache 块内去重出坐标。map 存 base
// 字符串 key。原生侧喂同一 dict 列 (Velox 解码后逻辑值相等) -> == 原生。
TEST_F(ChHashJoinCh2PipelineTest, lowCardinalityMatchesNative) {
  const std::vector<std::string> base{
      "alpha_long_dictionary_value_0001",
      "bravo_long_dictionary_value_0002",
      "charlie_long_dictionary_value_03",
      "delta_long_dictionary_value_0004",
  };
  auto makeDict = [&](const std::vector<vector_size_t>& indices) {
    auto baseVector = makeFlatVector<StringView>(
        base.size(), [&](auto row) { return StringView(base[row]); });
    auto indexBuffer =
        AlignedBuffer::allocate<vector_size_t>(indices.size(), pool());
    auto* raw = indexBuffer->asMutable<vector_size_t>();
    for (size_t i = 0; i < indices.size(); ++i) {
      raw[i] = indices[i];
    }
    return BaseVector::wrapInDictionary(
        nullptr, indexBuffer, indices.size(), baseVector);
  };

  // build indices: 含重复 (0 出现两次 -> RowRefList 链)。dict 覆盖 {0,1,2}。
  std::vector<vector_size_t> bIdx{0, 1, 2, 0, 1};
  // probe indices: 循环 {0,1,2,3},3 不在 build (miss);块内重复触发 visit_cache。
  std::vector<vector_size_t> pIdx;
  for (int i = 0; i < 12; ++i) {
    pIdx.push_back(i % 4);
  }
  auto buildKey = makeDict(bIdx);
  auto probeKey = makeDict(pIdx);
  auto buildInput = makeRowVector(
      {"b_key", "b_val"},
      {buildKey,
       makeFlatVector<int64_t>(
           std::vector<int64_t>{1, 2, 3, 4, 5})});
  std::vector<int64_t> pvals(pIdx.size());
  for (size_t i = 0; i < pvals.size(); ++i) {
    pvals[i] = static_cast<int64_t>(i * 10);
  }
  auto probeInput = makeRowVector(
      {"p_key", "p_val"}, {probeKey, makeFlatVector<int64_t>(pvals)});

  using Map = StringHashMapAdapter;
  using BaseMethod = HashMethodString<
      Map::value_type,
      ch::RowRefList,
      /*place_string_to_arena=*/true,
      /*use_cache=*/false>;
  using LowCardGetter =
      LowCardinalityKeyGetterForJoin<BaseMethod, ch::RowRefList>;
  Map map(pool());
  LowCardGetter buildMethod({buildInput->childAt(0)}, {}, nullptr);
  LowCardGetter probeMethod({probeInput->childAt(0)}, {}, nullptr);
  ASSERT_TRUE(buildMethod.isLowCardinality());
  ASSERT_TRUE(probeMethod.isLowCardinality());

  auto outputType = ROW(
      {"b_key", "b_val", "p_key", "p_val"},
      {VARCHAR(), BIGINT(), VARCHAR(), BIGINT()});
  auto chOut = runCh2(
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
  EXPECT_TRUE(exec::test::assertEqualResults({chOut}, {expected}));
}

} // namespace
} // namespace facebook::velox::exec::ch
