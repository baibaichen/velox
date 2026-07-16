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

#include "velox/exec/ch/Common/ColumnsHashing/HashMethod.h"
#include "velox/exec/ch/Common/HashTable/StringHashMapAdapter.h"
#include "velox/exec/ch/Interpreters/HashJoin/ChHashMethodDispatch.h"
#include "velox/exec/ch/Interpreters/HashJoin/LowCardinalityKeyGetterForJoin.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

using Map = StringHashMapAdapter;
// base method = HashMethodString on the dictionary's nested (base) VARCHAR
// vector。use_cache=false 同 HashMethodStringTest(StringRef 无 string_view
// 比较运算符)。
using BaseMethod = HashMethodString<
    Map::value_type,
    Map::mapped_type,
    /*place_string_to_arena=*/true,
    /*use_cache=*/false>;
using LowCardGetter =
    LowCardinalityKeyGetterForJoin<BaseMethod, Map::mapped_type>;

class LowCardinalityKeyGetterTest : public testing::Test,
                                    public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    mapPool_ = memory::memoryManager()->addLeafPool("ch-lc-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch-lc-arena");
  }

  // 建一个 DictionaryVector<StringView>:base(distinct 值)+ indices(row→index)。
  VectorPtr makeDictString(
      const std::vector<std::string>& baseValues,
      const std::vector<vector_size_t>& indices) {
    auto baseVector = makeFlatVector<StringView>(
        baseValues.size(),
        [&](auto row) { return StringView(baseValues[row]); });
    auto indexBuffer =
        AlignedBuffer::allocate<vector_size_t>(indices.size(), pool());
    auto* raw = indexBuffer->asMutable<vector_size_t>();
    for (size_t i = 0; i < indices.size(); ++i) {
      raw[i] = indices[i];
    }
    return BaseVector::wrapInDictionary(
        nullptr, indexBuffer, indices.size(), baseVector);
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

// ---------------------------------------------------------------------------
// O4(a) 核心:块内去重生效验证 + 命中集正确。
// 8 个 distinct base 值,但 probe 有 40 行,只用 index {0,1,2,3}(4 个 unique)。
// build 侧含 base index 0,1,2(命中),3 不在 build(miss)。
// 去重生效证明:getter.find_calls == 4(= 命中过的 unique dict index 数),
// 远小于 40 行 —— 同一 dict index 第二次起走 mapped_cache,零表查。
// ---------------------------------------------------------------------------
TEST_F(LowCardinalityKeyGetterTest, blockLocalDedupAndHitSet) {
  const std::vector<std::string> base{
      "alpha_long_dictionary_value_0001",
      "bravo_long_dictionary_value_0002",
      "charlie_long_dictionary_value_03",
      "delta_long_dictionary_value_0004",
      "echo", // short inline
      "fox",
      "golf",
      "hotel",
  };

  // Build 侧:插 "alpha","bravo","charlie" 三个 key(base index 0/1/2)。
  auto buildVector = makeFlatVector<StringView>(std::vector<StringView>{
      StringView(base[0]), StringView(base[1]), StringView(base[2])});
  BaseMethod buildMethod({buildVector}, {}, nullptr);
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());
  size_t inserted =
      ChHashMethodDispatch::build(buildMethod, map, 3, arena);
  EXPECT_EQ(inserted, 3);
  EXPECT_EQ(map.size(), 3);

  // Probe 侧:40 行,dict index 只用 {0,1,2,3}(4 个 unique),循环填充。
  std::vector<vector_size_t> indices;
  for (int i = 0; i < 40; ++i) {
    indices.push_back(i % 4); // 0,1,2,3,0,1,2,3,...
  }
  auto dictProbe = makeDictString(base, indices);

  LowCardGetter getter({dictProbe}, {}, nullptr);
  ASSERT_TRUE(getter.isLowCardinality());

  size_t found = 0;
  for (size_t row = 0; row < indices.size(); ++row) {
    auto fr = getter.findKey(map, row, arena);
    if (fr.isFound()) {
      ++found;
    }
    // 命中集正确:index 0/1/2 命中,index 3 miss。
    if (indices[row] == 3) {
      EXPECT_FALSE(fr.isFound()) << "row " << row;
    } else {
      EXPECT_TRUE(fr.isFound()) << "row " << row;
    }
  }

  // 命中集正确:index 0,1,2 各出现 10 次(40/4)= 30 命中,index 3 = 10 miss。
  EXPECT_EQ(found, 30);

  // **块内去重生效**(核心):真正 data.find 次数 == 4(= unique dict index 数
  // {0,1,2,3}),远小于 40 行。同一 dict index 第二次起走 mapped_cache,零表查。
  // 若没去重(每行都查表),find_calls 会是 40。
  EXPECT_EQ(getter.find_calls, 4u);
}

// ---------------------------------------------------------------------------
// visit_cache 缓存的是同一批 index 的稳定结果:重复 miss 的 index 也只查一次表
// (三态:2=没找到 也缓存)。
// ---------------------------------------------------------------------------
TEST_F(LowCardinalityKeyGetterTest, missIndexAlsoDedupedViaVisitCacheState2) {
  const std::vector<std::string> base{
      "present_key_long_value_aaaaaaaaaa",
      "absent_key_long_value_bbbbbbbbbbb",
  };
  // Build 只插 base[0]。
  auto buildVector = makeFlatVector<StringView>(
      std::vector<StringView>{StringView(base[0])});
  BaseMethod buildMethod({buildVector}, {}, nullptr);
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());
  ChHashMethodDispatch::build(buildMethod, map, 1, arena);

  // Probe:index 1(absent)重复 20 次 —— 全 miss。
  std::vector<vector_size_t> indices(20, 1);
  auto dictProbe = makeDictString(base, indices);
  LowCardGetter getter({dictProbe}, {}, nullptr);

  size_t found = 0;
  for (size_t row = 0; row < indices.size(); ++row) {
    if (getter.findKey(map, row, arena).isFound()) {
      ++found;
    }
  }
  EXPECT_EQ(found, 0);
  // miss 也只查一次表(visit_cache[1] 被置为 2,后续走缓存)。
  EXPECT_EQ(getter.find_calls, 1u);
}

// ---------------------------------------------------------------------------
// plain 列回退:probe key 是 plain(非 DictionaryVector)FlatVector。
// isLowCardinality()==false,base method 直接跑,无去重、无 dict 间接。
// 命中集正确(map 存 key 值,plain probe 与 dict build 兼容)。
// ---------------------------------------------------------------------------
TEST_F(LowCardinalityKeyGetterTest, plainColumnFallbackNoDedup) {
  // Build 侧:dict-encoded(base index 0,1)。
  const std::vector<std::string> base{
      "shared_key_long_value_1111111111",
      "shared_key_long_value_2222222222",
  };
  auto buildDict = makeDictString(base, {0, 1});
  LowCardGetter buildGetter({buildDict}, {}, nullptr);
  ASSERT_TRUE(buildGetter.isLowCardinality());
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());
  for (size_t row = 0; row < 2; ++row) {
    auto er = buildGetter.emplaceKey(map, row, arena);
    if (er.isInserted()) {
      new (&er.getMapped()) Map::mapped_type(0, static_cast<uint32_t>(row));
    }
  }
  EXPECT_EQ(map.size(), 2);

  // Probe 侧:plain FlatVector<StringView>(非 dict)。
  auto plainProbe = makeFlatVector<StringView>(std::vector<StringView>{
      StringView(base[0]),
      StringView("not_present_long_value_zzzzzzzz"),
      StringView(base[1]),
      StringView(base[0]), // 重复,plain 路径不去重
  });
  LowCardGetter getter({plainProbe}, {}, nullptr);
  // plain 列:positions null → isLowCardinality false。
  EXPECT_FALSE(getter.isLowCardinality());

  EXPECT_TRUE(getter.findKey(map, 0, arena).isFound());
  EXPECT_FALSE(getter.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(getter.findKey(map, 2, arena).isFound());
  EXPECT_TRUE(getter.findKey(map, 3, arena).isFound());

  // plain 路径不走 visit_cache,find_calls 恒 0(去重钩子只在 dict 路径 +1)。
  EXPECT_EQ(getter.find_calls, 0u);
}

// ---------------------------------------------------------------------------
// O4(b) saved_hash stub:saved_hash 恒 nullptr → 走"现算 hash"回退(data.find
// (key) / data.emplace 里现算),结果正确(只是没跨-block 优化)。
// 验证 stub 钩子留好、且是回退而非顶替(saved_hash 指针恒 null)。
// ---------------------------------------------------------------------------
TEST_F(LowCardinalityKeyGetterTest, savedHashStubIsNullptrCurrentHashFallback) {
  const std::vector<std::string> base{
      "stub_key_long_value_alpha_000000",
      "stub_key_long_value_beta_0000000",
  };
  auto dictProbe = makeDictString(base, {0, 1, 0, 1});
  LowCardGetter getter({dictProbe}, {}, nullptr);

  // O4(b) stub 断言:saved_hash 恒 nullptr(现算 hash 回退,非 Velox hashAll
  // 顶替)。这是 task10 的钩子落点。
  EXPECT_EQ(getter.saved_hash, nullptr);

  // 走现算 hash 分支仍能正确 build + find。
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());
  for (size_t row = 0; row < 2; ++row) {
    auto er = getter.emplaceKey(map, row, arena);
    if (er.isInserted()) {
      new (&er.getMapped()) Map::mapped_type(0, static_cast<uint32_t>(row));
    }
  }
  EXPECT_EQ(map.size(), 2);
  for (size_t row = 0; row < 4; ++row) {
    EXPECT_TRUE(getter.findKey(map, row, arena).isFound()) << "row " << row;
  }
}

} // namespace
} // namespace facebook::velox::exec::ch
