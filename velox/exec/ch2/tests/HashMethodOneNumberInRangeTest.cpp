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

#include "velox/exec/ch2/Common/ColumnsHashing/HashMethod.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using Hash = std::hash<int64_t>;
using Cell = ch::HashMapCell<int64_t, uint64_t, Hash>;
using Map = ch::HashMapTable<int64_t, Cell, Hash>;
using Method =
    HashMethodOneNumberInRange<Map::value_type, Map::mapped_type, int64_t>;

class HashMethodOneNumberInRangeTest : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-inrange-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-inrange-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

// 值域接入 infra 边界:computeKeyRange 扫 flat 列算 min/max,range=max-min+1。
TEST_F(HashMethodOneNumberInRangeTest, computeKeyRangeScansMinMax) {
  auto keyVector = makeFlatVector<int64_t>({105, 100, 130, 100, 120});
  auto range = computeKeyRange<int64_t>(keyVector);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->min_key, 100);
  // range = max(130) - min(100) + 1 = 31.
  EXPECT_EQ(range->range_size, 31);
}

// getKeyHolder 平移对拍:shifted = 原 key - min_key,逐值验证。
TEST_F(HashMethodOneNumberInRangeTest, getKeyHolderShiftsByMinKey) {
  const std::vector<int64_t> keys{100, 105, 130, 120};
  auto keyVector = makeFlatVector<int64_t>(keys);
  Method method({keyVector}, {}, nullptr);
  auto range = computeKeyRange<int64_t>(keyVector);
  ASSERT_TRUE(range.has_value());
  method.min_key = range->min_key;
  method.range_size = range->range_size;
  ch::Arena arena(arenaPool_.get());

  for (size_t row = 0; row < keys.size(); ++row) {
    EXPECT_EQ(method.getKeyHolder(row, arena), keys[row] - range->min_key);
    auto [shifted, inRange] = method.getKeyHolderInRange(row, arena);
    EXPECT_EQ(shifted, keys[row] - range->min_key);
    EXPECT_TRUE(inRange);
  }
}

// range 优化 emplace/find 命中:密集 key 0..N 平移后走普通定长 map。
TEST_F(HashMethodOneNumberInRangeTest, emplacesAndFindsInRangeKeys) {
  // 密集范围 key:1000..1009。
  std::vector<int64_t> keys;
  for (int64_t k = 1000; k < 1010; ++k) {
    keys.push_back(k);
  }
  auto keyVector = makeFlatVector<int64_t>(keys);
  auto range = computeKeyRange<int64_t>(keyVector);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->min_key, 1000);
  EXPECT_EQ(range->range_size, 10);

  Method build({keyVector}, {}, nullptr);
  build.min_key = range->min_key;
  build.range_size = range->range_size;
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  for (size_t row = 0; row < keys.size(); ++row) {
    auto result = build.emplaceKey(map, row, arena);
    ASSERT_TRUE(result.isInserted());
    result.setMapped(row + 1);
  }
  // 平移后 10 个 distinct key ∈ [0,10)。
  EXPECT_EQ(map.size(), 10);

  // probe 用 build 自身:每行命中平移后的 key。
  for (size_t row = 0; row < keys.size(); ++row) {
    auto result = build.findKey(map, row, arena);
    ASSERT_TRUE(result.isFound()) << "row " << row;
    EXPECT_EQ(result.getMapped(), row + 1);
  }
}

// 范围校验生效:范围外 key(min_key-1、min_key+range_size)走 miss。
TEST_F(HashMethodOneNumberInRangeTest, outOfRangeKeysMiss) {
  // build: 密集 key 200..209,range=[200,210)。
  std::vector<int64_t> buildKeys;
  for (int64_t k = 200; k < 210; ++k) {
    buildKeys.push_back(k);
  }
  auto buildVector = makeFlatVector<int64_t>(buildKeys);
  auto range = computeKeyRange<int64_t>(buildVector);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->min_key, 200);
  EXPECT_EQ(range->range_size, 10);

  Method build({buildVector}, {}, nullptr);
  build.min_key = range->min_key;
  build.range_size = range->range_size;
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());
  for (size_t row = 0; row < buildKeys.size(); ++row) {
    auto result = build.emplaceKey(map, row, arena);
    ASSERT_TRUE(result.isInserted());
    result.setMapped(row + 1);
  }

  // probe 列:含范围内命中 + 范围外(min_key-1=199、min_key+range_size=210)。
  //   205 -> in range, 命中
  //   199 = min_key-1 -> shifted = -1(wraps),校验 shifted<range_size 为 false -> miss
  //   210 = min_key+range_size -> shifted = 10, 10<10 false -> miss
  //   200 = min_key -> shifted 0 -> 命中
  //   209 = max_key -> shifted 9 -> 命中
  auto probeVector = makeFlatVector<int64_t>({205, 199, 210, 200, 209});
  Method probe({probeVector}, {}, nullptr);
  probe.min_key = range->min_key;
  probe.range_size = range->range_size;

  EXPECT_TRUE(probe.findKey(map, 0, arena).isFound()); // 205
  EXPECT_FALSE(probe.findKey(map, 1, arena).isFound()); // 199 min_key-1
  EXPECT_FALSE(probe.findKey(map, 2, arena).isFound()); // 210 min_key+range
  EXPECT_TRUE(probe.findKey(map, 3, arena).isFound()); // 200 min_key
  EXPECT_TRUE(probe.findKey(map, 4, arena).isFound()); // 209 max_key
}


// 溢出防护第 1 层:跨度 >= MAX_RANGE(2^18)不启用 range 优化(返回空 optional),
// 对齐 CH HashJoin.cpp:2242 的 return。用 INT64_MIN/MAX 极值验证无有符号溢出 UB。
TEST_F(HashMethodOneNumberInRangeTest, extremeKeysExceedMaxRangeDisableOpt) {
  // min=INT64_MIN, max=INT64_MAX:max-min 在有符号域是 UB,size_t 无符号域 wraps
  // 到一个巨大值,必 >= MAX_RANGE -> 空 optional(不启用),不 crash/UB。
  auto keyVector = makeFlatVector<int64_t>(
      {std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max(),
       0});
  auto range = computeKeyRange<int64_t>(keyVector);
  EXPECT_FALSE(range.has_value());
}

// 跨度恰好 == MAX_RANGE(max-min = 2^18)不启用(CH 用 >= 判定,边界含等号)。
TEST_F(HashMethodOneNumberInRangeTest, spanEqualsMaxRangeDisableOpt) {
  const int64_t kMaxRange = (1LL << 18);
  auto keyVector = makeFlatVector<int64_t>({0, kMaxRange});
  auto range = computeKeyRange<int64_t>(keyVector);
  EXPECT_FALSE(range.has_value());
}

// 跨度 == MAX_RANGE - 1(max-min = 2^18 - 1)仍启用,range_size = 2^18。
TEST_F(HashMethodOneNumberInRangeTest, spanJustBelowMaxRangeEnabled) {
  const int64_t kMaxRange = (1LL << 18);
  auto keyVector = makeFlatVector<int64_t>({0, kMaxRange - 1});
  auto range = computeKeyRange<int64_t>(keyVector);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->min_key, 0);
  EXPECT_EQ(range->range_size, kMaxRange);
}

// 稀疏但小跨度:只有 2 个 key,跨度小 -> 正常启用(稀疏度不影响 computeKeyRange
// 本身的启用判定,CH 稀疏度 factor 是后续 size_bits 选择,不在此边界)。
TEST_F(HashMethodOneNumberInRangeTest, sparseSmallSpanEnabled) {
  auto keyVector = makeFlatVector<int64_t>({-5, 10});
  auto range = computeKeyRange<int64_t>(keyVector);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->min_key, -5);
  // range = 10 - (-5) + 1 = 16.
  EXPECT_EQ(range->range_size, 16);
}

} // namespace
} // namespace facebook::velox::exec::ch2
