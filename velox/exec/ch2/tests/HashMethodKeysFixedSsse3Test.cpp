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

// ch2-task7 (O5): packFixedShuffle SSSE3 == 标量 packFixed 逐字节对拍 +
// SSSE3 路径 emplace/find + 运行时 A/B 开关能切两条路径。
//
// 硬关卡:同一批多列定长 key,SSSE3 packFixedShuffle 出的宽 key 跟标量
// packFixed 逐字节一致(mask 错一位结果就不同)。

#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h" // ch::UInt128
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/exec/ch2/Common/ColumnsHashing/HashMethod.h"
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashMethodDispatch.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using ch::UInt128;

using Map128 = ch::HashMapAll_keys128;

using Method128 = HashMethodKeysFixed<
    Map128::value_type,
    UInt128,
    Map128::mapped_type,
    /*has_nullable_keys_=*/false,
    /*has_low_cardinality_=*/false,
    /*use_cache=*/false>;

class HashMethodKeysFixedSsse3Test : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-ssse3-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-ssse3-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

// ---------------------------------------------------------------------------
// 硬关卡:SSSE3 packFixedShuffle 逐字节 == 标量 packFixed。
// 两列 bigint(size 8+8=16) -> UInt128。A/B 两入口都在 input-column-order
// consecutive 布局,pshufb 洗牌结果必须跟标量 memcpy 拼接逐字节相同。
// ---------------------------------------------------------------------------
TEST_F(HashMethodKeysFixedSsse3Test, ssse3EqualsScalarTwoBigint) {
  if (!Method128::ssse3Available()) {
    GTEST_SKIP() << "SSSE3 not available at compile time (ARM/no-SSSE3)";
  }
  const std::vector<int64_t> c0{10, 20, 10, 30, 0x0102030405060708LL, -7};
  const std::vector<int64_t> c1{100, 200, 100, 300, -1, 0x7fffffffffffffffLL};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);

  Sizes sizes{8, 8};
  Method128 method({v0, v1}, sizes, nullptr);

  for (size_t r = 0; r < rows; ++r) {
    UInt128 scalar = method.packRowScalar(r);
    UInt128 ssse3 = method.packRowSsse3(r);
    EXPECT_EQ(0, std::memcmp(&scalar, &ssse3, sizeof(UInt128)))
        << "SSSE3 != scalar at row " << r;
  }
}

// 混合 size:bigint(8)+int(4)+smallint(2)+tinyint(1) = 15B -> UInt128。
// mask 逐列 offset 累进,SSSE3 == 标量 逐字节。
TEST_F(HashMethodKeysFixedSsse3Test, ssse3EqualsScalarMixedSizes) {
  if (!Method128::ssse3Available()) {
    GTEST_SKIP();
  }
  const std::vector<int64_t> c0{1, 2, 0x1122334455667788LL, -3};
  const std::vector<int32_t> c1{7, 8, -1, 0x01020304};
  const std::vector<int16_t> c2{70, 80, -2, 0x0506};
  const std::vector<int8_t> c3{9, 10, -4, 0x07};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int32_t>(c1);
  auto v2 = makeFlatVector<int16_t>(c2);
  auto v3 = makeFlatVector<int8_t>(c3);

  Sizes sizes{8, 4, 2, 1};
  Method128 method({v0, v1, v2, v3}, sizes, nullptr);

  for (size_t r = 0; r < rows; ++r) {
    UInt128 scalar = method.packRowScalar(r);
    UInt128 ssse3 = method.packRowSsse3(r);
    EXPECT_EQ(0, std::memcmp(&scalar, &ssse3, sizeof(UInt128)))
        << "SSSE3 != scalar (mixed) at row " << r;
  }
}

// SSSE3 路径 emplace/find 命中正确:用 SSSE3 pack 出的 key 建表 + 查全命中,
// 且 distinct 计数正确。
TEST_F(HashMethodKeysFixedSsse3Test, ssse3EmplaceFind) {
  if (!Method128::ssse3Available()) {
    GTEST_SKIP();
  }
  const std::vector<int64_t> c0{10, 20, 10, 30, 20};
  const std::vector<int64_t> c1{100, 200, 100, 300, 999};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);

  Sizes sizes{8, 8};
  Method128 method({v0, v1}, sizes, nullptr);
  Map128 map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // 用 SSSE3 pack 出的宽 key 直接 emplace,验命中 + distinct=4。
  size_t inserted = 0;
  for (size_t r = 0; r < rows; ++r) {
    UInt128 key = method.packRowSsse3(r);
    bool isInserted = false;
    map.emplace(key, isInserted);
    if (isInserted) {
      ++inserted;
    }
  }
  EXPECT_EQ(inserted, 4);
  EXPECT_EQ(map.size(), 4);
  for (size_t r = 0; r < rows; ++r) {
    UInt128 key = method.packRowSsse3(r);
    EXPECT_NE(nullptr, map.find(key)) << "SSSE3 key miss row " << r;
  }
}

// 运行时 A/B 开关能切两条路径:env CH2_KEYSFIXED_USE_SSSE3 决定
// keysFixedSsse3DefaultEnabled() 默认。=0 强制标量、=1 强制 SSSE3。
// 两条路径 getKeyHolder(非 prepared 档时)与显式 A/B 入口对同一 key 都一致。
TEST_F(HashMethodKeysFixedSsse3Test, runtimeAbSwitch) {
  if (!Method128::ssse3Available()) {
    GTEST_SKIP();
  }
  // 强制标量默认。
  setenv("CH2_KEYSFIXED_USE_SSSE3", "0", 1);
  EXPECT_FALSE(keysFixedSsse3DefaultEnabled());
  // 强制 SSSE3 默认。
  setenv("CH2_KEYSFIXED_USE_SSSE3", "1", 1);
  EXPECT_TRUE(keysFixedSsse3DefaultEnabled());
  // 空/未设 -> 按平台(有 SSSE3 编译能力 -> true)。
  unsetenv("CH2_KEYSFIXED_USE_SSSE3");
  EXPECT_TRUE(keysFixedSsse3DefaultEnabled());

  // 两路径对同一批 key 逐字节一致(A/B 切换不改变结果,只改算法)。
  const std::vector<int64_t> c0{5, 6, 7};
  const std::vector<int64_t> c1{50, 60, 70};
  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);
  Sizes sizes{8, 8};
  Method128 method({v0, v1}, sizes, nullptr);
  for (size_t r = 0; r < 3; ++r) {
    UInt128 a = method.packRowScalar(r);
    UInt128 b = method.packRowSsse3(r);
    EXPECT_EQ(0, std::memcmp(&a, &b, sizeof(UInt128))) << "row " << r;
  }
}

} // namespace
} // namespace facebook::velox::exec::ch2
