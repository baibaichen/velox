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

#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h" // ch::UInt128 / ch::UInt256
#include "velox/exec/ch/Common/HashTable/HashMap.h" // ch::HashMapAll_keys128/256
#include "velox/exec/ch2/Common/ColumnsHashing/HashMethod.h"
#include "velox/exec/ch2/Interpreters/AggregationCommon.h" // packFixedBatch (order size premise test)
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashMethodDispatch.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using ch::UInt128;
using ch::UInt256;

// keys128/256 map 复用 port 的宽 key map(HashMapAll_keys128/256)。宽 key holder
// 是纯值(无 persist),port map.emplace(KeyHolder&&,...)/find(key) 直接当 Data。
using Map128 = ch::HashMapAll_keys128;
using Map256 = ch::HashMapAll_keys256;

using Method128 = HashMethodKeysFixed<
    Map128::value_type,
    UInt128,
    Map128::mapped_type,
    /*has_nullable_keys_=*/false,
    /*has_low_cardinality_=*/false,
    /*use_cache=*/false>;

using Method256 = HashMethodKeysFixed<
    Map256::value_type,
    UInt256,
    Map256::mapped_type,
    /*has_nullable_keys_=*/false,
    /*has_low_cardinality_=*/false,
    /*use_cache=*/false>;

class HashMethodKeysFixedTest : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-hmkf-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-hmkf-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;

  // 独立参考 pack:逐列按 column 顺序把 value 字节 memcpy 拼进宽 key。
  // 对应 CH 逐行 packFixed(consecutive 布局)。用来跟 ch2 pack 逐字节对拍。
  template <typename Key>
  static Key
  refPack(const std::vector<std::pair<const char*, size_t>>& cols, size_t row) {
    Key key{};
    char* bytes = reinterpret_cast<char*>(&key);
    size_t offset = 0;
    for (const auto& [base, sz] : cols) {
      std::memcpy(bytes + offset, base + row * sz, sz);
      offset += sz;
    }
    return key;
  }
};

// ---------------------------------------------------------------------------
// keys128 prepared_keys 路径:2×bigint(size 8+8=16,∈{1,2,4,8,16},sizeof(Key)
// =16 ≤16 → usePreparedKeys=true → packFixedBatch 整批预 pack)。
// 两列同 size(8),packFixedBatch 按 size 分组(此处仅一组 size=8)保持列顺序,
// 布局与逐行 consecutive 一致 → 可直接对拍。
// ---------------------------------------------------------------------------
TEST_F(HashMethodKeysFixedTest, keys128PreparedTwoBigint) {
  const std::vector<int64_t> c0{10, 20, 10, 30, 20};
  const std::vector<int64_t> c1{100, 200, 100, 300, 999};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);

  Sizes sizes{8, 8};
  Method128 method({v0, v1}, sizes, nullptr);
  Map128 map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // 逐字节对拍:ch2 getKeyHolder(prepared 路径)== 独立参考 pack。
  std::vector<std::pair<const char*, size_t>> cols{
      {reinterpret_cast<const char*>(v0->rawValues()), 8},
      {reinterpret_cast<const char*>(v1->rawValues()), 8}};
  for (size_t r = 0; r < rows; ++r) {
    UInt128 got = method.getKeyHolder(r, arena);
    UInt128 want = refPack<UInt128>(cols, r);
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt128)))
        << "prepared byte mismatch row " << r;
  }

  // build/find 命中正确。distinct = {(10,100),(20,200),(30,300),(20,999)} = 4。
  size_t inserted = ChHashMethodDispatch::build(method, map, rows, arena);
  EXPECT_EQ(inserted, 4);
  EXPECT_EQ(map.size(), 4);
  for (size_t r = 0; r < rows; ++r) {
    EXPECT_TRUE(method.findKey(map, r, arena).isFound()) << "row " << r;
  }

  // probe miss:(10,999) 不在 build 集。
  auto p0 = makeFlatVector<int64_t>(std::vector<int64_t>{10, 20});
  auto p1 = makeFlatVector<int64_t>(std::vector<int64_t>{999, 200});
  Method128 probe({p0, p1}, sizes, nullptr);
  EXPECT_FALSE(probe.findKey(map, 0, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 1, arena).isFound());
}

// ---------------------------------------------------------------------------
// keys128 prepared_keys 路径,混合 size:bigint(8)+int32(4)+int32(4)=16。
// 三列 size {8,4,4},packFixedBatch 分组顺序 64→32 → col0 先(8),再 col1,col2
// (都 4,组内保列顺序)→ 布局仍与列顺序 consecutive 一致 → 对拍。
// ---------------------------------------------------------------------------
TEST_F(HashMethodKeysFixedTest, keys128PreparedMixedSizes) {
  const std::vector<int64_t> c0{1, 2, 1, 2};
  const std::vector<int32_t> c1{7, 8, 7, 9};
  const std::vector<int32_t> c2{70, 80, 70, 90};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int32_t>(c1);
  auto v2 = makeFlatVector<int32_t>(c2);

  Sizes sizes{8, 4, 4};
  Method128 method({v0, v1, v2}, sizes, nullptr);
  Map128 map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  std::vector<std::pair<const char*, size_t>> cols{
      {reinterpret_cast<const char*>(v0->rawValues()), 8},
      {reinterpret_cast<const char*>(v1->rawValues()), 4},
      {reinterpret_cast<const char*>(v2->rawValues()), 4}};
  for (size_t r = 0; r < rows; ++r) {
    UInt128 got = method.getKeyHolder(r, arena);
    UInt128 want = refPack<UInt128>(cols, r);
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt128)))
        << "prepared mixed byte mismatch row " << r;
  }

  // distinct = {(1,7,70),(2,8,80),(2,9,90)} = 3(row0==row2)。
  size_t inserted = ChHashMethodDispatch::build(method, map, rows, arena);
  EXPECT_EQ(inserted, 3);
  for (size_t r = 0; r < rows; ++r) {
    EXPECT_TRUE(method.findKey(map, r, arena).isFound()) << "row " << r;
  }
}

// ---------------------------------------------------------------------------
// keys256 逐行 packFixed 路径:3×bigint(24 字节 > 16 → sizeof(Key)=32 >16 →
// usePreparedKeys=false → getKeyHolder 走逐行 packFixed,column-order consecutive
// 布局)。对拍逐行 pack。
// ---------------------------------------------------------------------------
TEST_F(HashMethodKeysFixedTest, keys256PerRowThreeBigint) {
  const std::vector<int64_t> c0{1, 2, 1, 3};
  const std::vector<int64_t> c1{11, 22, 11, 33};
  const std::vector<int64_t> c2{111, 222, 111, 333};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);
  auto v2 = makeFlatVector<int64_t>(c2);

  Sizes sizes{8, 8, 8};
  Method256 method({v0, v1, v2}, sizes, nullptr);
  Map256 map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // 确认走逐行路径:prepared_keys 应为空。
  EXPECT_TRUE(method.prepared_keys.empty());

  std::vector<std::pair<const char*, size_t>> cols{
      {reinterpret_cast<const char*>(v0->rawValues()), 8},
      {reinterpret_cast<const char*>(v1->rawValues()), 8},
      {reinterpret_cast<const char*>(v2->rawValues()), 8}};
  for (size_t r = 0; r < rows; ++r) {
    UInt256 got = method.getKeyHolder(r, arena);
    UInt256 want = refPack<UInt256>(cols, r);
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt256)))
        << "per-row byte mismatch row " << r;
  }

  // distinct = {(1,11,111),(2,22,222),(3,33,333)} = 3(row0==row2)。
  size_t inserted = ChHashMethodDispatch::build(method, map, rows, arena);
  EXPECT_EQ(inserted, 3);
  for (size_t r = 0; r < rows; ++r) {
    EXPECT_TRUE(method.findKey(map, r, arena).isFound()) << "row " << r;
  }

  auto p0 = makeFlatVector<int64_t>(std::vector<int64_t>{9});
  auto p1 = makeFlatVector<int64_t>(std::vector<int64_t>{11});
  auto p2 = makeFlatVector<int64_t>(std::vector<int64_t>{111});
  Method256 probe({p0, p1, p2}, sizes, nullptr);
  EXPECT_FALSE(probe.findKey(map, 0, arena).isFound());
}


// ---------------------------------------------------------------------------
// order-size premise: packFixedBatch groups by size DESCENDING (128->64->32->16
// ->8), independent of input column order. CH relies on upstream
// shuffleKeyColumns to feed columns already sorted descending; task3 did NOT
// port shuffleKeyColumns. So feeding non-descending columns directly makes the
// prepared batch layout (grouped by descending size) diverge from a per-row
// consecutive pack in input column order.
//
// This case calls packFixedBatch directly with non-descending sizes {4, 8}:
//   1) proves packFixedBatch output == consecutive pack of columns sorted
//      DESCENDING (size 8 first, then 4) -- fixing the "output layout is decided
//      by descending grouping" premise.
//   2) proves it != consecutive pack in INPUT order (4 first, then 8) -- makes
//      the "columns must be descending" premise explicit and avoids a false
//      green.
// ---------------------------------------------------------------------------
TEST_F(HashMethodKeysFixedTest, packFixedBatchDescendingGroupingPremise) {
  const std::vector<int32_t> a{7, 8, 9};
  const std::vector<int64_t> b{100, 200, 300};
  const size_t rows = a.size();

  auto va = makeFlatVector<int32_t>(a);
  auto vb = makeFlatVector<int64_t>(b);

  const char* baseA = reinterpret_cast<const char*>(va->rawValues());
  const char* baseB = reinterpret_cast<const char*>(vb->rawValues());

  // input order (non-descending): col_a(4) first, col_b(8) second.
  ColumnRawData columnData{baseA, baseB};
  Sizes sizes{4, 8};

  std::vector<UInt128> out;
  packFixedBatch<UInt128>(/*keys_size=*/2, columnData, sizes, rows, out);
  ASSERT_EQ(out.size(), rows);

  for (size_t r = 0; r < rows; ++r) {
    // (1) prepared output == consecutive pack of descending-sorted cols (8,4).
    std::vector<std::pair<const char*, size_t>> descending{
        {baseB, 8}, {baseA, 4}};
    UInt128 wantDescending = refPack<UInt128>(descending, r);
    EXPECT_EQ(0, std::memcmp(&out[r], &wantDescending, sizeof(UInt128)))
        << "packFixedBatch should group by descending size (8,4), row " << r;

    // (2) prepared output != consecutive pack in input order (4,8).
    std::vector<std::pair<const char*, size_t>> inputOrder{
        {baseA, 4}, {baseB, 8}};
    UInt128 wantInputOrder = refPack<UInt128>(inputOrder, r);
    EXPECT_NE(0, std::memcmp(&out[r], &wantInputOrder, sizeof(UInt128)))
        << "non-descending input must not match input-order pack "
           "(premise: columns must be descending), row "
        << r;
  }
}

} // namespace
} // namespace facebook::velox::exec::ch2
