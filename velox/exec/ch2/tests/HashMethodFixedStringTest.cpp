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
#include "velox/exec/ch2/Common/HashTable/StringHashMapAdapter.h"
#include "velox/exec/ch2/DataTypes/FixedStringType.h"
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashMethodDispatch.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using Map = StringHashMapAdapter;
// use_cache=false: 同 HashMethodStringTest —— port StringRef 无 std::string_view
// 比较运算符,字符串 key 关掉 LastElementCache,算法其余部分不变。
using Method = HashMethodFixedString<
    Map::value_type,
    Map::mapped_type,
    /*place_string_to_arena=*/true,
    /*use_cache=*/false>;

constexpr uint32_t kN = 16;

class HashMethodFixedStringTest : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-hmfs-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-hmfs-arena");
  }

  // 构造一个 FixedStringType(kN) 承载的 flat vector:每行正好 kN 字节。
  // pad 到 kN(不足补 '\0',超长截断),对齐 CH ColumnFixedString 定长 N 语义。
  VectorPtr makeFixedStringVector(const std::vector<std::string>& keys) {
    std::vector<std::string> padded;
    padded.reserve(keys.size());
    for (const auto& k : keys) {
      std::string p = k;
      p.resize(kN, '\0');
      padded.push_back(std::move(p));
    }
    auto vec = makeFlatVector<StringView>(
        padded.size(),
        [&](auto row) { return StringView(padded[row]); },
        /*isNullAt=*/nullptr,
        FIXED_STRING(kN));
    // makeFlatVector 会用元素类型建 vector;显式确保 type 是 FixedStringType。
    return vec;
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

// FixedStringType(N) 逻辑类型:标记 N 正确、equivalent 按 N 区分。
TEST_F(HashMethodFixedStringTest, fixedStringTypeCarriesN) {
  auto t16 = FIXED_STRING(16);
  auto t8 = FIXED_STRING(8);
  EXPECT_EQ(t16->fixedLength(), 16u);
  EXPECT_EQ(t8->fixedLength(), 8u);
  EXPECT_TRUE(isFixedStringType(t16));
  // 同 N 缓存同实例;equivalent 按 N。
  EXPECT_EQ(FIXED_STRING(16).get(), t16.get());
  EXPECT_TRUE(t16->equivalent(*FIXED_STRING(16)));
  EXPECT_FALSE(t16->equivalent(*t8));
  // 物理承载 VARBINARY。
  EXPECT_EQ(t16->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(t16->toString(), "FIXEDSTRING(16)");
}

// 定长 N=16 字节 key:含重复;emplace/find 命中正确。
TEST_F(HashMethodFixedStringTest, emplacesAndFindsFixedLengthKeys) {
  const std::vector<std::string> keys{
      "alpha", // -> padded to 16
      "beta_value_1234", // 15 chars -> padded
      "alpha", // dup
      "gamma_key_000001", // 16 chars exact
      "beta_value_1234", // dup
      "gamma_key_000001", // dup
  };
  auto keyVector = makeFixedStringVector(keys);

  Method method({keyVector}, {}, nullptr);
  // 取到的 N 正确。
  EXPECT_EQ(method.n, kN);

  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // distinct = {alpha, beta_value_1234, gamma_key_000001} = 3。
  size_t inserted =
      ChHashMethodDispatch::build(method, map, keys.size(), arena);
  EXPECT_EQ(inserted, 3);
  EXPECT_EQ(map.size(), 3);

  // 逐行 find:全部命中。
  for (size_t row = 0; row < keys.size(); ++row) {
    EXPECT_TRUE(method.findKey(map, row, arena).isFound()) << "row " << row;
  }

  // 独立 probe:命中集正确。
  auto probeVector = makeFixedStringVector(
      {"alpha", "missing_key_here", "gamma_key_000001", "absent_padded00"});
  Method probe({probeVector}, {}, nullptr);
  EXPECT_TRUE(probe.findKey(map, 0, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 2, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 3, arena).isFound());
}

// 对拍:getKeyHolder 取到的 key = 该行 StringView 前 N 字节,逐字节一致。
TEST_F(HashMethodFixedStringTest, getKeyHolderSlicesNBytes) {
  std::vector<std::string> padded;
  for (const std::string& k : {std::string("k0"), std::string("longer_key_1")}) {
    std::string p = k;
    p.resize(kN, '\0');
    padded.push_back(p);
  }
  auto vec = makeFlatVector<StringView>(
      padded.size(),
      [&](auto row) { return StringView(padded[row]); },
      nullptr,
      FIXED_STRING(kN));

  Method method({vec}, {}, nullptr);
  ch::Arena arena(arenaPool_.get());

  for (size_t row = 0; row < padded.size(); ++row) {
    auto holder = method.getKeyHolder(row, arena);
    const std::string_view sv = keyHolderGetKey(holder);
    ASSERT_EQ(sv.size(), kN) << "row " << row;
    // 逐字节 = padded[row][0..N)。
    for (uint32_t b = 0; b < kN; ++b) {
      EXPECT_EQ(sv[b], padded[row][b]) << "row " << row << " byte " << b;
    }
  }
}

// persist 协议:build 后原 input 列释放 + 改写,find 仍命中,证明 key 拷进
// arena、cell StringRef 不悬垂(同 HashMethodStringTest 的 persist 测试)。
TEST_F(HashMethodFixedStringTest, persistKeepsKeysAliveAfterInputMutated) {
  std::vector<std::string> keys{
      "persist_key_aaaa", // 16
      "persist_key_bbbb", // 16
      "persist_key_cccc", // 16
  };

  ch::Arena arena(arenaPool_.get());
  Map map(mapPool_.get());

  {
    auto keyVector = makeFixedStringVector(keys);
    Method method({keyVector}, {}, nullptr);
    ChHashMethodDispatch::build(method, map, keys.size(), arena);
    EXPECT_EQ(map.size(), 3);
    // keyVector 在此析构 —— 原列 buffer 释放。
  }

  // 覆盖原始 std::string 承载字节。
  for (auto& k : keys) {
    std::fill(k.begin(), k.end(), 'Z');
  }

  // 新构造 probe(字节来自独立存储)去 find 原始 key。
  auto probeVector = makeFixedStringVector(
      {"persist_key_aaaa",
       "persist_key_bbbb",
       "persist_key_cccc",
       "not_present_0000"});
  Method probe({probeVector}, {}, nullptr);

  EXPECT_TRUE(probe.findKey(map, 0, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 2, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 3, arena).isFound());
}

// 越界守卫(negative):某行 StringView size < N。N=16 > kInlineSize(12) → external,
// getKeyHolder 的 string_view(sv.data(), n) 会读 n 字节越界。构造时整列校验须触发。
TEST_F(HashMethodFixedStringTest, rejectsRowShorterThanN) {
  // row0 正好 16 字节;row1 只 8 字节(< N)。手工建 StringView,绕过 pad-to-N。
  std::string ok(kN, 'a'); // 16 external
  std::string short8(8, 'b'); // 8 external
  std::vector<std::string> storage{ok, short8};
  auto vec = makeFlatVector<StringView>(
      storage.size(),
      [&](auto row) { return StringView(storage[row]); },
      /*isNullAt=*/nullptr,
      FIXED_STRING(kN));
  // 构造时整列校验触发,不再 silently 越界。
  VELOX_ASSERT_THROW(
      Method({vec}, {}, nullptr),
      "StringView size 8 != FixedStringType(16)");
}

// 越界守卫(negative):某行 inline(<=12 字节)。sv.data() 指向 StringView 结构体
// 内部(仅 12 有效),读 n=16 字节会读到结构体尾部垃圾。构造时校验须触发。
TEST_F(HashMethodFixedStringTest, rejectsRowInlineShorterThanN) {
  std::string ok(kN, 'a'); // 16 external
  std::string inline6(6, 'c'); // 6 -> inline StringView
  std::vector<std::string> storage{ok, inline6};
  auto vec = makeFlatVector<StringView>(
      storage.size(),
      [&](auto row) { return StringView(storage[row]); },
      /*isNullAt=*/nullptr,
      FIXED_STRING(kN));
  VELOX_ASSERT_THROW(
      Method({vec}, {}, nullptr),
      "StringView size 6 != FixedStringType(16)");
}

} // namespace
} // namespace facebook::velox::exec::ch2
