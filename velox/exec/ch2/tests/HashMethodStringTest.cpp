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
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashMethodDispatch.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using Map = StringHashMapAdapter;
// use_cache=false: LastElementCache 会拿 StringRef 和 std::string_view 比,
// port StringRef 无 std::string_view 比较运算符;字符串 key 关掉 cache(CH 也
// 允许该模板参数),算法其余部分不变。
using Method = HashMethodString<
    Map::value_type,
    Map::mapped_type,
    /*place_string_to_arena=*/true,
    /*use_cache=*/false>;

class HashMethodStringTest : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-hms-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-hms-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

// 变长串:含重复、含内联短串(<13B)和长串(>=13B)。
TEST_F(HashMethodStringTest, emplacesAndFindsVarLengthKeys) {
  const std::vector<std::string> keys{
      "a", // short inline
      "hello", // short inline
      "a", // dup short
      "this_is_a_long_key_over_13_bytes", // long (>=13B)
      "hello", // dup short
      "another_long_string_key_value_xyz", // long (>=13B)
      "this_is_a_long_key_over_13_bytes", // dup long
  };
  auto keyVector = makeFlatVector<StringView>(
      keys.size(), [&](auto row) { return StringView(keys[row]); });

  Method method({keyVector}, {}, nullptr);
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // Build 驱动(ChHashMethodDispatch)。distinct = {a, hello, long1, long2} = 4。
  size_t inserted =
      ChHashMethodDispatch::build(method, map, keys.size(), arena);
  EXPECT_EQ(inserted, 4);
  EXPECT_EQ(map.size(), 4);

  // 逐行 find:全部命中(每行的 key 都在 map 里)。
  for (size_t row = 0; row < keys.size(); ++row) {
    EXPECT_TRUE(method.findKey(map, row, arena).isFound()) << "row " << row;
  }

  // 独立 probe 向量:命中集正确。
  auto probeVector = makeFlatVector<StringView>(
      std::vector<StringView>{
          StringView("a"),
          StringView("missing"),
          StringView("this_is_a_long_key_over_13_bytes"),
          StringView("also_absent_long_string_value_00"),
      });
  Method probe({probeVector}, {}, nullptr);
  EXPECT_TRUE(probe.findKey(map, 0, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 2, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 3, arena).isFound());
}

// persist 协议关键测试:插入后把原 input 列的字节改写/释放,find 仍命中,
// 证明 key 真拷进了 ch::Arena、cell 的 StringRef 没悬垂指向原列 buffer。
TEST_F(HashMethodStringTest, persistKeepsKeysAliveAfterInputMutated) {
  // 用长串确保 StringView 指向 buffer(非内联),这样改写/释放原 buffer 能
  // 真正证明"没有拷进 arena 就会悬垂/读到脏数据"。
  std::vector<std::string> keys{
      "persistent_long_key_number_one_aaaa",
      "persistent_long_key_number_two_bbbb",
      "persistent_long_key_number_three_cc",
  };

  ch::Arena arena(arenaPool_.get());
  Map map(mapPool_.get());

  {
    // input 列在这个作用域内构造并 build;离开作用域后释放。
    auto keyVector = makeFlatVector<StringView>(
        keys.size(), [&](auto row) { return StringView(keys[row]); });
    Method method({keyVector}, {}, nullptr);
    ChHashMethodDispatch::build(method, map, keys.size(), arena);
    EXPECT_EQ(map.size(), 3);
    // keyVector 在此析构 —— 原列 buffer 释放。
  }

  // 把承载原始字节的 std::string 全部改写(覆盖旧内容),进一步确保原
  // buffer 内容不再有效。
  for (auto& k : keys) {
    std::fill(k.begin(), k.end(), 'Z');
  }

  // 用一批**新构造**的 probe 向量(字节来自独立存储)去 find 原始 key:
  // 若 persist 没做对(cell 悬垂指向已释放/被改写的原 buffer),命中会失败
  // 或读到脏数据。
  std::vector<std::string> lookups{
      "persistent_long_key_number_one_aaaa",
      "persistent_long_key_number_two_bbbb",
      "persistent_long_key_number_three_cc",
      "not_present_long_key_value_zzzzzzz",
  };
  auto probeVector = makeFlatVector<StringView>(
      lookups.size(), [&](auto row) { return StringView(lookups[row]); });
  Method probe({probeVector}, {}, nullptr);

  EXPECT_TRUE(probe.findKey(map, 0, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 2, arena).isFound());
  EXPECT_FALSE(probe.findKey(map, 3, arena).isFound());
}

} // namespace
} // namespace facebook::velox::exec::ch2
