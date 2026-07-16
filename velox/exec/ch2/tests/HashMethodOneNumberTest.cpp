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
#include <functional>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace {

using Hash = std::hash<int64_t>;
using Cell = ch::HashMapCell<int64_t, uint64_t, Hash>;
using Map = ch::HashMapTable<int64_t, Cell, Hash>;
using Method = HashMethodOneNumber<Map::value_type, Map::mapped_type, int64_t>;

class HashMethodOneNumberTest : public testing::Test,
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
    mapPool_ = memory::memoryManager()->addLeafPool("ch2-hash-method-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch2-hash-method-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

TEST_F(HashMethodOneNumberTest, emplacesAndFindsFlatInt64Keys) {
  const std::vector<int64_t> keys{11, 22, 11, 0, -7};
  auto keyVector = makeFlatVector<int64_t>(keys);
  Method method({keyVector}, {}, nullptr);
  Map map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  for (size_t row = 0; row < keys.size(); ++row) {
    auto result = method.emplaceKey(map, row, arena);
    if (row == 2) {
      EXPECT_FALSE(result.isInserted());
      EXPECT_EQ(result.getMapped(), 100);
    } else {
      ASSERT_TRUE(result.isInserted());
      result.setMapped(row + 100);
    }
  }

  EXPECT_EQ(map.size(), 4);
  for (size_t row = 0; row < keys.size(); ++row) {
    auto result = method.findKey(map, row, arena);
    ASSERT_TRUE(result.isFound());
    EXPECT_EQ(result.getMapped(), row == 2 ? 100 : row + 100);
  }

  auto probeVector = makeFlatVector<int64_t>({22, 99, 0});
  Method probeMethod({probeVector}, {}, nullptr);
  EXPECT_TRUE(probeMethod.findKey(map, 0, arena).isFound());
  EXPECT_FALSE(probeMethod.findKey(map, 1, arena).isFound());
  EXPECT_TRUE(probeMethod.findKey(map, 2, arena).isFound());
}

} // namespace
} // namespace facebook::velox::exec::ch2
