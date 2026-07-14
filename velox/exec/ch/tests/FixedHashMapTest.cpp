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

#include "velox/exec/ch/FixedHashMap.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Arena.h"
#include "velox/exec/ch/RowRef.h"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class FixedHashMapTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    mapPool_ = memory::memoryManager()->addLeafPool("ch-fixed-map-test");
    arenaPool_ =
        memory::memoryManager()->addLeafPool("ch-fixed-map-arena-test");
  }

  static uint64_t refWord(uint32_t block, uint32_t row) {
    return RowRef(block, row).encode();
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

TEST_F(FixedHashMapTest, bufferSizesMatchKeyWidth) {
  EXPECT_EQ(FixedDirectMap_key8::kNumCells, 256u);
  EXPECT_EQ(FixedDirectMap_key16::kNumCells, 65'536u);

  FixedDirectMap_key8 map8(mapPool_.get());
  FixedDirectMap_key16 map16(mapPool_.get());
  EXPECT_EQ(map8.getBufferSizeInCells(), 256u);
  EXPECT_EQ(map16.getBufferSizeInCells(), 65'536u);
  EXPECT_EQ(
      map8.getBufferSizeInBytes(),
      256u * sizeof(FixedDirectMap_key8::cell_type));
  EXPECT_EQ(
      map16.getBufferSizeInBytes(),
      65'536u * sizeof(FixedDirectMap_key16::cell_type));
}

TEST_F(FixedHashMapTest, emplaceFindAndDuplicates) {
  FixedDirectMap_key8 map(mapPool_.get());
  Arena arena(arenaPool_.get());
  EXPECT_TRUE(map.empty());

  // Key 0 is not special-cased for a direct-address table.
  map.emplace(0).insert(refWord(1, 1), arena);
  map.emplace(200).insert(refWord(1, 2), arena);
  map.emplace(200).insert(refWord(1, 3), arena);

  EXPECT_FALSE(map.empty());
  EXPECT_EQ(map.size(), 2u);

  auto* zero = map.find(0);
  ASSERT_NE(zero, nullptr);
  EXPECT_EQ(zero->getMapped().firstWord(), refWord(1, 1));

  auto* dup = map.find(200);
  ASSERT_NE(dup, nullptr);
  EXPECT_EQ(dup->getMapped().rows(), 2u);

  EXPECT_EQ(map.find(199), nullptr);
}

TEST_F(FixedHashMapTest, iterationYieldsOccupiedCellsWithKeys) {
  FixedDirectMap_key16 map(mapPool_.get());
  Arena arena(arenaPool_.get());

  const std::vector<uint16_t> keys{0, 5, 300, 65'535};
  for (const auto key : keys) {
    map.emplace(key).insert(refWord(2, key), arena);
  }

  std::vector<uint16_t> seen;
  for (auto it = map.begin(); it != map.end(); ++it) {
    seen.push_back(it.getKey());
    EXPECT_EQ(it->getMapped().firstWord(), refWord(2, it.getKey()));
  }
  EXPECT_EQ(seen, keys);
}

} // namespace
} // namespace facebook::velox::exec::ch
