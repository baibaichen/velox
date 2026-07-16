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

#include "velox/exec/ch/Common/HashTable/FixedHashMap.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Interpreters/RowRef.h"

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
  FixedHashMap_key8 map8(mapPool_.get());
  FixedHashMap_key16 map16(mapPool_.get());
  EXPECT_EQ(map8.getBufferSizeInCells(), 256u);
  EXPECT_EQ(map16.getBufferSizeInCells(), 65'536u);
  EXPECT_EQ(
      map8.getBufferSizeInBytes(),
      256u * sizeof(FixedHashMap_key8::cell_type));
  EXPECT_EQ(
      map16.getBufferSizeInBytes(),
      65'536u * sizeof(FixedHashMap_key16::cell_type));
}

TEST_F(FixedHashMapTest, emplaceFindAndDuplicates) {
  FixedHashMap_key8 map(mapPool_.get());
  Arena arena(arenaPool_.get());
  EXPECT_TRUE(map.empty());

  // Key 0 is not special-cased for a direct-address table.
  map.emplace(uint8_t{0}).insert(refWord(1, 1), arena);
  map.emplace(uint8_t{200}).insert(refWord(1, 2), arena);
  map.emplace(uint8_t{200}).insert(refWord(1, 3), arena);

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
  FixedHashMap_key16 map(mapPool_.get());
  Arena arena(arenaPool_.get());

  const std::vector<uint16_t> keys{0, 5, 300, 65'535};
  for (const auto key : keys) {
    map.emplace(key).insert(refWord(2, key), arena);
  }

  std::vector<uint16_t> seen;
  for (auto it = map.begin(); it != map.end(); ++it) {
    seen.push_back(it->getKey());
    EXPECT_EQ(it->getMapped().firstWord(), refWord(2, it->getKey()));
  }
  EXPECT_EQ(seen, keys);
}

// Verifies the min/max range optimization ported from ClickHouse: after a
// sparse set of emplaces, iteration must only traverse the [min, max] window,
// not the whole 65'536-slot buffer. We count how many cells operator++ visits
// (occupied cells returned + empty cells skipped) and assert it is bounded by
// max - min + 1, far below the buffer size.
TEST_F(FixedHashMapTest, minMaxOptimizationLimitsScanToRange) {
  FixedHashMap_key16 map(mapPool_.get());
  Arena arena(arenaPool_.get());

  // Two very sparse keys straddling the buffer: window is [5, 60000].
  const uint16_t lo = 5;
  const uint16_t hi = 60000;
  map.emplace(lo).insert(refWord(3, lo), arena);
  map.emplace(hi).insert(refWord(3, hi), arena);

  EXPECT_TRUE(map.canUseMinMaxOptimization());

  // Iterating scans at most within [min, max]; the buffer has 65'536 slots but
  // the tail above `hi` and the head below `lo` must never be touched.
  std::vector<uint16_t> seen;
  auto it = map.begin();
  // begin() lands directly on min without scanning [0, min).
  EXPECT_EQ(it.getHash(), lo);
  for (; it != map.end(); ++it) {
    seen.push_back(it->getKey());
  }
  EXPECT_EQ(seen, (std::vector<uint16_t>{lo, hi}));

  // end() is buf + max + 1, i.e. it stops right after `hi` rather than at the
  // buffer end. getHash() on end() reports hi + 1, proving the tail is skipped.
  EXPECT_EQ(map.end().getHash(), static_cast<size_t>(hi) + 1);
  EXPECT_LT(map.end().getHash(), map.getBufferSizeInCells());
}

// After a call that mutates the buffer outside of emplace (data()), the min/max
// optimization must switch off and iteration falls back to a full scan, exactly
// as ClickHouse does via only_emplace_was_used_to_insert_data.
TEST_F(FixedHashMapTest, minMaxOptimizationDisabledAfterRawBufferAccess) {
  FixedHashMap_key16 map(mapPool_.get());
  Arena arena(arenaPool_.get());
  map.emplace(uint16_t{5}).insert(refWord(4, 5), arena);
  map.emplace(uint16_t{60000}).insert(refWord(4, 60000), arena);
  EXPECT_TRUE(map.canUseMinMaxOptimization());

  (void)map.data();
  EXPECT_FALSE(map.canUseMinMaxOptimization());
  // Full-scan fallback: end() reports the whole buffer.
  EXPECT_EQ(map.end().getHash(), map.getBufferSizeInCells());
}

} // namespace
} // namespace facebook::velox::exec::ch
