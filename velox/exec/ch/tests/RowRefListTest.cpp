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

#include "velox/exec/ch/RowRefList.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Arena.h"
#include "velox/exec/ch/RowRef.h"

#include <algorithm>
#include <cstdint>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class RowRefListTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Idempotent: sibling suites in this binary (e.g. ArenaTest) may have
    // already initialized the process-wide manager, and initialize() throws on
    // a second call. Guard on testInstance() so suite order does not matter.
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("ch-rowreflist-test");
  }

  // Builds an inline encoded ref word from a (driver, batch, row) triple, the
  // way HashBuild will: RowRef{packBlockNo(driver, batch), row}.encode().
  static uint64_t makeWord(uint32_t driverNo, uint32_t batchNo, uint32_t rowNo) {
    return RowRef(packBlockNo(driverNo, batchNo), rowNo).encode();
  }

  // Drains a list via its ForwardIterator into a vector of encoded words.
  static std::vector<uint64_t> collect(const RowRefList& list) {
    std::vector<uint64_t> out;
    for (auto it = list.begin(); it != list.end(); ++it) {
      out.push_back(*it);
    }
    return out;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

// N == 1: the first row stays inline; no arena allocation happens.
TEST_F(RowRefListTest, inlineSingleRow) {
  Arena arena(pool_.get());
  const uint64_t w = makeWord(1, 2, 3);

  RowRefList list;
  list.insert(w, arena);

  EXPECT_TRUE(list.isInline());
  EXPECT_EQ(list.rows(), 1u);
  EXPECT_EQ(list.firstWord(), w);
  EXPECT_EQ(pool_->usedBytes(), 0) << "unique key must not touch the arena";
  EXPECT_EQ(collect(list), (std::vector<uint64_t>{w}));
}

// N == 2: crossing from inline to a cell node allocates exactly one Batch and
// preserves insertion order (head first).
TEST_F(RowRefListTest, cellNodeTwoRows) {
  Arena arena(pool_.get());
  const uint64_t w0 = makeWord(0, 0, 10);
  const uint64_t w1 = makeWord(0, 0, 11);

  RowRefList list;
  list.insert(w0, arena);
  list.insert(w1, arena);

  EXPECT_FALSE(list.isInline());
  EXPECT_EQ(list.rows(), 2u);
  EXPECT_EQ(list.firstWord(), w0);
  EXPECT_GT(pool_->usedBytes(), 0) << "the first duplicate must allocate a node";
  EXPECT_EQ(collect(list), (std::vector<uint64_t>{w0, w1}));
}

// N == 7: fills the cell node to MAX_LOCAL without chaining; order is exact.
TEST_F(RowRefListTest, cellNodeFullNoChain) {
  Arena arena(pool_.get());
  std::vector<uint64_t> input;
  RowRefList list;
  for (uint32_t r = 0; r < RowRefList::MAX_LOCAL; ++r) {
    const uint64_t w = makeWord(0, 0, 100 + r);
    input.push_back(w);
    list.insert(w, arena);
  }
  ASSERT_EQ(input.size(), 7u);

  EXPECT_FALSE(list.isInline());
  EXPECT_EQ(list.rows(), 7u);
  // Unchained cell node: size == total_rows.
  const auto* b = list.asBatch();
  EXPECT_EQ(b->size, b->total_rows);
  // Iteration is exactly the insertion order for keys up to MAX_LOCAL rows.
  EXPECT_EQ(collect(list), input);
}

// N == 8: the 8th insert evicts the last local ref into a new overflow node,
// chaining the key. The multiset of returned words still equals the input.
TEST_F(RowRefListTest, evictToOverflowNodeAtEight) {
  Arena arena(pool_.get());
  std::vector<uint64_t> input;
  RowRefList list;
  for (uint32_t r = 0; r < 8; ++r) {
    const uint64_t w = makeWord(0, 0, 200 + r);
    input.push_back(w);
    list.insert(w, arena);
  }

  EXPECT_FALSE(list.isInline());
  EXPECT_EQ(list.rows(), 8u);
  // Chained cell node: size (6) != total_rows (8).
  const auto* b = list.asBatch();
  EXPECT_EQ(b->size, RowRefList::Batch::SLOTS);
  EXPECT_EQ(b->total_rows, 8u);

  auto got = collect(list);
  EXPECT_EQ(got.size(), 8u);
  // Exactly at 8 the CH order still equals insertion order (cell run w0..w5,
  // then the freshly-created overflow node's w6, w7).
  EXPECT_EQ(got, input);

  auto gotSorted = got;
  auto inSorted = input;
  std::sort(gotSorted.begin(), gotSorted.end());
  std::sort(inSorted.begin(), inSorted.end());
  EXPECT_EQ(gotSorted, inSorted);
}

// N > 8: multiple overflow nodes are chained newest-first. Assert the full set
// round-trips, the count/total_rows are right, and the newest-first ordering
// matches the CH insert algorithm.
TEST_F(RowRefListTest, overflowChainedManyRows) {
  Arena arena(pool_.get());
  constexpr uint32_t kN = 15;
  std::vector<uint64_t> input;
  RowRefList list;
  for (uint32_t r = 0; r < kN; ++r) {
    const uint64_t w = makeWord(0, 0, 300 + r);
    input.push_back(w);
    list.insert(w, arena);
  }

  EXPECT_FALSE(list.isInline());
  EXPECT_EQ(list.rows(), kN);
  EXPECT_EQ(list.firstWord(), input[0]);

  auto got = collect(list);
  ASSERT_EQ(got.size(), kN);

  // Full multiset round-trips.
  auto gotSorted = got;
  auto inSorted = input;
  std::sort(gotSorted.begin(), gotSorted.end());
  std::sort(inSorted.begin(), inSorted.end());
  EXPECT_EQ(gotSorted, inSorted);

  // Exact CH order for kN == 15 (derived from the insert algorithm):
  //   cell head-run w0..w5, then newest overflow node (w12,w13,w14),
  //   then the older overflow node (w6..w11).
  std::vector<uint64_t> expected = {
      input[0],  input[1],  input[2],  input[3], input[4],
      input[5],  input[12], input[13], input[14], input[6],
      input[7],  input[8],  input[9],  input[10], input[11]};
  EXPECT_EQ(got, expected);
}

// Full coordinate round-trip: encode (driver, batch, row) -> insert -> chain ->
// iterate -> decode back, for enough rows to exercise the overflow chain.
TEST_F(RowRefListTest, coordinateRoundTripThroughChain) {
  Arena arena(pool_.get());
  using Triple = std::tuple<uint32_t, uint32_t, uint32_t>;
  std::vector<Triple> triples;
  RowRefList list;
  // Use varied driver/batch/row values inside the field limits (driver < 64,
  // batch < 2^25) to prove the three-level coordinate survives the chain.
  for (uint32_t i = 0; i < 20; ++i) {
    const uint32_t driver = i % 64;
    const uint32_t batch = (i * 12345) % (1u << 25);
    const uint32_t row = 1000 + i;
    triples.emplace_back(driver, batch, row);
    list.insert(makeWord(driver, batch, row), arena);
  }

  EXPECT_EQ(list.rows(), 20u);

  // Decode every returned word back to (driver, batch, row) and compare as a
  // multiset against the inserted triples.
  std::vector<Triple> decoded;
  for (auto it = list.begin(); it != list.end(); ++it) {
    const uint64_t w = *it;
    EXPECT_TRUE(refWordIsInline(w)) << "every stored ref word is an inline ref";
    const uint32_t blockNo = refWordBlockNo(w);
    const uint32_t row = refWordRowNo(w);
    decoded.emplace_back(unpackDriverNo(blockNo), unpackBatchNo(blockNo), row);
  }

  std::sort(triples.begin(), triples.end());
  std::sort(decoded.begin(), decoded.end());
  EXPECT_EQ(decoded, triples);
}

// Saturating count: once total_rows reaches COUNT_SAT the cell word count field
// pins at COUNT_SAT and rows() falls back to the node's total_rows.
TEST_F(RowRefListTest, saturatingCountFallsBackToTotalRows) {
  Arena arena(pool_.get());
  RowRefList list;
  const uint32_t kN = RowRefList::COUNT_SAT + 5;
  for (uint32_t r = 0; r < kN; ++r) {
    list.insert(makeWord(0, 0, r), arena);
  }
  // Count field in the word saturates, but rows() reads the true total.
  const uint32_t countField = static_cast<uint32_t>(
      (list.word >> RowRefList::COUNT_SHIFT) & RowRefList::COUNT_SAT);
  EXPECT_EQ(countField, RowRefList::COUNT_SAT);
  EXPECT_EQ(list.rows(), kN);
  EXPECT_EQ(list.asBatch()->total_rows, kN);

  // All rows still round-trip.
  auto got = collect(list);
  EXPECT_EQ(got.size(), kN);
}

// Memory goes through the Arena / MemoryPool: repeated inserts grow usedBytes
// and everything is released when the Arena is destroyed.
TEST_F(RowRefListTest, memoryAccountedThroughArena) {
  EXPECT_EQ(pool_->usedBytes(), 0);
  {
    Arena arena(pool_.get());
    RowRefList list;
    list.insert(makeWord(0, 0, 0), arena);
    EXPECT_EQ(pool_->usedBytes(), 0) << "first (inline) row allocates nothing";

    list.insert(makeWord(0, 0, 1), arena);
    const auto afterFirstNode = pool_->usedBytes();
    EXPECT_GT(afterFirstNode, 0);

    for (uint32_t r = 2; r < 100; ++r) {
      list.insert(makeWord(0, 0, r), arena);
    }
    EXPECT_GE(pool_->usedBytes(), afterFirstNode);
    EXPECT_EQ(list.rows(), 100u);
  }
  EXPECT_EQ(pool_->usedBytes(), 0) << "Arena destruction returns all bytes";
}

} // namespace
} // namespace facebook::velox::exec::ch
