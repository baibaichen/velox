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

#include "velox/exec/ch/RetainedVectorsIndex.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class RetainedVectorsIndexTest : public testing::Test,
                                 public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }
};

TEST_F(RetainedVectorsIndexTest, addAndAtRoundTrip) {
  RetainedVectorsIndex index(3);
  auto first = makeRowVector({makeFlatVector<int64_t>({10, 11, 12})});
  auto second = makeRowVector({makeFlatVector<int64_t>({20, 21})});

  const auto firstBatch = index.add(first);
  const auto secondBatch = index.add(second);

  EXPECT_EQ(firstBatch, 0);
  EXPECT_EQ(secondBatch, 1);
  EXPECT_EQ(index.at(3, firstBatch), first.get());
  EXPECT_EQ(index.at(3, secondBatch), second.get());

  const auto* values = index.at(3, firstBatch)->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(2), 12);
}

TEST_F(RetainedVectorsIndexTest, retainsWithoutCopying) {
  RetainedVectorsIndex index(0);
  auto vector = makeRowVector({makeFlatVector<int64_t>({7, 8, 9})});
  const auto* original = vector.get();
  const auto* originalChild = vector->childAt(0).get();
  const auto useCount = vector.use_count();

  const auto batch = index.add(vector);

  EXPECT_EQ(vector.use_count(), useCount + 1);
  EXPECT_EQ(index.at(0, batch), original);
  EXPECT_EQ(index.at(0, batch)->childAt(0).get(), originalChild);

  vector.reset();
  EXPECT_EQ(index.at(0, batch), original);
  EXPECT_EQ(index.at(0, batch)->childAt(0).get(), originalChild);
}

TEST_F(RetainedVectorsIndexTest, mergeFromPreservesDriverBatchCoordinates) {
  RetainedVectorsIndex driver0(0);
  RetainedVectorsIndex driver1(1);
  auto d0b0 = makeRowVector({makeFlatVector<int64_t>({100})});
  auto d0b1 = makeRowVector({makeFlatVector<int64_t>({101})});
  auto d1b0 = makeRowVector({makeFlatVector<int64_t>({200})});
  auto d1b1 = makeRowVector({makeFlatVector<int64_t>({201})});

  const auto d0Batch0 = driver0.add(d0b0);
  const auto d0Batch1 = driver0.add(d0b1);
  const auto d1Batch0 = driver1.add(d1b0);
  const auto d1Batch1 = driver1.add(d1b1);

  driver0.mergeFrom(std::move(driver1));

  EXPECT_EQ(driver0.at(0, d0Batch0), d0b0.get());
  EXPECT_EQ(driver0.at(0, d0Batch1), d0b1.get());
  EXPECT_EQ(driver0.at(1, d1Batch0), d1b0.get());
  EXPECT_EQ(driver0.at(1, d1Batch1), d1b1.get());

  const auto d1Coordinate = packBlockNo(1, d1Batch1);
  EXPECT_EQ(
      driver0.at(unpackDriverNo(d1Coordinate), unpackBatchNo(d1Coordinate)),
      d1b1.get());
}

// Pins the (driver_no, batch_no) bit-layout to CONCRETE literal integers so an
// off-by-one in kBatchNoBits (the 6/25 driver/batch split) or a driver/batch
// swap cannot slip through. The literals below are hand-computed from
// kDriverNoBits=6, kBatchNoBits=25 (so kMaxBatchNo = (1<<25)-1 = 33554431);
// they are NOT derived from the SUT constants, so they fail if the split moves.
TEST_F(RetainedVectorsIndexTest, packBlockNoMatchesLiteralBitLayout) {
  // driver=1, batch=0 must set the first driver bit at position kBatchNoBits=25,
  // i.e. 1 << 25 = 33554432.
  EXPECT_EQ(packBlockNo(1, 0), 33554432u);
  // driver=0, batch=1 stays in the low bits.
  EXPECT_EQ(packBlockNo(0, 1), 1u);
  // Boundary: max batch_no with driver 0 fills exactly the low 25 bits.
  EXPECT_EQ(packBlockNo(0, 33554431u), 33554431u);
  // Boundary: driver=1 together with max batch_no occupies bits 0..25.
  EXPECT_EQ(packBlockNo(1, 33554431u), 67108863u);
  // A larger driver: driver=3 -> 3 << 25 = 100663296.
  EXPECT_EQ(packBlockNo(3, 0), 100663296u);

  // Unpack the literals back to confirm field boundaries are where we assert.
  EXPECT_EQ(unpackDriverNo(33554432u), 1u);
  EXPECT_EQ(unpackBatchNo(33554432u), 0u);
  EXPECT_EQ(unpackDriverNo(67108863u), 1u);
  EXPECT_EQ(unpackBatchNo(67108863u), 33554431u);
  // A large row/batch value near the top of the batch field round-trips and
  // stays inside driver 0.
  EXPECT_EQ(unpackDriverNo(33554430u), 0u);
  EXPECT_EQ(unpackBatchNo(33554430u), 33554430u);
}

TEST_F(RetainedVectorsIndexTest, mergeFromRejectsOccupiedDriverSlot) {
  RetainedVectorsIndex index(0);
  RetainedVectorsIndex duplicateDriver(0);
  auto original = makeRowVector({makeFlatVector<int64_t>({100})});
  auto duplicate = makeRowVector({makeFlatVector<int64_t>({200})});
  index.add(original);
  duplicateDriver.add(duplicate);

  EXPECT_ANY_THROW(index.mergeFrom(std::move(duplicateDriver)));
  EXPECT_EQ(index.at(0, 0), original.get());
}

TEST_F(RetainedVectorsIndexTest, resolvesSelectedColumnsForSingleBatch) {
  RetainedVectorsIndex index(0);
  auto vector = makeRowVector({
      makeFlatVector<int64_t>({1, 2}),
      makeFlatVector<double>({1.5, 2.5}),
      makeFlatVector<std::string>({"a", "b"}),
  });
  index.add(vector);

  const auto columns = index.resolveEmitColumns({0, 2});

  ASSERT_EQ(columns.emit.size(), 2);
  ASSERT_EQ(columns.emit[0].size(), 1);
  ASSERT_EQ(columns.emit[0][0].size(), 1);
  EXPECT_EQ(columns.emit[0][0][0], index.at(0, 0)->childAt(0).get());
  EXPECT_EQ(columns.emit[1][0][0], index.at(0, 0)->childAt(2).get());
}

TEST_F(RetainedVectorsIndexTest, resolvesEachRetainedBatch) {
  RetainedVectorsIndex index(0);
  for (int64_t value = 0; value < 3; ++value) {
    index.add(makeRowVector({
        makeFlatVector<int64_t>({value}),
        makeFlatVector<double>({static_cast<double>(value)}),
    }));
  }

  const auto columns = index.resolveEmitColumns({1});

  ASSERT_EQ(columns.emit.size(), 1);
  ASSERT_EQ(columns.emit[0][0].size(), 3);
  for (uint32_t batch = 0; batch < 3; ++batch) {
    EXPECT_EQ(
        columns.emit[0][0][batch],
        index.at(0, batch)->childAt(1).get());
  }
  EXPECT_NE(columns.emit[0][0][0], columns.emit[0][0][1]);
  EXPECT_NE(columns.emit[0][0][1], columns.emit[0][0][2]);
}

TEST_F(RetainedVectorsIndexTest, preservesDriverAndProjectionOrder) {
  RetainedVectorsIndex driver0(0);
  RetainedVectorsIndex driver1(1);
  driver0.add(makeRowVector({
      makeFlatVector<int64_t>({10}),
      makeFlatVector<double>({10.5}),
      makeFlatVector<std::string>({"d0"}),
  }));
  driver1.add(makeRowVector({
      makeFlatVector<int64_t>({20}),
      makeFlatVector<double>({20.5}),
      makeFlatVector<std::string>({"d1"}),
  }));
  driver0.mergeFrom(std::move(driver1));

  const auto columns = driver0.resolveEmitColumns({2, 0});

  ASSERT_EQ(columns.emit.size(), 2);
  ASSERT_EQ(columns.emit[0].size(), 2);
  EXPECT_EQ(columns.emit[0][0][0], driver0.at(0, 0)->childAt(2).get());
  EXPECT_EQ(columns.emit[0][1][0], driver0.at(1, 0)->childAt(2).get());
  EXPECT_EQ(columns.emit[1][0][0], driver0.at(0, 0)->childAt(0).get());
  EXPECT_EQ(columns.emit[1][1][0], driver0.at(1, 0)->childAt(0).get());
}

TEST_F(RetainedVectorsIndexTest, rejectsOutOfRangeDriver) {
  EXPECT_ANY_THROW(RetainedVectorsIndex(kMaxDriverNo + 1));
}

} // namespace
} // namespace facebook::velox::exec::ch
