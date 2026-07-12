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
#include "velox/exec/ch/RowRef.h"
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

TEST_F(RetainedVectorsIndexTest, rejectsOutOfRangeDriver) {
  EXPECT_ANY_THROW(RetainedVectorsIndex(kMaxDriverNo + 1));
}

} // namespace
} // namespace facebook::velox::exec::ch
