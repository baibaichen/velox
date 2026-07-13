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

#include "velox/exec/ch/ChHashBuild.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/RowRef.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class ChHashBuildTest : public testing::Test,
                        public velox::test::VectorTestBase {
 protected:
  static constexpr uint32_t kDriverNo = 5;
  static constexpr int64_t kDuplicateKey = 42;

  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  RowVectorPtr makeInput(int64_t keyBase, int32_t duplicateRows) {
    constexpr vector_size_t kSize = 128;
    return makeRowVector({
        makeFlatVector<int64_t>(kSize, [=](auto row) {
          return row < duplicateRows ? kDuplicateKey : keyBase + row;
        }),
        makeFlatVector<int64_t>(kSize, [=](auto row) {
          return keyBase * 10 + row;
        }),
    });
  }

  static int64_t keyAt(const RowVector* vector, vector_size_t row) {
    return vector->childAt(0)->asFlatVector<int64_t>()->valueAt(row);
  }
};

TEST_F(ChHashBuildTest, reservesForExpectedDistinctKeys) {
  ChHashBuild build(kDriverNo, 0, pool());

  build.reserve(4'096);

  EXPECT_EQ(build.rowsByKey().getBufferSizeInCells(), 8'192);
  EXPECT_TRUE(build.rowsByKey().empty());
}

TEST_F(ChHashBuildTest, prepareJoinTableOnlyCreatesProbeableKeys) {
  ChHashBuild build(kDriverNo, 0, pool());
  auto input = makeInput(1'000, 8);
  const auto inputUseCount = input.use_count();
  auto keyVector = input->childAt(0)->loadedVector();
  SelectivityVector rows(input->size());
  DecodedVector decodedKey(*keyVector, rows);

  build.prepareJoinTable(decodedKey, rows);

  const auto* duplicateCell = build.rowsByKey().find(kDuplicateKey);
  ASSERT_NE(duplicateCell, nullptr);
  EXPECT_EQ(duplicateCell->getMapped().rows(), 0);
  EXPECT_NE(build.rowsByKey().find(1'008), nullptr);
  EXPECT_EQ(input.use_count(), inputUseCount);
}

TEST_F(ChHashBuildTest, addRowReferencesAttachesCoordinatesAndRetainsInput) {
  ChHashBuild build(kDriverNo, 0, pool());
  auto input = makeInput(1'000, 8);
  const auto inputUseCount = input.use_count();
  auto keyVector = input->childAt(0)->loadedVector();
  SelectivityVector rows(input->size());
  DecodedVector decodedKey(*keyVector, rows);
  build.prepareJoinTable(decodedKey, rows);

  build.addRowReferences(input, decodedKey, rows);

  EXPECT_EQ(input.use_count(), inputUseCount + 1);
  EXPECT_EQ(build.retainedIndex().at(kDriverNo, 0), input.get());
  const auto* duplicateCell = build.rowsByKey().find(kDuplicateKey);
  ASSERT_NE(duplicateCell, nullptr);
  EXPECT_EQ(duplicateCell->getMapped().rows(), 8);
  for (const auto refWord : duplicateCell->getMapped()) {
    EXPECT_EQ(unpackDriverNo(refWordBlockNo(refWord)), kDriverNo);
    EXPECT_EQ(unpackBatchNo(refWordBlockNo(refWord)), 0);
    EXPECT_LT(refWordRowNo(refWord), 8);
  }
}

TEST_F(ChHashBuildTest, coordinatesResolveToOriginalRowsAcrossBatches) {
  ChHashBuild build(kDriverNo, 0, pool());
  auto first = makeInput(1'000, 8);
  auto second = makeInput(2'000, 5);
  build.addInput(first);
  build.addInput(second);

  uint32_t decodedRows = 0;
  for (const auto& cell : build.rowsByKey()) {
    const auto mapKey = cell.getKey();
    const auto& mapped = cell.getMapped();
    for (const auto refWord : mapped) {
      ASSERT_TRUE(refWordIsInline(refWord));
      const auto blockNo = refWordBlockNo(refWord);
      const auto driverNo = unpackDriverNo(blockNo);
      const auto batchNo = unpackBatchNo(blockNo);
      const auto rowNo = refWordRowNo(refWord);
      const auto* retained = build.retainedIndex().at(driverNo, batchNo);

      EXPECT_EQ(driverNo, kDriverNo);
      ASSERT_LT(rowNo, retained->size());
      EXPECT_EQ(static_cast<uint64_t>(keyAt(retained, rowNo)), mapKey);
      ++decodedRows;
    }
  }

  EXPECT_EQ(decodedRows, first->size() + second->size());
}

TEST_F(ChHashBuildTest, duplicateKeyUsesRowRefListChain) {
  ChHashBuild build(kDriverNo, 0, pool());
  auto first = makeInput(1'000, 8);
  auto second = makeInput(2'000, 5);
  build.addInput(first);
  build.addInput(second);

  const auto* cell = build.rowsByKey().find(kDuplicateKey);
  ASSERT_NE(cell, nullptr);
  EXPECT_FALSE(cell->getMapped().isInline());
  EXPECT_EQ(cell->getMapped().rows(), 13);

  uint32_t matches = 0;
  for (const auto refWord : cell->getMapped()) {
    const auto blockNo = refWordBlockNo(refWord);
    const auto* retained = build.retainedIndex().at(
        unpackDriverNo(blockNo), unpackBatchNo(blockNo));
    EXPECT_EQ(keyAt(retained, refWordRowNo(refWord)), kDuplicateKey);
    ++matches;
  }
  EXPECT_EQ(matches, 13);
}

TEST_F(ChHashBuildTest, retainsPayloadWithoutCopying) {
  ChHashBuild build(kDriverNo, 0, pool());
  auto first = makeInput(1'000, 8);
  auto second = makeInput(2'000, 5);
  const auto* firstPayload = first->childAt(1).get();
  const auto* secondPayload = second->childAt(1).get();
  const auto firstUseCount = first.use_count();
  const auto secondUseCount = second.use_count();

  build.addInput(first);
  build.addInput(second);

  EXPECT_EQ(first.use_count(), firstUseCount + 1);
  EXPECT_EQ(second.use_count(), secondUseCount + 1);
  EXPECT_EQ(build.retainedIndex().at(kDriverNo, 0), first.get());
  EXPECT_EQ(build.retainedIndex().at(kDriverNo, 1), second.get());
  EXPECT_EQ(
      build.retainedIndex().at(kDriverNo, 0)->childAt(1).get(), firstPayload);
  EXPECT_EQ(
      build.retainedIndex().at(kDriverNo, 1)->childAt(1).get(), secondPayload);
}

TEST_F(ChHashBuildTest, rejectsInputAfterNoMoreInput) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.noMoreInput();

  EXPECT_FALSE(build.needsInput());
  EXPECT_ANY_THROW(build.addInput(makeInput(1'000, 0)));
}

} // namespace
} // namespace facebook::velox::exec::ch
