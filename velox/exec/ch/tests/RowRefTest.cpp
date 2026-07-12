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

#include "velox/exec/ch/RowRef.h"

#include <array>
#include <limits>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

TEST(RowRefTest, roundTripsCoordinates) {
  struct Coordinates {
    uint32_t driverNo;
    uint32_t batchNo;
    uint32_t rowNo;
  };
  constexpr std::array<Coordinates, 3> kCoordinates{{
      {0, 0, 0},
      {1, 42, 99},
      {17, 1234567, 987654321},
  }};

  for (const auto& coordinates : kCoordinates) {
    const auto blockNo =
        packBlockNo(coordinates.driverNo, coordinates.batchNo);
    const auto word = RowRef{blockNo, coordinates.rowNo}.encode();

    EXPECT_TRUE(refWordIsInline(word));
    EXPECT_EQ(refWordBlockNo(word), blockNo);
    EXPECT_EQ(refWordRowNo(word), coordinates.rowNo);
    EXPECT_EQ(unpackDriverNo(refWordBlockNo(word)), coordinates.driverNo);
    EXPECT_EQ(unpackBatchNo(refWordBlockNo(word)), coordinates.batchNo);
  }
}

TEST(RowRefTest, preservesMaximumCoordinatesWithoutSettingBlockInlineBit) {
  constexpr uint32_t kMaxDriverNo = (1u << kDriverNoBits) - 1;
  constexpr uint32_t kMaxBatchNo = (1u << kBatchNoBits) - 1;
  constexpr uint32_t kMaxRowNo = std::numeric_limits<uint32_t>::max();

  const auto blockNo = packBlockNo(kMaxDriverNo, kMaxBatchNo);
  const auto word = RowRef{blockNo, kMaxRowNo}.encode();

  EXPECT_EQ(blockNo & RowRef::INLINE_FLAG, 0);
  EXPECT_TRUE(refWordIsInline(word));
  EXPECT_EQ(refWordBlockNo(word), blockNo);
  EXPECT_EQ(refWordRowNo(word), kMaxRowNo);
  EXPECT_EQ(unpackDriverNo(refWordBlockNo(word)), kMaxDriverNo);
  EXPECT_EQ(unpackBatchNo(refWordBlockNo(word)), kMaxBatchNo);
}

TEST(RowRefTest, distinguishesDefaultAndInlineWords) {
  EXPECT_FALSE(refWordIsInline(0));

  const RowRef ref{0, 0};
  EXPECT_TRUE(refWordIsInline(ref.encode()));
  EXPECT_EQ(ref.blockNo(), 0);
  EXPECT_EQ(ref.rowNo(), 0);
}

// The pack/constructor guards mirror ClickHouse RowRef out-of-range check and
// only fire when assertions are enabled (Debug builds). Skip in release/NDEBUG.
#ifndef NDEBUG
TEST(RowRefDeathTest, rejectsOutOfRangeDriverNo) {
  EXPECT_DEATH(
      { (void)packBlockNo(kMaxDriverNo + 1, 0); }, "driverNo <= kMaxDriverNo");
}

TEST(RowRefDeathTest, rejectsOutOfRangeBatchNo) {
  EXPECT_DEATH(
      { (void)packBlockNo(0, kMaxBatchNo + 1); }, "batchNo <= kMaxBatchNo");
}

TEST(RowRefDeathTest, rejectsBlockNoAboveMask) {
  EXPECT_DEATH(
      { (void)RowRef((RowRef::BLOCK_NO_MASK + 1), 0); },
      "blockNo <= BLOCK_NO_MASK");
}
#endif

} // namespace
} // namespace facebook::velox::exec::ch
