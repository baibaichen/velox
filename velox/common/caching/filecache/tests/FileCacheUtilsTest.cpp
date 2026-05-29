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
#include "velox/common/caching/filecache/FileCacheUtils.h"

#include <cstddef>
#include <limits>

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(FileCacheUtilsTest, roundDownReturnsInputForZeroMultiple) {
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(0, 0), 0);
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(17, 0), 17);
}

TEST(FileCacheUtilsTest, roundDownKeepsExactMultiples) {
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(0, 8), 0);
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(16, 8), 16);
}

TEST(FileCacheUtilsTest, roundDownTruncatesToPreviousMultiple) {
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(17, 8), 16);
  EXPECT_EQ(FileCacheUtils::roundDownToMultiple(7, 8), 0);
}

TEST(FileCacheUtilsTest, roundDownHandlesLargeValues) {
  constexpr size_t value = std::numeric_limits<size_t>::max() - 1024;
  constexpr size_t multiple = 4096;

  EXPECT_EQ(
      FileCacheUtils::roundDownToMultiple(value, multiple),
      (value / multiple) * multiple);
}

TEST(FileCacheUtilsTest, roundUpReturnsInputForZeroMultiple) {
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 0), 0);
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(17, 0), 17);
}

TEST(FileCacheUtilsTest, roundUpKeepsExactMultiples) {
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 8), 0);
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(16, 8), 16);
}

TEST(FileCacheUtilsTest, roundUpExtendsToNextMultiple) {
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(17, 8), 24);
  EXPECT_EQ(FileCacheUtils::roundUpToMultiple(1, 8), 8);
}

TEST(FileCacheUtilsTest, roundUpHandlesLargeValues) {
  constexpr size_t value = std::numeric_limits<size_t>::max() - 8190;
  constexpr size_t multiple = 4096;

  EXPECT_EQ(
      FileCacheUtils::roundUpToMultiple(value, multiple),
      ((value + multiple - 1) / multiple) * multiple);
}
