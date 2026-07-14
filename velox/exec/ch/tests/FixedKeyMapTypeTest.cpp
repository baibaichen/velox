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

#include "velox/exec/ch/FixedKeyMap.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

using Type = FixedKeyMap::Type;

TEST(FixedKeyMapTypeTest, singleBigintIsKey64) {
  EXPECT_EQ(FixedKeyMap::chooseType({BIGINT()}), Type::key64);
}

TEST(FixedKeyMapTypeTest, twoBigintsAreKeys128) {
  EXPECT_EQ(FixedKeyMap::chooseType({BIGINT(), BIGINT()}), Type::keys128);
}

TEST(FixedKeyMapTypeTest, bigintPlusTwoIntegersAreKeys128) {
  EXPECT_EQ(
      FixedKeyMap::chooseType({BIGINT(), INTEGER(), INTEGER()}),
      Type::keys128);
}

TEST(FixedKeyMapTypeTest, fiveBigintsFallBackToHashed) {
  EXPECT_EQ(
      FixedKeyMap::chooseType(
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()}),
      Type::hashed);
}

TEST(FixedKeyMapTypeTest, singleVarcharIsKeyString) {
  EXPECT_EQ(FixedKeyMap::chooseType({VARCHAR()}), Type::key_string);
}

TEST(FixedKeyMapTypeTest, integerPlusVarcharIsHashed) {
  EXPECT_EQ(FixedKeyMap::chooseType({INTEGER(), VARCHAR()}), Type::hashed);
}

TEST(FixedKeyMapTypeTest, singleNarrowIntegersRouteByWidth) {
  EXPECT_EQ(FixedKeyMap::chooseType({TINYINT()}), Type::key8);
  EXPECT_EQ(FixedKeyMap::chooseType({SMALLINT()}), Type::key16);
  EXPECT_EQ(FixedKeyMap::chooseType({INTEGER()}), Type::key32);
}

TEST(FixedKeyMapTypeTest, multipleFixedIntegersPackByTotalBytes) {
  // 2 + 2 = 4 bytes -> keys32.
  EXPECT_EQ(FixedKeyMap::chooseType({SMALLINT(), SMALLINT()}), Type::keys32);
  // 4 + 4 = 8 bytes -> keys64.
  EXPECT_EQ(FixedKeyMap::chooseType({INTEGER(), INTEGER()}), Type::keys64);
}

} // namespace
} // namespace facebook::velox::exec::ch
