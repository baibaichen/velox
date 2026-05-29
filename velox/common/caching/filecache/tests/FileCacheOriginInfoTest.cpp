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
#include "velox/common/caching/filecache/FileCacheOriginInfo.h"

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(FileCacheOriginInfoTest, defaultConstructorMatchesClickHouseDefaults) {
  const FileCacheOriginInfo info;

  EXPECT_EQ(info.userId, "");
  EXPECT_EQ(info.weight, std::nullopt);
  EXPECT_EQ(info.segmentType, FileCacheOriginInfo::SegmentKeyType::General);
}

TEST(FileCacheOriginInfoTest, userIdConstructorSetsUserIdOnly) {
  const FileCacheOriginInfo info("user");

  EXPECT_EQ(info.userId, "user");
  EXPECT_EQ(info.weight, std::nullopt);
}

TEST(FileCacheOriginInfoTest, fullConstructorSetsAllFields) {
  const FileCacheOriginInfo info(
      "user", 42, FileCacheOriginInfo::SegmentKeyType::Data);

  EXPECT_EQ(info.userId, "user");
  EXPECT_EQ(info.weight, 42);
  EXPECT_EQ(info.segmentType, FileCacheOriginInfo::SegmentKeyType::Data);
}

TEST(FileCacheOriginInfoTest, equalityComparesOnlyUserId) {
  const FileCacheOriginInfo lhs(
      "same", 1, FileCacheOriginInfo::SegmentKeyType::General);
  const FileCacheOriginInfo rhs(
      "same", 2, FileCacheOriginInfo::SegmentKeyType::Data);
  const FileCacheOriginInfo different(
      "different", 1, FileCacheOriginInfo::SegmentKeyType::General);

  EXPECT_EQ(lhs, rhs);
  EXPECT_NE(lhs, different);
}
