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
#include "velox/common/caching/filecache/FileSegmentKeyType.h"

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(FileSegmentKeyTypeTest, toStringMatchesClickHouseMagicEnumNames) {
  EXPECT_EQ(toString(FileSegmentKeyType::General), "General");
  EXPECT_EQ(toString(FileSegmentKeyType::System), "System");
  EXPECT_EQ(toString(FileSegmentKeyType::Data), "Data");
}

TEST(FileSegmentKeyTypeTest, getKeyTypePrefixMatchesClickHouse) {
  EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::General), "");
  EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::System), "System");
  EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::Data), "Data");
}

TEST(FileSegmentKeyTypeTest, invalidValueMatchesClickHouseMagicEnumEmptyName) {
  const auto invalid = static_cast<FileSegmentKeyType>(255);

  EXPECT_EQ(toString(invalid), "");
  EXPECT_EQ(getKeyTypePrefix(invalid), "");
}
