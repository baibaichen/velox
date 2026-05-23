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

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <gtest/gtest.h>
#include <unordered_map>
#include <unordered_set>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheKeyTest, equalityAndInequality) {
  FsCacheKey a{"s3://bucket/file", 0, 4096};
  FsCacheKey b{"s3://bucket/file", 0, 4096};
  FsCacheKey c{"s3://bucket/file", 4096, 4096};
  FsCacheKey d{"s3://bucket/other", 0, 4096};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
}

TEST(FsCacheKeyTest, hashStableAcrossInstances) {
  FsCacheKey a{"s3://bucket/file", 0, 4096};
  FsCacheKey b{"s3://bucket/file", 0, 4096};
  EXPECT_EQ(a.hash(), b.hash());
}

TEST(FsCacheKeyTest, hashDiffersWhenAnyFieldDiffers) {
  FsCacheKey base{"s3://bucket/file", 0, 4096};
  FsCacheKey differentPath{"s3://bucket/other", 0, 4096};
  FsCacheKey differentOffset{"s3://bucket/file", 4096, 4096};
  FsCacheKey differentSize{"s3://bucket/file", 0, 8192};

  std::unordered_set<uint64_t> hashes{
      base.hash(),
      differentPath.hash(),
      differentOffset.hash(),
      differentSize.hash(),
  };
  EXPECT_EQ(hashes.size(), 4);
}

TEST(FsCacheKeyTest, fileNameContainsHexHashOffsetSize) {
  FsCacheKey k{"s3://bucket/file", 0, 4096};
  const std::string name = k.fileName();
  // 64-bit hash printed as 16 hex chars, then ".0.4096".
  EXPECT_EQ(name.size(), 16 + std::string{".0.4096"}.size());
  EXPECT_NE(name.find(".0.4096"), std::string::npos);
}

TEST(FsCacheKeyTest, usableInStdUnorderedMap) {
  std::unordered_map<FsCacheKey, int, FsCacheKeyHash> m;
  m.emplace(FsCacheKey{"p", 0, 16}, 1);
  m.emplace(FsCacheKey{"p", 16, 16}, 2);
  EXPECT_EQ(m.size(), 2);
  EXPECT_EQ(m.at(FsCacheKey{"p", 0, 16}), 1);
}

} // namespace facebook::velox::cache::fs::test
