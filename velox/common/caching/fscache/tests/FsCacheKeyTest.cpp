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
#include <cstring>
#include <functional>
#include <unordered_map>
#include <unordered_set>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheKeyTest, equalityAndInequality) {
  FsCacheKey a{PathKey::fromPath("s3://bucket/file"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("s3://bucket/file"), 0, 4096};
  FsCacheKey c{PathKey::fromPath("s3://bucket/file"), 4096, 4096};
  FsCacheKey d{PathKey::fromPath("s3://bucket/other"), 0, 4096};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
}

TEST(FsCacheKeyTest, hashStableAcrossInstances) {
  FsCacheKey a{PathKey::fromPath("s3://bucket/file"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("s3://bucket/file"), 0, 4096};
  EXPECT_EQ(a.hash(), b.hash());
}

TEST(FsCacheKeyTest, hashIsPathOnlyAcrossOffsets) {
  FsCacheKey a{PathKey::fromPath("/data/x"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("/data/x"), 8192, 4096};
  EXPECT_EQ(a.hash(), b.hash())
      << "Phase-2: hash() depends on path only, not (offset, size)";
}

TEST(FsCacheKeyTest, hashDiffersByPath) {
  FsCacheKey a{PathKey::fromPath("/data/x"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("/data/y"), 0, 4096};
  EXPECT_NE(a.hash(), b.hash());
}

TEST(FsCacheKeyTest, fileNameSchemaUnchanged) {
  FsCacheKey key{PathKey::fromPath("/data/x"), 1234, 4096};
  const std::string name = key.fileName();
  // "<16-hex>.<offset>.<size>"
  EXPECT_EQ(name.size(), 16 + 1 + 4 + 1 + 4);
  EXPECT_EQ(name[16], '.');
  EXPECT_EQ(name.substr(17, 4), "1234");
  EXPECT_EQ(name.substr(22), "4096");
}

TEST(FsCacheKeyTest, usableInStdUnorderedMap) {
  std::unordered_map<FsCacheKey, int, FsCacheKeyHash> m;
  m.emplace(FsCacheKey{PathKey::fromPath("p"), 0, 16}, 1);
  m.emplace(FsCacheKey{PathKey::fromPath("p"), 16, 16}, 2);
  EXPECT_EQ(m.size(), 2);
  EXPECT_EQ(m.at(FsCacheKey{PathKey::fromPath("p"), 0, 16}), 1);
}

TEST(PathKeyTest, samePathYieldsSameHex) {
  const PathKey a = PathKey::fromPath("/data/file.parquet");
  const PathKey b = PathKey::fromPath("/data/file.parquet");
  EXPECT_EQ(a, b);
  EXPECT_EQ(a.hex().size(), 16);
  for (char c : a.hex()) {
    EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }
}

TEST(PathKeyTest, differentPathsYieldDifferentHex) {
  const PathKey a = PathKey::fromPath("/data/a");
  const PathKey b = PathKey::fromPath("/data/b");
  EXPECT_NE(a, b);
}

TEST(PathKeyTest, stdHashMatchesFirst8Bytes) {
  const PathKey k = PathKey::fromPath("/x");
  uint64_t expected{0};
  std::memcpy(&expected, k.hex().data(), sizeof(expected));
  EXPECT_EQ(std::hash<PathKey>{}(k), static_cast<size_t>(expected));
}

} // namespace facebook::velox::cache::fs::test
