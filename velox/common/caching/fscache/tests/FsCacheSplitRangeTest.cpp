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

#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/caching/fscache/FsCacheConfig.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

namespace {
constexpr uint64_t kAlign = 4UL * 1'024 * 1'024;
constexpr uint64_t kMaxSeg = 32UL * 1'024 * 1'024;

FsCacheConfig defaultConfig() {
  FsCacheConfig config;
  config.cacheRoot = "/tmp/unused";
  config.alignment = kAlign;
  config.maxSegmentSize = kMaxSeg;
  return config;
}
} // namespace

TEST(FsCacheSplitRangeTest, alignedRangeWithinMaxYieldsOneSegment) {
  const auto ranges = FsCache::splitRange(0, kAlign, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kAlign);
}

TEST(FsCacheSplitRangeTest, unalignedStartExpandsDown) {
  const auto ranges = FsCache::splitRange(100, 1'024, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kAlign);
}

TEST(FsCacheSplitRangeTest, unalignedEndExpandsUp) {
  const auto ranges = FsCache::splitRange(0, kAlign + 1, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, 2 * kAlign);
}

TEST(FsCacheSplitRangeTest, holeLargerThanMaxSplits) {
  // 64 MiB hole split at the 32 MiB max-segment boundary.
  const auto ranges =
      FsCache::splitRange(0, 64UL * 1'024 * 1'024, defaultConfig());
  ASSERT_EQ(ranges.size(), 2);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kMaxSeg);
  EXPECT_EQ(ranges[1].first, kMaxSeg);
  EXPECT_EQ(ranges[1].second, kMaxSeg);
}

TEST(FsCacheSplitRangeTest, holeOnSegmentBoundaryDoesNotProduceEmpty) {
  const auto ranges = FsCache::splitRange(0, 2 * kMaxSeg, defaultConfig());
  ASSERT_EQ(ranges.size(), 2);
  for (const auto& range : ranges) {
    EXPECT_GT(range.second, 0);
  }
}

TEST(FsCacheSplitRangeTest, smallTailKeepsAlignmentExpansion) {
  // Read [0, kAlign + 100) -> aligned-up end = 2*kAlign. Single segment of
  // 2*kAlign (well under kMaxSeg) covers the full aligned region.
  const auto ranges = FsCache::splitRange(0, kAlign + 100, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].second, 2 * kAlign);
}

TEST(FsCacheSplitRangeTest, zeroSizeReturnsEmpty) {
  // size==0 must short-circuit. Without the early return, outward alignment
  // would emit a [alignedStart, alignedStart) phantom that downstream code
  // would persist as a 0-byte cache file.
  EXPECT_TRUE(FsCache::splitRange(0, 0, defaultConfig()).empty());
  EXPECT_TRUE(
      FsCache::splitRange(7 * kAlign + 12'345, 0, defaultConfig()).empty());
}

} // namespace facebook::velox::cache::fs::test
