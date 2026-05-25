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

#include "velox/common/caching/fscache/FsCacheMetadata.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheKey.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <thread>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheMetadataTest, insertAndLookupSameKey) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  EXPECT_TRUE(metadata.insert(segment));

  auto found = metadata.lookup(FsCacheKey{PathKey::fromPath("p"), 0, 16});
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found.get(), segment.get());
}

TEST(FsCacheMetadataTest, lookupReturnsNullForMissingKey) {
  FsCacheMetadata metadata{8};
  EXPECT_EQ(
      metadata.lookup(FsCacheKey{PathKey::fromPath("missing"), 0, 16}),
      nullptr);
}

TEST(FsCacheMetadataTest, insertDuplicateReturnsFalse) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  auto b = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_FALSE(metadata.insert(b));
  EXPECT_EQ(
      metadata.lookup(FsCacheKey{PathKey::fromPath("p"), 0, 16}).get(),
      a.get());
}

TEST(FsCacheMetadataTest, eraseRemovesFromBucket) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  metadata.insert(segment);
  EXPECT_TRUE(metadata.erase(FsCacheKey{PathKey::fromPath("p"), 0, 16}));
  EXPECT_EQ(
      metadata.lookup(FsCacheKey{PathKey::fromPath("p"), 0, 16}), nullptr);
}

TEST(FsCacheMetadataTest, keysWithSamePathDifferentOffsetCoexist) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  auto b = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 16, 16}, "p");
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_TRUE(metadata.insert(b));
  EXPECT_EQ(
      metadata.lookup(FsCacheKey{PathKey::fromPath("p"), 0, 16}).get(),
      a.get());
  EXPECT_EQ(
      metadata.lookup(FsCacheKey{PathKey::fromPath("p"), 16, 16}).get(),
      b.get());
}

TEST(FsCacheMetadataTest, ctorRejectsNonPowerOfTwoBuckets) {
  // 6 is not a power of two; the bucketMask folding requires it.
  EXPECT_THROW(FsCacheMetadata{6}, ::facebook::velox::VeloxException);
  EXPECT_THROW(FsCacheMetadata{0}, ::facebook::velox::VeloxException);
  EXPECT_NO_THROW(FsCacheMetadata{1});
  EXPECT_NO_THROW(FsCacheMetadata{1'024});
}

TEST(FsCacheMetadataTest, snapshotReturnsAllSegments) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("p"), 0, 16}, "p");
  auto b = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("q"), 32, 16}, "q");
  metadata.insert(a);
  metadata.insert(b);
  EXPECT_THAT(metadata.snapshot(), testing::UnorderedElementsAre(a, b));
}

TEST(FsCacheMetadataTest, samePathSegmentsCoLocate) {
  FsCacheMetadata md{16};
  auto seg0 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x");
  auto seg1 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 4'096, 4'096}, "/x");
  EXPECT_TRUE(md.insert(seg0));
  EXPECT_TRUE(md.insert(seg1));
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4'096}), seg0);
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 4'096, 4'096}), seg1);
}

TEST(FsCacheMetadataTest, eraseLastSegmentDropsKeyMetadata) {
  FsCacheMetadata md{16};
  auto seg = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x");
  md.insert(seg);
  EXPECT_TRUE(md.erase(FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}));
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4'096}), nullptr);
  auto seg2 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x");
  EXPECT_TRUE(md.insert(seg2));
}

TEST(FsCacheMetadataTest, insertDuplicateOffsetFails) {
  FsCacheMetadata md{16};
  auto seg0 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x");
  auto seg1 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x");
  EXPECT_TRUE(md.insert(seg0));
  EXPECT_FALSE(md.insert(seg1));
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4'096}), seg0);
}

TEST(FsCacheMetadataTest, snapshotReturnsAllSegmentsAcrossKeys) {
  FsCacheMetadata md{16};
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4'096}, "/x"));
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/y"), 0, 4'096}, "/y"));
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 4'096, 4'096}, "/x"));
  const auto all = md.snapshot();
  EXPECT_EQ(all.size(), 3u);
}

// Regression test for the orphan-KeyMetadata race: an insert that grabs a
// KeyMetadataPtr just before a concurrent erase drops the entry from the
// bucket must not write its segment into the orphaned KeyMetadata. With the
// fix (bucket held across key for insert/erase), the schedule below
// serializes: either insert lands first and erase finds two segments and
// removes only one, or erase lands first and insert recreates the bucket
// entry. Either way the resulting segment must be reachable via lookup.
TEST(FsCacheMetadataTest, concurrentEraseAndInsertSamePathNoOrphan) {
  for (int iter = 0; iter < 200; ++iter) {
    FsCacheMetadata md{16};
    auto seg0 = std::make_shared<FileSegment>(
        FsCacheKey{PathKey::fromPath("/r"), 0, 4'096}, "/r");
    md.insert(seg0);

    auto seg1 = std::make_shared<FileSegment>(
        FsCacheKey{PathKey::fromPath("/r"), 4'096, 4'096}, "/r");

    std::thread t1{[&] { md.erase({PathKey::fromPath("/r"), 0, 4'096}); }};
    std::thread t2{[&] { md.insert(seg1); }};
    t1.join();
    t2.join();

    EXPECT_EQ(md.lookup({PathKey::fromPath("/r"), 4'096, 4'096}), seg1)
        << "iteration " << iter;
  }
}

} // namespace facebook::velox::cache::fs::test
