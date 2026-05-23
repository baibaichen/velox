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

namespace facebook::velox::cache::fs::test {

TEST(FsCacheMetadataTest, insertAndLookupSameKey) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  EXPECT_TRUE(metadata.insert(segment));

  auto found = metadata.lookup(FsCacheKey{"p", 0, 16});
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found.get(), segment.get());
}

TEST(FsCacheMetadataTest, lookupReturnsNullForMissingKey) {
  FsCacheMetadata metadata{8};
  EXPECT_EQ(metadata.lookup(FsCacheKey{"missing", 0, 16}), nullptr);
}

TEST(FsCacheMetadataTest, insertDuplicateReturnsFalse) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  auto b = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_FALSE(metadata.insert(b));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}).get(), a.get());
}

TEST(FsCacheMetadataTest, eraseRemovesFromBucket) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  metadata.insert(segment);
  EXPECT_TRUE(metadata.erase(FsCacheKey{"p", 0, 16}));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}), nullptr);
}

TEST(FsCacheMetadataTest, keysWithSamePathDifferentOffsetCoexist) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  auto b = std::make_shared<FileSegment>(FsCacheKey{"p", 16, 16});
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_TRUE(metadata.insert(b));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}).get(), a.get());
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 16, 16}).get(), b.get());
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
  auto a = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  auto b = std::make_shared<FileSegment>(FsCacheKey{"q", 32, 16});
  metadata.insert(a);
  metadata.insert(b);
  EXPECT_THAT(
      metadata.snapshot(),
      testing::UnorderedElementsAre(a, b));
}

} // namespace facebook::velox::cache::fs::test
