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

#include "velox/common/caching/fscache/LruPolicy.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

namespace {
FileSegment makeSegment(
    uint64_t size,
    FileSegment::State state = FileSegment::State::kDownloaded) {
  return FileSegment{FsCacheKey{"x", 0, size}, state};
}
} // namespace

TEST(EvictionPolicyTest, lruInsertSelectsOldestFirst) {
  LruPolicy policy;
  auto a = makeSegment(100);
  auto b = makeSegment(200);
  auto c = makeSegment(300);
  policy.onInsert(&a);
  policy.onInsert(&b);
  policy.onInsert(&c);

  // Need 250 bytes; a(100) + b(200) = 300 satisfies, c is newest so kept.
  EXPECT_THAT(policy.selectVictims(250), testing::ElementsAre(&a, &b));
}

TEST(EvictionPolicyTest, hitMovesEntryToMostRecentlyUsed) {
  LruPolicy policy;
  auto a = makeSegment(100);
  auto b = makeSegment(100);
  auto c = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);
  policy.onInsert(&c);

  policy.onHit(&a);

  // After hit on a, LRU order is b, c, a. Evicting 100 bytes picks b.
  EXPECT_THAT(policy.selectVictims(100), testing::ElementsAre(&b));
}

TEST(EvictionPolicyTest, removeDropsEntryFromTracking) {
  LruPolicy policy;
  auto a = makeSegment(100);
  auto b = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);

  policy.onRemove(&a);

  EXPECT_THAT(policy.selectVictims(100), testing::ElementsAre(&b));
}

TEST(EvictionPolicyTest, selectVictimsReturnsEmptyWhenZeroRequested) {
  LruPolicy policy;
  auto a = makeSegment(100);
  policy.onInsert(&a);
  EXPECT_THAT(policy.selectVictims(0), testing::IsEmpty());
}

TEST(EvictionPolicyTest, selectVictimsReturnsAllWhenRequestExceedsTotal) {
  LruPolicy policy;
  auto a = makeSegment(100);
  auto b = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);
  EXPECT_THAT(policy.selectVictims(10'000), testing::SizeIs(2));
}

TEST(EvictionPolicyTest, onInsertRejectsNonDownloadedSegments) {
  LruPolicy policy;
  auto downloading = makeSegment(100, FileSegment::State::kDownloading);
  auto empty = makeSegment(100, FileSegment::State::kEmpty);
  // Tracking non-kDownloaded segments would let selectVictims return a
  // segment whose download is still in-flight, racing with the writer over
  // the on-disk file.
  EXPECT_THROW(policy.onInsert(&downloading), VeloxException);
  EXPECT_THROW(policy.onInsert(&empty), VeloxException);
}

} // namespace facebook::velox::cache::fs::test
