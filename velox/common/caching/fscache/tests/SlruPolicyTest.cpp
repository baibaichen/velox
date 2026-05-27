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

#include "velox/common/caching/fscache/SlruPolicy.h"

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <memory>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class SlruPolicyTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath();
    remotePath_ = cacheRoot_ + "/remote.bin";
    // Large enough to back any segment requested below.
    std::ofstream out{remotePath_, std::ios::binary};
    const std::string blob(64UL * 1'024, 'a');
    out.write(blob.data(), blob.size());
  }

  // Builds a kDownloaded segment via real IO so SlruPolicy's underlying
  // LruPolicy contract (kDownloaded-only) is honored. Each call uses a
  // distinct offset.
  std::unique_ptr<FileSegment> downloaded(uint64_t size) {
    const uint64_t offset = nextOffset_;
    nextOffset_ += size;
    auto segment = std::make_unique<FileSegment>(
        FsCacheKey{PathKey::fromPath(remotePath_), offset, size}, remotePath_);
    VELOX_CHECK(segment->beginDownload());
    LocalReadFile remote{remotePath_};
    segment->download(remote, cacheRoot_);
    return segment;
  }

 private:
  uint64_t nextOffset_{0};
};

// Cites SLRUFileCachePriority.cpp:586-611 — first post-download hit on a
// probationary entry promotes it to protected, no hit counter.
TEST_F(SlruPolicyTest, probationaryFirstHitPromotesToProtected) {
  SlruPolicy policy{1'000, 0.6};
  auto seg = downloaded(100);
  policy.onInsert(seg.get());
  EXPECT_EQ(policy.probationaryBytes(), 100u);
  EXPECT_EQ(policy.protectedBytes(), 0u);

  policy.onHit(seg.get());

  EXPECT_EQ(policy.probationaryBytes(), 0u);
  EXPECT_EQ(policy.protectedBytes(), 100u);
}

// Cites SLRUFileCachePriority.cpp:398-584 — when protected has no room for a
// promoted entry, the LRU end of protected is demoted to probationary MRU
// (demotion is a move, not an evict).
TEST_F(SlruPolicyTest, protectedOverflowDemotesToProbationary) {
  // capacity=1000, ratio=0.6 -> protected cap = 600.
  SlruPolicy policy{1'000, 0.6};
  // Fill protected to capacity with three 200-byte segments.
  auto a = downloaded(200);
  auto b = downloaded(200);
  auto c = downloaded(200);
  policy.onInsert(a.get());
  policy.onHit(a.get()); // promote a
  policy.onInsert(b.get());
  policy.onHit(b.get()); // promote b
  policy.onInsert(c.get());
  policy.onHit(c.get()); // promote c
  EXPECT_EQ(policy.protectedBytes(), 600u);
  EXPECT_EQ(policy.probationaryBytes(), 0u);

  // Promote a fourth — `a` is LRU of protected, must be demoted.
  auto d = downloaded(200);
  policy.onInsert(d.get());
  policy.onHit(d.get());

  EXPECT_EQ(policy.protectedBytes(), 600u);
  EXPECT_EQ(policy.probationaryBytes(), 200u);
  // Confirm the demoted entry is `a` by triggering an eviction sized to drain
  // probationary — `a` should be the only victim.
  EXPECT_THAT(policy.selectVictims(200), testing::ElementsAre(a.get()));
}

// Cites SLRUFileCachePriority.cpp:213-247 — is_total_space_cleanup drains
// probationary first, protected second.
TEST_F(SlruPolicyTest, evictDrainsProbationaryFirst) {
  SlruPolicy policy{1'000, 0.6};
  auto p1 = downloaded(100);
  auto p2 = downloaded(100);
  auto prot1 = downloaded(100);
  auto prot2 = downloaded(100);
  policy.onInsert(p1.get());
  policy.onInsert(p2.get());
  policy.onInsert(prot1.get());
  policy.onHit(prot1.get()); // promote
  policy.onInsert(prot2.get());
  policy.onHit(prot2.get()); // promote
  ASSERT_EQ(policy.probationaryBytes(), 200u);
  ASSERT_EQ(policy.protectedBytes(), 200u);

  // Ask for 150 — must come entirely from probationary (LRU end first).
  EXPECT_THAT(
      policy.selectVictims(150), testing::ElementsAre(p1.get(), p2.get()));

  // Ask for 300 — drains all probationary then spills into protected LRU.
  EXPECT_THAT(
      policy.selectVictims(300),
      testing::ElementsAre(p1.get(), p2.get(), prot1.get()));
}

TEST_F(SlruPolicyTest, selectVictimsRespectsBytesNeeded) {
  SlruPolicy policy{1'000, 0.6};
  auto a = downloaded(100);
  auto b = downloaded(100);
  policy.onInsert(a.get());
  policy.onInsert(b.get());
  policy.onHit(b.get()); // b -> protected

  EXPECT_THAT(policy.selectVictims(0), testing::IsEmpty());
  // Larger than total — must return all entries across both queues.
  EXPECT_THAT(policy.selectVictims(10'000), testing::SizeIs(2));
}

TEST_F(SlruPolicyTest, onRemoveDropsFromCorrectList) {
  SlruPolicy policy{1'000, 0.6};
  auto a = downloaded(100);
  auto b = downloaded(100);
  policy.onInsert(a.get());
  policy.onInsert(b.get());
  policy.onHit(b.get()); // b promoted to protected
  ASSERT_EQ(policy.probationaryBytes(), 100u);
  ASSERT_EQ(policy.protectedBytes(), 100u);

  policy.onRemove(b.get()); // remove from protected
  EXPECT_EQ(policy.protectedBytes(), 0u);
  EXPECT_EQ(policy.probationaryBytes(), 100u);

  policy.onRemove(a.get()); // remove from probationary
  EXPECT_EQ(policy.probationaryBytes(), 0u);
  EXPECT_EQ(policy.protectedBytes(), 0u);
  EXPECT_THAT(policy.selectVictims(10'000), testing::IsEmpty());
}

} // namespace facebook::velox::cache::fs::test
