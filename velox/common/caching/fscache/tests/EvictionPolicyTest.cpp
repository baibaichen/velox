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
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class EvictionPolicyTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath();
    remotePath_ = cacheRoot_ + "/remote.bin";
    // 4 KiB is enough for any size used in tests below.
    std::ofstream out{remotePath_, std::ios::binary};
    const std::string blob(4'096, 'a');
    out.write(blob.data(), blob.size());
  }

  // Builds a segment and brings it to kDownloaded via real IO so the policy
  // contract (only kDownloaded segments may be tracked) is honored. Each
  // call uses a distinct offset to keep on-disk paths unique.
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

TEST_F(EvictionPolicyTest, lruInsertSelectsOldestFirst) {
  LruPolicy policy;
  auto a = downloaded(100);
  auto b = downloaded(200);
  auto c = downloaded(300);
  policy.onInsert(a.get());
  policy.onInsert(b.get());
  policy.onInsert(c.get());

  // Need 250 bytes; a(100) + b(200) = 300 satisfies, c is newest so kept.
  EXPECT_THAT(
      policy.selectVictims(250), testing::ElementsAre(a.get(), b.get()));
}

TEST_F(EvictionPolicyTest, hitMovesEntryToMostRecentlyUsed) {
  LruPolicy policy;
  auto a = downloaded(100);
  auto b = downloaded(100);
  auto c = downloaded(100);
  policy.onInsert(a.get());
  policy.onInsert(b.get());
  policy.onInsert(c.get());

  policy.onHit(a.get());

  // After hit on a, LRU order is b, c, a. Evicting 100 bytes picks b.
  EXPECT_THAT(policy.selectVictims(100), testing::ElementsAre(b.get()));
}

TEST_F(EvictionPolicyTest, removeDropsEntryFromTracking) {
  LruPolicy policy;
  auto a = downloaded(100);
  auto b = downloaded(100);
  policy.onInsert(a.get());
  policy.onInsert(b.get());

  policy.onRemove(a.get());

  EXPECT_THAT(policy.selectVictims(100), testing::ElementsAre(b.get()));
}

TEST_F(EvictionPolicyTest, selectVictimsReturnsEmptyWhenZeroRequested) {
  LruPolicy policy;
  auto a = downloaded(100);
  policy.onInsert(a.get());
  EXPECT_THAT(policy.selectVictims(0), testing::IsEmpty());
}

TEST_F(EvictionPolicyTest, selectVictimsReturnsAllWhenRequestExceedsTotal) {
  LruPolicy policy;
  auto a = downloaded(100);
  auto b = downloaded(100);
  policy.onInsert(a.get());
  policy.onInsert(b.get());
  EXPECT_THAT(policy.selectVictims(10'000), testing::SizeIs(2));
}

TEST_F(EvictionPolicyTest, onInsertRejectsNonDownloadedSegments) {
  LruPolicy policy;
  // Tracking non-kDownloaded segments would let selectVictims return a
  // segment whose download is still in-flight, racing with the writer over
  // the on-disk file.
  FileSegment empty{
      FsCacheKey{PathKey::fromPath(remotePath_), 0, 100}, remotePath_};
  FileSegment downloading{
      FsCacheKey{PathKey::fromPath(remotePath_), 100, 100}, remotePath_};
  ASSERT_TRUE(downloading.beginDownload());
  EXPECT_THROW(policy.onInsert(&empty), VeloxException);
  EXPECT_THROW(policy.onInsert(&downloading), VeloxException);
}

} // namespace facebook::velox::cache::fs::test
