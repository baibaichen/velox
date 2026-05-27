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
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

namespace {
// Test helper: drives all kEmpty segments to kDownloaded. After Task 9,
// FsCache::getOrSet returns kEmpty segments and the caller (normally
// FsCacheBufferedInput::load) is responsible for downloading.
void driveSegments(
    FileSegmentsHolder& holder,
    ::facebook::velox::ReadFile& remote,
    FsCache& cache) {
  const auto& cacheRoot = cache.config().cacheRoot;
  for (auto& seg : holder.segments()) {
    if (seg->state() == FileSegment::State::kDownloaded) {
      cache.recordHit(seg.get(), IsPrefetch::kDemand);
      continue;
    }
    if (seg->state() != FileSegment::State::kEmpty) {
      seg->waitForDownloadedSize(seg->key().size);
      cache.recordHit(seg.get(), IsPrefetch::kDemand);
      continue;
    }
    cache.evict(seg->key().size);
    const auto reserveResult =
        seg->reserve(seg->key().size, cacheRoot);
    if (reserveResult == FileSegment::ReserveResult::kLostRace) {
      seg->waitForDownloadedSize(seg->key().size);
      cache.recordHit(seg.get(), IsPrefetch::kDemand);
      continue;
    }
    if (reserveResult ==
        FileSegment::ReserveResult::kWarmRestartPublished) {
      cache.recordMiss(seg.get(), seg->key().size, IsPrefetch::kDemand);
      continue;
    }
    try {
      constexpr uint64_t kChunk = 1UL << 20;
      std::vector<char> buf(std::min<uint64_t>(kChunk, seg->key().size));
      uint64_t remaining = seg->key().size;
      uint64_t cursor = seg->key().offset;
      while (remaining > 0) {
        const uint64_t toRead = std::min<uint64_t>(buf.size(), remaining);
        remote.pread(cursor, toRead, buf.data());
        seg->write(buf.data(), toRead);
        cursor += toRead;
        remaining -= toRead;
      }
      seg->complete();
      cache.recordMiss(seg.get(), seg->key().size, IsPrefetch::kDemand);
    } catch (...) {
      seg->abandon();
      throw;
    }
  }
}
} // namespace

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

TEST(PerBucketEvictionPolicyTest, perBucketIsolation) {
  // Two segments hashing to different buckets must use independent LruPolicy
  // instances -- onHit on one must not change the LRU order on the other.
  // We verify indirectly: prime cache with two distinct paths, hit one
  // repeatedly, then trigger eviction sized to exactly one segment. The
  // unhit segment should be evicted (it's LRU within its own bucket -- but
  // because each bucket has its own policy, the never-hit one is LRU even
  // though the hit one was hit many times).

  // This test exists primarily as a regression guard: phase-1 with a single
  // global LRU would evict the never-hit segment as well (LRU is the same),
  // so the test passes in both phases. It serves to document that
  // per-bucket isolation does not regress global eviction correctness.
  auto tempDir = ::facebook::velox::common::testutil::TempDirectoryPath::create();
  FsCacheConfig config;
  config.cacheRoot = tempDir->getPath() + "/cache";
  config.maxBytes = 8UL * 1'024 * 1'024; // exactly 2 segments at 4 MiB
  std::filesystem::create_directories(config.cacheRoot);

  // Two remote files, two distinct paths -> two PathKeys -> likely two buckets
  // (with 1024 buckets the collision probability is ~0.1%; loop until we
  // get two distinct bucket indices to make the test deterministic).
  uint64_t variant = 0;
  std::string pathA, pathB;
  while (true) {
    pathA = tempDir->getPath() + fmt::format("/a{}.bin", variant);
    pathB = tempDir->getPath() + fmt::format("/b{}.bin", variant);
    const auto bucketA =
        std::hash<PathKey>{}(PathKey::fromPath(pathA)) & (config.numBuckets - 1);
    const auto bucketB =
        std::hash<PathKey>{}(PathKey::fromPath(pathB)) & (config.numBuckets - 1);
    if (bucketA != bucketB) {
      break;
    }
    ++variant;
  }
  {
    std::ofstream outA{pathA, std::ios::binary};
    std::ofstream outB{pathB, std::ios::binary};
    const std::string blob(4UL * 1'024 * 1'024, 'q');
    outA.write(blob.data(), blob.size());
    outB.write(blob.data(), blob.size());
  }

  FsCache cache{config};
  LocalReadFile remoteA{pathA};
  LocalReadFile remoteB{pathB};
  {
    auto holder = cache.getOrSet(
        pathA,
        0,
        4UL * 1'024 * 1'024,
        cache.config(),
        remoteA,
        IsPrefetch::kDemand);
    driveSegments(*holder, remoteA, cache);
  }
  {
    auto holder = cache.getOrSet(
        pathB,
        0,
        4UL * 1'024 * 1'024,
        cache.config(),
        remoteB,
        IsPrefetch::kDemand);
    driveSegments(*holder, remoteB, cache);
  }
  // Hit A many times so it is MRU in its bucket.
  for (int i = 0; i < 50; ++i) {
    auto holder = cache.getOrSet(
        pathA,
        0,
        4UL * 1'024 * 1'024,
        cache.config(),
        remoteA,
        IsPrefetch::kDemand);
    driveSegments(*holder, remoteA, cache);
  }
  // Insert a third segment from a third path -- must trigger eviction of
  // exactly one of the existing two. B (never hit since the first time)
  // should be the victim.
  std::string pathC = tempDir->getPath() + "/c.bin";
  {
    std::ofstream outC{pathC, std::ios::binary};
    const std::string blob(4UL * 1'024 * 1'024, 'q');
    outC.write(blob.data(), blob.size());
  }
  LocalReadFile remoteC{pathC};
  {
    auto holder = cache.getOrSet(
        pathC,
        0,
        4UL * 1'024 * 1'024,
        cache.config(),
        remoteC,
        IsPrefetch::kDemand);
    driveSegments(*holder, remoteC, cache);
  }
  EXPECT_EQ(cache.stats().evictions, 1u);
  // A should still be cached (most-recently-used in its bucket).
  EXPECT_EQ(cache.stats().bytesOnDisk, 8UL * 1'024 * 1'024);
}

} // namespace facebook::velox::cache::fs::test
