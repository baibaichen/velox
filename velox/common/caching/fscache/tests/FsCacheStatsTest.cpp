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

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::cache::fs {

// FsCacheStats now exposes 6 POD uint64_t fields per spec §6.3 Shape β
// (prefetch/demand split + retained evictions + bytesOnDisk). Updates happen
// through FsCache::recordHit / FsCache::recordMiss MEMBER methods against the
// private AtomicCounters; tests observe via cache.stats() which returns a
// copyable POD snapshot.

namespace {

using ::facebook::velox::common::testutil::TempDirectoryPath;

// Anchors the cache root + a small remote file used by every helper segment.
// One Fixture per TEST keeps remote / temp lifetimes scoped to the test.
struct Fixture {
  std::shared_ptr<TempDirectoryPath> tempDir;
  std::string remotePath;
  // Total bytes available on the remote stub. Sized big enough for the
  // 80'000 distinct segments built by concurrentIncrementsAreLossless.
  static constexpr uint64_t kRemoteBytes = 8UL * 1'024 * 1'024;

  Fixture() {
    tempDir = TempDirectoryPath::create();
    remotePath = tempDir->getPath() + "/remote.bin";
    std::ofstream out{remotePath, std::ios::binary};
    const std::string blob(kRemoteBytes, 'a');
    out.write(blob.data(), blob.size());
  }

  FsCacheConfig makeTinyConfig() const {
    FsCacheConfig cfg;
    cfg.cacheRoot = tempDir->getPath() + "/cache";
    // Big enough that no eviction kicks in during the lossless-increment
    // test — eviction would race with concurrent recordMiss bumps and turn
    // the test from "counter atomicity" into "evict() concurrency".
    cfg.maxBytes = 1UL << 30;
    std::filesystem::create_directories(cfg.cacheRoot);
    return cfg;
  }
};

// Builds a kDownloaded FileSegment by running the real reserve/write/complete
// dance through FileSegment::download. Mirrors EvictionPolicyTest.cpp's
// downloaded() helper so LruPolicy::onInsert accepts the segment when
// recordMiss is later called.
std::shared_ptr<FileSegment> makeDownloadedSegment(
    FsCache& cache,
    const std::string& remotePath,
    uint64_t offset,
    uint64_t size) {
  auto segment = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath(remotePath), offset, size}, remotePath);
  VELOX_CHECK(segment->beginDownload());
  ::facebook::velox::LocalReadFile remote{remotePath};
  segment->download(remote, cache.config().cacheRoot);
  return segment;
}

} // namespace

TEST(FsCacheStatsTest, freshStatsAreZero) {
  Fixture fx;
  FsCache cache{fx.makeTinyConfig()};
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, 0u);
  EXPECT_EQ(s.prefetchMisses, 0u);
  EXPECT_EQ(s.demandHits, 0u);
  EXPECT_EQ(s.demandMisses, 0u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.bytesOnDisk, 0u);
  EXPECT_DOUBLE_EQ(prefetchHitRate(s), 0.0);
  EXPECT_DOUBLE_EQ(prefetchMissShare(s), 0.0);
}

// Each recordMiss inserts the segment into LruPolicy (onInsert refuses
// duplicates), so the test uses distinct segments — one per recordMiss
// call — to exercise the counter-routing path without tripping the
// LRU invariant. recordHit is idempotent on the LRU side and can be
// called repeatedly on the same segment.
TEST(FsCacheStatsTest, recordHitMissIncrementsCorrectField) {
  Fixture fx;
  FsCache cache{fx.makeTinyConfig()};
  auto hitSeg = makeDownloadedSegment(cache, fx.remotePath, 0, 4'096);
  auto missPrefetch =
      makeDownloadedSegment(cache, fx.remotePath, 4'096, 4'096);
  auto missDemand1 =
      makeDownloadedSegment(cache, fx.remotePath, 8'192, 4'096);
  auto missDemand2 =
      makeDownloadedSegment(cache, fx.remotePath, 12'288, 4'096);
  cache.recordHit(hitSeg.get(), IsPrefetch::kPrefetch);
  cache.recordHit(hitSeg.get(), IsPrefetch::kPrefetch);
  cache.recordHit(hitSeg.get(), IsPrefetch::kDemand);
  cache.recordMiss(missPrefetch.get(), 4'096, IsPrefetch::kPrefetch);
  cache.recordMiss(missDemand1.get(), 4'096, IsPrefetch::kDemand);
  cache.recordMiss(missDemand2.get(), 4'096, IsPrefetch::kDemand);
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, 2u);
  EXPECT_EQ(s.prefetchMisses, 1u);
  EXPECT_EQ(s.demandHits, 1u);
  EXPECT_EQ(s.demandMisses, 2u);
}

TEST(FsCacheStatsTest, prefetchHitRateComputesFromPrefetchOnly) {
  FsCacheStats s;
  // 9 prefetch hits / 1 prefetch miss = 0.9 hit-rate. Demand counters must
  // not enter the calculation — demand reads are blocking by definition,
  // so they are not part of the prefetch *effectiveness* metric
  // (spec §9.2 perf gate keys off prefetchHitRate).
  s.prefetchHits = 9;
  s.prefetchMisses = 1;
  s.demandMisses = 100; // Must not affect prefetchHitRate.
  EXPECT_DOUBLE_EQ(prefetchHitRate(s), 0.9);
  // prefetchMissShare uses BOTH miss counters per spec §3 quantitative
  // target: "miss flow taken by prefetch path", not the prefetch hit rate.
  // 1 prefetch miss / (1 + 100) = 1 / 101.
  EXPECT_NEAR(prefetchMissShare(s), 1.0 / 101.0, 1e-9);
}

// Hammers a single shared segment with prefetch hits from kThreads
// concurrent writers. recordHit is the only entry point that can be
// called repeatedly on the same segment (onHit is a no-op when the
// segment isn't in the policy), so this isolates the relaxed-atomic
// counter from any LRU interference.
TEST(FsCacheStatsTest, concurrentIncrementsAreLossless) {
  Fixture fx;
  FsCache cache{fx.makeTinyConfig()};
  auto seg = makeDownloadedSegment(cache, fx.remotePath, 0, 4'096);
  constexpr int kThreads = 8;
  constexpr int kIters = 10'000;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIters; ++i) {
        cache.recordHit(seg.get(), IsPrefetch::kPrefetch);
        cache.recordHit(seg.get(), IsPrefetch::kDemand);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, static_cast<uint64_t>(kThreads * kIters));
  EXPECT_EQ(s.demandHits, static_cast<uint64_t>(kThreads * kIters));
}

} // namespace facebook::velox::cache::fs
