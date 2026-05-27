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

#include <atomic>
#include <exception>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

namespace {
// Test helper: drives all kEmpty segments to kDownloaded. After Task 9,
// FsCache::getOrSet returns kEmpty segments and the caller (normally
// FsCacheBufferedInput::load) is responsible for downloading. These unit
// tests call getOrSet directly, so they need this helper to complete the
// download before asserting on segment state.
//
// `cache` is required for stats accounting: recordMiss() must be called after
// complete() so LRU and bytesOnDisk are credited, and recordHit() for segments
// that were already kDownloaded. Without this, stats-based tests would fail.
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
    if (!seg->reserve(seg->key().size, cacheRoot)) {
      // reserve() may have taken the warm-restart short-circuit and
      // already set the segment to kDownloaded.
      if (seg->state() == FileSegment::State::kDownloaded) {
        cache.recordMiss(seg.get(), seg->key().size, IsPrefetch::kDemand);
      } else {
        seg->waitForDownloadedSize(seg->key().size);
        cache.recordHit(seg.get(), IsPrefetch::kDemand);
      }
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

class FsCacheTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  FsCacheConfig config_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    config_.cacheRoot = tempDir_->getPath() + "/cache";
    config_.maxBytes = 100UL * 1'024 * 1'024;
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::ofstream out{remotePath_, std::ios::binary};
    const std::string blob(8UL * 1'024 * 1'024, 'a');
    out.write(blob.data(), blob.size());
  }
};

TEST_F(FsCacheTest, getOrSetFirstCallDownloads) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  auto holder = cache.getOrSet(
      remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
  driveSegments(*holder, remote, cache);
  const auto& segments = holder->segments();
  ASSERT_FALSE(segments.empty());
  for (const auto& segment : segments) {
    EXPECT_EQ(segment->state(), FileSegment::State::kDownloaded);
  }
  EXPECT_EQ(cache.stats().demandMisses, 1);
}

TEST_F(FsCacheTest, getOrSetSecondCallHitsCache) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  {
    auto holder = cache.getOrSet(
        remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
    driveSegments(*holder, remote, cache);
  }
  auto holder = cache.getOrSet(
      remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
  driveSegments(*holder, remote, cache);
  const auto& segments = holder->segments();
  ASSERT_FALSE(segments.empty());
  for (const auto& segment : segments) {
    EXPECT_EQ(segment->state(), FileSegment::State::kDownloaded);
  }
  EXPECT_GT(cache.stats().demandHits, 0);
}

// Regression: when remote file size < cache alignment, splitRange's outward
// alignment used to overshoot EOF, causing LocalReadFile::preadInternal to
// crash on the short read. getOrSet must clamp the read length to the file's
// actual size so a tiny file (e.g. a Parquet footer-only fixture) round-trips
// successfully through FsCache. See spec 2026-05-23-fscache-vs-cbi-tpcds §7
// OQ #2.
TEST_F(FsCacheTest, getOrSetClampsToFileSizeWhenSmallerThanAlignment) {
  const std::string tinyPath = tempDir_->getPath() + "/tiny.bin";
  const std::string tinyContent(3UL * 1'024, 'b');
  {
    std::ofstream out{tinyPath, std::ios::binary};
    out.write(tinyContent.data(), tinyContent.size());
  }
  FsCacheConfig cfg = config_;
  cfg.alignment = 4UL * 1'024;
  cfg.maxSegmentSize = 64UL * 1'024;
  FsCache cache{cfg};
  LocalReadFile tinyRemote{tinyPath};
  // alignedEnd would be 4096, but file size is 3072. Without the clamp,
  // FileSegment::download asks remote for 4096 bytes and crashes.
  auto holder = cache.getOrSet(
      tinyPath,
      0,
      tinyContent.size(),
      cache.config(),
      tinyRemote,
      IsPrefetch::kDemand);
  driveSegments(*holder, tinyRemote, cache);
  const auto& segments = holder->segments();
  ASSERT_FALSE(segments.empty());
  for (const auto& segment : segments) {
    EXPECT_EQ(segment->state(), FileSegment::State::kDownloaded);
  }
}

TEST_F(FsCacheTest, getOrSetReturnsEmptyAtExactEof) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  // offset == size: legitimate zero-byte read at EOF must return an empty
  // vector rather than throw.
  EXPECT_TRUE(cache
                  .getOrSet(
                      remotePath_,
                      remote.size(),
                      0,
                      cache.config(),
                      remote,
                      IsPrefetch::kDemand)
                  ->empty());
  EXPECT_TRUE(
      cache.getOrSet(remotePath_, 0, 0, cache.config(), remote, IsPrefetch::kDemand)
          ->empty());
}

TEST_F(FsCacheTest, getOrSetThrowsWhenOffsetPastEof) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  // offset > size: caller bug; surface immediately rather than silently
  // returning an empty range.
  EXPECT_THROW(
      cache.getOrSet(
          remotePath_,
          remote.size() + 1,
          1,
          cache.config(),
          remote,
          IsPrefetch::kDemand),
      ::facebook::velox::VeloxException);
}

TEST_F(FsCacheTest, evictionRunsWhenOverCapacity) {
  FsCacheConfig tiny = config_;
  tiny.maxBytes = 5UL * 1'024 * 1'024;
  FsCache cache{tiny};
  LocalReadFile remote{remotePath_};
  // Read two disjoint 4 MiB chunks. Total 8 MiB > 5 MiB -> eviction.
  {
    auto holder = cache.getOrSet(
        remotePath_,
        0,
        4UL * 1'024 * 1'024,
        cache.config(),
        remote,
        IsPrefetch::kDemand);
    driveSegments(*holder, remote, cache);
  }
  {
    auto holder = cache.getOrSet(
        remotePath_,
        4UL * 1'024 * 1'024,
        4UL * 1'024 * 1'024,
        cache.config(),
        remote,
        IsPrefetch::kDemand);
    driveSegments(*holder, remote, cache);
  }
  EXPECT_LE(cache.stats().bytesOnDisk, tiny.maxBytes);
  EXPECT_GT(cache.stats().evictions, 0);
}

namespace {
// ReadFile whose pread always throws, used to simulate remote-IO failure.
class ThrowingReadFile : public ::facebook::velox::ReadFile {
 public:
  std::string_view pread(
      uint64_t /*offset*/,
      uint64_t /*length*/,
      void* /*buf*/,
      const ::facebook::velox::FileIoContext& /*context*/ = {}) const override {
    VELOX_FAIL("simulated remote IO failure");
  }
  uint64_t size() const override {
    return 8UL * 1'024 * 1'024;
  }
  uint64_t memoryUsage() const override {
    return 0;
  }
  bool shouldCoalesce() const override {
    return false;
  }
  std::string getName() const override {
    return "throwing";
  }
  uint64_t getNaturalReadSize() const override {
    return 4'096;
  }
};
} // namespace

// Regression: writer's IO failure must not crash waiters via VELOX_CHECK.
// Waiters must surface the failure via a user-facing throw instead.
TEST_F(FsCacheTest, waiterReceivesThrowWhenWriterFails) {
  FsCache cache{config_};
  ThrowingReadFile remote;
  // Two threads race on the same key. Exactly one becomes the writer (will
  // throw); the other is the waiter (must also throw, not CHECK-crash).
  std::atomic<int> throwCount{0};
  std::atomic<int> otherCount{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] {
      try {
        auto holder = cache.getOrSet(
            remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
        driveSegments(*holder, remote, cache);
      } catch (const std::exception&) {
        ++throwCount;
      } catch (...) {
        ++otherCount;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(throwCount.load(), 2);
  EXPECT_EQ(otherCount.load(), 0);
}

// Regression: a segment that already reached kDownloaded must increment
// stats.demandHits on re-read so it participates in eviction policy and does
// not leak.
TEST_F(FsCacheTest, secondReaderOfDownloadedSegmentCountsAsHit) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  {
    auto holder = cache.getOrSet(
        remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
    driveSegments(*holder, remote, cache);
  }
  const auto baseline = cache.stats();
  {
    auto holder = cache.getOrSet(
        remotePath_, 0, 4'096, cache.config(), remote, IsPrefetch::kDemand);
    driveSegments(*holder, remote, cache);
  }
  const auto after = cache.stats();
  EXPECT_EQ(after.demandMisses, baseline.demandMisses);
  EXPECT_EQ(after.demandHits, baseline.demandHits + 1);
  EXPECT_EQ(after.bytesOnDisk, baseline.bytesOnDisk);
}

TEST_F(FsCacheTest, ctorRejectsInvalidConfig) {
  FsCacheConfig bad = config_;
  bad.maxSegmentSize = 100;
  bad.alignment = 64;
  // 100 % 64 != 0 -> constructor must throw.
  EXPECT_THROW(FsCache{bad}, ::facebook::velox::VeloxException);
}

// Regression: two concurrent writers that both trigger evict() must not race
// on the LRU victim list. Without serialization in evict(), selectVictims()
// can return the same victim to both threads, and the first thread's metadata
// erase drops the only shared_ptr, leaving the second thread to dereference a
// freed FileSegment. The test only needs to drive the racy path -- reaching
// the join + stats() call without crashing or hanging is the assertion.
// bytesOnDisk is not strictly bounded by maxBytes during concurrent writes:
// each evict() reserves enough for its own miss before bytesOnDisk is
// incremented, so two writers passing their evict checks back-to-back can
// transiently push bytesOnDisk above maxBytes.
TEST_F(FsCacheTest, concurrentEvictionIsSafe) {
  FsCacheConfig tiny = config_;
  tiny.maxBytes = 1UL * 1'024 * 1'024;
  FsCache cache{tiny};
  LocalReadFile remote{remotePath_};
  constexpr int kThreads = 8;
  constexpr int kIterations = 16;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kIterations; ++i) {
        // Each thread reads a different 256 KiB window so threads do not
        // share a key and both reliably enter the writer/eviction path.
        const uint64_t offset =
            (static_cast<uint64_t>(t * kIterations + i) * 256UL * 1'024) %
            (4UL * 1'024 * 1'024);
        auto holder = cache.getOrSet(
            remotePath_,
            offset,
            256UL * 1'024,
            cache.config(),
            remote,
            IsPrefetch::kDemand);
        driveSegments(*holder, remote, cache);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Survives stats() with consistent counters: demandHits + demandMisses ==
  // total calls.
  const auto stats = cache.stats();
  EXPECT_EQ(stats.demandHits + stats.demandMisses, kThreads * kIterations);
}

// Phase-2 moves stats counters to std::atomic and drops the
// CacheStateGuard from the hit fast-path. All kThreads * kPerThread
// calls race for the same key from t=0; the implementation guarantees a
// single writer via beginDownload(), so the race produces exactly 1
// recordMiss with the rest going through recordHit. The hits assertion
// then catches any lost update on the relaxed atomic counter.
TEST_F(FsCacheTest, statsCountersIncrementAcrossThreadsWithoutLoss) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  constexpr int kThreads{16};
  constexpr int kPerThread{200};
  constexpr uint64_t kSegmentSize{4UL * 1'024 * 1'024};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      LocalReadFile threadRemote{remotePath_};
      for (int i = 0; i < kPerThread; ++i) {
        auto holder = cache.getOrSet(
            remotePath_,
            0,
            kSegmentSize,
            cache.config(),
            threadRemote,
            IsPrefetch::kDemand);
        driveSegments(*holder, threadRemote, cache);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  const auto snapshot = cache.stats();
  EXPECT_EQ(snapshot.demandMisses, 1u);
  EXPECT_EQ(snapshot.demandHits, kThreads * kPerThread - 1u);
}

TEST(FsCacheSingletonTest, defaultsToNullptr) {
  // Defensive: a prior test that forgot to reset the singleton would otherwise
  // poison this assertion. The singleton lives across the whole process.
  FsCache::setInstance(nullptr);
  EXPECT_EQ(FsCache::getInstance(), nullptr);
}

TEST(FsCacheSingletonTest, setInstanceRoundTrip) {
  FsCache::setInstance(nullptr);

  auto tempDir = TempDirectoryPath::create();
  FsCacheConfig config;
  config.cacheRoot = tempDir->getPath();
  config.maxBytes = 16ULL << 20;
  auto cache = std::make_unique<FsCache>(config);

  FsCache::setInstance(cache.get());
  EXPECT_EQ(FsCache::getInstance(), cache.get());

  FsCache::setInstance(nullptr);
  EXPECT_EQ(FsCache::getInstance(), nullptr);
}

// bypass_cache_threshold (spec §8.3).

TEST_F(FsCacheTest, getOrSetReturnsEmptyHolderWhenRegionExceedsBypassThreshold) {
  FsCacheConfig cfg = config_;
  cfg.bypassThresholdBytes = 4UL * 1'024 * 1'024;
  FsCache cache{cfg};
  LocalReadFile remote{remotePath_};
  auto holder = cache.getOrSet(
      remotePath_,
      0,
      8UL * 1'024 * 1'024,
      cache.config(),
      remote,
      IsPrefetch::kDemand);
  EXPECT_TRUE(holder->empty());
  EXPECT_EQ(cache.totalSize(), 0u);
}

TEST_F(FsCacheTest, getOrSetReturnsHolderUnderBypassThreshold) {
  FsCacheConfig cfg = config_;
  cfg.bypassThresholdBytes = 4UL * 1'024 * 1'024;
  FsCache cache{cfg};
  LocalReadFile remote{remotePath_};
  auto holder = cache.getOrSet(
      remotePath_,
      0,
      2UL * 1'024 * 1'024,
      cache.config(),
      remote,
      IsPrefetch::kDemand);
  EXPECT_FALSE(holder->empty());
}

TEST_F(FsCacheTest, shouldBypassMatchesGetOrSetBoundary) {
  FsCacheConfig cfg = config_;
  cfg.bypassThresholdBytes = 4UL * 1'024 * 1'024;
  FsCache cache{cfg};
  EXPECT_FALSE(cache.shouldBypass(4UL * 1'024 * 1'024 - 1));
  EXPECT_TRUE(cache.shouldBypass(4UL * 1'024 * 1'024));
  EXPECT_TRUE(cache.shouldBypass(8UL * 1'024 * 1'024));
}

TEST_F(FsCacheTest, shouldBypassDisabledWhenThresholdIsZero) {
  FsCacheConfig cfg = config_;
  cfg.bypassThresholdBytes = 0;
  FsCache cache{cfg};
  EXPECT_FALSE(cache.shouldBypass(0));
  EXPECT_FALSE(cache.shouldBypass(1ULL << 40));
}

TEST_F(FsCacheTest, totalSizeStartsAtZeroAndStaysZeroOnBypass) {
  FsCacheConfig cfg = config_;
  cfg.bypassThresholdBytes = 4UL * 1'024 * 1'024;
  FsCache cache{cfg};
  EXPECT_EQ(cache.totalSize(), 0u);
  LocalReadFile remote{remotePath_};
  (void)cache.getOrSet(
      remotePath_,
      0,
      8UL * 1'024 * 1'024,
      cache.config(),
      remote,
      IsPrefetch::kDemand);
  EXPECT_EQ(cache.totalSize(), 0u);
}

} // namespace facebook::velox::cache::fs::test
