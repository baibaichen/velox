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
  const auto segments = cache.getOrSet(remotePath_, 0, 4'096, remote);
  ASSERT_FALSE(segments.empty());
  for (const auto& segment : segments) {
    EXPECT_EQ(segment->state(), FileSegment::State::kDownloaded);
  }
  EXPECT_EQ(cache.stats().misses, 1);
}

TEST_F(FsCacheTest, getOrSetSecondCallHitsCache) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  cache.getOrSet(remotePath_, 0, 4'096, remote);
  const auto segments = cache.getOrSet(remotePath_, 0, 4'096, remote);
  ASSERT_FALSE(segments.empty());
  for (const auto& segment : segments) {
    EXPECT_EQ(segment->state(), FileSegment::State::kDownloaded);
  }
  EXPECT_GT(cache.stats().hits, 0);
}

TEST_F(FsCacheTest, evictionRunsWhenOverCapacity) {
  FsCacheConfig tiny = config_;
  tiny.maxBytes = 5UL * 1'024 * 1'024;
  FsCache cache{tiny};
  LocalReadFile remote{remotePath_};
  // Read two disjoint 4 MiB chunks. Total 8 MiB > 5 MiB -> eviction.
  cache.getOrSet(remotePath_, 0, 4UL * 1'024 * 1'024, remote);
  cache.getOrSet(
      remotePath_, 4UL * 1'024 * 1'024, 4UL * 1'024 * 1'024, remote);
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
        cache.getOrSet(remotePath_, 0, 4'096, remote);
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
// stats.hits on re-read so it participates in eviction policy and does not
// leak.
TEST_F(FsCacheTest, secondReaderOfDownloadedSegmentCountsAsHit) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  cache.getOrSet(remotePath_, 0, 4'096, remote);
  const auto baseline = cache.stats();
  cache.getOrSet(remotePath_, 0, 4'096, remote);
  const auto after = cache.stats();
  EXPECT_EQ(after.misses, baseline.misses);
  EXPECT_EQ(after.hits, baseline.hits + 1);
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
        cache.getOrSet(remotePath_, offset, 256UL * 1'024, remote);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Survives stats() with consistent counters: hits + misses == total calls.
  const auto stats = cache.stats();
  EXPECT_EQ(stats.hits + stats.misses, kThreads * kIterations);
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

} // namespace facebook::velox::cache::fs::test
