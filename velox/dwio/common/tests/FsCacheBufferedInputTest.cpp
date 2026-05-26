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

#include "velox/dwio/common/FsCacheBufferedInput.h"

#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheBufferedInputTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::unique_ptr<FsCache> fsCache_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::string remoteContent_;

  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
    pool_ = memory::memoryManager()->addLeafPool("FsCacheBufferedInputTest");

    tempDir_ = TempDirectoryPath::create();
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    remoteContent_.resize(8UL * 1'024 * 1'024);
    // Non-trivial pattern so off-by-one byte-shift bugs become visible.
    for (size_t i = 0; i < remoteContent_.size(); ++i) {
      remoteContent_[i] = static_cast<char>(i % 251);
    }
    std::ofstream out{remotePath_, std::ios::binary};
    out.write(remoteContent_.data(), remoteContent_.size());

    FsCacheConfig cfg;
    cfg.cacheRoot = tempDir_->getPath() + "/cache";
    cfg.maxBytes = 64UL * 1'024 * 1'024;
    std::filesystem::create_directories(cfg.cacheRoot);
    fsCache_ = std::make_unique<FsCache>(cfg);
  }

  // Reads exactly size bytes from stream, looping over Next() chunks.
  static std::string drain(SeekableInputStream& stream, size_t size) {
    std::string buf(size, '\0');
    size_t copied = 0;
    const void* data;
    int32_t len;
    while (copied < size && stream.Next(&data, &len)) {
      const size_t toCopy =
          std::min<size_t>(static_cast<size_t>(len), size - copied);
      std::memcpy(buf.data() + copied, data, toCopy);
      copied += toCopy;
    }
    buf.resize(copied);
    return buf;
  }
};

TEST_F(FsCacheBufferedInputTest, enqueueAndLoadReadsExpectedBytes) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto stream = input.enqueue({100, 2'048});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, 2'048), remoteContent_.substr(100, 2'048));
}

TEST_F(FsCacheBufferedInputTest, multipleRegionsServed) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto s1 = input.enqueue({0, 1'024});
  auto s2 = input.enqueue({4'096, 1'024});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*s1, 1'024), remoteContent_.substr(0, 1'024));
  EXPECT_EQ(drain(*s2, 1'024), remoteContent_.substr(4'096, 1'024));
}

TEST_F(FsCacheBufferedInputTest, hasCacheReturnsTrue) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  EXPECT_TRUE(input.hasCache());
}

// Regression: isBuffered() must return false so that callers like
// StructColumnReader::loadRowGroup do NOT take the fast path that skips
// load(). FsCache's enqueue() only registers the region; load() is what
// populates segments. If isBuffered() ever lies and returns true, the next
// read on the returned stream will throw "Stream used before
// FsCacheBufferedInput::load()" from DeferredStream::ensureWithData.
//
// We assert BOTH the contract (returns false) AND the consequence (reading
// without load throws) so the test still catches the regression even if
// someone "cleans up" the boolean by inverting the constant.
TEST_F(FsCacheBufferedInputTest, isBufferedReturnsFalseToForceLoad) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};

  EXPECT_FALSE(input.isBuffered(0, 1'024));
  EXPECT_FALSE(input.isBuffered(100, 2'048));

  // Simulate the fast path: caller trusts isBuffered, enqueues, then reads
  // WITHOUT calling load(). DeferredStream must refuse rather than silently
  // returning empty / stale bytes.
  auto stream = input.enqueue({0, 1'024});
  const void* data;
  int32_t len;
  EXPECT_THROW(stream->Next(&data, &len), ::facebook::velox::VeloxException);
}

TEST_F(FsCacheBufferedInputTest, skipBeforeLoadPositionsCorrectly) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto stream = input.enqueue({0, 4'096});
  // SkipInt64 BEFORE load() — DeferredStream must remember the position and
  // replay it onto the inner stream the first time bytes are needed.
  ASSERT_TRUE(stream->SkipInt64(100));
  EXPECT_EQ(stream->ByteCount(), 100);
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, 3'996), remoteContent_.substr(100, 3'996));
  EXPECT_EQ(stream->ByteCount(), 4'096);
}

TEST_F(FsCacheBufferedInputTest, rangeSpansMultipleSegments) {
  // Default alignment=4MiB and maxSegmentSize=32MiB would collapse a 6MiB
  // request into a single outer segment, hiding the multi-segment Next()
  // transition. Use a tight config so the request is guaranteed to split,
  // and assert the split actually happened to guard against future regressions
  // in splitRange.
  FsCacheConfig tightCfg;
  tightCfg.cacheRoot = tempDir_->getPath() + "/cache_tight";
  tightCfg.maxBytes = 64UL * 1'024 * 1'024;
  tightCfg.alignment = 1UL * 1'024 * 1'024;
  tightCfg.maxSegmentSize = 2UL * 1'024 * 1'024;
  std::filesystem::create_directories(tightCfg.cacheRoot);
  auto tightCache = std::make_unique<FsCache>(tightCfg);

  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, tightCache.get()};
  const uint64_t offset = 1UL * 1'024 * 1'024;
  const uint64_t length = 6UL * 1'024 * 1'024;

  ASSERT_GT(FsCache::splitRange(offset, length, tightCfg).size(), 1UL)
      << "Test misconfigured: range did not split into multiple segments";

  auto stream = input.enqueue({offset, length});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, length), remoteContent_.substr(offset, length));
}

TEST_F(FsCacheBufferedInputTest, cacheRegionThrowsUnsupported) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  EXPECT_THROW(
      input.cacheRegion(0, 1, std::string_view{}),
      ::facebook::velox::VeloxException);
  EXPECT_THROW(
      input.findCachedRegion(0), ::facebook::velox::VeloxException);
}

// Smoke gate: round-tripping a real Parquet file through FsCacheBufferedInput
// must produce the same raw bytes as a plain LocalReadFile. Per spec
// 2026-05-23-fscache-vs-cbi-tpcds §1.4 assumption 2, this is the integration
// guard between Phase-1 read-path work and the bench-level wiring; it does
// not exercise the Parquet decoder itself, only the byte-level fidelity of
// enqueue / load / Next().
TEST_F(FsCacheBufferedInputTest, parquetSampleRoundTrips) {
  // getDataFilePath ignores baseDir outside fbcode and only joins cwd + filePath.
  // ctest runs this binary from the build dir's velox/dwio/common/tests, so the
  // relative path walks up to velox/dwio/parquet/tests/examples/.
  const auto path = ::facebook::velox::test::getDataFilePath(
      "velox/dwio/common/tests",
      "../../parquet/tests/examples/sample.parquet");
  auto readFile = std::make_shared<LocalReadFile>(path);
  const uint64_t size = readFile->size();
  ASSERT_GT(size, 0UL);

  std::string truth(size, '\0');
  readFile->pread(0, size, truth.data());

  // The bundled sample.parquet is only a few KiB. The default 4 MiB alignment
  // would force FsCache to issue a 4 MiB pread that overshoots EOF and crashes
  // inside LocalReadFile. Use a 4 KiB alignment so a small fixture still
  // exercises the splitRange / segment plumbing without changing the read-path
  // logic under test.
  FsCacheConfig tinyCfg;
  tinyCfg.cacheRoot = tempDir_->getPath() + "/cache_parquet";
  tinyCfg.maxBytes = 64UL * 1'024 * 1'024;
  tinyCfg.alignment = 4UL * 1'024;
  tinyCfg.maxSegmentSize = 64UL * 1'024;
  std::filesystem::create_directories(tinyCfg.cacheRoot);
  auto tinyCache = std::make_unique<FsCache>(tinyCfg);

  FsCacheBufferedInput input{readFile, *pool_, tinyCache.get()};
  auto stream = input.enqueue({0, size});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, size), truth);
}

} // namespace facebook::velox::dwio::common::test
