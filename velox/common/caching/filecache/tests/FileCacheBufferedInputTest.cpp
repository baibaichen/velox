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

#include "velox/dwio/common/FileCacheBufferedInput.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheDownloadExecutor.h"
#include "velox/common/caching/filecache/FileCacheKey.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/ReadFileByteInputStream.h"

namespace facebook::velox::ch {
namespace {

using facebook::velox::common::testutil::TempDirectoryPath;
using facebook::velox::dwio::common::LogType;
using facebook::velox::dwio::common::SeekableInputStream;

std::string makeContent(size_t size) {
  std::string content(size, '\0');
  for (size_t i = 0; i < size; ++i) {
    content[i] = static_cast<char>(i % 256);
  }
  return content;
}

void writeFile(const std::string& path, const std::string& content) {
  std::ofstream out(path, std::ios::binary);
  ASSERT_TRUE(out.is_open()) << path;
  out.write(content.data(), content.size());
  ASSERT_TRUE(out.good()) << path;
}

std::string drain(SeekableInputStream& s, size_t size) {
  std::string buf(size, '\0');
  size_t copied = 0;
  const void* data;
  int32_t len;
  while (copied < size && s.Next(&data, &len)) {
    size_t toCopy = std::min<size_t>(len, size - copied);
    std::memcpy(buf.data() + copied, data, toCopy);
    copied += toCopy;
  }
  buf.resize(copied);
  return buf;
}

class FileCacheBufferedInputTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    root_ = TempDirectoryPath::create();
    pool_ = memory::memoryManager()->addLeafPool("FileCacheBufferedInputTest");
  }

  FileCacheSettings settings(
      const std::string& cacheDir,
      uint64_t maxSize = 64ULL << 20,
      uint64_t maxFileSegmentSize = 0,
      uint64_t boundaryAlignment = 0) const {
    FileCacheSettings settings;
    settings.path = cacheDir;
    settings.maxSize = maxSize;
    if (maxFileSegmentSize != 0) {
      settings.maxFileSegmentSize = maxFileSegmentSize;
    }
    if (boundaryAlignment != 0) {
      settings.boundaryAlignment = boundaryAlignment;
    }
    settings.validate();
    return settings;
  }

  std::string path(const std::string& name) const {
    return (std::filesystem::path(root_->getPath()) / name).string();
  }

  // Builds an input that reads filePath through the given cache/executor. The
  // cache and executor are owned by the caller so tests can vary their
  // settings/parallelism and share a single cache across multiple inputs.
  FileCacheBufferedInput makeInput(
      FileCache& cache,
      FileCacheDownloadExecutor& executor,
      const std::string& filePath,
      const FileCacheKey& key) const {
    return FileCacheBufferedInput(
        std::make_shared<LocalReadFile>(filePath),
        *pool_,
        &cache,
        &executor,
        key,
        FileCache::getCommonOrigin());
  }

  FileCacheBufferedInput makeInput(
      FileCache& cache,
      FileCacheDownloadExecutor& executor,
      const std::string& filePath) const {
    return makeInput(cache, executor, filePath, FileCacheKey::fromPath(filePath));
  }

  std::shared_ptr<TempDirectoryPath> root_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(FileCacheBufferedInputTest, enqueueAndLoadReadsExpectedBytes) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(1 << 20);
  writeFile(remotePath, content);

  FileCache cache("e2e_smoke", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(2);
  auto input = makeInput(cache, executor, remotePath);

  auto stream = input.enqueue({100, 2048});
  input.load(LogType::FILE);

  EXPECT_EQ(drain(*stream, 2048), content.substr(100, 2048));
}

TEST_F(FileCacheBufferedInputTest, multipleRegionsInOneInput) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(1 << 20);
  writeFile(remotePath, content);

  FileCache cache("multi_region", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(4);
  auto input = makeInput(cache, executor, remotePath);

  struct RegionExpect {
    uint64_t offset;
    uint64_t length;
    std::unique_ptr<SeekableInputStream> stream;
  };
  std::vector<RegionExpect> regions;
  regions.push_back({0, 4096, nullptr});
  regions.push_back({4096, 8192, nullptr});
  regions.push_back({500000, 12345, nullptr});
  for (auto& r : regions) {
    r.stream = input.enqueue({r.offset, r.length});
  }

  input.load(LogType::FILE);

  for (auto& r : regions) {
    EXPECT_EQ(drain(*r.stream, r.length), content.substr(r.offset, r.length))
        << "region offset=" << r.offset << " length=" << r.length;
  }
}

TEST_F(FileCacheBufferedInputTest, regionSpanningMultipleSegments) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // 64KiB segments force the region to span four cache segments that
  // FileCacheInputStream must stitch back together.
  FileCache cache(
      "spanning",
      settings(path("cache"), 64ULL << 20, 64ULL << 10, 64ULL << 10));
  cache.initialize();
  FileCacheDownloadExecutor executor(4);
  auto input = makeInput(cache, executor, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  EXPECT_EQ(drain(*stream, 256 << 10), content);
}

TEST_F(FileCacheBufferedInputTest, secondInputServesFromCache) {
  const auto remotePath = path("remote.bin");
  const auto wrongPath = path("wrong.bin");
  const auto content = makeContent(64 << 10);
  writeFile(remotePath, content);
  // Different bytes at the same length; a true cache hit must ignore this.
  writeFile(wrongPath, std::string(content.size(), '\xab'));

  FileCache cache("cache_hit", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(2);
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    auto input = makeInput(cache, executor, remotePath, key);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  // Second input shares the same cache + key but points at a file with
  // different bytes. A correct cache hit returns the original content.
  {
    auto input = makeInput(cache, executor, wrongPath, key);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }
}

TEST_F(FileCacheBufferedInputTest, skipThenReadAfterLoad) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(1 << 20);
  writeFile(remotePath, content);

  FileCache cache("skip", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(2);
  auto input = makeInput(cache, executor, remotePath);

  auto stream = input.enqueue({1000, 4096});
  input.load(LogType::FILE);

  ASSERT_TRUE(stream->SkipInt64(1000));
  EXPECT_EQ(drain(*stream, 3096), content.substr(2000, 3096));
}

TEST_F(FileCacheBufferedInputTest, isBufferedAlwaysFalse) {
  const auto remotePath = path("remote.bin");
  writeFile(remotePath, makeContent(1024));
  FileCache cache("is_buffered", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(1);
  auto input = makeInput(cache, executor, remotePath);

  EXPECT_FALSE(input.isBuffered(0, 1024));
  EXPECT_TRUE(input.hasCache());
}

TEST_F(FileCacheBufferedInputTest, unsupportedCacheRegionApisThrow) {
  const auto remotePath = path("remote.bin");
  writeFile(remotePath, makeContent(1024));
  FileCache cache("unsupported", settings(path("cache")));
  cache.initialize();
  FileCacheDownloadExecutor executor(1);
  auto input = makeInput(cache, executor, remotePath);

  VELOX_ASSERT_THROW(input.cacheRegion(0, 16, std::string_view{}), "");
  VELOX_ASSERT_THROW(input.findCachedRegion(0), "");
}

TEST_F(FileCacheBufferedInputTest, reserveFailureSurfacesErrorNotHang) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(64 << 10);
  writeFile(remotePath, content);

  // Cache far smaller than one chunk: reserve() can never satisfy the download,
  // so the task abandons. The reader must surface a deterministic failure
  // rather than spin forever (regression guard for the EMPTY-reset livelock).
  FileCache cache("reserve_fail", settings(path("cache"), 4096, 4096, 4096));
  cache.initialize();
  FileCacheDownloadExecutor executor(2);
  auto input = makeInput(cache, executor, remotePath);

  auto stream = input.enqueue({0, 64 << 10});
  input.load(LogType::FILE);

  VELOX_ASSERT_THROW(drain(*stream, 64 << 10), "");
}

// Builds an input wired with an IoStatistics sink so the Layer A operator-level
// counters (read/ssdRead/prefetch) can be asserted alongside Layer B.
FileCacheBufferedInput makeInputWithStats(
    memory::MemoryPool& pool,
    FileCache& cache,
    FileCacheDownloadExecutor& executor,
    const std::string& filePath,
    const FileCacheKey& key,
    std::shared_ptr<io::IoStatistics> ioStats) {
  return FileCacheBufferedInput(
      std::make_shared<LocalReadFile>(filePath),
      pool,
      &cache,
      &executor,
      key,
      FileCache::getCommonOrigin(),
      CreateFileSegmentSettings{},
      std::move(ioStats));
}

TEST_F(FileCacheBufferedInputTest, metricsColdReadRecordsMissAndDownload) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache("metrics_cold", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);
  auto ioStats = std::make_shared<io::IoStatistics>();

  {
    // Scope the executor + input so their destructors join all download tasks,
    // making the async miss/download counters deterministic before readback.
    FileCacheDownloadExecutor executor(2);
    auto input =
        makeInputWithStats(*pool_, cache, executor, remotePath, key, ioStats);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  const auto stats = cache.stats();
  // Cold read: at least one segment downloaded from source, none served warm.
  EXPECT_GT(stats.misses, 0u);
  EXPECT_EQ(stats.hits, 0u);
  EXPECT_EQ(stats.downloadedBytes, content.size());

  // Layer A: source + prefetch bytes track the downloaded payload; no ssd hit.
  EXPECT_EQ(ioStats->read().sum(), content.size());
  EXPECT_EQ(ioStats->prefetch().sum(), content.size());
  EXPECT_EQ(ioStats->ssdRead().sum(), 0u);
}

TEST_F(FileCacheBufferedInputTest, metricsWarmReadRecordsHit) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache("metrics_warm", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  // Cold pass populates the cache.
  {
    FileCacheDownloadExecutor executor(2);
    auto input = makeInput(cache, executor, remotePath, key);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  const auto afterCold = cache.stats();

  // Warm pass: every segment is already DOWNLOADED, so it counts as a hit and
  // serves bytes from the local cache file (Layer A ssdRead), downloading none.
  auto ioStats = std::make_shared<io::IoStatistics>();
  {
    FileCacheDownloadExecutor executor(2);
    auto input =
        makeInputWithStats(*pool_, cache, executor, remotePath, key, ioStats);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  const auto afterWarm = cache.stats();
  EXPECT_GT(afterWarm.hits, afterCold.hits);
  EXPECT_EQ(afterWarm.misses, afterCold.misses);
  EXPECT_EQ(afterWarm.downloadedBytes, afterCold.downloadedBytes);

  // Warm read is served entirely from cache: ssdRead covers the region, and no
  // source read/prefetch happens on this input.
  EXPECT_EQ(ioStats->ssdRead().sum(), content.size());
  EXPECT_EQ(ioStats->read().sum(), 0u);
  EXPECT_EQ(ioStats->prefetch().sum(), 0u);
}

// The process-global singleton is null until installed, returns the installed
// pointer, and clears back to null -- this is how createBufferedInput discovers
// whether to route reads through the ch::FileCache backend.
TEST_F(FileCacheBufferedInputTest, singletonInstallAndClear) {
  EXPECT_EQ(FileCache::getInstance(), nullptr);

  FileCache cache("singleton", settings(path("cache")));
  cache.initialize();

  FileCache::setInstance(&cache);
  EXPECT_EQ(FileCache::getInstance(), &cache);

  FileCache::setInstance(nullptr);
  EXPECT_EQ(FileCache::getInstance(), nullptr);
}

// initialize() eagerly builds the owned download executor from the configured
// thread count, and createBufferedInput-style consumers can read it back.
TEST_F(FileCacheBufferedInputTest, ownedDownloadExecutorReadback) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(64 << 10);
  writeFile(remotePath, content);

  FileCache cache("owned_executor", settings(path("cache")));
  cache.initialize();
  ASSERT_NE(cache.downloadExecutor(), nullptr);

  const auto key = FileCacheKey::fromPath(remotePath);
  auto input = FileCacheBufferedInput(
      std::make_shared<LocalReadFile>(remotePath),
      *pool_,
      &cache,
      cache.downloadExecutor(),
      key,
      FileCache::getCommonOrigin());
  auto stream = input.enqueue({0, content.size()});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, content.size()), content);
}

// ===========================================================================
// FileSegment::downloadFromReader — streaming download protocol (commit 2).
// Drives the helper directly with an in-memory BufferInputStream as the
// "remote" source so the nextView/reserve/write sequence is unit-tested
// without the async background queue.
// ===========================================================================

namespace {
std::string readFileFully(const std::string& filePath) {
  std::ifstream in(filePath, std::ios::binary);
  return std::string(
      (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

// Acquires the single segment covering [0, size) and makes this thread its
// downloader so reserve()/write() are permitted.
FileSegment& acquireDownloader(
    FileCache& cache,
    FileSegmentsHolderPtr& holder,
    const FileCacheKey& key,
    size_t size) {
  holder = cache.getOrSet(
      key,
      /*offset=*/0,
      size,
      /*file_size=*/size,
      CreateFileSegmentSettings{},
      /*file_segments_limit=*/0,
      FileCache::getCommonOrigin());
  EXPECT_EQ(holder->size(), 1u);
  auto& segment = holder->front();
  EXPECT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
  return segment;
}
} // namespace

TEST_F(FileCacheBufferedInputTest, downloadFromReaderFillsWholeSegment) {
  const size_t fileSize = 256 * 1024;
  std::string content = makeContent(fileSize);

  FileCache cache("download_whole", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  FileSegmentsHolderPtr holder;
  auto& segment = acquireDownloader(cache, holder, key, fileSize);

  BufferInputStream reader({ByteRange{
      reinterpret_cast<uint8_t*>(content.data()),
      static_cast<int64_t>(content.size()),
      0}});
  std::vector<char> scratch;
  const size_t written =
      FileSegment::downloadFromReader(segment, reader, fileSize, scratch, 10000);

  EXPECT_EQ(written, fileSize);
  EXPECT_EQ(segment.getDownloadedSize(), fileSize);
  segment.completePartAndResetDownloader();
  EXPECT_EQ(segment.state(), FileSegment::State::DOWNLOADED);
  EXPECT_EQ(readFileFully(segment.getPath()), content);
}

TEST_F(FileCacheBufferedInputTest, downloadFromReaderResumesPartialDownload) {
  const size_t fileSize = 256 * 1024;
  const size_t half = fileSize / 2;
  std::string content = makeContent(fileSize);

  FileCache cache("download_resume", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  FileSegmentsHolderPtr holder;
  auto& segment = acquireDownloader(cache, holder, key, fileSize);

  BufferInputStream reader({ByteRange{
      reinterpret_cast<uint8_t*>(content.data()),
      static_cast<int64_t>(content.size()),
      0}});
  std::vector<char> scratch;

  EXPECT_EQ(
      FileSegment::downloadFromReader(segment, reader, half, scratch, 10000),
      half);
  EXPECT_EQ(segment.getDownloadedSize(), half);
  // Still the downloader with more bytes to write: the segment stays
  // DOWNLOADING until the downloader is released.
  EXPECT_EQ(segment.state(), FileSegment::State::DOWNLOADING);

  // Continue from the current write offset; the reader cursor is already at
  // 'half', so no seek is needed.
  EXPECT_EQ(
      FileSegment::downloadFromReader(
          segment, reader, fileSize - half, scratch, 10000),
      fileSize - half);
  EXPECT_EQ(segment.getDownloadedSize(), fileSize);
  segment.completePartAndResetDownloader();
  EXPECT_EQ(segment.state(), FileSegment::State::DOWNLOADED);
  EXPECT_EQ(readFileFully(segment.getPath()), content);
}

TEST_F(FileCacheBufferedInputTest, downloadFromReaderHandlesMultiRangeReader) {
  const size_t fileSize = 200 * 1024;
  std::string content = makeContent(fileSize);

  FileCache cache("download_multirange", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  FileSegmentsHolderPtr holder;
  auto& segment = acquireDownloader(cache, holder, key, fileSize);

  // Split the source into uneven ranges so readBytes() crosses range
  // boundaries within a single write chunk.
  std::vector<ByteRange> ranges;
  const std::vector<size_t> sizes{37 * 1024, 1, 100 * 1024, fileSize - 137 * 1024 - 1};
  size_t off = 0;
  for (auto sz : sizes) {
    ranges.push_back(ByteRange{
        reinterpret_cast<uint8_t*>(content.data()) + off,
        static_cast<int64_t>(sz),
        0});
    off += sz;
  }
  ASSERT_EQ(off, fileSize);

  BufferInputStream reader(std::move(ranges));
  std::vector<char> scratch;
  EXPECT_EQ(
      FileSegment::downloadFromReader(segment, reader, fileSize, scratch, 10000),
      fileSize);
  segment.completePartAndResetDownloader();
  EXPECT_EQ(segment.state(), FileSegment::State::DOWNLOADED);
  EXPECT_EQ(readFileFully(segment.getPath()), content);
}

TEST_F(FileCacheBufferedInputTest, downloadFromReaderStopsAtReaderEof) {
  const size_t fileSize = 256 * 1024;
  const size_t available = 100 * 1024;
  std::string content = makeContent(fileSize);

  FileCache cache("download_eof", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  FileSegmentsHolderPtr holder;
  auto& segment = acquireDownloader(cache, holder, key, fileSize);

  // Reader exposes fewer bytes than requested: the helper must stop at EOF
  // and report only what it wrote, leaving the segment partially downloaded.
  BufferInputStream reader({ByteRange{
      reinterpret_cast<uint8_t*>(content.data()),
      static_cast<int64_t>(available),
      0}});
  std::vector<char> scratch;
  EXPECT_EQ(
      FileSegment::downloadFromReader(segment, reader, fileSize, scratch, 10000),
      available);
  EXPECT_EQ(segment.getDownloadedSize(), available);

  // Releasing the downloader with only part of the range written leaves the
  // segment resumable (PARTIALLY_DOWNLOADED), not failed.
  segment.completePartAndResetDownloader();
  EXPECT_EQ(segment.state(), FileSegment::State::PARTIALLY_DOWNLOADED);
  EXPECT_EQ(segment.getDownloadedSize(), available);
}

TEST_F(FileCacheBufferedInputTest, downloadFromReaderSeeksAbsoluteFileOffset) {
  // A segment whose range starts at a non-zero file offset: the reader must be
  // seeked to the absolute offset, so it writes the correct slice of the file.
  const size_t segSize = 64 * 1024;
  const size_t fileSize = 4 * segSize;
  const size_t segOffset = 2 * segSize;
  std::string content = makeContent(fileSize);

  FileCache cache("download_absolute", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  auto holder = cache.getOrSet(
      key,
      segOffset,
      segSize,
      fileSize,
      CreateFileSegmentSettings{},
      /*file_segments_limit=*/0,
      FileCache::getCommonOrigin(),
      /*boundary_alignment=*/segSize);
  ASSERT_EQ(holder->size(), 1u);
  auto& segment = holder->front();
  ASSERT_EQ(segment.range().left, segOffset);
  ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());

  // Reader spans the whole file in absolute coordinates.
  BufferInputStream reader({ByteRange{
      reinterpret_cast<uint8_t*>(content.data()),
      static_cast<int64_t>(content.size()),
      0}});
  std::vector<char> scratch;
  EXPECT_EQ(
      FileSegment::downloadFromReader(segment, reader, segSize, scratch, 10000),
      segSize);
  segment.completePartAndResetDownloader();
  EXPECT_EQ(segment.state(), FileSegment::State::DOWNLOADED);
  EXPECT_EQ(readFileFully(segment.getPath()), content.substr(segOffset, segSize));
}

// ===========================================================================
// ReadFileByteInputStream — positioned, random-access ByteInputStream over a
// shared ReadFile. seekp uses absolute file offsets and never reads the skipped
// bytes; readBytes is a single positioned pread. It keeps the underlying file
// alive so a segment's reader can outlive the BufferedInput that created it.
// ===========================================================================

TEST_F(FileCacheBufferedInputTest, readFileByteInputStreamPositionedReads) {
  const std::string content = makeContent(64 * 1024);
  auto source = std::make_shared<InMemoryReadFile>(content);
  std::weak_ptr<InMemoryReadFile> weak = source;

  ReadFileByteInputStream stream(source);
  EXPECT_EQ(stream.size(), content.size());
  EXPECT_EQ(stream.remainingSize(), content.size());
  EXPECT_FALSE(stream.atEnd());
  EXPECT_EQ(static_cast<int64_t>(stream.tellp()), 0);

  // Sequential read advances the cursor.
  std::vector<char> out(256);
  stream.readBytes(reinterpret_cast<uint8_t*>(out.data()), 256);
  EXPECT_EQ(std::string(out.data(), 256), content.substr(0, 256));
  EXPECT_EQ(static_cast<int64_t>(stream.tellp()), 256);
  EXPECT_EQ(stream.remainingSize(), content.size() - 256);

  // Absolute forward seek to a far offset reads only the requested bytes.
  stream.seekp(40000);
  EXPECT_EQ(static_cast<int64_t>(stream.tellp()), 40000);
  stream.readBytes(reinterpret_cast<uint8_t*>(out.data()), 512);
  EXPECT_EQ(std::string(out.data(), 512), content.substr(40000, 512));

  // Backward seek is supported (unlike FileInputStream).
  stream.seekp(0);
  EXPECT_EQ(static_cast<int64_t>(stream.tellp()), 0);
  EXPECT_EQ(stream.readByte(), static_cast<uint8_t>(content[0]));

  // skip advances the absolute cursor.
  stream.seekp(0);
  stream.skip(100);
  EXPECT_EQ(static_cast<int64_t>(stream.tellp()), 100);

  // Reaching the end.
  stream.seekp(content.size());
  EXPECT_TRUE(stream.atEnd());
  EXPECT_EQ(stream.remainingSize(), 0u);

  // Out-of-range operations throw rather than silently over-read.
  EXPECT_ANY_THROW(stream.seekp(content.size() + 1));
  stream.seekp(content.size() - 4);
  EXPECT_ANY_THROW(
      stream.readBytes(reinterpret_cast<uint8_t*>(out.data()), 8));

  // The stream keeps the underlying file alive after the caller drops it.
  source.reset();
  EXPECT_FALSE(weak.expired());
  stream.seekp(10);
  stream.readBytes(reinterpret_cast<uint8_t*>(out.data()), 16);
  EXPECT_EQ(std::string(out.data(), 16), content.substr(10, 16));
}

} // namespace
} // namespace facebook::velox::ch
