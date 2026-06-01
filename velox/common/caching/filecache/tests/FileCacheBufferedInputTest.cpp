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
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/filecache/FileCache.h"
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

// Reads up to `size` bytes from `s`, appending the length of every Next() chunk
// to `chunkSizes` so tests can assert the run-based chunking behaviour of
// FileCacheInputStream.
std::string drainRecordingChunks(
    SeekableInputStream& s,
    size_t size,
    std::vector<int32_t>& chunkSizes) {
  std::string buf(size, '\0');
  size_t copied = 0;
  const void* data;
  int32_t len;
  while (copied < size && s.Next(&data, &len)) {
    chunkSizes.push_back(len);
    const size_t toCopy = std::min<size_t>(len, size - copied);
    std::memcpy(buf.data() + copied, data, toCopy);
    copied += toCopy;
  }
  buf.resize(copied);
  return buf;
}

std::string drain(SeekableInputStream& s, size_t size) {
  std::vector<int32_t> ignoredChunkSizes;
  return drainRecordingChunks(s, size, ignoredChunkSizes);
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
      uint64_t boundaryAlignment = 0,
      std::optional<uint64_t> backgroundDownloadThreads = std::nullopt) const {
    FileCacheSettings settings;
    settings.path = cacheDir;
    settings.maxSize = maxSize;
    if (maxFileSegmentSize != 0) {
      settings.maxFileSegmentSize = maxFileSegmentSize;
    }
    if (boundaryAlignment != 0) {
      settings.boundaryAlignment = boundaryAlignment;
    }
    if (backgroundDownloadThreads.has_value()) {
      settings.backgroundDownloadThreads = backgroundDownloadThreads.value();
    }
    settings.validate();
    return settings;
  }

  std::string path(const std::string& name) const {
    return (std::filesystem::path(root_->getPath()) / name).string();
  }

  // Builds an input that reads filePath through the given cache. The cache is
  // owned by the caller so tests can vary its settings and share a single cache
  // across multiple inputs.
  FileCacheBufferedInput makeInput(
      FileCache& cache,
      const std::string& filePath,
      const FileCacheKey& key) const {
    return FileCacheBufferedInput(
        std::make_shared<LocalReadFile>(filePath),
        *pool_,
        &cache,
        key,
        FileCache::getCommonOrigin());
  }

  FileCacheBufferedInput makeInput(
      FileCache& cache,
      const std::string& filePath) const {
    return makeInput(cache, filePath, FileCacheKey::fromPath(filePath));
  }

  std::shared_ptr<TempDirectoryPath> root_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

// Builds an input wired with an IoStatistics sink so the Layer A operator-level
// counters (read/ssdRead/prefetch) can be asserted alongside Layer B.
FileCacheBufferedInput makeInputWithStats(
    memory::MemoryPool& pool,
    FileCache& cache,
    const std::string& filePath,
    const FileCacheKey& key,
    std::shared_ptr<io::IoStatistics> ioStats) {
  return FileCacheBufferedInput(
      std::make_shared<LocalReadFile>(filePath),
      pool,
      &cache,
      key,
      FileCache::getCommonOrigin(),
      CreateFileSegmentSettings{},
      std::move(ioStats));
}

TEST_F(FileCacheBufferedInputTest, enqueueAndLoadReadsExpectedBytes) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(1 << 20);
  writeFile(remotePath, content);

  FileCache cache("e2e_smoke", settings(path("cache")));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

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
  auto input = makeInput(cache, remotePath);

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
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  EXPECT_EQ(drain(*stream, 256 << 10), content);
}

// 64KiB segments, 256KiB region: the region spans >=4 cache segments and the
// run-based stream returns multiple Next() chunks that must reassemble exactly.
TEST_F(FileCacheBufferedInputTest, multiSegmentRegionRoundTripsInChunks) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache(
      "multi_chunk",
      settings(path("cache"), 64ULL << 20, 64ULL << 10, 64ULL << 10));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  std::vector<int32_t> chunks;
  const auto got = drainRecordingChunks(*stream, 256 << 10, chunks);
  EXPECT_EQ(got, content);
  EXPECT_GT(chunks.size(), 1u);
}

// CH-aligned streaming: a multi-MiB cold read of a SINGLE large segment is
// served one <=1MiB working buffer at a time straight from the just-downloaded
// bytes (serve-from-memory). Every Next() chunk is therefore bounded by the 1MiB
// streaming buffer (so a 3MiB region comes back in >=3 chunks), and the cold
// read re-reads no durable cache-file bytes (ssdRead stays 0).
TEST_F(FileCacheBufferedInputTest, streamingServesColdReadInBoundedChunksFromMemory) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(3 << 20); // 3 MiB, one 32MiB-max segment
  writeFile(remotePath, content);

  FileCache cache(
      "streaming_cold",
      settings(
          path("cache"),
          64ULL << 20,
          0,
          0,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);
  auto ioStats = std::make_shared<io::IoStatistics>();

  {
    auto input = makeInputWithStats(*pool_, cache, remotePath, key, ioStats);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);

    std::vector<int32_t> chunks;
    const auto got = drainRecordingChunks(*stream, content.size(), chunks);
    EXPECT_EQ(got, content);
    EXPECT_GE(chunks.size(), 3u);
    for (auto len : chunks) {
      EXPECT_LE(len, 1 << 20);
    }
  }

  EXPECT_EQ(ioStats->ssdRead().sum(), 0u);
  EXPECT_EQ(cache.stats().downloadedBytes, content.size());
}

// Streaming, within a SINGLE stream over one large segment: read the first
// buffer (advancing the download frontier), then SkipInt64 over an un-downloaded
// prefix gap and read the remainder. The landing read sits above the frontier,
// so CASE B must download the whole gap [frontier, landing) plus the served
// window, tee only the window into memory, and still hand back the correct
// bytes. Total downloaded covers the whole region (gap included).
TEST_F(FileCacheBufferedInputTest, streamingForwardGapDownloadsGapAndServesWindow) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(3 << 20); // 3 MiB single segment
  writeFile(remotePath, content);

  FileCache cache(
      "streaming_gap",
      settings(
          path("cache"),
          64ULL << 20,
          0,
          0,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);
  auto ioStats = std::make_shared<io::IoStatistics>();

  {
    auto input = makeInputWithStats(*pool_, cache, remotePath, key, ioStats);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);

    // First Next downloads and serves the leading 1MiB buffer from memory.
    const void* data;
    int32_t len;
    ASSERT_TRUE(stream->Next(&data, &len));
    EXPECT_EQ(std::string(static_cast<const char*>(data), len),
              content.substr(0, len));
    const uint64_t firstEnd = static_cast<uint64_t>(len);

    // Skip a half-MiB un-downloaded gap, then read the rest. The landing offset
    // is above the frontier -> CASE B downloads the gap + window.
    const uint64_t gap = 512 << 10;
    ASSERT_TRUE(stream->SkipInt64(gap));
    const uint64_t landing = firstEnd + gap;
    EXPECT_EQ(
        drain(*stream, content.size() - landing),
        content.substr(landing, content.size() - landing));
  }

  // The gap is downloaded too: the whole region ends up fetched once.
  EXPECT_EQ(cache.stats().downloadedBytes, content.size());

  // The cold read above was served from the tee'd memory window, which could
  // mask a bad/short write to the durable cache file (especially the gap). A
  // fresh warm stream reads the WHOLE region straight from the cache file
  // (CASE A) and must see the correct bytes, gap included -- proving the gap and
  // window were durably written.
  auto warmStats = std::make_shared<io::IoStatistics>();
  {
    auto input = makeInputWithStats(*pool_, cache, remotePath, key, warmStats);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }
  EXPECT_EQ(warmStats->ssdRead().sum(), content.size());
  EXPECT_EQ(warmStats->read().sum(), 0u);
  // Warm pass re-downloaded nothing: everything was already durable.
  EXPECT_EQ(cache.stats().downloadedBytes, content.size());
}

// Streaming with a region whose length is NOT a multiple of the 1MiB working
// buffer: the trailing partial buffer (here 0.5MiB) must be served correctly,
// so a 2.5MiB region comes back as 1MiB + 1MiB + 0.5MiB.
TEST_F(FileCacheBufferedInputTest, streamingServesSubBufferTrailingChunk) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent((2 << 20) + (512 << 10)); // 2.5 MiB
  writeFile(remotePath, content);

  FileCache cache(
      "streaming_tail",
      settings(
          path("cache"),
          64ULL << 20,
          0,
          0,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, content.size()});
  input.load(LogType::FILE);

  std::vector<int32_t> chunks;
  const auto got = drainRecordingChunks(*stream, content.size(), chunks);
  EXPECT_EQ(got, content);
  for (auto len : chunks) {
    EXPECT_LE(len, 1 << 20);
  }
  // 2.5MiB tiled by a 1MiB buffer => 1MiB + 1MiB + 0.5MiB.
  ASSERT_EQ(chunks.size(), 3u);
  EXPECT_EQ(chunks[2], 512 << 10);

  // Re-read warm straight from the cache file (CASE A) to prove the trailing
  // 0.5MiB chunk was durably written, not just served from memory.
  auto warmStats = std::make_shared<io::IoStatistics>();
  {
    const auto key = FileCacheKey::fromPath(remotePath);
    auto warmInput =
        makeInputWithStats(*pool_, cache, remotePath, key, warmStats);
    auto warmStream = warmInput.enqueue({0, content.size()});
    warmInput.load(LogType::FILE);
    EXPECT_EQ(drain(*warmStream, content.size()), content);
  }
  EXPECT_EQ(warmStats->ssdRead().sum(), content.size());
  EXPECT_EQ(warmStats->read().sum(), 0u);
}

// SkipInt64 that crosses several whole segments, then read the remainder.
TEST_F(FileCacheBufferedInputTest, skipSpanningSegmentsThenRead) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache(
      "skip_span",
      settings(path("cache"), 64ULL << 20, 64ULL << 10, 64ULL << 10));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  ASSERT_TRUE(stream->SkipInt64(200 << 10)); // skips ~3 segments
  EXPECT_EQ(
      drain(*stream, (256 << 10) - (200 << 10)),
      content.substr(200 << 10, (256 << 10) - (200 << 10)));
}

// BackUp the entire first chunk and re-read the whole region.
TEST_F(FileCacheBufferedInputTest, backUpFirstChunkRoundTrips) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache(
      "backup",
      settings(path("cache"), 64ULL << 20, 64ULL << 10, 64ULL << 10));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  const void* data;
  int32_t len;
  ASSERT_TRUE(stream->Next(&data, &len));
  ASSERT_GT(len, 0);
  stream->BackUp(len);
  EXPECT_EQ(drain(*stream, 256 << 10), content);
}

// seekToPosition into the middle of a later segment, then read to the end.
TEST_F(FileCacheBufferedInputTest, seekToPositionAcrossSegments) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache(
      "seek_span",
      settings(path("cache"), 64ULL << 20, 64ULL << 10, 64ULL << 10));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  std::vector<uint64_t> positions{150 << 10};
  dwio::common::PositionProvider pp(positions);
  stream->seekToPosition(pp);
  EXPECT_EQ(
      drain(*stream, (256 << 10) - (150 << 10)),
      content.substr(150 << 10, (256 << 10) - (150 << 10)));
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
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    auto input = makeInput(cache, remotePath, key);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  // Second input shares the same cache + key but points at a file with
  // different bytes. A correct cache hit returns the original content.
  {
    auto input = makeInput(cache, wrongPath, key);
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
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({1000, 4096});
  input.load(LogType::FILE);

  ASSERT_TRUE(stream->SkipInt64(1000));
  EXPECT_EQ(drain(*stream, 3096), content.substr(2000, 3096));
}

// Pull invariant: SkipInt64 over the whole region without ever calling Next()
// must not download anything. Under the push model load() eagerly downloads the
// region, so this fails until the download moves to the consume path.
TEST_F(FileCacheBufferedInputTest, skipOnlyDoesNotDownload) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // Background download disabled so nothing is fetched unless the consume path
  // drives it.
  FileCache cache(
      "skip_no_dl",
      settings(
          path("cache"),
          64ULL << 20,
          64ULL << 10,
          64ULL << 10,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();

  {
    auto input = makeInput(cache, remotePath);
    auto stream = input.enqueue({0, 256 << 10});
    input.load(LogType::FILE);
    // Skip the entire region; never call Next(). Pull must not build the inner
    // stream nor download.
    ASSERT_TRUE(stream->SkipInt64(256 << 10));
  }

  EXPECT_EQ(cache.stats().downloadedBytes, 0u);
}

// Pull/coverage invariant: a slice that lies fully inside an already-downloaded
// prefix is a CACHE HIT (CH canStartFromCache = coverage), even though the
// owning segment is only PARTIALLY_DOWNLOADED. Under push, the hit path checks
// state()==DOWNLOADED and the (already-covered) download path records nothing,
// so the read counts as neither hit nor miss until the coverage criterion
// lands.
TEST_F(FileCacheBufferedInputTest, partiallyCoveredSliceCountsAsHit) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // One large (default-sized) segment, background disabled so the segment stays
  // PARTIALLY_DOWNLOADED after the prime.
  FileCache cache(
      "partial_hit",
      settings(
          path("cache"),
          64ULL << 20,
          0,
          0,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  // Prime: download the prefix [0, 200000) only.
  {
    auto input = makeInput(cache, remotePath, key);
    auto stream = input.enqueue({0, 200000});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, 200000), content.substr(0, 200000));
  }
  const auto afterPrime = cache.stats();
  ASSERT_EQ(afterPrime.downloadedBytes, 200000u);

  // A region fully inside the downloaded prefix must be served as a hit with no
  // new source bytes.
  auto ioStats = std::make_shared<io::IoStatistics>();
  {
    auto input = makeInputWithStats(*pool_, cache, remotePath, key, ioStats);
    auto stream = input.enqueue({1000, 4096});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, 4096), content.substr(1000, 4096));
  }
  const auto after = cache.stats();
  EXPECT_GT(after.hits, afterPrime.hits);
  EXPECT_EQ(after.misses, afterPrime.misses);
  EXPECT_EQ(after.downloadedBytes, afterPrime.downloadedBytes);
  EXPECT_EQ(ioStats->ssdRead().sum(), 4096u);
  EXPECT_EQ(ioStats->read().sum(), 0u);
}

// Contrast to skipOnlyDoesNotDownload: skipping does not download, but the
// first Next() after a skip DOES download the segment covering the landing
// position (ensureWithData replays the skip, then loadPosition drives
// loadSegment).
TEST_F(FileCacheBufferedInputTest, skipThenNextDownloadsLandingSegment) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  FileCache cache(
      "skip_then_next",
      settings(
          path("cache"),
          64ULL << 20,
          64ULL << 10,
          64ULL << 10,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 256 << 10});
  input.load(LogType::FILE);

  ASSERT_TRUE(stream->SkipInt64(200 << 10)); // land in a later segment
  EXPECT_EQ(cache.stats().downloadedBytes, 0u); // skip alone downloaded nothing

  const void* data;
  int32_t len;
  ASSERT_TRUE(stream->Next(&data, &len)); // triggers landing-segment download
  ASSERT_GT(len, 0);
  EXPECT_GT(cache.stats().downloadedBytes, 0u);
  EXPECT_EQ(
      std::string(static_cast<const char*>(data), len),
      content.substr(200 << 10, len));
}

// Decision B (per-read-event accounting): two streams over the SAME cold
// segment each drive their own download, so each records a miss -- the pull
// model does not coalesce the count across streams.
TEST_F(FileCacheBufferedInputTest, sharedSegmentCountsMissPerStream) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // One large segment, background disabled.
  FileCache cache(
      "shared_seg",
      settings(
          path("cache"),
          64ULL << 20,
          0,
          0,
          /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);
  auto input = makeInput(cache, remotePath, key);

  auto shallow = input.enqueue({0, 4096});
  auto deep = input.enqueue({8192, 4096}); // same segment, deeper end
  input.load(LogType::FILE);

  EXPECT_EQ(drain(*shallow, 4096), content.substr(0, 4096));
  EXPECT_EQ(drain(*deep, 4096), content.substr(8192, 4096));

  // Two consume-time downloads of the same segment => two misses (decision B).
  EXPECT_EQ(cache.stats().misses, 2u);
  // Forward-resume downloads each gap once: bytes total to the deepest end.
  EXPECT_EQ(cache.stats().downloadedBytes, 8192u + 4096u);
}

TEST_F(FileCacheBufferedInputTest, isBufferedAlwaysFalse) {
  const auto remotePath = path("remote.bin");
  writeFile(remotePath, makeContent(1024));
  FileCache cache("is_buffered", settings(path("cache")));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  EXPECT_FALSE(input.isBuffered(0, 1024));
  EXPECT_TRUE(input.hasCache());
}

TEST_F(FileCacheBufferedInputTest, unsupportedCacheRegionApisThrow) {
  const auto remotePath = path("remote.bin");
  writeFile(remotePath, makeContent(1024));
  FileCache cache("unsupported", settings(path("cache")));
  cache.initialize();
  auto input = makeInput(cache, remotePath);

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
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({0, 64 << 10});
  input.load(LogType::FILE);

  VELOX_ASSERT_THROW(drain(*stream, 64 << 10), "");
}

TEST_F(FileCacheBufferedInputTest, prefixOnlyDownloadStopsAtRequestedEnd) {
  const auto remotePath = path("remote.bin");
  // 256 KiB file fits in a single (32 MiB default) segment.
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // Background download disabled so this test isolates the foreground prefix
  // behavior: the segment tail is never fetched (no background tail-fill).
  FileCache cache(
      "prefix_only",
      settings(path("cache"), 64ULL << 20, 0, 0, /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    auto input = makeInput(cache, remotePath, key);
    // Request only a small prefix of the much larger segment.
    auto stream = input.enqueue({0, 4096});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, 4096), content.substr(0, 4096));
  }

  // The foreground download fetches only the requested prefix; the rest of the
  // segment is left untouched. Background download is disabled, so holder
  // destruction does not trigger a tail-fill.
  const auto stats = cache.stats();
  EXPECT_EQ(stats.downloadedBytes, 4096u);
}

TEST_F(FileCacheBufferedInputTest, coalescedRegionsDownloadToFurthestEnd) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // Background download disabled so the only fetch is the foreground coalesced
  // prefix; the segment tail beyond the furthest requested end stays unfetched.
  FileCache cache(
      "coalesce",
      settings(path("cache"), 64ULL << 20, 0, 0, /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    // Several regions of the SAME segment with different ends in one load().
    // FileCacheInputStream only waits for the download and never drives it
    // itself, so the prefix download must coalesce to the furthest requested
    // end -- otherwise the deepest reader is starved.
    auto input = makeInput(cache, remotePath, key);
    struct Region {
      uint64_t offset;
      uint64_t length;
      std::unique_ptr<SeekableInputStream> stream;
    };
    std::vector<Region> regions;
    regions.push_back({0, 4096, nullptr});
    regions.push_back({8192, 4096, nullptr});
    regions.push_back({200000, 12345, nullptr}); // furthest end: 212345
    for (auto& r : regions) {
      r.stream = input.enqueue({r.offset, r.length});
    }
    input.load(LogType::FILE);
    for (auto& r : regions) {
      EXPECT_EQ(drain(*r.stream, r.length), content.substr(r.offset, r.length))
          << "region offset=" << r.offset;
    }
  }

  // Coalesced target is the furthest requested end (200000 + 12345); the segment
  // tail beyond it is not fetched.
  const auto stats = cache.stats();
  EXPECT_EQ(stats.downloadedBytes, 212345u);
}

TEST_F(FileCacheBufferedInputTest, crossLoadResumeDownloadsRemainingGap) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(256 << 10);
  writeFile(remotePath, content);

  // Background download disabled so the resume path is driven solely by the two
  // foreground load()s, deterministically, with no background tail-fill racing
  // between them.
  FileCache cache(
      "cross_load",
      settings(path("cache"), 64ULL << 20, 0, 0, /*backgroundDownloadThreads=*/0));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    auto input = makeInput(cache, remotePath, key);
    // First load() downloads only a shallow prefix of the segment.
    auto shallow = input.enqueue({0, 4096});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*shallow, 4096), content.substr(0, 4096));

    // A second load() on the SAME input requests a deeper region of the SAME
    // segment, which is now PARTIALLY_DOWNLOADED. The download must resume from
    // the current write offset and cover the deeper end rather than restart or
    // strand the reader.
    auto deep = input.enqueue({200000, 12345});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*deep, 12345), content.substr(200000, 12345));
  }

  // Total bytes pulled from source = the deepest requested end (212345); the
  // resume downloaded only the [4096, 212345) gap, not the whole prefix twice.
  const auto stats = cache.stats();
  EXPECT_EQ(stats.downloadedBytes, 212345u);
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
    // Pull model: load() only plans; drain() drives the download on the
    // consuming thread, so the miss/download counters are final after drain().
    auto input =
        makeInputWithStats(*pool_, cache, remotePath, key, ioStats);
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
    auto input = makeInput(cache, remotePath, key);
    auto stream = input.enqueue({0, content.size()});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, content.size()), content);
  }

  const auto afterCold = cache.stats();

  // Warm pass: every segment is already DOWNLOADED, so it counts as a hit and
  // serves bytes from the local cache file (Layer A ssdRead), downloading none.
  auto ioStats = std::make_shared<io::IoStatistics>();
  {
    auto input =
        makeInputWithStats(*pool_, cache, remotePath, key, ioStats);
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

// initialize() eagerly builds the owned background download executor from the
// configured thread count; a basic read through the cache still works. The
// background executor is what later wires segment tail-fill.
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

// After load() downloads only the requested prefix and the holder is released,
// the FileCache background pool fills the rest of the segment to the background
// target size using the reader the glue wired via setRemoteFileReader. This is
// ClickHouse's "foreground prefix + background tail-fill" model.
TEST_F(FileCacheBufferedInputTest, backgroundFillCompletesSegmentTailAfterHolderRelease) {
  const size_t segmentSize = 64 * 1024;
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(segmentSize);
  writeFile(remotePath, content);

  // One 64KiB segment (alignment must not exceed the segment size). Background
  // download is enabled; its target (default 4MiB) caps at the segment size, so
  // releasing the holder should fill the segment to the full 64KiB.
  FileCache cache(
      "bg_fill",
      settings(
          path("cache"),
          /*maxSize=*/64ULL << 20,
          /*maxFileSegmentSize=*/segmentSize,
          /*boundaryAlignment=*/segmentSize,
          /*backgroundDownloadThreads=*/2));
  cache.initialize();
  const auto key = FileCacheKey::fromPath(remotePath);

  {
    auto input = makeInput(cache, remotePath, key);
    auto stream = input.enqueue({0, 4096});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, 4096), content.substr(0, 4096));
    // While the holder is alive no background fill runs: the foreground fetched
    // only the requested prefix.
    EXPECT_EQ(cache.stats().downloadedBytes, 4096u);
  }

  // Holder released -> complete(allow_background_download=true) enqueues the
  // segment; the background pool fills the tail. Observe via a non-downloader
  // inspection holder. downloadedSize reaches the full size while the worker is
  // still DOWNLOADING; wait for the DOWNLOADED transition (which also resets the
  // reader) before asserting.
  auto holder = cache.getOrSet(
      key,
      /*offset=*/0,
      segmentSize,
      /*file_size=*/segmentSize,
      CreateFileSegmentSettings{},
      /*file_segments_limit=*/0,
      FileCache::getCommonOrigin());
  ASSERT_EQ(holder->size(), 1u);
  auto& segment = holder->front();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while ((segment.getDownloadedSize() < segmentSize ||
          segment.state() != FileSegment::State::DOWNLOADED) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_EQ(segment.getDownloadedSize(), segmentSize);
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

TEST_F(FileCacheBufferedInputTest, downloadFromReaderTeesRequestedWindow) {
  // A multi-chunk download (> kDownloadChunk) with a tee window that straddles a
  // chunk boundary: the helper must copy exactly the absolute window
  // [winStart, winStart+winLen) into dest as it writes, so a foreground consumer
  // obtains the just-downloaded bytes without re-reading the cache file.
  const size_t fileSize = 3 * (1 << 20); // 3 MiB -> 3 download chunks
  const size_t winStart = (3 * (1 << 20)) / 2; // 1.5 MiB, mid-chunk
  const size_t winLen = 512 * 1024;
  std::string content = makeContent(fileSize);

  FileCache cache("download_tee", settings(path("cache")));
  cache.initialize();
  const auto key = FileCacheKey::fromPath("remote.bin");
  FileSegmentsHolderPtr holder;
  auto& segment = acquireDownloader(cache, holder, key, fileSize);

  BufferInputStream reader({ByteRange{
      reinterpret_cast<uint8_t*>(content.data()),
      static_cast<int64_t>(content.size()),
      0}});
  std::vector<char> scratch;
  std::vector<char> dest(winLen, '\0');
  FileSegment::DownloadTee tee{winStart, winLen, dest.data(), 0};
  const size_t written = FileSegment::downloadFromReader(
      segment, reader, fileSize, scratch, 10000, &tee);

  EXPECT_EQ(written, fileSize);
  EXPECT_EQ(tee.copied, winLen);
  EXPECT_EQ(
      std::string(dest.begin(), dest.end()), content.substr(winStart, winLen));
  segment.completePartAndResetDownloader();
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

// Regression for the boundary-alignment divide-by-zero: ClickHouse treats a
// zero boundary_alignment as a valid value (its roundUpToMultiple guards
// `multiple == 0`). getOrSet() must not divide by zero when alignment is 0.
// Before the fix it used bits::roundUp(x, 0) and crashed with SIGFPE.
TEST_F(FileCacheBufferedInputTest, zeroBoundaryAlignmentDoesNotDivideByZero) {
  const auto remotePath = path("remote.bin");
  const auto content = makeContent(1 << 20);
  writeFile(remotePath, content);

  auto s = settings(path("cache"));
  s.boundaryAlignment = 0;
  s.validate();

  FileCache cache("zero_alignment", s);
  cache.initialize();
  auto input = makeInput(cache, remotePath);

  auto stream = input.enqueue({100, 2048});
  input.load(LogType::FILE);

  EXPECT_EQ(drain(*stream, 2048), content.substr(100, 2048));
}

} // namespace
} // namespace facebook::velox::ch
