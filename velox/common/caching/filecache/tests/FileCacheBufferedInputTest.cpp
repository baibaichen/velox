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
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/MetricsLog.h"

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

} // namespace
} // namespace facebook::velox::ch
