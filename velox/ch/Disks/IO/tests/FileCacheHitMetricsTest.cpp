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

// Task 017 post-acceptance amendment 1: prove the three global ProfileEvents
// hit/source/write byte counters are actually driven from the assembled read
// path.
//
//   CachedReadBufferReadFromCacheBytes   -- bytes served from a cache segment (hit)
//   CachedReadBufferReadFromSourceBytes  -- bytes read from the source (miss/refetch)
//   CachedReadBufferCacheWriteBytes      -- bytes written into cache (download)
//
// Attribution uses the existing `ReadType` decision in FileCacheInputStream:
// a CACHED read is a hit; a REMOTE_FS_READ_* read is a source read; a successful
// writeCache during download is a cache write. Assertions use the cumulative
// `ProfileEvents::get()` before/after DIFF (the counters are process-wide).

#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::ch
{
namespace
{
namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;

std::string makeContent(size_t n)
{
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i)
        s[i] = static_cast<char>((static_cast<uint32_t>(i) * 2654435761u) >> 24);
    return s;
}

std::string readAll(dwio::common::SeekableInputStream & stream)
{
    std::string out;
    const void * data = nullptr;
    int32_t size = 0;
    while (stream.Next(&data, &size))
        out.append(static_cast<const char *>(data), static_cast<size_t>(size));
    return out;
}

// Snapshot of the three hit/source/write counters for before/after diffing.
struct HitMetrics
{
    uint64_t cacheBytes;
    uint64_t sourceBytes;
    uint64_t writeBytes;

    static HitMetrics snapshot()
    {
        return {
            ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheBytes),
            ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes),
            ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes)};
    }
};

class FileCacheHitMetricsTest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { filesystems::registerLocalFileSystem(); }

    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        pool_ = memoryManager_.addLeafPool("filecache-hitmetrics-test");
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
    }

    void TearDown() override
    {
        if (FileCacheManager::getInstance())
        {
            FileCacheManager::getInstance()->shutdown();
            FileCacheManager::setInstance(nullptr);
        }
        manager_.reset();
    }

    std::string sub(const std::string & s) const { return (fs::path(temp_->getPath()) / s).string(); }

    std::string writeSourceFile(const std::string & name, const std::string & content)
    {
        const auto path = sub(name);
        std::ofstream(path, std::ios::binary) << content;
        return path;
    }

    FileCachePtr makeManagerCache(size_t seg, size_t align, size_t maxSize = 16 * 1024 * 1024)
    {
        FileCacheConfig c;
        c.path = sub("cache");
        c.maxSize = maxSize;
        c.maxElements = 100;
        c.maxFileSegmentSize = seg;
        c.boundaryAlignment = align;
        c.reserveGranularity = 1;
        c.cachePolicy = FileCachePolicy::LRU;
        c.useSplitCache = false;
        c.backgroundDownloadThreads = 0;
        c.loadMetadataThreads = 2;
        c.loadMetadataAsynchronously = false;
        c.keepFreeSpaceSizeRatio = 0.0;
        c.keepFreeSpaceElementsRatio = 0.0;

        FileCacheManager::Options o;
        o.commonUserId = "user-A";
        o.localFileSystem = filesystems::getFileSystem("/", nullptr);
        o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
        o.initializeOnCreate = true;
        o.defaultCacheName = "default";
        o.caches.push_back({"default", c, "conf.default"});

        manager_ = FileCacheManager::create(o);
        FileCacheManager::setInstance(manager_.get());
        auto cache = manager_->getDefault();
        EXPECT_NE(cache, nullptr);
        return cache;
    }

    dwio::common::ReaderOptions readerOptions() { return dwio::common::ReaderOptions(pool_.get()); }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key, FileCacheReadOptions readOptions = {})
    {
        FileCacheRequestContext ctx;
        ctx.queryId = "q1";
        ctx.userId = manager_->commonUserId();
        auto origin = cache->getCommonOrigin();
        return std::make_unique<FileCacheBufferedInput>(
            std::make_shared<velox::LocalReadFile>(path),
            std::move(cache),
            key,
            origin,
            readOptions,
            ctx,
            dwio::common::MetricsLog::voidLog(),
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get(),
            readerOptions());
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<FileCacheManager> manager_;
};

// ============================================================================
// A cold miss reads N bytes from the source AND writes N bytes into the cache;
// the hit counter does not move.
// ============================================================================
TEST_F(FileCacheHitMetricsTest, ColdMissCountsSourceAndWriteBytes)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    const auto before = HitMetrics::snapshot();
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->enqueue({0, n});
        EXPECT_EQ(readAll(*stream), content);
    }
    const auto after = HitMetrics::snapshot();

    // All N bytes were read from the source (miss) and all N written into cache.
    EXPECT_EQ(after.sourceBytes - before.sourceBytes, n);
    EXPECT_EQ(after.writeBytes - before.writeBytes, n);
    // A cold miss serves nothing from a cache segment.
    EXPECT_EQ(after.cacheBytes - before.cacheBytes, 0u);
}

// ============================================================================
// After a fill, re-reading the same range serves N bytes from the cache (hit);
// neither the source nor the write counter moves.
// ============================================================================
TEST_F(FileCacheHitMetricsTest, HitCountsCacheBytes)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Warm the cache.
    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }

    const auto before = HitMetrics::snapshot();
    {
        auto input = makeInput(cache, path, key);
        EXPECT_TRUE(input->isBuffered(0, n));
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    const auto after = HitMetrics::snapshot();

    // The whole range was served from the cache segments (hit).
    EXPECT_EQ(after.cacheBytes - before.cacheBytes, n);
    // A pure hit reads nothing from the source and writes nothing new.
    EXPECT_EQ(after.sourceBytes - before.sourceBytes, 0u);
    EXPECT_EQ(after.writeBytes - before.writeBytes, 0u);
}

// ============================================================================
// A bypass read (readIfExistsOtherwiseBypass on a cold key) reads N bytes from
// the source but writes nothing into the cache.
// ============================================================================
TEST_F(FileCacheHitMetricsTest, BypassCountsSourceOnly)
{
    const size_t n = 100 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    FileCacheReadOptions opts;
    opts.readIfExistsOtherwiseBypass = true;

    const auto before = HitMetrics::snapshot();
    {
        auto input = makeInput(cache, path, key, opts);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    const auto after = HitMetrics::snapshot();

    EXPECT_EQ(after.sourceBytes - before.sourceBytes, n);
    EXPECT_EQ(after.writeBytes - before.writeBytes, 0u);
    EXPECT_EQ(after.cacheBytes - before.cacheBytes, 0u);
}

// ============================================================================
// Hit ratio arithmetic at the consumer side: after a fill, one full re-read is
// a 100% hit (ratio == cache / (cache + source) == 1 for that window).
// ============================================================================
TEST_F(FileCacheHitMetricsTest, HitRatioComputableFromCounters)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }

    const auto before = HitMetrics::snapshot();
    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    const auto after = HitMetrics::snapshot();

    const uint64_t hit = after.cacheBytes - before.cacheBytes;
    const uint64_t source = after.sourceBytes - before.sourceBytes;
    ASSERT_GT(hit + source, 0u);
    EXPECT_DOUBLE_EQ(static_cast<double>(hit) / static_cast<double>(hit + source), 1.0);
}

// ============================================================================
// A cold read that starts partway into a fresh segment forces a predownload of
// the [segmentStart, offset) prefix. Those predownloaded bytes are read from the
// source and written into the cache, so BOTH the source and write counters must
// include the predownloaded prefix (mirrors CH, which attributes predownload
// bytes to CachedReadBufferReadFromSourceBytes and CachedReadBufferCacheWriteBytes).
// ============================================================================
TEST_F(FileCacheHitMetricsTest, PredownloadCountsSourceAndWriteBytes)
{
    const size_t seg = 64 * 1024;
    const size_t n = seg; // single segment [0, seg)
    // Read a window that starts inside the segment (offset 16 KiB), so the
    // downloader must predownload [0, 16 KiB) before serving [16 KiB, seg).
    const size_t off = 16 * 1024;
    const size_t len = seg - off;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ seg);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    const auto before = HitMetrics::snapshot();
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->enqueue({off, len});
        EXPECT_EQ(readAll(*stream), content.substr(off, len));
    }
    const auto after = HitMetrics::snapshot();

    // The predownloaded prefix [0, off) plus the served window [off, seg) were all
    // read from the source and written into cache: source and write deltas both
    // cover the whole segment, strictly more than the `len` bytes served to the
    // caller. Without predownload attribution the deltas would be only `len`.
    EXPECT_GE(after.sourceBytes - before.sourceBytes, off + len);
    EXPECT_GE(after.writeBytes - before.writeBytes, off + len);
    EXPECT_GT(after.sourceBytes - before.sourceBytes, len)
        << "predownloaded prefix must be counted as source bytes";
    EXPECT_GT(after.writeBytes - before.writeBytes, len)
        << "predownloaded prefix must be counted as cache-write bytes";
    // Nothing was served from an existing cache segment on this cold read.
    EXPECT_EQ(after.cacheBytes - before.cacheBytes, 0u);
}

} // namespace
} // namespace facebook::velox::ch
