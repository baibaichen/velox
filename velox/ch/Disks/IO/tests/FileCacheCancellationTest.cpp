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

// Task 017: cancellation wiring for FileCacheInputStream::Next. Drives the
// assembled read path through a real FileCacheManager and asserts that a
// cancelled QueryStatus makes Next throw at the safe check points, and that no
// downloader lease survives a cancellation (segments are not left DOWNLOADING
// and no caller id is registered on any segment visible to the stream).

#include "velox/ch/Common/QueryStatus.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <folly/CancellationToken.h>
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

class FileCacheCancellationTest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { filesystems::registerLocalFileSystem(); }

    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        pool_ = memoryManager_.addLeafPool("filecache-cancel-test");
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

    std::string sub(const std::string & s) const
    {
        return (fs::path(temp_->getPath()) / s).string();
    }

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

    dwio::common::ReaderOptions readerOptions()
    {
        return dwio::common::ReaderOptions(pool_.get());
    }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key)
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
            FileCacheReadOptions{},
            ctx,
            dwio::common::MetricsLog::voidLog(),
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get(),
            readerOptions());
    }

    // Construct a FileCacheInputStream directly with an explicit QueryStatus so
    // the test controls the cancellation token. enqueue() itself uses a default
    // (never-cancel) QueryStatus.
    std::unique_ptr<FileCacheInputStream> makeStream(
        FileCacheBufferedInput & input,
        velox::common::Region region,
        QueryStatus status)
    {
        FileCacheRequestContext ctx;
        ctx.queryId = "q1";
        ctx.userId = manager_->commonUserId();
        return std::make_unique<FileCacheInputStream>(
            &input, region, ctx, dwio::common::LogType::STREAM, std::move(status));
    }

    // No FileSegment held by any stream is left DOWNLOADING, and none reports a
    // downloader lease. This is the "downloader not held across a cancellation"
    // invariant: a no-create get() snapshot of the whole range must show only
    // non-DOWNLOADING states with isDownloader()==false.
    void expectNoHeldDownloader(FileCachePtr cache, const FileCacheKey & key,
        uint64_t offset, uint64_t length, const FileCacheOriginInfo & origin)
    {
        auto holder = cache->get(key, offset, length, 100, origin.user_id);
        if (!holder)
            return;
        for (const auto & segPtr : *holder)
        {
            const auto & seg = *segPtr;
            EXPECT_NE(seg.state(), FileSegment::State::DOWNLOADING)
                << "segment left DOWNLOADING after cancellation";
            EXPECT_FALSE(seg.isDownloader())
                << "downloader lease still held after cancellation";
        }
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<FileCacheManager> manager_;
};

// A cancelled token before the very first read makes Next throw at safe point 1
// (initializeIfNeeded, before getOrSet), leaving no DOWNLOADING segment behind.
TEST_F(FileCacheCancellationTest, NextThrowsWhenCancelledBeforeRead)
{
    const size_t n = 256 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    folly::CancellationSource src;
    src.requestCancellation();
    auto stream = makeStream(*input, {0, n}, QueryStatus{src.getToken()});

    const void * data = nullptr;
    int32_t size = 0;
    EXPECT_THROW(stream->Next(&data, &size), VeloxRuntimeError);

    // No segment was created or left DOWNLOADING by the aborted read.
    expectNoHeldDownloader(cache, key, 0, n, input->origin());
}

// Read the first segment successfully with a live token, then cancel and prove
// the next Next() throws and leaves no downloader lease.
TEST_F(FileCacheCancellationTest, NextThrowsWhenCancelledAfterFirstSegment)
{
    const size_t seg = 64 * 1024;
    const size_t n = 4 * seg; // 4 segments
    auto content = makeContent(n);
    auto cache = makeManagerCache(seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    folly::CancellationSource src;
    auto stream = makeStream(*input, {0, n}, QueryStatus{src.getToken()});

    // Read exactly the first segment's worth of bytes.
    std::string got;
    const void * data = nullptr;
    int32_t size = 0;
    while (got.size() < seg && stream->Next(&data, &size))
    {
        const size_t take = std::min<size_t>(seg - got.size(), static_cast<size_t>(size));
        got.append(static_cast<const char *>(data), take);
        if (take < static_cast<size_t>(size))
            stream->BackUp(static_cast<int32_t>(static_cast<size_t>(size) - take));
    }
    EXPECT_EQ(got, content.substr(0, seg));

    // Cancel, then the next Next() must throw at a safe point.
    src.requestCancellation();
    EXPECT_THROW(stream->Next(&data, &size), VeloxRuntimeError);

    expectNoHeldDownloader(cache, key, 0, n, input->origin());
}

// A default (never-cancel) QueryStatus reads the whole region with no exception.
TEST_F(FileCacheCancellationTest, NoCancellationTokenNeverCancels)
{
    const size_t n = 200 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    auto stream = makeStream(*input, {0, n}, QueryStatus{});
    EXPECT_EQ(readAll(*stream), content);
    expectNoHeldDownloader(cache, key, 0, n, input->origin());
}

// A stream constructed through the public enqueue() path (which supplies a
// default QueryStatus) reads the whole region and never cancels.
TEST_F(FileCacheCancellationTest, EnqueuePathUsesNeverCancelStatus)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    auto stream = input->enqueue({0, n});
    EXPECT_EQ(readAll(*stream), content);
}

// The downloader lease is never held across the cancellation check: cancelling
// mid-stream (after some segments are DOWNLOADED) throws, and a no-create
// snapshot confirms no segment is DOWNLOADING and none reports a downloader.
TEST_F(FileCacheCancellationTest, CancellationDoesNotLeakDownloaderLease)
{
    const size_t seg = 64 * 1024;
    const size_t n = 3 * seg;
    auto content = makeContent(n);
    auto cache = makeManagerCache(seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    folly::CancellationSource src;
    auto stream = makeStream(*input, {0, n}, QueryStatus{src.getToken()});

    // Read into the second segment (forces the first to DOWNLOADED and elects a
    // downloader for the second at least once).
    std::string got;
    const void * data = nullptr;
    int32_t size = 0;
    while (got.size() < seg + 1 && stream->Next(&data, &size))
        got.append(static_cast<const char *>(data), static_cast<size_t>(size));
    EXPECT_GE(got.size(), seg + 1);

    src.requestCancellation();
    // Drive the stream until it either finishes the region or throws. If more
    // data remains, the next safe-point check must throw.
    bool threw = false;
    try
    {
        while (stream->Next(&data, &size))
        {
            got.append(static_cast<const char *>(data), static_cast<size_t>(size));
        }
    }
    catch (const VeloxRuntimeError &)
    {
        threw = true;
    }
    EXPECT_TRUE(threw) << "cancellation must eventually throw while data remains";

    // No caller id is registered on any segment after the exception.
    expectNoHeldDownloader(cache, key, 0, n, input->origin());
}

} // namespace
} // namespace facebook::velox::ch
