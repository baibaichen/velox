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

// Task 018a: connector integration. Drives the real Hive connector extension
// point BufferedInputBuilder::getInstance()->create(...) and asserts that with
// FileCacheBufferedInputBuilder installed the read path selects our
// FileCacheBufferedInput (and populates/hits the FileCache), while an
// uninstalled deployment keeps the native buffered input. Also covers
// install-time fail-fast validation and the FileCache/AsyncDataCache
// mutual-exclusion guard.

#include "velox/ch/Disks/IO/FileCacheBufferedInputBuilder.h"

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch
{
namespace
{
namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;
using connector::ConnectorQueryCtx;
using connector::hive::BufferedInputBuilder;

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

/// `velox::ReadFile` that counts physical source reads: used to prove a cache
/// hit serves bytes from the local segment file (source read count does not grow).
class CountingReadFile : public velox::ReadFile
{
public:
    explicit CountingReadFile(const std::string & path) : inner_(std::make_unique<velox::LocalReadFile>(path)) { }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const velox::FileIoContext & ctx = {})
        const override
    {
        preadCount_.fetch_add(1);
        return inner_->pread(offset, length, buf, ctx);
    }

    uint64_t
    preadv(uint64_t offset, const std::vector<folly::Range<char *>> & buffers, const velox::FileIoContext & ctx = {})
        const override
    {
        preadCount_.fetch_add(1);
        return inner_->preadv(offset, buffers, ctx);
    }

    bool shouldCoalesce() const override { return inner_->shouldCoalesce(); }
    uint64_t size() const override { return inner_->size(); }
    uint64_t memoryUsage() const override { return inner_->memoryUsage(); }
    std::string getName() const override { return inner_->getName(); }
    uint64_t getNaturalReadSize() const override { return inner_->getNaturalReadSize(); }

    uint64_t preadCount() const { return preadCount_.load(); }

private:
    std::unique_ptr<velox::LocalReadFile> inner_;
    mutable std::atomic_uint64_t preadCount_{0};
};

class FileCacheBufferedInputBuilderTest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { filesystems::registerLocalFileSystem(); }

    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        pool_ = memoryManager_.addLeafPool("filecache-connector-test");
        connectorPool_ = memoryManager_.addLeafPool("filecache-connector-test-connector");
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
        sessionProperties_ = std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{});
    }

    void TearDown() override
    {
        // Order matters: re-register the native default builder FIRST so the
        // FileCacheBufferedInputBuilder (which aliases manager_ by reference) is
        // dropped before manager_ is shut down and reset below. Resetting the
        // Manager while a builder still referencing it were registered would
        // leave a dangling reference.
        BufferedInputBuilder::registerBuilder(makeDefaultBuilderClone());
        if (FileCacheManager::getInstance())
        {
            FileCacheManager::getInstance()->shutdown();
            FileCacheManager::setInstance(nullptr);
        }
        manager_.reset();
    }

    // Re-create a DefaultBufferInputBuilder-equivalent. The trunk's
    // DefaultBufferInputBuilder is file-local; we restore the native path by
    // registering a fresh instance that forwards to createBufferedInput, matching
    // the static default. (Named via an inline builder below.)
    std::shared_ptr<BufferedInputBuilder> makeDefaultBuilderClone();

    std::string sub(const std::string & s) const { return (fs::path(temp_->getPath()) / s).string(); }

    std::string writeSourceFile(const std::string & name, const std::string & content)
    {
        const auto path = sub(name);
        std::ofstream(path, std::ios::binary) << content;
        return path;
    }

    // Build + install a FileCacheManager with one default cache.
    FileCachePtr makeManagerCache(const std::string & defaultName = "default")
    {
        FileCacheConfig c;
        c.path = sub("cache");
        c.maxSize = 16 * 1024 * 1024;
        c.maxElements = 100;
        c.maxFileSegmentSize = 64 * 1024;
        c.boundaryAlignment = 1;
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
        o.defaultCacheName = defaultName;
        o.caches.push_back({"default", c, "conf.default"});

        manager_ = FileCacheManager::create(o);
        FileCacheManager::setInstance(manager_.get());
        return manager_->getDefault();
    }

    // Build a FileCacheManager WITHOUT resolving the default (used for the
    // empty-default fail-fast case, where getDefault() must throw).
    void makeManagerWithEmptyDefault()
    {
        FileCacheConfig c;
        c.path = sub("cache");
        c.maxSize = 16 * 1024 * 1024;
        c.maxElements = 100;
        c.maxFileSegmentSize = 64 * 1024;
        c.boundaryAlignment = 1;
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
        o.defaultCacheName = ""; // empty default: allowed at create, getDefault throws
        o.caches.push_back({"default", c, "conf.default"});

        manager_ = FileCacheManager::create(o);
        FileCacheManager::setInstance(manager_.get());
    }

    dwio::common::ReaderOptions readerOptions()
    {
        dwio::common::ReaderOptions opts(pool_.get());
        opts.setFileFormat(dwio::common::FileFormat::DWRF);
        return opts;
    }

    // A minimal ConnectorQueryCtx: create() reads only cache() + queryId(); the
    // native DirectBufferedInput fallback additionally needs scanId() (taskId +
    // planNodeId), which are provided.
    std::unique_ptr<ConnectorQueryCtx> makeCtx(cache::AsyncDataCache * cache)
    {
        return std::make_unique<ConnectorQueryCtx>(
            pool_.get(),
            connectorPool_.get(),
            sessionProperties_.get(),
            /*spillConfig*/ nullptr,
            common::PrefixSortConfig(),
            /*expressionEvaluator*/ nullptr,
            cache,
            /*queryId*/ "q1",
            /*taskId*/ "task1",
            /*planNodeId*/ "plan1",
            /*driverId*/ 0,
            /*sessionTimezone*/ "");
    }

    FileHandle makeFileHandle(std::shared_ptr<velox::ReadFile> file)
    {
        FileHandle h;
        h.file = std::move(file);
        return h;
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<velox::memory::MemoryPool> connectorPool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<config::ConfigBase> sessionProperties_;
    std::shared_ptr<FileCacheManager> manager_;
};

// A native-forwarding builder equivalent to the trunk's file-local
// DefaultBufferInputBuilder. Used to restore the default between tests so a
// leaked registration cannot false-green case 2.
class NativeForwardingBuilder final : public BufferedInputBuilder
{
public:
    std::unique_ptr<dwio::common::BufferedInput> create(
        const FileHandle & fileHandle,
        const dwio::common::ReaderOptions & readerOpts,
        const ConnectorQueryCtx * connectorQueryCtx,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        std::shared_ptr<IoStats> ioStats,
        folly::Executor * executor,
        const folly::F14FastMap<std::string, std::string> & fileReadOps) override
    {
        return connector::hive::createBufferedInput(
            fileHandle, readerOpts, connectorQueryCtx, ioStatistics, ioStats, executor, fileReadOps);
    }
};

std::shared_ptr<BufferedInputBuilder> FileCacheBufferedInputBuilderTest::makeDefaultBuilderClone()
{
    return std::make_shared<NativeForwardingBuilder>();
}

// ============================================================================
// Case 1: FileCache selected — create() returns FileCacheBufferedInput and the
// read populates/hits our cache.
// ============================================================================
TEST_F(FileCacheBufferedInputBuilderTest, FileCacheSelectedAndCacheHit)
{
    const size_t n = 200 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);
    auto path = writeSourceFile("src", content);

    registerFileCacheBufferedInputBuilder(*manager_);

    // A minimal ctx with no AsyncDataCache installed.
    auto ctx = makeCtx(/*cache*/ nullptr);

    // Cold read: the source is wrapped in a CountingReadFile so we can prove the
    // second read is a cache hit (no source I/O).
    auto countingA = std::make_shared<CountingReadFile>(path);
    auto handleA = makeFileHandle(countingA);
    auto inputA = BufferedInputBuilder::getInstance()->create(
        handleA,
        readerOptions(),
        ctx.get(),
        std::make_shared<io::IoStatistics>(),
        std::make_shared<velox::IoStats>(),
        executor_.get());

    // The selected buffered input is our FileCacheBufferedInput.
    auto * fcInput = dynamic_cast<FileCacheBufferedInput *>(inputA.get());
    ASSERT_NE(fcInput, nullptr) << "builder must select FileCacheBufferedInput";

    // Drive the real read path: enqueue a region and drain it.
    {
        auto stream = fcInput->enqueue({0, n});
        EXPECT_EQ(readAll(*stream), content);
    }
    EXPECT_GT(cache->getFileSegmentsNum(), 0u) << "cold read must populate the FileCache";
    EXPECT_GT(countingA->preadCount(), 0u) << "cold read must touch the source";

    // Second read through a fresh input for the same path: served from cache, so
    // the (new) source read file is never touched.
    auto countingB = std::make_shared<CountingReadFile>(path);
    auto handleB = makeFileHandle(countingB);
    auto inputB = BufferedInputBuilder::getInstance()->create(
        handleB,
        readerOptions(),
        ctx.get(),
        std::make_shared<io::IoStatistics>(),
        std::make_shared<velox::IoStats>(),
        executor_.get());
    auto * fcInputB = dynamic_cast<FileCacheBufferedInput *>(inputB.get());
    ASSERT_NE(fcInputB, nullptr);
    EXPECT_TRUE(fcInputB->isBuffered(0, n));
    {
        auto stream = fcInputB->enqueue({0, n});
        EXPECT_EQ(readAll(*stream), content);
    }
    EXPECT_EQ(countingB->preadCount(), 0u) << "cache hit must not read the source";
}

// ============================================================================
// Case 1b: the IoStatistics handed to the builder's create() (the exact path
// the Hive connector / Gluten use) receives the operator-level hit/source
// bytes. This is what surfaces as storageReadBytes / localReadBytes in
// OperatorStats. Proves the builder path forwards ioStats end to end, isolating
// any missing-metric problem to the layer above (Gluten).
// ============================================================================
TEST_F(FileCacheBufferedInputBuilderTest, BuilderPathRecordsBytesInIoStatistics)
{
    const size_t n = 200 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);
    auto path = writeSourceFile("src", content);

    registerFileCacheBufferedInputBuilder(*manager_);
    auto ctx = makeCtx(/*cache*/ nullptr);

    // Cold read through the builder: source bytes -> read() (storageReadBytes).
    auto ioStatsCold = std::make_shared<io::IoStatistics>();
    {
        auto handle = makeFileHandle(std::make_shared<CountingReadFile>(path));
        auto input = BufferedInputBuilder::getInstance()->create(
            handle,
            readerOptions(),
            ctx.get(),
            ioStatsCold,
            std::make_shared<velox::IoStats>(),
            executor_.get());
        ASSERT_NE(dynamic_cast<FileCacheBufferedInput *>(input.get()), nullptr);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    // Both sum and count must be non-zero: getRuntimeStats gates on count() > 0,
    // so a metric with count 0 never reaches OperatorStats.
    EXPECT_EQ(ioStatsCold->read().sum(), n);
    EXPECT_GT(ioStatsCold->read().count(), 0u);
    EXPECT_EQ(ioStatsCold->ssdRead().sum(), 0u);

    // Warm read through the builder: cache hit -> ssdRead() (localReadBytes).
    auto ioStatsWarm = std::make_shared<io::IoStatistics>();
    {
        auto handle = makeFileHandle(std::make_shared<CountingReadFile>(path));
        auto input = BufferedInputBuilder::getInstance()->create(
            handle,
            readerOptions(),
            ctx.get(),
            ioStatsWarm,
            std::make_shared<velox::IoStats>(),
            executor_.get());
        ASSERT_NE(dynamic_cast<FileCacheBufferedInput *>(input.get()), nullptr);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    EXPECT_EQ(ioStatsWarm->ssdRead().sum(), n);
    EXPECT_GT(ioStatsWarm->ssdRead().count(), 0u);
    EXPECT_EQ(ioStatsWarm->read().sum(), 0u);
}

// ============================================================================
// Case 2: not-installed deployment keeps the native buffered input.
// ============================================================================
TEST_F(FileCacheBufferedInputBuilderTest, NotInstalledKeepsNative)
{
    const size_t n = 100 * 1024;
    auto content = makeContent(n);
    auto path = writeSourceFile("src", content);

    // Do NOT register our builder. Restore the native default first in case a
    // prior test leaked a registration.
    BufferedInputBuilder::registerBuilder(makeDefaultBuilderClone());

    auto ctx = makeCtx(/*cache*/ nullptr);
    auto handle = makeFileHandle(std::make_shared<velox::LocalReadFile>(path));
    auto input = BufferedInputBuilder::getInstance()->create(
        handle,
        readerOptions(),
        ctx.get(),
        std::make_shared<io::IoStatistics>(),
        std::make_shared<velox::IoStats>(),
        executor_.get());

    // The native path is selected: NOT a FileCacheBufferedInput.
    EXPECT_EQ(dynamic_cast<FileCacheBufferedInput *>(input.get()), nullptr)
        << "uninstalled deployment must keep the native buffered input";

    // And a read still succeeds through the native input.
    auto stream = input->read(0, n, dwio::common::LogType::FILE);
    EXPECT_EQ(readAll(*stream), content);
}

// ============================================================================
// Case 3: install-time validation is fail-fast on an empty default cache name.
// ============================================================================
TEST_F(FileCacheBufferedInputBuilderTest, InstallTimeFailFastOnEmptyDefault)
{
    makeManagerWithEmptyDefault();

    // Record the currently-registered builder so we can prove registration did
    // not happen as a side effect of the throwing install call.
    auto before = BufferedInputBuilder::getInstance().get();

    EXPECT_THROW(registerFileCacheBufferedInputBuilder(*manager_), VeloxRuntimeError)
        << "install must fail-fast when the Manager has no default cache";

    // No builder was registered as a side effect: the singleton is unchanged.
    EXPECT_EQ(BufferedInputBuilder::getInstance().get(), before)
        << "a throwing install must not register a builder";
}

// ============================================================================
// Case 4: mutual-exclusion guard — FileCache + AsyncDataCache both active throws.
// ============================================================================
TEST_F(FileCacheBufferedInputBuilderTest, MutualExclusionGuardThrows)
{
    auto content = makeContent(64 * 1024);
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);
    auto path = writeSourceFile("src", content);

    registerFileCacheBufferedInputBuilder(*manager_);

    // Install a real AsyncDataCache into the ctx: both caches active.
    auto asyncCache = cache::AsyncDataCache::create(memoryManager_.allocator());
    auto ctx = makeCtx(asyncCache.get());
    auto handle = makeFileHandle(std::make_shared<velox::LocalReadFile>(path));

    VELOX_ASSERT_THROW(
        BufferedInputBuilder::getInstance()->create(
            handle,
            readerOptions(),
            ctx.get(),
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get()),
        "cannot both be installed");

    asyncCache->shutdown();
}

} // namespace
} // namespace facebook::velox::ch
