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
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/PositionProvider.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/common/testutil/TestValue.h"

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/synchronization/Baton.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using velox::common::testutil::ScopedTestValue;
using velox::common::testutil::TempDirectoryPath;
using velox::common::testutil::TestValue;

const char * const kFailPoint =
    "facebook::velox::ch::filecache::failpoint::cache_filesystem_failure";

std::string makeData(size_t n)
{
    std::string d(n, 0);
    for (size_t i = 0; i < n; ++i)
        d[i] = static_cast<char>('a' + (i % 26));
    return d;
}

// Bounded spin (not a fixed sleep) until `pred` holds or the deadline passes.
// Used to observe asynchronous background-download progress deterministically,
// mirroring the waitFor helper in MetadataTest.
bool spinUntil(const std::function<bool()> & pred, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
            return true;
        std::this_thread::yield();
    }
    return pred();
}

/// In-memory source that counts the bytes physically pread. `declaredSize` may
/// exceed the actual byte count to simulate a remote object listed larger than
/// the bytes present (external truncation) -- a pread past the real data returns
/// an empty view (physical EOF), so a reader hits EOF before `declaredSize`.
class CountingReadFile : public ReadFile
{
public:
    explicit CountingReadFile(std::string data, std::optional<uint64_t> declaredSize = std::nullopt)
        : data_(std::move(data)), declaredSize_(declaredSize.value_or(data_.size()))
    {
    }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const FileIoContext & = {}) const override
    {
        ++preadCalls_;
        if (offset >= data_.size())
            return {};
        const uint64_t n = std::min<uint64_t>(length, data_.size() - offset);
        std::memcpy(buf, data_.data() + offset, n);
        preadBytes_ += n;
        return std::string_view(static_cast<const char *>(buf), n);
    }

    uint64_t size() const override { return declaredSize_; }
    uint64_t memoryUsage() const override { return data_.size(); }
    bool shouldCoalesce() const override { return false; }
    std::string getName() const override { return "<CountingReadFile>"; }
    uint64_t getNaturalReadSize() const override { return 1024; }

    uint64_t preadBytes() const { return preadBytes_.load(); }
    uint64_t preadCalls() const { return preadCalls_.load(); }

private:
    std::string data_;
    uint64_t declaredSize_;
    mutable std::atomic<uint64_t> preadBytes_{0};
    mutable std::atomic<uint64_t> preadCalls_{0};
};

/// Source whose FIRST pread blocks until `release` is posted, parking a downloader
/// inside a DOWNLOADING segment so another reader is forced onto FileSegment::wait.
/// Subsequent preads serve data normally.
class StallingReadFile : public ReadFile
{
public:
    StallingReadFile(std::string data, std::atomic<bool> & entered, folly::Baton<> & release)
        : data_(std::move(data)), entered_(entered), release_(release)
    {
    }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const FileIoContext & = {}) const override
    {
        if (!stalled_.exchange(true))
        {
            entered_.store(true);
            release_.wait();
        }
        if (offset >= data_.size())
            return {};
        const uint64_t n = std::min<uint64_t>(length, data_.size() - offset);
        std::memcpy(buf, data_.data() + offset, n);
        return std::string_view(static_cast<const char *>(buf), n);
    }

    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    bool shouldCoalesce() const override { return false; }
    std::string getName() const override { return "<StallingReadFile>"; }
    uint64_t getNaturalReadSize() const override { return 1024; }

private:
    std::string data_;
    std::atomic<bool> & entered_;
    folly::Baton<> & release_;
    mutable std::atomic<bool> stalled_{false};
};

/// Source whose pread busy-spins (never a sleep) for a bounded duration against a
/// steady-clock deadline before returning data, so the wall-clock time spent
/// reading the source is observably positive. Used to prove that source-read and
/// predownload latency counters (scan time / microseconds) are actually wired.
class SpinningReadFile : public ReadFile
{
public:
    SpinningReadFile(std::string data, std::chrono::microseconds spin)
        : data_(std::move(data)), spin_(spin)
    {
    }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const FileIoContext & = {}) const override
    {
        // Bounded busy-spin against a steady-clock deadline -- deterministic and
        // sleep-free, so the source read provably consumes wall-clock time.
        const auto deadline = std::chrono::steady_clock::now() + spin_;
        while (std::chrono::steady_clock::now() < deadline)
        {
        }
        if (offset >= data_.size())
            return {};
        const uint64_t n = std::min<uint64_t>(length, data_.size() - offset);
        std::memcpy(buf, data_.data() + offset, n);
        return std::string_view(static_cast<const char *>(buf), n);
    }

    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    bool shouldCoalesce() const override { return false; }
    std::string getName() const override { return "<SpinningReadFile>"; }
    uint64_t getNaturalReadSize() const override { return 1024; }

private:
    std::string data_;
    std::chrono::microseconds spin_;
};

/// Source whose reads always fail, imitating a network error in a remote reader.
class FailingReadFile : public ReadFile
{
public:
    explicit FailingReadFile(uint64_t size) : size_(size) {}

    std::string_view pread(uint64_t, uint64_t, void *, const FileIoContext & = {}) const override
    {
        throw std::runtime_error("Simulated source read failure");
    }

    uint64_t size() const override { return size_; }
    uint64_t memoryUsage() const override { return size_; }
    bool shouldCoalesce() const override { return false; }
    std::string getName() const override { return "<FailingReadFile>"; }
    uint64_t getNaturalReadSize() const override { return 1024; }

private:
    uint64_t size_;
};

/// Source that reports a direct-IO alignment requirement.
class DirectIoReadFile : public CountingReadFile
{
public:
    DirectIoReadFile(std::string data, uint64_t alignment)
        : CountingReadFile(std::move(data)), alignment_(alignment)
    {
    }

    bool directIo(uint64_t & alignment) const override
    {
        alignment = alignment_;
        return true;
    }

private:
    uint64_t alignment_;
};

class FileCacheBufferedInputTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        TestValue::enable();
        filesystems::registerLocalFileSystem();
    }

    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("filecache-bufinput-test");
        root_ = TempDirectoryPath::create();
        fileSystem_ = filesystems::getFileSystem(root_->getPath(), {});
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
    }

    void TearDown() override
    {
        if (FileCacheManager::getInstance() != nullptr)
            FileCacheManager::setInstance(nullptr);
        for (auto & manager : managers_)
            if (manager)
                manager->shutdown();
        managers_.clear();
        dirs_.clear();
    }

    std::string newCacheDir()
    {
        auto dir = TempDirectoryPath::create();
        dirs_.push_back(dir);
        return dir->getPath();
    }

    FileCacheConfig makeConfig(const std::string & path, const std::function<void(FileCacheConfig &)> & mutate = {})
    {
        FileCacheConfig config;
        config.path = path;
        config.maxSize = 16ull << 20;
        config.maxFileSegmentSize = 1ull << 20;
        config.boundaryAlignment = 1;
        config.reserveGranularity = 0;
        config.loadMetadataThreads = 1;
        // Default the background download workers off so tests that do not need
        // them stay fully synchronous and deterministic. Background download is
        // supported (Task 007 set(nullptr, 0) now detaches to an empty internal
        // buffer, satisfying the Task 012 worker's internalBuffer().empty()
        // precondition); BackgroundDownloadCompletesHandedOffSegment opts in by
        // setting backgroundDownloadThreads > 0.
        config.backgroundDownloadThreads = 0;
        config.cachePolicy = FileCachePolicy::LRU;
        if (mutate)
            mutate(config);
        return config;
    }

    FileCacheManager::Options baseOptions()
    {
        FileCacheManager::Options options;
        options.commonUserId = "common-user";
        options.cachePathPrefix = root_->getPath();
        options.allowedCacheRoot = root_->getPath();
        options.localFileSystem = fileSystem_;
        options.memoryPool = pool_.get();
        options.timekeeper = timekeeper_;
        options.initializeOnCreate = false;
        return options;
    }

    std::shared_ptr<FileCacheManager> makeManager()
    {
        auto manager = FileCacheManager::create(baseOptions());
        manager->initialize();
        managers_.push_back(manager);
        return manager;
    }

    FileCachePtr makeCache(
        FileCacheManager & manager,
        const std::function<void(FileCacheConfig &)> & mutate = {},
        const std::string & name = "test")
    {
        auto cache = manager.factory().getOrCreate(name, makeConfig(newCacheDir(), mutate), name + ".cfg");
        cache->initialize();
        return cache;
    }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCacheManager & manager,
        FileCachePtr cache,
        std::shared_ptr<ReadFile> source,
        FileCacheKey key,
        FileCacheReadOptions opts = {},
        const std::string & queryId = "q",
        velox::memory::MemoryPool * readerPool = nullptr,
        std::shared_ptr<io::IoStatistics> ioStatistics = nullptr,
        std::shared_ptr<velox::IoStats> ioStats = nullptr,
        folly::CancellationToken cancellationToken = {})
    {
        dwio::common::ReaderOptions readerOptions(readerPool ? readerPool : pool_.get());
        FileCacheRequestContext context;
        context.queryId = queryId;
        context.userId = manager.commonUserId();
        FileCacheOriginInfo origin(manager.commonUserId(), context.userWeight);
        return std::make_unique<FileCacheBufferedInput>(
            std::move(source),
            std::move(cache),
            std::move(key),
            origin,
            std::move(opts),
            context,
            dwio::common::MetricsLog::voidLog(),
            std::move(ioStatistics),
            std::move(ioStats),
            executor_.get(),
            readerOptions,
            folly::F14FastMap<std::string, std::string>{},
            std::move(cancellationToken));
    }

    static std::string readAll(dwio::common::SeekableInputStream & stream)
    {
        std::string out;
        const void * data = nullptr;
        int size = 0;
        while (stream.Next(&data, &size))
            out.append(static_cast<const char *>(data), static_cast<size_t>(size));
        return out;
    }

    // A miss populates the cache; count the source bytes to prove a later stream
    // reads from the cache with no further source reads.
    std::string commonUser() const { return "common-user"; }

    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<folly::Timekeeper> timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    std::shared_ptr<filesystems::FileSystem> fileSystem_;
    std::shared_ptr<TempDirectoryPath> root_;
    std::vector<std::shared_ptr<TempDirectoryPath>> dirs_;
    std::vector<std::shared_ptr<FileCacheManager>> managers_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
};

// ===========================================================================
// enqueue / load lifetime: load must not dereference a discarded stream
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, EnqueueResultDiscardedBeforeLoad)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(64);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("enqueue-lifetime");
    auto input = makeInput(*manager, cache, source, key);

    // Discard the enqueue result before load(): load must touch no dead stream.
    { auto stream = input->enqueue({0, 32}); }
    EXPECT_NO_THROW(input->load(dwio::common::LogType::STREAM));

    // No source read and no segment metadata created by enqueue/load.
    EXPECT_EQ(source->preadBytes(), 0u);
    EXPECT_TRUE(cache->getFileSegmentInfos(commonUser()).empty());
}

// ===========================================================================
// DWIO contract values
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DwioContractValues)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    auto source = std::make_shared<CountingReadFile>(makeData(64));
    auto input = makeInput(*manager, cache, source, FileCacheKey::fromPath("dwio-contract"));

    EXPECT_FALSE(input->shouldPrefetchStripes());
    EXPECT_FALSE(input->preloaded());
    EXPECT_FALSE(input->shouldPreload());
    EXPECT_FALSE(input->hasCache());
    EXPECT_EQ(input->executor(), executor_.get());

    input->preload();
    EXPECT_FALSE(input->preloaded());
}

// ===========================================================================
// isBuffered uses no-create get on a cold cache
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, IsBufferedNoCreateProbe)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    auto source = std::make_shared<CountingReadFile>(makeData(64));
    auto input = makeInput(*manager, cache, source, FileCacheKey::fromPath("is-buffered"));

    EXPECT_FALSE(input->isBuffered(0, 32));

    // A cold probe must not create metadata, acquire a downloader, or reserve.
    EXPECT_TRUE(cache->getFileSegmentInfos(commonUser()).empty());
    EXPECT_EQ(source->preadBytes(), 0u);
}

// ===========================================================================
// miss then hit: first stream fills the cache, second reads it without source
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, MissThenHit)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(120);
    const auto key = FileCacheKey::fromPath("miss-then-hit");

    auto source1 = std::make_shared<CountingReadFile>(data);
    auto input1 = makeInput(*manager, cache, source1, key);
    auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream1), data);
    EXPECT_GT(source1->preadBytes(), 0u);
    EXPECT_FALSE(cache->getFileSegmentInfos(commonUser()).empty());

    // A second stream over a fresh source must read entirely from the cache.
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto input2 = makeInput(*manager, cache, source2, key);
    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream2), data);
    EXPECT_EQ(source2->preadBytes(), 0u);
}

// ===========================================================================
// bypass threshold: a read larger than the threshold reads the source but
// creates no cache metadata
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, BypassThreshold)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) {
        c.enableBypassCacheWithThreshold = true;
        c.bypassCacheThreshold = 8;
    });
    const auto data = makeData(64);
    auto source = std::make_shared<CountingReadFile>(data);
    auto input = makeInput(*manager, cache, source, FileCacheKey::fromPath("bypass"));

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream), data);
    EXPECT_GT(source->preadBytes(), 0u);
    // Bypassed reads create no cache metadata.
    EXPECT_TRUE(cache->getFileSegmentInfos(commonUser()).empty());
}

// ===========================================================================
// region-relative coordinates: ByteCount is relative, cache offsets absolute
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, RegionRelativeCoordinates)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(100);
    const auto key = FileCacheKey::fromPath("coordinates");
    auto source = std::make_shared<CountingReadFile>(data);
    auto input = makeInput(*manager, cache, source, key);

    auto stream = input->enqueue({40, 20});
    input->load(dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream), data.substr(40, 20));

    // ByteCount is region-relative (20), not the absolute end (60).
    EXPECT_EQ(stream->ByteCount(), 20);

    // The cache segment lives at the absolute offset 40, not 0.
    auto holder = cache->get(key, 40, 20, 0, commonUser());
    ASSERT_FALSE(holder->empty());
    EXPECT_EQ(holder->front().range().left, 40u);
    // Nothing was cached at absolute offset 0.
    auto miss = cache->get(key, 0, 4, 0, commonUser());
    ASSERT_FALSE(miss->empty());
    EXPECT_EQ(miss->front().state(), FileSegment::State::DETACHED);
}

// ===========================================================================
// overflow: region.offset + length is rejected before wrapping
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, RegionOverflowRejected)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    auto source = std::make_shared<CountingReadFile>(makeData(16));
    auto input = makeInput(*manager, cache, source, FileCacheKey::fromPath("overflow"));

    auto stream = input->read(
        std::numeric_limits<uint64_t>::max() - 5, 100, dwio::common::LogType::STREAM);
    const void * data = nullptr;
    int size = 0;
    EXPECT_THROW(stream->Next(&data, &size), VeloxException);
}

// ===========================================================================
// isBuffered rejects an overflowing (offset + length) range up front with a
// clear, function-specific error rather than wrapping to a spurious value
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, IsBufferedRejectsOverflow)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    auto source = std::make_shared<CountingReadFile>(makeData(16));
    auto input = makeInput(*manager, cache, source, FileCacheKey::fromPath("is-buffered-overflow"));

    // offset + length overflows uint64. The rejection must originate from
    // isBuffered's own checkedAdd guard -- its message names isBuffered -- proving
    // the guard runs before any wrap-prone arithmetic, not merely as a side effect
    // of FileCache::get's internal range check (whose message differs).
    VELOX_ASSERT_THROW(
        input->isBuffered(std::numeric_limits<uint64_t>::max() - 3, 10),
        "isBuffered");
}

// ===========================================================================
// seek within the current output buffer is cheap (no source re-read)
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, SeekWithinBufferIsCheap)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(40);
    const auto key = FileCacheKey::fromPath("seek-in-buffer");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 40; // one chunk holds the whole region
    // Bypass the cache so a slow-path rebuild would re-read the source (a cache
    // hit would otherwise mask a lost fast path).
    opts.readIfExistsOtherwiseBypass = true;
    auto input = makeInput(*manager, cache, source, key, opts);

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream->Next(&chunk, &size));
    ASSERT_EQ(size, 40);
    const uint64_t sourceBytesAfterFirstRead = source->preadBytes();

    // Seek back inside the already-filled buffer -> O(1), no new source read.
    std::vector<uint64_t> seekPositions{5};
    dwio::common::PositionProvider provider(seekPositions);
    stream->seekToPosition(provider);
    EXPECT_EQ(stream->ByteCount(), 5);

    const void * chunk2 = nullptr;
    int size2 = 0;
    ASSERT_TRUE(stream->Next(&chunk2, &size2));
    EXPECT_EQ(std::string(static_cast<const char *>(chunk2), size2), data.substr(5));
    EXPECT_EQ(source->preadBytes(), sourceBytesAfterFirstRead);
}

// ===========================================================================
// seek outside the current output buffer rebuilds and returns correct data
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, SeekOutOfBufferRebuilds)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(60);
    const auto key = FileCacheKey::fromPath("seek-out-buffer");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 10;
    auto input = makeInput(*manager, cache, source, key, opts);

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream->Next(&chunk, &size));
    EXPECT_EQ(std::string(static_cast<const char *>(chunk), size), data.substr(0, 10));

    // Seek far outside the current buffer -> slow path rebuild.
    std::vector<uint64_t> seekPositions{35};
    dwio::common::PositionProvider provider(seekPositions);
    stream->seekToPosition(provider);
    EXPECT_EQ(stream->ByteCount(), 35);
    EXPECT_EQ(readAll(*stream), data.substr(35));
}

// ===========================================================================
// reader handoff: Q1 writes one chunk, detaches its reader, Q2 continues from
// currentWriteOffset reusing the segment's remote reader
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, ReaderHandoffQ1Q2)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 32; });
    const auto data = makeData(20);
    const auto key = FileCacheKey::fromPath("handoff");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    // Pin the single segment [0, 19] to observe its state across the handoff.
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    // Q1 downloads one 4-byte chunk and detaches its reader.
    auto source1 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q1;
    q1.remoteFsBufferSize = 4;
    auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
    auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream1->Next(&chunk, &size));
    ASSERT_EQ(std::string(static_cast<const char *>(chunk), size), data.substr(0, 4));

    // Handoff invariant: Q1 downloaded a prefix into the segment and released the
    // downloader, leaving the segment continuable (PARTIALLY_DOWNLOADED) with a
    // reusable remote reader positioned at the current write offset.
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 4u);
    EXPECT_EQ(probe->front().state(), FileSegment::State::PARTIALLY_DOWNLOADED);
    const uint64_t source1AfterQ1 = source1->preadBytes();
    EXPECT_GT(source1AfterQ1, 0u);

    // Q2 reads the whole region. Because it reuses the remote reader Q1 left in
    // the segment (which wraps Q1's source), the continuation reads [4, 20) go
    // through source1; Q2's own source is never touched. That is the observable
    // proof that the reader was handed off rather than recreated.
    auto source2 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q2;
    q2.remoteFsBufferSize = 32;
    auto input2 = makeInput(*manager, cache, source2, key, q2, "q2");
    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream2), data);
    EXPECT_EQ(probe->front().getCurrentWriteOffset(), 20u);
    EXPECT_EQ(source2->preadBytes(), 0u);
    EXPECT_GT(source1->preadBytes(), source1AfterQ1);
}

// ===========================================================================
// a CACHED reader on a partial prefix must see bytes a concurrent downloader
// appends/completes afterwards, not stop at a stale frozen read bound
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, CachedReaderSeesGrownSegment)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 32; });
    const auto data = makeData(20);
    const auto key = FileCacheKey::fromPath("cached-grows");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    // Q1 downloads the prefix [0, 4).
    auto source1 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q1;
    q1.remoteFsBufferSize = 4;
    auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
    auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream1->Next(&chunk, &size));
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 4u);

    // Q2 opens a CACHED reader on the [0, 4) prefix (its read bound is 4).
    auto source2 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q2;
    q2.remoteFsBufferSize = 4;
    auto input2 = makeInput(*manager, cache, source2, key, q2, "q2");
    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * c2 = nullptr;
    int n2 = 0;
    ASSERT_TRUE(stream2->Next(&c2, &n2));
    ASSERT_EQ(std::string(static_cast<const char *>(c2), n2), data.substr(0, 4));

    // A third reader completes the segment ([4, 20) downloaded; now DOWNLOADED).
    auto source3 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q3;
    q3.remoteFsBufferSize = 32;
    auto input3 = makeInput(*manager, cache, source3, key, q3, "q3");
    EXPECT_EQ(readAll(*input3->read(0, data.size(), dwio::common::LogType::STREAM)), data);
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 20u);

    // Q2 continues: it must read [4, 20) from the now-grown cache file rather than
    // hit a spurious EOF at its stale bound of 4. All bytes are read from cache.
    std::string rest;
    const void * cr = nullptr;
    int nr = 0;
    while (stream2->Next(&cr, &nr))
        rest.append(static_cast<const char *>(cr), static_cast<size_t>(nr));
    EXPECT_EQ(rest, data.substr(4));
    EXPECT_EQ(source2->preadBytes(), 0u);
}

// ===========================================================================
// background download: a query reads only a prefix and hands off its remote
// reader; once the holder is released the Task 012 background worker continues
// from the segment's current write offset and completes the segment. This
// exercises the production path with backgroundDownloadThreads > 0 and the
// Task 007 detach precondition (internalBuffer().empty()) the worker asserts.
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, BackgroundDownloadCompletesHandedOffSegment)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) {
        c.maxFileSegmentSize = 64;       // a single segment covers the region
        c.backgroundDownloadThreads = 2; // enable the background workers
    });
    const auto data = makeData(40);
    const auto key = FileCacheKey::fromPath("bg-download");
    const std::string user = commonUser();

    // Q1 reads only the first 4-byte chunk, then is destroyed as the last holder.
    // Its source stays alive because the handed-off reader shares ownership, so
    // the background worker can continue reading through it.
    auto source1 = std::make_shared<CountingReadFile>(data);
    {
        FileCacheReadOptions q1;
        q1.remoteFsBufferSize = 4;
        auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
        auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
        const void * chunk = nullptr;
        int size = 0;
        ASSERT_TRUE(stream1->Next(&chunk, &size));
        ASSERT_EQ(std::string(static_cast<const char *>(chunk), size), data.substr(0, 4));
        // stream1 + input1 destroyed here: the last holder completes the partial
        // segment with allow_background_download=true -> background-queue push.
    }

    // The background worker continues from the handed-off reader (which wraps
    // source1) and completes the whole segment. Observe via a read-only snapshot
    // with a bounded spin, never a fixed sleep.
    auto segmentComplete = [&]() {
        const auto infos = cache->getFileSegmentInfos(key, user);
        return !infos.empty() && infos.front().state == FileSegment::State::DOWNLOADED
            && infos.front().downloaded_size == data.size();
    };
    ASSERT_TRUE(spinUntil(segmentComplete, std::chrono::seconds(20)))
        << "the background worker must complete the handed-off partial segment";

    // No duplicate source read: source1 served [0,4) to Q1 and [4,40) to the
    // background worker -- exactly the 40 region bytes, never re-reading a prefix.
    EXPECT_EQ(source1->preadBytes(), data.size());

    // A fresh query now reads entirely from the cache. This proves the completed
    // segment is coherent, its remote reader was released (no downloader leak or
    // stale pointer), and no new source read happens.
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto input2 = makeInput(*manager, cache, source2, key, {}, "q2");
    EXPECT_EQ(readAll(*input2->read(0, data.size(), dwio::common::LogType::STREAM)), data);
    EXPECT_EQ(source2->preadBytes(), 0u);
}

// ===========================================================================
// background handoff pool lifetime: a handed-off reader must retain no memory
// charged to the query-scoped pool, so an async worker that outlives the query
// (and its pool) never frees against a dead pool. Uses a distinct query pool and
// destroys it before the background download finishes.
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, BackgroundHandoffReleasesQueryPoolMemory)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) {
        c.maxFileSegmentSize = 64;
        c.backgroundDownloadThreads = 2;
    });
    const auto data = makeData(40);
    const auto key = FileCacheKey::fromPath("bg-pool-lifetime");
    const std::string user = commonUser();

    // A dedicated query-scoped pool, distinct from the manager/cache pool.
    auto queryPool = velox::memory::deprecatedAddDefaultLeafMemoryPool("bg-query-pool");

    auto source1 = std::make_shared<CountingReadFile>(data);
    {
        FileCacheReadOptions q1;
        q1.remoteFsBufferSize = 4;
        auto input1 = makeInput(*manager, cache, source1, key, q1, "q1", queryPool.get());
        auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
        const void * chunk = nullptr;
        int size = 0;
        ASSERT_TRUE(stream1->Next(&chunk, &size));
        ASSERT_EQ(std::string(static_cast<const char *>(chunk), size), data.substr(0, 4));
    }

    // The handed-off reader released its owned buffer, so the query pool holds
    // nothing even though the segment still keeps the reader for background use.
    EXPECT_EQ(queryPool->usedBytes(), 0) << "a handed-off reader must not retain query-pool memory";

    // Destroying the query pool must be safe while the background download (which
    // uses the manager pool for its own buffer) is still pending or running.
    queryPool.reset();

    auto segmentComplete = [&]() {
        const auto infos = cache->getFileSegmentInfos(key, user);
        return !infos.empty() && infos.front().state == FileSegment::State::DOWNLOADED
            && infos.front().downloaded_size == data.size();
    };
    ASSERT_TRUE(spinUntil(segmentComplete, std::chrono::seconds(20)))
        << "background download must complete after the query pool is destroyed";
    EXPECT_EQ(source1->preadBytes(), data.size());
}

// ===========================================================================
TEST_F(FileCacheBufferedInputTest, SourceFailureReleasesDownloader)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 32; });
    const auto data = makeData(16);
    const auto key = FileCacheKey::fromPath("source-failure");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    auto failing = std::make_shared<FailingReadFile>(data.size());
    auto input = makeInput(*manager, cache, failing, key, {}, "q-fail");
    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    EXPECT_THROW(stream->Next(&chunk, &size), std::exception);
    stream.reset();

    // Become the next downloader, as a waiting reader would; the failed
    // downloader must have withdrawn the shared reader on its unwind path.
    auto & segment = probe->front();
    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_FALSE(segment.getRemoteFileReader());
    segment.completePartAndResetDownloader();

    // A healthy reader reads the segment end to end.
    auto healthy = std::make_shared<CountingReadFile>(data);
    auto recovered = makeInput(*manager, cache, healthy, key, {}, "q-recover");
    auto recoveredStream = recovered->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*recoveredStream), data);
}

// ===========================================================================
// disk failure: skip_cache_on_disk_failure bypasses the cache write but keeps
// the source read; without the flag the failure propagates
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DiskFailureSkipBypasses)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.skipCacheOnDiskFailure = true; });
    const auto data = makeData(32);
    const auto key = FileCacheKey::fromPath("disk-failure-skip");
    auto source = std::make_shared<CountingReadFile>(data);
    auto input = makeInput(*manager, cache, source, key, {}, "q-skip");

    ScopedTestValue armed(
        kFailPoint,
        std::function<void(void *)>([](void *) {
            throw FileCacheErrnoException(__FILE__, __LINE__, __FUNCTION__, "simulated cache disk IO failure", EIO);
        }));

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    // The write failure is skipped: the source read still returns the data.
    EXPECT_EQ(readAll(*stream), data);
    EXPECT_GT(source->preadBytes(), 0u);
}

TEST_F(FileCacheBufferedInputTest, DiskFailurePropagatesWithoutSkip)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.skipCacheOnDiskFailure = false; });
    const auto data = makeData(32);
    const auto key = FileCacheKey::fromPath("disk-failure-propagate");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    auto source = std::make_shared<CountingReadFile>(data);
    auto input = makeInput(*manager, cache, source, key, {}, "q-noskip");

    ScopedTestValue armed(
        kFailPoint,
        std::function<void(void *)>([](void *) {
            throw FileCacheErrnoException(__FILE__, __LINE__, __FUNCTION__, "simulated cache disk IO failure", EIO);
        }));

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    EXPECT_THROW(stream->Next(&chunk, &size), std::exception);
    stream.reset();

    // The write failure propagated (no silent skip) and the downloader was
    // released, so the segment is marked no-continuation and not stuck with a
    // dangling downloader.
    auto & segment = probe->front();
    EXPECT_EQ(segment.state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    EXPECT_TRUE(segment.getDownloader().empty());
}

// ===========================================================================
// disk failure across several segments: a cache-write failure in the middle of a
// multi-segment region is skipped and the read still returns the whole region
// (the failing segment is served from the source; later segments keep caching).
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DiskFailureSkipContinuesAcrossSegments)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) {
        c.skipCacheOnDiskFailure = true;
        c.maxFileSegmentSize = 8; // region [0, 40) spans five segments
    });
    const auto data = makeData(40);
    const auto key = FileCacheKey::fromPath("disk-failure-multichunk");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 8; // one chunk per segment
    auto input = makeInput(*manager, cache, source, key, opts, "q-skip-multi");

    // Fail the third cache write (the middle segment [16, 24)); earlier and later
    // segment writes succeed.
    std::atomic<int> writeCount{0};
    ScopedTestValue armed(
        kFailPoint,
        std::function<void(void *)>([&writeCount](void *) {
            if (writeCount.fetch_add(1) + 1 == 3)
                throw FileCacheErrnoException(
                    __FILE__, __LINE__, __FUNCTION__, "simulated mid-region cache disk IO failure", EIO);
        }));

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    // The whole region is returned despite the mid-region write failure: the
    // failing segment is bypassed to the source and the read continues.
    EXPECT_EQ(readAll(*stream), data);
    EXPECT_GT(source->preadBytes(), 0u);
    EXPECT_GE(writeCount.load(), 3) << "the failing write and later segment writes must be attempted";
    stream.reset();

    // Caching resumed after the mid-region skip: at least one segment past the
    // failing one cached fully, and the total cached bytes are less than the whole
    // region (the failing segment [16, 24) was not fully cached).
    const auto infos = cache->getFileSegmentInfos(key, commonUser());
    uint64_t cachedAfterFailure = 0;
    uint64_t totalCached = 0;
    for (const auto & info : infos)
    {
        totalCached += info.downloaded_size;
        if (info.range_left >= 24 && info.downloaded_size == 8)
            ++cachedAfterFailure;
    }
    EXPECT_GT(cachedAfterFailure, 0u) << "a segment after the skipped one must cache successfully";
    EXPECT_LT(totalCached, data.size()) << "the failing segment must not be fully cached";
}

// ===========================================================================
// truncation with metadata absent: predownload hits EOF and fails cleanly;
// the truncation-decision helper covers both metadata-present and absent
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, TruncatedObjectPredownloadMetadataAbsent)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 10; });
    // Object listed at size 10 but only 4 bytes are actually present.
    const auto data = makeData(4);
    const uint64_t declaredSize = 10;
    const auto key = FileCacheKey::fromPath("truncated-predownload");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    auto probe = cache->getOrSet(key, 0, declaredSize, declaredSize, CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    // Q1 downloads [0, 2) and stops mid-segment.
    auto source1 = std::make_shared<CountingReadFile>(data, declaredSize);
    FileCacheReadOptions q1;
    q1.remoteFsBufferSize = 2;
    auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
    auto stream1 = input1->read(0, declaredSize, dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream1->Next(&chunk, &size));
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 2u);

    // Q2 seeks to offset 5 (beyond the 4 real bytes), becomes downloader, and
    // predownloads [2, 4) before hitting EOF with one more byte required.
    auto source2 = std::make_shared<CountingReadFile>(data, declaredSize);
    FileCacheReadOptions q2;
    q2.remoteFsBufferSize = 8;
    auto input2 = makeInput(*manager, cache, source2, key, q2, "q2");
    auto stream2 = input2->read(0, declaredSize, dwio::common::LogType::STREAM);
    std::vector<uint64_t> seekPositions{5};
    dwio::common::PositionProvider provider(seekPositions);
    stream2->seekToPosition(provider);
    const void * chunk2 = nullptr;
    int size2 = 0;
    EXPECT_THROW(stream2->Next(&chunk2, &size2), std::exception);
    stream2.reset();

    // The bytes that existed were predownloaded; the segment was released for
    // waiting readers with the shared reader withdrawn.
    EXPECT_EQ(probe->front().getDownloadedSize(), 4u);
    EXPECT_EQ(probe->front().state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
    EXPECT_FALSE(probe->front().extractRemoteFileReader());

    // Both branches of the truncation decision: present size == offset is a known
    // boundary; a nullopt (this port's default) is not evidence of truncation.
    EXPECT_TRUE(FileCacheInputStream::isRemoteTruncationConfirmed(
        FileCacheInputStream::RemoteFileMetadata{4}, 4));
    EXPECT_FALSE(FileCacheInputStream::isRemoteTruncationConfirmed(std::nullopt, 4));
}

// ===========================================================================
// direct IO: the stream provides an aligned output buffer and the reader
// rejects a misaligned external buffer
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DirectIoAlignment)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(64);
    const auto key = FileCacheKey::fromPath("direct-io");
    auto source = std::make_shared<DirectIoReadFile>(data, 8);
    auto input = makeInput(*manager, cache, source, key);

    // Aligned reads through the whole stack succeed.
    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream), data);

    // A misaligned external buffer is rejected by the reader the stream uses.
    auto directSource = std::make_shared<DirectIoReadFile>(data, 512);
    ReadBufferFromVeloxReadFile reader(directSource, pool_.get());
    auto buffer = velox::AlignedBuffer::allocate<char>(2048, pool_.get());
    char * aligned = buffer->asMutable<char>();
    // 64-byte AlignedBuffer is a multiple of 512 only when we round up.
    char * alignedStart = reinterpret_cast<char *>(
        (reinterpret_cast<uintptr_t>(aligned) + 511) & ~static_cast<uintptr_t>(511));
    EXPECT_NO_THROW(reader.set(alignedStart, 512));
    EXPECT_THROW(reader.set(alignedStart + 1, 512), VeloxException);
}

// ===========================================================================
// direct IO + predownload: when the source alignment cannot satisfy the
// predownload gap [currentWriteOffset, offset), the optional predownload is
// skipped and the segment is read through the normal aligned bypass path --
// never an unaligned direct-IO read, a buffered-IO fallback, or a fabricated
// size.
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DirectIoPredownloadSkipsWhenUnaligned)
{
    constexpr uint64_t kAlignment = 8;
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 64; });
    const auto data = makeData(40); // a multiple of the alignment
    const auto key = FileCacheKey::fromPath("direct-io-predownload");

    // A non-direct-IO writer leaves the segment's current write offset at 3, which
    // is not a multiple of the direct-IO alignment. It stays alive holding the
    // segment across the direct-IO read below.
    auto writer = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions w;
    w.remoteFsBufferSize = 3;
    auto writerInput = makeInput(*manager, cache, writer, key, w, "writer");
    auto writerStream = writerInput->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(writerStream->Next(&chunk, &size));
    ASSERT_EQ(std::string(static_cast<const char *>(chunk), size), data.substr(0, 3));

    // A direct-IO reader seeks to the aligned offset 8 (current write offset is 3),
    // electing a predownload of the misaligned gap [3, 8). The reader must skip
    // that predownload and read [8, 40) via the aligned bypass path instead of
    // seeking to the unaligned offset 3.
    auto directSource = std::make_shared<DirectIoReadFile>(data, kAlignment);
    FileCacheReadOptions r;
    r.remoteFsBufferSize = 16; // a multiple of the alignment
    auto readerInput = makeInput(*manager, cache, directSource, key, r, "direct");
    auto readerStream = readerInput->read(0, data.size(), dwio::common::LogType::STREAM);
    std::vector<uint64_t> seekPositions{8};
    dwio::common::PositionProvider provider(seekPositions);
    readerStream->seekToPosition(provider);

    // Reads correctly from offset 8 (the DirectIoReadFile mock asserts every pread
    // is aligned). Without the skip, prepareRead's seek to the unaligned current
    // write offset 3 would throw a direct-IO alignment exception.
    EXPECT_EQ(readAll(*readerStream), data.substr(8));
    EXPECT_GT(directSource->preadBytes(), 0u);

    // The predownload was skipped, so the gap [3, 8) was not populated: a bypass
    // read never writes to the cache, so the downloaded size is unchanged at 3.
    const auto infos = cache->getFileSegmentInfos(key, commonUser());
    ASSERT_FALSE(infos.empty());
    EXPECT_EQ(infos.front().downloaded_size, 3u);
}

// ===========================================================================
TEST_F(FileCacheBufferedInputTest, QueryContextLifetime)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.enableFilesystemQueryCacheLimit = true; });
    const auto data = makeData(60);
    const auto key = FileCacheKey::fromPath("query-context");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 10;
    opts.maxDownloadSizePerQuery = 1ull << 20;
    auto input = makeInput(*manager, cache, source, key, opts, "q-ctx");

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream->Next(&chunk, &size));

    // The stream's constructor acquired a query context for "q-ctx"; capture a
    // weak reference to the shared context.
    std::weak_ptr<FileCacheQueryLimit::QueryContext> weak;
    {
        auto holder = cache->getQueryContextHolder("q-ctx", opts);
        ASSERT_NE(holder, nullptr);
        ASSERT_NE(holder->context, nullptr);
        weak = holder->context;
    }
    EXPECT_FALSE(weak.expired());

    // An out-of-buffer seek must not release the query context.
    std::vector<uint64_t> seekPositions{40};
    dwio::common::PositionProvider provider(seekPositions);
    stream->seekToPosition(provider);
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(readAll(*stream), data.substr(40));
    EXPECT_FALSE(weak.expired());

    // Destroying the stream releases the query context.
    stream.reset();
    EXPECT_TRUE(weak.expired());
}

// ===========================================================================
// downloader cleanup on destruction: a partially-read stream releases its
// downloader/holder, and a fresh stream reads the whole region
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DownloaderCleanupOnDestruction)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 8; });
    const auto data = makeData(40);
    const auto key = FileCacheKey::fromPath("downloader-cleanup");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 8;
    auto input = makeInput(*manager, cache, source, key, opts, "q-cleanup");

    {
        auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
        const void * chunk = nullptr;
        int size = 0;
        ASSERT_TRUE(stream->Next(&chunk, &size));
        // Destroy mid-read: the destructor must release the downloader/holder.
    }

    // A fresh stream reads the whole region without being blocked.
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto input2 = makeInput(*manager, cache, source2, key, opts, "q-cleanup-2");
    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream2), data);
}

// ===========================================================================
// key derivation: empty etag -> fromPath; distinct etags -> distinct keys and
// distinct cache entries
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, PathAndEtagKeyDerivation)
{
    const std::string path = "s3://bucket/object";
    EXPECT_EQ(
        FileCacheFileIdentity::deriveKey({path, ""}),
        FileCacheKey::fromPath(path));

    const auto keyV1 = FileCacheFileIdentity::deriveKey({path, "etag-v1"});
    const auto keyV2 = FileCacheFileIdentity::deriveKey({path, "etag-v2"});
    EXPECT_NE(keyV1, keyV2);
    EXPECT_NE(keyV1, FileCacheKey::fromPath(path));

    // Different versioned keys route to different cache entries for the same
    // path: v2 content does not leak into a v1 read.
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto dataV1 = makeData(32);
    std::string dataV2 = makeData(32);
    dataV2[0] = 'Z';

    auto sourceV1 = std::make_shared<CountingReadFile>(dataV1);
    auto inputV1 = makeInput(*manager, cache, sourceV1, keyV1, {}, "qv1");
    EXPECT_EQ(readAll(*inputV1->read(0, dataV1.size(), dwio::common::LogType::STREAM)), dataV1);

    auto sourceV2 = std::make_shared<CountingReadFile>(dataV2);
    auto inputV2 = makeInput(*manager, cache, sourceV2, keyV2, {}, "qv2");
    auto streamV2 = inputV2->read(0, dataV2.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*streamV2), dataV2);
    EXPECT_GT(sourceV2->preadBytes(), 0u);
}

// ===========================================================================
// double-accounting: every I/O fact updates the global ProfileEvents ledger
// AND the query IoStatistics/IoStats ledger independently
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, CacheReadUpdatesGlobalAndIoStatistics)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto key = FileCacheKey::fromPath("stats-cache-read");
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto ioStats = std::make_shared<velox::IoStats>();

    // Warm the cache: the first read fully downloads [0, 4096) into one segment.
    {
        auto warmSource = std::make_shared<CountingReadFile>(data);
        auto warm = makeInput(*manager, cache, warmSource, key, {}, "q", nullptr, ioStatistics, ioStats);
        readAll(*warm->read(0, 4096, dwio::common::LogType::STREAM));
    }

    // Second read of the same key is a pure cache hit.
    const uint64_t globalBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheBytes);
    const uint64_t ssdSumBefore = ioStatistics->ssdRead().sum();
    const uint64_t ssdCountBefore = ioStatistics->ssdRead().count();
    const uint64_t rawBefore = ioStatistics->rawBytesRead();

    auto source = std::make_shared<CountingReadFile>(data);
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, ioStatistics, ioStats);
    readAll(*input->read(0, 4096, dwio::common::LogType::STREAM));

    // Cache read: global cache-read bytes and query ssdRead each advance by 4096,
    // the hit is counted as logical returned bytes (rawBytesRead), and no source
    // byte is touched.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheBytes) - globalBefore, 4096u);
    EXPECT_EQ(ioStatistics->ssdRead().sum() - ssdSumBefore, 4096u);
    EXPECT_GT(ioStatistics->ssdRead().count(), ssdCountBefore);
    EXPECT_EQ(ioStatistics->rawBytesRead() - rawBefore, 4096u);
    EXPECT_EQ(source->preadBytes(), 0u);
}

TEST_F(FileCacheBufferedInputTest, SourceReadUpdatesGlobalAndIoStatistics)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("stats-source-read");
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, ioStatistics);

    const uint64_t globalBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    const uint64_t rawBefore = ioStatistics->rawBytesRead();
    const uint64_t readSumBefore = ioStatistics->read().sum();

    readAll(*input->read(0, 4096, dwio::common::LogType::STREAM));

    // Cold read: 4096 source bytes returned -> global source bytes, query read
    // sum, and raw input bytes each advance by exactly 4096.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes) - globalBefore, 4096u);
    EXPECT_EQ(ioStatistics->read().sum() - readSumBefore, 4096u);
    EXPECT_EQ(ioStatistics->rawBytesRead() - rawBefore, 4096u);
}

TEST_F(FileCacheBufferedInputTest, CacheWriteUpdatesGlobalAndIoStats)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("stats-cache-write");
    auto ioStats = std::make_shared<velox::IoStats>();
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, nullptr, ioStats);

    const uint64_t globalBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes);

    readAll(*input->read(0, 4096, dwio::common::LogType::STREAM));

    // The cold read wrote the whole 4096-byte segment to cache exactly once.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes) - globalBefore, 4096u);
    auto stats = ioStats->stats();
    auto it = stats.find(kFileCacheWriteBytes);
    ASSERT_NE(it, stats.end());
    EXPECT_EQ(it->second.sum, 4096);
}

TEST_F(FileCacheBufferedInputTest, SameFactUpdatesBothLedgers)
{
    // A single cold read updates BOTH the global ProfileEvents ledger AND the
    // query IoStatistics/IoStats ledger independently -- neither is derived from
    // the other.
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("stats-dual-ledger");
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto ioStats = std::make_shared<velox::IoStats>();
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, ioStatistics, ioStats);

    const uint64_t gSrcBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    const uint64_t gWrBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes);
    const uint64_t rawBefore = ioStatistics->rawBytesRead();

    readAll(*input->read(0, 4096, dwio::common::LogType::STREAM));

    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes) - gSrcBefore, 4096u);
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes) - gWrBefore, 4096u);
    EXPECT_EQ(ioStatistics->rawBytesRead() - rawBefore, 4096u);
    auto stats = ioStats->stats();
    auto it = stats.find(kFileCacheWriteBytes);
    ASSERT_NE(it, stats.end());
    EXPECT_EQ(it->second.sum, 4096);
}

TEST_F(FileCacheBufferedInputTest, PredownloadUpdatesReadPrefetchButNotRawBytes)
{
    // Deterministic predownload built on the accepted
    // TruncatedObjectPredownloadMetadataAbsent scenario, but with the full object
    // present so the predownload SUCCEEDS and every byte count is exact. A first
    // reader partially fills a segment; a second reader seeks past the written
    // prefix, becomes the downloader, and predownloads the exact gap before its
    // own read.
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 10; });
    const auto data = makeData(10);
    const auto key = FileCacheKey::fromPath("predownload-stats");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    // Pin the single segment [0, 10) so its state is observable across readers.
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    // Q1 downloads [0, 2) and stops, leaving the segment PARTIALLY_DOWNLOADED.
    auto source1 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q1;
    q1.remoteFsBufferSize = 2;
    auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
    auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream1->Next(&chunk, &size));
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 2u);

    // Q2 seeks to offset 5 (> currentWriteOffset 2), becomes the downloader, and
    // predownloads the exact gap [2, 5) = 3 bytes from source, then reads [5, 10).
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto source2 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q2;
    q2.remoteFsBufferSize = 8;
    auto input2 = makeInput(*manager, cache, source2, key, q2, "q2", nullptr, ioStatistics);

    const uint64_t gPredownBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedBytes);
    const uint64_t gPredownSrcBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceBytes);
    const uint64_t gSrcBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    const uint64_t readSumBefore = ioStatistics->read().sum();
    const uint64_t prefetchSumBefore = ioStatistics->prefetch().sum();
    const uint64_t rawBefore = ioStatistics->rawBytesRead();

    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    std::vector<uint64_t> seekPositions{5};
    dwio::common::PositionProvider provider(seekPositions);
    stream2->seekToPosition(provider);
    const void * chunk2 = nullptr;
    int size2 = 0;
    ASSERT_TRUE(stream2->Next(&chunk2, &size2));
    const auto returned = static_cast<uint64_t>(size2);
    ASSERT_GT(returned, 0u);

    // Predownload of exactly 3 gap bytes: both global predownload counters += 3.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedBytes) - gPredownBefore, 3u);
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceBytes) - gPredownSrcBefore, 3u);
    // Global source-read total includes BOTH the 3 predownload gap bytes AND the
    // ordinary physical source read at offset 5: predownload source bytes feed the
    // same global CachedReadBufferReadFromSourceBytes ledger as an ordinary read.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes) - gSrcBefore, 3u + returned);
    // Query ledger: predownload maps to BOTH read and prefetch (design §3.4).
    EXPECT_EQ(ioStatistics->prefetch().sum() - prefetchSumBefore, 3u);
    // read() gets the 3 predownload bytes plus the `returned` bytes read at offset 5.
    EXPECT_EQ(ioStatistics->read().sum() - readSumBefore, 3u + returned);
    // KEY invariant (design §3.4): predownload is NOT logical returned bytes, so
    // rawBytesRead advances only by the bytes returned to the caller -- never the
    // 3-byte gap. This fails if the predownload path wrongly calls incRawBytesRead.
    EXPECT_EQ(ioStatistics->rawBytesRead() - rawBefore, returned);
}

// ===========================================================================
// hit/miss is counted per returned chunk, not once at reader creation: a reused
// bypass reader that returns several chunks records one miss per chunk
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, MultiChunkBypassCountsMissPerReturnedChunk)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(30);
    const auto key = FileCacheKey::fromPath("bypass-multichunk-miss");
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 10; // three 10-byte chunks
    // The segment is absent, so readIfExistsOtherwiseBypass forces a single
    // REMOTE_FS_READ_BYPASS_CACHE reader that is reused across all chunks.
    opts.readIfExistsOtherwiseBypass = true;
    auto input = makeInput(*manager, cache, source, key, opts);

    const uint64_t missBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheMisses);
    const uint64_t hitBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheHits);
    const uint64_t srcBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);

    auto stream = input->read(0, data.size(), dwio::common::LogType::STREAM);
    int chunks = 0;
    const void * chunk = nullptr;
    int size = 0;
    while (stream->Next(&chunk, &size))
    {
        EXPECT_EQ(size, 10);
        ++chunks;
    }

    // Three chunks are returned from one reused bypass reader.
    EXPECT_EQ(chunks, 3);
    // One miss per returned chunk: the counter advances by the chunk count, not by
    // 1 (which is what counting at reader creation would give) and not by 0.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheMisses) - missBefore, 3u);
    // A pure bypass read is never a hit.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheHits) - hitBefore, 0u);
    // All 30 bytes came from source.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes) - srcBefore, 30u);
}

// ===========================================================================
// last-segment clamp: physical bytes read/written differ from logical bytes
// returned. Cache-write and the physical source-read byte counters (global
// CachedReadBufferReadFromSourceBytes + query read()) use the physical
// (pre-clamp) size; only rawBytesRead uses the logical (post-clamp) returned
// size, matching ClickHouse physical-I/O semantics.
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, LastSegmentClampSeparatesPhysicalAndLogicalBytes)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 8; });
    const auto data = makeData(8);
    const auto key = FileCacheKey::fromPath("clamp-physical-vs-logical");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    // Pin a single [0, 8) segment so the segment extends past the [0, 7) region
    // end: the cold download reads and writes the full 8-byte segment, but the
    // last-segment clamp returns only the 7 requested bytes.
    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);
    ASSERT_EQ(probe->front().range().right, 7u);

    auto source = std::make_shared<CountingReadFile>(data);
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto ioStats = std::make_shared<velox::IoStats>();
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 16; // read the whole segment in one chunk
    auto input = makeInput(*manager, cache, source, key, opts, "q", nullptr, ioStatistics, ioStats);

    const uint64_t gSrcBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    const uint64_t gWrBefore = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes);
    const uint64_t rawBefore = ioStatistics->rawBytesRead();
    const uint64_t readSumBefore = ioStatistics->read().sum();

    // Read only [0, 7): the cold download reads and writes the full 8-byte
    // segment to cache but returns just 7 bytes to the caller.
    EXPECT_EQ(readAll(*input->read(0, 7, dwio::common::LogType::STREAM)), data.substr(0, 7));

    // Physical (pre-clamp) 8 bytes were written to cache -- both the global cache
    // write counter and the query fileCacheWriteBytes. Fails if the post-clamp
    // logical size (7) leaks into the cache-write accounting.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes) - gWrBefore, 8u);
    auto stats = ioStats->stats();
    auto it = stats.find(kFileCacheWriteBytes);
    ASSERT_NE(it, stats.end());
    EXPECT_EQ(it->second.sum, 8);

    // Physical (pre-clamp) 8 bytes were read from the source -- the global
    // source-read byte counter and the query read() both reflect the physical
    // read, matching ClickHouse. Fails if the post-clamp logical size (7) leaks
    // into the physical source accounting.
    EXPECT_EQ(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes) - gSrcBefore, 8u);
    EXPECT_EQ(ioStatistics->read().sum() - readSumBefore, 8u);

    // Logical (post-clamp) 7 bytes were returned to the caller -> rawBytesRead
    // records only the bytes actually handed back. Fails if the pre-clamp physical
    // size (8) leaks into rawBytesRead.
    EXPECT_EQ(ioStatistics->rawBytesRead() - rawBefore, 7u);
}

// ===========================================================================
// scan time: a source read whose pread busy-spins for a bounded duration records
// strictly positive scan time in the query IoStatistics (no sleep)
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, SourceReadRecordsPositiveScanTime)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    const auto key = FileCacheKey::fromPath("scan-time");
    auto source = std::make_shared<SpinningReadFile>(data, std::chrono::microseconds(1000));
    auto ioStatistics = std::make_shared<io::IoStatistics>();
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, ioStatistics);

    const uint64_t scanBefore = ioStatistics->totalScanTimeNs();
    readAll(*input->read(0, 4096, dwio::common::LogType::STREAM));

    // The bounded busy-spin guarantees the source read consumed observable
    // wall-clock time, so incTotalScanTimeNs strictly advances. Fails if the
    // source-read scan-time increment is dropped.
    EXPECT_GT(ioStatistics->totalScanTimeNs(), scanBefore);
}

// ===========================================================================
// predownload latency: the predownload source read is timed into
// CachedReadBufferPredownloadedFromSourceMicroseconds. A bounded busy-spin in the
// source pread makes that duration strictly positive (no sleep)
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, PredownloadRecordsPositiveSourceMicroseconds)
{
    // Same deterministic predownload setup as
    // PredownloadUpdatesReadPrefetchButNotRawBytes. The gap is predownloaded
    // through the reader Q1 hands off to the segment, which wraps Q1's source, so
    // Q1's source is the busy-spinning one: its pread is what the predownload
    // times.
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 10; });
    const auto data = makeData(10);
    const auto key = FileCacheKey::fromPath("predownload-source-micros");
    const FileCacheOriginInfo origin(manager->commonUserId(), 0);

    auto probe = cache->getOrSet(key, 0, data.size(), data.size(), CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(probe->size(), 1u);

    // Q1 downloads [0, 2) and stops, leaving the segment PARTIALLY_DOWNLOADED and
    // handing its busy-spinning source reader off to the segment.
    auto source1 = std::make_shared<SpinningReadFile>(data, std::chrono::microseconds(1000));
    FileCacheReadOptions q1;
    q1.remoteFsBufferSize = 2;
    auto input1 = makeInput(*manager, cache, source1, key, q1, "q1");
    auto stream1 = input1->read(0, data.size(), dwio::common::LogType::STREAM);
    const void * chunk = nullptr;
    int size = 0;
    ASSERT_TRUE(stream1->Next(&chunk, &size));
    ASSERT_EQ(probe->front().getCurrentWriteOffset(), 2u);

    // Q2 seeks past the written prefix, becomes the downloader, reuses Q1's
    // handed-off (spinning) reader, and predownloads the [2, 5) gap before its
    // own read.
    auto source2 = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions q2;
    q2.remoteFsBufferSize = 8;
    auto input2 = makeInput(*manager, cache, source2, key, q2, "q2");

    const uint64_t microsBefore =
        ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds);

    auto stream2 = input2->read(0, data.size(), dwio::common::LogType::STREAM);
    std::vector<uint64_t> seekPositions{5};
    dwio::common::PositionProvider provider(seekPositions);
    stream2->seekToPosition(provider);
    const void * chunk2 = nullptr;
    int size2 = 0;
    ASSERT_TRUE(stream2->Next(&chunk2, &size2));
    ASSERT_GT(size2, 0);

    // The predownload source read ran a bounded busy-spin, so the predownload
    // source-latency counter strictly advances. Fails if the predownload source
    // read is not timed into this counter (it was dead before).
    EXPECT_GT(
        ProfileEvents::get(ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds) - microsBefore,
        0u);
}

// ===========================================================================
// Cancellation token propagation (Task 017A / Task 3)
// ===========================================================================
TEST_F(FileCacheBufferedInputTest, DefaultTokenReadsFully)
{
    // Default (empty) token: nothing is ever cancelled, the read completes.
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("cancel-default");
    auto input = makeInput(*manager, cache, source, key);
    EXPECT_EQ(readAll(*input->read(0, 4096, dwio::common::LogType::STREAM)).size(), 4096u);
}

TEST_F(FileCacheBufferedInputTest, CopiedTokenReachesStream)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("cancel-token-copy");

    folly::CancellationSource src;
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, nullptr, nullptr, src.getToken());

    EXPECT_FALSE(input->cancellationToken().isCancellationRequested());
    src.requestCancellation();
    EXPECT_TRUE(input->cancellationToken().isCancellationRequested());
}

TEST_F(FileCacheBufferedInputTest, CancellationBeforeLookupThrows)
{
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("cancel-before-lookup");

    folly::CancellationSource src;
    src.requestCancellation(); // cancelled before any I/O
    auto input = makeInput(*manager, cache, source, key, {}, "q", nullptr, nullptr, nullptr, src.getToken());

    // The first nextFileSegmentsBatch check throws before any source read happens.
    VELOX_ASSERT_THROW(readAll(*input->read(0, 4096, dwio::common::LogType::STREAM)), "cancelled");
    EXPECT_EQ(source->preadBytes(), 0u);
}

TEST_F(FileCacheBufferedInputTest, CancellationDuringSegmentWaitThrows)
{
    // A downloader parks the segment in DOWNLOADING; a second reader is forced
    // onto FileSegment::wait with an *uncancelled* token, reaches the
    // beforeSegmentWait hook, and is cancelled only once it is actually there.
    // This exercises the cancellation check *inside* FileSegment::wait -- not the
    // pre-lookup check (the token is still uncancelled when the batch is looked
    // up).
    auto manager = makeManager();
    auto cache = makeCache(*manager);
    const auto data = makeData(4096);
    auto key = FileCacheKey::fromPath("cancel-during-wait");

    std::atomic<bool> downloaderParked{false};
    folly::Baton<> releaseDownloader;
    std::once_flag releaseOnce;
    auto releaseDownloaderFn = [&] { std::call_once(releaseOnce, [&] { releaseDownloader.post(); }); };
    auto stalling = std::make_shared<StallingReadFile>(data, downloaderParked, releaseDownloader);

    // The beforeSegmentWait hook fires once per wait() call; guard the post so a
    // (theoretical) second wait iteration cannot double-post the baton (UB).
    folly::Baton<> waiterAtWait;
    std::once_flag atWaitOnce;
    ScopedTestValue beforeWait(
        "facebook::velox::ch::FileCacheInputStream::beforeSegmentWait",
        std::function<void(void *)>(
            [&](void *) { std::call_once(atWaitOnce, [&] { waiterAtWait.post(); }); }));

    folly::CancellationSource cancelSrc;

    // Downloader: elects itself and parks in pread, holding the segment DOWNLOADING.
    std::exception_ptr downloaderError;
    std::thread downloader([&]
    {
        try
        {
            auto in = makeInput(*manager, cache, stalling, key, {}, "downloader");
            readAll(*in->read(0, 4096, dwio::common::LogType::STREAM));
        }
        catch (...)
        {
            downloaderError = std::current_exception();
        }
    });
    auto downloaderGuard = folly::makeGuard([&]
    {
        releaseDownloaderFn();
        if (downloader.joinable())
            downloader.join();
    });

    ASSERT_TRUE(spinUntil([&] { return downloaderParked.load(); }, std::chrono::seconds(20)))
        << "downloader never parked in pread (segment not DOWNLOADING)";

    // Waiter: same key, uncancelled token. It must reach FileSegment::wait.
    std::exception_ptr waiterError;
    std::atomic<bool> waiterDone{false};
    std::thread waiter([&]
    {
        try
        {
            auto in = makeInput(*manager, cache, stalling, key, {}, "waiter",
                                nullptr, nullptr, nullptr, cancelSrc.getToken());
            readAll(*in->read(0, 4096, dwio::common::LogType::STREAM));
        }
        catch (...)
        {
            waiterError = std::current_exception();
        }
        waiterDone.store(true);
    });
    auto waiterGuard = folly::makeGuard([&]
    {
        releaseDownloaderFn(); // let the waiter's wait() end even under a mutation
        if (waiter.joinable())
            waiter.join();
    });

    // The waiter is parked immediately before FileSegment::wait: cancel it there.
    // The wait loop observes the cancellation within one 1s slice and throws.
    waiterAtWait.wait();
    cancelSrc.requestCancellation();

    ASSERT_TRUE(spinUntil([&] { return waiterDone.load(); }, std::chrono::seconds(30)))
        << "waiter never observed cancellation inside FileSegment::wait";
    waiter.join();
    waiterGuard.dismiss();
    ASSERT_TRUE(waiterError != nullptr) << "waiter returned without throwing";
    VELOX_ASSERT_THROW(std::rethrow_exception(waiterError), "cancelled");

    // Release + join the downloader; its own read is uncancelled and must succeed.
    releaseDownloaderFn();
    downloader.join();
    downloaderGuard.dismiss();
    if (downloaderError)
        std::rethrow_exception(downloaderError);
}

TEST_F(FileCacheBufferedInputTest, CancellationDeferredUntilAfterSegmentWriteCompletes)
{
    // Request cancellation the instant this reader owns the downloader lease for
    // the first segment (mid-transaction). Cancellation must NOT interrupt the
    // reserve+write; the exception is deferred to the next safe boundary, by
    // which point the first segment is fully written.
    auto manager = makeManager();
    auto cache = makeCache(*manager, [](FileCacheConfig & c) { c.maxFileSegmentSize = 8; });
    const auto data = makeData(16);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("cancel-after-downloader-elected");

    folly::CancellationSource cancelSrc;
    std::atomic<bool> cancelledOnce{false};
    ScopedTestValue afterElected(
        "facebook::velox::ch::FileCacheInputStream::afterDownloaderElected",
        std::function<void(void *)>([&](void *)
        {
            if (!cancelledOnce.exchange(true))
                cancelSrc.requestCancellation();
        }));

    auto input = makeInput(*manager, cache, source, key, {}, "q",
                           nullptr, nullptr, nullptr, cancelSrc.getToken());

    // The read throws only at the safe boundary AFTER the first segment's
    // reserve+write completes -- never mid-transaction.
    VELOX_ASSERT_THROW(
        readAll(*input->read(0, 16, dwio::common::LogType::STREAM)), "cancelled");

    // Proof the write/complete happened before the exception: segment [0, 8) is
    // fully DOWNLOADED (8 bytes) and no segment is left DOWNLOADING.
    const auto infos = cache->getFileSegmentInfos(manager->commonUserId());
    bool firstComplete = false;
    for (const auto & info : infos)
    {
        EXPECT_NE(info.state, FileSegment::State::DOWNLOADING)
            << "segment at " << info.range_left << " left DOWNLOADING after cancellation";
        if (info.range_left == 0)
            firstComplete = info.state == FileSegment::State::DOWNLOADED && info.downloaded_size == 8;
    }
    EXPECT_TRUE(firstComplete)
        << "first segment [0, 8) was not fully written before the cancellation exception";
}

} // namespace
} // namespace facebook::velox::ch
