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
#include "velox/ch/Disks/IO/FileCacheCoalescedLoad.h"
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
#include <folly/synchronization/Baton.h>

#include <atomic>
#include <thread>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"

namespace facebook::velox::ch
{
namespace
{
namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;
using test::makeContent;

std::string readAll(dwio::common::SeekableInputStream & stream)
{
    std::string out;
    const void * data = nullptr;
    int32_t size = 0;
    while (stream.Next(&data, &size))
        out.append(static_cast<const char *>(data), static_cast<size_t>(size));
    return out;
}

// A ReadFile that blocks the FIRST pread on a baton so a test can deterministically
// suspend a warm task after it has entered its download loop but before it fills the
// segment. It wraps a real LocalReadFile and delegates every other call. The warm
// reader (ReadBufferFromVeloxReadFile) drives reads through the pread(void*) form, so
// only that path is gated; all others delegate. The gate fires exactly once.
class BlockingReadFile : public ReadFile
{
public:
    BlockingReadFile(
        std::shared_ptr<velox::LocalReadFile> inner,
        folly::Baton<> * firstReadStarted,
        folly::Baton<> * releaseFirstRead)
        : inner_(std::move(inner)), started_(firstReadStarted), release_(releaseFirstRead)
    {
    }

    std::string_view pread(
        uint64_t offset, uint64_t length, void * buf,
        const FileIoContext & context = {}) const override
    {
        gate();
        return inner_->pread(offset, length, buf, context);
    }

    std::string pread(
        uint64_t offset, uint64_t length,
        const FileIoContext & context = {}) const override
    {
        // Default base implementation routes through the gated pread(void*) above.
        return ReadFile::pread(offset, length, context);
    }

    bool shouldCoalesce() const override { return inner_->shouldCoalesce(); }
    uint64_t size() const override { return inner_->size(); }
    uint64_t memoryUsage() const override { return inner_->memoryUsage(); }
    std::string getName() const override { return inner_->getName(); }
    uint64_t getNaturalReadSize() const override { return inner_->getNaturalReadSize(); }

private:
    void gate() const
    {
        if (!gated_.exchange(true))
        {
            if (started_ != nullptr)
                started_->post();
            if (release_ != nullptr)
                release_->wait();
        }
    }

    std::shared_ptr<velox::LocalReadFile> inner_;
    folly::Baton<> * started_;
    folly::Baton<> * release_;
    mutable std::atomic<bool> gated_{false};
};

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
        return test::subPath(temp_->getPath(), s);
    }

    std::string writeSourceFile(const std::string & name, const std::string & content)
    {
        return test::writeSourceFile(temp_->getPath(), name, content);
    }

    FileCachePtr makeManagerCache(size_t seg, size_t align, size_t maxSize = 16 * 1024 * 1024)
    {
        auto cache = test::installManagerDefaultCache(manager_, sub("cache"), seg, align, maxSize);
        EXPECT_NE(cache, nullptr);
        return cache;
    }

    dwio::common::ReaderOptions readerOptions()
    {
        return dwio::common::ReaderOptions(pool_.get());
    }

    // Single construction point for the FileCacheBufferedInput used across these
    // tests. The variants below differ only in the source ReadFile, the executor
    // and the QueryStatus; everything else is fixed.
    std::unique_ptr<FileCacheBufferedInput> makeInputImpl(
        FileCachePtr cache,
        std::shared_ptr<ReadFile> readFile,
        const FileCacheKey & key,
        folly::Executor * executor,
        QueryStatus status)
    {
        FileCacheRequestContext ctx;
        ctx.queryId = "q1";
        ctx.userId = manager_->commonUserId();
        auto origin = cache->getCommonOrigin();
        return std::make_unique<FileCacheBufferedInput>(
            std::move(readFile),
            std::move(cache),
            key,
            origin,
            FileCacheReadOptions{},
            ctx,
            std::move(status),
            dwio::common::MetricsLog::voidLog(),
            velox::StringIdLease{},
            velox::StringIdLease{},
            /*tracker=*/nullptr,
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor,
            readerOptions());
    }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key)
    {
        return makeInputImpl(
            std::move(cache), std::make_shared<velox::LocalReadFile>(path), key,
            executor_.get(), QueryStatus{});
    }

    // Like makeInput, but wires a caller-supplied QueryStatus into the
    // FileCacheBufferedInput itself, so streams produced by enqueue()/read()
    // observe the cancellation token (the real query cancellation path).
    std::unique_ptr<FileCacheBufferedInput> makeInputWithStatus(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key,
        QueryStatus status)
    {
        return makeInputImpl(
            std::move(cache), std::make_shared<velox::LocalReadFile>(path), key,
            executor_.get(), std::move(status));
    }

    // Construct a FileCacheInputStream directly with an explicit QueryStatus so
    // the test controls the cancellation token. enqueue() itself uses a default
    // (never-cancel) QueryStatus.
    std::unique_ptr<FileCacheInputStream> makeStream(
        FileCacheBufferedInput & input,
        velox::common::Region region,
        QueryStatus status)
    {
        auto context = std::make_shared<FileCacheReadContext>();
        context->cache = manager_->getDefault();
        context->ioStatistics = std::make_shared<io::IoStatistics>();
        context->ioStats = std::make_shared<velox::IoStats>();
        context->source = input.sourceInputStream();
        context->pool = input.memoryPool()->shared_from_this();
        context->key = input.cacheKey();
        context->origin = input.origin();
        context->cacheOptions = input.cacheOptions();
        context->requestContext.queryId = "q1";
        context->requestContext.userId = manager_->commonUserId();
        context->queryStatus = std::move(status);
        context->tracker = input.tracker();
        context->fileNum = input.fileNum();
        context->groupId = input.groupId();
        context->fileSize = input.fileSize();
        return std::make_unique<FileCacheInputStream>(
            &input, context, region, dwio::common::LogType::STREAM);
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

    // Like makeInput, but with a caller-supplied warm executor so a test can
    // control the warm task's scheduling (e.g. a dedicated single-thread pool
    // whose only worker is held by a blocking task, forcing warm to queue).
    std::unique_ptr<FileCacheBufferedInput> makeInputWithExecutor(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key,
        folly::Executor * executor)
    {
        return makeInputImpl(
            std::move(cache), std::make_shared<velox::LocalReadFile>(path), key,
            executor, QueryStatus{});
    }

    // Like makeInput, but with a caller-supplied source ReadFile (e.g. a
    // BlockingReadFile) so a test can control when warm's source reads proceed.
    std::unique_ptr<FileCacheBufferedInput> makeInputWithReadFile(
        FileCachePtr cache, std::shared_ptr<ReadFile> readFile, const FileCacheKey & key)
    {
        return makeInputImpl(
            std::move(cache), std::move(readFile), key, executor_.get(), QueryStatus{});
    }

    // Like makeInputWithReadFile, but with a caller-supplied executor as well, so
    // a test can both gate the source reads (via the ReadFile) and control the
    // coalesced load's scheduling thread.
    std::unique_ptr<FileCacheBufferedInput> makeInputWithExecutorAndReadFile(
        FileCachePtr cache, std::shared_ptr<ReadFile> readFile, const FileCacheKey & key,
        folly::Executor * executor)
    {
        return makeInputImpl(
            std::move(cache), std::move(readFile), key, executor, QueryStatus{});
    }

    // Count how many segments over [offset, length) are fully DOWNLOADED, via a
    // no-create snapshot. Warm caches a segment only if it ran that segment to
    // completion; a segment left EMPTY/absent means warm bailed before it.
    size_t countDownloaded(FileCachePtr cache, const FileCacheKey & key,
        uint64_t offset, uint64_t length, const FileCacheOriginInfo & origin)
    {
        auto holder = cache->get(key, offset, length, 100, origin.user_id);
        if (!holder)
            return 0;
        size_t n = 0;
        for (const auto & segPtr : *holder)
        {
            if (segPtr->state() == FileSegment::State::DOWNLOADED)
                ++n;
        }
        return n;
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
TEST_F(FileCacheCancellationTest, CancellationDoesNotLeakDownloaderState)
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

// A stream constructed through the public enqueue() path with a cancellable
// QueryStatus wired into the FileCacheBufferedInput honours the token: once the
// query is cancelled, the next Next() throws. This is the real query
// cancellation path (builder -> ConnectorQueryCtx::cancellationToken ->
// QueryStatus -> FileCacheBufferedInput -> stream).
TEST_F(FileCacheCancellationTest, EnqueuePathHonoursQueryCancellation)
{
    const size_t n = 256 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    folly::CancellationSource src;
    src.requestCancellation();
    auto input = makeInputWithStatus(cache, path, key, QueryStatus{src.getToken()});

    auto stream = input->enqueue({0, n});
    const void * data = nullptr;
    int32_t size = 0;
    EXPECT_THROW(stream->Next(&data, &size), VeloxRuntimeError);

    expectNoHeldDownloader(cache, key, 0, n, input->origin());
}

// R2-5 (coalesced model): reset() must cancel every planned coalesced load.
// Prefetch/demand no longer run through a warm generation; load() builds one
// FileCacheCoalescedLoad per miss group and reset() flips each planned load to
// kCancelled (mirrors DirectBufferedInput::reset).
//
// Deterministic (no sleep, no executor): a NULL executor is wired into the
// input, so the prefetch group's coalesced load is built but never submitted --
// it stays kPlanned. We grab the load handle through the public
// coalescedLoads(stream) binding (which keeps the shared_ptr alive past the
// map erase), assert it is kPlanned, then call reset() and assert it is now
// kCancelled.
//
// RED (neutralise the fix): removing the `load->cancel()` loop from
// FileCacheBufferedInput::reset() leaves the load kPlanned -> the final
// EXPECT_EQ(kCancelled) fails.
TEST_F(FileCacheCancellationTest, ResetCancelsPlannedCoalescedLoad)
{
    const size_t seg = 64 * 1024;
    const size_t n = 2 * seg; // two segments
    auto content = makeContent(n);
    auto cache = makeManagerCache(seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Null executor: a prefetch group's coalesced load is built but not run.
    auto input = makeInputWithExecutor(cache, path, key, /*executor*/ nullptr);

    // Empty trackingId => prefetch group => one coalesced load, kPlanned.
    auto stream = input->enqueue({0, n});
    input->load(dwio::common::LogType::FILE);

    // Grab the load through the public binding. coalescedLoads() moves+erases the
    // binding out of the map, but the load shared_ptr stays alive here and is
    // still owned by the input's coalescedLoads_ vector (so reset() will cancel
    // it).
    auto bindings = input->coalescedLoads(stream.get());
    ASSERT_EQ(bindings.size(), 1u) << "prefetch group must build one coalesced load";
    auto load = bindings.front().load;
    ASSERT_NE(load, nullptr);
    EXPECT_EQ(load->state(), cache::CoalescedLoad::State::kPlanned)
        << "null-executor prefetch load must stay kPlanned before reset";

    // The caller discards the old plan. reset() must cancel the planned load.
    input->reset();

    EXPECT_EQ(load->state(), cache::CoalescedLoad::State::kCancelled)
        << "reset must cancel every planned coalesced load";
}

// R2-5 (coalesced model): a plain destructor cancels a PLANNED coalesced load
// (the destructor's own cancel path, distinct from reset). A null-executor
// prefetch load stays kPlanned; destroying the input (inner scope) must flip it
// to kCancelled.
//
// RED (neutralise the fix): removing the `load->cancel()` loop from
// ~FileCacheBufferedInput leaves the load kPlanned -> the final
// EXPECT_EQ(kCancelled) fails.
TEST_F(FileCacheCancellationTest, PlainDestructorCancelsPlannedCoalescedLoad)
{
    const size_t seg = 64 * 1024;
    const size_t n = 2 * seg; // two segments
    auto content = makeContent(n);
    auto cache = makeManagerCache(seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    std::shared_ptr<FileCacheCoalescedLoad> load;
    {
        // Null executor: the prefetch group's coalesced load is built but not run.
        auto input = makeInputWithExecutor(cache, path, key, /*executor*/ nullptr);
        auto stream = input->enqueue({0, n});
        input->load(dwio::common::LogType::FILE);
        auto bindings = input->coalescedLoads(stream.get());
        ASSERT_EQ(bindings.size(), 1u);
        load = bindings.front().load;
        ASSERT_NE(load, nullptr);
        EXPECT_EQ(load->state(), cache::CoalescedLoad::State::kPlanned);
        // input (and stream) destroyed here without reset().
    }
    EXPECT_EQ(load->state(), cache::CoalescedLoad::State::kCancelled)
        << "the destructor must cancel a planned coalesced load";
}

// R2-5 (coalesced model): a RUNNING coalesced load, kept alive by its shared
// context, completes its IO safely even after the input is destroyed.
//
// A real single-thread executor runs the prefetch load, whose first source read
// is parked on a BlockingReadFile gate, so the load is mid-loadData (kLoading)
// when we destroy the input. The load shared_ptr (grabbed via the binding) keeps
// its shared FileCacheReadContext -- source, pool, cache -- alive, so loadData
// finishes after the input is gone. We release the gate, join the executor, and
// assert both segments are cached.
//
// This proves shared-context safety; it is NOT toggled by the cancel loop
// (cancel() never aborts an in-flight loadData -- see the R2-5 report note), so
// there is no deterministic RED for this case: it is a safety/non-regression
// assertion, not a behaviour switched by the cancel logic.
TEST_F(FileCacheCancellationTest, RunningCoalescedLoadCompletesAfterDestruction)
{
    const size_t seg = 64 * 1024;
    const size_t n = 2 * seg; // two segments
    auto content = makeContent(n);
    auto cache = makeManagerCache(seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // A dedicated executor so the prefetch load runs on its own thread. Lives on
    // the stack until after join(), outliving the load it runs.
    folly::CPUThreadPoolExecutor prefetchExecutor(1);

    // Park the load's first source read on the gate: it enters loadData (kLoading)
    // and blocks, so it is RUNNING when we destroy the input.
    folly::Baton<> readStarted;
    folly::Baton<> releaseRead;
    auto blocking = std::make_shared<BlockingReadFile>(
        std::make_shared<velox::LocalReadFile>(path), &readStarted, &releaseRead);

    {
        auto input = makeInputWithExecutorAndReadFile(cache, blocking, key, &prefetchExecutor);
        auto stream = input->enqueue({0, n});
        input->load(dwio::common::LogType::FILE);

        // Wait until the prefetch load has entered its first source read
        // (mid-loadData) so it is genuinely running.
        readStarted.wait();

        // Destroy the input (and stream) WITHOUT reset while the load runs. The
        // destructor's cancel() must NOT abort the in-flight loadData.
    }

    // Let the running load finish its IO -- it survives on its shared context.
    releaseRead.post();
    prefetchExecutor.join();

    auto origin = cache->getCommonOrigin();
    EXPECT_EQ(countDownloaded(cache, key, 0, n, origin), 2u)
        << "a running coalesced load must complete safely after the input is destroyed";
}

} // namespace
} // namespace facebook::velox::ch
