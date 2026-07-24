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
#include "velox/ch/Interpreters/FileCache/FileCache.h"

#include "velox/ch/Common/FileCacheBoundedQueue.h"
#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <folly/futures/ManualTimekeeper.h>
#include "folly/synchronization/CallOnce.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <latch>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{

// ---------------------------------------------------------------------------
// Structural invariant (file scope, inside the anonymous namespace so that the
// explicit instantiation is at namespace scope as required by the standard).
// This explicit template instantiation forms a pointer-to-member of type
// `folly::once_flag FileCache::*`; it compiles only when FileCache has a
// member named `initialize_once_flag` of exactly that type.
//
// Any mutation that restores the old plain mutex+bool guard (making the field
// absent or the wrong type) turns this instantiation into a compile error —
// the build goes RED.  Substituting `std::once_flag` also goes RED because
// `std::once_flag FileCache::*` ≠ `folly::once_flag FileCache::*`.
// No public API is added: the member remains private; the standard allows
// pointer-to-member formation in explicit-instantiation arguments without
// access checks.
// ---------------------------------------------------------------------------
namespace
{
    template <typename Tag, typename Tag::type M>
    struct PrivateMemberPin {};

    struct FileCacheOnceFlagPin
    {
        using type = folly::once_flag FileCache::*;
    };

    // RED until `folly::once_flag initialize_once_flag` is added to FileCache.
    template struct PrivateMemberPin<FileCacheOnceFlagPin, &FileCache::initialize_once_flag>;
} // namespace

namespace
{

namespace fs = std::filesystem;
using velox::common::testutil::TempDirectoryPath;

// Minimal real-file-backed WriteFile: the download write path drives it through
// `set(from, size, size)` + `next()` -> `append`. Fully commits every byte to a
// real temporary file (no throw budget), so downloaded segments have real files.
class RealWriteFile : public velox::WriteFile
{
public:
    RealWriteFile(std::string path, bool append) : path_(std::move(path))
    {
        auto flags = std::ios::binary | std::ios::out;
        flags |= append ? std::ios::app : std::ios::trunc;
        out_.open(path_, flags);
        if (append)
        {
            std::error_code ec;
            const auto existing = fs::file_size(path_, ec);
            if (!ec)
                written_ = existing;
        }
    }

    void append(std::string_view data) override
    {
        out_.write(data.data(), static_cast<std::streamsize>(data.size()));
        out_.flush();
        written_ += data.size();
    }

    void flush() override { out_.flush(); }
    void close() override { out_.flush(); out_.close(); }
    uint64_t size() const override { return written_; }
    const std::string getName() const override { return path_; }

private:
    std::string path_;
    std::ofstream out_;
    uint64_t written_{0};
};

class FileCacheTest : public ::testing::Test
{
protected:
    // Build a FileCache bound to caller-provided shared services (scheduler +
    // worker pool). The worker pool must already be large enough for the cache's
    // metadata-load workers: FileCache::loadMetadataImpl no longer resizes the
    // shared pool, it verifies capacity fail-closed. Not initialized.
    std::unique_ptr<FileCache> makeCacheWithServices(
        FileCacheScheduler & scheduler,
        FileCacheWorkerPool & workerPool,
        const std::function<void(FileCacheConfig &)> & mutate = {},
        const std::string & name = "test")
    {
        FileCacheConfig config;
        config.path = cacheDir_->getPath();
        config.maxSize = 64ull << 20;
        config.maxFileSegmentSize = 1ull << 20;
        config.boundaryAlignment = 4096;
        config.reserveGranularity = 0;
        // Keep metadata-load parallelism modest by default so the fixture's
        // 8-thread pool satisfies the fail-close precondition (listing + loading
        // <= pool size). Tests that need more raise this and size their own pool.
        config.loadMetadataThreads = 2;
        if (mutate)
            mutate(config);

        return std::make_unique<FileCache>(
            name,
            config,
            scheduler,
            workerPool,
            pool_.get(),
            origin_,
            [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
            { return std::make_unique<RealWriteFile>(path, append); },
            [](const std::string &) -> std::shared_ptr<velox::ReadFile> { return nullptr; },
            [](const std::string &) {});
    }

    // Build a FileCache on the fixture's shared scheduler/worker pool. Not initialized.
    std::unique_ptr<FileCache> makeCache(
        const std::function<void(FileCacheConfig &)> & mutate = {},
        const std::string & name = "test")
    {
        return makeCacheWithServices(scheduler_, workerPool_, mutate, name);
    }

    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("filecache-test");
        cacheDir_ = TempDirectoryPath::create();
    }

    // Download a regular segment [0, size) fully; release the holder so it is a
    // releasable, DOWNLOADED cache entry.
    void fullyDownload(FileCache & cache, const FileCacheKey & key, size_t size)
    {
        downloadRange(cache, key, /*offset*/ 0, size, /*file_size*/ size);
    }

    // Download a regular segment [offset, offset+size) fully through the real
    // production path so a real file is written and (on completion) renamed to the
    // `<offset>_<size>` on-disk form used by metadata reload.
    void downloadRange(FileCache & cache, const FileCacheKey & key, size_t offset, size_t size, size_t file_size)
    {
        auto holder = cache.getOrSet(key, offset, size, file_size, CreateFileSegmentSettings{}, 0, origin_);
        auto segment = holder->getSingleFileSegment();
        ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
        std::string reason;
        ASSERT_TRUE(segment->reserve(size, 1000, reason)) << "reserve failed: " << reason;
        std::string data(size, 'z');
        segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());
        segment->resetDownloader();
        ASSERT_EQ(segment->state(), FileSegment::State::DOWNLOADED);
    }

    std::shared_ptr<folly::ManualTimekeeper> timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    FileCacheWorkerPool workerPool_{8, 1, "fc-test"};
    FileCacheScheduler scheduler_{timekeeper_, workerPool_};
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> cacheDir_;
    FileCacheOriginInfo origin_{"fc-user", 0};
};

// -- initialize is idempotent ----------------------------------------------

TEST_F(FileCacheTest, InitializeOnce)
{
    auto cache = makeCache();
    EXPECT_FALSE(cache->isInitialized());
    cache->initialize();
    EXPECT_TRUE(cache->isInitialized());
    // A second call is a no-op (std::call_once): still initialized, no throw.
    EXPECT_NO_THROW(cache->initialize());
    EXPECT_TRUE(cache->isInitialized());
    cache->deactivateBackgroundOperations();
}

// -- get() never creates metadata ------------------------------------------

TEST_F(FileCacheTest, GetDoesNotCreateMetadata)
{
    auto cache = makeCache();
    cache->initialize();

    const auto key = FileCacheKey::random();
    auto holder = cache->get(key, /*offset*/ 0, /*size*/ 4096, /*file_segments_limit*/ 0, origin_.user_id);
    ASSERT_FALSE(holder->empty());
    // A miss is served with a synthetic DETACHED placeholder, never persisted.
    for (const auto & segment : *holder)
        EXPECT_EQ(segment->state(), FileSegment::State::DETACHED);

    // No key/segment metadata was created.
    EXPECT_TRUE(cache->getFileSegmentInfos(origin_.user_id).empty());

    holder.reset();
    cache->deactivateBackgroundOperations();
}

// -- getOrSet fills holes with EMPTY metadata-owned segments ----------------

TEST_F(FileCacheTest, GetOrSetCreatesHoles)
{
    auto cache = makeCache();
    cache->initialize();

    const auto key = FileCacheKey::random();
    auto holder = cache->getOrSet(key, /*offset*/ 0, /*size*/ 8192, /*file_size*/ 8192, CreateFileSegmentSettings{}, /*limit*/ 0, origin_);
    ASSERT_FALSE(holder->empty());

    // Every hole is filled with a metadata-owned EMPTY segment covering the range.
    size_t covered = 0;
    for (const auto & segment : *holder)
    {
        EXPECT_EQ(segment->state(), FileSegment::State::EMPTY);
        covered += segment->range().size();
    }
    EXPECT_GE(covered, 8192u);

    // The EMPTY segments are persisted (getOrSet path), unlike get().
    EXPECT_FALSE(cache->getFileSegmentInfos(key, origin_.user_id).empty());

    holder.reset();
    cache->deactivateBackgroundOperations();
}

// -- tryReserve evicts a releasable segment to make room --------------------

TEST_F(FileCacheTest, TryReserveEvictsReleasable)
{
    auto cache = makeCache([](FileCacheConfig & c)
    {
        c.cachePolicy = FileCachePolicy::LRU; // single flat queue: a maxSize segment fits
        c.maxSize = 4096;          // room for exactly one 4096-byte segment
        c.maxFileSegmentSize = 4096;
        c.boundaryAlignment = 4096;
    });
    cache->initialize();

    // Fill the cache with a fully-downloaded, releasable segment A.
    const auto keyA = FileCacheKey::random();
    fullyDownload(*cache, keyA, 4096);
    ASSERT_EQ(cache->getFileSegmentInfos(keyA, origin_.user_id).size(), 1u);
    const auto usedAfterA = cache->getUsedCacheSize();
    EXPECT_EQ(usedAfterA, 4096u);

    // Reserving space for a new segment B must evict releasable A.
    const auto keyB = FileCacheKey::random();
    auto holderB = cache->getOrSet(keyB, 0, 4096, 4096, CreateFileSegmentSettings{}, 0, origin_);
    auto segmentB = holderB->getSingleFileSegment();
    ASSERT_EQ(segmentB->getOrSetDownloader(), FileSegment::getCallerId());
    std::string reason;
    FileCacheReserveStat stat;
    EXPECT_TRUE(cache->tryReserve(*segmentB, 4096, stat, origin_, /*lock_wait_timeout_ms*/ 1000, reason))
        << "reserve failed: " << reason;

    // A was evicted (its releasable entry removed); the cache still holds 4096.
    EXPECT_TRUE(cache->tryGetCachePaths(keyA).empty());
    EXPECT_EQ(cache->getUsedCacheSize(), 4096u);

    // Complete B's download so teardown finds a consistent DOWNLOADED segment.
    std::string dataB(4096, 'y');
    segmentB->write(dataB.data(), dataB.size(), segmentB->getCurrentWriteOffset());
    segmentB->resetDownloader();
    holderB.reset();
    cache->deactivateBackgroundOperations();
}

// -- deactivateBackgroundOperations joins all workers (bounded probe) --------

TEST_F(FileCacheTest, ShutdownJoinsWorkers)
{
    auto cache = makeCache([](FileCacheConfig & c) { c.backgroundDownloadThreads = 2; });
    cache->initialize();
    // The metadata download/cleanup workers are started and idle, parked in a
    // blocking pop(). CacheMetadata::shutdown must cancel the queues BEFORE
    // joining them; otherwise the join would hang. Observe the exact order via the
    // two shutdown checkpoints (afterCancelBeforeJoin, afterJoin) and require
    // completion under a bounded future, so a join-before-cancel regression shows
    // up as a timeout AND a missing/empty ordering, not merely a silent pass.
    facebook::velox::common::testutil::TestValue::enable();
    std::mutex order_mutex;
    std::vector<std::string> order;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::ch::CacheMetadata::shutdown::afterCancelBeforeJoin",
        std::function<void(void *)>([&](void *) { std::lock_guard<std::mutex> g(order_mutex); order.push_back("cancel"); }));
    SCOPED_TESTVALUE_SET(
        "facebook::velox::ch::CacheMetadata::shutdown::afterJoin",
        std::function<void(void *)>([&](void *) { std::lock_guard<std::mutex> g(order_mutex); order.push_back("join"); }));

    std::promise<void> done;
    auto future = done.get_future();
    std::thread shutdownThread([&] { cache->deactivateBackgroundOperations(); done.set_value(); });
    ASSERT_EQ(future.wait_for(std::chrono::seconds(15)), std::future_status::ready)
        << "deactivateBackgroundOperations did not complete: workers were not joined";
    shutdownThread.join();

    // The queues were cancelled strictly before the workers were joined.
    {
        std::lock_guard<std::mutex> g(order_mutex);
        ASSERT_EQ(order.size(), 2u);
        EXPECT_EQ(order[0], "cancel");
        EXPECT_EQ(order[1], "join");
    }

    // Idempotent: a second deactivate on a quiesced cache also returns.
    EXPECT_NO_THROW(cache->deactivateBackgroundOperations());
}

// -- a second live cache on the same path fails the StatusFile process lock,
//    can retry after the first is gone, and does not re-run after success ----

TEST_F(FileCacheTest, SecondProcessStatusLockFails)
{
    auto cache1 = makeCache({}, "first");
    cache1->initialize();
    EXPECT_TRUE(cache1->isInitialized());

    // A second cache instance over the same directory must fail to acquire the
    // <base>/status exclusive lock during initialize(). initialize() must throw a
    // catchable exception (not abort): folly::call_once leaves the once_flag in
    // the incomplete state on a throw, so the cache remains uninitialized and a
    // later call can retry.
    auto cache2 = makeCache({}, "second");
    EXPECT_ANY_THROW(cache2->initialize());
    EXPECT_FALSE(cache2->isInitialized());

    // Release cache1: deactivate background tasks then destroy it; destroying the
    // FileCache object destroys `status_file`, releasing the exclusive flock.
    cache1->deactivateBackgroundOperations();
    cache1.reset();

    // Retry: the status lock is now free; cache2's once_flag was left incomplete
    // by the first (throwing) call, so folly::call_once retries the body and
    // this call succeeds.
    EXPECT_NO_THROW(cache2->initialize());
    EXPECT_TRUE(cache2->isInitialized());

    // No-rerun: calling initialize() again on an already-initialized cache is a
    // no-op.  folly::call_once finds the flag in the `done` state and returns
    // immediately without invoking the callback.
    EXPECT_NO_THROW(cache2->initialize());
    EXPECT_TRUE(cache2->isInitialized());

    cache2->deactivateBackgroundOperations();
}

// -- bounded concurrent callers: every thread succeeds, cache initializes once

TEST_F(FileCacheTest, ConcurrentInitializersSucceedOnce)
{
    // N threads all call initialize() concurrently on the same FileCache.
    // folly::call_once must: let exactly one thread run the body; block the
    // others until that body completes (or fails); publish `is_initialized`
    // safely to all callers.  Every thread must return without throwing, and
    // the cache must be initialized afterwards.
    constexpr int N = 8;
    auto cache = makeCache();
    EXPECT_FALSE(cache->isInitialized());

    std::latch start_gate(N);
    std::atomic<int> throw_count{0};
    std::vector<std::thread> threads;
    threads.reserve(N);

    for (int i = 0; i < N; ++i)
    {
        threads.emplace_back([&]
        {
            start_gate.arrive_and_wait(); // all start together
            try
            {
                cache->initialize();
            }
            catch (...)
            {
                ++throw_count;
            }
        });
    }

    for (auto & t : threads)
        t.join();

    // Every concurrent caller must have succeeded.
    EXPECT_EQ(throw_count.load(), 0);
    // The cache was initialized exactly once (no double-init conflict; status
    // file created once; is_initialized is true).
    EXPECT_TRUE(cache->isInitialized());

    cache->deactivateBackgroundOperations();
}

// -- the internal origin can access keys created by any user ----------------

TEST_F(FileCacheTest, InternalOriginAccessAllKeys)
{
    auto cache = makeCache();
    cache->initialize();

    const auto key = FileCacheKey::random();
    fullyDownload(*cache, key, 4096);

    // The maintenance "internal" identity can read segment info for a key owned
    // by fc-user (checkAccess permits the internal user id).
    const auto & internalUser = FileCache::getInternalOrigin().user_id;
    EXPECT_EQ(cache->getFileSegmentInfos(key, internalUser).size(), 1u);

    cache->deactivateBackgroundOperations();
}

// -- getCommonOrigin returns the injected common user id --------------------

TEST_F(FileCacheTest, CommonOriginIsInjectedUserId)
{
    auto cache = makeCache();
    EXPECT_EQ(cache->getCommonOrigin().user_id, origin_.user_id);
    // The internal origin is distinct from the injected common origin.
    EXPECT_NE(FileCache::getInternalOrigin().user_id, cache->getCommonOrigin().user_id);
}

// -- doomed query context destruction runs after the write lock is released --
//    (production QueryContextHolder destructor; mandatory-tests "doomed context")

TEST_F(FileCacheTest, DoomedQueryContextDestroyedAfterWriteLockReleased)
{
    // The per-query cache limit must be enabled for the cache to own a
    // FileCacheQueryLimit and hand out QueryContextHolders.
    auto cache = makeCache([](FileCacheConfig & c) { c.enableFilesystemQueryCacheLimit = true; });
    cache->initialize();

    // A non-zero per-query download limit is required for getQueryContextHolder to
    // create a context and its owning holder.
    FileCacheReadOptions options;
    options.maxDownloadSizePerQuery = 4096;
    auto holder = cache->getQueryContextHolder("q-doomed", options);
    ASSERT_NE(holder, nullptr);
    ASSERT_NE(holder->context, nullptr);

    // Arm the ~QueryContext seam. It reacquires the cache write lock, which only
    // succeeds because the production QueryContextHolder destructor destroys the
    // orphaned ("doomed") context AFTER releasing that lock. The write lock is a
    // non-reentrant folly::SharedMutex, so a destroy-under-lock regression would
    // deadlock and time out the bounded future below. This couples the assertion
    // to the actual QueryContext destruction rather than a manually driven copy of
    // removeQueryContext.
    facebook::velox::common::testutil::TestValue::enable();
    std::atomic<bool> destroyed_after_unlock{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::ch::FileCacheQueryLimit::QueryContext::~QueryContext",
        std::function<void(void *)>([&](void *)
        {
            auto lock = cache->lockCache();
            destroyed_after_unlock.store(true);
        }));

    // Destroy the only holder on a separate thread so a destroy-under-lock
    // deadlock is observed as a timeout, not a hung test. This drives the real
    // ~QueryContextHolder -> removeQueryContext (last holder) -> destroy the doomed
    // context outside the lock -> ~QueryContext -> seam.
    std::promise<void> done;
    auto future = done.get_future();
    std::thread destroyer([&] { holder.reset(); done.set_value(); });
    ASSERT_EQ(future.wait_for(std::chrono::seconds(15)), std::future_status::ready)
        << "QueryContextHolder destructor deadlocked: doomed context destroyed under the cache lock";
    destroyer.join();

    EXPECT_TRUE(destroyed_after_unlock.load());

    cache->deactivateBackgroundOperations();
}

// -- failed eviction during dynamic resize restores limits and queue entries -
//    (ClickHouse gtest_filecache.cpp FailedEvictionRestorePreservesInvariants)

TEST_F(FileCacheTest, FailedEvictionRestorePreservesInvariants)
{
    // A resize that must evict, whose eviction fails, must roll back: the size
    // limit reverts to the previous value, no segment is lost, cache size
    // accounting is unchanged, and every segment stays reachable from the priority
    // queue. Failure is injected narrowly at the production eviction failpoint.
    auto cache = makeCache([](FileCacheConfig & c)
    {
        c.cachePolicy = FileCachePolicy::LRU;
        c.maxSize = 8192;          // room for exactly two 4096-byte segments
        c.maxElements = 8;
        c.maxFileSegmentSize = 4096;
        c.boundaryAlignment = 4096;
        c.allowDynamicCacheResize = true;
    });
    cache->initialize();

    const auto keyA = FileCacheKey::random();
    const auto keyB = FileCacheKey::random();
    fullyDownload(*cache, keyA, 4096);
    fullyDownload(*cache, keyB, 4096);
    ASSERT_EQ(cache->getUsedCacheSize(), 8192u);
    ASSERT_EQ(cache->getFileSegmentsNum(), 2u);

    FileCacheConfig actual;
    {
        // Snapshot the current (actual) settings the resize compares against.
        actual.maxSize = 8192;
        actual.maxElements = 8;
        actual.maxFileSegmentSize = 4096;
        actual.boundaryAlignment = 4096;
        actual.cachePolicy = FileCachePolicy::LRU;
        actual.allowDynamicCacheResize = true;
    }

    facebook::velox::common::testutil::TestValue::enable();
    {
        // Force the eviction to fail for the duration of the resize attempt.
        SCOPED_TESTVALUE_SET(
            "facebook::velox::ch::filecache::failpoint::file_cache_dynamic_resize_fail_to_evict",
            std::function<void(void *)>([](void *) { VELOX_FAIL("Injected failed eviction"); }));

        FileCacheConfig shrink = actual;
        shrink.maxSize = 4096; // requires evicting one segment

        // Failed eviction must be handled by the restore path, not thrown out.
        ASSERT_NO_THROW(cache->applySettingsIfPossible(shrink, actual));

        // The size limit reverted to the previous value.
        EXPECT_EQ(actual.maxSize, 8192u);
        // Nothing was lost and accounting is unchanged.
        EXPECT_EQ(cache->getUsedCacheSize(), 8192u);
        EXPECT_EQ(cache->getFileSegmentsNum(), 2u);
        // Both segments remain reachable from the priority queue.
        for (const auto * key : {&keyA, &keyB})
        {
            auto infos = cache->getFileSegmentInfos(*key, origin_.user_id);
            ASSERT_EQ(infos.size(), 1u);
            EXPECT_NE(infos[0].queue_entry_type, IFileCachePriority::QueueEntryType::None);
        }
    }

    // With the failpoint disarmed a real resize now succeeds, proving delayed
    // eviction state was cleared by the rollback.
    {
        FileCacheConfig shrink = actual;
        shrink.maxSize = 4096;
        ASSERT_NO_THROW(cache->applySettingsIfPossible(shrink, actual));
        EXPECT_LE(cache->getUsedCacheSize(), 4096u);
    }

    cache->deactivateBackgroundOperations();
}

// ===========================================================================
// Metadata load / reload (FileCache::loadMetadataImpl). The shared worker pool
// is never resized here; the manager budgets it and this path verifies capacity
// fail-closed. See ClickHouse gtest_filecache.cpp LoadMetadataParallelism.
// ===========================================================================

// -- a fresh cache on the same path recovers all downloaded (size-suffixed)
//    segments written by a prior instance -----------------------------------

TEST_F(FileCacheTest, MetadataReloadRecoversDownloadedSegments)
{
    std::vector<FileCacheKey> keys;
    {
        auto cache = makeCache();
        cache->initialize();
        for (int i = 0; i < 6; ++i)
        {
            const auto key = FileCacheKey::random();
            fullyDownload(*cache, key, 4096);
            keys.push_back(key);
        }
        cache->deactivateBackgroundOperations();
        // Destroyed at block end, releasing the <base>/status process lock.
    }

    auto reloaded = makeCache({}, "reloaded");
    reloaded->initialize();
    for (const auto & key : keys)
    {
        auto infos = reloaded->getFileSegmentInfos(key, origin_.user_id);
        ASSERT_EQ(infos.size(), 1u);
        EXPECT_EQ(infos[0].state, FileSegmentState::DOWNLOADED);
        EXPECT_EQ(infos[0].range_left, 0u);
        EXPECT_EQ(infos[0].range_right, 4095u);
    }
    reloaded->deactivateBackgroundOperations();
}

// -- reload with several listing/loading thread counts recovers the same,
//    complex multi-key/multi-segment structure (parallel listing + loading) --

TEST_F(FileCacheTest, MetadataReloadParallelListingAndLoading)
{
    constexpr int num_keys = 24;
    constexpr int segs_per_key = 2;
    constexpr size_t seg_size = 4096;

    std::vector<FileCacheKey> keys;
    {
        auto cache = makeCache([](FileCacheConfig & c) { c.maxSize = 64ull << 20; });
        cache->initialize();
        for (int k = 0; k < num_keys; ++k)
        {
            const auto key = FileCacheKey::random();
            for (int s = 0; s < segs_per_key; ++s)
                downloadRange(*cache, key, s * seg_size, seg_size, segs_per_key * seg_size);
            keys.push_back(key);
        }
        cache->deactivateBackgroundOperations();
    }

    // The fixture pool has 8 threads, so listing + loading must stay <= 8.
    for (uint64_t threads : {1u, 3u, 6u})
    {
        auto cache = makeCache(
            [threads](FileCacheConfig & c)
            {
                c.maxSize = 64ull << 20;
                c.loadMetadataThreads = threads;
            },
            "reload-" + std::to_string(threads));
        cache->initialize();

        size_t total = 0;
        for (const auto & key : keys)
        {
            auto infos = cache->getFileSegmentInfos(key, origin_.user_id);
            ASSERT_EQ(infos.size(), static_cast<size_t>(segs_per_key)) << "threads=" << threads;
            std::sort(infos.begin(), infos.end(), [](const auto & a, const auto & b) { return a.range_left < b.range_left; });
            for (int s = 0; s < segs_per_key; ++s)
            {
                EXPECT_EQ(infos[s].state, FileSegmentState::DOWNLOADED) << "threads=" << threads;
                EXPECT_EQ(infos[s].range_left, s * seg_size);
                EXPECT_EQ(infos[s].range_right, (s + 1) * seg_size - 1);
            }
            total += infos.size();
        }
        EXPECT_EQ(total, static_cast<size_t>(num_keys * segs_per_key)) << "threads=" << threads;
        cache->deactivateBackgroundOperations();
    }
}

// -- reload recovers a legacy (unsuffixed, stat-sized) segment and drops a
//    leftover temporary segment file --------------------------------------

TEST_F(FileCacheTest, MetadataReloadRecoversLegacyAndDropsTemporary)
{
    fs::path keyDir;
    {
        auto cache = makeCache();
        cache->initialize();
        const auto key = FileCacheKey::random();
        fullyDownload(*cache, key, 4096); // writes <keydir>/0_4096
        auto infos = cache->getFileSegmentInfos(key, origin_.user_id);
        ASSERT_EQ(infos.size(), 1u);
        keyDir = fs::path(infos[0].path).parent_path();
        cache->deactivateBackgroundOperations();
    }
    ASSERT_FALSE(keyDir.empty());
    ASSERT_TRUE(fs::exists(keyDir));

    // A legacy `<offset>` file (size read via stat) and a leftover
    // `<offset>_temporary` file (dropped during load).
    {
        std::ofstream legacy((keyDir / "8192").string(), std::ios::binary | std::ios::trunc);
        const std::string bytes(100, 'x');
        legacy.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    {
        std::ofstream temp((keyDir / "16384_temporary").string(), std::ios::binary | std::ios::trunc);
        const std::string bytes(50, 'y');
        temp.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    const auto key = FileCacheKey::fromKeyString(keyDir.filename().string());
    auto reloaded = makeCache({}, "reloaded-legacy");
    reloaded->initialize();

    auto infos = reloaded->getFileSegmentInfos(key, origin_.user_id);
    // The size-suffixed segment (offset 0) and the legacy segment (offset 8192)
    // are loaded; the temporary file is removed and never becomes a segment.
    ASSERT_EQ(infos.size(), 2u);
    std::sort(infos.begin(), infos.end(), [](const auto & a, const auto & b) { return a.range_left < b.range_left; });
    EXPECT_EQ(infos[0].range_left, 0u);
    EXPECT_EQ(infos[0].range_right, 4095u);
    EXPECT_EQ(infos[1].range_left, 8192u);
    EXPECT_EQ(infos[1].range_right, 8291u); // 8192 + 100 - 1
    EXPECT_FALSE(fs::exists(keyDir / "16384_temporary"));

    reloaded->deactivateBackgroundOperations();
}

// -- a shared pool too small for the load workers fails closed (no deadlock) --

TEST_F(FileCacheTest, MetadataLoadInsufficientPoolCapacityFailsClosed)
{
    // A private 1-thread pool cannot run the 4 concurrent listing/loading workers
    // that load_metadata_threads=4 needs. FileCache must not resize the shared
    // pool, so initialize fails closed with a required-vs-available message.
    FileCacheWorkerPool tinyPool{1, 1, "tiny"};
    FileCacheScheduler tinyScheduler{timekeeper_, tinyPool};
    auto cache = makeCacheWithServices(
        tinyScheduler, tinyPool, [](FileCacheConfig & c) { c.loadMetadataThreads = 4; }, "insufficient");

    try
    {
        cache->initialize();
        FAIL() << "initialize must fail the worker-pool capacity precondition";
    }
    catch (const std::exception & e)
    {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("needs 4"), std::string::npos) << msg;
        EXPECT_NE(msg.find("provides only 1"), std::string::npos) << msg;
    }
    EXPECT_FALSE(cache->isInitialized());
}

// -- the first exception raised while loading a key propagates out of load ----

TEST_F(FileCacheTest, MetadataLoadFirstExceptionPropagates)
{
    fs::path keyDir;
    {
        auto cache = makeCache();
        cache->initialize();
        const auto key = FileCacheKey::random();
        fullyDownload(*cache, key, 4096);
        auto infos = cache->getFileSegmentInfos(key, origin_.user_id);
        ASSERT_EQ(infos.size(), 1u);
        keyDir = fs::path(infos[0].path).parent_path();
        cache->deactivateBackgroundOperations();
    }

    // Inject a sibling "key" directory whose name is not a valid 32-char key.
    // loadMetadataForKey calls FileCacheKey::fromKeyString on the directory name,
    // which throws; loadMetadataImpl captures the first exception and rethrows it
    // after joining the workers, so initialize propagates it.
    const fs::path badKeyDir = keyDir.parent_path() / "not_a_valid_key";
    fs::create_directories(badKeyDir);
    {
        std::ofstream seg((badKeyDir / "0_16").string(), std::ios::binary | std::ios::trunc);
        const std::string bytes(16, 'z');
        seg.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    auto reloaded = makeCache({}, "reloaded-badkey");
    EXPECT_ANY_THROW(reloaded->initialize());
    EXPECT_FALSE(reloaded->isInitialized());
}

// -- two caches sharing one worker pool reload concurrently without either
//    shrinking the other's pool -------------------------------------------

TEST_F(FileCacheTest, TwoCachesShareWorkerPoolConcurrentReload)
{
    constexpr uint64_t per_cache_load_threads = 3;
    // The shared pool is sized to the aggregate budget of both caches (mirroring
    // the Task-013 manager). Neither cache may resize it.
    constexpr size_t aggregate_budget = 2 * per_cache_load_threads;

    FileCacheWorkerPool sharedPool{aggregate_budget, 1, "shared"};
    FileCacheScheduler sharedScheduler{timekeeper_, sharedPool};

    auto dirA = TempDirectoryPath::create();
    auto dirB = TempDirectoryPath::create();
    const std::string pathA = dirA->getPath();
    const std::string pathB = dirB->getPath();

    std::vector<FileCacheKey> keysA;
    std::vector<FileCacheKey> keysB;
    auto populate = [&](const std::string & path, std::vector<FileCacheKey> & keys)
    {
        auto cache = makeCacheWithServices(
            sharedScheduler, sharedPool,
            [&](FileCacheConfig & c)
            {
                c.path = path;
                c.loadMetadataThreads = per_cache_load_threads;
                c.backgroundDownloadThreads = 0;
            },
            "populate");
        cache->initialize();
        for (int i = 0; i < 4; ++i)
        {
            const auto key = FileCacheKey::random();
            fullyDownload(*cache, key, 4096);
            keys.push_back(key);
        }
        cache->deactivateBackgroundOperations();
    };
    populate(pathA, keysA);
    populate(pathB, keysB);

    // Reload both caches concurrently on the shared pool, started together via a
    // barrier so their listing/loading workers overlap.
    std::barrier sync(2);
    auto reloadOn = [&](const std::string & path) -> std::unique_ptr<FileCache>
    {
        auto cache = makeCacheWithServices(
            sharedScheduler, sharedPool,
            [&](FileCacheConfig & c)
            {
                c.path = path;
                c.loadMetadataThreads = per_cache_load_threads;
                c.backgroundDownloadThreads = 0;
            },
            "reload");
        sync.arrive_and_wait();
        cache->initialize();
        return cache;
    };

    std::unique_ptr<FileCache> cacheA;
    std::unique_ptr<FileCache> cacheB;
    std::promise<void> doneA;
    std::promise<void> doneB;
    auto futureA = doneA.get_future();
    auto futureB = doneB.get_future();
    std::thread ta([&] { cacheA = reloadOn(pathA); doneA.set_value(); });
    std::thread tb([&] { cacheB = reloadOn(pathB); doneB.set_value(); });

    ASSERT_EQ(futureA.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "cache A reload did not complete (a shrunk shared pool would deadlock)";
    ASSERT_EQ(futureB.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "cache B reload did not complete (a shrunk shared pool would deadlock)";
    ta.join();
    tb.join();

    // The shared pool was never resized by either cache.
    EXPECT_EQ(sharedPool.numThreads(), aggregate_budget);

    for (const auto & key : keysA)
        EXPECT_EQ(cacheA->getFileSegmentInfos(key, origin_.user_id).size(), 1u);
    for (const auto & key : keysB)
        EXPECT_EQ(cacheB->getFileSegmentInfos(key, origin_.user_id).size(), 1u);

    cacheA->deactivateBackgroundOperations();
    cacheB->deactivateBackgroundOperations();
}

// -- B5: SCC-owned queue-pipeline call shapes --------------------------------
//
// Exercises the exact FileCacheBoundedQueue call shapes used by FileCache.cpp's
// eviction/load pipelines and proves this binary catches regressions in those
// shapes.  The non-blocking tryPop is made observably necessary: replacing it
// with a blocking pop (the false-green mutation) hangs the test because the
// queue is not yet finished at that point and no further pushes occur.
TEST(FileCacheBoundedQueueTest, SccQueuePipelineCallShapes)
{
    // (1) Small bounded capacity, matching the bounded eviction queues in
    //     FileCache.cpp:1626-1627 (FileCacheBoundedQueue<EvictionBatchPtr>).
    FileCacheBoundedQueue<int> q(4);

    // (2) Timed tryPush — FileCache.cpp:1799 shape: tryPush(batch, push_timeout_ms).
    EXPECT_TRUE(q.tryPush(10, /*timeoutMilliseconds*/ 10));
    EXPECT_TRUE(q.tryPush(20, 10));

    // (3) Non-blocking tryPop — FileCache.cpp:1643 finalize_removed(false) shape.
    // Two items were pushed; FIFO ordering must be preserved.
    int val = 0;
    EXPECT_TRUE(q.tryPop(val));
    EXPECT_EQ(val, 10);
    EXPECT_TRUE(q.tryPop(val));
    EXPECT_EQ(val, 20);

    // The queue is now empty; non-blocking tryPop on an empty queue must return
    // false.  This assertion is observably necessary for the false-green mutation:
    // replacing this tryPop with a blocking pop causes the test to hang, because
    // the queue is not yet finished at this point and no further pushes are made
    // (a blocking pop on an unfinished empty queue waits indefinitely).
    EXPECT_FALSE(q.tryPop(val));

    // (4) Blocking pop + finish() wake/drain.
    // Matches pop() at FileCache.cpp:1691 and FileCache.cpp:2250, and the
    // finish() calls at FileCache.cpp:1720, 1831, 1833, 2241, 2328.
    // A second queue isolates this path from the timed/non-blocking tests above.
    FileCacheBoundedQueue<int> q2(4);

    std::promise<bool> pop_result;
    auto pop_future = pop_result.get_future();

    // One-shot latch: the consumer signals it is about to call pop(), and the
    // main thread waits on it before calling finish().  This guarantees that
    // finish() runs only after the consumer has entered pop() — proving the
    // wake-up path, not a finish-before-pop ordering.
    std::latch consumer_at_pop(1);

    std::thread consumer([&]
    {
        int item = 0;
        // Signal main that we are immediately about to call pop().
        consumer_at_pop.count_down();
        // Blocking pop on an empty queue; blocked until finish() wakes it.
        pop_result.set_value(q2.pop(item));
    });

    // Wait until the consumer has reached pop(), then wake it via finish().
    consumer_at_pop.wait();
    // finish() wakes the blocked pop; the queue is empty and marked done,
    // so pop() must return false (drain-and-finish semantics).
    q2.finish();

    ASSERT_EQ(pop_future.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "blocking pop did not return within 5 s after finish()";
    EXPECT_FALSE(pop_future.get());
    consumer.join();
}

} // namespace
} // namespace facebook::velox::ch
