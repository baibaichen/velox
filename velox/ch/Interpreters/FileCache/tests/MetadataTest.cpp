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
#include "velox/ch/Interpreters/FileCache/Metadata.h"

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentKeyType.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <folly/futures/ManualTimekeeper.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;

// Spin (not sleep) until `pred` holds or the deadline passes. Used to observe
// asynchronous cleanup-thread progress deterministically without a fixed delay.
bool waitFor(const std::function<bool()> & pred, std::chrono::milliseconds timeout)
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

// ===========================================================================
// Part 1: static path-layout contracts (no fixture; getFileNameForFileSegment
// is a pure static). Link with the rest of the SCC.
// ===========================================================================

TEST(PathLayoutTest, RegularDownloadingFilename)
{
    // Downloading segment: filename is just the offset decimal string.
    EXPECT_EQ(
        CacheMetadata::getFileNameForFileSegment(100, FileSegmentKind::Regular, std::nullopt),
        "100");
}

TEST(PathLayoutTest, RegularDownloadedFilename)
{
    // Downloaded segment: "<offset>_<size>".
    EXPECT_EQ(
        CacheMetadata::getFileNameForFileSegment(100, FileSegmentKind::Regular, 512),
        "100_512");
}

TEST(PathLayoutTest, EphemeralFilename)
{
    // Ephemeral segment: "<offset>_temporary" (size never encoded).
    EXPECT_EQ(
        CacheMetadata::getFileNameForFileSegment(0, FileSegmentKind::Ephemeral, std::nullopt),
        "0_temporary");
    EXPECT_EQ(
        CacheMetadata::getFileNameForFileSegment(7, FileSegmentKind::Ephemeral, 999),
        "7_temporary");
}

// ===========================================================================
// Part 2: standalone CacheMetadata fixture.
//
// CacheMetadata is constructed directly through its manager-injected
// constructor (worker pool + memory pool + reserve timeout + opened-file
// invalidation callback + common user id) over a real temp directory. These
// tests exercise the production metadata algorithms (path layout, key state
// machine, KeyNotFoundPolicy, origin dedup, ordered-map range intersection,
// real CleanupQueue/DownloadQueue, worker resize, shutdown) and link once the
// rest of the center SCC (FileCache.cpp, FileSegment.cpp) is present.
// ===========================================================================

class MetadataTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("metadata-test");
        cacheDir_ = TempDirectoryPath::create();
    }

    void TearDown() override
    {
        // Every started CacheMetadata must be shut down before destruction, or
        // ~FileCacheWorker VELOX_CHECK-fails on a still-joinable worker. shutdown
        // is idempotent, so a test that already shut down is unaffected.
        for (auto * md : started_)
            md->shutdown();
        mds_.clear();
    }

    CacheMetadata * makeMetadata(
        size_t download_threads = 0,
        size_t queue_limit = 0,
        bool per_user = false)
    {
        auto md = std::make_unique<CacheMetadata>(
            cacheDir_->getPath(),
            queue_limit,
            download_threads,
            per_user,
            workerPool_,
            pool_.get(),
            /*reserve_space_wait_lock_timeout_ms*/ 1000,
            [this](const std::string & path) { invalidated_.push_back(path); },
            /*common_user_id*/ std::string("common-user"));
        auto * raw = md.get();
        mds_.push_back(std::move(md));
        return raw;
    }

    void start(CacheMetadata * md)
    {
        md->startup();
        started_.push_back(md);
    }

    // Build and insert a real EMPTY FileSegment under `locked` at `offset`.
    // EMPTY segments never dereference the (null) cache pointer for the
    // read-only metadata operations exercised here (range/queue/get).
    FileSegmentPtr addEmptySegment(LockedKey & locked, size_t offset, size_t size)
    {
        auto key_metadata = locked.getKeyMetadata();
        auto segment = std::make_shared<FileSegment>(
            key_metadata->key,
            offset,
            size,
            FileSegment::State::EMPTY,
            CreateFileSegmentSettings{},
            /*background_download_enabled*/ false,
            /*cache*/ nullptr,
            std::weak_ptr<KeyMetadata>(key_metadata),
            /*queue_iterator*/ nullptr);
        FileSegmentPtr forMeta = segment;
        locked.emplace(offset, std::make_shared<FileSegmentMetadata>(std::move(forMeta)));
        return segment;
    }

    FileCacheOriginInfo origin(const std::string & user = "user1", uint64_t weight = 100, FileSegmentKeyType type = FileSegmentKeyType::General) const
    {
        return FileCacheOriginInfo(user, weight, type);
    }

    FileCacheWorkerPool workerPool_{16, 1, "meta-test"};
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> cacheDir_;
    std::vector<std::unique_ptr<CacheMetadata>> mds_;
    std::vector<CacheMetadata *> started_;
    std::vector<std::string> invalidated_;
};

// -- path layout / access ---------------------------------------------------

TEST_F(MetadataTest, KeyPathWithoutPerUserDirectory)
{
    auto * md = makeMetadata(/*download_threads*/ 0, /*queue_limit*/ 0, /*per_user*/ false);
    auto o = origin("alice", 100, FileSegmentKeyType::General);
    const auto key = FileCacheKey::random();
    const auto key_str = key.toString();

    // General type prefix is "" (cache-path invariant): base / key[0:3] / key.
    const fs::path expected = fs::path(cacheDir_->getPath()) / getKeyTypePrefix(FileSegmentKeyType::General)
        / key_str.substr(0, 3) / key_str;
    EXPECT_EQ(md->getKeyPath(key, o), expected.string());
}

TEST_F(MetadataTest, KeyPathWithPerUserDirectory)
{
    auto * md = makeMetadata(0, 0, /*per_user*/ true);
    auto o = origin("bob", 42, FileSegmentKeyType::General);
    const auto key = FileCacheKey::random();
    const auto key_str = key.toString();

    // With per-user directories the "<user>.<weight>" component is inserted.
    const fs::path expected = fs::path(cacheDir_->getPath()) / getKeyTypePrefix(FileSegmentKeyType::General)
        / "bob.42" / key_str.substr(0, 3) / key_str;
    EXPECT_EQ(md->getKeyPath(key, o), expected.string());
}

TEST_F(MetadataTest, KeyPathSegmentTypePrefixes)
{
    auto * md = makeMetadata();
    const auto key = FileCacheKey::random();
    const auto key_str = key.toString();

    for (auto type : {FileSegmentKeyType::General, FileSegmentKeyType::System, FileSegmentKeyType::Data})
    {
        auto o = origin("carol", 1, type);
        const fs::path expected = fs::path(cacheDir_->getPath()) / getKeyTypePrefix(type)
            / key_str.substr(0, 3) / key_str;
        EXPECT_EQ(md->getKeyPath(key, o), expected.string());
    }
}

TEST_F(MetadataTest, FileSegmentPathCombinesKeyPathAndFileName)
{
    auto * md = makeMetadata();
    auto o = origin("dave", 7, FileSegmentKeyType::General);
    const auto key = FileCacheKey::random();

    const fs::path expected_downloading = fs::path(md->getKeyPath(key, o)) / "128";
    EXPECT_EQ(md->getFileSegmentPath(key, 128, FileSegmentKind::Regular, o, std::nullopt), expected_downloading.string());

    const fs::path expected_downloaded = fs::path(md->getKeyPath(key, o)) / "128_64";
    EXPECT_EQ(md->getFileSegmentPath(key, 128, FileSegmentKind::Regular, o, 64), expected_downloaded.string());
}

TEST_F(MetadataTest, CheckAccessAllowsOwnerAndInternalDeniesOther)
{
    auto * md = makeMetadata();
    auto o = origin("erin", 5, FileSegmentKeyType::General);
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_TRUE(locked);
    auto key_metadata = locked->getKeyMetadata();

    EXPECT_TRUE(key_metadata->checkAccess("erin"));
    EXPECT_TRUE(key_metadata->checkAccess(FileCache::getInternalOrigin().user_id));
    EXPECT_FALSE(key_metadata->checkAccess("frank"));

    EXPECT_NO_THROW(key_metadata->assertAccess("erin"));
    EXPECT_THROW(key_metadata->assertAccess("frank"), velox::VeloxRuntimeError);
}

// -- KeyNotFoundPolicy (four behaviors) -------------------------------------

TEST_F(MetadataTest, KeyNotFoundPolicyThrow)
{
    auto * md = makeMetadata();
    auto o = origin();
    const auto key = FileCacheKey::random();

    // Missing key + THROW: production call throws the expected Velox exception.
    EXPECT_THROW(
        md->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, o),
        velox::VeloxRuntimeError);
    EXPECT_THROW(
        md->lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, o),
        velox::VeloxRuntimeError);
}

TEST_F(MetadataTest, KeyNotFoundPolicyThrowLogical)
{
    auto * md = makeMetadata();
    EXPECT_THROW(
        md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL, origin()),
        velox::VeloxRuntimeError);
}

TEST_F(MetadataTest, KeyNotFoundPolicyReturnNull)
{
    auto * md = makeMetadata();
    EXPECT_EQ(md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, origin()), nullptr);
    EXPECT_EQ(md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, origin()), nullptr);
}

TEST_F(MetadataTest, KeyNotFoundPolicyCreateEmpty)
{
    auto * md = makeMetadata();
    const auto key = FileCacheKey::random();
    auto km = md->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin());
    ASSERT_NE(km, nullptr);
    // The key now exists and is found without CREATE_EMPTY.
    EXPECT_NE(md->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, origin()), nullptr);
    EXPECT_FALSE(md->isEmpty());
}

// -- key state machine + delayed cleanup ------------------------------------

TEST_F(MetadataTest, EmptyLockedKeyReleaseTransitionsToRemovingAndEnqueues)
{
    auto * md = makeMetadata();
    auto o = origin();
    const auto key = FileCacheKey::random();

    {
        auto locked = md->lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
        ASSERT_TRUE(locked);
        EXPECT_EQ(locked->getKeyState(), KeyMetadata::KeyState::ACTIVE);
        // Releasing an empty ACTIVE locked key submits the key for delayed removal.
    }

    // The key is still in the bucket, now in REMOVING state (queued for cleanup).
    auto km = md->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, o);
    ASSERT_NE(km, nullptr);
    EXPECT_EQ(km->getState(), KeyMetadata::KeyState::REMOVING);
}

TEST_F(MetadataTest, RemovingKeyReactivatedByCreateEmpty)
{
    auto * md = makeMetadata();
    auto o = origin();
    const auto key = FileCacheKey::random();

    KeyMetadata * original = nullptr;
    {
        auto locked = md->lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
        ASSERT_TRUE(locked);
        original = locked->getKeyMetadata().get();
    }
    // Now REMOVING.
    ASSERT_EQ(md->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, o)->getState(),
              KeyMetadata::KeyState::REMOVING);

    // CREATE_EMPTY cancels the delayed removal, restores ACTIVE, and returns the
    // same KeyMetadata instance (not a fresh one).
    auto revived = md->lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_TRUE(revived);
    EXPECT_EQ(revived->getKeyState(), KeyMetadata::KeyState::ACTIVE);
    EXPECT_EQ(revived->getKeyMetadata().get(), original);
}

TEST_F(MetadataTest, RemoveKeyMissingThrowsUnlessIfExists)
{
    auto * md = makeMetadata();
    const auto key = FileCacheKey::random();
    EXPECT_NO_THROW(md->removeKey(key, /*if_exists*/ true, "user1"));
    EXPECT_THROW(md->removeKey(key, /*if_exists*/ false, "user1"), velox::VeloxRuntimeError);
}

TEST_F(MetadataTest, IsEmptyInitiallyTrue)
{
    auto * md = makeMetadata();
    EXPECT_TRUE(md->isEmpty());
}

// -- origin dedup (SD1: shared immutable origins, no reference escape) -------

TEST_F(MetadataTest, OriginDedupSameKeyShared)
{
    auto * md = makeMetadata();
    auto o = origin("shared-user", 100, FileSegmentKeyType::General);

    auto km1 = md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    auto km2 = md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_NE(km1, nullptr);
    ASSERT_NE(km2, nullptr);

    // Two keys with the same OriginPoolKey share one deduplicated origin instance.
    EXPECT_EQ(km1->origin.get(), km2->origin.get());
}

TEST_F(MetadataTest, OriginDedupDistinctForDifferentWeightOrType)
{
    auto * md = makeMetadata();
    auto base = origin("u", 100, FileSegmentKeyType::General);
    auto other_weight = origin("u", 200, FileSegmentKeyType::General);
    auto other_type = origin("u", 100, FileSegmentKeyType::System);

    auto a = md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, base);
    auto b = md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, other_weight);
    auto c = md->getKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, other_type);

    EXPECT_NE(a->origin.get(), b->origin.get());
    EXPECT_NE(a->origin.get(), c->origin.get());
    EXPECT_NE(b->origin.get(), c->origin.get());
}

// -- ordered std::map range intersection ------------------------------------

TEST_F(MetadataTest, RangeIntersectionUsesOrderedOffsets)
{
    auto * md = makeMetadata();
    auto o = origin();
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_TRUE(locked);

    // Two non-adjacent segments: [0, 9] and [20, 29].
    addEmptySegment(*locked, /*offset*/ 0, /*size*/ 10);
    addEmptySegment(*locked, /*offset*/ 20, /*size*/ 10);

    // A range that overlaps the second segment finds it (ordered lower_bound).
    auto hit = locked->hasIntersectingRange(FileSegment::Range{25, 35});
    ASSERT_TRUE(hit.has_value());
    EXPECT_EQ(hit->left, 20u);
    EXPECT_EQ(hit->right, 29u);

    // A range that overlaps the first (previous) segment finds it.
    auto hit_prev = locked->hasIntersectingRange(FileSegment::Range{5, 15});
    ASSERT_TRUE(hit_prev.has_value());
    EXPECT_EQ(hit_prev->left, 0u);

    // A range in the gap [10, 19] intersects nothing.
    EXPECT_FALSE(locked->hasIntersectingRange(FileSegment::Range{12, 18}).has_value());

    // toString lists offsets in ascending order (ordered map).
    EXPECT_EQ(locked->toString(), "0, 20");
}

TEST_F(MetadataTest, GetByOffsetAndTryGetByOffset)
{
    auto * md = makeMetadata();
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin());
    ASSERT_TRUE(locked);
    addEmptySegment(*locked, 0, 10);

    EXPECT_NE(locked->getByOffset(0), nullptr);
    EXPECT_NE(locked->tryGetByOffset(0), nullptr);
    EXPECT_EQ(locked->tryGetByOffset(999), nullptr);
    EXPECT_THROW(locked->getByOffset(999), velox::VeloxRuntimeError);
}

// -- real DownloadQueue: bounded capacity + weak_ptr identity ---------------

TEST_F(MetadataTest, DownloadQueueEnforcesLimit)
{
    // Queue limited to 2 elements.
    auto * md = makeMetadata(/*download_threads*/ 0, /*queue_limit*/ 2);
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin());
    ASSERT_TRUE(locked);
    auto segment = addEmptySegment(*locked, /*offset*/ 0, /*size*/ 10);

    // The std::queue-backed DownloadQueue does not deduplicate, so the same
    // segment can be enqueued until the bounded capacity is reached.
    EXPECT_TRUE(locked->addToDownloadQueue(0, segment->lock()));
    EXPECT_TRUE(locked->addToDownloadQueue(0, segment->lock()));
    EXPECT_FALSE(locked->addToDownloadQueue(0, segment->lock()));
}

TEST_F(MetadataTest, DownloadInfoWeakPtrExpiresAfterSegmentReset)
{
    auto * md = makeMetadata();
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin());
    ASSERT_TRUE(locked);
    auto segment = addEmptySegment(*locked, 0, 10);

    // DownloadInfo carries a weak_ptr so a background worker can distinguish the
    // original segment from a new one created at the same key/offset after the
    // original was deleted. Once the original is gone, the weak pointer expires.
    DownloadInfo info{segment->key(), segment->offset(), segment};
    EXPECT_FALSE(info.segment.expired());
    EXPECT_EQ(info.segment.lock().get(), segment.get());

    // Drop both the local handle and the metadata's copy: the weak pointer must
    // then observe the segment as gone.
    ASSERT_EQ(locked->removeFileSegmentIfExists(0, /*can_be_broken*/ true), locked->end());
    segment.reset();
    EXPECT_TRUE(info.segment.expired());
}

// -- iterators --------------------------------------------------------------

TEST_F(MetadataTest, IteratorVisitsEverySegmentOnce)
{
    auto * md = makeMetadata();
    auto o = origin();
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_TRUE(locked);
    addEmptySegment(*locked, 0, 10);
    addEmptySegment(*locked, 20, 10);
    locked.reset();

    auto it = md->getIterator("user1");
    size_t count = 0;
    while (it->next([&](const FileSegmentInfo &) { ++count; }))
    {
    }
    EXPECT_EQ(count, 2u);
}

TEST_F(MetadataTest, BatchedIteratorVisitsEverySegment)
{
    auto * md = makeMetadata();
    auto o = origin();
    auto locked = md->lockKeyMetadata(FileCacheKey::random(), CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, o);
    ASSERT_TRUE(locked);
    addEmptySegment(*locked, 0, 10);
    addEmptySegment(*locked, 20, 10);
    locked.reset();

    auto it = md->getIterator("user1");
    size_t count = 0;
    while (it->nextBatch([&](const FileSegmentInfo &) { ++count; }))
    {
    }
    EXPECT_EQ(count, 2u);
}

// -- background download worker resize --------------------------------------

TEST_F(MetadataTest, BackgroundDownloadDisabledWithZeroThreads)
{
    auto * md = makeMetadata(/*download_threads*/ 0);
    EXPECT_FALSE(md->isBackgroundDownloadEnabled());
    EXPECT_EQ(md->getBackgroundDownloadThreads(), 0u);
}

TEST_F(MetadataTest, WorkerGrowAndShrink)
{
    auto * md = makeMetadata(/*download_threads*/ 2);
    EXPECT_TRUE(md->isBackgroundDownloadEnabled());
    start(md);
    EXPECT_EQ(md->getBackgroundDownloadThreads(), 2u);

    // Grow.
    EXPECT_TRUE(md->setBackgroundDownloadThreads(4));
    EXPECT_EQ(md->getBackgroundDownloadThreads(), 4u);

    // Shrink: surplus workers get their stop flag set under the queue mutex, are
    // woken by notify_all, joined, and erased.
    EXPECT_TRUE(md->setBackgroundDownloadThreads(1));
    EXPECT_EQ(md->getBackgroundDownloadThreads(), 1u);

    // No-op resize returns false.
    EXPECT_FALSE(md->setBackgroundDownloadThreads(1));

    md->shutdown();
}

// -- shutdown cancels both queues before joining (observable order, no sleep) -

TEST_F(MetadataTest, ShutdownWakesAndJoinsBlockedWorkers)
{
    // Two download workers plus the cleanup worker all park in a blocking pop on
    // an empty queue.
    auto * md = makeMetadata(/*download_threads*/ 2);
    start(md);

    // shutdown() must cancel both queues (notify_all) BEFORE joining, otherwise
    // the join would hang on the parked workers. Observe the exact order via the
    // afterCancelBeforeJoin / afterJoin checkpoints and require completion under a
    // bounded future: a join-first implementation blocks on an un-woken worker and
    // never records either checkpoint.
    facebook::velox::common::testutil::TestValue::enable();
    std::mutex order_mutex;
    std::vector<std::string> order;
    facebook::velox::common::testutil::ScopedTestValue afterCancel(
        "facebook::velox::ch::CacheMetadata::shutdown::afterCancelBeforeJoin",
        std::function<void(void *)>([&](void *) { std::lock_guard<std::mutex> g(order_mutex); order.push_back("cancel"); }));
    facebook::velox::common::testutil::ScopedTestValue afterJoin(
        "facebook::velox::ch::CacheMetadata::shutdown::afterJoin",
        std::function<void(void *)>([&](void *) { std::lock_guard<std::mutex> g(order_mutex); order.push_back("join"); }));

    auto done = std::async(std::launch::async, [md]() { md->shutdown(); });
    ASSERT_EQ(done.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    done.get();

    {
        std::lock_guard<std::mutex> g(order_mutex);
        ASSERT_EQ(order.size(), 2u);
        EXPECT_EQ(order[0], "cancel");
        EXPECT_EQ(order[1], "join");
    }

    // Idempotent: TearDown's shutdown() is a no-op after this.
    EXPECT_EQ(md->getBackgroundDownloadThreads(), 2u);
}

TEST_F(MetadataTest, DelayedCleanupRemovesEmptyKey)
{
    // Cleanup worker running; no download workers needed.
    auto * md = makeMetadata(/*download_threads*/ 0);
    start(md);

    const auto key = FileCacheKey::random();
    {
        auto locked = md->lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin());
        ASSERT_TRUE(locked);
        // Releasing the empty ACTIVE key enqueues it for delayed cleanup.
    }

    // The cleanup worker dequeues the REMOVING key and erases it from metadata.
    EXPECT_TRUE(waitFor([&] { return md->isEmpty(); }, std::chrono::seconds(10)));

    md->shutdown();
}

// ===========================================================================
// Part 3: production-path fixture over a real temporary FileCache.
//
// These drive LockedKey::sync and CacheMetadata::removeAllKeys through the
// production FileCache, which owns the CacheMetadata and injects the same
// opened-file invalidation callback. They construct a real cache through the
// manager-injected constructor and link once FileCache.cpp exists.
// ===========================================================================

class TestBackedWriteFile : public velox::WriteFile
{
public:
    TestBackedWriteFile(std::string path, bool append) : path_(std::move(path))
    {
        if (append)
        {
            out_.open(path_, std::ios::binary | std::ios::out | std::ios::app);
            std::error_code ec;
            const auto existing = fs::file_size(path_, ec);
            if (!ec)
                written_ = existing;
        }
        else
        {
            std::error_code ec;
            if (fs::exists(path_, ec))
                VELOX_FAIL("TestBackedWriteFile: create-new for an existing path: {}", path_);
            out_.open(path_, std::ios::binary | std::ios::out | std::ios::trunc);
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

class MetadataFileCacheTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("metadata-fc-test");
        cacheDir_ = TempDirectoryPath::create();

        FileCacheConfig config;
        config.path = cacheDir_->getPath();
        config.maxSize = 64ull << 20;
        config.maxFileSegmentSize = 1ull << 20;
        config.boundaryAlignment = 4096;
        config.reserveGranularity = 0;
        // The 8-thread pool must fit the metadata-load workers (FileCache no longer
        // resizes the shared pool, it verifies capacity fail-closed).
        config.loadMetadataThreads = 2;

        cache_ = std::make_unique<FileCache>(
            "meta-test",
            config,
            scheduler_,
            workerPool_,
            pool_.get(),
            origin_,
            [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
            { return std::make_unique<TestBackedWriteFile>(path, append); },
            [](const std::string &) -> std::shared_ptr<velox::ReadFile> { return nullptr; },
            [this](const std::string & path) { invalidated_.push_back(path); });
        cache_->initialize();
    }

    void TearDown() override
    {
        if (cache_)
        {
            cache_->deactivateBackgroundOperations();
            cache_.reset();
        }
    }

    // Fully download a regular segment [0, size) and leave it cached and
    // releasable (holder released). Returns the on-disk path of the DOWNLOADED
    // file (`<offset>_<size>`).
    std::string fullyDownload(const FileCacheKey & key, size_t size)
    {
        std::string path;
        {
            auto holder = cache_->getOrSet(key, /*offset*/ 0, size, /*file_size*/ size, CreateFileSegmentSettings{}, /*file_segments_limit*/ 0, origin_);
            auto segment = holder->getSingleFileSegment();
            EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
            std::string failure_reason;
            EXPECT_TRUE(segment->reserve(size, /*lock_wait_timeout_ms*/ 1000, failure_reason));
            std::string data(size, 'z');
            segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());
            segment->resetDownloader();
            EXPECT_EQ(segment->state(), FileSegment::State::DOWNLOADED);
            path = segment->getPath();
        }
        return path;
    }

    std::shared_ptr<folly::ManualTimekeeper> timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    FileCacheWorkerPool workerPool_{8, 1, "meta-fc"};
    FileCacheScheduler scheduler_{timekeeper_, workerPool_};
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> cacheDir_;
    FileCacheOriginInfo origin_{"meta-user", 0};
    std::unique_ptr<FileCache> cache_;
    std::vector<std::string> invalidated_;
};

TEST_F(MetadataFileCacheTest, SyncRemovesMissingLocalSegment)
{
    const auto key = FileCacheKey::random();
    const auto path = fullyDownload(key, 64);
    ASSERT_TRUE(fs::exists(path));
    ASSERT_EQ(cache_->getFileSegmentInfos(key, origin_.user_id).size(), 1u);

    // Externally delete the cache file: sync must detect the DOWNLOADED segment
    // whose file is gone, report it broken, and remove it from metadata.
    fs::remove(path);

    auto broken = cache_->sync();
    EXPECT_EQ(broken.size(), 1u);
    EXPECT_TRUE(cache_->getFileSegmentInfos(origin_.user_id).empty());
}

TEST_F(MetadataFileCacheTest, SyncRemovesWrongSizeLocalSegmentAndInvalidatesOpenedFile)
{
    const auto key = FileCacheKey::random();
    const auto path = fullyDownload(key, 64);
    ASSERT_TRUE(fs::exists(path));

    // Grow the on-disk file so its size no longer matches the recorded size.
    {
        std::ofstream out(path, std::ios::binary | std::ios::out | std::ios::app);
        const std::string extra(8, 'x');
        out.write(extra.data(), static_cast<std::streamsize>(extra.size()));
    }
    ASSERT_EQ(fs::file_size(path), 72u);

    auto broken = cache_->sync();
    EXPECT_EQ(broken.size(), 1u);
    EXPECT_TRUE(cache_->getFileSegmentInfos(origin_.user_id).empty());

    // Removing the wrong-size file invalidates the manager-owned opened-file
    // cache for that path (injected callback), and physically removes the file.
    EXPECT_FALSE(fs::exists(path));
    EXPECT_NE(std::find(invalidated_.begin(), invalidated_.end(), path), invalidated_.end());
}

TEST_F(MetadataFileCacheTest, RemoveAllReleasableKeepsHeldSegment)
{
    const auto key = FileCacheKey::random();

    auto holder = cache_->getOrSet(key, /*offset*/ 0, /*size*/ 64, /*file_size*/ 64, CreateFileSegmentSettings{}, /*file_segments_limit*/ 0, origin_);
    ASSERT_EQ(holder->size(), 1u);

    // While the holder is alive the (non-releasable) segment survives a purge.
    cache_->removeAllReleasable(origin_.user_id);
    EXPECT_EQ(cache_->getFileSegmentInfos(key, origin_.user_id).size(), 1u);

    // Once released, the same purge removes it.
    holder.reset();
    cache_->removeAllReleasable(origin_.user_id);
    EXPECT_TRUE(cache_->getFileSegmentInfos(origin_.user_id).empty());
}

} // namespace
} // namespace facebook::velox::ch
