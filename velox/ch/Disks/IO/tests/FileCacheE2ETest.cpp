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

#include "velox/ch/Disks/IO/tests/FileCacheTestHelpers.h"

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/PositionProvider.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ManualTimekeeper.h>

#include <gtest/gtest.h>

#include <random>

namespace facebook::velox::ch
{
namespace
{

using velox::common::testutil::ScopedTestValue;
using velox::common::testutil::TestValue;
using test::CountingReadFile;
using test::DirectIoReadFile;
using test::FileCacheTestOptions;
using test::makeDeterministicData;
using test::makeInput;
using test::makeManager;
using test::readAll;
using test::readN;
using test::spinUntil;
using test::TempDirectoryPath;

const char * const kFailPoint =
    "facebook::velox::ch::filecache::failpoint::cache_filesystem_failure";

class FileCacheE2ETest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        TestValue::enable();
        filesystems::registerLocalFileSystem();
    }

    void SetUp() override
    {
        pool_ = memory::deprecatedAddDefaultLeafMemoryPool("e2e");
        tempDir_ = TempDirectoryPath::create();
        fileSystem_ = filesystems::getFileSystem(tempDir_->getPath(), {});
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
        timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
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

    void initManager(const FileCacheTestOptions & opts = {})
    {
        manager_ = makeManager(
            tempDir_->getPath(), pool_.get(), fileSystem_, timekeeper_, opts);
        FileCacheManager::setInstance(manager_.get());
        cache_ = manager_->getDefault();
        ASSERT_NE(cache_, nullptr);
    }

    std::unique_ptr<FileCacheBufferedInput> input(
        std::shared_ptr<ReadFile> source,
        FileCacheKey key,
        FileCacheReadOptions opts = {},
        const std::string & queryId = "q")
    {
        return makeInput(
            *manager_, cache_, std::move(source), std::move(key),
            pool_.get(), executor_.get(), std::move(opts), queryId);
    }

    std::string commonUser() const { return "e2e-user"; }

    std::shared_ptr<memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> tempDir_;
    std::shared_ptr<filesystems::FileSystem> fileSystem_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<folly::Timekeeper> timekeeper_;
    std::shared_ptr<FileCacheManager> manager_;
    FileCachePtr cache_;
};

// ===========================================================================
// MissFillHit
// ===========================================================================
TEST_F(FileCacheE2ETest, MissFillHit)
{
    initManager();
    const auto data = makeDeterministicData(256 * 1024);
    const auto key = FileCacheKey::fromPath("miss-fill-hit");

    // First read: cold miss -> fill.
    auto source1 = std::make_shared<CountingReadFile>(data);
    auto inp1 = input(source1, key);
    auto stream1 = inp1->enqueue({0, 128 * 1024});
    inp1->load(dwio::common::LogType::STREAM);
    auto bytes1 = readAll(*stream1);
    ASSERT_EQ(bytes1.size(), 128u * 1024);
    EXPECT_EQ(bytes1, std::vector<char>(data.begin(), data.begin() + 128 * 1024));
    EXPECT_GT(source1->preadCalls(), 0u);

    // Second read: cache hit — no remote I/O.
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto inp2 = input(source2, key);
    EXPECT_TRUE(inp2->isBuffered(0, 128 * 1024));
    auto stream2 = inp2->read(0, 128 * 1024, dwio::common::LogType::STREAM);
    auto bytes2 = readAll(*stream2);
    EXPECT_EQ(bytes2, bytes1);
    EXPECT_EQ(source2->preadCalls(), 0u);
}

// ===========================================================================
// CacheOnlyMissFails
// ===========================================================================
TEST_F(FileCacheE2ETest, CacheOnlyMissFails)
{
    initManager();
    const auto data = makeDeterministicData(1024);
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.tempCacheOnly = true;
    auto inp = input(source, FileCacheKey::fromPath("cache-only-miss"), opts);
    auto stream = inp->read(0, 1024, dwio::common::LogType::STREAM);
    const void * buf = nullptr;
    int size = 0;
    EXPECT_THROW(stream->Next(&buf, &size), VeloxException);
}

// ===========================================================================
// ReadIfExistsBypassMode
// ===========================================================================
TEST_F(FileCacheE2ETest, ReadIfExistsBypassMode)
{
    initManager();
    const auto data = makeDeterministicData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    FileCacheReadOptions opts;
    opts.readIfExistsOtherwiseBypass = true;
    auto key = FileCacheKey::fromPath("bypass-mode");
    auto inp = input(source, key, opts);
    auto stream = inp->read(0, data.size(), dwio::common::LogType::STREAM);
    auto bytes = readAll(*stream);
    EXPECT_EQ(bytes, data);
    // No segment should remain in the cache.
    EXPECT_TRUE(cache_->getFileSegmentInfos(commonUser()).empty());
}

// ===========================================================================
// BackUpWithinOutputBuffer
// ===========================================================================
TEST_F(FileCacheE2ETest, BackUpWithinOutputBuffer)
{
    initManager();
    const auto data = makeDeterministicData(128 * 1024);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("backup-test");
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 64 * 1024;
    auto inp = input(source, key, opts);
    auto stream = inp->read(0, data.size(), dwio::common::LogType::STREAM);

    // Read 64 KiB.
    auto chunk = readN(*stream, 64 * 1024);
    ASSERT_EQ(chunk.size(), 64u * 1024);

    // BackUp 1024 bytes.
    stream->BackUp(1024);
    EXPECT_EQ(stream->ByteCount(), 64 * 1024 - 1024);

    // Re-read those 1024 bytes.
    auto reread = readN(*stream, 1024);
    std::vector<char> expected(data.begin() + 64 * 1024 - 1024, data.begin() + 64 * 1024);
    EXPECT_EQ(reread, expected);
}

// ===========================================================================
// SkipAcrossSegmentBoundary
// ===========================================================================
TEST_F(FileCacheE2ETest, SkipAcrossSegmentBoundary)
{
    FileCacheTestOptions testOpts;
    testOpts.maxFileSegmentSize = 64 * 1024;
    initManager(testOpts);
    const auto data = makeDeterministicData(512 * 1024);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("skip-boundary");
    auto inp = input(source, key);
    auto stream = inp->enqueue({0, 512 * 1024});
    inp->load(dwio::common::LogType::STREAM);

    // Consume first segment worth.
    auto first = readN(*stream, 64 * 1024);
    EXPECT_EQ(first, std::vector<char>(data.begin(), data.begin() + 64 * 1024));

    // Skip past the first segment boundary.
    EXPECT_TRUE(stream->SkipInt64(64 * 1024));

    // Next() must return data at the correct absolute offset (128 KiB).
    auto after = readN(*stream, 1024);
    std::vector<char> expected(data.begin() + 128 * 1024, data.begin() + 128 * 1024 + 1024);
    EXPECT_EQ(after, expected);
}

// ===========================================================================
// SeekToPositionRegionRelative
// ===========================================================================
TEST_F(FileCacheE2ETest, SeekToPositionRegionRelative)
{
    initManager();
    const auto data = makeDeterministicData(64 * 1024);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("seek-region-relative");
    auto inp = input(source, key);
    auto stream = inp->enqueue({4096, 8192});
    inp->load(dwio::common::LogType::STREAM);

    // Seek to region-relative position 256.
    std::vector<uint64_t> seekPositions{256};
    dwio::common::PositionProvider provider(seekPositions);
    stream->seekToPosition(provider);
    EXPECT_EQ(stream->ByteCount(), 256);

    // Next() must return data starting at absolute offset 4096 + 256.
    auto chunk = readN(*stream, 512);
    std::vector<char> expected(data.begin() + 4096 + 256, data.begin() + 4096 + 256 + 512);
    EXPECT_EQ(chunk, expected);
}

// ===========================================================================
// NonzeroRegionOffsetAbsoluteCoordinates
// ===========================================================================
TEST_F(FileCacheE2ETest, NonzeroRegionOffsetAbsoluteCoordinates)
{
    initManager();
    const auto data = makeDeterministicData(128 * 1024);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("nonzero-region");
    auto inp = input(source, key);
    auto stream = inp->enqueue({65536, 32768});
    inp->load(dwio::common::LogType::STREAM);
    auto bytes = readAll(*stream);
    ASSERT_EQ(bytes.size(), 32768u);
    std::vector<char> expected(data.begin() + 65536, data.begin() + 65536 + 32768);
    EXPECT_EQ(bytes, expected);
}

// ===========================================================================
// DiscardedEnqueueNoUseAfterFree
// ===========================================================================
TEST_F(FileCacheE2ETest, DiscardedEnqueueNoUseAfterFree)
{
    initManager();
    const auto data = makeDeterministicData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("discard-enqueue");
    auto inp = input(source, key);
    { auto stream = inp->enqueue({0, 2048}); }
    EXPECT_NO_THROW(inp->load(dwio::common::LogType::STREAM));
}

// ===========================================================================
// LoadIsNopPlanningBarrier
// ===========================================================================
TEST_F(FileCacheE2ETest, LoadIsNopPlanningBarrier)
{
    initManager();
    const auto data = makeDeterministicData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("load-nop");
    auto inp = input(source, key);
    { auto s1 = inp->enqueue({0, 1024}); }
    { auto s2 = inp->enqueue({1024, 1024}); }
    { auto s3 = inp->enqueue({2048, 1024}); }
    EXPECT_NO_THROW(inp->load(dwio::common::LogType::STREAM));
}

// ===========================================================================
// DWRFShouldPrefetchStripesIsFalse
// ===========================================================================
TEST_F(FileCacheE2ETest, DWRFShouldPrefetchStripesIsFalse)
{
    initManager();
    const auto data = makeDeterministicData(64);
    auto source = std::make_shared<CountingReadFile>(data);
    auto inp = input(source, FileCacheKey::fromPath("prefetch-false"));
    EXPECT_FALSE(inp->shouldPrefetchStripes());
    EXPECT_FALSE(inp->preloaded());
}

// ===========================================================================
// PathOnlyKeyWhenEtagEmpty
// ===========================================================================
TEST_F(FileCacheE2ETest, PathOnlyKeyWhenEtagEmpty)
{
    initManager();
    const std::string path = "/data/file.parquet";
    const auto data = makeDeterministicData(2048);

    // Empty etag must produce the same key as FileCacheKey::fromPath.
    auto key = FileCacheFileIdentity::deriveKey({path, ""});
    EXPECT_EQ(key, FileCacheKey::fromPath(path));

    auto source1 = std::make_shared<CountingReadFile>(data);
    auto inp1 = input(source1, key);
    auto stream1 = inp1->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream1), data);

    // Re-derive key with same path + empty etag; must hit cache.
    auto key2 = FileCacheFileIdentity::deriveKey({path, ""});
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto inp2 = input(source2, key2);
    auto stream2 = inp2->read(0, data.size(), dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream2), data);
    EXPECT_EQ(source2->preadCalls(), 0u);
}

// ===========================================================================
// DifferentEtagsDifferentKeys
// ===========================================================================
TEST_F(FileCacheE2ETest, DifferentEtagsDifferentKeys)
{
    initManager();
    const std::string path = "/data/file.parquet";
    const auto data1 = makeDeterministicData(2048);
    auto data2 = data1;
    // Make data2 distinguishable so we can verify isolated hits.
    data2[0] = static_cast<char>(~data2[0]);

    // Derive keys through production identity so the test exercises deriveKey directly.
    auto key1 = FileCacheFileIdentity::deriveKey({path, "v1"});
    auto key2 = FileCacheFileIdentity::deriveKey({path, "v2"});
    EXPECT_NE(key1, key2);

    // Fill key1.
    auto source1 = std::make_shared<CountingReadFile>(data1);
    auto inp1 = input(source1, key1);
    EXPECT_EQ(readAll(*inp1->read(0, data1.size(), dwio::common::LogType::STREAM)), data1);

    // Fill key2.
    auto source2 = std::make_shared<CountingReadFile>(data2);
    auto inp2 = input(source2, key2);
    EXPECT_EQ(readAll(*inp2->read(0, data2.size(), dwio::common::LogType::STREAM)), data2);

    // Each key hits its own isolated segment.
    auto source3 = std::make_shared<CountingReadFile>(data1);
    auto inp3 = input(source3, key1);
    EXPECT_EQ(readAll(*inp3->read(0, data1.size(), dwio::common::LogType::STREAM)), data1);
    EXPECT_EQ(source3->preadCalls(), 0u);

    auto source4 = std::make_shared<CountingReadFile>(data2);
    auto inp4 = input(source4, key2);
    EXPECT_EQ(readAll(*inp4->read(0, data2.size(), dwio::common::LogType::STREAM)), data2);
    EXPECT_EQ(source4->preadCalls(), 0u);
}

// ===========================================================================
// ShutdownWhileStreamAliveNotReading
// ===========================================================================
TEST_F(FileCacheE2ETest, ShutdownWhileStreamAliveNotReading)
{
    initManager();
    const auto data = makeDeterministicData(4096);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("shutdown-alive");
    auto inp = input(source, key);
    auto stream = inp->enqueue({0, 2048});
    inp->load(dwio::common::LogType::STREAM);

    // Shutdown while the stream is alive but not reading.
    EXPECT_NO_THROW(manager_->shutdown());
    FileCacheManager::setInstance(nullptr);

    // The stream must be allowed to destruct without crash.
    stream.reset();
    inp.reset();
    manager_.reset();
}

// ===========================================================================
// PartialSegmentContinuationAcrossReaders
// ===========================================================================
TEST_F(FileCacheE2ETest, PartialSegmentContinuationAcrossReaders)
{
    FileCacheTestOptions testOpts;
    testOpts.maxFileSegmentSize = 64 * 1024;
    initManager(testOpts);
    const auto data = makeDeterministicData(32 * 1024);
    const auto key = FileCacheKey::fromPath("partial-continuation");

    // Reader 1: read only first 4 KiB then destroy.
    auto source1 = std::make_shared<CountingReadFile>(data);
    {
        FileCacheReadOptions opts;
        opts.remoteFsBufferSize = 4096;
        auto inp1 = input(source1, key, opts, "q1");
        auto stream1 = inp1->read(0, data.size(), dwio::common::LogType::STREAM);
        auto partial = readN(*stream1, 4096);
        EXPECT_EQ(partial, std::vector<char>(data.begin(), data.begin() + 4096));
    }
    const uint64_t source1Bytes = source1->preadBytes();
    EXPECT_GT(source1Bytes, 0u);

    // Reader 2: reads the full region. The first reader's partial download
    // is continued (not restarted from 0).
    auto source2 = std::make_shared<CountingReadFile>(data);
    auto inp2 = input(source2, key, {}, "q2");
    auto stream2 = inp2->read(0, data.size(), dwio::common::LogType::STREAM);
    auto full = readAll(*stream2);
    EXPECT_EQ(full, data);
    // source2's bytes should be less than the full size since source1 served part.
    EXPECT_LT(source2->preadBytes(), data.size());
}

// ===========================================================================
// CacheWriteFailureConfiguredBypassOrPropagate
// ===========================================================================
TEST_F(FileCacheE2ETest, CacheWriteFailureConfiguredBypassOrPropagate)
{
    // Case 1: default (no bypass) -> propagates.
    {
        FileCacheTestOptions testOpts;
        testOpts.skipCacheOnDiskFailure = false;
        initManager(testOpts);
        const auto data = makeDeterministicData(2048);
        auto source = std::make_shared<CountingReadFile>(data);
        auto key = FileCacheKey::fromPath("write-fail-propagate");

        ScopedTestValue armed(
            kFailPoint,
            std::function<void(void *)>([](void *) {
                throw FileCacheErrnoException(
                    __FILE__, __LINE__, __FUNCTION__,
                    "simulated cache disk IO failure", EIO);
            }));

        auto inp = input(source, key);
        auto stream = inp->read(0, data.size(), dwio::common::LogType::STREAM);
        const void * buf = nullptr;
        int size = 0;
        EXPECT_THROW(stream->Next(&buf, &size), std::exception);

        // Cleanup for next case.
        stream.reset();
        inp.reset();
        manager_->shutdown();
        FileCacheManager::setInstance(nullptr);
        manager_.reset();
    }

    // Case 2: bypass configured -> read succeeds.
    {
        tempDir_ = TempDirectoryPath::create();
        fileSystem_ = filesystems::getFileSystem(tempDir_->getPath(), {});
        FileCacheTestOptions testOpts;
        testOpts.skipCacheOnDiskFailure = true;
        initManager(testOpts);
        const auto data = makeDeterministicData(2048);
        auto source = std::make_shared<CountingReadFile>(data);
        auto key = FileCacheKey::fromPath("write-fail-bypass");

        ScopedTestValue armed(
            kFailPoint,
            std::function<void(void *)>([](void *) {
                throw FileCacheErrnoException(
                    __FILE__, __LINE__, __FUNCTION__,
                    "simulated cache disk IO failure", EIO);
            }));

        auto inp = input(source, key);
        auto stream = inp->read(0, data.size(), dwio::common::LogType::STREAM);
        auto bytes = readAll(*stream);
        EXPECT_EQ(bytes, data);

        // After bypass, no FileSegment/cache residue must remain for this key.
        EXPECT_EQ(cache_->getUsedCacheSize(), 0u)
            << "bypass must not leave residual FileSegments in the cache";
        // getFileSegmentInfos(key, user) throws when the key has no metadata
        // entry at all — exactly the condition we want to assert.
        EXPECT_THROW(
            cache_->getFileSegmentInfos(key, commonUser()), VeloxException)
            << "bypass must leave zero metadata entries for this key";
    }
}

// ===========================================================================
// TruncatedOrInvalidCachedDataSourceRecovery
// ===========================================================================
TEST_F(FileCacheE2ETest, TruncatedOrInvalidCachedDataSourceRecovery)
{
    initManager();
    const auto data = makeDeterministicData(8192);
    const auto key = FileCacheKey::fromPath("truncated-recovery");

    // Step 1: Fill cache.
    auto source1 = std::make_shared<CountingReadFile>(data);
    auto inp1 = input(source1, key);
    EXPECT_EQ(readAll(*inp1->read(0, data.size(), dwio::common::LogType::STREAM)), data);

    // Step 2: Invalidate the cached entry using the public API.
    cache_->removeKeyIfExists(key, commonUser());

    // Step 3: Fresh reader detects miss (not stale bytes) and refills.
    auto source3 = std::make_shared<CountingReadFile>(data);
    auto inp3 = input(source3, key);
    auto stream3 = inp3->read(0, data.size(), dwio::common::LogType::STREAM);
    auto bytes3 = readAll(*stream3);
    EXPECT_EQ(bytes3, data);
    EXPECT_GT(source3->preadCalls(), 0u);
}

// ===========================================================================
// ReserveAheadDownloadedSizeAccounting
// ===========================================================================
TEST_F(FileCacheE2ETest, ReserveAheadDownloadedSizeAccounting)
{
    FileCacheTestOptions testOpts;
    testOpts.maxFileSegmentSize = 4096;
    initManager(testOpts);
    const auto data = makeDeterministicData(8192);
    auto source = std::make_shared<CountingReadFile>(data);
    auto key = FileCacheKey::fromPath("reserve-accounting");
    auto inp = input(source, key);
    EXPECT_EQ(readAll(*inp->read(0, data.size(), dwio::common::LogType::STREAM)), data);

    EXPECT_EQ(cache_->getUsedCacheSize(), data.size());
    // 8192 / 4096 = 2 segments.
    EXPECT_EQ(cache_->getFileSegmentsNum(), 2u);
}

// ===========================================================================
// RandomSeeksAcrossHitMissBypassPaths
// ===========================================================================
TEST_F(FileCacheE2ETest, RandomSeeksAcrossHitMissBypassPaths)
{
    initManager();
    const size_t fileSize = 256 * 1024;
    const auto data = makeDeterministicData(fileSize);
    const auto key = FileCacheKey::fromPath("random-seeks");

    // Fixed-seed PRNG for deterministic offsets.
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<size_t> offsetDist(0, fileSize - 1024);
    std::uniform_int_distribution<size_t> lenDist(64, 1024);

    struct SeekSpec
    {
        size_t offset;
        size_t length;
    };
    std::vector<SeekSpec> specs;
    for (int i = 0; i < 60; ++i)
    {
        SeekSpec s;
        s.offset = offsetDist(rng);
        s.length = lenDist(rng);
        if (s.offset + s.length > fileSize)
            s.length = fileSize - s.offset;
        specs.push_back(s);
    }

    bool sawHit = false;
    bool sawMiss = false;

    // Execute all 60 pairs, alternating mode by index:
    //   index % 3 == 0: normal cache-eligible read
    //   index % 3 == 1: tempCacheOnly on fresh uncached key (expect throw)
    //   index % 3 == 2: bypass on fresh uncached key (expect correct bytes)
    for (int i = 0; i < 60; ++i)
    {
        const auto & spec = specs[i];
        const int mode = i % 3;

        if (mode == 0)
        {
            // Normal cache-eligible read (may hit or miss).
            auto source = std::make_shared<CountingReadFile>(data);
            auto inp = input(source, key);
            auto stream = inp->read(spec.offset, spec.length, dwio::common::LogType::STREAM);
            auto bytes = readAll(*stream);
            std::vector<char> expected(data.begin() + spec.offset,
                                       data.begin() + spec.offset + spec.length);
            EXPECT_EQ(bytes, expected) << "normal read failed at index " << i;
            if (source->preadCalls() == 0)
                sawHit = true;
            else
                sawMiss = true;
        }
        else if (mode == 1)
        {
            // tempCacheOnly on a fresh uncached key — expect throw.
            auto source = std::make_shared<CountingReadFile>(data);
            FileCacheReadOptions opts;
            opts.tempCacheOnly = true;
            auto freshKey = FileCacheKey::fromPath(
                "random-seeks-cacheonly-" + std::to_string(i));
            auto inp = input(source, freshKey, opts);
            auto stream = inp->read(spec.offset, spec.length, dwio::common::LogType::STREAM);
            const void * buf = nullptr;
            int size = 0;
            EXPECT_THROW(stream->Next(&buf, &size), VeloxException)
                << "tempCacheOnly on uncached key must throw at index " << i;
        }
        else
        {
            // Bypass on a fresh uncached key — expect correct bytes.
            auto source = std::make_shared<CountingReadFile>(data);
            FileCacheReadOptions opts;
            opts.readIfExistsOtherwiseBypass = true;
            auto freshKey = FileCacheKey::fromPath(
                "random-seeks-bypass-" + std::to_string(i));
            auto inp = input(source, freshKey, opts);
            auto stream = inp->read(spec.offset, spec.length, dwio::common::LogType::STREAM);
            auto bytes = readAll(*stream);
            std::vector<char> expected(data.begin() + spec.offset,
                                       data.begin() + spec.offset + spec.length);
            EXPECT_EQ(bytes, expected) << "bypass read failed at index " << i;
        }
    }

    EXPECT_TRUE(sawMiss) << "at least one cache miss required in normal reads";

    // Prove at least one hit by re-reading an offset that was already filled
    // during the normal-mode iterations above.
    {
        // Find the first normal-mode spec (index 0).
        const auto & spec = specs[0];
        auto source = std::make_shared<CountingReadFile>(data);
        auto inp = input(source, key);
        auto stream = inp->read(spec.offset, spec.length, dwio::common::LogType::STREAM);
        auto bytes = readAll(*stream);
        std::vector<char> expected(data.begin() + spec.offset,
                                   data.begin() + spec.offset + spec.length);
        EXPECT_EQ(bytes, expected);
        if (source->preadCalls() == 0)
            sawHit = true;
    }
    EXPECT_TRUE(sawHit) << "at least one cache hit required (re-read of filled offset)";
}

// ===========================================================================
// DirectIoSourceBackgroundDownloadCompletes (B1)
// ===========================================================================
TEST_F(FileCacheE2ETest, DirectIoSourceBackgroundDownloadCompletes)
{
    FileCacheTestOptions testOpts;
    testOpts.maxFileSegmentSize = 16384;
    testOpts.backgroundDownloadThreads = 2;
    initManager(testOpts);

    // Use a file whose total size is a full alignment multiple so background
    // can download everything (no unaligned tail to skip).
    const size_t kAlignment = 4096;
    const size_t fileSize = 12288; // 3 * 4096, fully aligned
    const auto data = makeDeterministicData(fileSize);
    const auto key = FileCacheKey::fromPath("b1-direct-io-bg");

    auto source = std::make_shared<DirectIoReadFile>(data, kAlignment);

    // Q1 reads first alignment unit, then hands off to background.
    {
        FileCacheReadOptions opts;
        opts.remoteFsBufferSize = kAlignment;
        auto inp = input(source, key, opts, "q1");
        auto stream = inp->read(0, fileSize, dwio::common::LogType::STREAM);
        auto chunk = readN(*stream, kAlignment);
        EXPECT_EQ(chunk, std::vector<char>(data.begin(), data.begin() + kAlignment));
    }

    // Wait for background to complete the segment (all remaining bytes are
    // alignment-multiple, so background downloads them fully).
    auto segmentDownloaded = [&]()
    {
        const auto infos = cache_->getFileSegmentInfos(key, commonUser());
        for (const auto & info : infos)
        {
            if (info.state == FileSegment::State::DOWNLOADED)
                return true;
        }
        return false;
    };
    ASSERT_TRUE(spinUntil(segmentDownloaded, std::chrono::seconds(20)))
        << "background download must complete for a fully-aligned direct-IO source";

    // A fresh DirectIoReadFile proves full cache hit with zero remote I/O.
    auto source2 = std::make_shared<DirectIoReadFile>(data, kAlignment);
    auto inp2 = input(source2, key, {}, "q2");
    auto stream2 = inp2->read(0, fileSize, dwio::common::LogType::STREAM);
    EXPECT_EQ(readAll(*stream2), data);
    EXPECT_EQ(source2->preadCalls(), 0u);
}

// ===========================================================================
// DirectIoUnalignedTailSkipsBackgroundDownload (B1)
// ===========================================================================
TEST_F(FileCacheE2ETest, DirectIoUnalignedTailSkipsBackgroundDownload)
{
    FileCacheTestOptions testOpts;
    // Single segment covers the entire file.
    testOpts.maxFileSegmentSize = 16384;
    testOpts.backgroundDownloadThreads = 2;
    initManager(testOpts);

    // alignment=4096, file size = 4096 + 2048 = 6144.
    // Q1 reads the first 4096 bytes.  Remaining for background = 2048 < 4096,
    // so aligned_download_size = 0 and the tail-skip path fires.
    const size_t kAlignment = 4096;
    const size_t fileSize = 4096 + 2048; // 6144
    const auto data = makeDeterministicData(fileSize);
    const auto key = FileCacheKey::fromPath("b1-unaligned-tail");

    auto source = std::make_shared<DirectIoReadFile>(data, kAlignment);
    const uint64_t preadBefore = source->preadCalls();

    // Register TestValue notification to prove the skip path was taken.
    std::atomic<bool> skipFired{false};
    ScopedTestValue skipNotification(
        "facebook::velox::ch::CacheMetadata::downloadImpl::unalignedTailSkip",
        std::function<void(FileSegment *)>([&](FileSegment *) {
            skipFired.store(true);
        }));

    // Q1: read aligned prefix, then hand off.
    {
        FileCacheReadOptions opts;
        opts.remoteFsBufferSize = kAlignment;
        auto inp = input(source, key, opts, "q1");
        auto stream = inp->read(0, fileSize, dwio::common::LogType::STREAM);
        auto chunk = readN(*stream, kAlignment);
        EXPECT_EQ(chunk, std::vector<char>(data.begin(), data.begin() + kAlignment));
    }

    // Wait for the background worker to execute the tail-skip notification.
    ASSERT_TRUE(spinUntil([&]() { return skipFired.load(); }, std::chrono::seconds(20)))
        << "background worker must invoke the unalignedTailSkip notification";

    // No additional source pread calls during background processing.
    EXPECT_EQ(source->preadCalls(), preadBefore + 1u)
        << "background must not issue pread for the skipped tail";

    // After the tail skip, completeAndPopFront shrinks the segment to the
    // downloaded portion [0, 4096) and marks it DOWNLOADED. The remaining
    // [4096, 6144) is not covered by this segment — a foreground read will
    // create a new segment for it.
    auto segReady = [&]()
    {
        const auto infos = cache_->getFileSegmentInfos(key, commonUser());
        if (infos.empty())
            return false;
        // The segment was shrunk to the aligned prefix and completed.
        return infos[0].state == FileSegment::State::DOWNLOADED;
    };
    ASSERT_TRUE(spinUntil(segReady, std::chrono::seconds(5)));
    const auto infos = cache_->getFileSegmentInfos(key, commonUser());
    ASSERT_FALSE(infos.empty());
    EXPECT_EQ(infos[0].downloaded_size, kAlignment)
        << "segment should cover only the aligned prefix that was foreground-downloaded";

    // A fresh DirectIoReadFile foreground read completes the tail through
    // the new aligned-physical / logical-clamp path in nextImpl.
    auto source2 = std::make_shared<DirectIoReadFile>(data, kAlignment);
    auto inp2 = input(source2, key, {}, "q2");
    auto stream2 = inp2->read(0, fileSize, dwio::common::LogType::STREAM);
    auto bytes = readAll(*stream2);
    EXPECT_EQ(bytes, data);
    // The foreground reader must actually perform remote I/O for the uncached
    // tail rather than silently being a full-cache-hit false green.
    EXPECT_GT(source2->preadCalls(), 0u)
        << "foreground direct-IO source must issue pread for the skipped tail";
}

} // namespace
} // namespace facebook::velox::ch
