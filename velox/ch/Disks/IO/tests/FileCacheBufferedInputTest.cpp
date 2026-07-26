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
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/file/tests/FaultyFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/StreamIdentifier.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include <gtest/gtest.h>

#include <unistd.h>

#include <cerrno>
#include <cstring>
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

std::string readAll(dwio::common::SeekableInputStream & stream)
{
    std::string out;
    const void * data = nullptr;
    int32_t size = 0;
    while (stream.Next(&data, &size))
        out.append(static_cast<const char *>(data), static_cast<size_t>(size));
    return out;
}

std::string readN(dwio::common::SeekableInputStream & stream, size_t n)
{
    std::string out;
    const void * data = nullptr;
    int32_t size = 0;
    while (out.size() < n && stream.Next(&data, &size))
    {
        const size_t take = std::min<size_t>(n - out.size(), static_cast<size_t>(size));
        out.append(static_cast<const char *>(data), take);
        if (take < static_cast<size_t>(size))
            stream.BackUp(static_cast<int32_t>(static_cast<size_t>(size) - take));
    }
    return out;
}

/// A `velox::ReadFile` that delegates to a `LocalReadFile` but records the
/// `FileIoContext` passed to each `pread`. Used to assert that FileCache source
/// reads route through the base `ReadFileInputStream` so `pread` receives the
/// populated context (ioStats identity + fileOpts + cacheable) the owning
/// `BufferedInput` built, rather than a bare empty context. Thread-safe because
/// the warm path reads on an executor thread.
class CapturingReadFile : public velox::ReadFile
{
public:
    explicit CapturingReadFile(const std::string & path)
        : inner_(std::make_shared<velox::LocalReadFile>(path))
    {
    }

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const velox::FileIoContext & context = {}) const override
    {
        record(context);
        return inner_->pread(offset, length, buf, context);
    }

    std::string pread(
        uint64_t offset,
        uint64_t length,
        const velox::FileIoContext & context = {}) const override
    {
        record(context);
        return inner_->velox::ReadFile::pread(offset, length, context);
    }

    bool shouldCoalesce() const override { return inner_->shouldCoalesce(); }
    uint64_t size() const override { return inner_->size(); }
    uint64_t memoryUsage() const override { return inner_->memoryUsage(); }
    std::string getName() const override { return inner_->getName(); }
    uint64_t getNaturalReadSize() const override { return inner_->getNaturalReadSize(); }

    struct Captured
    {
        bool seen = false;
        const velox::IoStats * ioStats = nullptr;
        folly::F14FastMap<std::string, std::string> fileOpts;
        bool cacheable = false;
    };

    Captured captured() const
    {
        std::lock_guard<std::mutex> g(mutex_);
        return captured_;
    }

private:
    void record(const velox::FileIoContext & context) const
    {
        std::lock_guard<std::mutex> g(mutex_);
        captured_.seen = true;
        captured_.ioStats = context.ioStats;
        captured_.fileOpts = context.fileOpts;
        captured_.cacheable = context.cacheable;
    }

    std::shared_ptr<velox::LocalReadFile> inner_;
    mutable std::mutex mutex_;
    mutable Captured captured_;
};

class FileCacheBufferedInputTest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { test::localFileSystemForTests(); }

    void SetUp() override
    {
        pool_ = memoryManager_.addLeafPool("filecache-buffered-input-test");
        temp_ = TempDirectoryPath::create();
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
    }

    std::string sub(const std::string & s) const
    {
        return test::subPath(temp_->getPath(), s);
    }

    // Write a source file with deterministic content and return its path.
    std::string writeSourceFile(const std::string & name, const std::string & content)
    {
        return test::writeSourceFile(temp_->getPath(), name, content);
    }

    FileCacheSettings settings(size_t seg)
    {
        FileCacheSettings s;
        s.path = sub("cache");
        s.maxSize = 16 * 1024 * 1024;
        s.maxElements = 100;
        s.maxFileSegmentSize = seg;
        s.boundaryAlignment = 1;
        s.reserveGranularity = 1;
        s.cachePolicy = FileCachePolicy::LRU;
        s.useSplitCache = false;
        s.backgroundDownloadThreads = 0;
        s.loadMetadataThreads = 2;
        s.loadMetadataAsynchronously = false;
        s.keepFreeSpaceSizeRatio = 0.0;
        s.keepFreeSpaceElementsRatio = 0.0;
        return s;
    }

    std::shared_ptr<FileCache> makeCache(const std::string & name, size_t seg)
    {
        std::shared_ptr<FileCache> cache = res_.makeFileCache(name, settings(seg), "user-A");
        cache->initialize();
        return cache;
    }

    dwio::common::ReaderOptions readerOptions()
    {
        dwio::common::ReaderOptions o(pool_.get());
        if (loadQuantum_ != 0)
        {
            o.setLoadQuantum(loadQuantum_);
        }
        return o;
    }

    // Build a FileCacheBufferedInput over `sourcePath` backed by `cache`.
    // Single construction point for the FileCacheBufferedInput used across these
    // tests. The variants below differ only in the source ReadFile, the tracker,
    // the ioStatistics and the read options; everything else is fixed.
    std::unique_ptr<FileCacheBufferedInput> makeInputImpl(
        std::shared_ptr<FileCache> cache,
        std::shared_ptr<velox::ReadFile> readFile,
        const FileCacheKey & key,
        std::shared_ptr<velox::cache::ScanTracker> tracker,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        FileCacheReadOptions readOptions)
    {
        FileCacheRequestContext ctx;
        ctx.queryId = "q1";
        ctx.userId = "user-A";
        auto origin = cache->getCommonOrigin();
        return std::make_unique<FileCacheBufferedInput>(
            std::move(readFile),
            std::move(cache),
            key,
            origin,
            readOptions,
            ctx,
            QueryStatus{},
            dwio::common::MetricsLog::voidLog(),
            velox::StringIdLease{},
            velox::StringIdLease{},
            std::move(tracker),
            ioStatistics ? std::move(ioStatistics) : std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get(),
            readerOptions());
    }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        std::shared_ptr<FileCache> cache,
        const std::string & sourcePath,
        const FileCacheKey & key,
        FileCacheReadOptions readOptions = {})
    {
        return makeInputImpl(
            std::move(cache), std::make_shared<velox::LocalReadFile>(sourcePath), key,
            /*tracker=*/nullptr, /*ioStatistics=*/nullptr, readOptions);
    }

    // Build a FileCacheBufferedInput over a CapturingReadFile with a KNOWN
    // ioStats, non-empty fileReadOps, and cacheable=true, so a test can assert
    // the FileIoContext that reaches ReadFile::pread on the source read paths.
    struct CapturingBuild
    {
        std::unique_ptr<FileCacheBufferedInput> input;
        std::shared_ptr<CapturingReadFile> readFile;
        std::shared_ptr<velox::IoStats> ioStats;
        folly::F14FastMap<std::string, std::string> fileReadOps;
        bool cacheable = true;
    };

    CapturingBuild makeCapturingInput(
        std::shared_ptr<FileCache> cache,
        const std::string & sourcePath,
        const FileCacheKey & key)
    {
        CapturingBuild b;
        b.readFile = std::make_shared<CapturingReadFile>(sourcePath);
        b.ioStats = std::make_shared<velox::IoStats>();
        b.fileReadOps = {{"opt-k", "opt-v"}};
        b.cacheable = true;

        FileCacheRequestContext ctx;
        ctx.queryId = "q1";
        ctx.userId = "user-A";
        ctx.cacheable = b.cacheable;
        auto origin = cache->getCommonOrigin();
        b.input = std::make_unique<FileCacheBufferedInput>(
            b.readFile,
            std::move(cache),
            key,
            origin,
            FileCacheReadOptions{},
            ctx,
            QueryStatus{},
            dwio::common::MetricsLog::voidLog(),
            velox::StringIdLease{},
            velox::StringIdLease{},
            /*tracker=*/nullptr,
            std::make_shared<io::IoStatistics>(),
            b.ioStats,
            executor_.get(),
            readerOptions(),
            b.fileReadOps);
        return b;
    }

    // Build a FileCacheBufferedInput over an explicit source ReadFile (e.g. a
    // FaultyReadFile) backed by `cache`.
    std::unique_ptr<FileCacheBufferedInput> makeInputWithReadFile(
        std::shared_ptr<FileCache> cache,
        std::shared_ptr<velox::ReadFile> readFile,
        const FileCacheKey & key,
        FileCacheReadOptions readOptions = {})
    {
        return makeInputImpl(
            std::move(cache), std::move(readFile), key,
            /*tracker=*/nullptr, /*ioStatistics=*/nullptr, readOptions);
    }

    // Build a FileCacheBufferedInput over `sourcePath` with an explicit
    // ScanTracker installed, so a test can observe recordRead (delivered bytes)
    // on the ScanTracker while also reading IoStatistics for physical bytes.
    std::unique_ptr<FileCacheBufferedInput> makeInputWithTracker(
        std::shared_ptr<FileCache> cache,
        const std::string & sourcePath,
        const FileCacheKey & key,
        std::shared_ptr<velox::cache::ScanTracker> tracker,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        FileCacheReadOptions readOptions = {})
    {
        return makeInputImpl(
            std::move(cache), std::make_shared<velox::LocalReadFile>(sourcePath), key,
            std::move(tracker), std::move(ioStatistics), readOptions);
    }

    std::unique_ptr<dwio::common::DirectBufferedInput> makeDirectInputWithTracker(
        const std::string & sourcePath,
        std::shared_ptr<velox::cache::ScanTracker> tracker)
    {
        return std::make_unique<dwio::common::DirectBufferedInput>(
            std::make_shared<velox::LocalReadFile>(sourcePath),
            dwio::common::MetricsLog::voidLog(),
            velox::StringIdLease{},
            std::move(tracker),
            velox::StringIdLease{},
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get(),
            readerOptions());
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    test::FileCacheTestResources res_;
    // When non-zero, readerOptions() applies this as the loadQuantum. 0 keeps the
    // io::ReaderOptions default (used by every pre-existing test).
    int32_t loadQuantum_ = 0;
};

// ============================ key derivation ============================

TEST_F(FileCacheBufferedInputTest, EmptyEtagUsesPathKey)
{
    FileCacheFileIdentity id{"/path/to/file", ""};
    EXPECT_EQ(FileCacheFileIdentity::deriveKey(id), FileCacheKey::fromPath("/path/to/file"));
}

TEST_F(FileCacheBufferedInputTest, DifferentEtagsDifferentKeys)
{
    FileCacheFileIdentity a{"/path/to/file", "etag-1"};
    FileCacheFileIdentity b{"/path/to/file", "etag-2"};
    auto ka = FileCacheFileIdentity::deriveKey(a);
    auto kb = FileCacheFileIdentity::deriveKey(b);
    EXPECT_NE(ka, kb);
    // Non-empty etag differs from the path-only key.
    EXPECT_NE(ka, FileCacheKey::fromPath("/path/to/file"));
}

// ======================= BufferedInput contract =======================

TEST_F(FileCacheBufferedInputTest, DwioContractFlags)
{
    auto cache = makeCache("dwio", 16);
    auto path = writeSourceFile("src", std::string(64, 'a'));
    auto input = makeInput(cache, path, FileCacheKey::random());

    EXPECT_FALSE(input->shouldPrefetchStripes());
    EXPECT_FALSE(input->preloaded());
    EXPECT_FALSE(input->shouldPreload());
    EXPECT_FALSE(input->hasCache());
    EXPECT_EQ(input->executor(), executor_.get());
    // A4: preload() performs a whole-file in-memory preload; preloaded() flips
    // to true and the file's bytes are now served from RAM.
    input->preload();
    EXPECT_TRUE(input->preloaded());
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, PreloadedStreamTracksReferenceAndReadLikeDirect)
{
    const uint64_t n = 64;
    const std::string content(n, 'a');
    const auto path = writeSourceFile("preload_tracking_src", content);
    const dwio::common::StreamIdentifier sid{17};
    const velox::cache::TrackingId trackingId{sid.getId()};

    auto directTracker = std::make_shared<velox::cache::ScanTracker>();
    auto directInput = makeDirectInputWithTracker(path, directTracker);
    directInput->preload();
    auto directStream = directInput->enqueue({0, n}, &sid);
    EXPECT_EQ(readAll(*directStream), content);

    const auto directTracking = directTracker->trackingData(trackingId);
    ASSERT_EQ(directTracking.referencedBytes, n);
    ASSERT_EQ(directTracking.readBytes, n);

    auto cache = makeCache("preload_tracking", n);
    auto fileCacheTracker = std::make_shared<velox::cache::ScanTracker>();
    auto fileCacheInput = makeInputWithTracker(
        cache,
        path,
        FileCacheKey::random(),
        fileCacheTracker,
        std::make_shared<io::IoStatistics>());
    fileCacheInput->preload();
    auto fileCacheStream = fileCacheInput->enqueue({0, n}, &sid);
    EXPECT_EQ(readAll(*fileCacheStream), content);

    const auto fileCacheTracking = fileCacheTracker->trackingData(trackingId);
    EXPECT_EQ(fileCacheTracking.referencedBytes, directTracking.referencedBytes);
    EXPECT_EQ(fileCacheTracking.readBytes, directTracking.readBytes);
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, BackedUpBytesAreRecordedAgainLikeDirect)
{
    const uint64_t n = 64;
    const int32_t backedUp = 16;
    const std::string content(n, 'a');
    const auto path = writeSourceFile("backup_tracking_src", content);
    const dwio::common::StreamIdentifier sid{18};
    const velox::cache::TrackingId trackingId{sid.getId()};

    auto consumeWithBackUp = [&](dwio::common::SeekableInputStream & stream)
    {
        const void * data = nullptr;
        int32_t size = 0;
        ASSERT_TRUE(stream.Next(&data, &size));
        ASSERT_EQ(size, n);
        const std::string expectedTail(static_cast<const char *>(data) + size - backedUp, backedUp);

        stream.BackUp(backedUp);
        ASSERT_TRUE(stream.Next(&data, &size));
        ASSERT_EQ(size, backedUp);
        EXPECT_EQ(std::string(static_cast<const char *>(data), size), expectedTail);
    };

    auto directTracker = std::make_shared<velox::cache::ScanTracker>();
    auto directInput = makeDirectInputWithTracker(path, directTracker);
    auto directStream = directInput->enqueue({0, n}, &sid);
    consumeWithBackUp(*directStream);

    const auto directTracking = directTracker->trackingData(trackingId);
    ASSERT_EQ(directTracking.referencedBytes, n);
    ASSERT_EQ(directTracking.readBytes, n + backedUp);

    auto cache = makeCache("backup_tracking", n);
    auto fileCacheTracker = std::make_shared<velox::cache::ScanTracker>();
    auto fileCacheInput = makeInputWithTracker(
        cache,
        path,
        FileCacheKey::random(),
        fileCacheTracker,
        std::make_shared<io::IoStatistics>());
    auto fileCacheStream = fileCacheInput->enqueue({0, n}, &sid);
    consumeWithBackUp(*fileCacheStream);

    const auto fileCacheTracking = fileCacheTracker->trackingData(trackingId);
    EXPECT_EQ(fileCacheTracking.referencedBytes, directTracking.referencedBytes);
    EXPECT_EQ(fileCacheTracking.readBytes, directTracking.readBytes);
    cache->deactivateBackgroundOperations();
}

// Design 6.4: makePreloadedStream must reject an offset/length whose sum
// overflows uint64. With the old `offset + length` check, offset near
// UINT64_MAX wraps the sum to a small value that passes the range check, then
// the real huge offset is used for pointer arithmetic / Allocation::findRun and
// reads out of bounds. The fixed two-step check (offset <= size, then
// length <= size - offset) throws instead.
TEST_F(FileCacheBufferedInputTest, PreloadedReadOffsetOverflowThrows)
{
    auto cache = makeCache("preload_ovf", 16);
    auto path = writeSourceFile("src", std::string(64, 'a'));
    auto input = makeInput(cache, path, FileCacheKey::random());

    input->preload();
    ASSERT_TRUE(input->preloaded());

    // offset near UINT64_MAX, length chosen so offset + length wraps to a small
    // value (< preload size). Old code: 9 <= 64 => passes => uses huge offset.
    const uint64_t hugeOffset = std::numeric_limits<uint64_t>::max() - 10;
    const uint64_t length = 20; // hugeOffset + 20 wraps to 9
    EXPECT_THROW(
        input->read(hugeOffset, length, dwio::common::LogType::FILE),
        VeloxException);

    cache->deactivateBackgroundOperations();
}

// Design 6.4: a preloaded FileCacheInputStream's seekToPosition must reject a
// target position beyond the stream's region length. The old code assigned
// position_ directly; an illegal large position then made start_ + position_ and
// findRun read out of bounds on the next Next(). The fixed check throws.
TEST_F(FileCacheBufferedInputTest, PreloadedSeekToPositionOutOfRangeThrows)
{
    auto cache = makeCache("preload_seek", 16);
    const uint64_t n = 64;
    auto path = writeSourceFile("src", std::string(n, 'a'));
    auto input = makeInput(cache, path, FileCacheKey::random());

    input->preload();
    ASSERT_TRUE(input->preloaded());

    // A preloaded stream over a small region; seeking past its length must throw.
    auto stream = input->read(0, n, dwio::common::LogType::FILE);
    std::vector<uint64_t> positions{std::numeric_limits<uint64_t>::max() - 5};
    dwio::common::PositionProvider provider(positions);
    EXPECT_THROW(stream->seekToPosition(provider), VeloxException);

    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, EnqueueResultDiscardedBeforeLoadNoCreate)
{
    auto cache = makeCache("lazy", 16);
    auto path = writeSourceFile("src", std::string(64, 'a'));
    auto key = FileCacheKey::random();
    auto input = makeInput(cache, path, key);

    // Discard the enqueue result BEFORE load; load must not dereference it or
    // create a segment.
    { auto stream = input->enqueue({0, 32}); }
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
    input->load(dwio::common::LogType::FILE);
    // Load creates no FileSegment (lazy Next semantics).
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, IsBufferedColdMissNoCreate)
{
    auto cache = makeCache("probe", 16);
    auto path = writeSourceFile("src", std::string(64, 'a'));
    auto input = makeInput(cache, path, FileCacheKey::random());

    EXPECT_FALSE(input->isBuffered(0, 32));
    // No metadata/segments created by the probe.
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
    cache->deactivateBackgroundOperations();
}

// ======================= miss / hit / bypass =======================

std::string makeContent(size_t n)
{
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i)
        s[i] = static_cast<char>('A' + (i % 26));
    return s;
}

TEST_F(FileCacheBufferedInputTest, MissThenHitFillsCache)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("misshit", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // First stream: miss -> reads remote, fills cache.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    EXPECT_GT(cache->getFileSegmentsNum(), 0u);

    // Second stream: should be fully cached now (served from disk cache).
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

// §11.9 (C3): on a demand read the last held segment is TRIMMED to the region's
// remaining length before handing bytes to the caller. The physical read (a
// cache HIT reads the whole downloaded segment) must be attributed as ACTUAL
// bytes to IoStatistics (raw + ssdRead for a hit), while the ScanTracker records
// only the DELIVERED (trimmed) bytes. Pre-fix code attributed the trimmed size
// to IoStatistics too, under-counting physical cache IO.
TEST_F(FileCacheBufferedInputTest, DemandTrimAccountsPhysicalRead)
{
    // One segment of 4096 bytes covering the whole file, so a HIT read pulls the
    // full 4096-byte segment physically, but the region only needs 100 bytes.
    const size_t segSize = 4096;
    const size_t fileSize = segSize;
    const size_t delivered = 100;
    auto content = makeContent(fileSize);
    auto cache = makeCache("trim_phys", segSize);
    auto path = writeSourceFile("src_trim_phys", content);
    auto key = FileCacheKey::random();

    // First pass: cold miss over the whole file to fully populate the segment.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, fileSize, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    ASSERT_GT(cache->getFileSegmentsNum(), 0u);

    // Second pass: cache HIT, but the region asks for only `delivered` bytes.
    auto tracker = std::make_shared<velox::cache::ScanTracker>();
    auto io = std::make_shared<io::IoStatistics>();
    auto input = makeInputWithTracker(cache, path, key, tracker, io);

    const velox::dwio::common::StreamIdentifier sid{7};
    const velox::cache::TrackingId id{sid.getId()};
    auto stream = input->enqueue({0, delivered}, &sid);
    ASSERT_EQ(
        tracker->trackingData(id).referencedBytes, static_cast<double>(delivered));

    // Read exactly the delivered region.
    EXPECT_EQ(readN(*stream, delivered), content.substr(0, delivered));

    // ScanTracker: delivered (trimmed) bytes only.
    EXPECT_EQ(tracker->trackingData(id).readBytes, static_cast<double>(delivered));

    // IoStatistics: the ACTUAL physical cache read (whole 4096-byte segment), a
    // hit -> ssdRead + raw bytes. It must be the full segment, NOT the trimmed
    // 100 bytes. The whole point of §11.9: actual (4096) != delivered (100).
    EXPECT_EQ(io->ssdRead().sum(), static_cast<uint64_t>(segSize));
    EXPECT_EQ(io->rawBytesRead(), static_cast<uint64_t>(segSize));
    // A hit does not touch the source `read()` bucket.
    EXPECT_EQ(io->read().sum(), 0u);

    cache->deactivateBackgroundOperations();
}

// §11.9 (C3), SOURCE (miss) branch counterpart of DemandTrimAccountsPhysicalRead.
// On a COLD cache miss the download readType REMOTE_FS_READ_AND_PUT_IN_CACHE bounds
// the source reader to the SEGMENT right boundary via
// setReadUntilPosition(min(range.right + 1, fileSize)) (FileCacheInputStream.cpp
// :531-532), so a request whose region ends strictly inside a larger segment
// physically reads the WHOLE segment from the source. actualBytes therefore equals
// the full segment size, while deliveredBytes is trimmed to the region length
// (:876-887). The source read must be attributed as ACTUAL bytes: read() (source
// bucket) + rawBytesRead over the physical read (:920 / base
// ReadFileInputStream::read), while the ScanTracker records only DELIVERED bytes
// (:927). boundaryAlignment == segSize forces getOrSet to align the created
// segment up to a full [0, 4095] segment (FileCache.cpp :822-826), so a cold
// {0,100} request downloads all 4096 bytes (actual) but delivers only 100
// (trimmed). No hit -> ssdRead stays 0.
TEST_F(FileCacheBufferedInputTest, DemandTrimAccountsPhysicalSourceRead)
{
    // boundaryAlignment == segSize is the KEY: getOrSet aligns the created
    // segment range up to the alignment (FileCache.cpp :822-826), so a {0,100}
    // request yields a full [0, 4095] segment, not a [0, 99] one. With
    // boundaryAlignment == 1 (the default) the segment would match the request
    // exactly and actual would equal delivered (100) -- no over-read to observe.
    const size_t segSize = 4096;
    const size_t fileSize = segSize;
    const size_t delivered = 100;
    auto content = makeContent(fileSize);
    auto s = settings(segSize);
    s.boundaryAlignment = segSize;
    std::shared_ptr<FileCache> cache =
        res_.makeFileCache("trim_phys_src", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src_trim_phys_src", content);
    auto key = FileCacheKey::random();

    // COLD cache: no prepopulation. A miss drives the source read + download path.
    auto tracker = std::make_shared<velox::cache::ScanTracker>();
    auto io = std::make_shared<io::IoStatistics>();
    auto input = makeInputWithTracker(cache, path, key, tracker, io);

    const velox::dwio::common::StreamIdentifier sid{9};
    const velox::cache::TrackingId id{sid.getId()};
    auto stream = input->enqueue({0, delivered}, &sid);
    ASSERT_EQ(
        tracker->trackingData(id).referencedBytes, static_cast<double>(delivered));

    // Read exactly the delivered region.
    EXPECT_EQ(readN(*stream, delivered), content.substr(0, delivered));

    // ScanTracker: delivered (trimmed) bytes only.
    EXPECT_EQ(tracker->trackingData(id).readBytes, static_cast<double>(delivered));

    // IoStatistics: the ACTUAL physical SOURCE read (whole 4096-byte segment) is a
    // miss -> read() (source bucket) + raw bytes, NOT the trimmed 100. The whole
    // point of §11.9 on the miss branch: actual (4096) != delivered (100).
    EXPECT_EQ(io->read().sum(), static_cast<uint64_t>(segSize));
    EXPECT_EQ(io->rawBytesRead(), static_cast<uint64_t>(segSize));
    // A miss does not touch the local-cache hit `ssdRead()` bucket.
    EXPECT_EQ(io->ssdRead().sum(), 0u);

    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, DemandSourceReadCarriesFileIoContext)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("ctx_demand", 16);
    auto path = writeSourceFile("src_ctx_demand", content);
    auto key = FileCacheKey::random();

    auto b = makeCapturingInput(cache, path, key);
    auto stream = b.input->read(0, n, dwio::common::LogType::FILE);
    EXPECT_EQ(readAll(*stream), content);

    const auto cap = b.readFile->captured();
    ASSERT_TRUE(cap.seen);
    EXPECT_EQ(cap.ioStats, b.ioStats.get());
    EXPECT_EQ(cap.fileOpts, b.fileReadOps);
    EXPECT_EQ(cap.cacheable, b.cacheable);

    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, WarmSourceReadCarriesFileIoContext)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("ctx_warm", 16);
    auto path = writeSourceFile("src_ctx_warm", content);
    auto key = FileCacheKey::random();

    auto b = makeCapturingInput(cache, path, key);
    // Empty trackingId classifies as prefetch, so load() submits the coalesced
    // load to the executor. join() waits for the source read to finish before
    // cache deactivation.
    b.input->enqueue({0, n});
    b.input->load(dwio::common::LogType::FILE);
    executor_->join();

    const auto cap = b.readFile->captured();
    ASSERT_TRUE(cap.seen);
    EXPECT_EQ(cap.ioStats, b.ioStats.get());
    EXPECT_EQ(cap.fileOpts, b.fileReadOps);
    EXPECT_EQ(cap.cacheable, b.cacheable);
    cache->deactivateBackgroundOperations();
}

// C2 review (11.3.1 external reviewer): routing warm source reads through the base
// ReadFileInputStream makes them dereference fileIoContext_.ioStats, a RAW
// velox::IoStats*. A warm task may outlive the FileCacheBufferedInput and its query
// owner, so the shared IoStats owner MUST live in the warm payload. This test only
// observes shared ownership (weak_ptr) -- it does not force a dangling access and
// adds no production test seam.
//
// Sequence (no sleep, no polling):
//   1. Single-thread executor blocked by a pre-submitted baton task.
//   2. Submit warm via load(); it is queued but cannot run (thread is blocked).
//   3. Take weak_ptr<IoStats>; destroy the input and drop the test-side owner.
//   4. While warm is still queued, the weak_ptr must NOT be expired -- proving the
//      warm payload holds a shared owner.
//   5. Release the baton, join the executor; after the payload is destroyed the
//      owner becomes releasable.
TEST_F(FileCacheBufferedInputTest, WarmPayloadKeepsIoStatsAlivePastInputDestruction)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("ctx_warm_life", 16);
    auto path = writeSourceFile("src_ctx_warm_life", content);
    auto key = FileCacheKey::random();

    // Single worker thread so a blocking task deterministically stalls the queue.
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    folly::Baton<> release;   // held until we let the blocking task finish
    folly::Baton<> occupied;  // posted once the blocking task owns the thread
    executor_->add(
        [&release, &occupied]()
        {
            occupied.post();
            release.wait();
        });
    // Ensure the blocking task actually occupies the single worker before we submit
    // warm, so warm is guaranteed to queue behind it rather than run immediately.
    occupied.wait();

    std::weak_ptr<velox::IoStats> ioStatsWeak;
    {
        auto b = makeCapturingInput(cache, path, key);
        ioStatsWeak = b.ioStats;
        b.input->enqueue({0, n});
        b.input->load(dwio::common::LogType::FILE);

        // Warm is queued behind the blocking task. Destroy the input and drop the
        // test-side IoStats owner while warm has NOT yet run.
        b.input.reset();
        b.ioStats.reset();

        // The warm payload must still own the IoStats: it is captured for the
        // in-flight ReadFile::pread the warm task will perform.
        EXPECT_FALSE(ioStatsWeak.expired())
            << "warm payload must keep IoStats alive past input destruction";
    }

    // Let the warm task run and finish, then drain the executor.
    release.post();
    executor_->join();

    // After the warm payload is destroyed, no owner remains.
    EXPECT_TRUE(ioStatsWeak.expired())
        << "IoStats owner should be released once the warm payload is gone";
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, PreloadSourceReadCarriesFileIoContext)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("ctx_preload", 16);
    auto path = writeSourceFile("src_ctx_preload", content);
    auto key = FileCacheKey::random();

    auto b = makeCapturingInput(cache, path, key);
    b.input->preload();
    ASSERT_TRUE(b.input->preloaded());

    const auto cap = b.readFile->captured();
    ASSERT_TRUE(cap.seen);
    EXPECT_EQ(cap.ioStats, b.ioStats.get());
    EXPECT_EQ(cap.fileOpts, b.fileReadOps);
    EXPECT_EQ(cap.cacheable, b.cacheable);

    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, BypassDoesNotCreateMetadata)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("bypass", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    FileCacheReadOptions opts;
    opts.readIfExistsOtherwiseBypass = true; // miss -> bypass, no create
    auto input = makeInput(cache, path, key, opts);
    auto stream = input->read(0, n, dwio::common::LogType::FILE);
    EXPECT_EQ(readAll(*stream), content);
    // Bypass read created no cached segment.
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
    cache->deactivateBackgroundOperations();
}

// ======================= region-relative coordinates =======================

TEST_F(FileCacheBufferedInputTest, NonZeroRegionRelativeCoordinatesAbsoluteData)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("region", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    const uint64_t off = 40;
    const uint64_t len = 30;
    auto input = makeInput(cache, path, key);
    auto stream = input->read(off, len, dwio::common::LogType::FILE);

    // ByteCount starts region-relative at 0.
    EXPECT_EQ(stream->ByteCount(), 0);
    auto out = readAll(*stream);
    // Data comes from ABSOLUTE offset [off, off+len).
    EXPECT_EQ(out, content.substr(off, len));
    // ByteCount ends at the region length (relative), not the absolute offset.
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(len));
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, RegionOverflowThrows)
{
    auto cache = makeCache("overflow", 16);
    auto path = writeSourceFile("src", std::string(64, 'a'));
    auto input = makeInput(cache, path, FileCacheKey::random());
    // region.offset + region.length overflows uint64_t -> checked add throws.
    EXPECT_THROW(
        input->read(std::numeric_limits<uint64_t>::max() - 4, 16, dwio::common::LogType::FILE),
        VeloxException);
    cache->deactivateBackgroundOperations();
}

// ======================= backup / seek =======================

TEST_F(FileCacheBufferedInputTest, BackUpWithinBufferPreservesData)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("backup", 128); // one segment
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();
    auto input = makeInput(cache, path, key);
    auto stream = input->read(0, n, dwio::common::LogType::FILE);

    const void * data = nullptr;
    int32_t size = 0;
    ASSERT_TRUE(stream->Next(&data, &size));
    ASSERT_GE(size, 10);
    // Back up 10 bytes; they must be re-read next.
    stream->BackUp(10);
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(size - 10));
    const void * data2 = nullptr;
    int32_t size2 = 0;
    ASSERT_TRUE(stream->Next(&data2, &size2));
    EXPECT_EQ(size2, 10);
    EXPECT_EQ(
        std::string(static_cast<const char *>(data2), 10),
        content.substr(size - 10, 10));
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, SeekWithinBufferFastPath)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("seekfast", 128);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();
    auto input = makeInput(cache, path, key);
    auto stream = input->read(0, n, dwio::common::LogType::FILE);

    const void * data = nullptr;
    int32_t size = 0;
    ASSERT_TRUE(stream->Next(&data, &size));
    ASSERT_GE(size, 20);

    // Seek back into the already-filled buffer (fast path, region-relative).
    std::vector<uint64_t> positions{5};
    dwio::common::PositionProvider pp(positions);
    stream->seekToPosition(pp);
    EXPECT_EQ(stream->ByteCount(), 5);
    auto tail = readN(*stream, 10);
    EXPECT_EQ(tail, content.substr(5, 10));
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, SeekOutsideBufferRebuildsButKeepsData)
{
    const size_t n = 200;
    auto content = makeContent(n);
    auto cache = makeCache("seekfar", 16); // many segments
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();
    auto input = makeInput(cache, path, key);
    auto stream = input->read(0, n, dwio::common::LogType::FILE);

    // Read a bit to fill a buffer.
    (void)readN(*stream, 8);
    // Seek far outside the current buffer; slow path rebuilds holder.
    std::vector<uint64_t> positions{150};
    dwio::common::PositionProvider pp(positions);
    stream->seekToPosition(pp);
    EXPECT_EQ(stream->ByteCount(), 150);
    auto tail = readN(*stream, 20);
    EXPECT_EQ(tail, content.substr(150, 20));
    cache->deactivateBackgroundOperations();
}

// ======================= Q1/Q2 handoff =======================

TEST_F(FileCacheBufferedInputTest, Q1Q2HandoffReusesReaderFromWriteOffset)
{
    // Q1 partially downloads a segment; Q2 continues from currentWriteOffset.
    const size_t n = 64;
    auto content = makeContent(n);
    auto cache = makeCache("handoff", 64); // single segment
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // Q1 reads only a prefix, then is destroyed (segment stays partially
    // downloaded with a reusable reader).
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        auto prefix = readN(*stream, 8);
        EXPECT_EQ(prefix, content.substr(0, 8));
    }

    // Q2 reads the full range; the prefix comes from cache, the rest continues
    // downloading from the write offset.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, PredownloadFromMidSegment)
{
    // Exercise the predownload path deterministically: pre-populate a single
    // segment [0, 64) with only a short prefix downloaded (via the direct
    // FileCache API, as the manager test does), leaving it PARTIALLY_DOWNLOADED.
    // A stream then reads region [20, 64): the segment's currentWriteOffset (8)
    // is < 20, so createReadFromFileSegmentState computes bytesToPredownload = 12
    // and the state machine must predownload [8, 20) into cache before returning
    // the requested [20, 64) bytes.
    const size_t n = 64;
    auto content = makeContent(n);
    // Segment size == n and alignment == n so [0, 64) is exactly one segment.
    auto s = settings(n);
    s.boundaryAlignment = n;
    std::shared_ptr<FileCache> cache = res_.makeFileCache("predownload", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // Populate a partial prefix [0, 8) of the single segment, leaving it
    // PARTIALLY_DOWNLOADED without completing (no shrink, no rename).
    {
        CreateFileSegmentSettings createSettings; // Regular
        auto holder = cache->getOrSet(key, 0, n, n, createSettings, 0, cache->getCommonOrigin());
        ASSERT_TRUE(holder && !holder->empty());
        auto segPtr = holder->getSingleFileSegment();
        ASSERT_TRUE(segPtr);
        FileSegment & seg = *segPtr;
        ASSERT_EQ(seg.getOrSetDownloader(), FileSegment::getCallerId());
        std::string reason;
        ASSERT_TRUE(seg.reserve(8, 100, reason)) << reason;
        std::vector<char> prefix(content.begin(), content.begin() + 8);
        seg.write(prefix.data(), 8, seg.getCurrentWriteOffset());
        seg.completePartAndResetDownloader();
        // Keep the segment PARTIALLY_DOWNLOADED (do not allow background download,
        // which would let it continue/shrink).
        holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink=*/false);
    }

    // A stream reading [20, 64) must predownload the [8, 20) gap, then read on.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(20, n - 20, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content.substr(20, n - 20));
    }
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, ReserveFailureBypassesCacheButReturnsData)
{
    // A tiny cache makes reserve fail during a remote-put read. With
    // skipCacheOnDiskFailure default, a reserve failure switches the read to
    // bypass: the data is still returned correctly, and nothing is written to
    // the cache (the bypass path never calls writeCache).
    const size_t n = 200;
    auto content = makeContent(n);
    auto s = settings(64);
    s.maxSize = 8;      // smaller than one segment -> reservation fails
    s.maxElements = 1;
    std::shared_ptr<FileCache> cache = res_.makeFileCache("tinycache", s, "user-A");
    cache->initialize();

    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }

    // Prove the BYPASS path was taken (not a cache-success path): no bytes were
    // downloaded into the cache. A cache-only read of the same range must throw
    // because nothing is cached. This assertion fails if the reserve-failure ->
    // bypass switch were broken and the segment were instead written to cache.
    {
        FileCacheReadOptions opts;
        opts.tempCacheOnly = true;
        auto input = makeInput(cache, path, key, opts);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        const void * data = nullptr;
        int32_t size = 0;
        EXPECT_ANY_THROW(stream->Next(&data, &size));
    }
    cache->deactivateBackgroundOperations();
}

// ======================= exception cleanup =======================

// Design 6.7 (§11.8 rewrite): reach FileCacheInputStream::Next's in-`try` catch
// block WITH a downloader actually held, then verify the catch releases the
// downloader and resets state without leaking. The write-fault seam is gone, so
// the failure is driven through a FAULTY SOURCE READER: a
// REMOTE_FS_READ_AND_PUT_IN_CACHE read acquires the downloader in
// prepareReadFromFileSegmentState, then the source `pread` throws. The catch
// block must release the downloader and reset state so an independent stream can
// afterwards become the segment's downloader and read it end to end.
TEST_F(FileCacheBufferedInputTest, MidDownloadSourceReadFailureReleasesDownloaderNoLeak)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto cache = makeCache("middownload", 64); // single segment [0, 64)
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    {
        auto delegate = std::make_shared<velox::LocalReadFile>(path);
        auto hook = [](tests::utils::FaultFileOperation * /*op*/)
        {
            // Fail every source read so the very first download attempt throws
            // while the stream holds the downloader lease.
            VELOX_FAIL("injected source read failure");
        };
        auto faulty = std::make_shared<tests::utils::FaultyReadFile>(
            path, delegate, std::move(hook), /*executor=*/nullptr);

        auto input = makeInputWithReadFile(cache, faulty, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        // First Next: becomes downloader, then the source read throws inside the
        // try -> the catch block runs and releases the downloader.
        const void * data = nullptr;
        int32_t size = 0;
        EXPECT_ANY_THROW(stream->Next(&data, &size));
    }

    // No downloader/holder leaked: an INDEPENDENT stream (over the real source)
    // can now become the segment's downloader and read it end to end (a leaked
    // downloader would deadlock/wait or trip a still-held-downloader assertion).
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

// ======================= preload fill: bypass on disk failure =============

// Design 6.7 (§11.8 rewrite): a cache-fill failure during preload is a bypass --
// reserveAndWriteSegmentChunk returns false and the fill breaks out of that segment,
// but the RAM preload is still committed (preload() does not throw and
// preloaded() is true). The fill failure is driven through a reserve() failure: a
// tiny cache cannot reserve space for the segment, so the reserve returns false
// and the best-effort fill bypasses UNCONDITIONALLY (before any errno/skip
// decision), while the whole-file RAM preload still succeeds. This test's
// contract is the RAM-preload commit, not skip wiring.
TEST_F(FileCacheBufferedInputTest, PreloadFillBypassDiskFailureStillCommits)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto s = settings(64);
    s.maxSize = 8;      // smaller than one segment -> reservation fails
    s.maxElements = 1;
    std::shared_ptr<FileCache> cache = res_.makeFileCache("preload_bypass", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    auto input = makeInput(cache, path, key);
    EXPECT_NO_THROW(input->preload());
    EXPECT_TRUE(input->preloaded());
    cache->deactivateBackgroundOperations();
}

// ============ preload source accounting order (Design 7.6) ==============

// Design 7.6 regression: a normal preload (no disk failure) records the source
// read exactly once -- read()/rawBytesRead == fileSize_ and ProfileEvents grows
// by fileSize_ -- and preloaded() is true.
TEST_F(FileCacheBufferedInputTest, PreloadNormalRecordsSourceStats)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto s = settings(64);
    std::shared_ptr<FileCache> cache = res_.makeFileCache("preload_stats_ok", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    const uint64_t before = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);

    auto input = makeInput(cache, path, key);
    input->preload();
    EXPECT_TRUE(input->preloaded());

    auto * io = input->ioStatistics();
    ASSERT_NE(io, nullptr);
    EXPECT_EQ(io->read().sum(), n);
    EXPECT_EQ(io->rawBytesRead(), n);

    const uint64_t after = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    EXPECT_EQ(after - before, n);
    cache->deactivateBackgroundOperations();
}

// Design 7.6 regression (§11.8 rewrite): a bypass-mode fill failure commits the
// RAM preload (preloaded() true) AND records the source read, since the source
// read still happened and the fill absorbed the fault. The fill failure is driven
// through a reserve() failure (a tiny cache): the reserve returns false and the
// fill bypasses UNCONDITIONALLY (before any errno/skip decision). This test's
// contract is the source accounting, not skip wiring.
TEST_F(FileCacheBufferedInputTest, PreloadBypassFillFailureRecordsSourceStats)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto s = settings(64);
    s.maxSize = 8;      // smaller than one segment -> reservation fails
    s.maxElements = 1;
    std::shared_ptr<FileCache> cache = res_.makeFileCache("preload_stats_bypass", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    const uint64_t before = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);

    auto input = makeInput(cache, path, key);
    EXPECT_NO_THROW(input->preload());
    EXPECT_TRUE(input->preloaded());

    auto * io = input->ioStatistics();
    ASSERT_NE(io, nullptr);
    EXPECT_EQ(io->read().sum(), n);
    EXPECT_EQ(io->rawBytesRead(), n);

    const uint64_t after = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    EXPECT_EQ(after - before, n);
    cache->deactivateBackgroundOperations();
}

TEST_F(FileCacheBufferedInputTest, TempCacheOnlyMissThrowsAndLeavesNoLeak)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto cache = makeCache("exc", 64);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // Cache-only read of a cold cache: nextFileSegmentsBatch throws (a miss is a
    // hard error), and must not leave a downloader or caller-buffer pointer held.
    {
        FileCacheReadOptions opts;
        opts.tempCacheOnly = true;
        auto input = makeInput(cache, path, key, opts);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        const void * data = nullptr;
        int32_t size = 0;
        EXPECT_ANY_THROW(stream->Next(&data, &size));
    }
    // No segment/downloader leaked: a subsequent normal read fully succeeds.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

// ==================== external truncation self-heal ====================

// F-014-1: a fully-DOWNLOADED size-in-filename segment whose on-disk cache file
// is truncated OUTSIDE FileCache must self-heal: getCacheReadBuffer detects the
// short file, bypasses the cache, and re-fetches the full original bytes from
// the source. It must NOT short-read and must NOT throw a detach/CANNOT_READ_ALL
// -class error. Ported from CH CachedOnDiskReadBufferFromFile.cpp:448-472.
TEST_F(FileCacheBufferedInputTest, ExternalTruncationSelfHealsFromSource)
{
    const size_t n = 64;
    auto content = makeContent(n);
    // Single segment [0, 64): segment size == n, alignment == n.
    auto s = settings(n);
    s.boundaryAlignment = n;
    std::shared_ptr<FileCache> cache =
        res_.makeFileCache("truncselfheal", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // Fully download the single segment through a normal read (miss -> fills
    // cache; on completion the file is renamed to <offset>_<size>).
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        ASSERT_EQ(readAll(*stream), content);
    }

    // Locate the fully-downloaded cache file and confirm it carries the size in
    // its name (the precondition the self-heal is gated on).
    std::string cacheFilePath;
    size_t downloadedSize = 0;
    {
        auto holder = cache->get(key, 0, n, 0, "user-A");
        ASSERT_TRUE(holder && !holder->empty());
        auto segPtr = holder->getSingleFileSegment();
        ASSERT_TRUE(segPtr);
        FileSegment & seg = *segPtr;
        ASSERT_EQ(seg.state(), FileSegment::State::DOWNLOADED);
        ASSERT_TRUE(seg.hasSizeInFileName());
        downloadedSize = seg.getDownloadedSize();
        ASSERT_EQ(downloadedSize, n);
        cacheFilePath = seg.getPath();
    }

    // Externally truncate the on-disk cache file WITHOUT going through FileCache
    // (simulates truncation outside ClickHouse).
    const size_t truncatedTo = n / 2;
    ASSERT_EQ(::truncate(cacheFilePath.c_str(), static_cast<off_t>(truncatedTo)), 0)
        << "truncate failed: " << std::strerror(errno);
    ASSERT_EQ(fs::file_size(cacheFilePath), truncatedTo);

    // Read through a NEW stream. The self-heal must re-fetch the FULL original
    // bytes from the source. Against the pre-fix unconditional-open code this
    // returns only the truncated prefix (short read) -> the RED failure.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

// F-014-1: an EMPTY (zero-byte) cache file for a size-in-filename DOWNLOADED
// segment is a corrupted-cache case. cacheFileSize == 0 < downloadedSize takes
// the same bypass branch, so the read still self-heals and returns full bytes.
TEST_F(FileCacheBufferedInputTest, EmptyCacheFileSelfHealsFromSource)
{
    const size_t n = 64;
    auto content = makeContent(n);
    auto s = settings(n);
    s.boundaryAlignment = n;
    std::shared_ptr<FileCache> cache =
        res_.makeFileCache("emptyselfheal", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        ASSERT_EQ(readAll(*stream), content);
    }

    std::string cacheFilePath;
    {
        auto holder = cache->get(key, 0, n, 0, "user-A");
        ASSERT_TRUE(holder && !holder->empty());
        auto segPtr = holder->getSingleFileSegment();
        ASSERT_TRUE(segPtr);
        ASSERT_TRUE(segPtr->hasSizeInFileName());
        cacheFilePath = segPtr->getPath();
    }

    // Truncate to zero bytes (cacheFileSize == 0 < downloadedSize -> bypass).
    ASSERT_EQ(::truncate(cacheFilePath.c_str(), 0), 0)
        << "truncate failed: " << std::strerror(errno);
    ASSERT_EQ(fs::file_size(cacheFilePath), 0u);

    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
    cache->deactivateBackgroundOperations();
}

// ======================= warm source IO statistics =======================

// Design 5.8 (coalesced model): a prefetch load reads bytes from the SOURCE file
// and writes them into the FileCache. Those source reads MUST be reflected in the
// operator-level IoStatistics: read() (storage bytes read from source, recorded by
// the internal FileCacheInputStream driving the load), rawBytesRead (raw bytes),
// and prefetch() (useful bytes prefetched into the cache and covering the
// requested range, recorded by FileCacheCoalescedLoad::loadData for a prefetch
// load). The coalesced model reads exactly the requested regions (no gap
// over-read), so rawOverreadBytes stays 0.
// R2-6: rewritten from the warmSourceGroup model to the coalesced-load model. The
// null StreamIdentifier => empty trackingId => prefetch group, submitted by load()
// to the executor; loadData(prefetch=true) records prefetch() over the requested
// region union.
TEST_F(FileCacheBufferedInputTest, WarmRecordsSourceAndPrefetchStats)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("warmstats", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    // Dedicated single-thread executor so we can deterministically wait for the
    // warm task to finish by join()-ing it -- no sleep, no polling.
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    auto input = makeInput(cache, path, key);
    // enqueue() with a null StreamIdentifier -> empty trackingId -> classified
    // as prefetch -> load() submits a warm source group to the executor.
    { auto stream = input->enqueue({0, n}); }
    input->load(dwio::common::LogType::FILE);
    // Block until the warm task has run to completion (not cancelled).
    executor_->join();

    auto * stats = input->ioStatistics();
    ASSERT_NE(stats, nullptr);
    // warm read the whole 100-byte file from source and cached it.
    EXPECT_EQ(stats->read().sum(), n) << "warm source read bytes";
    EXPECT_EQ(stats->rawBytesRead(), n) << "warm raw bytes read";
    // Every byte falls inside the single requested range [0,100), so all are
    // useful prefetch bytes; none are gap over-read.
    EXPECT_EQ(stats->prefetch().sum(), n) << "warm prefetch bytes";
    EXPECT_EQ(stats->rawOverreadBytes(), 0u) << "no gap over-read for a dense range";

    // The warm task really filled the cache.
    EXPECT_GT(cache->getFileSegmentsNum(), 0u);
    cache->deactivateBackgroundOperations();
}

// After a prefetch load has filled the segments, a demand read of the same range
// must be served from the (ssd) cache and recorded as ssdRead(), NOT counted a
// second time against read() (source). Otherwise the same source bytes are
// attributed twice. The demand path already distinguishes cache hit (ssdRead) from
// source (read); this is a regression guard proving prefetch + demand does not
// double-count source bytes.
// R2-6: rewritten for the coalesced-load model (prefetch load fills the cache on
// the executor; the later demand read hits it).
TEST_F(FileCacheBufferedInputTest, LocalHitAfterWarmDoesNotRecountSource)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("warmhit", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    auto input = makeInput(cache, path, key);
    { auto stream = input->enqueue({0, n}); }
    input->load(dwio::common::LogType::FILE);
    executor_->join();

    const uint64_t sourceAfterWarm = input->ioStatistics()->read().sum();
    EXPECT_EQ(sourceAfterWarm, n);

    // Clear the prefetch planning state (coalesced loads + stream bindings) before
    // the demand read. The prefetch load already ran to completion, so this only
    // drops the now-stale bindings keyed by the destroyed enqueue stream; without
    // it a fresh demand stream reallocated at the same address would consume the
    // coalesced RAM buffers (recorded as recordReadBytes, not ssdRead) instead of
    // exercising the local cache-hit path this test asserts.
    input->reset();

    // Demand read the same range on the SAME input (IoStatistics is per-input) so
    // the source/ssd accounting below is comparable to sourceAfterWarm.
    auto stream = input->read(0, n, dwio::common::LogType::FILE);
    EXPECT_EQ(readAll(*stream), content);

    auto * stats = input->ioStatistics();
    // The demand read hit the warmed cache: source (read) must NOT grow.
    EXPECT_EQ(stats->read().sum(), sourceAfterWarm) << "cache hit must not re-count source";
    // The hit is attributed to ssdRead instead.
    EXPECT_GE(stats->ssdRead().sum(), n) << "cache hit recorded as ssdRead";
    cache->deactivateBackgroundOperations();
}

// Design 6.9 (problem 1, §11.8 rewrite): the SOURCE read must be accounted the
// instant the bytes come back from storage, BEFORE reserveAndWriteSegmentChunk. If
// the cache fill is then skipped (reserveAndWriteSegmentChunk returns false -> the
// warm loop breaks), the real remote bytes were still read and MUST be reflected
// in read()/rawBytesRead(). The cache-fill skip is driven through a reserve()
// failure (a tiny cache): the source read succeeds, the reserve fails, and the
// fill bypasses UNCONDITIONALLY (before any errno/skip decision). This test's
// contract is the warm source accounting, not skip wiring.
TEST_F(FileCacheBufferedInputTest, WarmRecordsSourceReadEvenWhenCacheFillBypasses)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto s = settings(16);
    s.maxSize = 8;      // smaller than one segment -> reservation fails
    s.maxElements = 1;
    std::shared_ptr<FileCache> cache =
        res_.makeFileCache("warmwritefail", s, "user-A");
    cache->initialize();
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    auto input = makeInput(cache, path, key);
    const uint64_t sourceEventsBefore =
        ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
    { auto stream = input->enqueue({0, n}); }
    input->load(dwio::common::LogType::FILE);
    executor_->join();

    auto * stats = input->ioStatistics();
    ASSERT_NE(stats, nullptr);
    // The cache fill was skipped for every chunk, but the SOURCE really
    // returned bytes on the first read -- those must be accounted.
    EXPECT_GT(stats->read().sum(), 0u) << "source read bytes lost on cache-write skip";
    EXPECT_GT(stats->rawBytesRead(), 0u) << "raw bytes lost on cache-write skip";
    EXPECT_GT(
        ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes),
        sourceEventsBefore)
        << "ProfileEvents source bytes lost on cache-write skip";
    cache->deactivateBackgroundOperations();
}

// Design 6.9 (problem 2, coalesced model): useful prefetch must be computed as
// the interval-union of the requested regions, not a per-range sum. Two
// OVERLAPPING requested ranges over the same physical bytes must count each byte
// once. Enqueuing the same region [0,n) twice yields two prefetch requests with
// identical {offset,length}; FileCacheCoalescedLoad::loadData counts the union, so
// prefetch() == n (not 2n). A per-range sum would overshoot the real source bytes.
// R2-6: rewritten for the coalesced-load model; duplicate regions are materialised
// once and prefetch() reports the requested-region union.
TEST_F(FileCacheBufferedInputTest, WarmOverlappingRangesPrefetchCountsUnion)
{
    const size_t n = 100;
    auto content = makeContent(n);
    auto cache = makeCache("warmoverlap", 16);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    auto input = makeInput(cache, path, key);
    // Two overlapping enqueues of the SAME region -> overlapping ranges in the
    // coalesced warm group.
    { auto s1 = input->enqueue({0, n}); auto s2 = input->enqueue({0, n}); }
    input->load(dwio::common::LogType::FILE);
    executor_->join();

    auto * stats = input->ioStatistics();
    ASSERT_NE(stats, nullptr);
    // Source read the file once (n bytes). Useful prefetch cannot exceed that:
    // the overlapping ranges cover the same bytes, counted once by the union.
    EXPECT_EQ(stats->prefetch().sum(), n) << "overlapping ranges double-counted prefetch";
    EXPECT_LE(stats->prefetch().sum(), stats->read().sum())
        << "useful prefetch must not exceed source bytes";
    cache->deactivateBackgroundOperations();
}

// §11.4 (C7): classifyChunk must classify a whole loadQuantum chunk by looking
// at ALL overlapping FileSegments, not only front(). A 4 MiB chunk spans four
// 1-MiB segments. If only the first segment [0,1MiB) is pre-cached (DOWNLOADED)
// and segments 1..3 are EMPTY, the front-only classifier saw a DOWNLOADED
// front() whose downloadedEnd (1 MiB) < chunkEnd (4 MiB) and returned
// kDownloading for the whole chunk, so segments 1..3 never entered a miss warm
// group and stayed EMPTY. After the fix the chunk is kMiss (segments 1..3 are
// fillable-absent), the warm group runs and fills them to DOWNLOADED.
TEST_F(FileCacheBufferedInputTest, MixedChunkWarmsUncachedSegments)
{
    constexpr size_t kMiB = 1024 * 1024;
    const size_t segSize = kMiB;      // FileCacheSettings.maxFileSegmentSize
    loadQuantum_ = 4 * kMiB;          // one chunk spans four 1-MiB segments
    const size_t fileSize = 4 * kMiB;
    auto content = makeContent(fileSize);
    auto cache = makeCache("mixedchunk", segSize);
    auto path = writeSourceFile("src_mixed", content);
    auto key = FileCacheKey::random();

    const auto userId = cache->getCommonOrigin().user_id;
    const auto stateAt = [&](size_t offset) -> FileSegment::State
    {
        auto holder = cache->get(key, offset, segSize, 100, userId);
        EXPECT_TRUE(holder && !holder->empty()) << "no segment at offset " << offset;
        return holder->front().state();
    };

    // Pre-cache ONLY the first 1-MiB segment via a demand read of [0, 1 MiB).
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, segSize, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content.substr(0, segSize));
    }
    ASSERT_EQ(cache->getFileSegmentsNum(), 1u) << "only segment 0 must be cached";
    ASSERT_EQ(stateAt(0), FileSegment::State::DOWNLOADED);

    // Enqueue a sequential [0, 4 MiB) read. Empty trackingId classifies as
    // prefetch, so load() submits a warm group for the kMiss chunk.
    {
        auto input = makeInput(cache, path, key);
        input->enqueue({0, fileSize});
        input->load(dwio::common::LogType::FILE);
        executor_->join();
    }

    // After the fix: the mixed chunk is kMiss, so the warm group filled the
    // three previously-EMPTY segments. Pre-fix: the chunk was kDownloading,
    // segments 1..3 never warmed and getFileSegmentsNum stayed 1.
    EXPECT_EQ(cache->getFileSegmentsNum(), 4u)
        << "all four 1-MiB segments must be present after warming";
    EXPECT_EQ(stateAt(1 * kMiB), FileSegment::State::DOWNLOADED);
    EXPECT_EQ(stateAt(2 * kMiB), FileSegment::State::DOWNLOADED);
    EXPECT_EQ(stateAt(3 * kMiB), FileSegment::State::DOWNLOADED);
    cache->deactivateBackgroundOperations();
}

/// Design 7.5/8.3 errno policy (table-driven pure test, §11.8). The write-fault
/// integration seam is gone; the errno bypass/rethrow decision is now the pure
/// helper `classifyCacheWriteError`, exercised directly here. ENOSPC/EDQUOT are an
/// unconditional bypass (regardless of skipOnDiskFailure); any other errno bypasses
/// only when skipOnDiskFailure is true, and otherwise rethrows. A
/// non-FileCacheErrnoException never reaches this helper (the consumers catch only
/// the typed errno exception), so a logic error propagates naturally -- that path is
/// covered by the reader/reserve fault tests, not here.
TEST(CacheWriteErrorPolicyTest, ClassifiesByErrnoAndSkipFlag)
{
    struct Case
    {
        int error;
        bool skip;
        CacheWriteErrorAction expected;
    };

    const Case cases[] = {
        {ENOSPC, true, CacheWriteErrorAction::Bypass},
        {ENOSPC, false, CacheWriteErrorAction::Bypass},
        {EDQUOT, true, CacheWriteErrorAction::Bypass},
        {EDQUOT, false, CacheWriteErrorAction::Bypass},
        {EIO, true, CacheWriteErrorAction::Bypass},
        {EIO, false, CacheWriteErrorAction::Rethrow},
        {EACCES, true, CacheWriteErrorAction::Bypass},
        {EACCES, false, CacheWriteErrorAction::Rethrow},
    };

    for (const auto & c : cases)
    {
        EXPECT_EQ(classifyCacheWriteError(c.error, c.skip), c.expected)
            << "errno " << c.error << " skip " << c.skip;
    }
}

} // namespace
} // namespace facebook::velox::ch
