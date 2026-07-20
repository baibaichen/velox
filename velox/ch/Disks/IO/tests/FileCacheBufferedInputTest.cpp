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
#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

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

/// A `velox::WriteFile` that throws `FileCacheErrnoException` on its first append,
/// used to fault the cache write MID-download so `FileSegment::write` (and thus
/// `FileCacheInputStream::writeCache`, with skipCacheOnDiskFailure=false) throws
/// while the caller still holds the segment downloader.
class ThrowOnAppendWriteFile : public velox::WriteFile
{
public:
    explicit ThrowOnAppendWriteFile(const std::string & path)
        : inner_(std::make_unique<velox::LocalWriteFile>(
              path, /*createParentDirs*/ false, /*throwOnExists*/ false, /*bufferIo*/ true))
    {
    }

    void append(std::string_view /*data*/) override
    {
        throw FileCacheErrnoException(/* EIO */ 5, "injected cache write IO error");
    }
    void flush() override { inner_->flush(); }
    void close() override { inner_->close(); }
    uint64_t size() const override { return inner_->size(); }
    const std::string getName() const override { return inner_->getName(); }

private:
    std::unique_ptr<velox::LocalWriteFile> inner_;
};

/// RAII installer for the process-wide FileSegment write-file factory override;
/// restores the production-equivalent default on scope exit (mirrors the pattern
/// in FileSegmentTest.cpp so the override never leaks into other tests).
struct ScopedWriteFileFactory
{
    explicit ScopedWriteFileFactory(FileSegment::WriteFileFactory factory)
    {
        FileSegment::setWriteFileFactoryForTesting(std::move(factory));
    }
    ~ScopedWriteFileFactory()
    {
        FileSegment::setWriteFileFactoryForTesting(
            [](const std::string & path) -> std::unique_ptr<velox::WriteFile>
            {
                return std::make_unique<velox::LocalWriteFile>(path, false, false, true);
            });
    }
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
        return (fs::path(temp_->getPath()) / s).string();
    }

    // Write a source file with deterministic content and return its path.
    std::string writeSourceFile(const std::string & name, const std::string & content)
    {
        const auto path = sub(name);
        std::ofstream(path, std::ios::binary) << content;
        return path;
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
        return o;
    }

    // Build a FileCacheBufferedInput over `sourcePath` backed by `cache`.
    std::unique_ptr<FileCacheBufferedInput> makeInput(
        std::shared_ptr<FileCache> cache,
        const std::string & sourcePath,
        const FileCacheKey & key,
        FileCacheReadOptions readOptions = {})
    {
        auto readFile = std::make_shared<velox::LocalReadFile>(sourcePath);
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
    test::FileCacheTestResources res_;
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
    input->preload();
    EXPECT_FALSE(input->preloaded());
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

    // Second stream: should be fully cached now.
    {
        auto input = makeInput(cache, path, key);
        EXPECT_TRUE(input->isBuffered(0, n));
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
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

TEST_F(FileCacheBufferedInputTest, MidDownloadCacheWriteFailureReleasesDownloaderNoLeak)
{
    // Reach FileCacheInputStream::Next's in-`try` catch block WITH a downloader
    // actually held: a REMOTE_FS_READ_AND_PUT_IN_CACHE read acquires the
    // downloader in prepareReadFromFileSegmentState, then the cache write faults
    // mid-download (injected ThrowOnAppendWriteFile; skipCacheOnDiskFailure=false
    // -> writeCache rethrows). The catch block must release the downloader and
    // reset state without returning the canceled reader to the FileSegment.
    const size_t n = 64;
    auto content = makeContent(n);
    auto cache = makeCache("middownload", 64); // single segment [0, 64)
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::random();

    {
        ScopedWriteFileFactory scoped(
            [](const std::string & p) -> std::unique_ptr<velox::WriteFile>
            { return std::make_unique<ThrowOnAppendWriteFile>(p); });

        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        // First Next: becomes downloader, reads remote, then FileSegment::write
        // throws inside the try -> the catch block runs.
        const void * data = nullptr;
        int32_t size = 0;
        EXPECT_ANY_THROW(stream->Next(&data, &size));
    }

    // No downloader/holder leaked: an INDEPENDENT stream can now become the
    // segment's downloader and read it end to end (a leaked downloader would
    // deadlock/wait or trip a still-held-downloader assertion). The write-file
    // factory has been restored to the default by the scope above.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->read(0, n, dwio::common::LogType::FILE);
        EXPECT_EQ(readAll(*stream), content);
    }
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

} // namespace
} // namespace facebook::velox::ch
