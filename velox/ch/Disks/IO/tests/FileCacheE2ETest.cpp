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

// Task 015: Velox-only end-to-end validation of the assembled public FileCache
// read path, driven through a real `FileCacheManager`:
//
//     FileCacheBufferedInput -> FileCacheInputStream -> FileCache
//
// Every test exercises the assembled public path with concrete assertions.
// Focused reader/handoff unit tests live in FileCacheBufferedInputTest.cpp
// (Task 014) and focused segment resume/reconciliation UTs in the SCC test
// (Task 012); this suite covers the whole-system behavior through the manager.

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
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

// Deterministic byte pattern keyed on the absolute index via a Knuth
// multiplicative hash, so the effective period exceeds any file used here and a
// wrong absolute offset almost always yields a wrong byte (the
// absolute-coordinate tests assert on this).
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

/// A `velox::ReadFile` that wraps `LocalReadFile` and counts every physical read
/// (`pread`/`preadv`). Used to assert the "no remote I/O on a cache hit" contract:
/// after a segment is DOWNLOADED, a second stream must serve bytes from the local
/// cache segment file, so the SOURCE file's read count must not increase.
class CountingReadFile : public velox::ReadFile
{
public:
    explicit CountingReadFile(const std::string & path)
        : inner_(std::make_unique<velox::LocalReadFile>(path))
    {
    }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const velox::FileIoContext & ctx = {})
        const override
    {
        preadCount_.fetch_add(1);
        return inner_->pread(offset, length, buf, ctx);
    }

    uint64_t preadv(
        uint64_t offset,
        const std::vector<folly::Range<char *>> & buffers,
        const velox::FileIoContext & ctx = {}) const override
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

class FileCacheE2ETest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { filesystems::registerLocalFileSystem(); }

    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        pool_ = memoryManager_.addLeafPool("filecache-e2e-test");
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

    // Build and install a FileCacheManager with a single "default" cache. `seg`
    // is the segment size; `align` the boundary alignment (align==seg keeps a
    // partial segment at its stable `<offset>` name; align==1 lets full
    // downloads rename to `<offset>_<size>`).
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

    // Build a FileCacheBufferedInput over `readFile` with the given key/options.
    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCachePtr cache,
        std::shared_ptr<ReadFile> readFile,
        const FileCacheKey & key,
        FileCacheReadOptions readOptions = {})
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
            readOptions,
            ctx,
            dwio::common::MetricsLog::voidLog(),
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get(),
            readerOptions());
    }

    std::unique_ptr<FileCacheBufferedInput> makeInput(
        FileCachePtr cache, const std::string & path, const FileCacheKey & key, FileCacheReadOptions readOptions = {})
    {
        return makeInput(std::move(cache), std::make_shared<velox::LocalReadFile>(path), key, readOptions);
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<FileCacheManager> manager_;
};

// ============================================================================
// 1. MissFillHit: miss -> fill -> hit, and NO source I/O on the hit.
// ============================================================================
TEST_F(FileCacheE2ETest, MissFillHit)
{
    const size_t n = 256 * 1024;
    const size_t half = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Cold miss: fill [0, 128 KiB) from the source through the assembled path.
    auto countingA = std::make_shared<CountingReadFile>(path);
    {
        auto input = makeInput(cache, countingA, key);
        auto stream = input->enqueue({0, half});
        EXPECT_EQ(readAll(*stream), content.substr(0, half));
    }
    EXPECT_GT(cache->getFileSegmentsNum(), 0u);
    EXPECT_GT(countingA->preadCount(), 0u) << "cold miss must read the source";

    // A fresh input for the same key reports the range as buffered (no-create).
    auto countingB = std::make_shared<CountingReadFile>(path);
    {
        auto input = makeInput(cache, countingB, key);
        EXPECT_TRUE(input->isBuffered(0, half));
        auto stream = input->enqueue({0, half});
        EXPECT_EQ(readAll(*stream), content.substr(0, half));
    }
    // The hit was served entirely from the local cache segment file: the SOURCE
    // read file was never touched.
    EXPECT_EQ(countingB->preadCount(), 0u) << "cache hit must not read the remote source";
}

// ============================================================================
// 2. CacheOnlyMissFails: tempCacheOnly on a cold key -> Next throws.
// ============================================================================
TEST_F(FileCacheE2ETest, CacheOnlyMissFails)
{
    const size_t n = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    FileCacheReadOptions opts;
    opts.tempCacheOnly = true;
    auto input = makeInput(cache, path, key, opts);
    auto stream = input->enqueue({0, n});

    const void * data = nullptr;
    int32_t size = 0;
    EXPECT_THROW(stream->Next(&data, &size), VeloxRuntimeError);
    // Nothing was written into the cache by a failed cache-only read.
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
}

// ============================================================================
// 3. ReadIfExistsBypassMode: uncached key -> bypass source read, no segment.
// ============================================================================
TEST_F(FileCacheE2ETest, ReadIfExistsBypassMode)
{
    const size_t n = 100 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    FileCacheReadOptions opts;
    opts.readIfExistsOtherwiseBypass = true;
    auto input = makeInput(cache, path, key, opts);
    auto stream = input->enqueue({0, n});
    EXPECT_EQ(readAll(*stream), content);
    // The bypass read created no FileSegment.
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
}

// ============================================================================
// 4. BackUpWithinOutputBuffer: BackUp rewinds ByteCount and re-reads bytes.
// ============================================================================
TEST_F(FileCacheE2ETest, BackUpWithinOutputBuffer)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 128 * 1024, /*align*/ 1); // one segment
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({0, n});

    // Consume 64 KiB.
    auto first = readN(*stream, 64 * 1024);
    ASSERT_EQ(first.size(), 64u * 1024);
    ASSERT_EQ(stream->ByteCount(), static_cast<int64_t>(64 * 1024));

    stream->BackUp(1024);
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(64 * 1024 - 1024));

    // Re-reading the backed-up bytes returns the same content.
    auto again = readN(*stream, 1024);
    EXPECT_EQ(again, content.substr(64 * 1024 - 1024, 1024));
}

// ============================================================================
// 5. SkipAcrossSegmentBoundary: SkipInt64 past a segment boundary.
// ============================================================================
TEST_F(FileCacheE2ETest, SkipAcrossSegmentBoundary)
{
    const size_t n = 512 * 1024;
    const size_t seg = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({0, n});

    // Consume the first segment's worth of bytes.
    auto head = readN(*stream, seg);
    ASSERT_EQ(head, content.substr(0, seg));

    // Skip past the boundary into the third segment.
    const int64_t skip = seg + 100;
    ASSERT_TRUE(stream->SkipInt64(skip));
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(seg + skip));

    // Next() returns data from the correct absolute offset.
    auto tail = readN(*stream, 200);
    EXPECT_EQ(tail, content.substr(seg + skip, 200));
}

// ============================================================================
// 5b. SkipFromMidSegmentAcrossBoundary: the real bug pattern. Read only PART of
// segment 0 (stop mid-segment, never touching the boundary), then SkipInt64 a
// distance that both exceeds the remaining published buffer AND crosses into
// segment 1. The pre-fix "advance-via-Next then roll position_ back" desynced
// position_ from the held segment, drifting later reads into an early EOF
// ("Reading past end"). Asserts the ACTUAL bytes at the correct absolute offset.
// ============================================================================
TEST_F(FileCacheE2ETest, SkipFromMidSegmentAcrossBoundary)
{
    const size_t n = 512 * 1024;
    const size_t seg = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Pre-warm the whole region so every segment is DOWNLOADED/CACHED. The bug is
    // on the cache-HIT path (updateCurrentReaderIfNeeded is a no-op for a
    // DOWNLOADED CACHED segment), so a fresh stream over cached segments is what
    // exposes the desync.
    {
        auto warm = makeInput(cache, path, key);
        auto ws = warm->enqueue({0, n});
        ASSERT_EQ(readAll(*ws), content);
    }

    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({0, n});

    // Read only HALF of segment 0 -> stop in the middle, boundary not reached.
    auto head = readN(*stream, seg / 2);
    ASSERT_EQ(head, content.substr(0, seg / 2));

    // Skip from mid seg0 into seg1: distance > remaining published buffer and
    // crossing the segment boundary. Target absolute offset = seg/2 + skip.
    const int64_t skip = seg; // lands seg/2 into segment 1
    ASSERT_TRUE(stream->SkipInt64(skip));
    const size_t target = seg / 2 + skip;
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(target));

    // The next real read must serve the correct absolute bytes (this is where
    // the pre-fix drift/early-EOF manifested).
    auto tail = readN(*stream, 4096);
    EXPECT_EQ(tail, content.substr(target, 4096));
}

// ============================================================================
// 5c. SkipMidSegmentAcrossTwoSegments: skip from the middle of segment 0 all the
// way past segment 1 into segment 2, again after reading only part of segment 0.
// ============================================================================
TEST_F(FileCacheE2ETest, SkipMidSegmentAcrossTwoSegments)
{
    const size_t n = 512 * 1024;
    const size_t seg = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    {
        auto warm = makeInput(cache, path, key);
        auto ws = warm->enqueue({0, n});
        ASSERT_EQ(readAll(*ws), content);
    }

    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({0, n});

    auto head = readN(*stream, seg / 2);
    ASSERT_EQ(head, content.substr(0, seg / 2));

    // seg/2 + 2*seg lands in segment 2.
    const int64_t skip = 2 * static_cast<int64_t>(seg);
    ASSERT_TRUE(stream->SkipInt64(skip));
    const size_t target = seg / 2 + skip;
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(target));

    auto tail = readN(*stream, 4096);
    EXPECT_EQ(tail, content.substr(target, 4096));
}

// ============================================================================
// 5d. ConsecutiveSkipsAcrossBoundaries: skip across a boundary, read a little,
// then skip across another boundary. Exercises repeated invalidate/re-derive.
// ============================================================================
TEST_F(FileCacheE2ETest, ConsecutiveSkipsAcrossBoundaries)
{
    const size_t n = 512 * 1024;
    const size_t seg = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    {
        auto warm = makeInput(cache, path, key);
        auto ws = warm->enqueue({0, n});
        ASSERT_EQ(readAll(*ws), content);
    }

    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({0, n});

    // Read part of segment 0.
    ASSERT_EQ(readN(*stream, seg / 2), content.substr(0, seg / 2));

    // First cross-boundary skip: land mid segment 1.
    ASSERT_TRUE(stream->SkipInt64(seg));
    size_t pos = seg / 2 + seg;
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(pos));

    // Read a little at the new position.
    ASSERT_EQ(readN(*stream, 1024), content.substr(pos, 1024));
    pos += 1024;

    // Second cross-boundary skip from mid buffer into a later segment.
    ASSERT_TRUE(stream->SkipInt64(seg));
    pos += seg;
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(pos));

    auto tail = readN(*stream, 4096);
    EXPECT_EQ(tail, content.substr(pos, 4096));
}

// ============================================================================
// 6. SeekToPositionRegionRelative: seek uses region-relative coordinates.
// ============================================================================
TEST_F(FileCacheE2ETest, SeekToPositionRegionRelative)
{
    const size_t base = 4096;
    const size_t len = 8192;
    const size_t n = base + len;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);
    // Region begins at absolute offset 4096.
    auto stream = input->enqueue({base, len});

    // Seek to region-relative position 256.
    std::vector<uint64_t> positions{256};
    dwio::common::PositionProvider pp(positions);
    stream->seekToPosition(pp);
    // ByteCount is region-relative == 256 immediately after the seek.
    EXPECT_EQ(stream->ByteCount(), 256);

    // The bytes come from absolute file offset base + 256.
    auto out = readN(*stream, 128);
    EXPECT_EQ(out, content.substr(base + 256, 128));
}

// ============================================================================
// 7. NonzeroRegionOffsetAbsoluteCoordinates: region {65536, 32768}.
// ============================================================================
TEST_F(FileCacheE2ETest, NonzeroRegionOffsetAbsoluteCoordinates)
{
    const size_t base = 65536;
    const size_t len = 32768;
    const size_t n = base + len;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);
    auto stream = input->enqueue({base, len});

    // ByteCount starts region-relative at 0.
    EXPECT_EQ(stream->ByteCount(), 0);
    auto out = readAll(*stream);
    // Data is the absolute [65536, 98304) window, NOT [0, 32768).
    EXPECT_EQ(out, content.substr(base, len));
    EXPECT_NE(out, content.substr(0, len));
    EXPECT_EQ(stream->ByteCount(), static_cast<int64_t>(len));
}

// ============================================================================
// 8. DiscardedEnqueueNoUseAfterFree: discard stream then load(), no fault.
// ============================================================================
TEST_F(FileCacheE2ETest, DiscardedEnqueueNoUseAfterFree)
{
    const size_t n = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 16 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    // Discard the enqueue result BEFORE any Next().
    { auto stream = input->enqueue({0, 32 * 1024}); }
    // load() must not dereference the discarded stream or create a segment.
    input->load(dwio::common::LogType::FILE);
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
}

// ============================================================================
// 9. LoadIsNopPlanningBarrier: load() dereferences no stream pointer.
// ============================================================================
TEST_F(FileCacheE2ETest, LoadIsNopPlanningBarrier)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 16 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    // Enqueue three regions and discard all three streams.
    { auto s0 = input->enqueue({0, 16 * 1024}); }
    { auto s1 = input->enqueue({16 * 1024, 16 * 1024}); }
    { auto s2 = input->enqueue({32 * 1024, 16 * 1024}); }
    // load() must be a no-op planning barrier: no crash, no segment created.
    input->load(dwio::common::LogType::FILE);
    EXPECT_EQ(cache->getFileSegmentsNum(), 0u);
}

// ============================================================================
// 10. DWRFShouldPrefetchStripesIsFalse: DWRF stripe-metadata guard flags.
// ============================================================================
TEST_F(FileCacheE2ETest, DWRFShouldPrefetchStripesIsFalse)
{
    const size_t n = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 16 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    // These must be false so DWRF's StripeMetadataCache never hard-casts the
    // stream to CacheInputStream and never treats it as preloaded.
    EXPECT_FALSE(input->shouldPrefetchStripes());
    EXPECT_FALSE(input->preloaded());
    EXPECT_FALSE(input->hasCache());
}

// ============================================================================
// 11. PathOnlyKeyWhenEtagEmpty: empty etag -> path key; re-derive hits.
// ============================================================================
TEST_F(FileCacheE2ETest, PathOnlyKeyWhenEtagEmpty)
{
    const size_t n = 100 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);

    FileCacheFileIdentity id{path, /*etag*/ ""};
    auto key = FileCacheFileIdentity::deriveKey(id);
    EXPECT_EQ(key, FileCacheKey::fromPath(path));

    // Fill the cache through the path-only key.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->enqueue({0, n});
        EXPECT_EQ(readAll(*stream), content);
    }

    // Re-derive the SAME key from the same path+empty-etag and hit the cache.
    auto key2 = FileCacheFileIdentity::deriveKey(FileCacheFileIdentity{path, ""});
    EXPECT_EQ(key2, key);
    {
        auto input = makeInput(cache, path, key2);
        EXPECT_TRUE(input->isBuffered(0, n));
        auto stream = input->enqueue({0, n});
        EXPECT_EQ(readAll(*stream), content);
    }
}

// ============================================================================
// 12. DifferentEtagsDifferentKeys: v1/v2 map to separate segments.
// ============================================================================
TEST_F(FileCacheE2ETest, DifferentEtagsDifferentKeys)
{
    const size_t n = 64 * 1024;
    // Two DIFFERENT source payloads reachable at the same logical path but under
    // distinct etags; each key must serve its own bytes, never the other's.
    auto contentV1 = makeContent(n);
    auto contentV2 = makeContent(n);
    for (size_t i = 0; i < n; ++i)
        contentV2[i] = static_cast<char>(contentV2[i] ^ 0x5A);

    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto pathV1 = writeSourceFile("srcV1", contentV1);
    auto pathV2 = writeSourceFile("srcV2", contentV2);

    auto keyV1 = FileCacheFileIdentity::deriveKey(FileCacheFileIdentity{"/logical/path", "v1"});
    auto keyV2 = FileCacheFileIdentity::deriveKey(FileCacheFileIdentity{"/logical/path", "v2"});
    EXPECT_NE(keyV1, keyV2);

    // Fill both caches.
    {
        auto input = makeInput(cache, pathV1, keyV1);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), contentV1);
    }
    {
        auto input = makeInput(cache, pathV2, keyV2);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), contentV2);
    }

    // Each key hits its own segment, not the other's: re-reading through the
    // etag-v1 key returns v1 bytes even if the source is now the v2 file.
    {
        auto input = makeInput(cache, pathV2, keyV1); // source is v2, key is v1
        EXPECT_TRUE(input->isBuffered(0, n));
        EXPECT_EQ(readAll(*input->enqueue({0, n})), contentV1);
    }
    {
        auto input = makeInput(cache, pathV1, keyV2); // source is v1, key is v2
        EXPECT_TRUE(input->isBuffered(0, n));
        EXPECT_EQ(readAll(*input->enqueue({0, n})), contentV2);
    }
}

// ============================================================================
// 13. ShutdownWhileStreamAliveNotReading: shutdown with an idle live stream.
// ============================================================================
TEST_F(FileCacheE2ETest, ShutdownWhileStreamAliveNotReading)
{
    const size_t n = 128 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 16 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);
    auto input = makeInput(cache, path, key);

    // Hold a stream alive WITHOUT ever calling Next() (no downloader acquired,
    // no holder built — the holder is lazy).
    auto stream = input->enqueue({0, 64 * 1024});

    // Manager shutdown must complete without deadlock or crash while the stream
    // is alive but idle.
    manager_->shutdown();

    // The held stream and its owning input destruct here without further reads.
    stream.reset();
    input.reset();
    SUCCEED();
}

// ============================================================================
// CH integration-test migration coverage (through the assembled public path).
// ============================================================================

// cold miss -> cache fill -> later hit (test_filesystem_cache basic path).
TEST_F(FileCacheE2ETest, ColdMissFillThenHit)
{
    const size_t n = 200 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    {
        auto input = makeInput(cache, path, key);
        EXPECT_FALSE(input->isBuffered(0, n)); // cold
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
    {
        auto input = makeInput(cache, path, key);
        EXPECT_TRUE(input->isBuffered(0, n)); // hit
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
}

// partial segment continuation across readers (Q1 fills a prefix; Q2 continues).
TEST_F(FileCacheE2ETest, PartialSegmentContinuationAcrossReaders)
{
    const size_t n = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1); // single segment
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Reader 1 reads only a prefix, then is destroyed.
    {
        auto input = makeInput(cache, path, key);
        auto stream = input->enqueue({0, n});
        EXPECT_EQ(readN(*stream, 8 * 1024), content.substr(0, 8 * 1024));
    }
    // Reader 2 reads the full range: the prefix is served from cache, the rest
    // continues downloading from the segment write offset.
    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }
}

// reserve-ahead / downloaded-size accounting at the public boundary.
TEST_F(FileCacheE2ETest, DownloadedSizeAccountingAtPublicBoundary)
{
    const size_t n = 128 * 1024;
    const size_t seg = 64 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ seg, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n})), content);
    }

    // At the public FileCache boundary, the fully-read range is now downloaded:
    // every segment reports a downloaded prefix covering its range, summing to n.
    auto holder = cache->get(key, 0, n, /*batch*/ 0, manager_->commonUserId());
    ASSERT_TRUE(holder && !holder->empty());
    size_t downloadedTotal = 0;
    for (const auto & segPtr : *holder)
        downloadedTotal += segPtr->getDownloadedSize();
    EXPECT_EQ(downloadedTotal, n);
    EXPECT_TRUE(makeInput(cache, path, key)->isBuffered(0, n));
}

// random seeks across hit / miss / bypass paths on one file.
TEST_F(FileCacheE2ETest, RandomSeeksAcrossHitMissBypass)
{
    const size_t n = 512 * 1024;
    auto content = makeContent(n);
    auto cache = makeManagerCache(/*seg*/ 64 * 1024, /*align*/ 1);
    auto path = writeSourceFile("src", content);
    auto key = FileCacheKey::fromPath(path);

    // Pre-fill the first half so subsequent seeks hit a mix of cached and cold.
    {
        auto input = makeInput(cache, path, key);
        EXPECT_EQ(readAll(*input->enqueue({0, n / 2})), content.substr(0, n / 2));
    }

    const std::vector<size_t> offsets{0, 300 * 1024, 100 * 1024, 450 * 1024, 40 * 1024, 511 * 1024};
    // Cache-put path: each seek reads a 64-byte window from the correct offset,
    // mixing hits (first half) and misses (second half).
    for (size_t off : offsets)
    {
        const size_t len = std::min<size_t>(64, n - off);
        auto input = makeInput(cache, path, key);
        auto stream = input->enqueue({off, len});
        EXPECT_EQ(readAll(*stream), content.substr(off, len)) << "put path @ " << off;
    }

    // Bypass path: same offsets, readIfExistsOtherwiseBypass -> always correct
    // bytes regardless of cache state, creating no new segments beyond the fill.
    FileCacheReadOptions bypass;
    bypass.readIfExistsOtherwiseBypass = true;
    for (size_t off : offsets)
    {
        const size_t len = std::min<size_t>(64, n - off);
        auto input = makeInput(cache, path, key, bypass);
        auto stream = input->enqueue({off, len});
        EXPECT_EQ(readAll(*stream), content.substr(off, len)) << "bypass path @ " << off;
    }
}

} // namespace
} // namespace facebook::velox::ch
