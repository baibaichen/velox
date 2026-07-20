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
#include "velox/ch/Interpreters/FileCache/FileSegment.h"

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <folly/CancellationToken.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/system/ThreadId.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

namespace fs = std::filesystem;

// ===========================================================================
// Part 1: constant-state / range / settings / caller-identity contracts.
//
// These exercise real production code (the out-of-line `FileSegment::Range`
// constructor and `FileSegment::getCallerId`). They link once `FileSegment.o`
// is part of a linkable binary, i.e. together with the rest of the center SCC.
// ===========================================================================

// Range is inclusive [left, right].
TEST(RangeTest, SizeIsRightMinusLeftPlusOne)
{
    FileSegment::Range r{10, 19};
    EXPECT_EQ(r.size(), 10ULL);
    EXPECT_EQ(r.left, 10ULL);
    EXPECT_EQ(r.right, 19ULL);
}

TEST(RangeTest, SingleByteRangeHasSizeOne)
{
    FileSegment::Range r{7, 7};
    EXPECT_EQ(r.size(), 1ULL);
    EXPECT_TRUE(r.contains(7));
}

TEST(RangeTest, ContainsPoint)
{
    FileSegment::Range r{5, 10};
    EXPECT_TRUE(r.contains(size_t{5}));
    EXPECT_TRUE(r.contains(size_t{10}));
    EXPECT_FALSE(r.contains(size_t{4}));
    EXPECT_FALSE(r.contains(size_t{11}));
}

TEST(RangeTest, ContainsRange)
{
    FileSegment::Range outer{0, 100};
    FileSegment::Range inner{10, 50};
    FileSegment::Range overlap{80, 110};
    EXPECT_TRUE(outer.contains(inner));
    EXPECT_FALSE(outer.contains(overlap));
}

TEST(RangeTest, StrictOrderingNonOverlapping)
{
    FileSegment::Range a{0, 9};
    FileSegment::Range b{10, 19};
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
    EXPECT_EQ(a, (FileSegment::Range{0, 9}));
}

TEST(RangeTest, InvertedRangeThrows)
{
    // Range enforces left <= right in its constructor (LOGICAL_ERROR in CH).
    EXPECT_ANY_THROW((FileSegment::Range{10, 9}));
}

TEST(CreateFileSegmentSettingsTest, RegularIsBounded)
{
    CreateFileSegmentSettings s;
    EXPECT_EQ(s.kind, FileSegmentKind::Regular);
    EXPECT_FALSE(s.unbounded);
}

TEST(CreateFileSegmentSettingsTest, EphemeralIsUnbounded)
{
    CreateFileSegmentSettings s{FileSegmentKind::Ephemeral};
    EXPECT_EQ(s.kind, FileSegmentKind::Ephemeral);
    EXPECT_TRUE(s.unbounded);
}

TEST(StateToStringTest, CoversEveryState)
{
    EXPECT_EQ(FileSegment::stateToString(FileSegment::State::DOWNLOADED), "DOWNLOADED");
    EXPECT_EQ(FileSegment::stateToString(FileSegment::State::EMPTY), "EMPTY");
    EXPECT_EQ(FileSegment::stateToString(FileSegment::State::DOWNLOADING), "DOWNLOADING");
    EXPECT_EQ(FileSegment::stateToString(FileSegment::State::PARTIALLY_DOWNLOADED), "PARTIALLY DOWNLOADED");
    EXPECT_EQ(
        FileSegment::stateToString(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION),
        "PARTIALLY DOWNLOADED NO CONTINUATION");
    EXPECT_EQ(FileSegment::stateToString(FileSegment::State::DETACHED), "DETACHED");
}

TEST(CallerIdTest, SameScopeStableId)
{
    FileCacheQueryIdScope scope("q1");
    auto id1 = FileSegment::getCallerId();
    auto id2 = FileSegment::getCallerId();
    EXPECT_EQ(id1, id2);
    // With an active query scope, the id is "<query-id>:<tid>", not the
    // background "None:<tid>" shape.
    EXPECT_EQ(id1.rfind("q1:", 0), 0u);
    EXPECT_NE(id1, "None:" + std::to_string(folly::getOSThreadID()));
}

TEST(CallerIdTest, NoScopeBackgroundId)
{
    // Without a query scope, caller is "None:<tid>".
    auto id = FileSegment::getCallerId();
    EXPECT_EQ(id.rfind("None:", 0), 0u);
    EXPECT_EQ(id, "None:" + std::to_string(folly::getOSThreadID()));
}

TEST(CallerIdTest, DifferentThreadsHaveDifferentIds)
{
    FileCacheQueryIdScope scope("shared-query");
    auto mainId = FileSegment::getCallerId();

    std::string otherId;
    std::thread t([&]
    {
        // Same query id, different physical thread => different caller id
        // (the downloader lease is per physical thread, see design 09).
        FileCacheQueryIdScope innerScope("shared-query");
        otherId = FileSegment::getCallerId();
    });
    t.join();

    EXPECT_EQ(mainId.rfind("shared-query:", 0), 0u);
    EXPECT_EQ(otherId.rfind("shared-query:", 0), 0u);
    EXPECT_NE(mainId, otherId);
}

// ===========================================================================
// Part 2: real-file-backed WriteFile doubles.
//
// The default double appends caller bytes to a real on-disk file. The
// limited-budget double commits a strict prefix and then raises a chosen
// exception, simulating a disk-full short write WITHOUT doing any
// reconciliation itself: all reconciliation stays in production
// FileSegment::write.
// ===========================================================================

enum class ThrowKind
{
    None,
    ErrnoEnospc,
    ErrnoEdquot,
    ErrnoEio,
    Generic,
};

class TestBackedWriteFile : public velox::WriteFile
{
public:
    TestBackedWriteFile(std::string path, bool append, uint64_t byteBudget, ThrowKind throwKind)
        : path_(std::move(path)), byteBudget_(byteBudget), throwKind_(throwKind)
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
            // Mirror the production "create-new" contract: an already-existing
            // path is an error (CH opens with shouldThrowOnFileAlreadyExists),
            // so the double never silently truncates a pre-existing file.
            std::error_code ec;
            if (fs::exists(path_, ec))
                VELOX_FAIL("TestBackedWriteFile: create-new for an existing path: {}", path_);
            out_.open(path_, std::ios::binary | std::ios::out | std::ios::trunc);
        }
    }

    void append(std::string_view data) override
    {
        const uint64_t remaining = byteBudget_ > written_ ? byteBudget_ - written_ : 0;
        const uint64_t toWrite = std::min<uint64_t>(remaining, data.size());
        if (toWrite > 0)
        {
            // Physically commit the (possibly strict) prefix to the real file.
            out_.write(data.data(), static_cast<std::streamsize>(toWrite));
            out_.flush();
            written_ += toWrite;
        }
        if (toWrite < data.size())
            raise();
    }

    void flush() override { out_.flush(); }

    void close() override
    {
        out_.flush();
        out_.close();
    }

    uint64_t size() const override { return written_; }

    const std::string getName() const override { return path_; }

private:
    [[noreturn]] void raise() const
    {
        switch (throwKind_)
        {
            case ThrowKind::ErrnoEnospc:
                throw FileCacheErrnoException(__FILE__, __LINE__, __FUNCTION__, "No space left on device", ENOSPC);
            case ThrowKind::ErrnoEdquot:
                throw FileCacheErrnoException(__FILE__, __LINE__, __FUNCTION__, "Disk quota exceeded", EDQUOT);
            case ThrowKind::ErrnoEio:
                throw FileCacheErrnoException(__FILE__, __LINE__, __FUNCTION__, "Input/output error", EIO);
            case ThrowKind::Generic:
                VELOX_FAIL("Simulated non-errno cache write failure");
            case ThrowKind::None:
                break;
        }
        VELOX_FAIL("TestBackedWriteFile::raise called without a throw kind");
    }

    std::string path_;
    std::ofstream out_;
    uint64_t byteBudget_;
    uint64_t written_{0};
    ThrowKind throwKind_;
};

// ===========================================================================
// Part 3: production-path fixture over a real temporary FileCache.
//
// These cases drive the real FileSegment state machine (downloader lease,
// reserve/write, wait/cancellation, completion, detach, holder cleanup, the
// typed-errno reconciliation). They construct a real FileCache through the
// manager-injected constructor and therefore link only once Metadata.cpp and
// FileCache.cpp exist (the final SCC attempt). They are authored here in full
// so the SCC continuation links and runs them unchanged.
// ===========================================================================

class FileSegmentTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("filesegment-test");
        cacheDir_ = velox::common::testutil::TempDirectoryPath::create();

        FileCacheConfig config;
        config.path = cacheDir_->getPath();
        config.maxSize = 64ull << 20;
        config.maxFileSegmentSize = 1ull << 20;
        config.boundaryAlignment = 4096;
        config.reserveGranularity = 0;
        // The 4-thread pool must fit the metadata-load workers (FileCache no longer
        // resizes the shared pool, it verifies capacity fail-closed).
        config.loadMetadataThreads = 2;

        cache_ = std::make_unique<FileCache>(
            "test",
            config,
            scheduler_,
            workerPool_,
            pool_.get(),
            origin_,
            makeWriteFactory(),
            makeReadFactory(),
            [](const std::string &) {});
        cache_->initialize();
    }

    void TearDown() override
    {
        // Complete holders against a still-live cache, then quiesce it, in a
        // deterministic order that does not depend on member destruction order.
        holders_.clear();
        if (cache_)
        {
            cache_->deactivateBackgroundOperations();
            cache_.reset();
        }
    }

    FileSegmentPtr acquireEmptySegment(size_t size)
    {
        auto holder = cache_->getOrSet(
            key_, /*offset*/ 0, size, /*file_size*/ size, CreateFileSegmentSettings{}, /*file_segments_limit*/ 0, origin_);
        holders_.push_back(std::move(holder));
        return holders_.back()->getSingleFileSegment();
    }

    FileSegmentPtr acquireDownloadingSegment(size_t size, size_t reserve_size)
    {
        auto segment = acquireEmptySegment(size);
        EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
        std::string failure_reason;
        EXPECT_TRUE(segment->reserve(reserve_size, /*lock_wait_timeout_ms*/ 1000, failure_reason));
        return segment;
    }

    FileCache::CacheWriteFileFactory makeWriteFactory()
    {
        return [this](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
        {
            if (writeHook_)
                return writeHook_(path, append);
            return std::make_unique<TestBackedWriteFile>(
                path, append, std::numeric_limits<uint64_t>::max(), ThrowKind::None);
        };
    }

    FileCache::CacheReadFileFactory makeReadFactory()
    {
        // The remote reader is not exercised by these FileSegment write-path
        // tests; return an empty reader.
        return [](const std::string &) -> std::shared_ptr<velox::ReadFile> { return nullptr; };
    }

    std::shared_ptr<folly::ManualTimekeeper> timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    FileCacheWorkerPool workerPool_{4, 1, "fs-test"};
    FileCacheScheduler scheduler_{timekeeper_, workerPool_};
    std::function<std::unique_ptr<velox::WriteFile>(const std::string &, bool)> writeHook_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<velox::common::testutil::TempDirectoryPath> cacheDir_;
    FileCacheOriginInfo origin_{"test-user", 0};
    FileCacheKey key_ = FileCacheKey::random();
    std::unique_ptr<FileCache> cache_;
    std::vector<FileSegmentsHolderPtr> holders_;
};

// -- downloader election / lease / caller identity --------------------------

TEST_F(FileSegmentTest, DownloaderElectionAndLease)
{
    auto segment = acquireEmptySegment(100);
    EXPECT_EQ(segment->state(), FileSegment::State::EMPTY);
    EXPECT_TRUE(segment->getDownloader().empty());

    const auto downloader = segment->getOrSetDownloader();
    EXPECT_EQ(downloader, FileSegment::getCallerId());
    EXPECT_EQ(segment->state(), FileSegment::State::DOWNLOADING);
    EXPECT_TRUE(segment->isDownloader());

    // A second election by the same caller returns the same lease holder.
    EXPECT_EQ(segment->getOrSetDownloader(), downloader);
}

TEST_F(FileSegmentTest, OnlyDownloaderCanReserve)
{
    auto segment = acquireEmptySegment(100);

    {
        FileCacheQueryIdScope downloaderScope("downloader-query");
        ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

        {
            // A different caller identity (different query scope on this thread) is
            // not the downloader and must not be able to reserve.
            FileCacheQueryIdScope otherScope("other-query");
            std::string failure_reason;
            EXPECT_ANY_THROW(segment->reserve(10, /*lock_wait_timeout_ms*/ 1000, failure_reason));
        }

        // Release the lease while still in the downloader scope so the holder can be
        // completed cleanly at teardown (a downloader elected under a transient scope
        // otherwise leaves an unresettable DOWNLOADING segment).
        segment->resetDownloader();
    }
}

TEST_F(FileSegmentTest, ResetDownloaderReturnsSegmentToEmpty)
{
    auto segment = acquireEmptySegment(100);
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
    ASSERT_EQ(segment->state(), FileSegment::State::DOWNLOADING);

    segment->resetDownloader();

    // No bytes downloaded yet, so the segment returns to EMPTY and the lease is
    // released for another physical thread to acquire.
    EXPECT_EQ(segment->state(), FileSegment::State::EMPTY);
    EXPECT_TRUE(segment->getDownloader().empty());
    EXPECT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
}

// -- write: happy path + writer lifecycle -----------------------------------

TEST_F(FileSegmentTest, WriteAppendsAndAdvancesDownloadedSize)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    std::string chunk(40, 'a');
    segment->write(chunk.data(), chunk.size(), segment->getCurrentWriteOffset());

    EXPECT_EQ(segment->getDownloadedSize(), 40u);
    EXPECT_EQ(segment->getCurrentWriteOffset(), 40u);
    EXPECT_EQ(fs::file_size(segment->getPath()), 40u);
}

TEST_F(FileSegmentTest, WriteRejectsZeroSize)
{
    auto segment = acquireDownloadingSegment(100, 100);
    char byte = 'x';
    EXPECT_ANY_THROW(segment->write(&byte, 0, segment->getCurrentWriteOffset()));

    // Complete the download so teardown finds a consistent DOWNLOADED segment.
    std::string full(100, 'x');
    segment->write(full.data(), full.size(), segment->getCurrentWriteOffset());
}

TEST_F(FileSegmentTest, WriteRejectsWrongOffset)
{
    auto segment = acquireDownloadingSegment(100, 100);
    std::string chunk(10, 'a');
    // current write offset is 0; writing at offset 5 must be rejected.
    EXPECT_ANY_THROW(segment->write(chunk.data(), chunk.size(), 5));

    // Complete the download so teardown finds a consistent DOWNLOADED segment.
    std::string full(100, 'a');
    segment->write(full.data(), full.size(), segment->getCurrentWriteOffset());
}

TEST_F(FileSegmentTest, WriteRejectsBeyondRange)
{
    auto segment = acquireDownloadingSegment(/*size*/ 10, /*reserve*/ 10);
    std::string chunk(20, 'a');
    EXPECT_ANY_THROW(segment->write(chunk.data(), chunk.size(), 0));

    // Complete the download so teardown finds a consistent DOWNLOADED segment.
    std::string full(10, 'a');
    segment->write(full.data(), full.size(), segment->getCurrentWriteOffset());
}

// -- remote file reader handoff (set/stash, get, extract, reset/detach) -----
//
// `getRemoteFileReader`/`setRemoteFileReader`/`resetRemoteFileReader` are
// downloader-only (gated by `assertIsDownloaderUnlocked`), while
// `extractRemoteFileReader` is gated only on `download_state` (see the
// production comment above `setDownloadFinishedWithoutContinuation`). These
// tests drive the real `FileSegment` API with a real
// `ReadBufferFromVeloxReadFile` over an in-memory `velox::InMemoryReadFile`
// (buffer bookkeeping only; the adapter's own read/attach/detach semantics
// are covered by `IoAdaptersTest`, not repeated here).

TEST_F(FileSegmentTest, SetRemoteFileReaderStashesRealAdapterForDownloader)
{
    // Elect a downloader without reserving: the remote-reader handoff methods
    // are gated only on downloader identity, not on reserved space, and
    // avoiding `reserve` here keeps this focused on the handoff (a segment
    // that reserves and is abandoned without any write is out of this test's
    // scope; see `acquireDownloadingSegment` usage in the write-path tests).
    auto segment = acquireEmptySegment(64);
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_EQ(segment->getRemoteFileReader(), nullptr);

    auto readFile = std::make_shared<velox::InMemoryReadFile>(std::string(64, 'r'));
    auto reader = std::make_shared<ReadBufferFromVeloxReadFile>(readFile, pool_.get());
    segment->setRemoteFileReader(reader);

    // get identity: the exact stashed object comes back, not a copy.
    EXPECT_EQ(segment->getRemoteFileReader(), reader);

    // Prove it is the real, functional adapter (not a stub): reading through it
    // returns the real underlying bytes.
    char scratch[64];
    reader->set(scratch, sizeof(scratch));
    ASSERT_TRUE(reader->next());
    EXPECT_EQ(std::string(reader->buffer().begin(), reader->buffer().size()), std::string(64, 'r'));

    segment->resetRemoteFileReader();
    EXPECT_EQ(segment->getRemoteFileReader(), nullptr);
}

TEST_F(FileSegmentTest, SetRemoteFileReaderRejectsDoubleSet)
{
    auto segment = acquireEmptySegment(32);
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    auto readFile1 = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'a'));
    auto reader1 = std::make_shared<ReadBufferFromVeloxReadFile>(readFile1, pool_.get());
    segment->setRemoteFileReader(reader1);

    auto readFile2 = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'b'));
    auto reader2 = std::make_shared<ReadBufferFromVeloxReadFile>(readFile2, pool_.get());
    EXPECT_ANY_THROW(segment->setRemoteFileReader(reader2));

    // The rejected second set left the first stashed reader untouched.
    EXPECT_EQ(segment->getRemoteFileReader(), reader1);

    segment->resetRemoteFileReader();
}

TEST_F(FileSegmentTest, RemoteFileReaderAccessRequiresDownloaderIdentity)
{
    auto segment = acquireEmptySegment(32);

    FileCacheQueryIdScope downloaderScope("downloader-query");
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    auto readFile = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'c'));
    auto reader = std::make_shared<ReadBufferFromVeloxReadFile>(readFile, pool_.get());
    segment->setRemoteFileReader(reader);

    {
        // A different caller identity (different query scope on this thread) is
        // not the downloader: every get/set/reset handoff method must reject it,
        // not just `reserve` (mirrors `OnlyDownloaderCanReserve`).
        FileCacheQueryIdScope otherScope("other-query");
        EXPECT_ANY_THROW(segment->getRemoteFileReader());
        EXPECT_ANY_THROW(segment->setRemoteFileReader(reader));
        EXPECT_ANY_THROW(segment->resetRemoteFileReader());
    }

    // Release the lease while still the downloader for a clean teardown.
    segment->resetRemoteFileReader();
    segment->resetDownloader();
}

TEST_F(FileSegmentTest, ExtractRemoteFileReaderOnlyInTerminalOrPartialNoContinuationState)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    std::string prefix(40, 'a');
    segment->write(prefix.data(), prefix.size(), segment->getCurrentWriteOffset());

    auto readFile = std::make_shared<velox::InMemoryReadFile>(std::string(100, 'z'));
    auto reader = std::make_shared<ReadBufferFromVeloxReadFile>(readFile, pool_.get());
    segment->setRemoteFileReader(reader);

    // Invalid state: DOWNLOADING is neither DOWNLOADED nor
    // PARTIALLY_DOWNLOADED_NO_CONTINUATION, so extraction must not hand out
    // the reader while the download is still in flight, even though one is
    // stashed.
    ASSERT_EQ(segment->state(), FileSegment::State::DOWNLOADING);
    EXPECT_EQ(segment->extractRemoteFileReader(), nullptr);

    // The downloader must withdraw the reader before publishing
    // PARTIALLY_DOWNLOADED_NO_CONTINUATION (production precondition of
    // `setDownloadFinishedWithoutContinuation`).
    segment->resetRemoteFileReader();
    segment->setDownloadFinishedWithoutContinuation();
    ASSERT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

    // The segment's remote reader is now up for grabs: the same downloader
    // lease may stash it again once the state has published.
    segment->setRemoteFileReader(reader);

    FileSegment::RemoteFileReaderPtr extracted;
    {
        // Extraction is gated only on `download_state`, not on being the
        // downloader (unlike get/set/reset above): a different caller identity
        // can still extract once the state is valid.
        FileCacheQueryIdScope otherScope("some-other-caller");
        extracted = segment->extractRemoteFileReader();
    }
    EXPECT_EQ(extracted, reader);

    // Extraction moves the reader out: a second extraction finds nothing left.
    EXPECT_EQ(segment->extractRemoteFileReader(), nullptr);
    EXPECT_EQ(segment->getRemoteFileReader(), nullptr);

    segment->completePartAndResetDownloader();
}

TEST_F(FileSegmentTest, ResetRemoteFileReaderClearsStashWithoutAffectingDownloaderLease)
{
    auto segment = acquireEmptySegment(32);
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    auto readFile = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'd'));
    auto reader = std::make_shared<ReadBufferFromVeloxReadFile>(readFile, pool_.get());
    segment->setRemoteFileReader(reader);
    ASSERT_EQ(segment->getRemoteFileReader(), reader);

    segment->resetRemoteFileReader();
    EXPECT_EQ(segment->getRemoteFileReader(), nullptr);

    // The downloader lease itself is untouched by resetting the reader: the
    // same caller remains the downloader.
    EXPECT_TRUE(segment->isDownloader());
    EXPECT_EQ(segment->state(), FileSegment::State::DOWNLOADING);

    // The stash is not one-shot: a fresh reader can be set again after reset.
    auto readFile2 = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'e'));
    auto reader2 = std::make_shared<ReadBufferFromVeloxReadFile>(readFile2, pool_.get());
    segment->setRemoteFileReader(reader2);
    EXPECT_EQ(segment->getRemoteFileReader(), reader2);

    segment->resetRemoteFileReader();
}

TEST_F(FileSegmentTest, RemoteFileReaderAccessRejectedAfterDetach)
{
    auto segment = acquireEmptySegment(32);
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());

    auto readFile = std::make_shared<velox::InMemoryReadFile>(std::string(32, 'f'));
    auto reader = std::make_shared<ReadBufferFromVeloxReadFile>(readFile, pool_.get());
    segment->setRemoteFileReader(reader);

    auto key_metadata = segment->getKeyMetadata();
    {
        auto locked_key = key_metadata->lock();
        auto segment_lock = segment->lock();
        segment->detach(segment_lock, *locked_key);
    }
    ASSERT_TRUE(segment->isDetached());

    // Detach clears the downloader lease and destroys the stashed reader
    // (`setDetachedState` resets `download_data`), so every downloader-gated
    // handoff method must reject use after detach.
    EXPECT_ANY_THROW(segment->getRemoteFileReader());
    EXPECT_ANY_THROW(segment->setRemoteFileReader(reader));
    EXPECT_ANY_THROW(segment->resetRemoteFileReader());
}

// -- partial-file resume ----------------------------------------------------

TEST_F(FileSegmentTest, PartialFileResumeAppendsWithoutTruncatingPrefix)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    // First downloader writes a 40-byte prefix, then relinquishes the lease
    // without finishing the segment.
    std::string prefix(40, 'a');
    segment->write(prefix.data(), prefix.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), 40u);
    ASSERT_EQ(fs::file_size(segment->getPath()), 40u);

    segment->completePartAndResetDownloader();
    EXPECT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED);

    // A new download attempt re-elects a downloader and continues appending
    // through the real write path over the existing partial file.
    ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_EQ(segment->getCurrentWriteOffset(), 40u);
    EXPECT_EQ(segment->getDownloadedSize(), 40u);

    std::string suffix(60, 'b');
    segment->write(suffix.data(), suffix.size(), segment->getCurrentWriteOffset());

    // The existing 40-byte prefix must be preserved (append, not truncate), and
    // downloaded/physical sizes stay consistent.
    EXPECT_EQ(segment->getDownloadedSize(), 100u);
    EXPECT_EQ(fs::file_size(segment->getPath()), 100u);

    std::ifstream in(segment->getPath(), std::ios::binary);
    std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_EQ(contents.size(), 100u);
    EXPECT_EQ(contents.substr(0, 40), prefix);
    EXPECT_EQ(contents.substr(40), suffix);
}

// -- typed-errno reconciliation (positive) ----------------------------------

TEST_F(FileSegmentTest, TypedEnospcReconcilesDownloadedSizeToPhysicalPrefix)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    // The injected writer accepts up to 50 bytes total; the first write of 40
    // succeeds, the second write of 30 commits a strict 10-byte prefix (bringing
    // the file to 50 bytes on disk) and then throws a typed ENOSPC.
    writeHook_ = [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<TestBackedWriteFile>(path, append, /*byteBudget*/ 50, ThrowKind::ErrnoEnospc);
    };

    std::string first(40, 'a');
    segment->write(first.data(), first.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), 40u);

    std::string second(30, 'b');
    EXPECT_THROW(
        segment->write(second.data(), second.size(), segment->getCurrentWriteOffset()),
        FileCacheErrnoException);

    // Production FileSegment::write read filesystem::file_size, enforced
    // downloadedSize <= physicalSize <= reservedSize, and reconciled
    // downloaded_size to the physical prefix (40 + 10 = 50). The failure is
    // published and reserved-but-unwritten bytes are never counted.
    EXPECT_EQ(fs::file_size(segment->getPath()), 50u);
    EXPECT_EQ(segment->getDownloadedSize(), 50u);
    EXPECT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
}

TEST_F(FileSegmentTest, TypedEdquotAlsoReconciles)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    writeHook_ = [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<TestBackedWriteFile>(path, append, /*byteBudget*/ 25, ThrowKind::ErrnoEdquot);
    };

    std::string first(20, 'a');
    segment->write(first.data(), first.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), 20u);

    std::string second(30, 'b');
    EXPECT_THROW(
        segment->write(second.data(), second.size(), segment->getCurrentWriteOffset()),
        FileCacheErrnoException);

    EXPECT_EQ(fs::file_size(segment->getPath()), 25u);
    EXPECT_EQ(segment->getDownloadedSize(), 25u);
}

TEST_F(FileSegmentTest, ZeroDownloadedEnospcRemovesFailedNewFile)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    // Budget 0: the very first write commits nothing and immediately throws
    // ENOSPC. With prior downloaded_size == 0 the failed new file is removed.
    writeHook_ = [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<TestBackedWriteFile>(path, append, /*byteBudget*/ 0, ThrowKind::ErrnoEnospc);
    };

    std::string chunk(30, 'a');
    const auto path = segment->getPath();
    EXPECT_THROW(
        segment->write(chunk.data(), chunk.size(), segment->getCurrentWriteOffset()),
        FileCacheErrnoException);

    EXPECT_EQ(segment->getDownloadedSize(), 0u);
    EXPECT_FALSE(fs::exists(path));
    EXPECT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
}

// -- typed-errno reconciliation (negative) ----------------------------------

TEST_F(FileSegmentTest, GenericExceptionDoesNotReconcile)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    // Same short-write shape (commit a 10-byte prefix, then throw), but a
    // non-errno exception. Production must NOT reconcile downloaded_size.
    writeHook_ = [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<TestBackedWriteFile>(path, append, /*byteBudget*/ 50, ThrowKind::Generic);
    };

    std::string first(40, 'a');
    segment->write(first.data(), first.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), 40u);

    std::string second(30, 'b');
    EXPECT_ANY_THROW(segment->write(second.data(), second.size(), segment->getCurrentWriteOffset()));

    // The physical file grew to 50 (the committed prefix), but downloaded_size
    // is unchanged (40): a non-space failure must never be masked by a size
    // fix-up. The download is still published as failed.
    EXPECT_EQ(fs::file_size(segment->getPath()), 50u);
    EXPECT_EQ(segment->getDownloadedSize(), 40u);
    EXPECT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
}

TEST_F(FileSegmentTest, DifferentErrnoDoesNotReconcile)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);

    // A typed errno exception, but EIO rather than ENOSPC/EDQUOT: reconciliation
    // is gated on the space-exhaustion errnos only.
    writeHook_ = [](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<TestBackedWriteFile>(path, append, /*byteBudget*/ 50, ThrowKind::ErrnoEio);
    };

    std::string first(40, 'a');
    segment->write(first.data(), first.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), 40u);

    std::string second(30, 'b');
    EXPECT_THROW(
        segment->write(second.data(), second.size(), segment->getCurrentWriteOffset()),
        FileCacheErrnoException);

    EXPECT_EQ(fs::file_size(segment->getPath()), 50u);
    EXPECT_EQ(segment->getDownloadedSize(), 40u);
    EXPECT_EQ(segment->state(), FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
}

// -- terminal state publication + final rename ------------------------------

TEST_F(FileSegmentTest, FullDownloadPublishesDownloadedAndEncodesSizeInName)
{
    const size_t size = 64;
    auto segment = acquireDownloadingSegment(size, size);

    std::string data(size, 'z');
    segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());
    ASSERT_EQ(segment->getDownloadedSize(), size);

    // Resetting the downloader with a fully downloaded segment publishes the
    // terminal DOWNLOADED state and releases the download-only state.
    segment->resetDownloader();

    EXPECT_TRUE(segment->isDownloaded());
    EXPECT_EQ(segment->state(), FileSegment::State::DOWNLOADED);
    EXPECT_TRUE(segment->isCompleted());
    EXPECT_GT(segment->getFinishedDownloadTime(), 0);

    // A fully downloaded regular segment encodes its size in the file name.
    EXPECT_TRUE(segment->hasSizeInFileName());
    EXPECT_EQ(fs::file_size(segment->getPath()), size);
}

TEST_F(FileSegmentTest, RenameFailureKeepsSegmentConsistent)
{
    const size_t size = 64;
    auto segment = acquireDownloadingSegment(size, size);

    std::string data(size, 'z');
    segment->write(data.data(), data.size(), segment->getCurrentWriteOffset());

    // Make the size-encoding rename fail by pre-creating a directory exactly at
    // the target `<offset>_<size>` path. The rename is best-effort, so the
    // segment must still complete under its legacy name.
    const auto legacyPath = segment->getPath();
    const auto targetPath = legacyPath + "_" + std::to_string(size);
    std::error_code ec;
    fs::create_directory(targetPath, ec);

    segment->resetDownloader();

    EXPECT_TRUE(segment->isDownloaded());
    EXPECT_FALSE(segment->hasSizeInFileName());
    EXPECT_EQ(segment->getPath(), legacyPath);
    EXPECT_EQ(fs::file_size(segment->getPath()), size);
    EXPECT_TRUE(segment->assertCorrectness());
}

// -- detach -----------------------------------------------------------------

TEST_F(FileSegmentTest, DetachMakesStateImmutable)
{
    auto segment = acquireEmptySegment(100);
    auto key_metadata = segment->getKeyMetadata();

    {
        auto locked_key = key_metadata->lock();
        auto segment_lock = segment->lock();
        segment->detach(segment_lock, *locked_key);
    }

    EXPECT_TRUE(segment->isDetached());
    EXPECT_EQ(segment->state(), FileSegment::State::DETACHED);

    // Any stateful operation on a detached segment must fail.
    EXPECT_ANY_THROW(segment->getOrSetDownloader());
    std::string failure_reason;
    EXPECT_ANY_THROW(segment->reserve(10, /*lock_wait_timeout_ms*/ 1000, failure_reason));
}

// -- wait / cancellation ----------------------------------------------------

TEST_F(FileSegmentTest, WaitReturnsImmediatelyWhenOffsetAlreadyDownloaded)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);
    std::string chunk(40, 'a');
    segment->write(chunk.data(), chunk.size(), segment->getCurrentWriteOffset());

    // The requested offset (10) is below the current write offset (40), so wait
    // returns the current state without blocking, regardless of the token.
    folly::CancellationSource source;
    EXPECT_EQ(segment->wait(10, source.getToken()), FileSegment::State::DOWNLOADING);
}

TEST_F(FileSegmentTest, WaitObservesCancellationToken)
{
    auto segment = acquireEmptySegment(100);

    // A separate physical thread holds the downloader lease and never makes
    // progress, so a waiter for a not-yet-downloaded offset would block.
    std::promise<void> downloaderReady;
    std::promise<void> releaseDownloader;
    auto releaseFuture = releaseDownloader.get_future();
    std::thread downloader([&]
    {
        FileCacheQueryIdScope scope("downloader-query");
        ASSERT_EQ(segment->getOrSetDownloader(), FileSegment::getCallerId());
        downloaderReady.set_value();
        releaseFuture.wait();
        // Release the lease while still the downloader so teardown can complete
        // the (otherwise DOWNLOADING) segment cleanly.
        segment->resetDownloader();
    });
    downloaderReady.get_future().wait();

    // Pre-cancel the token before waiting: the wait loop checks
    // isCancellationRequested() at the top of every one-second slice (including
    // the first), so an already-cancelled token is observed promptly and raises a
    // Velox exception instead of blocking to the 60s deadline. Using a
    // pre-cancelled token proves the token is honored without a sleep/timing race.
    folly::CancellationSource source;
    source.requestCancellation();
    EXPECT_ANY_THROW(segment->wait(50, source.getToken()));

    releaseDownloader.set_value();
    downloader.join();
}

// -- holder RAII cleanup ----------------------------------------------------

TEST_F(FileSegmentTest, HolderDestructorCompletesEmptySegment)
{
    auto key_copy = FileCacheKey::random();
    auto holder = cache_->getOrSet(
        key_copy, /*offset*/ 0, /*size*/ 100, /*file_size*/ 100, CreateFileSegmentSettings{}, /*limit*/ 0, origin_);
    auto segment = holder->getSingleFileSegment();
    ASSERT_EQ(segment->state(), FileSegment::State::EMPTY);

    // Destroying the holder completes (and, being the last holder of an EMPTY
    // segment, removes) the file segment.
    holder.reset();

    auto paths = cache_->tryGetCachePaths(key_copy);
    EXPECT_TRUE(paths.empty());
}

TEST_F(FileSegmentTest, OtherThreadHolderDestructionDoesNotClearActiveDownloader)
{
    auto segment = acquireEmptySegment(100);

    // Elect a downloader on a dedicated physical thread; that thread's caller id
    // owns the lease.
    std::promise<std::string> downloaderId;
    std::promise<void> release;
    auto releaseFuture = release.get_future();
    std::thread downloader([&]
    {
        FileCacheQueryIdScope scope("downloader-query");
        const auto id = segment->getOrSetDownloader();
        downloaderId.set_value(id);
        releaseFuture.wait();
        // Release the lease while still the downloader so teardown can complete
        // the (otherwise DOWNLOADING) segment cleanly.
        segment->resetDownloader();
    });
    const auto electedId = downloaderId.get_future().get();
    EXPECT_FALSE(electedId.empty());

    // The current (different) thread is not the downloader.
    EXPECT_FALSE(segment->isDownloader());
    EXPECT_EQ(segment->getDownloader(), electedId);

    release.set_value();
    downloader.join();
}

// -- getInfo snapshot -------------------------------------------------------

TEST_F(FileSegmentTest, GetInfoSnapshotReflectsSegment)
{
    auto segment = acquireDownloadingSegment(/*size*/ 100, /*reserve*/ 100);
    std::string chunk(40, 'a');
    segment->write(chunk.data(), chunk.size(), segment->getCurrentWriteOffset());

    const auto info = FileSegment::getInfo(segment);
    EXPECT_EQ(info.offset, 0u);
    EXPECT_EQ(info.range_left, 0u);
    EXPECT_EQ(info.range_right, 99u);
    EXPECT_EQ(info.size, 100u);
    EXPECT_EQ(info.downloaded_size, 40u);
    EXPECT_EQ(info.kind, FileSegmentKind::Regular);
    EXPECT_EQ(info.state, FileSegment::State::DOWNLOADING);
    EXPECT_FALSE(info.is_unbound);
    EXPECT_GE(info.references, 1u);
    EXPECT_EQ(info.origin.user_id, origin_.user_id);
}

} // namespace
} // namespace facebook::velox::ch
