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

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"
#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <folly/system/ThreadId.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

/// Range is inclusive [left, right] (CH `FileSegment::Range`).
TEST(RangeTest, SizeIsRightMinusLeftPlusOne)
{
    FileSegment::Range r(10, 19);
    EXPECT_EQ(r.size(), 10ULL);
}

TEST(RangeTest, ContainsPoint)
{
    FileSegment::Range r(5, 10);
    EXPECT_TRUE(r.contains(static_cast<size_t>(5)));
    EXPECT_TRUE(r.contains(static_cast<size_t>(10)));
    EXPECT_FALSE(r.contains(static_cast<size_t>(4)));
    EXPECT_FALSE(r.contains(static_cast<size_t>(11)));
}

TEST(RangeTest, ContainsRange)
{
    FileSegment::Range outer(0, 100);
    FileSegment::Range inner(10, 50);
    FileSegment::Range overlap(80, 110);
    EXPECT_TRUE(outer.contains(inner));
    EXPECT_FALSE(outer.contains(overlap));
}

/// Strict weak ordering: a < b iff a.right < b.left (non-overlapping, a before b).
TEST(RangeTest, StrictOrderingNonOverlapping)
{
    FileSegment::Range a(0, 9);
    FileSegment::Range b(10, 19);
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
}

TEST(RangeTest, OverlappingRangesAreNotStrictlyOrdered)
{
    FileSegment::Range a(0, 10);
    FileSegment::Range b(5, 20);
    EXPECT_FALSE(a < b);
    EXPECT_FALSE(b < a);
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

/// getCallerId is stable within one thread/scope: `"<query-id>:<tid>"` with a scope.
TEST(CallerIdTest, SameScopeStableId)
{
    FileCacheQueryIdScope scope("q1");
    auto id1 = FileSegment::getCallerId();
    auto id2 = FileSegment::getCallerId();
    EXPECT_EQ(id1, id2);
    EXPECT_EQ(id1, "q1:" + std::to_string(folly::getOSThreadID()));
    EXPECT_NE(id1, "None:" + std::to_string(folly::getOSThreadID()));
}

/// Without a query scope, caller uses the CH diagnostic format
/// "None:<threadname>:<tid>" (F-CALLERID, Task 017). Exact-format check: three
/// colon-separated fields, first "None", last a decimal OS tid.
TEST(CallerIdTest, NoScopeBackgroundId)
{
    auto id = FileSegment::getCallerId();
    const auto firstColon = id.find(':');
    ASSERT_NE(firstColon, std::string::npos);
    const auto lastColon = id.rfind(':');
    ASSERT_NE(firstColon, lastColon) << "expected None:<threadname>:<tid>: " << id;
    EXPECT_EQ(id.substr(0, firstColon), "None");
    EXPECT_EQ(id.substr(lastColon + 1), std::to_string(folly::getOSThreadID()));
    // Regression guard: must NOT be the old two-field None:<tid> form.
    EXPECT_NE(id, "None:" + std::to_string(folly::getOSThreadID()));
}

/// stateToString mirrors the enum names (used by getInfoForLog and system tables).
TEST(FileSegmentStateTest, StateToString)
{
    EXPECT_EQ(FileSegment::stateToString(FileSegmentState::DOWNLOADED), "DOWNLOADED");
    EXPECT_EQ(FileSegment::stateToString(FileSegmentState::EMPTY), "EMPTY");
    EXPECT_EQ(FileSegment::stateToString(FileSegmentState::DOWNLOADING), "DOWNLOADING");
    EXPECT_EQ(FileSegment::stateToString(FileSegmentState::DETACHED), "DETACHED");
}

namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;

FileCacheSettings fsSettings(const std::string & path, size_t seg)
{
    FileCacheSettings s;
    s.path = path;
    s.maxSize = 16 * 1024 * 1024;
    s.maxElements = 100;
    s.maxFileSegmentSize = seg;
    // Alignment == segment size: a partial download, on completion, rounds its shrink target
    // up to the full range and stays PARTIALLY_DOWNLOADED, so the B2b VELOX_NYI rename/
    // opened-handle path is never exercised (the S4 mandatory tests must avoid it).
    s.boundaryAlignment = seg;
    s.reserveGranularity = 1;
    s.cachePolicy = FileCachePolicy::LRU;
    s.useSplitCache = false;
    s.backgroundDownloadThreads = 0;
    s.loadMetadataAsynchronously = false;
    s.keepFreeSpaceSizeRatio = 0.0;
    s.keepFreeSpaceElementsRatio = 0.0;
    return s;
}

class FileSegmentDownloadTest : public ::testing::Test
{
protected:
    void SetUp() override { temp_ = TempDirectoryPath::create(); }
    std::string cachePath() const { return (fs::path(temp_->getPath()) / "cache").string(); }
    std::shared_ptr<TempDirectoryPath> temp_;
    facebook::velox::ch::test::FileCacheTestResources res_;
};

/// Partial-file resume (mandatory contract): a downloader writes a first prefix, releases the
/// downloader through the real continuation path (completePartAndResetDownloader), then a new
/// downloader re-acquires the same segment, verifies the existing physical size, and appends the
/// remainder WITHOUT truncating the prefix. Downloaded size and the physical file size stay
/// consistent throughout, exercised entirely through the production FileSegment path.
TEST_F(FileSegmentDownloadTest, PartialFileResumeAppendsWithoutTruncation)
{
    const size_t seg = 8192;
    const size_t prefix = 3000;
    const size_t suffix = 2000;
    const size_t total = prefix + suffix; // < seg, so completion stays PARTIALLY_DOWNLOADED

    auto cache_ptr = res_.makeFileCache("resume", fsSettings(cachePath(), seg), "user-A");
    auto & cache = *cache_ptr;
    cache.initialize();

    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    auto holder = cache.getOrSet(key, 0, seg, seg, create_settings, 0, cache.getCommonOrigin());
    ASSERT_TRUE(holder);
    ASSERT_FALSE(holder->empty());
    auto segment_ptr = holder->getSingleFileSegment();
    ASSERT_TRUE(segment_ptr);
    FileSegment & segment = *segment_ptr;

    std::vector<char> data(total);
    for (size_t i = 0; i < total; ++i)
        data[i] = static_cast<char>('A' + (i % 26));

    // Phase 1: become downloader, reserve+write only the prefix.
    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    std::string reason;
    ASSERT_TRUE(segment.reserve(prefix, /*lock_wait_ms*/ 100, reason)) << reason;
    segment.write(data.data(), prefix, segment.getCurrentWriteOffset());
    EXPECT_EQ(segment.getDownloadedSize(), prefix);
    EXPECT_EQ(segment.getCurrentWriteOffset(), prefix);
    const auto seg_path = segment.getPath();
    ASSERT_TRUE(fs::exists(seg_path));
    EXPECT_EQ(fs::file_size(seg_path), prefix);

    // Release the downloader through the real continuation path. The segment becomes
    // PARTIALLY_DOWNLOADED so another owner can continue it.
    segment.completePartAndResetDownloader();
    EXPECT_EQ(segment.state(), FileSegmentState::PARTIALLY_DOWNLOADED);
    // The prefix on disk is untouched by releasing the downloader.
    EXPECT_EQ(fs::file_size(seg_path), prefix);
    EXPECT_EQ(segment.getDownloadedSize(), prefix);

    // Phase 2: re-acquire the downloader (allowed from PARTIALLY_DOWNLOADED), verify the physical
    // size matches the recorded downloaded size, then append the suffix.
    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    EXPECT_EQ(fs::file_size(seg_path), segment.getDownloadedSize());
    ASSERT_TRUE(segment.reserve(suffix, /*lock_wait_ms*/ 100, reason)) << reason;
    segment.write(data.data() + prefix, suffix, segment.getCurrentWriteOffset());

    // The file now holds prefix + suffix; the prefix was NOT truncated (append preserved it).
    EXPECT_EQ(segment.getDownloadedSize(), total);
    EXPECT_EQ(fs::file_size(seg_path), total);

    // Verify the on-disk bytes equal the original data (prefix intact + suffix appended).
    std::vector<char> readback(total);
    {
        std::FILE * f = std::fopen(seg_path.c_str(), "rb");
        ASSERT_NE(f, nullptr);
        ASSERT_EQ(std::fread(readback.data(), 1, total, f), total);
        std::fclose(f);
    }
    EXPECT_EQ(readback, data);

    // Release the downloader again; the holder's completion leaves the (partial) segment
    // PARTIALLY_DOWNLOADED (alignment rounds the shrink target to the full range), so no rename.
    segment.completePartAndResetDownloader();
    holder->completeAndPopFront(false, false);
    EXPECT_EQ(fs::file_size(seg_path), total);
    cache.deactivateBackgroundOperations();
}

/// A production `velox::WriteFile` that physically commits every append to a real
/// `LocalWriteFile`, but on a configured append commits only a STRICT PREFIX of that chunk
/// to disk and then throws `FileCacheErrnoException(ENOSPC)`. It injects ONLY the fault; all
/// reconciliation is left to production `FileSegment::write`.
class PartialCommitThenThrowWriteFile : public velox::WriteFile
{
public:
    PartialCommitThenThrowWriteFile(const std::string & path, size_t fail_on_append, size_t partial_commit)
        : inner_(std::make_unique<velox::LocalWriteFile>(
              path, /*createParentDirs*/ false, /*throwOnExists*/ false, /*bufferIo*/ true)),
          fail_on_append_(fail_on_append),
          partial_commit_(partial_commit)
    {
    }

    void append(std::string_view data) override
    {
        ++append_count_;
        if (append_count_ == fail_on_append_)
        {
            // Physically commit a strict prefix of this chunk, then fail with a no-space errno.
            const size_t commit = std::min(partial_commit_, data.size());
            if (commit)
                inner_->append(std::string_view(data.data(), commit));
            inner_->flush();
            throw FileCacheErrnoException(/* ENOSPC */ 28, "injected no space left on device");
        }
        inner_->append(data);
    }

    void flush() override { inner_->flush(); }
    void close() override { inner_->close(); }
    uint64_t size() const override { return inner_->size(); }
    const std::string getName() const override { return inner_->getName(); }

private:
    std::unique_ptr<velox::LocalWriteFile> inner_;
    const size_t fail_on_append_;
    const size_t partial_commit_;
    size_t append_count_ = 0;
};

/// RAII installer for the FileSegment write-file factory override; resets on scope exit so the
/// override never leaks into other tests (the factory is a static, process-wide seam).
struct ScopedWriteFileFactory
{
    explicit ScopedWriteFileFactory(FileSegment::WriteFileFactory factory)
    {
        FileSegment::setWriteFileFactoryForTesting(std::move(factory));
    }
    ~ScopedWriteFileFactory()
    {
        // Reinstall a factory equivalent to the production default (identical LocalWriteFile
        // construction). The seam is a single process-wide static, so this restores default
        // behavior for later tests; these scopes are used flat (not nested).
        FileSegment::setWriteFileFactoryForTesting(
            [](const std::string & path) -> std::unique_ptr<velox::WriteFile>
            {
                return std::make_unique<velox::LocalWriteFile>(path, false, false, true);
            });
    }
};

/// Partial physical append failure (mandatory contract): production `FileSegment::write` observes
/// an append that physically commits a strict prefix and then throws. Production must read the
/// physical file size, enforce downloaded <= physical <= reserved, set downloaded = physical,
/// mark the download failed, preserve (rethrow) the original exception, and never count
/// reserved-but-unwritten bytes. Reconciliation happens INSIDE production `FileSegment::write`;
/// the injected file only produces the fault.
TEST_F(FileSegmentDownloadTest, PartialPhysicalAppendFailureReconcilesDownloadedToPhysical)
{
    const size_t seg = 8192;
    const size_t prefix = 3000;   // first append: fully committed
    const size_t suffix = 4000;   // second append: fails after committing a strict prefix
    const size_t committed_suffix = 1000; // strict prefix of the failing chunk committed to disk

    auto cache_ptr = res_.makeFileCache("append-fail", fsSettings(cachePath(), seg), "user-A");
    auto & cache = *cache_ptr;
    cache.initialize();

    // Install the fault factory: the 2nd append commits `committed_suffix` bytes then throws ENOSPC.
    ScopedWriteFileFactory scoped(
        [&](const std::string & path) -> std::unique_ptr<velox::WriteFile>
        {
            return std::make_unique<PartialCommitThenThrowWriteFile>(
                path, /*fail_on_append*/ 2, /*partial_commit*/ committed_suffix);
        });

    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    auto holder = cache.getOrSet(key, 0, seg, seg, create_settings, 0, cache.getCommonOrigin());
    ASSERT_TRUE(holder);
    auto segment_ptr = holder->getSingleFileSegment();
    ASSERT_TRUE(segment_ptr);
    FileSegment & segment = *segment_ptr;

    std::vector<char> data(seg, 'Z');

    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    std::string reason;
    // Reserve the whole segment up front so reserved_size >= physical after the partial commit.
    ASSERT_TRUE(segment.reserve(seg, /*lock_wait_ms*/ 100, reason)) << reason;

    // First write: fully committed (downloaded becomes non-zero so the reconcile branch applies).
    segment.write(data.data(), prefix, segment.getCurrentWriteOffset());
    ASSERT_EQ(segment.getDownloadedSize(), prefix);

    const auto seg_path = segment.getPath();
    const size_t reserved_before = segment.getReservedSize();
    ASSERT_GE(reserved_before, seg);

    // Second write triggers the injected partial-commit-then-throw. The ORIGINAL exception must
    // propagate out of production write.
    bool threw = false;
    try
    {
        segment.write(data.data() + prefix, suffix, segment.getCurrentWriteOffset());
    }
    catch (const FileCacheErrnoException & e)
    {
        threw = true;
        EXPECT_EQ(e.getErrno(), 28); // ENOSPC preserved (original exception rethrown)
    }
    EXPECT_TRUE(threw);

    // PRODUCTION reconciliation: downloaded size was updated to the physical file size
    // (prefix + strict-prefix of the failing chunk), never the reserved-but-unwritten amount.
    const size_t expected_physical = prefix + committed_suffix;
    EXPECT_EQ(fs::file_size(seg_path), expected_physical);
    EXPECT_EQ(segment.getDownloadedSize(), expected_physical);
    // Invariant enforced by production: downloaded <= physical <= reserved.
    EXPECT_LE(segment.getDownloadedSize(), fs::file_size(seg_path));
    EXPECT_LE(fs::file_size(seg_path), reserved_before);
    // The download was marked failed (state is no longer DOWNLOADING).
    EXPECT_NE(segment.state(), FileSegmentState::DOWNLOADING);

    // Release the holder while the cache is alive; the failed (partial) segment completes without
    // a rename (alignment rounds the shrink target to the full range).
    holder.reset();
    cache.deactivateBackgroundOperations();
}

/// Remote reader handoff (mandatory contract, Task-007 integration half): a downloader installs a
/// real remote `ReadBufferFromVeloxReadFile`; after the segment is finished-without-continuation
/// the reader is DETACHED via extractRemoteFileReader (the segment no longer holds it), and the
/// extracted reader still satisfies the Task-007 buffer-end-offset / available-bytes contract.
TEST_F(FileSegmentDownloadTest, RemoteReaderHandoffDetachesAndPreservesOffsets)
{
    velox::memory::MemoryManager memory_manager;
    auto pool = memory_manager.addLeafPool("remote-reader-handoff");

    const size_t seg = 8192;
    const size_t prefix = 4000;

    // A real backing file for the remote reader.
    const std::string remote_path = (fs::path(temp_->getPath()) / "remote.bin").string();
    {
        std::vector<char> remote_data(seg, 'R');
        std::FILE * f = std::fopen(remote_path.c_str(), "wb");
        ASSERT_NE(f, nullptr);
        ASSERT_EQ(std::fwrite(remote_data.data(), 1, seg, f), seg);
        std::fclose(f);
    }

    auto cache_ptr = res_.makeFileCache("remote", fsSettings(cachePath(), seg), "user-A");
    auto & cache = *cache_ptr;
    cache.initialize();

    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    auto holder = cache.getOrSet(key, 0, seg, seg, create_settings, 0, cache.getCommonOrigin());
    ASSERT_TRUE(holder);
    auto segment_ptr = holder->getSingleFileSegment();
    ASSERT_TRUE(segment_ptr);
    FileSegment & segment = *segment_ptr;

    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());

    // Install a real remote reader and read a prefix through it so its buffer-end offset advances.
    auto read_file = std::make_shared<velox::LocalReadFile>(remote_path);
    auto remote_reader = std::make_shared<ReadBufferFromVeloxReadFile>(read_file, pool.get());

    // Read the prefix from the remote reader (advances its buffer-end offset); this is the
    // Task-007 reader contract: getFileOffsetOfBufferEnd tracks the file offset at the end of the
    // working view, and available() reports the bytes now readable in that view.
    std::vector<char> buf(prefix);
    remote_reader->set(buf.data(), prefix);
    ASSERT_TRUE(remote_reader->next());
    EXPECT_EQ(remote_reader->getFileOffsetOfBufferEnd(), prefix);
    EXPECT_EQ(remote_reader->available(), prefix);
    // getPosition() is the current logical offset (buffer end minus not-yet-consumed bytes) = 0.
    EXPECT_EQ(remote_reader->getPosition(), 0);

    // Downloader installs the reader on the segment; getRemoteFileReader returns the same object.
    segment.setRemoteFileReader(remote_reader);
    EXPECT_EQ(segment.getRemoteFileReader().get(), remote_reader.get());
    // A second install is rejected (the segment owns exactly one reader).
    EXPECT_ANY_THROW(segment.setRemoteFileReader(remote_reader));

    // Detach: resetRemoteFileReader releases the segment's reference (downloader release path),
    // while the externally held reader stays valid and keeps its Task-007 offsets.
    segment.resetRemoteFileReader();
    EXPECT_EQ(segment.getRemoteFileReader(), nullptr); // detached from the segment
    EXPECT_EQ(remote_reader->getFileOffsetOfBufferEnd(), prefix);
    EXPECT_EQ(remote_reader->available(), prefix);

    // Release the downloader and holder; the not-downloaded segment is removed cleanly (no file,
    // no rename): downloaded_size is 0 so no on-disk file exists.
    segment.resetDownloader();
    holder.reset();
    cache.deactivateBackgroundOperations();
}

} // namespace
} // namespace facebook::velox::ch
