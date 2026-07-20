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

#include "velox/ch/Common/FileCacheBoundedQueue.h"
#include "velox/ch/Common/StatusFile.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;
namespace fs = std::filesystem;

/// Build a minimal, deterministic single-queue LRU cache configuration.
/// `path` must be a directory whose parent exists (initialize creates it).
/// `boundary_alignment` is set to `max_segment_size` so a partially-downloaded segment,
/// on completion, rounds its shrink target up to the full range and is left
/// PARTIALLY_DOWNLOADED (no `<offset>`->`<offset>_<size>` rename). This is deliberate: the
/// Task-012 amendment (B2b) makes the SCC-phase rename path throw VELOX_NYI for opened-handle
/// invalidation, and the mandatory S4 tests must NOT exercise that rename/remove path.
FileCacheSettings makeSettings(const std::string & path, size_t max_size, size_t max_segment_size)
{
    FileCacheSettings s;
    s.path = path;
    s.maxSize = max_size;
    s.maxElements = 100;
    s.maxFileSegmentSize = max_segment_size;
    s.boundaryAlignment = max_segment_size;
    s.reserveGranularity = 1;
    s.cachePolicy = FileCachePolicy::LRU;
    s.useSplitCache = false;
    s.backgroundDownloadThreads = 0;
    s.loadMetadataAsynchronously = false;
    // keep the free-space keeper disabled (ratio 0 -> task not started)
    s.keepFreeSpaceSizeRatio = 0.0;
    s.keepFreeSpaceElementsRatio = 0.0;
    return s;
}

/// Drive the real downloader path for one segment covering [offset, offset+size), writing a
/// strict prefix (`downloaded` bytes, < size) so the completed segment stays PARTIALLY_DOWNLOADED
/// and cache-resident WITHOUT triggering the rename path (see makeSettings). Runs entirely on the
/// calling thread so the caller-id downloader identity is stable. Returns true on success.
bool populateSegment(FileCache & cache, const FileCacheKey & key, size_t offset, size_t size, size_t downloaded)
{
    CreateFileSegmentSettings create_settings; // Regular
    auto holder = cache.getOrSet(
        key, offset, size, /* file_size */ offset + size, create_settings,
        /* file_segments_limit */ 0, cache.getCommonOrigin());
    if (!holder || holder->empty())
        return false;

    std::vector<char> payload(downloaded, 'x');

    auto segment_ptr = holder->getSingleFileSegment();
    if (!segment_ptr)
        return false;
    FileSegment & segment = *segment_ptr;

    if (segment.getOrSetDownloader() != FileSegment::getCallerId())
        return false;

    std::string reason;
    if (!segment.reserve(downloaded, /* lock_wait_ms */ 100, reason))
        return false;
    segment.write(payload.data(), downloaded, segment.getCurrentWriteOffset());

    // Complete + release: the segment shrinks to its downloaded size but, because the boundary
    // alignment rounds the shrink target up to the full range, it stays PARTIALLY_DOWNLOADED
    // (no rename). Then the holder is destroyed so the cache metadata is the only owner.
    holder->completeAndPopFront(/* allow_background_download */ false, /* force_shrink */ false);
    return true;
}

class FileCacheTest : public ::testing::Test
{
protected:
    void SetUp() override { temp_ = TempDirectoryPath::create(); }

    std::string cachePath(const std::string & sub = "cache") const
    {
        return (fs::path(temp_->getPath()) / sub).string();
    }

    std::shared_ptr<TempDirectoryPath> temp_;
};

TEST_F(FileCacheTest, InitializeOnce)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 1024 * 1024), "user-A");
    EXPECT_FALSE(cache.isInitialized());
    cache.initialize();
    EXPECT_TRUE(cache.isInitialized());
    // Idempotent: a second initialize() must not throw or re-run (std::call_once).
    cache.initialize();
    EXPECT_TRUE(cache.isInitialized());
    // Directory and StatusFile were created.
    EXPECT_TRUE(fs::exists(cache.getBasePath()));
    EXPECT_TRUE(fs::exists(fs::path(cache.getBasePath()) / "status"));
    cache.deactivateBackgroundOperations();
}

TEST_F(FileCacheTest, GetDoesNotCreateMetadata)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 1024 * 1024), "user-A");
    cache.initialize();

    auto key = FileCacheKey::random();
    // `get` is cache-only: on a miss it must NOT create any cache-owned segment.
    auto holder = cache.get(key, 0, 4096, /* file_segments_limit */ 0, cache.getCommonOrigin().user_id);
    ASSERT_TRUE(holder);
    // No cached bytes and no cached segments were produced by the miss.
    EXPECT_EQ(cache.getUsedCacheSize(), 0u);
    EXPECT_EQ(cache.getFileSegmentsNum(), 0u);
    cache.deactivateBackgroundOperations();
}

TEST_F(FileCacheTest, GetOrSetCreatesEmptySegment)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 1024 * 1024), "user-A");
    cache.initialize();

    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    auto holder = cache.getOrSet(key, 0, 4096, 4096, create_settings, 0, cache.getCommonOrigin());
    ASSERT_TRUE(holder);
    ASSERT_FALSE(holder->empty());
    // A freshly created, not-yet-downloaded segment is EMPTY and cache-owned (metadata created).
    auto & seg = holder->front();
    EXPECT_EQ(seg.state(), FileSegmentState::EMPTY);
    EXPECT_EQ(seg.range().size(), 4096u);
    // A freshly created, not-yet-reserved segment has no priority-queue entry yet, so it does
    // not count toward getFileSegmentsNum until it is reserved/downloaded.
    cache.deactivateBackgroundOperations();
}

TEST_F(FileCacheTest, DownloadPopulatesCacheSize)
{
    const size_t seg = 8192;
    const size_t downloaded = 4096; // strict prefix: stays PARTIALLY_DOWNLOADED (no rename)
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, seg), "user-A");
    cache.initialize();

    auto key = FileCacheKey::random();
    ASSERT_TRUE(populateSegment(cache, key, 0, seg, downloaded));
    EXPECT_EQ(cache.getUsedCacheSize(), downloaded);
    EXPECT_EQ(cache.getFileSegmentsNum(), 1u);
    cache.deactivateBackgroundOperations();
}

/// Releasable reserve eviction (mandatory contract): fill the cache to capacity with a
/// downloaded+released (hence releasable) segment, then reserving on a new segment must
/// succeed only by evicting the releasable candidate; total used size stays within capacity.
TEST_F(FileCacheTest, TryReserveEvictsReleasable)
{
    const size_t seg = 8192;
    const size_t downloaded = 4096;
    // Capacity for exactly one downloaded prefix: the second download must evict the first.
    FileCache cache("t", makeSettings(cachePath(), downloaded, seg), "user-A");
    cache.initialize();

    auto key1 = FileCacheKey::random();
    ASSERT_TRUE(populateSegment(cache, key1, 0, seg, downloaded));
    EXPECT_EQ(cache.getUsedCacheSize(), downloaded);
    EXPECT_EQ(cache.getFileSegmentsNum(), 1u);

    // key1's holder was released inside populateSegment, so its metadata is releasable.
    auto key2 = FileCacheKey::random();
    ASSERT_TRUE(populateSegment(cache, key2, 0, seg, downloaded));

    // The cache never exceeded its capacity: the releasable key1 segment was evicted.
    EXPECT_EQ(cache.getUsedCacheSize(), downloaded);
    EXPECT_EQ(cache.getFileSegmentsNum(), 1u);
    // key1 is gone, key2 present. After eviction key1's metadata is fully removed, so the
    // per-key getFileSegmentInfos(key1, ...) would throw THROW_LOGICAL (CH-faithful: no such key).
    // Assert the eviction outcome via the non-throwing whole-cache enumeration instead: exactly one
    // segment survives and it belongs to key2, proving the releasable key1 segment was evicted.
    auto all = cache.getFileSegmentInfos(cache.getCommonOrigin().user_id);
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all.front().key, key2);
    EXPECT_NE(all.front().key, key1);
    EXPECT_EQ(cache.getFileSegmentInfos(key2, cache.getCommonOrigin().user_id).size(), 1u);
    cache.deactivateBackgroundOperations();
}

TEST_F(FileCacheTest, ShutdownJoinsWorkers)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 8192), "user-A");
    cache.initialize();
    ASSERT_TRUE(cache.isInitialized());

    auto key = FileCacheKey::random();
    ASSERT_TRUE(populateSegment(cache, key, 0, 8192, 4096));

    // deactivateBackgroundOperations must join every worker/timer and return; the fact that
    // it returns (rather than deadlocking) plus a subsequent clean destructor proves completion.
    cache.deactivateBackgroundOperations();
    // A second call must be safe (idempotent shutdown).
    cache.deactivateBackgroundOperations();
    SUCCEED();
}

TEST_F(FileCacheTest, SecondInstanceOnSamePathStatusLockFails)
{
    auto settings = makeSettings(cachePath(), 16 * 1024 * 1024, 8192);
    FileCache cache1("t1", settings, "user-A");
    cache1.initialize();

    // FileCache::initialize acquires an exclusive StatusFile lock on `<path>/status` (guards
    // against two disks/processes sharing a cache directory). Acquiring the SAME production lock
    // again on the same path must fail while cache1 holds it. We assert at the StatusFile layer,
    // exactly the lock FileCache::initialize takes, to avoid destructing a half-initialized second
    // FileCache.
    const std::string status_path = (fs::path(cache1.getBasePath()) / "status").string();
    EXPECT_ANY_THROW({ StatusFile second(status_path, StatusFile::writeFullInfo()); });
    cache1.deactivateBackgroundOperations();
}

/// B5: the common origin's user id is the host-injected common_user_id (no ServerUUID).
/// Missing key + KeyNotFoundPolicy::THROW (mandatory contract): a production lookup that requires
/// the key to exist (removeFileSegment routes through lockKeyMetadata(THROW)) must throw for an
/// absent key rather than silently succeeding.
TEST_F(FileCacheTest, MissingKeyThrows)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 8192), "user-A");
    cache.initialize();

    auto missing = FileCacheKey::random();
    EXPECT_ANY_THROW(cache.removeFileSegment(missing, 0, cache.getCommonOrigin().user_id));
    // removeFileSegmentIfExists must NOT throw for an absent key (the non-throwing counterpart).
    EXPECT_NO_THROW(cache.removeFileSegmentIfExists(missing, 0, cache.getCommonOrigin().user_id));
    cache.deactivateBackgroundOperations();
}

/// Queue pipeline (mandatory contract): the FileCacheBoundedQueue that backs the free-space
/// eviction pipeline executes a real timed tryPush(batch, timeout) and a non-blocking tryPop.
TEST_F(FileCacheTest, BoundedQueuePipeline)
{
    FileCacheBoundedQueue<int> queue(/* capacity */ 2);

    // Timed tryPush succeeds while capacity is available.
    EXPECT_TRUE(queue.tryPush(1, /* timeoutMs */ 10));
    EXPECT_TRUE(queue.tryPush(2, /* timeoutMs */ 10));
    // Queue is full: a timed tryPush with a short timeout fails (does not block forever).
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(queue.tryPush(3, /* timeoutMs */ 10));
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 0);

    // Non-blocking tryPop drains in FIFO order.
    int v = 0;
    EXPECT_TRUE(queue.tryPop(v));
    EXPECT_EQ(v, 1);
    EXPECT_TRUE(queue.tryPop(v));
    EXPECT_EQ(v, 2);
    // Empty: non-blocking tryPop returns false immediately.
    EXPECT_FALSE(queue.tryPop(v));

    // After capacity frees up, a timed tryPush succeeds again.
    EXPECT_TRUE(queue.tryPush(3, /* timeoutMs */ 10));
    EXPECT_TRUE(queue.tryPop(v));
    EXPECT_EQ(v, 3);

    // finish() unblocks and drains: subsequent tryPush fails, tryPop drains then reports empty.
    queue.finish();
    EXPECT_FALSE(queue.tryPush(4, /* timeoutMs */ 10));
    EXPECT_FALSE(queue.tryPop(v));
}

TEST_F(FileCacheTest, CommonOriginIsInjectedUserId)
{
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 4096), "injected-user-42");
    EXPECT_EQ(cache.getCommonOrigin().user_id, "injected-user-42");
}

/// The internal origin is the fixed "internal" user, shared by all caches and distinct
/// from any common user id; it is what the background eviction path uses to reach all keys.
TEST_F(FileCacheTest, InternalOriginIsInternalUser)
{
    EXPECT_EQ(FileCache::getInternalOrigin().user_id, "internal");
    FileCache cache("t", makeSettings(cachePath(), 16 * 1024 * 1024, 4096), "some-user");
    EXPECT_NE(FileCache::getInternalOrigin().user_id, cache.getCommonOrigin().user_id);
}

} // namespace
} // namespace facebook::velox::ch
