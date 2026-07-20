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
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"
#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"

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

FileCacheConfig makeConfig(const std::string & path, uint64_t max_size = 16 * 1024 * 1024)
{
    FileCacheConfig c;
    c.path = path;
    c.maxSize = max_size;
    c.maxFileSegmentSize = 1024 * 1024;
    c.cachePolicy = FileCachePolicy::LRU;
    c.loadMetadataThreads = 2;
    c.backgroundDownloadThreads = 2;
    c.keepFreeSpaceSizeRatio = 0.0;
    c.keepFreeSpaceElementsRatio = 0.0;
    return c;
}

// ============================ Factory ============================

class FactoryTest : public ::testing::Test
{
protected:
    void SetUp() override { temp_ = TempDirectoryPath::create(); }
    std::string sub(const std::string & s) const { return (fs::path(temp_->getPath()) / s).string(); }

    // A Factory backed by test-owned runtime resources (no Manager needed for pure registry tests).
    FileCacheFactory makeFactory()
    {
        return FileCacheFactory(FileCacheFactory::RuntimeServices{
            res_.workerPool_, res_.scheduler_, res_.openedFileCache_, *res_.fileSystem_, "user-A"});
    }

    std::shared_ptr<TempDirectoryPath> temp_;
    test::FileCacheTestResources res_;
};

TEST_F(FactoryTest, CreateGetMissing)
{
    auto factory = makeFactory();
    EXPECT_THROW(factory.get("absent"), VeloxRuntimeError);

    auto cache = factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    ASSERT_NE(cache, nullptr);
    EXPECT_EQ(factory.get("a"), cache);
    EXPECT_EQ(factory.getByName("a")->cache, cache);
}

TEST_F(FactoryTest, SameNameEqualSettingsReturnsSameDifferentSettingsRejects)
{
    auto factory = makeFactory();
    auto c1 = factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    // Equal settings -> same cache.
    auto c2 = factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    EXPECT_EQ(c1, c2);
    // Same name, different settings -> reject (rebind conflict); binding preserved.
    auto other = makeConfig(sub("a"));
    other.maxSize = 999999;
    EXPECT_THROW(factory.getOrCreate("a", other, "conf.a"), VeloxRuntimeError);
    EXPECT_EQ(factory.get("a"), c1);
}

TEST_F(FactoryTest, SamePathAliasDedupAndDifferentSettingsRejection)
{
    auto factory = makeFactory();
    auto c1 = factory.getOrCreate("primary", makeConfig(sub("shared")), "conf.p");
    // Same path, equal settings, different name -> alias to the same cache (dedup).
    auto c2 = factory.getOrCreate("alias", makeConfig(sub("shared")), "conf.a");
    EXPECT_EQ(c1, c2);
    EXPECT_EQ(factory.getUniqueInstances().size(), 1u);
    EXPECT_EQ(factory.getAll().size(), 2u);
    // Same path, different settings -> reject.
    auto other = makeConfig(sub("shared"));
    other.maxSize = 12345;
    EXPECT_THROW(factory.getOrCreate("thirdName", other, "conf.x"), VeloxRuntimeError);
}

TEST_F(FactoryTest, CreateRejectsExistingName)
{
    auto factory = makeFactory();
    factory.create("a", makeConfig(sub("a")), "conf.a");
    // create() always fails on an existing name, even with identical settings.
    EXPECT_THROW(factory.create("a", makeConfig(sub("a")), "conf.a"), VeloxRuntimeError);
}

TEST_F(FactoryTest, AliasEnumerationAndRemoveAllAliases)
{
    auto factory = makeFactory();
    auto c = factory.getOrCreate("primary", makeConfig(sub("shared")), "conf.p");
    factory.getOrCreate("alias1", makeConfig(sub("shared")), "conf.p");
    factory.getOrCreate("alias2", makeConfig(sub("shared")), "conf.p");
    EXPECT_EQ(factory.getAll().size(), 3u);
    EXPECT_EQ(factory.getUniqueInstances().size(), 1u);

    // remove() erases every name (alias) for the cache.
    factory.remove(c);
    EXPECT_EQ(factory.getAll().size(), 0u);
    EXPECT_THROW(factory.get("primary"), VeloxRuntimeError);
    EXPECT_THROW(factory.get("alias1"), VeloxRuntimeError);
}

TEST_F(FactoryTest, NameRebindConflictPreservesOriginalBinding)
{
    auto factory = makeFactory();
    auto original = factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    // A different path under the same name must not rebind "a".
    EXPECT_THROW(factory.getOrCreate("a", makeConfig(sub("b")), "conf.b"), VeloxRuntimeError);
    EXPECT_EQ(factory.get("a"), original);
    EXPECT_EQ(factory.get("a")->getBasePath(), original->getBasePath());
}

TEST_F(FactoryTest, WorkerBudgetGrowsOncePerUniqueCacheNotPerAlias)
{
    auto factory = makeFactory();
    const size_t per_cache = computeCacheWorkerMax(makeConfig(sub("shared")));
    ASSERT_GT(per_cache, 0u);

    factory.getOrCreate("primary", makeConfig(sub("shared")), "conf.p");
    // Two aliases of the same unique cache must not grow the budget again.
    factory.getOrCreate("alias1", makeConfig(sub("shared")), "conf.p");
    factory.getOrCreate("alias2", makeConfig(sub("shared")), "conf.p");
    EXPECT_EQ(factory.getUniqueInstances().size(), 1u);

    // A second, distinct cache grows the budget.
    factory.getOrCreate("second", makeConfig(sub("second")), "conf.s");
    EXPECT_EQ(factory.getUniqueInstances().size(), 2u);
}

TEST_F(FactoryTest, ClearDeactivatesAndLeavesFactoryReusable)
{
    auto factory = makeFactory();
    factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    factory.getOrCreate("b", makeConfig(sub("b")), "conf.b");
    factory.clear();
    EXPECT_EQ(factory.getAll().size(), 0u);
    // Reusable: a new cache can be registered after clear().
    auto c = factory.getOrCreate("a", makeConfig(sub("a")), "conf.a");
    EXPECT_NE(c, nullptr);
    EXPECT_EQ(factory.get("a"), c);
}

// ============================ Manager ============================

class ManagerTest : public ::testing::Test
{
protected:
    void SetUp() override { temp_ = TempDirectoryPath::create(); }
    std::string sub(const std::string & s) const { return (fs::path(temp_->getPath()) / s).string(); }

    FileCacheManager::Options baseOptions()
    {
        FileCacheManager::Options o;
        o.commonUserId = "user-A";
        o.localFileSystem = test::localFileSystemForTests();
        o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
        o.initializeOnCreate = true;
        return o;
    }

    std::shared_ptr<TempDirectoryPath> temp_;
};

TEST_F(ManagerTest, CreateRegistersAndInitializesCaches)
{
    auto o = baseOptions();
    o.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
    o.caches.push_back({"b", makeConfig(sub("b")), "conf.b"});
    o.defaultCacheName = "a";

    auto manager = FileCacheManager::create(o);
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->commonUserId(), "user-A");
    EXPECT_NE(manager->get("a"), nullptr);
    EXPECT_EQ(manager->getDefault(), manager->get("a"));
    EXPECT_TRUE(manager->get("a")->isInitialized());
    EXPECT_TRUE(manager->get("b")->isInitialized());
    manager->shutdown();
}

TEST_F(ManagerTest, ValidationRejectsBadCommonUserIdAndMissingDefault)
{
    {
        auto o = baseOptions();
        o.commonUserId = "";
        EXPECT_THROW(FileCacheManager::create(o), VeloxRuntimeError);
    }
    {
        auto o = baseOptions();
        o.commonUserId = "internal";
        EXPECT_THROW(FileCacheManager::create(o), VeloxRuntimeError);
    }
    {
        auto o = baseOptions();
        o.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
        o.defaultCacheName = "does-not-exist";
        EXPECT_THROW(FileCacheManager::create(o), VeloxRuntimeError);
    }
}

TEST_F(ManagerTest, WorkerBudgetDedupsByPath)
{
    auto o = baseOptions();
    // Two names sharing one path -> counted once by the budget.
    o.caches.push_back({"primary", makeConfig(sub("shared")), "conf.p"});
    o.caches.push_back({"alias", makeConfig(sub("shared")), "conf.a"});
    auto manager = FileCacheManager::create(o);
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
    EXPECT_EQ(manager->get("primary"), manager->get("alias"));
    manager->shutdown();
}

TEST_F(ManagerTest, InstallGetUninstallAndLiveReplacementRejection)
{
    auto o1 = baseOptions();
    o1.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
    auto m1 = FileCacheManager::create(o1);

    EXPECT_EQ(FileCacheManager::getInstance(), nullptr);
    FileCacheManager::setInstance(m1.get());
    EXPECT_EQ(FileCacheManager::getInstance(), m1.get());
    EXPECT_EQ(&FileCacheManager::instance(), m1.get());
    // The Factory singleton is now the manager's factory.
    EXPECT_EQ(&FileCacheFactory::instance(), &m1->factory());
    // Setting the same manager again is a no-op (not an error).
    FileCacheManager::setInstance(m1.get());
    EXPECT_EQ(FileCacheManager::getInstance(), m1.get());

    // A different live manager cannot replace it.
    auto o2 = baseOptions();
    auto m2 = FileCacheManager::create(o2);
    EXPECT_THROW(FileCacheManager::setInstance(m2.get()), VeloxRuntimeError);

    // Uninstall clears the Factory pointer first, then the Manager pointer.
    FileCacheManager::setInstance(nullptr);
    EXPECT_EQ(FileCacheManager::getInstance(), nullptr);
    EXPECT_THROW(FileCacheFactory::instance(), VeloxRuntimeError);
    m1->shutdown();
    m2->shutdown();
}

TEST_F(ManagerTest, ShutdownIsIdempotentAndLaterOperationsFail)
{
    auto o = baseOptions();
    o.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
    auto manager = FileCacheManager::create(o);
    manager->shutdown();
    // Idempotent.
    manager->shutdown();
    // initialize after shutdown fails.
    EXPECT_THROW(manager->initialize(), VeloxRuntimeError);
}

TEST_F(ManagerTest, RefreshStatsReportsCachesAndOpenedFileCache)
{
    auto o = baseOptions();
    o.caches.push_back({"a", makeConfig(sub("a"), 8 * 1024 * 1024), "conf.a"});
    auto manager = FileCacheManager::create(o);
    auto stats = manager->refreshStats();
    EXPECT_EQ(stats.uniqueCaches, 1u);
    ASSERT_EQ(stats.cachesByName.count("a"), 1u);
    EXPECT_EQ(stats.cachesByName.at("a").maxSize, manager->get("a")->getMaxCacheSize());
    manager->shutdown();
}

TEST_F(ManagerTest, HasDefaultAgreesWithGetDefault)
{
    // Case 1: a non-empty defaultCacheName -> hasDefault() is true AND getDefault()
    // returns that cache without throwing.
    {
        auto o = baseOptions();
        o.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
        o.defaultCacheName = "a";
        auto manager = FileCacheManager::create(o);
        EXPECT_TRUE(manager->hasDefault());
        EXPECT_EQ(manager->getDefault(), manager->get("a"));
        manager->shutdown();
    }
    // Case 2: an empty defaultCacheName -> hasDefault() is false AND getDefault()
    // throws the "no default cache configured" exception. hasDefault() is false
    // exactly when getDefault() would throw.
    {
        auto o = baseOptions();
        o.caches.push_back({"a", makeConfig(sub("a")), "conf.a"});
        auto manager = FileCacheManager::create(o);
        EXPECT_FALSE(manager->hasDefault());
        EXPECT_THROW(manager->getDefault(), VeloxRuntimeError);
        manager->shutdown();
    }
}

// ==================== B7: opened-handle invalidation ====================

// A real drop-on-remove/rename test for the D1/D2 OpenedFileCache seam. The handle for a path
// is cached; after the path is removed or renamed the cached handle is dropped so a re-open
// returns a fresh handle (a different shared_ptr) rather than a stale one.
class OpenedFileCacheTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        fileSystem_ = test::localFileSystemForTests();
    }

    std::string writeFile(const std::string & name, const std::string & content)
    {
        const auto path = (fs::path(temp_->getPath()) / name).string();
        std::ofstream(path) << content;
        return path;
    }

    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<filesystems::FileSystem> fileSystem_;
};

TEST_F(OpenedFileCacheTest, GetReusesLiveHandleThenDropsOnRemove)
{
    OpenedFileCache cache(*fileSystem_);
    const auto path = writeFile("seg", "hello world");

    auto h1 = cache.get(path);
    ASSERT_NE(h1, nullptr);
    // Live weak hit: same handle reused.
    auto h2 = cache.get(path);
    EXPECT_EQ(h1.get(), h2.get());
    EXPECT_GE(cache.stats().hits, 1u);
    EXPECT_EQ(cache.stats().numLiveHandles, 1u);

    // Invalidate the path -> the cached entry is dropped.
    const size_t erased = cache.removePath(path);
    EXPECT_EQ(erased, 1u);

    // A new get() opens a fresh handle (different pointer), even though the old handles are alive.
    auto h3 = cache.get(path);
    EXPECT_NE(h3.get(), h1.get());
}

TEST_F(OpenedFileCacheTest, LastReleaseErasesEntry)
{
    OpenedFileCache cache(*fileSystem_);
    const auto path = writeFile("seg2", "data");
    {
        auto h = cache.get(path);
        EXPECT_EQ(cache.stats().numCachedFiles, 1u);
    }
    // On last release the custom deleter erases the map entry.
    EXPECT_EQ(cache.stats().numCachedFiles, 0u);
}

// The rename seam (FileSegment::renameToIncludeSizeInNameUnlocked) and the remove seam
// (LockedKey::removeFileSegmentImpl) both call OpenedFileCache::removePath after the physical
// change. Drive removePath through the Manager-owned cache and confirm the handle is dropped.
TEST_F(OpenedFileCacheTest, RenameInvalidatesOldPathHandle)
{
    OpenedFileCache cache(*fileSystem_);
    const auto oldPath = writeFile("100", "segment-bytes");
    auto oldHandle = cache.get(oldPath);
    ASSERT_NE(oldHandle, nullptr);
    EXPECT_EQ(cache.stats().numLiveHandles, 1u);

    // Simulate the rename seam: physically move the file, then invalidate the OLD path.
    const auto newPath = (fs::path(temp_->getPath()) / "100_13").string();
    fs::rename(oldPath, newPath);
    cache.removePath(oldPath);

    // A future segment created again at the old name must open a fresh handle.
    std::ofstream(oldPath) << "new-segment";
    auto fresh = cache.get(oldPath);
    EXPECT_NE(fresh.get(), oldHandle.get());
}

// ============ B7 END-TO-END: seam reached through the real FileCache/FileSegment API ============
//
// These drive the two production seams through real cache operations, sharing the SAME
// OpenedFileCache the FileCache was constructed with (res_.openedFileCache_). Each assertion goes
// RED if the corresponding seam is reverted to the Task-012 no-op (`(void)removed_path;` /
// `(void)renamed;`), because then the cached handle would survive the physical change.

class SeamE2ETest : public ::testing::Test
{
protected:
    void SetUp() override { temp_ = TempDirectoryPath::create(); }
    std::string cachePath() const { return (fs::path(temp_->getPath()) / "cache").string(); }

    // Alignment == 1 so a full-range download completes as DOWNLOADED and triggers the
    // `<offset>` -> `<offset>_<size>` rename (unlike the SCC helper which pins alignment to the
    // segment size to AVOID the rename). Background download disabled; single-threaded metadata.
    FileCacheSettings settings(size_t seg)
    {
        FileCacheSettings s;
        s.path = cachePath();
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

    std::shared_ptr<TempDirectoryPath> temp_;
    test::FileCacheTestResources res_;
};

// (c) REAL removal reaches Metadata.cpp removeFileSegmentImpl -> fs::remove -> removePath seam.
TEST_F(SeamE2ETest, RemoveFileSegmentDropsCachedHandle)
{
    const size_t seg = 4096;
    // Alignment pinned to the segment size so a partial (non-zero) download completes and stays
    // PARTIALLY_DOWNLOADED at a STABLE `<offset>` name (no rename), giving a deterministic on-disk
    // path for the remove seam. The removal path only needs downloaded_size > 0 + the file present.
    auto s = settings(seg);
    s.boundaryAlignment = seg;
    auto cache_ptr = res_.makeFileCache("remove-seam", s, "user-A");
    auto & cache = *cache_ptr;
    cache.initialize();

    auto key = FileCacheKey::random();
    const size_t downloaded = 3000; // < seg -> stays PARTIALLY_DOWNLOADED, file kept as `<offset>`

    // Populate a partially-downloaded Regular segment at `offset`, on the calling thread. Returns
    // its on-disk path.
    auto populate = [&](size_t offset) -> std::string
    {
        CreateFileSegmentSettings create_settings; // Regular
        auto holder = cache.getOrSet(key, offset, seg, offset + seg, create_settings, 0, cache.getCommonOrigin());
        EXPECT_TRUE(holder && !holder->empty());
        auto segment_ptr = holder->getSingleFileSegment();
        EXPECT_TRUE(segment_ptr);
        FileSegment & segment = *segment_ptr;
        std::vector<char> payload(downloaded, 'x');
        EXPECT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
        std::string reason;
        EXPECT_TRUE(segment.reserve(downloaded, 100, reason)) << reason;
        segment.write(payload.data(), downloaded, segment.getCurrentWriteOffset());
        holder->completeAndPopFront(/*allow_background_download*/ false, /*force_shrink*/ false);
        return segment.getPath();
    };

    // Two segments under the SAME key: removing the first leaves the key non-empty (still holds the
    // second), so the removal does NOT empty the key and cannot race the background empty-key
    // cleanup thread — the remove seam is reached deterministically.
    const std::string seg_path = populate(0);
    populate(seg);
    ASSERT_TRUE(fs::exists(seg_path));

    // Cache a read handle for the first segment's file in the SHARED opened-file cache the FileCache
    // was constructed with.
    auto handle = res_.openedFileCache_.get(seg_path);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(res_.openedFileCache_.stats().numCachedFiles, 1u);
    EXPECT_EQ(res_.openedFileCache_.stats().numLiveHandles, 1u);

    // Trigger a REAL removal through the FileCache API: reaches removeFileSegmentImpl -> fs::remove
    // -> removePath(seg_path). If the seam is reverted to `(void)removed_path;`, the entry survives.
    cache.removeFileSegment(key, 0, cache.getCommonOrigin().user_id);
    EXPECT_FALSE(fs::exists(seg_path));

    // The cached entry for the removed path is dropped by the seam (numCachedFiles back to 0), and a
    // re-open produces a FRESH handle (different pointer) even though `handle` is still held here.
    EXPECT_EQ(res_.openedFileCache_.stats().numCachedFiles, 0u);
    std::ofstream(seg_path) << "resurrected";
    auto fresh = res_.openedFileCache_.get(seg_path);
    EXPECT_NE(fresh.get(), handle.get());

    cache.deactivateBackgroundOperations();
}

// (d) REAL size-rename reaches FileSegment.cpp renameToIncludeSizeInNameUnlocked -> fs::rename ->
// removePath(old_path) seam. Cache a handle for the pre-rename `<offset>` path, then complete the
// full download so the rename fires, and assert the OLD path's handle is dropped.
TEST_F(SeamE2ETest, RenameOnDownloadDropsOldPathHandle)
{
    const size_t seg = 4096;
    auto cache_ptr = res_.makeFileCache("rename-seam", settings(seg), "user-A");
    auto & cache = *cache_ptr;
    cache.initialize();

    auto key = FileCacheKey::random();

    // Drive the download up to (but not through) completion so the file exists at the `<offset>`
    // name, cache a handle for THAT old path, then complete -> rename -> removePath(old_path).
    CreateFileSegmentSettings create_settings;
    auto holder = cache.getOrSet(key, 0, seg, seg, create_settings, 0, cache.getCommonOrigin());
    ASSERT_TRUE(holder && !holder->empty());
    auto segment_ptr = holder->getSingleFileSegment();
    ASSERT_TRUE(segment_ptr);
    FileSegment & segment = *segment_ptr;

    std::vector<char> data(seg, 'Z');
    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    std::string reason;
    ASSERT_TRUE(segment.reserve(seg, 100, reason)) << reason;
    segment.write(data.data(), seg, segment.getCurrentWriteOffset());

    const std::string old_path = segment.getPath(); // `<offset>` (size not yet in the name)
    ASSERT_TRUE(fs::exists(old_path));
    // The pre-rename file name is the bare `<offset>` (no `_<size>` suffix). Check the FILE NAME
    // only — the temp directory path itself may contain underscores (e.g. `velox_test_*`).
    ASSERT_EQ(fs::path(old_path).filename().string().find('_'), std::string::npos)
        << "expected the pre-rename <offset> file name, got " << old_path;

    // Cache a handle for the OLD path in the SHARED opened-file cache.
    auto oldHandle = res_.openedFileCache_.get(old_path);
    ASSERT_NE(oldHandle, nullptr);
    EXPECT_EQ(res_.openedFileCache_.stats().numLiveHandles, 1u);

    // Complete the FULL download -> resetDownloadingStateUnlocked (downloaded == range size) ->
    // setDownloadedUnlocked -> renameToIncludeSizeInNameUnlocked: fs::rename(old_path, new_path)
    // then removePath(old_path). The file moves to `<offset>_<size>`.
    segment.completePartAndResetDownloader();
    EXPECT_EQ(segment.state(), FileSegmentState::DOWNLOADED);
    holder->completeAndPopFront(/*allow_background_download*/ false, /*force_shrink*/ false);
    EXPECT_FALSE(fs::exists(old_path));

    // The OLD path's cached handle is dropped by the seam. If the seam is reverted to
    // `(void)renamed;`, the entry survives and a re-open would reuse the stale handle.
    EXPECT_EQ(res_.openedFileCache_.stats().numCachedFiles, 0u);
    std::ofstream(old_path) << "new-segment-at-old-name";
    auto fresh = res_.openedFileCache_.get(old_path);
    EXPECT_NE(fresh.get(), oldHandle.get());

    cache.deactivateBackgroundOperations();
}

} // namespace
} // namespace facebook::velox::ch
