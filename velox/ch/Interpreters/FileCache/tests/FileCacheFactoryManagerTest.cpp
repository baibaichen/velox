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
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/common/testutil/TestValue.h"

#include <folly/futures/ManualTimekeeper.h>

#include <gtest/gtest.h>

#include <atomic>
#include <fstream>
#include <future>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using velox::common::testutil::ScopedTestValue;
using velox::common::testutil::TestValue;
using velox::common::testutil::TempDirectoryPath;

// Mirror of FileCacheManager's per-cache worker-budget formula, kept in the test
// so budget assertions are exact and independent of the production computation.
size_t expectedCacheWorkerMax(const FileCacheConfig & c)
{
    size_t total = c.loadMetadataThreads;
    total += c.loadMetadataAsynchronously ? 1 : 0;
    total += c.backgroundDownloadThreads;
    total += 1; // metadata cleanup worker
    total += 1; // scheduled background-cleanup callback
    if (c.keepFreeSpaceSizeRatio != 0.0 || c.keepFreeSpaceElementsRatio != 0.0)
        total += 1 + c.keepFreeSpaceEvictionThreads;
    return total;
}

class FileCacheFactoryManagerTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        TestValue::enable();
        filesystems::registerLocalFileSystem();
    }

    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("filecache-mgr-test");
        root_ = TempDirectoryPath::create();
        fileSystem_ = filesystems::getFileSystem(root_->getPath(), {});
    }

    void TearDown() override
    {
        // Detach any installed global instance before destroying managers so the
        // static atomic never dangles across tests.
        if (FileCacheManager::getInstance() != nullptr)
            FileCacheManager::setInstance(nullptr);
        for (auto & manager : managers_)
        {
            if (manager)
                manager->shutdown();
        }
        managers_.clear();
        dirs_.clear();
    }

    // Absolute, normalized cache directory unique to this test invocation.
    std::string newCacheDir()
    {
        auto dir = TempDirectoryPath::create();
        dirs_.push_back(dir);
        return dir->getPath();
    }

    FileCacheConfig makeConfig(
        const std::string & path,
        const std::function<void(FileCacheConfig &)> & mutate = {})
    {
        FileCacheConfig config;
        config.path = path;
        config.maxSize = 16ull << 20;
        config.maxFileSegmentSize = 1ull << 20;
        config.boundaryAlignment = 4096;
        // Keep pools tiny and metadata-load fail-close cheap.
        config.loadMetadataThreads = 2;
        config.backgroundDownloadThreads = 1;
        if (mutate)
            mutate(config);
        return config;
    }

    FileCacheManager::Options baseOptions()
    {
        FileCacheManager::Options options;
        options.commonUserId = "common-user";
        options.cachePathPrefix = root_->getPath();
        options.allowedCacheRoot = root_->getPath();
        options.localFileSystem = fileSystem_;
        options.memoryPool = pool_.get();
        options.timekeeper = timekeeper_;
        options.initializeOnCreate = false;
        return options;
    }

    std::shared_ptr<FileCacheManager> track(std::shared_ptr<FileCacheManager> manager)
    {
        managers_.push_back(manager);
        return manager;
    }

    // Empty manager (no caches) usable for dynamic getOrCreate / budget probes.
    std::shared_ptr<FileCacheManager> makeEmptyManager()
    {
        return track(FileCacheManager::create(baseOptions()));
    }

    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<folly::Timekeeper> timekeeper_ =
        std::make_shared<folly::ManualTimekeeper>();
    std::shared_ptr<filesystems::FileSystem> fileSystem_;
    std::shared_ptr<TempDirectoryPath> root_;
    std::vector<std::shared_ptr<TempDirectoryPath>> dirs_;
    std::vector<std::shared_ptr<FileCacheManager>> managers_;
};

// ===========================================================================
// Factory registry
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, CreateGetAndMissing)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto cache = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    ASSERT_NE(cache, nullptr);
    EXPECT_EQ(factory.get("cache-a"), cache);

    // Missing name is an explicit error, not a null return.
    EXPECT_ANY_THROW(factory.get("does-not-exist"));
}

TEST_F(FileCacheFactoryManagerTest, SameNameEqualSettingsReturnsSame)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto first = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    auto second = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    EXPECT_EQ(first, second);
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
}

TEST_F(FileCacheFactoryManagerTest, SameNameDifferentSettingsRejected)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto cache = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");

    // Rebinding "cache-a" to a different effective setting is a conflict; the
    // original binding must survive.
    EXPECT_ANY_THROW(factory.getOrCreate(
        "cache-a", makeConfig(dir, [](FileCacheConfig & c) { c.maxSize = 99ull << 20; }), "cfg.a"));
    EXPECT_EQ(factory.get("cache-a"), cache);
}

TEST_F(FileCacheFactoryManagerTest, SamePathAliasDedup)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto cache = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    // Different name, same path, equal settings -> alias to the same FileCache.
    auto alias = factory.getOrCreate("cache-b", makeConfig(dir), "cfg.b");
    EXPECT_EQ(cache, alias);

    // getAll shows both names; getUniqueInstances deduplicates to one instance.
    EXPECT_EQ(factory.getAll().size(), 2u);
    EXPECT_EQ(factory.getUniqueInstances().size(), 1u);
    EXPECT_EQ(factory.get("cache-a"), factory.get("cache-b"));
}

TEST_F(FileCacheFactoryManagerTest, SamePathDifferentSettingsRejected)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    // Same on-disk directory but a different effective setting -> reject: two
    // algorithm instances must never share one directory.
    EXPECT_ANY_THROW(factory.getOrCreate(
        "cache-b", makeConfig(dir, [](FileCacheConfig & c) { c.maxSize = 99ull << 20; }), "cfg.b"));
}

TEST_F(FileCacheFactoryManagerTest, CreateRejectsExistingName)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    factory.create("cache-a", makeConfig(dir), "cfg.a");
    // create() always rejects an existing name, even with equal settings.
    EXPECT_ANY_THROW(factory.create("cache-a", makeConfig(dir), "cfg.a"));
}

TEST_F(FileCacheFactoryManagerTest, CreateAliasesNewNameSamePath)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto cache = factory.create("cache-a", makeConfig(dir), "cfg.a");
    // A new name pointing at an existing path with equal settings is a legal alias.
    auto alias = factory.create("cache-b", makeConfig(dir), "cfg.b");
    EXPECT_EQ(cache, alias);
    EXPECT_EQ(factory.getUniqueInstances().size(), 1u);
}

TEST_F(FileCacheFactoryManagerTest, AliasEnumerationAndRemoveAll)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    auto cache = factory.getOrCreate("cache-a", makeConfig(dir), "cfg.a");
    factory.getOrCreate("cache-b", makeConfig(dir), "cfg.b");
    ASSERT_EQ(factory.getAll().size(), 2u);

    // remove() erases every alias that points at the cache.
    factory.remove(cache);
    EXPECT_TRUE(factory.getAll().empty());
    EXPECT_ANY_THROW(factory.get("cache-a"));
    EXPECT_ANY_THROW(factory.get("cache-b"));
}

TEST_F(FileCacheFactoryManagerTest, NameRebindConflictPreservesOriginal)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto oldDir = newCacheDir();
    const auto newDir = newCacheDir();
    auto a = factory.getOrCreate("A", makeConfig(oldDir), "cfg.A");
    auto b = factory.getOrCreate("B", makeConfig(newDir), "cfg.B");
    ASSERT_NE(a, b);

    // getOrCreate("A", settings-of-/new) is a conflicting rebind and must fail
    // WITHOUT silently returning B or corrupting A's binding (CH edge-bug fix).
    EXPECT_ANY_THROW(factory.getOrCreate("A", makeConfig(newDir), "cfg.A"));
    EXPECT_EQ(factory.get("A"), a);
    EXPECT_EQ(factory.get("B"), b);
}

// ===========================================================================
// Worker budget
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, WorkerBudgetGrowsOncePerUniqueCache)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();
    // Empty manager: worker budget starts at the minimum.
    EXPECT_EQ(factory.workerBudget(), 1u);

    const auto dirA = newCacheDir();
    const auto configA = makeConfig(dirA);
    factory.getOrCreate("A", configA, "cfg.A");
    const size_t afterA = expectedCacheWorkerMax(configA);
    EXPECT_EQ(factory.workerBudget(), afterA);
    EXPECT_GE(manager->workerPool().numThreads(), afterA);

    // A name alias (same path/settings) must NOT grow the budget.
    factory.getOrCreate("A-alias", configA, "cfg.A2");
    EXPECT_EQ(factory.workerBudget(), afterA);

    // A second unique cache grows the budget by exactly its own contribution.
    const auto dirB = newCacheDir();
    const auto configB = makeConfig(dirB, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 3; });
    factory.getOrCreate("B", configB, "cfg.B");
    EXPECT_EQ(factory.workerBudget(), afterA + expectedCacheWorkerMax(configB));
}

TEST_F(FileCacheFactoryManagerTest, FreeSpaceKeepingContributesToBudget)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    const auto config = makeConfig(dir, [](FileCacheConfig & c) {
        c.keepFreeSpaceSizeRatio = 0.1;
        c.keepFreeSpaceEvictionThreads = 3;
    });
    factory.getOrCreate("A", config, "cfg.A");
    // 2 load + 1 bg-download + 1 cleanup + 1 scheduled + (1 collector + 3 evict).
    EXPECT_EQ(factory.workerBudget(), expectedCacheWorkerMax(config));
    EXPECT_EQ(factory.workerBudget(), 2u + 1u + 1u + 1u + 1u + 3u);
}

TEST_F(FileCacheFactoryManagerTest, WorkerBudgetOverflowThroughCheckedAdd)
{
    auto manager = makeEmptyManager();
    auto & factory = manager->factory();

    const auto dir = newCacheDir();
    // loadMetadataThreads at the ceiling forces the checked summation to overflow.
    auto config = makeConfig(dir, [](FileCacheConfig & c) {
        c.loadMetadataThreads = std::numeric_limits<uint64_t>::max();
    });
    EXPECT_ANY_THROW(factory.getOrCreate("A", config, "cfg.A"));
    // The failed grow rolls back: the registry has no partial entry.
    EXPECT_ANY_THROW(factory.get("A"));
    EXPECT_EQ(factory.workerBudget(), 1u);
}

// ===========================================================================
// Manager two-phase create + initialize
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, CreateRegistersCachesAndSizesPool)
{
    const auto dirA = newCacheDir();
    const auto dirB = newCacheDir();
    auto options = baseOptions();
    options.caches = {
        {"A", makeConfig(dirA), "cfg.A"},
        {"B", makeConfig(dirB), "cfg.B"},
    };
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));

    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 2u);
    EXPECT_EQ(manager->get("A"), manager->getDefault());
    const size_t expected =
        expectedCacheWorkerMax(makeConfig(dirA)) + expectedCacheWorkerMax(makeConfig(dirB));
    EXPECT_GE(manager->workerPool().numThreads(), expected);
    EXPECT_EQ(manager->factory().workerBudget(), expected);
}

TEST_F(FileCacheFactoryManagerTest, AliasesRegisterOnceAndShareInstance)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir), "cfg.A2"},
    };
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));

    // Two names, one unique cache; the budget only reflects one contribution.
    EXPECT_EQ(manager->factory().getAll().size(), 2u);
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
    EXPECT_EQ(manager->factory().workerBudget(), expectedCacheWorkerMax(makeConfig(dir)));
}

TEST_F(FileCacheFactoryManagerTest, InitializeRunsOncePerUniqueCache)
{
    const auto dir = newCacheDir();
    const auto dirB = newCacheDir();
    auto options = baseOptions();
    options.caches = {
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir), "cfg.A2"}, // alias, must not re-initialize
        {"B", makeConfig(dirB), "cfg.B"},
    };
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));

    std::mutex mutex;
    std::vector<const void *> initialized;
    ScopedTestValue perCache(
        "facebook::velox::ch::FileCacheManager::initialize::perUniqueCache",
        std::function<void(void *)>([&](void * cache) {
            std::lock_guard<std::mutex> g(mutex);
            initialized.push_back(cache);
        }));

    manager->initialize();
    // Exactly two unique caches were initialized (the alias did not add a third).
    std::lock_guard<std::mutex> g(mutex);
    EXPECT_EQ(initialized.size(), 2u);
    EXPECT_TRUE(manager->get("A")->isInitialized());
    EXPECT_TRUE(manager->get("B")->isInitialized());
}

TEST_F(FileCacheFactoryManagerTest, InitializeOnCreateInitializesAllCaches)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.initializeOnCreate = true;
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    EXPECT_TRUE(manager->get("A")->isInitialized());
}

// ===========================================================================
// Singleton publication / release
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, GlobalInstallGetUninstall)
{
    // Before install, both entry points are explicit errors.
    EXPECT_EQ(FileCacheManager::getInstance(), nullptr);
    EXPECT_ANY_THROW(FileCacheManager::instance());
    EXPECT_ANY_THROW(FileCacheFactory::instance());

    auto manager = makeEmptyManager();
    FileCacheManager::setInstance(manager.get());

    // Both the Manager and its Factory become reachable.
    EXPECT_EQ(FileCacheManager::getInstance(), manager.get());
    EXPECT_EQ(&FileCacheManager::instance(), manager.get());
    EXPECT_EQ(&FileCacheFactory::instance(), &manager->factory());

    // Uninstall clears both entry points.
    FileCacheManager::setInstance(nullptr);
    EXPECT_EQ(FileCacheManager::getInstance(), nullptr);
    EXPECT_ANY_THROW(FileCacheManager::instance());
    EXPECT_ANY_THROW(FileCacheFactory::instance());
}

TEST_F(FileCacheFactoryManagerTest, LiveReplacementRejected)
{
    auto first = makeEmptyManager();
    auto second = makeEmptyManager();
    FileCacheManager::setInstance(first.get());

    // Re-installing the same manager is a no-op, not an error.
    EXPECT_NO_THROW(FileCacheManager::setInstance(first.get()));
    // Replacing a live, different manager is forbidden.
    EXPECT_ANY_THROW(FileCacheManager::setInstance(second.get()));
    EXPECT_EQ(FileCacheManager::getInstance(), first.get());

    FileCacheManager::setInstance(nullptr);
}

// ===========================================================================
// Reload / applyConfigs
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, EqualReloadIsNoOp)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    // Applying an identical config must not throw and must not change settings.
    EXPECT_NO_THROW(manager->applyConfigs({{"A", makeConfig(dir), "cfg.A"}}));
    EXPECT_EQ(manager->factory().getByName("A")->getSettings(), makeConfig(dir));
}

TEST_F(FileCacheFactoryManagerTest, ReloadRejectsPathChange)
{
    const auto dir = newCacheDir();
    const auto otherDir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    // The on-disk path is immutable; reloading with a new path is rejected.
    EXPECT_ANY_THROW(manager->applyConfigs({{"A", makeConfig(otherDir), "cfg.A"}}));
}

TEST_F(FileCacheFactoryManagerTest, ReloadDeduplicatesAliases)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir), "cfg.A2"},
    };
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    // Both aliases resolve to equal settings -> a single apply, no error.
    EXPECT_NO_THROW(manager->applyConfigs({
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir), "cfg.A2"},
    }));

    // Aliases carrying inconsistent settings for the same instance are rejected.
    EXPECT_ANY_THROW(manager->applyConfigs({
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir, [](FileCacheConfig & c) { c.maxSize = 99ull << 20; }), "cfg.A2"},
    }));
}

TEST_F(FileCacheFactoryManagerTest, ApplySettingsRunsOutsideRegistryLock)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::atomic<bool> lockFreeDuringApply{false};
    std::atomic<bool> seamFired{false};
    ScopedTestValue outsideLock(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            seamFired = true;
            lockFreeDuringApply = manager->factory().isRegistryLockFree();
        }));

    // Change a live-updatable setting so the apply path actually runs.
    manager->applyConfigs({{"A", makeConfig(dir, [](FileCacheConfig & c) { c.maxSize = 32ull << 20; }), "cfg.A"}});
    EXPECT_TRUE(seamFired);
    EXPECT_TRUE(lockFreeDuringApply);
}

// ===========================================================================
// clear / deactivate outside lock / reusability
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, ClearDeactivatesOutsideLockAndKeepsManagerReusable)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::atomic<int> deactivations{0};
    std::atomic<bool> lockFree{true};
    ScopedTestValue outsideLock(
        "facebook::velox::ch::FileCacheFactory::deactivateOutsideLock",
        std::function<void(void *)>([&](void *) {
            ++deactivations;
            if (!manager->factory().isRegistryLockFree())
                lockFree = false;
        }));

    manager->factory().clear();
    EXPECT_EQ(deactivations.load(), 1);
    EXPECT_TRUE(lockFree.load());
    EXPECT_TRUE(manager->factory().getAll().empty());
    // Worker budget shrinks back to the minimum after clear.
    EXPECT_EQ(manager->factory().workerBudget(), 1u);

    // The manager stays usable: a fresh cache can be created afterwards.
    const auto dir2 = newCacheDir();
    auto fresh = manager->factory().getOrCreate("C", makeConfig(dir2), "cfg.C");
    EXPECT_NE(fresh, nullptr);
    EXPECT_EQ(manager->factory().workerBudget(), expectedCacheWorkerMax(makeConfig(dir2)));
}

TEST_F(FileCacheFactoryManagerTest, RemoveDeactivatesOutsideLock)
{
    auto manager = makeEmptyManager();
    const auto dir = newCacheDir();
    auto cache = manager->factory().getOrCreate("A", makeConfig(dir), "cfg.A");
    manager->initialize();

    std::atomic<bool> lockFree{true};
    std::atomic<int> deactivations{0};
    ScopedTestValue outsideLock(
        "facebook::velox::ch::FileCacheFactory::deactivateOutsideLock",
        std::function<void(void *)>([&](void *) {
            ++deactivations;
            if (!manager->factory().isRegistryLockFree())
                lockFree = false;
        }));

    manager->factory().remove(cache);
    EXPECT_EQ(deactivations.load(), 1);
    EXPECT_TRUE(lockFree.load());
    EXPECT_ANY_THROW(manager->factory().get("A"));
}

// ===========================================================================
// shutdown
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, ShutdownIdempotentAndOperationsFail)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    manager->shutdown();
    // Idempotent: a second shutdown is a no-op.
    EXPECT_NO_THROW(manager->shutdown());

    // Post-shutdown registry / lifecycle operations are rejected.
    EXPECT_ANY_THROW(manager->factory().getOrCreate("B", makeConfig(newCacheDir()), "cfg.B"));
    EXPECT_ANY_THROW(manager->applyConfigs({{"A", makeConfig(dir), "cfg.A"}}));
    EXPECT_ANY_THROW(manager->initialize());
}

TEST_F(FileCacheFactoryManagerTest, ConcurrentShutdownReachesTerminalState)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&] { manager->shutdown(); });
    for (auto & t : threads)
        t.join();
    // All callers observed a clean terminal state; a later operation still fails.
    EXPECT_ANY_THROW(manager->initialize());
}

TEST_F(FileCacheFactoryManagerTest, ResourceShutdownOrder)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::mutex mutex;
    std::vector<std::string> order;
    auto record = [&](const std::string & step) {
        return std::function<void(void *)>([&, step](void *) {
            std::lock_guard<std::mutex> g(mutex);
            order.push_back(step);
        });
    };
    ScopedTestValue s1("facebook::velox::ch::FileCacheManager::shutdown::afterCachesDeactivated", record("caches"));
    ScopedTestValue s2("facebook::velox::ch::FileCacheManager::shutdown::afterSchedulerShutdown", record("timers"));
    ScopedTestValue s3("facebook::velox::ch::FileCacheManager::shutdown::afterWorkerPoolShutdown", record("pool"));
    ScopedTestValue s4("facebook::velox::ch::FileCacheManager::shutdown::afterHandlesCleared", record("handles"));

    manager->shutdown();
    std::lock_guard<std::mutex> g(mutex);
    ASSERT_EQ(order.size(), 4u);
    EXPECT_EQ(order[0], "caches");
    EXPECT_EQ(order[1], "timers");
    EXPECT_EQ(order[2], "pool");
    EXPECT_EQ(order[3], "handles");
}

TEST_F(FileCacheFactoryManagerTest, ExternalPointerSeesInactiveCacheAfterShutdown)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    // An external holder outlives registry removal; the runtime services it was
    // built on (worker pool / scheduler) belong to the manager and outlive it.
    FileCachePtr external = manager->get("A");
    manager->shutdown();
    ASSERT_NE(external, nullptr);
    EXPECT_EQ(external->getName(), "A");
}

// ===========================================================================
// OpenedFileCache (user-approved dependency mapping)
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, OpenedFileCacheSharesAndInvalidatesHandles)
{
    auto manager = makeEmptyManager();
    auto & opened = manager->openedFileCache();

    // Materialize a real file so get() can open it.
    const auto filePath = root_->getPath() + "/opened-a.bin";
    { std::ofstream(filePath) << "payload"; }

    auto first = opened.get(filePath, 0);
    ASSERT_NE(first, nullptr);
    // A second get shares the same handle (weak-pointer sharing).
    auto second = opened.get(filePath, 0);
    EXPECT_EQ(first.get(), second.get());
    EXPECT_EQ(opened.count(), 1u);

    // Removing by path invalidates the cached handle regardless of flags.
    opened.remove(filePath);
    EXPECT_EQ(opened.count(), 0u);
    auto reopened = opened.get(filePath, 0);
    EXPECT_NE(reopened.get(), first.get());
}

TEST_F(FileCacheFactoryManagerTest, OpenedFileCacheReleasesOnLastHolder)
{
    auto manager = makeEmptyManager();
    auto & opened = manager->openedFileCache();

    const auto filePath = root_->getPath() + "/opened-b.bin";
    { std::ofstream(filePath) << "payload"; }

    {
        auto handle = opened.get(filePath, 0);
        EXPECT_EQ(opened.count(), 1u);
    }
    // Dropping the last strong reference erases the weak entry (custom deleter).
    EXPECT_EQ(opened.count(), 0u);

    opened.clear();
    EXPECT_EQ(opened.count(), 0u);
}

// ===========================================================================
// stats
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, RefreshStatsCountsUniqueCaches)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {
        {"A", makeConfig(dir), "cfg.A"},
        {"A-alias", makeConfig(dir), "cfg.A2"},
    };
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    auto stats = manager->refreshStats();
    EXPECT_EQ(stats.uniqueCaches, 1u);
    EXPECT_EQ(stats.cachesByName.size(), 2u); // alias names appear separately
    EXPECT_GE(stats.workerPoolMax, expectedCacheWorkerMax(makeConfig(dir)));
}

// ===========================================================================
// applyConfigs: new caches / new aliases / init-failure rollback (fix wave)
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, ApplyConfigsCreatesAndInitializesNewUniqueCache)
{
    const auto dirA = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dirA), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::mutex mutex;
    std::vector<const void *> initializedNew;
    ScopedTestValue newCacheSeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::initializeNewCache",
        std::function<void(void *)>([&](void * cache) {
            std::lock_guard<std::mutex> g(mutex);
            initializedNew.push_back(cache);
        }));

    // Reload carries the existing cache (equal, a no-op) plus a brand-new unique
    // cache B: an initialized Manager must register AND initialize B exactly once.
    const auto dirB = newCacheDir();
    manager->applyConfigs({{"A", makeConfig(dirA), "cfg.A"}, {"B", makeConfig(dirB), "cfg.B"}});

    {
        std::lock_guard<std::mutex> g(mutex);
        ASSERT_EQ(initializedNew.size(), 1u);
        EXPECT_EQ(initializedNew.front(), manager->get("B").get());
    }
    EXPECT_TRUE(manager->get("B")->isInitialized());
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 2u);
    const size_t expected =
        expectedCacheWorkerMax(makeConfig(dirA)) + expectedCacheWorkerMax(makeConfig(dirB));
    EXPECT_EQ(manager->factory().workerBudget(), expected);
    EXPECT_GE(manager->workerPool().numThreads(), expected);
}

TEST_F(FileCacheFactoryManagerTest, ApplyConfigsNewAliasDoesNotReinitialize)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::atomic<int> newInits{0};
    ScopedTestValue newCacheSeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::initializeNewCache",
        std::function<void(void *)>([&](void *) { ++newInits; }));

    // A brand-new NAME pointing at A's existing path is an alias, not a new unique
    // cache: it must be registered but must NOT re-initialize the shared instance.
    manager->applyConfigs({{"A-alias", makeConfig(dir), "cfg.A2"}});

    EXPECT_EQ(newInits.load(), 0);
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
    EXPECT_EQ(manager->factory().getAll().size(), 2u);
    EXPECT_EQ(manager->get("A-alias"), manager->get("A"));
    EXPECT_EQ(manager->factory().workerBudget(), expectedCacheWorkerMax(makeConfig(dir)));
}

TEST_F(FileCacheFactoryManagerTest, ApplyConfigsNewCacheInitFailureRollsBack)
{
    const auto dirA = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dirA), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    const size_t budgetBefore = manager->factory().workerBudget();

    // A new cache whose initialize() fails closed: max_size exceeds disk capacity,
    // which FileCache::initialize rejects synchronously.
    const auto dirBad = newCacheDir();
    auto badConfig = makeConfig(dirBad, [](FileCacheConfig & c) { c.maxSize = 1ull << 50; });

    EXPECT_ANY_THROW(
        manager->applyConfigs({{"A", makeConfig(dirA), "cfg.A"}, {"bad", badConfig, "cfg.bad"}}));

    // Fail closed: the failed new cache left no retrievable binding.
    EXPECT_ANY_THROW(manager->get("bad"));
    EXPECT_ANY_THROW(manager->factory().getByName("bad"));
    // The pre-existing binding is preserved and still initialized.
    EXPECT_TRUE(manager->get("A")->isInitialized());
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
    // Truthful budget/pool restored to the surviving aggregate.
    EXPECT_EQ(manager->factory().workerBudget(), budgetBefore);
    EXPECT_GE(manager->workerPool().numThreads(), budgetBefore);
}

// A reload that registers one valid brand-new cache and then hits a conflicting
// later alias (same path, different settings) must fail-close the WHOLE new-cache
// registration phase: the earlier valid new cache/name/path is rolled back too,
// the pre-existing binding is untouched, and the worker budget/pool size return to
// their pre-apply values. This exercises the registration path (getOrCreateLocked
// throwing mid-loop), not the initialize path.
TEST_F(FileCacheFactoryManagerTest, ApplyConfigsNewCacheRegistrationFailureRollsBack)
{
    const auto dirA = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dirA), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    auto cacheA = manager->get("A");
    const size_t budgetBefore = manager->factory().workerBudget();
    const size_t poolBefore = manager->workerPool().numThreads();

    // "fresh" is a valid brand-new unique cache. "clash" is a new NAME pointing at
    // the SAME on-disk path as "fresh" but with different settings, which
    // getOrCreateLocked rejects as a conflicting alias -- and it is ordered AFTER
    // "fresh", so "fresh" has already been registered (and the pool/budget grown)
    // when the conflict throws.
    const auto dirFresh = newCacheDir();
    auto freshConfig = makeConfig(dirFresh);
    auto clashConfig = makeConfig(dirFresh, [](FileCacheConfig & c) { c.maxSize = 99ull << 20; });

    EXPECT_ANY_THROW(manager->applyConfigs(
        {{"A", makeConfig(dirA), "cfg.A"},
         {"fresh", freshConfig, "cfg.fresh"},
         {"clash", clashConfig, "cfg.clash"}}));

    // No leftover of either new name, the new instance, or the new path.
    EXPECT_ANY_THROW(manager->get("fresh"));
    EXPECT_ANY_THROW(manager->factory().getByName("fresh"));
    EXPECT_ANY_THROW(manager->get("clash"));
    EXPECT_ANY_THROW(manager->factory().getByName("clash"));
    EXPECT_EQ(manager->factory().getUniqueInstances().size(), 1u);
    EXPECT_EQ(manager->factory().getAll().size(), 1u);

    // The pre-existing binding is preserved unchanged and still initialized.
    EXPECT_EQ(manager->get("A"), cacheA);
    EXPECT_TRUE(manager->get("A")->isInitialized());

    // Truthful budget and pool size restored to their pre-apply values.
    EXPECT_EQ(manager->factory().workerBudget(), budgetBefore);
    EXPECT_EQ(manager->workerPool().numThreads(), poolBefore);
}

// ===========================================================================
// applyConfigs: worker-budget reload evidence (fix wave)
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, ReloadRaiseGrowsPoolBeforeApply)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    const size_t before = expectedCacheWorkerMax(makeConfig(dir));
    auto raised = makeConfig(dir, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 5; });
    const size_t after = expectedCacheWorkerMax(raised);
    ASSERT_GT(after, before);

    std::atomic<size_t> poolAtApply{0};
    std::atomic<bool> seamFired{false};
    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            seamFired = true;
            poolAtApply = manager->workerPool().numThreads();
        }));

    manager->applyConfigs({{"A", raised, "cfg.A"}});
    EXPECT_TRUE(seamFired);
    // Grow happened BEFORE the per-cache apply: the pool already covers the raised
    // aggregate need when the apply seam fires.
    EXPECT_GE(poolAtApply.load(), after);
    EXPECT_GE(manager->workerPool().numThreads(), after);
    EXPECT_EQ(manager->factory().workerBudget(), after);
}

TEST_F(FileCacheFactoryManagerTest, ReloadLowerShrinksPoolAfterQuiesce)
{
    const auto dir = newCacheDir();
    auto highConfig = makeConfig(dir, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 5; });
    auto options = baseOptions();
    options.caches = {{"A", highConfig, "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    const size_t high = expectedCacheWorkerMax(highConfig);
    ASSERT_GE(manager->workerPool().numThreads(), high);
    ASSERT_EQ(manager->factory().workerBudget(), high);

    auto lowConfig = makeConfig(dir, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 1; });
    const size_t low = expectedCacheWorkerMax(lowConfig);
    ASSERT_LT(low, high);

    std::atomic<size_t> poolAtApply{0};
    std::atomic<bool> seamFired{false};
    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            seamFired = true;
            poolAtApply = manager->workerPool().numThreads();
        }));

    manager->applyConfigs({{"A", lowConfig, "cfg.A"}});
    EXPECT_TRUE(seamFired);
    // The pool was NOT shrunk before/at apply time: the shrink is deferred until
    // the excess download workers have been quiesced by applySettingsIfPossible.
    EXPECT_EQ(poolAtApply.load(), high);
    // After apply, the pool shrank to the new aggregate need, never below it.
    EXPECT_EQ(manager->factory().workerBudget(), low);
    EXPECT_GE(manager->workerPool().numThreads(), low);
}

TEST_F(FileCacheFactoryManagerTest, ReloadPartialApplyKeepsTruthfulSnapshotsAndFirstException)
{
    const auto dirA = newCacheDir();
    const auto dirB = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dirA), "cfg.A"}, {"B", makeConfig(dirB), "cfg.B"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    const void * cacheA = manager->get("A").get();

    auto raisedA = makeConfig(dirA, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 4; });
    auto raisedB = makeConfig(dirB, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 3; });

    // Inject an apply failure for cache A only; B's apply must still run.
    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void * cache) {
            if (cache == cacheA)
                VELOX_FAIL("injected apply failure for cache A");
        }));

    EXPECT_ANY_THROW(manager->applyConfigs({{"A", raisedA, "cfg.A"}, {"B", raisedB, "cfg.B"}}));

    // A keeps its truthful (unchanged) snapshot: nothing was applied before the throw.
    EXPECT_EQ(manager->factory().getByName("A")->getSettings(), makeConfig(dirA));
    // B was applied successfully (partial apply is not pretended to roll back).
    EXPECT_EQ(manager->factory().getByName("B")->getSettings().backgroundDownloadThreads, 3u);
    // Worker budget recomputed from the truthful snapshots: A original + B raised.
    const size_t expected =
        expectedCacheWorkerMax(makeConfig(dirA)) + expectedCacheWorkerMax(raisedB);
    EXPECT_EQ(manager->factory().workerBudget(), expected);
    EXPECT_GE(manager->workerPool().numThreads(), expected);
}

// ===========================================================================
// Serialized mutating lifecycle/pool operations (fix wave)
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, ApplyConfigsHoldsMutationLockDuringApply)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::atomic<bool> seamFired{false};
    std::atomic<bool> mutationHeld{false};
    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            seamFired = true;
            mutationHeld = !manager->isMutationLockFree();
        }));

    manager->applyConfigs({{"A", makeConfig(dir, [](FileCacheConfig & c) { c.maxSize = 32ull << 20; }), "cfg.A"}});
    EXPECT_TRUE(seamFired);
    // apply runs its registry/budget/pool mutation under the single serialization
    // lock; a concurrent mutation cannot interleave.
    EXPECT_TRUE(mutationHeld);
}

TEST_F(FileCacheFactoryManagerTest, ApplyVsGetOrCreateSerialized)
{
    const auto dirA = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dirA), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::promise<void> applyInSeam;
    std::promise<void> releaseApply;
    auto applyInSeamF = applyInSeam.get_future();
    auto releaseApplyF = releaseApply.get_future();
    std::atomic<bool> signaled{false};
    std::atomic<bool> mutationHeldDuringApply{false};

    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            if (!signaled.exchange(true))
            {
                mutationHeldDuringApply = !manager->isMutationLockFree();
                applyInSeam.set_value();
                releaseApplyF.wait(); // hold the mutation lock until released
            }
        }));

    auto raisedA = makeConfig(dirA, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 4; });
    std::thread applyThread([&] { manager->applyConfigs({{"A", raisedA, "cfg.A"}}); });

    applyInSeamF.wait(); // apply now holds the mutation lock at the seam

    // getOrCreate from another thread must serialize behind apply (blocks on the
    // same mutation lock) instead of racing the shared-pool budget.
    const auto dirB = newCacheDir();
    auto configB = makeConfig(dirB);
    std::atomic<bool> getOrCreateDone{false};
    std::thread getThread([&] {
        manager->factory().getOrCreate("B", configB, "cfg.B");
        getOrCreateDone = true;
    });

    releaseApply.set_value();
    applyThread.join();
    getThread.join();

    EXPECT_TRUE(mutationHeldDuringApply);
    EXPECT_TRUE(getOrCreateDone.load());
    // Serialized: neither operation overwrote the other's worker budget, so the
    // final budget is the exact aggregate of A(raised) + B and the pool covers it.
    const size_t expected = expectedCacheWorkerMax(raisedA) + expectedCacheWorkerMax(configB);
    EXPECT_EQ(manager->factory().workerBudget(), expected);
    EXPECT_GE(manager->workerPool().numThreads(), expected);
}

TEST_F(FileCacheFactoryManagerTest, ApplyVsShutdownSerialized)
{
    const auto dir = newCacheDir();
    auto options = baseOptions();
    options.caches = {{"A", makeConfig(dir), "cfg.A"}};
    options.defaultCacheName = "A";
    auto manager = track(FileCacheManager::create(options));
    manager->initialize();

    std::promise<void> applyInSeam;
    std::promise<void> releaseApply;
    auto applyInSeamF = applyInSeam.get_future();
    auto releaseApplyF = releaseApply.get_future();
    std::atomic<bool> signaled{false};
    std::atomic<bool> mutationHeldDuringApply{false};

    ScopedTestValue applySeam(
        "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock",
        std::function<void(void *)>([&](void *) {
            if (!signaled.exchange(true))
            {
                mutationHeldDuringApply = !manager->isMutationLockFree();
                applyInSeam.set_value();
                releaseApplyF.wait();
            }
        }));

    auto raisedA = makeConfig(dir, [](FileCacheConfig & c) { c.backgroundDownloadThreads = 4; });
    std::thread applyThread([&] { manager->applyConfigs({{"A", raisedA, "cfg.A"}}); });
    applyInSeamF.wait(); // apply holds the mutation lock at the seam

    // shutdown must serialize behind apply; it cannot tear the pool down while
    // apply is resizing it.
    std::thread shutdownThread([&] { manager->shutdown(); });

    releaseApply.set_value();
    applyThread.join();
    shutdownThread.join();

    EXPECT_TRUE(mutationHeldDuringApply);
    // The manager reached the terminal shutdown state; later operations fail.
    EXPECT_ANY_THROW(manager->initialize());
}

// ===========================================================================
// OpenedFileCache handle outliving the cache (fix wave)
// ===========================================================================

TEST_F(FileCacheFactoryManagerTest, OpenedFileCacheHandleOutlivesCache)
{
    const auto filePath = root_->getPath() + "/opened-outlive.bin";
    { std::ofstream(filePath) << "payload"; }

    OpenedFileCache::OpenedFilePtr handle;
    {
        OpenedFileCache cache(*fileSystem_, *pool_);
        handle = cache.get(filePath, 0);
        ASSERT_NE(handle, nullptr);
        EXPECT_EQ(cache.count(), 1u);
        // Normal sharing still works while the cache is alive.
        auto shared = cache.get(filePath, 0);
        EXPECT_EQ(shared.get(), handle.get());
    }
    // The cache (and its buckets) are destroyed while `handle` is still held.
    // Releasing it must run the custom deleter against an EXPIRED weak bucket
    // state and free the file handle WITHOUT touching the destroyed map/mutex.
    ASSERT_NE(handle->getFile(), nullptr);
    EXPECT_NO_THROW(handle.reset());
    EXPECT_EQ(handle, nullptr);
}

} // namespace
} // namespace facebook::velox::ch
