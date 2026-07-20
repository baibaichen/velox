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
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/testutil/TestValue.h"

#include <fmt/format.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace facebook::velox::ch
{

namespace
{

std::string normalizePath(const std::string & path)
{
    if (path.empty())
        return path;
    std::string normalized = std::filesystem::path(path).lexically_normal().generic_string();
    if (normalized.size() > 1 && normalized.back() == '/')
        normalized.pop_back();
    return normalized;
}

bool settingsEqual(FileCacheConfig lhs, FileCacheConfig rhs)
{
    lhs.path = normalizePath(lhs.path);
    rhs.path = normalizePath(rhs.path);
    return lhs == rhs;
}

} // namespace

// ---------------------------------------------------------------------------
// Singleton
// ---------------------------------------------------------------------------

std::atomic<FileCacheManager *> FileCacheManager::global_instance_{nullptr};

FileCacheManager * FileCacheManager::getInstance()
{
    return global_instance_.load(std::memory_order_acquire);
}

FileCacheManager & FileCacheManager::instance()
{
    auto * manager = global_instance_.load(std::memory_order_acquire);
    VELOX_CHECK_NOT_NULL(manager, "FileCacheManager has no installed instance; create and publish one first");
    return *manager;
}

void FileCacheManager::setInstance(FileCacheManager * manager)
{
    if (manager == nullptr)
    {
        // Uninstall: clear the Factory pointer first so no new registry user can
        // enter a manager that is being torn down, then clear the Manager pointer.
        FileCacheFactory::setInstance(nullptr);
        global_instance_.store(nullptr, std::memory_order_release);
        return;
    }

    auto * current = global_instance_.load(std::memory_order_acquire);
    if (current == manager)
        return; // idempotent re-install of the same manager.
    VELOX_CHECK_NULL(current, "A different FileCacheManager is already installed; cannot replace a live instance");

    // Install: publish the Manager pointer, then its Factory pointer.
    global_instance_.store(manager, std::memory_order_release);
    FileCacheFactory::setInstance(&manager->factory_);
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

void FileCacheManager::validateOptions(const Options & options)
{
    VELOX_CHECK(!options.commonUserId.empty(), "FileCacheManager: commonUserId must be non-empty");
    VELOX_CHECK_NE(
        options.commonUserId,
        std::string("internal"),
        "FileCacheManager: commonUserId must not be the reserved 'internal' identity");
    VELOX_CHECK_NOT_NULL(options.localFileSystem, "FileCacheManager: localFileSystem must be non-null");
    VELOX_CHECK_NOT_NULL(options.memoryPool, "FileCacheManager: memoryPool must be non-null");
    VELOX_CHECK_NOT_NULL(options.timekeeper, "FileCacheManager: timekeeper must be non-null");

    const auto checkAbsolute = [](const std::string & path, const char * what)
    {
        if (path.empty())
            return;
        VELOX_CHECK(
            std::filesystem::path(path).is_absolute(), "FileCacheManager: {} must be an absolute path: {}", what, path);
    };
    checkAbsolute(options.cachePathPrefix, "cachePathPrefix");
    checkAbsolute(options.allowedCacheRoot, "allowedCacheRoot");

    for (const auto & named : options.caches)
    {
        VELOX_CHECK(!named.name.empty(), "FileCacheManager: every cache name must be non-empty");
        VELOX_CHECK(
            std::filesystem::path(named.config.path).is_absolute(),
            "FileCacheManager: cache '{}' path must be absolute and normalized: {}",
            named.name,
            named.config.path);
    }
}

size_t FileCacheManager::computeWorkerPoolMax(const std::vector<NamedFileCacheConfig> & caches)
{
    // Deduplicate by normalized on-disk path so aliases contribute once; sum each
    // unique cache's checked per-cache worker maximum. Overflow is a config error.
    std::unordered_set<std::string> seen;
    uint64_t raw = 0;
    for (const auto & named : caches)
    {
        const std::string normalized = normalizePath(named.config.path);
        if (!seen.insert(normalized).second)
            continue;
        raw = FileCacheUtils::checkedAdd(
            raw, FileCacheFactory::computeCacheWorkerMax(named.config), "file cache worker budget");
    }
    return std::max<size_t>(1, raw);
}

FileCacheManager::FileCacheManager(Options options)
    : localFileSystem_(std::move(options.localFileSystem))
    , memoryPool_(options.memoryPool)
    , timekeeper_(std::move(options.timekeeper))
    , commonUserId_(std::move(options.commonUserId))
    , defaultCacheName_(std::move(options.defaultCacheName))
    , workerPool_(computeWorkerPoolMax(options.caches), 1, "FileCache")
    , scheduler_(timekeeper_, workerPool_)
    , openedFileCache_(*localFileSystem_, *memoryPool_)
    , factory_(FileCacheFactory::RuntimeServices{
          workerPool_, scheduler_, openedFileCache_, *localFileSystem_, *memoryPool_, commonUserId_, mutation_mutex_})
{
    // The constructor only builds members. No background work that captures
    // `this` is started here; create() performs registration and initialization.
}

std::shared_ptr<FileCacheManager> FileCacheManager::create(Options options)
{
    validateOptions(options);

    // The private ctor copies what it needs from options; keep options here for
    // registration.
    std::shared_ptr<FileCacheManager> manager(new FileCacheManager(options));

    // Register every configured cache. getOrCreate deduplicates by path (aliases)
    // and grows the shared worker budget once per unique cache.
    for (const auto & named : options.caches)
        manager->factory_.getOrCreate(named.name, named.config, named.configPath);

    // The default cache, if named, must resolve now.
    if (!manager->defaultCacheName_.empty())
        (void)manager->factory_.get(manager->defaultCacheName_);

    if (options.initializeOnCreate)
        manager->initialize();

    return manager;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

FileCacheFactory & FileCacheManager::factory()
{
    return factory_;
}

const FileCacheFactory & FileCacheManager::factory() const
{
    return factory_;
}

FileCachePtr FileCacheManager::get(const std::string & name) const
{
    return factory_.get(name);
}

FileCachePtr FileCacheManager::getDefault() const
{
    VELOX_CHECK(!defaultCacheName_.empty(), "FileCacheManager: no default cache configured");
    return factory_.get(defaultCacheName_);
}

OpenedFileCache & FileCacheManager::openedFileCache()
{
    return openedFileCache_;
}

FileCacheWorkerPool & FileCacheManager::workerPool()
{
    return workerPool_;
}

FileCacheScheduler & FileCacheManager::scheduler()
{
    return scheduler_;
}

bool FileCacheManager::isMutationLockFree() const
{
    // Probe from a separate thread: a non-blocking try_lock never deadlocks even
    // when the caller thread holds the lock. Retry to tolerate spurious try_lock
    // failures on platforms where they are permitted.
    std::atomic<bool> acquired{false};
    std::thread probe(
        [&]
        {
            for (int i = 0; i < 1000 && !acquired.load(); ++i)
            {
                if (mutation_mutex_.try_lock())
                {
                    mutation_mutex_.unlock();
                    acquired.store(true);
                }
            }
        });
    probe.join();
    return acquired.load();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void FileCacheManager::initialize()
{
    // The mutation mutex is held for the whole operation so no getOrCreate/
    // applyConfigs/shutdown can race the shared-pool budget while caches are
    // coming up. FileCache never calls back into the Manager, so holding it
    // across the (possibly slow) per-cache initialize() cannot deadlock.
    std::lock_guard<std::mutex> mutation_lock(mutation_mutex_);
    VELOX_CHECK(
        state_ != State::ShuttingDown && state_ != State::Shutdown,
        "FileCacheManager is shutting down; cannot initialize");
    if (state_ == State::Initialized)
        return;

    // Initialize each unique cache exactly once.
    auto unique = factory_.getUniqueInstances();
    std::vector<FileCacheFactory::FileCacheDataPtr> initialized;
    try
    {
        for (const auto & data : unique)
        {
            common::testutil::TestValue::adjust(
                "facebook::velox::ch::FileCacheManager::initialize::perUniqueCache", data->cache.get());
            data->cache->initialize();
            initialized.push_back(data);
        }
    }
    catch (...)
    {
        // Deactivate already-initialized caches; leave the state in Created and
        // propagate (create(initializeOnCreate=true) then does not publish).
        for (const auto & data : initialized)
            data->cache->deactivateBackgroundOperations();
        throw;
    }

    state_ = State::Initialized;
}

void FileCacheManager::applyConfigs(const std::vector<NamedFileCacheConfig> & configs)
{
    // One serialization lock for the whole operation: registry mutation, new-cache
    // initialize, per-cache apply, budget recompute and pool resize all run under
    // it, so a concurrent getOrCreate/remove/clear/shutdown can neither overwrite a
    // newer worker budget nor resize a stopped pool.
    std::lock_guard<std::mutex> mutation_lock(mutation_mutex_);
    VELOX_CHECK(
        state_ != State::ShuttingDown && state_ != State::Shutdown,
        "FileCacheManager is shutting down; cannot apply configs");
    const bool manager_initialized = (state_ == State::Initialized);

    // Resolve each requested config to its target unique cache and validate:
    //  * the on-disk path is immutable;
    //  * all aliases for one cache must request equal settings;
    //  * unknown names create new caches/aliases via getOrCreate.
    struct Target
    {
        FileCacheFactory::FileCacheDataPtr data;
        FileCacheConfig config;
    };
    std::unordered_map<FileCacheFactory::FileCacheData *, Target> targets;
    std::vector<const NamedFileCacheConfig *> newConfigs;

    for (const auto & named : configs)
    {
        FileCacheFactory::FileCacheDataPtr data;
        try
        {
            data = factory_.getByName(named.name);
        }
        catch (...)
        {
            newConfigs.push_back(&named);
            continue;
        }

        VELOX_CHECK(
            normalizePath(data->getSettings().path) == normalizePath(named.config.path),
            "FileCacheManager: cache '{}' path change is not supported",
            named.name);

        auto it = targets.find(data.get());
        if (it == targets.end())
            targets.emplace(data.get(), Target{data, named.config});
        else
            VELOX_CHECK(
                settingsEqual(it->second.config, named.config),
                "FileCacheManager: aliases for cache '{}' request inconsistent settings",
                named.name);
    }

    // Snapshot the name bindings and the unique caches before registration so the
    // whole new-cache registration + pool grow + initialize phase can fail-close as
    // ONE transaction relative to this pre-apply snapshot. `namesBefore` records
    // every pre-existing name; `before` tells which of the post-registration unique
    // caches is brand-new (needs initialize) versus a new alias to an existing one
    // (must NOT re-initialize).
    const auto namesBefore = factory_.getAllNames();
    const auto before = factory_.getUniqueInstances();

    try
    {
        // Brand-new names/paths go through the normal registration path. We already
        // hold the mutation mutex, so call the *Locked variant (the public
        // getOrCreate would re-lock the same non-recursive mutex).
        for (const auto * named : newConfigs)
            factory_.getOrCreateLocked(named->name, named->config, named->configPath);

        // Grow the shared worker pool up-front if any changed or new cache needs
        // more workers than currently provided (settings apply may spawn download
        // threads, and a new cache's initialize loads metadata against the budgeted
        // capacity). Growth always happens BEFORE any apply/initialize; a shrink is
        // deferred to the end, after affected workers have been joined.
        {
            auto unique = factory_.getUniqueInstances();
            uint64_t prospective = 0;
            for (const auto & data : unique)
            {
                auto it = targets.find(data.get());
                const FileCacheConfig config = (it != targets.end()) ? it->second.config : data->getSettings();
                prospective = FileCacheUtils::checkedAdd(
                    prospective, FileCacheFactory::computeCacheWorkerMax(config), "file cache worker budget");
            }
            const size_t effective = std::max<size_t>(1, prospective);
            if (effective > workerPool_.numThreads())
                workerPool_.setNumThreads(effective);
        }

        // Initialize genuinely-new unique caches exactly once, but only when this
        // Manager is already Initialized (otherwise a later initialize() brings
        // every cache up). New aliases to an already-initialized cache are not in
        // `newlyCreated`, so they are never re-initialized.
        if (manager_initialized)
        {
            const auto after = factory_.getUniqueInstances();
            std::vector<FileCacheFactory::FileCacheDataPtr> newlyCreated;
            for (const auto & data : after)
                if (before.find(data) == before.end())
                    newlyCreated.push_back(data);

            for (const auto & data : newlyCreated)
            {
                common::testutil::TestValue::adjust(
                    "facebook::velox::ch::FileCacheManager::applyConfigs::initializeNewCache", data->cache.get());
                data->cache->initialize();
            }
        }
    }
    catch (...)
    {
        // Fail closed for the entire registration + grow + initialize phase: undo
        // every name/instance binding this call introduced (deactivate the new
        // caches, restore the truthful worker budget and pool size), keep all
        // pre-existing bindings, and report the original exception. This covers a
        // conflicting later alias/settings error and an up-front pool-grow overflow
        // just as it covers a new cache's initialize failure, so no retrievable
        // uninitialized cache/name binding and no grown budget/pool is ever left
        // behind.
        factory_.rollbackNewBindingsLocked(namesBefore);
        throw;
    }

    // Apply settings per unique cache OUTSIDE the registry lock (we hold only the
    // mutation mutex). Persist the truthful actual snapshot even when an apply
    // throws; remember the first exception and propagate it after every cache has
    // been visited.
    std::exception_ptr firstError;
    for (auto & [ptr, target] : targets)
    {
        FileCacheConfig actual = target.data->getSettings();
        if (settingsEqual(actual, target.config))
            continue; // equal reload is a no-op.

        try
        {
            common::testutil::TestValue::adjust(
                "facebook::velox::ch::FileCacheManager::applyConfigs::applyOutsideLock", target.data->cache.get());
            target.data->cache->applySettingsIfPossible(target.config, actual);
        }
        catch (...)
        {
            if (!firstError)
                firstError = std::current_exception();
        }
        target.data->setSettings(actual);
    }

    // Recompute the budget from the actual snapshots and resize the pool (only a
    // shrink is possible here; growth already happened above). The result is the
    // truthful aggregate active need, so the pool is never lowered below it.
    const size_t effective = factory_.recomputeWorkerBudget();
    workerPool_.setNumThreads(effective);

    if (firstError)
        std::rethrow_exception(firstError);
}

void FileCacheManager::shutdown()
{
    // Serialize the whole teardown with every other mutating operation so the
    // shared pool is never resized while it is being stopped, and concurrent
    // shutdown callers simply block on the mutex and then observe the terminal
    // state.
    std::lock_guard<std::mutex> mutation_lock(mutation_mutex_);
    if (state_ == State::Shutdown)
        return;
    state_ = State::ShuttingDown;

    // Strict teardown order:
    //   cache background workers -> scheduler timers -> physical pool -> handles.
    factory_.clearLocked();
    common::testutil::TestValue::adjust("facebook::velox::ch::FileCacheManager::shutdown::afterCachesDeactivated", this);
    factory_.markShutdown();

    scheduler_.shutdown();
    common::testutil::TestValue::adjust(
        "facebook::velox::ch::FileCacheManager::shutdown::afterSchedulerShutdown", this);

    workerPool_.shutdown();
    common::testutil::TestValue::adjust(
        "facebook::velox::ch::FileCacheManager::shutdown::afterWorkerPoolShutdown", this);

    openedFileCache_.clear();
    common::testutil::TestValue::adjust("facebook::velox::ch::FileCacheManager::shutdown::afterHandlesCleared", this);

    state_ = State::Shutdown;
}

// ---------------------------------------------------------------------------
// Stats / debug
// ---------------------------------------------------------------------------

FileCacheManager::FileCacheManagerStats FileCacheManager::refreshStats() const
{
    FileCacheManagerStats stats;

    // Snapshot the registry under the (Factory's) registry lock, then collect the
    // per-cache numbers without holding it.
    const auto all = factory_.getAll();
    stats.uniqueCaches = factory_.getUniqueInstances().size();
    for (const auto & [name, data] : all)
    {
        FileCacheStats cache_stats;
        const auto & cache = *data->cache;
        cache_stats.maxSize = cache.getMaxCacheSize();
        cache_stats.usedSize = cache.getUsedCacheSize();
        cache_stats.fileSegments = cache.getFileSegmentsNum();
        stats.cachesByName.emplace(name, cache_stats);
    }

    stats.openedFileCache.numElements = openedFileCache_.count();
    stats.workerPoolMax = workerPool_.numThreads();
    stats.workerPoolActive = factory_.workerBudget();
    return stats;
}

std::string FileCacheManager::toString(bool details) const
{
    const auto stats = refreshStats();
    std::string result = fmt::format(
        "FileCacheManager(uniqueCaches={}, names={}, workerPoolMax={}, workerPoolActive={}, openedFiles={})",
        stats.uniqueCaches,
        stats.cachesByName.size(),
        stats.workerPoolMax,
        stats.workerPoolActive,
        stats.openedFileCache.numElements);
    if (details)
    {
        for (const auto & [name, cache_stats] : stats.cachesByName)
            result += fmt::format(
                "\n  {}: maxSize={}, usedSize={}, fileSegments={}",
                name,
                cache_stats.maxSize,
                cache_stats.usedSize,
                cache_stats.fileSegments);
    }
    return result;
}

} // namespace facebook::velox::ch
