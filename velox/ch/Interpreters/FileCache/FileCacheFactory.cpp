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

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/testutil/TestValue.h"

#include <algorithm>
#include <filesystem>
#include <thread>
#include <utility>

namespace facebook::velox::ch
{

namespace
{

/// Normalize a cache directory path for registry deduplication. The manager
/// authorizes and normalizes paths before registration; this collapses `.`/`..`
/// and a single trailing separator so textually different but equivalent paths
/// still deduplicate to one on-disk directory.
std::string normalizePath(const std::string & path)
{
    if (path.empty())
        return path;
    std::string normalized = std::filesystem::path(path).lexically_normal().generic_string();
    if (normalized.size() > 1 && normalized.back() == '/')
        normalized.pop_back();
    return normalized;
}

/// Settings equality for the registry: excludes the cache name and config source
/// path (neither is part of `FileCacheConfig`) and compares the normalized
/// on-disk path plus every other effective field.
bool settingsEqual(FileCacheConfig lhs, FileCacheConfig rhs)
{
    lhs.path = normalizePath(lhs.path);
    rhs.path = normalizePath(rhs.path);
    return lhs == rhs;
}

/// Construct a `FileCache` with the Manager-owned runtime services injected. The
/// local-cache file factories and the opened-file invalidator replace CH's global
/// `FileSystem` / `OpenedFileCache` singleton.
FileCachePtr buildFileCache(
    FileCacheFactory::RuntimeServices & services,
    const std::string & name,
    const FileCacheConfig & settings)
{
    auto * fs = &services.localFileSystem;
    auto * opened = &services.openedFileCache;

    return std::make_shared<FileCache>(
        name,
        settings,
        services.scheduler,
        services.workerPool,
        &services.memoryPool,
        FileCacheOriginInfo(services.commonUserId, 0),
        [fs](const std::string & path, bool append) -> std::unique_ptr<velox::WriteFile>
        {
            velox::filesystems::FileOptions options;
            options.shouldCreateParentDirectories = true;
            // append -> open the existing partial file positioned at its end;
            // !append -> create a new file (error if it already exists).
            options.shouldThrowOnFileAlreadyExists = !append;
            return fs->openFileForWrite(path, options);
        },
        [fs](const std::string & path) -> std::shared_ptr<velox::ReadFile>
        { return fs->openFileForRead(path); },
        [opened](const std::string & path) { opened->remove(path); });
}

} // namespace

// ---------------------------------------------------------------------------
// FileCacheData
// ---------------------------------------------------------------------------

FileCacheFactory::FileCacheData::FileCacheData(
    FileCachePtr cache_, const FileCacheConfig & config_, std::string config_path_)
    : cache(std::move(cache_)), config_path(std::move(config_path_)), settings_(config_)
{
}

FileCacheConfig FileCacheFactory::FileCacheData::getSettings() const
{
    std::lock_guard<std::mutex> lock(settings_mutex_);
    return settings_;
}

void FileCacheFactory::FileCacheData::setSettings(FileCacheConfig config)
{
    std::lock_guard<std::mutex> lock(settings_mutex_);
    settings_ = std::move(config);
}

// ---------------------------------------------------------------------------
// Singleton
// ---------------------------------------------------------------------------

std::atomic<FileCacheFactory *> FileCacheFactory::global_instance_{nullptr};

FileCacheFactory & FileCacheFactory::instance()
{
    auto * factory = global_instance_.load(std::memory_order_acquire);
    VELOX_CHECK_NOT_NULL(
        factory,
        "FileCacheFactory has no installed instance; a FileCacheManager must be created and published first");
    return *factory;
}

void FileCacheFactory::setInstance(FileCacheFactory * factory)
{
    global_instance_.store(factory, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

FileCacheFactory::FileCacheFactory(RuntimeServices services) : services_(std::move(services))
{
}

// ---------------------------------------------------------------------------
// Registry API
// ---------------------------------------------------------------------------

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard<std::mutex> mutation_lock(services_.mutationMutex);
    return getOrCreateLocked(name, settings, config_path);
}

FileCachePtr FileCacheFactory::getOrCreateLocked(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    VELOX_CHECK(!shutdown_, "FileCacheFactory is shut down; cannot get or create cache '{}'", name);

    // 1. Name already bound.
    if (auto it = cache_by_name_.find(name); it != cache_by_name_.end())
    {
        auto data = it->second;
        if (settingsEqual(data->getSettings(), settings))
            return data->cache;
        VELOX_FAIL(
            "Cache with name '{}' already exists with different settings (name rebind conflict)", name);
    }

    // 2. Existing unique cache with the same on-disk path -> alias.
    const std::string normalized = normalizePath(settings.path);
    if (auto existing = findByPath(normalized))
    {
        if (!settingsEqual(existing->getSettings(), settings))
            VELOX_FAIL(
                "Found more than one cache configuration with the same path '{}' but with different "
                "cache settings (conflicting alias '{}')",
                settings.path,
                name);
        cache_by_name_.emplace(name, existing);
        return existing->cache;
    }

    // 3. New unique cache: grow the worker budget first, then construct/register.
    const size_t delta = growWorkerBudget(settings);
    try
    {
        auto cache = buildFileCache(services_, name, settings);
        auto data = std::make_shared<FileCacheData>(cache, settings, config_path);
        cache_by_name_.emplace(name, std::move(data));
        return cache;
    }
    catch (...)
    {
        rollbackWorkerBudget(delta);
        cache_by_name_.erase(name);
        throw;
    }
}

FileCachePtr FileCacheFactory::create(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard<std::mutex> mutation_lock(services_.mutationMutex);
    return createLocked(name, settings, config_path);
}

FileCachePtr FileCacheFactory::createLocked(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    VELOX_CHECK(!shutdown_, "FileCacheFactory is shut down; cannot create cache '{}'", name);

    // create() always rejects an existing name, even with equal settings.
    if (cache_by_name_.contains(name))
        VELOX_FAIL("Cache with name '{}' already exists", name);

    const std::string normalized = normalizePath(settings.path);
    if (auto existing = findByPath(normalized))
    {
        if (!settingsEqual(existing->getSettings(), settings))
            VELOX_FAIL(
                "Found more than one cache configuration with the same path '{}' but with different "
                "cache settings (conflicting alias '{}')",
                settings.path,
                name);
        cache_by_name_.emplace(name, existing);
        return existing->cache;
    }

    const size_t delta = growWorkerBudget(settings);
    try
    {
        auto cache = buildFileCache(services_, name, settings);
        auto data = std::make_shared<FileCacheData>(cache, settings, config_path);
        cache_by_name_.emplace(name, std::move(data));
        return cache;
    }
    catch (...)
    {
        rollbackWorkerBudget(delta);
        cache_by_name_.erase(name);
        throw;
    }
}

FileCachePtr FileCacheFactory::get(const std::string & name) const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = cache_by_name_.find(name);
    if (it == cache_by_name_.end())
        VELOX_FAIL("There is no cache by name '{}'", name);
    return it->second->cache;
}

FileCacheFactory::CacheByName FileCacheFactory::getAll() const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return cache_by_name_;
}

FileCacheFactory::Caches FileCacheFactory::getUniqueInstances() const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    Caches caches;
    for (const auto & [name, data] : cache_by_name_)
        caches.insert(data);
    return caches;
}

FileCacheFactory::FileCacheDataPtr FileCacheFactory::getByName(const std::string & name) const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = cache_by_name_.find(name);
    if (it == cache_by_name_.end())
        VELOX_FAIL("There is no cache by name '{}'", name);
    return it->second;
}

FileCacheFactory::CacheNames FileCacheFactory::getAllNames() const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    CacheNames names;
    names.reserve(cache_by_name_.size());
    for (const auto & [name, data] : cache_by_name_)
        names.insert(name);
    return names;
}

void FileCacheFactory::remove(const FileCachePtr & cache)
{
    std::lock_guard<std::mutex> mutation_lock(services_.mutationMutex);
    removeLocked(cache);
}

void FileCacheFactory::removeLocked(const FileCachePtr & cache)
{
    Caches removed;
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        for (auto it = cache_by_name_.begin(); it != cache_by_name_.end();)
        {
            if (it->second->cache == cache)
            {
                removed.insert(it->second);
                it = cache_by_name_.erase(it);
            }
            else
            {
                ++it;
            }
        }
        // Lower the logical budget under the lock; the pool itself is only shrunk
        // once the removed caches' workers have actually exited (below).
        for (const auto & data : removed)
        {
            const size_t delta = computeCacheWorkerMax(data->getSettings());
            worker_budget_ = (delta > worker_budget_) ? 0 : worker_budget_ - delta;
        }
    }

    // Outside the registry lock: deactivate each removed unique cache (joins its
    // background workers), then shrink the shared pool to the remaining budget.
    // Skip the pool resize once the Manager has torn the pool down (shutdown):
    // resizing a stopped executor is undefined.
    deactivateUnique(removed);
    if (!removed.empty() && !isShutdown())
        services_.workerPool.setNumThreads(workerBudget());
}

void FileCacheFactory::clear()
{
    std::lock_guard<std::mutex> mutation_lock(services_.mutationMutex);
    clearLocked();
}

void FileCacheFactory::clearLocked()
{
    Caches unique;
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        for (const auto & [name, data] : cache_by_name_)
            unique.insert(data);
        cache_by_name_.clear();
        worker_budget_ = 0;
    }

    // Outside the registry lock: deactivate every unique cache, then lower the
    // shared pool to the minimum. The scheduler/worker pool stay alive so the
    // manager can create new caches afterwards. During Manager shutdown the pool
    // is torn down separately, so skip the resize once the Factory is shut down
    // to avoid resizing a stopped executor.
    deactivateUnique(unique);
    if (!isShutdown())
        services_.workerPool.setNumThreads(workerBudget());
}

void FileCacheFactory::rollbackNewBindingsLocked(const CacheNames & keep_names)
{
    Caches orphaned;
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);

        // Unique instances still referenced by a kept (pre-existing) name survive.
        Caches surviving;
        for (const auto & [name, data] : cache_by_name_)
            if (keep_names.contains(name))
                surviving.insert(data);

        // Erase every name binding introduced since the snapshot. A unique cache
        // no longer referenced by any surviving name was created by this apply and
        // must be deactivated; a new alias that merely pointed at a pre-existing
        // instance is dropped without disturbing that surviving instance.
        for (auto it = cache_by_name_.begin(); it != cache_by_name_.end();)
        {
            if (keep_names.contains(it->first))
            {
                ++it;
            }
            else
            {
                if (!surviving.contains(it->second))
                    orphaned.insert(it->second);
                it = cache_by_name_.erase(it);
            }
        }

        // Restore the truthful logical budget from the survivors alone.
        uint64_t raw = 0;
        for (const auto & data : surviving)
            raw = FileCacheUtils::checkedAdd(
                raw, computeCacheWorkerMax(data->getSettings()), "file cache worker budget");
        worker_budget_ = raw;
    }

    // Outside the registry lock: join the orphaned new caches' workers, then
    // restore the shared pool to the truthful surviving budget (unless the pool is
    // already gone during shutdown).
    deactivateUnique(orphaned);
    if (!isShutdown())
        services_.workerPool.setNumThreads(workerBudget());
}

size_t FileCacheFactory::computeCacheWorkerMax(const FileCacheConfig & config)
{
    uint64_t total = config.loadMetadataThreads;
    total = FileCacheUtils::checkedAdd(total, config.loadMetadataAsynchronously ? 1 : 0, "file cache worker budget");
    total = FileCacheUtils::checkedAdd(total, config.backgroundDownloadThreads, "file cache worker budget");
    total = FileCacheUtils::checkedAdd(total, 1, "file cache worker budget"); // metadata cleanup worker
    total = FileCacheUtils::checkedAdd(total, 1, "file cache worker budget"); // scheduled background-cleanup callback
    if (config.keepFreeSpaceSizeRatio != 0.0 || config.keepFreeSpaceElementsRatio != 0.0)
    {
        total = FileCacheUtils::checkedAdd(total, 1, "file cache worker budget"); // scheduled free-space collector
        total = FileCacheUtils::checkedAdd(total, config.keepFreeSpaceEvictionThreads, "file cache worker budget");
    }
    return static_cast<size_t>(total);
}

size_t FileCacheFactory::workerBudget() const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return std::max<size_t>(1, worker_budget_);
}

size_t FileCacheFactory::growWorkerBudget(const FileCacheConfig & config)
{
    const size_t delta = computeCacheWorkerMax(config);
    const size_t new_raw = FileCacheUtils::checkedAdd(worker_budget_, delta, "file cache worker budget");
    const size_t effective = std::max<size_t>(1, new_raw);
    // Grow (never shrink) the shared pool BEFORE the cache is constructed so its
    // loadMetadata fail-close precondition is already satisfied.
    if (effective > services_.workerPool.numThreads())
        services_.workerPool.setNumThreads(effective);
    worker_budget_ = new_raw;
    return delta;
}

void FileCacheFactory::rollbackWorkerBudget(size_t budget_delta)
{
    // Undo the logical growth. Do not shrink the shared pool here: no workers ran
    // for the failed cache and an over-provisioned pool is safe. clear()/remove()/
    // applyConfigs resize the pool once workers have exited.
    worker_budget_ = (budget_delta > worker_budget_) ? 0 : worker_budget_ - budget_delta;
}

void FileCacheFactory::markShutdown()
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    shutdown_ = true;
}

bool FileCacheFactory::isShutdown() const
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return shutdown_;
}

size_t FileCacheFactory::recomputeWorkerBudget()
{
    std::lock_guard<std::mutex> lock(registry_mutex_);
    Caches unique;
    for (const auto & [name, data] : cache_by_name_)
        unique.insert(data);
    uint64_t raw = 0;
    for (const auto & data : unique)
        raw = FileCacheUtils::checkedAdd(raw, computeCacheWorkerMax(data->getSettings()), "file cache worker budget");
    worker_budget_ = raw;
    return std::max<size_t>(1, raw);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

FileCacheFactory::FileCacheDataPtr FileCacheFactory::findByPath(const std::string & normalized_path) const
{
    for (const auto & [name, data] : cache_by_name_)
    {
        if (normalizePath(data->getSettings().path) == normalized_path)
            return data;
    }
    return nullptr;
}

void FileCacheFactory::deactivateUnique(const Caches & unique)
{
    for (const auto & data : unique)
    {
        common::testutil::TestValue::adjust(
            "facebook::velox::ch::FileCacheFactory::deactivateOutsideLock", data->cache.get());
        data->cache->deactivateBackgroundOperations();
    }
}

bool FileCacheFactory::isRegistryLockFree() const
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
                if (registry_mutex_.try_lock())
                {
                    registry_mutex_.unlock();
                    acquired.store(true);
                }
            }
        });
    probe.join();
    return acquired.load();
}

} // namespace facebook::velox::ch
