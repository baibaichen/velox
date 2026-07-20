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

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"

#include <algorithm>
#include <utility>
#include <vector>

namespace facebook::velox::ch
{

std::atomic<FileCacheFactory *> FileCacheFactory::global_instance_{nullptr};

FileCacheFactory::FileCacheData::FileCacheData(FileCachePtr cache_, const FileCacheConfig & config_, std::string config_path_)
    : cache(std::move(cache_)), config_path(std::move(config_path_)), settings_(config_)
{
}

FileCacheConfig FileCacheFactory::FileCacheData::getSettings() const
{
    std::lock_guard lock(settings_mutex_);
    return settings_;
}

void FileCacheFactory::FileCacheData::setSettings(FileCacheConfig config)
{
    std::lock_guard lock(settings_mutex_);
    settings_ = std::move(config);
}

FileCacheFactory::FileCacheFactory(RuntimeServices services) : services_(std::move(services))
{
}

FileCacheFactory & FileCacheFactory::instance()
{
    auto * factory = global_instance_.load(std::memory_order_acquire);
    if (factory == nullptr)
        throwFileCacheException("FileCacheFactory is not installed (no live FileCacheManager)");
    return *factory;
}

void FileCacheFactory::setInstance(FileCacheFactory * factory)
{
    global_instance_.store(factory, std::memory_order_release);
}

size_t computeCacheWorkerMax(const FileCacheConfig & config)
{
    /// design 013 Step 7 worker-budget formula (checked addition; overflow is a config error).
    uint64_t total = config.loadMetadataThreads;
    total = FileCacheUtils::checkedAdd(total, config.loadMetadataAsynchronously ? 1 : 0, "cacheWorkerMax");
    total = FileCacheUtils::checkedAdd(total, config.backgroundDownloadThreads, "cacheWorkerMax");
    /// metadata cleanup worker + scheduled background-cleanup callback.
    total = FileCacheUtils::checkedAdd(total, 2, "cacheWorkerMax");

    const bool free_space_keeping_enabled = config.keepFreeSpaceSizeRatio != 0 || config.keepFreeSpaceElementsRatio != 0;
    if (free_space_keeping_enabled)
    {
        /// scheduled collector callback + the eviction remover threads.
        total = FileCacheUtils::checkedAdd(total, 1, "cacheWorkerMax");
        total = FileCacheUtils::checkedAdd(total, config.keepFreeSpaceEvictionThreads, "cacheWorkerMax");
    }
    return total;
}

FileCacheFactory::FileCacheDataPtr FileCacheFactory::findByPath(const std::string & normalized_path) const
{
    /// Caller holds registry_mutex_. The settings' `path` field is the immutable cache directory
    /// and is a faithful unique-instance key (equal to CH's `getSettings()[path].value` check).
    for (const auto & [_, data] : cache_by_name_)
        if (data->getSettings().path == normalized_path)
            return data;
    return nullptr;
}

size_t FileCacheFactory::growWorkerBudget(const FileCacheConfig & config)
{
    /// Caller holds registry_mutex_ and is about to add a NEW unique cache (not yet in the map).
    /// Grow the shared pool to the current aggregate PLUS this new cache's demand. Growing never
    /// blocks (only shrinking waits for threads to retire).
    const size_t delta = computeCacheWorkerMax(config);

    Caches unique;
    for (const auto & [_, data] : cache_by_name_)
        unique.insert(data);
    uint64_t total = delta;
    for (const auto & data : unique)
        total = FileCacheUtils::checkedAdd(total, computeCacheWorkerMax(data->getSettings()), "workerPoolMax");

    services_.workerPool.setNumThreads(std::max<size_t>(1, total));
    return delta;
}

void FileCacheFactory::recomputeWorkerBudgetLocked()
{
    /// Caller holds registry_mutex_. Dedup by unique FileCacheData (control-block address) so
    /// aliases do not inflate the budget, then set the pool max to max(1, sum of per-cache demand).
    Caches unique;
    for (const auto & [_, data] : cache_by_name_)
        unique.insert(data);

    uint64_t total = 0;
    for (const auto & data : unique)
        total = FileCacheUtils::checkedAdd(total, computeCacheWorkerMax(data->getSettings()), "workerPoolMax");

    services_.workerPool.setNumThreads(std::max<size_t>(1, total));
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard lock(registry_mutex_);

    /// 1. Existing name: return it iff settings are effectively equal, else reject (rebind conflict).
    if (auto it = cache_by_name_.find(name); it != cache_by_name_.end())
    {
        if (it->second->getSettings() == settings)
            return it->second->cache;
        throwFileCacheException("Cache with name {} already exists with different settings", name);
    }

    /// 2. Existing unique cache by path: alias it iff settings equal, else reject (path conflict).
    if (auto existing = findByPath(settings.path))
    {
        if (existing->getSettings() != settings)
            throwFileCacheException(
                "Found more than one cache configuration with the same path but different settings ({})", name);
        cache_by_name_.emplace(name, existing);
        /// Adding a name alias does NOT grow the worker budget (no new FileCache).
        return existing->cache;
    }

    /// 3. New unique cache. Grow the budget first, then construct; roll back on failure.
    growWorkerBudget(settings);
    try
    {
        auto cache = std::make_shared<FileCache>(
            name,
            settings,
            services_.workerPool,
            services_.scheduler,
            services_.openedFileCache,
            services_.localFileSystem,
            services_.commonUserId);
        auto data = std::make_shared<FileCacheData>(cache, settings, config_path);
        cache_by_name_.emplace(name, data);
        return cache;
    }
    catch (...)
    {
        cache_by_name_.erase(name);
        recomputeWorkerBudgetLocked();
        throw;
    }
}

FileCachePtr FileCacheFactory::create(
    const std::string & name, const FileCacheConfig & settings, const std::string & config_path)
{
    std::lock_guard lock(registry_mutex_);

    /// Existing name always fails (unlike getOrCreate).
    if (cache_by_name_.contains(name))
        throwFileCacheException("Cache with name {} already exists", name);

    if (auto existing = findByPath(settings.path))
    {
        if (existing->getSettings() != settings)
            throwFileCacheException(
                "Found more than one cache configuration with the same path but different settings ({})", name);
        cache_by_name_.emplace(name, existing);
        return existing->cache;
    }

    growWorkerBudget(settings);
    try
    {
        auto cache = std::make_shared<FileCache>(
            name,
            settings,
            services_.workerPool,
            services_.scheduler,
            services_.openedFileCache,
            services_.localFileSystem,
            services_.commonUserId);
        auto data = std::make_shared<FileCacheData>(cache, settings, config_path);
        cache_by_name_.emplace(name, data);
        return cache;
    }
    catch (...)
    {
        cache_by_name_.erase(name);
        recomputeWorkerBudgetLocked();
        throw;
    }
}

FileCachePtr FileCacheFactory::get(const std::string & name) const
{
    std::lock_guard lock(registry_mutex_);
    auto it = cache_by_name_.find(name);
    if (it == cache_by_name_.end())
        throwFileCacheException("There is no cache by name: {}", name);
    return it->second->cache;
}

FileCacheFactory::CacheByName FileCacheFactory::getAll() const
{
    std::lock_guard lock(registry_mutex_);
    return cache_by_name_;
}

FileCacheFactory::Caches FileCacheFactory::getUniqueInstances() const
{
    std::lock_guard lock(registry_mutex_);
    Caches caches;
    for (const auto & [_, data] : cache_by_name_)
        caches.insert(data);
    return caches;
}

FileCacheFactory::FileCacheDataPtr FileCacheFactory::getByName(const std::string & name) const
{
    std::lock_guard lock(registry_mutex_);
    auto it = cache_by_name_.find(name);
    if (it == cache_by_name_.end())
        throwFileCacheException("There is no cache by name: {}", name);
    return it->second;
}

void FileCacheFactory::remove(const FileCachePtr & cache)
{
    FileCacheDataPtr snapshot;
    {
        std::lock_guard lock(registry_mutex_);
        for (auto it = cache_by_name_.begin(); it != cache_by_name_.end();)
        {
            if (it->second->cache == cache)
            {
                snapshot = it->second;
                it = cache_by_name_.erase(it);
            }
            else
                ++it;
        }
    }

    /// Outside the registry lock: every name for this cache was erased above (all names for one
    /// cache share the same FileCacheData), so no registry entry remains — deactivate it FIRST so
    /// its persistent background threads retire from the shared pool, then release the snapshot,
    /// then lower the pool. (Shrinking before the threads stop would block in `setNumThreads`.)
    if (snapshot)
        snapshot->cache->deactivateBackgroundOperations();
    snapshot.reset();

    std::lock_guard lock(registry_mutex_);
    recomputeWorkerBudgetLocked();
}

void FileCacheFactory::clear()
{
    Caches unique;
    {
        std::lock_guard lock(registry_mutex_);
        for (const auto & [_, data] : cache_by_name_)
            unique.insert(data);
        cache_by_name_.clear();
    }

    /// Outside the lock: deactivate every unique cache FIRST (this stops each cache's persistent
    /// background threads so they retire from the shared pool), then release snapshots, then lower
    /// the worker-pool max. Shrinking the pool before the persistent threads stop would block
    /// forever in `setNumThreads` waiting for threads that never return.
    for (const auto & data : unique)
        data->cache->deactivateBackgroundOperations();
    unique.clear();

    services_.workerPool.setNumThreads(1);
}

} // namespace facebook::velox::ch
