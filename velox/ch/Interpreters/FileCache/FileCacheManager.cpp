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

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"

#include <algorithm>
#include <utility>

namespace facebook::velox::ch
{

std::atomic<FileCacheManager *> FileCacheManager::global_instance_{nullptr};

namespace
{
void validateOptions(const FileCacheManager::Options & options)
{
    if (options.commonUserId.empty())
        throwFileCacheException("FileCacheManager: commonUserId must be non-empty");
    if (options.commonUserId == "internal")
        throwFileCacheException("FileCacheManager: commonUserId must not be \"internal\"");
    if (!options.localFileSystem)
        throwFileCacheException("FileCacheManager: localFileSystem must be non-null");
    if (!options.timekeeper)
        throwFileCacheException("FileCacheManager: timekeeper must be non-null");

    for (const auto & named : options.caches)
    {
        if (named.name.empty())
            throwFileCacheException("FileCacheManager: cache name must be non-empty");
        if (named.config.path.empty())
            throwFileCacheException("FileCacheManager: cache {} has an empty path", named.name);
    }

    if (!options.defaultCacheName.empty())
    {
        const bool found = std::any_of(
            options.caches.begin(),
            options.caches.end(),
            [&](const auto & named) { return named.name == options.defaultCacheName; });
        if (!found)
            throwFileCacheException(
                "FileCacheManager: defaultCacheName {} is not among the configured caches", options.defaultCacheName);
    }
}
} // namespace

size_t FileCacheManager::computeWorkerPoolMax(const std::vector<NamedFileCacheConfig> & caches)
{
    /// Dedup unique caches by normalized path (same rule the Factory uses for aliasing) and sum
    /// their per-cache demand with checked addition. At least one worker.
    folly::F14FastMap<std::string, const FileCacheConfig *> unique;
    for (const auto & named : caches)
        unique.emplace(named.config.path, &named.config);

    uint64_t total = 0;
    for (const auto & [_, config] : unique)
        total = FileCacheUtils::checkedAdd(total, computeCacheWorkerMax(*config), "workerPoolMax");
    return std::max<size_t>(1, total);
}

FileCacheManager::FileCacheManager(Options options)
    : localFileSystem_(std::move(options.localFileSystem))
    , timekeeper_(std::move(options.timekeeper))
    , commonUserId_(options.commonUserId)
    , defaultCacheName_(options.defaultCacheName)
    , workerPool_(computeWorkerPoolMax(options.caches), 1, "FileCache")
    , scheduler_(timekeeper_, workerPool_)
    , openedFileCache_(*localFileSystem_)
    , factory_(FileCacheFactory::RuntimeServices{
          workerPool_, scheduler_, openedFileCache_, *localFileSystem_, commonUserId_})
{
    /// Register caches via the Factory. Construction starts no background work capturing `this`.
    for (const auto & named : options.caches)
        factory_.getOrCreate(named.name, named.config, named.configPath);
}

FileCacheManager::~FileCacheManager()
{
    /// If not already shut down, run the ordered shutdown so background work stops before the
    /// owned resources are destroyed. Member declaration order is the backstop (factory_ first).
    shutdown();
}

std::shared_ptr<FileCacheManager> FileCacheManager::create(Options options)
{
    validateOptions(options);
    const bool initialize_on_create = options.initializeOnCreate;
    std::shared_ptr<FileCacheManager> manager(new FileCacheManager(std::move(options)));
    if (initialize_on_create)
        manager->initialize();
    return manager;
}

FileCacheManager * FileCacheManager::getInstance()
{
    return global_instance_.load(std::memory_order_acquire);
}

FileCacheManager & FileCacheManager::instance()
{
    auto * manager = global_instance_.load(std::memory_order_acquire);
    if (manager == nullptr)
        throwFileCacheException("FileCacheManager is not installed");
    return *manager;
}

void FileCacheManager::setInstance(FileCacheManager * manager)
{
    auto * current = global_instance_.load(std::memory_order_acquire);
    if (manager != nullptr)
    {
        /// Install: reject replacing a different live manager; setting the same one is a no-op.
        if (current != nullptr && current != manager)
            throwFileCacheException("FileCacheManager: a different manager is already installed");
        global_instance_.store(manager, std::memory_order_release);
        FileCacheFactory::setInstance(&manager->factory_);
    }
    else
    {
        /// Uninstall: clear the Factory pointer first so a new instance() caller never sees a
        /// live Manager with a torn-down Factory, then clear the Manager pointer.
        FileCacheFactory::setInstance(nullptr);
        global_instance_.store(nullptr, std::memory_order_release);
    }
}

FileCachePtr FileCacheManager::get(const std::string & name) const
{
    return factory_.get(name);
}

FileCachePtr FileCacheManager::getDefault() const
{
    if (defaultCacheName_.empty())
        throwFileCacheException("FileCacheManager: no default cache configured");
    return factory_.get(defaultCacheName_);
}

bool FileCacheManager::hasDefault() const
{
    return !defaultCacheName_.empty();
}

void FileCacheManager::initialize()
{
    {
        std::unique_lock lock(lifecycle_mutex_);
        if (state_ == State::ShuttingDown || state_ == State::Shutdown)
            throwFileCacheException("FileCacheManager: cannot initialize after shutdown");
        if (state_ == State::Initialized)
            return;
    }

    auto snapshot = factory_.getUniqueInstances();

    std::vector<FileCacheFactory::FileCacheDataPtr> initialized;
    try
    {
        for (const auto & data : snapshot)
        {
            data->cache->initialize();
            initialized.push_back(data);
        }
    }
    catch (...)
    {
        /// Deactivate the caches initialized so far, outside the lifecycle lock; keep state Created
        /// (this run failed) and propagate.
        for (const auto & data : initialized)
            data->cache->deactivateBackgroundOperations();
        throw;
    }

    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Initialized;
    lifecycle_cv_.notify_all();
}

void FileCacheManager::shutdown()
{
    {
        std::unique_lock lock(lifecycle_mutex_);
        if (state_ == State::Shutdown)
            return;
        if (state_ == State::ShuttingDown)
        {
            lifecycle_cv_.wait(lock, [&] { return state_ == State::Shutdown; });
            return;
        }
        state_ = State::ShuttingDown;
    }

    /// Ordered teardown outside the lifecycle lock: cache background workers stop (clear snapshots
    /// and deactivates each unique cache) -> scheduler timer chains cancel -> worker pool drains
    /// -> opened-file cache clears.
    factory_.clear();
    scheduler_.shutdown();
    workerPool_.shutdown();
    openedFileCache_.clear();

    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Shutdown;
    lifecycle_cv_.notify_all();
}

FileCacheManager::FileCacheManagerStats FileCacheManager::refreshStats() const
{
    FileCacheManagerStats stats;
    auto all = factory_.getAll();
    for (const auto & [name, data] : all)
    {
        FileCacheStats cache_stats;
        cache_stats.maxSize = data->cache->getMaxCacheSize();
        cache_stats.usedSize = data->cache->getUsedCacheSize();
        cache_stats.fileSegments = data->cache->getFileSegmentsNum();
        stats.cachesByName.emplace(name, cache_stats);
    }
    stats.uniqueCaches = factory_.getUniqueInstances().size();
    stats.openedFileCache = openedFileCache_.stats();
    return stats;
}

} // namespace facebook::velox::ch
