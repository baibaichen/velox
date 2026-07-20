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
#pragma once

#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

namespace facebook::velox::ch
{

/// Sole name/path registry for `FileCache` instances. Port of ClickHouse
/// `FileCacheFactory`; the registry semantics come from design 2-file-cache/12.
///
/// The Factory does NOT own the runtime services it constructs `FileCache`s
/// with; those are owned by `FileCacheManager` and passed in as references
/// (`RuntimeServices`). The Factory does not store a `FileCacheManager*`, and
/// `FileCacheManager` implements no second registry.
class FileCacheFactory final
{
public:
    /// Per-instance registry record. `cache` and `config_path` are immutable after
    /// construction; `settings_` is guarded by its own mutex, separate from the
    /// Factory's `registry_mutex_` (lock order: registry_mutex_ -> settings_mutex_).
    class FileCacheData
    {
    public:
        FileCacheData(FileCachePtr cache_, const FileCacheConfig & config_, std::string config_path_);

        FileCacheConfig getSettings() const;
        void setSettings(FileCacheConfig config);

        const FileCachePtr cache;
        const std::string config_path;

    private:
        mutable std::mutex settings_mutex_;
        FileCacheConfig settings_;
    };
    using FileCacheDataPtr = std::shared_ptr<FileCacheData>;

    /// Manager-owned service references; the Factory does not own them. `memoryPool`
    /// from CH's illustrative shape is dropped (D1: the ReadFile-based OpenedFileCache
    /// port has no mmap accounting need).
    struct RuntimeServices
    {
        FileCacheWorkerPool & workerPool;
        FileCacheScheduler & scheduler;
        OpenedFileCache & openedFileCache;
        filesystems::FileSystem & localFileSystem;
        std::string commonUserId;
    };

    explicit FileCacheFactory(RuntimeServices services);

    FileCacheFactory(const FileCacheFactory &) = delete;
    FileCacheFactory & operator=(const FileCacheFactory &) = delete;

    /// Singleton entry point: returns the Manager-installed Factory. Throws if none installed.
    static FileCacheFactory & instance();

    using CacheByName = folly::F14FastMap<std::string, FileCacheDataPtr>;
    using Caches = folly::F14FastSet<FileCacheDataPtr>;

    /// Return the existing cache if `name` (or the settings' path) already maps to one with
    /// equal settings; otherwise create a new cache. See design 12 for the full decision table.
    FileCachePtr getOrCreate(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);

    /// Like `getOrCreate`, but an existing name always fails (never returns the existing cache).
    FileCachePtr create(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);

    /// Return the cache registered under `name`. Throws if absent.
    FileCachePtr get(const std::string & name) const;

    CacheByName getAll() const;
    /// One entry per distinct `FileCacheData` (dedup by control-block address).
    Caches getUniqueInstances() const;
    FileCacheDataPtr getByName(const std::string & name) const;

    /// Erase every name entry pointing to `cache`; deactivate it outside the lock; lower the
    /// worker-pool max. Does not shut the cache down under the registry lock.
    void remove(const FileCachePtr & cache);

    /// Snapshot and clear the registry, deactivate every unique cache outside the lock, and lower
    /// the worker-pool max to the minimum. Leaves the Factory reusable (empty registry).
    void clear();

    /// Install/uninstall the process-wide Factory pointer (called by `FileCacheManager` only).
    static void setInstance(FileCacheFactory * factory);

private:
    RuntimeServices services_;
    mutable std::mutex registry_mutex_;
    CacheByName cache_by_name_;

    /// Find an existing record whose settings path equals `normalized_path`. Under registry_mutex_.
    FileCacheDataPtr findByPath(const std::string & normalized_path) const;
    /// Grow the shared worker-pool budget by this cache's demand; returns the added delta.
    size_t growWorkerBudget(const FileCacheConfig & config);
    /// Recompute the aggregate budget from the current registry and apply it (>= 1).
    void recomputeWorkerBudgetLocked();

    static std::atomic<FileCacheFactory *> global_instance_;
};

/// Aggregate worker-pool demand of a single unique cache (design 013 Step 7 formula).
size_t computeCacheWorkerMax(const FileCacheConfig & config);

} // namespace facebook::velox::ch
