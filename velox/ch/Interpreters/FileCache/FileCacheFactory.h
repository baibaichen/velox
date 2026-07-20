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

#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>

namespace facebook::velox
{
namespace filesystems
{
class FileSystem;
}
namespace memory
{
class MemoryPool;
}
} // namespace facebook::velox

namespace facebook::velox::ch
{

class FileCacheManager;

/// Sole owner of the file-cache name/path registry.
///
/// Faithful port of ClickHouse `FileCacheFactory` (`src/Interpreters/FileCache/`),
/// with the process-global `Context`/singleton runtime dependencies replaced by
/// references injected from the owning `FileCacheManager`. The Factory does not
/// own those runtime services and does not store a `FileCacheManager *`; it uses
/// the injected references only to construct `FileCache` objects and to size the
/// Manager-owned shared worker pool.
class FileCacheFactory final
{
public:
    /// Registry entry: an immutable `FileCache` and its canonical config source
    /// path, plus a separately-locked settings snapshot so reload never has to
    /// hold the registry lock while touching settings.
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

    /// Manager-owned service references; the Factory does not own them.
    struct RuntimeServices
    {
        FileCacheWorkerPool & workerPool;
        FileCacheScheduler & scheduler;
        OpenedFileCache & openedFileCache;
        filesystems::FileSystem & localFileSystem;
        memory::MemoryPool & memoryPool;
        std::string commonUserId;
        /// Manager-owned mutex that serializes every mutating lifecycle/pool
        /// operation (Factory getOrCreate/create/remove/clear and Manager
        /// initialize/applyConfigs/shutdown) so budget recompute and shared-pool
        /// resize can never race or resize a stopped pool. It is the OUTERMOST
        /// lock: order is `mutationMutex -> registry_mutex_ -> settings_mutex_`,
        /// and it is never acquired while the registry lock is held.
        std::mutex & mutationMutex;
    };

    explicit FileCacheFactory(RuntimeServices services);

    FileCacheFactory(const FileCacheFactory &) = delete;
    FileCacheFactory & operator=(const FileCacheFactory &) = delete;

    /// Singleton: backed by the Manager-installed atomic pointer. Throws if no
    /// Manager has published a Factory.
    static FileCacheFactory & instance();

    // -- registry API --------------------------------------------------------
    //
    // The four mutating entry points below (getOrCreate/create/remove/clear) hold
    // the Manager-owned `mutationMutex` for their whole body so their budget
    // recompute and shared-pool resize are serialized with each other and with
    // `FileCacheManager::applyConfigs`/`shutdown`. The Manager, which already
    // holds that mutex, calls the private `*Locked` variants directly.

    FileCachePtr getOrCreate(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);

    FileCachePtr create(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);

    FileCachePtr get(const std::string & name) const;

    using CacheByName = folly::F14FastMap<std::string, FileCacheDataPtr>;
    using Caches = folly::F14FastSet<FileCacheDataPtr>;
    using CacheNames = folly::F14FastSet<std::string>;

    CacheByName getAll() const;
    Caches getUniqueInstances() const;
    /// Snapshot of every currently-bound cache name. Used by
    /// `FileCacheManager::applyConfigs` to record the pre-apply name set so the
    /// whole new-cache registration phase can be rolled back as one transaction.
    CacheNames getAllNames() const;
    FileCacheDataPtr getByName(const std::string & name) const;

    void remove(const FileCachePtr & cache);
    void clear();

    // -- worker-budget introspection ----------------------------------------

    /// Effective shared-worker-pool budget: the checked sum over every unique
    /// cache's per-cache worker maximum, floored at 1.
    size_t workerBudget() const;

    /// Per-cache shared-worker-pool budget. Shared by `FileCacheManager::create`
    /// (upfront pool sizing) and `getOrCreate`/`create` (incremental growth) so a
    /// single formula governs both. Uses `FileCacheUtils::checkedAdd`.
    static size_t computeCacheWorkerMax(const FileCacheConfig & config);

    /// Diagnostic: true iff the registry lock is currently free, probed from a
    /// separate thread with a non-blocking `try_lock` (retried to tolerate
    /// spurious failures). Lets tests assert that cache initialize/apply/
    /// deactivate run OUTSIDE the registry lock without risking a deadlock.
    bool isRegistryLockFree() const;

    // -- Manager-only singleton installation ---------------------------------

    static void setInstance(FileCacheFactory * factory);

private:
    friend class FileCacheManager;

    // Mutation bodies that assume the Manager-owned `services_.mutationMutex` is
    // already held. The public getOrCreate/create/remove/clear lock it and
    // delegate here; FileCacheManager (which holds it across applyConfigs/
    // shutdown) calls these directly to avoid re-locking a non-recursive mutex.
    FileCachePtr getOrCreateLocked(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);
    FileCachePtr createLocked(const std::string & name, const FileCacheConfig & settings, const std::string & config_path);
    void removeLocked(const FileCachePtr & cache);
    void clearLocked();

    // Fail-close transaction primitive for FileCacheManager::applyConfigs: remove
    // every name binding NOT in `keep_names` (the names that existed before the
    // apply started), deactivate any unique cache that becomes unreferenced (the
    // instances this apply created), recompute worker_budget_ from the survivors
    // and resize the shared pool to that truthful budget (unless shut down). Every
    // pre-existing name/instance binding is preserved. Assumes the Manager-owned
    // `services_.mutationMutex` is already held; deactivation and the pool resize
    // run OUTSIDE the registry lock.
    void rollbackNewBindingsLocked(const CacheNames & keep_names);

    FileCacheDataPtr findByPath(const std::string & normalized_path) const;

    // Permanently reject further registry mutations. Called by FileCacheManager
    // at the end of shutdown (the shared pool/scheduler are being torn down, so
    // no new FileCache may be constructed). clear() does NOT set this: it leaves
    // the Factory reusable.
    void markShutdown();

    // True once markShutdown() has run. Read under registry_mutex_. Used by
    // remove/clear to skip a shared-pool resize once the Manager has stopped the
    // pool during shutdown (resizing a stopped executor is undefined).
    bool isShutdown() const;

    // Recompute worker_budget_ from the currently registered settings (used by
    // FileCacheManager::applyConfigs once settings have changed). Returns the
    // effective (floored-at-1) budget. Locks the registry internally.
    size_t recomputeWorkerBudget();

    // registry_mutex_ must be held by the caller.
    size_t growWorkerBudget(const FileCacheConfig & config);
    void rollbackWorkerBudget(size_t budget_delta);

    // Deactivates each unique cache OUTSIDE the registry lock (fires the
    // `deactivateOutsideLock` seam once per cache). Used by remove()/clear().
    static void deactivateUnique(const Caches & unique);

    RuntimeServices services_;

    mutable std::mutex registry_mutex_;
    CacheByName cache_by_name_;
    /// Raw checked sum of every unique cache's per-cache worker maximum (0 when
    /// empty). `workerBudget()` floors this at 1 to match the pool minimum.
    size_t worker_budget_ = 0;
    bool shutdown_ = false;

    static std::atomic<FileCacheFactory *> global_instance_;
};

} // namespace facebook::velox::ch
