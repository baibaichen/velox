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
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"
#include "velox/common/caching/SimpleLRUCache.h"

#include <folly/container/F14Map.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace folly
{
class Timekeeper;
} // namespace folly

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

/// Opened-file-handle cache statistics, mirroring the Velox
/// `SimpleLRUCacheStats`/`FileHandleCacheStats` shape used elsewhere.
using FileHandleCacheStats = velox::SimpleLRUCacheStats;

/// Per-cache runtime snapshot exposed by `FileCacheManager::refreshStats`.
struct FileCacheStats
{
    size_t maxSize = 0;
    size_t usedSize = 0;
    size_t fileSegments = 0;
};

/// Process-level runtime owner of the file cache.
///
/// The Manager owns every runtime resource shared by the caches (the physical
/// worker pool, the scheduled-task timer runtime, the opened-file cache) and one
/// `FileCacheFactory` that owns the sole name/path registry. The dependency
/// direction is strict: Manager -> resources -> Factory -> FileCache; a
/// `FileCache` never calls or stores the Manager/Factory.
class FileCacheManager
{
public:
    /// One named cache configuration. `name`/`configPath` do not participate in
    /// `FileCacheConfig` equality (see `FileCacheFactory`).
    struct NamedFileCacheConfig
    {
        std::string name;
        FileCacheConfig config;
        std::string configPath;
    };

    struct FileCacheManagerStats
    {
        folly::F14FastMap<std::string, FileCacheStats> cachesByName;
        FileHandleCacheStats openedFileCache;
        size_t uniqueCaches = 0;
        size_t workerPoolMax = 0;
        size_t workerPoolActive = 0;
    };

    struct Options
    {
        std::vector<NamedFileCacheConfig> caches;
        std::string defaultCacheName;
        std::string commonUserId;
        std::string cachePathPrefix;
        std::string allowedCacheRoot;
        std::shared_ptr<filesystems::FileSystem> localFileSystem;
        memory::MemoryPool * memoryPool = nullptr;
        std::shared_ptr<folly::Timekeeper> timekeeper;
        bool initializeOnCreate = true;
    };

    static std::shared_ptr<FileCacheManager> create(Options options);

    static FileCacheManager * getInstance();
    static FileCacheManager & instance();
    /// Publishes/withdraws the process-global Manager (and its Factory) pointer.
    /// The stored pointer is NON-OWNING: the owner (e.g. Gluten `VeloxBackend`)
    /// keeps the `shared_ptr` and MUST call `setInstance(nullptr)` BEFORE the
    /// Manager is destroyed, otherwise `instance()`/`getInstance()` would dangle.
    /// The teardown order (see the manager design "global pointer lifetime") is
    /// strictly: `manager->shutdown()` -> `setInstance(nullptr)` -> drop the
    /// owning `shared_ptr`. This is an explicit, caller-driven contract: the
    /// Manager never auto-installs itself on `create`, and `setInstance` never
    /// silently replaces a live different Manager (that is rejected) nor
    /// self-uninstalls on destruction. Re-installing the same Manager is a no-op.
    static void setInstance(FileCacheManager * manager);

    FileCacheFactory & factory();
    const FileCacheFactory & factory() const;
    FileCachePtr get(const std::string & name) const;
    FileCachePtr getDefault() const;
    const std::string & commonUserId() const { return commonUserId_; }

    void initialize();
    void applyConfigs(const std::vector<NamedFileCacheConfig> & configs);
    void shutdown();

    FileCacheManagerStats refreshStats() const;
    std::string toString(bool details = true) const;

    OpenedFileCache & openedFileCache();
    FileCacheWorkerPool & workerPool();
    FileCacheScheduler & scheduler();

    /// Diagnostic: true iff the Manager's mutation mutex is currently free, probed
    /// from a separate thread with a non-blocking `try_lock` (retried to tolerate
    /// spurious failures). Lets tests assert that applyConfigs/getOrCreate/shutdown
    /// hold the single serialization lock across their budget-recompute/pool-resize
    /// critical section without risking a deadlock.
    bool isMutationLockFree() const;

    FileCacheManager(const FileCacheManager &) = delete;
    FileCacheManager & operator=(const FileCacheManager &) = delete;

private:
    explicit FileCacheManager(Options options);

    enum class State
    {
        Created,
        Initialized,
        ShuttingDown,
        Shutdown,
    };

    static void validateOptions(const Options & options);
    static size_t computeWorkerPoolMax(const std::vector<NamedFileCacheConfig> & caches);

    // Member declaration order determines destruction order (reverse). factory_
    // is declared AFTER the resources so it (and the FileCache objects it owns)
    // is destroyed FIRST, while the runtime services it references are still live.
    std::shared_ptr<filesystems::FileSystem> localFileSystem_;
    memory::MemoryPool * const memoryPool_;
    std::shared_ptr<folly::Timekeeper> timekeeper_;
    const std::string commonUserId_;
    const std::string defaultCacheName_;

    // The single Manager-owned serialization lock. It serializes every mutating
    // lifecycle/pool operation (initialize/applyConfigs/shutdown and the Factory's
    // getOrCreate/create/remove/clear, which take it through the injected
    // reference) and also guards `state_`. Declared BEFORE the resources/factory
    // so the reference handed to `factory_` (RuntimeServices::mutationMutex) is
    // valid for the Factory's whole lifetime; it is destroyed last (after
    // factory_), when nothing can still lock it.
    mutable std::mutex mutation_mutex_;

    FileCacheWorkerPool workerPool_;
    FileCacheScheduler scheduler_;
    OpenedFileCache openedFileCache_;
    FileCacheFactory factory_;

    State state_ = State::Created;

    static std::atomic<FileCacheManager *> global_instance_;
};

} // namespace facebook::velox::ch
