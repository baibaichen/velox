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
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"

#include <folly/container/F14Map.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace folly
{
class Timekeeper;
} // namespace folly

namespace facebook::velox::ch
{

/// Minimal per-cache stats surface reported by `FileCacheManager::refreshStats`.
/// Only fields with a concrete consumer are defined (no invented surface).
struct FileCacheStats
{
    size_t maxSize = 0;
    size_t usedSize = 0;
    size_t fileSegments = 0;
};

/// Runtime resource owner and singleton front-door for the `FileCache` subsystem.
///
/// Owns the shared worker pool, timekeeper + scheduler, opened-file cache, and the
/// sole `FileCacheFactory`. Dependency direction is strict (design 013):
///   FileCacheManager -> owns runtime resources + one FileCacheFactory
///   FileCacheFactory -> owns the registry, constructs FileCache with Manager refs
///   FileCache        -> never references the Manager/Factory
class FileCacheManager
{
public:
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
    };

    struct Options
    {
        std::vector<NamedFileCacheConfig> caches;
        std::string defaultCacheName;
        std::string commonUserId;
        std::shared_ptr<filesystems::FileSystem> localFileSystem;
        std::shared_ptr<folly::Timekeeper> timekeeper;
        bool initializeOnCreate = true;
    };

    static std::shared_ptr<FileCacheManager> create(Options options);

    static FileCacheManager * getInstance();
    static FileCacheManager & instance();
    static void setInstance(FileCacheManager * manager);

    FileCacheFactory & factory() { return factory_; }
    const FileCacheFactory & factory() const { return factory_; }
    FileCachePtr get(const std::string & name) const;
    FileCachePtr getDefault() const;
    /// True iff a default cache name is configured. Non-throwing companion to
    /// `getDefault` (which throws when no default is configured). Does NOT verify
    /// the named cache still exists in the factory — it reflects configuration, the
    /// same field `getDefault` guards on.
    bool hasDefault() const;
    const std::string & commonUserId() const { return commonUserId_; }

    void initialize();
    void shutdown();

    FileCacheManagerStats refreshStats() const;

    OpenedFileCache & openedFileCache() { return openedFileCache_; }
    FileCacheWorkerPool & workerPool() { return workerPool_; }
    FileCacheScheduler & scheduler() { return scheduler_; }

    FileCacheManager(const FileCacheManager &) = delete;
    FileCacheManager & operator=(const FileCacheManager &) = delete;

    ~FileCacheManager();

private:
    explicit FileCacheManager(Options options);

    enum class State : uint8_t
    {
        Created,
        Initialized,
        ShuttingDown,
        Shutdown,
    };

    static size_t computeWorkerPoolMax(const std::vector<NamedFileCacheConfig> & caches);

    // Member declaration order determines destruction order (reverse). `factory_` (and thus every
    // FileCache) is declared AFTER the resources it references so those resources outlive it.
    std::shared_ptr<filesystems::FileSystem> localFileSystem_;
    std::shared_ptr<folly::Timekeeper> timekeeper_;
    const std::string commonUserId_;
    const std::string defaultCacheName_;

    FileCacheWorkerPool workerPool_;
    FileCacheScheduler scheduler_;
    OpenedFileCache openedFileCache_;
    FileCacheFactory factory_;

    mutable std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_cv_;
    State state_ = State::Created;

    static std::atomic<FileCacheManager *> global_instance_;
};

} // namespace facebook::velox::ch
