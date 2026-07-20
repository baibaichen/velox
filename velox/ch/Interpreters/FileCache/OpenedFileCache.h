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

#include "velox/ch/Common/ProfileEvents.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace facebook::velox::ch
{

/// RAII holder for a readonly opened cache-segment file.
///
/// Velox analog of ClickHouse `OpenedFile` (`src/IO/OpenedFile.h`), which wraps
/// a raw file descriptor. Here it owns a `velox::ReadFile` opened through the
/// manager-injected local `FileSystem`, so multiple cache readers can share a
/// single handle for the same `(path, flags)` instead of reopening the file on
/// every read.
class OpenedFile
{
public:
    OpenedFile(std::string file_name, int flags, std::shared_ptr<velox::ReadFile> file)
        : file_name_(std::move(file_name)), flags_(flags), file_(std::move(file))
    {
    }

    const std::string & getFileName() const { return file_name_; }
    int getFlags() const { return flags_; }
    const std::shared_ptr<velox::ReadFile> & getFile() const { return file_; }

private:
    std::string file_name_;
    int flags_;
    std::shared_ptr<velox::ReadFile> file_;
};

/// Manager-owned cache of opened readonly cache-segment files.
///
/// Faithful port of ClickHouse `src/IO/OpenedFileCache.h`: a fixed 1024-bucket
/// vector of bucket-local maps keyed by `(path, flags)`, each guarded by its own
/// mutex and holding `std::weak_ptr<OpenedFile>` values, with a custom-deleter
/// `shared_ptr` that erases the weak entry when the last strong holder drops.
///
/// Two user-approved substitutions (see the Task-013 dependency mapping and the
/// cross-profile decisions):
///  * every container allocation (the bucket vector and every per-bucket map)
///    uses `velox::memory::StlAllocator` bound to the manager-injected
///    `MemoryPool`, replacing CH `VectorWithMemoryTracking`/`MapWithMemoryTracking`;
///    a plain, untracked container is forbidden.
///  * bucket selection uses `std::hash<std::string>` instead of CH
///    `CityHash_v1_0_2::CityHash64` -- internal-only, since no caller observes
///    which physical bucket an entry lands in, only `get`/`remove` correctness
///    and weak-pointer sharing.
///
/// The three `ProfileEvents::OpenedFileCache*` names are consumed from the
/// Task-003 no-op `ProfileEvents` shim (this file must not edit `ProfileEvents.h`).
///
/// Lifetime safety: unlike CH's immortal `OpenedFileCache::instance()` singleton,
/// this cache is a `FileCacheManager` member with a bounded lifetime, so a
/// returned `OpenedFilePtr` can in principle outlive it (a host lifecycle
/// violation: readers are expected to drop handles before the manager shuts down,
/// guaranteed by the process shutdown barrier in the manager design). To make
/// that case code-safe rather than merely documented, each bucket keeps its
/// `(map + mutex)` in a heap `shared_ptr<BucketState>` and every per-entry deleter
/// captures a `weak_ptr<BucketState>`. On the last strong holder dropping, the
/// deleter re-locks the state only if it is still alive (normal erase-on-last-
/// holder); if the state has expired (the `OpenedFileCache` was already
/// destroyed) it simply deletes the file handle without touching the freed
/// map/mutex -- no use-after-free.
///
/// `BucketState` (holding the pool-allocated `std::map`) is destroyed exactly when
/// its owning bucket is destroyed, i.e. during `OpenedFileCache` destruction while
/// the injected `MemoryPool` is still alive (the pool always outlives the manager,
/// hence the cache). The map's pool-charged nodes are therefore always freed
/// against a live pool. A deleter that outlives the cache never destroys
/// `BucketState` and never touches the pool: it only ever sees an expired weak
/// pointer and deletes the plain `OpenedFile`.
class OpenedFileCache
{
public:
    using OpenedFilePtr = std::shared_ptr<OpenedFile>;

    OpenedFileCache(velox::filesystems::FileSystem & file_system, velox::memory::MemoryPool & pool)
        : file_system_(&file_system), impls_(BucketAllocator(&pool))
    {
        // The bucket vector never grows or shrinks after construction, so its
        // buckets' addresses are stable for the lifetime of the cache. That
        // stability is what lets each per-entry deleter safely capture its owning
        // bucket. reserve() first, then emplace_back(): reserving avoids any
        // reallocation (and therefore any move) of the buckets.
        impls_.reserve(kBuckets);
        for (size_t i = 0; i < kBuckets; ++i)
            impls_.emplace_back(&pool);
    }

    OpenedFileCache(const OpenedFileCache &) = delete;
    OpenedFileCache & operator=(const OpenedFileCache &) = delete;

    OpenedFilePtr get(const std::string & path, int flags)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::OpenedFileCacheMicroseconds);
        return impls_[bucketFor(path)].get(path, flags, *file_system_);
    }

    void remove(const std::string & path, int flags)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::OpenedFileCacheMicroseconds);
        impls_[bucketFor(path)].remove(path, flags);
    }

    /// Path-only invalidation (all flags). Matches the manager-injected
    /// `OpenedFileInvalidator` (`void(path)`) that `FileSegment`/`CacheMetadata`
    /// call when a cache file is renamed or removed. Because the bucket index is
    /// a function of `path` alone, every `(path, *)` entry lives in one bucket.
    void remove(const std::string & path)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::OpenedFileCacheMicroseconds);
        impls_[bucketFor(path)].removeAllFlags(path);
    }

    void clear()
    {
        for (auto & impl : impls_)
            impl.clear();
    }

    /// Number of live (non-expired) handles across all buckets.
    size_t count() const
    {
        size_t total = 0;
        for (const auto & impl : impls_)
            total += impl.count();
        return total;
    }

private:
    static constexpr size_t kBuckets = 1024;

    /// One hash bucket: a pool-allocated `std::map` keyed by `(path, flags)`
    /// holding weak handles, guarded by its own mutex. The map and mutex live in
    /// a heap `shared_ptr<BucketState>` so a per-entry deleter can capture a
    /// `weak_ptr` and stay safe if the handle outlives this `OpenedFileCache`.
    class OpenedFileMap
    {
    public:
        using Key = std::pair<std::string /* path */, int /* flags */>;
        using OpenedFileWeakPtr = std::weak_ptr<OpenedFile>;
        using FilesAllocator = velox::memory::StlAllocator<std::pair<const Key, OpenedFileWeakPtr>>;
        using Files = std::map<Key, OpenedFileWeakPtr, std::less<Key>, FilesAllocator>;

        explicit OpenedFileMap(velox::memory::MemoryPool * pool)
            : state_(std::make_shared<BucketState>(pool))
        {
        }

        // Move-only via the `shared_ptr` member; the bucket vector is filled by
        // emplacing (the pool-bound `std::map` is not default-constructible), so
        // the bucket must be movable and must not copy its live weak entries.
        OpenedFileMap(OpenedFileMap &&) = default;
        OpenedFileMap & operator=(OpenedFileMap &&) = default;
        OpenedFileMap(const OpenedFileMap &) = delete;
        OpenedFileMap & operator=(const OpenedFileMap &) = delete;

        OpenedFilePtr get(const std::string & path, int flags, velox::filesystems::FileSystem & file_system)
        {
            Key key(path, flags);

            std::lock_guard<std::mutex> lock(state_->mutex);

            auto it = state_->files.find(key);
            if (it != state_->files.end())
            {
                if (auto res = it->second.lock())
                {
                    ProfileEvents::increment(ProfileEvents::OpenedFileCacheHits);
                    return res;
                }
            }
            ProfileEvents::increment(ProfileEvents::OpenedFileCacheMisses);

            // Open before recording the handle so a failed open leaves no stale
            // (expired) entry behind.
            std::shared_ptr<velox::ReadFile> file = file_system.openFileForRead(path);

            // The deleter captures a WEAK reference to the bucket state, never the
            // bucket itself. If the OpenedFileCache (and this bucket) is already
            // gone when the last strong holder drops, the weak pointer is expired
            // and the deleter just frees the file handle -- it never touches the
            // destroyed map/mutex.
            std::weak_ptr<BucketState> weak_state = state_;
            OpenedFilePtr res(
                new OpenedFile(path, flags, std::move(file)),
                [key, weak_state](OpenedFile * ptr)
                {
                    if (auto state = weak_state.lock())
                    {
                        std::lock_guard<std::mutex> another_lock(state->mutex);
                        state->files.erase(key);
                    }
                    delete ptr;
                });

            state_->files[key] = res;
            return res;
        }

        void remove(const std::string & path, int flags)
        {
            Key key(path, flags);
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->files.erase(key);
        }

        void removeAllFlags(const std::string & path)
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            for (auto it = state_->files.begin(); it != state_->files.end();)
            {
                if (it->first.first == path)
                    it = state_->files.erase(it);
                else
                    ++it;
            }
        }

        void clear()
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->files.clear();
        }

        size_t count() const
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            size_t live = 0;
            for (const auto & entry : state_->files)
                if (!entry.second.expired())
                    ++live;
            return live;
        }

    private:
        /// Map + mutex shared with every outstanding handle's deleter through a
        /// `weak_ptr`. The `std::map` keeps its pool-charged allocator (its nodes
        /// stay tracked); `BucketState` is destroyed together with its owning
        /// bucket during `OpenedFileCache` destruction, while the pool is alive.
        struct BucketState
        {
            explicit BucketState(velox::memory::MemoryPool * pool) : files(FilesAllocator(pool)) { }

            std::mutex mutex;
            Files files;
        };

        std::shared_ptr<BucketState> state_;
    };

    using BucketAllocator = velox::memory::StlAllocator<OpenedFileMap>;

    static size_t bucketFor(const std::string & path)
    {
        return std::hash<std::string>{}(path) % kBuckets;
    }

    velox::filesystems::FileSystem * file_system_;
    std::vector<OpenedFileMap, BucketAllocator> impls_;
};

} // namespace facebook::velox::ch
