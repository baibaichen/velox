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

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

#include <folly/hash/Hash.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace facebook::velox::ch
{

/// Minimal statistics for the opened-file (read-handle) cache. Mirrors CH's
/// `FileHandleCacheStats` surface that `FileCacheManager::refreshStats` reports;
/// only fields with a concrete consumer are defined (no invented surface).
struct FileHandleCacheStats
{
    /// Number of distinct cache keys currently held (live or stale-weak) across all shards.
    size_t numCachedFiles = 0;
    /// Number of live handles (weak pointers that still lock to a shared handle).
    size_t numLiveHandles = 0;
    size_t hits = 0;
    size_t misses = 0;
};

/// Cache of opened read handles for local cache-segment files.
///
/// Port of ClickHouse `src/IO/OpenedFileCache.h` (structure preserved 1:1). Two
/// forced substitutions (user-confirmed, D1 contract):
///   - CH caches `std::shared_ptr<OpenedFile>` (fd + mmap). The Velox port caches
///     the read handle for a cache-segment file: `std::shared_ptr<velox::ReadFile>`
///     (stored as `std::weak_ptr<velox::ReadFile>`). CH's `memoryPool` (mmap
///     accounting) is not needed and is dropped.
///   - Ownership is NOT a process singleton and NOT the Hive `FileHandleCache`.
///     `FileCacheManager` owns exactly one instance (`openedFileCache_`),
///     constructed with the injected `filesystems::FileSystem &` used to open.
///
/// The bucket hash is a pure in-memory distribution (no persistence, no
/// bit-compat requirement); CH uses CityHash64, the port uses `folly::hash`.
class OpenedFileCache
{
    class OpenedFileMap
    {
        /// `flags` is retained to mirror CH's key shape; local cache-segment reads
        /// do not use `O_DIRECT`, so in practice a single flag value is used.
        using Key = std::pair<std::string /* path */, int /* flags */>;
        using ReadFileWeakPtr = std::weak_ptr<ReadFile>;
        using Files = std::map<Key, ReadFileWeakPtr>;

        /// The shard state (mutex + map) lives in a `shared_ptr` so the on-last-release
        /// deleter can hold a `weak_ptr` to it: if the whole `OpenedFileCache` (and thus this
        /// shard) is destroyed while a `ReadFile` handle is still alive somewhere, the deleter
        /// locks the `weak_ptr`, finds it expired, and becomes a safe no-op instead of a
        /// use-after-free on a destroyed mutex/map. (H1.)
        struct Shard
        {
            mutable std::mutex mutex;
            Files files;
        };
        std::shared_ptr<Shard> shard_ = std::make_shared<Shard>();

    public:
        using ReadFilePtr = std::shared_ptr<ReadFile>;

        OpenedFileMap() = default;
        OpenedFileMap(const OpenedFileMap &) = delete;
        OpenedFileMap & operator=(const OpenedFileMap &) = delete;

        ReadFilePtr
        get(const std::string & path,
            int flags,
            filesystems::FileSystem & fileSystem,
            std::atomic<size_t> & hits,
            std::atomic<size_t> & misses)
        {
            Key key(path, flags);

            std::lock_guard lock(shard_->mutex);

            auto [it, inserted] = shard_->files.emplace(key, ReadFileWeakPtr{});
            if (!inserted)
            {
                if (auto res = it->second.lock())
                {
                    hits.fetch_add(1, std::memory_order_relaxed);
                    return res;
                }
            }
            misses.fetch_add(1, std::memory_order_relaxed);

            /// Open via the injected FileSystem and install with a custom deleter that erases the
            /// map entry on last release (mirrors CH's deleter). The deleter holds a weak_ptr to
            /// the shard (H1) and only erases the entry if it still refers to THIS dying pointer,
            /// so a key resurrected by a concurrent `get` is not dropped (M1).
            std::unique_ptr<ReadFile> opened = fileSystem.openFileForRead(path);
            std::weak_ptr<Shard> weak_shard = shard_;
            ReadFilePtr res(
                opened.release(),
                [key, weak_shard](ReadFile * ptr)
                {
                    if (auto shard = weak_shard.lock())
                    {
                        std::lock_guard another_lock(shard->mutex);
                        auto found = shard->files.find(key);
                        /// Erase only if the entry is still (or now expired to) this pointer's slot;
                        /// a live weak that resurrected the key belongs to a newer handle -> keep it.
                        if (found != shard->files.end() && found->second.expired())
                            shard->files.erase(found);
                    }
                    delete ptr;
                });

            it->second = res;
            return res;
        }

        void remove(const std::string & path, int flags)
        {
            Key key(path, flags);
            std::lock_guard lock(shard_->mutex);
            shard_->files.erase(key);
        }

        /// Erase every flag-variant for `path` (the port keys by `flags`, but a
        /// physical change to `path` invalidates all of them). Returns count erased.
        size_t removeAllFlags(const std::string & path)
        {
            std::lock_guard lock(shard_->mutex);
            size_t erased = 0;
            for (auto it = shard_->files.begin(); it != shard_->files.end();)
            {
                if (it->first.first == path)
                {
                    it = shard_->files.erase(it);
                    ++erased;
                }
                else
                    ++it;
            }
            return erased;
        }

        void collectStats(size_t & numCachedFiles, size_t & numLiveHandles) const
        {
            std::lock_guard lock(shard_->mutex);
            numCachedFiles += shard_->files.size();
            for (const auto & [_, weak] : shard_->files)
                if (!weak.expired())
                    ++numLiveHandles;
        }

        void clear()
        {
            std::lock_guard lock(shard_->mutex);
            shard_->files.clear();
        }
    };

    static constexpr size_t buckets = 1024;

    filesystems::FileSystem & fileSystem_;
    std::array<OpenedFileMap, buckets> impls;

    std::atomic<size_t> hits_{0};
    std::atomic<size_t> misses_{0};

    static size_t bucketOf(const std::string & path)
    {
        return folly::hash::fnv64_buf(path.data(), path.size()) % buckets;
    }

public:
    using ReadFilePtr = OpenedFileMap::ReadFilePtr;

    /// `localFileSystem` must outlive this cache (Manager owns both; the file
    /// system is declared before, and destroyed after, the cache).
    explicit OpenedFileCache(filesystems::FileSystem & localFileSystem) : fileSystem_(localFileSystem) { }

    OpenedFileCache(const OpenedFileCache &) = delete;
    OpenedFileCache & operator=(const OpenedFileCache &) = delete;

    /// Returns a shared read handle for `path`. On a live weak hit the existing
    /// handle is reused; otherwise the file is opened via the injected FileSystem.
    ReadFilePtr get(const std::string & path, int flags = 0)
    {
        return impls[bucketOf(path)].get(path, flags, fileSystem_, hits_, misses_);
    }

    /// Idempotently drop the cached handle for `(path, flags)`.
    void remove(const std::string & path, int flags = 0) { impls[bucketOf(path)].remove(path, flags); }

    /// Invalidate every cached handle for `path` regardless of flags. Used by the
    /// remove/rename seams after a physical change to the file at `path`.
    size_t removePath(const std::string & path) { return impls[bucketOf(path)].removeAllFlags(path); }

    /// Drop every cached handle (Manager shutdown).
    void clear()
    {
        for (auto & impl : impls)
            impl.clear();
    }

    FileHandleCacheStats stats() const
    {
        FileHandleCacheStats result;
        for (auto & impl : impls)
            impl.collectStats(result.numCachedFiles, result.numLiveHandles);
        result.hits = hits_.load(std::memory_order_relaxed);
        result.misses = misses_.load(std::memory_order_relaxed);
        return result;
    }
};

} // namespace facebook::velox::ch
