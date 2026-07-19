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

#include <folly/container/F14Map.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <mutex>

namespace facebook::velox::ch::FileCacheUtils
{

/// Hash-sharded map: keys are spread across independently-locked buckets so
/// concurrent operations on different keys rarely contend.
///
/// Callback contract (withShard / forEachShard):
///   - May read or mutate its shard map.
///   - Must not recursively call withShard/forEachShard on the same ShardedMap
///     (same-shard reentry deadlocks; cross-shard reentry risks livelock).
///   - Must not return or store iterators, references, or pointers into the map
///     beyond the callback scope (they are invalidated on unlock/rehash).
///   - Any result the callback needs after the lock is released must be returned
///     by value (a copy), not as a handle into the shard map. The origin dedup
///     caller returns a copied shared_ptr, never a map iterator/reference.
///
/// Size semantics:
///   - `size()` returns a relaxed snapshot: accurate after a completed mutation,
///     but a concurrent observer may see a transient value.
///   - Exception safety: if a callback mutates the map and then throws, the
///     mutation stands and `total_count_` is updated to the actual post-mutation
///     shard size before the exception propagates.
template <
    typename Key,
    typename Value,
    size_t num_shards = 32,
    typename Hash = std::hash<Key>>
class ShardedMap
{
    static_assert(num_shards > 0, "num_shards must be greater than zero");

public:
    using Map = folly::F14FastMap<Key, Value, Hash>;

    ShardedMap(const ShardedMap &) = delete;
    ShardedMap & operator=(const ShardedMap &) = delete;

    explicit ShardedMap(ProfileEvents::Event lock_wait_event)
        : lock_wait_event_(lock_wait_event)
    {
    }

    /// Run `f(map)` under the owning shard's lock. Returns the callback result.
    template <typename F>
    auto withShard(const Key & key, F && f) const
    {
        Shard & shard = shards_[Hash{}(key) % num_shards];
        std::unique_lock<std::mutex> lock(shard.mutex);
        const size_t size_before = shard.map.size();
        // Exception-safe size accounting: the guard fires even if f() throws.
        struct SizeGuard
        {
            const ShardedMap & self;
            const Shard & shard;
            size_t before;
            ~SizeGuard() noexcept
            {
                self.accountSizeDelta(before, shard.map.size());
            }
        } guard{*this, shard, size_before};
        return f(shard.map);
    }

    /// Run `f(map)` under each shard's lock in turn (sequential, not
    /// simultaneous). Provides no globally-atomic snapshot.
    template <typename F>
    void forEachShard(F && f) const
    {
        for (Shard & shard : shards_)
        {
            std::unique_lock<std::mutex> lock(shard.mutex);
            const size_t size_before = shard.map.size();
            struct SizeGuard
            {
                const ShardedMap & self;
                const Shard & shard;
                size_t before;
                ~SizeGuard() noexcept
                {
                    self.accountSizeDelta(before, shard.map.size());
                }
            } guard{*this, shard, size_before};
            f(shard.map);
        }
    }

    /// Relaxed snapshot of the total element count across all shards.
    size_t size() const noexcept
    {
        return total_count_.load(std::memory_order_relaxed);
    }

private:
    struct Shard
    {
        mutable std::mutex mutex;
        Map map;
    };

    void accountSizeDelta(size_t before, size_t after) const noexcept
    {
        if (after > before)
            total_count_.fetch_add(
                after - before, std::memory_order_relaxed);
        else if (after < before)
            total_count_.fetch_sub(
                before - after, std::memory_order_relaxed);
    }

    const ProfileEvents::Event lock_wait_event_;
    mutable std::array<Shard, num_shards> shards_;
    mutable std::atomic<size_t> total_count_{0};
};

} // namespace facebook::velox::ch::FileCacheUtils
