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
#include "velox/ch/Common/ClickHouseAliases.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/ShardedMap.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace facebook::velox::ch
{

/// This mutex guard is needed only for correctness of `OvercommitFileCachePriority::check` method,
/// but not for cache correctness in general,
/// because it makes sure per-user counters
/// (`total_size` and `total_elements` in `CacheUsage` per-user state)
/// are updated atomically with main cache state counters.
/// The per-user `total_size` and `total_elements` counters are used for:
/// 1. to decide from whom to evict according to overcommit policy
/// 2. for system tables
/// In both cases some temporary divergence from actual cache counters is fine,
/// but we now have no divergence as atomicity is needed for `OvercommitFileCachePriority::check`'s success.
struct CacheUsageStatGuard
{
    struct Lock : public std::unique_lock<std::mutex> { explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {} };
    Lock lock() { return Lock(mutex); }
    std::mutex mutex;

    CacheUsageStatGuard() = default;
    CacheUsageStatGuard(const CacheUsageStatGuard &) = delete;
    CacheUsageStatGuard & operator=(const CacheUsageStatGuard &) = delete;
};

/// A caching eviction strategy, which allows to evict more from users which use the cache more.
/// From each user cache is evicted according to LRU/SLRU eviction policies.
struct CacheUsage
{
    /// priority is a non-owning pointer; the pointed-to object is owned by CacheUsagePerUser::CacheUserData.
    CacheUsage(const FileCacheOriginInfo & origin_info_, IFileCachePriority * priority_);

    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    const FileCacheOriginInfo origin_info;
    /// A user priority, contains only entries which belong to `user`
    /// by corresponding eviction strategy priority.
    IFileCachePriority * const priority{};

    std::shared_ptr<CacheUsageStatGuard> guard;

    void update(int64_t size, int64_t elements, const CacheUsageStatGuard::Lock & lock);

    std::atomic<UInt64> total_size = 0;
    std::atomic<UInt64> total_elements = 0;

    /// Last cache access by this client, for the idle-client TTL.
    void touch(TimePoint now) { last_access.store(now, std::memory_order_relaxed); }
    bool idleFor(std::chrono::seconds ttl, TimePoint now) const
    {
        return now - last_access.load(std::memory_order_relaxed) >= ttl;
    }

    bool operator <(const CacheUsage & other) const;
    bool operator ==(const CacheUsage & other) const;

    bool lessWithAssumption(const CacheUsage & other, size_t released_size_assumption, size_t other_released_size_assumption) const;

private:
    std::atomic<TimePoint> last_access;
};
using CacheUsagePtr = std::shared_ptr<CacheUsage>;


/// Owns the per-user cache usage map, each method is thread-safe.
struct CacheUsagePerUser
{
    using UserID = std::string;

    CacheUsagePerUser();

    CacheUsagePerUser(const CacheUsagePerUser &) = delete;
    CacheUsagePerUser & operator=(const CacheUsagePerUser &) = delete;

    size_t size() const { return clients_map.size(); }

    /// Returns the usages of all non-empty users (lazy cleanup of empty entries as a side effect).
    std::vector<CacheUsagePtr> snapshot() const;

    /// Returns nullptr when not found.
    CacheUsagePtr tryGet(const UserID & user_id) const;

    /// Returns the existing entry or inserts a new one created by make().
    /// The returned CacheUsagePtr keeps use_count > 1 and prevents
    /// user's usage from being cleaned up due to being empty.
    CacheUsagePtr getOrSet(
        const UserID & user_id,
        std::function<std::pair<FileCachePriorityPtr, CacheUsagePtr>()> make);

    void touchClient(const UserID & user_id);

    /// Idle clients (>= ttl) with their usage, so the caller can re-check without
    /// a second lookup. Reaps empty unreferenced entries in passing.
    std::vector<std::pair<UserID, CacheUsagePtr>> collectIdleClients(std::chrono::seconds ttl) const;

private:
    struct CacheUserData
    {
        FileCachePriorityPtr priority;
        /// `usage` is declared after `priority`, so it is destroyed first: the final
        /// reference-count decrement of `usage` synchronizes with the releases of the
        /// copies handed out by `snapshot`, ordering the destruction of `priority`
        /// (and its queue) after any concurrent use of it through those copies.
        CacheUsagePtr usage;
    };

    static bool canRemoveUser(const CacheUsagePtr & usage);

    FileCacheUtils::ShardedMap<UserID, CacheUserData> clients_map;
};

}
