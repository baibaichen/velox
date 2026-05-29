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
#include "velox/common/caching/filecache/IFileCachePriority.h"
#include "velox/common/caching/filecache/FileCacheOriginInfo.h"
#include <boost/noncopyable.hpp>

namespace facebook::velox::ch
{

/// This mutex guard is needed only for correctness of `OvercommitFileCachePriority::check` method,
/// but not for cache correctness in general,
/// because it makes sure per client's counters
/// (`totalSize` and `totalElements` in `CacheUsage` per client state)
/// are updated atomically with main cache state counters.
/// The per-client `totalSize` and `totalElements` counters are used for:
/// 1. to decide from whom to evict according to overcommit policy
/// 2. for system tables
/// In both cases some temporary divergence from actual cache counters is fine,
/// but we now have no divergence as atomicity is needed for `OvercommitFileCachePriority::check`'s success.
struct CacheUsageStatGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex> { explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {} };
    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/// A caching eviction strategy, which allows to evict more from users which use the cache more.
/// From each user cache is evicted according to LRU/SLRU eviction policies.
struct CacheUsage
{
    CacheUsage(const FileCacheOriginInfo & originInfo_, FileCachePriorityPtr priority_);

    const FileCacheOriginInfo originInfo;
    /// A user priority, contains only entries which belong to `user`
    /// by corresponding eviction strategy priority.
    const FileCachePriorityPtr priority{};

    std::shared_ptr<CacheUsageStatGuard> guard;

    void update(int64_t size, int64_t elements, const CacheUsageStatGuard::Lock & lock);

    std::atomic<uint64_t> totalSize = 0;
    std::atomic<uint64_t> totalElements = 0;

    bool operator <(const CacheUsage & other) const;
    bool operator ==(const CacheUsage & other) const;

    bool lessWithAssumption(const CacheUsage & other, size_t released_size_assumption, size_t other_released_size_assumption) const;

};
using CacheUsagePtr = std::shared_ptr<CacheUsage>;

}
