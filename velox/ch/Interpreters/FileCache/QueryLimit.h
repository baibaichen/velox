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
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/LRUFileCachePriority.h"

#include <folly/container/F14Map.h>

#include <memory>
#include <mutex>
#include <string>

namespace facebook::velox::ch
{

class FileCache;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    QueryContextPtr tryGetQueryContext(const CacheStateGuard::Lock & lock);

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FileCacheReadOptions & settings,
        const CachePriorityGuard::WriteLock &);

    /// Releases this holder's reference to the query context and, when it was the last holder,
    /// removes the map entry and returns the now-orphaned context so the caller can destroy it
    /// after releasing the cache write lock (see ~QueryContextHolder). Returns nullptr when the
    /// context is still owned by another live holder.
    QueryContextPtr removeQueryContext(const std::string & query_id, QueryContextPtr & context, const CachePriorityGuard::WriteLock &);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;

        /// Copy/move are disabled (CH `boost::noncopyable`).
        QueryContext(const QueryContext &) = delete;
        QueryContext & operator=(const QueryContext &) = delete;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recache_on_query_limit_exceeded; }

        Priority::IteratorPtr tryGet(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::WriteLock &);

        void add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const CachePriorityGuard::WriteLock &);

        void remove(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::WriteLock &);

    private:
        using Records = folly::F14FastMap<FileCacheKeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        Records records;
        LRUFileCachePriority priority;
        const bool recache_on_query_limit_exceeded;
    };

    struct QueryContextHolder
    {
        QueryContextHolder(const String & query_id_, FileCache * cache_, FileCacheQueryLimit * query_limit_, QueryContextPtr context_);

        QueryContextHolder() = default;

        /// Non-copyable (CH `boost::noncopyable`).
        QueryContextHolder(const QueryContextHolder &) = delete;
        QueryContextHolder & operator=(const QueryContextHolder &) = delete;

        ~QueryContextHolder();

        String query_id;
        FileCache * cache{};
        FileCacheQueryLimit * query_limit{};
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;

private:
    using QueryContextMap = folly::F14FastMap<String, QueryContextPtr>;
    QueryContextMap query_map;
    /// query_map is reached under two different cache locks: reads (tryGetQueryContext) run under
    /// CacheStateGuard while writes (getOrSetQueryContext/removeQueryContext) run under
    /// CachePriorityGuard, so neither cache lock serializes access to the map by itself. This
    /// dedicated leaf mutex is the single lock that actually guards query_map.
    mutable std::mutex query_map_mutex;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
