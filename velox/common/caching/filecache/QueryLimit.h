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
#include "velox/common/caching/filecache/Guards.h"
#include "velox/common/caching/filecache/LRUFileCachePriority.h"

namespace facebook::velox::ch {
struct ReadSettings;
struct FilesystemCacheSettings;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    // TODO(query-context): CH 用 TLS，Velox 侧改显式上下文传递.
    QueryContextPtr tryGetQueryContext(const CacheStateGuard::Lock & lock);

    // TODO(query-context): CH 用 TLS，Velox 侧改显式上下文传递.
    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FilesystemCacheSettings & settings,
        const CachePriorityGuard::WriteLock &);

    // TODO(query-context): CH 用 TLS，Velox 侧改显式上下文传递.
    void removeQueryContext(const std::string & query_id, const CachePriorityGuard::WriteLock &);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recacheOnQueryLimitExceeded; }

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
        using Records = std::unordered_map<FileCacheKeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        Records records;
        LRUFileCachePriority priority;
        const bool recacheOnQueryLimitExceeded;
    };

    struct QueryContextHolder : private boost::noncopyable
    {
        // TODO(query-context): CH 用 TLS，Velox 侧改显式上下文传递.
        QueryContextHolder(const std::string & query_id_, FileCache * cache_, FileCacheQueryLimit * query_limit_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        std::string queryId;
        FileCache * cache;
        FileCacheQueryLimit * queryLimit;
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;

private:
    using QueryContextMap = std::unordered_map<std::string, QueryContextPtr>;
    QueryContextMap queryMap;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

} // namespace facebook::velox::ch
