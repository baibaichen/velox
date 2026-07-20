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
#include "velox/ch/Interpreters/FileCache/QueryLimit.h"

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"

#include "velox/common/testutil/TestValue.h"

namespace facebook::velox::ch
{

static bool isQueryInitialized()
{
    /// CH gates on CurrentThread having an initialized query context with a
    /// non-empty query id. In Velox the synchronous FileCacheInputStream entry
    /// point installs the id in FileCacheQueryIdScope; a non-empty current id is
    /// the equivalent condition (background workers have no scope).
    return !FileCacheQueryIdScope::currentQueryId().empty();
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext(const CacheStateGuard::Lock &)
{
    if (!isQueryInitialized())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto query_iter = query_map.find(std::string(FileCacheQueryIdScope::currentQueryId()));
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

FileCacheQueryLimit::QueryContextPtr
FileCacheQueryLimit::removeQueryContext(const std::string & query_id, QueryContextPtr & context, const CachePriorityGuard::WriteLock &)
{
    QueryContextPtr doomed;
    {
        std::lock_guard lock(query_map_mutex);

        auto query_iter = query_map.find(query_id);
        const bool owns_map_entry = query_iter != query_map.end() && query_iter->second == context;

        /// Drop this holder's own reference to the context under the lock, then
        /// decide. use_count() is not a synchronization primitive, so the
        /// decision must be made after every reference change is serialized by
        /// this mutex (which also guards getOrSetQueryContext). Deciding before
        /// dropping the reference (or dropping it outside the lock) is a TOCTOU.
        context.reset();

        if (owns_map_entry && query_iter->second.use_count() == 1)
        {
            /// The reference this holder held is gone and the map entry is now
            /// the sole owner, so this was the last holder. Extract the pointer
            /// instead of erasing in place so the QueryContext is destroyed by
            /// the caller after the cache write lock is released, not under it.
            doomed = std::move(query_iter->second);
            query_map.erase(query_iter);
        }
    }
    return doomed;
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const FileCacheReadOptions & options,
    const CachePriorityGuard::WriteLock &)
{
    if (query_id.empty())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            options.maxDownloadSizePerQuery,
            !options.skipDownloadIfExceedsPerQueryCacheWriteLimit);
    }

    return it->second;
}

FileCacheQueryLimit::QueryContext::QueryContext(
    size_t query_cache_size,
    bool recache_on_query_limit_exceeded_)
    : priority(LRUFileCachePriority(IFileCachePriority::QueueType::Query, query_cache_size, 0))
    , recache_on_query_limit_exceeded(recache_on_query_limit_exceeded_)
{
}

FileCacheQueryLimit::QueryContext::~QueryContext()
{
    /// TestValue injection point (inert unless a test arms it; release-elided).
    /// The doomed-context test arms this to reacquire the cache write lock, which
    /// only succeeds because the production QueryContextHolder destructor destroys
    /// the orphaned context here, AFTER releasing that lock (a non-reentrant lock
    /// would deadlock otherwise). This couples the test's assertion to the actual
    /// QueryContext destruction instead of a manually driven sequence.
    facebook::velox::common::testutil::TestValue::adjust(
        "facebook::velox::ch::FileCacheQueryLimit::QueryContext::~QueryContext", this);
}

void FileCacheQueryLimit::QueryContext::add(
    KeyMetadata & key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock)
{
    auto it = getPriority().add(key_metadata.shared_from_this(), offset, size, lock, /* state_lock */ nullptr);
    auto [_, inserted] = records.emplace(FileCacheKeyAndOffset{key_metadata.key, offset}, it);
    if (!inserted)
    {
        it->remove(lock);
        VELOX_FAIL(
            "Cannot add offset {} to query context under key {}, it already exists",
            offset,
            key_metadata.key.toString());
    }
}

void FileCacheQueryLimit::QueryContext::remove(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock & lock)
{
    auto record = records.find({key, offset});
    if (record == records.end())
        VELOX_FAIL("There is no {}:{} in query context", key.toString(), offset);

    record->second->remove(lock);
    records.erase({key, offset});
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock &)
{
    auto it = records.find({key, offset});
    if (it == records.end())
        return nullptr;
    return it->second;
}

FileCacheQueryLimit::QueryContextHolder::QueryContextHolder(
    const String & query_id_,
    FileCache * cache_,
    FileCacheQueryLimit * query_limit_,
    FileCacheQueryLimit::QueryContextPtr context_)
    : query_id(query_id_)
    , cache(cache_)
    , query_limit(query_limit_)
    , context(context_)
{
}

FileCacheQueryLimit::QueryContextHolder::~QueryContextHolder()
{
    /// The last-holder decision (and the drop of this holder's reference) must
    /// happen inside removeQueryContext under the cache write lock, not here:
    /// dropping the reference or deciding outside the lock races with revival
    /// via getOrSetQueryContext and can leak or orphan the entry. context is
    /// only set when the per-query download limit is enabled.
    if (context)
    {
        QueryContextPtr doomed;
        {
            auto lock = cache->lockCache();
            doomed = query_limit->removeQueryContext(query_id, context, lock);
        }
    }
}

} // namespace facebook::velox::ch
