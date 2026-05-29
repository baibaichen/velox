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
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FilesystemCacheSettings.h"
#include "velox/common/caching/filecache/Metadata.h"
#include "velox/common/caching/filecache/QueryLimit.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch {

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext(
    const CacheStateGuard::Lock&) {
  // TODO(query-context): CH reads TLS here; the Velox header has no
  // explicit query id/context parameter for this method yet.
  return nullptr;
}

void FileCacheQueryLimit::removeQueryContext(
    const std::string& query_id,
    const CachePriorityGuard::WriteLock&) {
  auto query_iter = queryMap.find(query_id);
  if (query_iter == queryMap.end()) {
    VELOX_FAIL(
        "Attempt to release query context that does not exist (query_id: {})",
        query_id);
  }
  queryMap.erase(query_iter);
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string& query_id,
    const FilesystemCacheSettings& settings,
    const CachePriorityGuard::WriteLock&) {
  if (query_id.empty()) {
    return nullptr;
  }

  auto [it, inserted] = queryMap.emplace(query_id, nullptr);
  if (inserted) {
    it->second = std::make_shared<QueryContext>(
        settings.maxDownloadSizePerQuery,
        !settings.skipDownloadIfExceedsPerQueryCacheWriteLimit);
  }

  return it->second;
}

FileCacheQueryLimit::QueryContext::QueryContext(
    size_t query_cache_size,
    bool recache_on_query_limit_exceeded_)
    : priority(LRUFileCachePriority(query_cache_size, 0)),
      recacheOnQueryLimitExceeded(recache_on_query_limit_exceeded_) {}

void FileCacheQueryLimit::QueryContext::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock& lock) {
  auto it = getPriority().add(
      key_metadata, offset, size, lock, /* state_lock */ nullptr);
  auto [_, inserted] =
      records.emplace(FileCacheKeyAndOffset{key_metadata->key, offset}, it);
  if (!inserted) {
    it->remove(lock);
    VELOX_FAIL(
        "Cannot add offset {} to query context under key {}, it already exists",
        offset,
        key_metadata->key);
  }
}

void FileCacheQueryLimit::QueryContext::remove(
    const Key& key,
    size_t offset,
    const CachePriorityGuard::WriteLock& lock) {
  auto record = records.find({key, offset});
  if (record == records.end()) {
    VELOX_FAIL("There is no {}:{} in query context", key, offset);
  }

  record->second->remove(lock);
  records.erase({key, offset});
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key& key,
    size_t offset,
    const CachePriorityGuard::WriteLock&) {
  auto it = records.find({key, offset});
  if (it == records.end()) {
    return nullptr;
  }
  return it->second;
}

FileCacheQueryLimit::QueryContextHolder::QueryContextHolder(
    const std::string& query_id_,
    FileCache* cache_,
    FileCacheQueryLimit* query_limit_,
    FileCacheQueryLimit::QueryContextPtr context_)
    : queryId(query_id_),
      cache(cache_),
      queryLimit(query_limit_),
      context(context_) {}

FileCacheQueryLimit::QueryContextHolder::~QueryContextHolder() {
  /// If only the query_map and the current holder hold the context_query,
  /// the query has been completed and the query_context is released.
  if (context && context.use_count() == 2) {
    auto lock = cache->lockCache();
    queryLimit->removeQueryContext(queryId, lock);
  }
}

} // namespace facebook::velox::ch
