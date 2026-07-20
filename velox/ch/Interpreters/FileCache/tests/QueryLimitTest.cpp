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
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/LRUFileCachePriority.h"

#include <gtest/gtest.h>

#include <atomic>
#include <string>

namespace facebook::velox::ch
{
namespace
{

FileCacheReadOptions makeOptions(size_t max_download_size_per_query)
{
    FileCacheReadOptions options;
    options.maxDownloadSizePerQuery = max_download_size_per_query;
    return options;
}

// The query-limit map/holder contract is exercised directly against the
// production FileCacheQueryLimit (matching ClickHouse's
// gtest_filecache.cpp QueryLimit* cases): getOrSetQueryContext creates/reuses a
// context under the write lock, tryGetQueryContext resolves the current query
// scope, and removeQueryContext performs the TOCTOU-safe last-holder release.

// -- empty query id: no context, no map entry ------------------------------

TEST(QueryLimitTest, EmptyQueryIdReturnsNullAndCreatesNoEntry)
{
    CachePriorityGuard cache_guard;
    CacheStateGuard state_guard;
    FileCacheQueryLimit query_limit;
    const auto options = makeOptions(1024);

    auto context = query_limit.getOrSetQueryContext("", options, cache_guard.writeLock());
    EXPECT_EQ(context, nullptr);

    // No map entry was created, so even under a matching query scope nothing is
    // discoverable.
    FileCacheQueryIdScope scope("");
    EXPECT_EQ(query_limit.tryGetQueryContext(state_guard.lock()), nullptr);
}

// -- same query id: two holders share one context and one map entry ---------

TEST(QueryLimitTest, SameQueryIdSharesOneContextAndOneEntry)
{
    CachePriorityGuard cache_guard;
    CacheStateGuard state_guard;
    FileCacheQueryLimit query_limit;
    const auto options = makeOptions(1024);
    const std::string query_id = "q-shared";

    auto context1 = query_limit.getOrSetQueryContext(query_id, options, cache_guard.writeLock());
    ASSERT_NE(context1, nullptr);
    // Referenced by the map and by context1.
    EXPECT_EQ(context1.use_count(), 2);

    auto context2 = query_limit.getOrSetQueryContext(query_id, options, cache_guard.writeLock());
    // The same context is returned (one map entry), now referenced three times.
    EXPECT_EQ(context1.get(), context2.get());
    EXPECT_EQ(context1.use_count(), 3);

    // Discoverable under the query scope.
    FileCacheQueryIdScope scope(query_id);
    EXPECT_EQ(query_limit.tryGetQueryContext(state_guard.lock()).get(), context1.get());
}

// -- last holder release removes the map entry -----------------------------

TEST(QueryLimitTest, LastHolderReleaseRemovesEntry)
{
    CachePriorityGuard cache_guard;
    CacheStateGuard state_guard;
    FileCacheQueryLimit query_limit;
    const auto options = makeOptions(1024);
    const std::string query_id = "q-last";

    auto holder1 = query_limit.getOrSetQueryContext(query_id, options, cache_guard.writeLock());
    auto holder2 = query_limit.getOrSetQueryContext(query_id, options, cache_guard.writeLock());
    ASSERT_EQ(holder1.get(), holder2.get());

    // holder1 releases while holder2 is still alive: entry kept, nothing doomed.
    FileCacheQueryLimit::QueryContextPtr doomed1;
    ASSERT_NO_THROW(doomed1 = query_limit.removeQueryContext(query_id, holder1, cache_guard.writeLock()));
    EXPECT_EQ(doomed1, nullptr);
    holder1.reset();

    {
        FileCacheQueryIdScope scope(query_id);
        EXPECT_EQ(query_limit.tryGetQueryContext(state_guard.lock()).get(), holder2.get());
    }

    // holder2 is the last holder: the entry is removed and the orphaned context
    // handed back (sole owner) for destruction outside the cache write lock.
    const auto * holder2_raw = holder2.get();
    FileCacheQueryLimit::QueryContextPtr doomed2;
    ASSERT_NO_THROW(doomed2 = query_limit.removeQueryContext(query_id, holder2, cache_guard.writeLock()));
    ASSERT_EQ(doomed2.get(), holder2_raw);
    EXPECT_EQ(doomed2.use_count(), 1);
    holder2.reset();

    FileCacheQueryIdScope scope(query_id);
    EXPECT_EQ(query_limit.tryGetQueryContext(state_guard.lock()), nullptr);
}

// -- doomed context destruction happens after the write lock is released ----
//
// This contract requires the production FileCache::QueryContextHolder destructor
// (which extracts the orphaned context under the cache write lock but destroys it
// only after releasing that lock) and therefore a real FileCache. It is covered
// by FileCacheTest.DoomedQueryContextDestroyedAfterWriteLockReleased, which arms
// a TestValue seam in ~QueryContext to observe the exact destruction moment.

// -- max download size: the query LRU limit equals the configured maximum and
//    rejects an over-limit reservation ------------------------------------

TEST(QueryLimitTest, MaxDownloadSizeBoundsQueryPriority)
{
    CachePriorityGuard cache_guard;
    CacheStateGuard state_guard;
    FileCacheQueryLimit query_limit;
    const size_t max_download = 4096;
    const auto options = makeOptions(max_download);

    auto context = query_limit.getOrSetQueryContext("q-limit", options, cache_guard.writeLock());
    ASSERT_NE(context, nullptr);

    // The per-query LRU priority is bounded by max_download_size_per_query.
    EXPECT_EQ(context->getPriority().getSizeLimitApprox(), max_download);

    // A reservation that fits is accepted; one that exceeds the per-query limit is
    // rejected by canFit (this is what doTryReserve consults for the query queue).
    auto state_lock = state_guard.lock();
    EXPECT_TRUE(context->getPriority().canFit(max_download, /*elements*/ 1, state_lock));
    EXPECT_FALSE(context->getPriority().canFit(max_download + 1, /*elements*/ 1, state_lock));
}

} // namespace
} // namespace facebook::velox::ch
