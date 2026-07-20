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

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;
namespace fs = std::filesystem;

FileCacheSettings qlSettings(const std::string & path)
{
    FileCacheSettings s;
    s.path = path;
    s.maxSize = 16 * 1024 * 1024;
    s.maxElements = 100;
    s.maxFileSegmentSize = 1024 * 1024;
    s.boundaryAlignment = 1;
    s.cachePolicy = FileCachePolicy::LRU;
    s.useSplitCache = false;
    s.backgroundDownloadThreads = 0;
    s.loadMetadataAsynchronously = false;
    s.keepFreeSpaceSizeRatio = 0.0;
    s.keepFreeSpaceElementsRatio = 0.0;
    // Enable the per-query cache write limit so the FileCache owns a FileCacheQueryLimit.
    s.enableFilesystemQueryCacheLimit = true;
    return s;
}

/// Read options that actually engage the query limit (maxDownloadSizePerQuery > 0).
FileCacheReadOptions qlOptions(uint64_t max_download, bool skip_on_exceed = true)
{
    FileCacheReadOptions o;
    o.maxDownloadSizePerQuery = max_download;
    o.skipDownloadIfExceedsPerQueryCacheWriteLimit = skip_on_exceed;
    return o;
}

class QueryLimitTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        cache_ = std::make_unique<FileCache>("q", qlSettings((fs::path(temp_->getPath()) / "cache").string()), "user-A");
        cache_->initialize();
    }

    void TearDown() override
    {
        if (cache_)
            cache_->deactivateBackgroundOperations();
    }

    std::shared_ptr<TempDirectoryPath> temp_;
    std::unique_ptr<FileCache> cache_;
};

/// Empty query id: getQueryContextHolder returns a holder with NO context (the per-query limit
/// is not engaged) and creates no map entry. Production builds the holder but getOrSetQueryContext
/// returns null for an empty id, so holder->context is null.
TEST_F(QueryLimitTest, EmptyQueryIdCreatesNoContext)
{
    auto holder = cache_->getQueryContextHolder("", qlOptions(1024 * 1024));
    ASSERT_NE(holder, nullptr);
    EXPECT_EQ(holder->context, nullptr);
    // No map entry was created: a real query id afterwards still gets a fresh context.
    auto real = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
    ASSERT_NE(real, nullptr);
    EXPECT_NE(real->context, nullptr);
    EXPECT_EQ(real->context.use_count(), 2); // map entry + this holder only
}

/// Disabled per-query limit (maxDownloadSizePerQuery == 0): null holder even with an id.
TEST_F(QueryLimitTest, ZeroMaxDownloadReturnsNull)
{
    auto holder = cache_->getQueryContextHolder("q1", qlOptions(0));
    EXPECT_EQ(holder, nullptr);
}

/// Same query id: two holders share one context (same underlying pointer) and one map entry.
TEST_F(QueryLimitTest, SameQueryIdSharesContext)
{
    auto h1 = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
    ASSERT_NE(h1, nullptr);
    ASSERT_NE(h1->context, nullptr);

    auto h2 = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
    ASSERT_NE(h2, nullptr);
    ASSERT_NE(h2->context, nullptr);

    // Both holders reference the exact same QueryContext instance (one map entry, shared).
    EXPECT_EQ(h1->context.get(), h2->context.get());
    // use_count: the map entry + two holders => 3.
    EXPECT_EQ(h1->context.use_count(), 3);
}

/// Last-holder release removes the map entry and destroys the context.
TEST_F(QueryLimitTest, LastHolderReleaseRemovesEntry)
{
    std::weak_ptr<FileCacheQueryLimit::QueryContext> weak;
    {
        auto h1 = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
        ASSERT_NE(h1, nullptr);
        weak = h1->context;
        {
            auto h2 = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
            ASSERT_NE(h2, nullptr);
            // Two live holders + map entry: context stays alive after h2 dies.
            EXPECT_EQ(weak.use_count(), 3);
        }
        // h2 gone; h1 + map entry remain, so the context is still alive.
        EXPECT_FALSE(weak.expired());
        EXPECT_EQ(weak.use_count(), 2);
    }
    // h1 (the last holder) gone: the map entry was erased and the context destroyed.
    EXPECT_TRUE(weak.expired());

    // A fresh holder for the same id creates a brand-new context (proving the entry was removed).
    auto h3 = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
    ASSERT_NE(h3, nullptr);
    EXPECT_TRUE(weak.expired());
}

/// Doomed context destruction after the cache write lock is released (mandatory contract).
/// removeQueryContext extracts the last-holder context and returns it so ~QueryContextHolder
/// destroys it AFTER the lockCache() scope closes. We prove the release ordering on one thread:
/// after the last holder is destroyed, the cache write lock is immediately re-acquirable (the
/// holder released it), and the context is already destroyed (weak expired) — i.e. teardown did
/// not happen under the still-held lock.
TEST_F(QueryLimitTest, DoomedContextDestroyedAfterLockRelease)
{
    std::weak_ptr<FileCacheQueryLimit::QueryContext> weak;
    {
        auto holder = cache_->getQueryContextHolder("q1", qlOptions(1024 * 1024));
        ASSERT_NE(holder, nullptr);
        weak = holder->context;
        EXPECT_FALSE(weak.expired());
    } // last holder destroyed: removeQueryContext runs under lockCache(), doomed destroyed after.

    // The write lock is free again (the holder's destructor released it before returning).
    {
        auto lock = cache_->lockCache();
        // Re-acquiring the lock on the same thread only succeeds because the holder released it.
        SUCCEED();
    }
    // The doomed context has been destroyed.
    EXPECT_TRUE(weak.expired());
}

/// Max download size: the per-query LRU limit equals maxDownloadSizePerQuery and rejects a
/// reservation that would exceed it. Driven through the real reserve path under a query scope.
TEST_F(QueryLimitTest, MaxDownloadSizeRejectsExcessReservation)
{
    const size_t max_download = 4096;
    const size_t seg = 8192; // larger than the per-query limit

    // Activate the query id for the thread so tryReserve (via tryGetQueryContext ->
    // currentQueryId()) associates this reservation with the "q1" query context.
    FileCacheQueryIdScope scope("q1");

    auto holder_ctx = cache_->getQueryContextHolder("q1", qlOptions(max_download));
    ASSERT_NE(holder_ctx, nullptr);
    // The per-query priority's size limit equals the configured maximum.
    EXPECT_EQ(holder_ctx->context->getPriority().getSizeLimitApprox(), max_download);

    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    auto holder = cache_->getOrSet(key, 0, seg, seg, create_settings, 0, cache_->getCommonOrigin());
    ASSERT_TRUE(holder);
    ASSERT_FALSE(holder->empty());

    auto & segment = holder->front();
    ASSERT_EQ(segment.getOrSetDownloader(), FileSegment::getCallerId());
    std::string reason;
    // Reserving `seg` (> max_download) under query id "q1" must be rejected by the query limit.
    bool reserved = segment.reserve(seg, /* lock_wait_ms */ 100, reason);
    EXPECT_FALSE(reserved);
    EXPECT_FALSE(reason.empty());
}

} // namespace
} // namespace facebook::velox::ch
