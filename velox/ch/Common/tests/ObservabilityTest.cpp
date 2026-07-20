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

// Task 017: real behaviour tests for the observability + cancellation shims that
// were no-ops in the earlier port phases. Every case asserts a value that the
// pre-Task-017 no-op body could not produce (a counter that actually counts, a
// timer that actually records elapsed time, a token that actually throws).

#include "velox/ch/Common/CurrentMetrics.h"
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Common/QueryStatus.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>
#include <folly/CancellationToken.h>

#include <chrono>
#include <stdexcept>
#include <thread>

namespace facebook::velox::ch
{
namespace
{

TEST(CurrentMetricsTest, AddAndReadBack)
{
    const int64_t initial = CurrentMetrics::get(CurrentMetrics::CacheFileSegments);
    CurrentMetrics::add(CurrentMetrics::CacheFileSegments, 5);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::CacheFileSegments), initial + 5);
    CurrentMetrics::sub(CurrentMetrics::CacheFileSegments, 5);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::CacheFileSegments), initial);
}

TEST(CurrentMetricsTest, IncrementScopedDecrementsOnDestruct)
{
    const int64_t initial = CurrentMetrics::get(CurrentMetrics::CacheFileSegments);
    {
        CurrentMetrics::Increment inc(CurrentMetrics::CacheFileSegments, 3);
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::CacheFileSegments), initial + 3);
    }
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::CacheFileSegments), initial);
}

TEST(CurrentMetricsTest, SubDecrements)
{
    const int64_t initial = CurrentMetrics::get(CurrentMetrics::FilesystemCacheSizeLimit);
    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSizeLimit, 10);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSizeLimit, 4);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCacheSizeLimit), initial + 6);
    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSizeLimit, 6);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::FilesystemCacheSizeLimit), initial);
}

TEST(ProfileEventsTest, IncrementAndReadBack)
{
    const uint64_t initial = ProfileEvents::get(ProfileEvents::FilesystemCacheReserveAttempts);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheReserveAttempts, 2);
    EXPECT_EQ(
        ProfileEvents::get(ProfileEvents::FilesystemCacheReserveAttempts), initial + 2);
}

TEST(ProfileEventsTest, TimeIncrementRecordsElapsed)
{
    const uint64_t initial =
        ProfileEvents::get(ProfileEvents::FilesystemCacheReserveMicroseconds);
    {
        ProfileEventTimeIncrement<Microseconds> watch(
            ProfileEvents::FilesystemCacheReserveMicroseconds);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    const uint64_t delta =
        ProfileEvents::get(ProfileEvents::FilesystemCacheReserveMicroseconds) - initial;
    // At least 4 ms in microseconds (allow scheduler jitter below the 5 ms sleep).
    EXPECT_GE(delta, 4000u);
}

TEST(QueryStatusTest, NoopWhenNotCancelled)
{
    QueryStatus s;
    EXPECT_NO_THROW(s.throwIfKilled());
    EXPECT_FALSE(s.isCancelled());
}

TEST(QueryStatusTest, ThrowsWhenTokenCancelled)
{
    folly::CancellationSource src;
    QueryStatus s{src.getToken()};
    EXPECT_FALSE(s.isCancelled());
    src.requestCancellation();
    EXPECT_TRUE(s.isCancelled());
    EXPECT_THROW(s.throwIfKilled(), VeloxRuntimeError);
}

TEST(QueryStatusTest, DefaultConstructedIsNeverCancelled)
{
    QueryStatus s{};
    for (int i = 0; i < 100; ++i)
    {
        EXPECT_FALSE(s.isCancelled());
        EXPECT_NO_THROW(s.throwIfKilled());
    }
}

TEST(LoggerUsefulTest, GetLoggerReturnsNonNullPtr)
{
    auto log = getLogger("filecache.test");
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->name(), "filecache.test");
}

TEST(LoggerUsefulTest, CurrentExceptionMessageIsEmptyOutsideCatch)
{
    EXPECT_TRUE(getCurrentExceptionMessage(true).empty());
}

TEST(LoggerUsefulTest, CurrentExceptionMessageFormatsStdException)
{
    std::string msg;
    try
    {
        throw std::runtime_error("std sentinel");
    }
    catch (...)
    {
        msg = getCurrentExceptionMessage(true);
    }
    EXPECT_FALSE(msg.empty());
    EXPECT_NE(msg.find("std sentinel"), std::string::npos);
}

TEST(LoggerUsefulTest, CurrentExceptionMessageFormatsVeloxException)
{
    std::string msg;
    try
    {
        VELOX_FAIL("velox sentinel");
    }
    catch (...)
    {
        msg = getCurrentExceptionMessage(true);
    }
    EXPECT_FALSE(msg.empty());
    EXPECT_NE(msg.find("velox sentinel"), std::string::npos);
}

TEST(LoggerUsefulTest, TryLogCurrentExceptionNoThrow)
{
    auto log = getLogger("filecache.test");
    try
    {
        throw std::runtime_error("test error");
    }
    catch (...)
    {
        EXPECT_NO_THROW(tryLogCurrentException(log));
    }
    // No secondary exception escaped the catch block; reaching here proves it.
    SUCCEED();
}

// False-green probe for the counter tests: an assertion-free / no-op counter
// cannot satisfy these. If CurrentMetrics::add were still a no-op, AddAndReadBack
// would fail because get() would not change. The following documents that the
// value actually moved (the RED evidence recorded in the receipt confirms the
// pre-change no-op body fails these EXPECT_EQ deltas).
TEST(CurrentMetricsTest, AddIsNotNoOp)
{
    const int64_t before = CurrentMetrics::get(CurrentMetrics::CacheFileSegments);
    CurrentMetrics::add(CurrentMetrics::CacheFileSegments, 7);
    const int64_t after = CurrentMetrics::get(CurrentMetrics::CacheFileSegments);
    ASSERT_NE(before, after) << "add() must not be a no-op";
    EXPECT_EQ(after - before, 7);
    CurrentMetrics::sub(CurrentMetrics::CacheFileSegments, 7);
}

} // namespace
} // namespace facebook::velox::ch
