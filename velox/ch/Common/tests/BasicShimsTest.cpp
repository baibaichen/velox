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
#include "velox/ch/Common/ClickHouseAliases.h"
#include "velox/ch/Common/ClickHouseAssert.h"
#include "velox/ch/Common/CurrentMetrics.h"
#include "velox/ch/Common/FileCacheBoundedQueue.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheFilesystem.h"
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Common/SharedMutex.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <future>
#include <iterator>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;
using namespace std::chrono_literals;

TEST(ClickHouseAliasesTest, PrimitiveTypes)
{
    static_assert(std::is_same_v<String, std::string>);
    static_assert(std::is_same_v<UInt8, uint8_t>);
    static_assert(std::is_same_v<UInt64, uint64_t>);
    static_assert(std::is_same_v<Int64, int64_t>);
}

TEST(LoggerUsefulTest, ArgumentsAreNotEvaluated)
{
    int evaluated = 0;
    LOG_TEST(getLogger("test"), "value {}", ++evaluated);
    EXPECT_EQ(evaluated, 0);
}

TEST(LoggerUsefulTest, AllLogMacrosDoNotEvaluateArguments)
{
    int evaluated = 0;
    auto logger = getLogger("test");
    LOG_TEST(logger, "value {}", ++evaluated);
    LOG_TRACE(logger, "value {}", ++evaluated);
    LOG_DEBUG(logger, "value {}", ++evaluated);
    LOG_INFO(logger, "value {}", ++evaluated);
    LOG_WARNING(logger, "value {}", ++evaluated);
    LOG_ERROR(logger, "value {}", ++evaluated);
    EXPECT_EQ(evaluated, 0);
}

TEST(LoggerUsefulTest, GetLoggerReturnsNonNullWithNameIdentity)
{
    auto logger = getLogger("FileCache(test)");
    ASSERT_NE(logger, nullptr);
    EXPECT_EQ(logger->name(), "FileCache(test)");
}

TEST(LoggerUsefulTest, CurrentExceptionMessageRemainsEmptyFirstPhase)
{
    try
    {
        throw std::runtime_error("boom");
    }
    catch (...)
    {
        EXPECT_TRUE(getCurrentExceptionMessage().empty());
        EXPECT_TRUE(getCurrentExceptionMessage(/*withStackTrace=*/true).empty());
    }
}

TEST(LoggerUsefulTest, TryLogCurrentExceptionIsNoOpFirstPhase)
{
    try
    {
        throw std::runtime_error("boom");
    }
    catch (...)
    {
        EXPECT_NO_THROW(tryLogCurrentException(__PRETTY_FUNCTION__));
        EXPECT_NO_THROW(tryLogCurrentException(getLogger("test"), "context"));
    }
}

TEST(FileCacheExceptionTest, ThrowsVeloxRuntimeError)
{
    EXPECT_THROW(
        throwFileCacheException("invalid value {}", 42),
        VeloxRuntimeError);
}

TEST(FileCacheExceptionTest, NeverThrowsVeloxUserError)
{
    try
    {
        throwFileCacheException("invalid value {}", 42);
        FAIL() << "Expected VeloxRuntimeError";
    }
    catch (const VeloxUserError &)
    {
        FAIL() << "throwFileCacheException must never throw VeloxUserError";
    }
    catch (const VeloxRuntimeError & exception)
    {
        EXPECT_FALSE(exception.isUserError());
        EXPECT_EQ(exception.errorCode(), "INVALID_STATE");
    }
}

TEST(SharedMutexTest, SupportsExclusiveAndSharedLocks)
{
    SharedMutex mutex;
    {
        std::unique_lock lock(mutex);
    }
    {
        std::shared_lock lock(mutex);
    }
}

TEST(FileCacheFilesystemTest, LocalFilesystemAlias)
{
    auto directory = TempDirectoryPath::create();
    const auto nested = directory->getPath() + "/a/b";
    EXPECT_TRUE(fs::create_directories(nested));
    EXPECT_TRUE(fs::exists(nested));
    EXPECT_GT(fs::remove_all(directory->getPath() + "/a"), 0);
    EXPECT_FALSE(fs::exists(nested));
}

TEST(FileCacheFilesystemTest, FilesystemErrorKeepsContext)
{
    const fs::filesystem_error error(
        "read failed",
        fs::path("/cache/file"),
        std::make_error_code(std::errc::io_error));

    try
    {
        throwFileCacheExceptionFromFilesystemError(error, "loading cache");
        FAIL() << "Expected VeloxRuntimeError";
    }
    catch (const VeloxUserError &)
    {
        FAIL()
            << "Filesystem failures must never throw VeloxUserError";
    }
    catch (const VeloxRuntimeError & exception)
    {
        const std::string message(exception.what());
        EXPECT_NE(message.find("loading cache"), std::string::npos);
        EXPECT_NE(message.find("/cache/file"), std::string::npos);
        EXPECT_NE(
            message.find(std::to_string(error.code().value())),
            std::string::npos);
        EXPECT_NE(message.find(error.code().message()), std::string::npos);
        EXPECT_FALSE(exception.isUserError());
    }
}

TEST(ClickHouseAssertTest, DebugDefaultDiagnosticIncludesExpressionText)
{
    EXPECT_DEATH(chassert(1 == 2), "1 == 2");
}

TEST(ClickHouseAssertTest, DebugCustomDiagnosticIncluded)
{
    EXPECT_DEATH(
        chassert(1 == 2, "custom diagnostic message"),
        "custom diagnostic message");
}

TEST(ClickHouseAssertTest, DebugTrueExpressionEvaluatedExactlyOnce)
{
    int evaluated = 0;
    chassert(++evaluated == 1);
    EXPECT_EQ(evaluated, 1);
}

TEST(FileCacheBoundedQueueTest, CapacityZeroTryPushFails)
{
    FileCacheBoundedQueue<int> queue(0);
    EXPECT_FALSE(queue.tryPush(1));
}

TEST(FileCacheBoundedQueueTest, FinishDrainsQueuedValues)
{
    FileCacheBoundedQueue<int> queue(2);
    ASSERT_TRUE(queue.push(1));
    ASSERT_TRUE(queue.push(2));
    queue.finish();

    int value = 0;
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 1);
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 2);
    EXPECT_FALSE(queue.pop(value));
    EXPECT_FALSE(queue.push(3));
}

TEST(FileCacheBoundedQueueTest, FinishReleasesBlockedConsumer)
{
    FileCacheBoundedQueue<int> queue(1);
    std::promise<void> started;
    auto startedFuture = started.get_future();
    auto result = std::async(std::launch::async, [&]
    {
        started.set_value();
        int value = 0;
        return queue.pop(value);
    });

    startedFuture.get();
    queue.finish();
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, FinishReleasesBlockedProducer)
{
    FileCacheBoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    std::promise<void> started;
    auto startedFuture = started.get_future();
    auto result = std::async(std::launch::async, [&]
    {
        started.set_value();
        return queue.push(2);
    });

    startedFuture.get();
    queue.finish();
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, Task012CallShapesCompile)
{
    FileCacheBoundedQueue<int> queue(2);
    int batch = 5;
    ASSERT_TRUE(queue.tryPush(batch, 10));

    int poppedBatch = 0;
    ASSERT_TRUE(queue.tryPop(poppedBatch));
    EXPECT_EQ(poppedBatch, batch);
}

TEST(FileCacheBoundedQueueTest, NonBlockingEmptyTryPopReturnsImmediately)
{
    FileCacheBoundedQueue<int> queue(1);
    int value = 0;
    EXPECT_FALSE(queue.tryPop(value));
}

TEST(FileCacheBoundedQueueTest, TimedTryPushRemainsPendingWhileFullAndSucceedsAfterPop)
{
    FileCacheBoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    auto result = std::async(std::launch::async, [&]
    {
        return queue.tryPush(2, 5000);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);

    int value = 0;
    ASSERT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 1);

    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_TRUE(result.get());
}

TEST(FileCacheBoundedQueueTest, TimedTryPushOnFullQueueTimesOutRatherThanReturningImmediately)
{
    FileCacheBoundedQueue<int> queue(1);
    ASSERT_TRUE(queue.push(1));

    auto result = std::async(std::launch::async, [&]
    {
        return queue.tryPush(2, 200);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, TimedTryPopWakesOnPush)
{
    FileCacheBoundedQueue<int> queue(1);

    auto result = std::async(std::launch::async, [&]
    {
        int value = 0;
        const bool popped = queue.tryPop(value, 5000);
        return std::make_pair(popped, value);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    ASSERT_TRUE(queue.push(42));
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);

    const auto [popped, value] = result.get();
    EXPECT_TRUE(popped);
    EXPECT_EQ(value, 42);
}

TEST(FileCacheBoundedQueueTest, TimedTryPopWakesOnFinish)
{
    FileCacheBoundedQueue<int> queue(1);

    auto result = std::async(std::launch::async, [&]
    {
        int value = 0;
        return queue.tryPop(value, 5000);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    queue.finish();
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, CapacityZeroBlockedProducerReleasedByFinish)
{
    FileCacheBoundedQueue<int> queue(0);
    EXPECT_FALSE(queue.tryPush(1));

    auto result = std::async(std::launch::async, [&]
    {
        return queue.push(2);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    queue.finish();
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, CapacityZeroTimedProducerReleasedByFinish)
{
    FileCacheBoundedQueue<int> queue(0);

    auto result = std::async(std::launch::async, [&]
    {
        return queue.tryPush(2, 5000);
    });

    EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
    queue.finish();
    ASSERT_EQ(result.wait_for(1s), std::future_status::ready);
    EXPECT_FALSE(result.get());
}

TEST(FileCacheBoundedQueueTest, AllPushFormsRejectAfterFinish)
{
    FileCacheBoundedQueue<int> queue(2);
    queue.finish();

    int lvalue = 1;
    EXPECT_FALSE(queue.push(1));
    EXPECT_FALSE(queue.tryPush(lvalue));
    EXPECT_FALSE(queue.tryPush(2));
    EXPECT_FALSE(queue.tryPush(lvalue, 10));
    EXPECT_FALSE(queue.tryPush(3, 10));
}

namespace
{

// Move assignment is noexcept, so the queue must move rather than copy.
struct NoexceptMoveAssignable
{
    int value = 0;
    bool movedInto = false;

    NoexceptMoveAssignable() = default;
    explicit NoexceptMoveAssignable(int initialValue)
        : value(initialValue)
    {
    }

    NoexceptMoveAssignable(const NoexceptMoveAssignable &) = default;
    NoexceptMoveAssignable(NoexceptMoveAssignable &&) noexcept = default;
    NoexceptMoveAssignable & operator=(const NoexceptMoveAssignable &) = default;

    NoexceptMoveAssignable & operator=(NoexceptMoveAssignable && other) noexcept
    {
        value = other.value;
        movedInto = true;
        return *this;
    }
};

static_assert(std::is_nothrow_move_assignable_v<NoexceptMoveAssignable>);

// Move assignment is not noexcept, so the queue must copy. Copy assignment
// can be made to throw to prove a failed copy leaves the source recoverable.
struct ThrowingMoveAssignable
{
    int value = 0;
    bool copyAssignCalled = false;
    static inline bool throwOnCopy = false;

    ThrowingMoveAssignable() = default;
    explicit ThrowingMoveAssignable(int initialValue)
        : value(initialValue)
    {
    }

    ThrowingMoveAssignable(const ThrowingMoveAssignable &) = default;
    ThrowingMoveAssignable(ThrowingMoveAssignable &&) = default;

    ThrowingMoveAssignable & operator=(const ThrowingMoveAssignable & other)
    {
        if (throwOnCopy)
            throw std::runtime_error("copy assignment failed");
        value = other.value;
        copyAssignCalled = true;
        return *this;
    }

    ThrowingMoveAssignable & operator=(ThrowingMoveAssignable && other)
    {
        value = other.value;
        return *this;
    }
};

static_assert(!std::is_nothrow_move_assignable_v<ThrowingMoveAssignable>);
static_assert(std::is_copy_assignable_v<ThrowingMoveAssignable>);

}

TEST(FileCacheBoundedQueueTest, NoexceptMoveAssignableTypeUsesMove)
{
    FileCacheBoundedQueue<NoexceptMoveAssignable> queue(1);
    ASSERT_TRUE(queue.push(NoexceptMoveAssignable(7)));

    NoexceptMoveAssignable out;
    ASSERT_TRUE(queue.pop(out));
    EXPECT_TRUE(out.movedInto);
    EXPECT_EQ(out.value, 7);
}

TEST(FileCacheBoundedQueueTest, ThrowingMoveAssignableTypeUsesCopy)
{
    ThrowingMoveAssignable::throwOnCopy = false;
    FileCacheBoundedQueue<ThrowingMoveAssignable> queue(1);
    ASSERT_TRUE(queue.push(ThrowingMoveAssignable(9)));

    ThrowingMoveAssignable out;
    ASSERT_TRUE(queue.pop(out));
    EXPECT_TRUE(out.copyAssignCalled);
    EXPECT_EQ(out.value, 9);
}

TEST(FileCacheBoundedQueueTest, ThrowingCopyLeavesElementQueuedAndRecoverable)
{
    ThrowingMoveAssignable::throwOnCopy = true;
    FileCacheBoundedQueue<ThrowingMoveAssignable> queue(1);
    ASSERT_TRUE(queue.push(ThrowingMoveAssignable(11)));

    ThrowingMoveAssignable out;
    EXPECT_THROW(queue.pop(out), std::runtime_error);

    ThrowingMoveAssignable::throwOnCopy = false;
    ASSERT_TRUE(queue.pop(out));
    EXPECT_EQ(out.value, 11);
}

namespace
{

// Tracks whether it was moved-from and how many times it was copied, so
// tests can prove a failed tryPush leaves the caller's argument untouched.
// Mirrors CH's ConcurrentBoundedQueue::emplaceImpl(Args &&...), which only
// forwards into the deque after the wait and is_finished checks succeed, so
// a failed/full push must not move-from or copy the caller's value at all.
struct MoveTrackingProbe
{
    int value = 0;
    bool movedFrom = false;
    static inline int copyCount = 0;

    MoveTrackingProbe() = default;
    explicit MoveTrackingProbe(int initialValue)
        : value(initialValue)
    {
    }

    MoveTrackingProbe(const MoveTrackingProbe & other)
        : value(other.value)
    {
        ++copyCount;
    }

    MoveTrackingProbe(MoveTrackingProbe && other) noexcept
        : value(other.value)
    {
        other.movedFrom = true;
    }

    MoveTrackingProbe & operator=(const MoveTrackingProbe &) = default;
    MoveTrackingProbe & operator=(MoveTrackingProbe &&) noexcept = default;
};

}

TEST(FileCacheBoundedQueueTest, FailedFullTryPushMoveDoesNotConsumeCallerValue)
{
    FileCacheBoundedQueue<MoveTrackingProbe> queue(1);
    ASSERT_TRUE(queue.push(MoveTrackingProbe(1)));

    MoveTrackingProbe value(42);
    EXPECT_FALSE(queue.tryPush(std::move(value), 0));
    EXPECT_FALSE(value.movedFrom);
    EXPECT_EQ(value.value, 42);
}

TEST(FileCacheBoundedQueueTest, FailedFullTryPushConstRefDoesNotCopyCallerValue)
{
    FileCacheBoundedQueue<MoveTrackingProbe> queue(1);
    ASSERT_TRUE(queue.push(MoveTrackingProbe(1)));

    MoveTrackingProbe::copyCount = 0;
    const MoveTrackingProbe value(42);
    EXPECT_FALSE(queue.tryPush(value, 0));
    EXPECT_EQ(MoveTrackingProbe::copyCount, 0);
}

// Compile-coverage for the complete required `ProfileEvents::Event` name
// surface: every pre-existing name plus the 34 approved B1 names (31
// referenced by CH src/Interpreters/FileCache/* plus the 3 OpenedFileCache
// names required by the Task-013 dependency mapping). Deleting any one name
// from ProfileEvents.h makes this array fail to compile, not just fail a
// runtime check.
TEST(ProfileEventsCoverageTest, AllRequiredEventNamesCompile)
{
    constexpr ProfileEvents::Event kRequiredEvents[] = {
        // Pre-existing names.
        ProfileEvents::FilesystemCacheGetOrSetMicroseconds,
        ProfileEvents::FilesystemCacheGetMicroseconds,
        ProfileEvents::FilesystemCacheReserveAttempts,
        ProfileEvents::FilesystemCacheFailedReserveAttempts,
        ProfileEvents::FilesystemCacheReserveMicroseconds,
        ProfileEvents::CachedReadBufferReadFromCacheBytes,
        ProfileEvents::CachedReadBufferReadFromSourceBytes,
        ProfileEvents::CachedReadBufferCacheWriteBytes,
        ProfileEvents::FileSegmentWaitMicroseconds,
        ProfileEvents::FileSegmentWriteMicroseconds,
        ProfileEvents::FileSegmentCompleteMicroseconds,
        ProfileEvents::FilesystemCacheCheckCorrectness,
        ProfileEvents::FilesystemCacheCheckCorrectnessMicroseconds,
        ProfileEvents::FilesystemCacheStateLockMicroseconds,
        ProfileEvents::FilesystemCachePriorityWriteLockMicroseconds,
        ProfileEvents::FilesystemCachePriorityReadLockMicroseconds,
        // B1: 31 names referenced by CH src/Interpreters/FileCache/*.
        ProfileEvents::FileSegmentFailToIncreasePriority,
        ProfileEvents::FileSegmentHolderCompleteMicroseconds,
        ProfileEvents::FileSegmentIncreasePriorityMicroseconds,
        ProfileEvents::FileSegmentLockMicroseconds,
        ProfileEvents::FilesystemCacheBackgroundDownloadQueuePush,
        ProfileEvents::FilesystemCacheBackgroundEvictedBytes,
        ProfileEvents::FilesystemCacheBackgroundEvictedFileSegments,
        ProfileEvents::FilesystemCacheBackgroundRemovedInvalidatedEntries,
        ProfileEvents::FilesystemCacheCreatedKeyDirectories,
        ProfileEvents::FilesystemCacheDowngradedFileSegments,
        ProfileEvents::FilesystemCacheEvictMicroseconds,
        ProfileEvents::FilesystemCacheEvictedBytes,
        ProfileEvents::FilesystemCacheEvictedFileSegments,
        ProfileEvents::FilesystemCacheEvictionReusedIterator,
        ProfileEvents::FilesystemCacheEvictionSkippedEvictingFileSegments,
        ProfileEvents::FilesystemCacheEvictionSkippedFileSegments,
        ProfileEvents::FilesystemCacheEvictionSkippedMovingFileSegments,
        ProfileEvents::FilesystemCacheEvictionTries,
        ProfileEvents::FilesystemCacheFailToReserveSpaceBecauseOfCacheResize,
        ProfileEvents::FilesystemCacheFailedEvictionCandidates,
        ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadErrors,
        ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadRun,
        ProfileEvents::FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds,
        ProfileEvents::FilesystemCacheHoldFileSegments,
        ProfileEvents::FilesystemCacheIdleClientEvictions,
        ProfileEvents::FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds,
        ProfileEvents::FilesystemCacheLoadMetadataMicroseconds,
        ProfileEvents::FilesystemCacheLockKeyMicroseconds,
        ProfileEvents::FilesystemCacheLockMetadataMicroseconds,
        ProfileEvents::FilesystemCacheLockOriginPoolMicroseconds,
        ProfileEvents::FilesystemCacheUnusedHoldFileSegments,
        // B1: 3 names referenced by CH src/IO/OpenedFileCache.h, required by
        // the Task-013 OpenedFileCache dependency mapping.
        ProfileEvents::OpenedFileCacheHits,
        ProfileEvents::OpenedFileCacheMisses,
        ProfileEvents::OpenedFileCacheMicroseconds,
    };

    constexpr std::size_t kExpectedCount = 16 + 34;
    static_assert(std::size(kRequiredEvents) == kExpectedCount);

    for (auto event : kRequiredEvents)
    {
        ProfileEvents::increment(event);
    }
    EXPECT_EQ(std::size(kRequiredEvents), kExpectedCount);
}

// Compile-coverage for the complete required `CurrentMetrics::Metric` name
// surface: every pre-existing name plus the 5 approved B2 names. Deleting
// any one name from CurrentMetrics.h makes this array fail to compile, not
// just fail a runtime check.
TEST(CurrentMetricsCoverageTest, AllRequiredMetricNamesCompile)
{
    constexpr CurrentMetrics::Metric kRequiredMetrics[] = {
        // Pre-existing names.
        CurrentMetrics::CacheFileSegments,
        CurrentMetrics::FilesystemCacheHoldFileSegments,
        CurrentMetrics::FilesystemCacheDownloadQueueElements,
        CurrentMetrics::FilesystemCacheDelayedCleanupElements,
        CurrentMetrics::FilesystemCacheReserveThreads,
        CurrentMetrics::FilesystemCacheSizeLimit,
        // B2: 5 approved names.
        CurrentMetrics::FilesystemCacheElements,
        CurrentMetrics::FilesystemCacheInvalidatedElements,
        CurrentMetrics::FilesystemCachePriorityQueueElements,
        CurrentMetrics::FilesystemCacheSize,
        CurrentMetrics::FilesystemCacheKeys,
    };

    constexpr std::size_t kExpectedCount = 6 + 5;
    static_assert(std::size(kRequiredMetrics) == kExpectedCount);

    for (auto metric : kRequiredMetrics)
    {
        CurrentMetrics::add(metric);
    }
    EXPECT_EQ(std::size(kRequiredMetrics), kExpectedCount);
}

}
}
