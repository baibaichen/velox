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
#include "velox/ch/Common/FileCacheBoundedQueue.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheFilesystem.h"
#include "velox/ch/Common/SharedMutex.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <type_traits>

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

TEST(FileCacheExceptionTest, ThrowsVeloxRuntimeError)
{
    EXPECT_THROW(
        throwFileCacheException("invalid value {}", 42),
        VeloxRuntimeError);
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
    catch (const VeloxRuntimeError & exception)
    {
        EXPECT_NE(
            std::string(exception.what()).find("loading cache"),
            std::string::npos);
    }
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

}
}
