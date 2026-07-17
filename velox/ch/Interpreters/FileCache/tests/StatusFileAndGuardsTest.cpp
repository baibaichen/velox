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

#include "velox/ch/Common/StatusFile.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// StatusFile tests
// ---------------------------------------------------------------------------

TEST(StatusFileTest, WritePidFillFunctionDoesNotThrow)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";
    EXPECT_NO_THROW(StatusFile file(path, StatusFile::writePid()));
    // After destruction the path is removed.
    EXPECT_FALSE(fs::exists(path));
}

TEST(StatusFileTest, EmptyFillFunctionDoesNotThrow)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";
    EXPECT_NO_THROW(StatusFile file(path, nullptr));
    EXPECT_FALSE(fs::exists(path));
}

TEST(StatusFileTest, SecondInstanceOnSamePathThrows)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";

    StatusFile first(path, StatusFile::writePid());

    // A second StatusFile on the same path must fail because the first
    // holds the exclusive flock.
    EXPECT_THROW(StatusFile second(path, StatusFile::writePid()), VeloxRuntimeError);
}

TEST(StatusFileTest, DestructorUnlinksPath)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";
    {
        StatusFile file(path, StatusFile::writePid());
        EXPECT_TRUE(fs::exists(path));
    }
    EXPECT_FALSE(fs::exists(path));
}

TEST(StatusFileTest, AfterDestructionNewInstanceSucceeds)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";
    {
        StatusFile first(path, StatusFile::writePid());
    }
    // Previous destructor closed and unlinked; a fresh instance must succeed.
    EXPECT_NO_THROW(StatusFile second(path, StatusFile::writePid()));
}

// ~StatusFile must never throw, even when the underlying close(2) call it
// performs fails. The FillFunction runs during construction with direct
// access to the real fd, so closing that fd early (before the destructor's
// own close attempt) forces close(2) to fail with EBADF when the destructor
// later calls folly::File::closeNoThrow() on the same fd number. If the
// destructor used folly::File::close() instead, that failing close(2) would
// throw a SystemError out of an implicitly noexcept destructor and terminate
// the process; std::terminate would abort this test process outright rather
// than reporting a gtest failure, so surviving to the assertions below is
// itself part of the evidence that closeNoThrow() is in effect.
TEST(StatusFileTest, DestructorDoesNotThrowWhenCloseFails)
{
    auto directory = TempDirectoryPath::create();
    const std::string path = directory->getPath() + "/status";

    auto closeFdEarly = [](int fd)
    {
        ASSERT_EQ(::close(fd), 0) << "precondition: fd must close successfully here";
    };

    EXPECT_NO_THROW({ StatusFile file(path, closeFdEarly); });

    // The destructor still unlinks the path even though its own close(2)
    // call on the already-closed fd failed.
    EXPECT_FALSE(fs::exists(path));
}

// ---------------------------------------------------------------------------
// Guards tests
// ---------------------------------------------------------------------------

// CachePriorityGuard locks compile and are the correct lock types.
TEST(GuardsTest, CachePriorityGuardLockTypes)
{
    CachePriorityGuard guard;

    CachePriorityGuard::ReadLock readLock = guard.readLock();
    static_assert(
        std::is_same_v<
            CachePriorityGuard::ReadLock,
            std::shared_lock<SharedMutex>>);

    readLock.unlock();

    CachePriorityGuard::WriteLock writeLock = guard.writeLock();
    static_assert(
        std::is_same_v<
            CachePriorityGuard::WriteLock,
            std::unique_lock<SharedMutex>>);
}

// tryReadLock / tryWriteLock return an unowned lock when they fail.
TEST(GuardsTest, CachePriorityGuardTryLockMayFail)
{
    CachePriorityGuard guard;

    // Hold write lock, then try to take another read lock (should fail).
    auto writeLock = guard.writeLock();

    auto tryRead = guard.tryReadLock();
    EXPECT_FALSE(tryRead.owns_lock());

    auto tryWrite = guard.tryWriteLock();
    EXPECT_FALSE(tryWrite.owns_lock());
}

// CacheStateGuard::tryLockFor compiles and returns within the timeout.
TEST(GuardsTest, CacheStateGuardTryLockFor)
{
    CacheStateGuard guard;
    CacheStateGuard::Lock lock = guard.tryLockFor(1ms);
    EXPECT_TRUE(lock.owns_lock());
}

// CacheStateGuard::tryLockFor with zero timeout on a held lock returns unowned.
TEST(GuardsTest, CacheStateGuardTryLockForFailsWhenHeld)
{
    CacheStateGuard guard;
    auto held = guard.lock();
    auto result = std::async(std::launch::async, [&guard]
    {
        return guard.tryLockFor(0ms).owns_lock();
    });
    EXPECT_FALSE(result.get());
}

// KeyGuard and FileSegmentGuard lock types are not interchangeable with
// CachePriorityGuard lock types — they are distinct struct::Lock types.
TEST(GuardsTest, LockTypesAreNotInterchangeable)
{
    static_assert(
        !std::is_same_v<KeyGuard::Lock, CachePriorityGuard::WriteLock>,
        "KeyGuard::Lock must differ from CachePriorityGuard::WriteLock");
    static_assert(
        !std::is_same_v<FileSegmentGuard::Lock, KeyGuard::Lock>,
        "FileSegmentGuard::Lock must differ from KeyGuard::Lock");
    static_assert(
        !std::is_same_v<CacheMetadataGuard::Lock, KeyGuard::Lock>,
        "CacheMetadataGuard::Lock must differ from KeyGuard::Lock");
}

// Each guard's lock() acquires and owns.
TEST(GuardsTest, AllGuardsLockSuccessfully)
{
    {
        CacheMetadataGuard g;
        auto lock = g.lock();
        EXPECT_TRUE(lock.owns_lock());
    }
    {
        KeyGuard g;
        auto lock = g.lock();
        EXPECT_TRUE(lock.owns_lock());
    }
    {
        FileSegmentGuard g;
        auto lock = g.lock();
        EXPECT_TRUE(lock.owns_lock());
    }
}

} // namespace
} // namespace facebook::velox::ch
