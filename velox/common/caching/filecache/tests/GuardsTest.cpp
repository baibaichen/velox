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
#include "velox/common/caching/filecache/Guards.h"

#include <chrono>

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(GuardsTest, cachePriorityGuardWriteLockReleases) {
  CachePriorityGuard guard;

  {
    auto lock = guard.writeLock();
    EXPECT_TRUE(lock.owns_lock());
  }

  auto lock = guard.tryWriteLock();
  EXPECT_TRUE(lock.owns_lock());
}

TEST(GuardsTest, cachePriorityGuardReadLockAllowsMultipleConcurrentReaders) {
  CachePriorityGuard guard;

  auto firstLock = guard.readLock();
  EXPECT_TRUE(firstLock.owns_lock());

  auto secondLock = guard.tryReadLock();
  EXPECT_TRUE(secondLock.owns_lock());
}

TEST(GuardsTest, cachePriorityGuardTryWriteLockFailsWhileWriteLocked) {
  CachePriorityGuard guard;

  auto firstLock = guard.writeLock();
  EXPECT_TRUE(firstLock.owns_lock());

  auto secondLock = guard.tryWriteLock();
  EXPECT_FALSE(secondLock.owns_lock());
}

TEST(GuardsTest, cachePriorityGuardTryWriteLockSucceedsWhenIdle) {
  CachePriorityGuard guard;

  auto lock = guard.tryWriteLock();
  EXPECT_TRUE(lock.owns_lock());
}

TEST(GuardsTest, cacheStateGuardLockReleases) {
  CacheStateGuard guard;

  {
    auto lock = guard.lock();
    EXPECT_TRUE(lock.owns_lock());
  }

  auto lock = guard.tryLock();
  EXPECT_TRUE(lock.owns_lock());
}

TEST(GuardsTest, cacheStateGuardTryLockSucceedsWhenIdleAndFailsWhenLocked) {
  CacheStateGuard guard;

  auto firstLock = guard.tryLock();
  EXPECT_TRUE(firstLock.owns_lock());

  auto secondLock = guard.tryLock();
  EXPECT_FALSE(secondLock.owns_lock());
}

TEST(GuardsTest, cacheStateGuardTryLockForTimesOutWhenLocked) {
  CacheStateGuard guard;

  auto firstLock = guard.lock();
  EXPECT_TRUE(firstLock.owns_lock());

  auto secondLock = guard.tryLockFor(std::chrono::milliseconds(1));
  EXPECT_FALSE(secondLock.owns_lock());
}

TEST(GuardsTest, cacheMetadataGuardLockReleases) {
  CacheMetadataGuard guard;

  {
    auto lock = guard.lock();
    EXPECT_TRUE(lock.owns_lock());
  }

  auto lock = guard.lock();
  EXPECT_TRUE(lock.owns_lock());
}

TEST(GuardsTest, keyGuardLockReleases) {
  KeyGuard guard;

  {
    auto lock = guard.lock();
    EXPECT_TRUE(lock.owns_lock());
  }

  auto lock = guard.lock();
  EXPECT_TRUE(lock.owns_lock());
}

TEST(GuardsTest, fileSegmentGuardLockReleases) {
  FileSegmentGuard guard;

  {
    auto lock = guard.lock();
    EXPECT_TRUE(lock.owns_lock());
  }

  auto lock = guard.lock();
  EXPECT_TRUE(lock.owns_lock());
}
