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

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheGuardsTest, eachGuardIsIndependentMutex) {
  CachePriorityMutex priorityMutex;
  CacheStateMutex stateMutex;
  CacheMetadataMutex metadataMutex;
  KeyMutex keyMutex;
  FileSegmentMutex segmentMutex;

  // Locking one does not block the others.
  CachePriorityGuard priorityGuard{priorityMutex};
  CacheStateGuard stateGuard{stateMutex};
  CacheMetadataGuard metadataGuard{metadataMutex};
  KeyGuard keyGuard{keyMutex};
  FileSegmentGuard segmentGuard{segmentMutex};

  // All five guards co-exist. If any pair shared a mutex this would deadlock.
  SUCCEED();
}

TEST(FsCacheGuardsTest, guardReleasesOnDestruction) {
  CachePriorityMutex mutex;
  {
    CachePriorityGuard guard{mutex};
    EXPECT_FALSE(mutex.try_lock());
  }
  EXPECT_TRUE(mutex.try_lock());
  mutex.unlock();
}

#ifndef NDEBUG
TEST(FsCacheGuardsTest, lockOrderViolationThrows) {
  CachePriorityMutex priorityMutex;
  KeyMutex keyMutex;

  // Acquiring KeyGuard then CachePriorityGuard is out of order.
  KeyGuard keyGuard{keyMutex};
  EXPECT_THROW(
      { CachePriorityGuard priorityGuard{priorityMutex}; },
      VeloxRuntimeError);
}
#endif

} // namespace facebook::velox::cache::fs::test
