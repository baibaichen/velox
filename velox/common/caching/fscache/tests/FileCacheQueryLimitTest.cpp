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

#include "velox/common/caching/fscache/FileCacheQueryLimit.h"

#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::cache::fs::test {

TEST(FileCacheQueryLimitTest, reserveSucceedsUnderLimit) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(1'000);
  EXPECT_TRUE(token->tryReserve(400));
  EXPECT_TRUE(token->tryReserve(500));
  EXPECT_EQ(token->reserved(), 900);
}

TEST(FileCacheQueryLimitTest, reserveRejectsBeyondLimit) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(1'000);
  EXPECT_TRUE(token->tryReserve(800));
  EXPECT_FALSE(token->tryReserve(300));
  EXPECT_EQ(token->reserved(), 800);
}

TEST(FileCacheQueryLimitTest, releaseFreesCapacity) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(1'000);
  ASSERT_TRUE(token->tryReserve(900));
  token->release(400);
  EXPECT_EQ(token->reserved(), 500);
  EXPECT_TRUE(token->tryReserve(400));
}

TEST(FileCacheQueryLimitTest, releaseChecksUnderflow) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(1'000);
  VELOX_ASSERT_THROW(token->release(1), "FileCacheQueryLimit underflow");
}

TEST(FileCacheQueryLimitTest, tokenDestructorReleasesAll) {
  FileCacheQueryLimit limit;
  {
    auto token = limit.reserveQuery(1'000);
    ASSERT_TRUE(token->tryReserve(700));
  }
  EXPECT_EQ(limit.totalReserved(), 0);
}

} // namespace facebook::velox::cache::fs::test
