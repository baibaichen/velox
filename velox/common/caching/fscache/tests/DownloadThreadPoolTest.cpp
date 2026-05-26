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

#include "velox/common/caching/fscache/DownloadThreadPool.h"

#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

#include <atomic>
#include <vector>

namespace facebook::velox::cache::fs::test {

TEST(DownloadThreadPoolTest, submitRunsTask) {
  DownloadThreadPool pool{2};
  std::atomic<int> counter{0};
  auto fut = pool.submit([&]() { counter.fetch_add(1); });
  std::move(fut).get();
  EXPECT_EQ(counter.load(), 1);
}

TEST(DownloadThreadPoolTest, submitParallelRunsAllTasks) {
  DownloadThreadPool pool{4};
  std::atomic<int> counter{0};
  std::vector<folly::SemiFuture<folly::Unit>> futs;
  for (int i = 0; i < 16; ++i) {
    futs.push_back(pool.submit([&]() { counter.fetch_add(1); }));
  }
  for (auto& f : futs) {
    std::move(f).get();
  }
  EXPECT_EQ(counter.load(), 16);
}

TEST(DownloadThreadPoolTest, rejectsZeroThreads) {
  EXPECT_THROW(DownloadThreadPool{0}, ::facebook::velox::VeloxException);
}

} // namespace facebook::velox::cache::fs::test
