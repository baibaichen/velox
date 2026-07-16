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

#include "velox/exec/ch/Common/HashTable/Prefetching.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

TEST(PrefetchingTest, usesClickHouseTuningConstants) {
  EXPECT_EQ(PrefetchingHelper::getInitialLookAheadValue(), 4);
  EXPECT_EQ(PrefetchingHelper::iterationsToMeasure(), 100);
}

TEST(PrefetchingTest, calculatesCeilingAndClampsLookAhead) {
  using namespace std::chrono_literals;

  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(0ns), 32);
  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(1250ns), 32);
  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(1600ns), 25);
  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(3000ns), 14);
  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(10'000ns), 4);
  EXPECT_EQ(PrefetchingHelper::calcPrefetchLookAhead(100'000ns), 4);
}

TEST(PrefetchingTest, prefetchesInitialLookAheadWithinBounds) {
  std::vector<size_t> prefetchedRows;
  auto prefetcher = makeJoinPrefetcher(
      true, 8, [&](size_t row) { prefetchedRows.push_back(row); });

  prefetcher.prefetchAt(0);
  prefetcher.prefetchAt(3);
  prefetcher.prefetchAt(4);
  EXPECT_EQ(prefetchedRows, std::vector<size_t>({4, 7}));

  auto disabled = makeJoinPrefetcher(
      false, 8, [&](size_t row) { prefetchedRows.push_back(row); });
  disabled.prefetchAt(0);
  EXPECT_EQ(prefetchedRows, std::vector<size_t>({4, 7}));
}

TEST(PrefetchingTest, reportsPositiveL2CacheThreshold) {
  EXPECT_GT(minTableBytesForPrefetch(), 0);
  EXPECT_EQ(minTableBytesForPrefetch(), minTableBytesForPrefetch());
}
} // namespace
} // namespace facebook::velox::exec::ch
