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

#include "velox/common/caching/fscache/benchmarks/KeyGenerator.h"

#include <gtest/gtest.h>

#include <set>
#include <vector>

namespace facebook::velox::cache::fs::bench::test {

TEST(KeyGeneratorTest, sequentialWalksAndWraps) {
  KeyGenerator g{Workload::kSequential, 4, /*seed=*/42};
  std::vector<uint64_t> seen;
  for (int i = 0; i < 10; ++i) {
    seen.push_back(g.next());
  }
  EXPECT_EQ(seen, (std::vector<uint64_t>{0, 1, 2, 3, 0, 1, 2, 3, 0, 1}));
}

TEST(KeyGeneratorTest, sequentialPartitionedBySeqStart) {
  KeyGenerator g0{Workload::kSequential, 8, /*seed=*/42, /*seqStart=*/0};
  KeyGenerator g1{Workload::kSequential, 8, /*seed=*/42, /*seqStart=*/4};
  std::set<uint64_t> first4;
  std::set<uint64_t> second4;
  for (int i = 0; i < 4; ++i) {
    first4.insert(g0.next());
    second4.insert(g1.next());
  }
  EXPECT_EQ(first4, (std::set<uint64_t>{0, 1, 2, 3}));
  EXPECT_EQ(second4, (std::set<uint64_t>{4, 5, 6, 7}));
}

TEST(KeyGeneratorTest, uniformCoversFullUniverse) {
  KeyGenerator g{Workload::kUniform, 16, /*seed=*/42};
  std::set<uint64_t> seen;
  for (int i = 0; i < 1'000; ++i) {
    seen.insert(g.next());
  }
  // 1000 draws over 16 keys: P(any miss) ~ 16 * (15/16)^1000 ~ 1e-27.
  EXPECT_EQ(seen.size(), 16u);
}

TEST(KeyGeneratorTest, zipfianHotKeysDominate) {
  KeyGenerator g{Workload::kZipfian, 100, /*seed=*/42};
  std::vector<uint64_t> hist(100, 0);
  constexpr int kDraws = 10'000;
  for (int i = 0; i < kDraws; ++i) {
    ++hist[g.next()];
  }
  // theta=1.0, seed=42: top-10 keys empirically cover ~50% of mass. Use a
  // generous [30%, 70%] band to stay stable across stdlib RNG impls.
  uint64_t top10 = 0;
  for (int i = 0; i < 10; ++i) {
    top10 += hist[i];
  }
  EXPECT_GT(top10, kDraws * 30 / 100);
  EXPECT_LT(top10, kDraws * 70 / 100);
}

TEST(KeyGeneratorTest, sameSeedProducesSameSequence) {
  KeyGenerator a{Workload::kZipfian, 64, /*seed=*/123};
  KeyGenerator b{Workload::kZipfian, 64, /*seed=*/123};
  for (int i = 0; i < 200; ++i) {
    EXPECT_EQ(a.next(), b.next()) << "draw " << i;
  }
}

} // namespace facebook::velox::cache::fs::bench::test
