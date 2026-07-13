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

#include "velox/exec/ch/ChHashProbe.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/RowRef.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <optional>
#include <tuple>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

class ChHashProbeTest : public testing::Test,
                        public velox::test::VectorTestBase {
 protected:
  static constexpr uint32_t kDriverNo = 5;

  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  RowVectorPtr makeInput(
      std::vector<std::optional<int64_t>> keys,
      int64_t payloadBase = 0) {
    const auto size = keys.size();
    return makeRowVector({
        makeNullableFlatVector<int64_t>(keys),
        makeFlatVector<int64_t>(size, [=](auto row) {
          return payloadBase + row;
        }),
    });
  }

  static int64_t buildKeyAt(
      const ChHashBuild& build,
      const ProbeMatch& match) {
    const auto* batch = build.retainedIndex().at(
        unpackDriverNo(match.buildBlockNo),
        unpackBatchNo(match.buildBlockNo));
    return batch->childAt(0)
        ->asFlatVector<int64_t>()
        ->valueAt(match.buildRowNo);
  }
};

TEST_F(ChHashProbeTest, separatesProbeHitsFromDuplicateExpansion) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.addInput(makeInput({10, 20, 20, std::nullopt}, 100));
  build.addInput(makeInput({20, 30, 10}, 200));

  auto probe = makeInput({20, 99, std::nullopt, 10});
  const auto hits = joinProbe(build.rowsByKey(), probe, 0);

  ASSERT_EQ(hits.size(), 2);
  EXPECT_EQ(hits[0].probeRow, 0);
  EXPECT_EQ(hits[0].matched->rows(), 3);
  EXPECT_EQ(hits[1].probeRow, 3);
  EXPECT_EQ(hits[1].matched->rows(), 2);

  const auto matches = listJoinResults(hits, build.retainedIndex());
  ASSERT_EQ(matches.size(), 5);
  const std::vector<std::tuple<vector_size_t, uint32_t, uint32_t>> expected{
      {0, packBlockNo(kDriverNo, 0), 1},
      {0, packBlockNo(kDriverNo, 0), 2},
      {0, packBlockNo(kDriverNo, 1), 0},
      {3, packBlockNo(kDriverNo, 0), 0},
      {3, packBlockNo(kDriverNo, 1), 2},
  };

  for (size_t i = 0; i < matches.size(); ++i) {
    EXPECT_EQ(
        std::make_tuple(
            matches[i].probeRow,
            matches[i].buildBlockNo,
            matches[i].buildRowNo),
        expected[i]);
  }
}

TEST_F(ChHashProbeTest, resolvesMatchesAcrossBuildBatches) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.addInput(makeInput({10, 20, 20, std::nullopt}, 100));
  build.addInput(makeInput({20, 30, 10}, 200));
  build.noMoreInput();

  auto probe = makeInput({20, 99, std::nullopt, 10});
  const auto matches = probeHashBuild(build, probe, 0);

  ASSERT_EQ(matches.size(), 5);
  const std::vector<std::tuple<vector_size_t, uint32_t, uint32_t>> expected{
      {0, packBlockNo(kDriverNo, 0), 1},
      {0, packBlockNo(kDriverNo, 0), 2},
      {0, packBlockNo(kDriverNo, 1), 0},
      {3, packBlockNo(kDriverNo, 0), 0},
      {3, packBlockNo(kDriverNo, 1), 2},
  };

  for (size_t i = 0; i < matches.size(); ++i) {
    EXPECT_EQ(
        std::make_tuple(
            matches[i].probeRow,
            matches[i].buildBlockNo,
            matches[i].buildRowNo),
        expected[i]);
    EXPECT_EQ(
        buildKeyAt(build, matches[i]),
        probe->childAt(0)->asFlatVector<int64_t>()->valueAt(
            matches[i].probeRow));
  }
}

TEST_F(ChHashProbeTest, returnsEveryDuplicateExactlyOnce) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.addInput(makeInput({7, 7, 7}));
  build.addInput(makeInput({7, 8, 7}));

  const auto matches = probeHashBuild(build, makeInput({7}), 0);

  ASSERT_EQ(matches.size(), 5);
  const std::vector<std::pair<uint32_t, uint32_t>> expected{
      {packBlockNo(kDriverNo, 0), 0},
      {packBlockNo(kDriverNo, 0), 1},
      {packBlockNo(kDriverNo, 0), 2},
      {packBlockNo(kDriverNo, 1), 0},
      {packBlockNo(kDriverNo, 1), 2},
  };
  for (size_t i = 0; i < matches.size(); ++i) {
    EXPECT_EQ(matches[i].probeRow, 0);
    EXPECT_EQ(
        std::make_pair(matches[i].buildBlockNo, matches[i].buildRowNo),
        expected[i]);
  }
}

TEST_F(ChHashProbeTest, skipsUnmatchedAndNullKeys) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.addInput(makeInput({1, std::nullopt, 2}));

  const auto matches =
      probeHashBuild(build, makeInput({99, std::nullopt}), 0);

  EXPECT_TRUE(matches.empty());
}

} // namespace
} // namespace facebook::velox::exec::ch
