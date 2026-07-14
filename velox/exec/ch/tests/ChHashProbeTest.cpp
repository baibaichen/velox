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

TEST_F(ChHashProbeTest, matchesMultipleFixedWidthKeyChannels) {
  auto buildInput = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 1, 2}),
      makeFlatVector<int32_t>({0, 10, 10, 20}),
      makeFlatVector<int32_t>({0, 100, 100, 200}),
  });
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0, 1, 2},
      std::vector<TypePtr>{BIGINT(), INTEGER(), INTEGER()},
      pool());
  build.addInput(buildInput);

  auto probe = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 9}),
      makeFlatVector<int32_t>({0, 10, 20, 9}),
      makeFlatVector<int32_t>({0, 100, 200, 9}),
  });
  const auto matches = probeHashBuild(
      build, probe, std::vector<column_index_t>{0, 1, 2});

  ASSERT_EQ(matches.size(), 4);
  EXPECT_EQ(matches[0].probeRow, 0);
  EXPECT_EQ(matches[0].buildRowNo, 0);
  EXPECT_EQ(matches[1].probeRow, 1);
  EXPECT_EQ(matches[1].buildRowNo, 1);
  EXPECT_EQ(matches[2].probeRow, 1);
  EXPECT_EQ(matches[2].buildRowNo, 2);
  EXPECT_EQ(matches[3].probeRow, 2);
  EXPECT_EQ(matches[3].buildRowNo, 3);
}

TEST_F(ChHashProbeTest, routesKeyTypesToMaps) {
  ChHashBuild singleBigint(
      kDriverNo, std::vector<column_index_t>{0}, {BIGINT()}, pool());
  EXPECT_EQ(singleBigint.keyMapType(), FixedKeyMap::Type::key64);

  ChHashBuild singleString(
      kDriverNo, std::vector<column_index_t>{0}, {VARCHAR()}, pool());
  EXPECT_EQ(singleString.keyMapType(), FixedKeyMap::Type::key_string);

  ChHashBuild mixed(
      kDriverNo,
      std::vector<column_index_t>{0, 1},
      {BIGINT(), VARCHAR()},
      pool());
  EXPECT_EQ(mixed.keyMapType(), FixedKeyMap::Type::hashed);
}

TEST_F(ChHashProbeTest, matchesSingleIntegerKeys) {
  // A single 4-byte INTEGER key routes to Type::key32, which is carried by the
  // 64-bit map today; verify build and probe still resolve matches.
  auto buildInput = makeRowVector({
      makeFlatVector<int32_t>({10, 20, 20, 30}),
  });
  ChHashBuild build(
      kDriverNo, std::vector<column_index_t>{0}, {INTEGER()}, pool());
  EXPECT_EQ(build.keyMapType(), FixedKeyMap::Type::key32);
  build.addInput(buildInput);

  auto probe = makeRowVector({
      makeFlatVector<int32_t>({20, 99, 10}),
  });
  const auto matches = probeHashBuild(build, probe, 0);

  ASSERT_EQ(matches.size(), 3);
  EXPECT_EQ(matches[0].probeRow, 0);
  EXPECT_EQ(matches[0].buildRowNo, 1);
  EXPECT_EQ(matches[1].probeRow, 0);
  EXPECT_EQ(matches[1].buildRowNo, 2);
  EXPECT_EQ(matches[2].probeRow, 2);
  EXPECT_EQ(matches[2].buildRowNo, 0);
}

TEST_F(ChHashProbeTest, matchesSerializedStringKeysIncludingEmpty) {
  auto buildInput = makeRowVector({
      makeFlatVector<std::string>({"", "alpha", "alpha", "long-string"}),
  });
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0},
      std::vector<TypePtr>{VARCHAR()},
      pool());
  build.addInput(buildInput);

  auto probe = makeRowVector({
      makeFlatVector<std::string>({"alpha", "", "missing"}),
  });
  const auto matches = probeHashBuild(build, probe, 0);

  ASSERT_EQ(matches.size(), 3);
  EXPECT_EQ(matches[0].probeRow, 0);
  EXPECT_EQ(matches[0].buildRowNo, 1);
  EXPECT_EQ(matches[1].probeRow, 0);
  EXPECT_EQ(matches[1].buildRowNo, 2);
  EXPECT_EQ(matches[2].probeRow, 1);
  EXPECT_EQ(matches[2].buildRowNo, 0);
}

TEST_F(ChHashProbeTest, matchesSerializedMixedKeys) {
  auto buildInput = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2}),
      makeFlatVector<std::string>({"one", "uno", "two"}),
  });
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0, 1},
      std::vector<TypePtr>{BIGINT(), VARCHAR()},
      pool());
  build.addInput(buildInput);

  auto probe = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1}),
      makeFlatVector<std::string>({"uno", "two", "missing"}),
  });
  const auto matches = probeHashBuild(
      build, probe, std::vector<column_index_t>{0, 1});

  ASSERT_EQ(matches.size(), 2);
  EXPECT_EQ(matches[0].buildRowNo, 1);
  EXPECT_EQ(matches[1].buildRowNo, 2);
}

TEST_F(ChHashProbeTest, matchesSerializedOverwideFixedKeys) {
  std::vector<VectorPtr> buildColumns;
  std::vector<VectorPtr> probeColumns;
  for (int64_t column = 0; column < 5; ++column) {
    buildColumns.push_back(makeFlatVector<int64_t>({column, column + 10}));
    probeColumns.push_back(makeFlatVector<int64_t>({column + 10, column + 20}));
  }
  auto buildInput = makeRowVector(std::move(buildColumns));
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0, 1, 2, 3, 4},
      std::vector<TypePtr>{
          BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      pool());
  build.addInput(buildInput);

  const auto matches = probeHashBuild(
      build,
      makeRowVector(std::move(probeColumns)),
      std::vector<column_index_t>{0, 1, 2, 3, 4});

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].probeRow, 0);
  EXPECT_EQ(matches[0].buildRowNo, 1);
}

TEST_F(ChHashProbeTest, matchesStringKeysIncludingDuplicatesAndEmpty) {
  auto buildInput = makeRowVector({
      makeFlatVector<std::string>({"", "alpha", "alpha", "long-string"}),
  });
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0},
      std::vector<TypePtr>{VARCHAR()},
      pool());
  build.addInput(buildInput);

  EXPECT_EQ(build.keyMapType(), FixedKeyMap::Type::key_string);
  auto probe = makeRowVector({
      makeFlatVector<std::string>({"alpha", "", "missing"}),
  });
  const auto matches = probeHashBuild(build, probe, 0);

  ASSERT_EQ(matches.size(), 3);
  EXPECT_EQ(matches[0].probeRow, 0);
  EXPECT_EQ(matches[0].buildRowNo, 1);
  EXPECT_EQ(matches[1].probeRow, 0);
  EXPECT_EQ(matches[1].buildRowNo, 2);
  EXPECT_EQ(matches[2].probeRow, 1);
  EXPECT_EQ(matches[2].buildRowNo, 0);
}

TEST_F(ChHashProbeTest, matchesHashedMixedAndOverwideKeys) {
  auto mixedBuild = makeRowVector({
      makeFlatVector<int64_t>({1, 1, 2}),
      makeFlatVector<std::string>({"one", "uno", "two"}),
  });
  ChHashBuild mixed(
      kDriverNo,
      std::vector<column_index_t>{0, 1},
      std::vector<TypePtr>{BIGINT(), VARCHAR()},
      pool());
  mixed.addInput(mixedBuild);
  EXPECT_EQ(mixed.keyMapType(), FixedKeyMap::Type::hashed);
  auto mixedProbe = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1}),
      makeFlatVector<std::string>({"uno", "two", "missing"}),
  });
  const auto mixedMatches = probeHashBuild(
      mixed, mixedProbe, std::vector<column_index_t>{0, 1});
  ASSERT_EQ(mixedMatches.size(), 2);
  EXPECT_EQ(mixedMatches[0].buildRowNo, 1);
  EXPECT_EQ(mixedMatches[1].buildRowNo, 2);

  std::vector<VectorPtr> buildColumns;
  std::vector<VectorPtr> probeColumns;
  for (int64_t column = 0; column < 5; ++column) {
    buildColumns.push_back(makeFlatVector<int64_t>({column, column + 10}));
    probeColumns.push_back(
        makeFlatVector<int64_t>({column + 10, column + 20}));
  }
  ChHashBuild wide(
      kDriverNo,
      std::vector<column_index_t>{0, 1, 2, 3, 4},
      std::vector<TypePtr>{
          BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      pool());
  wide.addInput(makeRowVector(std::move(buildColumns)));
  const auto wideMatches = probeHashBuild(
      wide,
      makeRowVector(std::move(probeColumns)),
      std::vector<column_index_t>{0, 1, 2, 3, 4});
  ASSERT_EQ(wideMatches.size(), 1);
  EXPECT_EQ(wideMatches[0].probeRow, 0);
  EXPECT_EQ(wideMatches[0].buildRowNo, 1);
}

TEST_F(ChHashProbeTest, hashedKeysSkipNullsEndToEnd) {
  auto buildInput = makeRowVector({makeNullableFlatVector<std::string>(
      {"alpha", std::nullopt, "beta"})});
  ChHashBuild build(
      kDriverNo,
      std::vector<column_index_t>{0},
      std::vector<TypePtr>{VARCHAR()},
      pool());
  build.addInput(buildInput);

  auto probe = makeRowVector({makeNullableFlatVector<std::string>(
      {std::nullopt, "beta", "missing"})});
  const auto matches = probeHashBuild(build, probe, 0);

  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].probeRow, 1);
  EXPECT_EQ(matches[0].buildRowNo, 2);
}
} // namespace
} // namespace facebook::velox::exec::ch
