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

#include "velox/exec/ch/EmitGather.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/exec/ch/ChHashProbe.h"
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

class EmitGatherTest : public testing::Test,
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

  RowVectorPtr makeBuildInput(
      std::vector<int64_t> keys,
      std::vector<double> doubles,
      std::vector<std::optional<std::string>> strings) {
    return makeRowVector({
        makeFlatVector<int64_t>(keys),
        makeFlatVector<double>(doubles),
        makeNullableFlatVector<std::string>(strings),
    });
  }

  RowVectorPtr makeProbeInput(
      std::vector<int64_t> keys,
      std::vector<int64_t> payloads) {
    return makeRowVector({
        makeFlatVector<int64_t>(keys),
        makeFlatVector<int64_t>(payloads),
    });
  }

  template <typename T>
  static T valueAt(const VectorPtr& vector, vector_size_t row) {
    return vector->as<SimpleVector<T>>()->valueAt(row);
  }
};

TEST_F(EmitGatherTest, emitsDictionaryViewsGroupedByBuildBlock) {
  ChHashBuild build(kDriverNo, 0, pool());
  build.addInput(makeBuildInput(
      {10, 20, 20},
      {1.5, 2.5, 3.5},
      {std::string("a"), std::string("b"), std::nullopt}));
  build.addInput(makeBuildInput(
      {20, 30, 10},
      {4.5, 5.5, 6.5},
      {std::string("d"), std::string("e"), std::string("f")}));
  build.addInput(makeBuildInput(
      {20, 40},
      {7.5, 8.5},
      {std::string("g"), std::string("h")}));
  build.noMoreInput();

  auto probe = makeProbeInput({20, 10}, {200, 100});
  const auto matches = probeHashBuild(build, probe, 0);
  EmitGather gather(
      build.retainedIndex(),
      {0, 1, 2},
      {1},
      ROW(
          {"build_key", "build_double", "build_string", "probe_payload"},
          {BIGINT(), DOUBLE(), VARCHAR(), BIGINT()}),
      pool());

  const auto output = gather.emit(matches, probe);

  ASSERT_EQ(output.size(), 3);
  ASSERT_EQ(output[0]->size(), 3);
  ASSERT_EQ(output[1]->size(), 2);
  ASSERT_EQ(output[2]->size(), 1);

  const std::vector<std::vector<int64_t>> expectedKeys{
      {20, 20, 10}, {20, 10}, {20}};
  const std::vector<std::vector<double>> expectedDoubles{
      {2.5, 3.5, 1.5}, {4.5, 6.5}, {7.5}};
  const std::vector<std::vector<int64_t>> expectedProbe{
      {200, 200, 100}, {200, 100}, {200}};
  const std::vector<std::vector<std::optional<std::string>>> expectedStrings{
      {std::string("b"), std::nullopt, std::string("a")},
      {std::string("d"), std::string("f")},
      {std::string("g")}};

  for (size_t batch = 0; batch < output.size(); ++batch) {
    const auto* retained = build.retainedIndex().at(kDriverNo, batch);
    for (column_index_t column = 0; column < 4; ++column) {
      EXPECT_EQ(
          output[batch]->childAt(column)->encoding(),
          VectorEncoding::Simple::DICTIONARY);
    }
    EXPECT_EQ(
        output[batch]->childAt(0)->valueVector().get(),
        retained->childAt(0).get());
    EXPECT_EQ(
        output[batch]->childAt(1)->valueVector().get(),
        retained->childAt(1).get());
    EXPECT_EQ(
        output[batch]->childAt(2)->valueVector().get(),
        retained->childAt(2).get());
    EXPECT_EQ(
        output[batch]->childAt(3)->valueVector().get(),
        probe->childAt(1).get());

    auto flatKey = BaseVector::copy(*output[batch]->childAt(0), pool());
    auto flatDouble = BaseVector::copy(*output[batch]->childAt(1), pool());
    auto flatString = BaseVector::copy(*output[batch]->childAt(2), pool());
    auto flatProbe = BaseVector::copy(*output[batch]->childAt(3), pool());
    for (vector_size_t row = 0; row < output[batch]->size(); ++row) {
      EXPECT_EQ(valueAt<int64_t>(flatKey, row), expectedKeys[batch][row]);
      EXPECT_EQ(valueAt<double>(flatDouble, row), expectedDoubles[batch][row]);
      EXPECT_EQ(valueAt<int64_t>(flatProbe, row), expectedProbe[batch][row]);
      if (expectedStrings[batch][row].has_value()) {
        ASSERT_FALSE(flatString->isNullAt(row));
        EXPECT_EQ(
            valueAt<StringView>(flatString, row).str(),
            expectedStrings[batch][row].value());
      } else {
        EXPECT_TRUE(flatString->isNullAt(row));
      }
    }
  }
}

TEST_F(EmitGatherTest, copiesNestedBuildColumns) {
  RetainedVectorsIndex retained(kDriverNo);
  auto arrays = makeArrayVector<int64_t>({{1, 2}, {3}, {4, 5, 6}});
  retained.add(makeRowVector({arrays}));
  auto probe = makeProbeInput({1, 2}, {10, 20});
  const std::vector<ProbeMatch> matches{
      {1, packBlockNo(kDriverNo, 0), 2},
      {0, packBlockNo(kDriverNo, 0), 0},
  };
  EmitGather gather(
      retained,
      {0},
      {},
      ROW({"build_array"}, {ARRAY(BIGINT())}),
      pool());

  const auto output = gather.emit(matches, probe);

  ASSERT_EQ(output.size(), 1);
  ASSERT_EQ(output[0]->size(), 2);
  EXPECT_EQ(output[0]->childAt(0)->encoding(), VectorEncoding::Simple::ARRAY);
  EXPECT_TRUE(output[0]->childAt(0)->equalValueAt(arrays.get(), 0, 2));
  EXPECT_TRUE(output[0]->childAt(0)->equalValueAt(arrays.get(), 1, 0));
}

} // namespace
} // namespace facebook::velox::exec::ch
