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

#include "velox/exec/ch/ChHashJoinBridge.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/synchronization/CallOnce.h>
#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

using exec::test::AssertQueryBuilder;
using exec::test::PlanBuilder;

class ChHashJoinPipelineTest : public testing::Test,
                               public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  static void ensureRegistered() {
    static folly::once_flag registerFlag;
    folly::call_once(registerFlag, [] { registerChHashJoin(); });
  }

  core::PlanNodePtr makeChPlan(
      const std::vector<RowVectorPtr>& probe,
      const std::vector<RowVectorPtr>& build,
      bool addDownstream = false) {
    auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto buildNode =
        PlanBuilder(idGenerator).values(build, true).planNode();
    auto builder = PlanBuilder(idGenerator).values(probe, true).addNode(
        [buildNode](std::string id, core::PlanNodePtr probeNode) {
          auto probeKey =
              std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "p_key");
          auto buildKey =
              std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "b_key");
          return std::make_shared<ChHashJoinNode>(
              id,
              core::JoinType::kInner,
              std::vector<core::FieldAccessTypedExprPtr>{probeKey},
              std::vector<core::FieldAccessTypedExprPtr>{buildKey},
              nullptr,
              std::move(probeNode),
              buildNode,
              ROW(
                  {"b_key", "b_value", "b_text", "p_key", "p_value", "p_text"},
                  {BIGINT(),
                   BIGINT(),
                   VARCHAR(),
                   BIGINT(),
                   BIGINT(),
                   VARCHAR()}));
        });
    if (addDownstream) {
      builder.filter("p_value >= 20")
          .project({"b_text", "p_value + b_value AS total"});
    }
    return builder.planNode();
  }

  core::PlanNodePtr makeNativePlan(
      const std::vector<RowVectorPtr>& probe,
      const std::vector<RowVectorPtr>& build,
      bool addDownstream = false) {
    auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto buildNode =
        PlanBuilder(idGenerator).values(build, true).planNode();
    auto builder = PlanBuilder(idGenerator)
                       .values(probe, true)
                       .hashJoin(
                           {"p_key"},
                           {"b_key"},
                           buildNode,
                           "",
                           {"b_key",
                            "b_value",
                            "b_text",
                            "p_key",
                            "p_value",
                            "p_text"});
    if (addDownstream) {
      builder.filter("p_value >= 20")
          .project({"b_text", "p_value + b_value AS total"});
    }
    return builder.planNode();
  }

  std::vector<RowVectorPtr> basicProbe() {
    return {makeRowVector(
        {"p_key", "p_value", "p_text"},
        {
            makeNullableFlatVector<int64_t>({1, 2, std::nullopt, 3, 1}),
            makeFlatVector<int64_t>({10, 20, 30, 40, 11}),
            makeFlatVector<std::string>({"p1a", "p2", "pn", "p3", "p1b"}),
        })};
  }

  std::vector<RowVectorPtr> basicBuild() {
    return {makeRowVector(
        {"b_key", "b_value", "b_text"},
        {
            makeNullableFlatVector<int64_t>({1, 1, 2, std::nullopt, 4}),
            makeFlatVector<int64_t>({100, 101, 200, 999, 400}),
            makeFlatVector<std::string>({"b1a", "b1b", "b2", "bn", "b4"}),
        })};
  }

  void assertMatchesNative(
      const core::PlanNodePtr& chPlan,
      const core::PlanNodePtr& nativePlan,
      std::shared_ptr<Task>* task = nullptr) {
    auto expected = AssertQueryBuilder(nativePlan).copyResultBatches(pool());
    if (task == nullptr) {
      AssertQueryBuilder(chPlan).maxDrivers(4).assertResults(expected);
    } else {
      *task =
          AssertQueryBuilder(chPlan).maxDrivers(4).assertResults(expected);
    }
  }
};

TEST_F(ChHashJoinPipelineTest, registrationIsRequiredBeforePlanning) {
  auto probe = basicProbe();
  auto build = basicBuild();
  auto plan = makeChPlan(probe, build);

  EXPECT_EQ(Operator::joinBridgeFromPlanNode(plan), nullptr);
  EXPECT_ANY_THROW(AssertQueryBuilder(plan).copyResults(pool()));

  ensureRegistered();
  auto bridge = Operator::joinBridgeFromPlanNode(plan);
  ASSERT_NE(bridge, nullptr);
  EXPECT_NE(dynamic_cast<ChHashJoinBridge*>(bridge.get()), nullptr);
}

TEST_F(ChHashJoinPipelineTest, innerJoinMatchesNativeWithNullsAndDuplicates) {
  ensureRegistered();
  auto probe = basicProbe();
  auto build = basicBuild();

  assertMatchesNative(
      makeChPlan(probe, build), makeNativePlan(probe, build));
}

TEST_F(ChHashJoinPipelineTest, waitsForBuildAndEmitsEveryBuildBatch) {
  ensureRegistered();
  std::vector<RowVectorPtr> build;
  for (int64_t batch = 0; batch < 4; ++batch) {
    build.push_back(makeRowVector(
        {"b_key", "b_value", "b_text"},
        {
            makeFlatVector<int64_t>(
                1024, [](vector_size_t row) { return row; }),
            makeFlatVector<int64_t>(
                1024,
                [batch](vector_size_t row) { return batch * 10'000 + row; }),
            makeFlatVector<std::string>(
                1024,
                [batch](vector_size_t row) {
                  return fmt::format("b{}-{}", batch, row);
                }),
        }));
  }
  auto probe = std::vector<RowVectorPtr>{makeRowVector(
      {"p_key", "p_value", "p_text"},
      {
          makeFlatVector<int64_t>(
              1024, [](vector_size_t row) { return row; }),
          makeFlatVector<int64_t>(
              1024, [](vector_size_t row) { return row * 2; }),
          makeFlatVector<std::string>(
              1024,
              [](vector_size_t row) { return fmt::format("p{}", row); }),
      })};

  std::shared_ptr<Task> task;
  assertMatchesNative(
      makeChPlan(probe, build), makeNativePlan(probe, build), &task);

  ASSERT_NE(task, nullptr);
  bool sawBuild = false;
  bool sawProbe = false;
  bool sawJoinBuildWait = false;
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& stats : pipeline.operatorStats) {
      if (stats.operatorType == "ChHashBuild") {
        sawBuild = true;
      }
      if (stats.operatorType == "ChHashProbe") {
        sawProbe = true;
        EXPECT_EQ(stats.outputPositions, 4096);
        EXPECT_GE(stats.outputVectors, 4);
        sawJoinBuildWait =
            stats.runtimeStats.count("blockedWaitForJoinBuildTimes") > 0;
      }
    }
  }
  EXPECT_TRUE(sawBuild);
  EXPECT_TRUE(sawProbe);
  EXPECT_TRUE(sawJoinBuildWait);
}

TEST_F(ChHashJoinPipelineTest, downstreamFilterAndProjectConsumeJoinOutput) {
  ensureRegistered();
  auto probe = basicProbe();
  auto build = basicBuild();

  assertMatchesNative(
      makeChPlan(probe, build, true), makeNativePlan(probe, build, true));
}

} // namespace
} // namespace facebook::velox::exec::ch
