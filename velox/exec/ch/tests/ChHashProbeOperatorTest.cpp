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

#include "velox/exec/ch/ChHashProbeOperator.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

class ChHashProbeOperatorTest : public testing::Test,
                                public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    core::PlanFragment plan;
    plan.planNode = std::make_shared<core::ValuesNode>(
        "taskValues",
        std::vector<RowVectorPtr>{
            makeRowVector({makeFlatVector<int64_t>({1})})});
    task_ = Task::create(
        "ChHashProbeOperatorTest",
        std::move(plan),
        0,
        core::QueryCtx::create(executor_.get()),
        Task::ExecutionMode::kParallel);
    driver_ = Driver::testingCreate();
    driverCtx_ = std::make_unique<DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
  }

  ChHashJoinNodePtr makeJoinNode() {
    auto probeNode = std::make_shared<core::ValuesNode>(
        "probe",
        std::vector<RowVectorPtr>{makeProbeInput({1}, {10})});
    auto buildNode = std::make_shared<core::ValuesNode>(
        "build",
        std::vector<RowVectorPtr>{makeBuildInput({1}, {100})});
    auto probeKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "probeKey");
    auto buildKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "buildKey");
    return std::make_shared<ChHashJoinNode>(
        "join",
        core::JoinType::kInner,
        std::vector<core::FieldAccessTypedExprPtr>{probeKey},
        std::vector<core::FieldAccessTypedExprPtr>{buildKey},
        nullptr,
        std::move(probeNode),
        std::move(buildNode),
        ROW(
            {"buildKey", "buildPayload", "probePayload"},
            {BIGINT(), BIGINT(), BIGINT()}));
  }

  RowVectorPtr makeBuildInput(
      std::vector<int64_t> keys,
      std::vector<int64_t> payloads) {
    return makeRowVector(
        {"buildKey", "buildPayload"},
        {
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(payloads),
        });
  }

  RowVectorPtr makeProbeInput(
      std::vector<std::optional<int64_t>> keys,
      std::vector<int64_t> payloads) {
    return makeRowVector(
        {"probeKey", "probePayload"},
        {
            makeNullableFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(payloads),
        });
  }

  std::pair<
      std::shared_ptr<ChHashJoinBridge::JoinMap>,
      std::shared_ptr<RetainedVectorsIndex>>
  makeBuildTable(const std::vector<RowVectorPtr>& inputs) {
    ChHashBuild build(0, 0, pool());
    for (const auto& input : inputs) {
      build.addInput(input);
    }
    build.noMoreInput();
    return {build.takeMap(), build.takeRetained()};
  }

  std::unique_ptr<ChHashProbeOperator> makeOperator(
      const std::shared_ptr<ChHashJoinBridge>& bridge) {
    auto probe = std::make_unique<ChHashProbeOperator>(
        0, driverCtx_.get(), makeJoinNode(), bridge);
    probe->initialize();
    return probe;
  }

  template <typename T>
  static std::vector<T> values(const VectorPtr& vector) {
    auto flat = BaseVector::copy(*vector, vector->pool());
    std::vector<T> result;
    result.reserve(flat->size());
    for (vector_size_t row = 0; row < flat->size(); ++row) {
      result.push_back(flat->as<SimpleVector<T>>()->valueAt(row));
    }
    return result;
  }

 private:
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<Task> task_;
  std::shared_ptr<Driver> driver_;
  std::unique_ptr<DriverCtx> driverCtx_;
};

TEST_F(ChHashProbeOperatorTest, waitsForBuildTableBeforeAcceptingInput) {
  auto bridge = std::make_shared<ChHashJoinBridge>();
  bridge->start();
  auto probe = makeOperator(bridge);
  ContinueFuture future = ContinueFuture::makeEmpty();

  EXPECT_EQ(
      probe->isBlocked(&future), BlockingReason::kWaitForJoinBuild);
  EXPECT_TRUE(future.valid());
  EXPECT_FALSE(future.isReady());
  EXPECT_FALSE(probe->needsInput());

  auto [map, retained] =
      makeBuildTable({makeBuildInput({1}, {100})});
  bridge->setChTable(std::move(map), std::move(retained));

  EXPECT_TRUE(future.isReady());
  ContinueFuture readyFuture = ContinueFuture::makeEmpty();
  EXPECT_EQ(
      probe->isBlocked(&readyFuture), BlockingReason::kNotBlocked);
  EXPECT_TRUE(probe->needsInput());
}

TEST_F(ChHashProbeOperatorTest, emitsAndDrainsOneBatchPerBuildBlock) {
  auto bridge = std::make_shared<ChHashJoinBridge>();
  bridge->start();
  auto [map, retained] = makeBuildTable({
      makeBuildInput({20, 20, 10}, {200, 201, 100}),
      makeBuildInput({20, 30, 10}, {202, 300, 101}),
  });
  bridge->setChTable(std::move(map), std::move(retained));
  auto probe = makeOperator(bridge);
  ContinueFuture future = ContinueFuture::makeEmpty();
  ASSERT_EQ(
      probe->isBlocked(&future), BlockingReason::kNotBlocked);

  probe->addInput(
      makeProbeInput({20, std::nullopt, 99, 10}, {2, 3, 4, 5}));
  probe->noMoreInput();
  EXPECT_FALSE(probe->isFinished());

  auto first = probe->getOutput();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(
      values<int64_t>(first->childAt(0)),
      (std::vector<int64_t>{20, 20, 10}));
  EXPECT_EQ(
      values<int64_t>(first->childAt(1)),
      (std::vector<int64_t>{200, 201, 100}));
  EXPECT_EQ(
      values<int64_t>(first->childAt(2)),
      (std::vector<int64_t>{2, 2, 5}));
  EXPECT_FALSE(probe->isFinished());

  auto second = probe->getOutput();
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(
      values<int64_t>(second->childAt(0)),
      (std::vector<int64_t>{20, 10}));
  EXPECT_EQ(
      values<int64_t>(second->childAt(1)),
      (std::vector<int64_t>{202, 101}));
  EXPECT_EQ(
      values<int64_t>(second->childAt(2)),
      (std::vector<int64_t>{2, 5}));
  EXPECT_TRUE(probe->isFinished());
  EXPECT_EQ(probe->getOutput(), nullptr);
}

TEST_F(ChHashProbeOperatorTest, skipsUnmatchedAndNullProbeKeys) {
  auto bridge = std::make_shared<ChHashJoinBridge>();
  bridge->start();
  auto [map, retained] =
      makeBuildTable({makeBuildInput({1}, {100})});
  bridge->setChTable(std::move(map), std::move(retained));
  auto probe = makeOperator(bridge);
  ContinueFuture future = ContinueFuture::makeEmpty();
  ASSERT_EQ(
      probe->isBlocked(&future), BlockingReason::kNotBlocked);

  probe->addInput(makeProbeInput({99, std::nullopt}, {1, 2}));

  EXPECT_EQ(probe->getOutput(), nullptr);
  EXPECT_TRUE(probe->needsInput());
  probe->noMoreInput();
  EXPECT_TRUE(probe->isFinished());
}

} // namespace
} // namespace facebook::velox::exec::ch
