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

#include "velox/exec/ch/ChHashBuildOperator.h"
#include "velox/exec/ch/ChHashJoinBridge.h"
#include "velox/exec/ch/RowRef.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

struct TestBuildState {
  std::optional<ChHashJoinBridge::ChBuildResult> result;
};

class TestJoinNode final : public core::PlanNode {
 public:
  TestJoinNode(ChHashJoinNodePtr delegate, std::weak_ptr<TestBuildState> state)
      : PlanNode(delegate->id()),
        delegate_(std::move(delegate)),
        state_(std::move(state)) {}

  const RowTypePtr& outputType() const override {
    return delegate_->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return delegate_->sources();
  }

  std::string_view name() const override {
    return "TestChHashJoin";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("TestJoinNode serialize");
  }

  const ChHashJoinNodePtr& delegate() const {
    return delegate_;
  }

  std::weak_ptr<TestBuildState> state() const {
    return state_;
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  ChHashJoinNodePtr delegate_;
  std::weak_ptr<TestBuildState> state_;
};

class TestProbeOperator final : public exec::Operator {
 public:
  TestProbeOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const TestJoinNode> joinNode)
      : Operator(
            driverCtx,
            joinNode->outputType(),
            operatorId,
            joinNode->id(),
            "TestChHashProbe"),
        joinNode_(std::move(joinNode)) {}

  bool needsInput() const override {
    return false;
  }

  void addInput(RowVectorPtr /* input */) override {}

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (finished_) {
      return BlockingReason::kNotBlocked;
    }
    if (joinBridge_ == nullptr) {
      joinBridge_ = std::dynamic_pointer_cast<ChHashJoinBridge>(
          operatorCtx_->task()->getCustomJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId()));
      VELOX_CHECK_NOT_NULL(joinBridge_);
    }

    auto result = joinBridge_->tableOrFuture(future);
    if (!result.has_value()) {
      return BlockingReason::kWaitForJoinBuild;
    }
    auto state = joinNode_->state().lock();
    VELOX_CHECK_NOT_NULL(state);
    state->result = std::move(result);
    finished_ = true;
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  std::shared_ptr<const TestJoinNode> joinNode_;
  std::shared_ptr<ChHashJoinBridge> joinBridge_;
  bool finished_{false};
};

class TestJoinTranslator final : public exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto joinNode = std::dynamic_pointer_cast<const TestJoinNode>(node)) {
      return std::make_unique<TestProbeOperator>(id, ctx, std::move(joinNode));
    }
    return nullptr;
  }

  std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const core::PlanNodePtr& node) override {
    return std::dynamic_pointer_cast<const TestJoinNode>(node)
        ? std::make_unique<ChHashJoinBridge>()
        : nullptr;
  }

  exec::OperatorSupplier toOperatorSupplier(
      const core::PlanNodePtr& node) override {
    if (auto joinNode = std::dynamic_pointer_cast<const TestJoinNode>(node)) {
      return [delegate = joinNode->delegate()](
                 int32_t operatorId, exec::DriverCtx* ctx) {
        return std::make_unique<ChHashBuildOperator>(operatorId, ctx, delegate);
      };
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    return std::dynamic_pointer_cast<const TestJoinNode>(node)
        ? std::optional<uint32_t>{1}
        : std::nullopt;
  }
};

class ChHashBuildOperatorTest : public testing::Test,
                                public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
    exec::Operator::registerOperator(std::make_unique<TestJoinTranslator>());
  }

  core::PlanNodePtr makeJoinNode(
      const RowVectorPtr& probe,
      const RowVectorPtr& build,
      const std::shared_ptr<TestBuildState>& state) {
    auto probeNode = std::make_shared<core::ValuesNode>(
        "probe", std::vector<RowVectorPtr>{probe}, true);
    auto buildNode = std::make_shared<core::ValuesNode>(
        "build", std::vector<RowVectorPtr>{build}, true);
    auto probeKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "probeKey");
    auto buildKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "buildKey");
    auto delegate = std::make_shared<ChHashJoinNode>(
        "join",
        core::JoinType::kInner,
        std::vector<core::FieldAccessTypedExprPtr>{probeKey},
        std::vector<core::FieldAccessTypedExprPtr>{buildKey},
        nullptr,
        std::move(probeNode),
        std::move(buildNode),
        ROW({"probeKey", "buildKey", "payload"},
            {BIGINT(), BIGINT(), BIGINT()}));
    return std::make_shared<TestJoinNode>(
        std::move(delegate), std::weak_ptr<TestBuildState>{state});
  }
};

TEST_F(ChHashBuildOperatorTest, handsBuiltTableToBridgeAndFinishesAfterInput) {
  auto probe = makeRowVector({"probeKey"}, {makeFlatVector<int64_t>({7, 11})});
  auto build = makeRowVector(
      {"buildKey", "payload"},
      {
          makeFlatVector<int64_t>({7, 7, 11}),
          makeFlatVector<int64_t>({70, 71, 110}),
      });
  std::shared_ptr<Task> task;
  auto state = std::make_shared<TestBuildState>();
  auto joinNode = makeJoinNode(probe, build, state);
  auto output =
      exec::test::AssertQueryBuilder(joinNode).maxDrivers(4).copyResults(
          pool(), task);

  EXPECT_EQ(output->size(), 0);
  ASSERT_NE(task, nullptr);
  ASSERT_TRUE(state->result.has_value());
  auto result = state->result;
  ASSERT_NE(result->map, nullptr);
  ASSERT_NE(result->retained, nullptr);

  const auto* cell = result->map->find(uint64_t{7});
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->getMapped().rows(), 2);

  std::vector<int64_t> payloads;
  for (const auto refWord : cell->getMapped()) {
    const auto blockNo = refWordBlockNo(refWord);
    EXPECT_EQ(unpackDriverNo(blockNo), 0);
    const auto* retained =
        result->retained->at(unpackDriverNo(blockNo), unpackBatchNo(blockNo));
    payloads.push_back(retained->childAt(1)->asFlatVector<int64_t>()->valueAt(
        refWordRowNo(refWord)));
  }
  EXPECT_EQ(payloads, (std::vector<int64_t>{70, 71}));

  result.reset();
  state->result.reset();
  state.reset();
  task.reset();
}

} // namespace
} // namespace facebook::velox::exec::ch
