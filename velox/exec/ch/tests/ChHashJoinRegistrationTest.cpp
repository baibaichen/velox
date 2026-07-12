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
#include "velox/exec/ch/ChHashJoinNode.h"

#include "velox/common/memory/Memory.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <folly/synchronization/CallOnce.h>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class ChHashJoinRegistrationTest : public testing::Test,
                                   public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
    // Register the Ch hash-join translator once for the whole suite. This does
    // NOT wipe the process-global translator registry (no
    // unregisterAllOperators()), so it will not pollute other tests running
    // later in the same binary.
    static folly::once_flag registerFlag;
    folly::call_once(registerFlag, [] { registerChHashJoin(); });
  }

  core::PlanNodePtr valuesNode(
      const std::string& id,
      const std::string& keyName) {
    return std::make_shared<core::ValuesNode>(
        id,
        std::vector<RowVectorPtr>{
            makeRowVector({keyName}, {makeFlatVector<int64_t>({1, 2, 3})})});
  }

  std::shared_ptr<const ChHashJoinNode> makeJoinNode() {
    auto left = valuesNode("left", "leftKey");
    auto right = valuesNode("right", "rightKey");
    auto leftKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "leftKey");
    auto rightKey =
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "rightKey");
    return std::make_shared<ChHashJoinNode>(
        "join",
        core::JoinType::kInner,
        std::vector<core::FieldAccessTypedExprPtr>{leftKey},
        std::vector<core::FieldAccessTypedExprPtr>{rightKey},
        nullptr,
        left,
        right,
        ROW({"leftKey", "rightKey"}, {BIGINT(), BIGINT()}));
  }
};

TEST_F(ChHashJoinRegistrationTest, exposesJoinPlanContract) {
  auto node = makeJoinNode();

  EXPECT_EQ(node->name(), "ChHashJoin");
  EXPECT_EQ(node->joinType(), core::JoinType::kInner);
  ASSERT_EQ(node->leftKeys().size(), 1);
  ASSERT_EQ(node->rightKeys().size(), 1);
  EXPECT_EQ(node->leftKeys()[0]->name(), "leftKey");
  EXPECT_EQ(node->rightKeys()[0]->name(), "rightKey");
  EXPECT_EQ(node->filter(), nullptr);
  auto expectedType = ROW({"leftKey", "rightKey"}, {BIGINT(), BIGINT()});
  EXPECT_TRUE(node->outputType()->equivalent(*expectedType));
  ASSERT_EQ(node->sources().size(), 2);
  EXPECT_EQ(node->sources()[0]->id(), "left");
  EXPECT_EQ(node->sources()[1]->id(), "right");
  EXPECT_ANY_THROW(node->serialize());
}

TEST_F(ChHashJoinRegistrationTest, translatorCreatesBridgeAndBuildSupplier) {
  auto node = makeJoinNode();
  ChHashJoinTranslator translator;

  auto bridge = translator.toJoinBridge(node);
  ASSERT_NE(bridge, nullptr);
  EXPECT_NE(dynamic_cast<ChHashJoinBridge*>(bridge.get()), nullptr);
  EXPECT_NE(translator.toOperatorSupplier(node), nullptr);
  EXPECT_EQ(translator.toJoinBridge(node->sources()[0]), nullptr);
  EXPECT_EQ(translator.toOperatorSupplier(node->sources()[0]), nullptr);
  EXPECT_EQ(translator.maxDrivers(node), 1);
  EXPECT_EQ(translator.maxDrivers(node->sources()[0]), std::nullopt);

  // Exercise the probe toOperator dispatch. Constructing the operator for the
  // matching node would require a live DriverCtx (a full Task/Driver), which is
  // out of scope at this placeholder stage; the non-Ch node path returns before
  // touching the ctx, so it can be exercised cleanly with a null ctx.
  EXPECT_EQ(translator.toOperator(nullptr, 0, node->sources()[0]), nullptr);
}

TEST_F(ChHashJoinRegistrationTest, registrationMakesBridgeDiscoverable) {
  // The translator was registered once in SetUpTestSuite() without clearing the
  // global registry, so the bridge must be discoverable via the registry.
  auto bridge = Operator::joinBridgeFromPlanNode(makeJoinNode());
  ASSERT_NE(bridge, nullptr);
  EXPECT_NE(dynamic_cast<ChHashJoinBridge*>(bridge.get()), nullptr);
}

TEST_F(ChHashJoinRegistrationTest, bridgeWakesWaiterAndPreservesTable) {
  ChHashJoinBridge bridge;
  bridge.start();
  ContinueFuture future = ContinueFuture::makeEmpty();

  EXPECT_FALSE(bridge.tableOrFuture(&future).has_value());
  EXPECT_TRUE(future.valid());
  EXPECT_FALSE(future.isReady());

  auto map = std::make_shared<ChHashJoinBridge::JoinMap>(pool());
  auto retained = std::make_shared<RetainedVectorsIndex>(0);
  bridge.setChTable(map, retained);

  EXPECT_TRUE(future.isReady());
  auto result = bridge.tableOrFuture(&future);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->map, map);
  EXPECT_EQ(result->retained, retained);
}

} // namespace
} // namespace facebook::velox::exec::ch
