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

#include "velox/exec/Task.h"

namespace facebook::velox::exec::ch {
namespace {

column_index_t buildKeyChannel(const ChHashJoinNode& joinNode) {
  VELOX_CHECK_EQ(
      joinNode.rightKeys().size(),
      1,
      "ChHashBuildOperator supports one build key");
  return exprToChannel(
      joinNode.rightKeys().front().get(), joinNode.sources()[1]->outputType());
}

std::shared_ptr<ChHashJoinBridge> getJoinBridge(
    exec::OperatorCtx* operatorCtx,
    const core::PlanNodeId& planNodeId) {
  auto bridge = operatorCtx->task()->getCustomJoinBridge(
      operatorCtx->driverCtx()->splitGroupId, planNodeId);
  auto chBridge = std::dynamic_pointer_cast<ChHashJoinBridge>(bridge);
  VELOX_CHECK_NOT_NULL(chBridge);
  return chBridge;
}

} // namespace

ChHashBuildOperator::ChHashBuildOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    ChHashJoinNodePtr joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "ChHashBuild"),
      joinNode_(std::move(joinNode)),
      driverNo_(driverCtx->partitionId),
      keyChannel_(buildKeyChannel(*joinNode_)) {}

void ChHashBuildOperator::initialize() {
  Operator::initialize();
  chBuild_ = std::make_unique<ChHashBuild>(driverNo_, keyChannel_, pool());
}

bool ChHashBuildOperator::needsInput() const {
  return !noMoreInput_;
}

void ChHashBuildOperator::addInput(RowVectorPtr input) {
  VELOX_CHECK_NOT_NULL(chBuild_);
  chBuild_->addInput(std::move(input));
}

void ChHashBuildOperator::noMoreInput() {
  if (noMoreInput_) {
    return;
  }
  VELOX_CHECK_NOT_NULL(chBuild_);
  Operator::noMoreInput();
  chBuild_->noMoreInput();
  joinBridge_ = getJoinBridge(operatorCtx_.get(), planNodeId());
  joinBridge_->setChTable(chBuild_->takeMap(), chBuild_->takeRetained());
  handedOff_ = true;
}

bool ChHashBuildOperator::isFinished() {
  return noMoreInput_ && handedOff_;
}

} // namespace facebook::velox::exec::ch
