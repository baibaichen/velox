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

#include "velox/exec/Task.h"

namespace facebook::velox::exec::ch {
namespace {

column_index_t probeKeyChannel(const ChHashJoinNode& joinNode) {
  VELOX_CHECK_EQ(
      joinNode.leftKeys().size(),
      1,
      "ChHashProbeOperator supports one probe key");
  return exprToChannel(
      joinNode.leftKeys().front().get(), joinNode.sources()[0]->outputType());
}

void resolveOutputProjections(
    const ChHashJoinNode& joinNode,
    std::vector<column_index_t>& buildProjections,
    std::vector<column_index_t>& probeProjections) {
  const auto& buildType = joinNode.sources()[1]->outputType();
  const auto& probeType = joinNode.sources()[0]->outputType();
  bool sawProbeProjection = false;

  for (const auto& name : joinNode.outputType()->names()) {
    const auto buildChannel = buildType->getChildIdxIfExists(name);
    const auto probeChannel = probeType->getChildIdxIfExists(name);
    VELOX_CHECK(
        buildChannel.has_value() != probeChannel.has_value(),
        "Output column '{}' must resolve to exactly one join input",
        name);

    if (buildChannel.has_value()) {
      VELOX_CHECK(
          !sawProbeProjection,
          "ChHashProbeOperator requires build output columns before probe "
          "output columns");
      buildProjections.push_back(*buildChannel);
    } else {
      sawProbeProjection = true;
      probeProjections.push_back(*probeChannel);
    }
  }
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

ChHashProbeOperator::ChHashProbeOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    ChHashJoinNodePtr joinNode)
    : ChHashProbeOperator(
          operatorId, driverCtx, std::move(joinNode), nullptr) {}

ChHashProbeOperator::ChHashProbeOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    ChHashJoinNodePtr joinNode,
    std::shared_ptr<ChHashJoinBridge> joinBridge)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "ChHashProbe"),
      joinNode_(std::move(joinNode)),
      joinBridge_(std::move(joinBridge)),
      keyChannel_(probeKeyChannel(*joinNode_)) {
  VELOX_CHECK_EQ(
      joinNode_->joinType(),
      core::JoinType::kInner,
      "ChHashProbeOperator supports inner joins");
  VELOX_CHECK_NULL(
      joinNode_->filter(), "ChHashProbeOperator does not support join filters");
  resolveOutputProjections(
      *joinNode_, buildProjections_, probeProjections_);
}

bool ChHashProbeOperator::needsInput() const {
  return tableReady_ && input_ == nullptr && pendingOutput_.empty() &&
      !noMoreInput_;
}

void ChHashProbeOperator::addInput(RowVectorPtr input) {
  VELOX_CHECK(needsInput());
  VELOX_CHECK_NOT_NULL(input);
  input_ = std::move(input);
}

void ChHashProbeOperator::noMoreInput() {
  if (!noMoreInput_) {
    Operator::noMoreInput();
  }
}

RowVectorPtr ChHashProbeOperator::nextPendingOutput() {
  if (nextOutput_ >= pendingOutput_.size()) {
    return nullptr;
  }

  auto output = std::move(pendingOutput_[nextOutput_++]);
  if (nextOutput_ == pendingOutput_.size()) {
    pendingOutput_.clear();
    nextOutput_ = 0;
  }
  return output;
}

RowVectorPtr ChHashProbeOperator::getOutput() {
  if (auto output = nextPendingOutput()) {
    return output;
  }
  if (input_ == nullptr) {
    return nullptr;
  }

  VELOX_CHECK(tableReady_);
  VELOX_CHECK_NOT_NULL(buildTable_.map);
  VELOX_CHECK_NOT_NULL(buildTable_.retained);
  VELOX_CHECK(emitGather_.has_value());

  auto input = std::move(input_);
  auto hits = joinProbe(*buildTable_.map, input, keyChannel_);
  auto matches = listJoinResults(hits, *buildTable_.retained);
  pendingOutput_ = emitGather_->emit(matches, input);
  return nextPendingOutput();
}

BlockingReason ChHashProbeOperator::isBlocked(ContinueFuture* future) {
  if (tableReady_) {
    return BlockingReason::kNotBlocked;
  }
  if (joinBridge_ == nullptr) {
    joinBridge_ = getJoinBridge(operatorCtx_.get(), planNodeId());
  }

  auto result = joinBridge_->tableOrFuture(future);
  if (!result.has_value()) {
    return BlockingReason::kWaitForJoinBuild;
  }

  buildTable_ = std::move(*result);
  VELOX_CHECK_NOT_NULL(buildTable_.map);
  VELOX_CHECK_NOT_NULL(buildTable_.retained);
  emitGather_.emplace(
      *buildTable_.retained,
      buildProjections_,
      probeProjections_,
      outputType_,
      pool());
  tableReady_ = true;
  return BlockingReason::kNotBlocked;
}

bool ChHashProbeOperator::isFinished() {
  return noMoreInput_ && input_ == nullptr && pendingOutput_.empty();
}

} // namespace facebook::velox::exec::ch
