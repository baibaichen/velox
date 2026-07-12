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

#include "velox/exec/ch/ChHashBuildOperator.h"
#include "velox/exec/ch/ChHashProbeOperator.h"

namespace facebook::velox::exec::ch {

void ChHashJoinBridge::setChTable(
    std::shared_ptr<JoinMap> map,
    std::shared_ptr<RetainedVectorsIndex> retained) {
  VELOX_CHECK_NOT_NULL(map);
  VELOX_CHECK_NOT_NULL(retained);

  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value(), "setChTable may be called only once");
    buildResult_ = ChBuildResult{std::move(map), std::move(retained)};
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<ChHashJoinBridge::ChBuildResult>
ChHashJoinBridge::tableOrFuture(ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(future);
  std::lock_guard<std::mutex> lock(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!cancelled_, "Getting CH hash table after join is aborted");
  if (buildResult_.has_value()) {
    return buildResult_;
  }
  promises_.emplace_back("ChHashJoinBridge::tableOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

std::unique_ptr<exec::Operator> ChHashJoinTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto joinNode =
          std::dynamic_pointer_cast<const ChHashJoinNode>(node)) {
    return std::make_unique<ChHashProbeOperator>(id, ctx, std::move(joinNode));
  }
  return nullptr;
}

std::unique_ptr<exec::JoinBridge> ChHashJoinTranslator::toJoinBridge(
    const core::PlanNodePtr& node) {
  if (std::dynamic_pointer_cast<const ChHashJoinNode>(node)) {
    return std::make_unique<ChHashJoinBridge>();
  }
  return nullptr;
}

exec::OperatorSupplier ChHashJoinTranslator::toOperatorSupplier(
    const core::PlanNodePtr& node) {
  if (auto joinNode =
          std::dynamic_pointer_cast<const ChHashJoinNode>(node)) {
    return [joinNode = std::move(joinNode)](
               int32_t operatorId, exec::DriverCtx* ctx) {
      return std::make_unique<ChHashBuildOperator>(
          operatorId, ctx, joinNode);
    };
  }
  return nullptr;
}

void registerChHashJoin() {
  exec::Operator::registerOperator(
      std::make_unique<ChHashJoinTranslator>());
}

} // namespace facebook::velox::exec::ch
