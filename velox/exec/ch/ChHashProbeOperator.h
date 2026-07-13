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

#pragma once

#include "velox/exec/Operator.h"
#include "velox/exec/ch/ChHashJoinBridge.h"
#include "velox/exec/ch/ChHashJoinNode.h"
#include "velox/exec/ch/EmitGather.h"

namespace facebook::velox::exec::ch {

class ChHashProbeOperator final : public exec::Operator {
 public:
  ChHashProbeOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      ChHashJoinNodePtr joinNode);

  ChHashProbeOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      ChHashJoinNodePtr joinNode,
      std::shared_ptr<ChHashJoinBridge> joinBridge);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 private:
  RowVectorPtr nextPendingOutput();

  ChHashJoinNodePtr joinNode_;
  std::shared_ptr<ChHashJoinBridge> joinBridge_;
  ChHashJoinBridge::ChBuildResult buildTable_;
  std::vector<column_index_t> keyChannels_;
  std::vector<column_index_t> buildProjections_;
  std::vector<column_index_t> probeProjections_;
  std::optional<EmitGather> emitGather_;
  RowVectorPtr input_;
  std::vector<RowVectorPtr> pendingOutput_;
  size_t nextOutput_{0};
  bool tableReady_{false};
};

} // namespace facebook::velox::exec::ch
