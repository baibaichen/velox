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
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/exec/ch/ChHashJoinBridge.h"
#include "velox/exec/ch/ChHashJoinNode.h"

namespace facebook::velox::exec::ch {

class ChHashBuildOperator final : public exec::Operator {
 public:
  ChHashBuildOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      ChHashJoinNodePtr joinNode);

  void initialize() override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  ChHashJoinNodePtr joinNode_;
  std::shared_ptr<ChHashJoinBridge> joinBridge_;
  uint32_t driverNo_;
  column_index_t keyChannel_;
  std::unique_ptr<ChHashBuild> chBuild_;
  bool handedOff_{false};
};

} // namespace facebook::velox::exec::ch
