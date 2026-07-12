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

#include "velox/exec/JoinBridge.h"
#include "velox/exec/Operator.h"
#include "velox/exec/ch/ChHashJoinNode.h"
#include "velox/exec/ch/HashMap.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"

namespace facebook::velox::exec::ch {

class ChHashJoinBridge : public exec::JoinBridge {
 public:
  using JoinMap = HashMapAll_key64;

  struct ChBuildResult {
    std::shared_ptr<JoinMap> map;
    std::shared_ptr<RetainedVectorsIndex> retained;
  };

  void setChTable(
      std::shared_ptr<JoinMap> map,
      std::shared_ptr<RetainedVectorsIndex> retained);

  std::optional<ChBuildResult> tableOrFuture(ContinueFuture* future);

 private:
  std::optional<ChBuildResult> buildResult_;
};

class ChHashJoinTranslator : public exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override;

  std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const core::PlanNodePtr& node) override;

  exec::OperatorSupplier toOperatorSupplier(
      const core::PlanNodePtr& node) override;
};

void registerChHashJoin();

} // namespace facebook::velox::exec::ch
