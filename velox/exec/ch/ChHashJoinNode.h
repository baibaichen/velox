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

#include "velox/core/PlanNode.h"

namespace facebook::velox::exec::ch {

class ChHashJoinNode : public core::PlanNode {
 public:
  ChHashJoinNode(
      const core::PlanNodeId& id,
      core::JoinType joinType,
      std::vector<core::FieldAccessTypedExprPtr> leftKeys,
      std::vector<core::FieldAccessTypedExprPtr> rightKeys,
      core::TypedExprPtr filter,
      core::PlanNodePtr left,
      core::PlanNodePtr right,
      RowTypePtr outputType)
      : PlanNode(id),
        joinType_(joinType),
        leftKeys_(std::move(leftKeys)),
        rightKeys_(std::move(rightKeys)),
        filter_(std::move(filter)),
        sources_{std::move(left), std::move(right)},
        outputType_(std::move(outputType)) {
    VELOX_CHECK_NOT_NULL(sources_[0]);
    VELOX_CHECK_NOT_NULL(sources_[1]);
    VELOX_CHECK_NOT_NULL(outputType_);
    VELOX_CHECK_EQ(leftKeys_.size(), rightKeys_.size());
  }

  const RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "ChHashJoin";
  }

  core::JoinType joinType() const {
    return joinType_;
  }

  const std::vector<core::FieldAccessTypedExprPtr>& leftKeys() const {
    return leftKeys_;
  }

  const std::vector<core::FieldAccessTypedExprPtr>& rightKeys() const {
    return rightKeys_;
  }

  const core::TypedExprPtr& filter() const {
    return filter_;
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("ChHashJoinNode serialize");
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "joinType: " << core::JoinTypeName::toName(joinType_);
  }

  const core::JoinType joinType_;
  const std::vector<core::FieldAccessTypedExprPtr> leftKeys_;
  const std::vector<core::FieldAccessTypedExprPtr> rightKeys_;
  const core::TypedExprPtr filter_;
  const std::vector<core::PlanNodePtr> sources_;
  const RowTypePtr outputType_;
};

using ChHashJoinNodePtr = std::shared_ptr<const ChHashJoinNode>;

} // namespace facebook::velox::exec::ch
