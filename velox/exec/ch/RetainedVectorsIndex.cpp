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

#include "velox/exec/ch/RetainedVectorsIndex.h"

#include "velox/common/base/Exceptions.h"
#include "velox/exec/ch/RowRef.h"

#include <utility>

namespace facebook::velox::exec::ch {

RetainedVectorsIndex::RetainedVectorsIndex(uint32_t driverNo)
    : driverNo_(driverNo) {
  VELOX_CHECK_LE(
      driverNo_,
      kMaxDriverNo,
      "Driver number {} exceeds the {}-bit coordinate field",
      driverNo_,
      kDriverNoBits);
  retained_.resize(driverNo_ + 1);
}

uint32_t RetainedVectorsIndex::add(const RowVectorPtr& vector) {
  auto& batches = retained_[driverNo_];
  VELOX_CHECK_LE(
      batches.size(),
      static_cast<size_t>(kMaxBatchNo),
      "Driver {} cannot retain more than {} batches",
      driverNo_,
      static_cast<uint64_t>(kMaxBatchNo) + 1);

  const auto batchNo = static_cast<uint32_t>(batches.size());
  batches.push_back(vector);
  return batchNo;
}

const RowVector* RetainedVectorsIndex::at(
    uint32_t driverNo,
    uint32_t batchNo) const {
  VELOX_CHECK_LT(driverNo, retained_.size());
  VELOX_CHECK_LT(batchNo, retained_[driverNo].size());
  return retained_[driverNo][batchNo].get();
}

void RetainedVectorsIndex::mergeFrom(RetainedVectorsIndex&& peer) {
  if (retained_.size() <= peer.driverNo_) {
    retained_.resize(peer.driverNo_ + 1);
  }
  VELOX_CHECK(
      retained_[peer.driverNo_].empty(),
      "Cannot merge driver {} more than once",
      peer.driverNo_);
  retained_[peer.driverNo_] =
      std::move(peer.retained_[peer.driverNo_]);
}

void RetainedVectorsIndex::resolveEmitColumns(
    const std::vector<size_t>&,
    std::vector<const BaseVector* const*>&) const {}

} // namespace facebook::velox::exec::ch
