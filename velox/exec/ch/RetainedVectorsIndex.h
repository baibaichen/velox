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

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace facebook::velox::exec::ch {

struct EmitColumns {
  std::vector<std::vector<std::vector<const BaseVector*>>> emit;
};

class RetainedVectorsIndex {
 public:
  explicit RetainedVectorsIndex(uint32_t driverNo);

  uint32_t add(const RowVectorPtr& vector);

  const RowVector* at(uint32_t driverNo, uint32_t batchNo) const;

  void mergeFrom(RetainedVectorsIndex&& peer);

  EmitColumns resolveEmitColumns(
      const std::vector<column_index_t>& positions) const;

 private:
  uint32_t driverNo_;
  std::vector<std::vector<RowVectorPtr>> retained_;
};

using RetainedVectorsIndexPtr = std::shared_ptr<RetainedVectorsIndex>;

} // namespace facebook::velox::exec::ch
