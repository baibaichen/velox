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

#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/ch/ChHashProbe.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"

#include <vector>

namespace facebook::velox::exec::ch {

class EmitGather {
 public:
  // Output columns are ordered as all build projections followed by all probe
  // projections. Each result vector contains matches from one build block.
  EmitGather(
      const RetainedVectorsIndex& retainedIndex,
      std::vector<column_index_t> buildProjections,
      std::vector<column_index_t> probeProjections,
      RowTypePtr outputType,
      memory::MemoryPool* pool);

  std::vector<RowVectorPtr> emit(
      const std::vector<ProbeMatch>& matches,
      const RowVectorPtr& probeInput) const;

 private:
  VectorPtr makeBuildColumn(
      size_t projection,
      uint32_t driverNo,
      uint32_t batchNo,
      const BufferPtr& indices,
      const std::vector<const ProbeMatch*>& matches) const;

  const RetainedVectorsIndex& retainedIndex_;
  std::vector<column_index_t> buildProjections_;
  std::vector<column_index_t> probeProjections_;
  RowTypePtr outputType_;
  memory::MemoryPool* pool_;
  EmitColumns emitColumns_;
};

} // namespace facebook::velox::exec::ch
