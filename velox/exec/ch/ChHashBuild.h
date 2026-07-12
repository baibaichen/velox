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
#include "velox/exec/ch/Arena.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"
#include "velox/exec/ch/RowRefList.h"

#include <folly/container/F14Map.h>

#include <cstdint>

namespace facebook::velox::exec::ch {

class ChHashBuild {
 public:
  using PlaceholderMap = folly::F14FastMap<uint64_t, RowRefList>;

  ChHashBuild(
      uint32_t driverNo,
      column_index_t keyChannel,
      memory::MemoryPool* pool);

  void addInput(RowVectorPtr input);

  void noMoreInput() {
    needsInput_ = false;
  }

  bool needsInput() const {
    return needsInput_;
  }

  const PlaceholderMap& rowsByKey() const {
    return rowsByKey_;
  }

  const RetainedVectorsIndex& retainedIndex() const {
    return retainedIndex_;
  }

 private:
  uint32_t driverNo_;
  column_index_t keyChannel_;
  RetainedVectorsIndex retainedIndex_;
  Arena arena_;
  PlaceholderMap rowsByKey_;
  bool needsInput_{true};
};

} // namespace facebook::velox::exec::ch
