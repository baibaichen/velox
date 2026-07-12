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

#include "velox/exec/ch/ChHashBuild.h"

#include "velox/common/base/Exceptions.h"
#include "velox/exec/ch/RowRef.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec::ch {

ChHashBuild::ChHashBuild(
    uint32_t driverNo,
    column_index_t keyChannel,
    memory::MemoryPool* pool)
    : driverNo_(driverNo),
      keyChannel_(keyChannel),
      retainedIndex_(driverNo),
      arena_(pool) {}

void ChHashBuild::addInput(RowVectorPtr input) {
  VELOX_CHECK(needsInput_, "Cannot add input after noMoreInput");
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_LT(keyChannel_, input->childrenSize());

  auto keyVector = input->childAt(keyChannel_)->loadedVector();
  VELOX_CHECK_EQ(
      keyVector->typeKind(),
      TypeKind::BIGINT,
      "ChHashBuild placeholder map supports one BIGINT key channel");

  SelectivityVector rows(input->size());
  DecodedVector decodedKey(*keyVector, rows);
  const uint32_t batchNo = retainedIndex_.add(input);
  const uint32_t blockNo = packBlockNo(driverNo_, batchNo);

  rows.applyToSelected([&](vector_size_t rowNo) {
    if (decodedKey.isNullAt(rowNo)) {
      return;
    }

    const uint64_t key =
        static_cast<uint64_t>(decodedKey.valueAt<int64_t>(rowNo));
    const uint64_t refWord =
        RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode();
    rowsByKey_[key].insert(refWord, arena_);
  });
}

} // namespace facebook::velox::exec::ch
