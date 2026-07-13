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

namespace facebook::velox::exec::ch {

ChHashBuild::ChHashBuild(
    uint32_t driverNo,
    column_index_t keyChannel,
    memory::MemoryPool* pool)
    : driverNo_(driverNo),
      keyChannel_(keyChannel),
      retainedIndex_(std::make_shared<RetainedVectorsIndex>(driverNo)),
      storage_(std::make_shared<BuildStorage>(pool)) {}

void ChHashBuild::prepareJoinTable(
    const DecodedVector& decodedKey,
    const SelectivityVector& rows) {
  VELOX_CHECK(needsInput_, "Cannot prepare table after noMoreInput");
  VELOX_CHECK_EQ(
      decodedKey.base()->typeKind(),
      TypeKind::BIGINT,
      "ChHashBuild supports one BIGINT key channel");

  rows.applyToSelected([&](vector_size_t rowNo) {
    if (decodedKey.isNullAt(rowNo)) {
      return;
    }

    const auto key =
        static_cast<uint64_t>(decodedKey.valueAt<int64_t>(rowNo));
    storage_->rowsByKey.emplace(key);
  });
}

void ChHashBuild::addRowReferences(
    RowVectorPtr input,
    const DecodedVector& decodedKey,
    const SelectivityVector& rows) {
  VELOX_CHECK(needsInput_, "Cannot add row references after noMoreInput");
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_EQ(rows.size(), input->size());
  VELOX_CHECK_EQ(
      decodedKey.base()->typeKind(),
      TypeKind::BIGINT,
      "ChHashBuild supports one BIGINT key channel");

  const uint32_t batchNo = retainedIndex_->add(std::move(input));
  const uint32_t blockNo = packBlockNo(driverNo_, batchNo);
  rows.applyToSelected([&](vector_size_t rowNo) {
    if (decodedKey.isNullAt(rowNo)) {
      return;
    }

    const auto key =
        static_cast<uint64_t>(decodedKey.valueAt<int64_t>(rowNo));
    auto* cell = storage_->rowsByKey.find(key, storage_->rowsByKey.hash(key));
    VELOX_CHECK_NOT_NULL(
        cell, "Key must be prepared before adding row references");
    const auto refWord =
        RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode();
    cell->getMapped().insert(refWord, storage_->arena);
  });
}

void ChHashBuild::addInput(RowVectorPtr input) {
  VELOX_CHECK(needsInput_, "Cannot add input after noMoreInput");
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK_LT(keyChannel_, input->childrenSize());

  auto keyVector = input->childAt(keyChannel_)->loadedVector();
  VELOX_CHECK_EQ(
      keyVector->typeKind(),
      TypeKind::BIGINT,
      "ChHashBuild supports one BIGINT key channel");

  SelectivityVector rows(input->size());
  DecodedVector decodedKey(*keyVector, rows);
  prepareJoinTable(decodedKey, rows);
  addRowReferences(std::move(input), decodedKey, rows);
}

std::shared_ptr<ChHashBuild::JoinMap> ChHashBuild::takeMap() {
  VELOX_CHECK(!needsInput_, "Cannot take map before noMoreInput");
  VELOX_CHECK_NOT_NULL(storage_, "Map has already been taken");

  auto map = std::shared_ptr<JoinMap>(storage_, &storage_->rowsByKey);
  storage_.reset();
  return map;
}

std::shared_ptr<RetainedVectorsIndex> ChHashBuild::takeRetained() {
  VELOX_CHECK(!needsInput_, "Cannot take retained vectors before noMoreInput");
  VELOX_CHECK_NOT_NULL(
      retainedIndex_, "Retained vectors have already been taken");
  return std::move(retainedIndex_);
}

} // namespace facebook::velox::exec::ch
