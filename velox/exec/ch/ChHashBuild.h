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
#include "velox/exec/ch/FixedKeyMap.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec::ch {

class ChHashBuild {
 public:
  using JoinMap = FixedKeyMap;

  ChHashBuild(
      uint32_t driverNo,
      column_index_t keyChannel,
      memory::MemoryPool* pool);

  ChHashBuild(
      uint32_t driverNo,
      std::vector<column_index_t> keyChannels,
      std::vector<TypePtr> keyTypes,
      memory::MemoryPool* pool);

  ChHashBuild(
      uint32_t driverNo,
      std::vector<column_index_t> keyChannels,
      std::vector<TypePtr> keyTypes,
      memory::MemoryPool* pool,
      ArbitraryKeyMode arbitraryMode);

  void reserve(size_t expectedDistinctKeys);

  void prepareJoinTable(
      const DecodedVector& decodedKey,
      const SelectivityVector& rows);

  void addRowReferences(
      RowVectorPtr input,
      const DecodedVector& decodedKey,
      const SelectivityVector& rows);

  void addInput(RowVectorPtr input);

  void noMoreInput() {
    needsInput_ = false;
  }

  bool needsInput() const {
    return needsInput_;
  }

  const JoinMap& rowsByKey() const {
    VELOX_CHECK_NOT_NULL(storage_);
    return storage_->rowsByKey;
  }

  const RetainedVectorsIndex& retainedIndex() const {
    VELOX_CHECK_NOT_NULL(retainedIndex_);
    return *retainedIndex_;
  }

  const std::vector<column_index_t>& keyChannels() const {
    return keyChannels_;
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

  FixedKeyWidth keyWidth() const {
    return rowsByKey().width();
  }

  bool usesSerializedKeys() const {
    return rowsByKey().serialized();
  }

  bool usesHashedKeys() const {
    return rowsByKey().hashed();
  }

  std::shared_ptr<JoinMap> takeMap();

  std::shared_ptr<RetainedVectorsIndex> takeRetained();

 private:
  struct BuildStorage {
    BuildStorage(
        memory::MemoryPool* pool,
        const std::vector<TypePtr>& keyTypes,
        ArbitraryKeyMode arbitraryMode)
        : arena(pool), rowsByKey(pool, keyTypes, arbitraryMode) {}

    Arena arena;
    JoinMap rowsByKey;
  };

  uint32_t driverNo_;
  std::vector<column_index_t> keyChannels_;
  std::vector<TypePtr> keyTypes_;
  std::shared_ptr<RetainedVectorsIndex> retainedIndex_;
  std::shared_ptr<BuildStorage> storage_;
  bool needsInput_{true};
};

} // namespace facebook::velox::exec::ch
