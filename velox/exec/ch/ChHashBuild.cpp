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
#include "velox/exec/ch/HashedKey.h"
#include "velox/exec/ch/RowRef.h"
#include "velox/exec/ch/SerializedKey.h"

namespace facebook::velox::exec::ch {

namespace {

StringRef stringRef(const std::string& bytes) {
  VELOX_CHECK_LE(bytes.size(), std::numeric_limits<uint32_t>::max());
  return {bytes.data(), static_cast<uint32_t>(bytes.size())};
}

} // namespace

ChHashBuild::ChHashBuild(
    uint32_t driverNo,
    column_index_t keyChannel,
    memory::MemoryPool* pool)
    : ChHashBuild(driverNo, {keyChannel}, {BIGINT()}, pool) {}

ChHashBuild::ChHashBuild(
    uint32_t driverNo,
    std::vector<column_index_t> keyChannels,
    std::vector<TypePtr> keyTypes,
    memory::MemoryPool* pool)
    : ChHashBuild(
          driverNo,
          std::move(keyChannels),
          std::move(keyTypes),
          pool,
          ArbitraryKeyMode::kSerialized) {}

ChHashBuild::ChHashBuild(
    uint32_t driverNo,
    std::vector<column_index_t> keyChannels,
    std::vector<TypePtr> keyTypes,
    memory::MemoryPool* pool,
    ArbitraryKeyMode arbitraryMode)
    : driverNo_(driverNo),
      keyChannels_(std::move(keyChannels)),
      keyTypes_(std::move(keyTypes)),
      retainedIndex_(std::make_shared<RetainedVectorsIndex>(driverNo)),
      storage_(std::make_shared<BuildStorage>(pool, keyTypes_, arbitraryMode)) {
  VELOX_USER_CHECK_EQ(keyChannels_.size(), keyTypes_.size());
}
void ChHashBuild::reserve(size_t expectedDistinctKeys) {
  VELOX_CHECK(needsInput_, "Cannot reserve after noMoreInput");
  VELOX_CHECK(storage_->rowsByKey.empty(), "Cannot reserve after build starts");
  storage_->rowsByKey.reserve(expectedDistinctKeys);
}

void ChHashBuild::prepareJoinTable(
    const DecodedVector& decodedKey,
    const SelectivityVector& rows) {
  VELOX_CHECK(needsInput_, "Cannot prepare table after noMoreInput");
  VELOX_CHECK(!storage_->rowsByKey.serialized());
  VELOX_CHECK(storage_->rowsByKey.width() == FixedKeyWidth::k64);
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
  VELOX_CHECK(!storage_->rowsByKey.serialized());
  VELOX_CHECK(storage_->rowsByKey.width() == FixedKeyWidth::k64);
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
    auto* cell = storage_->rowsByKey.find(key);
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
  SelectivityVector rows(input->size());
  for (size_t i = 0; i < keyChannels_.size(); ++i) {
    VELOX_CHECK_EQ(input->childAt(keyChannels_[i])->type(), keyTypes_[i]);
  }

  if (storage_->rowsByKey.hashed()) {
    HashedKeyDecoder decoder(input, keyChannels_, keyTypes_, rows);
    const uint32_t batchNo = retainedIndex_->add(input);
    const uint32_t blockNo = packBlockNo(driverNo_, batchNo);
    rows.applyToSelected([&](vector_size_t rowNo) {
      UInt128 digest;
      if (!decoder.hash(rowNo, digest)) {
        return;
      }
      storage_->rowsByKey.emplaceHashed(digest).insert(
          RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
          storage_->arena);
    });
    return;
  }
  if (storage_->rowsByKey.serialized()) {
    SerializedKeyDecoder decoder(input, keyChannels_, keyTypes_, rows);
    std::string bytes;
    rows.applyToSelected([&](vector_size_t rowNo) {
      if (!decoder.serialize(rowNo, bytes)) {
        return;
      }
      const auto temporary = stringRef(bytes);
      if (storage_->rowsByKey.find(temporary) == nullptr) {
        const StringRef persisted{
            storage_->arena.insert(temporary.data, temporary.size),
            temporary.size};
        storage_->rowsByKey.emplace(persisted);
      }
    });

    const uint32_t batchNo = retainedIndex_->add(input);
    const uint32_t blockNo = packBlockNo(driverNo_, batchNo);
    rows.applyToSelected([&](vector_size_t rowNo) {
      if (!decoder.serialize(rowNo, bytes)) {
        return;
      }
      auto* cell = storage_->rowsByKey.find(stringRef(bytes));
      VELOX_CHECK_NOT_NULL(cell);
      cell->getMapped().insert(
          RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
          storage_->arena);
    });
    return;
  }

  FixedKeyDecoder decoder(input, keyChannels_, rows);
  VELOX_CHECK(decoder.width() == storage_->rowsByKey.width());
  const auto prepare = [&]<typename Key>() {
    decoder.packAll<Key>();
    rows.applyToSelected([&](vector_size_t rowNo) {
      if (!decoder.mayHaveNulls() ||
          decoder.hasPackedKeyAt(rowNo)) {
        storage_->rowsByKey.emplace(decoder.packedAt<Key>(rowNo));
      }
    });
  };
  switch (storage_->rowsByKey.width()) {
    case FixedKeyWidth::k64:
      prepare.template operator()<uint64_t>();
      break;
    case FixedKeyWidth::k128:
      prepare.template operator()<UInt128>();
      break;
    case FixedKeyWidth::k256:
      prepare.template operator()<UInt256>();
      break;
  }

  const uint32_t batchNo = retainedIndex_->add(input);
  const uint32_t blockNo = packBlockNo(driverNo_, batchNo);
  const auto attach = [&]<typename Key>() {
    rows.applyToSelected([&](vector_size_t rowNo) {
      if (decoder.mayHaveNulls() &&
          !decoder.hasPackedKeyAt(rowNo)) {
        return;
      }
      auto* cell =
          storage_->rowsByKey.find(decoder.packedAt<Key>(rowNo));
      VELOX_CHECK_NOT_NULL(cell);
      cell->getMapped().insert(
          RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
          storage_->arena);
    });
  };
  switch (storage_->rowsByKey.width()) {
    case FixedKeyWidth::k64:
      attach.template operator()<uint64_t>();
      break;
    case FixedKeyWidth::k128:
      attach.template operator()<UInt128>();
      break;
    case FixedKeyWidth::k256:
      attach.template operator()<UInt256>();
      break;
  }
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
