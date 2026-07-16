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
#include "velox/exec/ch/Common/ColumnsHashing/HashedKey.h"
#include "velox/exec/ch/Common/HashTable/Prefetching.h"
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/exec/ch/Common/ColumnsHashing/SerializedKey.h"
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashRoute.h"

#include <cstdlib>

namespace facebook::velox::exec::ch {

namespace ch2Route {
// Default = drive ch2. CH_USE_CH2=0 reverts to the old ch decoder path.
bool useCh2Default() {
  const char* env = std::getenv("CH_USE_CH2");
  return !(env != nullptr && env[0] == '0');
}
} // namespace ch2Route

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
    : driverNo_(driverNo),
      keyChannels_(std::move(keyChannels)),
      keyTypes_(std::move(keyTypes)),
      retainedIndex_(std::make_shared<RetainedVectorsIndex>(driverNo)),
      storage_(std::make_shared<BuildStorage>(pool, keyTypes_)) {
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
  VELOX_CHECK(storage_->rowsByKey.type() == FixedKeyMap::Type::key64);
  VELOX_CHECK_EQ(
      decodedKey.base()->typeKind(),
      TypeKind::BIGINT,
      "ChHashBuild supports one BIGINT key channel");

  rows.applyToSelected([&](vector_size_t rowNo) {
    if (decodedKey.isNullAt(rowNo)) {
      return;
    }

    const auto key = static_cast<uint64_t>(decodedKey.valueAt<int64_t>(rowNo));
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
  VELOX_CHECK(storage_->rowsByKey.type() == FixedKeyMap::Type::key64);
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

    const auto key = static_cast<uint64_t>(decodedKey.valueAt<int64_t>(rowNo));
    auto* cell = storage_->rowsByKey.find(key);
    VELOX_CHECK_NOT_NULL(
        cell, "Key must be prepared before adding row references");
    const auto refWord = RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode();
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

  if (useCh2_) {
    // ch2-task9c1: default path. Drive the ch2 six-HashMethod route over the
    // FixedKeyMap coordinate variant; the compacted-flat-null-free adaptation
    // and ORIGINAL-row remapping live in ChHashRoute.h. Retains the ORIGINAL
    // input (route calls retainedIndex_->add), matching the old paths.
    ch2::route::buildViaCh2(
        storage_->rowsByKey,
        *retainedIndex_,
        storage_->arena,
        driverNo_,
        input,
        keyChannels_,
        keyTypes_,
        storage_->pool);
    return;
  }

  if (storage_->rowsByKey.type() == FixedKeyMap::Type::hashed) {
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
  if (storage_->rowsByKey.type() == FixedKeyMap::Type::key_string) {
    StringViewKeyDecoder decoder(input, keyChannels_, keyTypes_, rows);
    const uint32_t batchNo = retainedIndex_->add(input);
    const uint32_t blockNo = packBlockNo(driverNo_, batchNo);
    const bool usePrefetch =
        storage_->rowsByKey.getBufferSizeInBytes() > minTableBytesForPrefetch();
    auto prefetcher =
        makeJoinPrefetcher(usePrefetch, rows.size(), [&](size_t prefetchRow) {
          StringRef prefetchKey;
          // atForPrefetch() uses a separate inline buffer, so it does not
          // clobber the current row's key held below.
          if (decoder.atForPrefetch(prefetchRow, prefetchKey)) {
            storage_->rowsByKey.prefetchString(
                storage_->rowsByKey.hashString(prefetchKey));
          }
        });
    rows.applyToSelected([&](vector_size_t rowNo) {
      prefetcher.prefetchAt(rowNo);

      StringRef key;
      // Each iteration holds a single key; at() reuses the decoder's inline
      // storage, so key must be fully consumed before the next at() call.
      if (!decoder.at(rowNo, key)) {
        return;
      }
      // Hash once; reuse for the prefetch, the emplace, and the persist.
      const auto hashValue = storage_->rowsByKey.hashString(key);
      // Single emplace looks up or inserts and returns the resulting cell,
      // mirroring ClickHouse insertAll (emplaceKey + ArenaKeyHolder). The
      // transient key from the decoder is safe for the lookup and equality
      // check, but a newly inserted cell would hold that per-batch pointer, so
      // on insert we persist the bytes to the arena and swap the cell's key to
      // the arena-owned copy. The bytes are identical, so the saved hash and
      // future comparisons stay valid.
      bool inserted;
      auto* cell = storage_->rowsByKey.emplace(key, hashValue, inserted);
      VELOX_CHECK_NOT_NULL(cell);
      if (inserted) {
        cell->setKey(
            StringRef{storage_->arena.insert(key.data, key.size), key.size});
      }
      cell->getMapped().insert(
          RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
          storage_->arena);
    });
    return;
  }

  FixedKeyDecoder decoder(input, keyChannels_, rows);
  VELOX_CHECK(decoder.width() == storage_->rowsByKey.width());
  const auto prepare = [&]<typename Key>() {
    const bool usePrefetch = FixedKeyDecoder::hasCheapKeyCalculation &&
        storage_->rowsByKey.getBufferSizeInBytes() > minTableBytesForPrefetch();
    // CH batch-packs keys <= 16 bytes with no nullable column
    // (usePreparedKeys); wider or nullable keys pack per row.
    if (decoder.usePreparedKeys<Key>()) {
      decoder.packAll<Key>();
      auto prefetcher = makeJoinPrefetcher(
          usePrefetch, rows.size(), [&](size_t prefetchRow) {
            storage_->rowsByKey.prefetch(decoder.packedAt<Key>(prefetchRow));
          });
      rows.applyToSelected([&](vector_size_t rowNo) {
        prefetcher.prefetchAt(rowNo);
        storage_->rowsByKey.emplace(decoder.packedAt<Key>(rowNo));
      });
      return;
    }
    auto prefetcher =
        makeJoinPrefetcher(usePrefetch, rows.size(), [&](size_t prefetchRow) {
          Key prefetchKey;
          if (decoder.pack(prefetchRow, prefetchKey)) {
            storage_->rowsByKey.prefetch(prefetchKey);
          }
        });
    rows.applyToSelected([&](vector_size_t rowNo) {
      prefetcher.prefetchAt(rowNo);

      Key key;
      if (decoder.pack(rowNo, key)) {
        storage_->rowsByKey.emplace(key);
      }
    });
  };
  // Fixed-integer variants dispatch a second time by packed key width.
  switch (storage_->rowsByKey.width()) {
    case FixedKeyWidth::k8:
      prepare.template operator()<uint8_t>();
      break;
    case FixedKeyWidth::k16:
      prepare.template operator()<uint16_t>();
      break;
    case FixedKeyWidth::k32:
      prepare.template operator()<uint32_t>();
      break;
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
    const bool usePrefetch = FixedKeyDecoder::hasCheapKeyCalculation &&
        storage_->rowsByKey.getBufferSizeInBytes() > minTableBytesForPrefetch();
    if (decoder.usePreparedKeys<Key>()) {
      decoder.packAll<Key>();
      auto prefetcher = makeJoinPrefetcher(
          usePrefetch, rows.size(), [&](size_t prefetchRow) {
            storage_->rowsByKey.prefetch(decoder.packedAt<Key>(prefetchRow));
          });
      rows.applyToSelected([&](vector_size_t rowNo) {
        prefetcher.prefetchAt(rowNo);
        auto* cell = storage_->rowsByKey.find(decoder.packedAt<Key>(rowNo));
        VELOX_CHECK_NOT_NULL(cell);
        cell->getMapped().insert(
            RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
            storage_->arena);
      });
      return;
    }
    auto prefetcher =
        makeJoinPrefetcher(usePrefetch, rows.size(), [&](size_t prefetchRow) {
          Key prefetchKey;
          if (decoder.pack(prefetchRow, prefetchKey)) {
            storage_->rowsByKey.prefetch(prefetchKey);
          }
        });
    rows.applyToSelected([&](vector_size_t rowNo) {
      prefetcher.prefetchAt(rowNo);

      Key key;
      if (!decoder.pack(rowNo, key)) {
        return;
      }
      auto* cell = storage_->rowsByKey.find(key);
      VELOX_CHECK_NOT_NULL(cell);
      cell->getMapped().insert(
          RowRef(blockNo, static_cast<uint32_t>(rowNo)).encode(),
          storage_->arena);
    });
  };
  // Fixed-integer variants dispatch a second time by packed key width.
  switch (storage_->rowsByKey.width()) {
    case FixedKeyWidth::k8:
      attach.template operator()<uint8_t>();
      break;
    case FixedKeyWidth::k16:
      attach.template operator()<uint16_t>();
      break;
    case FixedKeyWidth::k32:
      attach.template operator()<uint32_t>();
      break;
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
