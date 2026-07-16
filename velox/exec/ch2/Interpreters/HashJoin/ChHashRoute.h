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

// ============================================================================
// ch2-task9c1: ch2-side runtime dispatch (switch spike).
//
// Given the bridge-owned ch::FixedKeyMap (a std::variant already routed by
// FixedKeyMap::chooseType), drive the SIX ch2 HashMethod classes over the
// concrete coordinate map. This EXACTLY mirrors FixedKeyMap::chooseType because
// the variant alternative IS the chooseType result: single narrow int ->
// FixedHashMap_key8/16 direct addressing; single 4/8-byte int ->
// HashMapAll_key32/64; multi-fixed packed by total bytes ->
// keys32/64/128/256, >32B -> hashed; single VARCHAR -> key_string; else ->
// hashed. Range(O2) / low-cardinality(O4) are NOT in ch chooseType and are
// deferred to task 9c-2 (this route never selects them).
//
// All HashMethod instances use_cache=false, mapped=ch::RowRefList (coordinate
// model correctness, see ChHashJoinCh2Pipeline.h header).
//
// NULL / NON-FLAT ADAPTATION (infra boundary): ch2 HashMethods require FLAT
// NON-NULL key columns. Existing ch end-to-end tests feed NULLABLE keys via
// Values nodes. So for each batch we build a compacted flat null-free key
// projection over the survivors (rows non-null in ALL key columns) plus a
// survivorRows mapping compacted-index -> ORIGINAL row. ch2 drives over the
// compacted columns, but build RowRefs use RowRef(blockNo, survivorRows[i])
// (ORIGINAL row into the ORIGINAL retained batch) and probe hits report
// survivorRows[i] as the probeRow. The retained index keeps the ORIGINAL
// input batch unchanged, so EmitGather gathers correct original rows.
// ============================================================================

#include "velox/exec/ch/ChHashProbe.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h"
#include "velox/exec/ch/Interpreters/HashJoin/FixedKeyMap.h"
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/exec/ch/RetainedVectorsIndex.h"
#include "velox/exec/ch2/Common/ColumnsHashing/HashMethod.h"
#include "velox/exec/ch2/Common/HashTable/StringHashMapAdapter.h"
#include "velox/exec/ch2/Interpreters/HashJoin/ChHashJoinCh2Pipeline.h"

#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

#include <memory>
#include <vector>

namespace facebook::velox::exec::ch2 {
namespace route {

// Non-owning ch2 string-map adapter over a bridge-owned raw
// ch::HashMapAll_key_string. Mirrors StringHashMapAdapter ArenaKeyHolder
// persist protocol (glue, not the HashMethod algorithm) so the operators can
// drive their FixedKeyMap variant string map through HashMethodString without
// copying the map.
class StringHashMapRefAdapter {
 public:
  using Impl = ch::HashMapAll_key_string;
  using LookupResult = typename Impl::LookupResult;
  using mapped_type = typename Impl::mapped_type;
  using value_type = typename Impl::cell_type::value_type;

  explicit StringHashMapRefAdapter(Impl& impl) : impl_(impl) {}

  size_t hash(std::string_view key) const {
    return ch::StringRefHash{}(toStringRef(key));
  }

  FOLLY_ALWAYS_INLINE void
  emplace(ArenaKeyHolder& keyHolder, LookupResult& it, bool& inserted) {
    emplace(keyHolder, it, inserted, hash(keyHolderGetKey(keyHolder)));
  }

  FOLLY_ALWAYS_INLINE void emplace(
      ArenaKeyHolder& keyHolder,
      LookupResult& it,
      bool& inserted,
      size_t hashValue) {
    const std::string_view& key = keyHolderGetKey(keyHolder);
    impl_.emplace(toStringRef(key), it, inserted, hashValue);
    if (inserted) {
      keyHolderPersistKey(keyHolder);
      const std::string_view& persisted = keyHolderGetKey(keyHolder);
      it->setKey(toStringRef(persisted));
    } else {
      keyHolderDiscardKey(keyHolder);
    }
  }

  FOLLY_ALWAYS_INLINE LookupResult find(std::string_view key) {
    return impl_.find(toStringRef(key));
  }

  size_t size() const {
    return impl_.size();
  }

 private:
  static FOLLY_ALWAYS_INLINE ch::StringRef toStringRef(std::string_view key) {
    return ch::StringRef{key.data(), static_cast<uint32_t>(key.size())};
  }

  Impl& impl_;
};

// Compacted, flat, null-free key projection for one batch. survivorRows maps
// compacted index -> ORIGINAL row number in the source batch.
struct CompactedKeys {
  std::vector<VectorPtr> columns; // flat, non-null, size == survivorRows.size()
  std::vector<vector_size_t> survivorRows;
};

// Builds the compacted flat null-free key columns. A row survives iff it is
// non-null in EVERY key column (mirrors the old ch decoders, which skip a row
// whenever any key value is null).
inline CompactedKeys compactKeys(
    const RowVectorPtr& input,
    const std::vector<column_index_t>& keyChannels,
    memory::MemoryPool* pool) {
  const vector_size_t numRows = input->size();
  const size_t numKeys = keyChannels.size();

  std::vector<DecodedVector> decoded(numKeys);
  SelectivityVector allRows(numRows);
  for (size_t k = 0; k < numKeys; ++k) {
    decoded[k].decode(*input->childAt(keyChannels[k]), allRows);
  }

  CompactedKeys out;
  out.survivorRows.reserve(numRows);
  for (vector_size_t row = 0; row < numRows; ++row) {
    bool anyNull = false;
    for (size_t k = 0; k < numKeys; ++k) {
      if (decoded[k].isNullAt(row)) {
        anyNull = true;
        break;
      }
    }
    if (!anyNull) {
      out.survivorRows.push_back(row);
    }
  }

  const vector_size_t numSurvivors =
      static_cast<vector_size_t>(out.survivorRows.size());
  out.columns.reserve(numKeys);
  for (size_t k = 0; k < numKeys; ++k) {
    const auto& child = input->childAt(keyChannels[k]);
    auto flat = BaseVector::create(child->type(), numSurvivors, pool);
    for (vector_size_t i = 0; i < numSurvivors; ++i) {
      flat->copy(child.get(), i, out.survivorRows[i], 1);
    }
    // Survivors are non-null in every key column; drop the null buffer so the
    // ch2 HashMethods (which hard-require !mayHaveNulls) accept the column.
    flat->resetNulls();
    out.columns.push_back(std::move(flat));
  }
  return out;
}

inline Sizes fixedKeySizes(const std::vector<TypePtr>& keyTypes) {
  Sizes sizes;
  sizes.reserve(keyTypes.size());
  for (const auto& t : keyTypes) {
    sizes.push_back(ch::fixedKeyTypeSize(t->kind()));
  }
  return sizes;
}

// ---- build ----------------------------------------------------------------

// Inserts coordinates for one batch over the ch2-driven map, remapping
// compacted-index -> ORIGINAL row via survivorRows. Retains the ORIGINAL input.
inline void buildViaCh2(
    ch::FixedKeyMap& map,
    ch::RetainedVectorsIndex& retained,
    ch::Arena& arena,
    uint32_t driverNo,
    const RowVectorPtr& input,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<TypePtr>& keyTypes,
    memory::MemoryPool* pool) {
  auto compacted = compactKeys(input, keyChannels, pool);
  const uint32_t batchNo = retained.add(input);
  const uint32_t blockNo = ch::packBlockNo(driverNo, batchNo);

  auto insertAll = [&](auto& hashMethod, auto& coordinateMap) {
    for (size_t i = 0; i < compacted.survivorRows.size(); ++i) {
      auto emplaceResult = hashMethod.emplaceKey(coordinateMap, i, arena);
      emplaceResult.getMapped().insert(
          ch::RowRef(blockNo, static_cast<uint32_t>(compacted.survivorRows[i]))
              .encode(),
          arena);
    }
  };

  const auto& cols = compacted.columns;

  switch (map.type()) {
    case ch::FixedKeyMap::Type::key8: {
      HashMethodOneNumber<
          ch::FixedHashMap_key8::value_type,
          ch::RowRefList,
          int8_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      insertAll(m, map.rawMap8());
      break;
    }
    case ch::FixedKeyMap::Type::key16: {
      HashMethodOneNumber<
          ch::FixedHashMap_key16::value_type,
          ch::RowRefList,
          int16_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      insertAll(m, map.rawMap16());
      break;
    }
    case ch::FixedKeyMap::Type::key32: {
      HashMethodOneNumber<
          ch::HashMapAll_key32::value_type,
          ch::RowRefList,
          int32_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      insertAll(m, map.rawMap32());
      break;
    }
    case ch::FixedKeyMap::Type::key64: {
      HashMethodOneNumber<
          ch::HashMapAll_key64::value_type,
          ch::RowRefList,
          int64_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      insertAll(m, map.rawMap64());
      break;
    }
    case ch::FixedKeyMap::Type::keys32: {
      HashMethodKeysFixed<
          ch::HashMapAll_key32::value_type,
          uint32_t,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      insertAll(m, map.rawMap32());
      break;
    }
    case ch::FixedKeyMap::Type::keys64: {
      HashMethodKeysFixed<
          ch::HashMapAll_key64::value_type,
          uint64_t,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      insertAll(m, map.rawMap64());
      break;
    }
    case ch::FixedKeyMap::Type::keys128: {
      HashMethodKeysFixed<
          ch::HashMapAll_keys128::value_type,
          ch::UInt128,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      insertAll(m, map.rawMap128());
      break;
    }
    case ch::FixedKeyMap::Type::keys256: {
      HashMethodKeysFixed<
          ch::HashMapAll_keys256::value_type,
          ch::UInt256,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      insertAll(m, map.rawMap256());
      break;
    }
    case ch::FixedKeyMap::Type::key_string: {
      StringHashMapRefAdapter adapter(map.rawKeyStringMap());
      HashMethodString<
          StringHashMapRefAdapter::value_type,
          ch::RowRefList,
          /*place_string_to_arena=*/true,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      insertAll(m, adapter);
      break;
    }
    case ch::FixedKeyMap::Type::hashed: {
      ColumnRawPtrs keys(cols.begin(), cols.end());
      HashMethodHashed<
          ch::HashMapAll_hashed::value_type,
          ch::RowRefList,
          /*use_cache=*/false>
          m(keys, {}, nullptr);
      insertAll(m, map.rawHashedMap());
      break;
    }
  }
}

// ---- probe ----------------------------------------------------------------

inline std::vector<ch::ProbeHit> probeViaCh2(
    ch::FixedKeyMap& map,
    ch::Arena& arena,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<TypePtr>& keyTypes,
    memory::MemoryPool* pool) {
  auto compacted = compactKeys(probe, keyChannels, pool);
  std::vector<ch::ProbeHit> hits;
  hits.reserve(compacted.survivorRows.size());

  auto findAll = [&](auto& hashMethod, auto& coordinateMap) {
    for (size_t i = 0; i < compacted.survivorRows.size(); ++i) {
      auto findResult = hashMethod.findKey(coordinateMap, i, arena);
      if (findResult.isFound()) {
        hits.push_back(
            {static_cast<vector_size_t>(compacted.survivorRows[i]),
             &findResult.getMapped()});
      }
    }
  };

  const auto& cols = compacted.columns;

  switch (map.type()) {
    case ch::FixedKeyMap::Type::key8: {
      HashMethodOneNumber<
          ch::FixedHashMap_key8::value_type,
          ch::RowRefList,
          int8_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      findAll(m, map.rawMap8());
      break;
    }
    case ch::FixedKeyMap::Type::key16: {
      HashMethodOneNumber<
          ch::FixedHashMap_key16::value_type,
          ch::RowRefList,
          int16_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      findAll(m, map.rawMap16());
      break;
    }
    case ch::FixedKeyMap::Type::key32: {
      HashMethodOneNumber<
          ch::HashMapAll_key32::value_type,
          ch::RowRefList,
          int32_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      findAll(m, map.rawMap32());
      break;
    }
    case ch::FixedKeyMap::Type::key64: {
      HashMethodOneNumber<
          ch::HashMapAll_key64::value_type,
          ch::RowRefList,
          int64_t,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      findAll(m, map.rawMap64());
      break;
    }
    case ch::FixedKeyMap::Type::keys32: {
      HashMethodKeysFixed<
          ch::HashMapAll_key32::value_type,
          uint32_t,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      findAll(m, map.rawMap32());
      break;
    }
    case ch::FixedKeyMap::Type::keys64: {
      HashMethodKeysFixed<
          ch::HashMapAll_key64::value_type,
          uint64_t,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      findAll(m, map.rawMap64());
      break;
    }
    case ch::FixedKeyMap::Type::keys128: {
      HashMethodKeysFixed<
          ch::HashMapAll_keys128::value_type,
          ch::UInt128,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      findAll(m, map.rawMap128());
      break;
    }
    case ch::FixedKeyMap::Type::keys256: {
      HashMethodKeysFixed<
          ch::HashMapAll_keys256::value_type,
          ch::UInt256,
          ch::RowRefList,
          false,
          false,
          /*use_cache=*/false>
          m(cols, fixedKeySizes(keyTypes), nullptr);
      findAll(m, map.rawMap256());
      break;
    }
    case ch::FixedKeyMap::Type::key_string: {
      StringHashMapRefAdapter adapter(map.rawKeyStringMap());
      HashMethodString<
          StringHashMapRefAdapter::value_type,
          ch::RowRefList,
          /*place_string_to_arena=*/true,
          /*use_cache=*/false>
          m({cols[0]}, {}, nullptr);
      findAll(m, adapter);
      break;
    }
    case ch::FixedKeyMap::Type::hashed: {
      ColumnRawPtrs keys(cols.begin(), cols.end());
      HashMethodHashed<
          ch::HashMapAll_hashed::value_type,
          ch::RowRefList,
          /*use_cache=*/false>
          m(keys, {}, nullptr);
      findAll(m, map.rawHashedMap());
      break;
    }
  }
  return hits;
}

} // namespace route
} // namespace facebook::velox::exec::ch2
