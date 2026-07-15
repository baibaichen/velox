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

#include "velox/exec/ch/ChHashProbe.h"

#include "velox/common/base/Exceptions.h"
#include "velox/exec/ch/HashedKey.h"
#include "velox/exec/ch/RowRef.h"
#include "velox/exec/ch/SerializedKey.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec::ch {

namespace {
constexpr vector_size_t kPrefetchLookAhead = 16;
constexpr size_t kMinTableBytesForPrefetch = 8UL << 20;

// Probes the layer-1 hash table for every row of `probe`, invoking
// `onHit(probeRow, cell)` for each matched row. Dispatches by map type
// (hashed / key_string / fixed-integer) and, for the fixed-integer case, a
// second time by packed key width. Probe-side rolling prefetch (look-ahead
// kPrefetchLookAhead, gated on table size) lives here so both the
// vector-collecting and count-only entry points share one copy. `onHit` is a
// compile-time-known callable (generic lambda), so the call fully inlines and
// the count-only path pays no ProbeHit collection cost.
template <typename OnHit>
void probeLoop(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels,
    OnHit&& onHit) {
  VELOX_CHECK_NOT_NULL(probe);
  SelectivityVector rows(probe->size());

  if (map.type() == FixedKeyMap::Type::hashed) {
    HashedKeyDecoder decoder(probe, probeKeyChannels, map.keyTypes(), rows);
    for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
      UInt128 digest;
      if (!decoder.hash(probeRow, digest)) {
        continue;
      }
      const auto* cell = map.findHashed(digest);
      if (cell != nullptr) {
        onHit(probeRow, cell);
      }
    }
    return;
  }
  if (map.type() == FixedKeyMap::Type::key_string) {
    StringViewKeyDecoder decoder(probe, probeKeyChannels, map.keyTypes(), rows);
    const bool usePrefetch =
        map.getBufferSizeInBytes() > kMinTableBytesForPrefetch;
    for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
      const auto prefetchRow = probeRow + kPrefetchLookAhead;
      if (usePrefetch && prefetchRow < probe->size()) {
        StringRef prefetchKey;
        // at() reuses the decoder's inline storage across calls, so prefetchKey
        // must be consumed (hashed) before the at(probeRow) call below reads
        // the next row into the same storage.
        if (decoder.at(prefetchRow, prefetchKey)) {
          map.prefetchString(map.hashString(prefetchKey));
        }
      }

      StringRef key;
      if (!decoder.at(probeRow, key)) {
        continue;
      }
      const auto* cell = map.find(key, map.hashString(key));
      if (cell != nullptr) {
        onHit(probeRow, cell);
      }
    }
    return;
  }

  FixedKeyDecoder decoder(probe, probeKeyChannels, rows);
  VELOX_CHECK(decoder.width() == map.width());

  const auto probeKeys = [&]<typename Key>() {
    const bool usePrefetch = FixedKeyDecoder::hasCheapKeyCalculation &&
        map.getBufferSizeInBytes() > kMinTableBytesForPrefetch;
    // CH batch-packs keys that fit in 16 bytes with no nullable column
    // (usePreparedKeys), then indexes prepared_keys[row]; wider or nullable
    // keys pack per row. Mirror both here.
    if (decoder.usePreparedKeys<Key>()) {
      decoder.packAll<Key>();
      for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
        const auto prefetchRow = probeRow + kPrefetchLookAhead;
        if (usePrefetch && prefetchRow < probe->size()) {
          map.prefetch(decoder.packedAt<Key>(prefetchRow));
        }
        const auto* cell = map.find(decoder.packedAt<Key>(probeRow));
        if (cell != nullptr) {
          onHit(probeRow, cell);
        }
      }
      return;
    }
    for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
      const auto prefetchRow = probeRow + kPrefetchLookAhead;
      if (usePrefetch && prefetchRow < probe->size()) {
        Key prefetchKey;
        if (decoder.pack(prefetchRow, prefetchKey)) {
          map.prefetch(prefetchKey);
        }
      }

      Key key;
      if (!decoder.pack(probeRow, key)) {
        continue;
      }
      const auto* cell = map.find(key);
      if (cell != nullptr) {
        onHit(probeRow, cell);
      }
    }
  };

  // Fixed-integer variants dispatch a second time by packed key width.
  switch (map.width()) {
    case FixedKeyWidth::k8:
      return probeKeys.template operator()<uint8_t>();
    case FixedKeyWidth::k16:
      return probeKeys.template operator()<uint16_t>();
    case FixedKeyWidth::k32:
      return probeKeys.template operator()<uint32_t>();
    case FixedKeyWidth::k64:
      return probeKeys.template operator()<uint64_t>();
    case FixedKeyWidth::k128:
      return probeKeys.template operator()<UInt128>();
    case FixedKeyWidth::k256:
      return probeKeys.template operator()<UInt256>();
  }
  VELOX_UNREACHABLE();
}
} // namespace

std::vector<ProbeHit> joinProbe(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  return joinProbe(build, probe, std::vector<column_index_t>{probeKeyChannel});
}

std::vector<ProbeHit> joinProbe(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  VELOX_CHECK_EQ(build.keyChannels().size(), probeKeyChannels.size());
  return joinProbe(build.rowsByKey(), probe, probeKeyChannels);
}

std::vector<ProbeHit> joinProbe(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  return joinProbe(map, probe, std::vector<column_index_t>{probeKeyChannel});
}

std::vector<ProbeHit> joinProbe(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  std::vector<ProbeHit> hits;
  VELOX_CHECK_NOT_NULL(probe);
  hits.reserve(probe->size());
  probeLoop(
      map,
      probe,
      probeKeyChannels,
      [&](vector_size_t probeRow, const auto* cell) {
        hits.push_back({probeRow, &cell->getMapped()});
      });
  return hits;
}

size_t joinProbeCount(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  size_t count = 0;
  probeLoop(map, probe, probeKeyChannels, [&](vector_size_t, const auto*) {
    ++count;
  });
  return count;
}

size_t joinProbeCount(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  VELOX_CHECK_EQ(build.keyChannels().size(), probeKeyChannels.size());
  return joinProbeCount(build.rowsByKey(), probe, probeKeyChannels);
}

std::vector<ProbeMatch> listJoinResults(
    const std::vector<ProbeHit>& hits,
    const RetainedVectorsIndex& retained) {
  std::vector<ProbeMatch> matches;

  size_t numMatches = 0;
  for (const auto& hit : hits) {
    VELOX_CHECK_NOT_NULL(hit.matched);
    numMatches += hit.matched->rows();
  }
  matches.reserve(numMatches);

  for (const auto& hit : hits) {
    for (const auto refWord : *hit.matched) {
      const auto blockNo = refWordBlockNo(refWord);
      const auto rowNo = refWordRowNo(refWord);
      const auto* buildBatch =
          retained.at(unpackDriverNo(blockNo), unpackBatchNo(blockNo));
      VELOX_CHECK_LT(rowNo, buildBatch->size());
      matches.push_back({hit.probeRow, blockNo, rowNo});
    }
  }

  return matches;
}

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  return listJoinResults(
      joinProbe(build, probe, probeKeyChannel), build.retainedIndex());
}

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild::JoinMap& map,
    const RetainedVectorsIndex& retained,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  return listJoinResults(joinProbe(map, probe, probeKeyChannel), retained);
}

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  return listJoinResults(
      joinProbe(build, probe, probeKeyChannels), build.retainedIndex());
}

} // namespace facebook::velox::exec::ch
