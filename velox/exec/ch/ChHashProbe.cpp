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

StringRef stringRef(const std::string& bytes) {
  VELOX_CHECK_LE(bytes.size(), std::numeric_limits<uint32_t>::max());
  return {bytes.data(), static_cast<uint32_t>(bytes.size())};
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
  VELOX_CHECK_NOT_NULL(probe);
  SelectivityVector rows(probe->size());

  if (map.hashed()) {
    HashedKeyDecoder decoder(probe, probeKeyChannels, map.keyTypes(), rows);
    std::vector<ProbeHit> hits;
    hits.reserve(probe->size());
    for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
      UInt128 digest;
      if (!decoder.hash(probeRow, digest)) {
        continue;
      }
      const auto* cell = map.findHashed(digest);
      if (cell != nullptr) {
        hits.push_back({probeRow, &cell->getMapped()});
      }
    }
    return hits;
  }
  if (map.serialized()) {
    SerializedKeyDecoder decoder(
        probe, probeKeyChannels, map.keyTypes(), rows);
    std::vector<ProbeHit> hits;
    hits.reserve(probe->size());
    std::string keyBytes;
    for (vector_size_t probeRow = 0; probeRow < probe->size(); ++probeRow) {
      if (!decoder.serialize(probeRow, keyBytes)) {
        continue;
      }
      const auto* cell = map.find(stringRef(keyBytes));
      if (cell != nullptr) {
        hits.push_back({probeRow, &cell->getMapped()});
      }
    }
    return hits;
  }

  FixedKeyDecoder decoder(probe, probeKeyChannels, rows);
  VELOX_CHECK(decoder.width() == map.width());

  const auto probeKeys = [&]<typename Key>() {
    std::vector<ProbeHit> hits;
    hits.reserve(probe->size());
    const bool usePrefetch =
        FixedKeyDecoder::hasCheapKeyCalculation &&
        map.getBufferSizeInBytes() > kMinTableBytesForPrefetch;
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
        hits.push_back({probeRow, &cell->getMapped()});
      }
    }
    return hits;
  };

  switch (map.width()) {
    case FixedKeyWidth::k64:
      return probeKeys.template operator()<uint64_t>();
    case FixedKeyWidth::k128:
      return probeKeys.template operator()<UInt128>();
    case FixedKeyWidth::k256:
      return probeKeys.template operator()<UInt256>();
  }
  VELOX_UNREACHABLE();
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
      const auto* buildBatch = retained.at(
          unpackDriverNo(blockNo), unpackBatchNo(blockNo));
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
