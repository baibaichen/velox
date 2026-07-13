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
#include "velox/exec/ch/RowRef.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec::ch {

std::vector<ProbeHit> joinProbe(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  return joinProbe(build.rowsByKey(), probe, probeKeyChannel);
}

std::vector<ProbeHit> joinProbe(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel) {
  VELOX_CHECK_NOT_NULL(probe);
  VELOX_CHECK_LT(probeKeyChannel, probe->childrenSize());

  auto keyVector = probe->childAt(probeKeyChannel)->loadedVector();
  VELOX_CHECK_EQ(
      keyVector->typeKind(),
      TypeKind::BIGINT,
      "ChHashProbe supports one BIGINT key channel");

  SelectivityVector rows(probe->size());
  DecodedVector decodedKey(*keyVector, rows);
  std::vector<ProbeHit> hits;
  hits.reserve(probe->size());

  rows.applyToSelected([&](vector_size_t probeRow) {
    if (decodedKey.isNullAt(probeRow)) {
      return;
    }

    const auto key =
        static_cast<uint64_t>(decodedKey.valueAt<int64_t>(probeRow));
    const auto* cell = map.find(key, map.hash(key));
    if (cell == nullptr) {
      return;
    }

    hits.push_back({probeRow, &cell->getMapped()});
  });

  return hits;
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

} // namespace facebook::velox::exec::ch
