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
#include "velox/exec/ch/Interpreters/RowRef.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/exec/ch/Interpreters/HashJoin/ChHashRoute.h"


namespace facebook::velox::exec::ch {


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
  // Drive the six-HashMethod route. findKey needs a mutable map ref (it only
  // reads cells, never mutates the shared build table), so we const_cast the
  // bridge-owned map at the route boundary. A per-call Arena backs any
  // transient key holders; probe never persists into it.
  Arena arena(probe->pool());
  auto& mutableMap = const_cast<ChHashBuild::JoinMap&>(map);
  return ch::route::probeViaCh2(
      mutableMap, arena, probe, probeKeyChannels, map.keyTypes(), probe->pool());
}

size_t joinProbeCount(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels) {
  return joinProbe(map, probe, probeKeyChannels).size();
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
