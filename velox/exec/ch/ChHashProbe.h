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

#include "velox/exec/ch/ChHashBuild.h"

#include <cstdint>
#include <vector>

namespace facebook::velox::exec::ch {

struct ProbeHit {
  vector_size_t probeRow;
  const RowRefList* matched;
};

struct ProbeMatch {
  vector_size_t probeRow;
  uint32_t buildBlockNo;
  uint32_t buildRowNo;
};

std::vector<ProbeHit> joinProbe(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel);

std::vector<ProbeHit> joinProbe(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels);

std::vector<ProbeHit> joinProbe(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel);

std::vector<ProbeHit> joinProbe(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels);

/// Probes the layer-1 hash table and returns only the number of matched probe
/// rows, without materializing the matched RowRefLists into a ProbeHit vector.
/// Mirrors joinProbe's find loop (including probe-side prefetch) so a caller
/// that only needs the hit count pays no second-layer collection cost. Used by
/// the layer benchmark to measure pure layer-1 probe work.
size_t joinProbeCount(
    const ChHashBuild::JoinMap& map,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels);

size_t joinProbeCount(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels);

std::vector<ProbeMatch> listJoinResults(
    const std::vector<ProbeHit>& hits,
    const RetainedVectorsIndex& retained);

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel);

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild& build,
    const RowVectorPtr& probe,
    const std::vector<column_index_t>& probeKeyChannels);

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild::JoinMap& map,
    const RetainedVectorsIndex& retained,
    const RowVectorPtr& probe,
    column_index_t probeKeyChannel);

} // namespace facebook::velox::exec::ch
