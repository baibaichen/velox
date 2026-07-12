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

std::vector<ProbeMatch> probeHashBuild(
    const ChHashBuild& build,
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
  std::vector<ProbeMatch> matches;
  const auto& map = build.rowsByKey();

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

    for (const auto refWord : cell->getMapped()) {
      const auto blockNo = refWordBlockNo(refWord);
      const auto rowNo = refWordRowNo(refWord);
      const auto* buildBatch = build.retainedIndex().at(
          unpackDriverNo(blockNo), unpackBatchNo(blockNo));
      VELOX_CHECK_LT(rowNo, buildBatch->size());
      matches.push_back({probeRow, blockNo, rowNo});
    }
  });

  return matches;
}

} // namespace facebook::velox::exec::ch
