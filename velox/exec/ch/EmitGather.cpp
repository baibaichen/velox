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

#include "velox/exec/ch/EmitGather.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/ch/RowRef.h"

#include <map>
#include <utility>

namespace facebook::velox::exec::ch {
namespace {

bool requiresCopy(TypeKind kind) {
  return kind == TypeKind::ARRAY || kind == TypeKind::MAP ||
      kind == TypeKind::ROW;
}

BufferPtr makeIndices(
    const std::vector<const ProbeMatch*>& matches,
    memory::MemoryPool* pool,
    bool buildSide) {
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(matches.size(), pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (size_t i = 0; i < matches.size(); ++i) {
    rawIndices[i] = buildSide ? matches[i]->buildRowNo : matches[i]->probeRow;
  }
  return indices;
}

} // namespace

EmitGather::EmitGather(
    const RetainedVectorsIndex& retainedIndex,
    std::vector<column_index_t> buildProjections,
    std::vector<column_index_t> probeProjections,
    RowTypePtr outputType,
    memory::MemoryPool* pool)
    : retainedIndex_(retainedIndex),
      buildProjections_(std::move(buildProjections)),
      probeProjections_(std::move(probeProjections)),
      outputType_(std::move(outputType)),
      pool_(pool),
      emitColumns_(retainedIndex_.resolveEmitColumns(buildProjections_)) {
  VELOX_CHECK_NOT_NULL(outputType_);
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_EQ(
      outputType_->size(),
      buildProjections_.size() + probeProjections_.size(),
      "Output type must contain all build projections followed by all probe "
      "projections");
}

VectorPtr EmitGather::makeBuildColumn(
    size_t projection,
    uint32_t driverNo,
    uint32_t batchNo,
    const BufferPtr& indices,
    const std::vector<const ProbeMatch*>& matches) const {
  VELOX_CHECK_LT(projection, emitColumns_.emit.size());
  VELOX_CHECK_LT(driverNo, emitColumns_.emit[projection].size());
  VELOX_CHECK_LT(
      batchNo, emitColumns_.emit[projection][driverNo].size());

  const auto* resolved =
      emitColumns_.emit[projection][driverNo][batchNo];
  auto base = retainedIndex_.at(driverNo, batchNo)
                  ->childAt(buildProjections_[projection]);
  VELOX_DCHECK(resolved == base.get());

  if (!requiresCopy(resolved->typeKind())) {
    return BaseVector::wrapInDictionary(
        nullptr, indices, matches.size(), std::move(base));
  }

  auto result = BaseVector::create(base->type(), matches.size(), pool_);
  std::vector<BaseVector::CopyRange> ranges;
  ranges.reserve(matches.size());
  for (vector_size_t row = 0; row < matches.size(); ++row) {
    ranges.push_back(
        {static_cast<vector_size_t>(matches[row]->buildRowNo),
         row,
         1});
  }
  result->copyRanges(
      base.get(), folly::Range<const BaseVector::CopyRange*>(ranges));
  return result;
}

std::vector<RowVectorPtr> EmitGather::emit(
    const std::vector<ProbeMatch>& matches,
    const RowVectorPtr& probeInput) const {
  VELOX_CHECK_NOT_NULL(probeInput);

  using Block = std::pair<uint32_t, uint32_t>;
  std::map<Block, std::vector<const ProbeMatch*>> matchesByBlock;
  for (const auto& match : matches) {
    VELOX_CHECK_LT(match.probeRow, probeInput->size());
    const auto driverNo = unpackDriverNo(match.buildBlockNo);
    const auto batchNo = unpackBatchNo(match.buildBlockNo);
    const auto* buildBatch = retainedIndex_.at(driverNo, batchNo);
    VELOX_CHECK_LT(match.buildRowNo, buildBatch->size());
    matchesByBlock[{driverNo, batchNo}].push_back(&match);
  }

  std::vector<RowVectorPtr> output;
  output.reserve(matchesByBlock.size());
  for (const auto& [block, blockMatches] : matchesByBlock) {
    const auto [driverNo, batchNo] = block;
    auto buildIndices = makeIndices(blockMatches, pool_, true);
    auto probeIndices = makeIndices(blockMatches, pool_, false);

    std::vector<VectorPtr> children;
    children.reserve(
        buildProjections_.size() + probeProjections_.size());
    for (size_t i = 0; i < buildProjections_.size(); ++i) {
      children.push_back(makeBuildColumn(
          i, driverNo, batchNo, buildIndices, blockMatches));
    }
    for (const auto projection : probeProjections_) {
      VELOX_CHECK_LT(projection, probeInput->childrenSize());
      children.push_back(BaseVector::wrapInDictionary(
          nullptr,
          probeIndices,
          blockMatches.size(),
          probeInput->childAt(projection)));
    }

    output.push_back(std::make_shared<RowVector>(
        pool_,
        outputType_,
        nullptr,
        blockMatches.size(),
        std::move(children)));
  }
  return output;
}


} // namespace facebook::velox::exec::ch
