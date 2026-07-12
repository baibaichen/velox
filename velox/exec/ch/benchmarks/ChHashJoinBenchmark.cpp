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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <iostream>
#include <limits>
#include <numeric>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/exec/ch/ChHashProbe.h"
#include "velox/exec/ch/EmitGather.h"
#include "velox/exec/ch/ChHashJoinBridge.h"
#include "velox/exec/Cursor.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(
    build_rows,
    200'000,
    "Number of build rows. Must be divisible by fanout.");
DEFINE_int32(
    payload_columns,
    39,
    "Number of build payload columns, cycling BIGINT/DOUBLE/VARCHAR.");
DEFINE_int32(fanout, 4, "Build rows per distinct join key.");
DEFINE_int32(batch_rows, 20'000, "Rows per build input batch.");
DEFINE_int32(
    measure_iterations,
    50,
    "Iterations in the reported probe sample; use enough to amortize warmup.");
DEFINE_double(
    filter_selectivity,
    0.1,
    "Fraction of emitted rows retained before delayed materialization.");
DEFINE_bool(
    run_e2e,
    true,
    "Run real Task/Driver end-to-end join benchmarks before microbenchmarks.");
DEFINE_int32(
    e2e_iterations,
    5,
    "Measured query executions per arm and selectivity.");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

struct BuildResult {
  uint64_t elapsedNanos;
  uint64_t peakBytes;
};

struct E2EResult {
  double medianElapsedMs;
  uint64_t queryPeakBytes;
  uint64_t resultPeakBytes;
  uint64_t observedTotalPeakBytes;
  uint64_t rows;
};

struct QueryResult {
  std::shared_ptr<Task> task;
  uint64_t rows{0};
  uint64_t hashSum{0};
  uint64_t hashXor{0};
  uint64_t observedTotalPeakBytes{0};
};

class ChHashJoinBenchmark : public VectorTestBase {
 public:
  ChHashJoinBenchmark(
      vector_size_t buildRows,
      int32_t payloadColumns,
      int32_t fanout,
      vector_size_t batchRows)
      : buildRows_(buildRows),
        payloadColumns_(payloadColumns),
        fanout_(fanout),
        batchRows_(batchRows),
        distinctKeys_(buildRows / fanout) {
    VELOX_CHECK_GT(buildRows_, 0);
    VELOX_CHECK_GT(payloadColumns_, 0);
    VELOX_CHECK_GT(fanout_, 0);
    VELOX_CHECK_GT(batchRows_, 0);
    VELOX_CHECK_EQ(buildRows_ % fanout_, 0);

    buildVectors_ = makeBuildVectors();
    probeVector_ = makeProbeVector();
    for (const auto& batch : buildVectors_) {
      retainedVectorBytes_ += batch->retainedSize();
    }
    e2eInputBytes_ = retainedVectorBytes_ + probeVector_->retainedSize();
  }

  core::PlanNodePtr makeE2EPlan(
      bool coordinate,
      int32_t selectivityPercent) const {
    VELOX_CHECK_GT(selectivityPercent, 0);
    VELOX_CHECK_LE(selectivityPercent, 100);
    auto buildNode = std::make_shared<core::ValuesNode>(
        "0", buildVectors_, true);
    auto probeNode = std::make_shared<core::ValuesNode>(
        "1", std::vector<RowVectorPtr>{probeVector_}, true);
    auto probeKey = std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(), "p_key");
    auto buildKey = std::make_shared<core::FieldAccessTypedExpr>(
        BIGINT(), "b_key");
    core::PlanNodePtr joinNode;
    if (coordinate) {
      joinNode = std::make_shared<ch::ChHashJoinNode>(
          "2",
          core::JoinType::kInner,
          std::vector<core::FieldAccessTypedExprPtr>{probeKey},
          std::vector<core::FieldAccessTypedExprPtr>{buildKey},
          nullptr,
          probeNode,
          buildNode,
          e2eOutputType_);
    } else {
      joinNode = std::make_shared<core::HashJoinNode>(
          "2",
          core::JoinType::kInner,
          false,
          std::vector<core::FieldAccessTypedExprPtr>{probeKey},
          std::vector<core::FieldAccessTypedExprPtr>{buildKey},
          nullptr,
          probeNode,
          buildNode,
          e2eOutputType_);
    }
    auto untypedFilter = parse::DuckSqlExpressionsParser().parseExpr(
        fmt::format("p_key % 100 < {}", selectivityPercent));
    auto filter = core::Expressions::inferTypes(
        untypedFilter, e2eOutputType_, pool());
    return std::make_shared<core::FilterNode>("3", filter, joinNode);
  }

  QueryResult runE2EPlan(
      const core::PlanNodePtr& plan,
      const std::shared_ptr<memory::MemoryPool>& resultPool,
      bool computeSignature) const {
    CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = 1;
    params.copyResult = false;
    auto cursor = TaskCursor::create(params);
    cursor->setNoMoreSplits();

    QueryResult result;
    result.task = cursor->task();
    while (cursor->moveNext()) {
      const auto& output = cursor->current();
      auto materialized =
          BaseVector::create<RowVector>(
              output->type(), output->size(), resultPool.get());
      materialized->copy(output.get(), 0, 0, output->size());
      result.rows += materialized->size();
      if (computeSignature) {
        for (vector_size_t row = 0; row < materialized->size(); ++row) {
          uint64_t rowHash = 0;
          for (const auto& child : materialized->children()) {
            const auto childHash = child->hashValueAt(row);
            rowHash ^= childHash + 0x9e3779b97f4a7c15ULL +
                (rowHash << 6) + (rowHash >> 2);
          }
          result.hashSum += rowHash;
          result.hashXor ^= rowHash;
        }
      }
      result.observedTotalPeakBytes = std::max<uint64_t>(
          result.observedTotalPeakBytes,
          e2eInputBytes_ + result.task->pool()->usedBytes() +
              resultPool->usedBytes());
      folly::doNotOptimizeAway(materialized.get());
    }
    return result;
  }

  void verifyE2EEquivalent(int32_t selectivityPercent) {
    auto nativePool = rootPool_->addLeafChild(
        fmt::format("E2EVerifyNative-{}", selectivityPercent));
    auto coordinatePool = rootPool_->addLeafChild(
        fmt::format("E2EVerifyCoordinate-{}", selectivityPercent));
    const auto native =
        runE2EPlan(makeE2EPlan(false, selectivityPercent), nativePool, true);
    const auto coordinate =
        runE2EPlan(makeE2EPlan(true, selectivityPercent), coordinatePool, true);
    VELOX_CHECK_EQ(coordinate.rows, native.rows);
    VELOX_CHECK_EQ(coordinate.hashSum, native.hashSum);
    VELOX_CHECK_EQ(coordinate.hashXor, native.hashXor);
  }

  E2EResult measureE2EOnce(
      bool coordinate,
      int32_t selectivityPercent,
      int32_t iteration) {
    auto resultPool = rootPool_->addLeafChild(fmt::format(
        "E2EResult-{}-{}-{}",
        coordinate ? "coordinate" : "native",
        selectivityPercent,
        iteration));
    const auto plan = makeE2EPlan(coordinate, selectivityPercent);
    const auto start = std::chrono::steady_clock::now();
    const auto result = runE2EPlan(plan, resultPool, false);
    const auto elapsedMs = std::chrono::duration<double, std::milli>(
                               std::chrono::steady_clock::now() - start)
                               .count();
    VELOX_CHECK_NOT_NULL(result.task);
    VELOX_CHECK_EQ(result.rows, expectedE2ERows(selectivityPercent));
    const auto queryPeakBytes =
        static_cast<uint64_t>(result.task->pool()->peakBytes());
    const auto observedTotalPeakBytes = std::max(
        result.observedTotalPeakBytes, e2eInputBytes_ + queryPeakBytes);
    return {
        elapsedMs,
        queryPeakBytes,
        static_cast<uint64_t>(resultPool->peakBytes()),
        observedTotalPeakBytes,
        result.rows};
  }

  BuildResult buildCoordinate() {
    coordinatePool_ = rootPool_->addLeafChild("ChHashBuild");
    const auto start = std::chrono::steady_clock::now();
    coordinateBuild_ =
        std::make_unique<ch::ChHashBuild>(0, 0, coordinatePool_.get());
    for (const auto& batch : buildVectors_) {
      coordinateBuild_->addInput(batch);
    }
    coordinateBuild_->noMoreInput();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    return {
        toNanos(elapsed),
        static_cast<uint64_t>(coordinatePool_->peakBytes()) +
            retainedVectorBytes_};
  }

  BuildResult buildNative() {
    nativePool_ = rootPool_->addLeafChild("VeloxNativeHashBuild");
    scratchPool_ = rootPool_->addLeafChild("VeloxNativeHashProbe");

    // Exercise the RowContainer and HashTable core used by HashBuild and
    // HashProbe directly. Their Operator shells require a Driver pipeline,
    // which this microbenchmark intentionally excludes.
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    std::vector<TypePtr> dependentTypes;
    dependentTypes.reserve(payloadColumns_);
    for (int32_t column = 0; column < payloadColumns_; ++column) {
      dependentTypes.push_back(payloadType(column));
    }

    const auto start = std::chrono::steady_clock::now();
    nativeTable_ = HashTable<true>::createForJoin(
        std::move(hashers),
        dependentTypes,
        true,
        false,
        false,
        1'000,
        nativePool_.get());
    for (const auto& batch : buildVectors_) {
      copyToNativeTable(batch);
    }
    nativeTable_->prepareJoinTable(
        {},
        BaseHashTable::kNoSpillInputStartPartitionBit,
        1'000'000,
        false,
        executor_.get());
    const auto elapsed = std::chrono::steady_clock::now() - start;

    nativeLookup_ =
        std::make_unique<HashLookup>(nativeTable_->hashers(), scratchPool_.get());
    return {
        toNanos(elapsed), static_cast<uint64_t>(nativePool_->peakBytes())};
  }

  uint64_t probeCoordinate() const {
    return ch::probeHashBuild(*coordinateBuild_, probeVector_, 0).size();
  }

  uint64_t probeNative() {
    return collectNativeMatches(nullptr, nullptr);
  }

  void prepareEmit() {
    coordinateMatches_ =
        ch::probeHashBuild(*coordinateBuild_, probeVector_, 0);
    collectNativeMatches(&nativeInputRows_, &nativeHits_);
    VELOX_CHECK_EQ(coordinateMatches_.size(), expectedMatches());
    VELOX_CHECK_EQ(nativeHits_.size(), expectedMatches());

    emitPool_ = rootPool_->addLeafChild("ChHashEmit");
    std::vector<column_index_t> buildProjections;
    std::vector<std::string> outputNames;
    std::vector<TypePtr> outputTypes;
    buildProjections.reserve(payloadColumns_);
    outputNames.reserve(payloadColumns_ + 1);
    outputTypes.reserve(payloadColumns_ + 1);
    for (int32_t column = 0; column < payloadColumns_; ++column) {
      buildProjections.push_back(column + 1);
      outputNames.push_back(fmt::format("build_payload_{}", column));
      outputTypes.push_back(payloadType(column));
    }
    outputNames.push_back("probe_key");
    outputTypes.push_back(BIGINT());
    outputType_ = ROW(std::move(outputNames), std::move(outputTypes));
    emitGather_ = std::make_unique<ch::EmitGather>(
        coordinateBuild_->retainedIndex(),
        std::move(buildProjections),
        std::vector<column_index_t>{0},
        outputType_,
        emitPool_.get());
  }

  uint64_t emitCoordinateViews() const {
    auto output = emitGather_->emit(coordinateMatches_, probeVector_);
    return consumeOutput(output);
  }

  uint64_t emitCoordinateFlattenAll() const {
    auto output = emitGather_->emit(coordinateMatches_, probeVector_);
    return materializeCoordinate(std::move(output), 1.0);
  }

  uint64_t emitCoordinateFilterThenFlatten() const {
    auto output = emitGather_->emit(coordinateMatches_, probeVector_);
    return materializeCoordinate(
        std::move(output), FLAGS_filter_selectivity);
  }

  uint64_t emitNativeCopy() const {
    auto output = makeNativeOutput();
    folly::doNotOptimizeAway(output.get());
    return output->size();
  }

  uint64_t emitNativeCopyThenFilter() const {
    auto output = makeNativeOutput();
    auto filtered = filterOutput(output, FLAGS_filter_selectivity);
    folly::doNotOptimizeAway(filtered.get());
    return filtered->size();
  }

  uint64_t expectedFilteredMatches() const {
    return selectedRows(expectedMatches(), FLAGS_filter_selectivity);
  }

  uint64_t expectedMatches() const {
    return buildRows_;
  }

  uint64_t expectedE2ERows(int32_t selectivityPercent) const {
    uint64_t selectedProbeRows = 0;
    for (vector_size_t key = 0; key < distinctKeys_; ++key) {
      if (key % 100 < selectivityPercent) {
        ++selectedProbeRows;
      }
    }
    return selectedProbeRows * fanout_;
  }

  vector_size_t probeRows() const {
    return distinctKeys_;
  }

  uint64_t retainedVectorBytes() const {
    return retainedVectorBytes_;
  }

  uint64_t e2eInputBytes() const {
    return e2eInputBytes_;
  }

 private:
  uint64_t collectNativeMatches(
      std::vector<vector_size_t>* allInputRows,
      std::vector<char*>* allHits) {
    SelectivityVector rows(probeVector_->size());
    auto& hasher = nativeTable_->hashers().front();
    nativeLookup_->reset(probeVector_->size());
    if (nativeTable_->hashMode() == BaseHashTable::HashMode::kHash) {
      hasher->decode(*probeVector_->childAt(0), rows);
      hasher->hash(rows, false, nativeLookup_->hashes);
    } else {
      hasher->lookupValueIds(
          *probeVector_->childAt(0),
          rows,
          nativeLookup_->scratchMemory,
          nativeLookup_->hashes);
    }
    nativeLookup_->rows.resize(rows.size());
    std::iota(nativeLookup_->rows.begin(), nativeLookup_->rows.end(), 0);
    nativeTable_->joinProbe(*nativeLookup_);

    BaseHashTable::JoinResultIterator results({}, 0, std::nullopt);
    results.reset(*nativeLookup_);
    constexpr vector_size_t kOutputBatchRows = 4'096;
    std::vector<vector_size_t> inputRows(kOutputBatchRows);
    std::vector<char*> hits(kOutputBatchRows);
    uint64_t matches = 0;
    while (!results.atEnd()) {
      const auto batchMatches = nativeTable_->listJoinResults(
          results,
          false,
          folly::Range(inputRows.data(), inputRows.size()),
          folly::Range(hits.data(), hits.size()),
          std::numeric_limits<uint64_t>::max());
      matches += batchMatches;
      if (allInputRows != nullptr) {
        allInputRows->insert(
            allInputRows->end(),
            inputRows.begin(),
            inputRows.begin() + batchMatches);
        allHits->insert(
            allHits->end(), hits.begin(), hits.begin() + batchMatches);
      }
    }
    return matches;
  }

  static vector_size_t selectedRows(
      vector_size_t size,
      double selectivity) {
    return static_cast<vector_size_t>(
        std::ceil(static_cast<double>(size) * selectivity));
  }

  BufferPtr makeFilterIndices(
      vector_size_t selected) const {
    auto indices =
        AlignedBuffer::allocate<vector_size_t>(selected, emitPool_.get());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (vector_size_t row = 0; row < selected; ++row) {
      rawIndices[row] = row;
    }
    return indices;
  }

  uint64_t consumeOutput(const std::vector<RowVectorPtr>& output) const {
    uint64_t rows = 0;
    for (const auto& batch : output) {
      rows += batch->size();
      folly::doNotOptimizeAway(batch.get());
    }
    return rows;
  }

  uint64_t materializeCoordinate(
      std::vector<RowVectorPtr> output,
      double selectivity) const {
    vector_size_t totalRows = 0;
    for (const auto& batch : output) {
      totalRows += batch->size();
    }
    auto rowsRemaining = selectedRows(totalRows, selectivity);
    uint64_t materializedRows = 0;
    for (auto& batch : output) {
      const auto selected = std::min(batch->size(), rowsRemaining);
      rowsRemaining -= selected;
      if (selected == 0) {
        break;
      }
      auto indices = makeFilterIndices(selected);
      std::vector<VectorPtr> children;
      children.reserve(batch->childrenSize());
      for (int32_t column = 0; column < payloadColumns_; ++column) {
        auto selectedView = BaseVector::wrapInDictionary(
            nullptr, indices, selected, batch->childAt(column));
        children.push_back(
            BaseVector::copy(*selectedView, emitPool_.get()));
      }
      children.push_back(BaseVector::wrapInDictionary(
          nullptr,
          indices,
          selected,
          batch->childAt(payloadColumns_)));
      batch = std::make_shared<RowVector>(
          emitPool_.get(),
          outputType_,
          nullptr,
          selected,
          std::move(children));
      materializedRows += selected;
      folly::doNotOptimizeAway(batch.get());
    }
    VELOX_CHECK_EQ(rowsRemaining, 0);
    return materializedRows;
  }

  RowVectorPtr makeNativeOutput() const {
    const auto size = static_cast<vector_size_t>(nativeHits_.size());
    std::vector<VectorPtr> children;
    children.reserve(payloadColumns_ + 1);
    auto rows = folly::Range<char* const*>(nativeHits_.data(), size);
    for (int32_t column = 0; column < payloadColumns_; ++column) {
      auto child =
          BaseVector::create(payloadType(column), size, emitPool_.get());
      nativeTable_->extractColumn(rows, column + 1, child);
      children.push_back(std::move(child));
    }

    auto probeIndices =
        AlignedBuffer::allocate<vector_size_t>(size, emitPool_.get());
    std::copy(
        nativeInputRows_.begin(),
        nativeInputRows_.end(),
        probeIndices->asMutable<vector_size_t>());
    children.push_back(BaseVector::wrapInDictionary(
        nullptr, probeIndices, size, probeVector_->childAt(0)));
    return std::make_shared<RowVector>(
        emitPool_.get(),
        outputType_,
        nullptr,
        size,
        std::move(children));
  }

  RowVectorPtr filterOutput(
      const RowVectorPtr& output,
      double selectivity) const {
    const auto selected = selectedRows(output->size(), selectivity);
    auto indices = makeFilterIndices(selected);
    std::vector<VectorPtr> children;
    children.reserve(output->childrenSize());
    for (const auto& child : output->children()) {
      children.push_back(BaseVector::wrapInDictionary(
          nullptr, indices, selected, child));
    }
    return std::make_shared<RowVector>(
        emitPool_.get(),
        outputType_,
        nullptr,
        selected,
        std::move(children));
  }

  template <typename Rep, typename Period>
  static uint64_t toNanos(std::chrono::duration<Rep, Period> duration) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration)
        .count();
  }

  TypePtr payloadType(int32_t column) const {
    switch (column % 3) {
      case 0:
        return BIGINT();
      case 1:
        return DOUBLE();
      default:
        return VARCHAR();
    }
  }

  std::vector<RowVectorPtr> makeBuildVectors() {
    std::vector<std::string> names{"b_key"};
    std::vector<TypePtr> types{BIGINT()};
    for (int32_t column = 0; column < payloadColumns_; ++column) {
      names.push_back(fmt::format("b_payload_{}", column));
      types.push_back(payloadType(column));
    }
    auto outputNames = names;
    outputNames.push_back("p_key");
    types.push_back(BIGINT());
    e2eOutputType_ = ROW(std::move(outputNames), std::move(types));

    std::vector<RowVectorPtr> batches;
    for (vector_size_t start = 0; start < buildRows_; start += batchRows_) {
      const auto size = std::min(batchRows_, buildRows_ - start);
      std::vector<VectorPtr> children;
      children.reserve(payloadColumns_ + 1);
      children.push_back(makeFlatVector<int64_t>(
          size,
          [this, start](vector_size_t row) {
            return (start + row) / fanout_;
          }));

      for (int32_t column = 0; column < payloadColumns_; ++column) {
        if (column % 3 == 0) {
          children.push_back(makeFlatVector<int64_t>(
              size, [=](vector_size_t row) {
                return (start + row) * 101 + column;
              }));
        } else if (column % 3 == 1) {
          children.push_back(makeFlatVector<double>(
              size, [=](vector_size_t row) {
                return static_cast<double>(start + row) * 0.25 + column;
              }));
        } else {
          std::string value;
          children.push_back(makeFlatVector<StringView>(
              size, [=, &value](vector_size_t row) {
                value = fmt::format(
                    "payload_{:08x}_{:02x}",
                    static_cast<uint32_t>(start + row),
                    column);
                return StringView(value);
              }));
        }
      }
      batches.push_back(makeRowVector(names, children));
    }
    return batches;
  }

  RowVectorPtr makeProbeVector() {
    return makeRowVector(
        {"p_key"},
        {makeFlatVector<int64_t>(
            distinctKeys_, [](vector_size_t row) { return row; })});
  }

  void copyToNativeTable(const RowVectorPtr& batch) {
    auto* rows = nativeTable_->rows();
    auto& hashers = nativeTable_->hashers();
    SelectivityVector selected(batch->size());
    raw_vector<uint64_t> valueIds(batch->size(), scratchPool_.get());

    hashers.front()->decode(*batch->childAt(0), selected);
    if (hashers.front()->mayUseValueIds()) {
      hashers.front()->computeValueIds(selected, valueIds);
    }

    std::vector<std::unique_ptr<DecodedVector>> payload;
    payload.reserve(payloadColumns_);
    for (int32_t column = 0; column < payloadColumns_; ++column) {
      payload.push_back(
          std::make_unique<DecodedVector>(
              *batch->childAt(column + 1), selected));
    }

    selected.applyToSelected([&](vector_size_t row) {
      auto* nativeRow = rows->newRow();
      *reinterpret_cast<char**>(nativeRow + rows->nextOffset()) = nullptr;
      rows->store(hashers.front()->decodedVector(), row, nativeRow, 0);
      for (int32_t column = 0; column < payloadColumns_; ++column) {
        rows->store(*payload[column], row, nativeRow, column + 1);
      }
    });
  }

  const vector_size_t buildRows_;
  const int32_t payloadColumns_;
  const int32_t fanout_;
  const vector_size_t batchRows_;
  const vector_size_t distinctKeys_;
  std::vector<RowVectorPtr> buildVectors_;
  RowVectorPtr probeVector_;
  uint64_t retainedVectorBytes_{0};
  uint64_t e2eInputBytes_{0};
  std::shared_ptr<memory::MemoryPool> coordinatePool_;
  std::shared_ptr<memory::MemoryPool> nativePool_;
  std::shared_ptr<memory::MemoryPool> scratchPool_;
  std::shared_ptr<memory::MemoryPool> emitPool_;
  std::unique_ptr<ch::ChHashBuild> coordinateBuild_;
  std::unique_ptr<HashTable<true>> nativeTable_;
  std::unique_ptr<HashLookup> nativeLookup_;
  std::vector<ch::ProbeMatch> coordinateMatches_;
  std::vector<vector_size_t> nativeInputRows_;
  std::vector<char*> nativeHits_;
  RowTypePtr e2eOutputType_;
  RowTypePtr outputType_;
  std::unique_ptr<ch::EmitGather> emitGather_;
};

std::unique_ptr<ChHashJoinBenchmark> benchmark;

BENCHMARK(CoordinateProbe) {
  folly::doNotOptimizeAway(benchmark->probeCoordinate());
}

BENCHMARK_RELATIVE(VeloxNativeRowProbe) {
  folly::doNotOptimizeAway(benchmark->probeNative());
}

BENCHMARK_DRAW_LINE();

BENCHMARK(A_CoordinateViewOnly_CopyDeferred_NotEndToEnd) {
  folly::doNotOptimizeAway(benchmark->emitCoordinateViews());
}

BENCHMARK_RELATIVE(A_VeloxNativeExtractCopy) {
  folly::doNotOptimizeAway(benchmark->emitNativeCopy());
}

BENCHMARK_DRAW_LINE();

BENCHMARK(B1_CoordinateFlattenAll) {
  folly::doNotOptimizeAway(benchmark->emitCoordinateFlattenAll());
}

BENCHMARK_RELATIVE(B1_VeloxNativeExtractCopy) {
  folly::doNotOptimizeAway(benchmark->emitNativeCopy());
}

BENCHMARK_DRAW_LINE();

BENCHMARK(B2_CoordinateFilterThenFlatten) {
  folly::doNotOptimizeAway(benchmark->emitCoordinateFilterThenFlatten());
}

BENCHMARK_RELATIVE(B2_VeloxNativeCopyThenFilter) {
  folly::doNotOptimizeAway(benchmark->emitNativeCopyThenFilter());
}

template <typename Probe>
double measureThroughput(Probe&& probe, int32_t iterations, uint64_t expected) {
  const auto start = std::chrono::steady_clock::now();
  uint64_t matches = 0;
  for (int32_t i = 0; i < iterations; ++i) {
    const auto iterationMatches = probe();
    VELOX_CHECK_EQ(iterationMatches, expected);
    matches += iterationMatches;
  }
  const auto elapsed = std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - start)
                           .count();
  folly::doNotOptimizeAway(matches);
  return iterations / elapsed;
}

E2EResult summarizeE2E(std::vector<E2EResult> samples) {
  VELOX_CHECK(!samples.empty());
  std::sort(
      samples.begin(),
      samples.end(),
      [](const auto& left, const auto& right) {
        return left.medianElapsedMs < right.medianElapsedMs;
      });
  E2EResult result = samples[samples.size() / 2];
  for (const auto& sample : samples) {
    VELOX_CHECK_EQ(sample.rows, result.rows);
    result.queryPeakBytes =
        std::max(result.queryPeakBytes, sample.queryPeakBytes);
    result.resultPeakBytes =
        std::max(result.resultPeakBytes, sample.resultPeakBytes);
    result.observedTotalPeakBytes = std::max(
        result.observedTotalPeakBytes, sample.observedTotalPeakBytes);
  }
  return result;
}

void runE2EBenchmarks() {
  constexpr std::array<int32_t, 4> kSelectivities{1, 10, 50, 100};
  std::cout << fmt::format(
      "E2E_CONFIG build_rows={} probe_rows={} payload_columns={} fanout={} "
      "batch_rows={} max_drivers=1 iterations={} consumer=flatten_copy "
      "shared_input_bytes={}\n",
      FLAGS_build_rows,
      benchmark->probeRows(),
      FLAGS_payload_columns,
      FLAGS_fanout,
      FLAGS_batch_rows,
      FLAGS_e2e_iterations,
      benchmark->e2eInputBytes());
  for (const auto selectivity : kSelectivities) {
    benchmark->verifyE2EEquivalent(selectivity);
    std::vector<E2EResult> coordinateSamples;
    std::vector<E2EResult> nativeSamples;
    coordinateSamples.reserve(FLAGS_e2e_iterations);
    nativeSamples.reserve(FLAGS_e2e_iterations);
    for (int32_t iteration = 0; iteration < FLAGS_e2e_iterations; ++iteration) {
      auto measure = [&](bool coordinate) {
        auto sample =
            benchmark->measureE2EOnce(
                coordinate, selectivity, iteration);
        (coordinate ? coordinateSamples : nativeSamples)
            .push_back(std::move(sample));
      };
      if (iteration % 2 == 0) {
        measure(true);
        measure(false);
      } else {
        measure(false);
        measure(true);
      }
    }
    const auto coordinate = summarizeE2E(std::move(coordinateSamples));
    const auto native = summarizeE2E(std::move(nativeSamples));
    for (const auto& [arm, result] :
         std::array<std::pair<const char*, E2EResult>, 2>{
             std::pair{"coordinate", coordinate},
             std::pair{"velox_native", native}}) {
      std::cout << fmt::format(
          "E2E selectivity_percent={} arm={} median_wall_ms={:.3f} "
          "query_peak_bytes={} result_peak_bytes={} "
          "observed_total_peak_bytes={} output_rows={}\n",
          selectivity,
          arm,
          result.medianElapsedMs,
          result.queryPeakBytes,
          result.resultPeakBytes,
          result.observedTotalPeakBytes,
          result.rows);
    }
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  VELOX_CHECK_GT(FLAGS_measure_iterations, 0);
  VELOX_CHECK_GT(FLAGS_filter_selectivity, 0);
  VELOX_CHECK_LE(FLAGS_filter_selectivity, 1);
  VELOX_CHECK_GT(FLAGS_e2e_iterations, 0);

  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();
  ch::registerChHashJoin();

  benchmark = std::make_unique<ChHashJoinBenchmark>(
      FLAGS_build_rows,
      FLAGS_payload_columns,
      FLAGS_fanout,
      FLAGS_batch_rows);
  const auto coordinateBuild = benchmark->buildCoordinate();
  const auto nativeBuild = benchmark->buildNative();
  benchmark->prepareEmit();

  VELOX_CHECK_EQ(
      benchmark->probeCoordinate(), benchmark->expectedMatches());
  VELOX_CHECK_EQ(benchmark->probeNative(), benchmark->expectedMatches());
  VELOX_CHECK_EQ(
      benchmark->emitCoordinateViews(), benchmark->expectedMatches());
  VELOX_CHECK_EQ(
      benchmark->emitNativeCopy(), benchmark->expectedMatches());
  VELOX_CHECK_EQ(
      benchmark->emitCoordinateFilterThenFlatten(),
      benchmark->expectedFilteredMatches());
  VELOX_CHECK_EQ(
      benchmark->emitNativeCopyThenFilter(),
      benchmark->expectedFilteredMatches());

  if (FLAGS_run_e2e) {
    runE2EBenchmarks();
  }

  const auto coordinateIterationsPerSecond = measureThroughput(
      [] { return benchmark->probeCoordinate(); },
      FLAGS_measure_iterations,
      benchmark->expectedMatches());
  const auto nativeIterationsPerSecond = measureThroughput(
      [] { return benchmark->probeNative(); },
      FLAGS_measure_iterations,
      benchmark->expectedMatches());

  std::cout << fmt::format(
      "CONFIG build_rows={} probe_rows={} payload_columns={} fanout={} "
      "batch_rows={} filter_selectivity={:.3f}\n",
      FLAGS_build_rows,
      benchmark->probeRows(),
      FLAGS_payload_columns,
      FLAGS_fanout,
      FLAGS_batch_rows,
      FLAGS_filter_selectivity);
  std::cout << fmt::format(
      "BUILD arm=coordinate elapsed_ms={:.3f} peak_bytes={} "
      "retained_vector_bytes={}\n",
      coordinateBuild.elapsedNanos / 1e6,
      coordinateBuild.peakBytes,
      benchmark->retainedVectorBytes());
  std::cout << fmt::format(
      "BUILD arm=velox_native_row elapsed_ms={:.3f} peak_bytes={}\n",
      nativeBuild.elapsedNanos / 1e6,
      nativeBuild.peakBytes);
  std::cout << fmt::format(
      "PROBE arm=coordinate probe_rows_per_second={:.0f} "
      "matches_per_second={:.0f}\n",
      coordinateIterationsPerSecond * benchmark->probeRows(),
      coordinateIterationsPerSecond * benchmark->expectedMatches());
  std::cout << fmt::format(
      "PROBE arm=velox_native_row probe_rows_per_second={:.0f} "
      "matches_per_second={:.0f}\n",
      nativeIterationsPerSecond * benchmark->probeRows(),
      nativeIterationsPerSecond * benchmark->expectedMatches());
  std::cout
      << "EMIT_NOTE A measures copy deferral only, not end-to-end savings; "
         "B1 materializes all build rows; B2 materializes only rows surviving "
         "the downstream filter. Lazy-output benefit depends on downstream "
         "filtering or dictionary consumption.\n";

  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
