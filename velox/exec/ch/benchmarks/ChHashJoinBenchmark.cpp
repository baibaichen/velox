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

#include <chrono>
#include <iostream>
#include <limits>
#include <numeric>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/exec/ch/ChHashProbe.h"
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

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

struct BuildResult {
  uint64_t elapsedNanos;
  uint64_t peakBytes;
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
      matches += nativeTable_->listJoinResults(
          results,
          false,
          folly::Range(inputRows.data(), inputRows.size()),
          folly::Range(hits.data(), hits.size()),
          std::numeric_limits<uint64_t>::max());
    }
    return matches;
  }

  uint64_t expectedMatches() const {
    return buildRows_;
  }

  vector_size_t probeRows() const {
    return distinctKeys_;
  }

  uint64_t retainedVectorBytes() const {
    return retainedVectorBytes_;
  }

 private:
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
      batches.push_back(makeRowVector(children));
    }
    return batches;
  }

  RowVectorPtr makeProbeVector() {
    return makeRowVector({makeFlatVector<int64_t>(
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
  std::shared_ptr<memory::MemoryPool> coordinatePool_;
  std::shared_ptr<memory::MemoryPool> nativePool_;
  std::shared_ptr<memory::MemoryPool> scratchPool_;
  std::unique_ptr<ch::ChHashBuild> coordinateBuild_;
  std::unique_ptr<HashTable<true>> nativeTable_;
  std::unique_ptr<HashLookup> nativeLookup_;
};

std::unique_ptr<ChHashJoinBenchmark> benchmark;

BENCHMARK(CoordinateProbe) {
  folly::doNotOptimizeAway(benchmark->probeCoordinate());
}

BENCHMARK_RELATIVE(VeloxNativeRowProbe) {
  folly::doNotOptimizeAway(benchmark->probeNative());
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

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  VELOX_CHECK_GT(FLAGS_measure_iterations, 0);

  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  benchmark = std::make_unique<ChHashJoinBenchmark>(
      FLAGS_build_rows,
      FLAGS_payload_columns,
      FLAGS_fanout,
      FLAGS_batch_rows);
  const auto coordinateBuild = benchmark->buildCoordinate();
  const auto nativeBuild = benchmark->buildNative();

  VELOX_CHECK_EQ(
      benchmark->probeCoordinate(), benchmark->expectedMatches());
  VELOX_CHECK_EQ(benchmark->probeNative(), benchmark->expectedMatches());

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
      "batch_rows={} output_materialization=false\n",
      FLAGS_build_rows,
      benchmark->probeRows(),
      FLAGS_payload_columns,
      FLAGS_fanout,
      FLAGS_batch_rows);
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

  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
