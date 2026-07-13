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
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/ch/ChHashBuild.h"
#include "velox/exec/ch/ChHashProbe.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int64(build_rows, 400'000, "Number of unique BIGINT build keys.");
DEFINE_int32(batch_rows, 20'000, "Rows per input batch.");
DEFINE_string(
    key_distribution,
    "uniform",
    "Key distribution: uniform or sequential.");
DEFINE_int32(
    probe_iterations,
    10,
    "Measured layer-1 probe iterations after one warmup.");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

struct BuildResult {
  uint64_t elapsedNanos;
  uint64_t peakBytes;
};

struct ArmResult {
  BuildResult build;
  uint64_t probeNanos;
  uint64_t hits;
  uint64_t peakBytes;
  std::string hashMode;
};

uint64_t nanosSince(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

uint64_t splitMix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

template <typename Engine>
BuildResult runBuild(
    Engine& engine,
    const std::vector<RowVectorPtr>& batches) {
  const auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < batches.size(); ++i) {
    engine.buildBatch(i);
  }
  engine.finishBuild();
  return {nanosSince(start), engine.peakBytes()};
}

template <typename Engine>
uint64_t runProbeL1(Engine& engine, const RowVectorPtr& probes) {
  const auto start = std::chrono::steady_clock::now();
  const auto hits = engine.probeL1(probes);
  engine.setLastProbeNanos(nanosSince(start));
  return hits;
}

class ChEngine {
 public:
  ChEngine(memory::MemoryPool* pool, const std::vector<RowVectorPtr>& batches)
      : pool_(pool), build_(0, 0, pool) {
    decodedKeys_.reserve(batches.size());
    selectedRows_.reserve(batches.size());
    for (const auto& batch : batches) {
      selectedRows_.push_back(
          std::make_unique<SelectivityVector>(batch->size()));
      decodedKeys_.push_back(std::make_unique<DecodedVector>(
          *batch->childAt(0), *selectedRows_.back()));
    }
  }

  void buildBatch(size_t index) {
    build_.prepareJoinTable(*decodedKeys_[index], *selectedRows_[index]);
  }

  void finishBuild() {
    build_.noMoreInput();
  }

  uint64_t probeL1(const RowVectorPtr& probes) {
    const auto hits = ch::joinProbe(build_, probes, 0);
    folly::doNotOptimizeAway(hits.data());
    return lastHits_ = hits.size();
  }

  void verifyAllHits(uint64_t expected) const {
    VELOX_CHECK_EQ(lastHits_, expected);
  }

  uint64_t peakBytes() const {
    return pool_->peakBytes();
  }

  void setLastProbeNanos(uint64_t nanos) {
    lastProbeNanos_ = nanos;
  }

  uint64_t lastProbeNanos() const {
    return lastProbeNanos_;
  }

  const char* hashModeName() const {
    return "saved_hash";
  }

 private:
  memory::MemoryPool* pool_;
  ch::ChHashBuild build_;
  std::vector<std::unique_ptr<SelectivityVector>> selectedRows_;
  std::vector<std::unique_ptr<DecodedVector>> decodedKeys_;
  uint64_t lastProbeNanos_{0};
  uint64_t lastHits_{0};
};

class VeloxEngine {
 public:
  VeloxEngine(
      memory::MemoryPool* pool,
      const std::vector<RowVectorPtr>& batches)
      : pool_(pool) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(std::make_unique<VectorHasher>(BIGINT(), 0));
    table_ = HashTable<true>::createForJoin(
        std::move(hashers),
        {},
        true,
        false,
        false,
        1'000,
        pool_);

    decodedKeys_.reserve(batches.size());
    selectedRows_.reserve(batches.size());
    for (const auto& batch : batches) {
      selectedRows_.push_back(
          std::make_unique<SelectivityVector>(batch->size()));
      auto decoded = std::make_unique<DecodedVector>(
          *batch->childAt(0), *selectedRows_.back());
      table_->hashers().front()->decode(
          *batch->childAt(0), *selectedRows_.back());
      if (table_->hashers().front()->mayUseValueIds()) {
        raw_vector<uint64_t> valueIds(batch->size(), pool_);
        table_->hashers().front()->computeValueIds(
            *selectedRows_.back(), valueIds);
      }
      decodedKeys_.push_back(std::move(decoded));
    }
  }

  void buildBatch(size_t index) {
    auto* rows = table_->rows();
    selectedRows_[index]->applyToSelected([&](vector_size_t row) {
      auto* newRow = rows->newRow();
      if (rows->nextOffset()) {
        *reinterpret_cast<char**>(newRow + rows->nextOffset()) = nullptr;
      }
      rows->store(*decodedKeys_[index], row, newRow, 0);
    });
  }

  void finishBuild() {
    table_->prepareJoinTable(
        {},
        BaseHashTable::kNoSpillInputStartPartitionBit,
        1'000'000,
        false,
        nullptr);
    lookup_ = std::make_unique<HashLookup>(table_->hashers(), pool_);
  }

  uint64_t probeL1(const RowVectorPtr& probes) {
    SelectivityVector rows(probes->size());
    table_->prepareForJoinProbe(*lookup_, probes, rows, true);
    table_->joinProbe(*lookup_);

    folly::doNotOptimizeAway(lookup_->hits.data());
    return lookup_->rows.size();
  }

  void verifyAllHits(uint64_t expected) const {
    uint64_t hits = 0;
    for (const auto row : lookup_->rows) {
      hits += lookup_->hits[row] != nullptr;
    }
    VELOX_CHECK_EQ(hits, expected);
  }

  uint64_t peakBytes() const {
    return pool_->peakBytes();
  }

  void setLastProbeNanos(uint64_t nanos) {
    lastProbeNanos_ = nanos;
  }

  uint64_t lastProbeNanos() const {
    return lastProbeNanos_;
  }

  const char* hashModeName() const {
    switch (table_->hashMode()) {
      case BaseHashTable::HashMode::kArray:
        return "array";
      case BaseHashTable::HashMode::kNormalizedKey:
        return "normalized_key";
      case BaseHashTable::HashMode::kHash:
        return "hash";
    }
    VELOX_UNREACHABLE();
  }

 private:
  memory::MemoryPool* pool_;
  std::unique_ptr<HashTable<true>> table_;
  std::unique_ptr<HashLookup> lookup_;
  std::vector<std::unique_ptr<SelectivityVector>> selectedRows_;
  std::vector<std::unique_ptr<DecodedVector>> decodedKeys_;
  uint64_t lastProbeNanos_{0};
};

class LayerBenchmark : public VectorTestBase {
 public:
  LayerBenchmark(vector_size_t buildRows, vector_size_t batchRows)
      : buildRows_(buildRows), batchRows_(batchRows) {
    VELOX_CHECK_GT(buildRows_, 0);
    VELOX_CHECK_GT(batchRows_, 0);
    VELOX_CHECK(
        FLAGS_key_distribution == "uniform" ||
            FLAGS_key_distribution == "sequential",
        "key_distribution must be uniform or sequential");
    makeInputs();
  }

  template <typename Engine>
  ArmResult runArm(const std::string& poolName) {
    auto armPool = rootPool_->addLeafChild(poolName);
    Engine engine(armPool.get(), buildVectors_);
    const auto build = runBuild(engine, buildVectors_);

    const auto warmupHits = runProbeL1(engine, probeVector_);
    VELOX_CHECK_EQ(warmupHits, buildRows_);
    engine.verifyAllHits(buildRows_);

    uint64_t probeNanos = 0;
    uint64_t totalHits = 0;
    for (int32_t i = 0; i < FLAGS_probe_iterations; ++i) {
      const auto hits = runProbeL1(engine, probeVector_);
      VELOX_CHECK_EQ(hits, buildRows_);
      probeNanos += engine.lastProbeNanos();
      totalHits += hits;
    }
    folly::doNotOptimizeAway(totalHits);
    engine.verifyAllHits(buildRows_);
    return {
        build, probeNanos, buildRows_, engine.peakBytes(), engine.hashModeName()};
  }

 private:
  int64_t keyAt(vector_size_t row) const {
    if (FLAGS_key_distribution == "sequential") {
      return row;
    }
    return static_cast<int64_t>(splitMix64(static_cast<uint64_t>(row)));
  }

  void makeInputs() {
    std::vector<VectorPtr> probeChildren;
    probeChildren.push_back(makeFlatVector<int64_t>(
        buildRows_, [this](vector_size_t row) { return keyAt(row); }));
    probeVector_ = makeRowVector({"key"}, std::move(probeChildren));

    for (vector_size_t start = 0; start < buildRows_; start += batchRows_) {
      const auto size = std::min(batchRows_, buildRows_ - start);
      std::vector<VectorPtr> children;
      children.push_back(makeFlatVector<int64_t>(
          size,
          [this, start](vector_size_t row) { return keyAt(start + row); }));
      buildVectors_.push_back(makeRowVector({"key"}, std::move(children)));
    }
  }

  vector_size_t buildRows_;
  vector_size_t batchRows_;
  std::vector<RowVectorPtr> buildVectors_;
  RowVectorPtr probeVector_;
};

void printResult(const char* arm, const ArmResult& result) {
  const auto buildSeconds = result.build.elapsedNanos / 1e9;
  const auto probeSeconds = result.probeNanos / 1e9;
  const auto buildThroughput = FLAGS_build_rows / buildSeconds;
  const auto probeKeys =
      static_cast<double>(FLAGS_build_rows) * FLAGS_probe_iterations;
  const auto probeThroughput = probeKeys / probeSeconds;
  std::cout << "RESULT distribution=" << FLAGS_key_distribution
            << " build_rows=" << FLAGS_build_rows << " fanout=1 arm=" << arm
            << " build_wall_ms=" << result.build.elapsedNanos / 1e6
            << " build_keys_per_s=" << buildThroughput
            << " probe_wall_ms="
            << result.probeNanos / 1e6 / FLAGS_probe_iterations
            << " probe_keys_per_s=" << probeThroughput
            << " hits=" << result.hits
            << " peak_bytes=" << result.peakBytes
            << " hash_mode=" << result.hashMode << '\n';
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  VELOX_CHECK_GT(FLAGS_probe_iterations, 0);
  VELOX_CHECK_LE(FLAGS_build_rows, std::numeric_limits<vector_size_t>::max());
  VELOX_CHECK_LE(FLAGS_batch_rows, std::numeric_limits<vector_size_t>::max());

  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 10UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  LayerBenchmark benchmark(
      static_cast<vector_size_t>(FLAGS_build_rows), FLAGS_batch_rows);
  const auto ch = benchmark.runArm<ChEngine>("ChLayer1");
  const auto velox = benchmark.runArm<VeloxEngine>("VeloxLayer1");
  printResult("ch", ch);
  printResult("velox", velox);
  return 0;
}
