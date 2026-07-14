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
DEFINE_string(
    key_layout,
    "bigint",
    "Key layout: fixed, VARCHAR short/long low/high, overwide, or mixed.");
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
  engine.startBuild();
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

template <ch::ArbitraryKeyMode Mode>
class ChEngine {
 public:
  ChEngine(memory::MemoryPool* pool, const std::vector<RowVectorPtr>& batches)
      : pool_(pool), batches_(batches), build_(makeBuild(pool, batches)) {}

  static ch::ChHashBuild makeBuild(
      memory::MemoryPool* pool,
      const std::vector<RowVectorPtr>& batches) {
    std::vector<column_index_t> channels(batches.front()->childrenSize());
    std::iota(channels.begin(), channels.end(), 0);
    std::vector<TypePtr> types;
    for (const auto& child : batches.front()->children()) {
      types.push_back(child->type());
    }
    return ch::ChHashBuild(
        0, std::move(channels), std::move(types), pool, Mode);
  }

  void startBuild() {
    size_t expectedKeys = 0;
    for (const auto& batch : batches_) {
      expectedKeys += batch->size();
    }
    build_.reserve(expectedKeys);
  }

  void buildBatch(size_t index) {
    build_.addInput(batches_[index]);
  }

  void finishBuild() {
    build_.noMoreInput();
  }

  uint64_t probeL1(const RowVectorPtr& probes) {
    std::vector<column_index_t> channels(probes->childrenSize());
    std::iota(channels.begin(), channels.end(), 0);
    const auto hits = ch::joinProbe(build_, probes, channels);
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
    return build_.usesHashedKeys() ? "digest128"
        : build_.usesSerializedKeys() ? "saved_hash" : "fixed";
  }

 private:
  memory::MemoryPool* pool_;
  std::vector<RowVectorPtr> batches_;
  ch::ChHashBuild build_;
  uint64_t lastProbeNanos_{0};
  uint64_t lastHits_{0};
};

class VeloxEngine {
 public:
  VeloxEngine(
      memory::MemoryPool* pool,
      const std::vector<RowVectorPtr>& batches)
      : pool_(pool), batches_(batches) {
    const auto numKeys = batches_.front()->childrenSize();
    keyTypes_.reserve(numKeys);
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    for (column_index_t channel = 0; channel < numKeys; ++channel) {
      keyTypes_.push_back(batches_.front()->childAt(channel)->type());
      hashers.push_back(std::make_unique<VectorHasher>(
          batches_.front()->childAt(channel)->type(), channel));
    }
    table_ = HashTable<true>::createForJoin(
        std::move(hashers), {}, true, false, false, 1'000, pool_);
  }

  void startBuild() {
    const auto numKeys = batches_.front()->childrenSize();
    const bool requireGenericHash = ch::useSerializedKey(keyTypes_);
    decodedKeys_.reserve(batches_.size());
    selectedRows_.reserve(batches_.size());
    for (const auto& batch : batches_) {
      selectedRows_.push_back(
          std::make_unique<SelectivityVector>(batch->size()));
      std::vector<std::unique_ptr<DecodedVector>> decoded;
      decoded.reserve(numKeys);
      for (column_index_t channel = 0; channel < numKeys; ++channel) {
        decoded.push_back(std::make_unique<DecodedVector>(
            *batch->childAt(channel), *selectedRows_.back()));
        auto& hasher = table_->hashers()[channel];
        hasher->decode(*batch->childAt(channel), *selectedRows_.back());
        if (!requireGenericHash && hasher->mayUseValueIds()) {
          raw_vector<uint64_t> valueIds(batch->size(), pool_);
          hasher->computeValueIds(*selectedRows_.back(), valueIds);
        }
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
      for (size_t key = 0; key < decodedKeys_[index].size(); ++key) {
        rows->store(*decodedKeys_[index][key], row, newRow, key);
      }
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
    uint64_t hits = 0;
    for (const auto row : lookup_->rows) {
      hits += lookup_->hits[row] != nullptr;
    }
    return hits;
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
  std::vector<RowVectorPtr> batches_;
  std::vector<TypePtr> keyTypes_;
  std::unique_ptr<HashTable<true>> table_;
  std::unique_ptr<HashLookup> lookup_;
  std::vector<std::unique_ptr<SelectivityVector>> selectedRows_;
  std::vector<std::vector<std::unique_ptr<DecodedVector>>> decodedKeys_;
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
    VELOX_CHECK(
        FLAGS_key_layout == "bigint" || FLAGS_key_layout == "2xbigint" ||
            FLAGS_key_layout == "bigint_2xint" ||
            FLAGS_key_layout == "varchar_short_low" ||
            FLAGS_key_layout == "varchar_short_high" ||
            FLAGS_key_layout == "varchar_long_low" ||
            FLAGS_key_layout == "varchar_long_high" ||
            FLAGS_key_layout == "5xbigint" ||
            FLAGS_key_layout == "bigint_varchar",
        "unsupported key_layout");
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
    uint64_t measuredHits = 0;
    for (int32_t i = 0; i < FLAGS_probe_iterations; ++i) {
      const auto hits = runProbeL1(engine, probeVector_);
      VELOX_CHECK_EQ(hits, buildRows_);
      probeNanos += engine.lastProbeNanos();
      totalHits += hits;
      measuredHits = hits;
    }
    folly::doNotOptimizeAway(totalHits);
    engine.verifyAllHits(buildRows_);
    return {
        build, probeNanos, measuredHits, engine.peakBytes(), engine.hashModeName()};
  }

 private:
  int64_t keyAt(vector_size_t row) const {
    if (FLAGS_key_distribution == "sequential") {
      return row;
    }
    return static_cast<int64_t>(splitMix64(static_cast<uint64_t>(row)));
  }

  std::string stringKeyAt(vector_size_t row) const {
    const bool lowCardinality = FLAGS_key_layout.ends_with("_low");
    const bool longString = FLAGS_key_layout.find("_long_") != std::string::npos;
    const auto value = lowCardinality
        ? static_cast<uint64_t>(row % 1'024)
        : static_cast<uint64_t>(keyAt(row));
    auto key = std::to_string(value);
    if (longString && key.size() < 128) {
      key.append(128 - key.size(), static_cast<char>('a' + value % 26));
    }
    return key;
  }

  std::vector<VectorPtr> makeKeys(vector_size_t start, vector_size_t size) {
    std::vector<VectorPtr> keys;
    if (FLAGS_key_layout.starts_with("varchar_")) {
      keys.push_back(makeFlatVector<std::string>(
          size,
          [this, start](vector_size_t row) {
            return stringKeyAt(start + row);
          }));
      return keys;
    }

    keys.push_back(makeFlatVector<int64_t>(
        size, [this, start](vector_size_t row) { return keyAt(start + row); }));
    if (FLAGS_key_layout == "2xbigint") {
      keys.push_back(makeFlatVector<int64_t>(size, [this, start](auto row) {
        return keyAt(start + row) ^ 0x5a5a5a5a5a5a5a5aLL;
      }));
    } else if (FLAGS_key_layout == "bigint_2xint") {
      keys.push_back(makeFlatVector<int32_t>(size, [start](auto row) {
        return static_cast<int32_t>(start + row);
      }));
      keys.push_back(makeFlatVector<int32_t>(size, [start](auto row) {
        return static_cast<int32_t>((start + row) * 17);
      }));
    } else if (FLAGS_key_layout == "5xbigint") {
      for (uint64_t salt = 1; salt < 5; ++salt) {
        keys.push_back(makeFlatVector<int64_t>(
            size, [this, start, salt](auto row) {
              return keyAt(start + row) ^
                  static_cast<int64_t>(splitMix64(salt));
            }));
      }
    } else if (FLAGS_key_layout == "bigint_varchar") {
      keys.push_back(makeFlatVector<std::string>(
          size,
          [this, start](vector_size_t row) {
            return stringKeyAt(start + row);
          }));
    }
    return keys;
  }
  void makeInputs() {
    probeVector_ = makeRowVector(makeKeys(0, buildRows_));

    for (vector_size_t start = 0; start < buildRows_; start += batchRows_) {
      const auto size = std::min(batchRows_, buildRows_ - start);
      buildVectors_.push_back(makeRowVector(makeKeys(start, size)));
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
            << " key_layout=" << FLAGS_key_layout
            << " build_rows=" << FLAGS_build_rows << " fanout=1 arm=" << arm
            << " build_wall_ms=" << result.build.elapsedNanos / 1e6
            << " build_keys_per_s=" << buildThroughput
            << " probe_wall_ms="
            << result.probeNanos / 1e6 / FLAGS_probe_iterations
            << " probe_keys_per_s=" << probeThroughput
            << " hits=" << result.hits
            << " peak_bytes=" << result.peakBytes
            << " hash_mode=" << result.hashMode
            << " comparison="
            << (std::string_view(arm) != "velox"
                    ? "not-applicable"
                    : result.hashMode == "hash" ? "apples-to-apples"
                                                : "apples-to-oranges")
            << '\n';
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
  const auto serialized = benchmark.runArm<
      ChEngine<ch::ArbitraryKeyMode::kSerialized>>("ChSerializedLayer1");
  const auto hashed = benchmark.runArm<
      ChEngine<ch::ArbitraryKeyMode::kHashed>>("ChHashedLayer1");
  const auto velox = benchmark.runArm<VeloxEngine>("VeloxLayer1");
  printResult("ch_serialized", serialized);
  printResult("ch_hashed", hashed);
  printResult("velox", velox);
  return 0;
}
