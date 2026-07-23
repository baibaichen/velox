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

#include <functional>
#include <optional>
#include <ostream>
#include <string>

#include "velox/benchmarks/QueryBenchmarkBase.h"

DECLARE_string(input_source);
DECLARE_int32(rounds);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_string(out);

namespace facebook::velox::benchmarks {

/// One CSV row emitted per (round, query) pair by the A/B sweep.
struct AbCsvRow
{
  int round{};
  int32_t queryId{};
  double wallMs{};
  uint64_t rows{};
  uint64_t resultHash{};
  std::optional<bool> resultMatch;
  uint64_t bytesRead{};
  double hitPct{};
  double cacheReadMib{};
  double predownloadMib{};
  double evictMib{};
  uint64_t evictCount{};
  double opP50Us{};
  double opP95Us{};
  std::string error;
};

/// Point-in-time snapshot of backend counters for per-query deltas.
struct BackendSnapshot
{
  uint64_t lookups{0};
  uint64_t hits{0};
  uint64_t cacheReadBytes{0};
  uint64_t predownloadBytes{0};
  uint64_t evictedBytes{0};
  uint64_t evictionCount{0};
};

/// Writes the 15-field CSV header.
void writeCsvHeader(std::ostream& out);

/// Writes one data row.
void writeCsvRow(std::ostream& out, const AbCsvRow& row);

/// Computes the per-query delta and fills backend columns.
void populateBackendDelta(
    AbCsvRow& row,
    const BackendSnapshot& before,
    const BackendSnapshot& after);

/// Counts the total number of final result rows across all returned
/// RowVectors, skipping nullptrs. Pipeline/operator statistics never
/// contribute to this count.
uint64_t countResultRows(const std::vector<RowVectorPtr>& results);

/// Computes an exact commutative hash over every row of every returned
/// RowVector, skipping nullptrs.
uint64_t computeResultHash(const std::vector<RowVectorPtr>& results);

/// Validates reference_num_drivers against the requested num_drivers.
/// Throws on invalid combinations.
void validateReferenceDrivers(
    int32_t referenceDrivers,
    int32_t requestedDrivers);

/// Base class for benchmarks that need the FileCache-vs-CBI A/B sweep harness:
/// snapshot backend stats, run one query, snapshot again, emit one CSV row,
/// repeat for FLAGS_rounds x numQueries() iterations. Derived classes plug
/// in the suite-specific query count and per-query plan construction;
/// everything else is shared.
class AbBenchmarkBase : public facebook::velox::QueryBenchmarkBase {
 public:
  /// Drives the A/B sweep: FLAGS_rounds outer iterations x numQueries()
  /// queries, plan construction hoisted via buildPlan() once before the
  /// round loop. Per-query wall_ms is measured by std::chrono::steady_clock
  /// around QueryBenchmarkBase::run(). Writes one CSV row per (round, query)
  /// to FLAGS_out. Returns the number of failed queries (rows with non-empty
  /// error column) so the caller can set a non-zero exit code without
  /// re-reading the CSV.
  int32_t runAb();

  /// Sets a callback invoked at the top of each round (after the first) when
  /// --cold_each_round is set, to return the active cache backend to a cold
  /// state. dispatchAbMain wires this per backend: fscache reinstalls its
  /// singleton; cbi clears its AsyncDataCache; direct has no cache so the reset
  /// is a no-op.
  void setColdResetFn(std::function<void()> fn) {
    coldResetFn_ = std::move(fn);
  }

  /// Clears the CBI AsyncDataCache (cache_) if present. Used by the
  /// --cold_each_round reset path for the cbi backend; no-op otherwise.
  void clearCbiCache();

 protected:
  /// Total number of queries in this suite (TPC-H: 22).
  virtual int32_t numQueries() const = 0;

  /// Builds the plan for queryId in [1, numQueries()]. Called once per query
  /// before the round loop, then the result is reused across rounds.
  virtual facebook::velox::exec::test::TpchPlan buildPlan(int32_t queryId) = 0;

  std::unordered_map<std::string, std::string> queryConfigs_;

 private:
  std::function<void()> coldResetFn_;
};

} // namespace facebook::velox::benchmarks
