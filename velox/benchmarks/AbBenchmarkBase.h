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
#include <sys/resource.h>

#include "velox/benchmarks/AbBenchmarkMain.h"
#include "velox/benchmarks/QueryBenchmarkBase.h"
#include "velox/dwio/common/BufferedInputTrace.h"
#include "velox/exec/OperatorStats.h"
#include "velox/exec/TaskStats.h"

DECLARE_string(input_source);
DECLARE_int32(rounds);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_string(out);

namespace facebook::velox::benchmarks {

/// Delta of getrusage(RUSAGE_SELF) counters captured around the timed run.
struct RusageDelta
{
  int64_t userNanos{0};        ///< User CPU time in nanoseconds.
  int64_t systemNanos{0};      ///< System CPU time in nanoseconds.
  int64_t voluntaryCsw{0};     ///< Voluntary context switches.
  int64_t involuntaryCsw{0};   ///< Involuntary context switches.
};

/// Aggregated I/O snapshot collected from all `TableScan` operators in one
/// query's `TaskStats`. Fields map 1:1 to the runtime-stat keys exported by
/// `FileDataSource::getRuntimeStats()` and the `BufferedInput` probe.
struct ScanIoSnapshot
{
  // --- storage / local / prefetch (from FileDataSource) ---
  int64_t storageReadOps{0};
  int64_t storageReadBytes{0};
  int64_t localReadOps{0};
  int64_t localReadBytes{0};
  int64_t prefetchOps{0};
  int64_t prefetchBytes{0};

  // --- BufferedInput probe ---
  int64_t enqueueCount{0};
  int64_t enqueueBytes{0};
  int64_t nextCount{0};
  int64_t returnedBytes{0};
  int64_t seekCount{0};
  int64_t maxChunkBytes{0};  ///< Running maximum (not summed).

  // --- FileCacheBufferedInput passthrough ---
  int64_t passthroughReadBytes{0};
};

/// One CSV row emitted per (round, query) pair by the A/B sweep.
struct AbCsvRow
{
  // --- original 15 fields ---
  int round{};
  int32_t queryId{};
  double wallMs{};
  uint64_t rows{};
  uint64_t resultHash{};
  std::optional<bool> resultMatch;
  uint64_t bytesRead{};
  double hitPct{};
  uint64_t missCount{};
  uint64_t sourceReadBytes{};
  uint64_t cacheWriteBytes{};
  double cacheReadMib{};
  double predownloadMib{};
  double evictMib{};
  uint64_t evictCount{};
  double opP50Us{};
  double opP95Us{};
  std::string error;

  // --- Task-4 fields (17 additional) ---
  RusageDelta rusage;      ///< getrusage delta around run().
  ScanIoSnapshot scanIo;   ///< Aggregated TableScan I/O counters.
};

/// Point-in-time snapshot of backend counters for per-query deltas.
struct BackendSnapshot
{
  uint64_t lookups{0};
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t sourceReadBytes{0};
  uint64_t cacheWriteBytes{0};
  uint64_t cacheReadBytes{0};
  uint64_t predownloadBytes{0};
  uint64_t evictedBytes{0};
  uint64_t evictionCount{0};
};

/// Writes the 35-field CSV header.
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

/// Configuration for a trace capture pass wired through the A/B benchmark.
/// A non-empty `traceRoot` activates capture for exactly the configured
/// query+round; an empty `traceRoot` disables capture.
struct BufferedInputTraceRunConfig
{
    std::string traceRoot;         ///< Trace output directory.  Must not exist.
    std::string datasetRoot;       ///< Canonical dataset root for file IDs.
    int32_t queryId{0};            ///< Query to capture (e.g. 4 for q04).
    int32_t round{1};              ///< Round to capture; must be 1.
    std::string veloxHead;         ///< Velox HEAD commit hash.
    std::string glutenHead;        ///< Gluten HEAD commit hash.
    std::string clickhouseHead;    ///< ClickHouse HEAD commit hash.
    std::string binaryBuildId;     ///< Binary build identifier.
    uint64_t maxEvents{5'000'000}; ///< Hard cap on buffered events.
};

/// Validates a `BufferedInputTraceRunConfig` against the active benchmark
/// gate context.  When `config.traceRoot` is empty, returns immediately (no
/// gate fires).  Otherwise throws `VeloxUserError` on any violation:
///   - src must be kDirect
///   - probeEnabled must be false
///   - queryIdFlag must be in [1, numQueriesTotal]
///   - numDrivers must be 1
///   - rounds must be 1
///   - config.round must be 1
///   - config.datasetRoot must be nonempty
///   - config.veloxHead/glutenHead/clickhouseHead must be nonempty
///   - config.binaryBuildId must be nonempty
///   - config.maxEvents must be > 0
void validateBufferedInputTraceConfig(
    const BufferedInputTraceRunConfig& config,
    AbInputSource src,
    bool probeEnabled,
    int32_t queryIdFlag,
    int32_t numDrivers,
    int32_t rounds,
    int32_t numQueriesTotal = 22);

/// Computes the delta between two `rusage` snapshots. Uses checked subtraction:
/// if any counter regresses (clock wrap or measurement error), the delta is
/// clamped to zero so callers do not see negative values in the CSV.
RusageDelta computeRusageDelta(const rusage& before, const rusage& after);

/// Walks `taskStats.pipelineStats`, selects all `TableScan` operators, and
/// aggregates their runtime stats into a `ScanIoSnapshot`. Non-scan operators
/// are ignored. `maxChunkBytes` is taken as the maximum (not the sum) across
/// all contributing operators.
ScanIoSnapshot collectScanIoStats(const exec::TaskStats& taskStats);

/// Destroys a query's borrowed result vectors before their owning cursor.
///
/// `results` borrow memory owned by `cursor` (see
/// `exec::test::readCursorAsync`: "'result' borrows memory from cursor so the
/// life cycle must be shorter"), so the results must be destroyed first; the
/// cursor (and its `Task`, which closes every `TracingBufferedInput` stream) is
/// destroyed second.  Destroying the cursor first is a heap-use-after-free.
/// Templated so the destruction-order invariant can be verified by a lifetime
/// probe without a live query.
template <typename Results, typename Cursor>
void releaseResultsThenCursor(Results& results, Cursor& cursor)
{
    results.clear();
    cursor.reset();
}

/// Calls `finish` on a trace-capture guard, converting a capture failure into a
/// returned diagnostic string instead of letting the exception propagate (which
/// would escape `runAb` and reach `std::terminate`).  Returns an empty string on
/// success.  Catches only `VeloxException` (the type `finish` raises); other
/// exception types are not swallowed.
std::string finishTraceCaptureOrError(
    dwio::common::ScopedBufferedInputTraceCapture& traceGuard);

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

  /// Sets a trace-capture run config.  Must be called before runAb().
  /// An empty traceRoot in the config disables capture.
  void setBufferedInputTraceRunConfig(BufferedInputTraceRunConfig config)
  {
    traceRunConfig_ = std::move(config);
  }

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
  std::optional<BufferedInputTraceRunConfig> traceRunConfig_;
};

} // namespace facebook::velox::benchmarks
