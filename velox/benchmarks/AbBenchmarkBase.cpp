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

#include "velox/benchmarks/AbBenchmarkBase.h"

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/fscache/FsCache.h"

#include <algorithm>
#include <chrono>
#include <fstream>

DEFINE_string(
    input_source,
    "",
    "Cache backend for the A/B sweep. One of: cbi, fscache. Empty disables "
    "the new A/B path and falls back to the legacy folly::runBenchmarks() "
    "flow (see the suite's BenchmarkMain.cpp).");

DEFINE_int32(
    rounds,
    3,
    "Outer round count for the A/B sweep. Each round runs all queries once. "
    "--num_repeats must be 1; see spec 2026-05-23-fscache-vs-cbi-tpcds 2.7.");

DEFINE_int32(
    fscache_disk_gib,
    58,
    "FsCache on-disk budget in GiB. Only used when --input_source=fscache.");

DEFINE_string(
    fscache_root,
    "/tmp/velox_fscache",
    "FsCache cache directory. Wiped at startup. "
    "Only used when --input_source=fscache.");

DEFINE_string(
    out,
    "",
    "CSV output path for the A/B sweep. "
    "Required when --input_source is set.");

// num_repeats is owned by QueryBenchmarkBase.cpp but not declared in its
// header. runAb() asserts it is 1 to keep the outer --rounds loop honest.
DECLARE_int32(num_repeats);

namespace facebook::velox::benchmarks {

namespace {

struct AbCsvRow {
  int round{};
  int32_t queryId{};
  double wallMs{};
  uint64_t rows{};
  uint64_t bytesRead{};
  double hitPct{};
  double bytesDlMib{};
  double evictMib{};
  double opP50Us{};
  double opP95Us{};
  std::string error;
};

void writeCsvHeader(std::ostream& out) {
  // Column names match spec 2026-05-23-fscache-vs-cbi-tpcds 3.2 so the
  // Task-5 merge script reads both backends with the same parser. The
  // bytes_dl_mib / evict_mib semantics differ per backend by necessity --
  // see the comment in runAb() where they are populated, and the merged
  // report header (spec 3.2 caveat).
  out << "round,query_id,wall_ms,rows,bytes_read,hit_pct,"
         "bytes_dl_mib,evict_mib,op_p50_us,op_p95_us,error\n";
}

void writeCsvRow(std::ostream& out, const AbCsvRow& row) {
  out << row.round << "," << fmt::format("q{:02d}", row.queryId) << ","
      << fmt::format("{:.3f}", row.wallMs) << "," << row.rows << ","
      << row.bytesRead << "," << fmt::format("{:.4f}", row.hitPct) << ","
      << fmt::format("{:.4f}", row.bytesDlMib) << ","
      << fmt::format("{:.4f}", row.evictMib) << ","
      << fmt::format("{:.3f}", row.opP50Us) << ","
      << fmt::format("{:.3f}", row.opP95Us) << "," << row.error << "\n";
}

// getOutputTiming is the operator hot path that emits batches; using it as the
// per-call latency sample matches what production profilers focus on. The
// alternatives (addInputTiming / finishTiming / backgroundTiming) either fire
// once per task or instrument cold paths, so they would dilute the signal.
double quantileUs(const std::vector<int64_t>& samplesNs, double q) {
  if (samplesNs.empty()) {
    return 0.0;
  }
  const auto idx = std::min(
      samplesNs.size() - 1, static_cast<size_t>(samplesNs.size() * q));
  return samplesNs[idx] / 1000.0;
}

// Both FsCache::stats() and AsyncDataCache::refreshStats() expose monotonic
// process-wide counters. To get per-query numbers we snapshot the relevant
// fields before each query's run() and record (after - before) into the CSV
// row. Without this delta the merge script in spec 3.3 would compare
// cumulative-since-start values across queries within a round, which is
// meaningless (q99-round-2 would dwarf q01-round-2 just by being later).
struct BackendSnapshot {
  uint64_t lookups{0};
  uint64_t hits{0};
  uint64_t downloadBytes{0};
  uint64_t evictUnits{0};
};

BackendSnapshot snapshotBackend() {
  BackendSnapshot s;
  if (auto* fsCache = cache::fs::FsCache::getInstance()) {
    const auto fs = fsCache->stats();
    s.hits = fs.hits;
    s.lookups = fs.hits + fs.misses;
    // bytes_dl_mib: FsCache exposes bytesOnDisk (current footprint after
    // evictions), not cumulative bytes-written. Spec 3.2 acknowledges this
    // semantic divergence -- the merged report's header carries the caveat.
    s.downloadBytes = fs.bytesOnDisk;
    // evict_mib: FsCache only tracks an eviction count. Convert to MiB via
    // alignment so the column reads sensibly; this overcounts because a
    // single eviction may be sub-alignment, but it stays in the right ballpark.
    s.evictUnits = fs.evictions;
  } else if (auto* dataCache = cache::AsyncDataCache::getInstance()) {
    const auto dc = dataCache->refreshStats();
    s.hits = static_cast<uint64_t>(dc.numHit);
    s.lookups =
        static_cast<uint64_t>(dc.numHit + dc.numNew + dc.numWaitExclusive);
    // bytes_dl_mib: AsyncDataCache exposes hitBytes (bytes served, not bytes
    // downloaded). No bytesNew-equivalent field exists; spec 3.2 caveat
    // covers this side too.
    s.downloadBytes = static_cast<uint64_t>(dc.hitBytes);
    // evict_mib: AsyncDataCache tracks an eviction count over variable-size
    // entries; reporting the raw count keeps the column populated even
    // though the unit is "count" not "MiB" on this side.
    s.evictUnits = static_cast<uint64_t>(dc.numEvict);
  }
  return s;
}

void populateBackendDelta(
    AbCsvRow& row,
    const BackendSnapshot& before,
    const BackendSnapshot& after) {
  const uint64_t lookups =
      after.lookups >= before.lookups ? after.lookups - before.lookups : 0;
  const uint64_t hits =
      after.hits >= before.hits ? after.hits - before.hits : 0;
  const uint64_t downloadDelta = after.downloadBytes >= before.downloadBytes
      ? after.downloadBytes - before.downloadBytes
      : 0;
  const uint64_t evictDelta = after.evictUnits >= before.evictUnits
      ? after.evictUnits - before.evictUnits
      : 0;
  row.hitPct = lookups ? 100.0 * hits / lookups : 0.0;
  row.bytesDlMib = static_cast<double>(downloadDelta) / (1ULL << 20);
  // FsCache: count of evictions (treated as MiB-magnitude via the alignment
  // assumption above); AsyncDataCache: raw eviction count. Spec 3.2 caveat.
  row.evictMib = static_cast<double>(evictDelta);
}

} // namespace

int32_t AbBenchmarkBase::runAb() {
  // Outer rounds use --rounds; inner repetition pinned to 1 because
  // QueryBenchmarkBase::run loops --num_repeats times internally and that
  // would N-multiply the cold-round wall time signal. See spec
  // 2026-05-23-fscache-vs-cbi-tpcds 2.7.
  VELOX_USER_CHECK_EQ(
      FLAGS_num_repeats,
      1,
      "--num_repeats must be 1; outer rounds use --rounds");
  VELOX_USER_CHECK(
      !FLAGS_out.empty(), "--out is required with --input_source");

  // Plan construction is hoisted out of the round loop so warm rounds do not
  // re-pay the JSON parse + deserialization cost (spec 4.5).
  const int32_t numQueriesTotal = numQueries();
  std::vector<exec::test::TpchPlan> plans;
  plans.reserve(numQueriesTotal);
  for (int32_t q = 1; q <= numQueriesTotal; ++q) {
    plans.push_back(buildPlan(q));
  }

  std::ofstream csv(FLAGS_out, std::ios::out | std::ios::trunc);
  VELOX_USER_CHECK(
      csv.is_open(), "Failed to open --out for write: {}", FLAGS_out);
  writeCsvHeader(csv);

  int32_t failed = 0;
  for (int32_t round = 1; round <= FLAGS_rounds; ++round) {
    for (int32_t q = 1; q <= numQueriesTotal; ++q) {
      // On the failure path below, the unfilled numeric fields stay at their
      // brace-default zeros and the error column carries the marker -- the
      // documented "all zeros + error message" row shape from spec 3.1.
      AbCsvRow row;
      row.round = round;
      row.queryId = q;
      const auto backendBefore = snapshotBackend();
      const auto wallStart = std::chrono::steady_clock::now();
      // wall_ms covers exactly one execution because the VELOX_USER_CHECK_EQ
      // on FLAGS_num_repeats above forbids the internal loop in run().
      auto [cursor, results] = run(plans[q - 1], queryConfigs_);
      const auto wallEnd = std::chrono::steady_clock::now();
      row.wallMs =
          std::chrono::duration<double, std::milli>(wallEnd - wallStart)
              .count();
      if (cursor == nullptr) {
        // run() catches std::exception, LOG(ERROR)'s it, and returns
        // {nullptr, {}}. Record a marker so downstream tooling can count
        // failures without re-reading the ERROR log.
        row.error = "task failed (see ERROR log)";
        ++failed;
      } else {
        const auto stats = cursor->task()->taskStats();
        // Match runMain's accounting: rawInputBytes lives on the leaf
        // TableScan operator of each pipeline. Output row count comes from
        // the root operator of each pipeline (last entry).
        std::vector<int64_t> samplesNs;
        for (const auto& pipeline : stats.pipelineStats) {
          if (pipeline.operatorStats.empty()) {
            continue;
          }
          const auto& leaf = pipeline.operatorStats.front();
          if (leaf.operatorType == "TableScan") {
            row.bytesRead += leaf.rawInputBytes;
          }
          row.rows += pipeline.operatorStats.back().outputPositions;
          for (const auto& op : pipeline.operatorStats) {
            if (op.getOutputTiming.count > 0) {
              samplesNs.push_back(static_cast<int64_t>(
                  op.getOutputTiming.cpuNanos / op.getOutputTiming.count));
            }
          }
        }
        const auto backendAfter = snapshotBackend();
        populateBackendDelta(row, backendBefore, backendAfter);
        std::sort(samplesNs.begin(), samplesNs.end());
        row.opP50Us = quantileUs(samplesNs, 0.50);
        row.opP95Us = quantileUs(samplesNs, 0.95);
      }
      writeCsvRow(csv, row);
      // Flush so a mid-sweep crash still leaves a parseable partial CSV.
      csv.flush();
    }
  }
  return failed;
}

} // namespace facebook::velox::benchmarks
