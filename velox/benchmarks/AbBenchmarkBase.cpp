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

#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/caching/AsyncDataCache.h"

#include <algorithm>
#include <chrono>
#include <fstream>

DEFINE_string(
    input_source,
    "",
    "Cache backend for the A/B sweep. One of: cbi, filecache, direct. 'direct' "
    "installs no application cache (pure Velox DirectBufferedInput reads). Empty "
    "disables the new A/B path and falls back to the legacy "
    "folly::runBenchmarks() flow (see the suite's BenchmarkMain.cpp).");

DEFINE_int32(
    rounds,
    3,
    "Outer round count for the A/B sweep. Each round runs all queries once. "
    "--num_repeats must be 1.");

DEFINE_int32(
    filecache_disk_gib,
    58,
    "FileCache on-disk budget in GiB. Only used when "
    "--input_source=filecache.");

DEFINE_string(
    filecache_root,
    "tmp/velox_filecache",
    "FileCache cache directory. Wiped at startup. "
    "Only used when --input_source=filecache.");

DEFINE_string(
    out,
    "",
    "CSV output path for the A/B sweep. "
    "Required when --input_source is set.");

DEFINE_int32(
    query_id,
    0,
    "If 0 (default) the A/B sweep runs every query in the suite. If positive, "
    "the sweep is restricted to that single 1-based query id (useful for "
    "isolating one query's cold-run behavior). Must be in [1, numQueries()].");

DEFINE_bool(
    cold_each_round,
    false,
    "If true, re-wipe the active cache backend at the top of every round so "
    "each round is an independent cold sample. Default false: only round 1 is "
    "cold and rounds 2+ are warm. Lets a single process collect multiple cold "
    "wall_ms samples without per-process restart overhead.");

// num_repeats is owned by QueryBenchmarkBase.cpp but not declared in its
// header. runAb() asserts it is 1 to keep the outer --rounds loop honest.
DECLARE_int32(num_repeats);

namespace facebook::velox::benchmarks {

namespace {

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

} // namespace

void writeCsvHeader(std::ostream& out) {
  out << "round,query_id,wall_ms,rows,result_hash,bytes_read,hit_pct,"
         "cache_read_mib,predownload_mib,evict_mib,evict_count,"
         "op_p50_us,op_p95_us,error\n";
}

void writeCsvRow(std::ostream& out, const AbCsvRow& row) {
  out << row.round << "," << fmt::format("q{:02d}", row.queryId) << ","
      << fmt::format("{:.3f}", row.wallMs) << "," << row.rows << ","
      << row.resultHash << "," << row.bytesRead << ","
      << fmt::format("{:.4f}", row.hitPct) << ","
      << fmt::format("{:.4f}", row.cacheReadMib) << ","
      << fmt::format("{:.4f}", row.predownloadMib) << ","
      << fmt::format("{:.4f}", row.evictMib) << ","
      << row.evictCount << ","
      << fmt::format("{:.3f}", row.opP50Us) << ","
      << fmt::format("{:.3f}", row.opP95Us) << "," << row.error << "\n";
}

void populateBackendDelta(
    AbCsvRow& row,
    const BackendSnapshot& before,
    const BackendSnapshot& after) {
  const uint64_t lookups =
      after.lookups >= before.lookups ? after.lookups - before.lookups : 0;
  const uint64_t hits =
      after.hits >= before.hits ? after.hits - before.hits : 0;
  const uint64_t cacheReadDelta = after.cacheReadBytes >= before.cacheReadBytes
      ? after.cacheReadBytes - before.cacheReadBytes
      : 0;
  const uint64_t predownloadDelta =
      after.predownloadBytes >= before.predownloadBytes
      ? after.predownloadBytes - before.predownloadBytes
      : 0;
  const uint64_t evictBytesDelta = after.evictedBytes >= before.evictedBytes
      ? after.evictedBytes - before.evictedBytes
      : 0;
  const uint64_t evictCountDelta = after.evictionCount >= before.evictionCount
      ? after.evictionCount - before.evictionCount
      : 0;
  row.hitPct = lookups ? 100.0 * hits / lookups : 0.0;
  row.cacheReadMib = static_cast<double>(cacheReadDelta) / (1ULL << 20);
  row.predownloadMib = static_cast<double>(predownloadDelta) / (1ULL << 20);
  row.evictMib = static_cast<double>(evictBytesDelta) / (1ULL << 20);
  row.evictCount = evictCountDelta;
}

namespace {

BackendSnapshot snapshotBackend() {
  BackendSnapshot s;
  if (ch::FileCacheManager::getInstance() != nullptr) {
    const auto fc = ch::takeFileCacheStatsSnapshot();
    s.hits = fc.cacheHitCount;
    s.lookups = fc.cacheHitCount + fc.cacheMissCount;
    s.cacheReadBytes = fc.cacheReadBytes;
    s.predownloadBytes = fc.predownloadedFromSourceBytes;
    s.evictedBytes = fc.evictedBytes;
    s.evictionCount = fc.evictedSegments;
  } else if (auto* dataCache = cache::AsyncDataCache::getInstance()) {
    const auto dc = dataCache->refreshStats();
    s.hits = static_cast<uint64_t>(dc.numHit);
    s.lookups =
        static_cast<uint64_t>(dc.numHit + dc.numNew + dc.numWaitExclusive);
    // CBI: hitBytes maps to cache_read_mib; no predownload concept.
    s.cacheReadBytes = static_cast<uint64_t>(dc.hitBytes);
    s.predownloadBytes = 0;
    // CBI: no evicted-byte counter; evict_mib stays zero. evict_count = numEvict.
    s.evictedBytes = 0;
    s.evictionCount = static_cast<uint64_t>(dc.numEvict);
  }
  return s;
}

} // namespace

void AbBenchmarkBase::clearCbiCache() {
  if (cache_ != nullptr) {
    cache_->clear();
  }
}

int32_t AbBenchmarkBase::runAb() {
  // Outer rounds use --rounds; inner repetition pinned to 1 because
  // QueryBenchmarkBase::run loops --num_repeats times internally and that
  // would N-multiply the cold-round wall time signal.
  VELOX_USER_CHECK_EQ(
      FLAGS_num_repeats,
      1,
      "--num_repeats must be 1; outer rounds use --rounds");
  VELOX_USER_CHECK(
      !FLAGS_out.empty(), "--out is required with --input_source");

  // Build the list of query ids to run. --query_id=0 (default) sweeps every
  // query in the suite; a positive value restricts the sweep to that single
  // query so its cold-run behavior can be isolated.
  const int32_t numQueriesTotal = numQueries();
  std::vector<int32_t> queryIds;
  if (FLAGS_query_id == 0) {
    queryIds.reserve(numQueriesTotal);
    for (int32_t q = 1; q <= numQueriesTotal; ++q) {
      queryIds.push_back(q);
    }
  } else {
    VELOX_USER_CHECK(
        FLAGS_query_id >= 1 && FLAGS_query_id <= numQueriesTotal,
        "--query_id={} is out of range [1, {}]",
        FLAGS_query_id,
        numQueriesTotal);
    queryIds.push_back(FLAGS_query_id);
  }

  // Plan construction is hoisted out of the round loop so warm rounds do not
  // re-pay the JSON parse + deserialization cost.
  std::vector<exec::test::TpchPlan> plans;
  plans.reserve(queryIds.size());
  for (int32_t q : queryIds) {
    plans.push_back(buildPlan(q));
  }

  std::ofstream csv(FLAGS_out, std::ios::out | std::ios::trunc);
  VELOX_USER_CHECK(
      csv.is_open(), "Failed to open --out for write: {}", FLAGS_out);
  writeCsvHeader(csv);

  int32_t failed = 0;
  for (int32_t round = 1; round <= FLAGS_rounds; ++round) {
    // Round 1 is already cold (caches wiped at process startup). For rounds
    // 2+, --cold_each_round returns the active backend to a cold state so each
    // round is an independent cold sample.
    if (FLAGS_cold_each_round && round > 1 && coldResetFn_) {
      coldResetFn_();
    }
    for (size_t i = 0; i < queryIds.size(); ++i) {
      const int32_t q = queryIds[i];
      AbCsvRow row;
      row.round = round;
      row.queryId = q;
      const auto backendBefore = snapshotBackend();
      const auto wallStart = std::chrono::steady_clock::now();
      auto [cursor, results] = run(plans[i], queryConfigs_);
      const auto wallEnd = std::chrono::steady_clock::now();
      row.wallMs =
          std::chrono::duration<double, std::milli>(wallEnd - wallStart)
              .count();
      if (cursor == nullptr) {
        row.error = "task failed (see ERROR log)";
        ++failed;
      } else {
        for (const auto& rv : results) {
          if (rv == nullptr) {
            continue;
          }
          for (vector_size_t r = 0; r < rv->size(); ++r) {
            row.resultHash += rv->hashValueAt(r);
          }
        }
        const auto stats = cursor->task()->taskStats();
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
      csv.flush();
    }
  }
  return failed;
}

} // namespace facebook::velox::benchmarks
