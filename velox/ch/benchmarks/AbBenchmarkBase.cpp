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

#include "velox/ch/benchmarks/AbBenchmarkBase.h"

#include "velox/ch/Common/ProfileEvents.h"
#include "velox/common/caching/AsyncDataCache.h"

#include <algorithm>
#include <chrono>
#include <fstream>

DEFINE_string(
    input_source,
    "",
    "Cache backend for the A/B sweep. One of: cbi, filecache, direct. 'direct' "
    "installs no application cache (pure Velox DirectBufferedInput reads). "
    "'filecache' routes reads through the Task 018a "
    "FileCacheBufferedInputBuilder (a FileCacheManager-built default cache). "
    "Empty disables the A/B path and falls back to the legacy "
    "folly::runBenchmarks() flow.");

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
    "/tmp/velox_filecache",
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
    "cold and rounds 2+ are warm.");

// num_repeats is owned by QueryBenchmarkBase.cpp but not declared in its
// header. runAb() asserts it is 1 to keep the outer --rounds loop honest.
DECLARE_int32(num_repeats);

namespace facebook::velox::ch::benchmarks
{

namespace
{

struct AbCsvRow
{
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

void writeCsvHeader(std::ostream & out)
{
    // Column names are shared across backends so a single merge script reads
    // all three engine runs with the same parser. IMPORTANT: the hit_pct /
    // bytes_dl_mib / evict_mib columns are cross-engine, diagnostic only -- they
    // come from DIFFERENT sources per backend (filecache: ProfileEvents byte
    // triplet; cbi: AsyncDataCache::refreshStats; direct: none) and are NOT
    // directly comparable across engines. The end-to-end wall_ms is the primary,
    // apples-to-apples metric.
    out << "round,query_id,wall_ms,rows,bytes_read,hit_pct_diag,"
           "bytes_dl_mib_diag,evict_diag,op_p50_us,op_p95_us,error\n";
}

void writeCsvRow(std::ostream & out, const AbCsvRow & row)
{
    out << row.round << "," << fmt::format("q{:02d}", row.queryId) << ","
        << fmt::format("{:.3f}", row.wallMs) << "," << row.rows << ","
        << row.bytesRead << "," << fmt::format("{:.4f}", row.hitPct) << ","
        << fmt::format("{:.4f}", row.bytesDlMib) << ","
        << fmt::format("{:.4f}", row.evictMib) << ","
        << fmt::format("{:.3f}", row.opP50Us) << ","
        << fmt::format("{:.3f}", row.opP95Us) << "," << row.error << "\n";
}

double quantileUs(const std::vector<int64_t> & samplesNs, double q)
{
    if (samplesNs.empty())
    {
        return 0.0;
    }
    const auto idx = std::min(samplesNs.size() - 1, static_cast<size_t>(samplesNs.size() * q));
    return samplesNs[idx] / 1000.0;
}

// Per-query cache snapshot. All fields are monotonic process-wide counters read
// as (after - before) deltas around each query's run(); a raw cumulative read
// would be meaningless (q22-round-2 would dwarf q01-round-2 just by being
// later). Filled from DIFFERENT sources per backend -- see snapshotBackend.
struct BackendSnapshot
{
    // fromCacheBytes / fromSourceBytes are the FileCache hit-ratio triplet
    // (bytes served from cache vs re-read from source); hits/lookups are only
    // used by the cbi (AsyncDataCache) path (count-based).
    uint64_t fromCacheBytes{0};
    uint64_t fromSourceBytes{0};
    uint64_t writeBytes{0};
    uint64_t hits{0};
    uint64_t lookups{0};
    uint64_t hitBytes{0};
    uint64_t evictUnits{0};
};

BackendSnapshot snapshotBackend(AbBackend backend)
{
    BackendSnapshot s;
    switch (backend)
    {
        case AbBackend::kFileCache:
        {
            // fcbi: the process-wide ProfileEvents byte triplet, populated in
            // the FileCacheInputStream read path (Task 017 / observability).
            // Non-zero here on a real TPCH run proves the query went through
            // our FileCache = Task 018a end-to-end works.
            s.fromCacheBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheBytes);
            s.fromSourceBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromSourceBytes);
            s.writeBytes = ProfileEvents::get(ProfileEvents::CachedReadBufferCacheWriteBytes);
            break;
        }
        case AbBackend::kCbi:
        {
            if (auto * dataCache = cache::AsyncDataCache::getInstance())
            {
                const auto dc = dataCache->refreshStats();
                s.hits = static_cast<uint64_t>(dc.numHit);
                s.lookups = static_cast<uint64_t>(dc.numHit + dc.numNew + dc.numWaitExclusive);
                s.hitBytes = static_cast<uint64_t>(dc.hitBytes);
                s.evictUnits = static_cast<uint64_t>(dc.numEvict);
            }
            break;
        }
        case AbBackend::kDirect:
            // No application cache: nothing to snapshot; hit columns stay 0.
            break;
    }
    return s;
}

uint64_t nonNegDelta(uint64_t after, uint64_t before)
{
    return after >= before ? after - before : 0;
}

void populateBackendDelta(AbCsvRow & row, AbBackend backend, const BackendSnapshot & before, const BackendSnapshot & after)
{
    switch (backend)
    {
        case AbBackend::kFileCache:
        {
            const uint64_t fromCache = nonNegDelta(after.fromCacheBytes, before.fromCacheBytes);
            const uint64_t fromSource = nonNegDelta(after.fromSourceBytes, before.fromSourceBytes);
            const uint64_t written = nonNegDelta(after.writeBytes, before.writeBytes);
            const uint64_t total = fromCache + fromSource;
            // Authoritative byte-based hit ratio (design §8), aligned with CH:
            //   hit = ReadFromCacheBytes / (ReadFromCacheBytes + ReadFromSourceBytes)
            row.hitPct = total ? 100.0 * fromCache / total : 0.0;
            // bytes_dl_mib: bytes written into the cache this query (cold-fill /
            // write-amplification cost).
            row.bytesDlMib = static_cast<double>(written) / (1ULL << 20);
            // No eviction-byte counter is wired for fcbi yet (design §6a shell);
            // leave the diagnostic evict column at 0.
            row.evictMib = 0.0;
            break;
        }
        case AbBackend::kCbi:
        {
            const uint64_t lookups = nonNegDelta(after.lookups, before.lookups);
            const uint64_t hits = nonNegDelta(after.hits, before.hits);
            const uint64_t hitBytes = nonNegDelta(after.hitBytes, before.hitBytes);
            const uint64_t evictDelta = nonNegDelta(after.evictUnits, before.evictUnits);
            // cbi hit column is COUNT-based (numHit/lookups), a different unit
            // from the fcbi byte ratio -- diagnostic only, not comparable.
            row.hitPct = lookups ? 100.0 * hits / lookups : 0.0;
            row.bytesDlMib = static_cast<double>(hitBytes) / (1ULL << 20);
            row.evictMib = static_cast<double>(evictDelta);
            break;
        }
        case AbBackend::kDirect:
            // No cache column.
            break;
    }
}

} // namespace

void AbBenchmarkBase::clearCbiCache()
{
    if (cache_ != nullptr)
    {
        cache_->clear();
    }
}

int32_t AbBenchmarkBase::runAb()
{
    // Outer rounds use --rounds; inner repetition pinned to 1 because
    // QueryBenchmarkBase::run loops --num_repeats times internally.
    VELOX_USER_CHECK_EQ(FLAGS_num_repeats, 1, "--num_repeats must be 1; outer rounds use --rounds");
    VELOX_USER_CHECK(!FLAGS_out.empty(), "--out is required with --input_source");

    const int32_t numQueriesTotal = numQueries();
    std::vector<int32_t> queryIds;
    if (FLAGS_query_id == 0)
    {
        queryIds.reserve(numQueriesTotal);
        for (int32_t q = 1; q <= numQueriesTotal; ++q)
        {
            queryIds.push_back(q);
        }
    }
    else
    {
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
    for (int32_t q : queryIds)
    {
        plans.push_back(buildPlan(q));
    }

    std::ofstream csv(FLAGS_out, std::ios::out | std::ios::trunc);
    VELOX_USER_CHECK(csv.is_open(), "Failed to open --out for write: {}", FLAGS_out);
    writeCsvHeader(csv);

    int32_t failed = 0;
    for (int32_t round = 1; round <= FLAGS_rounds; ++round)
    {
        if (FLAGS_cold_each_round && round > 1 && coldResetFn_)
        {
            coldResetFn_();
        }
        for (size_t i = 0; i < queryIds.size(); ++i)
        {
            const int32_t q = queryIds[i];
            AbCsvRow row;
            row.round = round;
            row.queryId = q;
            const auto backendBefore = snapshotBackend(backend_);
            const auto wallStart = std::chrono::steady_clock::now();
            auto [cursor, results] = run(plans[i], queryConfigs_);
            const auto wallEnd = std::chrono::steady_clock::now();
            row.wallMs = std::chrono::duration<double, std::milli>(wallEnd - wallStart).count();
            if (cursor == nullptr)
            {
                row.error = "task failed (see ERROR log)";
                ++failed;
            }
            else
            {
                const auto stats = cursor->task()->taskStats();
                std::vector<int64_t> samplesNs;
                for (const auto & pipeline : stats.pipelineStats)
                {
                    if (pipeline.operatorStats.empty())
                    {
                        continue;
                    }
                    const auto & leaf = pipeline.operatorStats.front();
                    if (leaf.operatorType == "TableScan")
                    {
                        row.bytesRead += leaf.rawInputBytes;
                    }
                    row.rows += pipeline.operatorStats.back().outputPositions;
                    for (const auto & op : pipeline.operatorStats)
                    {
                        if (op.getOutputTiming.count > 0)
                        {
                            samplesNs.push_back(
                                static_cast<int64_t>(op.getOutputTiming.cpuNanos / op.getOutputTiming.count));
                        }
                    }
                }
                const auto backendAfter = snapshotBackend(backend_);
                populateBackendDelta(row, backend_, backendBefore, backendAfter);
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

} // namespace facebook::velox::ch::benchmarks
