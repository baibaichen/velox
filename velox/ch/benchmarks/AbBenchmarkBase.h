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

#include "velox/benchmarks/QueryBenchmarkBase.h"

DECLARE_string(input_source);
DECLARE_int32(rounds);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_string(out);

namespace facebook::velox::ch::benchmarks
{

/// Cache backend selected for the A/B sweep. Each backend routes the Hive
/// connector read path differently:
///   - kDirect:   no application cache (pure Velox `DirectBufferedInput` reads);
///   - kCbi:      native `AsyncDataCache` (+ optional `SsdCache`) via
///                `connectorQueryCtx->cache()`;
///   - kFileCache: our ClickHouse `FileCache`, reached through the Task 018a
///                `FileCacheBufferedInputBuilder` (a `FileCacheManager`-built
///                default cache), with `connectorQueryCtx->cache() == nullptr`.
enum class AbBackend
{
    kDirect,
    kCbi,
    kFileCache,
};

/// Base class for benchmarks that need the three-engine (direct / cbi /
/// filecache) A/B sweep harness: snapshot backend stats, run one query,
/// snapshot again, emit one CSV row, repeat for FLAGS_rounds x numQueries()
/// iterations. Derived classes plug in the suite-specific query count and
/// per-query plan construction; everything else is shared.
///
/// NOTE: unlike the ch-filecache original which read a bare
/// `ch::FileCache::getInstance()` singleton for the hit column, the filecache
/// hit readout here uses the process-wide `ProfileEvents` hit triplet
/// (`CachedReadBufferReadFromCache/Source/CacheWriteBytes`), snapshotted
/// before/after each query, matching the Task 018a routing (reads go through
/// our cache, not a singleton this harness owns).
class AbBenchmarkBase : public facebook::velox::QueryBenchmarkBase
{
public:
    /// Drives the A/B sweep: FLAGS_rounds outer iterations x numQueries()
    /// queries, plan construction hoisted via buildPlan() once before the round
    /// loop. Per-query wall_ms is measured by std::chrono::steady_clock around
    /// QueryBenchmarkBase::run(). Writes one CSV row per (round, query) to
    /// FLAGS_out. Returns the number of failed queries so the caller can set a
    /// non-zero exit code without re-reading the CSV.
    int32_t runAb();

    /// Selects which backend's hit column the sweep reads. Set by
    /// dispatchAbMain from --input_source before runAb().
    void setBackend(AbBackend backend) { backend_ = backend; }

    /// Sets a callback invoked at the top of each round (after the first) when
    /// --cold_each_round is set, to return the active cache backend to a cold
    /// state. dispatchAbMain wires this per backend.
    void setColdResetFn(std::function<void()> fn) { coldResetFn_ = std::move(fn); }

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

    AbBackend backend_{AbBackend::kDirect};

private:
    std::function<void()> coldResetFn_;
};

} // namespace facebook::velox::ch::benchmarks
