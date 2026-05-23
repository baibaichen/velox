# FsCache vs CachedBufferedInput — TPC-DS A/B Spec

**Date:** 2026-05-23
**Branch:** `fscache-clickhouse-style`
**Status:** Draft, awaiting user review
**Predecessor:** `2026-05-23-fscache-microbench-design.md` (Phase-1 microbench baseline)

## 1. Goal & Scope

### 1.1 Goal

Decide whether the in-tree FsCache (ClickHouse-style local SSD cache) is
competitive with Velox's existing `CachedBufferedInput +
AsyncDataCache(RAM) + SsdCache(SSD)` stack ("CBI" for short) on a
realistic OLAP workload, and surface the categories of queries where it
wins or loses.

The deliverable is a structured comparison table from running TPC-DS
scale-factor 100 across both backends, **not** a binary
"FsCache wins / loses" verdict.

### 1.2 In scope

- Add `FsCache::setInstance` / `getInstance` (mirror
  `AsyncDataCache`'s singleton).
- Add `QueryCtx::fsCache_` slot (default-init from
  `FsCache::getInstance()`).
- Append `fsCache` param at the end of `ConnectorQueryCtx` ctor (after
  `tokenProvider`, with `nullptr` default).
- Thread `queryCtx->fsCache()` through
  `OperatorCtx::createConnectorQueryCtx`.
- Add new branch in `HiveConnectorUtil::createBufferedInput` that
  picks `FsCacheBufferedInput` when fsCache is non-null.
- Replace `TpcdsBenchmark`'s `folly::runBenchmarks()`-based main
  with a custom loop that hoists plan construction out of the round
  loop (see §4.5 — non-negotiable, not conditional).
- Add `--input_source={cbi,fscache}` + `--fscache_disk_gib` +
  `--fscache_root` + `--out` to `TpcdsBenchmarkMain`. Reuse existing
  `--cache_gb`, `--ssd_cache_gb`, `--ssd_path`, `--num_repeats`,
  `--num_drivers` flags from `QueryBenchmarkBase`.
- Run TPC-DS 99 queries × 3 rounds × 2 input sources = 594 query
  executions, organized as **two processes** (one per backend),
  3 rounds per process.
- Produce a Markdown report with cold/warm/summary tables to
  `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds.md`.

These map to 6 commits per §5.2.

### 1.3 Out of scope (explicit non-goals)

- **Not** a cache-algorithm A/B: with 58 GiB total cache vs
  ~30 GiB working set, warm rounds will hit near 100%. The numbers
  measure **steady-state read-path overhead**, not eviction quality.
- **No** multi-query concurrency. Each query runs serially; within a
  query Velox uses `--num_drivers=4` (the existing
  `QueryBenchmarkBase` default; both sides identical).
- **No** cold-disk timings. The dataset and cache roots are assumed to
  live on local SSD.
- **No** OS page cache drop between rounds. Round 1 is "cold relative
  to AsyncDataCache/FsCache", not "cold relative to the kernel".
- **No** failure injection (disk-full, file deleted mid-read).
- **No** changes to FsCache internals — this spec is integration only.

### 1.4 Load-bearing assumptions

These must hold or the spec's conclusions don't transfer:

1. Dwio readers in the TPC-DS query path never call
   `BufferedInput::cacheRegion` or `findCachedRegion`.
   **Status:** verified by `grep` 2026-05-23 — 0 callers anywhere in
   the repo outside `CachedBufferedInput` itself and its tests.
2. `FsCacheBufferedInput::enqueue + load` survives real Parquet
   files end-to-end.
   **Status:** *unverified*. Spec Step 0 is a smoke test on a real
   `store_sales` Parquet file before any benchmark work proceeds.
3. The dataset at
   `/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned/`
   is complete enough for all 99 queries.
   **Status:** verified 2026-05-23 — directory listed (24 tables
   present); per-query column coverage is not pre-validated — failures
   surface per-query in the `error` column.
4. Both `CachedBufferedInput::hasCache()` and
   `FsCacheBufferedInput::hasCache()` return `true`, so the two paths
   are symmetric w.r.t. how dwio callers (e.g. MetadataCache) decide
   to skip their own raw-byte caching.
   **Status:** verified 2026-05-23 by reading
   `velox/dwio/common/CachedBufferedInput.h:193` and
   `velox/dwio/common/FsCacheBufferedInput.h:60`. If either side ever
   returns `false`, observed cache stats on that side would mix dwio's
   own caching with the backend's, and the A/B is no longer
   apples-to-apples.

## 2. Architecture

The benchmark must pick the dwio buffered-input backend per query without
restructuring Velox internals. The existing `AsyncDataCache` plumbing
already does this exact job for the CBI side: a process-global singleton
(`AsyncDataCache::getInstance()`), defaulted into `QueryCtx`, threaded
into `ConnectorQueryCtx::cache_` by `OperatorCtx::createConnectorQueryCtx`,
read by `HiveConnectorUtil::createBufferedInput` to pick
`CachedBufferedInput`. We mirror that chain end-to-end for FsCache.

### 2.1 FsCache singleton — new

`velox/common/caching/fscache/FsCache.h/cpp` currently has no singleton
(verified by grep: `FsCache::setInstance` and `FsCache::getInstance` are
zero hits today). Add the standard pair, mirroring
`AsyncDataCache::setInstance` / `getInstance`
(`velox/common/caching/AsyncDataCache.cpp:840`):

```cpp
class FsCache {
 public:
  static FsCache* getInstance() { return instance_; }
  static void setInstance(FsCache* instance) { instance_ = instance; }
 private:
  static FsCache* instance_;  // raw pointer, lifetime managed by caller
};
```

Bench main owns the `unique_ptr<FsCache>` and calls `setInstance` /
clears on exit, exactly as `QueryBenchmarkBase.cpp:195` does for
`AsyncDataCache`.

### 2.2 QueryCtx — new `fsCache_` slot

`velox/core/QueryCtx.h:205` default-initializes `cache_` from
`AsyncDataCache::getInstance()`. Add a parallel `fsCache_`:

```cpp
// in QueryCtx::Builder and the two QueryCtx ctors (h:118, h:402)
cache::fs::FsCache* fsCache_{cache::fs::FsCache::getInstance()};
```

Plus a getter:

```cpp
cache::fs::FsCache* fsCache() const { return fsCache_; }
```

The default-init means every `QueryCtx` constructed during a bench run
automatically sees the FsCache the bench installed via `setInstance`,
without changing any test or production call site that constructs
`QueryCtx` without arguments.

### 2.3 ConnectorQueryCtx — new `fsCache_` slot, appended

`velox/connectors/Connector.h:440` lists the 14 positional ctor
parameters. The last two are default-valued
(`cancellationToken = {}`, `tokenProvider = {}`). The new `fsCache`
parameter MUST be **appended after `tokenProvider`** with a `= nullptr`
default — inserting it anywhere in the middle would silently shift
positional args and break every call site (verified: the only caller
that passes all args by position is `OperatorCtx::createConnectorQueryCtx`
at `velox/exec/Operator.cpp:57`).

```cpp
ConnectorQueryCtx(
    memory::MemoryPool* operatorPool,
    memory::MemoryPool* connectorPool,
    const config::ConfigBase* sessionProperties,
    const common::SpillConfig* spillConfig,
    common::PrefixSortConfig prefixSortConfig,
    std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator,
    cache::AsyncDataCache* cache,
    const std::string& queryId,
    const std::string& taskId,
    const std::string& planNodeId,
    int driverId,
    const std::string& sessionTimezone,
    bool adjustTimestampToTimezone = false,
    folly::CancellationToken cancellationToken = {},
    std::shared_ptr<filesystems::TokenProvider> tokenProvider = {},
    cache::fs::FsCache* fsCache = nullptr)  // ← new, last
    : ...,
      cache_(cache),
      ...,
      fsTokenProvider_(std::move(tokenProvider)),
      fsCache_(fsCache) {
  VELOX_CHECK_NOT_NULL(sessionProperties);
}

cache::fs::FsCache* fsCache() const { return fsCache_; }

private:
  cache::fs::FsCache* const fsCache_;
```

Raw pointer (not `shared_ptr`) because lifetime is bench-process scope,
identical to how `cache_` is held as `AsyncDataCache*`.

No mutex invariant in the ctor. **Backend selection is the bench
process's job at startup time** (§2.6), not a per-`ConnectorQueryCtx`
runtime check: production code paths that legitimately set neither
(uncached scans), only `cache_` (today's default), or only `fsCache_`
(this spec's new path) all need to compile and run. A
`VELOX_CHECK(!(cache_ && fsCache_))` in the ctor would crash the bench
on the very first query if §2.6's setup logic ever drifts; the failure
mode we actually want is "bench startup refuses to install both
singletons", caught once at process start.

### 2.4 OperatorCtx::createConnectorQueryCtx — thread fsCache through

`velox/exec/Operator.cpp:57` constructs every `ConnectorQueryCtx` in
the system. Append one argument at the end of the 14-arg call:

```cpp
auto connectorQueryCtx = std::make_shared<connector::ConnectorQueryCtx>(
    pool_, connectorPool,
    task->queryCtx()->connectorSessionProperties(connectorId),
    spillConfig, driverCtx_->prefixSortConfig(),
    std::make_unique<SimpleExpressionEvaluator>(
        execCtx()->queryCtx(), execCtx()->pool()),
    task->queryCtx()->cache(),
    task->queryCtx()->queryId(),
    taskId(),
    planNodeId,
    driverCtx_->driverId,
    driverCtx_->queryConfig().sessionTimezone(),
    driverCtx_->queryConfig().adjustTimestampToTimezone(),
    task->getCancellationToken(),
    task->queryCtx()->fsTokenProvider(),
    task->queryCtx()->fsCache());          // ← new
```

This is the **only** plumbing edit needed outside the new code; no
other call site in the tree passes 14+ positional args.

### 2.5 HiveConnectorUtil::createBufferedInput — new branch

`velox/connectors/hive/HiveConnectorUtil.cpp:653` already dispatches
on `connectorQueryCtx->cache()` (line 662) vs the Nimble-style
fallback `BufferedInput` (line 681-690) vs `DirectBufferedInput`
(line 691-703). Insert a fourth branch, **first** (FsCache takes
priority because the bench startup mutex ensures CBI's `cache_` is
nullptr whenever fsCache is set):

```cpp
std::unique_ptr<BufferedInput> createBufferedInput(...) {
  if (auto* fsCache = connectorQueryCtx->fsCache()) {
    // FsCacheBufferedInput.h:45 — real signature.
    return std::make_unique<dwio::common::FsCacheBufferedInput>(
        fileHandle.file,
        readerOpts.memoryPool(),   // memory::MemoryPool& by ref
        fsCache);                  // raw pointer
  }
  if (connectorQueryCtx->cache()) {
    return std::make_unique<dwio::common::CachedBufferedInput>(...);
  }
  // Nimble + Direct branches unchanged.
}
```

The `readerOpts.memoryPool()` precedent comes from the Nimble-style
fallback branch at line 683.

### 2.6 Bench startup logic — singleton install (mutex enforcement)

`velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp` is the only place where
the two backends are mutually exclusive. Logic before any `QueryCtx`
construction:

```cpp
switch (FLAGS_input_source) {
  case "cbi":
    asyncDataCache_ = make_async_data_cache(FLAGS_cache_gb,
                                            FLAGS_ssd_cache_gb,
                                            FLAGS_ssd_path);
    cache::AsyncDataCache::setInstance(asyncDataCache_.get());
    // FsCache::getInstance() stays nullptr — do NOT call setInstance.
    break;
  case "fscache":
    fsCache_ = std::make_unique<cache::fs::FsCache>(buildFsCacheConfig());
    cache::fs::FsCache::setInstance(fsCache_.get());
    // AsyncDataCache::getInstance() stays nullptr — do NOT call setInstance.
    // Critically, this means QueryCtx::cache_ defaults to nullptr on every
    // QueryCtx the bench constructs, and HiveConnectorUtil sees only the
    // fsCache branch.
    break;
}
```

This is where the mutual-exclusion invariant lives. If neither side is
installed (programmer error), the very first query falls into
`createBufferedInput`'s `DirectBufferedInput` path and metrics would
clearly show 0% hit on both sides — diagnosable.

### 2.7 TpcdsBenchmark — flags

Reuse flags from `velox/benchmarks/QueryBenchmarkBase.cpp` wherever
possible (verified by grep). New flags only where no equivalent
exists:

| Flag | Source | Notes for this A/B |
|---|---|---|
| `--input_source={cbi,fscache}` | **new** | required, no default |
| `--cache_gb=8` | existing (QueryBenchmarkBase) | CBI RAM tier; ignored in fscache mode |
| `--ssd_cache_gb=50` | existing | CBI SSD tier; ignored in fscache mode |
| `--ssd_path=/tmp/velox_cbi_ssd` | existing | CBI SsdCache dir; ignored in fscache mode |
| `--fscache_disk_gib=58` | **new** | fscache disk budget; ignored in cbi mode |
| `--fscache_root=/tmp/velox_fscache` | **new** | fscache dir; ignored in cbi mode |
| `--rounds=3` | **new** | outer round count owned by the new custom main |
| `--num_repeats=1` | existing | MUST be 1; see "Why a new `--rounds`" below |
| `--clear_ram_cache=false` | existing | MUST be false (default) for warm rounds 2/3 |
| `--clear_ssd_cache=false` | existing | MUST be false (default) for warm rounds 2/3 |
| `--out=PATH` | **new** | CSV output path; required |
| `--num_drivers=4` | existing | use existing default (`4`); identical on both sides — `run_ab.sh` passes it explicitly to immunize against env / gflag overrides |

**Why a new `--rounds` flag instead of reusing `--num_repeats`**:
`QueryBenchmarkBase::run()` at
`velox/benchmarks/QueryBenchmarkBase.cpp:286` already loops
`FLAGS_num_repeats` times internally
(`if (++repeat >= FLAGS_num_repeats) return result;`) and returns the
cursor from the **last** inner repeat. If the new custom main also
used `--num_repeats` as its outer round counter, the result would be:

- `--num_repeats × --num_repeats` executions per query (9× at the
  spec's intended 3 rounds → 4-hour A/B becomes 12-hour A/B);
- "round 1" cursor reflects the Nth inner repeat (warm) — the cold-read
  signal is destroyed;
- `--clear_ram_cache` runs at the wrong cadence;
- §5.2 commit-4 acceptance gate "round 2 wall_ms < round 1 wall_ms"
  becomes meaningless because both rounds are already warm.

Adding `--rounds` for the outer loop and pinning `--num_repeats=1`
keeps each call to `run()` a single execution. The earlier draft's
"don't introduce a second name for the same concept" reasoning was
based on the false premise that `--num_repeats` was free to repurpose;
it isn't.

### 2.8 Shell harness — `run_ab.sh`

A separate script under `velox/benchmarks/tpcds/run_ab.sh`:

```bash
#!/usr/bin/env bash
# Two processes — one per backend — each runs 3 rounds × 99 queries.
# Cache state persists across rounds inside one process and is freshly
# constructed on the other one starting. `--clear_ram_cache` /
# `--clear_ssd_cache` are left false (their defaults) so rounds 2/3 are
# warm.
#
# Explicit `set +e` so a one-sided crash still lets the other side
# finish and the merge step still runs.
set +e

BIN=./cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark
OUT=docs/superpowers/results
mkdir -p "$OUT"

# fscache_root is wiped by the bench at startup (cold round-1 invariant);
# ssd_path is wiped by SsdCache construction (existing behavior).

"$BIN" --input_source=cbi --rounds=3 --num_repeats=1 --num_drivers=4 \
  --cache_gb=8 --ssd_cache_gb=50 --ssd_path=/tmp/velox_cbi_ssd \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv"
cbi_exit=$?

"$BIN" --input_source=fscache --rounds=3 --num_repeats=1 --num_drivers=4 \
  --fscache_disk_gib=58 --fscache_root=/tmp/velox_fscache \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv"
fscache_exit=$?

python3 scripts/bench/merge_tpcds_ab.py \
  "$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv" \
  "$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv" \
  > "$OUT/2026-05-23-fscache-vs-cbi-tpcds.md"

[[ $cbi_exit -ne 0 || $fscache_exit -ne 0 ]] && \
  echo "WARN: partial run (cbi=$cbi_exit fscache=$fscache_exit)" >&2
```

### 2.9 Data flow (end-to-end)

```
TpcdsBenchmarkMain
  parse flags ── input_source = cbi | fscache
  VELOX_CHECK_EQ(FLAGS_num_repeats, 1, "use --rounds for outer loop")
  if cbi:     AsyncDataCache::setInstance(make_cache(--cache_gb,--ssd_cache_gb,--ssd_path))
  if fscache: FsCache::setInstance(make_fscache(--fscache_disk_gib,--fscache_root))
  hoisted_plans = build all 99 plans once   # §4.5 mandatory

  for round in 1..--rounds:
    for q in queries:
      plan = hoisted_plans[q.id]
      // QueryCtx default-inits cache_ from AsyncDataCache::getInstance()
      // and fsCache_ from FsCache::getInstance(); one of them is nullptr
      // by §2.6.
      wallStart = steady_clock::now()
      result = QueryBenchmarkBase::run(plan, queryConfigs_)
      wallMs = duration<double, milli>(steady_clock::now() - wallStart).count()
      // run() catches and returns null cursor + empty results on failure.
      if result.cursor == nullptr:
        record(round, q.id, /*stats=0*/, error="task failed (see ERROR log)")
      else:
        record(round, q.id, wallMs, rows, bytes, hit%, dl, p50, p95, error="")
  csv_dump(--out)

  // Inside each query, every Operator that creates a ConnectorQueryCtx
  // (OperatorCtx::createConnectorQueryCtx) passes through both
  // queryCtx->cache() and queryCtx->fsCache(). HiveConnectorUtil::
  // createBufferedInput picks FsCacheBufferedInput when fsCache != null,
  // CachedBufferedInput when cache != null, DirectBufferedInput otherwise.
```

## 3. Output & Metrics

### 3.1 Per-process CSV (binary output)

Each process emits an 11-column CSV; one row per (round, query):

```csv
round,query_id,wall_ms,rows,bytes_read,hit_pct,bytes_dl_mib,evict_mib,op_p50_us,op_p95_us,error
1,q01,1234.5,12345,5678901,0.0,540.2,0,12.3,98.7,
1,q02,234.1,890,123456,0.0,12.5,0,3.4,21.0,
3,q99,89.3,4567,2345678,98.5,0.8,0,2.1,15.4,
```

Column semantics:
- `round` — 1 / 2 / 3
- `query_id` — `q01` … `q99`, zero-padded
- `wall_ms` — query end-to-end wall time, measured by wrapping the
  `QueryBenchmarkBase::run()` call with `std::chrono::steady_clock`
  in the new round loop (NOT `taskStats().executionEndTimeMs -
  startTimeMs`; chrono around `run()` captures the full task
  lifecycle including cursor drain and is identical on both sides)
- `rows`, `bytes_read` — aggregated from task stats
- `hit_pct`, `bytes_dl_mib`, `evict_mib` — backend stats
- `op_p50_us`, `op_p95_us` — quantile across **all** operator runtime
  samples in the task (mixed across operator kinds; noisy by design)
- `error` — empty on success, exception message on failure

### 3.2 Caveat on cross-backend stats comparability

`bytes_dl_mib` does **not** mean exactly the same thing on both sides:

- **CBI**: bytes that AsyncDataCache had to pull from the underlying
  storage (SsdCache hit ⇒ SSD→RAM; SsdCache miss ⇒ remote→RAM).
  RAM↔SSD movement within the cache tier is not surfaced as
  "downloaded".
- **FsCache**: bytes the cache wrote to its disk root, i.e.
  remote→local-disk only. Reads served from a warm segment count as
  zero.

The merge report's header section spells this out explicitly so
readers don't compare apples-to-oranges.

### 3.3 Merge script — `scripts/bench/merge_tpcds_ab.py`

~50-line Python script. Inputs: two CSVs. Output: one Markdown file
with three tables.

**Δ% convention** (used in every table below): negative means FsCache
is faster, positive means slower.

$$
\Delta\% = \frac{\text{FsCache ms} - \text{CBI ms}}{\text{CBI ms}} \times 100
$$

Geomean of `wall_ms` excludes rows with `error≠""`.

**Table A — Cold round (round=1)**

```
| query | CBI ms | FsCache ms | Δ% | CBI hit% | FsCache hit% | CBI dl MiB | FsCache dl MiB |
|-------|-------:|-----------:|---:|---------:|-------------:|-----------:|---------------:|
| q01   | 1234.5 |     1198.3 | -2.9% | 0.0% |  0.0% | 540.2 | 540.0 |
...
```

**Table B — Warm mean of round 2 + round 3**

Same column shape as Table A; `wall_ms` is the mean of rounds 2 and 3.

**Table C — Summary**

```
|                  | CBI  | FsCache | Δ |
|------------------|-----:|--------:|---|
| cold geomean ms  | 234  | 245     | +4.7% |
| warm geomean ms  |  45  |  41     | -8.9% |
| wins  (FsCache ≥5% faster) | — | 31 / 99 | |
| losses (FsCache ≥5% slower)| — | 12 / 99 | |
| regressions > 20%          | — |  3 / 99 | (q07, q23, q88) |
| failed (excluded)          | — |  0 / 99 | |
```

Win/loss threshold is 5%. Failed queries (`error≠""`) are excluded
from geomean and listed separately.

### 3.4 Output location

```
docs/superpowers/results/
├── 2026-05-23-fscache-vs-cbi-tpcds.md           # merged report
├── 2026-05-23-fscache-vs-cbi-tpcds-cbi.csv      # raw CBI runs
└── 2026-05-23-fscache-vs-cbi-tpcds-fscache.csv  # raw FsCache runs
```

The Markdown file's header captures: dataset path, both cache budgets,
host (`hostname`), kernel (`uname -r`), CPU (`lscpu | grep "Model name"`),
build mode (RelWithDebInfo / GCC-13).

## 4. Error Handling & Edges

### 4.1 Per-query isolation

`QueryBenchmarkBase::run()` at
`velox/benchmarks/QueryBenchmarkBase.cpp:254` already catches all
`std::exception` (line 260-269), logs the error, and returns
`{nullptr, {}}`. So an outer `try/catch` in our new round loop would
never fire. Detect failure by null cursor instead:

```cpp
auto [cursor, results] = QueryBenchmarkBase::run(plan, queryConfigs_);
if (!cursor) {
  // run() already LOG(ERROR)'d the exception message; we don't have it
  // here, so the CSV's `error` column gets a generic marker. Operators
  // re-running the query under `--run_query_verbose` get the original
  // message from the log.
  writer.write(round, q.id, /*empty stats*/,
               /*error=*/"task failed (see ERROR log)");
  continue;
}
// success path: drain results, snapshot taskStats, write row
writer.write(round, q.id, computeStats(cursor.get(), results),
             /*error=*/"");
```

No retry. Same query failing three rounds means a stable bug, not a
flake.

**Caveat**: losing the exception message is the price of reusing
`QueryBenchmarkBase::run` instead of duplicating its body. The
alternative (copy-paste the 30-line `run` body into our loop and add
our own try/catch) is worse — it forks the benchmark plumbing. If the
generic marker proves too coarse during diagnosis, switch to grepping
the LOG(ERROR) stream into a sidecar file rather than restructuring
`run()`.

### 4.2 Failure threshold

If `> 10` of 99 queries fail in any single (round, backend) cell,
the run is marked **INVALID** in the report header and not published
as a baseline. 10 is a soft cap: chosen so that minor schema-coverage
gaps don't kill the whole run, but a systemic bug (e.g. broken
read path) trips it.

### 4.3 Process-level failure

`run_ab.sh` runs both processes regardless of either one's exit
code, so a one-sided crash still produces the other side's table.
The merged report flags one-sided crashes in its header.

### 4.4 Cache state across rounds

- **Round 1 cold**: the cache instance is constructed empty at process
  start. `--fscache_root` is wiped before `FsCache` constructs;
  `--ssd_path` is wiped before `SsdCache` constructs. AsyncDataCache
  has no persistent state, so a fresh instance always starts with an
  empty RAM tier.
- **Rounds 2 & 3 warm**: cache state persists from the previous round.
- **OS page cache**: not actively dropped. Implication: round 1 is
  cold for application caches but the kernel may already have hot
  Parquet pages from earlier runs. The report header notes this.

### 4.5 Plan caching — mandatory hoist

`TpcdsBenchmark::runQuery` at
`velox/benchmarks/tpcds/TpcdsBenchmark.cpp:164` currently rebuilds the
query plan on every call:

```cpp
void TpcdsBenchmark::runQuery(int32_t queryId) {
  auto plan = queryBuilder_->getQueryPlan(queryId, planDir_, pool_.get());
  run(plan, queryConfigs_);
}
```

Compounding this, the existing main runs via `folly::runBenchmarks()`
(line 173) which drives 99 `BENCHMARK(tpcds_qN)` macros — there is no
"round" concept; the round structure this spec needs does not exist
today.

The implementation MUST replace `runMain`'s `folly::runBenchmarks()`
path with a custom loop:

```cpp
// Inner repeat count must be 1; the outer round loop owns repetition.
// QueryBenchmarkBase::run() at QueryBenchmarkBase.cpp:286 loops
// FLAGS_num_repeats times internally and returns the last repeat's
// cursor, so FLAGS_num_repeats > 1 here would produce
// --rounds × --num_repeats executions and hide the cold-round signal.
// See §2.7 "Why a new --rounds flag".
VELOX_CHECK_EQ(FLAGS_num_repeats, 1,
    "--num_repeats must be 1; outer rounds use --rounds");

// Build all 99 plans once, before the round loop.
// TpcdsQueryBuilder::getQueryPlan returns VeloxPlan (alias for the
// dwio-side plan struct; not core::PlanNodePtr — verified in
// velox/exec/tests/utils/TpcdsQueryBuilder.cpp:110).
std::vector<exec::test::VeloxPlan> plans;
plans.reserve(99);
for (int32_t q = 1; q <= 99; ++q) {
  plans.push_back(queryBuilder_->getQueryPlan(q, planDir_, pool_.get()));
}
// Round loop drives the existing run() helper directly.
// QueryBenchmarkBase::run takes TpchPlan (= VeloxPlan, see
// velox/exec/tests/utils/TpchQueryBuilder.h:25), so plans are
// pass-through.
for (int round = 1; round <= FLAGS_rounds; ++round) {
  for (int32_t q = 1; q <= 99; ++q) {
    measureAndRecord(round, q, plans[q - 1]);
  }
}
```

The 99 `BENCHMARK(tpcds_qN)` macros at line 215 are unused by this
A/B and stay untouched; the new path lives next to `runMain`, gated
on `--input_source` being set (the existing `BENCHMARK` flow takes
over when `--input_source` is empty, preserving the old benchmark).

### 4.6 Concurrency

- Queries run serially: no two queries concurrent.
- Per-query parallelism uses `QueryBenchmarkBase`'s existing
  `--num_drivers=4` default. Both sides identical.

### 4.7 Time budget

- Estimate: ~5 s mean per query × 99 × 3 rounds = ~25 min per process
  in the **steady-warm** case (cache hits dominate).
- Cold round 1 reads ~30 GiB working set from remote-emulated storage
  (local SSD with no FsCache layer warmed up); plausible cold-round
  inflation is 3–4×, not the 2× originally estimated, because the cold
  round pays both remote-read latency AND FsCache write-back cost on
  every miss.
- Two processes × (cold + 2 warm) ⇒ **1.5 – 4 h total wall** depending
  on how cold the cold round actually runs.
- **Step 0 + Step 5 mandate a q1-q5 subset run first** (≤ 10 min for
  both backends combined) before committing to the full sweep — this
  also calibrates the cold-round multiplier on this specific host
  before the 4-hour run.

## 5. Testing & Acceptance

### 5.1 Test pyramid

| Level | Where | What |
|---|---|---|
| Unit | `velox/core/tests/QueryCtxTest.cpp` (extend) | `QueryCtx::fsCache()` default-inits from `FsCache::getInstance()`; explicit ctor arg overrides the singleton; nullptr is the legitimate default when no singleton has been installed |
| Unit | `velox/connectors/tests/ConnectorTest.cpp` (extend) | `ConnectorQueryCtx::fsCache()` getter returns what was passed to the ctor (or `nullptr` when the new default param is omitted) |
| Unit | `velox/connectors/hive/tests/HiveConnectorUtilTest.cpp` (extend) | `createBufferedInput` branch selection: fsCache non-null ⇒ `FsCacheBufferedInput`; cache non-null ⇒ `CachedBufferedInput`; both null ⇒ `DirectBufferedInput` (Nimble branch unaffected) |
| Smoke | `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` (extend) | Open one real `store_sales` Parquet file via `FsCacheBufferedInput` + Parquet reader, scan all rows, byte-equal vs `LocalReadFile` ground truth |
| Subset E2E | `TpcdsBenchmark` | q1-q5 × 3 rounds × 2 backends — gate before full sweep |
| Full sweep | `TpcdsBenchmark` | q1-q99 × 3 rounds × 2 backends — deliverable |

### 5.2 Per-commit acceptance

| Commit | Gate |
|---|---|
| 1. FsCache singleton + QueryCtx::fsCache_ slot + ConnectorQueryCtx fsCache_ append + OperatorCtx threading + unit tests | All four edits land together — they're each one-line changes that compile together; splitting would yield 4 noop commits. Unit test: `QueryCtx::fsCache()` defaults to `FsCache::getInstance()` |
| 2. `HiveConnectorUtil::createBufferedInput` branch + unit test | Unit test green: fsCache non-null ⇒ `FsCacheBufferedInput`; cache non-null ⇒ `CachedBufferedInput`; both null ⇒ `DirectBufferedInput`; Nimble branch unaffected |
| 3. FsCacheBufferedInput parquet smoke | One real `store_sales` parquet round-trips byte-equal vs `LocalReadFile` |
| 4. TpcdsBenchmark custom main + plan hoist + flags | `--input_source=cbi --rounds=2 --num_repeats=1` on q1: round 2 `wall_ms` < round 1 `wall_ms` AND round 2 has no plan-build cost (verified by adding a debug log around `getQueryPlan`); same on `fscache` PASS |
| 5. `run_ab.sh` + `merge_tpcds_ab.py` | q1-q5 × 3 × 2 produces a merged Markdown report; tables structurally correct |
| 6. Full 99-query sweep + result commit | failures ≤ 10 / 99 on each side; result file ~210 lines committed to `docs/superpowers/results/` |

### 5.3 What counts as "FsCache wins/loses"

The spec deliberately does **not** force a binary verdict. The merge
script outputs `cold geomean / warm geomean / wins / losses / regression
list`; a human reads the table:

| Reading | Implication |
|---|---|
| FsCache warm geomean ≤ CBI warm geomean **and** regressions > 20% ≤ 5 queries | Publishable; proceed to Phase 2 |
| FsCache warm geomean > CBI warm geomean + 10% | Stop. Investigate before any further FsCache work |
| Anything in between | Case-by-case: cluster regressions by query family, decide |

### 5.4 Failure signatures that invalidate the run

| Symptom | Meaning |
|---|---|
| Smoke crashes on q1 in `enqueue/load` | `FsCacheBufferedInput` has untested code paths — pause, extend Phase-1 tests, revisit |
| Failed > 10 / 99 | Dataset incomplete or deeper bug; mark INVALID, do not publish |
| Cold-round wall differs by > 5× between sides | Implementation bug (likely a redundant download path); fix before publishing |
| FsCache warm hit% < 50% | Cache mis-wired (wrong root, wrong segment size); diagnose before publishing |

### 5.5 Out of test scope

- OS-page-cache-cold A/B (§1.3)
- Multi-query concurrency
- Failure injection (disk full, file removed mid-read)
- Memory-pressure scenarios

## 6. FsCache config (CH defaults)

The spec mandates ClickHouse-default segment sizing, **not** the
Phase-1 microbench sizing:

| Param | Microbench (Phase-1) | This A/B (Phase 2.5) |
|---|---|---|
| `alignment` | 1 MiB | **4 MiB** |
| `maxSegmentSize` | 1 MiB | **32 MiB** |
| `numBuckets` | 1024 | 1024 |
| `maxBytes` | 512 MiB | **58 GiB** |

The microbench used 1 MiB segments so each `getOrSet` op had a tight,
predictable cost. Real Parquet workloads coalesce into larger reads;
matching CH defaults is the right setting for a production-shaped A/B.

`numBuckets=1024` is unchanged — it's a hash-bucket count, not a
sizing parameter, and the Phase-1 plan's reasoning still applies at
58 GiB working set.

## 7. Open Questions

These are explicit unknowns the implementation plan must resolve in
its first steps, not in the spec:

1. **`AsyncDataCache` and `SsdCache` construction APIs.** The
   `QueryBenchmarkBase::initialize()` path at
   `velox/benchmarks/QueryBenchmarkBase.cpp:170` constructs both today
   (gated on `FLAGS_cache_gb`). The fscache-mode bench needs to skip
   this initialization (leave AsyncDataCache::getInstance() nullptr)
   and instead instantiate `cache::fs::FsCache` and call
   `FsCache::setInstance`. Open question: do this via an `--input_source=fscache`
   short-circuit inside `initialize()`, via a new
   `initFsCache()` sibling method, or by overriding `initialize()`
   in `TpcdsBenchmark`. Decide during commit-4 design. Note: §2.6's
   `make_async_data_cache()` / `make_fscache()` are pseudocode
   placeholders standing in for whichever of the three options wins;
   the dataflow is fixed (one cache lives, the other stays null) but
   the call shape is not.

2. **Smoke survival of `FsCacheBufferedInput` on Parquet.** §1.4
   assumption 2 — validated by Step 0 smoke. If `enqueue/load`
   crashes on a real Parquet file, the spec is paused (not "the spec
   is wrong" — the production code path needs Phase-1-level fixes
   first).

3. **CSV vs Markdown for the per-process binary output.** §3.1 picks
   CSV (process emits CSV; merge script renders Markdown). If the CSV
   intermediate proves a friction point in practice (e.g. parsing
   pain in the merge script), revisit during Step 5.

The previous spec draft listed two more open questions (the
`FsCacheBufferedInput` ctor signature and TpcdsBenchmark plan reuse).
Both were resolved by reading the source while writing this spec:
§2.5 cites the real ctor at `FsCacheBufferedInput.h:45`; §4.5 cites
the rebuild-every-call behavior at `TpcdsBenchmark.cpp:164` and
mandates the hoist as commit 4.

## 8. References

- Phase-1 microbench spec: `docs/superpowers/specs/2026-05-23-fscache-microbench-design.md`
- Phase-1 microbench baseline results: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`
- FsCache module design: `velox/docs/designs/fscache-clickhouse-style.md`
- FsCache Phase-1 implementation plan: `docs/superpowers/plans/2026-05-22-fscache-phase1.md`
- Plumbing chain reference:
  - `velox/common/caching/AsyncDataCache.cpp:840` (singleton pattern to mirror)
  - `velox/core/QueryCtx.h:205` (cache_ default-init pattern to mirror)
  - `velox/connectors/Connector.h:440` (ConnectorQueryCtx ctor — append at end)
  - `velox/exec/Operator.cpp:57` (createConnectorQueryCtx — only call site passing 14+ positional args)
  - `velox/connectors/hive/HiveConnectorUtil.cpp:653` (createBufferedInput dispatch point)
  - `velox/dwio/common/FsCacheBufferedInput.h:45` (real ctor signature: file, pool&, fsCache*)
  - `velox/dwio/common/BufferedInput.h:202` and `CachedBufferedInput.h:193` (hasCache symmetry)
- Existing benchmark base: `velox/benchmarks/QueryBenchmarkBase.cpp:56,64,77,78,85,90` (flag definitions reused: `num_drivers`, `num_repeats`, `ssd_path`, `ssd_cache_gb`, `clear_ram_cache`, `clear_ssd_cache`), `:170` (`initialize()` — AsyncDataCache + SsdCache construction; `--cache_gb`-gated), `:254-269` (run() swallow-and-return-null pattern)
- TpcdsBenchmark current main: `velox/benchmarks/tpcds/TpcdsBenchmark.cpp:164` (per-call plan rebuild), `:173` (folly::runBenchmarks driver to replace)
- Dataset: `/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned/` (TPC-DS scale 100, Parquet, non-partitioned)
