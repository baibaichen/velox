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

- Wire `FsCacheBufferedInput` into the Hive connector via a new
  `ConnectorQueryCtx::fsCache()` slot.
- Add `--input_source={cbi,fscache}` to `TpcdsBenchmark`, plus the
  per-backend cache budget flags.
- Run TPC-DS 99 queries × 3 rounds × 2 input sources = 594 query
  executions, organized as **two processes** (one per backend),
  3 rounds per process.
- Produce a Markdown report with cold/warm/summary tables to
  `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds.md`.
- *(Conditional)* Hoist per-query plan construction out of
  `TpcdsBenchmark`'s round loop if inspection per §4.5 shows that
  plans are rebuilt every round (otherwise `wall_ms` includes plan
  build time on every round, not just round 1).

These map to 6 commits per §5.2 (the plan-hoist conditional commit
may collapse into commit 4 if no hoist is needed).

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
   **Status:** verified by `grep` — 0 callers anywhere in the repo
   outside `CachedBufferedInput` itself and its tests.
2. `FsCacheBufferedInput::enqueue + load` survives real Parquet
   files end-to-end.
   **Status:** *unverified*. Spec Step 0 is a smoke test on a real
   `store_sales` Parquet file before any benchmark work proceeds.
3. The dataset at
   `/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned/`
   is complete enough for all 99 queries.
   **Status:** directory listed (24 tables present); per-query column
   coverage is not pre-validated — failures surface per-query in the
   `error` column.

## 2. Architecture

### 2.1 ConnectorQueryCtx — new `fsCache_` slot

Add a parallel slot to the existing `cache_` member in
`velox/connectors/Connector.h`:

```cpp
class ConnectorQueryCtx {
 public:
  ConnectorQueryCtx(
      ...,
      cache::AsyncDataCache* cache,
      std::shared_ptr<cache::fs::FsCache> fsCache,  // new, defaults to nullptr
      ...);

  cache::AsyncDataCache* cache() const { return cache_; }

  const std::shared_ptr<cache::fs::FsCache>& fsCache() const {
    return fsCache_;
  }

 private:
  cache::AsyncDataCache* const cache_;
  const std::shared_ptr<cache::fs::FsCache> fsCache_;
};
```

**Mutual-exclusion invariant**: the constructor enforces
`VELOX_CHECK(!(cache_ && fsCache_),
"cannot enable AsyncDataCache and FsCache simultaneously")`.
Both null is allowed (uncached path); both set is a programmer
error and aborts.

**Backward compat**: `fsCache` defaults to `nullptr` in the constructor,
so all existing call sites compile unchanged.

### 2.2 createBufferedInput — new branch

`velox/connectors/hive/HiveConnectorUtil.cpp:653` currently branches
three ways (`CachedBufferedInput`, Nimble `BufferedInput`,
`DirectBufferedInput`). Insert a fourth, **first**:

```cpp
std::unique_ptr<BufferedInput> createBufferedInput(...) {
  if (connectorQueryCtx->fsCache()) {
    return std::make_unique<dwio::common::FsCacheBufferedInput>(
        fileHandle.file, connectorQueryCtx->fsCache(), readerOpts);
  }
  if (connectorQueryCtx->cache()) {
    return std::make_unique<dwio::common::CachedBufferedInput>(...);
  }
  // Nimble + Direct branches unchanged.
}
```

The `FsCacheBufferedInput` constructor signature shown above is
**tentative** — the implementation step must read the current header
and adjust (it may not need `Tracker` / `IoStatistics` the way CBI
does). See `Open Questions` §7.

### 2.3 TpcdsBenchmark — flags & backend wiring

Add to `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp`:

```
--input_source={cbi,fscache}              (required, no default)
--ram_gib=8                               CBI only — AsyncDataCache RAM tier
--ssd_gib=50                              CBI only — SsdCache SSD tier
--ssd_path=/tmp/velox_cbi_ssd             CBI only — SsdCache directory
--fscache_disk_gib=58                     fscache only — FsCache disk budget
--fscache_root=/tmp/velox_fscache         fscache only — FsCache directory
--rounds=3                                both
--out=PATH                                CSV output path (required)
```

`--num_drivers` already exists in `QueryBenchmarkBase`; the spec
mandates the default value (`4`) and identical setting on both sides.

`TpcdsBenchmark` startup logic:
- parse `--input_source`
- build either `AsyncDataCache + SsdCache` or `FsCache` accordingly
- pass the resulting handle into `ConnectorQueryCtx` via the slot
  that matches the choice
- if `--fscache_root` exists from a prior run, wipe it on startup
  (cold round 1 invariant)

### 2.4 Shell harness — `run_ab.sh`

A separate script under `velox/benchmarks/tpcds/run_ab.sh`:

```bash
#!/usr/bin/env bash
# Two processes — one per backend — each runs 3 rounds × 99 queries.
# Cold/warm rounds happen within a single process (cache state persists
# across rounds inside one process, gets reset on the other one starting).
#
# Explicit `set +e` so a one-sided crash still lets the other side
# finish and the merge step still runs.
set +e

BIN=./cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark
OUT=docs/superpowers/results
mkdir -p "$OUT"

"$BIN" --input_source=cbi --rounds=3 \
  --ram_gib=8 --ssd_gib=50 --ssd_path=/tmp/velox_cbi_ssd \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv"
cbi_exit=$?

"$BIN" --input_source=fscache --rounds=3 \
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

### 2.5 Data flow

```
TpcdsBenchmarkMain
  parse flags ── input_source = cbi | fscache
  build cache instance
  TpcdsBenchmark::run()
    for round in 1..rounds:
      for q in queries:
        plan = lookup_or_build_plan(q)         # see §4.5
        connectorQueryCtx = make_ctx(cache or fsCache)
        try:
          task.start(plan, num_drivers=4)
          while consume(task): pass
          record(round, q.id, wall, rows, bytes, hit%, dl, p50, p95, error="")
        except std::exception e:
          record(round, q.id, 0, 0, 0, 0, 0, 0, 0, error=e.what())
    csv_dump(--out)
```

Inside the per-query path, `HiveConnector → createBufferedInput`
routes to either `FsCacheBufferedInput` or `CachedBufferedInput`
based on which slot in `ConnectorQueryCtx` is set.

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
- `wall_ms` — query end-to-end wall time
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

A query that throws does not abort the run:

```cpp
try {
  result = runOneQuery(q);
  writer.write(round, q.id, result, /*error=*/"");
} catch (const std::exception& e) {
  writer.write(round, q.id, /*empty stats*/, /*error=*/e.what());
  LOG(ERROR) << "query " << q.id << " round " << round
             << " failed: " << e.what();
}
```

No retry. Same query failing three rounds means a stable bug, not a
flake.

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

### 4.5 Plan caching

`TpcdsBenchmark` is expected to build each query's plan once and reuse
across rounds, so plan-construction time doesn't pollute `wall_ms`.
The implementation step must verify this by inspection — current code
behavior is **not pre-validated in this spec**. If today's
`TpcdsBenchmark` rebuilds the plan every round, the implementation
plan must hoist the plan build out of the round loop.

### 4.6 Concurrency

- Queries run serially: no two queries concurrent.
- Per-query parallelism uses `QueryBenchmarkBase`'s existing
  `--num_drivers=4` default. Both sides identical.

### 4.7 Time budget

- Estimate: ~5 s mean per query × 99 × 3 rounds = ~25 min per process.
- Two processes ⇒ ~50 min total wall.
- Cold round 1 may be 2× slower than warm; total estimate **1.5 – 2 h**.
- **Step 0 + Step 5 mandate a q1-q5 subset run first** (≤ 5 min)
  before committing to the full sweep.

## 5. Testing & Acceptance

### 5.1 Test pyramid

| Level | Where | What |
|---|---|---|
| Unit | `velox/connectors/tests/ConnectorQueryCtxTest.cpp` (extend) | Mutex invariant: cache+fsCache both set ⇒ `VELOX_CHECK` aborts; either alone ⇒ constructs; both null ⇒ constructs |
| Unit | `velox/connectors/hive/tests/HiveConnectorUtilTest.cpp` (extend) | `createBufferedInput` branch selection: fsCache non-null ⇒ `FsCacheBufferedInput`; cache non-null ⇒ `CachedBufferedInput`; both null ⇒ `DirectBufferedInput` (Nimble branch unaffected) |
| Smoke | `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` (extend) | Open one real `store_sales` Parquet file via `FsCacheBufferedInput` + Parquet reader, scan all rows, byte-equal vs `LocalReadFile` ground truth |
| Subset E2E | `TpcdsBenchmark` | q1-q5 × 3 rounds × 2 backends — gate before full sweep |
| Full sweep | `TpcdsBenchmark` | q1-q99 × 3 rounds × 2 backends — deliverable |

### 5.2 Per-commit acceptance

| Commit | Gate |
|---|---|
| 1. ConnectorQueryCtx adds `fsCache_` + unit test | Unit test green; all existing call sites still compile |
| 2. `createBufferedInput` adds branch + unit test | Unit test green; branch selection correct |
| 3. FsCacheBufferedInput parquet smoke | One real `store_sales` parquet round-trips byte-equal vs `LocalReadFile` |
| 4. TpcdsBenchmark flags + backend wiring (+ plan-hoist if §4.5 requires) | `--rounds=1 --input_source=cbi` on q1 PASS; same on `fscache` PASS; plan build does not appear in round-2 wall time |
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

1. **`FsCacheBufferedInput` constructor signature.** The spec sketch
   in §2.2 (`file, fsCache, readerOpts`) is tentative. Step 0/1 reads
   the current header to confirm exact parameters and adjusts the
   `createBufferedInput` call site accordingly.

2. **TpcdsBenchmark plan reuse across rounds.** §4.5 documents the
   assumption that plans are built once. The implementation plan's
   first benchmark-side step must verify this by inspection and, if
   wrong, hoist plan construction out of the round loop before any
   measurement.

3. **`AsyncDataCache` and `SsdCache` construction APIs.** The spec
   names `--ram_gib` / `--ssd_gib` / `--ssd_path` flags but does not
   pin down which class constructors take these (the existing
   `CacheBackendBenchmark.cpp` on the `cachelib` branch has working
   code; the implementation plan should crib from it rather than
   guess).

4. **Whether `FsCacheBufferedInput` can satisfy a Parquet scan
   without further stubs.** Validated by Step 0 smoke. If it crashes
   in `enqueue/load`, the spec is paused, not the spec is wrong.

## 8. References

- Phase-1 microbench spec: `docs/superpowers/specs/2026-05-23-fscache-microbench-design.md`
- Phase-1 microbench baseline results: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`
- FsCache module design: `velox/docs/designs/fscache-clickhouse-style.md`
- FsCache Phase-1 implementation plan: `docs/superpowers/plans/2026-05-22-fscache-phase1.md`
- Connector hook point: `velox/connectors/hive/HiveConnectorUtil.cpp:653` (`createBufferedInput`)
- Existing benchmark base: `velox/benchmarks/QueryBenchmarkBase.cpp:56` (`--num_drivers` default)
- Dataset: `/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned/` (TPC-DS scale 100, Parquet, non-partitioned)
