# FsCache Phase-2 Perf Gate — Full 36-Cell Sweep, Both Gates Still Miss

**Status**: Measured (post race-fix `9a0cfd3bd`, `--num_files=16`). Both
spec §9.4 hard gates still **fail**, but the 16-thread scaling miss
shrank by ~10× vs. the prior pre-race-fix run. Documenting honestly per
八荣八耻 #5 — no threshold lowering, no cherry-picked cells.

## Headline

| Metric                                                | Spec §9.4 gate | Measured     | Pass? |
|-------------------------------------------------------|----------------|--------------|-------|
| single-thread `ops/s` (sequential, ws_mult=0.5, lat=0)| ≥ 7.0 M        | **6.60 M**   | ❌ (-5.8%) |
| 16-thread efficiency = t16 / (t1 × 16)                | ≥ 0.80×        | **0.234×**   | ❌ (-71%) |

Per-metric assertion failure means the perf gate is RED. We do **not**
lower the gate threshold to pass.

Note: 16-thread absolute throughput is **24.72 M ops/s**, i.e. the
caller did get a 3.75× speed-up out of 16 threads on the hot path —
which is a real improvement over the prior 0.02× collapse — but it is
still far from linear scaling.

## Delta vs. prior measurement (2026-05-27 pre-race-fix, `--num_files=1`)

| Metric                  | Prior   | Now (HEAD + num_files=16) | Δ          |
|-------------------------|---------|---------------------------|------------|
| Single-thread M ops/s   | 6.12    | 6.60                      | +7.8%      |
| 16-thread M ops/s       | ~2.02   | 24.72                     | +12.2×     |
| 16-thread efficiency    | 0.02×   | 0.234×                    | +11.7×     |
| Cells executed          | 22 / 36 | 36 / 36                   | full sweep |

The cell-22 `LruPolicy::onInsert` "Segment already tracked" race fix
landed in `9a0cfd3bd` — the full 36-cell sweep now runs to completion
in 3 seconds wall, no crashes. The `--num_files=16` flag means each
thread maps to its own `pathKey` bucket, so the per-bucket `KeyMutex`
split (spec §0) is exercised.

## Full 36-cell table

```
| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |       1 |    0.50 |      0 |   6597001 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.030 |
| sequential |       1 |    0.50 |    200 |   5553130 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.2 |   0.036 |
| sequential |       1 |    2.00 |      0 |   6169489 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.032 |
| sequential |       1 |    2.00 |    200 |   6695820 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.030 |
| sequential |       4 |    0.50 |      0 |  13339559 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.015 |
| sequential |       4 |    0.50 |    200 |  14164632 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.014 |
| sequential |       4 |    2.00 |      0 |  12497847 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.016 |
| sequential |       4 |    2.00 |    200 |  13699128 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.015 |
| sequential |      16 |    0.50 |      0 |  24721790 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   0.008 |
| sequential |      16 |    0.50 |    200 |  25189393 | 100.0% |     0 |          0 |       0 |    0.4 |    1.0 |    1.5 |   0.008 |
| sequential |      16 |    2.00 |      0 |   2086519 |  99.5% |  1047 |       1047 |    1047 |    0.2 |    0.4 |    0.5 |   0.096 |
| sequential |      16 |    2.00 |    200 |   2815571 |  99.6% |   708 |        708 |     708 |    0.2 |    0.4 |    0.5 |   0.071 |
| zipfian    |       1 |    0.50 |      0 |   5793314 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.035 |
| zipfian    |       1 |    0.50 |    200 |   6328320 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.032 |
| zipfian    |       1 |    2.00 |      0 |   5453148 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.037 |
| zipfian    |       1 |    2.00 |    200 |   5170746 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.039 |
| zipfian    |       4 |    0.50 |      0 |  12298014 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.4 |   0.016 |
| zipfian    |       4 |    0.50 |    200 |  12827940 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.016 |
| zipfian    |       4 |    2.00 |      0 |  12039095 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.017 |
| zipfian    |       4 |    2.00 |    200 |  15485878 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.013 |
| zipfian    |      16 |    0.50 |      0 |  25774940 | 100.0% |     0 |          0 |       0 |    0.4 |    0.8 |    1.1 |   0.008 |
| zipfian    |      16 |    0.50 |    200 |  23085318 | 100.0% |     0 |          0 |       0 |    0.4 |    1.0 |    1.7 |   0.009 |
| zipfian    |      16 |    2.00 |      0 |   3200927 |  99.6% |   753 |        753 |     753 |    0.2 |    0.4 |    0.6 |   0.062 |
| zipfian    |      16 |    2.00 |    200 |   2519747 |  99.6% |   729 |        728 |     728 |    0.2 |    0.3 |    0.6 |   0.079 |
| uniform    |       1 |    0.50 |      0 |   6112972 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.033 |
| uniform    |       1 |    0.50 |    200 |   6375259 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.031 |
| uniform    |       1 |    2.00 |      0 |   6291359 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.032 |
| uniform    |       1 |    2.00 |    200 |   6453678 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.031 |
| uniform    |       4 |    0.50 |      0 |  13236335 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.015 |
| uniform    |       4 |    0.50 |    200 |  12387200 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.016 |
| uniform    |       4 |    2.00 |      0 |  11662782 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.3 |   0.017 |
| uniform    |       4 |    2.00 |    200 |  15714211 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.013 |
| uniform    |      16 |    0.50 |      0 |  24688944 | 100.0% |     0 |          0 |       0 |    0.3 |    0.9 |    1.5 |   0.008 |
| uniform    |      16 |    0.50 |    200 |  25816315 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   0.008 |
| uniform    |      16 |    2.00 |      0 |   2155218 |  99.5% |   958 |        958 |     958 |    0.2 |    0.4 |    0.6 |   0.093 |
| uniform    |      16 |    2.00 |    200 |   1908329 |  99.6% |   786 |        786 |     786 |    0.2 |    0.4 |    0.5 |   0.105 |
```

## Pattern breakdown

### By thread count (ws_mult=0.5, lat=0 — pure hit path)

| Threads | sequential | zipfian | uniform | Per-thread |
|--------:|-----------:|--------:|--------:|-----------:|
| 1       | 6.60 M     | 5.79 M  | 6.11 M  | 1.00×      |
| 4       | 13.34 M    | 12.30 M | 13.24 M | ~0.50× ⚠ |
| 16      | 24.72 M    | 25.77 M | 24.69 M | ~0.23× ⚠ |

Even at 4 threads, per-thread throughput already halved — the scaling
penalty is **not** new at 16 threads, it begins immediately. Suggests
the contention point is on shared state hit by every op, not a
saturated bucket lock.

### By workload (ws_mult=2.0, t=16 — eviction path)

| Workload    | lat=0 ops/s | lat=200 ops/s | hit% | dl_MB |
|-------------|------------:|--------------:|-----:|------:|
| sequential  | 2.09 M      | 2.82 M        | 99.5%| ~880  |
| zipfian     | 3.20 M      | 2.52 M        | 99.6%| ~740  |
| uniform     | 2.16 M      | 1.91 M        | 99.6%| ~870  |

Under cache pressure (`ws_mult=2.0`, working set 2× cache size),
throughput collapses to 1.9–3.2 M ops/s with ~99.5% hit rate — the
remaining 0.5% is downloads + evictions, and each miss costs enough
that aggregate throughput drops ~10× from the pure-hit `ws_mult=0.5`
case. p99 stays ≤ 0.6 µs even on this path (i.e. tail latency is fine;
average is dominated by the miss handler).

### By latency injection

Adding `lat_us=200` per remote-fetch generally does **not** hurt the
pure-hit cells (all misses are zero in `ws_mult=0.5` runs because the
warm-up phase fully populates the cache), and only mildly shifts the
eviction-path cells. The cache hot path is the limiter, not network.

### Hit rate

All 36 cells report ≥ 99.5% hit rate, 24/36 at exactly 100.0%. The
caller-driven advance protocol (Task 9) is wired correctly; no false
hits, no miss-counted hits.

## What the cell-22 race fix (`9a0cfd3bd`) **did** solve

- 36-cell sweep completes without crash (previously aborted at cell 22).
- 16-thread efficiency climbed from 0.02× → 0.234× (a real 11.7×
  improvement on the same gate cell).
- p99 latency on the hot path stays ≤ 1.7 µs across all 16-thread cells.

## What the race fix **did not** solve

1. **Single-thread hot path still 5.8% below gate.** The race fix is
   purely a tri-state `ReserveResult` rework; it does not touch the
   single-thread code path. Suspected per-op costs from earlier
   analysis still apply:
   - Task 14's `IsPrefetch` branch in front of `++stats.hits`.
   - Task 9's `recordHit` virtual call moved out of `getOrSet`.
   - `LruPolicy::onHit` mutex (uncontended at t=1, but still locked).

2. **Per-thread throughput halves at just 4 threads** (1.00× → 0.50×).
   This is too early to blame on the per-bucket split — `--num_files=16`
   should map distinct paths to distinct buckets. Suspect candidates:
   - Shared `FsCacheStats` atomics on the every-op path.
   - `LruPolicy` global state (lookup of recency entries is per-instance,
     not per-bucket).
   - `QueryLimitToken` shared counters.

3. **Eviction-path throughput (ws_mult=2.0, t=16) is 8–13× slower** than
   the pure-hit path even at the same thread count. Acceptable for
   correctness, but means any workload that exceeds cache capacity will
   see a sharp throughput cliff. Not in the gate's scope, noted for the
   record.

## What needs to land before Task 16 can be marked PASS

1. **Profile the single-thread hot path** (`perf record` on
   `velox_fscache_benchmark --workloads=sequential --ws_mult_list=0.5
   --threads_list=1 --remote_latency_us_list=0`). The gap is only 5.8%
   — likely one or two specific call sites that grew between Task 8 and
   Task 14.

2. **Profile the 4-thread regression.** If per-thread throughput halves
   already at t=4 with distinct buckets, the bottleneck is not the
   per-bucket mutex split that spec §0 fixed. Look at `LruPolicy`
   global lock, atomic-counter false-sharing in `FsCacheStats`, and any
   shared queue in `QueryLimitToken`.

3. **Decide whether the 7.0 M / 0.80× thresholds are achievable.** If
   the bottleneck is `LruPolicy` (already documented as deferred to
   phase-2 SLRU rework in Task 13), the gate may need to wait until
   Task 13 lands rather than chase the last 5.8% under the current
   policy.

## What did get validated

| Gate                            | Source                                                   | Result      |
|---------------------------------|----------------------------------------------------------|-------------|
| TPC-H q1-q22 equivalence        | `FsCacheTpchEquivalenceTest`                             | 22/22 PASS  |
| `prefetchHitRate ≥ 0.95`        | `FsCacheBufferedInputTest::prefetchHitRateOnWarmReread`  | PASS        |
| `prefetchMissShare ≥ 0.80`      | `FsCacheBufferedInputTest::prefetchMissShare`            | PASS        |
| Concurrent counter losslessness | `FsCacheStatsTest::concurrentIncrementsAreLossless`      | PASS        |
| All fscache UTs                 | group0 54/54 + group1 68/68                              | PASS        |
| dwio_common ctest               | 100%                                                     | PASS        |
| 36-cell microbench (no crashes) | post `9a0cfd3bd`                                         | PASS        |
| `single_thread_m_ops ≥ 7.0`     | spec §9.4                                                | **FAIL** 6.60 |
| `efficiency_16t ≥ 0.80`         | spec §9.4                                                | **FAIL** 0.234 |

Correctness gates all hold. Throughput / scaling perf gates miss.

## Methodology

- Binary: `cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark`
- HEAD: `9a0cfd3bd` (`ReserveResult` tri-state race fix) on top of
  Task 12 (`61676e7b3`).
- Command:
  ```
  ./velox_fscache_benchmark --out /tmp/fscache-final.md \
    --ops=200000 --warmup_ops=20000 --min_wall_seconds=0 \
    --num_files=16
  ```
- Sweep wall time: **3 seconds** for all 36 cells.
- Cell count: 38 lines in output (2 header + 36 data).
- Host: chang's laptop, kernel 6.17.0-29-generic, ext4 on SSD.
- Baseline: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`.
