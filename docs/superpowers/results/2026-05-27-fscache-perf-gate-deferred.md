# FsCache Phase-2 Perf Gate — Deferred

**Status**: Not run. Deferred pending spec §9.4 threshold reconciliation.

## Why this run did not produce numbers

Phase-1's `FsCacheBenchmark.cpp` measurement loop just called `getOrSet`
and discarded the holder. That worked because phase-1 `getOrSet`
synchronously downloaded inside, so a single call exercised the full
hit-path / miss-path.

Task 9 of the CH-aligned redesign moved download responsibility to the
caller (`FsCacheBufferedInput::load`). After Task 9, calling `getOrSet`
alone — as the phase-1 microbench still does — produces:

- `hit%` = 0 (no `recordHit` / `recordMiss` is ever invoked)
- `evic_count` = 0 (cache never inserts anything tracked)
- `ops/s` reflects only metadata lookup + holder construction cost,
  not the hit-path cost spec §9.4's 7.0 M ops/s threshold was set
  against

Running `velox_fscache_benchmark --ops=200000 --warmup_ops=20000
--min_wall_seconds=2.0` against HEAD `c6636bc18` (benchmark 6-arg fix)
on this host produced:

| workload   | t  | ws_mult | lat_us | ops/s   | hit% | wallSec |
|------------|----|---------|--------|---------|------|---------|
| sequential | 1  | 0.50    | 0      | 8628759 | 0.0% | 1.876   |
| sequential | 16 | 0.50    | 0      | 2440679 | 0.0% | 1.843   |

8.6 M and 2.4 M ops/s nominally exceed and fall below the throughput /
scaling gates, but the `hit% = 0` column reveals the measurement is
not what spec §9.4 intended.

Attempting to fix the microbench by adding the full caller-driven
advance loop (mirroring `FsCacheBufferedInput::load`'s reserve / write
/ complete + recordMiss + waitForDownloadedSize) inside the
measurement loop took the cell run time from milliseconds to many
minutes per cell (`SleepyReadFile`'s 200μs latency × the per-segment
download chunk count × ops). A 2h11m run on this host produced
results only through cell 15 of 36 before being killed. The
caller-advance experiment was reverted (not committed).

## What the spec gate actually needs

Two paths to a real Task 16 measurement, neither cheap:

1. **Reconcile spec §9.4 thresholds with the post-redesign microbench
   shape.** The "7.0 M ops/s" baseline was a phase-1 number on
   sync-download getOrSet. The new contract (caller-driven advance)
   measures a different thing. Either:
   - Rewrite microbench cells into "pure hit-path" mode (prewarm cache
     so every measured op is `state == kDownloaded`; only call
     `recordHit`). This restores comparability with phase-1 and the
     7.0 M / 0.80× thresholds keep their meaning.
   - OR rewrite spec §9.4 to set new thresholds against the full
     caller-advance path, run a baseline once, freeze it.
   This is a plan-level decision and should not be made by guessing
   during a benchmark run.

2. **Run a dedicated, separate session** (host idle, hours-long
   budget) after option 1 lands.

## What did get verified in this session

| Gate                         | Source                                | Result          |
|------------------------------|---------------------------------------|-----------------|
| TPC-H q1-q22 equivalence     | `FsCacheTpchEquivalenceTest`           | 22/22 PASS      |
| `prefetchHitRate ≥ 0.95`     | `FsCacheBufferedInputTest::prefetchHitRateOnWarmReread` | PASS |
| `prefetchMissShare ≥ 0.80`   | `FsCacheBufferedInputTest::prefetchMissShare`           | PASS |
| Concurrent counter losslessness | `FsCacheStatsTest::concurrentIncrementsAreLossless` | PASS |
| All fscache UTs              | group0 54/54 + group1 68/68            | PASS            |
| dwio_common ctest            | 100%                                  | PASS            |

The microbench `ops/s` numbers stay unmeasured, but the **correctness
guarantees** spec asked for (equivalence, prefetch metrics, lossless
atomic increments) all hold.

## Recommended next step

Author a follow-up patch that decides whether `FsCacheBenchmark.cpp`
cells should:
- Prewarm cache + only call `recordHit` (pure hit-path mode), OR
- Run the full caller-advance loop + accept that thresholds need
  re-baselining.

That patch should land before phase-2 closure claims Task 16.
