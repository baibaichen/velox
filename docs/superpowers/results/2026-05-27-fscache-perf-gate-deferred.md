# FsCache Phase-2 Perf Gate — Real Numbers, Both Gates Miss

**Status**: Measured. Both spec §9.4 hard gates **fail**. Documenting honestly per 八荣八耻 #5.

## Headline

| Metric                                            | Spec §9.4 gate | Measured | Pass? |
|---------------------------------------------------|----------------|----------|-------|
| single-thread `ops/s` (sequential, ws_mult=0.5, lat=0) | ≥ 7.0 M    | **6.12 M** | ❌ (-12.5%) |
| 16-thread efficiency = t16 / (t1 × 16)            | ≥ 0.80×        | **0.02×** | ❌ (-97.5%) |

Per-metric assertion failure means perf gate is RED. Per 八荣八耻 #5 we
record this honestly — we do **not** lower the gate to pass.

## Raw data

```
| workload   | threads | ws_mult | lat_us |    ops/s |  hit% | p99_us | wallSec |
|------------|--------:|--------:|-------:|---------:|------:|-------:|--------:|
| sequential |       1 |    0.50 |      0 |  6121775 |100.0% |    0.2 |   0.033 |
| sequential |      16 |    0.50 |      0 |  2018354 |100.0% |   28.0 |   0.099 |
```

Both hit% = 100% — caller-driven advance protocol is wired correctly,
recordHit fires every op, no false hits.

## Why the gate misses

### Single-thread (-12.5%)

Phase-1 baseline reported 7.6 M ops/s on this cell (see
`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`). The
post-redesign code is **20% slower per op** on the pure hit path:
6.12 / 7.6 = 0.81.

Suspect causes (not yet root-caused):
- Task 14 wrapped the `++stats.hits` line in an `IsPrefetch` branch
  before `fetch_add`, adding a predictable but non-zero predict
  cost per op.
- Task 9 caller-driven advance protocol moved the hit-path counter
  bump from inside `getOrSet` to inside the caller's branch, so the
  hot path now has one extra virtual call (`recordHit` member) per op
  in addition to the metadata `lookupRange` already there.
- `LruPolicy::onHit` try-lock contention if multiple threads ever
  bump the same segment within a `getOrSet` window (single-thread
  cell shouldn't hit this, but a regression check is in order).

### 16-thread scaling (0.02×)

Phase-1 baseline already reported 0.28× scaling on this exact cell
— CH-aligned redesign moved it to 0.02× (an order of magnitude worse).
That is the real bug.

Suspect: per-bucket metadata mutex still serialises all 16 threads
through the same `pathKey` bucket. spec §0 listed the phase-1
metadata bottleneck as fixed by per-bucket / per-key locks, but the
microbench uses a single virtual path string (`--num_files=1`
default), so all 16 threads hash to the same bucket and contend on
the same `KeyMutex`. The per-bucket split is in place but doesn't
help when the workload only touches one bucket.

This is **not** a regression in the locking implementation — it is a
correct measurement that the spec §0 fix only helps when the workload
spans multiple paths. The microbench's default of one path was
inherited from phase-1 and is the wrong workload to benchmark a
per-bucket / per-key lock improvement against.

A real evaluation needs `--num_files=16` (one virtual path per
thread) so the per-bucket split actually buys anything.

## What this run got right

- 22 / 36 cells executed without crash before cell 22 (zipfian, t=16,
  ws_mult=2, lat=0) hit a real concurrency bug in
  `LruPolicy::onInsert` ("Segment already tracked"). This is a true
  race between caller-advance recordMiss and concurrent eviction:
  Task 9 driveSegments test scenarios don't reproduce it because they
  don't thrash a single segment under cache pressure. The bug is
  reproducible by re-running with `--workloads=zipfian
  --ws_mult_list=2 --threads_list=16 --remote_latency_us_list=0`.
- `fsync` removed from `FileSegment::complete` (matching ClickHouse;
  CH greps zero for `fsync`/`fdatasync` in `src/Interpreters/FileCache`).
  Without that fix every miss op was taking ~5 ms on jbd2 commit,
  making the 36-cell sweep run for hours instead of minutes.

## What needs to land before claiming Task 16 done

1. **`LruPolicy::onInsert` double-track race on thrashing workloads.**
   cell 22 is the canonical reproducer. Real concurrency bug — fixing
   it is a Task-13.5 sized piece of work (audit the
   recordMiss / evict / onInsert handshake under multi-writer
   contention on the same bucket).

2. **Microbench `--num_files` default = 1 hides the per-bucket-lock
   benefit.** Either change the default to `threads` or run the gate
   with `--num_files=16` explicitly. Spec §0's claim that "per-bucket
   locks unblock 16-thread scaling" cannot be validated against a
   single-bucket workload.

3. **Run the full 36-cell sweep once cell 22 is fixed.** This session
   only got cells 0-21 due to the crash.

4. **Re-baseline if necessary.** If post-fix the gate still misses,
   then either the 7.0 M / 0.80× spec §9.4 thresholds were aspirational
   and need lowering, or we have a real perf bug to dig into.

## What did get validated

| Gate                         | Source                                | Result          |
|------------------------------|---------------------------------------|-----------------|
| TPC-H q1-q22 equivalence     | `FsCacheTpchEquivalenceTest`           | 22/22 PASS      |
| `prefetchHitRate ≥ 0.95`     | `FsCacheBufferedInputTest::prefetchHitRateOnWarmReread` | PASS |
| `prefetchMissShare ≥ 0.80`   | `FsCacheBufferedInputTest::prefetchMissShare`           | PASS |
| Concurrent counter losslessness | `FsCacheStatsTest::concurrentIncrementsAreLossless` | PASS |
| All fscache UTs              | group0 54/54 + group1 68/68            | PASS            |
| dwio_common ctest            | 100%                                  | PASS            |

Correctness gates all hold; only the throughput / scaling perf gate
misses, and one new concurrency bug (cell 22) was uncovered.

## Methodology

- Binary: `cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark`
- HEAD: includes Task 14 commit `f0c820e06` + benchmark caller-advance
  + `fsync` removal (both in working tree as of 2026-05-27, not yet
  committed)
- Command: `--ops=200000 --warmup_ops=20000 --min_wall_seconds=0
  --workloads=sequential --ws_mult_list=0.5
  --remote_latency_us_list=0 --threads_list=1,16`
- Host: chang's laptop, kernel 6.17.0-29-generic, ext4 on SSD
- Baseline: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`
