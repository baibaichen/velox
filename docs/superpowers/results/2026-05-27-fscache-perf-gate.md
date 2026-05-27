# FsCache Phase-2 Perf Gate — Single-Thread Gate PASS after R1+R3, Scaling Gate Improves 2.5x after R2 but Still Miss

**Status**: Measured post R1 (`85f6bbc4d`) + R3 (`2933ddda7`) + R2
(`403f52755` shard hot AtomicCounters). Spec §9.4 single-thread `ops/s`
gate **passes**; 16-thread efficiency gate **still fails** but improves
from 0.186x to 0.475x. Documenting honestly per 八荣八耻 #5 — no
threshold lowering, no cherry-picked cells.

## Headline

| Metric                                                | Spec §9.4 gate | Measured     | Pass? |
|-------------------------------------------------------|----------------|--------------|-------|
| single-thread `ops/s` (sequential, ws_mult=0.5, lat=0)| ≥ 7.0 M        | **8.47 M**   | PASS (+21.0%)|
| 16-thread efficiency = t16 / (t1 × 16)                | ≥ 0.80×        | **0.475×**   | FAIL (-41%)|

Single-thread gate clears with margin. The efficiency gate metric
improved 2.5x post-R2 (0.186x -> 0.475x) — t=16 absolute throughput
jumped from 25.3 M to 64.4 M ops/s once the hot counters stopped
bouncing across cores. Remaining gap to 0.80x points to other
cross-core contention (bucket priorityMutex on miss/evict path is the
next suspect; raising LRU dedup window N=16 may also help).

## Optimization-by-optimization

| Variant         | t=1 M ops/s | t=4 M ops/s | t=16 M ops/s | t=1 gate | eff_16 |
|-----------------|------------:|------------:|-------------:|---------:|-------:|
| Baseline        |        6.60 |       13.34 |        24.72 | FAIL -5.8% | 0.234 |
| + R1            |        6.64 |       16.79 |        27.78 | FAIL -5.1% | 0.262 |
| + R1 + R3       |    **8.51** |       17.13 |        25.30 | PASS +21.5%| 0.186 |
| + R1 + R3 + R2  |    **8.47** |       16.53 |    **64.37** | PASS +21.0%| **0.475** |

R2 (shard hot counters): collapses the cross-core bouncing on the
single cache line holding all 6 AtomicCounters. Each thread bumps its
own shard (32 cache-line-padded slots, picked by thread_local id %
32). t=1 stays flat; t=4 stays flat (already had 4 distinct shards
naturally); t=16 finally scales — every recordHit() used to
invalidate the line on 15 other cores, now it touches its own line.

## What is still wrong (still FAIL)

R2 unblocked the hot-counter true-sharing, but 0.475x still misses
0.80x. Remaining candidates:

- **Bucket priorityMutex on miss/evict.** At `ws_mult=2.0, t=16` the
  cells drop to ~2.5-3.4 M ops/s (vs 64 M at ws_mult=0.5). recordMiss
  takes the bucket priorityMutex unconditionally; this is the next
  hot lock once the hit-path counters are sharded.
- **LRU dedup window N=16.** 1/16 of hits still acquire the bucket
  priorityMutex. Raising N (or making it adaptive) may help, but
  bounds LRU accuracy.
- **`QueryLimitToken` / DownloadThreadPool dispatch path** under high
  concurrency miss workload — needs re-profile under R1+R3+R2.

R2 is the cheapest available single-knob win; further work needs new
profile data.

## Final gate scoreboard

| Gate                            | Source                                                   | Result      |
|---------------------------------|----------------------------------------------------------|-------------|
| TPC-H q1-q22 equivalence        | `FsCacheTpchEquivalenceTest`                             | 22/22 PASS  |
| `prefetchHitRate ≥ 0.95`        | `FsCacheBufferedInputTest::prefetchHitRateOnWarmReread`  | PASS        |
| `prefetchMissShare ≥ 0.80`      | `FsCacheBufferedInputTest::prefetchMissShare`            | PASS        |
| Concurrent counter losslessness | `FsCacheStatsTest::concurrentIncrementsAreLossless`      | PASS        |
| All fscache UTs                 | group0 54/54 + group1 68/68                              | PASS        |
| dwio_common ctest               | 100%                                                     | PASS        |
| 36-cell microbench (no crashes) | post R2 `403f52755`                                      | PASS 36/36  |
| `single_thread_m_ops ≥ 7.0`     | spec §9.4                                                | **PASS** 8.47 |
| `efficiency_16t ≥ 0.80`         | spec §9.4                                                | **FAIL** 0.475 (was 0.186 pre-R2) |

Correctness gates all hold. Single-thread perf gate passes
post-R1+R3. Scaling gate moved 2.5x post-R2 but still misses;
remaining gap is in the miss/evict path (bucket priorityMutex).

## Methodology

- Binary: `cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark`
- HEAD: `403f52755` (R2, on top of R3 `2933ddda7`, on top of R1
  `85f6bbc4d`, on top of Task 12 `61676e7b3`).
- Gate run command:
  ```
  ./velox_fscache_benchmark --out /tmp/r2-gate.md \
    --ops=200000 --warmup_ops=20000 --min_wall_seconds=0 \
    --workloads=sequential --ws_mult_list=0.5 \
    --remote_latency_us_list=0 --num_files=16 --threads_list=1,4,16
  ```
- Full sweep command:
  ```
  ./velox_fscache_benchmark --out /tmp/r2-sweep.md \
    --ops=200000 --warmup_ops=20000 --min_wall_seconds=0 \
    --num_files=16
  ```
- Sweep wall time: ~3 seconds for all 36 cells.
- Host: chang's laptop, kernel 6.17.0-29-generic, ext4 on SSD.
- Baseline: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`.
- Hot-path profile: `docs/superpowers/results/2026-05-27-fscache-hot-path-profile.md`.
