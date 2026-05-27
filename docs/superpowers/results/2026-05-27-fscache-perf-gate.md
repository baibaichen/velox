# FsCache Phase-2 Perf Gate — Single-Thread Gate PASS after R1+R3, Scaling Gate Still Miss

**Status**: Measured post R1 (`85f6bbc4d` bucket.guard → KeyMutex hand-off) +
R3 (`2933ddda7` sequence-windowed LRU bump dedup), full 36-cell sweep
(`--num_files=16`). Spec §9.4 single-thread `ops/s` gate **passes**;
16-thread efficiency gate **still fails** (R1+R3 targeted the hot path
shared by all thread counts, not the scaling bottleneck). Documenting
honestly per 八荣八耻 #5 — no threshold lowering, no cherry-picked cells.

## Headline

| Metric                                                | Spec §9.4 gate | Measured     | Pass? |
|-------------------------------------------------------|----------------|--------------|-------|
| single-thread `ops/s` (sequential, ws_mult=0.5, lat=0)| ≥ 7.0 M        | **8.51 M**   | PASS (+21.5%)|
| 16-thread efficiency = t16 / (t1 × 16)                | ≥ 0.80×        | **0.186×**   | FAIL (-77%)|

Single-thread gate clears with margin. The efficiency gate metric got
worse vs. R1-only (0.234× → 0.186×) **because t=1 throughput went up
faster than t=16**; the absolute t=16 throughput is roughly flat (24.7 M
→ 25.3 M ops/s). The scaling bottleneck is therefore something the
hot-path lock collapse (R1) and LRU bump dedup (R3) do **not** touch.

## Optimization-by-optimization

Profile prediction in `docs/superpowers/results/2026-05-27-fscache-hot-path-profile.md`
was that R1 + R3 together should clear t=1; that prediction is
confirmed.

| Variant      | t=1 M ops/s | t=4 M ops/s | t=16 M ops/s | t=1 gate |
|--------------|------------:|------------:|-------------:|---------:|
| Baseline     |        6.60 |       13.34 |        24.72 | FAIL -5.8% |
| + R1         |        6.64 |       16.79 |        27.78 | FAIL -5.1% |
| + R1 + R3    |    **8.51** |       17.13 |        25.30 | PASS +21.5%|

Notes on R3's effect on the headline single-thread cell: the
`--workloads=sequential --ws_mult_list=0.5` gate cell shows **8.51 M
ops/s** in the full sweep and **7.998 M ops/s** in the headline-only
re-run (within ±10% noise on a 0.024 s wall). Both clear the 7.0 M
gate.

## Full 36-cell table (R1 + R3, HEAD = 2933ddda7)

```
| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |       1 |    0.50 |      0 |   8507634 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   0.024 |
| sequential |       1 |    0.50 |    200 |   7817842 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.026 |
| sequential |       1 |    2.00 |      0 |   8290731 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.024 |
| sequential |       1 |    2.00 |    200 |   7221842 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.028 |
| sequential |       4 |    0.50 |      0 |  17131758 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.2 |   0.012 |
| sequential |       4 |    0.50 |    200 |  16757853 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.2 |   0.012 |
| sequential |       4 |    2.00 |      0 |  17889449 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.3 |   0.011 |
| sequential |       4 |    2.00 |    200 |  15682727 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.2 |   0.013 |
| sequential |      16 |    0.50 |      0 |  25298891 | 100.0% |     0 |          0 |       0 |    0.3 |    0.9 |    1.5 |   0.008 |
| sequential |      16 |    0.50 |    200 |  26284963 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.3 |   0.008 |
| sequential |      16 |    2.00 |      0 |   2627941 | 99.5% |   952 |        952 |     952 |    0.1 |    0.3 |    0.4 |   0.076 |
| sequential |      16 |    2.00 |    200 |   2779695 | 99.6% |   704 |        704 |     704 |    0.1 |    0.3 |    0.4 |   0.072 |
| zipfian    |       1 |    0.50 |      0 |   7330085 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.027 |
| zipfian    |       1 |    0.50 |    200 |   7553686 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   0.026 |
| zipfian    |       1 |    2.00 |      0 |   6660766 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.030 |
| zipfian    |       1 |    2.00 |    200 |   6408899 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   0.031 |
| zipfian    |       4 |    0.50 |      0 |  12932630 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.5 |   0.015 |
| zipfian    |       4 |    0.50 |    200 |  16663748 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.012 |
| zipfian    |       4 |    2.00 |      0 |  14496512 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.014 |
| zipfian    |       4 |    2.00 |    200 |  14982681 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.013 |
| zipfian    |      16 |    0.50 |      0 |  25341688 | 100.0% |     0 |          0 |       0 |    0.3 |    0.9 |    1.2 |   0.008 |
| zipfian    |      16 |    0.50 |    200 |  25860452 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   0.008 |
| zipfian    |      16 |    2.00 |      0 |   3600356 | 99.6% |   733 |        733 |     733 |    0.1 |    0.3 |    0.5 |   0.056 |
| zipfian    |      16 |    2.00 |    200 |   2989919 | 99.7% |   670 |        670 |     670 |    0.1 |    0.3 |    0.4 |   0.067 |
| uniform    |       1 |    0.50 |      0 |   7764676 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   0.026 |
| uniform    |       1 |    0.50 |    200 |   7707197 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   0.026 |
| uniform    |       1 |    2.00 |      0 |   6834024 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.029 |
| uniform    |       1 |    2.00 |    200 |   6751284 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.030 |
| uniform    |       4 |    0.50 |      0 |  15371506 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.013 |
| uniform    |       4 |    0.50 |    200 |  16715817 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.2 |   0.012 |
| uniform    |       4 |    2.00 |      0 |  14645790 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.014 |
| uniform    |       4 |    2.00 |    200 |  15229356 | 100.0% |     0 |          0 |       0 |    0.2 |    0.2 |    0.3 |   0.013 |
| uniform    |      16 |    0.50 |      0 |  26162067 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   0.008 |
| uniform    |      16 |    0.50 |    200 |  26276002 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   0.008 |
| uniform    |      16 |    2.00 |      0 |   3167596 | 99.6% |   748 |        748 |     748 |    0.2 |    0.4 |    0.6 |   0.063 |
| uniform    |      16 |    2.00 |    200 |   2760529 | 99.7% |   700 |        700 |     700 |    0.2 |    0.3 |    0.5 |   0.072 |
```

## What R1 and R3 actually solved

R1 (bucket.guard → KeyMutex hand-off): collapses two of the four hot-
path mutex pairs into one nested critical section, shrinking the
bucket-level futex window to ~10 ns of pointer work. Side effect:
closed the prior best-effort lookup race where an erase could orphan
KeyMetadata between bucket unlock and KeyMutex lock.

R3 (sequence-windowed LRU bump dedup): per-hit LRU `splice`-to-MRU
on the bucket priorityMutex collapses to ~1/16 of calls via an atomic
`hits_` counter. Hot segments hit thousands of times in a burst now
take the bucket priorityMutex ~6% as often as before; uncontended t=1
sees the largest gain because every recordHit() previously took a full
mutex pair, now most just do an atomic fetch_add.

Together they remove ~3 of 4 mutex pairs from the steady-state hit
path. The benchmark gate t=1 cell climbed +20% on top of R1.

## What is still wrong (still FAIL)

**16-thread efficiency is 0.186×**, well below the 0.80× gate. R1+R3
moved the hot path's *single-core cost* down (t=1 throughput up), but
did not move the *cross-core contention floor*. Per-thread throughput
already halves at 4 threads (8.51 M → 17.13/4 = 4.28 M per thread) and
keeps degrading at 16 (25.3/16 = 1.58 M per thread). Profile suspects
that remain:

- **`FsCacheStats` atomics false-sharing.** Every recordHit/recordMiss
  touches `counters_.{demand,prefetch}{Hits,Misses}` — four atomics in
  one struct, almost certainly on the same cache line. 16 cores all
  RMW-ing the same line is the textbook cause of the half-at-4-threads
  curve.
- **`QueryLimitToken` shared counters.** Whether they hit the every-op
  path needs a re-profile under R1+R3.
- **Bucket priorityMutex still hot under eviction.** R3 only deduplicates
  the hit path; under `ws_mult=2.0` (t=16, 16 cells), throughput drops
  to 2.6-3.6 M ops/s and the bucket mutex is taken on every miss path.

R2 from the profile doc (cache-line-pad the FsCacheStats counters) is
the cheapest next step and is the strongest candidate for the
efficiency gate.

## Final gate scoreboard

| Gate                            | Source                                                   | Result      |
|---------------------------------|----------------------------------------------------------|-------------|
| TPC-H q1-q22 equivalence        | `FsCacheTpchEquivalenceTest`                             | 22/22 PASS  |
| `prefetchHitRate ≥ 0.95`        | `FsCacheBufferedInputTest::prefetchHitRateOnWarmReread`  | PASS        |
| `prefetchMissShare ≥ 0.80`      | `FsCacheBufferedInputTest::prefetchMissShare`            | PASS        |
| Concurrent counter losslessness | `FsCacheStatsTest::concurrentIncrementsAreLossless`      | PASS        |
| All fscache UTs                 | group0 54/54 + group1 68/68                              | PASS        |
| dwio_common ctest               | 100%                                                     | PASS        |
| 36-cell microbench (no crashes) | post R3 `2933ddda7`                                      | PASS 36/36  |
| `single_thread_m_ops ≥ 7.0`     | spec §9.4                                                | **PASS** 8.51 |
| `efficiency_16t ≥ 0.80`         | spec §9.4                                                | **FAIL** 0.186 |

Correctness gates all hold. Single-thread perf gate now passes
post-R1+R3. Scaling gate awaits R2 (or further work on `FsCacheStats`
false-sharing / `QueryLimitToken`).

## Methodology

- Binary: `cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark`
- HEAD: `2933ddda7` (R3, on top of R1 `85f6bbc4d`, on top of Task 12
  `61676e7b3`).
- Command:
  ```
  ./velox_fscache_benchmark --out /tmp/r3-sweep.md \
    --ops=200000 --warmup_ops=20000 --min_wall_seconds=0 \
    --num_files=16
  ```
- Sweep wall time: ~3 seconds for all 36 cells.
- Cell count: 38 lines in output (2 header + 36 data).
- Host: chang's laptop, kernel 6.17.0-29-generic, ext4 on SSD.
- Baseline: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`.
- Hot-path profile: `docs/superpowers/results/2026-05-27-fscache-hot-path-profile.md`.
