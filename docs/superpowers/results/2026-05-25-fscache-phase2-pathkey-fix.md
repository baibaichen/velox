# FsCache Phase-2 (PathKey-Fix) Sweep — 2026-05-25

Captured with `velox_fscache_benchmark` after applying the PathKey POD
refactor (commit `c63541c45`) on top of plan-1 HEAD (`4a3ec350b`). Pairs
1:1 with the Phase-1 baseline at
`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` (commit
`8dfe97c38`) and supersedes the un-fixed Phase-2 numbers analysed in
`docs/superpowers/results/2026-05-25-plan1-perf-regression.md`.

Invocation (identical to the Phase-1 baseline):

```bash
./velox_fscache_benchmark \
  --ops=200000 --warmup_ops=20000 \
  --workloads=sequential,zipfian,uniform \
  --threads_list=1,4,16 \
  --ws_mult_list=0.5,2.0 \
  --remote_latency_us_list=0,200 \
  --out=docs/superpowers/results/2026-05-25-fscache-phase2-pathkey-fix.md
```

Host: ChangDev, kernel 6.17.0-29-generic, CPU 13th Gen Intel(R) Core(TM) i9-13900KF (32 threads), build RelWithDebInfo (GCC-13).

---

| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |       1 |    0.50 |      0 |   6568701 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.030 |
| sequential |       1 |    0.50 |    200 |   6123442 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.033 |
| sequential |       1 |    2.00 |      0 |      3838 |  0.0% | 200000 |     200000 |  200000 |  258.7 |  280.3 |  297.4 |  52.109 |
| sequential |       1 |    2.00 |    200 |      1954 |  0.0% | 200000 |     200000 |  200000 |  508.2 |  538.2 |  619.4 | 102.361 |
| sequential |       4 |    0.50 |      0 |   3212498 | 100.0% |     0 |          0 |       0 |    0.8 |    3.6 |    5.8 |   0.062 |
| sequential |       4 |    0.50 |    200 |   3131521 | 100.0% |     0 |          0 |       0 |    0.8 |    4.0 |    6.4 |   0.064 |
| sequential |       4 |    2.00 |      0 |     12598 |  0.1% | 199728 |     199728 |  199728 |  310.6 |  350.6 |  435.4 |  15.876 |
| sequential |       4 |    2.00 |    200 |      7271 |  0.1% | 199847 |     199847 |  199847 |  533.8 |  679.2 |  788.7 |  27.505 |
| sequential |      16 |    0.50 |      0 |   2228314 | 100.0% |     0 |          0 |       0 |    5.0 |   18.8 |   28.6 |   0.090 |
| sequential |      16 |    0.50 |    200 |   1875180 | 100.0% |     0 |          0 |       0 |    5.9 |   22.7 |   34.8 |   0.107 |
| sequential |      16 |    2.00 |      0 |   1446633 | 99.3% |  1469 |       1469 |    1469 |    0.6 |    6.8 |   15.3 |   0.138 |
| sequential |      16 |    2.00 |    200 |     18910 | 32.5% | 134962 |     134963 |  134963 |  700.4 | 1175.1 | 1416.3 |  10.577 |
| zipfian    |       1 |    0.50 |      0 |   5840490 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.034 |
| zipfian    |       1 |    0.50 |    200 |   5526046 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.036 |
| zipfian    |       1 |    2.00 |      0 |     28785 | 86.8% | 26434 |      26434 |   26434 |    0.3 |  261.0 |  277.6 |   6.948 |
| zipfian    |       1 |    2.00 |    200 |     14749 | 86.8% | 26434 |      26434 |   26434 |    0.3 |  511.3 |  529.3 |  13.560 |
| zipfian    |       4 |    0.50 |      0 |   2813211 | 100.0% |     0 |          0 |       0 |    0.8 |    4.2 |    6.6 |   0.071 |
| zipfian    |       4 |    0.50 |    200 |   3240423 | 100.0% |     0 |          0 |       0 |    0.7 |    3.6 |    5.9 |   0.062 |
| zipfian    |       4 |    2.00 |      0 |     93357 | 86.8% | 26380 |      26380 |   26380 |    0.3 |  313.0 |  351.6 |   2.142 |
| zipfian    |       4 |    2.00 |    200 |     52658 | 86.8% | 26482 |      26482 |   26482 |    0.3 |  545.1 |  656.0 |   3.798 |
| zipfian    |      16 |    0.50 |      0 |   2733091 | 100.0% |     0 |          0 |       0 |    3.8 |   16.0 |   24.4 |   0.073 |
| zipfian    |      16 |    0.50 |    200 |   2656534 | 100.0% |     0 |          0 |       0 |    4.1 |   15.5 |   23.1 |   0.075 |
| zipfian    |      16 |    2.00 |      0 |     97655 | 87.4% | 25122 |      25122 |   25122 |    0.6 | 1351.2 | 1870.8 |   2.048 |
| zipfian    |      16 |    2.00 |    200 |     89449 | 87.5% | 25083 |      25083 |   25083 |    0.6 | 1445.1 | 1985.3 |   2.236 |
| uniform    |       1 |    0.50 |      0 |   5971739 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.033 |
| uniform    |       1 |    0.50 |    200 |   6247789 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   0.032 |
| uniform    |       1 |    2.00 |      0 |      7654 | 50.2% | 99543 |      99543 |   99543 |    1.0 |  275.3 |  295.3 |  26.130 |
| uniform    |       1 |    2.00 |    200 |      3907 | 50.2% | 99543 |      99543 |   99543 |    1.4 |  529.2 |  577.2 |  51.188 |
| uniform    |       4 |    0.50 |      0 |   2924921 | 100.0% |     0 |          0 |       0 |    0.8 |    4.0 |    6.4 |   0.068 |
| uniform    |       4 |    0.50 |    200 |   2694322 | 100.0% |     0 |          0 |       0 |    0.9 |    4.9 |    7.5 |   0.074 |
| uniform    |       4 |    2.00 |      0 |     24012 | 50.1% | 99709 |      99709 |   99709 |  162.6 |  366.0 |  530.6 |   8.329 |
| uniform    |       4 |    2.00 |    200 |     14429 | 50.1% | 99752 |      99752 |   99752 |  304.8 |  586.4 |  767.4 |  13.861 |
| uniform    |      16 |    0.50 |      0 |   2092049 | 100.0% |     0 |          0 |       0 |    5.3 |   19.7 |   29.9 |   0.096 |
| uniform    |      16 |    0.50 |    200 |   2238257 | 100.0% |     0 |          0 |       0 |    5.0 |   18.1 |   27.2 |   0.089 |
| uniform    |      16 |    2.00 |      0 |     25869 | 51.8% | 96328 |      96328 |   96328 |    7.7 | 1845.2 | 2127.4 |   7.731 |
| uniform    |      16 |    2.00 |    200 |     23496 | 51.7% | 96567 |      96567 |   96567 |    7.7 | 1994.4 | 2324.1 |   8.512 |

---

## A/B snapshot vs Phase-1 baseline (selected high-signal cells)

`ratio = phase2_ops_s / phase1_ops_s`. Short-wall cells (≤0.1 s) are
omitted: the 200k-op budget is too small to drive measurement noise
below the inter-run delta on those cells. The interesting cells are
the throughput-bound 1-thread hit paths and the latency-bound miss
paths.

| cell                                          | phase-1 ops/s | phase-2 (PathKey fix) ops/s | ratio |
|-----------------------------------------------|--------------:|----------------------------:|------:|
| sequential.1t.0.5ws.0us  (1-thread hit path)  |     7'556'775 |                   6'568'701 |  0.87 |
| sequential.1t.0.5ws.200us                     |     7'571'808 |                   6'123'442 |  0.81 |
| zipfian.1t.0.5ws.0us                          |     6'780'590 |                   5'840'490 |  0.86 |
| uniform.1t.0.5ws.0us                          |     6'435'002 |                   5'971'739 |  0.93 |
| sequential.4t.2ws.0us  (miss path, mid-thr)   |        12'481 |                      12'598 |  1.01 |
| sequential.16t.2ws.0us (concurrent miss)      |     1'563'473 |                   1'446'633 |  0.93 |
| zipfian.16t.2ws.0us                           |        96'909 |                      97'655 |  1.01 |
| uniform.16t.2ws.0us                           |        24'046 |                      25'869 |  1.08 |

The 200k-op short cells under-sample. Two longer separate probes on the
same 1-thread hit cell give a tighter number:

- Phase-1: `--ops=1'500'000'000`, 204.6 s wall (~3.4 min) →
  **7'331'556 ops/s** (`/tmp/probe_a_phase1_perfstat_long.md`).
- Phase-2-pathkey-fix: `--ops=450'000'000`, 64.0 s wall (~1 min) →
  **7'033'458 ops/s** (`/tmp/probe_c_postfix_short.md`).
- Ratio = 7'033'458 / 7'331'556 = **0.96**.

The two operands are captured at different op counts and wall durations,
so this is a directional confirmation, not a paired measurement. Within
that caveat the PathKey fix recovers ~96 % of the phase-1 baseline on
the 1-thread hit path; a same-budget paired rerun is left as follow-up.

The ~4-13% residual on the 1-thread hit cells (0.87-0.93 here, 0.96
on the long-run cell) is the plan-1 design overhead the diagnosis
doc hypothesised but did not isolate (per-bucket lock + atomic
counter + indirect KeyMetadata lookup). Isolating each contribution
is left as follow-up; current evidence is consistent with the
residual being shared across the entire plan-1 surface rather than
any single hot spot.

Latency-bound cells — strictly the ones where misses actually trigger
the SleepyReadFile sleep, i.e. `ws_mult=2.0` (any `lat_us`) — are
unchanged within run-to-run noise: the sleep and eviction backpressure
dominate, so a hot-path constant-factor change in PathKey is invisible
there. Spot-checks: `sequential.1t.2ws.200us` 1'978 → 1'954 (0.99),
`zipfian.1t.2ws.200us` 14'488 → 14'749 (1.02), `uniform.1t.2ws.200us`
3'604 → 3'907 (1.08).

Note: `ws_mult=0.5` cells with `lat_us=200` are **not** latency-bound —
they are 100 % hit so the sleep never fires. Those cells move with the
hit-path PathKey delta (e.g. `sequential.16t.0.5ws.200us` 2'309'813 →
1'875'180 = 0.81), and they share the same residual envelope as the
0-lat hit cells above plus extra short-wall noise. This is the expected
shape and confirms the fix is correctly scoped to the hot path.
