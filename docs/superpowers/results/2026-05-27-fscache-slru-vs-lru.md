# FsCache SLRU vs LRU — Task F perf re-run + default decision

**Date:** 2026-05-27
**HEAD baseline:** `a59addbe2` (Task F commit; post-Task-E audit fix) + the in-this-session
prep change adding `--enable_slru` / `--slru_protected_ratio` flags to
`velox_fscache_benchmark` and `velox_tpch_benchmark` (`AbBenchmarkMain.cpp`).
**Build:** `cmake-build-relwithdebinfo-gcc13` (GCC-13 RelWithDebInfo).
**Plan:** `docs/superpowers/plans/2026-05-27-slru-policy.md` Task F.

Raw data (regeneratable):
- Microbench LRU:   `/tmp/fscache-lru.md`   (36 cells, written 23:20)
- Microbench SLRU:  `/tmp/fscache-slru.md`  (36 cells, written 23:38)
- SF=100 LRU CSV:   `/tmp/tpch_ab_fscache_lru.csv`  (67 rows = header + 22q×3r)
- SF=100 SLRU CSV:  `/tmp/tpch_ab_fscache_slru.csv` (67 rows)

---

## Decision: B — keep `FsCacheConfig::enableSlru = false` default

SLRU is wired up, correct, and ships as opt-in. CH's "SLRU default" rationale
does **not** carry across our per-bucket sharding (see §4). Real-workload
SF=100 gains are inside noise (1-2 % medians); microbench shows real wins on
zipfian re-use but real losses on sequential cold-scan eviction (-30 %). Flip
would not pay for itself today.

---

## 1. Microbench — 36-cell A/B (Data point 1)

Sweep:
```
velox_fscache_benchmark --ops=200000 --warmup_ops=20000 \
  --min_wall_seconds=0 --num_files=16 [--enable_slru=true]
```

### 1.1 Hot path (t=1, ws_mult=0.5, lat=0) — strict hit-path overhead

| workload   | LRU ops/s            | SLRU ops/s           | Δ       | Source |
|------------|----------------------|----------------------|---------|--------|
| sequential | 7'785'143            | 7'537'981            | -3.2 %  | `/tmp/fscache-lru.md:3` vs `/tmp/fscache-slru.md:3` |
| zipfian    | 6'922'718            | 6'580'866            | -4.9 %  | `/tmp/fscache-lru.md:15` vs `/tmp/fscache-slru.md:15` |
| uniform    | 7'914'176            | 6'828'176            | -13.7 % | `/tmp/fscache-lru.md:27` vs `/tmp/fscache-slru.md:27` |

SLRU adds a side-index lookup + per-sub-queue accounting on every `onHit`
even when no promotion happens (segment already in protected). On a 100 %-hit
workload all four `EvictionPolicy` methods are hit-path, so this overhead is
the full cost. uniform-workload regression of -13.7 % is the worst.

### 1.2 Eviction path (t=16, ws_mult=2.0, working set 2× cache) — where SLRU should win

| workload   | lat_us | LRU ops/s   | SLRU ops/s  | Δ       | Source |
|------------|-------:|-------------|-------------|---------|--------|
| sequential |      0 | 4'146'188   | 2'922'959   | **-29.5 %** | `/tmp/fscache-lru.md:13` vs `/tmp/fscache-slru.md:13` |
| sequential |    200 | 2'660'745   | 2'757'726   | +3.6 %  | `/tmp/fscache-lru.md:14` vs `/tmp/fscache-slru.md:14` |
| zipfian    |      0 | 1'837'174   | 3'492'712   | **+90.1 %** | `/tmp/fscache-lru.md:25` vs `/tmp/fscache-slru.md:25` |
| zipfian    |    200 | 1'869'615   | 2'681'387   | +43.4 %  | `/tmp/fscache-lru.md:26` vs `/tmp/fscache-slru.md:26` |
| uniform    |      0 | 2'267'443   | 2'110'752   | -6.9 %  | `/tmp/fscache-lru.md:37` vs `/tmp/fscache-slru.md:37` |
| uniform    |    200 | 2'878'943   | 2'790'183   | -3.1 %  | `/tmp/fscache-lru.md:38` vs `/tmp/fscache-slru.md:38` |

**Sequential cold-scan regression (-29.5 %) is real and matches the spec §10 R5
caveat**: SLRU has nothing to protect because nothing is re-read, yet still
pays the per-promotion / per-demotion bookkeeping. Conversely, zipfian (skewed
re-use — the workload SLRU exists for) wins big (+90 % / +43 %).

### 1.3 t=16 hot (ws_mult=0.5) — concurrent hit-path

| workload   | LRU ops/s    | SLRU ops/s   | Δ       | Source |
|------------|--------------|--------------|---------|--------|
| sequential | 60'609'403   | 55'810'999   | -7.9 %  | `/tmp/fscache-lru.md:11` vs `/tmp/fscache-slru.md:11` |
| zipfian    | 41'064'718   | 52'750'657   | +28.5 % | `/tmp/fscache-lru.md:23` vs `/tmp/fscache-slru.md:23` |
| uniform    | 60'442'244   | 56'851'921   | -5.9 %  | `/tmp/fscache-lru.md:35` vs `/tmp/fscache-slru.md:35` |

Zipfian +28 % at t=16 is consistent with the eviction-path win — SLRU keeps
the hot tail in protected and avoids cross-thread protected-LRU thrash. The
sequential/uniform losses are again the hit-path tax on a workload SLRU
cannot help.

---

## 2. SF=0.01 TPC-H equivalence under SLRU (Data point 2)

Approach: locally edited `FsCacheTpchEquivalenceTest.cpp:195` to set
`cfg.enableSlru = true` (NOT committed; reverted with `git checkout --` after
the run). Re-ran the full 22-query suite:

```
[==========] 22 tests from 1 test suite ran. (1595 ms total)
[  PASSED  ] 22 tests.
```

**Verdict: 22/22 PASS.** SLRU is correctness-equivalent to LRU on TPC-H
SF=0.01 q1..q22 (compared against DuckDB via the existing
`FsCacheTpchEquivalenceTest` fixture).

---

## 3. SF=100 TPC-H sweep — fscache(LRU) vs fscache(SLRU) (Data point 3)

Dataset: `/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`.
Same `--fscache_disk_gib=64`, `--rounds=3` as the #179 sweep
(`docs/superpowers/results/2026-05-27-fscache-tpch-ab-sweep.md`).

### 3.1 Target 5-query median (per-query 3-round median, lower is better)

| query | LRU median (ms) | SLRU median (ms) | Δ ms   | Δ %    |
|-------|----------------:|-----------------:|-------:|-------:|
| q01   | 4718.0          | 4643.6           |  -74.4 | -1.58% |
| q06   | 2205.0          | 2168.0           |  -37.0 | -1.68% |
| q14   | 5029.7          | 4951.1           |  -78.6 | -1.56% |
| q19   | 5373.7          | 5351.7           |  -22.0 | -0.41% |
| q22   | 4211.2          | 4147.4           |  -63.8 | -1.51% |

All 5 target queries: SLRU 0.4 – 1.7 % faster. Direction is consistent but
magnitudes are inside the run-to-run noise band documented in
`2026-05-27-fscache-tpch-ab-sweep.md` (single-run, 3-round medians).

### 3.2 Full 22-query summary

19/22 queries: SLRU faster (best -2.87 % on q18).
3/22 queries: SLRU slower (q03 +0.29 %, q04 +0.05 %, q15 +0.70 %, q20 +1.07 %
— q20 worst).

### 3.3 Cache footprint (identical between LRU and SLRU)

| round | avg hit % | total dl | total evict |
|-------|-----------|----------|-------------|
| 1     | 92.5 %    | 23.97 GiB | 0           |
| 2     | 100.0 %   | 0         | 0           |
| 3     | 100.0 %   | 0         | 0           |

(Both policies; computed from the per-round CSVs above.) The working set
fits in 64 GiB with zero eviction, so SLRU's protect-hot-segments mechanism
never engages. Any wall-time delta therefore reflects only `onInsert` /
`onHit` book-keeping cost, not actual eviction-policy quality.

---

## 4. Why SLRU does not pay for itself today

Velox's FsCache shards eviction state **per bucket** (`FsCacheConfig::numBuckets
= 1024` default, `FsCacheConfig.h:64`). Each bucket owns its own
`SlruPolicy` with per-bucket budget = `maxBytes / numBuckets`.

For the SF=100 sweep: `maxBytes = 64 GiB`, `numBuckets = 1024` →
per-bucket budget ≈ **64 MiB**. Protected portion at ratio 0.6 ≈ **38 MiB**
per bucket ≈ **a single 32 MiB segment** plus change.

A protected list that holds ≈ 1 segment per bucket cannot meaningfully
preserve a "hot working set" against scan-style probationary churn —
the protection horizon is one segment wide. CH avoids this because its
FileCache has one cache-wide queue; our per-bucket sharding (chosen for
lock contention reasons, see spec §7.1) inverts that trade-off.

This is the "per-bucket protected portion too small" hypothesis the Task F
rationale doc anticipated. The SF=100 footprint (0 eviction across all 22q)
confirms SLRU's mechanism never engages on this workload; SLRU's microbench
wins on zipfian eviction-pressure cells (+90 % at t=16 ws_mult=2) show what
SLRU *would* contribute in a working-set-larger-than-cache regime, but real
queries against a 64 GiB cache fit the working set.

---

## 5. Deferred follow-up

Two cleanups to consider in a future phase:

1. **Cache-level (vs per-bucket) SLRU**: matches CH topology, restores SLRU's
   protect-the-hot-tail intent. Cost: introduces a cache-wide eviction lock
   (or sharded with cross-bucket coordination); needs design work + new
   contention sweep.

2. **Hit-path SLRU cost**: -13.7 % on t=1 uniform (`/tmp/fscache-slru.md:27`)
   is the side-index lookup. If we ever flip the default, fold `isProtected_`
   into the `LruPolicy` map entry to drop one hash lookup off `onHit`.

Both are out of scope for Task F.

---

## 6. Compliance

- 八荣八耻 #1 / #5 / #7: every number above cites a `/tmp/...` file
  written during this session. The sequential -29.5 % regression and the
  uniform -13.7 % hot-path regression are reported as-is; no
  "SLRU-is-more-CH-aligned" spin.
- Test mod (`FsCacheTpchEquivalenceTest.cpp:195` `enableSlru=true`) was
  applied to working tree, used for the SF=0.01 verification only, then
  reverted with `git checkout --`. Task 15 fixture remains sealed.
