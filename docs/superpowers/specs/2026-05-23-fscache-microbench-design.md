# FsCache Phase-1 Microbenchmark — Design

**Status**: Approved 2026-05-23
**Scope**: Self-cost profiling of FsCache::getOrSet under controlled workloads
**Not in scope**: cross-backend comparison vs AsyncDataCache+SsdCache, dwio
end-to-end runs, real remote (S3/HTTP) backends. Those belong to a later
microbench (Phase D-style cross-backend) or end-to-end suite.

## Goal

Answer "where does FsCache::getOrSet hurt, and where is there headroom" by
producing a Markdown table of throughput, hit-rate, write-amplification, and
latency tails across a controlled cell sweep. The same binary will be re-run
after Phase 2 (unified in-flight reservation accounting) to produce an A/B
delta — so the binary must be deterministic in inputs and stable in output
schema across phases.

## Architecture

Single binary `velox_fscache_benchmark`, folly + gflags driver, gated on
`VELOX_ENABLE_BENCHMARKS_BASIC` (same flag as ReadBenchmark and the cachelib
branch's CacheBackendBenchmark).

Single driver class `FsCacheDriver` owns one `FsCache` instance plus one
`SleepyReadFile` (which wraps `LocalReadFile`). A cell run = one driver
lifetime: construct → warmup → main loop → collect → destruct (cacheRoot
removed in destructor).

### Cell sweep dimensions

Cartesian product of four axes:

| Axis | Values | Rationale |
|---|---|---|
| workload | sequential, zipfian (theta=1.0), uniform | Cold scan / skewed / no-locality |
| threads | 1, 4, 16 | Show lock contention scaling |
| ws_mult (working_set / maxBytes) | 0.5, 2.0 | One no-eviction cell, one eviction-pressure cell per other axis combo |
| remote_latency_us | 0, 200 | One hot-local cell, one "remote miss IO" cell — 0 elides the sleep entirely so it degenerates to LocalReadFile |

= 3 × 3 × 2 × 2 = **36 cells**. Each axis is independently overridable via
flags (`--workloads=zipfian --threads_list=4 --ws_mult_list=2.0
--remote_latency_us_list=200` runs a single cell).

### Fixed FsCache configuration

```
alignment        = 1 MiB
maxSegmentSize   = 1 MiB
maxBytes         = 512 MiB
numBuckets       = 1024  (FsCacheConfig default)
```

`workingSetKeys = ws_mult * maxBytes / 1 MiB` ⇒ 256 keys (ws_mult=0.5) or
1024 keys (ws_mult=2.0). One keyIndex maps to exactly one 1 MiB segment, so
`bytesDownloaded` and `bytesEvicted` are interpretable as
`misses * 1 MiB` and `evictions * 1 MiB` respectively.

## Components

### SleepyReadFile

~30 LOC, anonymous namespace inside `FsCacheBenchmark.cpp`. Wraps a
`LocalReadFile`. `pread()` does `sleep_for(latency_us)` then delegates;
`latency_us == 0` skips the sleep call entirely (not `sleep_for(0)`) so the
"hot local" cell pays zero scheduler overhead. Other `ReadFile` virtuals
(`size()`, `getName()`, etc.) forward unchanged.

All cells share one pre-populated 8 GiB random-bytes file at
`/tmp/velox_fscache_bench_remote.bin`. Bench rebuilds this file at startup if
absent or `--rebuild_remote_file` is set. Size is overridable via
`--remote_file_size_gb` (must be >= max keyIndex * 1 MiB across all cells in
the run).

### KeyGenerator

Pattern lifted from cachelib branch's `CacheBackendBenchmark.cpp`:
sequential / zipfian (theta=1.0) / uniform. Per-thread instance, per-thread
seed (`seed_base + tid`).

Returns `keyIndex ∈ [0, workingSetKeys)`. Driver maps to
`offset = keyIndex * 1 MiB`, `size = 1 MiB`.

### ValueSizer

Phase 1 is **fixed 1 MiB only**. Mixed sizes (a la CacheBackendBenchmark's
8/32/128/512 KiB tiers) are deferred — FsCache's segment alignment collapses
small reads onto whole segments, so mixed-mode would not reflect FsCache
behavior, only inflate the explanation surface. Phase 3 (dwio integration)
will revisit.

### Driver main loop

```cpp
CellResult runCell(workload, threads, wsMult, latencyUs) {
  FsCacheDriver d(workingSetKeys, latencyUs);  // fresh cache + cacheRoot
  parallelRun(d, threads, warmupOps / threads, /*recordLatency=*/false);

  auto wallStart = steady_clock::now();
  parallelRun(d, threads, ops / threads, /*recordLatency=*/true);
  auto wallSec = duration<double>(steady_clock::now() - wallStart).count();

  CellResult r;
  r.opsPerSec    = ops / wallSec;
  r.hitRatePct   = 100.0 * d.fsCache.stats().hits / ops;
  r.bytesDl_MB   = sleepyReadFile.bytesRead() / (1ULL << 20);
  r.bytesEvic_MB = d.fsCache.stats().evictions;  // already in MiB-segments
  r.p50_us       = quantile(allLatencies, 0.50) / 1000.0;
  r.p95_us       = quantile(allLatencies, 0.95) / 1000.0;
  r.p99_us       = quantile(allLatencies, 0.99) / 1000.0;
  r.wallSec      = wallSec;
  return r;
}
```

Each worker thread owns a `std::vector<uint64_t>` pre-reserved to
`ops/threads` for per-op nanosecond latencies, avoiding malloc on the hot
path. After all threads join, the main thread concatenates and runs
`std::nth_element` for each quantile.

Memory budget for latency vectors: 1M ops × 8 bytes = 8 MB peak per cell —
negligible.

## Metrics (per cell)

Eight metric columns plus four dimension columns echoed back from the cell
key, for 12 total per row.

| Column | Source | Tells us |
|---|---|---|
| ops/s | ops / wallSec | Aggregate throughput |
| hit% | stats.hits / ops | Whether cache is actually working |
| dl MB | SleepyReadFile cumulative bytes read | Write amplification, cross-check vs hit% |
| evic MB | stats.evictions * 1 MiB | Eviction pressure |
| p50/p95/p99 µs | Per-op latency quantiles | Lock contention, eviction spikes |
| wallSec | Wall clock | Debugging / sanity |

## Output

Markdown table on stdout. Single table with one row per cell, columns:
```
| workload | threads | ws_mult | lat_us | ops/s | hit% | dl_MB | evic_MB | p50_us | p95_us | p99_us | wallSec |
```

`--out=<path>` redirects the table (and only the table) to a file; glog still
goes to stderr. Used for archival to
`docs/superpowers/results/<date>-<topic>.md`.

## File layout

```
velox/common/caching/fscache/
├── CMakeLists.txt                      # MODIFY: add_subdirectory(benchmarks) under VELOX_ENABLE_BENCHMARKS_BASIC
└── benchmarks/
    ├── CMakeLists.txt                  # NEW: add_executable + target_link_libraries
    └── FsCacheBenchmark.cpp            # NEW: ~500 LOC
```

### Build

`velox/common/caching/fscache/CMakeLists.txt` append:
```cmake
if(${VELOX_ENABLE_BENCHMARKS_BASIC})
  add_subdirectory(benchmarks)
endif()
```

`velox/common/caching/fscache/benchmarks/CMakeLists.txt`:
```cmake
add_executable(velox_fscache_benchmark FsCacheBenchmark.cpp)
target_link_libraries(
  velox_fscache_benchmark
  PRIVATE
    velox_fscache
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog
)
```

Default `VELOX_ENABLE_BENCHMARKS_BASIC=OFF` ⇒ no CI/TSAN/Debug impact.
Build with `cmake -DVELOX_ENABLE_BENCHMARKS_BASIC=ON` in the existing
`cmake-build-relwithdebinfo-gcc13` directory.

## Phase 1 baseline run

Run immediately after the bench is committed and tested. Fixed invocation:

```bash
./velox_fscache_benchmark \
  --ops=200000 --warmup_ops=20000 \
  --workloads=sequential,zipfian,uniform \
  --threads_list=1,4,16 \
  --ws_mult_list=0.5,2.0 \
  --remote_latency_us_list=0,200 \
  --out=docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
```

Commit the resulting `.md` file. Phase 2 re-uses the same flags, changes only
the date in `--out` and saves a sibling file; A/B comparison is then a
straightforward two-table diff.

Estimated wall time: 36 cells × ~10s avg = ~6 minutes per full sweep
(latency=200 cells dominate; latency=0 cells finish sub-second).

## Out of scope (YAGNI)

- Mixed value sizes (collapses onto segment alignment ⇒ no signal in Phase 1)
- Real S3 / HTTP remote backends
- p99.9 / p99.99 quantiles (200k ops is not enough samples for reliable estimates)
- perf counters / flamegraphs (PMC needs root; separate task)
- ctest integration (perf bench has no place in ctest)

## Phase 2 hand-off

Bench is the contract for measuring the two CAVEATs documented in
`FsCache::evict` and `FsCache::loadFromDisk`. Phase 2 must:

1. Re-run baseline before any change to verify the bench still works against
   unchanged code.
2. Make Phase 2 changes.
3. Re-run with same flags, save to
   `docs/superpowers/results/<date>-fscache-phase2-with-inflight-reservation.md`.
4. Compare side by side. Expectation: p99 in eviction-pressure cells drops,
   `bytesEvic_MB` smoothed across cells, ops/s unchanged or slightly higher.
