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
`SleepyReadFile` (which wraps `LocalReadFile`). Each cell gets a fresh
driver with cacheRoot at `/tmp/velox_fscache_bench/<pid>/<cell_idx>/`;
the driver's destructor `rm -rf`s it. `main()` installs a SIGINT handler
that recursively removes `/tmp/velox_fscache_bench/<pid>/` so Ctrl-C
during a long sweep does not leave gigabytes behind.

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

~40 LOC, anonymous namespace inside `FsCacheBenchmark.cpp`. Requires
`#include "velox/common/file/File.h"` (precedent: `FileSegment.cpp:20`).
Holds a `LocalReadFile` by value. `pread()` does `sleep_for(latency_us)`
then delegates; `latency_us == 0` skips the sleep call entirely (not
`sleep_for(0)`) so the "hot local" cell pays zero scheduler overhead.
Other `ReadFile` virtuals (`size()`, `getName()`, `shouldCoalesce()`,
`memoryUsage()`, `getNaturalReadSize()`) forward unchanged.

`ReadFile::bytesRead()` is provided by the base class but only increments
when the implementing `pread` writes to `bytesRead_`. SleepyReadFile MUST
either (a) update its own `bytesRead_` in `pread()` (`bytesRead_ +=
length`), or (b) override `bytesRead()` to return `inner_.bytesRead()`.
Pick (a) so SleepyReadFile is the single source of truth. The driver
exposes `bytesRead()` via the cell's `SleepyReadFile&` reference.

Reset between cells via `d.sleepyReadFile.resetBytesRead()` so each cell's
`dl_MB` reflects that cell only.

All cells share one pre-populated random-bytes file at
`/tmp/velox_fscache_bench_remote.bin`. Size defaults to 2 GiB (covers max
1024 keys * 1 MiB = 1 GiB working set with comfortable headroom);
overridable via `--remote_file_size_gb`. Bench rebuilds the file at startup
if absent, size-mismatched, or `--rebuild_remote_file` is set.

### KeyGenerator

Pattern lifted from cachelib branch's `CacheBackendBenchmark.cpp`:
sequential / zipfian (theta=1.0) / uniform. Per-thread instance, per-thread
seed (`seed_base + tid`, where `seed_base` defaults to **42** and is
overridable via `--seed_base`).

**Multi-thread semantics** (explicit, since CacheBackendBenchmark exposes a
`--keyspace_mode` flag for this and we deliberately fix it):

- **sequential**: each thread owns a **disjoint** slice
  `[tid * workingSetKeys / threads, (tid + 1) * workingSetKeys / threads)`
  and walks it in order, wrapping at slice end. Rationale: makes the
  per-cell hit% deterministic (each thread does one full scan over its
  slice during warmup, all subsequent main-loop ops hit), and matches
  CacheBackendBenchmark's `keyspace_mode=partitioned` —
  the option more typical of "many independent table scans".
- **zipfian** and **uniform**: full `[0, workingSetKeys)` universe per
  thread (shared keyspace), matching CacheBackendBenchmark's default
  `keyspace_mode=shared` — the option more typical of "concurrent queries
  on hot data".

`--keyspace_mode` is NOT exposed in Phase 1; the per-workload defaults
above are fixed to keep the cell key small.

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
  // cacheRoot is per-cell: /tmp/velox_fscache_bench/<pid>/<cell_idx>/
  // Driver dtor rm -rf's it. Main also installs a SIGINT handler that walks
  // /tmp/velox_fscache_bench/<pid>/ on Ctrl-C.
  FsCacheDriver d(workingSetKeys, latencyUs, cellIdx);

  // --- Warmup phase: do NOT count in any metric ---
  d.sleepyReadFile.resetBytesRead();
  parallelRun(d, threads, warmupOps / threads, /*recordLatency=*/false);

  // --- Take baselines AFTER warmup ---
  // stats() returns cumulative counters. recordHit/recordMiss bump them
  // during warmup too, so we must subtract baseline below or the reported
  // hit% can exceed 100%.
  const auto statsBase = d.fsCache.stats();
  d.sleepyReadFile.resetBytesRead();

  // --- Main loop ---
  auto wallStart = steady_clock::now();
  parallelRun(d, threads, ops / threads, /*recordLatency=*/true);
  auto wallSec = duration<double>(steady_clock::now() - wallStart).count();

  // --- Deltas (single source of truth: cumulative - baseline) ---
  const auto statsFinal = d.fsCache.stats();
  const uint64_t hitsDelta      = statsFinal.hits      - statsBase.hits;
  const uint64_t missesDelta    = statsFinal.misses    - statsBase.misses;
  const uint64_t evictionsDelta = statsFinal.evictions - statsBase.evictions;
  const uint64_t bytesReadDelta = d.sleepyReadFile.bytesRead();  // reset above

  CellResult r;
  r.opsPerSec    = ops / wallSec;
  // Phase 1 invariant: every getOrSet returns exactly one segment (size =
  // segmentSize = 1 MiB), so lookupOrCreate bumps either hits or misses
  // once per op. hitsDelta + missesDelta == ops absent IO failure. If a
  // future caller passes size > maxSegmentSize, getOrSet emits N segments
  // per op and the identity becomes hits + misses == N * ops; the
  // hit-rate formula still wants ops in the denominator because ops is
  // the user-visible work unit.
  r.hitRatePct   = 100.0 * hitsDelta / ops;
  r.bytesDl_MB   = bytesReadDelta / (1ULL << 20);
  // stats_.evictions is a COUNT of segments evicted, NOT bytes. Phase 1
  // segments are uniformly 1 MiB (FsCacheConfig: alignment = maxSegmentSize
  // = 1 MiB), so count == MiB; if either knob changes this formula must
  // change too. Multiply before divide so a sub-MiB maxSegmentSize does
  // not silently truncate to 0.
  const auto& cfg = d.fsCache.config();   // FsCache.h:72
  r.evicCount    = evictionsDelta;
  r.bytesEvic_MB = (evictionsDelta * cfg.maxSegmentSize) / (1ULL << 20);
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

`ops` MUST be divisible by every value in `--threads_list` (the default
`{1, 4, 16}` divides 200K cleanly). If not divisible the bench aborts at
startup with a clear message — silently dropping ops would corrupt the
A/B comparison.

Memory budget for latency vectors: 1M ops × 8 bytes = 8 MB peak per cell —
negligible.

## Metrics (per cell)

Nine metric columns plus four dimension columns echoed back from the cell
key, for 13 total per row.

| Column | Source | Tells us |
|---|---|---|
| ops/s | ops / wallSec | Aggregate throughput |
| hit% | (hits_final - hits_base) / ops | Whether cache is actually working |
| dl MB | sleepyReadFile.bytesRead() (reset post-warmup) | Write amplification, cross-check vs hit% |
| evic count | evictions_final - evictions_base | Eviction pressure (raw segment count) |
| evic MB | (evic_count * cfg.maxSegmentSize) / 1 MiB; Phase 1 = evic_count * 1 MiB | Eviction pressure (bytes); cross-check vs dl_MB |
| p50/p95/p99 µs | Per-op latency quantiles | Lock contention, eviction spikes |
| wallSec | Wall clock | Debugging / sanity |

## Output

Markdown table on stdout. Single table with one row per cell, columns:
```
| workload | threads | ws_mult | lat_us | ops/s | hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
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
