# FsCache TPC-H A/B Sweep — Implementation Plan

**Goal:** Reuse the existing TPC-DS A/B sweep harness for TPC-H by refactoring
the generic round-loop / CSV-writer / FsCache-vs-CBI plumbing out of
`TpcdsBenchmark` into a shared base, then plug `TpchBenchmark` and its `main`
into the same harness. Phase-2 substitute for the TPC-DS Task 6 sweep, which
cannot run because TPC-DS data is not on this host. User-provided TPC-H data:
`/home/chang/test/tpch/tpch-generated-100.0-parquet` (SF-100, parquet, 8 tables,
lineitem split into ~101 part files — natural per-file PathKey distribution).

**Architecture:** Single inheritance — `AbBenchmarkBase` derives from
`QueryBenchmarkBase` and owns the round loop, CSV writer, FsCache/CBI snapshot
helpers, and the A/B-related flag declarations. `TpcdsBenchmark` and
`TpchBenchmark` derive from `AbBenchmarkBase`, override two virtuals
(`numQueries()`, `buildPlan(int)`), and keep their TPC-suite-specific
construction (query builder, plan-JSON dir vs in-memory build, Presto-connector
ID for TPC-DS). The two `main`s share an `installFsCache()` helper extracted to
a small library header used by both.

**Tech Stack:** C++20, gflags, gtest (none new); folly Benchmark
(`folly::runBenchmarks()` only used on the legacy non-A/B path, untouched);
GCC-13 RelWithDebInfo build at `cmake-build-relwithdebinfo-gcc13/`. Phase 5
sweep runs out-of-process from a small shell wrapper (no new Python).

**Sweep policy:** 5-query subset × 3 rounds × {cbi, fscache}. Candidate queries
chosen for breadth: **q1** (lineitem scan + agg, biggest table), **q6**
(lineitem filter + agg, smallest plan), **q14** (lineitem ⨝ part), **q19**
(lineitem ⨝ part with multi-predicate), **q22** (customer + orders correlated).
Three rounds keeps cold-vs-warm signal (round 1 cold miss, rounds 2-3 hit
steady-state). **Code commits per Tasks 1-3; sweep results NOT committed**
(per user choice).

---

## File Structure

**New files:**
- `velox/benchmarks/AbBenchmarkBase.h` — base class declaration + A/B flag
  DECLARE_*; ~80 LOC
- `velox/benchmarks/AbBenchmarkBase.cpp` — base class definitions
  (`AbCsvRow`, `runAb()`, snapshot helpers, CSV writer, A/B flag DEFINE_*);
  ~250 LOC (moved verbatim from TpcdsBenchmark.cpp where possible)
- `velox/benchmarks/AbBenchmarkMain.h` — shared `installFsCache()` and
  `dispatchAbMain()` helper; ~30 LOC
- `velox/benchmarks/AbBenchmarkMain.cpp` — definitions; ~50 LOC

**Modified files:**
- `velox/benchmarks/CMakeLists.txt` — add `velox_benchmark_ab` static lib
  (sources: `AbBenchmarkBase.cpp`, `AbBenchmarkMain.cpp`)
- `velox/benchmarks/tpcds/CMakeLists.txt` — link `velox_benchmark_ab`
- `velox/benchmarks/tpcds/TpcdsBenchmark.h` — change base from
  `QueryBenchmarkBase` to `AbBenchmarkBase`; drop `runAb()` decl + doc
  (now inherited); keep `listSplits` override, `initQueryBuilder` virtual,
  `queryConfigs_`, `planDir_`, `pool_`; add `numQueries()` and
  `buildPlan(int32_t)` overrides
- `velox/benchmarks/tpcds/TpcdsBenchmark.cpp` — delete the moved-out code
  (AbCsvRow, snapshotBackend, populateBackendDelta, writeCsvHeader,
  writeCsvRow, quantileUs, runAb body, the DEFINE_* for `input_source`,
  `rounds`, `fscache_disk_gib`, `fscache_root`, `out`, the `DECLARE_int32(
  num_repeats)`); replace with `numQueries()` returning 99 and `buildPlan(q)`
  returning `queryBuilder_->getQueryPlan(q, planDir_, pool_.get())`
- `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp` — delete `installFsCache`,
  replace `main()` body with `dispatchAbMain(...)` call; keep TPC-DS-specific
  usage string
- `velox/benchmarks/tpch/CMakeLists.txt` — link `velox_benchmark_ab`
- `velox/benchmarks/tpch/TpchBenchmark.h` — change base to `AbBenchmarkBase`;
  add `numQueries()` and `buildPlan(int32_t)` overrides; keep `queryBuilder_`,
  `queryConfigs_`, `initQueryBuilder`, existing `runQuery` inline
- `velox/benchmarks/tpch/TpchBenchmark.cpp` — no body changes (already inherits
  whatever changes via header); add `numQueries()` and `buildPlan` implementations
- `velox/benchmarks/tpch/TpchBenchmarkMain.cpp` — gate on `--input_source`:
  empty → existing `tpchBenchmarkMain()`; otherwise → `dispatchAbMain(...)`

**Excluded files (must not be committed, per global rule):**
- `velox/common/caching/benchmarks/CacheBackendBenchmark.cpp`

---

## Task 1: Extract AbBenchmarkBase

**Files:**
- Create: `velox/benchmarks/AbBenchmarkBase.h`
- Create: `velox/benchmarks/AbBenchmarkBase.cpp`
- Modify: `velox/benchmarks/CMakeLists.txt`
- Modify: `velox/benchmarks/tpcds/CMakeLists.txt`
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmark.h`
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmark.cpp`
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp` (link only; substantive change in Task 2)

### Step 1.1: Create `AbBenchmarkBase.h`

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/benchmarks/QueryBenchmarkBase.h"

DECLARE_string(input_source);
DECLARE_int32(rounds);
DECLARE_int32(fscache_disk_gib);
DECLARE_string(fscache_root);
DECLARE_string(out);

namespace facebook::velox::benchmarks {

/// Base class for benchmarks that want the FsCache-vs-CBI A/B sweep harness:
/// snapshot backend stats, run one query, snapshot again, emit one CSV row,
/// repeat for FLAGS_rounds x numQueries() iterations. Derived classes plug in
/// the suite-specific query count and per-query plan construction; everything
/// else is shared.
class AbBenchmarkBase : public facebook::velox::QueryBenchmarkBase {
 public:
  /// Drives the A/B sweep: FLAGS_rounds outer iterations x numQueries()
  /// queries, plan construction hoisted via buildPlan(). Wall_ms is measured
  /// by std::chrono::steady_clock around QueryBenchmarkBase::run(). Writes
  /// one CSV row per (round, query) to FLAGS_out. Returns the number of
  /// failed queries so the caller can set a non-zero exit code without
  /// re-reading the CSV.
  int32_t runAb();

 protected:
  /// Total number of queries in this suite (TPC-DS: 99, TPC-H: 22).
  virtual int32_t numQueries() const = 0;

  /// Builds the plan for queryId in [1, numQueries()]. Called once per query
  /// before the round loop, then the result is reused across rounds.
  virtual facebook::velox::exec::test::VeloxPlan buildPlan(
      int32_t queryId) = 0;

  std::unordered_map<std::string, std::string> queryConfigs_;
};

} // namespace facebook::velox::benchmarks
```

### Step 1.2: Create `AbBenchmarkBase.cpp`

Move these verbatim from `TpcdsBenchmark.cpp` (lines ~76-322 inclusive):
- `DEFINE_string(input_source, ...)`
- `DEFINE_int32(rounds, ...)`
- `DEFINE_int32(fscache_disk_gib, ...)`
- `DEFINE_string(fscache_root, ...)`
- `DEFINE_string(out, ...)`
- `DECLARE_int32(num_repeats)`
- Anonymous-namespace block: `struct AbCsvRow`, `writeCsvHeader`,
  `writeCsvRow`, `quantileUs`, `struct BackendSnapshot`, `snapshotBackend`,
  `populateBackendDelta`

Then move `runAb()` body, with two changes:
- Replace `constexpr int32_t kNumQueries = 99;` with `const int32_t kNumQueries = numQueries();`
- Replace `plans.push_back(queryBuilder_->getQueryPlan(q, planDir_, pool_.get()));` with `plans.push_back(buildPlan(q));`

Wrap in `namespace facebook::velox::benchmarks { ... }`.

The fmt for `query_id` in `writeCsvRow` stays `q{:02d}` — works for both 22
and 99.

### Step 1.3: Update `velox/benchmarks/CMakeLists.txt`

Add a static library target. Inspect the existing file first; expected
addition (subject to fitting the existing pattern):

```cmake
add_library(velox_benchmark_ab STATIC
  AbBenchmarkBase.cpp
)
target_link_libraries(velox_benchmark_ab
  velox_benchmark_query_base
  velox_caching
  velox_fscache
)
```

(Exact dep names will follow whatever `QueryBenchmarkBase` already links —
read the file first.)

### Step 1.4: Modify `TpcdsBenchmark.h`

Change `#include "velox/benchmarks/QueryBenchmarkBase.h"` to
`#include "velox/benchmarks/AbBenchmarkBase.h"`. Change `: public
facebook::velox::QueryBenchmarkBase` to `: public
facebook::velox::benchmarks::AbBenchmarkBase`. Delete the `runAb()`
declaration and its doc comment. Delete `queryConfigs_` (now inherited).
Add to `protected:`:

```cpp
  int32_t numQueries() const override {
    return 99;
  }
  facebook::velox::exec::test::VeloxPlan buildPlan(int32_t queryId) override {
    return queryBuilder_->getQueryPlan(queryId, planDir_, pool_.get());
  }
```

### Step 1.5: Modify `TpcdsBenchmark.cpp`

Delete:
- `DEFINE_string(input_source, ...)`
- `DEFINE_int32(rounds, ...)`
- `DEFINE_int32(fscache_disk_gib, ...)`
- `DEFINE_string(fscache_root, ...)`
- `DEFINE_string(out, ...)`
- `DECLARE_int32(num_repeats)`
- The `AbCsvRow` / `writeCsvHeader` / `writeCsvRow` / `quantileUs` /
  `BackendSnapshot` / `snapshotBackend` / `populateBackendDelta` block
- `int32_t TpcdsBenchmark::runAb() { ... }` (entire definition)

Delete the now-unused `#include` for `AsyncDataCache.h` and `FsCache.h` if
nothing else references them in this file (`grep` to verify).

### Step 1.6: Update `velox/benchmarks/tpcds/CMakeLists.txt`

Add `velox_benchmark_ab` to the link list of the `velox_tpcds_benchmark_lib`
target (or whatever the existing target name is — `grep` first).

### Step 1.7: Build + verify

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_tpcds_benchmark -j 8
```

Expected: clean build. If link errors mention duplicate symbols for the
A/B flags, that's the gflags one-definition rule — make sure the DEFINE_*
lives only in `AbBenchmarkBase.cpp` and the .h only DECLARE_*s them.

### Step 1.8: Smoke-test TPC-DS (legacy path still works)

The legacy non-A/B path must keep working — we have not changed any of its
behavior:

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark \
  --help 2>&1 | grep -E "input_source|rounds|fscache_disk_gib" | head -5
```

Expected: `input_source`, `rounds`, `fscache_disk_gib`, `fscache_root`, `out`
all still present in `--help`.

### Step 1.9: Commit

```bash
git add velox/benchmarks/AbBenchmarkBase.h \
        velox/benchmarks/AbBenchmarkBase.cpp \
        velox/benchmarks/CMakeLists.txt \
        velox/benchmarks/tpcds/CMakeLists.txt \
        velox/benchmarks/tpcds/TpcdsBenchmark.h \
        velox/benchmarks/tpcds/TpcdsBenchmark.cpp
```

Commit message:

```
refactor(benchmarks): extract AbBenchmarkBase from TpcdsBenchmark

Pull the FsCache-vs-CBI A/B sweep harness (runAb + AbCsvRow + CSV writer +
backend snapshot helpers + --input_source/--rounds/--fscache_*/--out flag
defines) out of TpcdsBenchmark into a new AbBenchmarkBase so TpchBenchmark
can plug into the same harness. Derived classes provide numQueries() and
buildPlan(queryId); everything else is shared.

No behavior change: TPC-DS legacy and A/B paths both still work; the
flags, CSV format, and per-query semantics are unchanged.
```

---

## Task 2: Share installFsCache + dispatchAbMain

**Files:**
- Create: `velox/benchmarks/AbBenchmarkMain.h`
- Create: `velox/benchmarks/AbBenchmarkMain.cpp`
- Modify: `velox/benchmarks/CMakeLists.txt`
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp`

### Step 2.1: Create `AbBenchmarkMain.h`

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * (license header — same as Tpcds)
 */
#pragma once

#include <functional>
#include <memory>

namespace facebook::velox {
namespace cache::fs {
class FsCache;
}
namespace benchmarks {
class AbBenchmarkBase;
} // namespace benchmarks
} // namespace facebook::velox

namespace facebook::velox::benchmarks {

/// Wipes FLAGS_fscache_root, creates a fresh FsCache sized to
/// FLAGS_fscache_disk_gib, and installs it as the process-wide singleton.
/// Returns owning handle; caller drops it after the sweep to tear down.
std::unique_ptr<facebook::velox::cache::fs::FsCache> installFsCache();

/// Common --input_source dispatch for TPC-DS and TPC-H main():
///   empty           -> runLegacy() (the suite's existing folly::runBenchmarks path)
///   "fscache"       -> install FsCache, force --cache_gb=0, call ab.runAb()
///   "cbi"           -> require --cache_gb>0, call ab.runAb()
/// Returns the process exit code (0 unless >10 query failures).
int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy);

} // namespace facebook::velox::benchmarks
```

### Step 2.2: Create `AbBenchmarkMain.cpp`

Move `installFsCache()` verbatim from `TpcdsBenchmarkMain.cpp` (lines 36-57).
Add `dispatchAbMain` body — straight port of `TpcdsBenchmarkMain.cpp` lines
71-100 except:
- Take `AbBenchmarkBase& ab` and `runLegacy` as args.
- Replace `tpcdsBenchmarkMain()` with `runLegacy()`.
- Replace `tpcdsBenchmark->initialize() / runAb() / shutdown()` with
  `ab.initialize() / ab.runAb() / ab.shutdown()`.

Include `DECLARE_string(input_source);`, `DECLARE_int32(fscache_disk_gib);`,
`DECLARE_string(fscache_root);`, `DECLARE_int32(cache_gb);`.

### Step 2.3: Update `velox/benchmarks/CMakeLists.txt`

Add `AbBenchmarkMain.cpp` to `velox_benchmark_ab` sources.

### Step 2.4: Modify `TpcdsBenchmarkMain.cpp`

Delete `installFsCache` and its includes (`<filesystem>`, `<system_error>`,
`FsCacheConfig.h`, the `installFsCache` definition).

Replace `main()` body with:

```cpp
int main(int argc, char** argv) {
  std::string kUsage(
      "TPC-DS benchmark. With --input_source={cbi,fscache} runs the "
      "FsCache-vs-CBI A/B sweep (spec 2026-05-23-fscache-vs-cbi-tpcds). "
      "Without it, runs the legacy folly::runBenchmarks() flow.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init{&argc, &argv, false};

  tpcdsBenchmark = std::make_unique<TpcdsBenchmark>();
  return facebook::velox::benchmarks::dispatchAbMain(
      *tpcdsBenchmark, tpcdsBenchmarkMain);
}
```

### Step 2.5: Build + verify legacy + A/B path

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_tpcds_benchmark -j 8
```

Verify the resulting binary still recognizes `--input_source` and the legacy
path still runs `--run_query_verbose=N`.

If TPC-DS test data is reachable, smoke a 1-round 1-query cbi run to confirm
the dispatch — otherwise just verify the binary builds and `--help` shows
the right flags.

### Step 2.6: Commit

```bash
git add velox/benchmarks/AbBenchmarkMain.h \
        velox/benchmarks/AbBenchmarkMain.cpp \
        velox/benchmarks/CMakeLists.txt \
        velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp
```

```
refactor(benchmarks): extract installFsCache + dispatchAbMain

The FsCache install (wipe root, create dir, build FsCache sized to
FLAGS_fscache_disk_gib, setInstance) and the --input_source dispatch
(legacy / cbi / fscache branches) are byte-identical between TpcdsMain
and what TpchMain needs. Move both into AbBenchmarkMain.{h,cpp}.

TpcdsBenchmarkMain's main() becomes a 3-line trampoline.
```

---

## Task 3: Wire TpchBenchmark to AbBenchmarkBase

**Files:**
- Modify: `velox/benchmarks/tpch/TpchBenchmark.h`
- Modify: `velox/benchmarks/tpch/TpchBenchmark.cpp`
- Modify: `velox/benchmarks/tpch/TpchBenchmarkMain.cpp`
- Modify: `velox/benchmarks/tpch/CMakeLists.txt`

### Step 3.1: Modify `TpchBenchmark.h`

Change include + base class:

```cpp
#include "velox/benchmarks/AbBenchmarkBase.h"
// ...
class TpchBenchmark : public facebook::velox::benchmarks::AbBenchmarkBase {
```

Delete `queryConfigs_` from this header (now inherited).

Add the two virtual overrides:

```cpp
 protected:
  int32_t numQueries() const override {
    return 22;
  }
  facebook::velox::exec::test::VeloxPlan buildPlan(int32_t queryId) override {
    return queryBuilder_->getQueryPlan(queryId);
  }
```

`runQuery` inline stays as-is; it calls `queryBuilder_->getQueryPlan(queryId)`
which still works.

### Step 3.2: Modify `TpchBenchmark.cpp`

No body changes expected (the overrides are inline in the header). Confirm
the file still compiles. If the existing file references `queryConfigs_`
as if from the local class — it does, line 88, 121 — those still resolve
because it's inherited from the base. No edit needed.

### Step 3.3: Modify `TpchBenchmarkMain.cpp`

Add includes for `AbBenchmarkMain.h` and gflags `DECLARE_string(input_source)`.

Replace `main()` body (current 30 LOC `main` is in this file — read it
first) with the same trampoline pattern as TPC-DS:

```cpp
int main(int argc, char** argv) {
  // existing usage string + folly::Init stays
  // ...
  benchmark = std::make_unique<TpchBenchmark>();
  return facebook::velox::benchmarks::dispatchAbMain(
      *benchmark, tpchBenchmarkMain);
}
```

(Adapt to whatever the current `main()` already looks like — preserve usage
string and any TPC-H-specific argv prep.)

### Step 3.4: Update `velox/benchmarks/tpch/CMakeLists.txt`

Add `velox_benchmark_ab` to the link list of the TPC-H benchmark binary
target.

### Step 3.5: Build + verify

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_tpch_benchmark -j 8
```

Expected: clean build. Verify A/B flags surface:

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --help 2>&1 | grep -E "input_source|rounds|fscache_disk_gib|fscache_root|^    -out " | head -10
```

### Step 3.6: Smoke-test TPC-H A/B on real data

CBI side (1 round, q6 only is hard without a query subset flag — so just run
the full 22-query sweep once with rounds=1 to prove plumbing works):

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --data_path=/home/chang/test/tpch/tpch-generated-100.0-parquet \
  --data_format=parquet \
  --input_source=cbi \
  --cache_gb=8 \
  --rounds=1 \
  --num_repeats=1 \
  --out=/tmp/tpch_ab_smoke_cbi.csv 2>&1 | tail -20
wc -l /tmp/tpch_ab_smoke_cbi.csv  # expect 23 (header + 22 rows)
head -3 /tmp/tpch_ab_smoke_cbi.csv
```

Expected: 23 lines, header matches `round,query_id,wall_ms,...,error`, q01 row
has non-zero wall_ms.

FsCache side:

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --data_path=/home/chang/test/tpch/tpch-generated-100.0-parquet \
  --data_format=parquet \
  --input_source=fscache \
  --fscache_disk_gib=8 \
  --fscache_root=/tmp/velox_fscache_smoke \
  --rounds=1 \
  --num_repeats=1 \
  --out=/tmp/tpch_ab_smoke_fscache.csv 2>&1 | tail -20
wc -l /tmp/tpch_ab_smoke_fscache.csv  # expect 23
```

Expected: 23 lines, hit% column non-zero on at least round 2 once warmed
(round 1 alone may be 0% — fine, that's the cold path).

### Step 3.7: Commit

```bash
git add velox/benchmarks/tpch/TpchBenchmark.h \
        velox/benchmarks/tpch/TpchBenchmark.cpp \
        velox/benchmarks/tpch/TpchBenchmarkMain.cpp \
        velox/benchmarks/tpch/CMakeLists.txt
```

```
feat(benchmarks/tpch): wire TpchBenchmark to AbBenchmarkBase A/B harness

TpchBenchmark now inherits the FsCache-vs-CBI sweep harness from
AbBenchmarkBase (--input_source={cbi,fscache} --rounds N --out file.csv,
22 queries with q01..q22). Legacy folly::runBenchmarks path is preserved
when --input_source is empty.

Enables Phase-2 TPC-H sweep on
/home/chang/test/tpch/tpch-generated-100.0-parquet as the substitute for
the TPC-DS Task 6 sweep (TPC-DS data is not on this host).
```

---

## Task 4: Run the 5-query × 3-round sweep (results NOT committed)

**Per user choice:** code commits land in Tasks 1-3. Sweep data and any
ad-hoc analysis notes live under `/tmp/`. No files added to git in this task.

### Step 4.1: Run cbi side, 3 rounds, all 22 queries

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --data_path=/home/chang/test/tpch/tpch-generated-100.0-parquet \
  --data_format=parquet \
  --input_source=cbi \
  --cache_gb=16 \
  --rounds=3 \
  --num_repeats=1 \
  --out=/tmp/tpch_ab_cbi.csv 2>&1 | tee /tmp/tpch_ab_cbi.log
wc -l /tmp/tpch_ab_cbi.csv  # expect 67 (1 header + 22 * 3)
```

### Step 4.2: Run fscache side, 3 rounds, all 22 queries

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --data_path=/home/chang/test/tpch/tpch-generated-100.0-parquet \
  --data_format=parquet \
  --input_source=fscache \
  --fscache_disk_gib=16 \
  --fscache_root=/tmp/velox_fscache_ab \
  --rounds=3 \
  --num_repeats=1 \
  --out=/tmp/tpch_ab_fscache.csv 2>&1 | tee /tmp/tpch_ab_fscache.log
wc -l /tmp/tpch_ab_fscache.csv  # expect 67
```

### Step 4.3: Extract the 5-query subset and compare

The harness runs all 22 queries because there's no `--query_filter` flag —
that's fine; we just slice the rows we care about post-hoc.

```bash
# Pull q01, q06, q14, q19, q22 from both files, side-by-side.
for q in q01 q06 q14 q19 q22; do
  echo "=== $q ==="
  echo "cbi:     $(awk -F, -v q=$q '$2==q {printf "round=%s wall=%s hit%%=%s\n", $1, $3, $6}' /tmp/tpch_ab_cbi.csv)"
  echo "fscache: $(awk -F, -v q=$q '$2==q {printf "round=%s wall=%s hit%%=%s\n", $1, $3, $6}' /tmp/tpch_ab_fscache.csv)"
done
```

### Step 4.4: Write findings to `/tmp/tpch_ab_findings.md`

Tally the q01/q06/q14/q19/q22 numbers, note per-query wall_ms delta
(fscache vs cbi), hit% trajectory across rounds 1→2→3, and any failures.

Do **NOT** `git add` this file. Surface the takeaway in the chat summary
instead.

---

## Verification

After Tasks 1-3:

- `git log --oneline upstream/main..HEAD | head` shows 3 new commits beyond
  the existing branch state.
- `git diff upstream/main -- velox/common/caching/benchmarks/CacheBackendBenchmark.cpp`
  is empty (excluded file untouched).
- `git grep "DEFINE_string(input_source" velox/benchmarks/` shows exactly
  **one** definition, in `AbBenchmarkBase.cpp`.
- `git grep "installFsCache" velox/benchmarks/` shows exactly **one**
  definition, in `AbBenchmarkMain.cpp`.
- Both `velox_tpcds_benchmark` and `velox_tpch_benchmark` build clean and
  `--help` lists `input_source`, `rounds`, `fscache_disk_gib`, `fscache_root`,
  `out`.

After Task 4:

- `/tmp/tpch_ab_cbi.csv` and `/tmp/tpch_ab_fscache.csv` exist, each 67 lines,
  no `error` column populated for q01/q06/q14/q19/q22.
- A findings note exists in `/tmp/tpch_ab_findings.md` and is summarized in
  chat.
- `git status` shows clean (no result files staged).

## Failure handling

- Phase 2/4 reviewer retries 3× still CRITICAL/HIGH → stop and report.
- Build failures in Step 1.7/2.5/3.5 → treat as CRITICAL, fix via review-fix
  loop (max 3 retries per phase).
- Smoke test in 3.6 shows 0% hit even on round 2 → likely the FsCache
  singleton wasn't installed or `--cache_gb` wasn't forced to 0; verify
  `dispatchAbMain` actually ran the fscache branch (add a stderr log if
  ambiguous).
- Sweep in Task 4 has >2 query failures → stop, inspect the ERROR log,
  decide whether to drop the query from the subset or fix.
