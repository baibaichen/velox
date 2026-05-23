# FsCache Phase-1 Microbenchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `velox_fscache_benchmark`, a single-binary microbenchmark that runs the 36-cell sweep specified in `docs/superpowers/specs/2026-05-23-fscache-microbench-design.md` and emits a Markdown table of throughput, hit-rate, write-amplification, and latency tails for `FsCache::getOrSet`.

**Architecture:** One `.cpp` file under `velox/common/caching/fscache/benchmarks/`, gated on the existing `VELOX_ENABLE_BENCHMARKS_BASIC` cmake option. The file contains, in this order: includes + flags, `SleepyReadFile` (composition wrapper over `LocalReadFile`), `KeyGenerator` (sequential/zipfian/uniform — lifted from the cachelib branch), `FsCacheDriver` (per-cell owner of FsCache + SleepyReadFile + cacheRoot), `runCell()` (warmup → baseline snapshot → measured loop → deltas), Markdown printer, and `main()` (flag parsing + Cartesian sweep + SIGINT cleanup). Each cell gets a fresh driver with `/tmp/velox_fscache_bench/<pid>/<cellIdx>/`; the destructor `rm -rf`s it.

**Tech Stack:** C++20, folly (`folly::sformat`, `folly::split`, `folly::trimWhitespace`), gflags, glog, GoogleTest+gmock (for the supporting unit test), `std::thread` / `std::mt19937_64` / `std::nth_element`, `std::filesystem` for cleanup, POSIX `signal()` for SIGINT.

---

## Scope & Invariants

These are referenced throughout the tasks; collect them here so reviewers can spot violations quickly.

- **Phase-1 FsCache config (locked, overrides defaults):** `alignment = 1 MiB`, `maxSegmentSize = 1 MiB`, `maxBytes = 512 MiB`, `numBuckets = 1024`. With these knobs, `getOrSet(_, _, 1 MiB, _)` returns exactly one segment per op, so `hits + misses == ops` per cell.
- **Cell sweep:** 4 axes × Cartesian product = 36 cells. Each axis flag is a CSV: `--workloads`, `--threads_list`, `--ws_mult_list`, `--remote_latency_us_list`. Defaults reproduce the full 36-cell sweep.
- **Per-cell sandbox:** `cacheRoot = /tmp/velox_fscache_bench/<pid>/<cellIdx>/`. Driver dtor `rm -rf`s it. `main()` SIGINT handler `rm -rf`s `/tmp/velox_fscache_bench/<pid>/` so Ctrl-C does not leak gigabytes.
- **Shared remote blob:** `/tmp/velox_fscache_bench_remote.bin`, default 2 GiB, rebuilt if missing / size-mismatched / `--rebuild_remote_file`. All cells reuse it.
- **Baseline snapshot pattern:** After warmup, snapshot `fsCache.stats()` and reset `SleepyReadFile::bytesRead_`. Compute deltas at end of measured loop. Avoids the hit% > 100% bug where warmup counters bleed into the cell's reported hit%.
- **`ops` must divide cleanly by every value in `--threads_list`.** Validate at startup (`VELOX_USER_CHECK`); silently dropping ops would corrupt the Phase-1 ↔ Phase-2 A/B comparison.
- **Constraints that already passed user review:** SleepyReadFile MUST use composition (not inheritance) — `LocalReadFile::pread/size/preadv/memoryUsage/shouldCoalesce` are `final`. SleepyReadFile bumps inherited `ReadFile::bytesRead_` directly in its own `pread()`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp` | NEW | Single-file benchmark binary (~500 LOC). |
| `velox/common/caching/fscache/benchmarks/CMakeLists.txt` | NEW | `add_executable(velox_fscache_benchmark …)` + link libs. |
| `velox/common/caching/fscache/CMakeLists.txt` | MODIFY (append) | Add `add_subdirectory(benchmarks)` under `VELOX_ENABLE_BENCHMARKS_BASIC`. |
| `velox/common/caching/fscache/tests/KeyGeneratorTest.cpp` | NEW | gtest for KeyGenerator (sequential partitioning, full-universe zipfian/uniform, determinism). |
| `velox/common/caching/fscache/tests/CMakeLists.txt` | MODIFY (1 line) | Add `KeyGeneratorTest.cpp` to the test executable. |
| `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` | NEW (Task 9) | Captured output of the Phase-1 baseline run, committed for Phase-2 A/B. |

KeyGenerator gets its own tiny header `velox/common/caching/fscache/benchmarks/KeyGenerator.h` so it can be unit-tested without linking the whole benchmark binary. SleepyReadFile stays in the anonymous namespace inside `FsCacheBenchmark.cpp` — it's purely a benchmark fixture, no value in exposing it.

| File | Status | Responsibility |
|---|---|---|
| `velox/common/caching/fscache/benchmarks/KeyGenerator.h` | NEW | Header-only KeyGenerator class (so the test can include it). |

---

## Commit Decomposition

Nine commits, each independently buildable and (where applicable) testable. After every commit run `make format` and re-build before staging.

1. **Scaffold** — empty binary + CMake gate + `--help` works. (Task 1)
2. **KeyGenerator + unit test** — pure logic, no FsCache yet. (Task 2)
3. **SleepyReadFile** — drop-in inside the binary, with a tiny smoke check in `main()`. (Task 3)
4. **Remote blob + cleanup helpers** — shared file creation, SIGINT handler, per-cell cacheRoot. (Task 4)
5. **FsCacheDriver + runCell skeleton** — single-thread, no metrics yet, prove a cell runs end-to-end. (Task 5)
6. **Per-thread latency + quantiles + parallel run** — multi-thread, latency vectors, p50/p95/p99. (Task 6)
7. **Baseline snapshot + delta accounting + 13-column Markdown** — the real metrics. (Task 7)
8. **Flag parsing + Cartesian sweep + `--out` file redirect + startup validation** — the sweep driver. (Task 8)
9. **Phase-1 baseline run + commit results** — capture the .md, commit alongside the bench. (Task 9)

---

## Task 1: Scaffold the benchmark binary

**Goal:** `velox_fscache_benchmark --help` runs, exits 0, prints gflags help. No FsCache work yet. Proves the CMake gate and link wiring.

**Files:**
- Create: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`
- Create: `velox/common/caching/fscache/benchmarks/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/CMakeLists.txt` (append `add_subdirectory(benchmarks)` block)

- [ ] **Step 1.1: Create `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`**

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

// Phase-1 microbenchmark for FsCache::getOrSet. See
// docs/superpowers/specs/2026-05-23-fscache-microbench-design.md for the
// design and the 36-cell sweep specification.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/file/FileSystems.h"

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();
  LOG(INFO) << "velox_fscache_benchmark scaffold OK";
  return 0;
}
```

- [ ] **Step 1.2: Create `velox/common/caching/fscache/benchmarks/CMakeLists.txt`**

```cmake
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Phase-1 FsCache::getOrSet self-cost microbenchmark. Gated on
# VELOX_ENABLE_BENCHMARKS_BASIC so the default OFF build is unaffected.
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

- [ ] **Step 1.3: Modify `velox/common/caching/fscache/CMakeLists.txt`** — append below the `if(${VELOX_BUILD_TESTING}) add_subdirectory(tests) endif()` block:

```cmake
if(${VELOX_ENABLE_BENCHMARKS_BASIC})
  add_subdirectory(benchmarks)
endif()
```

- [ ] **Step 1.4: Build with the gate ON**

Run from the repo root:
```bash
cmake -S . -B cmake-build-bench-rwdi -GNinja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DVELOX_ENABLE_BENCHMARKS_BASIC=ON
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
```
Expected: target builds without errors. `cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark` exists.

- [ ] **Step 1.5: Smoke test the binary**

Run:
```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark --help 2>&1 | head -5
echo "exit=$?"
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark 2>&1 | tail -2
```
Expected: `--help` exits 0 and prints flag listings; running with no args logs `velox_fscache_benchmark scaffold OK` and exits 0.

- [ ] **Step 1.6: Verify gate OFF leaves CI untouched**

Run:
```bash
cmake -S . -B cmake-build-default -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build cmake-build-default --target velox_fscache_benchmark 2>&1 | tail -3 || echo "expected: target not found"
```
Expected: ninja fails with "Unknown target" (or similar). Confirms the gate works.

- [ ] **Step 1.7: Commit**

```bash
git add velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/benchmarks/CMakeLists.txt \
        velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): scaffold FsCacheBenchmark binary

Empty driver gated on VELOX_ENABLE_BENCHMARKS_BASIC. Subsequent
commits fill in SleepyReadFile, KeyGenerator, runCell, and the
36-cell sweep per the design doc.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 2: KeyGenerator + unit test

**Goal:** Three workloads (sequential / zipfian theta=1.0 / uniform), each deterministic given a seed. Sequential walks `[0, n)` and wraps; zipfian/uniform draw from `[0, n)` shared. Pure logic, testable without FsCache.

**Files:**
- Create: `velox/common/caching/fscache/benchmarks/KeyGenerator.h`
- Create: `velox/common/caching/fscache/tests/KeyGeneratorTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt` (add the test file)

- [ ] **Step 2.1: Create `velox/common/caching/fscache/benchmarks/KeyGenerator.h`**

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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

namespace facebook::velox::cache::fs::bench {

enum class Workload { kSequential, kZipfian, kUniform };

/// Per-thread key index generator. Pure logic, no IO. `next()` returns an
/// index in `[0, n)`. Sequential walks the half-open range `[seqStart,
/// seqStart + n)` modulo `n`, so giving each thread a distinct seqStart and
/// the same n yields partitioned scans; giving every thread the same start
/// yields aligned shared scans. Zipfian/uniform draw from `[0, n)`.
class KeyGenerator {
 public:
  KeyGenerator(
      Workload workload,
      uint64_t n,
      uint64_t seed,
      uint64_t seqStart = 0,
      double zipfTheta = 1.0)
      : workload_(workload),
        n_(n),
        rng_(seed),
        seqPos_(n_ == 0 ? 0 : seqStart % n_) {
    if (workload_ == Workload::kZipfian) {
      buildZipfCdf(zipfTheta);
    }
  }

  uint64_t next() {
    switch (workload_) {
      case Workload::kSequential: {
        const auto k = seqPos_;
        seqPos_ = (seqPos_ + 1) % n_;
        return k;
      }
      case Workload::kZipfian: {
        std::uniform_real_distribution<double> u(0.0, 1.0);
        const double r = u(rng_);
        auto it = std::lower_bound(cdf_.begin(), cdf_.end(), r);
        return static_cast<uint64_t>(std::distance(cdf_.begin(), it));
      }
      case Workload::kUniform: {
        std::uniform_int_distribution<uint64_t> d(0, n_ - 1);
        return d(rng_);
      }
    }
    return 0;
  }

 private:
  void buildZipfCdf(double theta) {
    cdf_.resize(n_);
    double sum = 0.0;
    for (uint64_t i = 1; i <= n_; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i), theta);
      cdf_[i - 1] = sum;
    }
    for (auto& c : cdf_) {
      c /= sum;
    }
  }

  const Workload workload_;
  const uint64_t n_;
  std::mt19937_64 rng_;
  std::vector<double> cdf_;
  uint64_t seqPos_;
};

} // namespace facebook::velox::cache::fs::bench
```

- [ ] **Step 2.2: Write the failing test — create `velox/common/caching/fscache/tests/KeyGeneratorTest.cpp`**

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

#include "velox/common/caching/fscache/benchmarks/KeyGenerator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <vector>

namespace facebook::velox::cache::fs::bench::test {

TEST(KeyGeneratorTest, sequentialWalksAndWraps) {
  KeyGenerator g{Workload::kSequential, 4, /*seed=*/42};
  std::vector<uint64_t> seen;
  for (int i = 0; i < 10; ++i) {
    seen.push_back(g.next());
  }
  // Walks 0..3 then wraps.
  EXPECT_EQ(seen, (std::vector<uint64_t>{0, 1, 2, 3, 0, 1, 2, 3, 0, 1}));
}

TEST(KeyGeneratorTest, sequentialPartitionedBySeqStart) {
  // Two "threads" each owning half of the 8-key universe via disjoint seqStart.
  KeyGenerator g0{Workload::kSequential, 8, /*seed=*/42, /*seqStart=*/0};
  KeyGenerator g1{Workload::kSequential, 8, /*seed=*/42, /*seqStart=*/4};
  std::set<uint64_t> first4, second4;
  for (int i = 0; i < 4; ++i) {
    first4.insert(g0.next());
    second4.insert(g1.next());
  }
  EXPECT_EQ(first4, (std::set<uint64_t>{0, 1, 2, 3}));
  EXPECT_EQ(second4, (std::set<uint64_t>{4, 5, 6, 7}));
}

TEST(KeyGeneratorTest, uniformCoversFullUniverse) {
  KeyGenerator g{Workload::kUniform, 16, /*seed=*/42};
  std::set<uint64_t> seen;
  for (int i = 0; i < 1000; ++i) {
    seen.insert(g.next());
  }
  // 1000 draws over 16 keys: P(any miss) ≈ 16 * (15/16)^1000 ≈ 1e-27.
  EXPECT_EQ(seen.size(), 16u);
  for (auto k : seen) {
    EXPECT_LT(k, 16u);
  }
}

TEST(KeyGeneratorTest, zipfianHotKeysDominate) {
  KeyGenerator g{Workload::kZipfian, 100, /*seed=*/42};
  std::vector<uint64_t> hist(100, 0);
  constexpr int kDraws = 10'000;
  for (int i = 0; i < kDraws; ++i) {
    ++hist[g.next()];
  }
  // theta=1.0: top-10 keys cover roughly 0.5 * H_100 / H_100 ≈ 50% of mass.
  // Empirically with seed=42 this is ~50%; use a generous bound to keep the
  // test stable across stdlib RNG impls.
  uint64_t top10 = 0;
  for (int i = 0; i < 10; ++i) {
    top10 += hist[i];
  }
  EXPECT_GT(top10, kDraws * 30 / 100);
  EXPECT_LT(top10, kDraws * 70 / 100);
}

TEST(KeyGeneratorTest, sameSeedProducesSameSequence) {
  KeyGenerator a{Workload::kZipfian, 64, /*seed=*/123};
  KeyGenerator b{Workload::kZipfian, 64, /*seed=*/123};
  for (int i = 0; i < 200; ++i) {
    EXPECT_EQ(a.next(), b.next()) << "draw " << i;
  }
}

} // namespace facebook::velox::cache::fs::bench::test
```

- [ ] **Step 2.3: Wire the test into CMake** — modify `velox/common/caching/fscache/tests/CMakeLists.txt` by adding `KeyGeneratorTest.cpp` to the list of test sources. Open the file and locate the `add_executable(velox_fscache_test …)` block (or whatever lists `FsCacheConcurrencyTest.cpp`); insert `KeyGeneratorTest.cpp` in alphabetical order. After the edit, the relevant section should look like:

```cmake
add_executable(
  velox_fscache_test
  EvictionPolicyTest.cpp
  FileSegmentTest.cpp
  FsCacheConcurrencyTest.cpp
  FsCacheGuardsTest.cpp
  FsCacheKeyTest.cpp
  FsCacheMetadataTest.cpp
  FsCachePersistenceTest.cpp
  FsCacheRecoveryTest.cpp
  FsCacheScaffoldTest.cpp
  FsCacheSplitRangeTest.cpp
  FsCacheTest.cpp
  KeyGeneratorTest.cpp
)
```
(Read the actual file first — the exact target name may differ; preserve existing surrounding lines.)

- [ ] **Step 2.4: Run the test to verify it passes**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_test
./cmake-build-bench-rwdi/velox/common/caching/fscache/tests/velox_fscache_test --gtest_filter='KeyGeneratorTest.*'
```
Expected: 5 tests, all PASS. (If `velox_fscache_test` is not the actual target name, substitute the correct one from the CMake file.)

- [ ] **Step 2.5: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/KeyGenerator.h \
        velox/common/caching/fscache/tests/KeyGeneratorTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "bench(fscache): KeyGenerator with sequential/zipfian/uniform

Header-only generator lifted from the cachelib branch's
CacheBackendBenchmark, plus gtest coverage for walk/wrap,
partitioning via seqStart, uniform full-universe coverage,
zipfian skew, and seed determinism.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 3: SleepyReadFile

**Goal:** A `ReadFile` wrapper that delegates to an inner `LocalReadFile` and optionally sleeps `latency_us` before each `pread`. `latency_us == 0` ⇒ no sleep call at all (not `sleep_for(0)`), so the hot-local cell pays zero scheduler overhead.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 3.1: Edit `FsCacheBenchmark.cpp`** — add the include for `velox/common/file/File.h`, the chrono/thread/string headers, and the anonymous-namespace `SleepyReadFile`. Replace the file contents with:

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

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace {

using namespace facebook::velox;

/// ReadFile that wraps an inner LocalReadFile and optionally sleeps
/// `latencyUs_` microseconds before each pread to simulate remote IO.
/// Composition (not inheritance) because LocalReadFile's pread/size/preadv/
/// memoryUsage/shouldCoalesce are `final`. Bumps the inherited
/// ReadFile::bytesRead_ counter directly so the driver can read
/// `bytesRead()` to see how many bytes hit the remote.
class SleepyReadFile : public ReadFile {
 public:
  SleepyReadFile(const std::string& path, uint64_t latencyUs)
      : inner_(path), latencyUs_(latencyUs) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override {
    if (latencyUs_ != 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(latencyUs_));
    }
    auto out = inner_.pread(offset, length, buf, context);
    bytesRead_ += length;
    return out;
  }

  uint64_t size() const override {
    return inner_.size();
  }

  uint64_t memoryUsage() const override {
    return inner_.memoryUsage();
  }

  bool shouldCoalesce() const override {
    return inner_.shouldCoalesce();
  }

  std::string getName() const override {
    return inner_.getName();
  }

  uint64_t getNaturalReadSize() const override {
    return inner_.getNaturalReadSize();
  }

 private:
  mutable LocalReadFile inner_;
  const uint64_t latencyUs_;
};

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();
  LOG(INFO) << "velox_fscache_benchmark scaffold OK";
  return 0;
}
```

- [ ] **Step 3.2: Build**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
```
Expected: builds without errors. (If the compiler complains that `LocalReadFile::pread` is overloaded — the base class declares both buffer-and-no-buffer forms — verify the `override` matches the pure-virtual `pread(offset, length, void*, context)` signature only. We do not need to override the buffer-allocating overload because the base class provides a default implementation.)

- [ ] **Step 3.3: Add a smoke check** — temporarily edit `main()` to construct one `SleepyReadFile` over `/etc/hostname` (always exists and small), call `pread`, and log the bytes read. Replace the `LOG(INFO) << "…"` line with:

```cpp
  SleepyReadFile f{"/etc/hostname", /*latencyUs=*/0};
  char buf[64];
  auto got = f.pread(0, std::min<uint64_t>(64, f.size()), buf);
  LOG(INFO) << "SleepyReadFile smoke: size=" << f.size()
            << " bytesRead=" << f.bytesRead() << " content=" << got;
```

- [ ] **Step 3.4: Run the smoke check**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark 2>&1 | tail -2
```
Expected: logs `SleepyReadFile smoke: size=<n> bytesRead=<n> content=<hostname>` and exits 0. `bytesRead` should equal the `length` argument we passed (not the actual `size()` if smaller).

- [ ] **Step 3.5: Revert the smoke check** — delete the temporary `SleepyReadFile f{...}` block, restore `LOG(INFO) << "velox_fscache_benchmark scaffold OK";`. Rebuild to confirm still green.

- [ ] **Step 3.6: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): SleepyReadFile composition wrapper

Wraps LocalReadFile and optionally sleeps before each pread to
simulate remote IO. Composition because LocalReadFile's overrides
are final; bumps ReadFile::bytesRead_ in its own pread so the
driver can read bytesRead() to measure write amplification.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 4: Remote blob + cleanup helpers

**Goal:** Lazy-initialize `/tmp/velox_fscache_bench_remote.bin` (default 2 GiB of pseudo-random bytes) and install a SIGINT handler that recursively removes `/tmp/velox_fscache_bench/<pid>/`. Both are pure utilities; no FsCache yet.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 4.1: Add flags and helpers** — insert after the SleepyReadFile class, still inside the anonymous namespace:

```cpp
} // namespace

DEFINE_uint64(
    remote_file_size_gb,
    2,
    "Size of /tmp/velox_fscache_bench_remote.bin in GiB. Rebuilt if "
    "missing or size-mismatched.");
DEFINE_bool(
    rebuild_remote_file,
    false,
    "Force rebuild of the shared remote blob even if size matches.");

namespace {

using namespace facebook::velox;

constexpr const char* kRemotePath = "/tmp/velox_fscache_bench_remote.bin";

std::string benchTmpRoot() {
  return "/tmp/velox_fscache_bench/" + std::to_string(::getpid());
}

void ensureRemoteFile() {
  namespace fs = std::filesystem;
  const uint64_t want = FLAGS_remote_file_size_gb * (1ULL << 30);
  if (!FLAGS_rebuild_remote_file && fs::exists(kRemotePath) &&
      fs::file_size(kRemotePath) == want) {
    return;
  }
  LOG(INFO) << "Building remote blob " << kRemotePath << " (" << want
            << " bytes)";
  std::ofstream out{kRemotePath, std::ios::binary | std::ios::trunc};
  constexpr size_t kChunk = 1 << 20;
  std::vector<char> chunk(kChunk);
  std::mt19937_64 rng{0xfeedfaceULL};
  for (uint64_t written = 0; written < want; written += kChunk) {
    for (size_t i = 0; i < kChunk; i += 8) {
      const uint64_t v = rng();
      std::memcpy(chunk.data() + i, &v, 8);
    }
    const size_t toWrite =
        static_cast<size_t>(std::min<uint64_t>(kChunk, want - written));
    out.write(chunk.data(), toWrite);
  }
  VELOX_CHECK(out.good(), "Failed to write remote blob");
}

void cleanupBenchTmp() {
  std::error_code ec;
  std::filesystem::remove_all(benchTmpRoot(), ec);
}

void onSigint(int /*signo*/) {
  cleanupBenchTmp();
  // Re-raise with default handler so the process exits with the conventional
  // 128+SIGINT status instead of swallowing the signal silently.
  signal(SIGINT, SIG_DFL);
  raise(SIGINT);
}

} // namespace
```

You will also need these includes at the top (add only the ones not already present):

```cpp
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <unistd.h>

#include "velox/common/base/Exceptions.h"
```

- [ ] **Step 4.2: Wire helpers into `main()`** — replace the body of `main()` with:

```cpp
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();

  signal(SIGINT, onSigint);
  std::filesystem::create_directories(benchTmpRoot());

  ensureRemoteFile();
  LOG(INFO) << "Setup complete. tmpRoot=" << benchTmpRoot()
            << " remote=" << kRemotePath;

  cleanupBenchTmp();
  return 0;
}
```

- [ ] **Step 4.3: Build and run with a small remote**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 2>&1 | tail -3
ls -lh /tmp/velox_fscache_bench_remote.bin
ls -la /tmp/velox_fscache_bench/ 2>&1
```
Expected: log line `Building remote blob …` (first run), `1.0G` blob exists, `/tmp/velox_fscache_bench/` no longer contains the just-removed `<pid>` directory (cleanupBenchTmp ran). Second run: no "Building" log (size matches) and exits cleanly.

- [ ] **Step 4.4: Test SIGINT path manually**

```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark --remote_file_size_gb=1 &
PID=$!
sleep 0.2
kill -INT $PID
wait $PID; echo "exit=$?"
ls /tmp/velox_fscache_bench/ 2>&1
```
Expected: process exits non-zero (SIGINT), `/tmp/velox_fscache_bench/` either empty or does not exist. (May be flaky if the process finishes before the signal arrives; if so, increase the sleep.)

- [ ] **Step 4.5: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): remote blob + per-pid tmp + SIGINT cleanup

Lazy-initializes the shared /tmp/velox_fscache_bench_remote.bin
(rebuilt on size mismatch or --rebuild_remote_file) and installs
a SIGINT handler that recursively removes /tmp/velox_fscache_bench/<pid>/
so Ctrl-C during a long sweep does not leave gigabytes behind.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 5: FsCacheDriver + single-thread `runCell` skeleton

**Goal:** One driver class per cell, owning an `FsCache` (locked Phase-1 config) and a `SleepyReadFile` over the shared remote blob. `runCell` runs single-thread warmup + measured loop, logs `ops` and `wallSec`. No multi-thread, no metrics columns yet — that comes in Tasks 6 and 7.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 5.1: Add includes**

```cpp
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/caching/fscache/benchmarks/KeyGenerator.h"
```

- [ ] **Step 5.2: Add the driver + skeleton runCell, inside the anonymous namespace, after the helpers from Task 4:**

```cpp
using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;

constexpr uint64_t kSegmentBytes = 1ULL << 20; // 1 MiB
constexpr uint64_t kMaxCacheBytes = 512ULL * (1ULL << 20); // 512 MiB

class FsCacheDriver {
 public:
  FsCacheDriver(uint64_t workingSetKeys, uint64_t latencyUs, int cellIdx)
      : cacheRoot_(benchTmpRoot() + "/" + std::to_string(cellIdx)),
        workingSetKeys_(workingSetKeys),
        sleepyReadFile_(kRemotePath, latencyUs) {
    std::filesystem::create_directories(cacheRoot_);
    FsCacheConfig cfg;
    cfg.cacheRoot = cacheRoot_;
    cfg.maxBytes = kMaxCacheBytes;
    cfg.alignment = kSegmentBytes;
    cfg.maxSegmentSize = kSegmentBytes;
    fsCache_ = std::make_unique<FsCache>(cfg);
  }

  ~FsCacheDriver() {
    fsCache_.reset();
    std::error_code ec;
    std::filesystem::remove_all(cacheRoot_, ec);
  }

  FsCache& fsCache() { return *fsCache_; }
  SleepyReadFile& sleepyReadFile() { return sleepyReadFile_; }
  uint64_t workingSetKeys() const { return workingSetKeys_; }

 private:
  const std::string cacheRoot_;
  const uint64_t workingSetKeys_;
  SleepyReadFile sleepyReadFile_;
  std::unique_ptr<FsCache> fsCache_;
};

void doOps(
    FsCacheDriver& d,
    KeyGenerator& gen,
    uint64_t ops) {
  for (uint64_t i = 0; i < ops; ++i) {
    const uint64_t keyIdx = gen.next();
    const uint64_t offset = keyIdx * kSegmentBytes;
    auto segs = d.fsCache().getOrSet(
        kRemotePath, offset, kSegmentBytes, d.sleepyReadFile());
    (void)segs;
  }
}

void runCellSkeleton(
    Workload workload,
    uint64_t wsKeys,
    uint64_t latencyUs,
    uint64_t warmupOps,
    uint64_t ops,
    int cellIdx) {
  FsCacheDriver d(wsKeys, latencyUs, cellIdx);
  KeyGenerator warm{workload, wsKeys, /*seed=*/42};
  doOps(d, warm, warmupOps);
  KeyGenerator main{workload, wsKeys, /*seed=*/43};
  const auto t0 = std::chrono::steady_clock::now();
  doOps(d, main, ops);
  const auto t1 = std::chrono::steady_clock::now();
  const double wallSec = std::chrono::duration<double>(t1 - t0).count();
  LOG(INFO) << "cell " << cellIdx << " ops=" << ops
            << " wallSec=" << wallSec
            << " bytesOnDisk=" << d.fsCache().stats().bytesOnDisk;
}
```

- [ ] **Step 5.3: Drive one cell from `main()`** — append before `cleanupBenchTmp()`:

```cpp
  runCellSkeleton(
      Workload::kSequential,
      /*wsKeys=*/256,
      /*latencyUs=*/0,
      /*warmupOps=*/1000,
      /*ops=*/5000,
      /*cellIdx=*/0);
```

- [ ] **Step 5.4: Build and run**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 2>&1 | tail -3
```
Expected: one log line `cell 0 ops=5000 wallSec=… bytesOnDisk=…`. bytesOnDisk should be `256 * 1 MiB = 268435456` exactly (the working set fits in 512 MiB and sequential walks every key during warmup, so all 256 keys are downloaded once).

- [ ] **Step 5.5: Remove the temporary call** — delete the `runCellSkeleton(...)` block from `main()` so the next task can wire in the real sweep without churn. Rebuild to confirm clean.

- [ ] **Step 5.6: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): FsCacheDriver + single-thread runCell skeleton

Per-cell driver owns one FsCache (Phase-1 locked config:
alignment = maxSegmentSize = 1 MiB, maxBytes = 512 MiB) and one
SleepyReadFile over the shared remote blob. Driver dtor rm -rf's
its cacheRoot. runCellSkeleton proves the warmup+measured pattern
end-to-end before adding multi-thread, latency vectors, and the
13-column metrics table.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 6: Per-thread latency + quantiles + parallel run

**Goal:** Multi-thread the measured loop. Each worker owns a pre-reserved `std::vector<uint64_t>` of per-op nanosecond latencies (no malloc on hot path). After join, concatenate and compute p50/p95/p99 via `std::nth_element`. Sequential: each thread gets a disjoint slice via `seqStart`; zipfian/uniform share the full keyspace.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 6.1: Replace `doOps` with `parallelRun`.** Delete `doOps` from Task 5 and insert:

```cpp
void parallelRun(
    FsCacheDriver& d,
    Workload workload,
    uint64_t threads,
    uint64_t opsPerThread,
    bool recordLatency,
    uint64_t seedBase,
    std::vector<std::vector<uint64_t>>* perThreadLatencies) {
  if (recordLatency) {
    perThreadLatencies->assign(threads, {});
    for (auto& v : *perThreadLatencies) {
      v.reserve(opsPerThread);
    }
  }
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (uint64_t t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      // Sequential: each thread owns a disjoint slice of [0, workingSetKeys)
      //   by giving KeyGenerator universe = slice and adding keyOffset to
      //   every output. KeyGenerator stays workload-agnostic; the per-thread
      //   offset lives here in the driver.
      // Zipfian / uniform: shared keyspace, no offset.
      uint64_t universe;
      uint64_t keyOffset;
      if (workload == Workload::kSequential) {
        const uint64_t slice = d.workingSetKeys() / threads;
        universe = slice;
        keyOffset = t * slice;
      } else {
        universe = d.workingSetKeys();
        keyOffset = 0;
      }
      KeyGenerator gen{workload, universe, seedBase + t};
      auto* lat = recordLatency ? &(*perThreadLatencies)[t] : nullptr;
      for (uint64_t i = 0; i < opsPerThread; ++i) {
        const uint64_t keyIdx = keyOffset + gen.next();
        const uint64_t offset = keyIdx * kSegmentBytes;
        const auto t0 = std::chrono::steady_clock::now();
        auto segs = d.fsCache().getOrSet(
            kRemotePath, offset, kSegmentBytes, d.sleepyReadFile());
        const auto t1 = std::chrono::steady_clock::now();
        (void)segs;
        if (lat != nullptr) {
          lat->push_back(
              std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
                  .count());
        }
      }
    });
  }
  for (auto& th : workers) {
    th.join();
  }
}
```

Note for sequential mode: `workingSetKeys` must be divisible by `threads` for the slicing to cover the full universe without remainder. Phase-1 defaults satisfy this: `ws_mult=0.5` ⇒ 256 keys, `ws_mult=2.0` ⇒ 1024 keys, both divisible by all of `{1, 4, 16}`. If a future caller passes mismatched values, the last `slice` keys are unreachable but the bench does not crash — acceptable until a Phase-2 caller actually needs other thread counts, at which point this should become a `VELOX_USER_CHECK`.

- [ ] **Step 6.2: Add quantile helper** — insert above `parallelRun`:

```cpp
double quantileNs(std::vector<uint64_t>& v, double q) {
  if (v.empty()) {
    return 0.0;
  }
  const size_t idx = std::min<size_t>(
      v.size() - 1, static_cast<size_t>(q * v.size()));
  std::nth_element(v.begin(), v.begin() + idx, v.end());
  return static_cast<double>(v[idx]);
}
```

- [ ] **Step 6.3: Drive a multi-thread cell from `main()`** — append before `cleanupBenchTmp()`:

```cpp
  {
    FsCacheDriver d(/*wsKeys=*/256, /*latencyUs=*/0, /*cellIdx=*/0);
    std::vector<std::vector<uint64_t>> warmLat;
    parallelRun(
        d, Workload::kSequential, /*threads=*/4,
        /*opsPerThread=*/2500, /*recordLatency=*/false,
        /*seedBase=*/42, &warmLat);
    std::vector<std::vector<uint64_t>> mainLat;
    parallelRun(
        d, Workload::kSequential, /*threads=*/4,
        /*opsPerThread=*/12500, /*recordLatency=*/true,
        /*seedBase=*/43, &mainLat);
    std::vector<uint64_t> all;
    for (auto& v : mainLat) {
      all.insert(all.end(), v.begin(), v.end());
    }
    LOG(INFO) << "smoke ops=" << all.size()
              << " p50_us=" << (quantileNs(all, 0.50) / 1000.0)
              << " p95_us=" << (quantileNs(all, 0.95) / 1000.0)
              << " p99_us=" << (quantileNs(all, 0.99) / 1000.0)
              << " bytesOnDisk=" << d.fsCache().stats().bytesOnDisk;
  }
```

- [ ] **Step 6.4: Build and run**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 2>&1 | tail -3
```
Expected: log line `smoke ops=50000 p50_us=… p95_us=… p99_us=… bytesOnDisk=268435456`. bytesOnDisk again equals 256 MiB (working set fully cached). p99 should be larger than p50; both should be sub-millisecond on a warm cell with latency=0.

- [ ] **Step 6.5: Remove the smoke block** — delete the `{ FsCacheDriver d(...); … }` block. Rebuild clean.

- [ ] **Step 6.6: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): parallel run with per-thread latency + quantiles

Each worker pre-reserves an ops/threads-sized vector for ns latencies
(no malloc on hot path). After join, main thread concatenates and runs
nth_element for p50/p95/p99. Sequential mode partitions [0,
workingSetKeys) into disjoint per-thread slices by adding a per-thread
keyOffset to each KeyGenerator output; zipfian/uniform share the full
universe.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 7: Baseline snapshot + delta accounting + 13-column Markdown table

**Goal:** Real `runCell` returning the 9-metric `CellResult`, baseline pattern locked in, and the Markdown printer. After this commit, a single call to `runCell` produces one valid row of the final table.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 7.1: Add `CellResult` and a workload-name helper** — insert above `FsCacheDriver`:

```cpp
struct CellKey {
  Workload workload;
  uint64_t threads;
  double wsMult;
  uint64_t latencyUs;
};

struct CellResult {
  CellKey key;
  double opsPerSec{0};
  double hitRatePct{0};
  uint64_t bytesDlMB{0};
  uint64_t evicCount{0};
  uint64_t bytesEvicMB{0};
  double p50Us{0};
  double p95Us{0};
  double p99Us{0};
  double wallSec{0};
};

const char* workloadName(Workload w) {
  switch (w) {
    case Workload::kSequential: return "sequential";
    case Workload::kZipfian:    return "zipfian";
    case Workload::kUniform:    return "uniform";
  }
  return "?";
}
```

- [ ] **Step 7.2: Replace `runCellSkeleton` with the real `runCell`** — insert (deleting the skeleton from Task 5):

```cpp
CellResult runCell(
    const CellKey& key,
    uint64_t warmupOps,
    uint64_t ops,
    uint64_t seedBase,
    int cellIdx) {
  const uint64_t wsKeys = static_cast<uint64_t>(
      key.wsMult * static_cast<double>(kMaxCacheBytes) /
      static_cast<double>(kSegmentBytes));
  VELOX_USER_CHECK_GT(wsKeys, 0, "ws_mult too small for kMaxCacheBytes");
  VELOX_USER_CHECK_EQ(
      ops % key.threads,
      0,
      "ops {} must divide cleanly by threads {}",
      ops,
      key.threads);

  FsCacheDriver d(wsKeys, key.latencyUs, cellIdx);

  // Warmup. recordLatency=false; bytesRead_ reset below post-warmup.
  std::vector<std::vector<uint64_t>> dummyLat;
  parallelRun(
      d, key.workload, key.threads, warmupOps / key.threads,
      /*recordLatency=*/false, seedBase, &dummyLat);

  // Baseline snapshot AFTER warmup so deltas exclude warmup counters.
  const auto statsBase = d.fsCache().stats();
  d.sleepyReadFile().resetBytesRead();

  std::vector<std::vector<uint64_t>> mainLat;
  const auto wallStart = std::chrono::steady_clock::now();
  parallelRun(
      d, key.workload, key.threads, ops / key.threads,
      /*recordLatency=*/true, seedBase + key.threads, &mainLat);
  const double wallSec = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - wallStart)
                             .count();

  const auto statsFinal = d.fsCache().stats();
  const uint64_t hitsDelta = statsFinal.hits - statsBase.hits;
  const uint64_t evictionsDelta = statsFinal.evictions - statsBase.evictions;
  const uint64_t bytesReadDelta = d.sleepyReadFile().bytesRead();

  std::vector<uint64_t> all;
  size_t total = 0;
  for (auto& v : mainLat) {
    total += v.size();
  }
  all.reserve(total);
  for (auto& v : mainLat) {
    all.insert(all.end(), v.begin(), v.end());
  }

  CellResult r;
  r.key = key;
  r.opsPerSec = static_cast<double>(ops) / wallSec;
  r.hitRatePct = 100.0 * static_cast<double>(hitsDelta) /
      static_cast<double>(ops);
  r.bytesDlMB = bytesReadDelta / (1ULL << 20);
  // stats_.evictions is a COUNT of segments evicted, not bytes. Multiply
  // before divide so a (future) sub-MiB maxSegmentSize would not silently
  // truncate to 0.
  const auto& cfg = d.fsCache().config();
  r.evicCount = evictionsDelta;
  r.bytesEvicMB = (evictionsDelta * cfg.maxSegmentSize) / (1ULL << 20);
  r.p50Us = quantileNs(all, 0.50) / 1000.0;
  r.p95Us = quantileNs(all, 0.95) / 1000.0;
  r.p99Us = quantileNs(all, 0.99) / 1000.0;
  r.wallSec = wallSec;
  return r;
}
```

- [ ] **Step 7.3: Add the Markdown printer**

```cpp
void printMarkdownTable(
    std::ostream& os,
    const std::vector<CellResult>& rows) {
  os << "| workload   | threads | ws_mult | lat_us |     ops/s |  hit% |"
     << " dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us |"
     << " wallSec |\n"
     << "|------------|--------:|--------:|-------:|----------:|------:|"
     << "------:|-----------:|--------:|-------:|-------:|-------:|"
     << "--------:|\n";
  for (const auto& r : rows) {
    os << folly::sformat(
        "| {:<10} | {:>7} | {:>7.2f} | {:>6} | {:>9.0f} | {:>4.1f}% |"
        " {:>5} | {:>10} | {:>7} | {:>6.1f} | {:>6.1f} | {:>6.1f} |"
        " {:>7.3f} |\n",
        workloadName(r.key.workload),
        r.key.threads,
        r.key.wsMult,
        r.key.latencyUs,
        r.opsPerSec,
        r.hitRatePct,
        r.bytesDlMB,
        r.evicCount,
        r.bytesEvicMB,
        r.p50Us,
        r.p95Us,
        r.p99Us,
        r.wallSec);
  }
}
```

Add `#include <folly/Format.h>` and `#include <ostream>` to the top of the file.

- [ ] **Step 7.4: Drive one real cell from `main()`** — append before `cleanupBenchTmp()`:

```cpp
  CellKey k{Workload::kSequential, /*threads=*/4, /*wsMult=*/0.5,
            /*latencyUs=*/0};
  auto r = runCell(k, /*warmupOps=*/4000, /*ops=*/20000,
                   /*seedBase=*/42, /*cellIdx=*/0);
  printMarkdownTable(std::cout, {r});
```

- [ ] **Step 7.5: Build and run**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 2>&1 | tail -10
```
Expected: stderr has the setup logs; stdout has the 2-row Markdown table (header + one data row). For this cell (sequential, threads=4, ws_mult=0.5, latency=0):
  - `wsKeys = 0.5 * 512 = 256`. With 4 threads partitioned, each owns 64 keys → warmup of 1000 ops/thread walks each thread's slice 15×, so by warmup end every key is downloaded.
  - hit% ≈ 100%, dl_MB == 0 (post-warmup baseline reset means main loop is all hits), evic_count == 0 (working set fits).

- [ ] **Step 7.6: Remove the smoke block** — delete the `CellKey k…` block. Rebuild clean.

- [ ] **Step 7.7: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): runCell with baseline-delta accounting + table

CellResult holds the 9 metric columns + 4 dimension columns. runCell
snapshots stats() after warmup and resets SleepyReadFile bytesRead_
so deltas exclude warmup (would otherwise produce hit% > 100%).
evic_MB multiplies before dividing so future sub-MiB segments would
not silently truncate. Markdown printer renders a 13-column table.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 8: Flag parsing + Cartesian sweep + `--out` redirect + startup validation

**Goal:** Wire the four CSV flags into the Cartesian product loop, redirect the table to `--out` if set, validate `ops % threads == 0` for every threads value up-front.

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`

- [ ] **Step 8.1: Add the sweep flags** — insert with the other `DEFINE_*` lines from Task 4:

```cpp
DEFINE_uint64(ops, 200000, "Measured ops per cell. Must divide by every "
              "value in --threads_list cleanly.");
DEFINE_uint64(warmup_ops, 20000, "Warmup ops per cell (not counted).");
DEFINE_string(workloads, "sequential,zipfian,uniform",
              "Comma-separated subset of {sequential,zipfian,uniform}.");
DEFINE_string(threads_list, "1,4,16",
              "Comma-separated thread counts to sweep.");
DEFINE_string(ws_mult_list, "0.5,2.0",
              "Comma-separated working-set / maxBytes ratios.");
DEFINE_string(remote_latency_us_list, "0,200",
              "Comma-separated SleepyReadFile sleep durations (us).");
DEFINE_string(out, "",
              "If non-empty, write the Markdown table to this path instead "
              "of stdout. glog still goes to stderr.");
DEFINE_uint64(seed_base, 42, "Base seed; per-thread seed = seed_base + tid.");
```

- [ ] **Step 8.2: Add CSV parsers** — insert in the anonymous namespace after the cleanup helpers:

```cpp
template <typename T>
std::vector<T> parseCsv(const std::string& csv, T (*parse)(const std::string&)) {
  std::vector<std::string> toks;
  folly::split(',', csv, toks);
  std::vector<T> out;
  for (auto& t : toks) {
    auto s = folly::trimWhitespace(t).str();
    if (!s.empty()) {
      out.push_back(parse(s));
    }
  }
  return out;
}

Workload parseWorkload(const std::string& s) {
  if (s == "sequential") return Workload::kSequential;
  if (s == "zipfian") return Workload::kZipfian;
  if (s == "uniform") return Workload::kUniform;
  VELOX_USER_FAIL("Unknown workload: {}", s);
}

uint64_t parseU64(const std::string& s) {
  return std::stoull(s);
}

double parseDouble(const std::string& s) {
  return std::stod(s);
}
```

Add `#include <folly/String.h>` to the includes.

- [ ] **Step 8.3: Replace `main()` body** — replace the `int main(...)` block in its entirety with:

```cpp
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();

  signal(SIGINT, onSigint);
  std::filesystem::create_directories(benchTmpRoot());
  ensureRemoteFile();

  const auto workloads = parseCsv<Workload>(FLAGS_workloads, parseWorkload);
  const auto threadsList = parseCsv<uint64_t>(FLAGS_threads_list, parseU64);
  const auto wsMultList = parseCsv<double>(FLAGS_ws_mult_list, parseDouble);
  const auto latencyList =
      parseCsv<uint64_t>(FLAGS_remote_latency_us_list, parseU64);
  VELOX_USER_CHECK(!workloads.empty(), "--workloads is empty");
  VELOX_USER_CHECK(!threadsList.empty(), "--threads_list is empty");
  VELOX_USER_CHECK(!wsMultList.empty(), "--ws_mult_list is empty");
  VELOX_USER_CHECK(!latencyList.empty(), "--remote_latency_us_list is empty");
  for (auto t : threadsList) {
    VELOX_USER_CHECK_GT(t, 0, "threads must be > 0");
    VELOX_USER_CHECK_EQ(
        FLAGS_ops % t,
        0,
        "ops {} must divide cleanly by threads {}",
        FLAGS_ops,
        t);
    VELOX_USER_CHECK_EQ(
        FLAGS_warmup_ops % t,
        0,
        "warmup_ops {} must divide cleanly by threads {}",
        FLAGS_warmup_ops,
        t);
  }

  std::vector<CellResult> rows;
  int cellIdx = 0;
  for (auto w : workloads) {
    for (auto th : threadsList) {
      for (auto mult : wsMultList) {
        for (auto lat : latencyList) {
          CellKey k{w, th, mult, lat};
          LOG(INFO) << "cell " << cellIdx << " workload=" << workloadName(w)
                    << " threads=" << th << " ws_mult=" << mult
                    << " lat_us=" << lat;
          rows.push_back(runCell(
              k, FLAGS_warmup_ops, FLAGS_ops, FLAGS_seed_base, cellIdx));
          ++cellIdx;
        }
      }
    }
  }

  if (FLAGS_out.empty()) {
    printMarkdownTable(std::cout, rows);
  } else {
    std::ofstream out{FLAGS_out};
    VELOX_USER_CHECK(out.good(), "Failed to open --out path: {}", FLAGS_out);
    printMarkdownTable(out, rows);
    LOG(INFO) << "Wrote " << rows.size() << " rows to " << FLAGS_out;
  }

  cleanupBenchTmp();
  return 0;
}
```

- [ ] **Step 8.4: Build and run a 1-cell smoke**

```bash
cmake --build cmake-build-bench-rwdi --target velox_fscache_benchmark
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 \
  --workloads=sequential --threads_list=4 \
  --ws_mult_list=0.5 --remote_latency_us_list=0 \
  --ops=20000 --warmup_ops=4000 \
  2>&1 | tail -5
```
Expected: stderr `cell 0 …` log line; stdout 2-row Markdown table.

- [ ] **Step 8.5: Run a 4-cell smoke and verify table shape**

```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 \
  --workloads=sequential,zipfian --threads_list=1,4 \
  --ws_mult_list=0.5 --remote_latency_us_list=0 \
  --ops=20000 --warmup_ops=4000
```
Expected: 4 data rows. Sanity: zipfian with ws_mult=0.5 should have hit% near 100% (warmup downloads everything); sequential with threads=1 ws_mult=0.5 should also be ~100% hit. dl_MB should be 0 for both (post-warmup, all hits, no remote IO).

- [ ] **Step 8.6: Verify `--out` redirect**

```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 \
  --workloads=sequential --threads_list=1 \
  --ws_mult_list=0.5 --remote_latency_us_list=0 \
  --ops=10000 --warmup_ops=2000 \
  --out=/tmp/_fscache_smoke.md
wc -l /tmp/_fscache_smoke.md
head -3 /tmp/_fscache_smoke.md
```
Expected: file has 3 lines (header, separator, 1 data row); stdout has no table; stderr has the cell log line and `Wrote 1 rows to /tmp/_fscache_smoke.md`.

- [ ] **Step 8.7: Verify startup validation**

```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --remote_file_size_gb=1 \
  --workloads=sequential --threads_list=3 \
  --ws_mult_list=0.5 --remote_latency_us_list=0 \
  --ops=20000 --warmup_ops=4000 2>&1 | tail -3
echo "exit=$?"
```
Expected: aborts with `ops 20000 must divide cleanly by threads 3`, non-zero exit. (20000 % 3 = 2.)

- [ ] **Step 8.8: Commit**

```bash
git add velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp
git commit -m "bench(fscache): flag parsing, 36-cell sweep, --out redirect

Cartesian product of four CSV flags (workloads × threads_list ×
ws_mult_list × remote_latency_us_list) defaults to 3 × 3 × 2 × 2
= 36 cells matching the design doc. Up-front validation aborts
with a clear message if ops or warmup_ops does not divide cleanly
by every value in --threads_list, so silent op-dropping cannot
corrupt the Phase-1 ↔ Phase-2 A/B comparison. --out redirects the
Markdown table to a file while glog still goes to stderr.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Task 9: Phase-1 baseline run + commit results

**Goal:** Run the canonical Phase-1 invocation from the spec, capture the table, commit it as `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` for Phase-2 A/B.

**Files:**
- Create: `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`

- [ ] **Step 9.1: Confirm the results directory exists**

```bash
mkdir -p docs/superpowers/results
ls docs/superpowers/results/
```
Expected: directory exists (may already have other reports — leave them alone).

- [ ] **Step 9.2: Run the canonical sweep**

```bash
./cmake-build-bench-rwdi/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --ops=200000 --warmup_ops=20000 \
  --workloads=sequential,zipfian,uniform \
  --threads_list=1,4,16 \
  --ws_mult_list=0.5,2.0 \
  --remote_latency_us_list=0,200 \
  --out=docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
```
Expected: completes in ~6 minutes (per the design doc estimate; latency=200 cells dominate). 36 cells logged to stderr, 38-line Markdown file written.

- [ ] **Step 9.3: Spot-check the table**

```bash
wc -l docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
head -3 docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
tail -5 docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
```
Expected: 38 lines total (2 header + 36 data). Header columns match `| workload | threads | ws_mult | lat_us | ops/s | hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |`. Spot checks (from spec rationale):
  - `lat_us=0, ws_mult=0.5` cells: hit% ≈ 100%, dl_MB ≈ 0, evic_count = 0.
  - `lat_us=0, ws_mult=2.0` cells: hit% well below 100% (working set 2× cache), evic_count > 0, dl_MB > 0.
  - `lat_us=200, ws_mult=0.5` cells: ops/s far lower than the lat_us=0 sibling (sleep dominates).
  - `lat_us=200, ws_mult=2.0` cells: highest p99_us (eviction + sleep stack up).

If any spot check is way off (e.g. ws_mult=0.5 cell has dl_MB > 0), investigate before committing — the table is what Phase-2 will diff against.

- [ ] **Step 9.4: Prepend a short header to the results file** — open the file and prepend (above the existing Markdown table):

```markdown
# FsCache Phase-1 Baseline — 2026-05-23

Captured with `velox_fscache_benchmark` per the canonical invocation in
`docs/superpowers/specs/2026-05-23-fscache-microbench-design.md` § "Phase 1
baseline run". This file is the A-side input for the Phase-2 A/B comparison.

Invocation:

```bash
./velox_fscache_benchmark \
  --ops=200000 --warmup_ops=20000 \
  --workloads=sequential,zipfian,uniform \
  --threads_list=1,4,16 \
  --ws_mult_list=0.5,2.0 \
  --remote_latency_us_list=0,200 \
  --out=docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
```

Host: <hostname>, kernel <uname -r>, CPU <lscpu | grep 'Model name' | sed 's/  */ /g'>, build RelWithDebInfo.

---

```

Fill in `<hostname>`, kernel, and CPU from the actual machine the bench ran on (use `hostname`, `uname -r`, `lscpu | grep 'Model name'`).

- [ ] **Step 9.5: Commit**

```bash
git add docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
git commit -m "bench(fscache): Phase-1 baseline results

Captured 36-cell sweep with the canonical invocation from the
design doc. Serves as the A-side for the Phase-2 in-flight
reservation A/B comparison.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>"
```

---

## Done

After Task 9, the deliverable is:
- `velox_fscache_benchmark` binary (gated, no impact on default builds)
- 5 KeyGenerator unit tests covering the three workloads
- A committed baseline results file for Phase-2 to diff against

The next phase (Phase 2 — unified in-flight reservation accounting) re-uses this exact binary with the same flag set, saves the output to a sibling `docs/superpowers/results/<date>-fscache-phase2-with-inflight-reservation.md`, and a two-table diff visualises the expected p99 / `bytesEvic_MB` improvement in the eviction-pressure cells.
