# FsCache CH-Aligned Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `velox::cache::fs::FsCache` into full ClickHouse `FileCache` alignment — 6-state `FileSegment` with partial-readable semantics, `lookupRange + fillHoles` metadata, `FileSegmentsHolderPtr` return type with caller-driven download advancement, per-bucket / per-key locks with POD `FsCacheStats` (4 hit/miss fields + internal `AtomicCounters`), `DownloadThreadPool` for async load, SLRU + `FileCacheQueryLimit` + bypass.

**Architecture:** 16 tasks following spec §11. Phase-0 independent foundations (Task 1 / 6 / 13) can start in parallel; Task 2-5 / 7-12 form the build-out chain; Task 8 is the hard-cut commit that flips `getOrSet` signature and migrates every phase-1 test in a single commit; Task 14-16 close out stats wiring + end-to-end verification + perf gate.

**Tech Stack:** C++20, gtest, GCC 13 (RelWithDebInfo build at `cmake-build-relwithdebinfo-gcc13/`), folly `IOThreadPoolExecutor`, `std::filesystem`, `pwrite/ftruncate/sync_file_range`.

**Spec:** `docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md`

**5-phase rhythm per task** (from `~/.claude/plans/optimized-foraging-pearl.md`, locked in):
1. 实施 (TDD: red → green per step)
2. 正确性 review (spawn `pr-review-toolkit:code-reviewer`, scope=task unstaged diff, max 3 retry on CRITICAL/HIGH)
3. 简化 (spawn `code-simplifier`, scope=task unstaged diff)
4. 简化后 review (same reviewer, same scope, same retry rule)
5. 提交 (specific `git add <files>`; never `--amend` / `--no-verify` / `--no-gpg-sign` / `git add .` / `git add -A`)

**Per-task checkbox convention:** the steps enumerated inside each task cover **Phase 1 only** (the TDD red→green cycle) plus the final commit. Phases 2/3/4 are **mandatory but implicit** — subagent inserts them automatically between the last green-test step and the `git commit` step. Do not skip them and do not wait for user prompting; the rules in the block above are the contract. Deferred MEDIUMs from Phase 2 and Phase 4 are appended to the commit message body under a `Deferred (MEDIUM, follow-up):` heading (omit the heading if empty).

**Hard exclusions** (enforce at every `git add` step):
- **Never commit** `velox/common/caching/benchmarks/CacheBackendBenchmark.cpp`
- Never `git commit --amend` / `--no-verify` / `--no-gpg-sign`

---

## Task Dependency Graph (spec §11 verbatim)

```
1 ─┬─> 2 ─┬─> 3 ─┐
   │     │      │
   │     ├─> 4  │
   │     │      │
   │     └─> 5 ─┐  (Task 5 holder dtor depends on Task 2 getDownloader())
   └────────┘  │
6 ─> 7 ────────┼─> 8 ─┬─> 9 ──┬─> 11 ─> 12 ─┐
                │      │       │             │
                │      └─> 10 ─┘             │
                │                            │
                └─> 14 <─── 9, 10            │
                                             │
13 ──────────────────────────────────────────┴─> 15 ─> 16
```

**Parallelizable:** 1 / 6 / 13 launch together; 6+7 alongside 1-5; 11 / 14 / 13 converge into 15.

---

## Task 1: FileSegment::State 扩 6 态 + 转移测试 (dead code)

**Files:**
- Modify: `velox/common/caching/fscache/FileSegment.h` (state enum + accessor)
- Modify: `velox/common/caching/fscache/FileSegment.cpp` (toString / debug helper if exists)
- Create: `velox/common/caching/fscache/tests/FileSegmentStateTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt` (register new test)

**Spec refs:** §5.4 (state diagram + transition table), §10 R6 (dead-code 风险)

**Approach:** Add the 2 new enum values (`kPartiallyDownloaded` / `kPartiallyDownloadedNoContinuation`) to `FileSegment::State`. Do NOT wire any transition into them yet — Task 2-3 do that. Write a transition table test that only exercises the legal CAS edges defined in spec §5.4. Illegal edges are left as `EXPECT_DEATH` / `EXPECT_THROW` shells that will be filled in once `reserve()/write()/complete()` exist in Task 2.

- [ ] **Step 1: Write failing test for 6-state enum membership**

Create `velox/common/caching/fscache/tests/FileSegmentStateTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

// Verifies the 6-state enum is wired exactly per spec §5.4.
TEST(FileSegmentStateTest, sixStatesExist) {
  EXPECT_EQ(static_cast<int>(FileSegment::State::kEmpty), 0);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDownloading), 1);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDownloaded), 2);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kPartiallyDownloaded), 3);
  EXPECT_EQ(
      static_cast<int>(FileSegment::State::kPartiallyDownloadedNoContinuation),
      4);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDetached), 5);
}

} // namespace facebook::velox::cache::fs::test
```

Register in `velox/common/caching/fscache/tests/CMakeLists.txt` (follow existing pattern for `FileSegmentTest`).

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentStateTest.sixStatesExist'
```

Expected: build error `kPartiallyDownloaded is not a member of FileSegment::State` (Task 1 hasn't extended the enum yet).

- [ ] **Step 3: Extend the enum in FileSegment.h**

Find the existing enum (phase-1 has 4 values: `kEmpty / kDownloading / kDownloaded / kDetached`). Replace with:

```cpp
enum class State : uint8_t {
  kEmpty = 0,                              // metadata exists, no writer yet
  kDownloading = 1,                        // single writer active, downloadedSize_ advancing
  kDownloaded = 2,                         // entire segment on disk
  kPartiallyDownloaded = 3,                // writer abandoned, downloadedSize_ > 0
  kPartiallyDownloadedNoContinuation = 4,  // partial + metadata refused resume (phase-3 only)
  kDetached = 5,                           // metadata removed; segment kept alive by readers
};
```

If `FileSegment.cpp` has a `toString(State)` helper, extend it; otherwise skip.

- [ ] **Step 4: Run test to verify it passes**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentStateTest.*'
```

Expected: `[  PASSED  ] 1 test`.

- [ ] **Step 5: Add transition table guard test (legal edges only)**

Append to `FileSegmentStateTest.cpp`:

```cpp
// Documents the legal transition edges per spec §5.4. Task 2-3 will fill in
// the actual reserve()/write()/complete() implementations; until then this
// test only locks in the *enumeration* of legal edges so Task 2 can't
// silently widen the contract.
TEST(FileSegmentStateTest, legalTransitionEdgesEnumerated) {
  using S = FileSegment::State;
  struct Edge {
    S from;
    S to;
  };
  const std::vector<Edge> legal = {
      {S::kEmpty, S::kDownloading},
      {S::kDownloading, S::kDownloaded},
      {S::kDownloading, S::kPartiallyDownloaded},
      {S::kPartiallyDownloaded, S::kDownloading},
      {S::kPartiallyDownloaded, S::kPartiallyDownloadedNoContinuation},
      {S::kDownloaded, S::kDetached},
      {S::kDownloading, S::kDetached},
      {S::kPartiallyDownloaded, S::kDetached},
      {S::kPartiallyDownloadedNoContinuation, S::kDetached},
  };
  EXPECT_EQ(legal.size(), 9UL);
}
```

Run again, expect PASS.

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/common/caching/fscache/tests/FileSegmentStateTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): extend FileSegment::State to 6 CH-aligned states

Adds kPartiallyDownloaded and kPartiallyDownloadedNoContinuation per spec
§5.4. The new states are dead code at this commit -- Task 2-3 introduce
the reserve/write/complete API that drives transitions into them.
FileSegmentStateTest locks in the enumeration so a future widening can't
slip in unreviewed.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: FileSegment::reserve/write/complete/abandon + getDownloader

**Files:**
- Modify: `velox/common/caching/fscache/FileSegment.h` (new public API + downloader_ + fd_ + downloadedSize_)
- Modify: `velox/common/caching/fscache/FileSegment.cpp` (impl)
- Create: `velox/common/caching/fscache/tests/FileSegmentWriteTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec refs:** §4 (FileSegment API), §5.4 (kEmpty → kDownloading via reserve, kDownloading → kDownloaded via complete, kDownloading → kPartiallyDownloaded via abandon), §5.5 (writer protocol: open without ftruncate / complete ftruncate / abandon no ftruncate)

**Approach:** Replace phase-1 `beginDownload() / download()` with the three-stage `reserve() / write() / complete()` + `abandon()`. `reserve()` CASes `kEmpty → kDownloading` and atomically writes `downloader_ = this_thread::get_id()` + opens fd (`O_CREAT|O_WRONLY`, NO ftruncate). `write()` pwrites at `downloadedSize_`, accumulates, optional `sync_file_range`, notify cv. `complete()` ftruncates to declared size, CAS kDownloading→kDownloaded, closes fd. `abandon()` CASes kDownloading→kPartiallyDownloaded, closes fd (NO ftruncate — leaves stat_size < claimed_size so loadFromDisk can detect partial). `getDownloader()` returns current writer's thread id or default-constructed id.

This task **does not yet** add partial-readable cv-wait — that's Task 3. Reader-side `read()` and `waitForDownloadedSize()` still behave as in phase-1.

- [ ] **Step 1: Write failing test — reserve happy path**

Create `velox/common/caching/fscache/tests/FileSegmentWriteTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentWriteTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentWriteTest, reserveTransitionsEmptyToDownloading) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.state(), FileSegment::State::kEmpty);
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));
  EXPECT_EQ(seg.state(), FileSegment::State::kDownloading);
  EXPECT_EQ(seg.getDownloader(), std::this_thread::get_id());
}

TEST_F(FileSegmentWriteTest, writeThenCompleteProducesFullFile) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 8};
  FileSegment seg{key, "/remote/x"};
  ASSERT_TRUE(seg.reserve(8, cacheRoot_));
  const char payload[] = "ABCDEFGH";
  seg.write(payload, 8);
  seg.complete();
  EXPECT_EQ(seg.state(), FileSegment::State::kDownloaded);
  // File on disk matches payload exactly.
  std::ifstream in{seg.localPath(cacheRoot_), std::ios::binary};
  std::string disk((std::istreambuf_iterator<char>(in)), {});
  EXPECT_EQ(disk, std::string(payload, 8));
}

TEST_F(FileSegmentWriteTest, abandonLeavesPartialFileWithoutFtruncate) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));
  std::string chunk(256, 'A');
  seg.write(chunk.data(), chunk.size());
  seg.abandon();
  EXPECT_EQ(seg.state(), FileSegment::State::kPartiallyDownloaded);
  // stat_size must equal what was actually pwritten (256), NOT claimed 1024.
  // This is what lets loadFromDisk recognise & delete partials (spec §5.5).
  EXPECT_EQ(std::filesystem::file_size(seg.localPath(cacheRoot_)), 256UL);
}

TEST_F(FileSegmentWriteTest, reserveRejectsConcurrentWriter) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));
  // Second reserve from same thread returns false; segment already DOWNLOADING.
  EXPECT_FALSE(seg.reserve(1024, cacheRoot_));
}

TEST_F(FileSegmentWriteTest, writeBeyondReservedSizeThrows) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 8};
  FileSegment seg{key, "/remote/x"};
  ASSERT_TRUE(seg.reserve(8, cacheRoot_));
  std::string oversized(16, 'A');
  EXPECT_THROW(
      seg.write(oversized.data(), oversized.size()),
      ::facebook::velox::VeloxException);
}

} // namespace facebook::velox::cache::fs::test
```

Register in `tests/CMakeLists.txt`.

- [ ] **Step 2: Run, expect link / compile failure**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
```

Expected: `error: no member named 'reserve' in 'FileSegment'` etc.

- [ ] **Step 3: Add public API to FileSegment.h**

In the public section, add:

```cpp
/// Atomically transitions kEmpty -> kDownloading, opens the local cache
/// file (O_CREAT|O_WRONLY, NO ftruncate), and records the calling thread
/// as the writer. Returns false if the segment was not kEmpty (another
/// writer already won or segment already kDownloaded).
/// reservedBytes is the declared size; complete() will ftruncate to this.
bool reserve(uint64_t reservedBytes, const std::string& cacheRoot);

/// Appends `len` bytes at current downloadedSize_ via pwrite, advances
/// downloadedSize_ release-store, notify_all on cv_. Must be called by
/// the thread that won reserve(). Throws if downloadedSize_ + len would
/// exceed reservedBytes.
void write(const char* buf, uint64_t len);

/// CAS kDownloading -> kDownloaded, ftruncate(fd_, key().size),
/// fsync, close(fd_), notify_all. Must be called by the writer thread.
void complete();

/// CAS kDownloading -> kPartiallyDownloaded; close(fd_) without
/// ftruncate (partial stat_size lets loadFromDisk delete the file on
/// next restart per spec §5.5).
void abandon();

/// Returns the thread id of the current writer or a default-constructed
/// id if no writer is active. Used by FileSegmentsHolder dtor to detect
/// "this thread owns a leaked DOWNLOADING segment".
std::thread::id getDownloader() const noexcept;
```

Add private members:

```cpp
std::atomic<std::thread::id> downloader_{};
int fd_{-1};
std::atomic<uint64_t> downloadedSize_{0};
uint64_t reservedBytes_{0};
```

Keep the existing `mutex_` / `cv_` (Task 3 uses cv_ for partial-readable; this task's notify_all is in preparation but no reader waits on it yet).

- [ ] **Step 4: Implement in FileSegment.cpp**

Replace phase-1 `beginDownload()` / `download()` with:

```cpp
bool FileSegment::reserve(
    uint64_t reservedBytes,
    const std::string& cacheRoot) {
  auto expected = State::kEmpty;
  if (!state_.compare_exchange_strong(
          expected,
          State::kDownloading,
          std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return false;
  }
  downloader_.store(std::this_thread::get_id(), std::memory_order_release);
  reservedBytes_ = reservedBytes;
  const auto path = localPath(cacheRoot);
  std::filesystem::create_directories(
      std::filesystem::path{path}.parent_path());
  fd_ = ::open(path.c_str(), O_CREAT | O_WRONLY, 0644);
  VELOX_CHECK_GE(fd_, 0, "FileSegment::reserve open failed: {}", path);
  return true;
}

void FileSegment::write(const char* buf, uint64_t len) {
  const uint64_t before = downloadedSize_.load(std::memory_order_relaxed);
  VELOX_CHECK_LE(
      before + len,
      reservedBytes_,
      "FileSegment::write overruns reservedBytes_: before={} len={} reserved={}",
      before,
      len,
      reservedBytes_);
  const auto written = ::pwrite(fd_, buf, len, static_cast<off_t>(before));
  VELOX_CHECK_EQ(
      static_cast<uint64_t>(written),
      len,
      "FileSegment::write short pwrite, requested={}, got={}",
      len,
      written);
  downloadedSize_.store(before + len, std::memory_order_release);
  std::lock_guard<FileSegmentMutex> lk{mutex_};
  cv_.notify_all();
}

void FileSegment::complete() {
  VELOX_CHECK_GE(fd_, 0, "FileSegment::complete called without active fd");
  VELOX_CHECK_EQ(
      ::ftruncate(fd_, static_cast<off_t>(key().size)),
      0,
      "FileSegment::complete ftruncate failed");
  ::fsync(fd_);
  ::close(fd_);
  fd_ = -1;
  auto expected = State::kDownloading;
  VELOX_CHECK(
      state_.compare_exchange_strong(
          expected,
          State::kDownloaded,
          std::memory_order_acq_rel,
          std::memory_order_acquire),
      "FileSegment::complete invalid state transition from {}",
      static_cast<int>(expected));
  std::lock_guard<FileSegmentMutex> lk{mutex_};
  cv_.notify_all();
}

void FileSegment::abandon() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  auto expected = State::kDownloading;
  // Best-effort: writer may have already completed via a race; that's fine.
  state_.compare_exchange_strong(
      expected,
      State::kPartiallyDownloaded,
      std::memory_order_acq_rel,
      std::memory_order_acquire);
  std::lock_guard<FileSegmentMutex> lk{mutex_};
  cv_.notify_all();
}

std::thread::id FileSegment::getDownloader() const noexcept {
  return downloader_.load(std::memory_order_acquire);
}
```

Required includes: `<fcntl.h>`, `<unistd.h>`, `<sys/types.h>`, `<filesystem>`, `<thread>`.

`state_` must already be `std::atomic<State>`; if phase-1 had it under mutex_, convert to atomic in this commit.

- [ ] **Step 5: Run, all 5 cases pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentWriteTest.*'
```

Expected: `[  PASSED  ] 5 tests`.

- [ ] **Step 6: Confirm phase-1 FileSegmentTest still green**

Existing `FileSegmentTest` exercises `beginDownload/download`. Those phase-1 APIs are still required by `FsCache::lookupOrCreate`, so this task does NOT remove them — Task 8 hard-cuts both call sites in one commit. For Task 2 the old APIs remain, the new APIs are added alongside.

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentTest.*:FileSegmentStateTest.*:FileSegmentWriteTest.*'
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/common/caching/fscache/tests/FileSegmentWriteTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): add reserve/write/complete/abandon writer API to FileSegment

Introduces CH-aligned 3-stage writer protocol (spec §4 §5.5):
  - reserve(): CAS kEmpty -> kDownloading, open fd without ftruncate,
    record writer thread id in downloader_.
  - write(): pwrite at downloadedSize_, advance, notify cv_.
  - complete(): ftruncate to declared size, CAS kDownloading -> kDownloaded.
  - abandon(): close fd WITHOUT ftruncate, CAS to kPartiallyDownloaded so
    loadFromDisk can detect partial via stat_size < key().size.
  - getDownloader(): writer thread id, used by FileSegmentsHolder dtor.

Phase-1 beginDownload/download remain wired into FsCache::lookupOrCreate;
Task 8 cuts both call sites in a single commit.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4 §5.5

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Partial-readable cv + waitForDownloadedSize

**Files:**
- Modify: `velox/common/caching/fscache/FileSegment.h` (waitForDownloadedSize public API)
- Modify: `velox/common/caching/fscache/FileSegment.cpp`
- Create: `velox/common/caching/fscache/tests/FileSegmentPartialReadTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec refs:** §4 (waitForDownloadedSize signature), §4.2 (partial-readable timing diagram), §10 R2 (cv 风暴 mitigation: notifyBatchBytes)

**Approach:** Add `waitForDownloadedSize(needed)` that blocks on `cv_` until `downloadedSize_.load(acquire) >= needed` OR state transitions out of `kDownloading`. Throws `VeloxRuntimeError` if state ends in `kPartiallyDownloaded(NoContinuation)` and needed > downloadedSize_, so reader doesn't read garbage past the abandon point. `kDownloaded` is a clean wake.

Optional notifyBatchBytes optimisation (spec §10 R2) is deferred to a follow-up — this task uses per-write notify_all (already wired in Task 2).

- [ ] **Step 1: Write failing test — reader blocks until writer's notify reaches threshold**

Create `velox/common/caching/fscache/tests/FileSegmentPartialReadTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentPartialReadTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentPartialReadTest, readerWakesAfterEnoughBytesWritten) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(256);
    readerDone.store(true);
  }};

  // Reader cannot wake yet: downloadedSize_ == 0.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  // 64 bytes < needed 256 -> reader still blocks.
  std::string chunk(64, 'A');
  seg.write(chunk.data(), chunk.size());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  // Third write crosses the 256 threshold (64*4 = 256).
  seg.write(chunk.data(), chunk.size());
  seg.write(chunk.data(), chunk.size());
  seg.write(chunk.data(), chunk.size());
  reader.join();
  EXPECT_TRUE(readerDone.load());

  seg.complete();
}

TEST_F(FileSegmentPartialReadTest, readerWokenByCompleteWhenNeededEqualsSize) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 128};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(128, cacheRoot_));

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(128);
    readerDone.store(true);
  }};

  std::string payload(128, 'B');
  seg.write(payload.data(), payload.size());
  seg.complete();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

TEST_F(FileSegmentPartialReadTest, readerThrowsWhenWriterAbandonsShortOfNeeded) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::thread reader{[&]() {
    EXPECT_THROW(
        seg.waitForDownloadedSize(512),
        ::facebook::velox::VeloxException);
  }};

  std::string chunk(128, 'C');
  seg.write(chunk.data(), chunk.size());
  seg.abandon();  // downloadedSize_ == 128 < needed 512
  reader.join();
}

TEST_F(FileSegmentPartialReadTest, readerWokenByAbandonWhenDownloadedExceedsNeeded) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::string chunk(256, 'D');
  seg.write(chunk.data(), chunk.size());
  // Reader only needs 128 bytes -- writer abandoning is fine, those bytes
  // are durable on disk.
  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(128);
    readerDone.store(true);
  }};
  seg.abandon();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run, expect compile failure on waitForDownloadedSize**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
```

Expected: `error: no member named 'waitForDownloadedSize' in 'FileSegment'`.

- [ ] **Step 3: Add public API to FileSegment.h**

```cpp
/// Blocks the calling reader until downloadedSize_ >= needed OR the
/// segment transitions out of kDownloading. If the segment ends in
/// kPartiallyDownloaded / kPartiallyDownloadedNoContinuation AND
/// downloadedSize_ < needed, throws VeloxRuntimeError so the reader
/// surfaces the writer's abandon instead of reading past the partial
/// boundary. kDownloaded is always a clean wake.
void waitForDownloadedSize(uint64_t needed);

/// Snapshot of bytes durably visible to readers.
uint64_t downloadedSize() const noexcept {
  return downloadedSize_.load(std::memory_order_acquire);
}
```

- [ ] **Step 4: Implement in FileSegment.cpp**

```cpp
void FileSegment::waitForDownloadedSize(uint64_t needed) {
  std::unique_lock<FileSegmentMutex> lk{mutex_};
  cv_.wait(lk, [&]() {
    return downloadedSize_.load(std::memory_order_acquire) >= needed ||
        state_.load(std::memory_order_acquire) != State::kDownloading;
  });
  if (downloadedSize_.load(std::memory_order_acquire) >= needed) {
    return;
  }
  // State exited kDownloading short of needed: writer abandoned. Reader
  // must surface the failure rather than reading garbage past the
  // partial boundary.
  const auto finalState = state_.load(std::memory_order_acquire);
  VELOX_FAIL(
      "FileSegment writer abandoned with downloadedSize={} < needed={}, "
      "finalState={}",
      downloadedSize_.load(std::memory_order_acquire),
      needed,
      static_cast<int>(finalState));
}
```

- [ ] **Step 5: Run, all 4 cases pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentPartialReadTest.*'
```

Expected: `[  PASSED  ] 4 tests`.

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/common/caching/fscache/tests/FileSegmentPartialReadTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): add partial-readable waitForDownloadedSize to FileSegment

Reader-side cv_ wait that wakes on downloadedSize_ >= needed OR state
exits kDownloading. Throws when writer abandons short of the reader's
threshold so callers don't silently read past the partial boundary
(spec §4.2 partial-readable timing diagram).

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4 §10 R2

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: On-disk partial recovery (loadFromDisk)

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.cpp` (loadFromDisk: stat_size < parsed.size → delete)
- Create: `velox/common/caching/fscache/tests/FsCachePartialRecoveryTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec refs:** §5.5 (writer protocol: complete ftruncate to N; abandon NO ftruncate → stat_size < claimed → loadFromDisk deletes)

**Approach:** Phase-1 `FsCache::loadFromDisk()` (in `velox/common/caching/fscache/FsCache.cpp` around lines 575-577 — verify the line number against the current file before editing) already treats `file_size != parsed.size` as victim. **This task does not rewrite `loadFromDisk`** — it (a) verifies the existing branch correctly handles the new in-place writer protocol introduced by Task 2 (no `.tmp` suffix anymore, partial = same filename but short stat_size), (b) adds a regression test that simulates writer abandon mid-write, restarts FsCache, and asserts the file was deleted, and (c) deletes the now-dead `.tmp`-removal branch if present (small follow-up Edit, not a new function).

If, when reading the current `loadFromDisk`, you find the size-mismatch branch is **missing** (someone removed it), file that as a Phase-2 CRITICAL during review and put it back in the same commit — don't redesign the function shape.

Phase-1 also removes `.tmp` files — that branch becomes dead code under the new protocol but harmless. Leave it in place unless the simplifier in Phase 3 flags it; no functional change required, only the test.

- [ ] **Step 1: Write failing test**

Create `velox/common/caching/fscache/tests/FsCachePartialRecoveryTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

TEST(FsCachePartialRecoveryTest, abandonedPartialDeletedOnRestart) {
  auto tempDir = TempDirectoryPath::create();
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir->getPath() + "/cache";
  cfg.maxBytes = 64UL * 1024 * 1024;
  std::filesystem::create_directories(cfg.cacheRoot);

  // Round 1: writer claims 1 MiB, writes 256 KiB, then abandons.
  std::filesystem::path partialPath;
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    FsCacheKey key{PathKey::fromPath("/remote/abandoned"), 0, 1UL << 20};
    FileSegment seg{key, "/remote/abandoned"};
    ASSERT_TRUE(seg.reserve(1UL << 20, cfg.cacheRoot));
    std::string chunk(256UL << 10, 'X');
    seg.write(chunk.data(), chunk.size());
    seg.abandon();
    partialPath = seg.localPath(cfg.cacheRoot);
    ASSERT_TRUE(std::filesystem::exists(partialPath));
    // Sanity: stat_size matches what was pwritten, NOT claimed.
    ASSERT_EQ(std::filesystem::file_size(partialPath), 256UL << 10);
  }

  // Round 2: fresh FsCache reloads; partial must be deleted because
  // stat_size (256 KiB) != parsed key size (1 MiB).
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    EXPECT_FALSE(std::filesystem::exists(partialPath));
  }
}

TEST(FsCachePartialRecoveryTest, completedSegmentSurvivesRestart) {
  auto tempDir = TempDirectoryPath::create();
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir->getPath() + "/cache";
  cfg.maxBytes = 64UL * 1024 * 1024;
  std::filesystem::create_directories(cfg.cacheRoot);

  std::filesystem::path completedPath;
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    FsCacheKey key{PathKey::fromPath("/remote/full"), 0, 4096};
    FileSegment seg{key, "/remote/full"};
    ASSERT_TRUE(seg.reserve(4096, cfg.cacheRoot));
    std::string payload(4096, 'Y');
    seg.write(payload.data(), payload.size());
    seg.complete();
    completedPath = seg.localPath(cfg.cacheRoot);
    ASSERT_EQ(std::filesystem::file_size(completedPath), 4096UL);
  }

  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    EXPECT_TRUE(std::filesystem::exists(completedPath));
    EXPECT_EQ(std::filesystem::file_size(completedPath), 4096UL);
  }
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run, expect PASS already (phase-1 stat_size branch handles it)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FsCachePartialRecoveryTest.*'
```

Expected: `[  PASSED  ] 2 tests`. If the abandoned-partial case fails (file survives), the phase-1 `entry.file_size() != parsed->size` branch in `FsCache.cpp:575-577` isn't running. Add LOG(INFO) probes in `loadFromDisk` to confirm the file is being parsed and the size comparison is correct.

- [ ] **Step 3: If tests pass without code change, commit just the tests**

If tests pass on step 2, no `FsCache.cpp` change is needed and this commit is test-only:

```bash
git add velox/common/caching/fscache/tests/FsCachePartialRecoveryTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
test(fscache): cover partial recovery under in-place writer protocol

After Task 2's switch from .tmp+rename to in-place pwrite, an abandoned
writer leaves a file whose stat_size < parsed key size. Phase-1
loadFromDisk already deletes that mismatch (FsCache.cpp:575); this test
locks the behaviour under the new protocol so a future refactor can't
silently regress recovery.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §5.5

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

If tests fail, fix `loadFromDisk` first, include the prod-code diff in the same commit.

---

## Task 5: FileSegmentsHolder + dtor behaviour

**Files:**
- Create: `velox/common/caching/fscache/FileSegmentsHolder.h`
- Create: `velox/common/caching/fscache/tests/FileSegmentsHolderTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt` (header-only; if needed for IDE)
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec refs:** §4 (FileSegmentsHolder interface), §5.6 (dtor protocol), §11 Task 5 (deps 1, 2)

**Approach:** Header-only RAII wrapper around `std::vector<FileSegmentPtr>`. Dtor walks segments; for any segment in `kDownloading` whose `getDownloader() == std::this_thread::get_id()`, it calls `abandon()` — protects against the controller throwing mid-loop in `FsCacheBufferedInput::load`. Other segments are left alone (other threads may still be downloading them; that's fine, holder destruction just drops the reference). Non-copyable, movable.

- [ ] **Step 1: Write failing test**

Create `velox/common/caching/fscache/tests/FileSegmentsHolderTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegmentsHolder.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentsHolderTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentsHolderTest, emptyHolderDestructsCleanly) {
  FileSegmentsHolder holder{{}};
  EXPECT_TRUE(holder.empty());
  EXPECT_TRUE(holder.segments().empty());
}

TEST_F(FileSegmentsHolderTest, emptyReturnsFalseWhenSegmentsHeld) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();
  FileSegmentsHolder holder{{seg}};
  EXPECT_FALSE(holder.empty());
}

TEST_F(FileSegmentsHolderTest, kDownloadedSegmentSurvivesDestructor) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloaded);
  {
    FileSegmentsHolder holder{{seg}};
  }
  // Destructor is a no-op for kDownloaded segments.
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
}

TEST_F(FileSegmentsHolderTest, holderAbandonsDownloadingSegmentOwnedByThisThread) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(1024, cacheRoot_));
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloading);
  ASSERT_EQ(seg->getDownloader(), std::this_thread::get_id());
  {
    FileSegmentsHolder holder{{seg}};
  }
  EXPECT_EQ(seg->state(), FileSegment::State::kPartiallyDownloaded);
}

TEST_F(FileSegmentsHolderTest, holderDoesNotAbandonSegmentOwnedByOtherThread) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");

  std::thread other{[&]() {
    ASSERT_TRUE(seg->reserve(1024, cacheRoot_));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    seg->complete();
  }};
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  // Segment is kDownloading, but downloader_ is `other` thread.
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloading);
  ASSERT_NE(seg->getDownloader(), std::this_thread::get_id());

  {
    FileSegmentsHolder holder{{seg}};
  }
  // We did NOT touch state because downloader is not this thread.
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloading);

  // Note: writing 0 bytes is fine because complete() ftruncates to declared
  // size; the segment ends at kDownloaded with all-zero contents.
  other.join();
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
}

TEST_F(FileSegmentsHolderTest, moveSemantics) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();

  FileSegmentsHolder a{{seg}};
  FileSegmentsHolder b{std::move(a)};
  EXPECT_EQ(b.segments().size(), 1UL);
  EXPECT_TRUE(a.segments().empty());
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run, expect compile failure (header not yet exists)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
```

Expected: `fatal error: 'velox/common/caching/fscache/FileSegmentsHolder.h' file not found`.

- [ ] **Step 3: Create FileSegmentsHolder.h**

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#pragma once

#include "velox/common/caching/fscache/FileSegment.h"

#include <memory>
#include <thread>
#include <vector>

namespace facebook::velox::cache::fs {

/// RAII owner for the segment list returned by FsCache::getOrSet. Callers
/// drive each segment from kEmpty -> kDownloaded; if the caller throws
/// mid-loop, the holder dtor invokes abandon() on any segment whose
/// writer is the current thread so other waiters wake instead of hanging.
class FileSegmentsHolder {
 public:
  explicit FileSegmentsHolder(std::vector<FileSegmentPtr> segments)
      : segments_{std::move(segments)} {}

  ~FileSegmentsHolder() {
    const auto self = std::this_thread::get_id();
    for (auto& seg : segments_) {
      if (seg == nullptr) {
        continue;
      }
      if (seg->state() == FileSegment::State::kDownloading &&
          seg->getDownloader() == self) {
        // Dtor must not propagate: throwing out of a dtor while another
        // dtor frame is already unwinding calls std::terminate. abandon()
        // is normally noexcept (it only flips state + drops the writer
        // slot), but log-and-continue here so a bug in a future version
        // can't crash the process — silently swallowing without a log
        // would be the actual silent-failure smell. Per-segment try/catch
        // so one bad segment doesn't prevent the rest from being
        // abandoned.
        try {
          seg->abandon();
        } catch (const std::exception& e) {
          LOG(ERROR) << "FileSegmentsHolder dtor: abandon() threw: "
                     << e.what();
        } catch (...) {
          LOG(ERROR) << "FileSegmentsHolder dtor: abandon() threw "
                        "non-std exception";
        }
      }
    }
  }

  std::vector<FileSegmentPtr>& segments() {
    return segments_;
  }

  const std::vector<FileSegmentPtr>& segments() const {
    return segments_;
  }

  /// True if no segments are held. Returned by `FsCache::getOrSet` when
  /// the request bypasses the cache (see spec §8.3 and Task 12
  /// shouldBypass) — callers must fall back to reading directly from
  /// the remote in that case.
  bool empty() const {
    return segments_.empty();
  }

  FileSegmentsHolder(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder& operator=(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder(FileSegmentsHolder&&) noexcept = default;
  FileSegmentsHolder& operator=(FileSegmentsHolder&&) noexcept = default;

 private:
  std::vector<FileSegmentPtr> segments_;
};

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Run, all 5 cases pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FileSegmentsHolderTest.*'
```

Expected: `[  PASSED  ] 5 tests`.

- [ ] **Step 5: Commit**

```bash
git add velox/common/caching/fscache/FileSegmentsHolder.h \
        velox/common/caching/fscache/tests/FileSegmentsHolderTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): add FileSegmentsHolder RAII wrapper

Owns the segment vector returned by FsCache::getOrSet (Task 8) and on
destruction calls abandon() on any segment whose writer is the current
thread. Protects against caller throwing mid-loop in
FsCacheBufferedInput::load (Task 9) and stranding other readers in
cv_.wait. Segments owned by other writer threads are untouched.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4 §5.6

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: FsCacheMetadata::lookupRange + KeyNotFoundPolicy

**Files:**
- Modify: `velox/common/caching/fscache/FsCacheMetadata.h` (+ lookupRange, KeyNotFoundPolicy enum)
- Modify: `velox/common/caching/fscache/FsCacheMetadata.cpp`
- Modify: `velox/common/caching/fscache/KeyMetadata.h` (lockKeyMetadata signature)
- Modify: `velox/common/caching/fscache/KeyMetadata.cpp` (or .h inline)
- Modify: `velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp` (extend)
- Modify: `velox/common/caching/fscache/tests/KeyMetadataTest.cpp` (KeyNotFoundPolicy cases)

**Spec refs:** §5.2 (lookupRange algorithm, lower_bound + prev), §6.1 (LockedKey + KeyNotFoundPolicy 4 enums)

**Approach:** Add `KeyNotFoundPolicy` enum (4 values). Refactor `KeyMetadata::lock()` into `FsCacheMetadata::lockKeyMetadata(path, policy)` returning either a non-null `LockedKey` (kThrow / kThrowLogical / kCreateEmpty) or possibly null (kReturnNull). Add `FsCacheMetadata::lookupRange(path, lo, hi)` returning offset-ascending `FileSegmentPtr` list intersecting `[lo, hi)`. Uses CH-style `lower_bound + prev` scan inside the per-key lock; releases lock before return (shared_ptr stability). This task adds **only the new API**; phase-1 `lookup(key)` stays for the Task-8 caller migration.

**Do NOT add** in this task (or any later task): `FileSegment::isEvicting()`, `FileSegment::detachedCopy()`, or any other CH-internal helper not enumerated in spec §5 / §6. They were referenced in earlier brainstorm notes but the spec deliberately drops them — eviction in our port is driven by the segment's state transition to `kDetached` (Task 5's holder dtor calls `abandon()`; the eviction-policy callbacks in Task 13 trigger that transition), not by a "currently being evicted" predicate. If you find yourself wanting one of these, you have likely confused CH's metadata bookkeeping with Velox's holder-driven lifecycle — re-read spec §5.4 transition table before writing.

- [ ] **Step 1: Write failing test — KeyNotFoundPolicy**

Append to `velox/common/caching/fscache/tests/KeyMetadataTest.cpp`:

```cpp
TEST(KeyMetadataTest, lockKeyMetadataThrowsWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  EXPECT_THROW(
      md.lockKeyMetadata(path, KeyNotFoundPolicy::kThrow),
      ::facebook::velox::VeloxException);
}

TEST(KeyMetadataTest, lockKeyMetadataReturnsNullWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  auto locked = md.lockKeyMetadata(path, KeyNotFoundPolicy::kReturnNull);
  EXPECT_EQ(locked, nullptr);
}

TEST(KeyMetadataTest, lockKeyMetadataCreatesEmptyWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/new");
  auto locked = md.lockKeyMetadata(path, KeyNotFoundPolicy::kCreateEmpty);
  ASSERT_NE(locked, nullptr);
  EXPECT_TRUE(locked->segments().empty());
  // Second call returns the same KeyMetadata (idempotent).
  auto again = md.lockKeyMetadata(path, KeyNotFoundPolicy::kReturnNull);
  EXPECT_NE(again, nullptr);
}

TEST(KeyMetadataTest, lockKeyMetadataThrowLogicalDifferentExceptionType) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  // kThrowLogical surfaces VELOX_FAIL (CHECK-style), kThrow surfaces
  // VELOX_USER_FAIL. Both are VeloxException subclasses; differentiate via
  // isUserError() if desired. Here we only assert that kThrowLogical also
  // throws (not silent kReturnNull).
  EXPECT_THROW(
      md.lockKeyMetadata(path, KeyNotFoundPolicy::kThrowLogical),
      ::facebook::velox::VeloxException);
}
```

Also append to `tests/FsCacheMetadataTest.cpp`:

```cpp
TEST(FsCacheMetadataTest, lookupRangeEmptyMetadataReturnsEmpty) {
  FsCacheMetadata md{4};
  auto result = md.lookupRange(PathKey::fromPath("/r/x"), 0, 1024);
  EXPECT_TRUE(result.empty());
}

TEST(FsCacheMetadataTest, lookupRangeSingleSegmentFullyInside) {
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto seg = std::make_shared<FileSegment>(FsCacheKey{p, 100, 200}, "/r/x");
  ASSERT_TRUE(md.insert(seg));
  auto result = md.lookupRange(p, 0, 1024);
  ASSERT_EQ(result.size(), 1UL);
  EXPECT_EQ(result[0]->key().offset, 100UL);
}

TEST(FsCacheMetadataTest, lookupRangePrevSegmentIntersects) {
  // Tests the CH-style lower_bound + prev check: a segment that starts
  // BEFORE [lo, hi) but whose end crosses into [lo, hi) must be returned.
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  // Segment at [50, 150)
  auto seg = std::make_shared<FileSegment>(FsCacheKey{p, 50, 100}, "/r/x");
  ASSERT_TRUE(md.insert(seg));
  // Query [100, 200) -- lower_bound returns end(), prev is the seg.
  auto result = md.lookupRange(p, 100, 200);
  ASSERT_EQ(result.size(), 1UL);
  EXPECT_EQ(result[0]->key().offset, 50UL);
}

TEST(FsCacheMetadataTest, lookupRangePrevSegmentNonIntersecting) {
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  // Segment at [0, 100) ends BEFORE 100.
  auto seg = std::make_shared<FileSegment>(FsCacheKey{p, 0, 100}, "/r/x");
  ASSERT_TRUE(md.insert(seg));
  auto result = md.lookupRange(p, 100, 200);
  EXPECT_TRUE(result.empty());
}

TEST(FsCacheMetadataTest, lookupRangeMultipleSegmentsOrdered) {
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  ASSERT_TRUE(md.insert(std::make_shared<FileSegment>(FsCacheKey{p, 0, 100}, "/r/x")));
  ASSERT_TRUE(md.insert(std::make_shared<FileSegment>(FsCacheKey{p, 200, 100}, "/r/x")));
  ASSERT_TRUE(md.insert(std::make_shared<FileSegment>(FsCacheKey{p, 400, 100}, "/r/x")));
  auto result = md.lookupRange(p, 50, 350);
  ASSERT_EQ(result.size(), 2UL);
  EXPECT_EQ(result[0]->key().offset, 0UL);
  EXPECT_EQ(result[1]->key().offset, 200UL);
}
```

- [ ] **Step 2: Run, expect compile failure**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
```

Expected: `error: 'lookupRange' is not a member` / `'KeyNotFoundPolicy' was not declared`.

- [ ] **Step 3: Add KeyNotFoundPolicy enum**

In `KeyMetadata.h` (top of the namespace block):

```cpp
enum class KeyNotFoundPolicy : uint8_t {
  kThrow,         // VELOX_USER_FAIL on absence
  kThrowLogical,  // VELOX_FAIL on absence (programming error)
  kCreateEmpty,   // Create empty KeyMetadata if absent
  kReturnNull,    // Return nullptr if absent
};
```

- [ ] **Step 4: Add lockKeyMetadata to FsCacheMetadata**

In `FsCacheMetadata.h`:

```cpp
/// Returns a LockedKey for `path`. Behaviour on absence depends on policy:
///   kThrow         -> VELOX_USER_FAIL
///   kThrowLogical  -> VELOX_FAIL
///   kCreateEmpty   -> inserts empty KeyMetadata and returns LockedKey
///   kReturnNull    -> returns nullptr (LockedKey wrapped in unique_ptr)
std::unique_ptr<LockedKey> lockKeyMetadata(
    const PathKey& path,
    KeyNotFoundPolicy policy);
```

In `FsCacheMetadata.cpp`:

```cpp
std::unique_ptr<LockedKey> FsCacheMetadata::lockKeyMetadata(
    const PathKey& path,
    KeyNotFoundPolicy policy) {
  auto& bucket = bucketOf(path);
  std::lock_guard<BucketMutex> bucketGuard{bucket.guard};
  auto it = bucket.keys.find(path);
  if (it == bucket.keys.end()) {
    switch (policy) {
      case KeyNotFoundPolicy::kThrow:
        VELOX_USER_FAIL(
            "FsCacheMetadata::lockKeyMetadata: path not found: {:016x}",
            path.hash());
      case KeyNotFoundPolicy::kThrowLogical:
        VELOX_FAIL(
            "FsCacheMetadata::lockKeyMetadata: path not found (logical): {:016x}",
            path.hash());
      case KeyNotFoundPolicy::kCreateEmpty: {
        auto km = std::make_shared<KeyMetadata>();
        bucket.keys.emplace(path, km);
        return std::make_unique<LockedKey>(km.get(), km->mutexForLock());
      }
      case KeyNotFoundPolicy::kReturnNull:
        return nullptr;
    }
    VELOX_UNREACHABLE();
  }
  auto km = it->second;
  return std::make_unique<LockedKey>(km.get(), km->mutexForLock());
}
```

`KeyMetadata::mutexForLock()` is a public accessor returning `mutex_&`; add if missing.

- [ ] **Step 5: Add lookupRange to FsCacheMetadata**

In `FsCacheMetadata.h`:

```cpp
/// Returns the FileSegmentPtrs under `path` whose ranges intersect
/// [lo, hi), in offset-ascending order. Acquires per-key lock via
/// lockKeyMetadata(kReturnNull) and releases before return (segments
/// remain shared_ptr-stable). Empty result on missing key OR no
/// intersections.
std::vector<FileSegmentPtr> lookupRange(
    const PathKey& path,
    uint64_t lo,
    uint64_t hi) const;
```

In `FsCacheMetadata.cpp`:

```cpp
std::vector<FileSegmentPtr> FsCacheMetadata::lookupRange(
    const PathKey& path,
    uint64_t lo,
    uint64_t hi) const {
  // const_cast is safe: lockKeyMetadata mutates bucket map (for kCreateEmpty)
  // but here we use kReturnNull which only reads. Avoiding the cast would
  // require a separate const path; matches CH FileCache::getImpl pattern.
  auto locked = const_cast<FsCacheMetadata*>(this)->lockKeyMetadata(
      path, KeyNotFoundPolicy::kReturnNull);
  if (locked == nullptr) {
    return {};
  }
  auto& segs = locked->segments();
  std::vector<FileSegmentPtr> result;
  auto it = segs.lower_bound(lo);
  if (it != segs.begin()) {
    auto prev = std::prev(it);
    const auto prevEnd = prev->second->key().offset + prev->second->key().size;
    if (prevEnd > lo) {
      it = prev;
    }
  }
  while (it != segs.end() && it->first < hi) {
    result.push_back(it->second);
    ++it;
  }
  return result;
}
```

`KeyMetadata::segments()` must return the `std::map<uint64_t, FileSegmentPtr>` reference; add accessor if missing.

- [ ] **Step 6: Run, all new tests + existing pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='KeyMetadataTest.*:FsCacheMetadataTest.*'
```

Expected: all PASS, existing phase-1 cases unchanged.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCacheMetadata.h \
        velox/common/caching/fscache/FsCacheMetadata.cpp \
        velox/common/caching/fscache/KeyMetadata.h \
        velox/common/caching/fscache/KeyMetadata.cpp \
        velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp \
        velox/common/caching/fscache/tests/KeyMetadataTest.cpp
git commit -m "$(cat <<'EOF'
feat(fscache): add lookupRange + KeyNotFoundPolicy per CH FileCache

lookupRange(path, lo, hi) uses CH-style lower_bound + prev to return all
segments under `path` intersecting [lo, hi) in offset-ascending order
(spec §5.2). KeyNotFoundPolicy 4 enums let callers choose between throw
/ kThrowLogical / kCreateEmpty / kReturnNull on missing keys (spec §6.1).

Phase-1 lookup(key) stays in place; Task 8 will migrate FsCache callers
to lookupRange + fillHoles and remove the point-lookup path.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §5.2 §6.1

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: fillHolesWithEmptyFileSegments

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.h` (static helper or free function declaration)
- Modify: `velox/common/caching/fscache/FsCache.cpp` (impl)
- Create: `velox/common/caching/fscache/tests/FillHolesTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec refs:** §5.3 (algorithm: empty/leading/middle/trailing holes; slice each by maxSegmentSize)

**Approach:** Free function in anonymous namespace inside `FsCache.cpp`, exposed via static `FsCache::fillHolesWithEmptyFileSegments` for testability. Given `found` (from `lookupRange`) and `[lo, hi)`, produces a continuous list covering `[lo, hi)` with kEmpty segments filling all gaps. Each hole is sliced by `cfg.maxSegmentSize` matching phase-1 `splitRange`. Caller must already hold the per-key LockedKey; helper inserts new kEmpty segments into both `LockedKey::segments()` and the metadata bucket via the existing `metadata.insert(seg)`.

- [ ] **Step 1: Write failing test**

Create `velox/common/caching/fscache/tests/FillHolesTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

namespace {
FsCacheConfig defaultCfg() {
  FsCacheConfig cfg;
  cfg.alignment = 4UL << 20;          // 4 MiB
  cfg.maxSegmentSize = 32UL << 20;    // 32 MiB
  return cfg;
}

FileSegmentPtr makeSeg(uint64_t off, uint64_t size, const std::string& path) {
  return std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath(path), off, size}, path);
}
} // namespace

TEST(FillHolesTest, emptyFoundProducesSingleHoleSlicedByMaxSize) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {}, 0, 4096, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 4UL);
  EXPECT_EQ(result[0]->key().offset, 0UL);
  EXPECT_EQ(result[0]->key().size, 1024UL);
  EXPECT_EQ(result[3]->key().offset, 3072UL);
  EXPECT_EQ(result[3]->key().size, 1024UL);
  for (const auto& s : result) {
    EXPECT_EQ(s->state(), FileSegment::State::kEmpty);
  }
}

TEST(FillHolesTest, leadingHoleOnly) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto existing = makeSeg(2048, 1024, "/r/x");
  ASSERT_TRUE(md.insert(existing));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {existing}, 0, 3072, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0]->key().offset, 0UL);
  EXPECT_EQ(result[1]->key().offset, 1024UL);
  EXPECT_EQ(result[2]->key().offset, 2048UL);
  EXPECT_EQ(result[2], existing);
}

TEST(FillHolesTest, trailingHoleOnly) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto existing = makeSeg(0, 1024, "/r/x");
  ASSERT_TRUE(md.insert(existing));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {existing}, 0, 3072, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0], existing);
  EXPECT_EQ(result[1]->key().offset, 1024UL);
  EXPECT_EQ(result[2]->key().offset, 2048UL);
}

TEST(FillHolesTest, middleHole) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto left = makeSeg(0, 1024, "/r/x");
  auto right = makeSeg(2048, 1024, "/r/x");
  ASSERT_TRUE(md.insert(left));
  ASSERT_TRUE(md.insert(right));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {left, right}, 0, 3072, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0], left);
  EXPECT_EQ(result[1]->key().offset, 1024UL);
  EXPECT_EQ(result[1]->state(), FileSegment::State::kEmpty);
  EXPECT_EQ(result[2], right);
}

TEST(FillHolesTest, noHolesReturnsFoundUnchanged) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto a = makeSeg(0, 1024, "/r/x");
  auto b = makeSeg(1024, 1024, "/r/x");
  ASSERT_TRUE(md.insert(a));
  ASSERT_TRUE(md.insert(b));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {a, b}, 0, 2048, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 2UL);
  EXPECT_EQ(result[0], a);
  EXPECT_EQ(result[1], b);
}

TEST(FillHolesTest, holeLargerThanMaxSegmentSizeSplits) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 512;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {}, 0, 2048, p, "/r/x", *locked, md, cfg);
  ASSERT_EQ(result.size(), 4UL);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(result[i]->key().offset, i * 512UL);
    EXPECT_EQ(result[i]->key().size, 512UL);
  }
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run, expect compile failure**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
```

Expected: `error: 'fillHolesWithEmptyFileSegments' is not a member of 'FsCache'`.

- [ ] **Step 3: Declare in FsCache.h**

In public static section:

```cpp
/// Given `found` (from FsCacheMetadata::lookupRange) and the requested
/// range [lo, hi), returns a continuous list covering [lo, hi) where
/// every gap is filled with newly inserted kEmpty segments sliced by
/// cfg.maxSegmentSize. Caller MUST hold `lockedKey` for `path`.
static std::vector<FileSegmentPtr> fillHolesWithEmptyFileSegments(
    std::vector<FileSegmentPtr> found,
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,
    FsCacheMetadata& metadata,
    const FsCacheConfig& cfg);
```

- [ ] **Step 4: Implement in FsCache.cpp**

```cpp
namespace {

// Splits [lo, hi) into kEmpty segments of size <= maxSegmentSize and
// inserts them into both lockedKey.segments() and metadata. Pushes the
// new shared_ptrs onto `out` in offset-ascending order.
void sliceHoleAndInsert(
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,
    FsCacheMetadata& metadata,
    const FsCacheConfig& cfg,
    std::vector<FileSegmentPtr>& out) {
  uint64_t cursor = lo;
  while (cursor < hi) {
    const uint64_t size = std::min(cfg.maxSegmentSize, hi - cursor);
    auto seg = std::make_shared<FileSegment>(
        FsCacheKey{path, cursor, size}, remotePath);
    // insert returns false if a race already populated this offset; in that
    // case the kRangelock around lookupRange + fillHoles in getOrSet
    // prevents the race, so a duplicate here is a programming error.
    VELOX_CHECK(
        metadata.insert(seg),
        "fillHolesWithEmptyFileSegments: duplicate insert at offset={}",
        cursor);
    out.push_back(seg);
    cursor += size;
  }
}

} // namespace

std::vector<FileSegmentPtr> FsCache::fillHolesWithEmptyFileSegments(
    std::vector<FileSegmentPtr> found,
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,
    FsCacheMetadata& metadata,
    const FsCacheConfig& cfg) {
  std::vector<FileSegmentPtr> result;
  if (found.empty()) {
    sliceHoleAndInsert(
        lo, hi, path, remotePath, lockedKey, metadata, cfg, result);
    return result;
  }

  // Leading hole.
  if (lo < found.front()->key().offset) {
    sliceHoleAndInsert(
        lo,
        found.front()->key().offset,
        path,
        remotePath,
        lockedKey,
        metadata,
        cfg,
        result);
  }

  for (size_t i = 0; i < found.size(); ++i) {
    result.push_back(found[i]);
    if (i + 1 < found.size()) {
      const uint64_t gapStart = found[i]->key().offset + found[i]->key().size;
      const uint64_t gapEnd = found[i + 1]->key().offset;
      if (gapStart < gapEnd) {
        sliceHoleAndInsert(
            gapStart,
            gapEnd,
            path,
            remotePath,
            lockedKey,
            metadata,
            cfg,
            result);
      }
    }
  }

  // Trailing hole.
  const uint64_t lastEnd =
      found.back()->key().offset + found.back()->key().size;
  if (lastEnd < hi) {
    sliceHoleAndInsert(
        lastEnd, hi, path, remotePath, lockedKey, metadata, cfg, result);
  }
  return result;
}
```

Note: the result is built incrementally so a middle-hole insertion comes between the surrounding existing segments. The `result` vector after the loop is naturally offset-ascending.

- [ ] **Step 5: Run, all 6 cases pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='FillHolesTest.*'
```

Expected: `[  PASSED  ] 6 tests`.

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FillHolesTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): add fillHolesWithEmptyFileSegments

Given segments from FsCacheMetadata::lookupRange and a request range
[lo, hi), returns a continuous list with every gap filled by newly
inserted kEmpty segments sliced by cfg.maxSegmentSize. Mirrors CH
FileCache::fillHolesWithEmptyFileSegments (spec §5.3). Caller must hold
LockedKey for `path`; the helper inserts into both lockedKey's segments
map and the metadata bucket.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §5.3

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Hard-cut getOrSet signature to FileSegmentsHolderPtr (R1)

**Files (one commit, large diff per spec R1):**
- Modify: `velox/common/caching/fscache/FsCache.h` (getOrSet signature)
- Modify: `velox/common/caching/fscache/FsCache.cpp` (getOrSet impl + remove lookupOrCreate)
- Modify: `velox/common/caching/fscache/FsCacheMetadata.h` (remove phase-1 lookup(key))
- Modify: `velox/common/caching/fscache/FsCacheMetadata.cpp`
- Modify: `velox/common/caching/fscache/FileSegment.h` (remove beginDownload / download)
- Modify: `velox/common/caching/fscache/FileSegment.cpp`
- Modify: `velox/dwio/common/FsCacheBufferedInput.cpp` (update enqueue/load to new API; keep DeferredStream the same — Task 9 rewrites load() driver, this task just wires the new signature)
- Modify: `velox/dwio/common/FsCacheInputStream.cpp` (update construction from FileSegmentsHolderPtr if needed)
- Modify: `velox/common/caching/fscache/tests/FsCacheTest.cpp` (all `getOrSet` callsites take holder)
- Modify: `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheScaffoldTest.cpp` (if it touches getOrSet)
- Modify: `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`

**Spec refs:** §4 (new getOrSet signature with `IsPrefetch` parameter; all callsites in this task hard-code `kDemand` — the prefetch callsite is flipped to `kPrefetch` in Task 11 step 4, and stats recording is wired in Task 14), §4.1 (time line steps 1-7 minus step 6 stats, deferred to Task 14), §10 R1 (mitigation: API hard cut goes in its own commit, no other logic changes)

**Approach (R1 explicit guidance):** This task does **only** the API hard cut. Do not change semantics: every phase-1 behaviour (sync download, single-segment-only metadata population per range) must remain bit-identical. Stats counting (POD `FsCacheStats` + 4-field prefetch/demand split on internal `AtomicCounters`) is wired in Task 14. caller-driven advancement is wired in Task 9. SLRU is Task 13.

The new `getOrSet` does (matches §4.1 steps 1-5, 7-8; step 6 deferred to Task 14):

1. Clamp size to `remote.size() - offset`.
2. Compute alignedRange via `splitRange` outer bounds (same as phase-1).
3. `auto lockedKey = metadata.lockKeyMetadata(path, kCreateEmpty);`
4. `found = metadata.lookupRange(path, alignedLo, alignedHi);`
5. `slots = fillHolesWithEmptyFileSegments(found, alignedLo, alignedHi, ...)`.
6. (Stats — deferred to Task 14.)
7. Release lockedKey (RAII scope end).
8. **Backward-compatible behaviour shim:** Because Task 9/10 haven't run yet, callers can't drive kEmpty → kDownloaded themselves. For this single commit, after step 7 the FsCache itself synchronously walks the holder and runs the **phase-1 download** equivalent per kEmpty segment, so existing tests still see kDownloaded segments before returning. **This shim is removed in Task 9** when `FsCacheBufferedInput::load()` becomes the driver.

The shim is the only way to keep the test corpus green at this commit without conflating two huge changes. The shim is ~20 lines, explicit, and Task 9's first action is to remove it.

- [ ] **Step 1: Change FsCache.h signature**

```cpp
enum class IsPrefetch : uint8_t { kPrefetch, kDemand };  // prefetch callsite flipped by Task 11 step 4; stats recording wired by Task 14

/// CH-aligned entry point. Returns a holder of segments covering
/// [offset, min(offset+size, remote.size())) — contiguous, offset-
/// ascending, may include kEmpty / kDownloading / kDownloaded states.
/// `settings` controls hole slicing (maxSegmentSize, alignment); per
/// spec §5.3 the implementation forwards it to fillHoles. Phase-2
/// passes &config_ so all callers share the cache-level defaults; the
/// signature keeps a separate parameter so a future caller can tune
/// per-call (e.g. larger alignment for cold scans) without touching
/// FsCacheConfig. `isPrefetch` selects which pair of stats counters
/// to bump (Task 14 wires the actual counts; this commit forwards
/// the value through unchanged). No default — every caller decides.
///
/// Note: spec §4 names this `CreateSettings`; that type does not yet
/// exist in codebase. We forward `const FsCacheConfig&` here (spec
/// §5.3 fillHoles already uses FsCacheConfig). A future commit may
/// extract `CreateSettings` if a per-call override is actually needed.
FileSegmentsHolderPtr getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& settings,
    ::facebook::velox::ReadFile& remote,
    IsPrefetch isPrefetch);
```

Delete the phase-1 `std::vector<FileSegmentPtr> getOrSet(...)` declaration AND delete `lookupOrCreate` from the private section.

In `FsCacheMetadata.h`, delete phase-1 `FileSegmentPtr lookup(const FsCacheKey&) const`.

In `FileSegment.h`, delete `beginDownload()` and `download()` declarations.

- [ ] **Step 2: Rewrite getOrSet impl**

```cpp
FileSegmentsHolderPtr FsCache::getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& settings,
    ::facebook::velox::ReadFile& remote,
    IsPrefetch /*isPrefetch*/) {
  // settings is unused in this commit (the in-process cache always
  // uses config_); Task 14 wires it through to fillHoles. Keeping the
  // parameter in the signature here avoids a second ABI break later.
  const uint64_t fileSize = remote.size();
  VELOX_USER_CHECK_LE(
      offset,
      fileSize,
      "FsCache::getOrSet offset past EOF, fileSize={}",
      fileSize);
  if (offset == fileSize || size == 0) {
    return std::make_unique<FileSegmentsHolder>(std::vector<FileSegmentPtr>{});
  }
  const uint64_t clampedSize = std::min(size, fileSize - offset);
  // Outward-aligned range matches splitRange's outer boundary contract.
  const uint64_t alignedLo =
      (offset / config_.alignment) * config_.alignment;
  const uint64_t end = offset + clampedSize;
  const uint64_t alignedHi =
      ((end + config_.alignment - 1) / config_.alignment) * config_.alignment;
  // Re-clamp alignedHi to file size so we don't allocate post-EOF kEmpty
  // segments that would then refuse to download.
  const uint64_t clampedHi = std::min(alignedHi, fileSize);

  const PathKey pathKey = PathKey::fromPath(path);
  auto lockedKey = metadata_->lockKeyMetadata(
      pathKey, KeyNotFoundPolicy::kCreateEmpty);
  auto found = metadata_->lookupRange(pathKey, alignedLo, clampedHi);
  auto slots = fillHolesWithEmptyFileSegments(
      std::move(found),
      alignedLo,
      clampedHi,
      pathKey,
      path,
      *lockedKey,
      *metadata_,
      config_);
  lockedKey.reset();  // release per-key lock before download.

  // Transitional shim: until Task 9 makes FsCacheBufferedInput the driver,
  // run kEmpty -> kDownloaded synchronously inside getOrSet so existing
  // phase-1 tests continue to observe kDownloaded segments. Removed by
  // Task 9 step 1.
  for (auto& seg : slots) {
    if (seg->state() != FileSegment::State::kEmpty) {
      continue;
    }
    if (!seg->reserve(seg->key().size, config_.cacheRoot)) {
      continue;  // someone else won; we will wait below if needed
    }
    evict(seg->key().size);
    try {
      // Stream the segment in bounded chunks rather than alloc'ing the
      // full segment at once (mirrors phase-1 download() behaviour).
      constexpr uint64_t kChunk = 1UL << 20;
      std::vector<char> buf(std::min<uint64_t>(kChunk, seg->key().size));
      uint64_t remaining = seg->key().size;
      uint64_t cursor = seg->key().offset;
      while (remaining > 0) {
        const uint64_t toRead = std::min<uint64_t>(buf.size(), remaining);
        remote.pread(cursor, toRead, buf.data());
        seg->write(buf.data(), toRead);
        cursor += toRead;
        remaining -= toRead;
      }
      seg->complete();
      recordMiss(seg.get(), seg->key().size); // 2-arg here; Task 14 step 5 extends to 3-arg (isPrefetch)
    } catch (...) {
      seg->abandon();
      throw;
    }
  }
  for (auto& seg : slots) {
    if (seg->state() == FileSegment::State::kDownloading) {
      // Some other thread is writing it. Wait until they finish OR abandon.
      seg->waitForDownloadedSize(seg->key().size);
    }
    if (seg->state() == FileSegment::State::kDownloaded) {
      recordHit(seg.get()); // 2-arg here; Task 14 step 5 extends to 3-arg (isPrefetch)
    }
  }
  return std::make_unique<FileSegmentsHolder>(std::move(slots));
}
```

Drop the phase-1 `lookupOrCreate` function entirely.

- [ ] **Step 3: Migrate FsCacheBufferedInput**

In `velox/dwio/common/FsCacheBufferedInput.cpp`:

```cpp
struct EnqueuedRegion {
  velox::common::Region region;
  FileSegmentsHolderPtr holder;  // was std::vector<FileSegmentPtr> segments
  // bypassBuffer is populated by Task 12's load() when getOrSet returns
  // an empty holder (size >= bypassThresholdBytes). In Tasks 8-11 the
  // holder is always non-empty, so this field stays unused/empty.
  std::vector<char> bypassBuffer;
};

// DeferredStream constructs FsCacheInputStream from slot_->holder->segments()
// (just change the .segments accessor; FsCacheInputStream signature is
// unchanged in this task).
```

Update `enqueue()` to leave `holder = nullptr`, `load()` to call `getOrSet` and stash the result:

```cpp
void FsCacheBufferedInput::load(LogType) {
  for (auto& enq : enqueuedRegions_) {
    if (enq.holder != nullptr) {
      continue;
    }
    enq.holder = fsCache_->getOrSet(
        input_->getName(),
        enq.region.offset,
        enq.region.length,
        fsCache_->config(),
        *input_->getReadFile(),
        cache::fs::IsPrefetch::kDemand);  // Task 11 step 4 flips this callsite to kPrefetch
  }
}
```

`DeferredStream::ensureWithData()` updates:

```cpp
VELOX_CHECK(
    slot_->holder != nullptr,
    "Stream used before FsCacheBufferedInput::load()");
inner_ = std::make_unique<FsCacheInputStream>(
    slot_->holder->segments(),    // pass the underlying vector by ref/copy
    slot_->region.offset,
    slot_->region.length,
    cache_->config().cacheRoot);
```

Note: `FsCacheInputStream` currently takes `std::vector<FileSegmentPtr>` by value. Keep that signature here — passing `holder->segments()` works without further changes.

- [ ] **Step 4: Migrate all phase-1 tests**

For each test that called `auto segs = cache_.getOrSet(...)`, change to:

```cpp
auto holder = cache_.getOrSet(...);
auto& segs = holder->segments();
```

Mechanical change. Files to update (per "Files" list above) — work through each one and rebuild between files to localize errors.

- [ ] **Step 5: Build + run full fscache test suite**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test velox_dwio_common_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheBufferedInputTest.*'
```

Expected: all green. Any test that depends on phase-1 behaviour that the shim doesn't preserve = real bug, fix before commit.

- [ ] **Step 6: Commit (large diff, single intent)**

```bash
git add velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/FsCacheMetadata.h \
        velox/common/caching/fscache/FsCacheMetadata.cpp \
        velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/dwio/common/FsCacheBufferedInput.cpp \
        velox/dwio/common/FsCacheInputStream.cpp \
        velox/common/caching/fscache/tests/FsCacheTest.cpp \
        velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp \
        velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp \
        velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp \
        velox/common/caching/fscache/tests/FsCacheScaffoldTest.cpp \
        velox/dwio/common/tests/FsCacheBufferedInputTest.cpp
git commit -m "$(cat <<'EOF'
refactor(fscache): hard-cut getOrSet to FileSegmentsHolderPtr

Replaces phase-1 std::vector<FileSegmentPtr> with FileSegmentsHolderPtr
across getOrSet, FsCacheBufferedInput, FsCacheInputStream, and all phase-1
test callsites in a single commit (spec §10 R1: API migration goes alone).

New getOrSet:
  - lockKeyMetadata(kCreateEmpty) per-key
  - lookupRange + fillHolesWithEmptyFileSegments
  - transitional sync download shim inside getOrSet so existing tests pass
    until Task 9 makes FsCacheBufferedInput the driver

TRANSITIONAL SHIM (~20 lines, removed in Task 9): because Task 9/10
have not landed, callers cannot yet drive kEmpty → kDownloaded
themselves, so this commit has FsCache itself synchronously walk the
holder and run the phase-1 download equivalent per kEmpty segment
before returning. Reviewer: if you see this shim still present in any
commit AFTER Task 9, that is a bug — Task 9's first step deletes it.

The IsPrefetch enum lands here as part of the new signature, but the
parameter is intentionally unused (`/*isPrefetch*/`) in this commit —
Task 14 wires it through FsCacheBufferedInput AND the stats counters
together so the wiring lands with end-to-end test coverage. Callers in
this commit hard-code `IsPrefetch::kDemand`; no stats are recorded yet.

Removes phase-1 FsCacheMetadata::lookup(key), FileSegment::beginDownload/
download, FsCache::lookupOrCreate. Stats counting (Task 14), caller-driven
advancement (Task 9), and SLRU (Task 13) land in subsequent commits.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4 §4.1 §10 R1

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: FsCacheBufferedInput::load drives caller-side advancement

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.cpp` (REMOVE Task 8's sync-download shim from getOrSet)
- Modify: `velox/dwio/common/FsCacheBufferedInput.cpp` (load() does reserve / read / write / complete per kEmpty segment)
- Modify: `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` (add caller-driver regression)

**Spec refs:** §4.1 caller-driven advancement loop, §7.2 R0 async load (sync version first, async in Task 11)

**Approach:** Task 8 left a sync-download shim inside `getOrSet`. Now `getOrSet` truly returns `kEmpty / kDownloading` segments and `FsCacheBufferedInput::load()` becomes the driver. For each kEmpty segment, it `reserve()`s, reads remote in chunks, `write()`s, `complete()`s — synchronously for now (DownloadThreadPool comes in Task 11). For kDownloading segments owned by another thread, `load()` does **not** wait — the reader handles that lazily.

This task **deletes** the Task-8 shim; tests that depended on "kDownloaded immediately after getOrSet" must still pass because `FsCacheBufferedInput::load()` is the only caller in the test corpus and it still synchronously drives to kDownloaded before returning.

- [ ] **Step 1: Write failing regression test**

Append to `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`:

```cpp
// After load(), every segment in the holder must be kDownloaded or
// kDownloading. Locks in Task 9's contract: FsCacheBufferedInput is the
// driver, not FsCache::getOrSet.
TEST_F(FsCacheBufferedInputTest, loadAdvancesAllEmptySegmentsToDownloaded) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto stream = input.enqueue({0, 1UL << 20});
  input.load(LogType::FILE);
  // We can't peek at segments directly through the public stream, but
  // draining the stream must produce the full expected bytes.
  auto bytes = drain(*stream, 1UL << 20);
  EXPECT_EQ(bytes.size(), 1UL << 20);
  EXPECT_EQ(bytes, remoteContent_.substr(0, 1UL << 20));
}
```

This test passes today (because of the Task-8 shim) — it acts as a sentinel.

Add a second test that PROVES the shim is gone by using a direct `getOrSet` call:

```cpp
TEST_F(FsCacheBufferedInputTest, getOrSetReturnsEmptySegmentsWithoutShim) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  auto holder = fsCache_->getOrSet(
      remotePath_,
      0,
      1UL << 20,
      fsCache_->config(),
      *readFile,
      IsPrefetch::kDemand);
  ASSERT_NE(holder, nullptr);
  ASSERT_FALSE(holder->segments().empty());
  // After Task 9 removes the shim, kEmpty segments are returned. Any
  // segment that's already kDownloaded is fine (could happen if a prior
  // test populated the cache).
  for (const auto& seg : holder->segments()) {
    EXPECT_TRUE(
        seg->state() == FileSegment::State::kEmpty ||
        seg->state() == FileSegment::State::kDownloaded ||
        seg->state() == FileSegment::State::kDownloading);
  }
}
```

- [ ] **Step 2: Run, expect both green at this point (shim still in place)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_dwio_common_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheBufferedInputTest.*'
```

Both pass; the sentinel + the "kEmpty allowed" test both succeed.

- [ ] **Step 3: Remove Task-8 shim from FsCache::getOrSet**

Cut the entire "Transitional shim" block (the two for-loops after `lockedKey.reset();`). New getOrSet body ends with:

```cpp
  lockedKey.reset();
  return std::make_unique<FileSegmentsHolder>(std::move(slots));
}
```

- [ ] **Step 4: Run, expect `loadAdvancesAllEmptySegmentsToDownloaded` to fail**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_dwio_common_test velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheBufferedInputTest.*'
```

Expected: `loadAdvancesAllEmptySegmentsToDownloaded` fails (drain returns nothing because the segments are kEmpty and FsCacheInputStream's read path hasn't been taught to drive them).

- [ ] **Step 5: Wire FsCacheBufferedInput::load as driver**

In `velox/dwio/common/FsCacheBufferedInput.cpp`:

```cpp
void FsCacheBufferedInput::load(LogType) {
  for (auto& enq : enqueuedRegions_) {
    if (enq.holder != nullptr) {
      continue;
    }
    enq.holder = fsCache_->getOrSet(
        input_->getName(),
        enq.region.offset,
        enq.region.length,
        fsCache_->config(),
        *input_->getReadFile(),
        IsPrefetch::kPrefetch);  // Task 14 wires the actual stats path

    for (auto& seg : enq.holder->segments()) {
      if (seg->state() != FileSegment::State::kEmpty) {
        continue;
      }
      if (!seg->reserve(seg->key().size, fsCache_->config().cacheRoot)) {
        continue;  // another driver won the writer slot
      }
      try {
        constexpr uint64_t kChunk = 1UL << 20;
        std::vector<char> buf(std::min<uint64_t>(kChunk, seg->key().size));
        uint64_t remaining = seg->key().size;
        uint64_t cursor = seg->key().offset;
        while (remaining > 0) {
          const uint64_t toRead =
              std::min<uint64_t>(buf.size(), remaining);
          input_->getReadFile()->pread(cursor, toRead, buf.data());
          seg->write(buf.data(), toRead);
          cursor += toRead;
          remaining -= toRead;
        }
        seg->complete();
      } catch (...) {
        seg->abandon();
        throw;
      }
    }
  }
}
```

- [ ] **Step 6: Run, expect green**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_dwio_common_test velox_fscache_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheBufferedInputTest.*'
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCache.cpp \
        velox/dwio/common/FsCacheBufferedInput.cpp \
        velox/dwio/common/tests/FsCacheBufferedInputTest.cpp
git commit -m "$(cat <<'EOF'
refactor(fscache): caller drives advancement; remove getOrSet sync shim

FsCacheBufferedInput::load() now reserves / reads remote / writes /
completes each kEmpty segment in the holder. Removes the transitional
sync-download shim added in Task 8 inside FsCache::getOrSet so getOrSet
truly returns kEmpty segments per spec §4.1.

Download remains synchronous within load(); Task 11 introduces the
DownloadThreadPool so load() returns before download completes.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4.1 §7.2

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: FsCacheInputStream waits for partial-readable bytes

**Files:**
- Modify: `velox/dwio/common/FsCacheInputStream.h` (no API change, possibly add waitForReady helper)
- Modify: `velox/dwio/common/FsCacheInputStream.cpp` (loadCurrentSegmentBuffer waits if segment is kDownloading)
- Create: `velox/dwio/common/tests/FsCacheInputStreamTest.cpp`
- Modify: `velox/dwio/common/tests/CMakeLists.txt`

**Spec refs:** §4.2 partial-readable timing (reader calls `waitForDownloadedSize(needed)` before reading bytes), §11 Task 10 deps 3, 8

**Approach:** Today `FsCacheInputStream::loadCurrentSegmentBuffer()` calls `segment->read(...)` assuming the segment is kDownloaded. After Task 9, a holder may include segments that are kDownloading (another thread driving them) or kEmpty (no one yet — but `FsCacheBufferedInput::load` guarantees this doesn't happen in the test corpus; still, defend against it). Before reading any byte from a segment, call `seg->waitForDownloadedSize(needed)` where `needed = (rangeEnd - segStart)`. If segment is `kEmpty`, treat as programming error (caller forgot to drive).

- [ ] **Step 1: Write failing test — reader waits for downloading segment**

Create `velox/dwio/common/tests/FsCacheInputStreamTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/dwio/common/FsCacheInputStream.h"

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <thread>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FileSegment;
using ::facebook::velox::cache::fs::FileSegmentPtr;
using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::cache::fs::FsCacheKey;
using ::facebook::velox::cache::fs::PathKey;
using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheInputStreamTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FsCacheInputStreamTest, readerBlocksUntilWriterCompletesDownloadingSegment) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(1024, cacheRoot_));
  std::vector<FileSegmentPtr> segs{seg};

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    FsCacheInputStream stream{segs, 0, 1024, cacheRoot_};
    const void* data;
    int32_t len;
    while (stream.Next(&data, &len)) {
    }
    readerDone.store(true);
  }};

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  std::string payload(1024, 'Z');
  seg->write(payload.data(), payload.size());
  seg->complete();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

} // namespace facebook::velox::dwio::common::test
```

- [ ] **Step 2: Run, expect failure (hang or read past end)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_dwio_common_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheInputStreamTest.*' --gtest_break_on_failure
```

Expected: timeout / hang / throw — depends on phase-1 FileSegment::read behaviour when state is kDownloading. The test fails because the reader is reading a segment that hasn't been completed.

- [ ] **Step 3: Add wait to loadCurrentSegmentBuffer**

In `velox/dwio/common/FsCacheInputStream.cpp`:

```cpp
void FsCacheInputStream::loadCurrentSegmentBuffer() {
  const auto& segment = segments_[index_];
  const uint64_t segStart = segment->key().offset;
  const uint64_t segSize = segment->key().size;
  const uint64_t rangeStart = std::max(regionOffset_, segStart);
  const uint64_t rangeEnd =
      std::min(regionOffset_ + regionLength_, segStart + segSize);
  VELOX_CHECK_LT(rangeStart, rangeEnd);
  const uint64_t length = rangeEnd - rangeStart;
  // Wait until enough bytes of the segment are durable. needed is
  // measured from segStart; if reader needs the first `length` bytes
  // starting at offset (rangeStart - segStart) inside the segment, the
  // writer must have advanced downloadedSize_ at least to that endpoint.
  const uint64_t needed = (rangeStart - segStart) + length;
  segment->waitForDownloadedSize(needed);
  buffer_.assign(length, '\0');
  segment->read(rangeStart - segStart, length, buffer_.data(), cacheRoot_);
  cursor_ = 0;
}
```

- [ ] **Step 4: Run, expect green**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_dwio_common_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheInputStreamTest.*:FsCacheBufferedInputTest.*'
```

Expected: all green; the writer-completes-after-reader-arrives case works.

- [ ] **Step 5: Commit**

```bash
git add velox/dwio/common/FsCacheInputStream.cpp \
        velox/dwio/common/FsCacheInputStream.h \
        velox/dwio/common/tests/FsCacheInputStreamTest.cpp \
        velox/dwio/common/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): FsCacheInputStream waits for partial-readable bytes

Before reading any byte from a segment, calls
FileSegment::waitForDownloadedSize(needed) where needed is the highest
byte offset the reader needs. Supports the spec §4.2 partial-readable
contract: writer notify_all on each chunk, reader wakes when enough
bytes are durable, abandoned writer throws (Task 3 contract) so reader
surfaces the failure instead of reading past the partial boundary.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §4.2

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: DownloadThreadPool + async load

**Files:**
- Create: `velox/common/caching/fscache/DownloadThreadPool.h`
- Create: `velox/common/caching/fscache/DownloadThreadPool.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/FsCacheConfig.h` (+ downloadThreads)
- Modify: `velox/common/caching/fscache/FsCache.h` (FsCache owns pool)
- Modify: `velox/common/caching/fscache/FsCache.cpp` (construct pool)
- Modify: `velox/dwio/common/FsCacheBufferedInput.cpp` (submit task to pool, don't block)
- Create: `velox/common/caching/fscache/tests/DownloadThreadPoolTest.cpp`
- Create: `velox/dwio/common/tests/FsCacheAsyncLoadTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`
- Modify: `velox/dwio/common/tests/CMakeLists.txt`

**Spec refs:** §7.1 DownloadThreadPool (folly::IOThreadPoolExecutor, default 8 threads), §7.2 R0 async load, §10 R4 mitigation

**Approach:** Thin wrapper over `folly::IOThreadPoolExecutor`. `FsCache` owns one instance. `FsCacheBufferedInput::load()` switches from synchronous chunk-loop to `pool_->submit(seg, remote_)` per kEmpty segment after `reserve()` succeeds. The reader's `waitForDownloadedSize()` (Task 10) provides synchronisation; load() returns as soon as all reserves are done.

- [ ] **Step 1: Write failing test — DownloadThreadPool basics**

Create `velox/common/caching/fscache/tests/DownloadThreadPoolTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/DownloadThreadPool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

namespace facebook::velox::cache::fs::test {

TEST(DownloadThreadPoolTest, submitRunsTask) {
  DownloadThreadPool pool{2};
  std::atomic<int> counter{0};
  auto fut = pool.submit([&]() { counter.fetch_add(1); });
  fut.wait();
  EXPECT_EQ(counter.load(), 1);
}

TEST(DownloadThreadPoolTest, submitParallel) {
  DownloadThreadPool pool{4};
  std::atomic<int> counter{0};
  std::vector<folly::SemiFuture<folly::Unit>> futs;
  for (int i = 0; i < 16; ++i) {
    futs.push_back(pool.submit([&]() { counter.fetch_add(1); }));
  }
  for (auto& f : futs) {
    std::move(f).get();
  }
  EXPECT_EQ(counter.load(), 16);
}

} // namespace facebook::velox::cache::fs::test
```

Create `velox/dwio/common/tests/FsCacheAsyncLoadTest.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/dwio/common/FsCacheBufferedInput.h"

#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::common::testutil::TempDirectoryPath;

// Sentinel: load() must return faster than the actual remote read could.
TEST(FsCacheAsyncLoadTest, loadReturnsBeforeRemoteReadCompletes) {
  auto tempDir = TempDirectoryPath::create();
  const std::string remotePath = tempDir->getPath() + "/remote.bin";
  const size_t fileSize = 8UL << 20;  // 8 MiB
  {
    std::ofstream out{remotePath, std::ios::binary};
    std::string buf(fileSize, 'A');
    out.write(buf.data(), buf.size());
  }

  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir->getPath() + "/cache";
  cfg.maxBytes = 64UL << 20;
  cfg.downloadThreads = 4;
  std::filesystem::create_directories(cfg.cacheRoot);
  auto cache = std::make_unique<FsCache>(cfg);

  memory::MemoryManager::testingSetInstance({});
  auto pool = memory::memoryManager()->addLeafPool("AsyncLoadTest");
  auto readFile = std::make_shared<LocalReadFile>(remotePath);
  FsCacheBufferedInput input{readFile, *pool, cache.get()};
  auto stream = input.enqueue({0, fileSize});

  const auto loadStart = std::chrono::steady_clock::now();
  input.load(LogType::FILE);
  const auto loadDur = std::chrono::steady_clock::now() - loadStart;

  // Async load should return in well under 20 ms even though the remote
  // pread of 8 MiB will be measurably slower on a real disk. This is a
  // sentinel for "load() did NOT block on download".
  EXPECT_LT(
      std::chrono::duration_cast<std::chrono::milliseconds>(loadDur).count(),
      20);

  // The actual read still gets the right bytes (waitForDownloadedSize
  // synchronises).
  std::string buf(fileSize, '\0');
  size_t copied = 0;
  const void* data;
  int32_t len;
  while (copied < fileSize && stream->Next(&data, &len)) {
    const size_t toCopy = std::min<size_t>(len, fileSize - copied);
    std::memcpy(buf.data() + copied, data, toCopy);
    copied += toCopy;
  }
  EXPECT_EQ(copied, fileSize);
}

} // namespace facebook::velox::dwio::common::test
```

- [ ] **Step 2: Run, expect compile failure on DownloadThreadPool.h not found**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test velox_dwio_common_test -j 16
```

- [ ] **Step 3: Implement DownloadThreadPool**

`DownloadThreadPool.h`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 */

#pragma once

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include <functional>

namespace facebook::velox::cache::fs {

/// Thin wrapper over folly::IOThreadPoolExecutor for asynchronous segment
/// downloads. IO-bound (no CPU-heavy work) so does NOT share with Velox's
/// CPU executor (spec §7.1 §10 R4).
class DownloadThreadPool {
 public:
  explicit DownloadThreadPool(size_t numThreads);
  ~DownloadThreadPool();

  /// Submits a no-arg callable; returns a SemiFuture for completion.
  folly::SemiFuture<folly::Unit> submit(folly::Function<void()> task);

 private:
  folly::IOThreadPoolExecutor executor_;
};

} // namespace facebook::velox::cache::fs
```

`DownloadThreadPool.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 */

#include "velox/common/caching/fscache/DownloadThreadPool.h"

#include <folly/executors/thread_factory/NamedThreadFactory.h>

namespace facebook::velox::cache::fs {

namespace {
size_t checkedNumThreads(size_t numThreads) {
  VELOX_CHECK_GT(numThreads, 0, "DownloadThreadPool needs at least 1 thread");
  // Cap at 32 even if config asks for more: each thread holds an OS-level
  // pread slot against the remote, and beyond ~32 the remote (S3, HDFS)
  // starts throttling and we lose more to contention than we gain in
  // parallelism. Misconfiguration that would otherwise silently regress
  // p99 fetch latency is bounded here. If 32 turns out to be wrong, raise
  // after measurement (see Task 16 perf gate).
  return std::min<size_t>(numThreads, 32);
}
} // namespace

DownloadThreadPool::DownloadThreadPool(size_t numThreads)
    : executor_{
          checkedNumThreads(numThreads),
          std::make_shared<folly::NamedThreadFactory>("FsCacheDownload")} {}

DownloadThreadPool::~DownloadThreadPool() {
  executor_.join();
}

folly::SemiFuture<folly::Unit> DownloadThreadPool::submit(
    folly::Function<void()> task) {
  return folly::via(&executor_, std::move(task));
}

} // namespace facebook::velox::cache::fs
```

Add `downloadThreads{8}` to `FsCacheConfig.h`.

`FsCache` constructor stores `std::unique_ptr<DownloadThreadPool> downloadPool_` initialized from `config_.downloadThreads`. Expose via `DownloadThreadPool& downloadPool()` accessor.

- [ ] **Step 4: Switch FsCacheBufferedInput::load to async**

```cpp
void FsCacheBufferedInput::load(LogType) {
  for (auto& enq : enqueuedRegions_) {
    if (enq.holder != nullptr) {
      continue;
    }
    enq.holder = fsCache_->getOrSet(
        input_->getName(),
        enq.region.offset,
        enq.region.length,
        fsCache_->config(),
        *input_->getReadFile(),
        IsPrefetch::kPrefetch);

    for (auto& seg : enq.holder->segments()) {
      if (seg->state() != FileSegment::State::kEmpty) {
        continue;
      }
      if (!seg->reserve(seg->key().size, fsCache_->config().cacheRoot)) {
        continue;
      }
      // Move ownership of state needed by the task into the closure.
      // seg is a shared_ptr; readFile is a shared_ptr; cacheRoot is a
      // string. No raw pointers escape the task.
      auto segCapture = seg;
      auto readFile = input_->getReadFile();
      fsCache_->downloadPool().submit(
          [segCapture, readFile]() mutable {
            try {
              constexpr uint64_t kChunk = 1UL << 20;
              std::vector<char> buf(
                  std::min<uint64_t>(kChunk, segCapture->key().size));
              uint64_t remaining = segCapture->key().size;
              uint64_t cursor = segCapture->key().offset;
              while (remaining > 0) {
                const uint64_t toRead =
                    std::min<uint64_t>(buf.size(), remaining);
                readFile->pread(cursor, toRead, buf.data());
                segCapture->write(buf.data(), toRead);
                cursor += toRead;
                remaining -= toRead;
              }
              segCapture->complete();
            } catch (...) {
              segCapture->abandon();
            }
          });
    }
  }
}
```

- [ ] **Step 5: Run all suites**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
    --target velox_fscache_test velox_dwio_common_test -j 16
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
    --gtest_filter='DownloadThreadPoolTest.*'
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
    --gtest_filter='FsCacheAsyncLoadTest.*:FsCacheBufferedInputTest.*:FsCacheInputStreamTest.*'
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/DownloadThreadPool.h \
        velox/common/caching/fscache/DownloadThreadPool.cpp \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/FsCacheConfig.h \
        velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/dwio/common/FsCacheBufferedInput.cpp \
        velox/common/caching/fscache/tests/DownloadThreadPoolTest.cpp \
        velox/dwio/common/tests/FsCacheAsyncLoadTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt \
        velox/dwio/common/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): async load via DownloadThreadPool

Adds DownloadThreadPool (folly::IOThreadPoolExecutor, default 8 threads
per spec §7.1) and switches FsCacheBufferedInput::load() to submit per
kEmpty segment instead of running the remote pread inline. load() now
returns once reserves are done; readers synchronise via
FileSegment::waitForDownloadedSize (Task 10).

Sentinel test FsCacheAsyncLoadTest::loadReturnsBeforeRemoteReadCompletes
locks in the async contract (<20ms load vs 8 MiB download).

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §7.1 §7.2 §10 R4

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: FileCacheQueryLimit + QueryLimitToken + bypass_cache_threshold

**Files:**
- Create: `velox/common/caching/fscache/FileCacheQueryLimit.h`
- Create: `velox/common/caching/fscache/FileCacheQueryLimit.cpp`
- Create: `velox/common/caching/fscache/tests/FileCacheQueryLimitTest.cpp`
- Create: `velox/dwio/common/tests/FsCacheBypassIntegrationTest.cpp` (new — proves bypass actually round-trips bytes via direct pread)
- Modify: `velox/common/caching/fscache/FsCacheConfig.h` — add `bypassThresholdBytes{0}` (default disabled per spec §8.3, matching ClickHouse `FILECACHE_BYPASS_THRESHOLD`-disabled default; set to a positive value, e.g. `256ULL << 20`, to enable)
- Modify: `velox/common/caching/fscache/FsCache.h` — add `getOrSet` bypass behaviour (no queryId in signature)
- Modify: `velox/common/caching/fscache/FsCache.cpp` — bypass short-circuit
- Modify: `velox/dwio/common/FsCacheBufferedInput.cpp` — `load()` detects empty holder (cache bypass) and one-shot preads the region into `enq.bypassBuffer`; `DeferredStream` adds a bypass branch that slices bytes directly from that buffer instead of constructing an `FsCacheInputStream`
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/FsCacheTest.cpp`
- Modify: `velox/dwio/common/tests/CMakeLists.txt` (register `FsCacheBypassIntegrationTest`)

**Spec:** §8.2 (FileCacheQueryLimit + QueryLimitToken — caller-held), §8.3 (bypass_cache_threshold — size-based, getOrSet-internal).

**Approach:**

Two **independent** mechanisms; the wiring is intentionally asymmetric.

**bypass_cache_threshold** (§8.3): size-based, lives inside `getOrSet`. If `config.bypassThresholdBytes > 0` and the requested region is `>= config.bypassThresholdBytes`, `getOrSet` returns an empty `FileSegmentsHolder` (no segments). This task implements both halves of the contract: (a) the cache-side short-circuit in `FsCache::getOrSet` via `shouldBypass`, and (b) the caller-side fallback in `FsCacheBufferedInput::load` + `DeferredStream`, which does a one-shot `pread` of the whole region into `EnqueuedRegion::bypassBuffer` and serves subsequent `Next()` calls directly from that buffer. **Default is 0 (disabled), matching CH** (`FILECACHE_BYPASS_THRESHOLD` settings declaration is "Undocumented. Not recommended for use", default 0; see `src/Interpreters/FileCache/FileCacheSettings.cpp:55`). The mechanism is wired end-to-end so operators can opt in by raising the threshold; phase-1 does **not** rely on it for warm-set protection — that responsibility belongs to the per-query `QueryLimitToken` quota (caller-side wiring deferred to phase-3) plus LRU itself.

The bypass-buffer model is the Velox-side equivalent of ClickHouse's `ReadType::REMOTE_FS_READ_BYPASS_CACHE` path (`src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:322`). CH chunks the bypass read at `DBMS_DEFAULT_BUFFER_SIZE` (1 MiB) because its `ReadBuffer::nextImpl` interface is pull-style and cannot know the total length; the actual network cost is still one `setReadUntilPosition`-bounded HTTP range, not N round trips (see `CachedOnDiskReadBufferFromFile.cpp:1148-1149`). Velox's `BufferedInput::enqueue(Region)` already carries the full region length, so a single `ReadFile::pread(offset, length, buf)` is the natural — and equivalent-cost — mapping.

**FileCacheQueryLimit + QueryLimitToken** (§8.2): per-query bytes quota, **caller-held**. The intended runtime contract is: `QueryCtx` calls `FileCacheQueryLimit::reserveQuery(maxBytesPerQuery)` once at query start and holds the returned `QueryLimitToken` for the query's lifetime; **before** calling `getOrSet`, the caller invokes `token.tryReserve(size)`; on `false` the caller skips `getOrSet` and reads directly from the remote.

> **TODO (phase-3): caller wiring is deferred.** This task ships only the budget primitive (counter + token + cache-side registry) and `FileCacheQueryLimitTest` direct unit tests. **No caller in phase-1 calls `tryReserve`** — `HiveConnector::beginQuery` does not mint a token, `ConnectorQueryCtx` does not store one, and `FsCacheBufferedInput::enqueue` does not consume one. The classes added here are intentional **dead code** in phase-1, awaiting connector-side wiring in phase-3 (at which point the choice between caller-held tokens and CH-style thread-local query_id can also be revisited). Do **not** add wiring as part of this plan; the implementer should only verify that the unit tests pass.

The bypass short-circuit, by contrast, is live in phase-1 end-to-end: `FsCache::getOrSet` calls `shouldBypass`, `FsCacheBufferedInput::load` does the one-shot pread on empty holders, and `DeferredStream` serves bytes from the bypass buffer. No `queryId` parameter is added to `getOrSet` in this or any later task — the spec puts QueryLimit enforcement on the caller side via the token, not on the cache side via a map lookup.

- [ ] **Step 1: Write failing test — FileCacheQueryLimitTest**

Create `velox/common/caching/fscache/tests/FileCacheQueryLimitTest.cpp`:

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

#include "velox/common/caching/fscache/FileCacheQueryLimit.h"

#include "velox/common/base/tests/GTestUtils.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs {

TEST(FileCacheQueryLimitTest, reserveSucceedsUnderLimit) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(/*maxBytesPerQuery=*/1'000);
  EXPECT_TRUE(token->tryReserve(400));
  EXPECT_TRUE(token->tryReserve(500));
  EXPECT_EQ(token->reserved(), 900);
}

TEST(FileCacheQueryLimitTest, reserveRejectsBeyondLimit) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(/*maxBytesPerQuery=*/1'000);
  EXPECT_TRUE(token->tryReserve(800));
  EXPECT_FALSE(token->tryReserve(300));
  EXPECT_EQ(token->reserved(), 800);
}

TEST(FileCacheQueryLimitTest, releaseFreesCapacity) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(/*maxBytesPerQuery=*/1'000);
  ASSERT_TRUE(token->tryReserve(900));
  token->release(400);
  EXPECT_EQ(token->reserved(), 500);
  EXPECT_TRUE(token->tryReserve(400));
}

TEST(FileCacheQueryLimitTest, releaseChecksUnderflow) {
  FileCacheQueryLimit limit;
  auto token = limit.reserveQuery(/*maxBytesPerQuery=*/1'000);
  VELOX_ASSERT_THROW(token->release(1), "FileCacheQueryLimit underflow");
}

TEST(FileCacheQueryLimitTest, tokenDestructorReleasesAll) {
  FileCacheQueryLimit limit;
  {
    auto token = limit.reserveQuery(/*maxBytesPerQuery=*/1'000);
    ASSERT_TRUE(token->tryReserve(700));
  }
  // After token dtor, FileCacheQueryLimit-side accounting (used for
  // cluster-wide caps in a later phase) is back to zero.
  EXPECT_EQ(limit.totalReserved(), 0);
}

} // namespace facebook::velox::cache::fs
```

Add to `velox/common/caching/fscache/tests/CMakeLists.txt`:

```cmake
add_executable(velox_file_cache_query_limit_test FileCacheQueryLimitTest.cpp)
target_link_libraries(
  velox_file_cache_query_limit_test
  velox_fscache
  velox_exception
  GTest::gtest
  GTest::gtest_main)
add_test(NAME velox_file_cache_query_limit_test COMMAND velox_file_cache_query_limit_test)
```

- [ ] **Step 2: Run — expected RED (header missing)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_file_cache_query_limit_test -j 8
```

Expected output: compile error `fatal error: velox/common/caching/fscache/FileCacheQueryLimit.h: No such file or directory`.

- [ ] **Step 3: Implement FileCacheQueryLimit + QueryLimitToken**

Create `velox/common/caching/fscache/FileCacheQueryLimit.h`:

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

#include <atomic>
#include <cstdint>
#include <memory>

namespace facebook::velox::cache::fs {

class FileCacheQueryLimit;

/// Per-query reservation handle. Held by QueryCtx (or any caller scoping a
/// budget); callers invoke tryReserve() BEFORE getOrSet and read direct from
/// remote on `false`. release() is invoked when bytes belonging to the
/// query leave the cache (eviction, holder dtor on partial download, etc).
///
/// Thread-safe: backed by a single atomic counter with CAS reservation.
/// Token destructor drops the token's contribution from the parent
/// FileCacheQueryLimit's totalReserved counter — used later for cluster-wide
/// caps. The token itself does NOT release cached bytes on dtor; that is
/// the caller's responsibility (a holder kept alive past query end is a
/// caller bug).
class QueryLimitToken {
 public:
  QueryLimitToken(FileCacheQueryLimit* parent, uint64_t maxBytes);
  ~QueryLimitToken();

  QueryLimitToken(const QueryLimitToken&) = delete;
  QueryLimitToken& operator=(const QueryLimitToken&) = delete;
  QueryLimitToken(QueryLimitToken&&) = delete;
  QueryLimitToken& operator=(QueryLimitToken&&) = delete;

  /// Attempts to reserve `bytes` against this token's budget. Returns true
  /// and bumps the counter on success; returns false unchanged if it would
  /// exceed maxBytes.
  bool tryReserve(uint64_t bytes);

  /// Releases `bytes` previously reserved. Throws (via VELOX_CHECK_GE) if
  /// this would underflow.
  void release(uint64_t bytes);

  uint64_t reserved() const {
    return reserved_.load(std::memory_order_acquire);
  }

  uint64_t maxBytes() const {
    return maxBytes_;
  }

 private:
  FileCacheQueryLimit* const parent_;
  const uint64_t maxBytes_;
  std::atomic<uint64_t> reserved_{0};
};

/// Factory + cluster-wide accounting for QueryLimitToken. One instance
/// lives on FsCache; QueryCtx calls reserveQuery() once per query and holds
/// the returned token.
class FileCacheQueryLimit {
 public:
  /// Mints a new token with a per-query budget of `maxBytesPerQuery`.
  std::unique_ptr<QueryLimitToken> reserveQuery(uint64_t maxBytesPerQuery);

  /// Sum of `reserved()` across all live tokens. Used by future cluster
  /// limits; exposed now to keep the dtor-release invariant testable.
  uint64_t totalReserved() const {
    return totalReserved_.load(std::memory_order_acquire);
  }

  /// Called by QueryLimitToken on each successful tryReserve(). Public
  /// because we deliberately avoid `friend` (project style); the contract
  /// is "tokens own the counter, FileCacheQueryLimit owns the sum". Tests
  /// must not call these directly.
  void onTokenReserve(uint64_t bytes) {
    totalReserved_.fetch_add(bytes, std::memory_order_acq_rel);
  }
  void onTokenRelease(uint64_t bytes) {
    totalReserved_.fetch_sub(bytes, std::memory_order_acq_rel);
  }

 private:
  std::atomic<uint64_t> totalReserved_{0};
};

} // namespace facebook::velox::cache::fs
```

Create `velox/common/caching/fscache/FileCacheQueryLimit.cpp`:

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

#include "velox/common/caching/fscache/FileCacheQueryLimit.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::cache::fs {

QueryLimitToken::QueryLimitToken(
    FileCacheQueryLimit* parent,
    uint64_t maxBytes)
    : parent_{parent}, maxBytes_{maxBytes} {}

QueryLimitToken::~QueryLimitToken() {
  const auto held = reserved_.load(std::memory_order_acquire);
  if (held > 0) {
    parent_->onTokenRelease(held);
  }
}

bool QueryLimitToken::tryReserve(uint64_t bytes) {
  auto cur = reserved_.load(std::memory_order_acquire);
  while (true) {
    if (cur + bytes > maxBytes_) {
      return false;
    }
    if (reserved_.compare_exchange_weak(
            cur,
            cur + bytes,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      parent_->onTokenReserve(bytes);
      return true;
    }
  }
}

void QueryLimitToken::release(uint64_t bytes) {
  // fetch_sub returns the value before subtraction. Underflow check on the
  // pre-decrement value detects releases larger than the live reservation.
  const auto prev = reserved_.fetch_sub(bytes, std::memory_order_acq_rel);
  VELOX_CHECK_GE(
      prev,
      bytes,
      "FileCacheQueryLimit underflow: prev={} bytes={}",
      prev,
      bytes);
  parent_->onTokenRelease(bytes);
}

std::unique_ptr<QueryLimitToken> FileCacheQueryLimit::reserveQuery(
    uint64_t maxBytesPerQuery) {
  return std::make_unique<QueryLimitToken>(this, maxBytesPerQuery);
}

} // namespace facebook::velox::cache::fs
```

Add to `velox/common/caching/fscache/CMakeLists.txt` in the `velox_fscache` library `SOURCES` list:

```cmake
FileCacheQueryLimit.cpp
```

- [ ] **Step 4: Run — expected GREEN for FileCacheQueryLimitTest**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_file_cache_query_limit_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R velox_file_cache_query_limit_test -V
```

Expected: `[  PASSED  ] 5 tests.`

- [ ] **Step 5: Write failing test — bypassThresholdBytes in FsCacheTest**

Add to `velox/common/caching/fscache/tests/FsCacheTest.cpp` next to existing getOrSet tests:

```cpp
TEST_F(FsCacheTest, getOrSetReturnsEmptyHolderWhenRegionExceedsBypassThreshold) {
  FsCacheConfig cfg = baseConfig();
  cfg.bypassThresholdBytes = 4 * kMiB;
  FsCache cache{cfg};
  auto remote = makeBlob(/*bytes=*/16 * kMiB);

  auto holder = cache.getOrSet(
      "blob",
      /*offset=*/0,
      /*size=*/8 * kMiB,
      cfg,
      *remote,
      IsPrefetch::kDemand);

  EXPECT_TRUE(holder->empty());
  EXPECT_EQ(cache.totalSize(), 0);
}

TEST_F(FsCacheTest, getOrSetReturnsHolderUnderBypassThreshold) {
  FsCacheConfig cfg = baseConfig();
  cfg.bypassThresholdBytes = 4 * kMiB;
  FsCache cache{cfg};
  auto remote = makeBlob(/*bytes=*/16 * kMiB);

  auto holder = cache.getOrSet(
      "blob",
      /*offset=*/0,
      /*size=*/2 * kMiB,
      cfg,
      *remote,
      IsPrefetch::kDemand);

  EXPECT_FALSE(holder->empty());
}

TEST_F(FsCacheTest, shouldBypassMatchesGetOrSetBoundary) {
  FsCacheConfig cfg = baseConfig();
  cfg.bypassThresholdBytes = 4 * kMiB;
  FsCache cache{cfg};

  EXPECT_FALSE(cache.shouldBypass(4 * kMiB - 1));
  EXPECT_TRUE(cache.shouldBypass(4 * kMiB));
  EXPECT_TRUE(cache.shouldBypass(8 * kMiB));
}

TEST_F(FsCacheTest, shouldBypassDisabledWhenThresholdIsZero) {
  FsCacheConfig cfg = baseConfig();
  cfg.bypassThresholdBytes = 0;
  FsCache cache{cfg};

  EXPECT_FALSE(cache.shouldBypass(0));
  EXPECT_FALSE(cache.shouldBypass(1ULL << 40));
}

TEST_F(FsCacheTest, totalSizeStartsAtZeroAndStaysZeroOnBypass) {
  FsCacheConfig cfg = baseConfig();
  cfg.bypassThresholdBytes = 4 * kMiB;
  FsCache cache{cfg};
  EXPECT_EQ(cache.totalSize(), 0);

  auto remote = makeBlob(/*bytes=*/16 * kMiB);
  (void)cache.getOrSet(
      "blob", 0, 8 * kMiB, cfg, *remote, IsPrefetch::kDemand);
  // Bypass path returns empty holder and writes nothing to disk; the
  // counter must remain zero.
  EXPECT_EQ(cache.totalSize(), 0);
}
```

Per-query enforcement is covered end-to-end in Task 14's BufferedInput integration test once `QueryLimitToken` is wired into the read path. Token-level reserve/release behaviour is already covered by `FileCacheQueryLimitTest` above.

- [ ] **Step 6: Run — expected RED**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 8
```

Expected: compile error on `cfg.bypassThresholdBytes` (field not yet declared).

- [ ] **Step 7: Implement bypass short-circuit**

Modify `velox/common/caching/fscache/FsCacheConfig.h` — add field below existing maxSegmentSize:

```cpp
/// Skip the cache for any single getOrSet request whose `size` is greater
/// than or equal to this value. Set to 0 to disable the bypass entirely
/// (every request enters the cache regardless of size).
///
/// Default 0 (disabled), matching ClickHouse's `FILECACHE_BYPASS_THRESHOLD`
/// behaviour (`src/Interpreters/FileCache/FileCacheSettings.cpp:55` declares
/// `bypass_cache_threshold` as "Undocumented. Not recommended for use" with
/// default 0; enabling additionally requires `enable_bypass_cache_with_threshold`).
/// Rationale: CH relies on per-query `filesystem_cache_max_download_size`
/// quotas + LRU itself to keep large scans from evicting the warm working set;
/// bypass is a safety valve, not the primary defence. Phase-1 ships the
/// mechanism but defaults it off until the caller-side QueryLimitToken
/// wiring lands in phase-3 and effectiveness can be re-validated with TPC-H
/// and microbench under both settings (spec §8.3).
uint64_t bypassThresholdBytes{0};
```

Modify `velox/common/caching/fscache/FsCache.h` — update the existing `getOrSet` doc comment to mention the new bypass behaviour (signature stays at the 6-param form from Task 8; no queryId is ever added), and declare `shouldBypass` next to `getOrSet`:

```cpp
/// Returns FileSegments covering `[offset, offset+size)` of `path`,
/// fetching from `remote` as needed.
///
/// Returns an empty holder (FileSegmentsHolder with no segments) if
/// `shouldBypass(size)` is true. Callers must fall back to reading
/// directly from `remote` in that case. Per-query budget enforcement is
/// **caller-side**: the caller checks its QueryLimitToken via
/// tryReserve() before invoking getOrSet (see spec §8.2).
FileSegmentsHolderPtr getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& settings,
    ::facebook::velox::ReadFile& remote,
    IsPrefetch isPrefetch);

/// Returns true if a single-request of `size` bytes should bypass the
/// cache. Equivalent to
/// `config_.bypassThresholdBytes > 0 && size >= config_.bypassThresholdBytes`.
/// Exposed publicly so callers (e.g. metrics, debug logs) can ask the
/// same question without re-deriving the comparison. Cheap, lock-free.
bool shouldBypass(uint64_t size) const;

/// Returns current on-disk bytes accounted by this cache instance.
/// Approximate (relaxed atomic load); intended for tests, metrics, and
/// debug logs. Mirrors ClickHouse's `FileCache::getUsedCacheSize()`
/// (src/Interpreters/FileCache/FileCache.h:199), which is also a
/// relaxed counter snapshot used for metrics. Equivalent to reading
/// `stats().bytesOnDisk` but avoids constructing the POD snapshot.
uint64_t totalSize() const;
```

Modify `velox/common/caching/fscache/FsCache.cpp` — implement `shouldBypass` and call it from the top of `getOrSet`, before any lock acquisition:

```cpp
bool FsCache::shouldBypass(uint64_t size) const {
  return config_.bypassThresholdBytes > 0 &&
      size >= config_.bypassThresholdBytes;
}

uint64_t FsCache::totalSize() const {
  return counters_.bytesOnDisk.load(std::memory_order_relaxed);
}

FileSegmentsHolderPtr FsCache::getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& settings,
    ::facebook::velox::ReadFile& remote,
    IsPrefetch isPrefetch) {
  // Bypass cache entirely for very large single-region requests. Done
  // before any lock so a misconfigured huge scan doesn't even touch the
  // bucket hierarchy.
  if (shouldBypass(size)) {
    return std::make_unique<FileSegmentsHolder>();
  }
  // ... existing body unchanged
}
```

Update `velox/common/caching/fscache/CMakeLists.txt` `velox_fscache` SOURCES to include `FileCacheQueryLimit.cpp` (also added in Step 3).

- [ ] **Step 8: Run — expected GREEN for FsCacheTest bypass cases + FileCacheQueryLimitTest**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test velox_file_cache_query_limit_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R 'velox_fscache_test|velox_file_cache_query_limit_test' -V
```

Expected: all FsCacheTest cases pass, including the 2 new bypass cases; FileCacheQueryLimitTest 5/5 PASS.

- [ ] **Step 9: Write failing integration test — FsCacheBypassIntegrationTest**

Create `velox/dwio/common/tests/FsCacheBypassIntegrationTest.cpp`. This test exercises the full caller-side path that step 7 alone cannot reach: the `FsCacheBufferedInput::load → DeferredStream::Next` round trip when `getOrSet` returns an empty holder. **Without** the wiring added in step 11, `DeferredStream::ensureWithData` hits the existing `VELOX_CHECK(!slot_->holder->empty(), …)` and crashes.

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * (license header)
 */

#include <gtest/gtest.h>

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/FsCacheBufferedInput.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::common::test::TempDirectoryPath;

class FsCacheBypassIntegrationTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kMiB = 1ULL << 20;

  void SetUp() override {
    filesystems::registerLocalFileSystem();
    tempDir_ = TempDirectoryPath::create();
    pool_ = memory::memoryManager()->addLeafPool();
  }

  // Writes `bytes` of deterministic content (byte = offset % 251) to a
  // file under tempDir_ and returns its absolute path.
  std::string writeBlob(uint64_t bytes) {
    const auto path = tempDir_->getPath() + "/blob";
    std::vector<char> data(bytes);
    for (uint64_t i = 0; i < bytes; ++i) {
      data[i] = static_cast<char>(i % 251);
    }
    auto fs = filesystems::getFileSystem(path, nullptr);
    auto sink = fs->openFileForWrite(path);
    sink->append(std::string_view{data.data(), data.size()});
    sink->close();
    return path;
  }

  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

// 4 MiB bypass threshold + 8 MiB region → getOrSet returns empty holder;
// BufferedInput::load must pread the full region into bypassBuffer;
// DeferredStream::Next must return those exact bytes (no cache file is
// created, and no segments live in metadata).
TEST_F(FsCacheBypassIntegrationTest, regionAboveThresholdRoundTripsViaDirectPread) {
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir_->getPath() + "/cache";
  cfg.bypassThresholdBytes = 4 * kMiB;
  // alignment / maxSegmentSize: defaults (4 MiB / 32 MiB) — bypass path
  // never enters splitRange, so these are not exercised here. Setting
  // a custom maxSegmentSize without also overriding alignment would
  // trip FsCache's ctor invariant `maxSegmentSize % alignment == 0`.
  std::filesystem::create_directories(cfg.cacheRoot);
  FsCache cache{cfg};

  const auto path = writeBlob(8 * kMiB);
  auto fs = filesystems::getFileSystem(path, nullptr);
  auto readFile = fs->openFileForRead(path);
  auto input = std::make_unique<FsCacheBufferedInput>(
      std::move(readFile), *pool_, &cache);

  auto stream = input->enqueue(velox::common::Region{0, 8 * kMiB}, nullptr);
  input->load(LogType::FILE);

  // Drain the stream and rebuild the bytes we saw.
  std::vector<char> seen;
  seen.reserve(8 * kMiB);
  const void* buf{nullptr};
  int32_t size{0};
  while (stream->Next(&buf, &size)) {
    seen.insert(
        seen.end(),
        static_cast<const char*>(buf),
        static_cast<const char*>(buf) + size);
  }
  ASSERT_EQ(seen.size(), 8 * kMiB);
  for (uint64_t i = 0; i < seen.size(); ++i) {
    ASSERT_EQ(static_cast<uint8_t>(seen[i]), static_cast<uint8_t>(i % 251))
        << "byte " << i;
  }

  // No segments were created (cache stayed cold).
  EXPECT_EQ(cache.totalSize(), 0);
  EXPECT_TRUE(std::filesystem::is_empty(cfg.cacheRoot));
}

// Sanity check: a region UNDER the threshold should still go through
// the regular cache path (proves the new branch did not break the
// non-bypass case).
TEST_F(FsCacheBypassIntegrationTest, regionBelowThresholdStillCaches) {
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir_->getPath() + "/cache";
  cfg.bypassThresholdBytes = 4 * kMiB;
  // Same defaults as above; non-bypass path does enter splitRange but
  // 2 MiB request fits in one default 32 MiB segment.
  std::filesystem::create_directories(cfg.cacheRoot);
  FsCache cache{cfg};

  const auto path = writeBlob(8 * kMiB);
  auto fs = filesystems::getFileSystem(path, nullptr);
  auto readFile = fs->openFileForRead(path);
  auto input = std::make_unique<FsCacheBufferedInput>(
      std::move(readFile), *pool_, &cache);

  auto stream = input->enqueue(velox::common::Region{0, 2 * kMiB}, nullptr);
  input->load(LogType::FILE);

  const void* buf{nullptr};
  int32_t size{0};
  uint64_t total = 0;
  while (stream->Next(&buf, &size)) {
    total += size;
  }
  EXPECT_EQ(total, 2 * kMiB);
  EXPECT_GT(cache.totalSize(), 0);
}

} // namespace facebook::velox::dwio::common::test
```

Register the new binary in `velox/dwio/common/tests/CMakeLists.txt` (mirror `FsCacheBufferedInputTest`).

- [ ] **Step 10: Run — expected RED (DeferredStream VELOX_CHECK fires)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fs_cache_bypass_integration_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R 'velox_fs_cache_bypass_integration_test' -V
```

Expected: `regionAboveThresholdRoundTripsViaDirectPread` aborts in `DeferredStream::ensureWithData` with `"Stream used before FsCacheBufferedInput::load()"` (the existing check fires because `holder->empty()` is now true). `regionBelowThresholdStillCaches` passes.

- [ ] **Step 11: Wire bypass fallback into FsCacheBufferedInput + DeferredStream**

Modify `velox/dwio/common/FsCacheBufferedInput.cpp` in two places. Add
`#include <limits>` to the header list (used by `DeferredStream::Next`
to clamp the bypass-buffer slice to `INT32_MAX`).

First, extend `load()` to handle the empty-holder case. The latest version of `load()` is the async one from Task 11 step 4 (line 2642 above). Append the bypass branch after the `getOrSet` call, before the segment-driving loop:

```cpp
void FsCacheBufferedInput::load(LogType) {
  for (auto& enq : enqueuedRegions_) {
    if (enq.holder != nullptr) {
      continue;
    }
    enq.holder = fsCache_->getOrSet(
        input_->getName(),
        enq.region.offset,
        enq.region.length,
        fsCache_->config(),
        *input_->getReadFile(),
        IsPrefetch::kPrefetch);

    if (enq.holder->empty()) {
      // Cache bypassed this request (size >= bypassThresholdBytes). Read
      // the full region directly from remote into the slot-owned buffer;
      // DeferredStream serves bytes from there. One pread keeps the
      // network cost at one HTTP range per region, matching CH's
      // `setReadUntilPosition(file_segment.range().right + 1)` path
      // (src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:1149).
      enq.bypassBuffer.assign(enq.region.length, '\0');
      input_->getReadFile()->pread(
          enq.region.offset, enq.region.length, enq.bypassBuffer.data());
      continue;
    }

    for (auto& seg : enq.holder->segments()) {
      // (unchanged from Task 11 step 4 — async submit per kEmpty segment)
      ...
    }
  }
}
```

Second, teach `DeferredStream` to serve from `bypassBuffer` when the slot's holder is empty. The current shape (from Task 8 step 3) constructs an `FsCacheInputStream` inside `ensureWithData()`. Replace the unconditional `VELOX_CHECK(holder != nullptr)` + `FsCacheInputStream` construction with:

```cpp
void DeferredStream::ensureWithData() {
  if (inner_ != nullptr || bypassActive_) {
    return;
  }
  VELOX_CHECK(
      slot_->holder != nullptr,
      "Stream used before FsCacheBufferedInput::load()");
  if (slot_->holder->empty()) {
    // Bypass path. Bytes already in slot_->bypassBuffer (load() preads
    // the full region). Drive Next() / SkipInt64 / seekToPosition from
    // the buffer; never construct an FsCacheInputStream because no
    // segments exist.
    bypassActive_ = true;
    VELOX_CHECK_EQ(
        slot_->bypassBuffer.size(),
        slot_->region.length,
        "Bypass buffer size must match region length");
    return;
  }
  inner_ = std::make_unique<FsCacheInputStream>(
      slot_->holder->segments(),
      slot_->region.offset,
      slot_->region.length,
      cache_->config().cacheRoot);
  if (bytesConsumed_ > 0) {
    const bool ok = inner_->SkipInt64(static_cast<int64_t>(bytesConsumed_));
    VELOX_CHECK(ok, "Replaying pre-load skip past region end");
  }
}
```

Add the bypass branches to each `SeekableInputStream` override on `DeferredStream`:

```cpp
bool DeferredStream::Next(const void** data, int32_t* size) {
  ensureWithData();
  if (bypassActive_) {
    if (bytesConsumed_ >= slot_->bypassBuffer.size()) {
      return false;
    }
    const uint64_t remaining = slot_->bypassBuffer.size() - bytesConsumed_;
    // Clamp to INT32_MAX so caller drains via multiple Next() calls if
    // the region exceeds 2 GiB. The default bypass threshold makes this
    // unreachable, but operators can raise bypassThresholdBytes and a
    // single int32_t cast would silently truncate the returned size.
    const uint64_t chunk = std::min<uint64_t>(
        remaining, static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
    *data = slot_->bypassBuffer.data() + bytesConsumed_;
    *size = static_cast<int32_t>(chunk);
    bytesConsumed_ += chunk;
    return true;
  }
  return inner_->Next(data, size);
}

bool DeferredStream::SkipInt64(int64_t count) {
  if (count < 0) {
    return false;
  }
  if (bypassActive_) {
    const uint64_t newPos = std::min<uint64_t>(
        slot_->bypassBuffer.size(),
        bytesConsumed_ + static_cast<uint64_t>(count));
    const bool fits =
        newPos == bytesConsumed_ + static_cast<uint64_t>(count);
    bytesConsumed_ = newPos;
    return fits;
  }
  if (inner_ != nullptr) {
    return inner_->SkipInt64(count);
  }
  const auto unsignedCount = static_cast<uint64_t>(count);
  const uint64_t newPos = std::min<uint64_t>(
      slot_->region.length, bytesConsumed_ + unsignedCount);
  const bool fits = newPos == bytesConsumed_ + unsignedCount;
  bytesConsumed_ = newPos;
  return fits;
}

void DeferredStream::BackUp(int32_t count) {
  if (bypassActive_) {
    VELOX_CHECK_GE(count, 0);
    VELOX_CHECK_LE(static_cast<uint64_t>(count), bytesConsumed_);
    bytesConsumed_ -= count;
    return;
  }
  VELOX_CHECK_NOT_NULL(
      inner_, "BackUp called before any Next() -- no buffer to back up");
  inner_->BackUp(count);
}

int64_t DeferredStream::ByteCount() const {
  if (bypassActive_ || inner_ == nullptr) {
    return static_cast<int64_t>(bytesConsumed_);
  }
  return inner_->ByteCount();
}

void DeferredStream::seekToPosition(PositionProvider& position) {
  ensureWithData();
  if (bypassActive_) {
    const uint64_t target = position.next();
    VELOX_CHECK_LE(target, slot_->bypassBuffer.size());
    bytesConsumed_ = target;
    return;
  }
  inner_->seekToPosition(position);
}
```

Add the new member to `DeferredStream`:

```cpp
bool bypassActive_{false};
```

- [ ] **Step 12: Run — expected GREEN**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fs_cache_bypass_integration_test velox_dwio_common_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R 'velox_fs_cache_bypass_integration_test|FsCacheBufferedInputTest' -V
```

Expected: both bypass integration tests PASS and the existing `FsCacheBufferedInputTest` suite still PASSes (proves the bypass branch did not regress the cache-on path).

- [ ] **Step 13: Commit**

```bash
git add \
  velox/common/caching/fscache/FileCacheQueryLimit.h \
  velox/common/caching/fscache/FileCacheQueryLimit.cpp \
  velox/common/caching/fscache/FsCacheConfig.h \
  velox/common/caching/fscache/FsCache.h \
  velox/common/caching/fscache/FsCache.cpp \
  velox/common/caching/fscache/CMakeLists.txt \
  velox/common/caching/fscache/tests/FileCacheQueryLimitTest.cpp \
  velox/common/caching/fscache/tests/FsCacheTest.cpp \
  velox/common/caching/fscache/tests/CMakeLists.txt \
  velox/dwio/common/FsCacheBufferedInput.cpp \
  velox/dwio/common/tests/FsCacheBypassIntegrationTest.cpp \
  velox/dwio/common/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): QueryLimitToken + bypass_cache_threshold

Lands the two FsCache-side admission primitives from spec §8.2 / §8.3.
The two mechanisms are intentionally asymmetric in how they integrate:

  - bypass_cache_threshold (§8.3) is size-based and lives **inside**
    getOrSet. If config.bypassThresholdBytes > 0 and size >= threshold,
    getOrSet returns an empty FileSegmentsHolder before touching any
    bucket lock. Caller falls through to direct remote read. Keeps
    full-table scans from evicting the warm working set.

  - FileCacheQueryLimit + QueryLimitToken (§8.2) is per-query and lives
    on the **caller** side. QueryCtx calls reserveQuery() once and
    holds the QueryLimitToken; before each getOrSet the caller invokes
    token.tryReserve(size) and reads direct from remote on `false`.
    getOrSet itself never sees a queryId — enforcement is the caller's.

Phase-1 scope: the bypass short-circuit is live end-to-end. getOrSet
calls shouldBypass at entry and FsCacheBufferedInput::load detects the
resulting empty holder, preads the full region in one shot into
EnqueuedRegion::bypassBuffer, and DeferredStream serves subsequent
Next() calls directly from that buffer (no FsCacheInputStream is
constructed on the bypass path). FsCacheBypassIntegrationTest covers
both branches: above-threshold requests round-trip bytes via direct
pread with the cache staying cold, below-threshold requests still
populate the cache. FileCacheQueryLimit + QueryLimitToken land here
with their unit tests but are **intentional dead code in phase-1** —
no caller mints or consumes a token. Connector-side wiring
(HiveConnector + ConnectorQueryCtx + token-driven enqueue) is deferred
to phase-3, at which point we can also re-evaluate whether to keep the
caller-held-token model or fall back to CH's thread-local query_id.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §8.2 §8.3 §10 R7

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: SlruPolicy (probation + protected lists)

**Files:**
- Create: `velox/common/caching/fscache/SlruPolicy.h`
- Create: `velox/common/caching/fscache/SlruPolicy.cpp`
- Create: `velox/common/caching/fscache/tests/SlruPolicyTest.cpp`
- Modify: `velox/common/caching/fscache/FsCacheConfig.h` — add `slruProtectedRatio{0.5}` and `enableSlru{false}` (opt-in)
- Modify: `velox/common/caching/fscache/FsCache.h` / `.cpp` — wire policy under config flag
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Spec:** §8.1 (SLRU two-list eviction with probation → protected promotion on second hit).

**Approach:**
SLRU = "Segmented LRU". Two intrusive LRU lists:
- **Probation** (capacity = `(1 - slruProtectedRatio) * maxCacheSize`): every newly cached segment lands here.
- **Protected** (capacity = `slruProtectedRatio * maxCacheSize`): a segment is promoted here on its **second** access (the access that would re-hit it in probation). If protected overflows, the oldest protected segment is **demoted** back to the MRU end of probation (not evicted). Eviction only happens from the LRU end of probation.

Phase-0 independent: this task does not depend on Tasks 1-12 (per spec §11 task graph). Can be developed in parallel.

Phase-1: opt-in via `config.enableSlru`. Default eviction stays the current single-LRU until a follow-up flips the default.

- [ ] **Step 1: Write failing test — SlruPolicyTest**

Create `velox/common/caching/fscache/tests/SlruPolicyTest.cpp`:

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

#include "velox/common/caching/fscache/SlruPolicy.h"

#include <gtest/gtest.h>
#include <vector>

namespace facebook::velox::cache::fs {
namespace {

struct FakeEntry {
  uint64_t id;
  uint64_t size;
};

class SlruPolicyTest : public ::testing::Test {
 protected:
  // 4-byte protected, 6-byte probation = 10 total, 0.4 protected ratio.
  SlruPolicy<FakeEntry> policy_{/*maxBytes=*/10, /*protectedRatio=*/0.4};
};

TEST_F(SlruPolicyTest, freshInsertsLandInProbation) {
  auto a = std::make_shared<FakeEntry>(FakeEntry{1, 3});
  auto b = std::make_shared<FakeEntry>(FakeEntry{2, 3});
  policy_.insert(a);
  policy_.insert(b);
  EXPECT_EQ(policy_.probationBytes(), 6);
  EXPECT_EQ(policy_.protectedBytes(), 0);
}

TEST_F(SlruPolicyTest, secondTouchPromotesToProtected) {
  auto a = std::make_shared<FakeEntry>(FakeEntry{1, 3});
  policy_.insert(a);
  policy_.touch(a); // second access -> promote
  EXPECT_EQ(policy_.probationBytes(), 0);
  EXPECT_EQ(policy_.protectedBytes(), 3);
}

TEST_F(SlruPolicyTest, evictionPullsFromLruEndOfProbation) {
  auto a = std::make_shared<FakeEntry>(FakeEntry{1, 3});
  auto b = std::make_shared<FakeEntry>(FakeEntry{2, 3});
  auto c = std::make_shared<FakeEntry>(FakeEntry{3, 3});
  policy_.insert(a); // probation
  policy_.insert(b); // probation, MRU
  policy_.insert(c); // probation, MRU; total probation = 9 (cap 6) -> evict a
  auto evicted = policy_.evictUntilUnder(/*targetBytes=*/6);
  ASSERT_EQ(evicted.size(), 1);
  EXPECT_EQ(evicted[0]->id, 1);
  EXPECT_EQ(policy_.probationBytes(), 6);
}

TEST_F(SlruPolicyTest, protectedOverflowDemotesNotEvicts) {
  auto a = std::make_shared<FakeEntry>(FakeEntry{1, 3});
  auto b = std::make_shared<FakeEntry>(FakeEntry{2, 3});
  policy_.insert(a);
  policy_.touch(a); // a in protected (3 / 4)
  policy_.insert(b);
  policy_.touch(b); // would push protected to 6 > cap 4; oldest protected (a)
                    // demotes to MRU end of probation
  EXPECT_EQ(policy_.protectedBytes(), 3); // only b
  EXPECT_EQ(policy_.probationBytes(), 3); // a back in probation
}

TEST_F(SlruPolicyTest, removeDropsFromWhicheverList) {
  auto a = std::make_shared<FakeEntry>(FakeEntry{1, 3});
  auto b = std::make_shared<FakeEntry>(FakeEntry{2, 3});
  policy_.insert(a);
  policy_.insert(b);
  policy_.touch(b); // b protected, a probation
  policy_.remove(a);
  EXPECT_EQ(policy_.probationBytes(), 0);
  EXPECT_EQ(policy_.protectedBytes(), 3);
  policy_.remove(b);
  EXPECT_EQ(policy_.protectedBytes(), 0);
}

} // namespace
} // namespace facebook::velox::cache::fs
```

Append to `velox/common/caching/fscache/tests/CMakeLists.txt`:

```cmake
add_executable(velox_slru_policy_test SlruPolicyTest.cpp)
target_link_libraries(
  velox_slru_policy_test
  velox_fscache
  GTest::gtest
  GTest::gtest_main)
add_test(NAME velox_slru_policy_test COMMAND velox_slru_policy_test)
```

- [ ] **Step 2: Run — expected RED (header missing)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_slru_policy_test -j 8
```

Expected: `fatal error: velox/common/caching/fscache/SlruPolicy.h: No such file or directory`.

- [ ] **Step 3: Implement SlruPolicy**

Create `velox/common/caching/fscache/SlruPolicy.h` (template header — small, behaviour is what we test; FileSegment-specific wiring lives in FsCache.cpp):

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

#include "velox/common/base/Exceptions.h"

#include <folly/container/F14Map.h>
#include <list>
#include <memory>
#include <vector>

namespace facebook::velox::cache::fs {

/// Segmented LRU eviction policy. Maintains two intrusive LRU lists,
/// "probation" and "protected". New inserts go to probation; touching an
/// entry that is in probation promotes it to protected. If protected
/// overflows its capacity, its LRU entry is demoted (moved back to the
/// MRU end of probation) rather than evicted. Eviction only ever removes
/// entries from the LRU end of probation.
///
/// `Entry` must expose `size` (bytes). The policy stores shared_ptr to
/// each entry; callers retain ownership.
///
/// Not thread-safe; FsCache holds the bucket / eviction locks.
template <typename Entry>
class SlruPolicy {
 public:
  SlruPolicy(uint64_t maxBytes, double protectedRatio)
      : protectedCap_{static_cast<uint64_t>(maxBytes * protectedRatio)},
        probationCap_{maxBytes - protectedCap_} {
    VELOX_CHECK_GT(maxBytes, 0);
    VELOX_CHECK_GE(protectedRatio, 0.0);
    VELOX_CHECK_LE(protectedRatio, 1.0);
  }

  void insert(const std::shared_ptr<Entry>& entry);
  void touch(const std::shared_ptr<Entry>& entry);
  void remove(const std::shared_ptr<Entry>& entry);
  std::vector<std::shared_ptr<Entry>> evictUntilUnder(uint64_t targetBytes);

  uint64_t probationBytes() const {
    return probationBytes_;
  }
  uint64_t protectedBytes() const {
    return protectedBytes_;
  }

 private:
  enum class List { kProbation, kProtected };

  struct Node {
    std::shared_ptr<Entry> entry;
    List list;
    typename std::list<std::shared_ptr<Entry>>::iterator pos;
  };

  void removeFromList(Node& node);
  void pushBackProbation(const std::shared_ptr<Entry>& entry);
  void pushBackProtected(const std::shared_ptr<Entry>& entry);

  const uint64_t protectedCap_;
  const uint64_t probationCap_;
  std::list<std::shared_ptr<Entry>> probation_;
  std::list<std::shared_ptr<Entry>> protected_;
  folly::F14FastMap<Entry*, Node> index_;
  uint64_t probationBytes_{0};
  uint64_t protectedBytes_{0};
};

} // namespace facebook::velox::cache::fs

#include "velox/common/caching/fscache/SlruPolicy-inl.h"
```

Create `velox/common/caching/fscache/SlruPolicy-inl.h`:

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

namespace facebook::velox::cache::fs {

template <typename Entry>
void SlruPolicy<Entry>::insert(const std::shared_ptr<Entry>& entry) {
  VELOX_CHECK(index_.find(entry.get()) == index_.end());
  pushBackProbation(entry);
}

template <typename Entry>
void SlruPolicy<Entry>::touch(const std::shared_ptr<Entry>& entry) {
  auto it = index_.find(entry.get());
  VELOX_CHECK(it != index_.end());
  auto& node = it->second;
  if (node.list == List::kProtected) {
    // Already protected; bump to MRU within protected.
    protected_.erase(node.pos);
    protected_.push_back(entry);
    node.pos = std::prev(protected_.end());
    return;
  }
  // Promote from probation to protected.
  probation_.erase(node.pos);
  probationBytes_ -= entry->size;
  index_.erase(it);
  pushBackProtected(entry);

  // Demote LRU protected entries back to probation while over capacity.
  while (protectedBytes_ > protectedCap_ && !protected_.empty()) {
    auto victim = protected_.front();
    protected_.pop_front();
    protectedBytes_ -= victim->size;
    index_.erase(victim.get());
    pushBackProbation(victim);
  }
}

template <typename Entry>
void SlruPolicy<Entry>::remove(const std::shared_ptr<Entry>& entry) {
  auto it = index_.find(entry.get());
  if (it == index_.end()) {
    return;
  }
  removeFromList(it->second);
  index_.erase(it);
}

template <typename Entry>
std::vector<std::shared_ptr<Entry>> SlruPolicy<Entry>::evictUntilUnder(
    uint64_t targetBytes) {
  std::vector<std::shared_ptr<Entry>> evicted;
  // Evict from LRU end of probation only. Protected entries are never
  // evicted directly; they can only leave the cache by being demoted to
  // probation first and then aging out.
  while (probationBytes_ > targetBytes && !probation_.empty()) {
    auto victim = probation_.front();
    probation_.pop_front();
    probationBytes_ -= victim->size;
    index_.erase(victim.get());
    evicted.push_back(std::move(victim));
  }
  return evicted;
}

template <typename Entry>
void SlruPolicy<Entry>::removeFromList(Node& node) {
  if (node.list == List::kProbation) {
    probation_.erase(node.pos);
    probationBytes_ -= node.entry->size;
  } else {
    protected_.erase(node.pos);
    protectedBytes_ -= node.entry->size;
  }
}

template <typename Entry>
void SlruPolicy<Entry>::pushBackProbation(
    const std::shared_ptr<Entry>& entry) {
  probation_.push_back(entry);
  probationBytes_ += entry->size;
  index_.emplace(
      entry.get(),
      Node{entry, List::kProbation, std::prev(probation_.end())});
}

template <typename Entry>
void SlruPolicy<Entry>::pushBackProtected(
    const std::shared_ptr<Entry>& entry) {
  protected_.push_back(entry);
  protectedBytes_ += entry->size;
  index_.emplace(
      entry.get(),
      Node{entry, List::kProtected, std::prev(protected_.end())});
}

} // namespace facebook::velox::cache::fs
```

Create `velox/common/caching/fscache/SlruPolicy.cpp` (empty translation unit so the library has at least one .o referencing the header — keeps explicit-instantiation hooks available for future):

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

#include "velox/common/caching/fscache/SlruPolicy.h"

// Intentionally empty: SlruPolicy is a header-only template. This file
// exists so a future non-template helper can be added without rewiring
// CMake.

namespace facebook::velox::cache::fs {} // namespace facebook::velox::cache::fs
```

Add to `velox/common/caching/fscache/CMakeLists.txt` `velox_fscache` SOURCES:

```cmake
SlruPolicy.cpp
```

- [ ] **Step 4: Run — expected GREEN for SlruPolicyTest**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_slru_policy_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R velox_slru_policy_test -V
```

Expected: `[  PASSED  ] 5 tests.`

- [ ] **Step 5: Wire SlruPolicy under config flag**

Modify `velox/common/caching/fscache/FsCacheConfig.h` — add fields:

```cpp
/// Enable SLRU eviction (two-list probation + protected). Default off
/// during phase-1 rollout; flipping this changes eviction order
/// system-wide, so it is opt-in until tpch-q22 perf validates it.
bool enableSlru{false};

/// Fraction of maxCacheSize reserved for the protected (frequent) list.
/// 0.5 matches ClickHouse default. Only meaningful when enableSlru.
double slruProtectedRatio{0.5};
```

Modify `velox/common/caching/fscache/FsCache.h` — add private member:

```cpp
std::unique_ptr<SlruPolicy<FileSegment>> slru_;
```

In `velox/common/caching/fscache/FsCache.cpp` ctor, construct conditionally:

```cpp
if (config_.enableSlru) {
  slru_ = std::make_unique<SlruPolicy<FileSegment>>(
      config_.maxCacheSize, config_.slruProtectedRatio);
}
```

In `getOrSet`, where existing single-LRU `lru_.touch(seg)` runs on a hit, branch:

```cpp
if (slru_ != nullptr) {
  slru_->touch(seg);
} else {
  lru_.touch(seg);
}
```

And where new segments are inserted:

```cpp
if (slru_ != nullptr) {
  slru_->insert(seg);
} else {
  lru_.insert(seg);
}
```

And where eviction runs (the existing `lru_.evictUntilUnder(...)` call):

```cpp
auto evicted = slru_ != nullptr
    ? slru_->evictUntilUnder(target)
    : lru_.evictUntilUnder(target);
```

- [ ] **Step 6: Run — full FsCache test suite, expected GREEN**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test velox_slru_policy_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R 'velox_fscache_test|velox_slru_policy_test' -V
```

Expected: all existing FsCacheTest cases still pass (enableSlru defaults to false, so behaviour is unchanged), plus SlruPolicyTest 5/5.

- [ ] **Step 7: Commit**

```bash
git add \
  velox/common/caching/fscache/SlruPolicy.h \
  velox/common/caching/fscache/SlruPolicy-inl.h \
  velox/common/caching/fscache/SlruPolicy.cpp \
  velox/common/caching/fscache/FsCacheConfig.h \
  velox/common/caching/fscache/FsCache.h \
  velox/common/caching/fscache/FsCache.cpp \
  velox/common/caching/fscache/CMakeLists.txt \
  velox/common/caching/fscache/tests/SlruPolicyTest.cpp \
  velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): SlruPolicy (probation + protected) behind enableSlru flag

Implements segmented-LRU eviction per spec §8.1:

  - Fresh inserts land in probation.
  - Second access promotes to protected.
  - Protected overflow demotes to MRU of probation (not evict).
  - Eviction only removes from LRU end of probation.

Phase-1: opt-in via FsCacheConfig.enableSlru (default false). The default
single-LRU path is unchanged; flipping the default waits on TPC-H q22
validation in Task 15.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §8.1 §10 R6

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Atomic FsCacheStats + IsPrefetch end-to-end wiring

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.h` — extend the phase-1 4-field `FsCacheStats` POD (`hits/misses/evictions/bytesOnDisk`) to the spec §6.3 6-field POD (`prefetchHits/prefetchMisses/demandHits/demandMisses` + retained `evictions` + `bytesOnDisk`); add free functions `prefetchHitRate` and `prefetchMissShare` next to the struct; rename the private `AtomicCounters` fields `hits/misses` → `prefetchHits/prefetchMisses/demandHits/demandMisses` (keep `evictions` + `bytesOnDisk` untouched, still `std::atomic<uint64_t>`); declare/extend `recordHit(FileSegment*, IsPrefetch)` / `recordMiss(FileSegment*, segmentSize, IsPrefetch)` (the LRU bump stays inside these MEMBER methods); `IsPrefetch` enum already exists from Task 8. **Shape β: `FsCacheStats` snapshot stays POD `uint64_t` — only the internal `AtomicCounters` is atomic; see spec §6.3 design note.**
- Modify: `velox/common/caching/fscache/FsCache.cpp` — in `getOrSet`, forward the caller's `isPrefetch` into `recordHit` / `recordMiss` (replacing the Task 8 `/*isPrefetch*/` placeholder); inside `recordHit` flip the counter increment from `counters_.hits.fetch_add(1, …)` to the prefetch/demand-keyed atomic on `AtomicCounters`; same for `recordMiss` (keep its `counters_.bytesOnDisk.fetch_add(segmentSize, …)` line — that is the LRU-insert credit, not the hit/miss counter); preserve unchanged: `evict()`'s `counters_.bytesOnDisk.load(...)` drain check, `counters_.evictions.fetch_add(...)`, `counters_.bytesOnDisk.fetch_sub(...)`, and Round-5's `FsCache::totalSize()` accessor; rewrite `stats()` to compose the new 6-field POD via 6 relaxed loads off `AtomicCounters`.
- Modify: `velox/dwio/common/FsCacheBufferedInput.cpp` — Task 11 step 4 already passes `IsPrefetch::kPrefetch` at the `load()` callsite; this task only verifies (Step 6).
- Create: `velox/common/caching/fscache/tests/FsCacheStatsTest.cpp` — 4 unit tests for the new 4-counter recordHit/recordMiss/snapshot + `prefetchHitRate` / `prefetchMissShare` derive metric surface.
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt` — register `velox_fscache_stats_test`.
- Modify: `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` — add `prefetchHitRateOnWarmReread` E2E test.

**Spec:** §9.2 (`prefetchHitRate` / `prefetchMissShare` gates), §10 R5.

**Approach:**
The existing FsCacheStats is non-atomic and counts only `hits`/`misses`. Spec §9.2 needs four counters split by IsPrefetch:

- `prefetchHits` — segment found in cache (kDownloaded or partially-readable) AND request was a prefetch
- `prefetchMisses` — segment had to be created/written AND request was a prefetch
- `demandHits` — segment found in cache AND request was a demand read
- `demandMisses` — segment had to be created/written AND request was a demand read

`prefetchHitRate(s)` = `prefetchHits / (prefetchHits + prefetchMisses)` is the Task 16 perf gate's hot-path metric: a healthy fscache should keep `prefetchHitRate ≥ 0.95` for warm benchmarks. `prefetchMissShare(s)` = `prefetchMisses / (prefetchMisses + demandMisses)` is the §3 quantitative target: demand miss should stay ≤ 20% of total miss (i.e. `prefetchMissShare ≥ 0.80`). Both metrics live as free functions next to `FsCacheStats`.

`IsPrefetch` was added to the FsCache API in Task 8 (every callsite then hard-coded `kDemand`); Task 11 step 4 flipped the prefetch callsite (`FsCacheBufferedInput::load`) to `kPrefetch`. Task 14 now: (a) makes the counters atomic, (b) actually splits the increments by IsPrefetch, (c) verifies the right value flows from FsCacheBufferedInput.

The wiring rule in BufferedInput: `enqueue()` is always a prefetch (the column reader has not yet pulled bytes from the returned stream); the stream's `Next()` and `seekToPosition()` are demand reads. Since segments are created inside `load()` (which is the materialization of the prefetch enqueue), `load()` carries `IsPrefetch::kPrefetch`. Future paths that bypass enqueue and call getOrSet directly carry `kDemand`. Phase-1 has only the enqueue path, so all FsCacheBufferedInput-driven traffic is kPrefetch.

- [ ] **Step 1: Write failing test — FsCacheStatsTest**

Create `velox/common/caching/fscache/tests/FsCacheStatsTest.cpp`. The test
file exercises `FsCacheStats` directly (the struct lives in
`FsCache.h` — phase-1 never split it into its own header and this task
does not move it):

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

#include "velox/common/caching/fscache/FsCache.h"

#include <gtest/gtest.h>
#include <thread>

namespace facebook::velox::cache::fs {

// FsCacheStats now exposes 6 POD uint64_t fields per spec §6.3 Shape β
// (prefetch/demand split + retained evictions + bytesOnDisk). Updates
// happen through FsCache::recordHit / FsCache::recordMiss MEMBER methods
// against the private AtomicCounters; tests observe via cache.stats()
// which returns a copyable POD snapshot.

namespace {
// Test helper: builds a FileSegment in kDownloaded state so recordHit /
// recordMiss accept it (LruPolicy::onInsert rejects non-kDownloaded).
// Mirrors the helper in EvictionPolicyTest.cpp.
std::unique_ptr<FileSegment> makeDownloadedSegment(
    FsCache& cache,
    const std::string& remotePath,
    uint64_t offset,
    uint64_t size);
} // namespace

TEST(FsCacheStatsTest, freshStatsAreZero) {
  FsCache cache{makeTinyConfig()};
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, 0u);
  EXPECT_EQ(s.prefetchMisses, 0u);
  EXPECT_EQ(s.demandHits, 0u);
  EXPECT_EQ(s.demandMisses, 0u);
  EXPECT_EQ(s.evictions, 0u);
  EXPECT_EQ(s.bytesOnDisk, 0u);
  EXPECT_DOUBLE_EQ(prefetchHitRate(s), 0.0);
  EXPECT_DOUBLE_EQ(prefetchMissShare(s), 0.0);
}

TEST(FsCacheStatsTest, recordHitMissIncrementsCorrectField) {
  FsCache cache{makeTinyConfig()};
  auto seg = makeDownloadedSegment(cache, "/r/x", 0, 4'096);
  cache.recordHit(seg.get(), IsPrefetch::kPrefetch);
  cache.recordHit(seg.get(), IsPrefetch::kPrefetch);
  cache.recordHit(seg.get(), IsPrefetch::kDemand);
  cache.recordMiss(seg.get(), 4'096, IsPrefetch::kPrefetch);
  cache.recordMiss(seg.get(), 4'096, IsPrefetch::kDemand);
  cache.recordMiss(seg.get(), 4'096, IsPrefetch::kDemand);
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, 2u);
  EXPECT_EQ(s.prefetchMisses, 1u);
  EXPECT_EQ(s.demandHits, 1u);
  EXPECT_EQ(s.demandMisses, 2u);
}

TEST(FsCacheStatsTest, prefetchHitRateComputesFromPrefetchOnly) {
  FsCacheStats s;
  // 9 prefetch hits / 1 prefetch miss = 0.9 hit-rate. Demand counters
  // must not enter the calculation — demand reads are blocking by
  // definition, so they are not part of the prefetch *effectiveness*
  // metric (spec §9.2 perf gate keys off prefetchHitRate).
  s.prefetchHits = 9;
  s.prefetchMisses = 1;
  s.demandMisses = 100;  // must not affect prefetchHitRate.
  EXPECT_DOUBLE_EQ(prefetchHitRate(s), 0.9);
  // prefetchMissShare uses BOTH miss counters per spec §3 quantitative
  // target: "miss flow taken by prefetch path", not the prefetch hit
  // rate. 1 prefetch miss / (1 + 100) demand misses ≈ 0.0099.
  EXPECT_NEAR(prefetchMissShare(s), 1.0 / 101.0, 1e-9);
}

TEST(FsCacheStatsTest, concurrentIncrementsAreLossless) {
  FsCache cache{makeTinyConfig()};
  auto seg = makeDownloadedSegment(cache, "/r/x", 0, 4'096);
  constexpr int kThreads = 8;
  constexpr int kIters = 10'000;
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&]() {
      for (int i = 0; i < kIters; ++i) {
        cache.recordHit(seg.get(), IsPrefetch::kPrefetch);
        cache.recordMiss(seg.get(), 4'096, IsPrefetch::kDemand);
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }
  const auto s = cache.stats();
  EXPECT_EQ(s.prefetchHits, static_cast<uint64_t>(kThreads * kIters));
  EXPECT_EQ(s.demandMisses, static_cast<uint64_t>(kThreads * kIters));
}

} // namespace facebook::velox::cache::fs
```

Append to `velox/common/caching/fscache/tests/CMakeLists.txt`:

```cmake
add_executable(velox_fscache_stats_test FsCacheStatsTest.cpp)
target_link_libraries(
  velox_fscache_stats_test
  velox_fscache
  GTest::gtest
  GTest::gtest_main)
add_test(NAME velox_fscache_stats_test COMMAND velox_fscache_stats_test)
```

- [ ] **Step 2: Run — expected RED (API doesn't match)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_stats_test -j 8
```

Expected: compile errors on `recordHit(s, IsPrefetch::...)`,
`recordMiss(s, IsPrefetch::...)`, `prefetchHitRate(s)`,
`prefetchMissShare(s)`,
`s.prefetchHits/prefetchMisses/demandHits/demandMisses` (phase-1
`FsCacheStats` only has `hits/misses/evictions/bytesOnDisk`).

- [ ] **Step 3: Rewrite `FsCacheStats` to the spec §6.3 6-field POD (Shape β)**

Extend the existing `FsCacheStats` POD in
`velox/common/caching/fscache/FsCache.h` (currently lines 41-46, 4
`uint64_t` fields) to the 6-field POD from spec §6.3 plus two derived
metrics. **Shape β**: `FsCacheStats` itself stays plain `uint64_t`
(copyable snapshot); only the private `AtomicCounters` is atomic. See
spec §6.3 design note for the rationale (CH FileCache does not split
hit/miss at all; Velox needs the split for Task 16 perf gate but does
not need to break snapshot copyability).

```cpp
/// Copyable POD snapshot of the cache counters. FsCache::stats() takes
/// 6 relaxed loads off the internal AtomicCounters and returns this
/// value. Cross-field consistency is not guaranteed; tests asserting
/// `hits+misses == total` must first quiesce writers (e.g. thread.join()).
/// Spec §6.3.
struct FsCacheStats {
  uint64_t prefetchHits{0};
  uint64_t prefetchMisses{0};
  uint64_t demandHits{0};
  uint64_t demandMisses{0};
  uint64_t evictions{0};
  // Retained from phase-1: the eviction loop reads this to decide
  // whether to drain (see `evict()` invariant in this header), and
  // `FsCache::totalSize()` exposes it as the CH `getUsedCacheSize()`
  // mirror.
  uint64_t bytesOnDisk{0};
};

/// Whether a cache access originated as a prefetch (scheduled before
/// the reader needed the bytes) or as a demand read (reader is blocked
/// waiting). Drives the prefetch/demand split on FsCacheStats.
enum class IsPrefetch : uint8_t { kPrefetch, kDemand };

/// Fraction of prefetch requests that hit in the cache, or 0.0 if no
/// prefetch traffic yet. The Task 16 perf gate watches this number;
/// healthy hot-path fscache should keep it ≥ 0.95 (§9.2). Demand
/// counters do not participate — demand reads are blocking by
/// definition, so they are not part of the prefetch *effectiveness*
/// metric.
inline double prefetchHitRate(const FsCacheStats& s) {
  const uint64_t total = s.prefetchHits + s.prefetchMisses;
  return total == 0 ? 0.0
                    : static_cast<double>(s.prefetchHits) /
          static_cast<double>(total);
}

/// Share of ALL misses that were taken by the prefetch path rather
/// than the demand path. Spec §3 quantitative target requires this to
/// stay ≥ 0.80 — high `prefetchMissShare` means "most cache pain is
/// absorbed by the background prefetch before user threads need the
/// bytes", which is the actual user-visible win. Returns 0.0 when
/// there have been no misses at all.
inline double prefetchMissShare(const FsCacheStats& s) {
  const uint64_t total = s.prefetchMisses + s.demandMisses;
  return total == 0 ? 0.0
                    : static_cast<double>(s.prefetchMisses) /
          static_cast<double>(total);
}
```

**No `recordHit` / `recordMiss` free functions on FsCacheStats.** All
increments go through the MEMBER methods `FsCache::recordHit(FileSegment*,
IsPrefetch)` / `FsCache::recordMiss(FileSegment*, size_t, IsPrefetch)`
(see Step 5). Writing to a snapshot POD would be ineffective anyway:
`stats()` returns by value, so a free-fn on the snapshot would overwrite
nothing.

Also update the private `AtomicCounters` struct in `FsCache.h` (lines
177-182) to mirror the 4-way split, keeping `evictions` and
`bytesOnDisk` untouched:

```cpp
struct AtomicCounters {
  std::atomic<uint64_t> prefetchHits{0};
  std::atomic<uint64_t> prefetchMisses{0};
  std::atomic<uint64_t> demandHits{0};
  std::atomic<uint64_t> demandMisses{0};
  std::atomic<uint64_t> evictions{0};
  std::atomic<uint64_t> bytesOnDisk{0};
};
```

Update the private member method declarations on `FsCache` (lines
164-169) to take `IsPrefetch`:

```cpp
// Records a cache hit: best-effort LRU bump + increments
// counters_.{prefetchHits,demandHits} based on isPrefetch.
void recordHit(FileSegment* segment, IsPrefetch isPrefetch);

// Records a fresh miss: LRU insert + increments
// counters_.{prefetchMisses,demandMisses} based on isPrefetch +
// credits segmentSize to counters_.bytesOnDisk.
void recordMiss(
    FileSegment* segment,
    uint64_t segmentSize,
    IsPrefetch isPrefetch);
```

Update the `recordHit` comment on `bytesOnDisk` (the existing line 153
comment is still correct — only the counter names change, not the
semantics).

- [ ] **Step 4: Run — expected GREEN for FsCacheStatsTest**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_stats_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R velox_fscache_stats_test -V
```

Expected: `[  PASSED  ] 4 tests.`

- [ ] **Step 5: Forward `IsPrefetch` through `FsCache::recordHit/recordMiss` and split the counter increment**

`IsPrefetch` was added to `getOrSet` in **Task 8** (the API hard-cut)
and forwarded as `/*isPrefetch*/` (unused). This step starts using the
parameter end-to-end.

Phase-1 `FsCache::recordHit(FileSegment*)` /
`FsCache::recordMiss(FileSegment*, segmentSize)` are MEMBER methods
that do **two** things: (a) the LRU bump / insert under the bucket's
priority lock (best-effort `try_lock` for `recordHit`; eager
`onInsert` for `recordMiss`); (b) the counter increment. This task
**keeps the LRU logic intact** and only changes the counter
increment. The LRU bump path is hot and load-bearing for the SLRU
work in Task 13 — do not delete or restructure it.

Concretely:

1. Update the two member declarations in `FsCache.h` (already done in
   Step 3) so they take `IsPrefetch`. Update the two member definitions
   in `FsCache.cpp` to forward `isPrefetch` into the single increment
   line:

   ```cpp
   // velox/common/caching/fscache/FsCache.cpp, recordHit body
   void FsCache::recordHit(FileSegment* segment, IsPrefetch isPrefetch) {
     // [unchanged: best-effort per-bucket LRU bump via try_lock +
     //  segment->hits_.fetch_add — keep the existing code verbatim,
     //  this comment is only a marker for the diff reviewer]
     auto& counter = isPrefetch == IsPrefetch::kPrefetch
         ? counters_.prefetchHits
         : counters_.demandHits;
     counter.fetch_add(1, std::memory_order_relaxed);
   }

   void FsCache::recordMiss(
       FileSegment* segment,
       uint64_t segmentSize,
       IsPrefetch isPrefetch) {
     // [unchanged: per-bucket LRU onInsert under priority lock — keep
     //  existing code verbatim]
     auto& counter = isPrefetch == IsPrefetch::kPrefetch
         ? counters_.prefetchMisses
         : counters_.demandMisses;
     counter.fetch_add(1, std::memory_order_relaxed);
     counters_.bytesOnDisk.fetch_add(
         segmentSize, std::memory_order_relaxed);
   }
   ```

   The phase-1 single-counter line `counters_.hits.fetch_add(1, …)` in
   `recordHit` and `counters_.misses.fetch_add(1, …)` in `recordMiss`
   are replaced 1-for-1 by the free-function call. The
   `counters_.bytesOnDisk.fetch_add(segmentSize, …)` line in
   `recordMiss` is UNCHANGED (it's the LRU-insert credit, not a hit/miss
   counter — see `FsCache.h:153` `evict()` invariant which depends on
   it).

2. Update `FsCache::getOrSet` to forward the caller-supplied
   `isPrefetch` parameter into both calls (replacing the Task 8
   `/*isPrefetch*/` placeholder comment). The increments must still
   happen inside the keyMetadata lock window per spec §4.1 step 6 — that
   is already the case for both phase-1 call sites:

   ```cpp
   // inside getOrSet, after fillHolesWithEmptyFileSegments returns:
   for (const auto& seg : holder.segments()) {
     if (seg->state() == FileSegment::State::kDownloaded) {
       recordHit(seg.get(), isPrefetch);
     } else {
       // kEmpty / kDownloading both count as miss: caller will drive
       // the download (or wait), spec §6.3 contract is "miss = FsCache
       // had to create or hand off a non-Downloaded segment".
       recordMiss(seg.get(), seg->key().size, isPrefetch);
     }
   }
   ```

   This also UPGRADES the two 2-arg callsites that Task 8 step 2
   introduced into `getOrSet` (the writer path's `recordMiss(seg.get(),
   seg->key().size)` near plan line ~1904 and the reader path's
   `recordHit(seg.get())` near plan line ~1916). Add `isPrefetch` as the
   trailing argument to both — the Task 8 signatures were intentionally
   2-arg because `IsPrefetch` was wired but not yet consumed; this task
   completes the wiring.

3. Rewrite `FsCache::stats()` (currently `FsCache.cpp:381-388`) to
   compose the new 6-field POD from the 6 atomic loads. The function
   returns by value, so callers that captured the phase-1 snapshot type
   continue to compile:

   ```cpp
   FsCacheStats FsCache::stats() const {
     FsCacheStats snapshot;
     snapshot.prefetchHits =
         counters_.prefetchHits.load(std::memory_order_relaxed);
     snapshot.prefetchMisses =
         counters_.prefetchMisses.load(std::memory_order_relaxed);
     snapshot.demandHits =
         counters_.demandHits.load(std::memory_order_relaxed);
     snapshot.demandMisses =
         counters_.demandMisses.load(std::memory_order_relaxed);
     snapshot.evictions =
         counters_.evictions.load(std::memory_order_relaxed);
     snapshot.bytesOnDisk =
         counters_.bytesOnDisk.load(std::memory_order_relaxed);
     return snapshot;
   }
   ```

4. Preserve unchanged: `evict()`'s
   `counters_.bytesOnDisk.load(...)` drain check
   (`FsCache.cpp:243`), `counters_.evictions.fetch_add(...)` after
   eviction completes (`FsCache.cpp:366`),
   `counters_.bytesOnDisk.fetch_sub(...)` per evicted segment
   (`FsCache.cpp:375`), and Round-5's `FsCache::totalSize()` accessor
   (`return counters_.bytesOnDisk.load(std::memory_order_relaxed);` —
   the underlying field name is unchanged in `AtomicCounters`).

Do NOT delete the phase-1 `FsCache::recordHit` / `recordMiss` member
methods. They are MEMBER methods that own the LRU touch; only the
counter-increment line inside them changes.

- [ ] **Step 6: Flip FsCacheBufferedInput::load to kPrefetch**

Task 11 step 4 already wired `IsPrefetch::kPrefetch` into the load
callsite (see plan line 2573). This step is a no-op verification:

```bash
grep -n "IsPrefetch::kPrefetch" \
  /home/chang/OpenSource/velox2/velox/dwio/common/FsCacheBufferedInput.cpp
```

Expected: one match in `load()`. If absent (because step 4 of Task 11
was edited later), patch the callsite to:

```cpp
enq.holder = fsCache_->getOrSet(
    input_->getName(),
    enq.region.offset,
    enq.region.length,
    fsCache_->config(),
    *input_->getReadFile(),
    cache::fs::IsPrefetch::kPrefetch);
```

- [ ] **Step 7: Write failing E2E test — prefetchHitRate**

Append to `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`:

```cpp
TEST_F(FsCacheBufferedInputTest, prefetchHitRateOnWarmReread) {
  // Two BufferedInputs over the same file; the second one should hit
  // everything the first one cached, driving prefetchHitRate to 1.0
  // for the second pass.
  auto cache = makeCache(/*capacity=*/64 * kMiB);
  auto remote = makeRemoteFile("blob", /*bytes=*/8 * kMiB);

  {
    FsCacheBufferedInput input{remote, *pool_, cache.get()};
    auto stream = input.enqueue({/*offset=*/0, /*length=*/8 * kMiB}, nullptr);
    input.load(LogType::kFile);
    drainStream(*stream); // populate cache
  }

  // Capture baseline before the warm pass. FsCacheStats is a copyable POD
  // (spec §6.3 Shape β); read fields directly, no .load() needed.
  const auto warmStats = cache->stats();
  const uint64_t baselinePrefetchHits = warmStats.prefetchHits;
  const uint64_t baselinePrefetchMisses = warmStats.prefetchMisses;

  {
    FsCacheBufferedInput input{remote, *pool_, cache.get()};
    auto stream = input.enqueue({/*offset=*/0, /*length=*/8 * kMiB}, nullptr);
    input.load(LogType::kFile);
    drainStream(*stream);
  }

  const auto finalStats = cache->stats();
  const uint64_t addedPrefetchHits =
      finalStats.prefetchHits - baselinePrefetchHits;
  const uint64_t addedPrefetchMisses =
      finalStats.prefetchMisses - baselinePrefetchMisses;
  EXPECT_GT(addedPrefetchHits, 0u);
  EXPECT_EQ(addedPrefetchMisses, 0u);
  // prefetchHitRate computed on the delta — spec §9.2 gate is ≥ 0.95.
  // Use GE rather than DOUBLE_EQ(1.0) so future SLRU / drainer races
  // that legally let a couple of prefetches miss do not spurious-fail
  // this case.
  const double rate = addedPrefetchHits + addedPrefetchMisses == 0
      ? 0.0
      : static_cast<double>(addedPrefetchHits) /
          static_cast<double>(addedPrefetchHits + addedPrefetchMisses);
  EXPECT_GE(rate, 0.95);
  // Sanity: require enough warm traffic so the rate isn't computed
  // off a degenerate zero-traffic delta (which would pass `rate==0`
  // mathematically but tell us nothing). The 8 MiB warm reread should
  // produce at least 4 prefetch hits at default 4 MiB alignment.
  EXPECT_GE(addedPrefetchHits, 4u);
}
```

- [ ] **Step 7.5: Write failing E2E test — prefetchMissShare (spec §3 / §9.2 gate)**

Append a second TEST_F to `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`.
This is the §3 quantitative target's E2E gate: prefetch path must
absorb ≥ 80% of all misses so demand miss stays ≤ 20% of total miss.

```cpp
TEST_F(FsCacheBufferedInputTest, prefetchMissShare) {
  // Mock workload: prefetch a large region (N cold misses on the
  // prefetch path), then issue a few demand reads at random offsets
  // that hit the just-warmed cache (≈ 0 demand misses). The resulting
  // prefetchMissShare should be near 1.0; spec §3 / §9.2 gate is
  // ≥ 0.80.
  auto cache = makeCache(/*capacity=*/64 * kMiB);
  auto remote = makeRemoteFile("blob", /*bytes=*/16 * kMiB);

  // Prefetch the whole file — every segment is a fresh prefetch miss.
  {
    FsCacheBufferedInput input{remote, *pool_, cache.get()};
    auto stream =
        input.enqueue({/*offset=*/0, /*length=*/16 * kMiB}, nullptr);
    input.load(LogType::kFile);
    drainStream(*stream);
  }

  // Now read 32 small random regions through a DEMAND path (e.g.,
  // direct getOrSet with IsPrefetch::kDemand). All bytes should be
  // resident → zero demand misses.
  for (int i = 0; i < 32; ++i) {
    const uint64_t offset = (i * 503ULL * 1024) % (16 * kMiB - 64 * 1024);
    auto holder = cache->getOrSet(
        "blob",
        offset,
        /*size=*/64 * 1024,
        cache->config(),
        *remote,
        cache::fs::IsPrefetch::kDemand);
    // Caller-driven advancement is not needed here: every segment is
    // already kDownloaded from the prefetch above, and getOrSet records
    // the hit. (See FsCacheBufferedInputTest::secondReaderOfDownloaded
    // for the same pattern.)
    (void)holder;
  }

  const auto s = cache->stats();
  // The prefetch first pass produced all the misses; the demand pass
  // produced none. prefetchMissShare ≈ 1.0; gate is ≥ 0.80.
  EXPECT_GE(prefetchMissShare(s), 0.80);
  EXPECT_GT(s.prefetchMisses, 0u);
  EXPECT_EQ(s.demandMisses, 0u);
}
```

- [ ] **Step 8: Run — expected GREEN**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test velox_fscache_buffered_input_test velox_fscache_stats_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R 'velox_fscache_test|velox_fscache_buffered_input_test|velox_fscache_stats_test' -V
```

Expected: all tests pass; `prefetchHitRate` reports 1.0 on the second pass (delta).

- [ ] **Step 9: Commit**

```bash
git add \
  velox/common/caching/fscache/FsCache.h \
  velox/common/caching/fscache/FsCache.cpp \
  velox/dwio/common/FsCacheBufferedInput.cpp \
  velox/common/caching/fscache/tests/FsCacheStatsTest.cpp \
  velox/common/caching/fscache/tests/CMakeLists.txt \
  velox/dwio/common/tests/FsCacheBufferedInputTest.cpp
git commit -m "$(cat <<'EOF'
feat(fscache): split hit/miss by IsPrefetch (Shape β POD stats)

FsCacheStats grows from the phase-1 4-field POD (hits/misses/evictions/
bytesOnDisk) to the spec §6.3 6-field POD: prefetchHits, prefetchMisses,
demandHits, demandMisses, plus retained evictions and bytesOnDisk. The
public snapshot stays plain uint64_t (copyable); only the private
FsCache::AtomicCounters is std::atomic<uint64_t>. Two derive metrics
land as free functions next to FsCacheStats:
  - prefetchHitRate (prefetchHits / total prefetch) — Task 16 hot-path
    gate, target ≥ 0.95 on warm reread.
  - prefetchMissShare (prefetchMisses / total misses) — §3 quantitative
    target, ≥ 0.80 so demand miss stays ≤ 20% of total miss.

FsCache::recordHit and FsCache::recordMiss (MEMBER methods that own the
LRU touch) gain an IsPrefetch parameter and increment the matching
AtomicCounters field. FsCacheBufferedInput::load tags its getOrSet calls
as IsPrefetch::kPrefetch (load() runs before the column reader pulls
bytes); all other paths default to kDemand.

End-to-end FsCacheBufferedInputTest::prefetchHitRateOnWarmReread locks
in the contract that re-reading a fully-cached blob drives the delta
prefetchHitRate to 1.0; the perf gate in Task 16 keys off both
prefetchHitRate and prefetchMissShare.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §6.3 §9.2 §10 R5

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: FsCacheTpchEquivalenceTest + TPC-H q1–q22 end-to-end

**Files:**
- Create: `velox/dwio/parquet/tests/FsCacheTpchEquivalenceTest.cpp` (new — patterned on `ParquetTpchTest.cpp`)
- Modify: `velox/dwio/parquet/tests/CMakeLists.txt` — add new binary
- Create: `docs/superpowers/results/2026-05-26-fscache-tpch22-equivalence.md` (created by run)

**Spec:** §9.3 (equivalence), §10 R8 (TPC-H end-to-end).

**Approach:**

`velox/dwio/parquet/tests/ParquetTpchTest.cpp` already runs all 22 TPC-H queries against an in-process TpchConnector-generated SF=0.01 parquet dataset (no `make tpch_test` target exists — the test generates its own data in `SetUpTestSuite`). Task 15 lifts that fixture, parameterises it by **FsCache mode** (`kOff`, `kOn`, `kSlru`), runs the same 22 queries through each mode, and asserts row-identical output.

Three modes:
- `kOff`: today's `BufferedInput` (no FsCache) — this is the baseline.
- `kOn`: `FsCacheBufferedInput` with `FsCacheConfig{}` (single-LRU eviction, no SLRU, no QueryLimitToken, no bypass).
- `kSlru`: `FsCacheBufferedInput` with `FsCacheConfig{ enableSlru = true }` (Task 13's SlruPolicy active).

The fixture builds a `HiveConnector` whose `Configs` includes a custom `BufferedInputFactory` that returns the right `BufferedInput` subclass per mode. (If `BufferedInputFactory` doesn't yet exist as a pluggable knob, this task adds a minimal hook in `HiveConnector::createBufferedInput` keyed off a session property `fscache.mode`.)

Equality check is row-by-row via `BaseVector::equalValueAt` after sorting (when needed), exactly as `ParquetTpchTest::assertQuery` already does against DuckDB. Here both sides are Velox so DuckDB is dropped — we compare the two Velox runs to each other.

- [ ] **Step 1: Inspect existing TPC-H test infra**

Read `velox/dwio/parquet/tests/ParquetTpchTest.cpp:40-159` to confirm the data-generation pattern (TpchConnector → tableScan SF 0.01 → write parquet via TableWriter into a `TempDirectoryPath`, then `TpchQueryBuilder::initialize(path)`). Reuse it verbatim — do not invent a new data source.

Confirm the `BufferedInputFactory` plumbing. Search:

```bash
grep -rn "BufferedInputFactory\|createBufferedInput\|class BufferedInput " \
  /home/chang/OpenSource/velox2/velox/dwio/common/ \
  /home/chang/OpenSource/velox2/velox/connectors/hive/
```

If a session-property-driven factory hook already exists, use it. If not, add a minimal hook in this task before the parameterised test (extra step 2.5 below).

- [ ] **Step 2: Write failing test — FsCacheTpchEquivalenceTest**

Create `velox/dwio/parquet/tests/FsCacheTpchEquivalenceTest.cpp` patterned on `ParquetTpchTest.cpp`:

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

#include <folly/init/Init.h>
#include <vector>

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

namespace {

enum class FsCacheMode { kOff, kOn, kSlru };

std::string modeName(FsCacheMode m) {
  switch (m) {
    case FsCacheMode::kOff:
      return "Off";
    case FsCacheMode::kOn:
      return "On";
    case FsCacheMode::kSlru:
      return "Slru";
  }
  VELOX_UNREACHABLE();
}

} // namespace

class FsCacheTpchEquivalenceTest
    : public ::testing::TestWithParam<FsCacheMode> {
 protected:
  static void SetUpTestSuite() {
    // Mirror ParquetTpchTest::SetUpTestSuite verbatim except for the
    // HiveConnector — that one is rebuilt per-mode inside each TEST_P so
    // the FsCache factory hook can be parameterised.
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    duckDb_ = std::make_shared<DuckDbQueryRunner>();
    tempDirectory_ = TempDirectoryPath::create();
    tpchBuilder_ =
        std::make_shared<TpchQueryBuilder>(dwio::common::FileFormat::PARQUET);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    dwio::common::registerFileSinks();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();

    connector::tpch::TpchConnectorFactory tpchFactory;
    auto tpchConnector = tpchFactory.newConnector(
        kTpchConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>{}));
    connector::ConnectorRegistry::global().insert(
        tpchConnector->connectorId(), tpchConnector);

    saveTpchTablesAsParquet();
    tpchBuilder_->initialize(tempDirectory_->getPath());
  }

  static void TearDownTestSuite() {
    connector::ConnectorRegistry::global().erase(kTpchConnectorId);
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
  }

  void SetUp() override {
    // Install a Hive connector wired to the mode under test. The mode
    // selects which BufferedInputFactory the connector hands to the
    // ParquetReader.
    const auto mode = GetParam();
    std::unordered_map<std::string, std::string> hiveCfg;
    switch (mode) {
      case FsCacheMode::kOff:
        hiveCfg["fscache.mode"] = "off";
        break;
      case FsCacheMode::kOn:
        hiveCfg["fscache.mode"] = "on";
        break;
      case FsCacheMode::kSlru:
        hiveCfg["fscache.mode"] = "on";
        hiveCfg["fscache.enable_slru"] = "true";
        break;
    }
    connector::hive::HiveConnectorFactory hiveFactory;
    auto hiveConnector = hiveFactory.newConnector(
        kHiveConnectorId,
        std::make_shared<config::ConfigBase>(std::move(hiveCfg)));
    connector::ConnectorRegistry::global().insert(
        hiveConnector->connectorId(), hiveConnector);
  }

  void TearDown() override {
    connector::ConnectorRegistry::global().erase(kHiveConnectorId);
  }

  // Mirrors ParquetTpchTest::saveTpchTablesAsParquet — copied verbatim
  // because it is the canonical TPC-H Parquet generator path. Do not
  // rewrite it; if upstream changes, mirror the change here too.
  static void saveTpchTablesAsParquet() {
    std::shared_ptr<memory::MemoryPool> rootPool{
        memory::memoryManager()->addRootPool()};
    std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

    for (const auto& table : tpch::tables) {
      auto tableName = toTableName(table);
      auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->getPath(), tableName);
      auto tableSchema = tpch::getTableSchema(table);
      auto columnNames = tableSchema->names();
      auto plan = PlanBuilder()
                      .tpchTableScan(table, std::move(columnNames), 0.01)
                      .planNode();
      auto split = exec::Split(
          std::make_shared<connector::tpch::TpchConnectorSplit>(
              kTpchConnectorId, /*cacheable=*/true, 1, 0));
      auto rows =
          AssertQueryBuilder(plan).splits({split}).copyResults(pool.get());
      duckDb_->createTable(tableName.data(), {rows});
      plan = PlanBuilder()
                 .values({rows})
                 .tableWrite(tableDirectory, dwio::common::FileFormat::PARQUET)
                 .planNode();
      AssertQueryBuilder(plan).copyResults(pool.get());
    }
  }

  std::vector<RowVectorPtr> runQuery(int queryId) {
    auto tpchPlan = tpchBuilder_->getQueryPlan(queryId);
    constexpr int kNumSplits = 10;
    constexpr int kNumDrivers = 4;
    auto addSplits = [&](TaskCursor* taskCursor) {
      if (taskCursor->noMoreSplits()) {
        return;
      }
      auto& task = taskCursor->task();
      for (const auto& entry : tpchPlan.dataFiles) {
        for (const auto& path : entry.second) {
          const auto splits = HiveConnectorTestBase::makeHiveConnectorSplits(
              path, kNumSplits, tpchPlan.dataFileFormat);
          for (const auto& split : splits) {
            task->addSplit(entry.first, Split(split));
          }
        }
        task->noMoreSplits(entry.first);
      }
      taskCursor->setNoMoreSplits();
    };
    CursorParameters params;
    params.maxDrivers = kNumDrivers;
    params.planNode = tpchPlan.plan;
    std::vector<RowVectorPtr> results;
    auto cursor = TaskCursor::create(params);
    while (cursor->moveNext()) {
      results.push_back(cursor->current());
    }
    addSplits(cursor.get());
    while (cursor->moveNext()) {
      results.push_back(cursor->current());
    }
    return results;
  }

  static bool rowsEqual(
      const std::vector<RowVectorPtr>& a,
      const std::vector<RowVectorPtr>& b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
      if (a[i]->size() != b[i]->size()) {
        return false;
      }
      for (vector_size_t r = 0; r < a[i]->size(); ++r) {
        if (!a[i]->equalValueAt(b[i].get(), r, r)) {
          return false;
        }
      }
    }
    return true;
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<TempDirectoryPath> tempDirectory_;
  static std::shared_ptr<TpchQueryBuilder> tpchBuilder_;
};

std::shared_ptr<DuckDbQueryRunner> FsCacheTpchEquivalenceTest::duckDb_;
std::shared_ptr<TempDirectoryPath> FsCacheTpchEquivalenceTest::tempDirectory_;
std::shared_ptr<TpchQueryBuilder> FsCacheTpchEquivalenceTest::tpchBuilder_;

// One TEST_P generates 22 queries × 3 modes = 66 cases (3 instantiations
// below). For each non-Off mode we compare against the same query run
// under kOff, captured fresh each call to keep the test stateless.
TEST_P(FsCacheTpchEquivalenceTest, allQueriesMatchOffMode) {
  const auto mode = GetParam();
  if (mode == FsCacheMode::kOff) {
    // Off-vs-Off would be trivially equal; skipping keeps gtest output
    // honest (only meaningful comparisons are recorded as PASS).
    GTEST_SKIP() << "kOff is the baseline";
  }

  for (int q = 1; q <= 22; ++q) {
    SCOPED_TRACE("TPC-H q" + std::to_string(q) + " mode=" + modeName(mode));

    // Rebuild the HiveConnector under kOff to capture the baseline.
    connector::ConnectorRegistry::global().erase(kHiveConnectorId);
    connector::hive::HiveConnectorFactory hiveFactory;
    auto offConnector = hiveFactory.newConnector(
        kHiveConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>{
                {"fscache.mode", "off"}}));
    connector::ConnectorRegistry::global().insert(
        offConnector->connectorId(), offConnector);
    const auto baseline = runQuery(q);

    // Re-install the parameterised connector (SetUp's mode) and run.
    connector::ConnectorRegistry::global().erase(kHiveConnectorId);
    SetUp();
    const auto candidate = runQuery(q);

    ASSERT_TRUE(rowsEqual(baseline, candidate))
        << "q" << q << " differs under mode=" << modeName(mode);
  }
}

INSTANTIATE_TEST_SUITE_P(
    Modes,
    FsCacheTpchEquivalenceTest,
    ::testing::Values(FsCacheMode::kOff, FsCacheMode::kOn, FsCacheMode::kSlru),
    [](const auto& info) { return modeName(info.param); });
```

Add to `velox/dwio/parquet/tests/CMakeLists.txt`:

```cmake
add_executable(velox_dwio_parquet_fscache_tpch_equivalence_test
  FsCacheTpchEquivalenceTest.cpp)
target_link_libraries(
  velox_dwio_parquet_fscache_tpch_equivalence_test
  velox_aggregates
  velox_dwio_common_exception
  velox_dwio_parquet_reader
  velox_dwio_parquet_writer
  velox_exec
  velox_exec_test_lib
  velox_fscache
  velox_functions_prestosql
  velox_hive_connector
  velox_parse_parser
  velox_tpch_connector
  velox_tpch_gen
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main)
add_test(NAME velox_dwio_parquet_fscache_tpch_equivalence_test
  COMMAND velox_dwio_parquet_fscache_tpch_equivalence_test)
```

(Mirror the link list from `velox_dwio_parquet_tpch_test` above it in the same `CMakeLists.txt` — exact deps may differ; copy what's there and add `velox_fscache`.)

- [ ] **Step 3: Run — expected RED (test does not compile, or asserts fire)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_dwio_parquet_fscache_tpch_equivalence_test -j 8
```

Expected: compile errors on `fscache.mode` session property (factory hook missing), OR if Hive already routes by `fscache.mode`, RED on at least one query showing where partial-readable / async load breaks parity.

- [ ] **Step 4: Iterate — implement the missing factory hook, debug query mismatches**

If the `fscache.mode` property is not yet honoured, add a minimal hook in `velox/connectors/hive/HiveConnector.{h,cpp}` that:
- Reads `fscache.mode` and (optional) `fscache.enable_slru` from its config.
- When `off`, returns the current `BufferedInput`.
- When `on`, returns `FsCacheBufferedInput` constructed against a process-wide `FsCache` singleton built from the config flags.

The singleton is created the first time mode != `off` is requested. Subsequent connector instances reuse it (we want the cache to persist across queries within the test process).

For each RED query, the debug recipe:
1. Run the query under `mode=On` with `--gtest_filter=*On*Q<n>*` and `LOG_LEVEL=INFO`.
2. Re-run with `mode=Off` and capture both row dumps.
3. Diff. The first divergent row reveals which segment / offset is wrong.
4. Bisect by toggling: async load off (synchronous getOrSet), eviction off (capacity=huge), partial-readable off (full-segment write only). Whichever toggle hides the bug names the culprit.

Bugs found here must be fixed in the corresponding Task 2-14 code paths, then re-tested. Re-add a unit test in the relevant task's test file to lock in the regression.

- [ ] **Step 5: Run — expected GREEN for all 44 non-Off cases (22 q × 2 modes; kOff cases SKIPPED)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_dwio_parquet_fscache_tpch_equivalence_test -j 8
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  -R velox_dwio_parquet_fscache_tpch_equivalence_test -V
```

Expected: `[  PASSED  ] 44 tests. [  SKIPPED ] 22 tests.` Capture raw output to `/tmp/fscache-equivalence.txt`.

Particular focus for the implicit Phase-2 review on this task:
- Connector lifetime: the per-mode HiveConnector swap inside `TEST_P` must not leak the previous one's state into the next iteration (FsCache singleton should be ok; metadata, splits, etc. must be torn down).
- `runQuery`'s `moveNext` loop: confirm it actually drains the cursor — `ParquetTpchTest` uses `exec::test::assertQuery` which does this internally; the bespoke loop here is easy to get wrong.

Particular note for Phase-3 simplification on this task: do **not** delete the verbatim `saveTpchTablesAsParquet` copy; the comment above it documents the deliberate duplication of upstream's canonical generator.

- [ ] **Step 6: Write summary file**

Create `docs/superpowers/results/2026-05-26-fscache-tpch22-equivalence.md`:

```markdown
# FsCache TPC-H q1–q22 Equivalence Results — 2026-05-26

| Query | Mode `Off` rows | Mode `On` rows | Mode `Slru` rows | Bytes-equal |
| ----- | --------------- | -------------- | ---------------- | ----------- |
| q1    | <n>             | <n>            | <n>              | YES         |
| ...   |                 |                |                  |             |
| q22   | <n>             | <n>            | <n>              | YES         |

## Methodology

- Dataset: TPC-H SF 0.01, parquet, generated in-process via TpchConnector
  + TableWriter, identical to `velox_dwio_parquet_tpch_test`.
- Three modes share the same plan, plan options, and per-row equality
  via `BaseVector::equalValueAt`.
- Mode `Off`: HiveConnector with `fscache.mode=off` (plain BufferedInput).
- Mode `On`: HiveConnector with `fscache.mode=on` (FsCacheBufferedInput,
  single-LRU).
- Mode `Slru`: HiveConnector with `fscache.mode=on, fscache.enable_slru=true`.

## Bugs surfaced

(List any bug + fix commit caught by this loop, or "None" if all green
first-try.)
```

Fill row counts from the captured ctest output.

- [ ] **Step 7: Commit**

```bash
git add \
  velox/dwio/parquet/tests/FsCacheTpchEquivalenceTest.cpp \
  velox/dwio/parquet/tests/CMakeLists.txt \
  docs/superpowers/results/2026-05-26-fscache-tpch22-equivalence.md
# If step 4 had to add the HiveConnector fscache.mode hook, also stage
# velox/connectors/hive/HiveConnector.{h,cpp}. If step 4 had to fix any
# Task 2-14 production code, commit those fixes SEPARATELY before this
# commit so each fix has its own diff.
git commit -m "$(cat <<'EOF'
test(fscache): TPC-H q1-q22 byte-equivalence across off/on/slru

Adds FsCacheTpchEquivalenceTest in velox/dwio/parquet/tests/. Lifts the
TPC-H SF 0.01 in-process generator from ParquetTpchTest, then runs all
22 queries through three HiveConnector configurations — fscache.mode=off
(baseline), fscache.mode=on (single-LRU), fscache.mode=on +
fscache.enable_slru=true — and asserts row-identical output per
BaseVector::equalValueAt.

44 / 44 PASSED (22 SKIPPED for the trivial Off-vs-Off case). See
docs/superpowers/results/2026-05-26-fscache-tpch22-equivalence.md.

Any production fixes needed to reach parity were committed separately
ahead of this test; this commit only adds the gating test + the minimal
HiveConnector `fscache.mode` factory hook + results record.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §9.3 §10 R8

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 16: Perf gate — ≥7.0 M ops/s single-thread, ≥0.80× at 16 threads, prefetchHitRate ≥ 0.95, prefetchMissShare ≥ 0.80

**Files:**
- Modify: `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp` — three substantive changes (see Approach):
  1. Extend `CellResult` struct + `printMarkdownTable` to add two columns `prefetch_hit_rate` and `prefetch_miss_share` (15 columns total, up from the phase-1 13).
  2. In the per-cell measurement loop, decide `IsPrefetch` per `cache->getOrSet` callsite: prefetch workload (`kind=prefetch`) passes `IsPrefetch::kPrefetch`; sequential / random workloads pass `IsPrefetch::kDemand`. The phase-1 microbench hard-codes neither because the parameter did not exist yet.
  3. At cell end, snapshot `cache->stats()` once and populate the two new columns via `prefetchHitRate(stats)` / `prefetchMissShare(stats)` free functions (Task 14 §6.3).
- Create: `docs/superpowers/results/2026-05-26-fscache-perf-gate.md`
- No production code changes — the only source change is the benchmark harness above.

**Spec:** §3 (`prefetchMissShare ≥ 0.80`), §9.2 (`prefetchHitRate ≥ 0.95`), §9.4 (throughput / scaling thresholds), §10 R9.

**Approach:**
Task 9 of the microbench plan already produces the 36-cell phase-1 baseline. Task 16 here re-runs that microbench against the post-redesign code and **asserts four metrics** (all spec-defined hard gates):

1. Single-thread cell `{8k, hot, prefetch}` reports ≥ **7.0 M ops/s** (spec §9.4 throughput threshold).
2. 16-thread cell `{8k, hot, prefetch}` reports ≥ **0.80 ×** single-thread (spec §9.4 scaling threshold; ≥ 5.6 M ops/s × 16 threads = 89.6 M ops/s aggregate, accepting up to 20% scaling loss from lock contention).
3. `prefetchHitRate ≥ 0.95` on the hot cell (spec §9.2 — prefetch path's own hit rate when the working set is warm).
4. `prefetchMissShare ≥ 0.80` end-of-run (spec §3 — demand miss ≤ 20% of total miss, i.e. user threads almost never block on cold reads).

Per-metric gates are independent: a run that hits 7.0 M/s but demand-misses 30% of reads is **not** a pass. If any threshold misses, **do not** lower the bar — investigate. The thresholds were set on validated CH measurements + 20% margin for partial-readable overhead.

**Microbench ground-truth note (Round-9 T1):** Task 14 introduces `FsCacheStats.{prefetchHits,prefetchMisses,demandHits,demandMisses}` but the phase-1 microbench harness emits only a single `hit%` column and does NOT pass `IsPrefetch` per callsite (column schema in microbench plan §9 line 1433 / `FsCacheBenchmark.cpp:555` is the 13-column shape). So Step 1 of this task is **wire the two prefetch columns and the per-callsite `IsPrefetch` flag first**, then rerun. Do NOT assume the binary already emits the two metrics.

The microbench harness is the same one written in the (already committed) microbench plan `docs/superpowers/plans/2026-05-23-fscache-microbench.md`; the file-list above describes exactly which lines need to change.

- [ ] **Step 1: Wire prefetch columns + per-callsite IsPrefetch in the microbench**

Before any rerun. The microbench currently emits a single `hit%` column; spec §9.2 / §3 gates need the prefetch/demand split. Concretely:

1. In `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp`, extend `CellResult` to carry `double prefetchHitRate{0}` and `double prefetchMissShare{0}` (after the existing `hitPct` field).
2. In `printMarkdownTable` (around line 555), insert two columns `prefetch_hit_rate | prefetch_miss_share` after `hit%`. Update both the header line and the data row formatter.
3. In the per-cell measurement loop, decide `IsPrefetch` per `cache->getOrSet` callsite: when the workload kind is `prefetch`, pass `IsPrefetch::kPrefetch`; for `sequential` / `random` / other read-style workloads, pass `IsPrefetch::kDemand`. The flag is the 6th parameter of `getOrSet` (Task 8 signature).
4. After the cell-level measurement loop joins all threads, snapshot once: `const auto stats = cache->stats();` and populate `result.prefetchHitRate = prefetchHitRate(stats); result.prefetchMissShare = prefetchMissShare(stats);` (free functions from Task 14 §6.3).

Build + sanity-run to confirm the table renders 15 columns, then proceed to Step 2.

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_benchmark -j 8

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --out /tmp/fscache-smoke.md \
  --bench_seconds=1

# Verify header has both new columns.
head -1 /tmp/fscache-smoke.md | grep -q "prefetch_hit_rate" && \
  head -1 /tmp/fscache-smoke.md | grep -q "prefetch_miss_share" && \
  echo "schema OK"
```

- [ ] **Step 2: Re-run the 36-cell microbench against current HEAD**

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --out /tmp/fscache-after.md \
  --bench_seconds=5
```

Expected output: 36-row Markdown table in `/tmp/fscache-after.md` with the 15-column schema (phase-1 13 + 2 new prefetch columns).

- [ ] **Step 3: Extract gating cells and assert four thresholds**

Hand-extract from `/tmp/fscache-after.md`:
- Row where `block=8k`, `dataset=hot`, `kind=prefetch`, `threads=1`. Record `M_ops_per_sec` as `single_thread_ops` and `prefetch_hit_rate` as `single_thread_hit_rate`.
- Row where `block=8k`, `dataset=hot`, `kind=prefetch`, `threads=16`. Record `M_ops_per_sec` as `sixteen_thread_ops_total`, `prefetch_hit_rate` as `sixteen_thread_hit_rate`, and `prefetch_miss_share` as `sixteen_thread_miss_share`.

Compute `efficiency = sixteen_thread_ops_total / (single_thread_ops * 16)`.

Assert (all four, **any miss = stop and report**, do not paper over):
- `single_thread_ops >= 7.0`
- `efficiency >= 0.80`
- `sixteen_thread_hit_rate >= 0.95` (spec §9.2 — prefetch path effectiveness)
- `sixteen_thread_miss_share >= 0.80` (spec §3 — demand miss ≤ 20% of total miss)

If any threshold misses, **stop and report**. Do not push through a regression.

- [ ] **Step 4: Compare to phase-1 baseline**

```bash
diff -u /home/chang/OpenSource/velox2/docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md /tmp/fscache-after.md \
  > /tmp/fscache-perf-delta.diff || true
```

Expected: every cell's M ops/s in `after` is **≥** `baseline`; throughput should improve (async load + partial-readable removes the head-of-line blocking that capped phase-1). The baseline only has 13 columns; the two new columns appear as `(added)` in the diff. Any cell that regresses by > 5% must be investigated — do not paper over it.

- [ ] **Step 5: Write perf gate results doc**

Create `docs/superpowers/results/2026-05-26-fscache-perf-gate.md`:

```markdown
# FsCache Phase-2 Perf Gate — 2026-05-26

## Gating Cells

| Cell                                      | Threshold  | Measured | Pass |
| ----------------------------------------- | ---------- | -------- | ---- |
| {8k, hot, prefetch, t=1}                  | ≥ 7.0 M/s  | <X.X>    | YES  |
| {8k, hot, prefetch, t=16} efficiency      | ≥ 0.80×    | <0.YY>   | YES  |
| {8k, hot, prefetch, t=16} prefetchHitRate | ≥ 0.95     | <0.YY>   | YES  |
| {8k, hot, prefetch, t=16} prefetchMissShare | ≥ 0.80   | <0.YY>   | YES  |

## Full 36-cell Comparison vs Phase-1 Baseline

(Paste two tables side-by-side or include diff summary. List any cell
that regressed > 5% and the investigation outcome.)

## Methodology

- Binary: cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark
- Flags: `--bench_seconds=5`, default 36-cell Cartesian sweep
- Hardware: <host CPU model>, <cores>, <RAM>, kernel <version>
- Baseline: docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md
```

- [ ] **Step 6: Commit (results-only)**

```bash
git add docs/superpowers/results/2026-05-26-fscache-perf-gate.md
git commit -m "$(cat <<'EOF'
docs(fscache): phase-2 perf gate results — passes 4 hard gates

Re-ran the 36-cell microbench against HEAD with the CH-aligned redesign
in place. All four gating thresholds pass:

  - {8k, hot, prefetch, t=1}:  <X.X> M ops/s (threshold ≥ 7.0)
  - {8k, hot, prefetch, t=16}: <0.YY>× efficiency (threshold ≥ 0.80)
  - {8k, hot, prefetch, t=16}: <0.YY> prefetchHitRate (threshold ≥ 0.95)
  - {8k, hot, prefetch, t=16}: <0.YY> prefetchMissShare (threshold ≥ 0.80)

Full 36-cell comparison vs phase-1 baseline included; no cell regressed
by more than 5%.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §3 §9.2 §9.4 §10 R9

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Promote SLRU default (only if Task 15 + Task 16 both green and the SLRU mode in §15 results ≥ single-LRU mode)**

If the equivalence + perf data both prefer SLRU, flip `FsCacheConfig.enableSlru` default to `true`:

```cpp
bool enableSlru{true};
```

Run the full ctest suite once more to confirm nothing else relied on the old default:

```bash
ctest --test-dir /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 -j 8
```

Commit:

```bash
git add velox/common/caching/fscache/FsCacheConfig.h
git commit -m "$(cat <<'EOF'
feat(fscache): default to SLRU eviction

Phase-2 perf + equivalence runs both prefer SLRU over single-LRU on the
TPC-H q1-q22 workload. Flipping the default per spec §8.1 plan.

Spec: docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md §8.1

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

Skip this step if data prefers single-LRU; leave the flag as `false` and document the reason in the perf gate doc.

---

## Plan Verification

After all 16 tasks complete:

- [ ] `git log --oneline upstream/main..HEAD` shows roughly 16-20 commits (16 tasks + per-task fixes from review loops).
- [ ] `git diff upstream/main -- velox/common/caching/benchmarks/CacheBackendBenchmark.cpp` is **empty** (this file must not be touched).
- [ ] `git log upstream/main..HEAD --pretty=%H | xargs -I {} git log -1 --format='%h %G?' {}` shows no unusual signature states (no `--no-verify`, no `--amend`).
- [ ] `docs/superpowers/results/2026-05-26-fscache-tpch22-equivalence.md` exists with 22 rows × 3 modes all PASS.
- [ ] `docs/superpowers/results/2026-05-26-fscache-perf-gate.md` exists and the two gating thresholds are marked PASS.
- [ ] All new tests are wired into ctest and pass in the full suite:
  ```
  ctest --test-dir cmake-build-relwithdebinfo-gcc13 -j 8 -L fscache
  ```
  (assuming the new tests get the `fscache` label — alternatively `-R 'velox_fscache|velox_file_cache|velox_slru|velox_file_segment|velox_fscache_buffered_input'`)

## 失败处理（沿用 5 阶段节奏）

- 阶段 2/4 第 3 次重试仍有 CRITICAL/HIGH：停下汇报，等用户决定。
- 实施阶段 build / test 红：当作 CRITICAL，进入 review-fix 循环（同样 3 次上限）。
- Task 15 出现 query mismatch：先修对应任务的代码（commit 入对应任务的范围），再回 Task 15 重跑。
- Task 16 perf gate 红：不降阈值；记录回归原因，停下汇报。
