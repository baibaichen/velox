# FsCache Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Velox 引入 `velox/common/caching/fscache/` 模块 + `velox/dwio/common/FsCacheBufferedInput`，落地 ClickHouse 风格本地 SSD cache 的第一阶段（无 background download、LRU 占位、5 锁类型已建但共享 mutex），并通过等价测试证明读路径与源文件的 canonical bytes 字节一致。

**Architecture:** 新模块完全旁路现有 `AsyncDataCache` / `SsdCache`；通过新的 `BufferedInput` 子类接入 dwio。读路径 `FsCacheBufferedInput::enqueue → load → FsCache::getOrSet → splitRange → FileSegment(lookupOrCreate + download)`。落盘 `<root>/<hash[0:2]>/<hash[2:4]>/<full_hash>.<offset>.<size>`，`.tmp + rename` 原子发布；崩溃恢复靠目录扫描。

**Tech Stack:** C++20、Velox `BufferedInput`/`SeekableInputStream`/`ReadFile`、folly（IOBuf、Executor、SpookyHashV2）、`VELOX_CHECK_*`、gtest/gmock、`velox_add_grouped_tests`、`TempDirectoryPath`。

**Constraints (来自设计文档 §设计决策记录):**
- 全 10 个 commit 完成前**不做任何性能测试 / 不写性能 commit message**，只跑 UT。
- 每个 commit 必须 `make debug && make unittest` 全绿。
- 设计源：`velox/docs/designs/fscache-clickhouse-style.md`。
- 不动 `ReadFile`、不动 `BufferedInput` 基类、不依赖 `AsyncDataCache.h`/`SsdCache.h`。
- Comment style 严格按 `.claude/CLAUDE.md`：header 公共 API 用 `///`、非公共/实现内用 `//`，full sentence 首字母大写句号结尾。

---

## File Structure

下表列出第一阶段新增/修改的所有文件。每个文件单一职责，方便聚焦阅读与编辑。

### 新增文件 — `velox/common/caching/fscache/`

| 文件 | 责任 |
|---|---|
| `CMakeLists.txt` | `velox_add_library(velox_fscache ...)` + `tests/` 子目录 |
| `FsCacheKey.h` / `.cpp` | `FsCacheKey{path, offset, size}` + SpookyHashV2 计算 + `==` / `hash` |
| `FsCacheGuards.h` | 5 个锁类型（`CachePriorityGuard`/`CacheStateGuard`/`CacheMetadataGuard`/`KeyGuard`/`FileSegmentGuard`）+ 锁顺序文档 + debug-only `LockOrderChecker` |
| `EvictionPolicy.h` | 抽象基类 `EvictionPolicy`（4 个虚函数） |
| `LruPolicy.h` / `.cpp` | `LruPolicy : public EvictionPolicy`，单链表实现 |
| `FileSegment.h` / `.cpp` | 3 态机（`EMPTY` → `DOWNLOADING` → `DOWNLOADED`，加 `DETACHED`）+ `download()` + `read()` + `cv_` |
| `FsCacheMetadata.h` / `.cpp` | bucket 数组（默认 1024）+ per-key `std::map<offset, FileSegmentPtr>` + 增删查 |
| `FsCacheConfig.h` | POD config（`cacheRoot`, `maxBytes`, `alignment=4MiB`, `maxSegmentSize=32MiB`, `numBuckets=1024`） |
| `FsCache.h` / `.cpp` | 顶层入口；`getOrSet(path, offset, size)` → `std::vector<FileSegmentPtr>`、`splitRange()`、`loadFromDisk()` |

### 新增文件 — `velox/common/caching/fscache/tests/`

| 文件 | 测试内容 |
|---|---|
| `CMakeLists.txt` | `velox_add_grouped_tests(PREFIX velox_fscache_test ...)` |
| `FsCacheKeyTest.cpp` | hash 稳定性、不同字段差异化 |
| `FsCacheGuardsTest.cpp` | RAII 行为、debug-only 锁顺序检查 |
| `EvictionPolicyTest.cpp` | LRU 顺序、`selectVictims` 数量、`onRemove` |
| `FileSegmentTest.cpp` | 单线程状态机转换、download 成功/失败 |
| `FsCacheMetadataTest.cpp` | bucket 分布、insert/lookup/erase |
| `FsCacheSplitRangeTest.cpp` | CH 风格 align + max 切分边界 |
| `FsCacheTest.cpp` | `getOrSet` 端到端（含 eviction 触发） |
| `FsCacheConcurrencyTest.cpp` | 多 reader 同 segment：唯一 download |
| `FsCacheRecoveryTest.cpp` | `.tmp` 清理、size mismatch、unknown filename |
| `FsCachePersistenceTest.cpp` | 多次启停 cache 命中 |

### 新增文件 — `velox/dwio/common/`

| 文件 | 责任 |
|---|---|
| `FsCacheBufferedInput.h` / `.cpp` | `: public BufferedInput`，覆写 `enqueue/load/clone/hasCache` |
| `FsCacheInputStream.h` / `.cpp` | `: public SeekableInputStream`，从 `FileSegment` 本地文件读 |

### 新增文件 — `velox/dwio/common/tests/`

| 文件 | 测试内容 |
|---|---|
| `FsCacheBufferedInputTest.cpp` | enqueue / load 行为、stream 读 |
| `FsCacheEquivalenceTest.cpp` | **关键**：vs 源文件 canonical bytes，证明读路径等价 |

### 修改文件

| 文件 | 修改 |
|---|---|
| `velox/common/caching/CMakeLists.txt` | `add_subdirectory(fscache)` |
| `velox/common/caching/tests/CMakeLists.txt` | 无需改（fscache tests 自带 grouped tests） |
| `velox/dwio/common/CMakeLists.txt` | 新增 `FsCacheBufferedInput.cpp` / `FsCacheInputStream.cpp`，link `velox_fscache` |
| `velox/dwio/common/tests/CMakeLists.txt` | 新增 `FsCacheBufferedInputTest.cpp`、`FsCacheEquivalenceTest.cpp` 到 grouped tests |

---

## Naming & Style Quick Reference

- Namespace：`facebook::velox::cache::fs`（避免与 `facebook::velox::cache` 现有 `AsyncDataCache` 冲突）。
- BufferedInput 子类 namespace 沿用 `facebook::velox::dwio::common`。
- Comments：headers 公共 API 用 `///` 全句号；实现/私有/局部用 `//`。
- 命名：types `PascalCase`、methods/locals `camelCase`、private members `camelCase_`、constants `kPascalCase`、namespaces `snake_case`。
- 数字字面量 ≥ 4 位用 `'` 分隔（`4'194'304`）。
- 初始化用 `{}` 而非 `=`。
- 多行参数列表加 trailing comma。

---

## Task Order & Dependencies

```
1. Scaffold
   └─→ 2. FsCacheKey
        └─→ 3. FsCacheGuards
             └─→ 4. EvictionPolicy + LruPolicy
                  └─→ 5. FileSegment + splitRange
                       └─→ 6. FsCacheMetadata + top-level FsCache
                            └─→ 7. FsCacheBufferedInput
                                 └─→ 8. Crash recovery
                                      └─→ 9. Concurrent stress
                                           └─→ 10. Equivalence test
```

每个 commit 之前必须 `make debug && make unittest` 全绿才能进入下一个。

---

## Commit 1: `feat(fscache): scaffold module skeleton`

**Files:**
- Create: `velox/common/caching/fscache/CMakeLists.txt`
- Create: `velox/common/caching/fscache/FsCacheConfig.h`
- Create: `velox/common/caching/fscache/FsCache.h` (forward decl only)
- Create: `velox/common/caching/fscache/tests/CMakeLists.txt`
- Create: `velox/common/caching/fscache/tests/FsCacheScaffoldTest.cpp`
- Modify: `velox/common/caching/CMakeLists.txt` (一行 `add_subdirectory(fscache)`)

**Why a real test in the scaffold commit:** 不留空目录 / 死代码；用一个最小 `Config` 字段测试确认链接通。

- [ ] **Step 1: Write the failing test**

Create `velox/common/caching/fscache/tests/FsCacheScaffoldTest.cpp`:

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

#include "velox/common/caching/fscache/FsCacheConfig.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheScaffoldTest, configDefaults) {
  FsCacheConfig config;
  EXPECT_EQ(config.alignment, 4UL * 1024 * 1024);
  EXPECT_EQ(config.maxSegmentSize, 32UL * 1024 * 1024);
  EXPECT_EQ(config.numBuckets, 1024);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run test to verify build fails**

Run: `make debug 2>&1 | tail -30`
Expected: failure — `FsCacheConfig.h` not found / no `velox_fscache_test` target.

- [ ] **Step 3: Create `FsCacheConfig.h`**

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * (license header trimmed for brevity — copy in full from FsCacheScaffoldTest.cpp)
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace facebook::velox::cache::fs {

/// Holds configuration for an FsCache instance. All fields are immutable after
/// the FsCache is constructed.
struct FsCacheConfig {
  /// Filesystem root under which cache files live.
  std::string cacheRoot;

  /// Maximum total bytes of cached data on disk. Eviction triggers when usage
  /// would exceed this value.
  uint64_t maxBytes{0};

  /// Segment start/end alignment in bytes. Matches ClickHouse default of
  /// 4 MiB so that adjacent reads share a segment.
  uint64_t alignment{4UL * 1024 * 1024};

  /// Maximum size of a single segment in bytes. Holes larger than this are
  /// split into multiple segments.
  uint64_t maxSegmentSize{32UL * 1024 * 1024};

  /// Number of buckets in the top-level metadata array. A larger value reduces
  /// per-bucket contention at the cost of memory.
  ///
  /// Tuning guidance: target ~16-64 live FileSegments per bucket at peak. With
  /// 4 MiB align and 32 MiB max segment size, 1 TiB of warm working set is
  /// ~32 K-256 K segments, so 1024 buckets keeps the per-bucket chain short
  /// without blowing up the metadata array. Bucket count should be a power of
  /// two for the hash → bucket modulo to fold cleanly.
  size_t numBuckets{1024};
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Create `FsCache.h` with forward declarations only**

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FsCacheConfig.h"

namespace facebook::velox::cache::fs {

class FsCacheKey;
class FileSegment;
class FsCacheMetadata;
class EvictionPolicy;

/// Top-level entry point of the FsCache module. Methods are added in later
/// commits (FsCacheKey first, then EvictionPolicy, then FileSegment, then
/// the public getOrSet API in commit 6).
class FsCache {
 public:
  explicit FsCache(FsCacheConfig config);
  ~FsCache();

 private:
  const FsCacheConfig config_;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Create `FsCache.cpp` (constructor / destructor stubs)**

`velox/common/caching/fscache/FsCache.cpp`:

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"

namespace facebook::velox::cache::fs {

FsCache::FsCache(FsCacheConfig config) : config_{std::move(config)} {}

FsCache::~FsCache() = default;

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 6: Create module `CMakeLists.txt`**

`velox/common/caching/fscache/CMakeLists.txt`:

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

velox_add_library(
  velox_fscache
  FsCache.cpp
  HEADERS
  FsCache.h
  FsCacheConfig.h
)

velox_link_libraries(
  velox_fscache
  PUBLIC
    velox_common_base
    velox_exception
    Folly::folly
    fmt::fmt
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
```

- [ ] **Step 7: Create tests `CMakeLists.txt`**

`velox/common/caching/fscache/tests/CMakeLists.txt`:

```cmake
# (license header)

set(
  VELOX_FSCACHE_TEST_SOURCES
  FsCacheScaffoldTest.cpp
)

set(
  VELOX_FSCACHE_TEST_DEPS
  velox_fscache
  Folly::folly
  glog::glog
  GTest::gtest
  GTest::gtest_main
)

velox_add_grouped_tests(
  PREFIX velox_fscache_test
  SOURCES ${VELOX_FSCACHE_TEST_SOURCES}
  DEPS ${VELOX_FSCACHE_TEST_DEPS}
)
```

- [ ] **Step 8: Hook scaffold into parent CMake**

Edit `velox/common/caching/CMakeLists.txt`. At end of file (after the existing
`velox_add_library(velox_cached_factory ...)` line), append:

```cmake
add_subdirectory(fscache)
```

- [ ] **Step 9: Build and run the test**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: PASS — `FsCacheScaffoldTest.configDefaults` green.

- [ ] **Step 10: Commit**

```bash
git add velox/common/caching/CMakeLists.txt \
        velox/common/caching/fscache/
git commit -m "feat(fscache): scaffold module skeleton

Adds velox_fscache library and tests subdirectory with FsCacheConfig
defaults locked in (4 MiB alignment, 32 MiB max segment, 1024 buckets).
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 2: `feat(fscache): FsCacheKey + hash`

**Files:**
- Create: `velox/common/caching/fscache/FsCacheKey.h`
- Create: `velox/common/caching/fscache/FsCacheKey.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt` (add `FsCacheKey.cpp` + header)
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt` (add `FsCacheKeyTest.cpp`)

- [ ] **Step 1: Write the failing test**

`velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`:

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <gtest/gtest.h>
#include <unordered_set>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheKeyTest, equalityAndInequality) {
  FsCacheKey a{"s3://bucket/file", 0, 4096};
  FsCacheKey b{"s3://bucket/file", 0, 4096};
  FsCacheKey c{"s3://bucket/file", 4096, 4096};
  FsCacheKey d{"s3://bucket/other", 0, 4096};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
}

TEST(FsCacheKeyTest, hashStableAcrossInstances) {
  FsCacheKey a{"s3://bucket/file", 0, 4096};
  FsCacheKey b{"s3://bucket/file", 0, 4096};
  EXPECT_EQ(a.hash(), b.hash());
}

TEST(FsCacheKeyTest, hashDiffersWhenAnyFieldDiffers) {
  FsCacheKey base{"s3://bucket/file", 0, 4096};
  FsCacheKey differentPath{"s3://bucket/other", 0, 4096};
  FsCacheKey differentOffset{"s3://bucket/file", 4096, 4096};
  FsCacheKey differentSize{"s3://bucket/file", 0, 8192};

  std::unordered_set<uint64_t> hashes{
      base.hash(),
      differentPath.hash(),
      differentOffset.hash(),
      differentSize.hash(),
  };
  EXPECT_EQ(hashes.size(), 4);
}

TEST(FsCacheKeyTest, fileNameContainsHexHashOffsetSize) {
  FsCacheKey k{"s3://bucket/file", 0, 4096};
  const std::string name = k.fileName();
  // 64-bit hash printed as 16 hex chars, then ".0.4096".
  EXPECT_EQ(name.size(), 16 + std::string{".0.4096"}.size());
  EXPECT_NE(name.find(".0.4096"), std::string::npos);
}

TEST(FsCacheKeyTest, usableInStdUnorderedMap) {
  std::unordered_map<FsCacheKey, int, FsCacheKeyHash> m;
  m.emplace(FsCacheKey{"p", 0, 16}, 1);
  m.emplace(FsCacheKey{"p", 16, 16}, 2);
  EXPECT_EQ(m.size(), 2);
  EXPECT_EQ(m.at(FsCacheKey{"p", 0, 16}), 1);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run test to verify it fails (link/compile error)**

Run: `make debug 2>&1 | tail -20`
Expected: failure — `FsCacheKey.h` not found.

- [ ] **Step 3: Write `FsCacheKey.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include <cstdint>
#include <functional>
#include <string>

namespace facebook::velox::cache::fs {

/// Identifies a single cache segment by remote file path, byte offset, and
/// segment size. Phase 1 carries no file version — the design assumes remote
/// files are immutable.
struct FsCacheKey {
  std::string path;
  uint64_t offset{0};
  uint64_t size{0};

  bool operator==(const FsCacheKey& other) const noexcept {
    return offset == other.offset && size == other.size && path == other.path;
  }

  bool operator!=(const FsCacheKey& other) const noexcept {
    return !(*this == other);
  }

  /// Returns a 64-bit hash derived from all three fields. Stable across
  /// processes on the same architecture (folly SpookyHashV2 is fixed seed).
  uint64_t hash() const noexcept;

  /// Returns the on-disk file name "<16-hex-hash>.<offset>.<size>". The hash
  /// is hex (only [0-9a-f]) so the "." separator is unambiguous and the
  /// numeric fields cannot collide with the hash. Used both for file
  /// placement and for parsing during recovery.
  std::string fileName() const;
};

/// Hash functor for std unordered containers.
struct FsCacheKeyHash {
  size_t operator()(const FsCacheKey& key) const noexcept {
    return static_cast<size_t>(key.hash());
  }
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Write `FsCacheKey.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <folly/hash/SpookyHashV2.h>
#include <fmt/format.h>

namespace facebook::velox::cache::fs {

namespace {
// SpookyHashV2 64-bit hash mixes path bytes with offset and size.
uint64_t combinedHash(const std::string& path, uint64_t offset, uint64_t size) {
  uint64_t hash1 = offset;
  uint64_t hash2 = size;
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &hash1, &hash2);
  return hash1 ^ hash2;
}
} // namespace

uint64_t FsCacheKey::hash() const noexcept {
  return combinedHash(path, offset, size);
}

std::string FsCacheKey::fileName() const {
  return fmt::format("{:016x}.{}.{}", hash(), offset, size);
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Wire into CMake**

Edit `velox/common/caching/fscache/CMakeLists.txt`:
- Add `FsCacheKey.cpp` to the `velox_add_library(velox_fscache ...)` SOURCES list (after `FsCache.cpp`).
- Add `FsCacheKey.h` to HEADERS (after `FsCacheConfig.h`).

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FsCacheKeyTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES` (after `FsCacheScaffoldTest.cpp`).

- [ ] **Step 6: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: all 5 FsCacheKeyTest cases PASS plus the scaffold test.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCacheKey.{h,cpp} \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/tests/CMakeLists.txt \
        velox/common/caching/fscache/tests/FsCacheKeyTest.cpp
git commit -m "feat(fscache): FsCacheKey + hash

Path/offset/size identity for cache segments. SpookyHashV2 64-bit
hash used for both bucket selection and on-disk file name.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 3: `feat(fscache): FsCacheGuards (5 lock types)`

**Files:**
- Create: `velox/common/caching/fscache/FsCacheGuards.h`
- Create: `velox/common/caching/fscache/FsCacheGuards.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheGuardsTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt` (add header + cpp)
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt` (add test)

**Why a `.cpp` for a header of types:** debug-only `LockOrderChecker` needs a thread_local definition; keeping the impl out-of-line lets us guard it with `#ifndef NDEBUG`.

- [ ] **Step 1: Write the failing test**

`velox/common/caching/fscache/tests/FsCacheGuardsTest.cpp`:

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheGuardsTest, eachGuardIsIndependentMutex) {
  CachePriorityMutex priorityMutex;
  CacheStateMutex stateMutex;
  CacheMetadataMutex metadataMutex;
  KeyMutex keyMutex;
  FileSegmentMutex segmentMutex;

  // Locking one does not block the others.
  CachePriorityGuard priorityGuard{priorityMutex};
  CacheStateGuard stateGuard{stateMutex};
  CacheMetadataGuard metadataGuard{metadataMutex};
  KeyGuard keyGuard{keyMutex};
  FileSegmentGuard segmentGuard{segmentMutex};

  // All five guards co-exist. If any pair shared a mutex this would deadlock.
  SUCCEED();
}

TEST(FsCacheGuardsTest, guardReleasesOnDestruction) {
  CachePriorityMutex m;
  {
    CachePriorityGuard guard{m};
    EXPECT_FALSE(m.try_lock());
  }
  EXPECT_TRUE(m.try_lock());
  m.unlock();
}

#ifndef NDEBUG
TEST(FsCacheGuardsTest, lockOrderViolationFiresCheck) {
  CachePriorityMutex priorityMutex;
  KeyMutex keyMutex;

  // Acquiring KeyGuard then CachePriorityGuard is out of order.
  KeyGuard keyGuard{keyMutex};
  EXPECT_DEATH({ CachePriorityGuard priorityGuard{priorityMutex}; }, "");
}
#endif

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make debug 2>&1 | tail -20`
Expected: failure — `FsCacheGuards.h` not found.

- [ ] **Step 3: Write `FsCacheGuards.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include <mutex>

namespace facebook::velox::cache::fs {

/// Lock hierarchy. Acquire in this top-to-bottom order to avoid deadlock:
///
///   CachePriorityGuard > CacheStateGuard > CacheMetadataGuard
///     > KeyGuard > FileSegmentGuard
///
/// Phase 1: each guard type wraps an independent std::mutex instance. No
/// per-bucket or per-key subdivision yet. Phase 2 swaps the implementations
/// under the same type names so callers do not change.

/// Order rank used by LockOrderChecker. Lower values must be acquired before
/// higher values on the same thread.
enum class LockRank : int {
  kCachePriority = 0,
  kCacheState = 1,
  kCacheMetadata = 2,
  kKey = 3,
  kFileSegment = 4,
};

#ifndef NDEBUG
/// Records the highest lock rank held on the current thread and fires a CHECK
/// if a lower-ranked lock is then acquired. Debug builds only.
class LockOrderChecker {
 public:
  static void onAcquire(LockRank rank);
  static void onRelease(LockRank rank);
};
#endif

namespace detail {

template <LockRank kRank>
class RankedMutex {
 public:
  void lock() {
    mutex_.lock();
#ifndef NDEBUG
    LockOrderChecker::onAcquire(kRank);
#endif
  }

  void unlock() {
#ifndef NDEBUG
    LockOrderChecker::onRelease(kRank);
#endif
    mutex_.unlock();
  }

  bool try_lock() {
    if (!mutex_.try_lock()) {
      return false;
    }
#ifndef NDEBUG
    LockOrderChecker::onAcquire(kRank);
#endif
    return true;
  }

 private:
  std::mutex mutex_;
};

template <LockRank kRank>
class RankedGuard {
 public:
  explicit RankedGuard(RankedMutex<kRank>& mutex) : mutex_{&mutex} {
    mutex_->lock();
  }

  ~RankedGuard() {
    mutex_->unlock();
  }

  RankedGuard(const RankedGuard&) = delete;
  RankedGuard& operator=(const RankedGuard&) = delete;

 private:
  RankedMutex<kRank>* mutex_;
};

} // namespace detail

using CachePriorityMutex = detail::RankedMutex<LockRank::kCachePriority>;
using CacheStateMutex = detail::RankedMutex<LockRank::kCacheState>;
using CacheMetadataMutex = detail::RankedMutex<LockRank::kCacheMetadata>;
using KeyMutex = detail::RankedMutex<LockRank::kKey>;
using FileSegmentMutex = detail::RankedMutex<LockRank::kFileSegment>;

using CachePriorityGuard = detail::RankedGuard<LockRank::kCachePriority>;
using CacheStateGuard = detail::RankedGuard<LockRank::kCacheState>;
using CacheMetadataGuard = detail::RankedGuard<LockRank::kCacheMetadata>;
using KeyGuard = detail::RankedGuard<LockRank::kKey>;
using FileSegmentGuard = detail::RankedGuard<LockRank::kFileSegment>;

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Write `FsCacheGuards.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include "velox/common/base/Exceptions.h"

#include <vector>

namespace facebook::velox::cache::fs {

#ifndef NDEBUG
namespace {
thread_local std::vector<LockRank> kHeldRanks;
} // namespace

void LockOrderChecker::onAcquire(LockRank rank) {
  if (!kHeldRanks.empty()) {
    VELOX_CHECK_GT(
        static_cast<int>(rank),
        static_cast<int>(kHeldRanks.back()),
        "FsCache lock order violation");
  }
  kHeldRanks.push_back(rank);
}

void LockOrderChecker::onRelease(LockRank rank) {
  VELOX_CHECK(!kHeldRanks.empty(), "Releasing lock with no held lock");
  VELOX_CHECK_EQ(
      static_cast<int>(kHeldRanks.back()),
      static_cast<int>(rank),
      "FsCache lock release out of order");
  kHeldRanks.pop_back();
}
#endif

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Wire into CMake**

Edit `velox/common/caching/fscache/CMakeLists.txt`:
- Add `FsCacheGuards.cpp` to SOURCES.
- Add `FsCacheGuards.h` to HEADERS.

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FsCacheGuardsTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.

- [ ] **Step 6: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: all 3 FsCacheGuardsTest cases PASS (the death test only runs in debug).

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCacheGuards.{h,cpp} \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/tests/CMakeLists.txt \
        velox/common/caching/fscache/tests/FsCacheGuardsTest.cpp
git commit -m "feat(fscache): FsCacheGuards (5 lock types)

Five named lock types with per-instance std::mutex backing and a
debug-only thread-local LockOrderChecker that CHECKs the acquire
order: CachePriority > CacheState > CacheMetadata > Key > FileSegment.

Known limitation (phase 1): all five wrappers share the same backing
std::mutex per FsCache instance, so the rank check is enforced but
true concurrency is still limited to the granularity of that single
mutex. Phase 2 will swap to per-bucket / per-key instances under the
same type names without callers changing.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 4: `feat(fscache): EvictionPolicy interface + LruPolicy`

**Files:**
- Create: `velox/common/caching/fscache/EvictionPolicy.h`
- Create: `velox/common/caching/fscache/LruPolicy.h`
- Create: `velox/common/caching/fscache/LruPolicy.cpp`
- Create: `velox/common/caching/fscache/tests/EvictionPolicyTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

**Forward-decl note:** `EvictionPolicy` uses `FileSegment*` as opaque pointer; full `FileSegment` definition comes in commit 5. Commit 4 adds a minimal `FileSegment` (key/size/state) in step 7 — the test can include `FileSegment.h` directly once that step is done.

- [ ] **Step 1: Write the failing test**

`velox/common/caching/fscache/tests/EvictionPolicyTest.cpp`:

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/LruPolicy.h"

#include <gtest/gtest.h>
#include <vector>

namespace facebook::velox::cache::fs::test {

namespace {
FileSegment makeSegment(uint64_t size, FileSegment::State state = FileSegment::State::kDownloaded) {
  return FileSegment{FsCacheKey{"x", 0, size}, state};
}
} // namespace

TEST(EvictionPolicyTest, lruInsertSelectsOldestFirst) {
  LruPolicy policy;
  auto a = makeSegment(100), b = makeSegment(200), c = makeSegment(300);
  policy.onInsert(&a);
  policy.onInsert(&b);
  policy.onInsert(&c);

  auto victims = policy.selectVictims(250);
  // Need 250 bytes; a(100) + b(200) = 300 satisfies, c is newest so kept.
  EXPECT_EQ(victims.size(), 2);
  EXPECT_EQ(victims[0], &a);
  EXPECT_EQ(victims[1], &b);
}

TEST(EvictionPolicyTest, hitMovesEntryToMostRecentlyUsed) {
  LruPolicy policy;
  auto a = makeSegment(100), b = makeSegment(100), c = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);
  policy.onInsert(&c);

  policy.onHit(&a);

  auto victims = policy.selectVictims(100);
  // After hit on a, LRU order is b, c, a. Evicting 100 bytes picks b.
  ASSERT_EQ(victims.size(), 1);
  EXPECT_EQ(victims[0], &b);
}

TEST(EvictionPolicyTest, removeDropsEntryFromTracking) {
  LruPolicy policy;
  auto a = makeSegment(100), b = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);

  policy.onRemove(&a);

  auto victims = policy.selectVictims(100);
  ASSERT_EQ(victims.size(), 1);
  EXPECT_EQ(victims[0], &b);
}

TEST(EvictionPolicyTest, selectVictimsReturnsEmptyWhenZeroRequested) {
  LruPolicy policy;
  auto a = makeSegment(100);
  policy.onInsert(&a);
  EXPECT_TRUE(policy.selectVictims(0).empty());
}

TEST(EvictionPolicyTest, selectVictimsReturnsAllWhenRequestExceedsTotal) {
  LruPolicy policy;
  auto a = makeSegment(100), b = makeSegment(100);
  policy.onInsert(&a);
  policy.onInsert(&b);
  auto victims = policy.selectVictims(10'000);
  EXPECT_EQ(victims.size(), 2);
}

TEST(EvictionPolicyTest, onInsertRejectsNonDownloadedSegments) {
  LruPolicy policy;
  auto downloading = makeSegment(100, FileSegment::State::kDownloading);
  auto empty = makeSegment(100, FileSegment::State::kEmpty);
  // LruPolicy must only track kDownloaded segments; otherwise selectVictims
  // could return a segment whose download is still in-flight and the writer
  // would race with eviction over the on-disk file.
  EXPECT_THROW(policy.onInsert(&downloading), facebook::velox::VeloxException);
  EXPECT_THROW(policy.onInsert(&empty), facebook::velox::VeloxException);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make debug 2>&1 | tail -20`
Expected: failure — `LruPolicy.h` not found.

- [ ] **Step 3: Write `EvictionPolicy.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include <vector>

namespace facebook::velox::cache::fs {

class FileSegment;

/// Abstract eviction policy. FsCache invokes onInsert when a new segment
/// becomes DOWNLOADED, onHit when an existing segment is read, onRemove
/// when a segment is permanently dropped, and selectVictims to pick segments
/// to evict when bytes are needed.
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  virtual void onInsert(FileSegment* segment) = 0;
  virtual void onHit(FileSegment* segment) = 0;
  virtual void onRemove(FileSegment* segment) = 0;

  /// Returns segments whose combined size is at least bytesNeeded, in the
  /// order they should be evicted (least valuable first). Returns fewer if
  /// the policy holds less than bytesNeeded total.
  virtual std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) = 0;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Write `LruPolicy.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/EvictionPolicy.h"

#include <list>
#include <unordered_map>

namespace facebook::velox::cache::fs {

/// Single-linked LRU implementation. Front of the list is most recently used;
/// back is least recently used and is evicted first. All operations are O(1)
/// via an iterator side index.
///
/// Thread safety: not internally synchronized. Callers hold the appropriate
/// FsCacheGuards lock before calling.
class LruPolicy final : public EvictionPolicy {
 public:
  void onInsert(FileSegment* segment) override;
  void onHit(FileSegment* segment) override;
  void onRemove(FileSegment* segment) override;
  std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) override;

 private:
  std::list<FileSegment*> mruToLru_;
  std::unordered_map<FileSegment*, std::list<FileSegment*>::iterator> index_;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Write `LruPolicy.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/LruPolicy.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"

namespace facebook::velox::cache::fs {

void LruPolicy::onInsert(FileSegment* segment) {
  VELOX_CHECK_NOT_NULL(segment);
  // Callers must only insert segments that have reached kDownloaded; this
  // keeps selectVictims() from returning a segment whose download is still
  // in-flight, which would race with the writer over the on-disk file.
  VELOX_CHECK_EQ(
      static_cast<int>(segment->state()),
      static_cast<int>(FileSegment::State::kDownloaded),
      "LruPolicy tracks only kDownloaded segments");
  VELOX_CHECK_EQ(index_.count(segment), 0, "Segment already tracked");
  mruToLru_.push_front(segment);
  index_[segment] = mruToLru_.begin();
}

void LruPolicy::onHit(FileSegment* segment) {
  auto it = index_.find(segment);
  if (it == index_.end()) {
    return;
  }
  mruToLru_.erase(it->second);
  mruToLru_.push_front(segment);
  it->second = mruToLru_.begin();
}

void LruPolicy::onRemove(FileSegment* segment) {
  auto it = index_.find(segment);
  if (it == index_.end()) {
    return;
  }
  mruToLru_.erase(it->second);
  index_.erase(it);
}

std::vector<FileSegment*> LruPolicy::selectVictims(uint64_t bytesNeeded) {
  std::vector<FileSegment*> victims;
  if (bytesNeeded == 0) {
    return victims;
  }
  uint64_t accumulated = 0;
  for (auto rit = mruToLru_.rbegin(); rit != mruToLru_.rend(); ++rit) {
    victims.push_back(*rit);
    accumulated += (*rit)->size();
    if (accumulated >= bytesNeeded) {
      break;
    }
  }
  // Caller expects oldest-first order; we collected from oldest already.
  return victims;
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 6: Wire into CMake**

Edit `velox/common/caching/fscache/CMakeLists.txt`:
- Add `LruPolicy.cpp` to SOURCES.
- Add `EvictionPolicy.h`, `LruPolicy.h` to HEADERS.

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `EvictionPolicyTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.

**Important compile note:** `LruPolicy.cpp` includes `FileSegment.h` for the
`segment->size()` call. Commit 5 defines `FileSegment`. Until then, the build
will fail. The test stand-in won't help production code.

**Resolution:** In this commit, also add a **minimal** `FileSegment.h` and
`.cpp` with just enough surface for `size()`. Commit 5 then **extends** them
with the full state machine (existing test cases keep passing; new state-machine
tests are added).

- [ ] **Step 7: Add minimal `FileSegment.h`**

`velox/common/caching/fscache/FileSegment.h`:

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <cstdint>

namespace facebook::velox::cache::fs {

/// Single cache segment. Commit 4 introduces only the key/size/state surface
/// used by EvictionPolicy. Commit 5 extends this class with the full download
/// state machine (atomic transitions, beginDownload, download, read).
class FileSegment {
 public:
  enum class State : uint8_t {
    kEmpty = 0,
    kDownloading = 1,
    kDownloaded = 2,
    kDetached = 3,
  };

  /// Constructs a fresh segment. The starting state is kEmpty in commit 5's
  /// extended version; in commit 4 we accept an explicit state so the
  /// EvictionPolicy tests can exercise LruPolicy::onInsert's invariant
  /// (only kDownloaded segments are tracked) without depending on the
  /// download machinery that commit 5 introduces.
  explicit FileSegment(FsCacheKey key, State state = State::kDownloaded)
      : key_{std::move(key)}, state_{state} {}

  const FsCacheKey& key() const {
    return key_;
  }

  uint64_t size() const {
    return key_.size;
  }

  State state() const {
    return state_;
  }

 private:
  FsCacheKey key_;
  State state_;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 8: Add `FileSegment.cpp`**

`velox/common/caching/fscache/FileSegment.cpp`:

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FileSegment.h"

// Definitions added in commit 5.

namespace facebook::velox::cache::fs {} // namespace facebook::velox::cache::fs
```

Add both to `CMakeLists.txt`: `FileSegment.cpp` in SOURCES, `FileSegment.h` in HEADERS.

- [ ] **Step 9: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: 6 EvictionPolicyTest cases PASS plus all earlier tests.

- [ ] **Step 10: Commit**

```bash
git add velox/common/caching/fscache/EvictionPolicy.h \
        velox/common/caching/fscache/LruPolicy.{h,cpp} \
        velox/common/caching/fscache/FileSegment.{h,cpp} \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/tests/CMakeLists.txt \
        velox/common/caching/fscache/tests/EvictionPolicyTest.cpp
git commit -m "feat(fscache): EvictionPolicy interface + LruPolicy

Abstract base class with onInsert/onHit/onRemove/selectVictims, plus a
single-linked LRU implementation with O(1) updates via iterator index.
FileSegment skeleton introduced with size()/key() surface; commit 5
will extend with the download state machine.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 5: `feat(fscache): FileSegment 3-state machine + splitRange`

**Files:**
- Modify: `velox/common/caching/fscache/FileSegment.h` (extend with state machine)
- Modify: `velox/common/caching/fscache/FileSegment.cpp` (implement download/read)
- Modify: `velox/common/caching/fscache/FsCache.h` (declare splitRange)
- Modify: `velox/common/caching/fscache/FsCache.cpp` (implement splitRange)
- Create: `velox/common/caching/fscache/tests/FileSegmentTest.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheSplitRangeTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

- [ ] **Step 1: Write `FileSegmentTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheGuards.h"

#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);

    // Write a remote source file with deterministic content.
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::ofstream remote{remotePath_, std::ios::binary};
    for (uint64_t i = 0; i < 8192; ++i) {
      char c = static_cast<char>(i % 251);
      remote.write(&c, 1);
    }
  }
};

TEST_F(FileSegmentTest, freshSegmentStartsEmpty) {
  FsCacheKey key{remotePath_, 0, 4096};
  FileSegment segment{key};
  EXPECT_EQ(segment.state(), FileSegment::State::kEmpty);
  EXPECT_EQ(segment.downloadedSize(), 0);
}

TEST_F(FileSegmentTest, downloadFromLocalFileSucceeds) {
  FsCacheKey key{remotePath_, 0, 4096};
  FileSegment segment{key};

  auto remote = std::make_shared<LocalReadFile>(remotePath_);
  ASSERT_TRUE(segment.beginDownload());
  segment.download(*remote, cacheRoot_);

  EXPECT_EQ(segment.state(), FileSegment::State::kDownloaded);
  EXPECT_EQ(segment.downloadedSize(), 4096);

  // Local file exists at <cacheRoot>/<hash[0:2]>/<hash[2:4]>/<fileName>.
  EXPECT_TRUE(std::filesystem::exists(segment.localPath(cacheRoot_)));
  EXPECT_EQ(
      std::filesystem::file_size(segment.localPath(cacheRoot_)), 4096);
}

TEST_F(FileSegmentTest, readReturnsExactBytes) {
  FsCacheKey key{remotePath_, 256, 1024};
  FileSegment segment{key};
  auto remote = std::make_shared<LocalReadFile>(remotePath_);
  ASSERT_TRUE(segment.beginDownload());
  segment.download(*remote, cacheRoot_);

  std::string buf(1024, '\0');
  segment.read(0, 1024, buf.data(), cacheRoot_);
  for (uint64_t i = 0; i < 1024; ++i) {
    EXPECT_EQ(
        static_cast<unsigned char>(buf[i]),
        static_cast<unsigned char>((256 + i) % 251));
  }
}

TEST_F(FileSegmentTest, downloadFailureLeavesEmpty) {
  FsCacheKey key{"/nonexistent/path", 0, 4096};
  FileSegment segment{key};
  ASSERT_TRUE(segment.beginDownload());
  EXPECT_THROW(
      {
        LocalReadFile bogus{"/nonexistent/path"};
        segment.download(bogus, cacheRoot_);
      },
      std::exception);
  EXPECT_EQ(segment.state(), FileSegment::State::kEmpty);
  EXPECT_FALSE(std::filesystem::exists(segment.localPath(cacheRoot_)));
  EXPECT_FALSE(
      std::filesystem::exists(segment.localPath(cacheRoot_) + ".tmp"));
}

TEST_F(FileSegmentTest, downloadAtomicityViaTmpRename) {
  FsCacheKey key{remotePath_, 0, 4096};
  FileSegment segment{key};
  auto remote = std::make_shared<LocalReadFile>(remotePath_);
  ASSERT_TRUE(segment.beginDownload());
  segment.download(*remote, cacheRoot_);
  // After successful download, .tmp must not exist.
  EXPECT_FALSE(
      std::filesystem::exists(segment.localPath(cacheRoot_) + ".tmp"));
}

TEST_F(FileSegmentTest, beginDownloadIsOneShot) {
  FsCacheKey key{remotePath_, 0, 4096};
  FileSegment segment{key};
  EXPECT_TRUE(segment.beginDownload());
  // Subsequent callers must lose the CAS race.
  EXPECT_FALSE(segment.beginDownload());
  EXPECT_FALSE(segment.beginDownload());
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Write `FsCacheSplitRangeTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

namespace {
constexpr uint64_t kAlign = 4UL * 1024 * 1024;
constexpr uint64_t kMaxSeg = 32UL * 1024 * 1024;

FsCacheConfig defaultConfig() {
  FsCacheConfig c;
  c.cacheRoot = "/tmp/unused";
  c.alignment = kAlign;
  c.maxSegmentSize = kMaxSeg;
  return c;
}
} // namespace

TEST(FsCacheSplitRangeTest, alignedRangeWithinMaxYieldsOneSegment) {
  auto ranges = FsCache::splitRange(0, kAlign, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kAlign);
}

TEST(FsCacheSplitRangeTest, unalignedStartExpandsDown) {
  auto ranges = FsCache::splitRange(100, 1024, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kAlign);
}

TEST(FsCacheSplitRangeTest, unalignedEndExpandsUp) {
  auto ranges = FsCache::splitRange(0, kAlign + 1, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, 2 * kAlign);
}

TEST(FsCacheSplitRangeTest, holeLargerThanMaxSplits) {
  // 64 MiB hole split at max 32 MiB.
  auto ranges = FsCache::splitRange(0, 64UL * 1024 * 1024, defaultConfig());
  ASSERT_EQ(ranges.size(), 2);
  EXPECT_EQ(ranges[0].first, 0);
  EXPECT_EQ(ranges[0].second, kMaxSeg);
  EXPECT_EQ(ranges[1].first, kMaxSeg);
  EXPECT_EQ(ranges[1].second, kMaxSeg);
}

TEST(FsCacheSplitRangeTest, holeOnSegmentBoundaryDoesNotProduceEmpty) {
  auto ranges = FsCache::splitRange(0, 2 * kMaxSeg, defaultConfig());
  ASSERT_EQ(ranges.size(), 2);
  for (const auto& r : ranges) {
    EXPECT_GT(r.second, 0);
  }
}

TEST(FsCacheSplitRangeTest, smallTailKeepsAlignmentExpansion) {
  // Read [0, kAlign + 100): aligned-up end = 2*kAlign. Single segment of
  // 2*kAlign (well under kMaxSeg).
  auto ranges = FsCache::splitRange(0, kAlign + 100, defaultConfig());
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].second, 2 * kAlign);
}

TEST(FsCacheSplitRangeTest, zeroSizeReturnsEmpty) {
  // size==0 must short-circuit to an empty range list; otherwise the
  // outward-alignment math would produce a phantom segment [alignedStart,
  // alignedStart) which downstream code would persist as a 0-byte file.
  EXPECT_TRUE(FsCache::splitRange(0, 0, defaultConfig()).empty());
  EXPECT_TRUE(FsCache::splitRange(7 * kAlign + 12345, 0, defaultConfig()).empty());
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make debug 2>&1 | tail -20`
Expected: failures — `FileSegment::State`, `download`, `read`, `localPath`, `FsCache::splitRange` undefined.

- [ ] **Step 4: Extend `FileSegment.h`**

Replace the commit-4 minimal version with:

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <string>

namespace facebook::velox::cache {
class ReadFile; // forward
} // namespace facebook::velox::cache

namespace facebook::velox {
class ReadFile; // velox::ReadFile lives in facebook::velox
} // namespace facebook::velox

namespace facebook::velox::cache::fs {

/// Single cache segment with download state machine.
///
/// Lifecycle:
///   kEmpty --download()--> kDownloading --on completion--> kDownloaded
///   kDownloaded --evict() while refCount > 0--> kDetached
///
/// Phase 1 download is synchronous: one writer blocks all other waiters via
/// the per-segment cv until either kDownloaded or kEmpty (on failure).
class FileSegment {
 public:
  enum class State : uint8_t {
    kEmpty = 0,
    kDownloading = 1,
    kDownloaded = 2,
    kDetached = 3,
  };

  explicit FileSegment(FsCacheKey key) : key_{std::move(key)} {}

  const FsCacheKey& key() const {
    return key_;
  }

  uint64_t size() const {
    return key_.size;
  }

  State state() const {
    return state_.load(std::memory_order_acquire);
  }

  uint64_t downloadedSize() const {
    return downloadedSize_.load(std::memory_order_acquire);
  }

  /// Computes the local file path for this segment under cacheRoot. Does not
  /// touch the filesystem.
  std::string localPath(const std::string& cacheRoot) const;

  /// Atomically transitions kEmpty → kDownloading. Returns true if this
  /// caller won the race and must follow up with download(); returns false
  /// if another thread already started or completed. Standalone callers
  /// (and tests) must call this before download(). FsCache calls it under
  /// mutex_ during coordination.
  bool beginDownload();

  /// Downloads the segment from remote into cacheRoot. Caller must have won
  /// beginDownload() (state must be kDownloading on entry). Throws on
  /// failure and resets state to kEmpty.
  void download(::facebook::velox::ReadFile& remote, const std::string& cacheRoot);

  /// Reads bytes [offsetInSegment, offsetInSegment + length) from the local
  /// file into outBuf. State must be kDownloaded or kDetached.
  void read(
      uint64_t offsetInSegment,
      uint64_t length,
      char* outBuf,
      const std::string& cacheRoot) const;

 public:
  /// Mutex held by FsCache::lookupOrCreate while CAS-ing state_ and waiting
  /// on cv_. Exposed publicly (rather than via friend) because FileSegment is
  /// a coordination object whose synchronization is orchestrated by FsCache;
  /// hiding mutex_/cv_ would only push FsCache logic into FileSegment.
  mutable FileSegmentMutex mutex_;

  /// Notifies waiters when state_ leaves kDownloading. Paired with mutex_.
  mutable std::condition_variable_any cv_;

 private:
  FsCacheKey key_;
  std::atomic<State> state_{State::kEmpty};
  std::atomic<uint64_t> downloadedSize_{0};

  // Phase 1: hits_ recorded but not consumed (SLRU upgrade happens in phase 2).
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> refCount_{0};
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Implement `FileSegment.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <fmt/format.h>
#include <filesystem>
#include <fstream>

namespace facebook::velox::cache::fs {

std::string FileSegment::localPath(const std::string& cacheRoot) const {
  const std::string name = key_.fileName();
  return fmt::format(
      "{}/{}/{}/{}", cacheRoot, name.substr(0, 2), name.substr(2, 2), name);
}

bool FileSegment::beginDownload() {
  State expected = State::kEmpty;
  return state_.compare_exchange_strong(
      expected, State::kDownloading, std::memory_order_acq_rel);
}

void FileSegment::download(
    ::facebook::velox::ReadFile& remote,
    const std::string& cacheRoot) {
  VELOX_CHECK_EQ(
      static_cast<int>(state()),
      static_cast<int>(State::kDownloading),
      "FileSegment::download requires beginDownload() to have been called");
  const std::string finalPath = localPath(cacheRoot);
  const std::string tmpPath = finalPath + ".tmp";

  std::filesystem::create_directories(
      std::filesystem::path{finalPath}.parent_path());

  try {
    std::ofstream out{tmpPath, std::ios::binary | std::ios::trunc};
    VELOX_CHECK(out.is_open(), "Cannot open .tmp for write: {}", tmpPath);

    constexpr uint64_t kChunk = 1UL * 1024 * 1024;
    std::string buffer(kChunk, '\0');
    uint64_t remaining = key_.size;
    uint64_t offset = key_.offset;
    while (remaining > 0) {
      const uint64_t toRead = std::min(kChunk, remaining);
      std::string_view view = remote.pread(offset, toRead, buffer.data());
      VELOX_CHECK_EQ(
          view.size(),
          toRead,
          "Short read from remote: got {}, expected {}",
          view.size(),
          toRead);
      out.write(view.data(), view.size());
      VELOX_CHECK(out.good(), "Write to .tmp failed");
      offset += toRead;
      remaining -= toRead;
      downloadedSize_.fetch_add(toRead, std::memory_order_release);
    }
    out.flush();
    out.close();

    std::filesystem::rename(tmpPath, finalPath);
    state_.store(State::kDownloaded, std::memory_order_release);
  } catch (...) {
    std::error_code ignore;
    std::filesystem::remove(tmpPath, ignore);
    std::filesystem::remove(finalPath, ignore);
    downloadedSize_.store(0, std::memory_order_release);
    state_.store(State::kEmpty, std::memory_order_release);
    throw;
  }
}

void FileSegment::read(
    uint64_t offsetInSegment,
    uint64_t length,
    char* outBuf,
    const std::string& cacheRoot) const {
  VELOX_CHECK(
      state() == State::kDownloaded || state() == State::kDetached,
      "FileSegment::read called in state {}",
      static_cast<int>(state()));
  VELOX_CHECK_LE(offsetInSegment + length, key_.size);

  const std::string path = localPath(cacheRoot);
  std::ifstream in{path, std::ios::binary};
  VELOX_CHECK(in.is_open(), "Cannot open cache file: {}", path);
  in.seekg(static_cast<std::streamoff>(offsetInSegment));
  in.read(outBuf, static_cast<std::streamsize>(length));
  VELOX_CHECK_EQ(
      static_cast<uint64_t>(in.gcount()),
      length,
      "Short read from cache file: {}",
      path);
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 6: Add `splitRange` to `FsCache.h`**

Edit `FsCache.h`. Add to the public section:

```cpp
 public:
  /// Splits an arbitrary [offset, offset+size) range into aligned cache
  /// segments. Each returned (offset, size) is:
  ///   - aligned to config.alignment on both ends, and
  ///   - at most config.maxSegmentSize bytes.
  /// Returned ranges cover the original input exactly (after alignment).
  static std::vector<std::pair<uint64_t, uint64_t>> splitRange(
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& config);
```

Add `#include <utility>` and `#include <vector>`.

- [ ] **Step 7: Implement `splitRange` in `FsCache.cpp`**

```cpp
std::vector<std::pair<uint64_t, uint64_t>> FsCache::splitRange(
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& config) {
  std::vector<std::pair<uint64_t, uint64_t>> result;
  if (size == 0) {
    return result;
  }
  // Align outwards.
  const uint64_t alignedStart = (offset / config.alignment) * config.alignment;
  const uint64_t end = offset + size;
  uint64_t alignedEnd = end;
  if (end % config.alignment != 0) {
    alignedEnd = ((end / config.alignment) + 1) * config.alignment;
  }
  // Split into max-size chunks. The size>0 early-return plus outward
  // alignment guarantees alignedStart < alignedEnd, so at least one chunk
  // is produced; the explicit VELOX_CHECK_GT guards against a future
  // refactor accidentally producing a zero-size chunk (which would create
  // a "<hash>.<offset>.0" cache file that loadFromDisk would silently
  // accept).
  uint64_t cursor = alignedStart;
  while (cursor < alignedEnd) {
    const uint64_t chunkSize =
        std::min(config.maxSegmentSize, alignedEnd - cursor);
    VELOX_CHECK_GT(chunkSize, 0);
    result.emplace_back(cursor, chunkSize);
    cursor += chunkSize;
  }
  return result;
}
```

- [ ] **Step 8: Adapt `EvictionPolicyTest.cpp` to the extended `FileSegment`**

Commit 5 dropped the `(FsCacheKey, State)` ctor (state is now driven by the
download state machine; fresh segments start at kEmpty). Adjust
`EvictionPolicyTest.cpp` accordingly.

Add a fixture that owns a small remote file used by the happy-path tests:

```cpp
class EvictionPolicyTest : public ::testing::Test {
 protected:
  std::shared_ptr<::facebook::velox::common::testutil::TempDirectoryPath>
      tempDir_;
  std::string remotePath_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = ::facebook::velox::common::testutil::TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::ofstream out{remotePath_, std::ios::binary};
    // 4 KiB is more than enough for any size used below.
    std::string blob(4096, 'a');
    out.write(blob.data(), blob.size());
  }

  // Builds a segment and brings it to kDownloaded via real IO.
  std::unique_ptr<FileSegment> downloaded(uint64_t size) {
    auto seg = std::make_unique<FileSegment>(FsCacheKey{remotePath_, 0, size});
    VELOX_CHECK(seg->beginDownload());
    LocalReadFile remote{remotePath_};
    seg->download(remote, cacheRoot_);
    return seg;
  }
};
```

Convert every existing `TEST(EvictionPolicyTest, ...)` to
`TEST_F(EvictionPolicyTest, ...)`. Replace each `makeSegment(N)` call with
`downloaded(N)`; store the returned `unique_ptr` in the test scope and pass
its `.get()` to `policy.onInsert`. Update the size-based assertions to use
the same numeric sizes as before.

For `onInsertRejectsNonDownloadedSegments`, build the kEmpty and
kDownloading segments directly without calling `download()`:

```cpp
TEST_F(EvictionPolicyTest, onInsertRejectsNonDownloadedSegments) {
  LruPolicy policy;
  FileSegment empty{FsCacheKey{remotePath_, 0, 100}};
  FileSegment downloading{FsCacheKey{remotePath_, 100, 100}};
  ASSERT_TRUE(downloading.beginDownload());
  EXPECT_THROW(policy.onInsert(&empty), facebook::velox::VeloxException);
  EXPECT_THROW(policy.onInsert(&downloading), facebook::velox::VeloxException);
}
```

Drop the free-function `makeSegment` helper introduced in commit 4.

- [ ] **Step 9: Wire test files into CMake**

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FileSegmentTest.cpp` and `FsCacheSplitRangeTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.
- Add `velox_file` and `velox_temp_path` to `VELOX_FSCACHE_TEST_DEPS` (the deps are listed in `velox/common/testutil/CMakeLists.txt`; check the existing target name with `grep -r "velox_temp_path\|velox_test_util" /home/chang/OpenSource/velox2/velox/common/testutil/CMakeLists.txt` and use the matching target).

- [ ] **Step 10: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: 6 EvictionPolicyTest + 6 FileSegmentTest + 7 FsCacheSplitRangeTest cases PASS, plus all prior tests.

- [ ] **Step 11: Commit**

```bash
git add velox/common/caching/fscache/FileSegment.{h,cpp} \
        velox/common/caching/fscache/FsCache.{h,cpp} \
        velox/common/caching/fscache/tests/EvictionPolicyTest.cpp \
        velox/common/caching/fscache/tests/FileSegmentTest.cpp \
        velox/common/caching/fscache/tests/FsCacheSplitRangeTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "feat(fscache): FileSegment 3-state machine + splitRange

FileSegment gains the kEmpty/kDownloading/kDownloaded/kDetached state
machine, .tmp+rename atomic write, and chunked pread from the remote
ReadFile. FsCache::splitRange implements ClickHouse-style outward
alignment to 4 MiB plus 32 MiB max-segment splitting.

Known limitations (phase 1):
- 3 active states (+ kDetached) vs ClickHouse's 6
  (EMPTY/DOWNLOADING/DOWNLOADED/PARTIALLY_DOWNLOADED/PARTIALLY_DOWNLOADED_NO_CONTINUATION/DETACHED).
  We drop the partial-download progression because phase 1 publishes
  a segment only on full download (all-or-nothing via .tmp + rename).
  Partial-progress states return when background download lands in
  phase 2.
- download() reads the whole segment (up to 32 MiB) into a heap
  buffer before writing to .tmp. Memory pressure proportional to
  download concurrency. A streaming pread → write loop is a
  follow-up; phase 1 keeps the implementation small to keep the
  correctness audit short.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 6: `feat(fscache): FsCacheMetadata + top-level FsCache`

**Files:**
- Create: `velox/common/caching/fscache/FsCacheMetadata.h`
- Create: `velox/common/caching/fscache/FsCacheMetadata.cpp`
- Modify: `velox/common/caching/fscache/FsCache.h` (add `getOrSet`)
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

- [ ] **Step 1: Write `FsCacheMetadataTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheMetadata.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

TEST(FsCacheMetadataTest, insertAndLookupSameKey) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  EXPECT_TRUE(metadata.insert(segment));

  auto found = metadata.lookup(FsCacheKey{"p", 0, 16});
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found.get(), segment.get());
}

TEST(FsCacheMetadataTest, lookupReturnsNullForMissingKey) {
  FsCacheMetadata metadata{8};
  EXPECT_EQ(metadata.lookup(FsCacheKey{"missing", 0, 16}), nullptr);
}

TEST(FsCacheMetadataTest, insertDuplicateReturnsFalse) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  auto b = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_FALSE(metadata.insert(b));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}).get(), a.get());
}

TEST(FsCacheMetadataTest, eraseRemovesFromBucket) {
  FsCacheMetadata metadata{8};
  auto segment = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  metadata.insert(segment);
  EXPECT_TRUE(metadata.erase(FsCacheKey{"p", 0, 16}));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}), nullptr);
}

TEST(FsCacheMetadataTest, keysWithSamePathDifferentOffsetCoexist) {
  FsCacheMetadata metadata{8};
  auto a = std::make_shared<FileSegment>(FsCacheKey{"p", 0, 16});
  auto b = std::make_shared<FileSegment>(FsCacheKey{"p", 16, 16});
  EXPECT_TRUE(metadata.insert(a));
  EXPECT_TRUE(metadata.insert(b));
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 0, 16}).get(), a.get());
  EXPECT_EQ(metadata.lookup(FsCacheKey{"p", 16, 16}).get(), b.get());
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Write `FsCacheTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  FsCacheConfig config_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    config_.cacheRoot = tempDir_->getPath() + "/cache";
    config_.maxBytes = 100UL * 1024 * 1024;
    std::filesystem::create_directories(config_.cacheRoot);
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::ofstream out{remotePath_, std::ios::binary};
    std::string blob(8UL * 1024 * 1024, 'a');
    out.write(blob.data(), blob.size());
  }
};

TEST_F(FsCacheTest, getOrSetFirstCallDownloads) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  auto segments = cache.getOrSet(remotePath_, 0, 4096, remote);
  ASSERT_FALSE(segments.empty());
  for (auto& seg : segments) {
    EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
  }
}

TEST_F(FsCacheTest, getOrSetSecondCallHitsCache) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  cache.getOrSet(remotePath_, 0, 4096, remote);
  // Second call: must not download again. Verified by checking downloadedSize
  // remains stable and state is already kDownloaded before any work.
  auto segments = cache.getOrSet(remotePath_, 0, 4096, remote);
  ASSERT_FALSE(segments.empty());
  for (auto& seg : segments) {
    EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
  }
  EXPECT_GT(cache.stats().hits, 0);
}

TEST_F(FsCacheTest, evictionRunsWhenOverCapacity) {
  FsCacheConfig tiny = config_;
  tiny.maxBytes = 5UL * 1024 * 1024; // 5 MiB
  FsCache cache{tiny};
  LocalReadFile remote{remotePath_};
  // Fill: read 2 disjoint 4 MiB chunks. Total 8 MiB > 5 MiB -> eviction.
  cache.getOrSet(remotePath_, 0, 4UL * 1024 * 1024, remote);
  cache.getOrSet(remotePath_, 4UL * 1024 * 1024, 4UL * 1024 * 1024, remote);
  EXPECT_LE(cache.stats().bytesOnDisk, tiny.maxBytes);
  EXPECT_GT(cache.stats().evictions, 0);
}

// Regression: writer's IO failure must not crash waiters via VELOX_CHECK.
// Waiters surface the failure via a user-facing throw instead.
TEST_F(FsCacheTest, waiterReceivesThrowWhenWriterFails) {
  // Build a remote ReadFile that throws on pread to simulate IO failure.
  class ThrowingReadFile : public ::facebook::velox::ReadFile {
   public:
    std::string_view pread(
        uint64_t /*offset*/,
        uint64_t /*length*/,
        void* /*buf*/,
        const ::facebook::velox::FileIoContext& /*context*/)
        const override {
      VELOX_FAIL("simulated remote IO failure");
    }
    uint64_t size() const override {
      return 8UL * 1024 * 1024;
    }
    uint64_t memoryUsage() const override {
      return 0;
    }
    bool shouldCoalesce() const override {
      return false;
    }
    std::string getName() const override {
      return "throwing";
    }
    uint64_t getNaturalReadSize() const override {
      return 4096;
    }
  };

  FsCache cache{config_};
  ThrowingReadFile remote;
  // Two threads race on the same key. Exactly one becomes the writer (will
  // throw); the other is the waiter (must also throw, not CHECK-crash).
  std::atomic<int> throwCount{0};
  std::atomic<int> crashCount{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&] {
      try {
        cache.getOrSet(remotePath_, 0, 4096, remote);
      } catch (const std::exception&) {
        ++throwCount;
      } catch (...) {
        ++crashCount;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(throwCount.load(), 2);
  EXPECT_EQ(crashCount.load(), 0);
}

// Regression for #12: a segment that already reached kDownloaded (recovered
// from disk or completed earlier) must increment stats.hits when re-read,
// so it participates in eviction policy and does not leak.
TEST_F(FsCacheTest, secondReaderOfDownloadedSegmentCountsAsHit) {
  FsCache cache{config_};
  LocalReadFile remote{remotePath_};
  cache.getOrSet(remotePath_, 0, 4096, remote);
  const auto baseline = cache.stats();
  cache.getOrSet(remotePath_, 0, 4096, remote);
  const auto after = cache.stats();
  EXPECT_EQ(after.misses, baseline.misses);
  EXPECT_EQ(after.hits, baseline.hits + 1);
  EXPECT_EQ(after.bytesOnDisk, baseline.bytesOnDisk);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 3: Run to verify failure**

Run: `make debug 2>&1 | tail -20`
Expected: failure — `FsCacheMetadata.h` missing, `FsCache::getOrSet` and `FsCache::stats()` undefined.

- [ ] **Step 4: Write `FsCacheMetadata.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheGuards.h"

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

namespace facebook::velox::cache::fs {

using FileSegmentPtr = std::shared_ptr<FileSegment>;

/// Two-level index:
///   buckets[N]                     <- bucket = hash(key) % N
///     -> unordered_map<FsCacheKey, KeyMetadataPtr>
///          -> KeyMetadata
///               -> std::map<offset, FileSegmentPtr>
///
/// Phase 1: a single CacheMetadataMutex guards all buckets and a single
/// KeyMutex guards all per-key maps. Phase 2 subdivides without changing
/// public surface.
class FsCacheMetadata {
 public:
  explicit FsCacheMetadata(size_t numBuckets);

  /// Inserts the segment. Returns false if a segment with the same key
  /// already exists.
  bool insert(FileSegmentPtr segment);

  /// Returns the segment for key or nullptr if not present.
  FileSegmentPtr lookup(const FsCacheKey& key) const;

  /// Removes the segment for key. Returns true if it existed.
  bool erase(const FsCacheKey& key);

  /// Returns all segments currently in the index. Order is unspecified.
  /// Used by recovery and tests; not on the hot path.
  std::vector<FileSegmentPtr> snapshot() const;

 private:
  size_t bucketIndex(const FsCacheKey& key) const {
    return static_cast<size_t>(key.hash() % buckets_.size());
  }

  mutable CacheMetadataMutex mutex_;
  std::vector<std::unordered_map<FsCacheKey, FileSegmentPtr, FsCacheKeyHash>>
      buckets_;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Write `FsCacheMetadata.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCacheMetadata.h"

namespace facebook::velox::cache::fs {

FsCacheMetadata::FsCacheMetadata(size_t numBuckets) : buckets_(numBuckets) {}

bool FsCacheMetadata::insert(FileSegmentPtr segment) {
  CacheMetadataGuard guard{mutex_};
  auto& bucket = buckets_[bucketIndex(segment->key())];
  auto [it, inserted] = bucket.emplace(segment->key(), std::move(segment));
  return inserted;
}

FileSegmentPtr FsCacheMetadata::lookup(const FsCacheKey& key) const {
  CacheMetadataGuard guard{mutex_};
  const auto& bucket = buckets_[bucketIndex(key)];
  auto it = bucket.find(key);
  if (it == bucket.end()) {
    return nullptr;
  }
  return it->second;
}

bool FsCacheMetadata::erase(const FsCacheKey& key) {
  CacheMetadataGuard guard{mutex_};
  auto& bucket = buckets_[bucketIndex(key)];
  return bucket.erase(key) > 0;
}

std::vector<FileSegmentPtr> FsCacheMetadata::snapshot() const {
  CacheMetadataGuard guard{mutex_};
  std::vector<FileSegmentPtr> result;
  for (const auto& bucket : buckets_) {
    for (const auto& [_, seg] : bucket) {
      result.push_back(seg);
    }
  }
  return result;
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 6: Extend `FsCache.h` with `getOrSet` and `stats()`**

Replace `FsCache.h` body section:

```cpp
namespace facebook::velox::cache::fs {

struct FsCacheStats {
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t evictions{0};
  uint64_t bytesOnDisk{0};
};

class FsCache {
 public:
  explicit FsCache(FsCacheConfig config);
  ~FsCache();

  /// Returns segments covering [offset, offset+size) for the given path,
  /// downloading any missing segments synchronously from remote.
  std::vector<FileSegmentPtr> getOrSet(
      const std::string& path,
      uint64_t offset,
      uint64_t size,
      ::facebook::velox::ReadFile& remote);

  /// Snapshot of counters. Cheap; for tests / observability.
  FsCacheStats stats() const;

  static std::vector<std::pair<uint64_t, uint64_t>> splitRange(
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& config);

 private:
  // Downloads one segment under proper lock coordination. Used by getOrSet.
  FileSegmentPtr lookupOrCreate(
      const FsCacheKey& key,
      ::facebook::velox::ReadFile& remote);

  // Evicts until bytesOnDisk + bytesNeeded <= maxBytes.
  void evict(uint64_t bytesNeeded);

  const FsCacheConfig config_;
  std::unique_ptr<FsCacheMetadata> metadata_;
  std::unique_ptr<EvictionPolicy> policy_;

  mutable CacheStateMutex stateMutex_;
  mutable CachePriorityMutex priorityMutex_;
  FsCacheStats stats_;
};

} // namespace facebook::velox::cache::fs
```

Add includes: `<memory>`, `EvictionPolicy.h`, `FsCacheMetadata.h`.

- [ ] **Step 7: Implement `FsCache.cpp`**

Append below the existing constructor:

```cpp
#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/LruPolicy.h"
#include "velox/common/file/File.h"

namespace facebook::velox::cache::fs {

FsCache::FsCache(FsCacheConfig config)
    : config_{std::move(config)},
      metadata_{std::make_unique<FsCacheMetadata>(config_.numBuckets)},
      policy_{std::make_unique<LruPolicy>()} {}

FsCache::~FsCache() = default;

std::vector<FileSegmentPtr> FsCache::getOrSet(
    const std::string& path,
    uint64_t offset,
    uint64_t size,
    ::facebook::velox::ReadFile& remote) {
  auto ranges = splitRange(offset, size, config_);
  std::vector<FileSegmentPtr> result;
  result.reserve(ranges.size());
  for (const auto& [segOffset, segSize] : ranges) {
    FsCacheKey key{path, segOffset, segSize};
    result.push_back(lookupOrCreate(key, remote));
  }
  return result;
}

FileSegmentPtr FsCache::lookupOrCreate(
    const FsCacheKey& key,
    ::facebook::velox::ReadFile& remote) {
  // 1. Fast path: existing kDownloaded segment.
  if (auto existing = metadata_->lookup(key);
      existing != nullptr &&
      existing->state() == FileSegment::State::kDownloaded) {
    {
      CachePriorityGuard guard{priorityMutex_};
      policy_->onHit(existing.get());
    }
    {
      CacheStateGuard guard{stateMutex_};
      ++stats_.hits;
    }
    return existing;
  }

  // 2. Insert (or pick up existing) segment under the metadata lock.
  auto segment = std::make_shared<FileSegment>(key);
  if (!metadata_->insert(segment)) {
    segment = metadata_->lookup(key);
    VELOX_CHECK_NOT_NULL(segment);
  }

  // 3. Coordinate download. Only the thread that wins beginDownload() does
  //    the actual fetch; concurrent waiters block on cv_ until the writer
  //    either completes (kDownloaded) or fails (kEmpty). On failure each
  //    waiter throws so the caller can retry or surface the error; the
  //    segment metadata entry stays so a subsequent caller can race for
  //    beginDownload() again.
  std::unique_lock<FileSegmentMutex> lock{segment->mutex_};
  if (segment->state() == FileSegment::State::kDownloaded) {
    // Another thread completed between fast-path lookup and metadata insert.
    // Treat as a hit; the slow-path fall-through is not a miss.
    lock.unlock();
    {
      CachePriorityGuard guard{priorityMutex_};
      policy_->onHit(segment.get());
    }
    {
      CacheStateGuard guard{stateMutex_};
      ++stats_.hits;
    }
    return segment;
  }
  if (segment->beginDownload()) {
    // 3a. Writer path.
    lock.unlock();
    // Reserve capacity by evicting kDownloaded victims; never touches
    // kDownloading segments because LruPolicy only contains segments that
    // reached kDownloaded (onInsert is called on completion, see below).
    evict(key.size);
    bool succeeded = false;
    try {
      segment->download(remote, config_.cacheRoot);
      succeeded = true;
    } catch (...) {
      // download() already reset state_ to kEmpty and removed the .tmp/file.
      std::lock_guard<FileSegmentMutex> resetLock{segment->mutex_};
      segment->cv_.notify_all();
      throw;
    }
    if (succeeded) {
      {
        CachePriorityGuard guard{priorityMutex_};
        policy_->onInsert(segment.get());
      }
      {
        CacheStateGuard guard{stateMutex_};
        ++stats_.misses;
        stats_.bytesOnDisk += key.size;
      }
      std::lock_guard<FileSegmentMutex> notifyLock{segment->mutex_};
      segment->cv_.notify_all();
    }
    return segment;
  }
  // 3b. Waiter path: another thread is downloading; wait for completion or
  //     failure. cv_ is notified on both outcomes (see writer path above).
  segment->cv_.wait(lock, [&] {
    return segment->state() != FileSegment::State::kDownloading;
  });
  const auto finalState = segment->state();
  lock.unlock();
  if (finalState != FileSegment::State::kDownloaded) {
    // Writer threw. Surface as a user-level error; caller may retry by
    // calling getOrSet again, at which point a fresh race for beginDownload()
    // happens. Do not VELOX_CHECK here: that would convert another thread's
    // I/O failure into a CHECK-failure CRASH on the waiter.
    VELOX_USER_FAIL(
        "FsCache concurrent download failed for {} [{}..{})",
        key.path,
        key.offset,
        key.offset + key.size);
  }
  // Successful concurrent download counts as a hit for this thread.
  {
    CachePriorityGuard guard{priorityMutex_};
    policy_->onHit(segment.get());
  }
  {
    CacheStateGuard guard{stateMutex_};
    ++stats_.hits;
  }
  return segment;
}

void FsCache::evict(uint64_t bytesNeeded) {
  uint64_t current;
  {
    CacheStateGuard guard{stateMutex_};
    current = stats_.bytesOnDisk;
  }
  if (current + bytesNeeded <= config_.maxBytes) {
    return;
  }
  const uint64_t toFree = current + bytesNeeded - config_.maxBytes;

  std::vector<FileSegment*> victims;
  {
    CachePriorityGuard guard{priorityMutex_};
    victims = policy_->selectVictims(toFree);
    for (auto* v : victims) {
      policy_->onRemove(v);
    }
  }

  uint64_t freed = 0;
  for (auto* victim : victims) {
    const auto key = victim->key();
    metadata_->erase(key);
    std::error_code ignore;
    std::filesystem::remove(victim->localPath(config_.cacheRoot), ignore);
    freed += key.size;
  }
  {
    CacheStateGuard guard{stateMutex_};
    stats_.evictions += victims.size();
    stats_.bytesOnDisk -= std::min(stats_.bytesOnDisk, freed);
  }
}

FsCacheStats FsCache::stats() const {
  CacheStateGuard guard{stateMutex_};
  return stats_;
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 8: Wire CMake**

Edit `velox/common/caching/fscache/CMakeLists.txt`:
- Add `FsCacheMetadata.cpp` to SOURCES.
- Add `FsCacheMetadata.h` to HEADERS.

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FsCacheMetadataTest.cpp`, `FsCacheTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.

- [ ] **Step 9: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: 5 FsCacheMetadataTest + 3 FsCacheTest cases PASS, plus all prior tests.

- [ ] **Step 10: Commit**

```bash
git add velox/common/caching/fscache/FsCacheMetadata.{h,cpp} \
        velox/common/caching/fscache/FsCache.{h,cpp} \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/tests/CMakeLists.txt \
        velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp \
        velox/common/caching/fscache/tests/FsCacheTest.cpp
git commit -m "feat(fscache): FsCacheMetadata + top-level FsCache

Two-level bucket/key index; FsCache::getOrSet drives splitRange ->
lookupOrCreate -> download with FileSegment cv coordination. LRU
eviction kicks in when bytesOnDisk + needed exceeds maxBytes.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 7: `feat(dwio): FsCacheBufferedInput`

**Files:**
- Create: `velox/dwio/common/FsCacheBufferedInput.h`
- Create: `velox/dwio/common/FsCacheBufferedInput.cpp`
- Create: `velox/dwio/common/FsCacheInputStream.h`
- Create: `velox/dwio/common/FsCacheInputStream.cpp`
- Create: `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`
- Modify: `velox/dwio/common/CMakeLists.txt`
- Modify: `velox/dwio/common/tests/CMakeLists.txt`

- [ ] **Step 1: Write `FsCacheBufferedInputTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/dwio/common/FsCacheBufferedInput.h"

#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheBufferedInputTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::unique_ptr<FsCache> fsCache_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::string remoteContent_;

  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
    pool_ = memory::memoryManager()->addLeafPool("FsCacheBufferedInputTest");

    tempDir_ = TempDirectoryPath::create();
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    remoteContent_.resize(8UL * 1024 * 1024);
    for (size_t i = 0; i < remoteContent_.size(); ++i) {
      remoteContent_[i] = static_cast<char>(i % 251);
    }
    std::ofstream out{remotePath_, std::ios::binary};
    out.write(remoteContent_.data(), remoteContent_.size());

    FsCacheConfig cfg;
    cfg.cacheRoot = tempDir_->getPath() + "/cache";
    cfg.maxBytes = 64UL * 1024 * 1024;
    std::filesystem::create_directories(cfg.cacheRoot);
    fsCache_ = std::make_unique<FsCache>(cfg);
  }
};

TEST_F(FsCacheBufferedInputTest, enqueueAndLoadReadsExpectedBytes) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto stream = input.enqueue({100, 2048});
  input.load(LogType::FILE);

  std::string got(2048, '\0');
  const void* data;
  int32_t len;
  size_t copied = 0;
  while (copied < 2048 && stream->Next(&data, &len)) {
    const size_t toCopy = std::min<size_t>(len, 2048 - copied);
    std::memcpy(got.data() + copied, data, toCopy);
    copied += toCopy;
  }
  EXPECT_EQ(copied, 2048);
  EXPECT_EQ(got, remoteContent_.substr(100, 2048));
}

TEST_F(FsCacheBufferedInputTest, multipleRegionsServed) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto s1 = input.enqueue({0, 1024});
  auto s2 = input.enqueue({4096, 1024});
  input.load(LogType::FILE);

  auto readAll = [](auto& s, size_t size) {
    std::string buf(size, '\0');
    size_t copied = 0;
    const void* data;
    int32_t len;
    while (copied < size && s->Next(&data, &len)) {
      const size_t toCopy = std::min<size_t>(len, size - copied);
      std::memcpy(buf.data() + copied, data, toCopy);
      copied += toCopy;
    }
    return buf;
  };

  EXPECT_EQ(readAll(s1, 1024), remoteContent_.substr(0, 1024));
  EXPECT_EQ(readAll(s2, 1024), remoteContent_.substr(4096, 1024));
}

TEST_F(FsCacheBufferedInputTest, hasCacheReturnsTrue) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  EXPECT_TRUE(input.hasCache());
}

} // namespace facebook::velox::dwio::common::test
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make debug 2>&1 | tail -20`
Expected: `FsCacheBufferedInput.h` not found.

- [ ] **Step 3: Write `FsCacheInputStream.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::dwio::common {

/// Reads bytes from a sequence of FileSegments (already DOWNLOADED) covering
/// a contiguous range. ZeroCopyInputStream semantics with a per-segment
/// buffer; BackUp/Skip operate within the current segment.
class FsCacheInputStream final : public SeekableInputStream {
 public:
  FsCacheInputStream(
      std::vector<cache::fs::FileSegmentPtr> segments,
      uint64_t regionOffset,
      uint64_t regionLength,
      std::string cacheRoot);

  bool Next(const void** data, int32_t* size) override;
  void BackUp(int32_t count) override;
  bool SkipInt64(int64_t count) override;
  int64_t ByteCount() const override;
  void seekToPosition(PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() const override;

 private:
  // Loads the buffer for segments_[index_] into buffer_.
  void loadCurrentSegmentBuffer();

  const std::vector<cache::fs::FileSegmentPtr> segments_;
  const uint64_t regionOffset_;
  const uint64_t regionLength_;
  const std::string cacheRoot_;

  size_t index_{0};
  uint64_t byteCount_{0};
  std::string buffer_;
  // Offset within buffer_ of the next byte to deliver.
  size_t cursor_{0};
};

} // namespace facebook::velox::dwio::common
```

- [ ] **Step 4: Write `FsCacheInputStream.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/dwio/common/FsCacheInputStream.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

FsCacheInputStream::FsCacheInputStream(
    std::vector<cache::fs::FileSegmentPtr> segments,
    uint64_t regionOffset,
    uint64_t regionLength,
    std::string cacheRoot)
    : segments_{std::move(segments)},
      regionOffset_{regionOffset},
      regionLength_{regionLength},
      cacheRoot_{std::move(cacheRoot)} {
  VELOX_CHECK(!segments_.empty(), "FsCacheInputStream needs >=1 segment");
  loadCurrentSegmentBuffer();
}

void FsCacheInputStream::loadCurrentSegmentBuffer() {
  const auto& seg = segments_[index_];
  const uint64_t segStart = seg->key().offset;
  const uint64_t segSize = seg->key().size;
  // Intersection of [regionOffset_, regionOffset_+regionLength_) with segment.
  const uint64_t rangeStart = std::max(regionOffset_, segStart);
  const uint64_t rangeEnd =
      std::min(regionOffset_ + regionLength_, segStart + segSize);
  VELOX_CHECK_LT(rangeStart, rangeEnd);
  const uint64_t length = rangeEnd - rangeStart;
  buffer_.assign(length, '\0');
  seg->read(rangeStart - segStart, length, buffer_.data(), cacheRoot_);
  cursor_ = 0;
}

bool FsCacheInputStream::Next(const void** data, int32_t* size) {
  if (cursor_ >= buffer_.size()) {
    if (index_ + 1 >= segments_.size()) {
      return false;
    }
    ++index_;
    loadCurrentSegmentBuffer();
  }
  *data = buffer_.data() + cursor_;
  *size = static_cast<int32_t>(buffer_.size() - cursor_);
  byteCount_ += *size;
  cursor_ = buffer_.size();
  return true;
}

void FsCacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  VELOX_CHECK_LE(static_cast<size_t>(count), cursor_);
  cursor_ -= count;
  byteCount_ -= count;
}

bool FsCacheInputStream::SkipInt64(int64_t count) {
  VELOX_CHECK_GE(count, 0);
  while (count > 0) {
    const int64_t available =
        static_cast<int64_t>(buffer_.size()) - static_cast<int64_t>(cursor_);
    if (count <= available) {
      cursor_ += count;
      byteCount_ += count;
      return true;
    }
    cursor_ = buffer_.size();
    byteCount_ += available;
    count -= available;
    if (index_ + 1 >= segments_.size()) {
      return false;
    }
    ++index_;
    loadCurrentSegmentBuffer();
  }
  return true;
}

int64_t FsCacheInputStream::ByteCount() const {
  return static_cast<int64_t>(byteCount_);
}

void FsCacheInputStream::seekToPosition(PositionProvider& position) {
  const uint64_t target = position.next();
  // Reset to beginning, then skip. Acceptable for phase 1; phase 2 can index
  // segments by cumulative offset for O(log N) seek.
  index_ = 0;
  byteCount_ = 0;
  loadCurrentSegmentBuffer();
  SkipInt64(static_cast<int64_t>(target));
}

std::string FsCacheInputStream::getName() const {
  return "FsCacheInputStream";
}

size_t FsCacheInputStream::positionSize() const {
  return 1;
}

} // namespace facebook::velox::dwio::common
```

- [ ] **Step 5: Write `FsCacheBufferedInput.h`**

```cpp
/*
 * (license header)
 */

#pragma once

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/dwio/common/BufferedInput.h"

namespace facebook::velox::dwio::common {

/// BufferedInput subclass backed by FsCache. Bypasses AsyncDataCache /
/// SsdCache entirely.
///
/// Phase 1 implements the read path only (`enqueue` + `load` + clone).
/// `cacheRegion()` / `findCachedRegion()` inherit the base
/// `VELOX_UNSUPPORTED` behaviour: third-party write-through into the cache
/// is out of scope for phase 1 (no caller in Velox invokes these on the
/// hot read path). They are added in a later phase if pre-fetching code
/// outside the BufferedInput needs to populate FsCache.
class FsCacheBufferedInput final : public BufferedInput {
 public:
  FsCacheBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      cache::fs::FsCache* fsCache);

  std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region,
      const StreamIdentifier* sid = nullptr) override;

  void load(const LogType) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  std::unique_ptr<BufferedInput> clone() const override;

  bool hasCache() const override {
    return true;
  }

 private:
  struct EnqueuedRegion {
    velox::common::Region region;
    // Set during load().
    std::vector<cache::fs::FileSegmentPtr> segments;
  };

  cache::fs::FsCache* const fsCache_;
  std::vector<EnqueuedRegion> regions_;
};

} // namespace facebook::velox::dwio::common
```

- [ ] **Step 6: Write `FsCacheBufferedInput.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/dwio/common/FsCacheBufferedInput.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/FsCacheInputStream.h"

namespace facebook::velox::dwio::common {

FsCacheBufferedInput::FsCacheBufferedInput(
    std::shared_ptr<ReadFile> readFile,
    memory::MemoryPool& pool,
    cache::fs::FsCache* fsCache)
    : BufferedInput(std::move(readFile), pool),
      fsCache_{fsCache} {
  VELOX_CHECK_NOT_NULL(fsCache_, "FsCacheBufferedInput requires an FsCache");
}

std::unique_ptr<SeekableInputStream> FsCacheBufferedInput::enqueue(
    velox::common::Region region,
    const StreamIdentifier* /*sid*/) {
  regions_.push_back(EnqueuedRegion{region, {}});
  EnqueuedRegion* slot = &regions_.back();
  // The stream is constructed at load() time once segments are materialized.
  // To return a SeekableInputStream now, wrap the lookup in a small lambda
  // captured by a custom stream type. Simplest: defer construction by
  // returning a wrapper stream that lazily reads via slot->segments after
  // load() populates them.
  class DeferredStream final : public SeekableInputStream {
   public:
    DeferredStream(EnqueuedRegion* slot, cache::fs::FsCache* cache)
        : slot_{slot}, cache_{cache} {}
    bool Next(const void** data, int32_t* size) override {
      ensure();
      return inner_->Next(data, size);
    }
    void BackUp(int32_t count) override {
      ensure();
      inner_->BackUp(count);
    }
    bool SkipInt64(int64_t count) override {
      ensure();
      return inner_->SkipInt64(count);
    }
    int64_t ByteCount() const override {
      return inner_ ? inner_->ByteCount() : 0;
    }
    void seekToPosition(PositionProvider& p) override {
      ensure();
      inner_->seekToPosition(p);
    }
    std::string getName() const override {
      return "FsCacheBufferedInput::DeferredStream";
    }
    size_t positionSize() const override {
      return 1;
    }

   private:
    void ensure() {
      if (inner_) {
        return;
      }
      VELOX_CHECK(
          !slot_->segments.empty(),
          "Stream used before BufferedInput::load()");
      inner_ = std::make_unique<FsCacheInputStream>(
          slot_->segments,
          slot_->region.offset,
          slot_->region.length,
          cache_->config().cacheRoot);
    }
    EnqueuedRegion* slot_;
    cache::fs::FsCache* cache_;
    std::unique_ptr<FsCacheInputStream> inner_;
  };
  return std::make_unique<DeferredStream>(slot, fsCache_);
}

void FsCacheBufferedInput::load(const LogType /*unused*/) {
  for (auto& r : regions_) {
    if (!r.segments.empty()) {
      continue;
    }
    r.segments = fsCache_->getOrSet(
        input_->getName(),
        r.region.offset,
        r.region.length,
        *input_->getReadFile());
  }
}

bool FsCacheBufferedInput::isBuffered(
    uint64_t /*offset*/,
    uint64_t /*length*/) const {
  // Phase 1: trust enqueue + load. Phase 2 could probe metadata.
  return true;
}

std::unique_ptr<BufferedInput> FsCacheBufferedInput::clone() const {
  return std::make_unique<FsCacheBufferedInput>(
      input_->getReadFile(), *pool_, fsCache_);
}

} // namespace facebook::velox::dwio::common
```

**Note on `config()` accessor:** the above uses `cache_->config().cacheRoot`.
This requires adding a public accessor to `FsCache`. Edit `FsCache.h` and add:

```cpp
 public:
  const FsCacheConfig& config() const {
    return config_;
  }
```

- [ ] **Step 7: Wire CMake**

Edit `velox/dwio/common/CMakeLists.txt`:
- Add `FsCacheBufferedInput.cpp` and `FsCacheInputStream.cpp` to the SOURCES list (alongside `CachedBufferedInput.cpp` and `DirectBufferedInput.cpp`).
- Add `FsCacheBufferedInput.h` and `FsCacheInputStream.h` to HEADERS.
- In the `velox_link_libraries` call for the library that owns these `.cpp`s, add `velox_fscache` to PUBLIC.

Edit `velox/dwio/common/tests/CMakeLists.txt`:
- Add `FsCacheBufferedInputTest.cpp` to the grouped-tests SOURCES list.
- Add `velox_fscache` to deps if not already pulled in transitively.

- [ ] **Step 8: Build and run**

Run: `make debug && cd _build/debug && ctest -R FsCacheBufferedInput -V`
Expected: 3 cases PASS. Also run `ctest -R velox_fscache_test` to confirm no regressions.

- [ ] **Step 9: Commit**

```bash
git add velox/dwio/common/FsCacheBufferedInput.{h,cpp} \
        velox/dwio/common/FsCacheInputStream.{h,cpp} \
        velox/dwio/common/CMakeLists.txt \
        velox/dwio/common/tests/CMakeLists.txt \
        velox/dwio/common/tests/FsCacheBufferedInputTest.cpp \
        velox/common/caching/fscache/FsCache.h
git commit -m "feat(dwio): FsCacheBufferedInput

BufferedInput subclass that routes enqueue/load through FsCache. A
DeferredStream wrapper lets callers obtain a SeekableInputStream at
enqueue time while the actual FileSegment download happens during
load(). Bypasses AsyncDataCache / SsdCache entirely.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 8: `feat(fscache): crash recovery via directory scan`

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.h` (add `loadFromDisk`)
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Create: `velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp`
- Create: `velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

- [ ] **Step 1: Write `FsCacheRecoveryTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheRecoveryTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;
  std::string remotePath_;
  FsCacheConfig config_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::filesystem::create_directories(cacheRoot_);

    std::ofstream out{remotePath_, std::ios::binary};
    std::string blob(8UL * 1024 * 1024, 'x');
    out.write(blob.data(), blob.size());

    config_.cacheRoot = cacheRoot_;
    config_.maxBytes = 64UL * 1024 * 1024;
  }

  void warmCache(uint64_t offset, uint64_t size) {
    FsCache cache{config_};
    LocalReadFile remote{remotePath_};
    cache.getOrSet(remotePath_, offset, size, remote);
  }
};

TEST_F(FsCacheRecoveryTest, tmpFilesRemovedOnLoad) {
  warmCache(0, 4096);
  // Drop a stray .tmp file.
  const std::string stray = cacheRoot_ + "/aa/bb/deadbeef00000000.0.1024.tmp";
  std::filesystem::create_directories(
      std::filesystem::path{stray}.parent_path());
  std::ofstream{stray} << "junk";

  FsCache cache{config_};
  cache.loadFromDisk();
  EXPECT_FALSE(std::filesystem::exists(stray));
}

TEST_F(FsCacheRecoveryTest, sizeMismatchFilesRemoved) {
  // Write a synthetic file with declared size 1024 but actual size 500.
  const std::string fake = cacheRoot_ + "/aa/bb/abcdef0123456789_0_1024";
  std::filesystem::create_directories(
      std::filesystem::path{fake}.parent_path());
  std::ofstream out{fake, std::ios::binary};
  out << std::string(500, 'y');
  out.close();

  FsCache cache{config_};
  cache.loadFromDisk();
  EXPECT_FALSE(std::filesystem::exists(fake));
}

TEST_F(FsCacheRecoveryTest, unparsableNamesIgnored) {
  const std::string bad = cacheRoot_ + "/aa/bb/not_a_valid_name";
  std::filesystem::create_directories(
      std::filesystem::path{bad}.parent_path());
  std::ofstream{bad} << "x";

  FsCache cache{config_};
  cache.loadFromDisk();
  // File is left alone (recovery does not delete unrecognized files; admin
  // tooling responsibility). Cache is empty.
  EXPECT_EQ(cache.stats().bytesOnDisk, 0);
}

TEST_F(FsCacheRecoveryTest, validFilesRecovered) {
  warmCache(0, 4UL * 1024 * 1024);
  // Inspect: file under cacheRoot_ with size 4 MiB exists.
  uint64_t bytesOnDiskBefore = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot_}) {
    if (p.is_regular_file()) {
      bytesOnDiskBefore += p.file_size();
    }
  }
  EXPECT_GT(bytesOnDiskBefore, 0);

  FsCache cache{config_};
  cache.loadFromDisk();
  EXPECT_EQ(cache.stats().bytesOnDisk, bytesOnDiskBefore);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Write `FsCachePersistenceTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

TEST(FsCachePersistenceTest, dataSurvivesRestart) {
  auto tempDir = TempDirectoryPath::create();
  const std::string remotePath = tempDir->getPath() + "/remote.bin";
  const std::string cacheRoot = tempDir->getPath() + "/cache";
  std::filesystem::create_directories(cacheRoot);
  std::ofstream out{remotePath, std::ios::binary};
  std::string blob(4UL * 1024 * 1024, 'z');
  out.write(blob.data(), blob.size());
  out.close();

  FsCacheConfig cfg;
  cfg.cacheRoot = cacheRoot;
  cfg.maxBytes = 16UL * 1024 * 1024;

  {
    FsCache cache{cfg};
    LocalReadFile remote{remotePath};
    cache.getOrSet(remotePath, 0, 4UL * 1024 * 1024, remote);
    EXPECT_GT(cache.stats().bytesOnDisk, 0);
  }

  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    LocalReadFile remote{remotePath};
    auto before = cache.stats();
    cache.getOrSet(remotePath, 0, 4UL * 1024 * 1024, remote);
    auto after = cache.stats();
    EXPECT_EQ(after.misses, before.misses); // No new download.
    EXPECT_GT(after.hits, before.hits);
  }
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `make debug 2>&1 | tail -20`
Expected: `FsCache::loadFromDisk` undefined.

- [ ] **Step 4: Add `loadFromDisk` declaration to `FsCache.h`**

```cpp
 public:
  /// Scans cacheRoot for cache files left over from a previous run. Removes
  /// .tmp files and size-mismatched files. Does NOT repopulate metadata
  /// (path information is lost on disk by design) and does NOT credit any
  /// bytes to stats_.bytesOnDisk — orphan files that are never re-requested
  /// would be untracked by LruPolicy and could not be evicted, eventually
  /// filling the disk. Instead, the next download() of a matching key
  /// short-circuits when the expected file already exists; that path goes
  /// through lookupOrCreate which performs the normal onInsert + bytesOnDisk
  /// accounting. Idempotent.
  ///
  /// NOT called from the FsCache constructor; the caller (typically the
  /// Velox process startup hook that constructs the singleton FsCache) must
  /// invoke it explicitly before serving traffic if persistence across
  /// restarts is desired. This keeps construction side-effect-free and
  /// keeps unit tests from paying directory-scan cost.
  void loadFromDisk();
```

- [ ] **Step 5: Implement `loadFromDisk` in `FsCache.cpp`**

```cpp
namespace {
// Parses "<hexHash>.<offset>.<size>" from a file name. Returns nullopt if the
// name does not fit the expected three-part form with numeric offset/size.
struct ParsedName {
  uint64_t offset;
  uint64_t size;
};
std::optional<ParsedName> parseFileName(const std::string& name) {
  const auto firstDot = name.find('.');
  if (firstDot == std::string::npos) {
    return std::nullopt;
  }
  const auto secondDot = name.find('.', firstDot + 1);
  if (secondDot == std::string::npos) {
    return std::nullopt;
  }
  try {
    const uint64_t offset =
        std::stoull(name.substr(firstDot + 1, secondDot - firstDot - 1));
    const uint64_t size = std::stoull(name.substr(secondDot + 1));
    return ParsedName{offset, size};
  } catch (const std::exception&) {
    return std::nullopt;
  }
}
} // namespace

void FsCache::loadFromDisk() {
  if (!std::filesystem::exists(config_.cacheRoot)) {
    return;
  }
  std::error_code ignore;
  // Two passes simplify empty-directory cleanup (issue #7 in review): first
  // pass removes .tmp / size-mismatched / unparseable files; second pass
  // rmdirs any subdirectory left empty afterwards.
  for (auto& entry :
       std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const std::string filename = entry.path().filename().string();
    if (filename.size() > 4 &&
        filename.substr(filename.size() - 4) == ".tmp") {
      std::filesystem::remove(entry.path(), ignore);
      continue;
    }
    auto parsed = parseFileName(filename);
    if (!parsed.has_value()) {
      // Unknown file name. Leave it untouched; admin tooling owns cleanup.
      continue;
    }
    if (entry.file_size() != parsed->size) {
      std::filesystem::remove(entry.path(), ignore);
      continue;
    }
    // Survivor: download() will short-circuit when this file is next
    // requested, and lookupOrCreate will then call onInsert/bytesOnDisk.
  }
  // Cleanup empty directories left by removed files.
  for (auto& entry :
       std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
    if (entry.is_directory() && std::filesystem::is_empty(entry.path())) {
      std::filesystem::remove(entry.path(), ignore);
    }
  }
}
```

**Why no metadata re-registration or byte accounting:** the on-disk file
name encodes only the hash, not the original remote path (design Q10
explicitly accepts this trade-off — see
`velox/docs/designs/fscache-clickhouse-style.md` §持久化与暖启 / path 反查).
Reconstructing a `FsCacheKey{path, offset, size}` from the on-disk name is
therefore impossible. Crediting orphan bytes to `bytesOnDisk` without a
matching LruPolicy entry would also create un-evictable disk usage. Instead,
the next call to `getOrSet` re-hashes the same `{path, offset, size}` to
the same on-disk filename, `FileSegment::download` short-circuits when it
finds the file already present with the right size, and `lookupOrCreate`'s
writer path then performs the normal `onInsert` + `bytesOnDisk += size`
accounting so the segment participates in eviction (regression #12).

- [ ] **Step 5a: Add download() short-circuit in `FileSegment.cpp`**

Edit the existing `FileSegment::download` body added in commit 5. Insert at
the top, immediately after computing `finalPath`:

```cpp
void FileSegment::download(
    ::facebook::velox::ReadFile& remote,
    const std::string& cacheRoot) {
  const std::string finalPath = localPath(cacheRoot);
  // Warm-restart short-circuit: a previous run already produced this file.
  if (std::filesystem::exists(finalPath) &&
      std::filesystem::file_size(finalPath) == key_.size) {
    downloadedSize_.store(key_.size, std::memory_order_release);
    state_.store(State::kDownloaded, std::memory_order_release);
    return;
  }
  const std::string tmpPath = finalPath + ".tmp";
  // ... rest of commit-5 implementation unchanged
```

- [ ] **Step 5b: Adjust the persistence test expectations**

In `FsCachePersistenceTest.cpp` (written in Step 2), the second-block
assertions must match the actual semantics — the second `getOrSet` registers
as a miss because metadata is not pre-populated, but the download short-
circuits so no remote read happens and no new bytes hit disk. Replace the
second block assertions with:

```cpp
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    LocalReadFile remote{remotePath};
    auto before = cache.stats();
    cache.getOrSet(remotePath, 0, 4UL * 1024 * 1024, remote);
    auto after = cache.stats();
    // Recovery does not pre-populate metadata, so this counts as a miss,
    // but the download short-circuits because the file already exists.
    EXPECT_EQ(after.misses, before.misses + 1);
    EXPECT_EQ(after.bytesOnDisk, before.bytesOnDisk);
  }
```

- [ ] **Step 6: Wire test CMake**

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FsCacheRecoveryTest.cpp` and `FsCachePersistenceTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.

- [ ] **Step 7: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: 4 FsCacheRecoveryTest cases + 1 FsCachePersistenceTest case PASS, plus all prior tests.

- [ ] **Step 8: Commit**

```bash
git add velox/common/caching/fscache/FsCache.{h,cpp} \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp \
        velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "feat(fscache): crash recovery via directory scan

loadFromDisk removes .tmp and unparseable / size-mismatched files
and rmdirs empty bucket dirs. It does NOT re-register surviving
files in metadata_ or credit them to stats_.bytesOnDisk; that
would create un-evictable disk usage. Instead, FileSegment::download
short-circuits when the final file already exists with the expected
size, and the lookupOrCreate writer path credits bytesOnDisk + calls
onInsert at first access — surviving files are picked up on demand.
This enables persistence-aware restarts without persisting path metadata.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 9: `test(fscache): concurrent stress test`

**Files:**
- Create: `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

UT-only commit. No production code changes.

- [ ] **Step 1: Write `FsCacheConcurrencyTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <atomic>
#include <fstream>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheConcurrencyTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  FsCacheConfig config_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::ofstream out{remotePath_, std::ios::binary};
    std::string blob(16UL * 1024 * 1024, 'q');
    out.write(blob.data(), blob.size());

    config_.cacheRoot = tempDir_->getPath() + "/cache";
    config_.maxBytes = 32UL * 1024 * 1024;
    std::filesystem::create_directories(config_.cacheRoot);
  }
};

TEST_F(FsCacheConcurrencyTest, sameSegmentMultipleReadersExactlyOneDownload) {
  FsCache cache{config_};
  constexpr int kThreads = 16;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      try {
        LocalReadFile remote{remotePath_};
        auto segs =
            cache.getOrSet(remotePath_, 0, 4UL * 1024 * 1024, remote);
        for (auto& s : segs) {
          if (s->state() != FileSegment::State::kDownloaded) {
            ++errors;
          }
        }
      } catch (const std::exception&) {
        ++errors;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(errors, 0);
  // Exactly one miss across all 16 readers; the rest are hits or wait-then-hit.
  EXPECT_EQ(cache.stats().misses, 1);
}

TEST_F(FsCacheConcurrencyTest, differentSegmentsParallelDownloads) {
  FsCache cache{config_};
  constexpr int kThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      try {
        LocalReadFile remote{remotePath_};
        // Each thread reads a disjoint 1 MiB region.
        cache.getOrSet(
            remotePath_, i * 1UL * 1024 * 1024, 1UL * 1024 * 1024, remote);
      } catch (const std::exception&) {
        ++errors;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(errors, 0);
  EXPECT_GT(cache.stats().misses, 0);
}

TEST_F(FsCacheConcurrencyTest, evictionUnderConcurrentLoadIsRaceFree) {
  FsCacheConfig tiny = config_;
  tiny.maxBytes = 8UL * 1024 * 1024;
  FsCache cache{tiny};
  constexpr int kThreads = 4;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      try {
        LocalReadFile remote{remotePath_};
        for (int round = 0; round < 4; ++round) {
          const uint64_t off =
              (t * 4 + round) * 1UL * 1024 * 1024 % (12UL * 1024 * 1024);
          cache.getOrSet(remotePath_, off, 1UL * 1024 * 1024, remote);
        }
      } catch (const std::exception&) {
        ++errors;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(errors, 0);
  EXPECT_LE(cache.stats().bytesOnDisk, tiny.maxBytes);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Wire CMake**

Edit `velox/common/caching/fscache/tests/CMakeLists.txt`:
- Add `FsCacheConcurrencyTest.cpp` to `VELOX_FSCACHE_TEST_SOURCES`.

- [ ] **Step 3: Build and run**

Run: `make debug && cd _build/debug && ctest -R velox_fscache_test -V`
Expected: 3 FsCacheConcurrencyTest cases PASS. **Also run under TSAN** if
available locally to confirm no data races:
`cmake -DVELOX_ENABLE_TSAN=ON ... && make debug && ctest -R FsCacheConcurrency -V`.

- [ ] **Step 4: Commit**

```bash
git add velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "test(fscache): concurrent stress test

Three multi-threaded scenarios: 16-reader race on the same segment
(exactly one miss expected), 8-reader parallel disjoint downloads,
and concurrent load+eviction under tight capacity. Validates the
5-lock hierarchy and FileSegment cv coordination.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Commit 10: `test(fscache): equivalence vs ground-truth bytes`

**Files:**
- Create: `velox/dwio/common/tests/FsCacheEquivalenceTest.cpp`
- Modify: `velox/dwio/common/tests/CMakeLists.txt`

UT-only commit. **Phase 1 终极正确性 gate**：FsCacheBufferedInput 的读路径与
源文件的 canonical bytes 完全一致。

**为什么不直接对比 `CachedBufferedInput`？** Velox 正确性不变式保证
`CachedBufferedInput` 返回的就是 canonical bytes（也就是这里的 `content_`），
所以拉 `AsyncDataCache + StringIdLease + ScanTracker` 起一份只是为了对同一段
ground truth — 对比下来必然一致，但额外的脚手架会让测试失败原因更难定位。
直接对 `content_` 比较是等价但更简洁的方案。当怀疑 `CachedBufferedInput`
本身有问题时再加这条对比。

- [ ] **Step 1: Write `FsCacheEquivalenceTest.cpp`**

```cpp
/*
 * (license header)
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/FsCacheBufferedInput.h"
#include "velox/dwio/common/Options.h"

#include <gtest/gtest.h>
#include <fstream>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;
using ::facebook::velox::dwio::common::FsCacheBufferedInput;

class FsCacheEquivalenceTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<cache::fs::FsCache> fsCache_;
  std::string content_;

  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
    pool_ = memory::memoryManager()->addLeafPool("FsCacheEquivalenceTest");

    tempDir_ = TempDirectoryPath::create();
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    content_.resize(16UL * 1024 * 1024);
    for (size_t i = 0; i < content_.size(); ++i) {
      content_[i] = static_cast<char>((i * 7 + 13) % 256);
    }
    std::ofstream out{remotePath_, std::ios::binary};
    out.write(content_.data(), content_.size());

    cache::fs::FsCacheConfig cfg;
    cfg.cacheRoot = tempDir_->getPath() + "/cache";
    cfg.maxBytes = 64UL * 1024 * 1024;
    std::filesystem::create_directories(cfg.cacheRoot);
    fsCache_ = std::make_unique<cache::fs::FsCache>(cfg);
  }

  static std::string drain(SeekableInputStream* stream, size_t size) {
    std::string out;
    out.reserve(size);
    const void* data;
    int32_t len;
    while (out.size() < size && stream->Next(&data, &len)) {
      const size_t toCopy = std::min<size_t>(len, size - out.size());
      out.append(static_cast<const char*>(data), toCopy);
    }
    return out;
  }
};

TEST_F(FsCacheEquivalenceTest, singleRegionByteForByte) {
  const uint64_t offset = 1234;
  const uint64_t length = 8UL * 1024 * 1024 - offset;

  // FsCache path.
  std::string fromFs;
  {
    auto readFile = std::make_shared<LocalReadFile>(remotePath_);
    FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
    auto stream = input.enqueue({offset, length});
    input.load(LogType::FILE);
    fromFs = drain(stream.get(), length);
  }

  // Direct local read (ground truth — equivalence vs canonical bytes).
  const std::string expected = content_.substr(offset, length);

  EXPECT_EQ(fromFs.size(), length);
  EXPECT_EQ(fromFs, expected);
}

TEST_F(FsCacheEquivalenceTest, manySmallRegionsByteForByte) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};

  struct Req {
    uint64_t offset;
    uint64_t length;
  };
  std::vector<Req> reqs{
      {0, 4096},
      {4096, 4096},
      {1UL * 1024 * 1024, 64 * 1024},
      {3UL * 1024 * 1024 + 17, 1024},
      {7UL * 1024 * 1024, 2UL * 1024 * 1024},
  };
  std::vector<std::unique_ptr<SeekableInputStream>> streams;
  for (auto& r : reqs) {
    streams.push_back(input.enqueue({r.offset, r.length}));
  }
  input.load(LogType::FILE);
  for (size_t i = 0; i < reqs.size(); ++i) {
    const std::string got = drain(streams[i].get(), reqs[i].length);
    const std::string expected = content_.substr(reqs[i].offset, reqs[i].length);
    ASSERT_EQ(got.size(), expected.size()) << "request " << i;
    EXPECT_EQ(got, expected) << "request " << i;
  }
}

TEST_F(FsCacheEquivalenceTest, repeatedReadConsistent) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  for (int round = 0; round < 3; ++round) {
    FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
    auto stream = input.enqueue({0, 4 * 1024 * 1024});
    input.load(LogType::FILE);
    EXPECT_EQ(
        drain(stream.get(), 4 * 1024 * 1024),
        content_.substr(0, 4 * 1024 * 1024));
  }
}

} // namespace facebook::velox::dwio::common::test
```

- [ ] **Step 2: Wire CMake**

Edit `velox/dwio/common/tests/CMakeLists.txt`:
- Add `FsCacheEquivalenceTest.cpp` to the grouped-tests SOURCES list.

- [ ] **Step 3: Build and run**

Run: `make debug && cd _build/debug && ctest -R FsCacheEquivalence -V`
Expected: 3 cases PASS.

- [ ] **Step 4: Final sanity check — entire fscache + dwio test suite**

```bash
cd _build/debug && ctest -R "velox_fscache_test|FsCache" -V
```

Expected: ALL pass. This is Phase 1's exit criterion.

- [ ] **Step 5: Commit**

```bash
git add velox/dwio/common/tests/FsCacheEquivalenceTest.cpp \
        velox/dwio/common/tests/CMakeLists.txt
git commit -m "test(fscache): equivalence vs ground-truth bytes

Three byte-for-byte tests prove FsCacheBufferedInput delivers
canonical content for single-region, many-small-region, and
repeated-read workloads. The reference is the source file's
canonical bytes (which CachedBufferedInput is also required to
serve, by Velox invariant). This is Phase 1's correctness gate
per the design doc, addressing the E2E verification gap from the
CacheLib PoC audit.
Design: velox/docs/designs/fscache-clickhouse-style.md."
```

---

## Done Criteria

Phase 1 is complete when:

1. All 10 commits land on the `fscache-clickhouse-style` branch.
2. `make debug && make unittest` is green at every commit.
3. `ctest -R "velox_fscache_test|FsCache"` lists ≥ 50 passing test cases:
   - FsCacheScaffoldTest: 1
   - FsCacheKeyTest: 5
   - FsCacheGuardsTest: 2 or 3 (depends on debug build for death test)
   - EvictionPolicyTest: 6
   - FileSegmentTest: 6
   - FsCacheSplitRangeTest: 7
   - FsCacheMetadataTest: 5
   - FsCacheTest: 5
   - FsCacheConcurrencyTest: 3
   - FsCacheRecoveryTest: 4
   - FsCachePersistenceTest: 1
   - FsCacheBufferedInputTest: 3
   - FsCacheEquivalenceTest: 3
4. `FsCacheConcurrencyTest` passes under TSAN
   (`cmake -DVELOX_ENABLE_TSAN=ON ... && ctest -R FsCacheConcurrency`). This
   is the explicit signal that the 5-lock hierarchy + FileSegment cv
   coordination is race-free; the docs-only LockOrderChecker is not enough
   on its own.
5. **No benchmark numbers** or performance claims appear in any commit
   message in this branch (per the iron rule from the design doc).
6. Branch is pushable but **not pushed automatically**; user decides when.

---
