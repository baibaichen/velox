# FsCache vs CBI TPC-DS A/B — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the 6 commits required by `docs/superpowers/specs/2026-05-23-fscache-vs-cbi-tpcds-design.md` §5.2 — enough plumbing to flip `TpcdsBenchmark` between CBI and FsCache via `--input_source`, plus the harness to run a 99-query × 3-round × 2-backend sweep and produce the merged Markdown report.

**Architecture:** Mirror the existing `AsyncDataCache` plumbing chain end-to-end for `FsCache`: process-global singleton → `QueryCtx::fsCache_` default-init → `ConnectorQueryCtx::fsCache_` (appended last) → `OperatorCtx::createConnectorQueryCtx` threads it through → `HiveConnectorUtil::createBufferedInput` picks `FsCacheBufferedInput` when set. The bench main installs exactly one of the two singletons (the other stays null), so `HiveConnectorUtil`'s dispatch is naturally exclusive. A custom round loop owns repetition and hoists plans out of the inner loop; the existing `folly::runBenchmarks()` path stays as the fallback when `--input_source` is absent.

**Tech Stack:** C++20, Velox (gtest+gmock for unit tests, gflags+folly::Init for bench main, std::chrono::steady_clock for timing, Python 3 stdlib for the merge script), GCC-13 RelWithDebInfo build in `cmake-build-relwithdebinfo-gcc13/`.

---

## Build & Test Commands

All build/test commands run against the existing build directory `cmake-build-relwithdebinfo-gcc13/` (GCC-13, RelWithDebInfo). Do NOT create a new build dir.

```bash
# Reconfigure once if needed (Phase-1 already did this for VELOX_ENABLE_BENCHMARKS_BASIC=ON):
cmake -S /home/chang/OpenSource/velox2 \
      -B /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      -DVELOX_ENABLE_BENCHMARKS_BASIC=ON

# Build the specific test/binary for each task:
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target <target> -j

# Run a test binary with a gtest filter:
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/<path-to-binary> \
      --gtest_filter='<Suite>.<Case>'
```

## Files at a Glance

The 6 commits touch the following files. Tests live next to source.

| Layer | Production | Test |
|---|---|---|
| FsCache singleton | `velox/common/caching/fscache/FsCache.{h,cpp}` | `velox/common/caching/fscache/tests/FsCacheTest.cpp` (new test cases) |
| QueryCtx slot | `velox/core/QueryCtx.{h,cpp}` | `velox/core/tests/QueryCtxTest.cpp` (new test case) |
| ConnectorQueryCtx slot | `velox/connectors/Connector.h` | `velox/connectors/tests/ConnectorTest.cpp` (new test case) |
| OperatorCtx threading | `velox/exec/Operator.cpp` | covered by ConnectorQueryCtx tests + smoke run |
| HiveConnectorUtil branch | `velox/connectors/hive/HiveConnectorUtil.cpp` | `velox/connectors/hive/tests/HiveConnectorUtilTest.cpp` (new test case) |
| FsCacheBufferedInput parquet smoke | (none — read-only on existing impl) | `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` (new test case) |
| Bench main | `velox/benchmarks/tpcds/TpcdsBenchmark.{h,cpp}`, `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp` | gate by hand (commit-4 acceptance gate; no unit test — driven by the smoke run) |
| Shell harness | `velox/benchmarks/tpcds/run_ab.sh`, `scripts/bench/merge_tpcds_ab.py` | gate by hand (commit-5 acceptance gate) |
| Result | `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds.md` + the two raw CSVs | gate by hand (commit-6 acceptance gate) |

The `FsCacheBufferedInputTest` file already exists (see Phase-1 work); commit 3 adds **one** new TEST_F that opens a real Parquet fixture (`sample.parquet` shipped via `ParquetTestBase::getExampleFilePath()`).

---

## Task 1: FsCache Singleton + QueryCtx::fsCache_ + ConnectorQueryCtx::fsCache_ + OperatorCtx Threading

Matches spec §2.1 + §2.2 + §2.3 + §2.4 and §5.2 commit-1. All four edits land in one commit because each is one-line / one-arg and they only compile together — splitting yields broken intermediate commits.

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.h:51-118` (add `setInstance` / `getInstance` decls + private static slot)
- Modify: `velox/common/caching/fscache/FsCache.cpp:32-50` (add singleton bodies mirroring `AsyncDataCache.cpp:835-852`)
- Modify: `velox/core/QueryCtx.h:118` (`create` signature), `:205` (Builder default), `:402` (ctor signature), `:469` (member)
- Modify: `velox/core/QueryCtx.h` Builder block at ~`:157` (add `fsCache()` setter parallel to `asyncDataCache()`), public getter near `:230`
- Modify: `velox/core/QueryCtx.cpp:25-45` (`create`), `:47-63` (`Builder::build`), `:65-86` (`QueryCtx` ctor)
- Modify: `velox/connectors/Connector.h:440-473` (`ConnectorQueryCtx` ctor — append `fsCache` last with `nullptr` default, init `fsCache_` member, add `fsCache()` getter, add `fsCache_` member)
- Modify: `velox/exec/Operator.cpp:51-79` (append `task->queryCtx()->fsCache()` as the last ctor arg)
- Modify: `velox/common/caching/fscache/tests/FsCacheTest.cpp` — add TEST(s) for singleton (Test file already exists; add cases at end)
- Modify: `velox/core/tests/QueryCtxTest.cpp:23-` — add TEST_F for `fsCache()` default-init / override
- Modify: `velox/connectors/tests/ConnectorTest.cpp` — add TEST for `ConnectorQueryCtx::fsCache()` round-trip

### Step 1.1: Write failing test — FsCache singleton default null + setInstance round-trip

- [ ] Open `velox/common/caching/fscache/tests/FsCacheTest.cpp` and append at the end of the file (before the closing namespace):

```cpp
TEST(FsCacheSingletonTest, defaultsToNullptr) {
  EXPECT_EQ(FsCache::getInstance(), nullptr);
}

TEST(FsCacheSingletonTest, setInstanceRoundTrip) {
  FsCacheConfig config;
  auto tempDir = velox::exec::test::TempDirectoryPath::create();
  config.cacheRoot = tempDir->getPath();
  config.maxBytes = 16ULL << 20;
  auto cache = std::make_unique<FsCache>(config);

  FsCache::setInstance(cache.get());
  EXPECT_EQ(FsCache::getInstance(), cache.get());

  FsCache::setInstance(nullptr);
  EXPECT_EQ(FsCache::getInstance(), nullptr);
}
```

Use whatever existing include style + namespace the existing tests use; if `TempDirectoryPath` is not already included, mirror the include from the existing setup in this file.

### Step 1.2: Run the test — expect compile failure

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_fscache_test -j
```

Expected: build fails with `'setInstance' is not a member of 'facebook::velox::cache::fs::FsCache'` (or equivalent for `getInstance`).

### Step 1.3: Add singleton to FsCache.h

In `velox/common/caching/fscache/FsCache.h` inside `class FsCache` (after the `config()` getter at line 72, before `loadFromDisk()` at line 117), add:

```cpp
  /// Process-global singleton. Mirrors AsyncDataCache::getInstance(). The
  /// caller owns the FsCache lifetime; this only stores a raw pointer.
  /// Used by the default-init in QueryCtx so bench / test setups that want
  /// FsCache plumbing can install one without touching every QueryCtx
  /// construction site.
  static FsCache* getInstance();

  /// Installs the singleton. Pass nullptr on teardown to reset.
  static void setInstance(FsCache* instance);
```

(No new member field in the header — keep the storage in the .cpp using the local-static-pointer idiom from `AsyncDataCache`, which avoids needing a definition for a class-scope static.)

### Step 1.4: Add singleton bodies to FsCache.cpp

In `velox/common/caching/fscache/FsCache.cpp` just below `FsCache::~FsCache() = default;` (line 50), add:

```cpp
namespace {
FsCache** instancePtr() {
  static FsCache* instance{nullptr};
  return &instance;
}
} // namespace

// static
FsCache* FsCache::getInstance() {
  return *instancePtr();
}

// static
void FsCache::setInstance(FsCache* instance) {
  *instancePtr() = instance;
}
```

### Step 1.5: Build + run the singleton test — expect PASS

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_fscache_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
      --gtest_filter='FsCacheSingletonTest.*'
```

Expected: 2/2 PASS.

### Step 1.6: Write failing test — QueryCtx::fsCache() default-init from singleton

In `velox/core/tests/QueryCtxTest.cpp`, before the closing namespace add:

```cpp
TEST_F(QueryCtxTest, fsCacheDefaultsToSingleton) {
  ASSERT_EQ(cache::fs::FsCache::getInstance(), nullptr);
  auto ctx = QueryCtx::create();
  EXPECT_EQ(ctx->fsCache(), nullptr);

  cache::fs::FsCacheConfig config;
  auto tempDir = velox::exec::test::TempDirectoryPath::create();
  config.cacheRoot = tempDir->getPath();
  config.maxBytes = 16ULL << 20;
  auto installed = std::make_unique<cache::fs::FsCache>(config);
  cache::fs::FsCache::setInstance(installed.get());

  auto ctxWithSingleton = QueryCtx::create();
  EXPECT_EQ(ctxWithSingleton->fsCache(), installed.get());

  cache::fs::FsCache::setInstance(nullptr);
}
```

Add the necessary includes to the top of the file:

```cpp
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
```

### Step 1.7: Run the test — expect compile failure

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_core_test -j
```

Expected: `'fsCache' is not a member of 'facebook::velox::core::QueryCtx'`.

### Step 1.8: Add fsCache_ slot to QueryCtx

In `velox/core/QueryCtx.h` make these edits in order:

(a) Add include near the top alongside the existing caching include:

```cpp
#include "velox/common/caching/fscache/FsCache.h"
```

(b) In `class Builder` (h:138-200), parallel to the existing `asyncDataCache()` setter at h:157, add after it:

```cpp
    Builder& fsCache(cache::fs::FsCache* fsCache) {
      fsCache_ = fsCache;
      return *this;
    }
```

(c) In `class Builder`'s private member block (h:200-208), parallel to the `cache_` default at h:205, add after it:

```cpp
    cache::fs::FsCache* fsCache_{cache::fs::FsCache::getInstance()};
```

(d) In `QueryCtx::create` decl (h:113-122), append a parameter after `tokenProvider`:

```cpp
      std::shared_ptr<filesystems::TokenProvider> tokenProvider = {},
      cache::fs::FsCache* fsCache = cache::fs::FsCache::getInstance());
```

(e) In `QueryCtx` ctor decl (h:397-407), append a parameter after `tokenProvider` (before `traceCtxProvider`):

```cpp
      std::shared_ptr<filesystems::TokenProvider> tokenProvider = {},
      cache::fs::FsCache* fsCache = cache::fs::FsCache::getInstance(),
      TraceCtxProvider traceCtxProvider = nullptr);
```

(f) Public getter (parallel to existing `cache()` at h:230):

```cpp
  cache::fs::FsCache* fsCache() const {
    return fsCache_;
  }
```

(g) Member at h:469 (parallel to `cache_`):

```cpp
  cache::fs::FsCache* const fsCache_;
```

### Step 1.9: Update QueryCtx.cpp ctor + create + Builder::build

In `velox/core/QueryCtx.cpp`:

(a) `QueryCtx::create` (cpp:25-45) — add parameter and pass to Builder:

```cpp
std::shared_ptr<QueryCtx> QueryCtx::create(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorConfigs,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    std::string queryId,
    std::shared_ptr<filesystems::TokenProvider> tokenProvider,
    cache::fs::FsCache* fsCache) {
  return QueryCtx::Builder()
      .executor(executor)
      .queryConfig(std::move(queryConfig))
      .connectorConfigs(std::move(connectorConfigs))
      .asyncDataCache(cache)
      .pool(std::move(pool))
      .spillExecutor(spillExecutor)
      .queryId(std::move(queryId))
      .tokenProvider(std::move(tokenProvider))
      .fsCache(fsCache)
      .build();
}
```

(b) `Builder::build()` (cpp:47-63) — pass `fsCache_` to the ctor:

```cpp
std::shared_ptr<QueryCtx> QueryCtx::Builder::build() {
  std::shared_ptr<QueryCtx> queryCtx(new QueryCtx(
      executor_,
      std::move(queryConfig_),
      std::move(connectorConfigs_),
      cache_,
      std::move(pool_),
      spillExecutor_,
      std::move(queryId_),
      std::move(tokenProvider_),
      fsCache_,
      std::move(traceCtxProvider_)));
  queryCtx->maybeSetReclaimer();
  for (auto& cb : releaseCallbacks_) {
    queryCtx->addReleaseCallback(std::move(cb));
  }
  return queryCtx;
}
```

(c) `QueryCtx` ctor (cpp:65-86) — add the parameter (after `tokenProvider`, before `traceCtxProvider`) and initialize `fsCache_`:

```cpp
QueryCtx::QueryCtx(
    folly::Executor* executor,
    QueryConfig&& queryConfig,
    std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
        connectorSessionProperties,
    cache::AsyncDataCache* cache,
    std::shared_ptr<memory::MemoryPool> pool,
    folly::Executor* spillExecutor,
    const std::string& queryId,
    std::shared_ptr<filesystems::TokenProvider> tokenProvider,
    cache::fs::FsCache* fsCache,
    TraceCtxProvider traceCtxProvider)
    : queryId_(queryId),
      executor_(executor),
      spillExecutor_(spillExecutor),
      cache_(cache),
      fsCache_(fsCache),
      connectorSessionProperties_(connectorSessionProperties),
      pool_(std::move(pool)),
      queryConfig_{std::move(queryConfig)},
      fsTokenProvider_(std::move(tokenProvider)),
      traceCtxProvider_(std::move(traceCtxProvider)) {
  initPool(queryId);
}
```

### Step 1.10: Build + run the QueryCtx test — expect PASS

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_core_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/core/tests/velox_core_test \
      --gtest_filter='QueryCtxTest.fsCacheDefaultsToSingleton'
```

Expected: 1/1 PASS.

### Step 1.11: Write failing test — ConnectorQueryCtx::fsCache() round-trip

In `velox/connectors/tests/ConnectorTest.cpp`, find the existing `TEST(ConnectorTest, ...)` for `connectorSplit` (line 181 area). Append, before the closing namespace:

```cpp
TEST(ConnectorQueryCtxTest, fsCacheGetterRoundTrip) {
  auto pool = memory::memoryManager()->addLeafPool("ConnectorQueryCtxTest");
  config::ConfigBase sessionProperties{{}};

  // Default param → nullptr.
  ConnectorQueryCtx ctxDefault{
      pool.get(),
      pool.get(),
      &sessionProperties,
      /*spillConfig=*/nullptr,
      common::PrefixSortConfig{},
      /*expressionEvaluator=*/nullptr,
      /*cache=*/nullptr,
      "q",
      "t",
      "p",
      /*driverId=*/0,
      "UTC"};
  EXPECT_EQ(ctxDefault.fsCache(), nullptr);

  // Passing a non-null sentinel → getter returns it.
  cache::fs::FsCacheConfig fsCfg;
  auto tempDir = velox::exec::test::TempDirectoryPath::create();
  fsCfg.cacheRoot = tempDir->getPath();
  fsCfg.maxBytes = 16ULL << 20;
  auto fsCache = std::make_unique<cache::fs::FsCache>(fsCfg);

  ConnectorQueryCtx ctxWith{
      pool.get(),
      pool.get(),
      &sessionProperties,
      /*spillConfig=*/nullptr,
      common::PrefixSortConfig{},
      /*expressionEvaluator=*/nullptr,
      /*cache=*/nullptr,
      "q",
      "t",
      "p",
      /*driverId=*/0,
      "UTC",
      /*adjustTimestampToTimezone=*/false,
      /*cancellationToken=*/{},
      /*tokenProvider=*/{},
      fsCache.get()};
  EXPECT_EQ(ctxWith.fsCache(), fsCache.get());
}
```

Add the necessary includes at the top of the file:

```cpp
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
```

### Step 1.12: Run — expect compile failure on `fsCache()` / `fsCache.get()` argument

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_connector_test -j
```

Expected: `'fsCache' is not a member of ConnectorQueryCtx` OR `no matching constructor`.

### Step 1.13: Append fsCache_ to ConnectorQueryCtx

In `velox/connectors/Connector.h` (lines 437-490 area):

(a) Add `#include "velox/common/caching/fscache/FsCache.h"` near the top with the other caching includes.

(b) Extend the ctor decl — append `fsCache` param last with `nullptr` default:

```cpp
  ConnectorQueryCtx(
      memory::MemoryPool* operatorPool,
      memory::MemoryPool* connectorPool,
      const config::ConfigBase* sessionProperties,
      const common::SpillConfig* spillConfig,
      common::PrefixSortConfig prefixSortConfig,
      std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator,
      cache::AsyncDataCache* cache,
      const std::string& queryId,
      const std::string& taskId,
      const std::string& planNodeId,
      int driverId,
      const std::string& sessionTimezone,
      bool adjustTimestampToTimezone = false,
      folly::CancellationToken cancellationToken = {},
      std::shared_ptr<filesystems::TokenProvider> tokenProvider = {},
      cache::fs::FsCache* fsCache = nullptr)
      : operatorPool_(operatorPool),
        connectorPool_(connectorPool),
        sessionProperties_(sessionProperties),
        spillConfig_(spillConfig),
        prefixSortConfig_(prefixSortConfig),
        expressionEvaluator_(std::move(expressionEvaluator)),
        cache_(cache),
        scanId_(fmt::format("{}.{}", taskId, planNodeId)),
        queryId_(queryId),
        taskId_(taskId),
        driverId_(driverId),
        planNodeId_(planNodeId),
        sessionTimezone_(sessionTimezone),
        adjustTimestampToTimezone_(adjustTimestampToTimezone),
        cancellationToken_(std::move(cancellationToken)),
        fsTokenProvider_(std::move(tokenProvider)),
        fsCache_(fsCache) {
    VELOX_CHECK_NOT_NULL(sessionProperties);
  }
```

(c) Add getter (parallel to existing `cache()` getter):

```cpp
  /// Returns the FsCache singleton-wrapping pointer, or nullptr if no
  /// FsCache plumbing is installed for this query.
  cache::fs::FsCache* fsCache() const {
    return fsCache_;
  }
```

(d) Add private member next to `cache_`:

```cpp
  cache::fs::FsCache* const fsCache_;
```

### Step 1.14: Thread fsCache through OperatorCtx::createConnectorQueryCtx

In `velox/exec/Operator.cpp` modify the `createConnectorQueryCtx` body (lines 51-74) — append one final argument after `task->queryCtx()->fsTokenProvider()`:

```cpp
  auto connectorQueryCtx = std::make_shared<connector::ConnectorQueryCtx>(
      pool_,
      connectorPool,
      task->queryCtx()->connectorSessionProperties(connectorId),
      spillConfig,
      driverCtx_->prefixSortConfig(),
      std::make_unique<SimpleExpressionEvaluator>(
          execCtx()->queryCtx(), execCtx()->pool()),
      task->queryCtx()->cache(),
      task->queryCtx()->queryId(),
      taskId(),
      planNodeId,
      driverCtx_->driverId,
      driverCtx_->queryConfig().sessionTimezone(),
      driverCtx_->queryConfig().adjustTimestampToTimezone(),
      task->getCancellationToken(),
      task->queryCtx()->fsTokenProvider(),
      task->queryCtx()->fsCache());
```

### Step 1.15: Build + run the connector test — expect PASS

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_connector_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/connectors/tests/velox_connector_test \
      --gtest_filter='ConnectorQueryCtxTest.fsCacheGetterRoundTrip'
```

Expected: 1/1 PASS.

### Step 1.16: Verify no compile regressions across the tree

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 -j 2>&1 | tail -50
```

Expected: clean build, no warnings introduced. If new warnings about the extra arg appear at other ConnectorQueryCtx construction sites, those sites would need updating — but the audit showed `OperatorCtx::createConnectorQueryCtx` is the only positional construction site, and the new arg is defaulted.

### Step 1.17: Run all touched test suites

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/core/tests/velox_core_test --gtest_filter='QueryCtxTest.*'
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/connectors/tests/velox_connector_test
```

Expected: all pre-existing tests still pass, plus the 4 new cases pass.

### Step 1.18: Commit

Files staged (explicit list — no `git add .`):

```bash
git add \
  velox/common/caching/fscache/FsCache.h \
  velox/common/caching/fscache/FsCache.cpp \
  velox/common/caching/fscache/tests/FsCacheTest.cpp \
  velox/core/QueryCtx.h \
  velox/core/QueryCtx.cpp \
  velox/core/tests/QueryCtxTest.cpp \
  velox/connectors/Connector.h \
  velox/connectors/tests/ConnectorTest.cpp \
  velox/exec/Operator.cpp

git commit -m "$(cat <<'EOF'
feat(fscache): plumb FsCache through QueryCtx/ConnectorQueryCtx

Mirror AsyncDataCache's plumbing chain for the FsCache backend so the
TpcdsBenchmark A/B (spec 2026-05-23-fscache-vs-cbi-tpcds) can flip
between the two cache stacks via a process-startup singleton install
without touching every QueryCtx construction site.

Per spec sections 2.1-2.4:
- FsCache::set/getInstance singleton (FsCache.cpp local-static idiom,
  mirrors AsyncDataCache.cpp:835-852).
- QueryCtx::fsCache_ slot, default-initialized from
  FsCache::getInstance() so every QueryCtx auto-picks up whatever the
  bench installed.
- ConnectorQueryCtx::fsCache_ appended LAST in the ctor (after
  tokenProvider) with nullptr default; inserting it mid-list would
  silently shift positional args at every call site.
- OperatorCtx::createConnectorQueryCtx (the sole 14+arg positional
  construction site) threads queryCtx->fsCache() through.

Tests: FsCacheSingletonTest x2, QueryCtxTest.fsCacheDefaultsToSingleton,
ConnectorQueryCtxTest.fsCacheGetterRoundTrip.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-1):** the 4 new tests pass; existing tree still compiles and passes.

---

## Task 2: HiveConnectorUtil::createBufferedInput — FsCache Branch

Matches spec §2.5 and §5.2 commit-2.

**Files:**
- Modify: `velox/connectors/hive/HiveConnectorUtil.cpp:653-704` (`createBufferedInput` — prepend the fsCache branch)
- Modify: `velox/connectors/hive/tests/HiveConnectorUtilTest.cpp` (add new TEST_F for branch selection)

### Step 2.1: Write failing test — fsCache non-null ⇒ FsCacheBufferedInput

Add a new TEST_F at the end of `velox/connectors/hive/tests/HiveConnectorUtilTest.cpp` (before the closing namespace). The test exercises the dispatch by checking the dynamic type of the returned `BufferedInput`. Use `dynamic_cast` against the four candidate types.

```cpp
TEST_F(HiveConnectorUtilTest, createBufferedInputBackendSelection) {
  // Reuses the fixture's pool_, ioStats_, ioStatistics_, executor_ if
  // present; otherwise add minimal setup locally (mirror what the existing
  // tests in this file do for ReaderOptions / FileHandle).
  dwio::common::ReaderOptions readerOpts{pool_.get()};

  // Build a tiny in-memory file handle.
  std::string payload = "hello fscache";
  auto readFile = std::make_shared<InMemoryReadFile>(payload);
  FileHandle fileHandle{
      readFile, /*uuid=*/"u", /*groupId=*/0};

  // FsCache branch.
  {
    cache::fs::FsCacheConfig fsCfg;
    auto tempDir = velox::exec::test::TempDirectoryPath::create();
    fsCfg.cacheRoot = tempDir->getPath();
    fsCfg.maxBytes = 16ULL << 20;
    auto fsCache = std::make_unique<cache::fs::FsCache>(fsCfg);

    ConnectorQueryCtx ctx{
        pool_.get(), pool_.get(), &sessionProperties_,
        /*spill=*/nullptr, common::PrefixSortConfig{},
        /*expr=*/nullptr, /*cache=*/nullptr,
        "q", "t", "p", 0, "UTC",
        /*adjust=*/false, /*cancel=*/{}, /*token=*/{}, fsCache.get()};

    auto buf = createBufferedInput(
        fileHandle, readerOpts, &ctx, ioStatistics_, ioStats_,
        executor_.get(), /*fileReadOps=*/{});
    EXPECT_NE(dynamic_cast<dwio::common::FsCacheBufferedInput*>(buf.get()), nullptr);
  }

  // CBI branch: cache non-null, fscache null.
  {
    auto cache = cache::AsyncDataCache::create(
        memory::memoryManager()->allocator(), /*ssdCache=*/nullptr);
    ConnectorQueryCtx ctx{
        pool_.get(), pool_.get(), &sessionProperties_,
        /*spill=*/nullptr, common::PrefixSortConfig{},
        /*expr=*/nullptr, cache.get(),
        "q", "t", "p", 0, "UTC"};
    auto buf = createBufferedInput(
        fileHandle, readerOpts, &ctx, ioStatistics_, ioStats_,
        executor_.get(), /*fileReadOps=*/{});
    EXPECT_NE(dynamic_cast<dwio::common::CachedBufferedInput*>(buf.get()), nullptr);
    cache->shutdown();
  }

  // Direct branch: both null, non-Nimble format.
  {
    ConnectorQueryCtx ctx{
        pool_.get(), pool_.get(), &sessionProperties_,
        /*spill=*/nullptr, common::PrefixSortConfig{},
        /*expr=*/nullptr, /*cache=*/nullptr,
        "q", "t", "p", 0, "UTC"};
    auto buf = createBufferedInput(
        fileHandle, readerOpts, &ctx, ioStatistics_, ioStats_,
        executor_.get(), /*fileReadOps=*/{});
    EXPECT_NE(dynamic_cast<dwio::common::DirectBufferedInput*>(buf.get()), nullptr);
  }
}
```

Add the necessary includes at the top of the file:

```cpp
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/FsCacheBufferedInput.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
```

If the existing fixture doesn't already provide `ioStatistics_`, `ioStats_`, `executor_`, `sessionProperties_`, add the minimum needed inside the test body (mirror what an adjacent `HiveConnectorUtilTest` does).

### Step 2.2: Run — expect FAIL (the FsCache branch test fails because the code still falls through to Direct/Nimble)

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_hive_connector_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/connectors/hive/tests/velox_hive_connector_test \
      --gtest_filter='HiveConnectorUtilTest.createBufferedInputBackendSelection'
```

Expected: the first sub-assertion fails (`dynamic_cast<FsCacheBufferedInput*>` returns nullptr because the current code path returns `DirectBufferedInput`).

### Step 2.3: Add the FsCache branch

In `velox/connectors/hive/HiveConnectorUtil.cpp` modify `createBufferedInput` (line 653). Insert the new branch FIRST, before the existing `if (connectorQueryCtx->cache())` at line 662:

```cpp
std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStatistics,
    std::shared_ptr<IoStats> ioStats,
    folly::Executor* executor,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  if (auto* fsCache = connectorQueryCtx->fsCache()) {
    // FsCache is mutually exclusive with AsyncDataCache by bench startup
    // invariant (spec §2.6); this branch is taken first so that misconfigured
    // setups installing both would still take the FsCache path consistently.
    return std::make_unique<dwio::common::FsCacheBufferedInput>(
        fileHandle.file,
        readerOpts.memoryPool(),
        fsCache);
  }
  if (connectorQueryCtx->cache()) {
    // ... existing CBI branch unchanged ...
  }
  // ... rest unchanged ...
}
```

Add the include near the top of the file:

```cpp
#include "velox/dwio/common/FsCacheBufferedInput.h"
```

### Step 2.4: Build + run the test — expect PASS

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_hive_connector_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/connectors/hive/tests/velox_hive_connector_test \
      --gtest_filter='HiveConnectorUtilTest.*'
```

Expected: all existing `HiveConnectorUtilTest.*` cases still pass + new case passes.

### Step 2.5: Commit

```bash
git add \
  velox/connectors/hive/HiveConnectorUtil.cpp \
  velox/connectors/hive/tests/HiveConnectorUtilTest.cpp

git commit -m "$(cat <<'EOF'
feat(fscache): pick FsCacheBufferedInput in HiveConnectorUtil

Per spec 2026-05-23-fscache-vs-cbi-tpcds §2.5: when
ConnectorQueryCtx::fsCache() is non-null, createBufferedInput returns
an FsCacheBufferedInput. The new branch is FIRST in the dispatch chain
so misconfigured setups installing both backends still pick FsCache
consistently. The mutual-exclusion invariant lives in the bench
startup (§2.6) — this code path tolerates either configuration.

Test: HiveConnectorUtilTest.createBufferedInputBackendSelection
covers all three cases (fsCache only ⇒ FsCacheBufferedInput,
cache only ⇒ CachedBufferedInput, both null ⇒ DirectBufferedInput).
Nimble branch is unaffected.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-2):** the four-way branch test passes; Nimble branch untouched.

---

## Task 3: FsCacheBufferedInput Parquet Smoke Test

Matches spec §5.2 commit-3 and §1.4 assumption 2. Read-only commit on existing production code.

**Files:**
- Modify: `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp` (add one new TEST_F that opens a real Parquet fixture)

### Step 3.1: Write the smoke test

In `velox/dwio/common/tests/FsCacheBufferedInputTest.cpp`, add a TEST_F at the end (before the closing namespace) that:

1. Opens `sample.parquet` (shipped fixture, accessed via `ParquetTestBase::getExampleFilePath("sample.parquet")`) through a `LocalReadFile`.
2. Wraps that ReadFile in an `FsCacheBufferedInput`.
3. Constructs the Parquet reader on top, scans all rows, and compares row-count + raw bytes to a `LocalReadFile`-only ground truth.

Sketch:

```cpp
#include "velox/dwio/parquet/tests/ParquetTestBase.h"

TEST_F(FsCacheBufferedInputTest, parquetSampleRoundTrips) {
  const auto path = parquet::test::getExampleFilePath("sample.parquet");
  auto remote = std::make_shared<LocalReadFile>(path);
  const uint64_t size = remote->size();

  // Ground truth: full read via LocalReadFile.
  std::string truth(size, '\0');
  remote->pread(0, size, truth.data());

  // Round-trip via FsCacheBufferedInput.enqueue/load.
  FsCacheBufferedInput input{remote, *pool_, fsCache_.get()};
  auto stream = input.enqueue({0, size});
  input.load(LogType::kFile);

  std::string roundTripped(size, '\0');
  const void* data;
  int32_t length = 0;
  uint64_t copied = 0;
  while (copied < size) {
    ASSERT_TRUE(stream->Next(&data, &length));
    ASSERT_GT(length, 0);
    std::memcpy(roundTripped.data() + copied, data, length);
    copied += length;
  }
  EXPECT_EQ(copied, size);
  EXPECT_EQ(roundTripped, truth);
}
```

(If `ParquetTestBase` is not a transitively-accessible header here, fall back to constructing the path with `test::getDataFilePath("velox/dwio/parquet/tests/reader/", "examples/sample.parquet")` — whatever matches the actual fixture location surfaced by `getExampleFilePath`.)

### Step 3.2: Build + run — expect PASS

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_dwio_common_test -j

/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/dwio/common/tests/velox_dwio_common_test \
      --gtest_filter='FsCacheBufferedInputTest.parquetSampleRoundTrips'
```

Expected: PASS — bytes byte-equal vs `LocalReadFile`.

**If FAIL:** that triggers spec §7 OQ #2 — pause and report. Do NOT modify `FsCacheBufferedInput` production code in this task; commit-3 is a smoke gate, not a fix.

### Step 3.3: Commit

```bash
git add velox/dwio/common/tests/FsCacheBufferedInputTest.cpp

git commit -m "$(cat <<'EOF'
test(fscache): FsCacheBufferedInput round-trips a real Parquet file

Spec 2026-05-23-fscache-vs-cbi-tpcds §1.4 assumption 2 + §5.2 commit-3:
validate that FsCacheBufferedInput.enqueue/load survives end-to-end on
a real Parquet file before the bench-level integration lands. Uses
the shipped sample.parquet fixture, compares bytes against
LocalReadFile ground truth.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-3):** `sample.parquet` round-trips byte-equal vs `LocalReadFile`.

---

## Task 4: TpcdsBenchmark Custom Main + Plan Hoist + Flags

Matches spec §2.6, §2.7, §2.9, §3.1, §4.5 and §5.2 commit-4. This is the heaviest commit.

**Files:**
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmark.h` (add public `runAb()` method + new flag externs if needed)
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmark.cpp` (new `runAb()` body — round loop + plan hoist + CSV writer; do NOT touch existing `runMain` / `BENCHMARK` macros)
- Modify: `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp` (parse `--input_source`; if set ⇒ install singleton + call `runAb()`, else ⇒ existing `tpcdsBenchmarkMain()` path)

### Step 4.1: Add new flag definitions

Top of `velox/benchmarks/tpcds/TpcdsBenchmark.cpp` (with the other DEFINE_*'s near the top of the file), add:

```cpp
DEFINE_string(input_source, "",
    "Cache backend for the A/B sweep. One of: cbi, fscache. Empty disables "
    "the new A/B path and falls back to the legacy folly::runBenchmarks() "
    "flow (see TpcdsBenchmark.cpp::runMain).");
DEFINE_int32(rounds, 3,
    "Outer round count for the A/B sweep. Each round runs all queries once. "
    "--num_repeats must be 1; see spec §2.7 \"Why a new --rounds flag\".");
DEFINE_int32(fscache_disk_gib, 58,
    "FsCache on-disk budget in GiB. Only used when --input_source=fscache.");
DEFINE_string(fscache_root, "/tmp/velox_fscache",
    "FsCache cache directory. Wiped at startup. Only used when --input_source=fscache.");
DEFINE_string(out, "",
    "CSV output path for the A/B sweep. Required when --input_source is set.");
```

### Step 4.2: Add `runAb()` public method to TpcdsBenchmark

In `velox/benchmarks/tpcds/TpcdsBenchmark.h` add (next to the existing `initialize()` override at h:33):

```cpp
  /// Runs the spec 2026-05-23-fscache-vs-cbi-tpcds A/B sweep:
  /// FLAGS_rounds outer iterations × 99 queries, plan construction hoisted
  /// once before the round loop, per-query wall_ms measured via
  /// std::chrono::steady_clock around QueryBenchmarkBase::run(). Writes
  /// one CSV row per (round, query) to FLAGS_out. Returns the number of
  /// failed queries (rows with non-empty error column) so the caller can
  /// set a non-zero exit code without re-reading the CSV.
  int32_t runAb();
```

### Step 4.3: Write failing test for plan hoist + round loop correctness

Spec gate: `--input_source=cbi --rounds=2 --num_repeats=1` on q1 — round 2 `wall_ms` < round 1 `wall_ms` AND round 2 has no plan-build cost.

This is a hand-driven acceptance gate (no unit test framework). The plan-build-cost check happens by inserting a one-shot debug log around `getQueryPlan` calls and verifying it fires exactly 99 times (once per query), regardless of `FLAGS_rounds`. After verifying, the debug log is removed before commit.

For TDD on the round loop itself, write a minimal acceptance script (`/tmp/ab_smoke.sh`) that runs `--input_source=cbi --rounds=2 --num_repeats=1 --out=/tmp/ab.csv` and asserts:
- CSV has 99 × 2 = 198 data rows + 1 header.
- All rounds appear (round=1 and round=2 both present).
- No row has `error≠""` for q1.

Run **before** writing `runAb()`. Expected: binary exits non-zero (flag unknown / runAb not implemented).

### Step 4.4: Implement `runAb()` body

In `velox/benchmarks/tpcds/TpcdsBenchmark.cpp` add (near `runMain` body, ~line 173):

```cpp
namespace {

struct AbCsvRow {
  int round{};
  int32_t queryId{};
  double wallMs{};
  uint64_t rows{};
  uint64_t bytesRead{};
  double hitPct{};
  double bytesDlMib{};
  double evictMib{};
  double opP50Us{};
  double opP95Us{};
  std::string error;
};

void writeCsvHeader(std::ostream& out) {
  out << "round,query_id,wall_ms,rows,bytes_read,hit_pct,"
         "bytes_dl_mib,evict_mib,op_p50_us,op_p95_us,error\n";
}

void writeCsvRow(std::ostream& out, const AbCsvRow& row) {
  out << row.round << ","
      << fmt::format("q{:02d}", row.queryId) << ","
      << fmt::format("{:.3f}", row.wallMs) << ","
      << row.rows << ","
      << row.bytesRead << ","
      << fmt::format("{:.4f}", row.hitPct) << ","
      << fmt::format("{:.4f}", row.bytesDlMib) << ","
      << fmt::format("{:.4f}", row.evictMib) << ","
      << fmt::format("{:.3f}", row.opP50Us) << ","
      << fmt::format("{:.3f}", row.opP95Us) << ","
      << row.error << "\n";
}

} // namespace

int32_t TpcdsBenchmark::runAb() {
  // Spec §2.9 startup guard: outer rounds use --rounds, inner repetition
  // pinned to 1; see spec §2.7 "Why a new --rounds flag".
  VELOX_CHECK_EQ(
      FLAGS_num_repeats, 1, "--num_repeats must be 1; outer rounds use --rounds");
  VELOX_USER_CHECK(!FLAGS_out.empty(), "--out is required with --input_source");

  // Hoist plan construction (spec §4.5). getQueryPlan returns
  // exec::test::VeloxPlan (alias for TpchPlan); QueryBenchmarkBase::run
  // takes TpchPlan so pass-through.
  constexpr int32_t kNumQueries = 99;
  std::vector<exec::test::VeloxPlan> plans;
  plans.reserve(kNumQueries);
  for (int32_t q = 1; q <= kNumQueries; ++q) {
    plans.push_back(queryBuilder_->getQueryPlan(q, planDir_, pool_.get()));
  }

  std::ofstream csv(FLAGS_out, std::ios::out | std::ios::trunc);
  VELOX_USER_CHECK(csv.is_open(), "Failed to open --out for write: {}", FLAGS_out);
  writeCsvHeader(csv);

  int32_t failed = 0;
  for (int round = 1; round <= FLAGS_rounds; ++round) {
    for (int32_t q = 1; q <= kNumQueries; ++q) {
      AbCsvRow row;
      row.round = round;
      row.queryId = q;
      const auto wallStart = std::chrono::steady_clock::now();
      auto [cursor, results] = run(plans[q - 1], queryConfigs_);
      const auto wallEnd = std::chrono::steady_clock::now();
      row.wallMs =
          std::chrono::duration<double, std::milli>(wallEnd - wallStart).count();
      if (cursor == nullptr) {
        // run() already LOG(ERROR)'d the exception message; record a marker.
        row.error = "task failed (see ERROR log)";
        ++failed;
      } else {
        const auto stats = cursor->task()->taskStats();
        for (const auto& pipeline : stats.pipelineStats) {
          for (const auto& op : pipeline.operatorStats) {
            row.rows += op.outputPositions;
            row.bytesRead += op.rawInputBytes;
          }
        }
        // Backend stats: read from the installed singleton; one of the two
        // is null per spec §2.6.
        if (auto* fsCache = cache::fs::FsCache::getInstance()) {
          const auto s = fsCache->stats();
          const uint64_t total = s.hits + s.misses;
          row.hitPct = total ? 100.0 * static_cast<double>(s.hits) / total : 0.0;
          row.bytesDlMib = static_cast<double>(s.bytesOnDisk) / (1ULL << 20);
          row.evictMib = static_cast<double>(s.evictions); // count, not bytes
        } else if (auto* cache = cache::AsyncDataCache::getInstance()) {
          const auto s = cache->refreshStats();
          // Hit% and download bytes from AsyncDataCache::Stats — names per
          // velox/common/caching/AsyncDataCache.h.
          const uint64_t hits = s.numHit;
          const uint64_t lookups = s.numHit + s.numNew + s.numWaitExclusive;
          row.hitPct =
              lookups ? 100.0 * static_cast<double>(hits) / lookups : 0.0;
          row.bytesDlMib = static_cast<double>(s.bytesCached) / (1ULL << 20);
          row.evictMib = static_cast<double>(s.numEvict);
        }
        // Operator latency quantiles: aggregate cpuTimings across all ops.
        std::vector<int64_t> samplesNs;
        for (const auto& pipeline : stats.pipelineStats) {
          for (const auto& op : pipeline.operatorStats) {
            if (op.cpuTiming.count > 0) {
              samplesNs.push_back(op.cpuTiming.cpuNanos / op.cpuTiming.count);
            }
          }
        }
        std::sort(samplesNs.begin(), samplesNs.end());
        const auto quantileUs = [&](double q) -> double {
          if (samplesNs.empty()) {
            return 0.0;
          }
          const auto idx =
              std::min(samplesNs.size() - 1, static_cast<size_t>(samplesNs.size() * q));
          return samplesNs[idx] / 1000.0;
        };
        row.opP50Us = quantileUs(0.50);
        row.opP95Us = quantileUs(0.95);
      }
      writeCsvRow(csv, row);
      csv.flush(); // visible to merge tooling even if a later query crashes.
    }
  }
  csv.close();
  return failed;
}
```

Add includes at the top of the file:

```cpp
#include <chrono>
#include <fstream>
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/fscache/FsCache.h"
```

If `AsyncDataCache::Stats` field names differ from `numHit / numNew / numWaitExclusive / bytesCached / numEvict`, grep `velox/common/caching/AsyncDataCache.h` for the actual `Stats` struct and adjust. (This is the only "OQ#1 placeholder resolution" the spec defers to commit-4 — see spec §7 OQ#1.)

### Step 4.5: Update TpcdsBenchmarkMain to dispatch on --input_source

Rewrite `velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp`:

```cpp
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed under the Apache License, Version 2.0.
 */

#include <cstdlib>
#include <filesystem>
#include <memory>

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/benchmarks/tpcds/TpcdsBenchmark.h"
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"

DECLARE_string(input_source);
DECLARE_int32(fscache_disk_gib);
DECLARE_string(fscache_root);

namespace {

std::unique_ptr<facebook::velox::cache::fs::FsCache> installFsCache() {
  using facebook::velox::cache::fs::FsCache;
  using facebook::velox::cache::fs::FsCacheConfig;
  // Wipe cache root for the cold-round-1 invariant (spec §4.4).
  std::error_code ec;
  std::filesystem::remove_all(FLAGS_fscache_root, ec);
  std::filesystem::create_directories(FLAGS_fscache_root, ec);

  FsCacheConfig cfg;
  cfg.cacheRoot = FLAGS_fscache_root;
  cfg.maxBytes = static_cast<uint64_t>(FLAGS_fscache_disk_gib) << 30;
  // alignment / maxSegmentSize / numBuckets keep their CH-default values
  // (4 MiB / 32 MiB / 1024) per spec §6.
  auto cache = std::make_unique<FsCache>(cfg);
  FsCache::setInstance(cache.get());
  return cache;
}

} // namespace

int main(int argc, char** argv) {
  std::string kUsage(
      "TPC-DS benchmark. With --input_source={cbi,fscache} runs the new "
      "A/B sweep (spec 2026-05-23-fscache-vs-cbi-tpcds). Without it, runs "
      "the legacy folly::runBenchmarks() flow.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init{&argc, &argv, false};

  tpcdsBenchmark = std::make_unique<facebook::velox::TpcdsBenchmark>();

  if (FLAGS_input_source.empty()) {
    tpcdsBenchmarkMain();
    return 0;
  }

  // New A/B path.
  std::unique_ptr<facebook::velox::cache::fs::FsCache> ownedFsCache;
  if (FLAGS_input_source == "fscache") {
    // initialize() below skips AsyncDataCache when FLAGS_cache_gb is 0; force
    // it to 0 here so the CBI tier stays nullptr (spec §2.6).
    FLAGS_cache_gb = 0;
    ownedFsCache = installFsCache();
  } else if (FLAGS_input_source == "cbi") {
    // Existing behavior: QueryBenchmarkBase::initialize() builds the
    // AsyncDataCache + SsdCache when FLAGS_cache_gb > 0.
    VELOX_USER_CHECK_GT(
        FLAGS_cache_gb, 0,
        "--input_source=cbi requires --cache_gb > 0");
  } else {
    VELOX_USER_FAIL("Unknown --input_source: {} (expected cbi or fscache)",
                    FLAGS_input_source);
  }

  tpcdsBenchmark->initialize();
  const int32_t failed = tpcdsBenchmark->runAb();
  tpcdsBenchmark->shutdown();

  if (FLAGS_input_source == "fscache") {
    facebook::velox::cache::fs::FsCache::setInstance(nullptr);
  }
  return failed > 10 ? 1 : 0;  // spec §4.2 soft cap; main exits non-zero only on systemic failure.
}
```

The `FLAGS_cache_gb = 0` override resolves spec §7 OQ#1 by short-circuiting `QueryBenchmarkBase::initialize()`'s AsyncDataCache branch from main — no override of `initialize()` itself needed.

### Step 4.6: Build + run the commit-4 acceptance gate

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_tpcds_benchmark -j

BIN=/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark

# CBI side, 2 rounds, q1 only — easiest way: --run_query_verbose=1 doesn't
# apply here (it's a legacy-path flag). Run the full sweep at small scale
# and grep round-1 / round-2 q1 from the CSV.
"$BIN" --input_source=cbi --rounds=2 --num_repeats=1 --num_drivers=4 \
       --cache_gb=8 --ssd_cache_gb=50 --ssd_path=/tmp/velox_cbi_ssd_smoke \
       --data_path=/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned \
       --out=/tmp/ab_cbi_smoke.csv

# Verify: q01 round 2 wall_ms < q01 round 1 wall_ms (warm faster than cold).
grep ',q01,' /tmp/ab_cbi_smoke.csv

# Verify plan-build cost is paid exactly once: count construction in cpu profile,
# OR add a temporary LOG(INFO) inside the hoist loop, run with --logtostderr=1,
# count "BUILT PLAN q" log lines = 99 (not 99 * rounds).
```

Expected: q01's round-2 `wall_ms` is strictly less than round-1. Repeat with `--input_source=fscache` (plus `--fscache_disk_gib=58 --fscache_root=/tmp/velox_fscache_smoke`) — same property must hold.

If the warm-vs-cold property fails: investigate before committing — likely a regression in plan hoist or in the singleton install order.

### Step 4.7: Commit

```bash
git add \
  velox/benchmarks/tpcds/TpcdsBenchmark.h \
  velox/benchmarks/tpcds/TpcdsBenchmark.cpp \
  velox/benchmarks/tpcds/TpcdsBenchmarkMain.cpp

git commit -m "$(cat <<'EOF'
feat(fscache): TpcdsBenchmark A/B path with plan hoist and CSV output

Adds --input_source={cbi,fscache} dispatch to the TPC-DS benchmark.
When set, the binary takes a new code path (TpcdsBenchmark::runAb)
that hoists all 99 plan constructions out of the round loop, runs
--rounds outer iterations × 99 queries, and writes one CSV row per
(round, query) to --out. Per-query wall time is measured by chrono
around QueryBenchmarkBase::run().

Per spec 2026-05-23-fscache-vs-cbi-tpcds §2.6 / §2.7 / §4.5:
- --num_repeats pinned to 1 (VELOX_CHECK_EQ at startup); outer
  --rounds owns repetition. Reusing --num_repeats would cause N²
  executions and destroy the cold-round signal (run() at
  QueryBenchmarkBase.cpp:286 loops internally).
- fscache mode forces FLAGS_cache_gb=0 to keep
  QueryBenchmarkBase::initialize()'s AsyncDataCache branch null
  (resolves spec §7 OQ#1 — short-circuit option).
- cbi mode requires --cache_gb > 0 so AsyncDataCache constructs.
- Legacy folly::runBenchmarks() path untouched (preserved when
  --input_source is empty per spec §4.5).

Acceptance: cbi q01 round-2 wall_ms < round-1 wall_ms verified.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-4):** `--input_source=cbi --rounds=2 --num_repeats=1` on q1 — round 2 `wall_ms` < round 1 `wall_ms` AND plan-build fires exactly 99 times; same on `fscache`.

---

## Task 5: run_ab.sh + merge_tpcds_ab.py

Matches spec §2.8, §3.3 and §5.2 commit-5.

**Files:**
- Create: `velox/benchmarks/tpcds/run_ab.sh` (shell driver — two binary invocations + merge)
- Create: `scripts/bench/merge_tpcds_ab.py` (CSV → Markdown report)

### Step 5.1: Create run_ab.sh

Write `velox/benchmarks/tpcds/run_ab.sh` exactly as the spec §2.8 prescribes:

```bash
#!/usr/bin/env bash
# Two processes — one per backend — each runs 3 rounds × 99 queries.
# Cache state persists across rounds inside one process and is freshly
# constructed on the other one starting. `--clear_ram_cache` /
# `--clear_ssd_cache` are left false (their defaults) so rounds 2/3 are
# warm.
#
# Explicit `set +e` so a one-sided crash still lets the other side
# finish and the merge step still runs.
set +e

BIN=./cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark
OUT=docs/superpowers/results
DATA=/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned
mkdir -p "$OUT"

"$BIN" --input_source=cbi --rounds=3 --num_repeats=1 --num_drivers=4 \
  --cache_gb=8 --ssd_cache_gb=50 --ssd_path=/tmp/velox_cbi_ssd \
  --data_path="$DATA" \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv"
cbi_exit=$?

"$BIN" --input_source=fscache --rounds=3 --num_repeats=1 --num_drivers=4 \
  --fscache_disk_gib=58 --fscache_root=/tmp/velox_fscache \
  --data_path="$DATA" \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv"
fscache_exit=$?

python3 scripts/bench/merge_tpcds_ab.py \
  "$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv" \
  "$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv" \
  > "$OUT/2026-05-23-fscache-vs-cbi-tpcds.md"

[[ $cbi_exit -ne 0 || $fscache_exit -ne 0 ]] && \
  echo "WARN: partial run (cbi=$cbi_exit fscache=$fscache_exit)" >&2
```

Set executable: `chmod +x velox/benchmarks/tpcds/run_ab.sh`.

### Step 5.2: Create merge_tpcds_ab.py

Write `scripts/bench/merge_tpcds_ab.py` — stdlib-only Python that takes two CSV paths, emits the three tables (cold round / warm mean / summary) from spec §3.3, plus a header block with dataset path / cache budgets / host info / caveat from §3.2:

```python
#!/usr/bin/env python3
"""Merge two CSV outputs from velox_tpcds_benchmark (--input_source=cbi vs
fscache) into a single Markdown report. See spec §3.3.

Usage:
    merge_tpcds_ab.py CBI_CSV FSCACHE_CSV > report.md
"""
import csv
import math
import platform
import socket
import statistics
import subprocess
import sys
from collections import defaultdict


def read_csv(path):
    rows_by_round_qid = defaultdict(dict)  # (round, qid) → row
    with open(path) as f:
        for row in csv.DictReader(f):
            r = int(row["round"])
            rows_by_round_qid[(r, row["query_id"])] = row
    return rows_by_round_qid


def geomean(values):
    if not values:
        return float("nan")
    log_sum = sum(math.log(v) for v in values if v > 0)
    n = sum(1 for v in values if v > 0)
    return math.exp(log_sum / n) if n else float("nan")


def cold_table(cbi, fscache, queries):
    print("**Table A — Cold round (round=1)**\n")
    print("| query | CBI ms | FsCache ms | Δ% | CBI hit% | FsCache hit% | CBI dl MiB | FsCache dl MiB |")
    print("|-------|-------:|-----------:|---:|---------:|-------------:|-----------:|---------------:|")
    for q in queries:
        c = cbi.get((1, q))
        f = fscache.get((1, q))
        if not c or not f or c["error"] or f["error"]:
            print(f"| {q} | — | — | — | — | — | — | — |")
            continue
        c_ms = float(c["wall_ms"])
        f_ms = float(f["wall_ms"])
        delta = 100.0 * (f_ms - c_ms) / c_ms if c_ms else float("nan")
        print(f"| {q} | {c_ms:.1f} | {f_ms:.1f} | {delta:+.1f}% | "
              f"{float(c['hit_pct']):.1f}% | {float(f['hit_pct']):.1f}% | "
              f"{float(c['bytes_dl_mib']):.1f} | {float(f['bytes_dl_mib']):.1f} |")


def warm_table(cbi, fscache, queries):
    print("\n**Table B — Warm mean of round 2 + round 3**\n")
    print("| query | CBI ms | FsCache ms | Δ% | CBI hit% | FsCache hit% | CBI dl MiB | FsCache dl MiB |")
    print("|-------|-------:|-----------:|---:|---------:|-------------:|-----------:|---------------:|")
    for q in queries:
        c2, c3 = cbi.get((2, q)), cbi.get((3, q))
        f2, f3 = fscache.get((2, q)), fscache.get((3, q))
        if not all([c2, c3, f2, f3]) or any(r["error"] for r in [c2, c3, f2, f3]):
            print(f"| {q} | — | — | — | — | — | — | — |")
            continue
        c_ms = (float(c2["wall_ms"]) + float(c3["wall_ms"])) / 2
        f_ms = (float(f2["wall_ms"]) + float(f3["wall_ms"])) / 2
        delta = 100.0 * (f_ms - c_ms) / c_ms if c_ms else float("nan")
        print(f"| {q} | {c_ms:.1f} | {f_ms:.1f} | {delta:+.1f}% | "
              f"{float(f3['hit_pct']):.1f}% | {float(f3['hit_pct']):.1f}% | "
              f"{float(c3['bytes_dl_mib']):.1f} | {float(f3['bytes_dl_mib']):.1f} |")


def summary_table(cbi, fscache, queries):
    cbi_cold = [float(cbi[(1, q)]["wall_ms"]) for q in queries
                if (1, q) in cbi and not cbi[(1, q)]["error"]]
    fscache_cold = [float(fscache[(1, q)]["wall_ms"]) for q in queries
                    if (1, q) in fscache and not fscache[(1, q)]["error"]]
    cbi_warm, fscache_warm, deltas = [], [], []
    wins = losses = regressions = 0
    regression_qs = []
    failed = 0
    for q in queries:
        cset = [cbi.get((r, q)) for r in (2, 3)]
        fset = [fscache.get((r, q)) for r in (2, 3)]
        if not all(cset) or not all(fset) or any(r["error"] for r in cset + fset):
            failed += 1
            continue
        c_ms = sum(float(r["wall_ms"]) for r in cset) / 2
        f_ms = sum(float(r["wall_ms"]) for r in fset) / 2
        cbi_warm.append(c_ms)
        fscache_warm.append(f_ms)
        delta = 100.0 * (f_ms - c_ms) / c_ms if c_ms else 0.0
        deltas.append(delta)
        if delta <= -5.0:
            wins += 1
        elif delta >= 5.0:
            losses += 1
        if delta >= 20.0:
            regressions += 1
            regression_qs.append(q)

    print("\n**Table C — Summary**\n")
    print("|                  | CBI  | FsCache | Δ |")
    print("|------------------|-----:|--------:|---|")
    c_geo = geomean(cbi_cold)
    f_geo = geomean(fscache_cold)
    cold_delta = 100.0 * (f_geo - c_geo) / c_geo if c_geo else 0.0
    print(f"| cold geomean ms  | {c_geo:.0f} | {f_geo:.0f} | {cold_delta:+.1f}% |")
    cw_geo = geomean(cbi_warm)
    fw_geo = geomean(fscache_warm)
    warm_delta = 100.0 * (fw_geo - cw_geo) / cw_geo if cw_geo else 0.0
    print(f"| warm geomean ms  | {cw_geo:.0f} | {fw_geo:.0f} | {warm_delta:+.1f}% |")
    print(f"| wins  (FsCache ≥5% faster) | — | {wins} / {len(deltas)} | |")
    print(f"| losses (FsCache ≥5% slower)| — | {losses} / {len(deltas)} | |")
    print(f"| regressions > 20%          | — | {regressions} / {len(deltas)} | "
          f"({', '.join(regression_qs)}) |")
    print(f"| failed (excluded)          | — | {failed} / {len(deltas) + failed} | |")


def header(cbi_csv, fscache_csv):
    print("# FsCache vs CBI — TPC-DS scale 100 A/B\n")
    print("**Spec:** `docs/superpowers/specs/2026-05-23-fscache-vs-cbi-tpcds-design.md`\n")
    print(f"**Host:** {socket.gethostname()}, kernel {platform.release()}")
    try:
        cpu = subprocess.check_output(
            ["bash", "-c", "lscpu | grep 'Model name' | head -1"]).decode().strip()
        print(f"**CPU:** {cpu}")
    except Exception:
        pass
    print("**Build:** RelWithDebInfo, GCC-13\n")
    print("**Δ% convention:** negative ⇒ FsCache faster, positive ⇒ slower.\n")
    print("**Caveat (spec §3.2):** `bytes_dl_mib` semantics differ between sides. "
          "CBI = remote→RAM (SsdCache hits not counted); FsCache = remote→disk only. "
          "Do not compare absolute download volumes across sides without this context.\n")


def main():
    if len(sys.argv) != 3:
        sys.exit(f"Usage: {sys.argv[0]} CBI_CSV FSCACHE_CSV")
    cbi = read_csv(sys.argv[1])
    fscache = read_csv(sys.argv[2])
    queries = sorted({q for (_, q) in set(cbi) | set(fscache)})

    header(sys.argv[1], sys.argv[2])
    cold_table(cbi, fscache, queries)
    warm_table(cbi, fscache, queries)
    summary_table(cbi, fscache, queries)


if __name__ == "__main__":
    main()
```

Set executable: `chmod +x scripts/bench/merge_tpcds_ab.py`.

### Step 5.3: Smoke test the harness with q1-q5 subset (spec §4.7)

Hand-driven gate; not a unit test. There is no flag to limit to q1-q5 today, so substitute a 99-query small run (`--rounds=1`) and verify the merge script renders cleanly:

```bash
mkdir -p /tmp/ab_smoke
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
      --target velox_tpcds_benchmark -j

BIN=/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark
DATA=/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned

"$BIN" --input_source=cbi --rounds=1 --num_repeats=1 --num_drivers=4 \
  --cache_gb=8 --ssd_cache_gb=50 --ssd_path=/tmp/velox_cbi_ssd_smoke \
  --data_path="$DATA" \
  --out=/tmp/ab_smoke/cbi.csv

"$BIN" --input_source=fscache --rounds=1 --num_repeats=1 --num_drivers=4 \
  --fscache_disk_gib=58 --fscache_root=/tmp/velox_fscache_smoke \
  --data_path="$DATA" \
  --out=/tmp/ab_smoke/fscache.csv

python3 scripts/bench/merge_tpcds_ab.py \
  /tmp/ab_smoke/cbi.csv /tmp/ab_smoke/fscache.csv > /tmp/ab_smoke/report.md

head -30 /tmp/ab_smoke/report.md
```

Expected: report.md has the three table structures present (cold / warm / summary), even if warm tables are mostly `—` (only 1 round ran). No Python tracebacks.

### Step 5.4: Commit

```bash
git add \
  velox/benchmarks/tpcds/run_ab.sh \
  scripts/bench/merge_tpcds_ab.py

git commit -m "$(cat <<'EOF'
feat(fscache): A/B sweep harness — run_ab.sh + merge_tpcds_ab.py

Per spec 2026-05-23-fscache-vs-cbi-tpcds §2.8 / §3.3:
- run_ab.sh drives two processes (CBI + FsCache), each runs the new
  --input_source A/B sweep, then merges the two CSVs into one
  Markdown report. `set +e` so a one-sided crash still produces the
  other side's table.
- merge_tpcds_ab.py renders the three tables from §3.3 (cold round
  1, warm mean of rounds 2+3, summary with geomean / wins / losses /
  regressions > 20%) plus the §3.2 cross-backend caveat. Stdlib-only
  Python; geomean excludes rows with non-empty error.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-5):** harness produces a structurally-correct merged Markdown report from a small-scale run.

---

## Task 6: Full 99-Query Sweep + Result Commit

Matches spec §5.2 commit-6.

**Files:**
- Create: `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds.md` (merged report)
- Create: `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv`
- Create: `docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv`

### Step 6.1: Run the full sweep

```bash
cd /home/chang/OpenSource/velox2
bash velox/benchmarks/tpcds/run_ab.sh 2>&1 | tee /tmp/ab_run.log
```

Expected: ~1.5–4h wall time per spec §4.7. Two CSVs land under `docs/superpowers/results/` plus the merged `.md`.

### Step 6.2: Inspect the result for the §4.2 + §5.4 invalidators

```bash
RES=docs/superpowers/results
# Failures per (round, side).
for f in $RES/2026-05-23-fscache-vs-cbi-tpcds-{cbi,fscache}.csv; do
  echo "=== $f ==="
  awk -F, 'NR>1 && $NF!="" {print $1}' "$f" | sort | uniq -c
done

# Wall-second sanity: cold round shouldn't differ by >5x (spec §5.4).
# Eyeball the cold-round geomean from Table C.
head -60 $RES/2026-05-23-fscache-vs-cbi-tpcds.md
```

Expected: ≤ 10 failures on each side; cold round ratio < 5×. If either invariant trips, add an `INVALID:` line to the report header and pause — don't commit as a published baseline.

### Step 6.3: Commit

```bash
git add \
  docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds.md \
  docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv \
  docs/superpowers/results/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv

git commit -m "$(cat <<'EOF'
bench(fscache): TPC-DS scale 100 A/B baseline — FsCache vs CBI

Per spec 2026-05-23-fscache-vs-cbi-tpcds §5.2 commit-6: full 99-query
× 3-round × 2-backend sweep on TPC-DS scale 100 parquet, host
ChangDev (13th Gen Intel Core i9-13900KF, 32 threads), GCC-13
RelWithDebInfo build. Failures per (round, side) ≤ 10; cold-round
ratio < 5×.

See report header for the cross-backend `bytes_dl_mib` caveat
(spec §3.2). Δ% convention: negative ⇒ FsCache faster.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

**Acceptance gate (spec §5.2 commit-6):** failures ≤ 10 / 99 on each side; result file committed.

---

## Self-Review Notes

Coverage vs spec sections:
- §1.2 in-scope bullets 1-8 → Tasks 1, 2, 4, 5, 6.
- §2.1-§2.5 plumbing → Task 1 + Task 2.
- §2.6 startup logic → Task 4 (TpcdsBenchmarkMain).
- §2.7 flags → Task 4 (flag DEFINEs + VELOX_CHECK_EQ guard).
- §2.8 shell harness → Task 5.
- §2.9 data flow → Tasks 4 + 5.
- §3.1-§3.4 output → Tasks 4 (CSV) + 5 (Markdown merge).
- §4.1 per-query isolation → Task 4 (`if (cursor == nullptr)` branch).
- §4.2 failure threshold → Task 4 (`failed > 10 ? 1 : 0`) + Task 6 (manual gate).
- §4.3 process-level failure → Task 5 (`set +e`).
- §4.4 cache state → Task 4 (`filesystem::remove_all(FLAGS_fscache_root)` at startup).
- §4.5 plan caching → Task 4 (hoist out of round loop).
- §4.6 concurrency → covered by reusing `QueryBenchmarkBase` (no change needed).
- §4.7 time budget → Task 6 (manual run).
- §5.1 test pyramid → Tasks 1-3 cover unit + smoke; Tasks 5-6 cover E2E.
- §5.2 per-commit gates → each task ends with the matching gate.
- §6 FsCache config → Task 4 (`installFsCache()` keeps CH defaults).
- §7 open questions → OQ#1 resolved by Task 4 (`FLAGS_cache_gb=0` short-circuit); OQ#2 by Task 3 smoke gate; OQ#3 deferred.

Type / name consistency check:
- `FsCache::setInstance` / `getInstance` — same names across Tasks 1, 2, 4.
- `cache::fs::FsCache` namespace consistent throughout.
- `QueryCtx::fsCache()` getter name consistent with `cache()` precedent and reused in Task 1 OperatorCtx threading.
- `ConnectorQueryCtx::fsCache()` name parallel to `cache()`; used by Task 2's HiveConnectorUtil branch.
- `FsCacheConfig` field names (`cacheRoot`, `maxBytes`, `alignment`, `maxSegmentSize`, `numBuckets`) verified against `FsCacheConfig.h:27-52`.
- `FsCacheBufferedInput` ctor `(shared_ptr<ReadFile>, memory::MemoryPool&, FsCache*)` verified against `FsCacheBufferedInput.h:45-48`.
- `TpcdsBenchmark::queryBuilder_` / `planDir_` / `pool_` / `queryConfigs_` accessed from `runAb()` — confirm they are protected/public in TpcdsBenchmark.h before writing; if private, either add public accessors or move `runAb` inside the class definition (it's already declared as a member method, so they're accessible).

Placeholder scan: none — all flag defaults, type names, and command strings are concrete.

---

## Execution Handoff

This plan is saved to `docs/superpowers/plans/2026-05-24-fscache-vs-cbi-tpcds.md`.

Per the user's session-level directive, execution proceeds **inline in this session**, one task at a time, with each task driven through the agreed 5-phase rhythm (TDD → reviewer → simplifier → re-review → commit) and auto-advancing to the next task without intermediate user confirmation. Reviewer / simplifier subagent dispatches happen at phase boundaries; user is only interrupted on the explicit failure conditions (3-strikes CRITICAL/HIGH or unexpected exception).
