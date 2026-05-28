# Task D + Task E — retroactive 5-phase audit (2026-05-27)

Tasks D (`9f2c55f9c`) and E (`d8cecd11a`) were landed without the
per-commit 5-phase rhythm (implement → spec review → simplify →
post-simplify review → commit). This note captures the retroactive
phases 2-4 and records the phase-5 verification numbers that should
have been in the original commits.

## Task D — `feat(fscache): add SLRU config fields`

- **Phase 1 (implement)**: `9f2c55f9c` — adds `enableSlru` (bool, default
  false) and `slruProtectedRatio` (double, default 0.6) to
  `FsCacheConfig`. Header-only, no factory wiring yet.
- **Phase 2 (spec review)**: clean.
  - CH citations verified:
    `FileCache_fwd.h:21` is `FILECACHE_DEFAULT_CACHE_POLICY = SLRU` ✓.
    `FileCache_fwd.h:26` is `FILECACHE_DEFAULT_SLRU_RATIO = 0.6` ✓.
    `FileCacheSettings.cpp:43` declares `slru_size_ratio` ✓.
  - Defaults match CH (LRU keeps default false here per the staged
    flip plan; Task F decides the flip).
  - No new public methods; no interface expansion.
- **Phase 3 (simplify)**: skipped — phase 2 had no findings.
- **Phase 4 (post-simplify review)**: skipped — nothing was changed.
- **Phase 5 (UT)**: validated jointly with Task E below
  (D is config-only; nothing executes until E wires the factory).

## Task E — `feat(fscache): wire SLRU into per-bucket policy factory`

- **Phase 1 (implement)**: `d8cecd11a` — passes a lambda
  `PolicyFactory` into `FsCacheMetadata`'s ctor that returns either
  `LruPolicy` (default) or `SlruPolicy(maxBytes/numBuckets,
  slruProtectedRatio)`. Adds smoke test
  `FsCacheTest.enableSlruConstructsAndServesRequests`.
- **Phase 2 (spec review)** — findings:
  - **IMPORTANT (1)**: incorrect CH citation. Commit message and the
    inline comment at `velox/common/caching/fscache/FsCache.cpp:50`
    cited `SLRUFileCachePriority.cpp:81` as the
    promote-on-first-hit / demote-on-protected-overflow logic. Line
    81 is in fact `SLRUFileCachePriority::copy()`
    (`std::make_unique<SLRUFileCachePriority>` body). The actual
    promote logic lives in
    `SLRUFileCachePriority::tryIncreasePriority` starting at
    `SLRUFileCachePriority.cpp:586`, with the move-to-protected at
    `:705` (`protected_queue.add(...)`). Per 八荣八耻 #1 this is a
    fabricated/incorrect citation — fixed in phase 3.
  - **WORTH-KNOWING (1)**: `slruProtectedRatio` is not validated at
    `FsCache` ctor entry. Invalid values are still caught — they
    propagate into `SlruPolicy`'s ctor which does
    `VELOX_CHECK_GT(protectedRatio, 0.0)` and
    `VELOX_CHECK_LT(protectedRatio, 1.0)`. The check fires before any
    I/O. Acceptable, no change.
  - **Lifetime** — the `[&config = std::as_const(config_)]` lambda
    is consumed inside `FsCacheMetadata`'s ctor loop and not stored
    (see `FsCacheMetadata.cpp:39` — `bucket->priority =
    policyFactory()`), so the const-ref to `config_` does not need to
    outlive the metadata member. Safe.
  - **Concurrency** — each bucket calls `policyFactory()` once,
    producing an independent `SlruPolicy` instance per bucket; the
    per-bucket `CachePriorityMutex` lives in
    `FsCacheMetadata::Bucket` and is unaffected by the policy
    choice. Identical concurrency model to the prior `LruPolicy`
    path.
  - **Degenerate `numBuckets = 0`** — guarded by the existing
    `VELOX_CHECK_GT(numBuckets, 0, ...)` at
    `FsCacheMetadata.cpp:27`, which runs before the per-bucket
    factory loop, so the `config.maxBytes / config.numBuckets`
    division inside the lambda is unreachable when `numBuckets == 0`.
  - **Interface / scope** — Task E did NOT modify the
    `EvictionPolicy` interface and did NOT touch `SlruPolicy.{h,cpp}`
    logic. Diff is confined to `FsCache.cpp` (factory) and
    `tests/FsCacheTest.cpp` (smoke test). ✓.
- **Phase 3 (simplify / fix)**: corrected the incorrect CH citation
  in the inline comment at
  `velox/common/caching/fscache/FsCache.cpp:50` from
  `SLRUFileCachePriority.cpp:81 (increment)` to
  `SLRUFileCachePriority::tryIncreasePriority
  (SLRUFileCachePriority.cpp:586, move-to-protected at :705)`.
  Comment-only; no behavioural change. The original commit message
  on `d8cecd11a` retains the wrong citation in the historical record
  — not rewriting history.
- **Phase 4 (post-simplify review)**: clean. The fixed citation is
  verified by direct inspection of the CH source: line 586 is the
  `tryIncreasePriority` signature; the body around lines 639–723
  contains the protected-queue add and demotion handling.
- **Phase 5 (UT)** — full local matrix on
  `cmake-build-relwithdebinfo-gcc13`, post-fix tree:
  - `velox_fscache_test_group0`: **51 / 51 PASSED**.
  - `velox_fscache_test_group1`: **72 / 72 PASSED** (includes the new
    `FsCacheTest.enableSlruConstructsAndServesRequests` smoke test in
    group1).
  - `FsCacheTest.enableSlruConstructsAndServesRequests` standalone:
    **1 / 1 PASSED**.
  - `velox_dwio_common_test`: **100% PASSED** (1/1 ctest entry,
    59.99 s).
  - `velox_fscache_benchmark`: builds clean.

## Critical findings → follow-up commit?

None. The only finding requiring a code change was the
WRONG-CITATION comment in `FsCache.cpp`, fixed in phase 3 above and
landed in the same audit commit as this doc.

## Process note for Task F

Task F (next) must run phases 2-4 **inline** before committing — not
retroactively. The retroactive audit above caught a real (if
low-severity) citation bug that an inline phase-2 review would have
flagged before commit.
