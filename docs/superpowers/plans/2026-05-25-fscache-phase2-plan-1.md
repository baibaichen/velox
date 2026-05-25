# FsCache Phase-2 Plan-1: Locks + Data Structures + FsCacheKey Split

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land spec §4 (lock topology) + §5 (data structures) — per-bucket metadata locks, per-segment try_lock LRU bump, atomic stats, KeyMetadata + LockedKey RAII, PathKey/offset key split, per-bucket LRU + cross-bucket evict polling, and cacheRoot blind-clear on first phase-2 start.

**Architecture:** Phase-2 instantiates the lock topology that phase-1 only sketched: 1024 buckets each carry their own `CacheMetadataGuard`, `EvictionPolicy` instance, and `CachePriorityGuard`; per-key `KeyMetadata` (with its own `KeyGuard`) sits between bucket and segments; `FileSegment` gains an `increasePriorityMutex_` for try_lock LRU bump. `FsCacheKey` splits into `PathKey` (16-hex of path-only hash) + `(offset, size)`, breaking phase-1 cache files — `loadFromDisk` blind-clears `cacheRoot` on first phase-2 start. Stats counters move to `std::atomic<uint64_t>` so hit/miss/eviction fast-paths skip `stateMutex_`. evict() goes round-robin with `try_lock`, N=numBuckets passes, then per-bucket blocking fallback.

**Tech Stack:** C++20, GoogleTest, folly (SpookyHashV2 already in tree), GCC 13 (build dir `cmake-build-relwithdebinfo-gcc13`).

**Spec:** `docs/superpowers/specs/2026-05-25-fscache-phase2-design.md` §4 + §5 (plus §10 R7 for blind-clear motivation). Other spec sections (§6 plan-2, §7 plan-3, §8 plan-4) are out of scope.

**Files modified:**
- `velox/common/caching/fscache/FsCacheKey.h` / `.cpp` — PathKey introduction, hash() PathKey-only
- `velox/common/caching/fscache/FileSegment.h` / `.cpp` — `increasePriorityMutex_` added
- `velox/common/caching/fscache/FsCacheMetadata.h` / `.cpp` — Bucket struct (per-bucket guard + per-bucket priority + `unordered_map<PathKey, KeyMetadataPtr>`); LockedKey API
- `velox/common/caching/fscache/FsCache.h` / `.cpp` — atomic stats, per-bucket bucketOf(segment), evict() round-robin, loadFromDisk blind-clear
- `velox/common/caching/fscache/LruPolicy.cpp` — per-bucket usage (no API change)
- `velox/common/caching/fscache/CMakeLists.txt` — add new files
- `velox/common/caching/fscache/tests/CMakeLists.txt` — add new test files

**Files created:**
- `velox/common/caching/fscache/KeyMetadata.h` / `.cpp`
- `velox/common/caching/fscache/tests/KeyMetadataTest.cpp`

**Tests touched (regression must pass):**
- `tests/FsCacheKeyTest.cpp`
- `tests/FsCacheMetadataTest.cpp`
- `tests/FsCacheConcurrencyTest.cpp`
- `tests/FsCacheRecoveryTest.cpp`
- `tests/FsCacheTest.cpp`
- `tests/EvictionPolicyTest.cpp`
- `tests/FileSegmentTest.cpp`
- `tests/FsCachePersistenceTest.cpp`
- `tests/FsCacheGuardsTest.cpp`
- `tests/FsCacheScaffoldTest.cpp`
- `tests/FsCacheSplitRangeTest.cpp`

**Build dir:** `/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13` (already configured; do NOT create a new one).

**Test target:** `velox_fscache_test` (single grouped binary under `velox/common/caching/fscache/tests/`).

**Commit hygiene:**
- Each task = one commit. Conventional commit subject. Co-Authored-By trailer.
- Never `--amend`, never `--no-verify`, never `--no-gpg-sign`.
- Never `git add .` / `git add -A`. Always list specific files.
- Never touch `velox/common/caching/benchmarks/CacheBackendBenchmark.cpp`.

---

## Task 1: Introduce PathKey type

**Spec:** §4.4, §5.1

**Files:**
- Modify: `velox/common/caching/fscache/FsCacheKey.h`
- Modify: `velox/common/caching/fscache/FsCacheKey.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`

### Background

Phase-1 `FsCacheKey{path, offset, size}` combines all three into a single 64-bit hash. Phase-2 needs to hash by path alone so that `bucketIndex(pathKey) = hash(pathKey) & (numBuckets - 1)` groups all segments of the same file into the same bucket (precondition for the per-key `KeyMetadata` indirection in Task 4–5).

`PathKey` is a trivially-copyable `std::array<char, 16>` storing the 16 lowercase hex chars of `SpookyHashV2(path)`. Equality is `memcmp` on the 16 bytes. `std::hash<PathKey>` returns the first 8 bytes reinterpreted as `uint64_t` (the hex digits are already mixed by SpookyHash, so re-mixing is wasted work).

This task ONLY introduces `PathKey` and its hash specialization. `FsCacheKey` is NOT modified yet (Task 2). Existing tests still pass unchanged.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`:

```cpp
TEST(PathKeyTest, samePathYieldsSameHex) {
  const PathKey a = PathKey::fromPath("/data/file.parquet");
  const PathKey b = PathKey::fromPath("/data/file.parquet");
  EXPECT_EQ(a, b);
  // 16 lowercase hex chars.
  EXPECT_EQ(a.hex().size(), 16);
  for (char c : a.hex()) {
    EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }
}

TEST(PathKeyTest, differentPathsYieldDifferentHex) {
  const PathKey a = PathKey::fromPath("/data/a");
  const PathKey b = PathKey::fromPath("/data/b");
  EXPECT_NE(a, b);
}

TEST(PathKeyTest, stdHashMatchesFirst8Bytes) {
  const PathKey k = PathKey::fromPath("/x");
  uint64_t expected{0};
  std::memcpy(&expected, k.hex().data(), sizeof(expected));
  EXPECT_EQ(std::hash<PathKey>{}(k), static_cast<size_t>(expected));
}
```

Add the includes if not already there:
```cpp
#include <cstring>
#include <functional>
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Expected: COMPILE ERROR (`PathKey` is undefined).

- [ ] **Step 3: Add PathKey to FsCacheKey.h**

Insert into `velox/common/caching/fscache/FsCacheKey.h` (before the existing `FsCacheKey` struct):

```cpp
#include <array>
#include <cstring>
#include <functional>
#include <string_view>

namespace facebook::velox::cache::fs {

/// Path-only hash key. Holds the 16 lowercase hex chars of SpookyHashV2 over
/// the remote file path. Used to bucket and index per-key metadata so that all
/// segments of the same file land in the same bucket and KeyMetadata entry.
struct PathKey {
  std::array<char, 16> chars;

  /// Computes PathKey from a remote file path.
  static PathKey fromPath(std::string_view path);

  /// Returns the 16 hex chars as a string_view (non-owning, valid as long as
  /// the PathKey is alive).
  std::string_view hex() const noexcept {
    return std::string_view(chars.data(), chars.size());
  }

  bool operator==(const PathKey& other) const noexcept {
    return std::memcmp(chars.data(), other.chars.data(), chars.size()) == 0;
  }

  bool operator!=(const PathKey& other) const noexcept {
    return !(*this == other);
  }
};

} // namespace facebook::velox::cache::fs

namespace std {
template <>
struct hash<::facebook::velox::cache::fs::PathKey> {
  size_t operator()(
      const ::facebook::velox::cache::fs::PathKey& key) const noexcept {
    uint64_t first8{0};
    std::memcpy(&first8, key.chars.data(), sizeof(first8));
    return static_cast<size_t>(first8);
  }
};
} // namespace std
```

- [ ] **Step 4: Implement PathKey::fromPath in FsCacheKey.cpp**

Insert into `velox/common/caching/fscache/FsCacheKey.cpp` (before the existing anonymous namespace closes, or in a new helper near the top):

```cpp
PathKey PathKey::fromPath(std::string_view path) {
  uint64_t hash1{0};
  uint64_t hash2{0};
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &hash1, &hash2);
  const uint64_t combined = hash1 ^ hash2;
  PathKey key;
  // 16 lowercase hex chars, zero-padded. fmt::format_to writes exactly 16.
  fmt::format_to(key.chars.data(), "{:016x}", combined);
  return key;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:
```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='PathKeyTest.*'
```

Expected: 3/3 PASS.

- [ ] **Step 6: Regression — all FsCacheKey tests still pass**

Run:
```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheKeyTest.*:PathKeyTest.*'
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCacheKey.h \
        velox/common/caching/fscache/FsCacheKey.cpp \
        velox/common/caching/fscache/tests/FsCacheKeyTest.cpp
git commit -m "$(cat <<'EOF'
feat(fscache): introduce PathKey for path-only hashing

PathKey holds 16 lowercase hex chars of SpookyHashV2 over the remote
file path. It is the bucketing key for the per-bucket / per-key
metadata layout introduced in Tasks 2–5. std::hash<PathKey> returns
the first 8 hex bytes reinterpreted as uint64_t since the hex
rendering is already well-distributed.

FsCacheKey itself is unchanged in this commit; the rewire happens in
Task 2.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Rewire FsCacheKey to use PathKey + path-only hash

**Spec:** §4.4, §5.1, §5.5

**Files:**
- Modify: `velox/common/caching/fscache/FsCacheKey.h`
- Modify: `velox/common/caching/fscache/FsCacheKey.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`

### Background

`FsCacheKey` changes from `{std::string path, uint64_t offset, uint64_t size}` to `{PathKey path, uint64_t offset, uint64_t size}`. `hash()` returns the first 8 bytes of the PathKey's hex (path-only). `fileName()` returns `"<pathkey-hex>.<offset>.<size>"` — identical schema as phase-1, different hash value. The full `std::string` original path is no longer stored on the key; callers that need it (e.g. `FileSegment::download`) must keep it elsewhere.

`FileSegment::key().path` was `std::string`; it becomes `PathKey`. `FileSegment::download` will need the original path passed in separately — verify all current callers and either thread the path explicitly or store it on `FileSegment` as a separate field. (Spec §5.2 explicitly does NOT cache path on KeyMetadata — but `FileSegment` still needs it to fetch from remote. The simplest solution is a `std::string remotePath_` field on `FileSegment`, set at construction.)

After this task, phase-1 cache files on disk become unreadable (different hash). Recovery handling is Task 10 (blind clear). Existing recovery tests will break in this task — they're updated here to use the new hash.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`:

```cpp
TEST(FsCacheKeyTest, hashIsPathOnlyAcrossOffsets) {
  FsCacheKey a{PathKey::fromPath("/data/x"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("/data/x"), 8192, 4096};
  EXPECT_EQ(a.hash(), b.hash())
      << "Phase-2: hash() depends on path only, not (offset, size)";
}

TEST(FsCacheKeyTest, hashDiffersByPath) {
  FsCacheKey a{PathKey::fromPath("/data/x"), 0, 4096};
  FsCacheKey b{PathKey::fromPath("/data/y"), 0, 4096};
  EXPECT_NE(a.hash(), b.hash());
}

TEST(FsCacheKeyTest, fileNameSchemaUnchanged) {
  FsCacheKey key{PathKey::fromPath("/data/x"), 1234, 4096};
  const std::string name = key.fileName();
  // "<16-hex>.<offset>.<size>"
  EXPECT_EQ(name.size(), 16 + 1 + 4 + 1 + 4);
  EXPECT_EQ(name[16], '.');
  EXPECT_EQ(name.substr(17, 4), "1234");
  EXPECT_EQ(name.substr(22), "4096");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Expected: COMPILE ERROR (FsCacheKey ctor still takes `std::string`, not `PathKey`).

- [ ] **Step 3: Rewrite FsCacheKey in FsCacheKey.h**

Replace the existing `FsCacheKey` struct in `velox/common/caching/fscache/FsCacheKey.h`:

```cpp
/// Identifies a single cache segment by PathKey (path-only hash) + offset +
/// size. Phase-2 splits the phase-1 (path, offset, size) composite key so that
/// all segments of the same file share the same hash bucket, enabling
/// per-key metadata indirection.
struct FsCacheKey {
  PathKey path;
  uint64_t offset{0};
  uint64_t size{0};

  bool operator==(const FsCacheKey& other) const noexcept {
    return offset == other.offset && size == other.size && path == other.path;
  }

  bool operator!=(const FsCacheKey& other) const noexcept {
    return !(*this == other);
  }

  /// Returns a 64-bit hash derived from PathKey only (NOT offset/size).
  /// Two FsCacheKeys with the same path but different offsets hash equally.
  uint64_t hash() const noexcept;

  /// Returns "<pathkey-hex>.<offset>.<size>". Schema matches phase-1 but the
  /// hash value differs — phase-1 cache files cannot be re-keyed by phase-2.
  std::string fileName() const;
};
```

`FsCacheKeyHash` stays as-is.

- [ ] **Step 4: Rewrite FsCacheKey.cpp**

Replace `combinedHash`, `hash()`, and `fileName()` in `velox/common/caching/fscache/FsCacheKey.cpp`:

```cpp
uint64_t FsCacheKey::hash() const noexcept {
  uint64_t first8{0};
  std::memcpy(&first8, path.chars.data(), sizeof(first8));
  return first8;
}

std::string FsCacheKey::fileName() const {
  return fmt::format(
      "{}.{}.{}", std::string_view{path.chars.data(), path.chars.size()},
      offset, size);
}
```

Remove the now-unused `combinedHash` helper from the anonymous namespace.

- [ ] **Step 5: Add remotePath_ to FileSegment**

`FileSegment` previously got the remote path from `key_.path`. Now `key_.path` is a `PathKey`. Add `std::string remotePath_` and accept it at construction.

Modify `velox/common/caching/fscache/FileSegment.h`:

```cpp
/// Constructs a fresh kEmpty segment for the given key. remotePath is the
/// original (string) path used to fetch missing bytes from remote; it is
/// stored alongside the (path-only-hashed) key because FsCacheKey itself no
/// longer carries the human-readable path.
FileSegment(FsCacheKey key, std::string remotePath)
    : key_{std::move(key)}, remotePath_{std::move(remotePath)} {}

/// Returns the remote file path string for download().
const std::string& remotePath() const {
  return remotePath_;
}
```

Add private field at the bottom of the private section:
```cpp
std::string remotePath_;
```

Modify `velox/common/caching/fscache/FileSegment.cpp` `download()` to use `remotePath_` if it previously read `key_.path`. (If it already accepts `remote` as a `ReadFile&` arg, only the `LOG`/error messages need updating to use `remotePath_` where they currently use `key_.path`.)

- [ ] **Step 6: Update FsCache.cpp call sites**

`FsCache::lookupOrCreate` constructs `FsCacheKey` and `FileSegment`. The string path is available there (parameter). Modify:

```cpp
// Before:
//   FsCacheKey key{path, segOffset, effectiveSize};
//   auto segment = std::make_shared<FileSegment>(key);
// After:
FsCacheKey key{PathKey::fromPath(path), segOffset, effectiveSize};
auto segment = std::make_shared<FileSegment>(key, path);
```

Update the `lookupOrCreate(FsCacheKey, ReadFile&)` overload signature if it stores the segment to also accept and forward the string path. The simplest is to thread `path` through and construct `FileSegment(key, path)` inside.

Search for other `std::make_shared<FileSegment>(` call sites:
```bash
grep -rn "make_shared<FileSegment>\|make_unique<FileSegment>\|new FileSegment" \
  velox/common/caching/fscache/
```
Update each.

Also update any `FsCacheKey{path, ...}` constructions that used `std::string`:
```bash
grep -rn "FsCacheKey{" velox/common/caching/fscache/
```
Wrap the path with `PathKey::fromPath(...)`.

- [ ] **Step 7: Update error messages in FsCache.cpp**

In `lookupOrCreate` waiter path:
```cpp
// Before:
//   VELOX_USER_FAIL(
//       "FsCache concurrent download failed for path={} offset={} size={}",
//       key.path, key.offset, key.size);
// After:
VELOX_USER_FAIL(
    "FsCache concurrent download failed for path={} offset={} size={}",
    segment->remotePath(), key.offset, key.size);
```

In `evict()` warning log (search for `key.path`):
```cpp
// Replace `<< key.path <<` with `<< victim->remotePath() <<`.
```

- [ ] **Step 8: Update existing FsCacheKey tests**

In `velox/common/caching/fscache/tests/FsCacheKeyTest.cpp`, every place that constructs `FsCacheKey{"some/path", ...}` becomes `FsCacheKey{PathKey::fromPath("some/path"), ...}`. Grep first:

```bash
grep -n "FsCacheKey{" velox/common/caching/fscache/tests/FsCacheKeyTest.cpp
```

For each match, wrap the string with `PathKey::fromPath(...)`.

If a phase-1 test asserts the old `combinedHash` value, update it to the new path-only hash. Compute the expected value by running:
```cpp
EXPECT_EQ(
    FsCacheKey{PathKey::fromPath("/data/x"), 0, 4096}.hash(),
    /* fill in after first run */);
```

The simplest pattern: replace any expected-hash literal with a recomputed value, OR change the test to assert only the structural property (e.g. "same path → same hash regardless of offset"). Prefer the structural form.

- [ ] **Step 9: Run all fscache tests to find regressions**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
```

Expected: most pass. Recovery tests that pre-write phase-1-hashed files to disk and expect them to survive will FAIL — that's expected (those tests are updated in Task 10). Note which tests fail; they should all be recovery-related.

If non-recovery tests fail, fix them: the only legitimate failure is a test that hard-codes a phase-1 hash value. Update such tests to use structural assertions or recomputed values.

- [ ] **Step 10: Commit**

```bash
git add velox/common/caching/fscache/FsCacheKey.h \
        velox/common/caching/fscache/FsCacheKey.cpp \
        velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/FileSegment.cpp \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheKeyTest.cpp
git commit -m "$(cat <<'EOF'
refactor(fscache): rewire FsCacheKey to PathKey + path-only hash

FsCacheKey now holds (PathKey, offset, size) where PathKey is the 16-hex
SpookyHashV2 of the remote path only. hash() returns the first 8 hex
bytes; fileName() schema "<16-hex>.<offset>.<size>" is preserved so
on-disk layout is unchanged, but hash values differ from phase-1.

FileSegment gains a remotePath_ field (passed at construction) because
the key no longer carries the string path; download() and error
messages read remotePath() instead of key_.path.

Phase-1 cache files become unreadable — recovery handling is Task 10.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: FileSegment::increasePriorityMutex_

**Spec:** §4.2 (last bullet)

**Files:**
- Modify: `velox/common/caching/fscache/FileSegment.h`
- Modify: `velox/common/caching/fscache/tests/FileSegmentTest.cpp`

### Background

Phase-2 LRU bump on hit goes through `try_lock` on a per-segment `std::mutex` so concurrent hits on the same segment collapse to one LRU splice (CH `FileSegment.cpp:1196-1223` `increasePriority` pattern). This task introduces ONLY the mutex field — the bump logic itself is wired in Task 7.

Plain `std::mutex` (not `RankedMutex`) because this mutex is held strictly outside the priority/state/metadata acquisitions inside the recordHit fast-path; no rank ordering applies. The bump under this mutex acquires `CachePriorityGuard` next, so the lock-order chain is `increasePriorityMutex_ (unranked) → CachePriorityGuard (rank 0)`. The `try_lock` callers do nothing on failure (LRU bump is best-effort), so blocking semantics don't matter here.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FileSegmentTest.cpp`:

```cpp
TEST(FileSegmentTest, increasePriorityMutexIsAccessible) {
  FsCacheKey key{PathKey::fromPath("/x"), 0, 4096};
  FileSegment seg{key, "/x"};
  std::unique_lock<std::mutex> lk{seg.increasePriorityMutex_, std::try_to_lock};
  EXPECT_TRUE(lk.owns_lock());
}

TEST(FileSegmentTest, increasePriorityMutexExcludesConcurrentTryLock) {
  FsCacheKey key{PathKey::fromPath("/x"), 0, 4096};
  FileSegment seg{key, "/x"};
  std::unique_lock<std::mutex> first{seg.increasePriorityMutex_};
  std::unique_lock<std::mutex> second{
      seg.increasePriorityMutex_, std::try_to_lock};
  EXPECT_FALSE(second.owns_lock());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Expected: COMPILE ERROR (`increasePriorityMutex_` undefined).

- [ ] **Step 3: Add increasePriorityMutex_ to FileSegment.h**

Insert into `velox/common/caching/fscache/FileSegment.h`, in the existing `public:` section near `mutable FileSegmentMutex mutex_;`:

```cpp
/// Collapses concurrent LRU bumps on the same segment. recordHit() callers
/// take this mutex with try_to_lock; losers skip the bump (LRU bump is
/// best-effort, and one bump per burst of concurrent hits is enough to
/// move the segment toward MRU). Plain std::mutex (not RankedMutex)
/// because it is acquired strictly outside any cache-level mutex chain
/// and held only across a single onHit() call.
mutable std::mutex increasePriorityMutex_;
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FileSegmentTest.*'
```

Expected: all PASS, including the 2 new cases.

- [ ] **Step 5: Commit**

```bash
git add velox/common/caching/fscache/FileSegment.h \
        velox/common/caching/fscache/tests/FileSegmentTest.cpp
git commit -m "$(cat <<'EOF'
feat(fscache): add FileSegment::increasePriorityMutex_

Per-segment std::mutex used in Task 7 to collapse concurrent LRU bumps
on the same segment under try_to_lock — mirrors CH's increase_priority
pattern (FileSegment.cpp:1196-1223). Unranked because it sits outside
the cache-level lock chain and is acquired in isolation.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: KeyMetadata + LockedKey RAII

**Spec:** §4.3, §5.2

**Files:**
- Create: `velox/common/caching/fscache/KeyMetadata.h`
- Create: `velox/common/caching/fscache/KeyMetadata.cpp`
- Create: `velox/common/caching/fscache/tests/KeyMetadataTest.cpp`
- Modify: `velox/common/caching/fscache/CMakeLists.txt`
- Modify: `velox/common/caching/fscache/tests/CMakeLists.txt`

### Background

`KeyMetadata` holds all segments of a given PathKey, keyed by offset (so range queries on the same path don't need bucket-wide iteration). It owns a `KeyGuard` (rank 3, from `FsCacheGuards.h`). Access is mediated by a `LockedKey` RAII: callers obtain a `LockedKey` from `KeyMetadata::lock()` and hold it for the duration of any read/write to `segments` / `numSegments`. While `LockedKey` is alive, callers MAY touch any field; once it goes out of scope, callers MUST NOT.

The spec writes the API as `KeyGuard::Lock lock() const` returning a "lock" object, but `RankedGuard` (the actual `KeyGuard`) is non-movable and works as a destructor-locks-the-mutex RAII. We introduce a `LockedKey` wrapper that owns a `KeyGuard` and exposes `KeyMetadata*` access. Pattern matches CH `Metadata.h:LockedKey`.

- [ ] **Step 1: Write the failing test**

Create `velox/common/caching/fscache/tests/KeyMetadataTest.cpp`:

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

#include "velox/common/caching/fscache/KeyMetadata.h"

#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

namespace facebook::velox::cache::fs::test {

namespace {
FileSegmentPtr makeSegment(std::string_view path, uint64_t offset, uint64_t size) {
  FsCacheKey key{PathKey::fromPath(path), offset, size};
  return std::make_shared<FileSegment>(key, std::string{path});
}
} // namespace

TEST(KeyMetadataTest, lockedKeyOwnsMutex) {
  KeyMetadata meta;
  auto locked = meta.lock();
  EXPECT_NE(locked.get(), nullptr);
}

TEST(KeyMetadataTest, segmentsMapStoresByOffset) {
  KeyMetadata meta;
  auto locked = meta.lock();
  locked->segments.emplace(0, makeSegment("/x", 0, 4096));
  locked->segments.emplace(4096, makeSegment("/x", 4096, 4096));
  ++locked->numSegments;
  ++locked->numSegments;
  EXPECT_EQ(locked->segments.size(), 2u);
  EXPECT_EQ(locked->numSegments, 2u);
  EXPECT_EQ(locked->segments.begin()->first, 0u);
  EXPECT_EQ(std::next(locked->segments.begin())->first, 4096u);
}

TEST(KeyMetadataTest, secondLockBlocksUntilFirstReleased) {
  KeyMetadata meta;
  std::atomic<bool> secondAcquired{false};
  auto first = meta.lock();
  std::thread t{[&] {
    auto second = meta.lock();
    secondAcquired = true;
  }};
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(secondAcquired)
      << "Second lock must block while first is held";
  // Release first by ending its scope.
  first = LockedKey{};  // assign empty to drop the guard
  t.join();
  EXPECT_TRUE(secondAcquired);
}

} // namespace facebook::velox::cache::fs::test
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Expected: COMPILE ERROR (`KeyMetadata` and `LockedKey` undefined, `KeyMetadata.h` missing).

- [ ] **Step 3: Create KeyMetadata.h**

Create `velox/common/caching/fscache/KeyMetadata.h`:

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

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

namespace facebook::velox::cache::fs {

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;

class KeyMetadata;

/// RAII handle that owns a KeyGuard on a specific KeyMetadata. While alive,
/// the holder may read and write the KeyMetadata's segments / numSegments.
/// Non-copyable; movable. An empty (default-constructed) LockedKey holds no
/// mutex and dereferences to nullptr — used as a sentinel when transferring
/// ownership.
class LockedKey {
 public:
  LockedKey() = default;
  LockedKey(LockedKey&& other) noexcept;
  LockedKey& operator=(LockedKey&& other) noexcept;
  ~LockedKey();

  LockedKey(const LockedKey&) = delete;
  LockedKey& operator=(const LockedKey&) = delete;

  KeyMetadata* get() const noexcept {
    return meta_;
  }

  KeyMetadata* operator->() const noexcept {
    return meta_;
  }

 private:
  friend class KeyMetadata;
  LockedKey(KeyMetadata* meta, KeyMutex& mutex);

  KeyMetadata* meta_{nullptr};
  KeyMutex* mutex_{nullptr};
};

/// Per-PathKey container: all FileSegments of one remote file, indexed by
/// offset. Phase-2 introduces this indirection so the per-bucket
/// CacheMetadataGuard can be released as soon as we obtain a KeyMetadataPtr,
/// and finer-grained operations on a single file's segments serialize on the
/// per-key KeyGuard instead of contending on the bucket.
class KeyMetadata {
 public:
  KeyMetadata() = default;

  /// Acquires the per-key mutex and returns a LockedKey RAII for accessing
  /// segments / numSegments. Blocks if another thread holds the mutex.
  LockedKey lock();

  /// Segments belonging to this PathKey, keyed by FsCacheKey::offset. size is
  /// stored on each FileSegment; KeyMetadata does not duplicate it. Access is
  /// only legal while a LockedKey for this object is alive.
  std::map<uint64_t, FileSegmentPtr> segments;

  /// Cached count of segments (== segments.size()). Maintained by callers
  /// under the LockedKey. Useful to read without iterating segments.
  size_t numSegments{0};

 private:
  mutable KeyMutex mutex_;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Create KeyMetadata.cpp**

Create `velox/common/caching/fscache/KeyMetadata.cpp`:

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

#include "velox/common/caching/fscache/KeyMetadata.h"

namespace facebook::velox::cache::fs {

LockedKey::LockedKey(KeyMetadata* meta, KeyMutex& mutex)
    : meta_{meta}, mutex_{&mutex} {
  mutex_->lock();
}

LockedKey::LockedKey(LockedKey&& other) noexcept
    : meta_{other.meta_}, mutex_{other.mutex_} {
  other.meta_ = nullptr;
  other.mutex_ = nullptr;
}

LockedKey& LockedKey::operator=(LockedKey&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (mutex_ != nullptr) {
    mutex_->unlock();
  }
  meta_ = other.meta_;
  mutex_ = other.mutex_;
  other.meta_ = nullptr;
  other.mutex_ = nullptr;
  return *this;
}

LockedKey::~LockedKey() {
  if (mutex_ != nullptr) {
    mutex_->unlock();
  }
}

LockedKey KeyMetadata::lock() {
  return LockedKey{this, mutex_};
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Register new sources in CMake**

Read the existing source list in `velox/common/caching/fscache/CMakeLists.txt`:
```bash
grep -n "FsCacheKey\|FsCacheMetadata\|FileSegment" \
  velox/common/caching/fscache/CMakeLists.txt
```

Add `KeyMetadata.cpp` to whichever `add_library` / `target_sources` block already lists `FsCacheKey.cpp`. (No header registration needed — pure includes resolve via project include path.)

Similarly for the test: read `velox/common/caching/fscache/tests/CMakeLists.txt` and add `KeyMetadataTest.cpp` to the `SOURCES` list of `velox_fscache_test`.

- [ ] **Step 6: Run tests to verify they pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='KeyMetadataTest.*'
```

Expected: 3/3 PASS.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/KeyMetadata.h \
        velox/common/caching/fscache/KeyMetadata.cpp \
        velox/common/caching/fscache/tests/KeyMetadataTest.cpp \
        velox/common/caching/fscache/CMakeLists.txt \
        velox/common/caching/fscache/tests/CMakeLists.txt
git commit -m "$(cat <<'EOF'
feat(fscache): KeyMetadata + LockedKey RAII

Per-PathKey container holding all FileSegments for one remote file,
indexed by offset. KeyGuard (rank 3) is owned by KeyMetadata; access
goes through LockedKey RAII so callers can release the bucket-level
CacheMetadataGuard as soon as they obtain the KeyMetadataPtr, then
serialize finer-grained ops on the per-key mutex (matches CH
Metadata.h:LockedKey pattern).

Wire-up into FsCacheMetadata buckets is Task 5.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: FsCacheMetadata buckets — per-bucket CacheMetadataGuard + KeyMetadata indirection

**Spec:** §4.1, §4.3, §5.3

**Files:**
- Modify: `velox/common/caching/fscache/FsCacheMetadata.h`
- Modify: `velox/common/caching/fscache/FsCacheMetadata.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp`

### Background

Phase-1 `FsCacheMetadata`: single `CacheMetadataMutex mutex_` + `std::vector<std::unordered_map<FsCacheKey, FileSegmentPtr, ...>>` (one map per bucket but a shared lock).

Phase-2 `FsCacheMetadata::Bucket` holds:
- `mutable CacheMetadataMutex guard` — per-bucket lock (rank 2)
- `std::unordered_map<PathKey, KeyMetadataPtr> keys` — bucket-local index

(Per-bucket `EvictionPolicy` + `CachePriorityMutex` are added to `Bucket` in Task 8.)

Public surface stays the same in shape — `lookup(key)`, `insert(segment)`, `erase(key)`, `snapshot()` — but the internals now go through `KeyMetadata`:

- `lookup(key)`: lock bucket → find KeyMetadataPtr → release bucket → lock key → find segment by offset → return.
- `insert(segment)`: lock bucket → find/insert KeyMetadataPtr → release bucket → lock key → `segments.emplace(offset, segment)`; return false if a segment already exists at that offset.
- `erase(key)`: lock bucket → find KeyMetadataPtr (or return false) → release bucket → lock key → `segments.erase(offset)`; if `segments.empty()` after erase, re-lock bucket and erase the KeyMetadataPtr from `keys`.
- `snapshot()`: lock each bucket sequentially, for each KeyMetadataPtr lock the key and copy out the segments.

This task does NOT yet wire per-bucket EvictionPolicy or per-bucket priorityMutex into `Bucket` — those land in Task 8 (LRU migration). For now `FsCache` still holds the single global `policy_` and `priorityMutex_`.

The `bucketIndex(key)` switches from hashing the full `FsCacheKey` to hashing the `PathKey` only: `std::hash<PathKey>{}(key.path) & bucketMask_`.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp`:

```cpp
TEST(FsCacheMetadataTest, samePathSegmentsCoLocate) {
  FsCacheMetadata md{16};
  auto seg0 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x");
  auto seg1 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 4096, 4096}, "/x");
  EXPECT_TRUE(md.insert(seg0));
  EXPECT_TRUE(md.insert(seg1));
  // Both segments resolve from the same KeyMetadata, so lookup hits.
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4096}), seg0);
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 4096, 4096}), seg1);
}

TEST(FsCacheMetadataTest, eraseLastSegmentDropsKeyMetadata) {
  FsCacheMetadata md{16};
  auto seg = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x");
  md.insert(seg);
  EXPECT_TRUE(md.erase(FsCacheKey{PathKey::fromPath("/x"), 0, 4096}));
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4096}), nullptr);
  // Re-insert under the same path must succeed (KeyMetadata recreated).
  auto seg2 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x");
  EXPECT_TRUE(md.insert(seg2));
}

TEST(FsCacheMetadataTest, insertDuplicateOffsetFails) {
  FsCacheMetadata md{16};
  auto seg0 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x");
  auto seg1 = std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x");
  EXPECT_TRUE(md.insert(seg0));
  EXPECT_FALSE(md.insert(seg1));
  // The original wins.
  EXPECT_EQ(md.lookup({PathKey::fromPath("/x"), 0, 4096}), seg0);
}

TEST(FsCacheMetadataTest, snapshotReturnsAllSegmentsAcrossKeys) {
  FsCacheMetadata md{16};
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 0, 4096}, "/x"));
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/y"), 0, 4096}, "/y"));
  md.insert(std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath("/x"), 4096, 4096}, "/x"));
  const auto all = md.snapshot();
  EXPECT_EQ(all.size(), 3u);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Expected: COMPILE or RUNTIME failure (depending on how phase-1 `insert` is currently implemented; either way, the test exercises new behavior that doesn't exist yet).

- [ ] **Step 3: Rewrite FsCacheMetadata.h**

Replace `velox/common/caching/fscache/FsCacheMetadata.h`:

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

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheGuards.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/caching/fscache/KeyMetadata.h"

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

namespace facebook::velox::cache::fs {

/// Two-level index: numBuckets buckets each holding an unordered_map keyed by
/// PathKey and pointing at KeyMetadata. Per-bucket CacheMetadataGuard
/// (rank 2) and per-key KeyGuard (rank 3) together replace phase-1's single
/// global metadata mutex.
class FsCacheMetadata {
 public:
  /// Constructs with numBuckets buckets. numBuckets must be a power of two so
  /// the hash->bucket modulo folds to a single bitwise AND in bucketIndex().
  explicit FsCacheMetadata(size_t numBuckets);

  /// Inserts the segment into its KeyMetadata. Returns false if a segment is
  /// already present at the same (path, offset); the caller's segment is then
  /// dropped and the existing entry is preserved.
  bool insert(FileSegmentPtr segment);

  /// Returns the segment for key, or nullptr if not present.
  FileSegmentPtr lookup(const FsCacheKey& key) const;

  /// Removes the segment for key. Returns true if it existed. If the parent
  /// KeyMetadata becomes empty as a result, the KeyMetadata entry is also
  /// removed from the bucket.
  bool erase(const FsCacheKey& key);

  /// Returns all segments currently held across all buckets / keys. Order is
  /// unspecified. Used by recovery and tests; not on the hot path.
  std::vector<FileSegmentPtr> snapshot() const;

  /// Number of buckets. Const after construction.
  size_t numBuckets() const {
    return buckets_.size();
  }

 private:
  struct Bucket {
    mutable CacheMetadataMutex guard;
    std::unordered_map<PathKey, KeyMetadataPtr> keys;
  };

  size_t bucketIndex(const PathKey& path) const {
    return static_cast<size_t>(std::hash<PathKey>{}(path) & bucketMask_);
  }

  const size_t bucketMask_;
  // unique_ptr<Bucket> because Bucket holds a mutex (non-movable) and the
  // vector needs stable addresses across initialization.
  std::vector<std::unique_ptr<Bucket>> buckets_;
};

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 4: Rewrite FsCacheMetadata.cpp**

Replace `velox/common/caching/fscache/FsCacheMetadata.cpp`:

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

#include "velox/common/caching/fscache/FsCacheMetadata.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::cache::fs {

namespace {

KeyMetadataPtr getOrInsertKey(
    FsCacheMetadata::Bucket& bucket,
    const PathKey& path) {
  auto it = bucket.keys.find(path);
  if (it != bucket.keys.end()) {
    return it->second;
  }
  auto fresh = std::make_shared<KeyMetadata>();
  bucket.keys.emplace(path, fresh);
  return fresh;
}

} // namespace

FsCacheMetadata::FsCacheMetadata(size_t numBuckets)
    : bucketMask_{numBuckets - 1} {
  VELOX_CHECK_GT(numBuckets, 0);
  VELOX_CHECK_EQ(
      numBuckets & (numBuckets - 1),
      0,
      "numBuckets must be power of two: {}",
      numBuckets);
  buckets_.reserve(numBuckets);
  for (size_t i = 0; i < numBuckets; ++i) {
    buckets_.emplace_back(std::make_unique<Bucket>());
  }
}

bool FsCacheMetadata::insert(FileSegmentPtr segment) {
  VELOX_CHECK_NOT_NULL(segment);
  const auto& key = segment->key();
  auto& bucket = *buckets_[bucketIndex(key.path)];
  KeyMetadataPtr keyMeta;
  {
    CacheMetadataGuard bg{bucket.guard};
    keyMeta = getOrInsertKey(bucket, key.path);
  }
  auto locked = keyMeta->lock();
  auto [it, inserted] = locked->segments.emplace(key.offset, segment);
  if (!inserted) {
    return false;
  }
  ++locked->numSegments;
  return true;
}

FileSegmentPtr FsCacheMetadata::lookup(const FsCacheKey& key) const {
  auto& bucket = *buckets_[bucketIndex(key.path)];
  KeyMetadataPtr keyMeta;
  {
    CacheMetadataGuard bg{bucket.guard};
    auto it = bucket.keys.find(key.path);
    if (it == bucket.keys.end()) {
      return nullptr;
    }
    keyMeta = it->second;
  }
  auto locked = keyMeta->lock();
  auto sit = locked->segments.find(key.offset);
  if (sit == locked->segments.end()) {
    return nullptr;
  }
  return sit->second;
}

bool FsCacheMetadata::erase(const FsCacheKey& key) {
  auto& bucket = *buckets_[bucketIndex(key.path)];
  KeyMetadataPtr keyMeta;
  {
    CacheMetadataGuard bg{bucket.guard};
    auto it = bucket.keys.find(key.path);
    if (it == bucket.keys.end()) {
      return false;
    }
    keyMeta = it->second;
  }
  bool nowEmpty = false;
  {
    auto locked = keyMeta->lock();
    auto erased = locked->segments.erase(key.offset);
    if (erased == 0) {
      return false;
    }
    --locked->numSegments;
    nowEmpty = locked->segments.empty();
  }
  if (nowEmpty) {
    // Re-acquire bucket lock to drop the empty KeyMetadata. Re-check the
    // segments map under the key lock again in case a concurrent insert
    // populated it between our drop and re-lock — if so, leave the
    // KeyMetadata in place.
    CacheMetadataGuard bg{bucket.guard};
    auto it = bucket.keys.find(key.path);
    if (it != bucket.keys.end()) {
      auto recheck = it->second->lock();
      if (recheck->segments.empty()) {
        bucket.keys.erase(it);
      }
    }
  }
  return true;
}

std::vector<FileSegmentPtr> FsCacheMetadata::snapshot() const {
  std::vector<FileSegmentPtr> out;
  for (const auto& bucketPtr : buckets_) {
    auto& bucket = *bucketPtr;
    std::vector<KeyMetadataPtr> keyMetas;
    {
      CacheMetadataGuard bg{bucket.guard};
      keyMetas.reserve(bucket.keys.size());
      for (const auto& [_, ptr] : bucket.keys) {
        keyMetas.push_back(ptr);
      }
    }
    for (auto& km : keyMetas) {
      auto locked = km->lock();
      for (const auto& [_, seg] : locked->segments) {
        out.push_back(seg);
      }
    }
  }
  return out;
}

} // namespace facebook::velox::cache::fs
```

- [ ] **Step 5: Expose Bucket struct for friend tests if needed**

The test in Step 1 does NOT need access to `Bucket` directly — it goes through public API. If a phase-1 test pokes internals via friendship or templates, find them:

```bash
grep -rn "FsCacheMetadata::\|friend.*FsCacheMetadata" velox/common/caching/fscache/tests/
```

If any test reaches into the old `buckets_` (e.g. iterating), rewrite to use `snapshot()` instead. Phase-2 forbids `friend` (per project CLAUDE.md) so do NOT add friend declarations.

- [ ] **Step 6: Update FsCache.cpp to wrap path with PathKey at bucketIndex points**

Search FsCache.cpp for any explicit `bucketIndex` call or hash-of-key usage outside `FsCacheMetadata`. The public `FsCacheMetadata` API now takes `FsCacheKey` (which already holds a `PathKey`), so most callers need no change. But if `FsCache::lookupOrCreate` or `evict` previously constructed an `FsCacheKey` with a `std::string`, those constructions already moved to `PathKey::fromPath(path)` in Task 2.

Double-check by building:

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16
```

Fix any compile errors that surface. The most likely culprit is `FsCacheMetadata` callers that relied on `Bucket` being publicly visible — change them to use the public API.

- [ ] **Step 7: Run tests to verify they pass**

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheMetadataTest.*'
```

Expected: all PASS, including the 4 new cases.

Then run the broader regression:

```bash
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
```

Expected: pass except for recovery tests that pre-write disk files (deferred to Task 10).

- [ ] **Step 8: Commit**

```bash
git add velox/common/caching/fscache/FsCacheMetadata.h \
        velox/common/caching/fscache/FsCacheMetadata.cpp \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheMetadataTest.cpp
git commit -m "$(cat <<'EOF'
refactor(fscache): per-bucket metadata mutex + KeyMetadata indirection

FsCacheMetadata::Bucket now carries its own CacheMetadataGuard and an
unordered_map<PathKey, KeyMetadataPtr>; the 1024 buckets no longer
share a single global mutex. lookup/insert/erase release the bucket
lock as soon as they hand off to the per-key KeyGuard.

erase drops the KeyMetadata entry when the segments map becomes empty,
re-locking the bucket and re-checking under the key lock so a
concurrent insert at the same path doesn't lose its KeyMetadata.

snapshot() locks each bucket sequentially and each KeyMetadata in
turn — used by recovery and tests, off the hot path.

Per-bucket EvictionPolicy / CachePriorityMutex land in Task 8.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Atomic FsCacheStats — fast-path skips stateMutex

**Spec:** §4.2 (bullets 1–4)

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.h`
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheTest.cpp`

### Background

Phase-1 `FsCacheStats` fields are bare `uint64_t` protected by `stateMutex_`. Every `recordHit` / `recordMiss` / `evict()` `bytesOnDisk` update takes `CacheStateGuard` (`FsCache.cpp:296-313, 285-288, 241-242, 291-294`). At 16-thread hit storms this serializes the entire hit fast-path on one mutex.

Phase-2 changes:
1. Fields → `std::atomic<uint64_t>`. `bytesOnDisk` uses `fetch_add` on miss and `fetch_sub` on evict.
2. `recordHit` / `recordMiss` no longer take `CacheStateGuard` for counter updates — just `fetch_add(1, std::memory_order_relaxed)`.
3. `stats()` returns a snapshot built from per-field relaxed loads. Per-field atomic; across-field consistency is best-effort (observability use case accepts weakly consistent snapshots).
4. `stateMutex_` field **stays** — used only at the `evict()` reserve→evict decision boundary (read `bytesOnDisk` + decide whether to drain). Whether to keep that single critical section or also drop it is left to the simplifier (both behaviors are correct; the spec §4.2 bullet 4 explicitly defers).

The `FsCacheStats` struct that `stats()` *returns* is still POD `uint64_t` — the atomicity is internal. So tests that read `stats().hits` continue to work; only the storage and update paths change.

This task does NOT change `recordHit`'s LRU-bump portion (that's Task 7). It only swaps the counter accounting.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheTest.cpp`:

```cpp
TEST(FsCacheTest, statsCountersIncrementAcrossThreadsWithoutLoss) {
  auto tempDir = ::facebook::velox::common::testutil::TempDirectoryPath::create();
  FsCacheConfig config;
  config.cacheRoot = tempDir->getPath() + "/cache";
  config.maxBytes = 256UL * 1'024 * 1'024;
  std::filesystem::create_directories(config.cacheRoot);

  const auto remotePath = tempDir->getPath() + "/big.bin";
  {
    std::ofstream out{remotePath, std::ios::binary};
    const std::string blob(16UL * 1'024 * 1'024, 'x');
    out.write(blob.data(), blob.size());
  }

  FsCache cache{config};
  constexpr int kThreads = 16;
  constexpr int kPerThread = 200;
  constexpr uint64_t kSegmentSize = 4UL * 1'024 * 1'024;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      LocalReadFile remote{remotePath};
      for (int i = 0; i < kPerThread; ++i) {
        (void)cache.getOrSet(remotePath, 0, kSegmentSize, remote);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  const auto s = cache.stats();
  // First call across threads races for the single miss; the remaining
  // kThreads * kPerThread - 1 access the kDownloaded segment as hits.
  EXPECT_EQ(s.misses, 1u);
  EXPECT_EQ(s.hits, kThreads * kPerThread - 1u);
}
```

- [ ] **Step 2: Run test to verify it passes (phase-1 behavior already correct under lock)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheTest.statsCountersIncrementAcrossThreadsWithoutLoss'
```

Expected: PASS (phase-1 mutex-protected counters already satisfy the count). The test is a regression guard for Task 6's refactor — once stats go atomic, we re-run it to ensure no counts are lost.

- [ ] **Step 3: Convert FsCacheStats storage to atomic**

In `velox/common/caching/fscache/FsCache.h`, the public `FsCacheStats` struct stays bare `uint64_t` (that's the snapshot returned by `stats()`). Add a separate internal struct for the live counters. Insert at the top of the `private:` section of `FsCache`:

```cpp
// Live atomic counters. stats() composes an FsCacheStats POD snapshot from
// per-field relaxed loads. Per-field atomicity is enough for the
// observability use case; cross-field consistency between hits/misses/bytes
// is best-effort.
struct AtomicCounters {
  std::atomic<uint64_t> hits{0};
  std::atomic<uint64_t> misses{0};
  std::atomic<uint64_t> evictions{0};
  std::atomic<uint64_t> bytesOnDisk{0};
};
```

Replace the existing `FsCacheStats stats_;` member with:
```cpp
AtomicCounters counters_;
```

Add `#include <atomic>` to `FsCache.h` if not already present.

- [ ] **Step 4: Rewrite recordHit, recordMiss, stats, evict counter touches**

Replace `recordHit` in `velox/common/caching/fscache/FsCache.cpp`:

```cpp
void FsCache::recordHit(FileSegment* segment) {
  {
    CachePriorityGuard guard{priorityMutex_};
    policy_->onHit(segment);
  }
  counters_.hits.fetch_add(1, std::memory_order_relaxed);
}
```

Replace `recordMiss`:

```cpp
void FsCache::recordMiss(FileSegment* segment, uint64_t segmentSize) {
  {
    CachePriorityGuard guard{priorityMutex_};
    policy_->onInsert(segment);
  }
  counters_.misses.fetch_add(1, std::memory_order_relaxed);
  counters_.bytesOnDisk.fetch_add(segmentSize, std::memory_order_relaxed);
}
```

Replace `stats()`:

```cpp
FsCacheStats FsCache::stats() const {
  FsCacheStats snapshot;
  snapshot.hits = counters_.hits.load(std::memory_order_relaxed);
  snapshot.misses = counters_.misses.load(std::memory_order_relaxed);
  snapshot.evictions = counters_.evictions.load(std::memory_order_relaxed);
  snapshot.bytesOnDisk = counters_.bytesOnDisk.load(std::memory_order_relaxed);
  return snapshot;
}
```

In `evict()`, replace the two `CacheStateGuard` blocks:

```cpp
// Old top:
//   uint64_t current;
//   {
//     CacheStateGuard guard{stateMutex_};
//     current = stats_.bytesOnDisk;
//   }
// New:
const uint64_t current =
    counters_.bytesOnDisk.load(std::memory_order_relaxed);
```

```cpp
// Old bottom:
//   {
//     CacheStateGuard guard{stateMutex_};
//     stats_.evictions += evictedCount;
//     stats_.bytesOnDisk -= std::min(stats_.bytesOnDisk, freed);
//   }
// New:
counters_.evictions.fetch_add(evictedCount, std::memory_order_relaxed);
// fetch_sub does not clamp to zero; bytesOnDisk should never go negative
// (recordMiss always credits before evict can debit), but guard with
// CHECK in debug to surface accounting bugs early.
const uint64_t prev =
    counters_.bytesOnDisk.fetch_sub(freed, std::memory_order_relaxed);
VELOX_DCHECK_GE(prev, freed, "bytesOnDisk underflow: prev={} freed={}", prev, freed);
```

The unused `stateMutex_` member can stay (other code may still reference it; removing it is a separate cleanup). But the `CacheStateMutex` include / member can remain — it's now unused on the hot path, which is the point.

- [ ] **Step 5: Run tests to verify they still pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheTest.*:FsCacheConcurrencyTest.*'
```

Expected: all PASS (recovery tests still excluded — Task 10).

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheTest.cpp
git commit -m "$(cat <<'EOF'
perf(fscache): move stats counters to std::atomic, drop CacheStateGuard on hit path

recordHit / recordMiss now use fetch_add(relaxed) instead of taking
CacheStateGuard. evict() reads bytesOnDisk via atomic load and updates
via fetch_add / fetch_sub. The public FsCacheStats struct stays POD;
stats() composes a snapshot from per-field relaxed loads.

Per-field atomicity matches the observability use case (counters move
monotonically and any across-field skew is bounded by a single in-flight
update). stateMutex_ stays as a field but is no longer on the hit path.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: try_lock LRU bump on hit

**Spec:** §4.2 (last bullet + code block)

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`

### Background

Phase-1 `recordHit` takes `CachePriorityGuard` for every hit and calls `policy_->onHit(segment)`. At 16 threads racing on one segment, that single mutex serializes the bumps.

Phase-2 uses `FileSegment::increasePriorityMutex_` (added in Task 3) as a try_lock front: if a concurrent hit already holds the mutex, skip the bump entirely. LRU bump is best-effort; one bump per burst of concurrent hits is enough to move the segment toward MRU.

Per spec §4.2 code block: the try_lock front guards a `CachePriorityGuard pg{bucket.priorityMutex}; bucket.priority->onHit(segment);` block. **Bucket-local priority is added in Task 8** — for Task 7 we keep the call against the single global `priorityMutex_` / `policy_`. Task 8 then changes only the inner block, not the try_lock outer layer.

The test demonstrates that under concurrent hits on the same segment, the LRU position is updated at least once but possibly fewer times than the hit count (which is the desired coalescing behavior).

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`:

```cpp
// 32 threads each hit the SAME segment 100 times. With try_lock-coalesced
// LRU bumps, the total onHit invocation count is bounded above by hit
// count (trivially) and below by at least 1 (the first hit must bump).
// The counter we can observe through public API is FsCacheStats::hits,
// which still reflects every hit — what we're proving here is that the
// test does not deadlock and produces consistent hit counts under
// concurrent contention on increasePriorityMutex_.
TEST_F(FsCacheConcurrencyTest, hitPathTolerates32WayContention) {
  FsCache cache{config_};
  constexpr int kThreads = 32;
  constexpr int kPerThread = 100;
  constexpr uint64_t kSegmentSize = 4UL * 1'024 * 1'024;
  // Prime the segment as kDownloaded before racing on hits, so all kThreads
  // observe a hit (not a miss-cv-wait).
  {
    LocalReadFile remote{remotePath_};
    (void)cache.getOrSet(remotePath_, 0, kSegmentSize, remote);
  }
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      LocalReadFile remote{remotePath_};
      for (int i = 0; i < kPerThread; ++i) {
        (void)cache.getOrSet(remotePath_, 0, kSegmentSize, remote);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  const auto s = cache.stats();
  EXPECT_EQ(s.misses, 1u);
  // 1 priming hit (from getOrSet returning kDownloaded after the priming
  // call's beginDownload) + 32 * 100 racing hits = 3200 + (priming itself
  // is counted as 1 miss, not a hit, so total hits == 32*100).
  EXPECT_EQ(s.hits, kThreads * kPerThread);
}
```

- [ ] **Step 2: Run test to verify it passes (phase-1 already correct)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheConcurrencyTest.hitPathTolerates32WayContention'
```

Expected: PASS under phase-1 mutex semantics. This is a regression guard for Task 7's refactor.

- [ ] **Step 3: Add try_lock LRU bump to recordHit**

Replace `recordHit` in `velox/common/caching/fscache/FsCache.cpp`:

```cpp
void FsCache::recordHit(FileSegment* segment) {
  counters_.hits.fetch_add(1, std::memory_order_relaxed);
  // increasePriorityMutex_ collapses concurrent LRU bumps on the same
  // segment. Losers drop the bump entirely — one bump per burst is
  // enough to move the segment toward MRU (CH FileSegment.cpp:1196-1223
  // pattern).
  std::unique_lock<std::mutex> bumpLock{
      segment->increasePriorityMutex_, std::try_to_lock};
  if (!bumpLock.owns_lock()) {
    return;
  }
  CachePriorityGuard guard{priorityMutex_};
  policy_->onHit(segment);
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheConcurrencyTest.*:FsCacheTest.*'
```

Expected: all PASS, including the new 32-way contention test.

- [ ] **Step 5: Commit**

```bash
git add velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp
git commit -m "$(cat <<'EOF'
perf(fscache): try_lock LRU bump on hit path

recordHit now takes FileSegment::increasePriorityMutex_ with
std::try_to_lock as a front before CachePriorityGuard. Concurrent hits
on the same segment collapse to a single LRU bump per burst (CH
FileSegment.cpp:1196-1223 pattern). hits counter still increments per
call so observability is unaffected.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Per-bucket EvictionPolicy

**Spec:** §5.3, §5.4

**Files:**
- Modify: `velox/common/caching/fscache/FsCacheMetadata.h`
- Modify: `velox/common/caching/fscache/FsCacheMetadata.cpp`
- Modify: `velox/common/caching/fscache/FsCache.h`
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Modify: `velox/common/caching/fscache/tests/EvictionPolicyTest.cpp`

### Background

Phase-1 has one global `LruPolicy` instance under one `CachePriorityMutex`. Phase-2 puts one `EvictionPolicy` instance per bucket (default `LruPolicy`; `SlruPolicy` lands in plan-3), each guarded by the bucket's own `CachePriorityMutex`. `recordHit` / `recordMiss` now need to resolve `segment → bucket`, then act on that bucket's policy under that bucket's mutex.

The bucket index of a segment is `bucketIndex(segment->key().path)`, which `FsCacheMetadata` already computes internally. Expose a `bucketOf(segment)` accessor on `FsCacheMetadata` that returns a `Bucket&` (we need both `priorityMutex` and `priority` from it).

To keep `Bucket` consistent, we extend the existing struct from Task 5 with the two new fields:

```cpp
struct Bucket {
  mutable CacheMetadataMutex guard;
  std::unordered_map<PathKey, KeyMetadataPtr> keys;
  mutable CachePriorityMutex priorityMutex;
  std::unique_ptr<EvictionPolicy> priority;
};
```

`FsCacheMetadata`'s constructor takes a `policyFactory: std::function<std::unique_ptr<EvictionPolicy>()>` so it can construct the right policy per bucket (default factory → `std::make_unique<LruPolicy>()`).

`FsCache` no longer holds `policy_` / `priorityMutex_` — both move to the bucket. The global `evictionMutex_` (the `std::mutex` that serializes evict()) is retained — Task 9 changes evict() to round-robin across buckets, but a single global "eviction in progress" serialization stays.

This task does NOT yet change `evict()` to round-robin across buckets — `evict()` continues to pick all victims from one (or all) buckets in some simple way. Task 9 does the round-robin. Here `evict()` is updated only to access bucket policies correctly: it iterates all buckets, accumulates victims under each bucket's priorityMutex, then proceeds with the same two-phase remove pattern.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/EvictionPolicyTest.cpp`:

```cpp
TEST(EvictionPolicyTest, perBucketIsolation) {
  // Two segments hashing to different buckets must use independent LruPolicy
  // instances — onHit on one must not change the LRU order on the other.
  // We verify indirectly: prime cache with two distinct paths, hit one
  // repeatedly, then trigger eviction sized to exactly one segment. The
  // unhit segment should be evicted (it's LRU within its own bucket — but
  // because each bucket has its own policy, the never-hit one is LRU even
  // though the hit one was hit many times).

  // This test exists primarily as a regression guard: phase-1 with a single
  // global LRU would evict the never-hit segment as well (LRU is the same),
  // so the test passes in both phases. It serves to document that
  // per-bucket isolation does not regress global eviction correctness.
  auto tempDir = ::facebook::velox::common::testutil::TempDirectoryPath::create();
  FsCacheConfig config;
  config.cacheRoot = tempDir->getPath() + "/cache";
  config.maxBytes = 8UL * 1'024 * 1'024;  // exactly 2 segments at 4 MiB
  std::filesystem::create_directories(config.cacheRoot);

  // Two remote files, two distinct paths -> two PathKeys -> likely two buckets
  // (with 1024 buckets the collision probability is ~0.1%; loop until we
  // get two distinct bucket indices to make the test deterministic).
  uint64_t variant = 0;
  std::string pathA, pathB;
  while (true) {
    pathA = tempDir->getPath() + fmt::format("/a{}.bin", variant);
    pathB = tempDir->getPath() + fmt::format("/b{}.bin", variant);
    const auto bucketA =
        std::hash<PathKey>{}(PathKey::fromPath(pathA)) & (config.numBuckets - 1);
    const auto bucketB =
        std::hash<PathKey>{}(PathKey::fromPath(pathB)) & (config.numBuckets - 1);
    if (bucketA != bucketB) {
      break;
    }
    ++variant;
  }
  {
    std::ofstream outA{pathA, std::ios::binary};
    std::ofstream outB{pathB, std::ios::binary};
    const std::string blob(4UL * 1'024 * 1'024, 'q');
    outA.write(blob.data(), blob.size());
    outB.write(blob.data(), blob.size());
  }

  FsCache cache{config};
  LocalReadFile remoteA{pathA};
  LocalReadFile remoteB{pathB};
  (void)cache.getOrSet(pathA, 0, 4UL * 1'024 * 1'024, remoteA);
  (void)cache.getOrSet(pathB, 0, 4UL * 1'024 * 1'024, remoteB);
  // Hit A many times so it is MRU in its bucket.
  for (int i = 0; i < 50; ++i) {
    (void)cache.getOrSet(pathA, 0, 4UL * 1'024 * 1'024, remoteA);
  }
  // Insert a third segment from a third path — must trigger eviction of
  // exactly one of the existing two. B (never hit since the first time)
  // should be the victim.
  std::string pathC = tempDir->getPath() + "/c.bin";
  {
    std::ofstream outC{pathC, std::ios::binary};
    const std::string blob(4UL * 1'024 * 1'024, 'q');
    outC.write(blob.data(), blob.size());
  }
  LocalReadFile remoteC{pathC};
  (void)cache.getOrSet(pathC, 0, 4UL * 1'024 * 1'024, remoteC);
  EXPECT_EQ(cache.stats().evictions, 1u);
  // A should still be cached (most-recently-used in its bucket).
  EXPECT_EQ(cache.stats().bytesOnDisk, 8UL * 1'024 * 1'024);
}
```

- [ ] **Step 2: Run test to verify state**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='EvictionPolicyTest.perBucketIsolation'
```

Expected: PASS under phase-1 (single global LRU also picks B as victim). Acts as a regression guard.

- [ ] **Step 3: Extend FsCacheMetadata::Bucket with priority + priorityMutex**

In `velox/common/caching/fscache/FsCacheMetadata.h`, extend `Bucket`:

```cpp
struct Bucket {
  mutable CacheMetadataMutex guard;
  std::unordered_map<PathKey, KeyMetadataPtr> keys;
  mutable CachePriorityMutex priorityMutex;
  std::unique_ptr<EvictionPolicy> priority;
};
```

Add `#include "velox/common/caching/fscache/EvictionPolicy.h"` to the includes.

Add a `bucketOf` accessor (public):

```cpp
/// Returns the Bucket housing the given path. Used by FsCache to access
/// per-bucket priority. The returned reference is stable for the lifetime
/// of this FsCacheMetadata.
Bucket& bucketOf(const PathKey& path) {
  return *buckets_[bucketIndex(path)];
}
const Bucket& bucketOf(const PathKey& path) const {
  return *buckets_[bucketIndex(path)];
}

/// Returns all buckets in order. Used by evict() to round-robin and by
/// snapshot(). Reference is stable for the FsCacheMetadata's lifetime.
const std::vector<std::unique_ptr<Bucket>>& buckets() const {
  return buckets_;
}
```

Note this also exposes `Bucket` publicly. That's fine — phase-2 design makes `Bucket` a documented value (the spec §5.3 specifies its shape).

Add a constructor overload that takes a policy factory:

```cpp
using PolicyFactory = std::function<std::unique_ptr<EvictionPolicy>()>;

/// Constructs with numBuckets buckets, each holding a freshly-built
/// EvictionPolicy from policyFactory. numBuckets must be power-of-two.
FsCacheMetadata(size_t numBuckets, PolicyFactory policyFactory);

/// Convenience: default factory builds LruPolicy per bucket.
explicit FsCacheMetadata(size_t numBuckets)
    : FsCacheMetadata(
          numBuckets,
          [] { return std::make_unique<LruPolicy>(); }) {}
```

Add `#include "velox/common/caching/fscache/LruPolicy.h"` for the default factory.

In `FsCacheMetadata.cpp`, implement the new constructor:

```cpp
FsCacheMetadata::FsCacheMetadata(size_t numBuckets, PolicyFactory policyFactory)
    : bucketMask_{numBuckets - 1} {
  VELOX_CHECK_GT(numBuckets, 0);
  VELOX_CHECK_EQ(
      numBuckets & (numBuckets - 1),
      0,
      "numBuckets must be power of two: {}",
      numBuckets);
  VELOX_CHECK(static_cast<bool>(policyFactory), "policyFactory must be set");
  buckets_.reserve(numBuckets);
  for (size_t i = 0; i < numBuckets; ++i) {
    auto bucket = std::make_unique<Bucket>();
    bucket->priority = policyFactory();
    VELOX_CHECK_NOT_NULL(bucket->priority);
    buckets_.emplace_back(std::move(bucket));
  }
}
```

Delete the previous standalone definition; the inline default constructor delegates to the new overload.

- [ ] **Step 4: Drop global priority from FsCache**

Edit `velox/common/caching/fscache/FsCache.h`:

Remove fields:
```cpp
// std::unique_ptr<EvictionPolicy> policy_;
// mutable CachePriorityMutex priorityMutex_;
```

Keep `evictionMutex_` (still needed for serializing concurrent evict() calls; see Task 9).

Edit `velox/common/caching/fscache/FsCache.cpp` constructor — remove the `policy_{std::make_unique<LruPolicy>()}` initializer.

- [ ] **Step 5: Route recordHit / recordMiss / evict() through bucket-local policy**

Replace `recordHit` and `recordMiss`:

```cpp
void FsCache::recordHit(FileSegment* segment) {
  counters_.hits.fetch_add(1, std::memory_order_relaxed);
  std::unique_lock<std::mutex> bumpLock{
      segment->increasePriorityMutex_, std::try_to_lock};
  if (!bumpLock.owns_lock()) {
    return;
  }
  auto& bucket = metadata_->bucketOf(segment->key().path);
  CachePriorityGuard guard{bucket.priorityMutex};
  bucket.priority->onHit(segment);
}

void FsCache::recordMiss(FileSegment* segment, uint64_t segmentSize) {
  {
    auto& bucket = metadata_->bucketOf(segment->key().path);
    CachePriorityGuard guard{bucket.priorityMutex};
    bucket.priority->onInsert(segment);
  }
  counters_.misses.fetch_add(1, std::memory_order_relaxed);
  counters_.bytesOnDisk.fetch_add(segmentSize, std::memory_order_relaxed);
}
```

Update `evict()` to iterate buckets (still single global pass; Task 9 changes the iteration strategy):

```cpp
void FsCache::evict(uint64_t bytesNeeded) {
  std::lock_guard<std::mutex> evictGuard{evictionMutex_};
  const uint64_t current =
      counters_.bytesOnDisk.load(std::memory_order_relaxed);
  if (current + bytesNeeded <= config_.maxBytes) {
    return;
  }
  const uint64_t toFree = current + bytesNeeded - config_.maxBytes;

  // Per-bucket candidate selection. Each bucket contributes victims under
  // its own priorityMutex; the candidates list is filled in deterministic
  // bucket order. Task 9 changes this to round-robin try_lock; Task 8
  // keeps the simple full-pass behavior.
  struct VictimEntry {
    FileSegment* victim;
    FsCacheMetadata::Bucket* bucket;
  };
  std::vector<VictimEntry> candidates;
  uint64_t accumulated = 0;
  for (const auto& bucketPtr : metadata_->buckets()) {
    if (accumulated >= toFree) {
      break;
    }
    CachePriorityGuard guard{bucketPtr->priorityMutex};
    auto picked = bucketPtr->priority->selectVictims(toFree - accumulated);
    for (auto* v : picked) {
      candidates.push_back({v, bucketPtr.get()});
      accumulated += v->size();
      if (accumulated >= toFree) {
        break;
      }
    }
  }

  uint64_t freed = 0;
  uint32_t evictedCount = 0;
  for (auto& entry : candidates) {
    const auto key = entry.victim->key();
    std::error_code ec;
    std::filesystem::remove(
        entry.victim->localPath(config_.cacheRoot), ec);
    if (ec) {
      LOG(WARNING) << "FsCache evict: failed to remove "
                   << entry.victim->remotePath() << " [" << key.offset
                   << ".." << key.offset + key.size << "): " << ec.message();
      continue;
    }
    {
      CachePriorityGuard guard{entry.bucket->priorityMutex};
      entry.bucket->priority->onRemove(entry.victim);
    }
    metadata_->erase(key);
    freed += key.size;
    ++evictedCount;
  }
  counters_.evictions.fetch_add(evictedCount, std::memory_order_relaxed);
  if (freed > 0) {
    const uint64_t prev =
        counters_.bytesOnDisk.fetch_sub(freed, std::memory_order_relaxed);
    VELOX_DCHECK_GE(prev, freed, "bytesOnDisk underflow: prev={} freed={}", prev, freed);
  }
}
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='EvictionPolicyTest.*:FsCacheTest.*:FsCacheConcurrencyTest.*:FsCacheMetadataTest.*'
```

Expected: all PASS, including the new perBucketIsolation test.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCacheMetadata.h \
        velox/common/caching/fscache/FsCacheMetadata.cpp \
        velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/EvictionPolicyTest.cpp
git commit -m "$(cat <<'EOF'
refactor(fscache): per-bucket EvictionPolicy + CachePriorityMutex

Each FsCacheMetadata::Bucket now carries its own EvictionPolicy
(default LruPolicy) and CachePriorityMutex. FsCache drops the global
policy_/priorityMutex_ and routes recordHit/recordMiss/evict through
metadata_->bucketOf(segment->key().path).

evict() iterates buckets and selects victims under per-bucket
priority mutexes; Task 9 replaces this simple full-pass with
round-robin try_lock + blocking fallback. evictionMutex_ stays as
the global serializer for concurrent evict() calls.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Cross-bucket evict round-robin (try_lock N rounds + blocking fallback)

**Spec:** §4.6

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`

### Background

Spec §4.6: `evict(bytesNeeded)` does N=`numBuckets` rounds of round-robin `try_lock` across buckets; each round skips buckets it couldn't acquire. If all N rounds together still didn't reach `bytesNeeded`, fall back to per-bucket blocking `lock()` (still round-robin) until `bytesNeeded` is met or all buckets have been visited.

The round-robin starting offset rotates per call so a bucket that is "first" in one call is "last" in the next — this distributes the eviction load. Use an atomic `std::atomic<size_t> evictStart_{0}` member on `FsCache` and `fetch_add(1, relaxed) % numBuckets` for the start offset.

This task does NOT change the two-phase remove (filesystem first, then policy + metadata) — only the candidate selection loop.

The test starves the cache to force eviction under concurrent hits and asserts no crash + accounting consistency.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`:

```cpp
// 16 threads each insert + read distinct segments while the cache
// capacity is half of total demand. evict() must run many times across
// many buckets. Per-bucket try_lock + blocking fallback must produce
// correct accounting and never deadlock.
TEST_F(FsCacheConcurrencyTest, roundRobinEvictUnderConcurrentInserts) {
  FsCacheConfig tighter = config_;
  tighter.maxBytes = 16UL * 1'024 * 1'024;  // 4 segments at 4 MiB
  FsCache cache{tighter};

  constexpr int kThreads = 16;
  constexpr int kSegmentsPerThread = 8;
  constexpr uint64_t kSegmentSize = 4UL * 1'024 * 1'024;

  // Create 16 * 8 = 128 distinct remote files so insertions spread across
  // buckets.
  std::vector<std::string> paths(kThreads * kSegmentsPerThread);
  for (size_t i = 0; i < paths.size(); ++i) {
    paths[i] = tempDir_->getPath() + fmt::format("/blob-{}.bin", i);
    std::ofstream out{paths[i], std::ios::binary};
    const std::string blob(kSegmentSize, 'q');
    out.write(blob.data(), blob.size());
  }

  std::vector<std::thread> threads;
  std::atomic<int> errors{0};
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      try {
        for (int i = 0; i < kSegmentsPerThread; ++i) {
          const auto& path = paths[t * kSegmentsPerThread + i];
          LocalReadFile remote{path};
          auto segs = cache.getOrSet(path, 0, kSegmentSize, remote);
          for (auto& s : segs) {
            if (s->state() != FileSegment::State::kDownloaded) {
              ++errors;
            }
          }
        }
      } catch (const std::exception&) {
        ++errors;
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(errors, 0);
  // At least 4 * 31 = 124 evictions for 128 inserts into a 4-slot cache;
  // exact count depends on race ordering. We just require many evictions
  // happened and no overshoot.
  EXPECT_GT(cache.stats().evictions, 100u);
  EXPECT_LE(cache.stats().bytesOnDisk, tighter.maxBytes);
}
```

- [ ] **Step 2: Run test (likely PASS under Task 8 simple full-pass)**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheConcurrencyTest.roundRobinEvictUnderConcurrentInserts'
```

Expected: PASS under Task 8's simple iteration. The test is the regression guard for Task 9's round-robin/fallback refactor.

- [ ] **Step 3: Add evictStart_ field to FsCache**

In `velox/common/caching/fscache/FsCache.h`, add to private members:

```cpp
// Rotating start offset for evict() round-robin. fetch_add(1, relaxed)
// per evict() call; distributes eviction load across buckets across many
// calls so the same buckets aren't always queried first.
mutable std::atomic<size_t> evictStart_{0};
```

- [ ] **Step 4: Replace evict() with round-robin + fallback**

Replace the entire `evict()` function in `velox/common/caching/fscache/FsCache.cpp`:

```cpp
void FsCache::evict(uint64_t bytesNeeded) {
  std::lock_guard<std::mutex> evictGuard{evictionMutex_};
  const uint64_t current =
      counters_.bytesOnDisk.load(std::memory_order_relaxed);
  if (current + bytesNeeded <= config_.maxBytes) {
    return;
  }
  const uint64_t toFree = current + bytesNeeded - config_.maxBytes;

  struct VictimEntry {
    FileSegment* victim;
    FsCacheMetadata::Bucket* bucket;
  };
  std::vector<VictimEntry> candidates;
  uint64_t accumulated = 0;

  const auto& buckets = metadata_->buckets();
  const size_t n = buckets.size();
  const size_t start =
      evictStart_.fetch_add(1, std::memory_order_relaxed) % n;
  // Track which buckets we couldn't acquire on the try_lock pass.
  std::vector<bool> skipped(n, false);

  // Phase A: N try_lock rounds across buckets in round-robin order.
  // Each bucket is attempted N times (one per round) — concretely, each
  // round walks all buckets once and try_locks each. Buckets that succeed
  // contribute victims; failures are recorded in skipped[] and retried in
  // the next round. (Single round walks all buckets; N=numBuckets rounds
  // gives every bucket numBuckets attempts.)
  for (size_t round = 0; round < n && accumulated < toFree; ++round) {
    for (size_t i = 0; i < n && accumulated < toFree; ++i) {
      const size_t idx = (start + i) % n;
      auto& bucket = *buckets[idx];
      std::unique_lock<CachePriorityMutex> lk{
          bucket.priorityMutex, std::try_to_lock};
      if (!lk.owns_lock()) {
        skipped[idx] = true;
        continue;
      }
      skipped[idx] = false;
      auto picked = bucket.priority->selectVictims(toFree - accumulated);
      for (auto* v : picked) {
        candidates.push_back({v, &bucket});
        accumulated += v->size();
        if (accumulated >= toFree) {
          break;
        }
      }
    }
  }

  // Phase B: blocking fallback for any bucket still skipped or for any
  // remaining shortfall. Worst case: numBuckets * per-bucket lock hold
  // (~10 us in measurements; ~10 ms total at 1024 buckets — spec §4.6
  // accepts this as evict is off the hot path).
  if (accumulated < toFree) {
    for (size_t i = 0; i < n && accumulated < toFree; ++i) {
      const size_t idx = (start + i) % n;
      auto& bucket = *buckets[idx];
      CachePriorityGuard guard{bucket.priorityMutex};
      auto picked = bucket.priority->selectVictims(toFree - accumulated);
      for (auto* v : picked) {
        candidates.push_back({v, &bucket});
        accumulated += v->size();
        if (accumulated >= toFree) {
          break;
        }
      }
    }
  }

  // Two-phase remove: filesystem first (per spec §4.6 + phase-1 behavior).
  uint64_t freed = 0;
  uint32_t evictedCount = 0;
  for (auto& entry : candidates) {
    const auto key = entry.victim->key();
    std::error_code ec;
    std::filesystem::remove(
        entry.victim->localPath(config_.cacheRoot), ec);
    if (ec) {
      LOG(WARNING) << "FsCache evict: failed to remove "
                   << entry.victim->remotePath() << " [" << key.offset
                   << ".." << key.offset + key.size << "): " << ec.message();
      continue;
    }
    {
      CachePriorityGuard guard{entry.bucket->priorityMutex};
      entry.bucket->priority->onRemove(entry.victim);
    }
    metadata_->erase(key);
    freed += key.size;
    ++evictedCount;
  }
  counters_.evictions.fetch_add(evictedCount, std::memory_order_relaxed);
  if (freed > 0) {
    const uint64_t prev =
        counters_.bytesOnDisk.fetch_sub(freed, std::memory_order_relaxed);
    VELOX_DCHECK_GE(prev, freed, "bytesOnDisk underflow: prev={} freed={}", prev, freed);
  }
}
```

- [ ] **Step 5: Run all fscache tests**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='-FsCacheRecoveryTest.*:-FsCachePersistenceTest.*'
```

(Recovery / persistence tests skipped — they're handled in Task 10.)

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add velox/common/caching/fscache/FsCache.h \
        velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp
git commit -m "$(cat <<'EOF'
perf(fscache): round-robin evict with try_lock + blocking fallback

evict() now walks buckets in rotating round-robin order (start offset
from evictStart_.fetch_add(1)). Phase A does N=numBuckets rounds of
try_lock per bucket — busy buckets are skipped each round. Phase B
falls back to per-bucket blocking lock() in round-robin order until
bytesNeeded is satisfied or all buckets visited.

The rotating start distributes eviction load across calls so the same
buckets aren't always probed first. Worst-case fallback wait is bounded
by numBuckets * per-bucket hold time (~10 ms at 1024 buckets); spec
§4.6 accepts this since evict() is off the hit fast-path.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: loadFromDisk blind-clear on first phase-2 start

**Spec:** §4.4, §5.5, §10 R7

**Files:**
- Modify: `velox/common/caching/fscache/FsCache.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp`
- Modify: `velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp`

### Background

Phase-2 changes the `FsCacheKey::hash()` algorithm — phase-1 cache files on disk now hash differently and can't be re-keyed. Spec §10 R7 (and §5.5) commit to **blind-clear**: `loadFromDisk()` detects non-empty `cacheRoot` on first phase-2 start and removes everything, then proceeds with normal scan. The "first phase-2 start" detection is conservative — we always clear if non-empty, accepting that subsequent phase-2 restarts also clear (since on each restart the previous run's files are still incompatible with the next... wait, that's wrong).

Re-read: phase-2 vs phase-2 restarts MUST preserve cache. The clear must only happen once per *version transition*, not once per restart. The spec leaves the mechanism open but commits to "phase-2 首次启动如检测到 cacheRoot 非空，整体清空". The cleanest "first phase-2 start" signal is a version sentinel file: write `cacheRoot/.fscache_version` containing "2" on first scan; on every subsequent scan, if the sentinel exists and reads "2", skip the blind clear. If the sentinel is missing OR reads anything other than "2", blind-clear then write the sentinel.

This matches spec §10 R7's rejected `v2-` filename prefix alternative without breaking the §5.5 "schema 不变" promise.

Existing `FsCachePersistenceTest` (phase-1) expects `loadFromDisk()` to preserve survivor files across restart. That test creates the cache → puts files in → destroys → recreates → loadFromDisk → checks files survive. In phase-2 this still passes **if the sentinel was written on the first loadFromDisk**, because subsequent loadFromDisks see the sentinel and skip the clear.

But the phase-1 `FsCachePersistenceTest` doesn't currently call `loadFromDisk` between create-and-write and the destroy step. The test pattern is: cache.getOrSet (creates file) → cache destroyed → new cache → loadFromDisk → assert file exists. The first loadFromDisk in the new cache sees:
- `cacheRoot` non-empty (has the survivor file)
- No sentinel (first-ever loadFromDisk in phase-2)
- → blind-clear → write sentinel

That would break the existing persistence test, which is the spec-intended phase-1 → phase-2 transition behavior. We need to either:
(a) Update the persistence test to pre-create the sentinel before destroying the first FsCache (simulating "this was already a phase-2 cache").
(b) Move sentinel writing to `FsCache` constructor instead of `loadFromDisk`.

Option (b) is cleaner: any `FsCache` instance writes the sentinel on construction. `loadFromDisk` then sees the sentinel (because the constructor just wrote it!) and skips the clear. But that means `loadFromDisk` never blind-clears on the first phase-2 start of an existing cache. Bad.

The right ordering: `loadFromDisk` writes the sentinel AFTER the clear, but BEFORE any normal disk scan. Existing tests that re-`loadFromDisk` after creating files should pre-write the sentinel to simulate a phase-2 cache (or the test creates files via getOrSet on a phase-2 FsCache, which is itself a phase-2 cache, so the sentinel was written by that first cache's loadFromDisk... wait, but the test never calls loadFromDisk on the writing cache).

Cleanest plan:
- `loadFromDisk()` reads `cacheRoot/.fscache_version` first.
- If sentinel exists and contains exactly `2`, skip clear; proceed with normal scan.
- Otherwise, blind-clear `cacheRoot`, then write sentinel, then return (skip the normal scan — disk is empty now).
- Existing persistence test: callers that pre-populate `cacheRoot` for a "phase-2 survivor" scenario must pre-write the sentinel.

Add a helper `static constexpr std::string_view kVersionSentinelName = ".fscache_version";` and the version string `"2"`.

- [ ] **Step 1: Write the failing test**

Append to `velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp`:

```cpp
TEST_F(FsCacheRecoveryTest, blindClearsCacheRootWithoutVersionSentinel) {
  // Simulate a phase-1 (or unknown) cache: cacheRoot has files but no
  // .fscache_version sentinel.
  std::filesystem::create_directories(config_.cacheRoot);
  const auto stale = config_.cacheRoot + "/aa/bb/legacy.0.4096";
  std::filesystem::create_directories(config_.cacheRoot + "/aa/bb");
  {
    std::ofstream out{stale, std::ios::binary};
    out.write("garbage", 7);
  }
  EXPECT_TRUE(std::filesystem::exists(stale));

  FsCache cache{config_};
  cache.loadFromDisk();

  EXPECT_FALSE(std::filesystem::exists(stale))
      << "phase-2 loadFromDisk must blind-clear cacheRoot when no version "
         "sentinel is present";
  // Sentinel must exist after clear so subsequent loadFromDisk calls
  // skip the clear.
  EXPECT_TRUE(std::filesystem::exists(
      config_.cacheRoot + "/.fscache_version"));
}

TEST_F(FsCacheRecoveryTest, preservesCacheRootWithMatchingVersionSentinel) {
  std::filesystem::create_directories(config_.cacheRoot);
  // Write the v2 sentinel before any files exist.
  {
    std::ofstream out{config_.cacheRoot + "/.fscache_version"};
    out << "2";
  }
  // Write a survivor file (in the layout FsCacheKey::fileName produces).
  const FsCacheKey key{PathKey::fromPath("/data/x"), 0, 4096};
  const std::string fileName = key.fileName();
  const std::string prefix = fileName.substr(0, 2);
  const std::string sub = fileName.substr(2, 2);
  std::filesystem::create_directories(
      config_.cacheRoot + "/" + prefix + "/" + sub);
  const auto survivor =
      config_.cacheRoot + "/" + prefix + "/" + sub + "/" + fileName;
  {
    std::ofstream out{survivor, std::ios::binary};
    const std::string blob(4096, 'q');
    out.write(blob.data(), blob.size());
  }

  FsCache cache{config_};
  cache.loadFromDisk();

  EXPECT_TRUE(std::filesystem::exists(survivor))
      << "Phase-2 cache with matching sentinel must preserve survivor files";
}
```

The fixture `FsCacheRecoveryTest` already exists in the file (check the top of the file for its `SetUp` — config_ and tempDir_ should be in scope).

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheRecoveryTest.blindClearsCacheRootWithoutVersionSentinel:FsCacheRecoveryTest.preservesCacheRootWithMatchingVersionSentinel'
```

Expected: both FAIL (loadFromDisk doesn't blind-clear, sentinel never written).

- [ ] **Step 3: Implement sentinel + blind-clear in loadFromDisk**

In `velox/common/caching/fscache/FsCache.cpp`, replace the body of `loadFromDisk()`. Add at the top of the file (or near existing static constants):

```cpp
namespace {
constexpr std::string_view kVersionSentinelName = ".fscache_version";
constexpr std::string_view kCurrentVersion = "2";

bool sentinelMatches(const std::string& cacheRoot) {
  const auto path =
      std::filesystem::path{cacheRoot} / kVersionSentinelName;
  std::ifstream in{path};
  if (!in.is_open()) {
    return false;
  }
  std::string content;
  std::getline(in, content);
  return content == kCurrentVersion;
}

void writeSentinel(const std::string& cacheRoot) {
  const auto path =
      std::filesystem::path{cacheRoot} / kVersionSentinelName;
  std::ofstream out{path, std::ios::trunc};
  out << kCurrentVersion;
}

void blindClearCacheRoot(const std::string& cacheRoot) {
  std::error_code ec;
  for (auto& entry :
       std::filesystem::directory_iterator{cacheRoot, ec}) {
    std::error_code rmEc;
    std::filesystem::remove_all(entry.path(), rmEc);
    if (rmEc) {
      LOG(WARNING) << "FsCache blind-clear: failed to remove "
                   << entry.path().string() << ": " << rmEc.message();
    }
  }
}
} // namespace
```

Replace the body of `loadFromDisk()`:

```cpp
void FsCache::loadFromDisk() {
  if (!std::filesystem::exists(config_.cacheRoot)) {
    std::filesystem::create_directories(config_.cacheRoot);
    writeSentinel(config_.cacheRoot);
    return;
  }
  if (!sentinelMatches(config_.cacheRoot)) {
    // Either a fresh phase-2 install on top of phase-1 files, or an
    // unrelated foreign cacheRoot. Disk layout from phase-1 is not
    // re-keyable (filename only encodes hash, not original path), so spec
    // §10 R7 commits to blind-clear and re-fetch on demand. Write the
    // sentinel after the clear so subsequent restarts skip this branch.
    blindClearCacheRoot(config_.cacheRoot);
    writeSentinel(config_.cacheRoot);
    return;
  }

  // Phase-2 cache with matching sentinel: do the phase-1 cleanup pass for
  // .tmp leftovers and size-mismatched files. Survivors are picked up on
  // demand by subsequent getOrSet() calls (FileSegment::download
  // short-circuits when it finds the file already present with the
  // expected size).
  std::vector<std::filesystem::path> toRemove;
  for (auto& entry :
       std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
    if (!entry.is_regular_file()) {
      continue;
    }
    // Skip the sentinel itself.
    if (entry.path().filename() == kVersionSentinelName) {
      continue;
    }
    if (entry.path().extension() == ".tmp") {
      toRemove.push_back(entry.path());
      continue;
    }
    const auto parsed = parseFileName(entry.path().filename().string());
    if (!parsed.has_value()) {
      continue;
    }
    if (entry.file_size() != parsed->size) {
      toRemove.push_back(entry.path());
    }
  }
  std::error_code ignore;
  for (const auto& path : toRemove) {
    std::filesystem::remove(path, ignore);
  }
  // Second pass: rmdir any subdirectory left empty.
  bool removedAny = true;
  while (removedAny) {
    removedAny = false;
    std::vector<std::filesystem::path> emptyDirs;
    for (auto& entry :
         std::filesystem::recursive_directory_iterator{config_.cacheRoot}) {
      if (entry.is_directory() && std::filesystem::is_empty(entry.path())) {
        emptyDirs.push_back(entry.path());
      }
    }
    for (const auto& dir : emptyDirs) {
      if (std::filesystem::remove(dir, ignore)) {
        removedAny = true;
      }
    }
  }
}
```

- [ ] **Step 4: Run new recovery tests**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test \
  --gtest_filter='FsCacheRecoveryTest.*'
```

Expected: the 2 new tests PASS. Existing recovery tests may FAIL because they don't write the sentinel — fix in Step 5.

- [ ] **Step 5: Update existing recovery + persistence tests to write the sentinel**

The phase-1 fixtures pre-populate `cacheRoot` with valid-looking cache files and expect `loadFromDisk()` to preserve them. Under phase-2 these files would be hashed with the phase-1 algorithm and ALSO lack the sentinel — both reasons to clear. To preserve the test intent (verify cleanup of .tmp + size-mismatch), update each fixture to:

1. Write all survivor files with `FsCacheKey::fileName()` under phase-2 hashing (i.e. wrap path with `PathKey::fromPath(...)` when computing the expected filename).
2. Write the `.fscache_version` sentinel before constructing the FsCache.

Find every place in `FsCacheRecoveryTest.cpp` and `FsCachePersistenceTest.cpp` where a file is pre-written to `cacheRoot`:

```bash
grep -n "cacheRoot\|fileName\|ofstream" \
  velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp \
  velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp
```

For each test that pre-populates `cacheRoot`:
- Add `{ std::ofstream out{config_.cacheRoot + "/.fscache_version"}; out << "2"; }` immediately after the `create_directories(cacheRoot)` call.
- If the test computes a survivor filename, ensure it uses `FsCacheKey{PathKey::fromPath(path), offset, size}.fileName()` (Task 2 already changed the API, but if a literal hash was hard-coded, replace with a computed name).

For tests that don't pre-populate `cacheRoot` (e.g. they start empty, getOrSet writes files via the API), no change is needed: the first `loadFromDisk()` on an empty `cacheRoot` writes the sentinel; subsequent calls preserve it.

- [ ] **Step 6: Run full fscache test suite**

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
```

Expected: ALL PASS. This is the first task that requires the full suite green.

- [ ] **Step 7: Commit**

```bash
git add velox/common/caching/fscache/FsCache.cpp \
        velox/common/caching/fscache/tests/FsCacheRecoveryTest.cpp \
        velox/common/caching/fscache/tests/FsCachePersistenceTest.cpp
git commit -m "$(cat <<'EOF'
feat(fscache): blind-clear cacheRoot on first phase-2 start

loadFromDisk() now reads cacheRoot/.fscache_version. If absent or not
equal to "2", remove everything under cacheRoot, then write the
sentinel; subsequent calls observe the sentinel and skip the clear,
proceeding with the existing .tmp / size-mismatch cleanup pass.

Phase-1 cache files use a different FsCacheKey hash and cannot be
re-keyed (filename only encodes hash, not path; spec §10 R7). The
blind clear is the only viable "accept dropping phase-1 cache" path.

Existing recovery / persistence fixtures pre-write the v2 sentinel
when they pre-populate cacheRoot, so .tmp + size-mismatch cleanup
test coverage is preserved.

Co-Authored-By: Claude Opus 4 <noreply@anthropic.com>
EOF
)"
```

---

## Verification (post-plan)

After all 10 tasks land, run the full fscache test suite plus regression check across the broader caching tests that depend on FsCache:

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_test -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/tests/velox_fscache_test
```

Expected: 100% pass.

```bash
cd /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 && \
ctest -R fscache -j 8 --output-on-failure
```

Expected: all `*fscache*` test binaries PASS.

Micro-benchmark sanity (manual, separate from this plan):

```bash
cmake --build /home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13 \
  --target velox_fscache_microbench -j 16 && \
/home/chang/OpenSource/velox2/cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_microbench \
  --threads=16 --out=/tmp/phase2-plan1-baseline.md
```

Compare against `docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` (the existing phase-1 baseline). The 16-thread hit-path improvement is the primary signal that the per-bucket / atomic-stats / try_lock-bump changes paid off; the spec's §3 success criterion is "≥ 2× hit-rate ops/s at 16 threads" but that confirmation is plan-3's territory (with SLRU) — plan-1 should show meaningful improvement but not necessarily 2×.

Git log check:

```bash
git log --oneline upstream/main..HEAD | head -20
```

Expected: 10 new commits on top of the plan-1 doc commit. No `--amend`, no `--no-verify`.

Verify the benchmark file is NOT touched:

```bash
git diff upstream/main -- velox/common/caching/benchmarks/CacheBackendBenchmark.cpp | wc -l
```

Expected: 0.

---

## Notes

- The numbering "Task 1..10" matches commit boundaries; each Task ends with one `git commit`. There are no hidden sub-commits.
- Phase 2.5 (`PARTIALLY_DOWNLOADED`), plan-2 (bg download + R0), plan-3 (SLRU), plan-4 (QueryLimit + bypass) are explicitly OUT OF SCOPE. The `LruPolicy` stays as the default per-bucket policy; `FsCacheBufferedInput::load()` stays synchronous.
- Spec §6.2 cross-spec impact note (TPC-DS A/B spec §2.5 needs the `FsCacheBufferedInput` ctor to take a `folly::Executor*`) is plan-2's responsibility, not plan-1's. No TPC-DS spec files are touched by this plan.
- LockOrderChecker enhancement (spec §4.5 W1: per-bucket index assertion) is explicitly NOT in this plan — spec leaves it as optional belt-and-suspenders.

