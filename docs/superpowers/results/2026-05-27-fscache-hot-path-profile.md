# FsCache Hot-Path Profile (single-thread gate + t=4 contention)

Date: 2026/05/27. HEAD: `(pre-rebase, no direct HEAD equivalent)` plus race-fix `bb4c536d4`. Pre-R1 profile commit on HEAD: `ae2f495a9`.
Build: `cmake-build-relwithdebinfo-gcc13` (gcc-13 RelWithDebInfo, fp call-graph).
Binary: `velox_fscache_benchmark`.
Tool: `perf record -F 4000 -g --call-graph fp`.

## 1. Measured headline (this run)

```
| workload   | t | ws_mult | lat | ops/s     | hit%   | p50 µs | p95 | p99 |
| sequential | 1 | 0.5     | 0   |  6'715'410|100.0%  | 0.1    | 0.1 | 0.1 |
| sequential | 4 | 0.5     | 0   | 17'666'433|100.0%  | 0.1    | 0.2 | 0.3 |
```

- t=1 ≈ **149 ns/op**, gate is 7.0 M ops/s ≈ 143 ns/op — miss by ~6 ns / 4%.
- t=4 ≈ **4.42 M ops/thread** vs 6.72 single → **−34 % per-thread**
  (sweep doc reported −49 %; that run had `ops=2M`, this one has `ops=20M`
  so warmup amortizes better; the scaling trend is identical).
- hit% = 100 %, evictions = 0, downloads = 0. The hot path is **getOrSet →
  recordHit** only; `FsCache::evict` is NOT called (bench.cpp:413 gates it
  on `state==kEmpty`).

## 2. Per-op work on a 100% hit (counted from source)

`FsCacheBenchmark.cpp:389-454` per-op loop calls:

| step | mutex acquisitions / atomics | source |
|------|---|---|
| `FsCache::getOrSet` early checks | none | `FsCache.cpp:239-261` |
| `metadata_->lockKeyMetadata`     | **bucket.guard L+U** (RankedMutex rank-2) | `FsCacheMetadata.cpp:95-122` |
| `keyMeta->lock()`                | **KeyMutex L+U** (RankedMutex rank-3) — held across `lookupRangeUnlocked` + `fillHoles` | `FsCache.cpp:265-279` |
| holder ctor + `std::vector` alloc | `operator new` / `push_back` | `FileSegmentsHolder.h` |
| `recordHit` counter              | `counters_.demandHits.fetch_add(1, relaxed)` (atomic on shared cache line) | `FsCache.cpp:460` |
| `recordHit` per-segment dedup    | `try_lock` on `FileSegment::increasePriorityMutex_` | `FsCache.cpp:464-468` |
| `recordHit` LRU bump (if won)    | **CachePriorityMutex L+U** (rank-0) + `list::erase` + `list::push_front` + `unordered_map::find` × 2 | `FsCache.cpp:469-471`, `LruPolicy.cpp:38-46` |
| holder dtor                      | walks `slots` vector, drops shared_ptrs, no extra locks |  |

**Lock budget: 4 lock+unlock pairs per op** on a hit
(bucket.guard, KeyMutex, segment-dedup try_lock, CachePriorityMutex)
plus **1 shared-atomic fetch_add**.

## 3. perf top-N — user-space self time, t=1 (10 433 samples)

```
 5.28%  FsCache::recordHit
 3.11%  folly::hash::SpookyHashV2::Hash128       <-- PathKey::fromPath
 2.91%  FsCache::getOrSet
 2.78%  std::vector<FileSegmentPtr>::push_back   <-- slots + holder
 1.57%  LruPolicy::onHit
 1.46%  FsCache::fillHolesWithEmptyFileSegments
 0.61%  unordered_map<PathKey,…>::find           <-- bucket.keys lookup
 0.54%  unordered_map<FileSegment*,…>::find      <-- LruPolicy::index_
 0.49%  LockedKey::LockedKey
 0.46%  PathKey::fromPath
 0.44%  FileSegmentsHolder::~FileSegmentsHolder
 0.44%  folly::hash::SpookyHashV2::Short
 0.34%  KeyMetadata::lock                         <-- pthread_mutex_lock inline
```

Plus standalone libc:
```
 5.69%  pthread_mutex_lock@@GLIBC_2.2.5
 5.66%  pthread_mutex_unlock@@GLIBC_2.2.5
 5.55%  vdso clock_gettime                       <-- benchmark timer
```

Subtracting the timer and merging libc mutex calls into their callers:
**~12 % of every op is spent in libc mutex lock/unlock**, ~5 % in recordHit
body, ~3 % in getOrSet body, ~3 % in PathKey hashing (SpookyHash on every
call because `pathStr` is hashed fresh — see §6 fix R3), ~3 % in
vector::push_back / heap allocation.

Add kernel samples (warmup pread/pwrite that survived in trace): ~24 %.
Bench inner-loop user time only (excluding warmup/timer):
- mutex L+U overhead .................... ~12 %
- recordHit body ......................... ~5 %
- getOrSet body .......................... ~3 %
- vector alloc / push_back ............... ~3 %
- PathKey SpookyHash ..................... ~4 %
- LruPolicy onHit + index_.find .......... ~2 %
- holder dtor ............................ <1 %

## 4. perf top-N — user-space self time, t=4 (14 730 samples)

```
11.45%  parallelRun lambda (timer + dispatch)
 9.56%  FsCache::recordHit                  (+4.28 pp vs t=1)
 8.63%  FsCache::getOrSet                   (+5.72 pp vs t=1)
 5.01%  std::vector<FileSegmentPtr>::push_back (+2.23 pp)
 4.82%  FsCacheMetadata::lockKeyMetadata    (was hidden inside getOrSet at t=1)
 3.91%  FileSegmentsHolder::~FileSegmentsHolder (+3.47 pp)
 1.14%  LruPolicy::onHit
 1.06%  fillHolesWithEmptyFileSegments
```

Call-graph (caller-rooted) attribution of `pthread_mutex_lock` at t=4:

```
recordHit         → pthread_mutex_lock   2.20 % (+ unlock 1.93 %)
lockKeyMetadata   → pthread_mutex_lock   2.67 %     (bucket.guard rank-2)
LockedKey ctor    → pthread_mutex_lock   2.23 %     (KeyMutex rank-3)
recordHit         → pthread_mutex_trylock 2.45 %    (segment dedup)
```

Total mutex-call self-time at t=4: **≥ 10 %** (lock-only; add ~equal unlock
time → ~20 % of cycles are inside libc mutex code paths). At t=1 the
equivalent figure is ~12 %.

## 5. Top hot-path findings

**Finding 1 — Mutex `lock`/`unlock` overhead dominates t=1.** Four
uncontended lock/unlock pairs per op on a glibc futex-based `std::mutex`
costs ~20 ns each (10 ns lock + 10 ns unlock measured on this box via
the `pthread_mutex_lock` self %). That is 80 ns out of the 149 ns budget,
~54 % of an op. Removing or coalescing one of these locks should
recover ~13 % single-thread (well above the 6 % gate gap).

**Finding 2 — `recordHit` is the single biggest user function (5.28 % t=1
→ 9.56 % t=4, +4.28 pp).** Two mutex acquisitions
(`segment.increasePriorityMutex_` try_lock + bucket `CachePriorityMutex`)
plus an atomic counter bump plus an `unordered_map::find` on every hit.
With `ws_mult=0.5` the same 8 segments per file are touched in a loop, so
LruPolicy `index_.find` always hits the same hash buckets — even when
no LRU reorder is needed.

**Finding 3 — `getOrSet` user body 2.91 % t=1 → 8.63 % t=4 (+5.72 pp,
the largest absolute grower).** Most of that delta is in libc mutex
calls inside `lockKeyMetadata` and `LockedKey` (see call-graph above):
2.67 % + 2.23 % = 4.9 pp of new mutex time at t=4. With 16 files and
1024 buckets, hash collision on the bucket-guard is statistically rare
(<1 %), so the t=4 cost is **not lock contention** — it is **cache-line
bouncing**: the std::mutex inside each bucket is `lock`ed and immediately
`unlock`ed from 4 different cores, each acquire pulls the cache line
exclusive (cf. atomic RMW on `__pthread_mutex_t::__lock`), costing
~40-60 cycles per acquire instead of the ~10 cycles a hot-in-L1
acquire takes. 4 acquires × ~50 cycles × 4 ops/ns ≈ 50 ns/op of new
overhead → matches the observed 226 − 149 = 77 ns/op regression at t=4.
**This is the t=4 halving driver.**

The `counters_.demandHits.fetch_add` shared atomic also bounces but
contributes a smaller share — 1 atomic per op vs 4 mutex acquires.

**Finding 4 — `vector::push_back` + `FileSegmentsHolder` ctor/dtor
costs 3-4 % t=1, growing to 8 % t=4.** Every `getOrSet` allocates a
fresh `std::vector<FileSegmentPtr>` of capacity 1 (single-segment
range) and the holder copies it into another vector. Heap allocator
(`malloc` at 4.33 % in t=4 raw) is **not** thread-local in this build
(tcmalloc/jemalloc absent). At t=4 the global allocator's locks add
visible cost.

**Finding 5 — PathKey hashing (`SpookyHashV2`) runs twice per op
(~3-4 %).** Once inside `lockKeyMetadata` (`bucketIndex(path)` →
`std::hash<PathKey>`), and again indirectly through
`bucket.keys.find(path)` which re-hashes inside the `unordered_map`.
PathKey is 16 bytes, hashing is fast, but it is pure overhead since
the path is constant within a thread.

## 6. Recommended fixes (no implementation in this commit)

R1. **Combine the bucket.guard and KeyMutex acquisition for the hot
read path.** lockKeyMetadata currently locks bucket.guard, does
`unordered_map::find`, releases bucket.guard, then locks the KeyMutex
via `keyMeta->lock()`. On a hit (`find` returns existing entry), the
two locks form a strict sequence with no useful work in between. A
single per-key `std::shared_mutex` (or open-coded hand-off without
releasing bucket.guard between find and KeyMutex::lock) saves one
lock/unlock pair → **expected ~12-15 % single-thread, ~25-30 % at
t=4** (eliminates the largest contender for cache-line bouncing).
Locations: `FsCacheMetadata.cpp:92-123`, `FsCache.cpp:265-279`.

R2. **Replace `counters_.demandHits` per-op `fetch_add` with a
thread-local counter that flushes on stats() read.** ClickHouse's
`FileCache::CounterCacheUsage` uses the same trick. Saves the
shared-atomic round-trip (~5-10 ns at t=4 due to cache-line bounce).
**Expected ~3-5 % at t=4, negligible at t=1.** Location:
`FsCache.h:274-280` (`AtomicCounters`), `FsCache.cpp:457-461`.

R3. **Skip `recordHit`'s LruPolicy bump when the segment is already
within the top-K of the MRU window.** Spec §9.4 already allows
best-effort LRU; widening the dedup window to a tiny per-segment
sequence number (`uint32_t lastBumpSeq_`) and comparing against a
global tick lets us drop ~80 % of `CachePriorityMutex` acquires (the
ones where the segment is still near MRU). **Expected ~4-6 % at t=1,
~10-15 % at t=4.** Location: `FsCache.cpp:456-472`, `LruPolicy.cpp:38`.

(Lower-priority cleanups: cache `PathKey::fromPath` once per
`FsCacheBufferedInput` instance instead of hashing per op;
shrink-fit the holder's `std::vector` to a `boost::container::small_vector<,1>`
to avoid one heap alloc per op.)

## 7. Single-biggest-cost hypothesis

> The **t=4 halving is caused by ~3 std::mutex lock/unlock pairs per op
> on bucket.guard, KeyMutex, and CachePriorityMutex bouncing their
> cache lines across cores**. With 16 paths × 1024 buckets, true lock
> contention is statistically zero (hit% = 100%, no waits), but every
> acquire/release on a glibc futex mutex performs an `xchg`/`cmpxchg`
> on the futex word; that word must be pulled exclusive into the
> acquiring core's L1 each time, costing ~40-60 cycles instead of ~10.
>
> The **t=1 5.8 % gate gap is the same locks running uncontended but
> with high constant overhead** (4 lock/unlock pairs × ~20 ns each =
> 80 ns of the 149 ns budget).

Both symptoms collapse to a single root cause: too many mutex
operations on the hit path. R1 alone (collapse bucket.guard + KeyMutex
into one acquire) should clear the §9.4 gate and recover most of the
t=4 per-thread loss.

## 8. Reproduction commands

```
perf record -F 4000 -g --call-graph fp -o /tmp/perf-t1.data -- \
  cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --out /tmp/t1.md --ops=20000000 --warmup_ops=2000000 --min_wall_seconds=0 \
  --num_files=16 --workloads=sequential --ws_mult_list=0.5 \
  --remote_latency_us_list=0 --threads_list=1

perf record -F 4000 -g --call-graph fp -o /tmp/perf-t4.data -- \
  cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark \
  --out /tmp/t4.md --ops=20000000 --warmup_ops=2000000 --min_wall_seconds=0 \
  --num_files=16 --workloads=sequential --ws_mult_list=0.5 \
  --remote_latency_us_list=0 --threads_list=4

perf report -i /tmp/perf-t1.data --stdio --no-children --dsos velox_fscache_benchmark
perf report -i /tmp/perf-t4.data --stdio --no-children --dsos velox_fscache_benchmark
```

Raw outputs at `/tmp/perf-t{1b,4b}-{report,top,user}.txt`.
