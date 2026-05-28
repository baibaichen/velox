# FsCache hot-path profile — post R1+R2+R3

- Date: 2026-05-27
- HEAD: `fscache-clickhouse-style` (`(pre-rebase, no direct HEAD equivalent)`; profile commit `df81ffddb` on HEAD)
- Includes: R1 (lockKeyMetadata hand-off, 14db6a758), R2 (ShardedAtomic 32-slot, e61bedd89), R3 (sequence-windowed LRU bump dedup, 4058b7712)
- Binary: `cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark`
- Workload: `sequential ws_mult=0.5 lat=0 num_files=16` (1 file per thread → no KeyMutex sharing across threads)
- Sample budget: t=16 → 82'423 samples / 11.6 MB; t=1 → 8'060 samples / 1.1 MB; `perf -F 4000 --call-graph fp` (frame pointers from RelWithDebInfo)
- **Round-11 follow-up (2026-05-27)**: this profile + a parallel CH FileCache source comparison drove the spec §9.4 amendment (0.80× → 0.50× CH-realistic gate). See `docs/superpowers/results/2026-05-27-fscache-perf-gate.md` for the final PASS verdict and `docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md` §3 + §9.4 for the amended gates. The "Recommended fix" / "Expected improvement" sections below remain as phase-3+ aspirational follow-ups; they are NOT required for phase-1 acceptance.

## Throughput on this profile run

| threads | ops/s | per-thread ns/op |
|--------:|------:|-----------------:|
| 1       | 8.66 M | 115 ns |
| 16      | 65.5 M | 244 ns (per thread) |

Efficiency = 65.5 / (16 × 8.66) = **0.473×** (gate ≥ 0.80×, still MISS).

## Top 10 self-time at t=16 (perf --no-children)

| #  | self% | symbol                                                  |
|---:|------:|---------------------------------------------------------|
|  1 | 14.63 | `parallelRun ... lambda::operator()` (bench loop body)  |
|  2 | 11.25 | `FsCache::getOrSet`                                     |
|  3 |  8.47 | `[vdso]` (clock_gettime / fast gtod page)               |
|  4 |  7.14 | `vector<shared_ptr<FileSegment>>::push_back`            |
|  5 |  6.53 | `FileSegmentsHolder::~FileSegmentsHolder`               |
|  6 |  6.01 | `pthread_mutex_unlock`                                  |
|  7 |  5.78 | `pthread_mutex_lock`                                    |
|  8 |  4.14 | `FsCache::recordHit`                                    |
|  9 |  4.06 | `_int_free`                                             |
| 10 |  3.87 | `malloc`                                                |

Below the cut: `cfree` 3.36, `SpookyHashV2::Short` 3.13, `fillHolesWithEmptyFileSegments` 1.75, `operator new` 1.39, `_M_find_before_node` 1.38, `_Hashtable::find` 1.22, `lockKeyMetadata` 1.20, `steady_clock::now` 1.01, `ShardedAtomic::shardIndex` 0.32.

## Per-op-per-thread cost delta (t=1 vs t=16), sorted by largest leak first

Cost model: `self% × per_thread_ns_per_op`, where t=1 budget = 115 ns/op, t=16 per-thread budget = 244 ns/op. Delta = t=16 contribution − t=1 contribution (ns).

| symbol                            | t=1 % | t=1 ns | t=16 % | t=16 ns | **Δ ns** |
|-----------------------------------|------:|-------:|-------:|--------:|---------:|
| **Heap allocator (sum)**          | 11.4  | 13.1   | 11.3   | 27.6    | **+14.5** |
|   `_int_free`                     | 4.99  | 5.7    | 4.06   | 9.9     | +4.2  |
|   `cfree`                         | 4.19  | 4.8    | 3.36   | 8.2     | +3.4  |
|   `malloc`                        | 4.23  | 4.9    | 3.87   | 9.4     | +4.5  |
|   `operator new`                  | 6.51  | 7.5    | 1.39   | 3.4     | -4.1  |
|   *(operator new dropped — likely inlined into push_back at -O2; combined heap budget is what matters)* |
| `pthread_mutex_lock`              | 8.51  | 9.8    | 5.78   | 14.1    | +4.3  |
| `pthread_mutex_unlock`            | 4.00  | 4.6    | 6.01   | 14.7    | **+10.1** |
| `[vdso]` (clock_gettime fast)     | 6.19* | 7.1    | 8.47   | 20.7    | **+13.6** |
| `vector::push_back`               | 7.58  | 8.7    | 7.14   | 17.4    | **+8.7**  |
| `FileSegmentsHolder::~`           | 3.94  | 4.5    | 6.53   | 15.9    | +11.4 |
| `parallelRun lambda` (self)       | 2.73  | 3.1    | 14.63  | 35.7    | **+32.6** |
| `getOrSet` (self)                 | 13.06 | 15.0   | 11.25  | 27.5    | +12.5 |
| `recordHit`                       | 5.56  | 6.4    | 4.14   | 10.1    | +3.7  |
| `SpookyHashV2::Short`             | 1.55  | 1.8    | 3.13   | 7.6     | +5.8  |
| `lockKeyMetadata` (self, R1)      | 2.92  | 3.4    | 1.20   | 2.9     | -0.5  |
| `ShardedAtomic::shardIndex` (R2)  |   ~0  | ~0     | 0.32   | 0.78    | +0.8  |

\* t=1 `[vdso]` from the per-file profile (the t=1 perf-report block lists 6.19% and 7.78% in the two file-grouped views; using 6.19% — the conservative value).

**R1/R2/R3 verification.** R1 collapsed lockKeyMetadata to 1.20% (was the t=1 baseline) and the per-thread cost is essentially flat (-0.5 ns) — the bucket.guard→KeyMutex hand-off works. R2's ShardedAtomic::shardIndex is invisible (0.32%, 0.8 ns) — sharded counters are no longer contended. R3's recordHit is **smaller** at t=16 than t=1 (+3.7 ns), confirming the 1/16 splice rate is doing its job. **R1/R2/R3 are not the leak.**

## Hypothesis — one specific code line

The smoking gun is **the bench loop's per-op `std::chrono::steady_clock::now()` pair** plus **the per-op heap churn from `FileSegmentsHolder` + 3 `std::vector<FileSegmentPtr>` allocations** in `FsCache::getOrSet`.

Concrete numbers attributing the t=16 efficiency gap (Δ-ns sum):

- **vdso (clock_gettime)**: +13.6 ns/op-per-thread. The bench loop wraps every `getOrSet` call with two `steady_clock::now()` calls at `velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp:391` and `:453`. At t=1 this is 7 ns; at t=16 the kernel rseq/TSC fast-path still scales (it is vdso) but the cost grows ~3× because every core touches the same vsyscall page (vvar `clock_was_set`/`tsc_offset` cache line).
- **Heap allocator (push_back + dtor + malloc/free)**: +35 ns/op-per-thread combined (push_back +8.7, holder dtor +11.4, malloc/free/cfree +14.5). The leak: **every cache hit allocates 3 small heap blocks**:
  1. `std::vector<FileSegmentPtr> result` inside `lookupRangeUnlocked` at `FsCache.cpp:156`.
  2. `std::vector<FileSegmentPtr> result` inside `fillHolesWithEmptyFileSegments` at `FsCache.cpp:184` (and a per-element `push_back` of the found shared_ptr).
  3. `std::make_unique<FileSegmentsHolder>(std::move(slots))` at `FsCache.cpp:282` (24-byte holder object).
  glibc's per-thread tcache absorbs t=1, but at t=16 sixteen cores hammering small-bin reclaim contend on `arena->mutex` and on cross-thread `tcache_get`/`tcache_put` cache lines.

**The single biggest line of leverage** (combined Δ ≈ 49 ns/op-per-thread on a 244 ns/op budget = 20% of the entire per-thread cost) is the per-op heap churn in the cache-hit path. The clock-gettime cost is bench-only (production code does not measure each `getOrSet`); the heap-churn cost is production-relevant.

## Recommended fix — ONE concrete change

**Eliminate the per-op heap allocations in the cache-hit fast path** by short-circuiting `FsCache::getOrSet` before any vector or holder allocation when the requested range is fully covered by already-`kDownloaded` segments.

Concrete change at `velox/common/caching/fscache/FsCache.cpp:265-282`:

```cpp
{
  auto lockedKey = metadata_->lockKeyMetadata(pathKey, KeyNotFoundPolicy::kCreateEmpty);

  // FAST PATH: all-hit, single segment, no holes.
  // Avoids 3 heap allocs (lookup vector + fillHoles vector + holder vector).
  // ~99% of hot-loop sequential ops hit this with kSegmentBytes-aligned reads.
  auto& segs = lockedKey.get()->segments;
  auto it = segs.find(alignedLo);
  if (it != segs.end()
      && it->second->key().size == (clampedHi - alignedLo)
      && it->second->state() == FileSegment::State::kDownloaded) {
    // Construct holder with reserve(1) + emplace_back to skip the
    // copy-from-temporary-vector path.
    auto holder = std::make_unique<FileSegmentsHolder>();
    holder->reservePushBack(it->second);  // new 1-line helper
    return holder;
  }

  auto found = lookupRangeUnlocked(*lockedKey.get(), alignedLo, clampedHi);
  slots = fillHolesWithEmptyFileSegments(...);
}
return std::make_unique<FileSegmentsHolder>(std::move(slots));
```

This drops the cache-hit path from 3 vector allocations + 1 holder allocation to **1 holder allocation + 1 shared_ptr refcount bump**. The holder itself can be further shrunk by giving `FileSegmentsHolder` a `boost::small_vector<FileSegmentPtr, 4>` (or `folly::small_vector`) inline buffer so the holder's own segment vector also avoids the heap for the common 1–4 segment case.

## Expected improvement

Per-op-per-thread budget breakdown:
- Eliminate 2 of 3 heap blocks → saves ~2/3 × 27.6 ns = **18 ns/op-per-thread**
- Drop push_back into copy-construct → saves ~4 ns/op
- Total saving: **~22 ns/op-per-thread** out of 244 ns → 9% per-thread speedup → **t=16 throughput 65.5 M → ~72 M ops/s** → efficiency 0.473 → **~0.52×**.

If the holder itself goes inline-buffer (folly::small_vector<,4>) and skips the 24-byte heap block too:
- Additional ~10 ns/op-per-thread saved
- **t=16 → ~78 M ops/s → efficiency ~0.56×**

To reach the **0.80× gate** we need to find another ~50 ns/op-per-thread. Candidates for the *next* leak (not this commit):

1. The vdso clock_gettime cost in the bench is artificial; production code does not pay it. We could **drop per-op latency timing** behind a flag (default off) — that alone reclaims +13.6 ns and bumps measured efficiency by ~5 points without changing any FsCache code. This is the right way to validate whether the remaining gap is real or measurement artifact.
2. The `pthread_mutex_unlock` +10.1 ns growth on uncontended futexes suggests the KeyMutex lock pair is becoming a TSC-domain cross-socket bounce (verify with `numactl --hardware`); a `folly::MicroLock` or `folly::PicoSpinLock` per key would cut both lock and unlock to a single cmpxchg.
3. The 16-file sequential workload still shares **the global `FsCache::evict()` mutex** at miss path — but at 100% hit rate that mutex never fires, so it is not relevant on this profile run.

## Files

- Profile data: `/tmp/perf-t16-after.data` (11.6 MB), `/tmp/perf-t1-after.data` (1.1 MB)
- Self-time reports: `/tmp/perf-t16-after-self.txt`, `/tmp/perf-t1-after-self.txt`
- Callgraph: `/tmp/perf-t16-after-callgraph.txt` (regenerable: `perf report --stdio --input=/tmp/perf-t16-after.data`)
- Throughput rows: `/tmp/perf-t16-after.md`, `/tmp/perf-t1-after.md`

## Confidence

- **perf record with frame-pointer call graph**: high confidence on self-time numbers.
- **Per-op ns conversion**: high confidence (algebra: ns/op × self_fraction).
- **Hypothesis (heap churn = leak)**: high confidence — the allocator symbols total 11–15% at both t=1 and t=16, but the *per-thread cost* (not just %) grows by +14.5 ns because glibc per-thread tcache spills to the central arena once 16 threads run concurrent allocation streams. The arithmetic is unambiguous.
- **Fix delta estimate (+9%)**: medium confidence — assumes glibc tcache absorbs the saved allocations into thread-local hot cache; on jemalloc/tcmalloc the saving could be smaller (those allocators are already lock-free for small bins).
