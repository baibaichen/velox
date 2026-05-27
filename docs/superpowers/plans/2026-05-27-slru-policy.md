# Plan: SLRU Eviction Policy (CH-aligned, default-on)

Author: planning-only round. No production code touched.
Spec: `docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md` §8.1
Predecessors: Tasks 1-16 of `2026-05-26-fscache-ch-aligned-redesign.md` (all
committed; this plan replaces the stale Task 13 sketch at lines 3635-... of
that file).

---

## 1. Files

Create:

- `velox/common/caching/fscache/SlruPolicy.h` — concrete `EvictionPolicy`
  subclass holding two `LruPolicy`-equivalent intrusive lists + a per-segment
  `is_protected` flag.
- `velox/common/caching/fscache/SlruPolicy.cpp` — implementation.
- `velox/common/caching/fscache/tests/SlruPolicyTest.cpp` — UTs driving the
  contract (5 cases — see Task B).

Modify:

- `velox/common/caching/fscache/FsCacheConfig.h` — add
  `EvictionPolicyKind { kLru, kSlru }` enum, `evictionPolicy` field default
  `kSlru`, `slruProtectedRatio` field default `0.6` (matches CH
  `FILECACHE_DEFAULT_SLRU_RATIO`, `FileCache_fwd.h:26`).
- `velox/common/caching/fscache/FsCacheMetadata.h` /
  `FsCacheMetadata.cpp` — extend the existing `PolicyFactory`
  (`FsCacheMetadata.h:59`) callsite in `FsCache.cpp` to choose
  `LruPolicy` vs `SlruPolicy` from config; **no interface change to
  `FsCacheMetadata` itself**.
- `velox/common/caching/fscache/FsCache.cpp` — at construction, build a
  `PolicyFactory` lambda that branches on `config.evictionPolicy` and passes
  through `slruProtectedRatio` + per-bucket capacity.
- `velox/common/caching/fscache/CMakeLists.txt` — add `SlruPolicy.cpp`.
- `velox/common/caching/fscache/tests/CMakeLists.txt` — add
  `SlruPolicyTest.cpp` test target.
- `docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md` — rewrite
  §8.1 promotion semantics (see Task G + §7 Spec amendment).
- `docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md` — append Round-13 SLRU
  re-baseline entry (see Task F).

**No change to** `velox/common/caching/fscache/EvictionPolicy.h` — see Task A.

---

## 2. Spec refs + CH alignment evidence

- Spec §8.1 (current text incorrect — promotes on `hits_ >= 2`; see §7 below).
- CH default policy: `FileCache_fwd.h:21`
  `static constexpr auto FILECACHE_DEFAULT_CACHE_POLICY = FileCachePolicy::SLRU;`
- CH default ratio: `FileCache_fwd.h:26`
  `static constexpr double FILECACHE_DEFAULT_SLRU_RATIO = 0.6;` (60% protected,
  40% probationary).
- CH settings DECLARE: `FileCacheSettings.cpp:42-43`
  `cache_policy = FILECACHE_DEFAULT_CACHE_POLICY` / `slru_size_ratio = FILECACHE_DEFAULT_SLRU_RATIO`.

---

## 3. Approach

### What CH actually does (line-cited)

CH `SLRUFileCachePriority` (`SLRUFileCachePriority.h:14-205`,
`SLRUFileCachePriority.cpp:35-924`) holds **two `LRUFileCachePriority`
sub-queues** as data members (`SLRUFileCachePriority.h:138-139`:
`LRUFileCachePriority protected_queue; LRUFileCachePriority probationary_queue;`).
The split is **by size**, computed from `slru_size_ratio` via
`getRatio(max_size, ratio)` (`SLRUFileCachePriority.cpp:27-32, 45-52`).
`size_ratio = 0.6` ⇒ protected gets 60 % of `max_size` and 60 % of
`max_elements`; probationary gets the remaining 40 %.

**Insert / miss path** (`SLRUFileCachePriority.cpp:126-161`): new entries
always go to **probationary** in the steady-state. Only the startup
`is_initial_load` branch falls back to protected when probationary
cannot fit (`:144`). There is no ghost list.

**Promote-on-hit semantics** (`SLRUFileCachePriority.cpp:586-730`,
`tryIncreasePriority`):

1. If already in protected → reorder within protected (`:597-601`).
2. Else if `!is_space_reservation_complete` (segment not fully downloaded)
   → just reorder within probationary (`:602-611`).
3. Else (in probationary AND fully downloaded) → **promote to protected**
   on this hit (`:613` onward). **No hit counter.** First post-download hit
   promotes.

If protected has no room for the promoted entry, CH **downgrades** the
LRU end of protected back to probationary (`collectCandidatesForEvictionInProtected`,
`SLRUFileCachePriority.cpp:398-584`). Downgrade is a move, not an evict —
size is transferred via a two-phase add-empty / increment-size dance
(`:535-573`) under separate `WriteLock` and `state_lock`. If probationary
also cannot fit the downgraded entries, those entries are evicted from
the cache entirely (`:457-471`).

**Evict semantics** (`SLRUFileCachePriority.cpp:199-274`,
`collectEvictionInfo`): when a reservee is given, eviction drains the queue
**the reservee lives in** — probationary entries trigger probationary
eviction, protected entries trigger protected eviction (with downgrade).
First-time reservation (`!reservee`, `:336-350`) drains probationary only.
For `is_total_space_cleanup` (background free-space upkeep), it drains
probationary first then protected (`:213-247`). The `size_ratio` is a
**size split, not a count split**; `max_elements` is also split by the
same ratio (`:45-52`).

**Concurrency**: each sub-queue has its own atomic state but they share
the SLRU-level `CachePriorityGuard` + `CacheStateGuard` (`SLRUFileCachePriority.cpp:586-730`
acquires both for promotion). One mutex per cache, not per list.

**Stats**: CH exposes `getProtectedSize` / `getProbationarySize` / element
counts separately (`SLRUFileCachePriority.h:35-38`) and a combined log
string (`:905-915`). Hit / miss counters are not split per sub-queue.

### Existing skeleton's inventions (per 八荣八耻 #7)

The Task 13 skeleton in `2026-05-26-fscache-ch-aligned-redesign.md`
(lines ~3635-3990) is materially wrong vs CH:

1. **"Promote on second touch"** (line 3651, 3706-3712 of redesign plan)
   — CH does *not* count hits; promotion is on the first post-download hit
   while in probationary (cited above).
2. **Template `SlruPolicy<Entry>` with `shared_ptr<Entry>`** (line 3820-3833)
   — our existing `EvictionPolicy` takes `FileSegment*` directly
   (`EvictionPolicy.h:36-49`), and the existing `LruPolicy` already follows
   that convention. SLRU should mirror, not introduce a parallel template.
3. **`evictUntilUnder(targetBytes)` API** (line 3833) — diverges from the
   existing `selectVictims(bytesNeeded)` contract
   (`EvictionPolicy.h:49`). The skeleton would force changing every
   FsCache eviction call site for no gain.
4. **`enableSlru = false` opt-in default** (line 3641) — wrong relative to
   CH, which defaults SLRU **on** (`FileCache_fwd.h:21`). This plan flips
   the default per spec §8.1 alignment goal.
5. **Protected ratio = 0.5 then 0.7** (line 3641 vs spec §8.1 line 895)
   — CH default is 0.6 (`FileCache_fwd.h:26`). The plan uses 0.6.

This new plan adopts CH's "two `LruPolicy` instances inside one class,
promote on first post-download hit, size-split capacity, share one mutex"
shape and drops the template / `shared_ptr<Entry>` / `evictUntilUnder`
inventions.

### Data structure choice

`SlruPolicy` holds two **`LruPolicy` data members** (not pointers), mirroring
CH (`SLRUFileCachePriority.h:138-139`). Each `LruPolicy` already implements
the four `EvictionPolicy` methods. `SlruPolicy` dispatches by looking up
the segment in a small `unordered_map<FileSegment*, bool> isProtected_`
side index (CH uses an iterator-side `is_protected` field; we cannot embed
this in `LruPolicy`'s map without breaking its encapsulation, so we keep a
parallel index).

Per-sub-queue capacity is **a soft accounting target** — `LruPolicy` does
not currently know its capacity; eviction is driven by FsCache asking for
`bytesNeeded`. `SlruPolicy` therefore needs to track per-sub-queue **byte
counts** internally (`probationaryBytes_` / `protectedBytes_`), plus
`probationaryCap_` / `protectedCap_` constants (derived from the
per-bucket budget × ratio) used only to decide **when to downgrade** on
promotion. Eviction `bytesNeeded` is still passed in from FsCache —
SlruPolicy just decides which queue to drain.

### Concurrency

`SlruPolicy` inherits the same non-thread-safety contract as `LruPolicy`
(`LruPolicy.h:31-33`: "callers must hold the appropriate FsCacheGuards
lock"). Per-bucket `CachePriorityMutex` already serialises all four
methods; no new locks.

---

## 4. Tasks (5-phase rhythm per task)

Each task follows the rhythm used for Tasks 1-16:
**implement → spec review → simplify → post-simplify review → commit**.

### Task A: `EvictionPolicy` interface — keep or extend?

**Decision: no interface change.** The four methods (`onInsert`, `onHit`,
`onRemove`, `selectVictims`) already give SLRU everything it needs:

- `onInsert` → push to probationary (mirrors CH `add` default branch,
  `SLRUFileCachePriority.cpp:126-161`).
- `onHit` → decide promote / reorder (CH's `tryIncreasePriority`,
  `SLRUFileCachePriority.cpp:586-611`).
- `onRemove` → drop from whichever sub-queue holds it.
- `selectVictims(bytesNeeded)` → drain probationary first; if exhausted,
  drain protected (matches CH `is_total_space_cleanup` path,
  `SLRUFileCachePriority.cpp:213-247`).

The CH-specific `tryIncreasePriority`'s `is_space_reservation_complete`
gate is **handled at the FsCache call site, not in the policy**: FsCache
only calls `onHit` from already-`kDownloaded` segments (the same precondition
`LruPolicy::onInsert` enforces today, `LruPolicy.cpp:29-32`). So the
"only-promote-when-fully-downloaded" guarantee is naturally upheld
without a new method.

八荣八耻 #4 (创造接口) — no new interface methods.
八荣八耻 #6 (破坏架构) — existing `LruPolicy` callers untouched.

**Round outputs:** decision memo in the commit message; no code yet.

### Task B: `SlruPolicy.h` skeleton + 5 UT cases (TDD red)

Header:

```cpp
class SlruPolicy final : public EvictionPolicy {
 public:
  SlruPolicy(uint64_t totalBytes, double protectedRatio);
  void onInsert(FileSegment*) override;
  void onHit(FileSegment*) override;
  void onRemove(FileSegment*) override;
  std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) override;

  // Test-only observers (no friend; public methods, mirroring CH's
  // getProtectedSize/getProbationarySize at SLRUFileCachePriority.h:35-38).
  uint64_t probationaryBytes() const;
  uint64_t protectedBytes() const;

 private:
  LruPolicy probationary_;
  LruPolicy protected_;
  std::unordered_map<FileSegment*, bool> isProtected_;
  uint64_t probationaryBytes_{0};
  uint64_t protectedBytes_{0};
  const uint64_t probationaryCap_;  // (1 - ratio) * totalBytes
  const uint64_t protectedCap_;     // ratio * totalBytes
};
```

UTs (`SlruPolicyTest.cpp`, 5 cases, each cites the CH behavior it verifies):

1. **probationaryOnFirstInsert** — `onInsert(seg)`; `probationaryBytes() == seg.size()`,
   `protectedBytes() == 0`. Cites `SLRUFileCachePriority.cpp:126-161` default branch.
2. **promoteOnFirstHit** — `onInsert(seg); onHit(seg);` → `protectedBytes() == seg.size()`,
   `probationaryBytes() == 0`. Cites `SLRUFileCachePriority.cpp:613` onward
   (first post-download hit promotes). This is the case that REPLACES the stale
   "second-touch" skeleton (八荣八耻 #7 — earlier skeleton invented this).
3. **protectedOverflowDemotesLruProtected** — fill protected to capacity, then
   promote one more from probationary; oldest protected entry moves back to MRU
   end of probationary. Verify byte accounting + that the demoted segment is
   reported by a subsequent `selectVictims` ahead of the freshly-promoted one.
   Cites `SLRUFileCachePriority.cpp:398-584` `collectCandidatesForEvictionInProtected`.
4. **selectVictimsDrainsProbationaryFirst** — fill probationary AND protected,
   call `selectVictims(N)` where `N` ≤ probationary total; all victims come
   from probationary LRU end. Cites `SLRUFileCachePriority.cpp:213-247`
   (probationary-first drain in `is_total_space_cleanup`).
5. **ratioRespectedInCapacities** — construct with `(totalBytes=10, ratio=0.6)`,
   verify `probationaryCap_ == 4` and `protectedCap_ == 6` (via observer methods
   exposing caps, or via behaviour: fill to caps and observe demote / drain).
   Cites `SLRUFileCachePriority.cpp:45-52` + `FileCache_fwd.h:26`.

**Round outputs:** RED build with header missing → header skeleton + UT
file → RED test run (impl methods empty / throw).

### Task C: `SlruPolicy.cpp` implementation (GREEN)

Method bodies:

- `onInsert(seg)`: `probationary_.onInsert(seg); isProtected_[seg] = false;
  probationaryBytes_ += seg->size();`
- `onHit(seg)`: lookup in `isProtected_`; if `true` → `protected_.onHit(seg)`;
  else → promote: `probationary_.onRemove(seg); probationaryBytes_ -=
  seg->size();` then evict-or-demote protected if `protectedBytes_ +
  seg->size() > protectedCap_` (call `protected_.selectVictims(delta)`,
  for each victim: `protected_.onRemove(v); protectedBytes_ -= v->size();
  probationary_.onInsert(v); isProtected_[v] = false; probationaryBytes_ +=
  v->size();`), then `protected_.onInsert(seg); isProtected_[seg] = true;
  protectedBytes_ += seg->size();`.
- `onRemove(seg)`: dispatch on `isProtected_`, decrement counter, erase
  from index.
- `selectVictims(bytesNeeded)`: drain `probationary_.selectVictims(bytesNeeded)`
  first; if accumulated `< bytesNeeded`, drain
  `protected_.selectVictims(remaining)`. Concatenate, return. (Mirrors
  CH `is_total_space_cleanup` ordering at `SLRUFileCachePriority.cpp:213-247`.)

**Edge cases to verify per 八荣八耻 #5 (验证):**

- Demote ordering: oldest protected demotes to MRU of probationary (so it
  is more likely to be re-promoted than fresh probationary tails). Verified
  by UT case 3.
- `seg->size()` is stable between `onInsert` and `onRemove` — `FileSegment`
  size is fixed after `kDownloaded` (`FileSegment.h:221` and surrounding
  state machine). No re-sizing in our port (we do not implement CH's
  partial-segment growth).

**Round outputs:** all 5 UTs GREEN; `ctest -R SlruPolicy` passes locally.

### Task D: `FsCacheConfig.h` adds enum + ratio + default flip

Append to `FsCacheConfig`:

```cpp
enum class EvictionPolicyKind { kLru, kSlru };

/// Default kSlru to match ClickHouse FileCache default
/// (FileCache_fwd.h:21 FILECACHE_DEFAULT_CACHE_POLICY = SLRU).
/// Spec §8.1's "opt-in default off" was a phase-1 divergence; this
/// plan reverses it.
EvictionPolicyKind evictionPolicy{EvictionPolicyKind::kSlru};

/// Protected-list fraction of per-bucket budget. CH default
/// (FileCache_fwd.h:26 FILECACHE_DEFAULT_SLRU_RATIO = 0.6).
double slruProtectedRatio{0.6};
```

**Behaviour-change note** in the field doc-comment: callers who were on
the implicit `LruPolicy` default in phase-1 will now get SLRU. To preserve
phase-1 behaviour, set `evictionPolicy = kLru` explicitly. The
`FsCache_benchmark` defaults should be updated in Task F to allow A/B
comparison.

**Round outputs:** config compiles; one downstream unit test (existing
`FsCacheTest`) may need an explicit `kLru` to keep its existing assertions —
audit during commit.

### Task E: `FsCache.cpp` factory chooses LruPolicy vs SlruPolicy

Locate the `PolicyFactory` callsite for `FsCacheMetadata`
(`FsCacheMetadata.h:59-70`) — currently defaults to
`[] { return std::make_unique<LruPolicy>(); }`. In `FsCache::FsCache(config)`,
override to:

```cpp
const auto perBucketBytes = config.maxBytes / config.numBuckets;
PolicyFactory factory;
switch (config.evictionPolicy) {
  case EvictionPolicyKind::kLru:
    factory = [] { return std::make_unique<LruPolicy>(); };
    break;
  case EvictionPolicyKind::kSlru:
    factory = [perBucketBytes, ratio = config.slruProtectedRatio] {
      return std::make_unique<SlruPolicy>(perBucketBytes, ratio);
    };
    break;
}
```

Per-bucket budget split mirrors CH's per-cache split — SLRU operates at
**the same scope as LRU does in our port** (per-bucket, since FsCache's
mutex granularity is per-bucket, not per-cache; see `FsCache.h:244`).
This is a deliberate Velox-side adaptation; CH has only one cache-level
queue. Document the divergence in spec §8.1.

**Round outputs:** an end-to-end FsCache test runs with SLRU default and
passes existing functional assertions (read-after-write, evict-on-full).

### Task F: re-baseline benchmarks under SLRU default

In `docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md`, append a
"Round-13 SLRU" section:

1. Re-run `velox_fscache_benchmark` with `evictionPolicy = kSlru, ratio = 0.6`
   AND with `kLru` for A/B. Record p50 / p99 / hit-rate delta.
2. Re-run TPC-H SF=0.01 equivalence (per existing Round-11 plan) under
   `kSlru` — expect bit-identical query results (eviction policy is
   transparent to correctness).
3. Re-run TPC-H SF=100 sweep (Round-12) under both policies. Capture the
   delta on Q1/Q6/Q21 (scan-heavy) and Q5/Q9 (join-heavy with re-reads).
   Per spec §10 R5: SLRU is expected to lose on cold-scan workloads where
   nothing is re-read; spec acknowledges this. Document.

**Round outputs:** A/B perf table appended to `2026-05-26-fscache-tpch-ab.md`
with hard numbers; if SLRU regresses badly on Q1/Q6, **do not flip the
default silently** — escalate per 八荣八耻 #3 (人类确认).

### Task G: spec §8.1 amendment

Rewrite lines 873-896 of
`docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md` to
match CH semantics (no hit counter, promote on first post-download hit,
0.6 default ratio, default-on). Exact line changes listed in §7 below.

**Round outputs:** spec PR diff + commit.

---

## 5. Verification (per-task green gates)

| Task | Green gate |
|------|------------|
| A | Decision memo in commit; no code; reviewer agrees interface stays. |
| B | `velox_slru_policy_test` builds, 5 UTs RED with empty impl. |
| C | All 5 UTs GREEN under `ctest -R SlruPolicy`. |
| D | Config field compiles; `velox_fscache_test` still GREEN (audit existing tests for implicit-policy assumptions). |
| E | An end-to-end FsCache integration test (existing `FsCacheTest` suite) GREEN with SLRU default. |
| F | A/B perf numbers logged in `2026-05-26-fscache-tpch-ab.md`. SLRU win on join/skew workloads; LRU floor on cold scans known and accepted. |
| G | Spec diff approved by user; commit on spec branch. |

---

## 6. Hard rules

- **八荣八耻 #1 (瞎猜)**: every CH semantic claim in this plan carries a
  `path/file.cpp:line` citation. If a reviewer cannot trace a claim back to
  source, treat it as a bug in the plan.
- **#4 (创造接口)**: no new methods on `EvictionPolicy` (Task A justification).
- **#6 (破坏架构)**: `LruPolicy` callers continue to work; SLRU is selected
  at construction time via `PolicyFactory` (Task E).
- **#7 (假装理解)**: the redesign plan's Task 13 skeleton at lines 3635-...
  invented (a) hit-counter promotion, (b) `evictUntilUnder` API, (c)
  template entry type, (d) 0.5/0.7 ratios, (e) opt-in default-off. All five
  are documented as inventions in §3 above and **none are adopted**.

---

## 7. Spec amendment (exact line changes)

In `docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md`:

- **Lines 873-877** (current text):
  > `### 8.1 SlruPolicy`
  > `CH 默认策略：每个 cache 维护 probation list 和 protected list，新插入`
  > `段进 probation；命中两次（hits_ >= 2）promote 到 protected。protected`
  > `满了 demote 到 probation。两个 list 各自 LRU。`

  Replace with:

  > `### 8.1 SlruPolicy（CH 对齐，默认开启）`
  > `CH 默认策略 (FileCache_fwd.h:21 FILECACHE_DEFAULT_CACHE_POLICY=SLRU)：`
  > `每个 cache 维护 probationary list 和 protected list，新插入段进 probationary；`
  > `命中时若段已 kDownloaded 且当前在 probationary，则 promote 到 protected`
  > `(SLRUFileCachePriority.cpp:586-611)，不依赖 hit 计数器。protected 满则`
  > `demote 其 LRU 端回 probationary (:398-584)；若 probationary 也满，被`
  > `demote 的段才真正驱逐 (:457-471)。两个 sub-queue 各自 LRU，共享一把`
  > `bucket 锁。`

- **Lines 891-894** (`SlruPolicy::onHit` 检查 `seg->hits_.load()` … `>= 2`):
  delete. Replace with:

  > `SlruPolicy::onHit(seg)：如果 seg 已在 protected，仅在 protected 内 reorder；`
  > `否则 promote 到 protected（若 protected 容量不足，先 demote LRU 端的`
  > `protected 段回 probationary）。无 hit 计数器，与 CH 完全一致。`

- **Line 895** `slruProtectedRatio（默认 0.7）` → `slruProtectedRatio（默认 0.6，`
  `匹配 CH FileCache_fwd.h:26 FILECACHE_DEFAULT_SLRU_RATIO=0.6）。`

- **Line 896** `LruPolicy 保留作为可选 … FsCacheConfig::evictionPolicy：kLru | kSlru` →
  保留语义，但补一句：`默认 kSlru（与 CH 一致）；想要 phase-1 LruPolicy 行为的`
  `caller 需要显式设置 kLru。`

- Spec §10 R5 (line 1095-1098) remains valid — it already acknowledges cold-scan
  regression. No change.

---

## 8. Open questions requiring user input before Task D/E

1. **Default flip**: phase-1 callers built against `LruPolicy` (the implicit
   default) will silently switch to `SlruPolicy` once Task D lands. Acceptable,
   or should the default remain `kLru` for one release and only be flipped
   after Round-13 perf is collected? (Recommendation: flip in same PR as
   Task F, gated on perf data; do NOT flip in Task D alone.)
2. **Per-bucket vs per-cache scope**: Velox's FsCache shards by bucket; CH's
   FileCache has one queue per cache. We adopt per-bucket SLRU (Task E). This
   means each bucket's protected list is `(1/numBuckets) × ratio × maxBytes`.
   For `numBuckets=1024` and a 1 GiB cache, protected per bucket is ~600 KiB —
   roughly 18 × 32 MiB segments. Acceptable, or should we add a future task
   to consolidate SLRU at the cache level?
3. **`hits_` counter retirement**: `FileSegment::hits_`
   (`FileSegment.h:221`) was added under the assumption SLRU would use it
   (spec §8.1 old text). With CH-aligned SLRU it is unused for promotion.
   Keep for stats / observability, or remove in a follow-up?

---

## 9. Task F outcome (2026-05-28)

**Decision: B — keep `enableSlru = false` default.** See
`docs/superpowers/results/2026-05-27-fscache-slru-vs-lru.md` for the 3
data tables.

- Microbench: SLRU wins +90 % on zipfian-eviction (ws_mult=2, t=16), loses
  -29 % on sequential-eviction and -14 % on t=1 uniform hot path.
- SF=0.01 equivalence: 22/22 PASS under SLRU (correctness clean).
- SF=100 sweep: SLRU 0.4 – 1.7 % faster on q01/q06/q14/q19/q22 medians,
  inside noise. Working set fits in cache → 0 eviction → SLRU's mechanism
  never engages.

**Deferred** (not blocking phase-1):

- Cache-level SLRU (vs per-bucket) — required for SLRU to protect a
  meaningful hot tail. Per-bucket protected ≈ 1 segment at 1024 buckets,
  64 GiB cap, ratio 0.6.
- Hit-path SLRU cost (-13.7 % t=1 uniform) — fold `isProtected_` into
  LruPolicy map entry to drop one hash lookup off `onHit`.
