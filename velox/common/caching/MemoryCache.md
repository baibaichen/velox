# MemoryCache

MemoryCache<Key, Value, Map> is a header-only cache. Map has no default.
Entry and coherent state are private. Set returns the supplied shared_ptr,
including null; this does not mean admission succeeded. Values are shared,
not copied, and callers synchronize mutations of the pointed-to object.

## Contracts

- Map supplies concurrent cvisit/visit, cvisit_all, try_emplace,
  erase_if(key,predicate), and size. Visitors cannot reenter that map.
  Mutating primitives require a strong logical guarantee. Production visitors
  only compare identity or exchange owners and do not throw. String/shared_ptr
  with Boost.Unordered 1.84 and standard allocation is the validated first
  combination. Throwing migration/hash/allocator-construct implementations
  are not supported merely because they compile.
- Duration is signed 100ns ticks. TimePoint is UTC, Unix epoch, limited to the
  .NET year 1–9999 range. An empty clock uses system UTC. A custom function
  owns its captures and must support concurrent calls.
- Negative size/limit and nonpositive relative/sliding expiry throw
  invalid_argument. Missing required size throws logic_error. UTC overflow
  throws out_of_range. Allocation failures propagate bad_alloc.
- Stats are diagnostic snapshots. No limit means estimated size is absent.
  Clear exchanges coherent state; in-flight old operations stay on old state.
  Folly hazard holders protect state without shared reference-count updates.
  Clear conditionally removes collected old entries outside visitor locks.
  A per-Core tagged cohort reclaims remaining retired states at Core teardown;
  accepted tasks keep Core alive until their holders are gone.
- Capacity follows the fixed C# CAS algorithm. A stale prior-entry credit can
  admit a fallback insert above the limit; size remains accounted for. The
  controlled K=6/Q=10 interleaving produces size 16 in both implementations.
  Capacity comparison uses unsigned 128-bit intermediates: the source's
  concurrent MaxValue reservations admit a second MaxValue entry and leave
  size -2; C++ rejects that overflowing admission instead.
- Shared maintenance uses one fixed Folly worker and a preallocated queue of
  4096 tasks shared across instances (an explicitly accepted limit).
  Each instance runs at most one scan. A qualifying trigger during a scan is
  retained, and the accepted task scans again after normal or exceptional
  completion; multiple triggers may coalesce into one follow-up scan.
  Unlike C#, scans do not overlap within an instance, so cleanup can be later.
  Capacity compaction retains its separate per-instance gate.
  QueueFullException means submission was rejected before publication.
  Published entries retain their size responsibility if later scheduling fails.
  Tasks own the core and an executor KeepAlive. Cache destruction closes
  admission and does not wait for maintenance or implicitly clear entries.
- report() is an explicit sampling entry point for an application-owned
  periodic collector; the cache does not register or schedule automatic reports.
  This explicit calling convention is an accepted integration choice.
  It serializes each instance, advances each counter's
  cursor before trying delivery, and never retries a failed/missing interval.
  Hits/misses/evictions use SUM deltas; entries/size use AVG samples, not gauges.
  Names are velox.memory_cache.<name>.<metric>. Instances sharing a name
  aggregate together. Report does not mutate internal stats. External callers
  must not race object destruction with a member invocation.

## Source and validation

Algorithms and business assertions derive from .NET runtime commit
6f1d9331b9b477df73982a0fabedefe27f36d8a3. See MemoryCache.LICENSE for upstream
MIT terms. Excluded: tokens, callbacks, public entries, factories, linked
entries, Dispose/GC, DI and MeterFactory.

Dedicated CTest targets: memory_cache_test, memory_cache_contract_test,
memory_cache_no_boost_test. Debug enables TestValue interleavings. The checked-in
1000-operation trace compares values, keys, size, stats and internal reasons.
MEMORY_CACHE_SANITIZERS=address,undefined (or thread) instruments these test
translation units and the cache template, not prebuilt dependency libraries.

The user has relaxed the 398-line constraint for this performance work.
The earlier 1992-line C# source accounting remains documented in development
records; correctness and performance take priority during this revision.

For hazard-pointer TSAN validation, configure MEMORY_CACHE_SANITIZERS=thread
and MEMORY_CACHE_TSAN_FOLLY=ON. This additionally instruments bundled Folly
translation units; template-only instrumentation can miss Folly synchronization
and produce incomplete reports. Other dependencies are not fully instrumented.

## State publication and reclamation

The published state is an atomic raw pointer. Each operation holds a Folly
hazard holder until all map/accounting work is complete; it does not update a
shared state reference count. Before publication a state is tagged with its
Core's cohort. Clear exchanges a fully constructed state, then retires the old
state even on exceptions. Collected old entries are marked Removed and erased
conditionally by identity, with successful deletions charged only to old size.
Concurrent old writers remain valid and cannot modify the new state's size.

The Core outlives all holders: foreground calls cannot race cache destruction,
and accepted background tasks own Core. Core destruction deletes the current
state and lets its cohort reclaim remaining retired states. No task wait is
introduced into cache destruction. The atomic last-scan timestamp allows a
lock-free interval check; maintenance admission still rechecks under its mutex
and preserves pending rescans.
