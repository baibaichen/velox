# Folly ConcurrentHashMap benchmark

Built by the main Velox CMake project with VELOX_ENABLE_BENCHMARKS=ON.
Uses that build's Folly/gflags/glog targets; no private dependency installation.

~~~bash
cmake --build cmake-build-relwithdebinfo --target concurrent_hash_map_bench -j 30
~~~

From the repository root, run correctness checks only:

~~~bash
DOTNET=/home/chang/.dotnet/dotnet velox/benchmarks/concurrent_dictionary/run.sh check
~~~

After explicit user approval, run both full matrices sequentially:

~~~bash
DOTNET=/home/chang/.dotnet/dotnet velox/benchmarks/concurrent_dictionary/run.sh run --approved
~~~

The runner writes timestamped logs and JSON under concurrent_dictionary/results.
It does not build automatically; C# must also be built with dotnet build -c Release.
The --approved flag is an explicit execution gate, not a substitute for user consent.
BUILD_DIR, DOTNET, BENCH_THREADS and RESULTS_DIR can be overridden.
Folly uses its default measurement policy; C# uses ShortRun. Their aggregation
policies differ, so results are not identically measured statistics.

## Matching and differences

- Same 1024/65536 sizes, integer/string keys, scalar/object values, per-worker
  xorshift32 trace, hot-key distribution and write positions as C#.
- Steady-state batches contain 262144 total logical operations, with persistent
  workers and two barriers. Setup and teardown are excluded. Each Folly trial
  starts a fresh map and runs a warmup batch; C# retains state across BDN batches.
- Hit/Miss only check existence (as C# currently does); they do not copy a
  shared_ptr out to a caller. Conditional operations copy the old value and
  release the hazard iterator before mutation. Object values use shared_ptr and
  identity equality; the same 256-value pool is reused, including recurring
  identities. Real per-write object allocation is only in InsertNewValues.
- TryUpdate maps to assign_if_equal; conditional TryRemove maps to
  erase_if_equal; set maps to insert_or_assign. Never use erase(key) for
  conditional removal: it could remove a concurrent replacement.
- Folly's default hasher, sharding and hazard-pointer reclamation remain enabled.
  C++ strings are 16 bytes (std::string), C# strings contain 16 UTF-16 characters;
  hashing and memory layout are intentionally native, not artificially matched.
- ns/op for steady-state is inverse aggregate throughput, NOT request latency.
  Update/churn are composite operations, and return success counts rather than
  retrying. The benchmark output does not report contention failure rates.
- Growth reports time per whole table, including destruction. C# managed
  reclamation follows GC scheduling; growth results are not identical lifetime
  measurements. Presized uses Folly's constructor hint, not .NET's capacity.
- As with the C# suite, this does not measure per-request p99, concurrent
  enumeration or whole-process memory usage. No allocation equivalence claim.

The self-test checks all type pairs, both distributions, 1/4 workers, stable
cardinality, hit/miss counts and stale-entry identity checks. Build artifacts
are ignored by the repository's existing build/ rule.


## SIMD variant

Build target concurrent_hash_map_simd_bench uses the same source and workload,
with folly::ConcurrentHashMapSIMD instead of folly::ConcurrentHashMap.
Both targets have CTest self-tests. Representative selection (run only with
approval): --bm_regex='String_Entry/(Hit|Mixed95)/65536/Uniform/'.

## Boost variant

concurrent_flat_map_bench uses the existing Boost::unordered dependency and
boost::concurrent_flat_map. A small adapter uses cvisit for lookup, visit for
conditional replacement, and erase_if(key, predicate) for identity-checked
removal. Copied values leave the callback; references never do. Workload traces
and the benchmark harness are shared with both Folly variants. CTest includes
concurrent_flat_map_self_test. The representative filter is the same as above.

## MemoryCache overhead benchmark

Target memory_cache_bench reuses FixedTime.cpp with MemoryCache<string,string>,
100000 slots, no TTL, and shared_ptr<const string> return values. It measures
get/put including entry allocation, clock checks, and the cache's writer mutex.
No TTL expiration, pruning, clear, or capacity rejection is exercised: all writes
replace existing keys. These need separate scenarios, not interpretation of this
steady-state result as a complete cache benchmark.

Build from repository root:

~~~bash
cmake --build cmake-build-relwithdebinfo --target memory_cache_bench -j 12
~~~

Short correctness smoke (not performance measurement):

~~~bash
python3 velox/benchmarks/concurrent_dictionary/fixed_time.py \
  --smoke --implementations cache --output /tmp/cache-bench-smoke
~~~

After approval, compare cache overhead with the bare Boost map at 99% reads:

~~~bash
python3 velox/benchmarks/concurrent_dictionary/fixed_time.py \
  --approved --implementations boost cache --write-percent 1 \
  --output /tmp/cache-vs-boost
~~~

28 cases, 10s warmup plus 50s measurement each: about 28 minutes plus setup.
C# in this runner is ConcurrentDictionary, NOT Microsoft MemoryCache.

## C# MemoryCache

Select csharp-cache to run Microsoft.Extensions.Caching.Memory.MemoryCache from
Microsoft.AspNetCore.App (installed runtime, not the user's locally modified
MemoryCache.cs). --fixed-cache uses the same C# fixed-duration workload and CPU
mapping as --fixed-time. SizeLimit=100000 and Size=1 on every entry; no TTL,
linked entries or statistics are enabled. CacheEntry allocation and native
capacity/compaction behavior are included. Each run prints its assembly path.

.NET's capacity admission/compaction semantics differ from C++'s serialized
replacement: under concurrent writes it may reject entries or evict others.
JSON therefore includes entries and misses; writes counts Set attempts, not
successful admissions. Do not rank throughput without inspecting these counts.

After approval, use --implementations cache csharp-cache --write-percent 1
with fixed_time.py to compare the two caches. This is not a comparison against
the earlier ConcurrentDictionary result.
