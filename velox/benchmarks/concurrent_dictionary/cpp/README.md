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

## MemoryCache benchmark

Target memory_cache_bench now consumes velox_memory_cache and uses the current
set/tryGetValue/count API with explicit Boost Map and Size=1. The old TTL PoC
writer-mutex description no longer applies.

The six positional arguments remain: workers, write percentage, hot distribution,
warmup seconds, measurement seconds, comma-separated worker CPU IDs.
CACHE_SCENARIO selects steady (default), expiry (10ms relative TTL), or capacity
(SizeLimit=10000 against 100000 keys). Steady/expiry use SizeLimit=100000.
Both cache implementations use a 10ms expiration scan frequency. Writes replace
prepopulated keys or reinsert evicted/expired keys; they allocate a new 128-character
value. There is no independent ever-growing unique-key insertion workload.

Output includes attempted operations, misses, entry count, process peak RSS,
and per-worker operation latency sampled every 1024 operations. The p50/p99
are sampled request latencies, not inverse aggregate throughput. Sampling and
sample storage add overhead, and mixed-workload samples can be biased by the
periodic write selector. RSS includes runtime, setup, warmup and samples; it is
not isolated cache memory. C# strings use UTF-16; C++ strings use bytes.

The installed-runtime csharp-cache runner remains available but is not a
fixed-source oracle. The approved MemoryCache comparison instead compiles
unmodified caching sources from commit
6f1d9331b9b477df73982a0fabedefe27f36d8a3 into a Release net10.0 executable,
using the same FixedTime.cs workload. Its project, commands, binary hashes,
logs and results are retained under the user's MemoryCache evidence directory.
No instrumented correctness-oracle assembly is used for timing.

Inspect misses and count before comparing throughput: expiry/capacity scenarios
may do different amounts of successful lookup/eviction work. Foreground worker
affinity is matched; native shared background pools are not pinned. This harness
does not measure queue depth or queue waiting time, so it cannot establish that
the shared 4096-task queue is sufficient for many simultaneous cache instances.
