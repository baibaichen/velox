# ConcurrentDictionary benchmark

Standalone .NET 10 / BenchmarkDotNet 0.15.8 project. Open
ConcurrentDictionaryBench.csproj directly in Rider, or use the CLI below.
It measures the installed runtime's dictionary, not a locally rebuilt runtime
or MemoryCache. No changes to Velox's CMake build are required.

## Run

Install a .NET 10 SDK. On this machine it is installed in /home/chang/.dotnet:

~~~bash
export DOTNET_ROOT=/home/chang/.dotnet
export PATH="$DOTNET_ROOT:$PATH"
cd /home/chang/OpenSource/velox/velox/benchmarks/concurrent_dictionary/csharp
dotnet restore --locked-mode
dotnet build -c Release --no-restore
dotnet run -c Release --no-build -- --self-test
dotnet run -c Release --no-build -- --list flat
~~~

Quick execution check (Dry is NOT a performance baseline):

~~~bash
dotnet run -c Release --no-build -- --filter '*DictionaryBenchmarks<Int64, Int64>.Hit*' --job Dry
~~~

Representative measurement, with pre-created integer values:

~~~bash
CD_BENCH_THREADS=1,4 dotnet run -c Release --no-build --   --filter '*DictionaryBenchmarks<Int64, Int64>.Hit*'            '*DictionaryBenchmarks<Int64, Int64>.Mixed95*'   --job Short --exporters json
~~~

Use --filter '*' to run the complete matrix. Omit --job Short for BDN's
normal measurement policy. That matrix is deliberately not the default command:
it contains 224 cases with the default parameters and can take a long time.
Use the same CLI arguments in a Rider Release run configuration, without a
debugger. CLI is preferred for reproducible measurement.

BDN writes CSV, Markdown, HTML and requested JSON under
BenchmarkDotNet.Artifacts/results. It records runtime, CPU, GC and JIT details.
Save dotnet --info and lscpu alongside results. CD_BENCH_THREADS accepts positive
divisors of 262144, for example 1,2,4,8,16,32. Set CPU affinity externally with
taskset if needed; use the same CPU set for the eventual C++ comparison.

## Workloads and units

Four type pairs: long/long, string/long, long/Entry and string/Entry. Entry is
an immutable object using reference identity, not payload equality. String keys
are 16 ASCII decimal digits, stored as UTF-16 by .NET; long keys are unboxed.
String hashes use the runtime's implementation and can differ across processes.

Steady-state cases contain 1024 or 65536 entries, default dictionary lock policy,
and 1 or 4 persistent dedicated workers. Initialization, thread creation, trace
construction and random generation occur outside timing. Each invocation performs
262144 total logical operations split equally across workers. Timed work includes
two barriers, result aggregation and worker execution; even one worker uses this
same harness. Traces repeat between batches; write workloads retain state.

| Method | One logical operation |
| --- | --- |
| Hit / Miss | One TryGetValue, 100% present / absent keys |
| Mixed95 / Mixed50 | Approximately 95% / 50% reads; remaining operations overwrite existing keys |
| ConditionalUpdate | Lookup followed by one TryUpdate attempt; no retry |
| Churn | Lookup, conditional remove attempt, then TryAdd attempt |

Uniform sampling covers all keys. Hot sampling directs 90% of requests to the
first max(1, Count/100) keys, with the remaining 10% sampling the whole map.
The portable PRNG is xorshift32 (shifts 13,17,5), seeded per worker with
0x9e3779b9 XOR (worker+1). Each request draws once for hot selection and again
for key index. Mixed writes occur at (requestIndex+workerIndex) modulo 20 or 2
== 0. Values come from a pre-created pool of 256 values/objects. Reused object
identities can recur; this workload is not an ABA-prevention test.

GrowthBenchmarks measures construction and insertion of the entire table,
including final Count. InsertAll reuses pre-created keys and values;
InsertNewValues creates values inside timing (Entry allocates; long is scalar).
Both capacity variants explicitly use concurrencyLevel=-1; only initial capacity
changes (31 versus Count). This explicit-constructor lock policy differs from the
steady-state default constructor. Growth is single-threaded.

## Interpretation and limits

- Steady-state Mean is batch wall time / 262144: inverse aggregate throughput,
  NOT individual request latency. Approximate throughput is 1e9 / Mean(ns).
  ConditionalUpdate and Churn each bundle multiple dictionary calls.
- Growth Mean and allocations are per entire constructed table, not per insert.
- BDN percentiles describe measured batches, not request p95/p99.
- MemoryDiagnoser allocated-byte figures must not be treated as whole-process
  allocation totals for dedicated-worker workloads. Growth allocations occur on
  the benchmark thread and are the suitable allocation comparison here.
- CAS/remove successes can vary with contention. Returned counts prevent unused
  results and support self-test checks; BDN does not export them as success rates.
  The self-test prints counts, but those are correctness-run observations only.
- This first runnable suite does not yet measure request tail latency, RSS,
  concurrent enumeration, parallel growth, or fresh Entry allocation on every
  steady-state replacement. Those require separate workloads before claiming a
  complete MemoryCache-like performance result.
- The self-test checks all four type combinations and both patterns with 1/4
  workers, missing/hit counts, stable cardinality, restored keys after churn,
  conditional identity semantics and both growth variants. It is not a proof of
  arbitrary concurrent histories.
