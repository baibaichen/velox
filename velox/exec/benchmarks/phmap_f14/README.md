# UniqueValue: F14 versus phmap

This standalone experiment includes `velox/exec/VectorHasher.h` directly. Both
containers use the actual `UniqueValue`, `UniqueValueHasher`, and
`UniqueValueComparer`. No production container or dependency is changed.

The source has a compile-time check that `UniqueValue` is 16 bytes and that
`F14FastSet` selects the SIMD `F14ValueSet` implementation. Both implementations
retain their default hash mixing and use `std::allocator` for timings.

## Build

The experiment needs a compatible installed Folly (including `follybenchmark`),
its dependencies, and xsimd. It does not build the Velox execution engine.

```bash
git clone https://github.com/greg7mdp/parallel-hashmap.git /tmp/phmap
git -C /tmp/phmap checkout 2a6e2aca3b129b907d3a108ecc562ef9ddc478b4
cmake -S velox/exec/benchmarks/phmap_f14 -B /tmp/unique-value-bench -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DPHMAP_ROOT=/tmp/phmap
cmake --build /tmp/unique-value-bench -j 2
```

The recorded host also needed these explicit dependency locations to avoid
mixing Conda's fmt with the fmt version used to build the installed Folly:

```bash
-Dfmt_DIR=/usr/local/lib/cmake/fmt \
-Dgflags_DIR=/usr/lib/x86_64-linux-gnu/cmake/gflags \
-Dglog_DIR=/usr/lib/x86_64-linux-gnu/cmake/glog
```

Use the dependency locations appropriate for the host. On x86-64, this target
uses the AVX2/FMA/BMI2 options from the current Velox build; the host must support
them. The recorded build is GCC 13.3, C++20, `-O3 -DNDEBUG`.

## Run

```bash
python3 velox/exec/benchmarks/phmap_f14/run.py \
  --binary /tmp/unique-value-bench/unique_value_set_benchmark \
  --phmap-root /tmp/phmap --output /tmp/unique-value-results --cpu 8 \
  --deadline 2026-11-09T16:25:00+00:00
```

Set a future deadline when reproducing. The runner stops at that deadline,
kills an overlong child process, and retains completed measurements. A new run
requires a new output directory. To regenerate CSV files from existing raw data:

```bash
python3 velox/exec/benchmarks/phmap_f14/run.py \
  --output /tmp/unique-value-results --summarize-only
```

## Workloads

The matrix has five key families, six cardinalities, and twelve operations:

| Dimension | Values |
| --- | --- |
| Keys | Contiguous int64 values; bijectively mixed int64 values; 8/24/128-byte strings |
| Distinct values | 16, 300, 4,096, 32,768, 90,000, 100,000 |
| Construction | Grow from empty; insert after `reserve(N)` |
| Existing table | Duplicate insert; 90% duplicate / 10% new insert; 100% hit / 100% miss / 50% hit lookup |
| Access | Uniform; hot (approximately 90% of requests target 1% of keys) |

The five existing-table operations have both access patterns. Growing and
reserved construction each have one shuffled unique-key stream. Even the
contiguous-integer dataset uses shuffled insertion order. Seeds and inputs are
identical for the two containers. Strings have printable, deterministic random
prefixes and an eight-character hexadecimal suffix that guarantees uniqueness.
For mixed insertion, eight of nine duplicate operations target the hot subset
in the hot case; every tenth operation inserts one new value.

Lookup streams have `max(N, 4096)` entries; construction streams have N entries;
mixed-insertion streams have `10 * max(1, N / 10)` entries. Small tables therefore
exercise repeated operations without timing an individual lookup. String probes
have separate backing storage from inserted keys. Storage remains alive through
all measurements. All input IDs are initialized.

The actual string-payload limit in VectorHasher is approximately 1 MiB. Cases
with long-string payload above that are labeled `container_stress` in the CSV;
they do not describe the normal VectorHasher distinct-value path.

## Timing and correctness

Folly owns timing, calibration, suspension, warmup, and sampling. Setup, reserve,
prefilling, validation, and destruction are outside measured time. Allocation
and rehash during the `grow` operation are included. Every construction or
mixed-insertion repetition starts from a fresh table. Duplicate insertion and
lookup reuse a prefilled table within the timed callback.

The callback rounds the requested operation count up to a whole input batch and
returns the actual number of operations to Folly. `doNotOptimizeAway` consumes
the ID/inserted-status checksum. Lookup batches also pass the container to
`doNotOptimizeAway`: this supplies a memory clobber and prevents the compiler
from hoisting an unchanged batch out of the repetition loop. There is no custom
timer or clock.

Each process checks both containers against expected IDs, sizes, insertion
status, and lookup outcomes before timing. Additional string checks cover empty
strings, embedded NULs, the 8/9-byte inline boundary, and partial CRC words at
15/17 bytes. Checks remain enabled in release builds.

Seven independent processes measure every dataset. The runner shuffles dataset
order between rounds and reverses container registration order on alternate
rounds. Folly adaptive mode interleaves samples and targets p50 with a 3%
precision target, at least 20 samples, and 0.05 to 1 second per benchmark. The
same CPU affinity is used throughout. The host governor and turbo settings are
not changed, and the CPU is not exclusively reserved.
The runner rejects ptrace supervision so tracing overhead cannot distort
allocation measurements. Final measurements run directly on the host.

`summary.csv` reports the median and range of the seven process-level p50
estimates, plus the median and range of paired phmap/F14 ratios. Lower ratios
favor phmap. A stable winner requires all seven ratios to differ by at least 5%
in the same direction. If Folly reports any nonconverged sample for either
container, the case is marked `unconverged`; its estimates remain available but
are not declared a stable win. This is a conservative classification, not a
statistical confidence interval or a query-speedup prediction.

## Memory

Memory runs are separate from timings. A shared counting-allocator implementation
tracks allocations across allocator rebinds, current requested bytes, and peak
live requested bytes (including the old and new tables during rehash). Checks
verify that accounting returns to zero after destruction, that capacities match
the default allocator, and that F14's own allocated-byte report matches.

`object_bytes` is the default-allocator container object size; it is separate
from `live_bytes` and `peak_bytes`. Long-string payload is also listed separately.
The counts do not include allocator bookkeeping, OS page rounding, input/query
arrays, or strings owned by the fixture. The whole-child-process peak RSS in
metadata is only an auxiliary measurement, not a per-container comparison.

## Evidence and limits

- `raw/`: Folly's original JSON for every completed process.
- `quality/`: nonconverged cases extracted from Folly diagnostics.
- `samples.csv`, `summary.csv`, `memory.csv`: machine-readable measurements.
- `metadata.json`, `run.log`: versions, hashes, host details, flags, and diagnostics.
- `build.json`, `validation/`: recorded build commands and sanitizer check evidence.

The runner contains assert-based checks for its ratio/winner calculation and
diagnostic parser. The benchmark itself supplies the container correctness checks.
The recorded native sanitizer run enables AddressSanitizer,
UndefinedBehaviorSanitizer, and LeakSanitizer. It checks all five families at
16 and 100,000 distinct values and exercises representative timed callbacks.
Installed Folly libraries are not sanitizer-instrumented. A preliminary sandbox
run was discarded before the final matrix: ptrace prevented LeakSanitizer from
running and could affect allocation timing; the lookup memory barrier was also
added before collecting the final matrix. Preliminary timings are not mixed
into the recorded results.

This measures container operations with actual Velox key/hash/equality code, not
the entire VectorHasher or a SQL query. In particular, it does not measure
`copyStringToLocal`, dictionary decoding, range mapping, merging, or concurrency.
Fixture strings are separate `std::string` allocations rather than VectorHasher's
packed string buffers. Full operator benchmarks are needed before a production
replacement decision.
