# Publish-time Ground-Truth Verification — pre-commit SOP

Round-10 retro showed a recurring "fix one class, introduce another" failure
mode in plan/spec patches:

| Round | Fixed | Introduced |
|-------|-------|------------|
| 7     | spec §6.3 Shape α/β identifier drift | spec single-edit didn't sync plan Task 14 |
| 9     | Task 16 file/binary name typo (`FsCacheMicroBench` → `FsCacheBenchmark`) | plan invented `--bench_seconds`, `block=8k`, `kind=prefetch` — none exist |
| 10    | plan Task 16 CLI flags + column names + workload axis | (none yet — scan R-11 will catch) |

Scan script (`plan-identifier-scan.py`) catches drift **after** the patch
lands. This SOP catches it **before**.

## Before adding any new identifier to plan or spec

If the change introduces a new …

1. **`--flag_name`** (CLI flag) — `grep -n "DEFINE_.*\b<name>\b" velox/**/*.cpp`
   to confirm the flag actually exists. If it doesn't, you are inventing a
   flag — stop and either add the `DEFINE_*` to source first or pick the
   real flag name.

2. **Column header** (e.g. for benchmark / results markdown) — `grep -n
   "<header>" velox/**/*.cpp` against the table-printing function. If
   absent, you are describing a schema the source does not emit; either
   add the column to source first or drop the reference.

3. **`FsCache::xxx()` / `FileSegment::xxx()` member** — `grep -nE
   "\bxxx\(" velox/common/caching/fscache/*.h`. If not declared, you are
   inventing an accessor.

4. **File path** `velox/.../*.cpp|*.h` — `ls velox/<path>` to confirm.
   If absent, the plan must either declare `- Create: \`<path>\`` in the
   Files section, or pick the real path.

5. **Build target** `velox_*_test` / `velox_*_benchmark` — `grep -rn
   "add_executable(\b<name>\b" velox/**/CMakeLists.txt`. If absent, the
   plan/spec must declare which `CMakeLists.txt` will add it.

6. **Workload / enum value** referenced in a perf cell — `grep -n "k<Name>\b"
   velox/<source-of-truth-header>.h`. CH/Velox enums look like `kSequential`
   / `kZipfian` / `kUniform`. Don't write `kind=prefetch` if the enum
   doesn't have `kPrefetch`.

7. **`prefetchRatio` / `xxxRate` / `xxxShare` derive metric** — read the
   spec section that defines the math. If the proposed name has a
   different formula, you are introducing semantic drift and ALL three
   downstream consumers (UT, perf gate, results doc) will disagree.

## Verification order

For each new identifier in your patch: do the grep / ls **before**
saving the patch. The cost is 5 seconds per identifier; the cost of
finding it 2 rounds later is half an hour of round-trip review.

The 八荣八耻 violations that match this SOP:

- #1 瞎猜接口 — every "inventing an identifier" failure
- #5 跳过验证 — every "didn't grep before adding" failure

## What scan catches that this SOP doesn't

`plan-identifier-scan.py` complements this SOP — neither replaces the
other:

- SOP catches drift **before** the commit (prevent).
- scan catches drift **after** the commit (detect).

The scan's 4 layers (identifier patterns, spec→plan tests cross-check,
file/target existence, CLI flag existence) are the safety net. The SOP
is the wing — keep both.

## When to update this doc

Each round that finds a *new class* of drift (not just another instance
of an existing class), add a row to the table at the top and a numbered
item to the "Before adding any new identifier" list. The doc should
grow shorter over time as scan layers replace SOP layers.
