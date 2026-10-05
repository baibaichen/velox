#!/usr/bin/env bash
set -euo pipefail
bench_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$bench_dir/../../.." && pwd)
build_dir=${BUILD_DIR:-$repo_dir/cmake-build-relwithdebinfo}
dotnet_bin=${DOTNET:-dotnet}
cpp_bin="$build_dir/velox/benchmarks/concurrent_dictionary/cpp/concurrent_hash_map_bench"
threads=${BENCH_THREADS:-1,4}
case ${1:-help} in
  check)
    /usr/bin/ctest --test-dir "$build_dir/velox/benchmarks/concurrent_dictionary/cpp" --output-on-failure
    "$dotnet_bin" run --project "$bench_dir/csharp" -c Release --no-build -- --self-test
    ;;
  run)
    if [[ ${2:-} != --approved ]]; then
      echo 'Formal benchmarks require approval. After approval: run.sh run --approved' >&2
      exit 2
    fi
    out=${RESULTS_DIR:-$bench_dir/results/$(date +%Y%m%d-%H%M%S)}
    mkdir -p "$out"
    out=$(cd -- "$out" && pwd)
    "$dotnet_bin" --info > "$out/dotnet-info.txt"
    lscpu > "$out/cpu-info.txt"
    git -C "$repo_dir" rev-parse HEAD > "$out/revision.txt"
    cp "$build_dir/CMakeCache.txt" "$out/CMakeCache.txt"
    "$cpp_bin" --threads="$threads" --bm_json_verbose="$out/cpp.json" > "$out/cpp.log" 2>&1
    CD_BENCH_THREADS="$threads" "$dotnet_bin" run --project "$bench_dir/csharp" -c Release --no-build --       --filter '*' --job Short --exporters json --artifacts "$out/csharp" > "$out/csharp.log" 2>&1
    echo "Results: $out"
    ;;
  help|--help|-h)
    echo 'Usage: run.sh check | run --approved'
    echo 'check runs correctness checks only; run executes C++ then C# benchmarks.'
    echo 'Environment: BUILD_DIR, DOTNET, BENCH_THREADS (default 1,4), RESULTS_DIR'
    ;;
  *) echo 'Unknown command; use --help' >&2; exit 2 ;;
esac
