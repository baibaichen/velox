#!/usr/bin/env bash
# Task 018S: Fail-close sequential A/B/C matrix runner for the TPC-H
# BufferedInput performance investigation.
#
# Required environment variables:
#   TPCH_APPROVED=1        Safety gate (set explicitly to run).
#   BIN=<path>             RelWithDebInfo/Release velox_tpch_benchmark binary.
#   TPCH_DATA=<path>       TPC-H SF100 Parquet dataset root.
#   DRIVERS=1|4            Number of drivers.
#   OUT_ROOT=<path>        Absolute result root (CSVs, metadata, manifests).
#   LOG_ROOT=<path>        Absolute log root (must be under a build directory).
#
# Optional:
#   QUERIES=9,20,17,21,4   Focused query list (default: the five authorized ones).
#   CACHE_ROOT             FileCache root (default: $OUT_ROOT/cache).
#   QUERY_MEM_GB=32        Per-query memory budget in GiB.
#   FILECACHE_DISK_GIB=80  FileCache on-disk budget (cell C only).
#   NUM_SPLITS_PER_FILE=1  Splits per Parquet file.
#   REFERENCE_NUM_DRIVERS=1  Reference driver count for result equality.
#
# Special modes (mutually exclusive):
#   TPCH_SMOKE=1           One A/B/C sample for one query; calls analyzer --smoke.
#   PROBE_VALIDATION=1     A/C probe-off/on runs at all focused queries; calls
#                          analyzer --probe-validation.
#
# All output is deterministic and fail-close: any single step failure aborts
# the runner and preserves all accumulated artifacts.
set -euo pipefail

# ---------------------------------------------------------------------------
# Source the shared cache-cleanup library.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib_cache_cleanup.sh"

# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

if [[ "${TPCH_APPROVED:-0}" != "1" ]]; then
  echo "ERROR: set TPCH_APPROVED=1 to authorize this run." >&2
  exit 1
fi

if [[ -z "${BIN:-}" ]]; then
  echo "ERROR: BIN is required." >&2
  exit 1
fi
validate_benchmark_binary "$BIN"
REAL_BIN="$(realpath -- "$BIN")"

if [[ -z "${TPCH_DATA:-}" ]]; then
  echo "ERROR: TPCH_DATA is required." >&2
  exit 1
fi
if [[ ! -d "$TPCH_DATA" ]]; then
  echo "ERROR: TPCH_DATA directory does not exist: $TPCH_DATA" >&2
  exit 1
fi
REAL_DATA="$(realpath -- "$TPCH_DATA")"

if [[ -z "${DRIVERS:-}" ]]; then
  echo "ERROR: DRIVERS is required (1 or 4)." >&2
  exit 1
fi
if [[ "$DRIVERS" != "1" && "$DRIVERS" != "4" ]]; then
  echo "ERROR: DRIVERS must be 1 or 4, got: $DRIVERS" >&2
  exit 1
fi

if [[ -z "${OUT_ROOT:-}" ]]; then
  echo "ERROR: OUT_ROOT is required." >&2
  exit 1
fi
if [[ -z "${LOG_ROOT:-}" ]]; then
  echo "ERROR: LOG_ROOT is required." >&2
  exit 1
fi

# Defaults.
QUERIES="${QUERIES:-9,20,17,21,4}"
CACHE_ROOT="${CACHE_ROOT:-$OUT_ROOT/cache}"
QUERY_MEM_GB="${QUERY_MEM_GB:-32}"
FILECACHE_DISK_GIB="${FILECACHE_DISK_GIB:-80}"
NUM_SPLITS_PER_FILE="${NUM_SPLITS_PER_FILE:-1}"
REFERENCE_NUM_DRIVERS="${REFERENCE_NUM_DRIVERS:-1}"

AUTHORIZED_QUERIES="9,20,17,21,4"

SMOKE_MODE=0
PROBE_MODE=0
if [[ "${TPCH_SMOKE:-0}" == "1" ]]; then
  SMOKE_MODE=1
fi
if [[ "${PROBE_VALIDATION:-0}" == "1" ]]; then
  PROBE_MODE=1
fi
if [[ $SMOKE_MODE -eq 1 && $PROBE_MODE -eq 1 ]]; then
  echo "ERROR: TPCH_SMOKE and PROBE_VALIDATION are mutually exclusive." >&2
  exit 1
fi

# Validate query list.
IFS=',' read -ra QUERY_ARRAY <<< "$QUERIES"
if [[ $SMOKE_MODE -eq 1 && ${#QUERY_ARRAY[@]} -ne 1 ]]; then
  echo "ERROR: TPCH_SMOKE=1 requires exactly one query, got: $QUERIES" >&2
  exit 1
fi
if [[ $PROBE_MODE -eq 1 && "$QUERIES" != "$AUTHORIZED_QUERIES" ]]; then
  echo "ERROR: PROBE_VALIDATION=1 requires QUERIES=$AUTHORIZED_QUERIES, got: $QUERIES" >&2
  exit 1
fi
if [[ $SMOKE_MODE -eq 0 && $PROBE_MODE -eq 0 && "$QUERIES" != "$AUTHORIZED_QUERIES" ]]; then
  echo "ERROR: Normal mode requires QUERIES=$AUTHORIZED_QUERIES, got: $QUERIES" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Identity capture
# ---------------------------------------------------------------------------

REAL_OUT="$(realpath -m -- "$OUT_ROOT")"
REAL_LOG="$(realpath -m -- "$LOG_ROOT")"
mkdir -p "$REAL_OUT" "$REAL_LOG"

VELOX_HEAD="$(git -C "$(dirname "$REAL_BIN")" rev-parse HEAD 2>/dev/null || echo "unknown")"
# Try to get HEAD from velox repo directory near the binary.
# The binary is under _build/..., so look for the velox repo root.
VELOX_REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "unknown")"
VELOX_HEAD="$(git -C "$VELOX_REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")"
GLUTEN_HEAD="$(git -C /root/oss/gluten rev-parse HEAD 2>/dev/null || echo "unknown")"
CLICKHOUSE_HEAD="$(git -C /root/oss/clickhouse rev-parse HEAD 2>/dev/null || echo "unknown")"

# ELF build ID.
BIN_BUILD_ID="$(readelf -n "$REAL_BIN" 2>/dev/null \
  | grep -oP 'Build ID:\s*\K[0-9a-f]+' | head -1 || echo "unknown")"

# Arrow library.
ARROW_LIB="$(find /root/oss/gluten/dev/vcpkg/installed/x64-linux-avx \
  -name 'libarrow.a' 2>/dev/null | head -1 || echo "unknown")"
if [[ -z "$ARROW_LIB" ]]; then
  ARROW_LIB="unknown"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_query_fmt() {
  # Zero-pad to 2 digits: 9 -> q09, 20 -> q20.
  printf "q%02d" "$1"
}

_write_meta() {
  local meta_path="$1"
  local cell="$2"
  local block="$3"
  local sample="$4"
  local probe_enabled="$5"
  local result_csv="$6"
  local run_log="$7"
  local query_id="$8"
  local filecache_gib
  if [[ "$cell" == "C" ]]; then
    filecache_gib=$FILECACHE_DISK_GIB
  else
    filecache_gib=0
  fi
  local input_source
  case "$cell" in
    A) input_source="direct" ;;
    B) input_source="filecache_passthrough" ;;
    C) input_source="filecache" ;;
    *) echo "ERROR: unknown cell: $cell" >&2; return 1 ;;
  esac
  cat > "$meta_path" <<EOF
{
  "schema_version": 1,
  "drivers": $DRIVERS,
  "query_id": "$query_id",
  "cell": "$cell",
  "input_source": "$input_source",
  "block": "$block",
  "sample": $sample,
  "warm_round": 2,
  "probe_enabled": $probe_enabled,
  "binary_realpath": "$REAL_BIN",
  "binary_build_id": "$BIN_BUILD_ID",
  "velox_head": "$VELOX_HEAD",
  "gluten_head": "$GLUTEN_HEAD",
  "clickhouse_head": "$CLICKHOUSE_HEAD",
  "cmake_build_type": "RelWithDebInfo",
  "arrow_lib": "$ARROW_LIB",
  "dataset_realpath": "$REAL_DATA",
  "num_splits_per_file": $NUM_SPLITS_PER_FILE,
  "reference_num_drivers": $REFERENCE_NUM_DRIVERS,
  "query_mem_gb": $QUERY_MEM_GB,
  "filecache_disk_gib": $filecache_gib,
  "result_csv": "$result_csv",
  "run_log": "$run_log"
}
EOF
}

# Run one benchmark sample. Writes result CSV, meta.json, and a run log.
# For cell C, the caller must set up the cache sentinel before calling this
# function and register cleanup with setup_trap_cleanup.
_run_sample() {
  local cell="$1"
  local block="$2"
  local sample="$3"
  local query_num="$4"
  local probe_enabled="$5"    # "true" or "false"
  local cache_child="${6:-}"  # Only for cell C.
  local q_fmt
  q_fmt="$(_query_fmt "$query_num")"

  local sample_dir="$REAL_OUT/drivers_${DRIVERS}/${q_fmt}/${cell}/${block}/sample_${sample}"
  local log_dir="$REAL_LOG/drivers_${DRIVERS}/${q_fmt}/${cell}/${block}"
  mkdir -p "$sample_dir" "$log_dir"

  local result_csv="$sample_dir/result.csv"
  local run_log="$log_dir/sample_${sample}.log"
  local meta_path="$sample_dir/meta.json"

  # Build base args.
  local -a args=(
    --data_path="$REAL_DATA"
    --data_format=parquet
    --query_id="$query_num"
    --rounds=2
    --num_splits_per_file="$NUM_SPLITS_PER_FILE"
    --num_drivers="$DRIVERS"
    --reference_num_drivers="$REFERENCE_NUM_DRIVERS"
    --query_mem_gb="$QUERY_MEM_GB"
    --cache_gb=0
    --out="$result_csv"
    --buffered_input_perf_probe="$probe_enabled"
  )

  case "$cell" in
    A)
      args+=(--input_source=direct)
      ;;
    B)
      args+=(--input_source=filecache_passthrough)
      ;;
    C)
      if [[ -z "$cache_child" ]]; then
        echo "ERROR: cache_child required for cell C" >&2
        return 1
      fi
      args+=(
        --input_source=filecache
        --filecache_root="$cache_child"
        --filecache_disk_gib="$FILECACHE_DISK_GIB"
      )
      ;;
  esac

  echo "[$(date -u +%T)] RUN drivers=${DRIVERS} ${q_fmt}/${cell}/${block}/sample_${sample} probe=${probe_enabled}"
  "$REAL_BIN" "${args[@]}" > "$run_log" 2>&1
  echo "  -> exit $? | log: $run_log"

  local probe_json
  if [[ "$probe_enabled" == "true" ]]; then
    probe_json="true"
  else
    probe_json="false"
  fi
  _write_meta "$meta_path" "$cell" "$block" "$sample" "$probe_json" \
    "$result_csv" "$run_log" "$q_fmt"
}

# Run one cell-C sample in its own subshell so the EXIT trap removes only
# this run's cache child. The caller asserts the child is gone afterward.
_run_c_sample() {
  local block="$1"
  local sample="$2"
  local query_num="$3"
  local probe_enabled="$4"
  local q_fmt
  q_fmt="$(_query_fmt "$query_num")"

  local cache_root_real
  cache_root_real="$(realpath -m -- "$CACHE_ROOT")"
  local cache_child="${cache_root_real}/drivers_${DRIVERS}/${q_fmt}/${block}/sample_${sample}"

  (
    set -euo pipefail
    source "$SCRIPT_DIR/lib_cache_cleanup.sh"
    create_sentinel "$cache_child" "$cache_root_real"
    setup_trap_cleanup "$cache_child" "$cache_root_real"
    _run_sample C "$block" "$sample" "$query_num" "$probe_enabled" "$cache_child"
  )

  # After the subshell exits, assert the cache child was cleaned up.
  if [[ -e "$cache_child" || -L "$cache_child" ]]; then
    echo "ERROR: cache child was not cleaned after sample: $cache_child" >&2
    return 1
  fi
}

# ---------------------------------------------------------------------------
# Manifest writer
# ---------------------------------------------------------------------------

_write_manifest() {
  local manifest_path="$REAL_OUT/run_manifest.json"
  local queries_json
  queries_json="$(printf '"%s",' "${QUERY_ARRAY[@]}" | sed 's/,$//')"
  cat > "$manifest_path" <<EOF
{
  "schema_version": 1,
  "binary_realpath": "$REAL_BIN",
  "binary_build_id": "$BIN_BUILD_ID",
  "velox_head": "$VELOX_HEAD",
  "gluten_head": "$GLUTEN_HEAD",
  "clickhouse_head": "$CLICKHOUSE_HEAD",
  "cmake_build_type": "RelWithDebInfo",
  "arrow_lib": "$ARROW_LIB",
  "dataset_realpath": "$REAL_DATA",
  "num_splits_per_file": $NUM_SPLITS_PER_FILE,
  "reference_num_drivers": $REFERENCE_NUM_DRIVERS,
  "query_mem_gb": $QUERY_MEM_GB,
  "filecache_disk_gib": $FILECACHE_DISK_GIB,
  "drivers": $DRIVERS,
  "queries": [$queries_json]
}
EOF
  echo "Manifest: $manifest_path"
}

# ---------------------------------------------------------------------------
# Normal mode: 3+2 A/B/C sequential matrix
# ---------------------------------------------------------------------------

_run_normal() {
  echo "=== Task-018S Normal matrix: $QUERIES @ drivers=${DRIVERS} ==="
  _write_manifest

  for query_num in "${QUERY_ARRAY[@]}"; do
    local q_fmt
    q_fmt="$(_query_fmt "$query_num")"
    echo "--- Query ${q_fmt} ---"

    # Forward block: A,B,C for samples 1,2,3.
    for sample in 1 2 3; do
      for cell in A B C; do
        if [[ "$cell" == "C" ]]; then
          _run_c_sample forward "$sample" "$query_num" "true"
        else
          _run_sample "$cell" forward "$sample" "$query_num" "true"
        fi
      done
    done

    # Reverse block: C,B,A for samples 1,2.
    for sample in 1 2; do
      for cell in C B A; do
        if [[ "$cell" == "C" ]]; then
          _run_c_sample reverse "$sample" "$query_num" "true"
        else
          _run_sample "$cell" reverse "$sample" "$query_num" "true"
        fi
      done
    done
  done
}

# ---------------------------------------------------------------------------
# Smoke mode: one A/B/C sample per query
# ---------------------------------------------------------------------------

_run_smoke() {
  echo "=== Task-018S Smoke: ${QUERY_ARRAY[0]} @ drivers=${DRIVERS} ==="
  _write_manifest
  local query_num="${QUERY_ARRAY[0]}"
  for cell in A B C; do
    if [[ "$cell" == "C" ]]; then
      _run_c_sample forward 1 "$query_num" "true"
    else
      _run_sample "$cell" forward 1 "$query_num" "true"
    fi
  done
}

# ---------------------------------------------------------------------------
# Probe-validation mode: A/C with probe off and on, both orders
# ---------------------------------------------------------------------------

_run_probe_validation() {
  echo "=== Task-018S Probe validation: $QUERIES @ drivers=${DRIVERS} ==="
  _write_manifest

  for query_num in "${QUERY_ARRAY[@]}"; do
    local q_fmt
    q_fmt="$(_query_fmt "$query_num")"
    echo "--- Probe validation: ${q_fmt} ---"

    # Forward order: A_off(s1), C_off(s1), A_on(s2), C_on(s2).
    _run_sample A forward 1 "$query_num" "false"
    _run_c_sample forward 1 "$query_num" "false"
    _run_sample A forward 2 "$query_num" "true"
    _run_c_sample forward 2 "$query_num" "true"

    # Reverse order: C_on(s1), A_on(s1), C_off(s2), A_off(s2).
    _run_c_sample reverse 1 "$query_num" "true"
    _run_sample A reverse 1 "$query_num" "true"
    _run_c_sample reverse 2 "$query_num" "false"
    _run_sample A reverse 2 "$query_num" "false"
  done
}

# ---------------------------------------------------------------------------
# Analyzer invocation
# ---------------------------------------------------------------------------

_run_analyzer() {
  local -a analyzer_args=(
    --input-root "$REAL_OUT"
    --drivers "$DRIVERS"
    --queries "$QUERIES"
  )
  if [[ $SMOKE_MODE -eq 1 ]]; then
    analyzer_args+=(--smoke)
  elif [[ $PROBE_MODE -eq 1 ]]; then
    analyzer_args+=(--probe-validation)
  fi

  local analyzer
  analyzer="$SCRIPT_DIR/analyze_tpch_buffered_input_matrix.py"
  if [[ ! -f "$analyzer" ]]; then
    echo "ERROR: analyzer not found: $analyzer" >&2
    return 1
  fi

  echo "Running analyzer: python3 $analyzer ${analyzer_args[*]}"
  python3 "$analyzer" "${analyzer_args[@]}"
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if [[ $SMOKE_MODE -eq 1 ]]; then
  _run_smoke
elif [[ $PROBE_MODE -eq 1 ]]; then
  _run_probe_validation
else
  _run_normal
fi

analyzer_exit=0
_run_analyzer || analyzer_exit=$?

if [[ $analyzer_exit -ne 0 ]]; then
  echo "FAILED: analyzer returned nonzero ($analyzer_exit); artifacts preserved in $REAL_OUT" >&2
  exit "$analyzer_exit"
fi

echo "SUCCESS: all samples valid. Artifacts: $REAL_OUT"
