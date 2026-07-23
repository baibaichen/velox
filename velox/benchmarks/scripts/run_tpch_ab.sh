#!/usr/bin/env bash
# Task 018-D TPCH A/B orchestrator.
#
# 018-P GATE: TPCH is forbidden until the user approves the pre-TPCH checkpoint.
# This script therefore refuses to touch anything TPCH-related -- it does NOT
# even look at BIN or TPCH_DATA -- unless TPCH_APPROVED=1 is set in the
# environment. Creation/syntax of this script is part of Task 018-D; actually
# running it against real TPCH data happens only in Task 018-H2, after approval.
#
# When approved, it sweeps velox_tpch_benchmark across the three A/B backends:
#   direct    -- pure Velox DirectBufferedInput reads (no application cache)
#   cbi       -- in-process AsyncDataCache sized by --cache_gb
#   filecache -- ch::FileCache on a sentinel-marked, trap-cleaned disk root
# Only the filecache backend owns a task-managed disk directory; direct and cbi
# need no task-owned cache root.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_cache_cleanup.sh
source "$SCRIPT_DIR/lib_cache_cleanup.sh"

# --- 018-P approval gate: checked before BIN / TPCH_DATA are referenced. ---
: "${TPCH_APPROVED:?refusing to run TPCH: set TPCH_APPROVED=1 only after the 018-P pre-TPCH checkpoint is approved}"
if [[ "$TPCH_APPROVED" != "1" ]]; then
  echo "ERROR: TPCH_APPROVED must be exactly 1 (got '$TPCH_APPROVED'); 018-P approval required" >&2
  exit 1
fi

# Only AFTER approval do we require the data directory and the binary.
: "${TPCH_DATA:?set TPCH_DATA to the TPCH Parquet directory}"
: "${BIN:?set BIN to the velox_tpch_benchmark path (RelWithDebInfo/Release)}"
validate_benchmark_binary "$BIN"

: "${CACHE_ROOT:=$(pwd)/tmp/velox_tpch_ab_cache}"
: "${OUT_DIR:=$(pwd)/tmp/tpch_ab_results}"
: "${DATA_FORMAT:=parquet}"
: "${QUERY_ID:=0}"
: "${ROUNDS:=3}"
: "${NUM_SPLITS_PER_FILE:=1}"
: "${NUM_DRIVERS:=4}"
: "${REFERENCE_NUM_DRIVERS:=1}"
: "${FILECACHE_DISK_GIB:=58}"
: "${QUERY_MEM_GB:=32}"
: "${CACHE_GB:=32}"
: "${CACHE_MEM_GB:=4}"

CACHE_ROOT="$(realpath -m -- "$CACHE_ROOT")"
if [[ "$CACHE_ROOT" != /* ]]; then
  echo "ERROR: CACHE_ROOT must resolve to an absolute path: $CACHE_ROOT" >&2
  exit 1
fi
mkdir -p -- "$OUT_DIR" "$CACHE_ROOT"

for MODE in direct cbi filecache; do
  args=(
    --input_source="$MODE"
    --data_path="$TPCH_DATA"
    --data_format="$DATA_FORMAT"
    --query_id="$QUERY_ID"
    --rounds="$ROUNDS"
    --num_splits_per_file="$NUM_SPLITS_PER_FILE"
    --num_drivers="$NUM_DRIVERS"
    --reference_num_drivers="$REFERENCE_NUM_DRIVERS"
    --query_mem_gb="$QUERY_MEM_GB"
    --out="$OUT_DIR/tpch_${MODE}.csv"
  )
  case "$MODE" in
    direct)
      # No application cache and no task-owned disk root.
      args+=(--cache_gb=0)
      ;;
    cbi)
      # CBI uses the common query-memory budget and a dedicated cache allocator.
      args+=(--cache_gb="$CACHE_GB" --cache_mem_gb="$CACHE_MEM_GB")
      ;;
    filecache)
      # Only this backend owns a disk cache root; sentinel it and arm cleanup.
      FC_DIR="$CACHE_ROOT/filecache_run"
      create_sentinel "$FC_DIR" "$CACHE_ROOT"
      setup_trap_cleanup "$FC_DIR" "$CACHE_ROOT"
      args+=(
        --cache_gb=0
        --filecache_root="$FC_DIR"
        --filecache_disk_gib="$FILECACHE_DISK_GIB"
      )
      ;;
  esac
  echo "Running TPCH A/B backend '$MODE' -> $OUT_DIR/tpch_${MODE}.csv" >&2
  "$BIN" "${args[@]}"
done

echo "TPCH A/B complete; CSVs under $OUT_DIR" >&2
# No manual wipe: the EXIT trap authenticates the filecache sentinel and removes
# the disk cache child directory.
