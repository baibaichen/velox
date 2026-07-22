#!/usr/bin/env bash
# Task 018-D wrapper A/B orchestrator.
#
# Runs velox_bufferedinput_wrapper_benchmark ONCE with --wrappers=all so the
# Markdown table carries real CBI-relative deltas for all three read paths:
# cbi (AsyncDataCache RAM+SSD), fcbi (ch::FileCache) and dbi (DirectBufferedInput).
# cbi is the delta baseline; fcbi/dbi rows report `Δ vs cbi`.
#
# The cbi SSD tier and the fcbi disk tier each get their own sentinel-marked
# child directory under an absolute CACHE_ROOT. Both are registered for
# trap-driven cleanup: the benchmark preserves the sentinel when it resets those
# roots, and the EXIT trap authenticates and removes them afterwards. Nothing is
# deleted manually here -- the trap does it so the sentinel + strict-child checks
# always gate the removal.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_cache_cleanup.sh
source "$SCRIPT_DIR/lib_cache_cleanup.sh"

: "${BIN:?set BIN to the velox_bufferedinput_wrapper_benchmark path (RelWithDebInfo/Release)}"
: "${CACHE_ROOT:=$(pwd)/tmp/velox_wrapper_ab_cache}"
: "${OUT:=$(pwd)/tmp/wrapper_ab_results/wrapper_all.md}"

# A/B knobs (env-overridable). Defaults mirror the benchmark's own gflags.
: "${RAM_CACHE_GB:=4}"
: "${SSD_CACHE_GB:=80}"
: "${FILECACHE_DISK_GB:=80}"
: "${TARGET_WS_GB:=32}"
: "${REMOTE_GB:=0}"
: "${READ_SIZES_KIB:=1024,8192}"
: "${WORKLOADS:=sequential,zipfian}"
: "${MEASURE_PASSES:=3}"
# COLD_EACH_PASS is optional; set it to 1/true to wipe the local tier before
# every measure pass (cold cache-populate path). Left unset means warm reuse.

validate_benchmark_binary "$BIN"

CACHE_ROOT="$(realpath -m -- "$CACHE_ROOT")"
if [[ "$CACHE_ROOT" != /* ]]; then
  echo "ERROR: CACHE_ROOT must resolve to an absolute path: $CACHE_ROOT" >&2
  exit 1
fi
mkdir -p -- "$CACHE_ROOT"
mkdir -p -- "$(dirname -- "$OUT")"

# Separate sentineled child dirs: cbi owns the SSD tier, fcbi owns the disk tier.
CBI_SSD_DIR="$CACHE_ROOT/cbi_ssd"
FCBI_DIR="$CACHE_ROOT/fcbi_cache"

create_sentinel "$CBI_SSD_DIR" "$CACHE_ROOT"
setup_trap_cleanup "$CBI_SSD_DIR" "$CACHE_ROOT"
create_sentinel "$FCBI_DIR" "$CACHE_ROOT"
setup_trap_cleanup "$FCBI_DIR" "$CACHE_ROOT"

args=(
  --wrappers=all
  --ram_cache_gb="$RAM_CACHE_GB"
  --ssd_cache_gb="$SSD_CACHE_GB"
  --filecache_disk_gb="$FILECACHE_DISK_GB"
  --target_ws_gb="$TARGET_WS_GB"
  --remote_gb="$REMOTE_GB"
  --read_sizes_kib="$READ_SIZES_KIB"
  --workloads="$WORKLOADS"
  --measure_passes="$MEASURE_PASSES"
  --ssd_path="$CBI_SSD_DIR"
  --filecache_root="$FCBI_DIR"
  --report_dir=
  --out="$OUT"
)
if [[ "${COLD_EACH_PASS:-}" == "1" || "${COLD_EACH_PASS:-}" == "true" ]]; then
  args+=(--cold_each_pass)
fi

echo "Running wrapper A/B (all wrappers) -> $OUT" >&2
"$BIN" "${args[@]}"

# Validate the Markdown output: non-empty, carries the common header, and has a
# row for each of the three wrappers (labels cbi/fcbi/dbi; workload labels are
# seq/zipf/uni, not the long 'sequential').
if [[ ! -s "$OUT" ]]; then
  echo "ERROR: wrapper output is empty: $OUT" >&2
  exit 1
fi
HEADER='| pattern | read | wrapper | wall_ms | MB/s | ram_MB | ssd_MB | src_MB | Δ vs cbi |'
if ! grep -qF -- "$HEADER" "$OUT"; then
  echo "ERROR: wrapper output missing the common header: $OUT" >&2
  exit 1
fi
for w in cbi fcbi dbi; do
  if ! grep -qF -- "| $w |" "$OUT"; then
    echo "ERROR: wrapper output missing the '$w' row: $OUT" >&2
    exit 1
  fi
done

echo "Wrapper A/B complete; validated $OUT (cbi/fcbi/dbi rows present)." >&2
# No manual wipe: the EXIT trap authenticates each sentinel and removes the two
# cache child dirs.
