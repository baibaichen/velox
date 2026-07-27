#!/usr/bin/env bash
# Task 018S: Tests for run_tpch_buffered_input_matrix.sh.
#
# Validates:
# - Correct launch counts and ordering for normal, smoke, and probe-validation modes.
# - Required args on every launch (rounds, splits, reference_drivers, query_mem_gb).
# - Cache-sentinel cleanup after each C sample.
# - Rejection of unauthenticated cache directories.
# - Fake binary refuses concurrent invocations.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
RUNNER="$SCRIPT_DIR/run_tpch_buffered_input_matrix.sh"

# ---------------------------------------------------------------------------
# Scratch workspace under /tmp-equivalent inside the velox repo tmp dir.
# ---------------------------------------------------------------------------
TMPROOT="$(cd "$SCRIPT_DIR/../.." && pwd)/tmp/018s_runner_test_$$"
mkdir -p "$TMPROOT"
trap 'rm -rf "$TMPROOT"' EXIT

pass_count=0
fail_count=0

_pass() { echo "  PASS: $1"; pass_count=$((pass_count + 1)); }
_fail() { echo "  FAIL: $1"; fail_count=$((fail_count + 1)); }

# ---------------------------------------------------------------------------
# Fake binary: writes a valid two-row CSV to --out; records args; refuses
# concurrent invocations via an exclusive lock file.
# ---------------------------------------------------------------------------
FAKE_BIN_DIR="$TMPROOT/relwithdebinfo/velox/benchmarks/tpch"
mkdir -p "$FAKE_BIN_DIR"
FAKE_BIN="$FAKE_BIN_DIR/fake_velox_tpch_benchmark"
ARG_LOG="$TMPROOT/arg_log.txt"
LOCKFILE="$TMPROOT/fake_bin.lock"

cat > "$FAKE_BIN" <<'FAKEEOF'
#!/usr/bin/env bash
set -euo pipefail

# Exclusive lock to detect concurrent invocations.
LOCKFILE="${TMPROOT}/fake_bin.lock"
if ! ( set -C; : > "$LOCKFILE" ) 2>/dev/null; then
  echo "ERROR: concurrent invocation detected" >&2
  exit 1
fi
trap 'rm -f "$LOCKFILE"' EXIT

# Record args.
echo "$*" >> "${TMPROOT}/arg_log.txt"

# Parse --out, --query_id, --input_source.
OUT=""
QUERY_ID=""
CELL="A"
for arg in "$@"; do
  case "$arg" in
    --out=*) OUT="${arg#--out=}" ;;
    --query_id=*) QUERY_ID="${arg#--query_id=}" ;;
    --input_source=*) SRC="${arg#--input_source=}"; case "$SRC" in
      direct) CELL=A ;;
      filecache_passthrough) CELL=B ;;
      filecache) CELL=C ;;
    esac ;;
  esac
done

# Determine wall_ms by cell.
case "$CELL" in
  A) WALL=1000.0 ;;
  B) WALL=1100.0 ;;
  C) WALL=1200.0 ;;
esac

QFMT="$(printf 'q%02d' "$QUERY_ID")"

HEADER="round,query_id,wall_ms,rows,result_hash,result_match,bytes_read,hit_pct,cache_read_mib,predownload_mib,evict_mib,evict_count,op_p50_us,op_p95_us,error,user_ns,system_ns,voluntary_csw,involuntary_csw,storage_read_ops,storage_read_bytes,local_read_ops,local_read_bytes,prefetch_ops,prefetch_bytes,enqueue_count,enqueue_bytes,next_count,returned_bytes,seek_count,max_chunk_bytes,passthrough_read_bytes"

case "$CELL" in
  A) ROW_FIELDS="100,10485760,0,0,0,0,10485760,0,0,10,10485760,10,10485760,0,1048576,0" ;;
  B) ROW_FIELDS="100,10485760,0,0,0,0,10485760,0,0,10,10485760,10,10485760,0,1048576,10485760" ;;
  C) ROW_FIELDS="0,0,100,10485760,0,0,10,10485760,10,10485760,0,1048576,0" ;;
esac

if [[ -z "$OUT" ]]; then
  echo "ERROR: --out not provided" >&2
  exit 1
fi
mkdir -p "$(dirname "$OUT")"

# Write two rows (round 1 and round 2) with cell-appropriate metrics.
for rnd in 1 2; do
  if [[ "$CELL" == "A" || "$CELL" == "B" ]]; then
    echo "${rnd},${QFMT},${WALL},1000,12345678,1,10485760,0.0000,0.0000,0.0000,0.0000,0,100.000,200.000,,500000000,100000000,100,10,${ROW_FIELDS}"
  else
    echo "${rnd},${QFMT},${WALL},1000,12345678,1,10485760,100.0000,0.0000,0.0000,0.0000,0,100.000,200.000,,500000000,100000000,100,10,${ROW_FIELDS}"
  fi
done > "$OUT"
# Prepend header.
{ echo "$HEADER"; cat "$OUT"; } > "$OUT.tmp" && mv "$OUT.tmp" "$OUT"
FAKEEOF
chmod +x "$FAKE_BIN"

# Export TMPROOT so the fake binary can read it.
export TMPROOT

# ---------------------------------------------------------------------------
# Helper: build a minimal manifest so the analyzer passes.
# ---------------------------------------------------------------------------

_write_manifest_for() {
  local out_root="$1"
  local drivers="$2"
  local queries="$3"
  mkdir -p "$out_root"
  cat > "$out_root/run_manifest.json" <<EOF
{
  "schema_version": 1,
  "binary_realpath": "$FAKE_BIN",
  "binary_build_id": "testbuildid",
  "velox_head": "abc123",
  "gluten_head": "def456",
  "clickhouse_head": "ghi789",
  "cmake_build_type": "RelWithDebInfo",
  "arrow_lib": "/fake/libarrow.a",
  "dataset_realpath": "$TMPROOT/data",
  "num_splits_per_file": 1,
  "reference_num_drivers": 1,
  "query_mem_gb": 32,
  "filecache_disk_gib": 80,
  "drivers": $drivers,
  "queries": [$queries]
}
EOF
}

# ---------------------------------------------------------------------------
# Helper: run the runner and capture launch counts.
# ---------------------------------------------------------------------------

_run_matrix() {
  local mode="$1"        # normal|smoke|probe
  local drivers="$2"
  local queries="$3"
  local out_root="$TMPROOT/$mode"
  local log_root="$TMPROOT/${mode}_log"
  : > "$ARG_LOG"
  mkdir -p "$out_root" "$log_root" "$TMPROOT/data"

  local env_extra=()
  case "$mode" in
    smoke)   env_extra=(TPCH_SMOKE=1) ;;
    probe)   env_extra=(PROBE_VALIDATION=1) ;;
  esac

  TPCH_APPROVED=1 \
  BIN="$FAKE_BIN" \
  TPCH_DATA="$TMPROOT/data" \
  DRIVERS="$drivers" \
  QUERIES="$queries" \
  OUT_ROOT="$out_root" \
  LOG_ROOT="$log_root" \
  CACHE_ROOT="$out_root/cache" \
  "${env_extra[@]}" \
  bash "$RUNNER" > "$TMPROOT/${mode}.runner.log" 2>&1 || true

  wc -l < "$ARG_LOG"
}

# ---------------------------------------------------------------------------
# Test: normal mode launch count = 75 (5 queries * 3 cells * 5 samples).
# ---------------------------------------------------------------------------

echo "=== Test: normal mode launch count ==="
: > "$ARG_LOG"
mkdir -p "$TMPROOT/normal" "$TMPROOT/normal_log" "$TMPROOT/data"
TPCH_APPROVED=1 \
BIN="$FAKE_BIN" \
TPCH_DATA="$TMPROOT/data" \
DRIVERS=1 \
QUERIES="9,20,17,21,4" \
OUT_ROOT="$TMPROOT/normal" \
LOG_ROOT="$TMPROOT/normal_log" \
CACHE_ROOT="$TMPROOT/normal/cache" \
bash "$RUNNER" > "$TMPROOT/normal.runner.log" 2>&1 || true

count="$(wc -l < "$ARG_LOG")"
if [[ "$count" -eq 75 ]]; then
  _pass "normal mode: $count launches (expected 75)"
else
  _fail "normal mode: $count launches (expected 75)"
  echo "Runner log tail:"
  tail -20 "$TMPROOT/normal.runner.log" || true
fi

# ---------------------------------------------------------------------------
# Test: forward block order A,B,C repeated for samples 1,2,3.
# ---------------------------------------------------------------------------

echo "=== Test: forward block order ==="
# First query is q09. Lines 1-9 (forward block: 3 samples * 3 cells).
_check_order() {
  local log="$ARG_LOG"
  local q="$(printf '%02d' 9)"
  local -a fwd_cells=()
  local line_no=0
  while IFS= read -r line; do
    line_no=$((line_no+1))
    if [[ "$line" == *"--query_id=$q"* || "$line" == *"--query_id=9"* ]]; then
      if echo "$line" | grep -q 'input_source=direct'; then
        fwd_cells+=("A")
      elif echo "$line" | grep -q 'filecache_passthrough'; then
        fwd_cells+=("B")
      elif echo "$line" | grep -q 'filecache'; then
        fwd_cells+=("C")
      fi
    fi
  done < "$log"
  # Expect A,B,C,A,B,C,A,B,C (forward) then C,B,A,C,B,A (reverse) = 15 total.
  local expected="A B C A B C A B C C B A C B A"
  local actual="${fwd_cells[*]}"
  if [[ "$actual" == "$expected" ]]; then
    _pass "q09 cell order: $actual"
  else
    _fail "q09 cell order: got '$actual', expected '$expected'"
  fi
}
_check_order

# ---------------------------------------------------------------------------
# Test: every launch has rounds=2, num_splits_per_file=1, reference_num_drivers=1, query_mem_gb=32.
# ---------------------------------------------------------------------------

echo "=== Test: required args on every launch ==="
missing=0
while IFS= read -r line; do
  for arg in "--rounds=2" "--num_splits_per_file=1" "--reference_num_drivers=1" "--query_mem_gb=32"; do
    if [[ "$line" != *"$arg"* ]]; then
      echo "  MISSING $arg in: $line"
      missing=$((missing+1))
    fi
  done
done < "$ARG_LOG"
if [[ "$missing" -eq 0 ]]; then
  _pass "all launches have required args"
else
  _fail "$missing missing required args across launches"
fi

# ---------------------------------------------------------------------------
# Test: only C launches have filecache_root and filecache_disk_gib=80.
# ---------------------------------------------------------------------------

echo "=== Test: filecache_root and disk_gib only for C ==="
fc_err=0
while IFS= read -r line; do
  is_c=0
  if echo "$line" | grep -q 'input_source=filecache[^_]'; then
    is_c=1
  fi
  has_root=0
  if echo "$line" | grep -q 'filecache_root'; then
    has_root=1
  fi
  has_gib=0
  if echo "$line" | grep -q 'filecache_disk_gib=80'; then
    has_gib=1
  fi
  if [[ $is_c -eq 0 && ($has_root -eq 1 || $has_gib -eq 1) ]]; then
    echo "  Non-C launch has filecache_root/disk_gib: $line"
    fc_err=$((fc_err+1))
  fi
  if [[ $is_c -eq 1 && ($has_root -eq 0 || $has_gib -eq 0) ]]; then
    echo "  C launch missing filecache_root or disk_gib: $line"
    fc_err=$((fc_err+1))
  fi
done < "$ARG_LOG"
if [[ $fc_err -eq 0 ]]; then
  _pass "filecache_root/disk_gib only for C"
else
  _fail "$fc_err filecache arg violations"
fi

# ---------------------------------------------------------------------------
# Test: cache sentinel cleanup — cache child removed after each C sample.
# ---------------------------------------------------------------------------

echo "=== Test: cache sentinel cleanup ==="
# All C cache children should be gone after the runner.
cache_root="$TMPROOT/normal/cache"
remaining=0
if [[ -d "$cache_root" ]]; then
  remaining="$(find "$cache_root" -mindepth 1 -maxdepth 5 -name '.velox_benchmark_cache_sentinel' 2>/dev/null | wc -l)"
fi
if [[ "$remaining" -eq 0 ]]; then
  _pass "all cache sentinels cleaned up"
else
  _fail "$remaining cache sentinel files remain after runner"
fi

# ---------------------------------------------------------------------------
# Test: unauthenticated cache directory is never deleted.
# ---------------------------------------------------------------------------

echo "=== Test: unauthenticated cache dir not deleted ==="
# Create a directory without a sentinel and verify the lib refuses to wipe it.
fake_dir="$TMPROOT/fake_cache_root/fake_child"
mkdir -p "$fake_dir"
if bash -c "
  source '$SCRIPT_DIR/lib_cache_cleanup.sh'
  safe_wipe_cache_dir '$fake_dir' '$TMPROOT/fake_cache_root'
" 2>/dev/null; then
  _fail "safe_wipe_cache_dir accepted unauthenticated dir"
else
  if [[ -d "$fake_dir" ]]; then
    _pass "unauthenticated dir preserved"
  else
    _fail "unauthenticated dir was removed despite rejection"
  fi
fi

# ---------------------------------------------------------------------------
# Test: smoke mode — exactly 3 launches (one per cell).
# ---------------------------------------------------------------------------

echo "=== Test: smoke mode launch count ==="
: > "$ARG_LOG"
mkdir -p "$TMPROOT/smoke" "$TMPROOT/smoke_log"
TPCH_APPROVED=1 \
BIN="$FAKE_BIN" \
TPCH_DATA="$TMPROOT/data" \
DRIVERS=1 \
TPCH_SMOKE=1 \
QUERIES="4" \
OUT_ROOT="$TMPROOT/smoke" \
LOG_ROOT="$TMPROOT/smoke_log" \
CACHE_ROOT="$TMPROOT/smoke/cache" \
bash "$RUNNER" > "$TMPROOT/smoke.runner.log" 2>&1 || true
smoke_count="$(wc -l < "$ARG_LOG")"
if [[ "$smoke_count" -eq 3 ]]; then
  _pass "smoke mode: $smoke_count launches (expected 3)"
else
  _fail "smoke mode: $smoke_count launches (expected 3)"
  tail -10 "$TMPROOT/smoke.runner.log" || true
fi

# ---------------------------------------------------------------------------
# Test: probe-validation mode — exactly 40 launches
# (5 queries * 4 orders/query * 2 cells = 40).
# ---------------------------------------------------------------------------

echo "=== Test: probe-validation mode launch count ==="
: > "$ARG_LOG"
mkdir -p "$TMPROOT/probe" "$TMPROOT/probe_log"
TPCH_APPROVED=1 \
BIN="$FAKE_BIN" \
TPCH_DATA="$TMPROOT/data" \
DRIVERS=1 \
PROBE_VALIDATION=1 \
QUERIES="9,20,17,21,4" \
OUT_ROOT="$TMPROOT/probe" \
LOG_ROOT="$TMPROOT/probe_log" \
CACHE_ROOT="$TMPROOT/probe/cache" \
bash "$RUNNER" > "$TMPROOT/probe.runner.log" 2>&1 || true
probe_count="$(wc -l < "$ARG_LOG")"
if [[ "$probe_count" -eq 40 ]]; then
  _pass "probe-validation mode: $probe_count launches (expected 40)"
else
  _fail "probe-validation mode: $probe_count launches (expected 40)"
  tail -10 "$TMPROOT/probe.runner.log" || true
fi

# ---------------------------------------------------------------------------
# Test: probe-validation mode forward order: A_off(1), C_off(1), A_on(2), C_on(2).
# ---------------------------------------------------------------------------

echo "=== Test: probe-validation forward order for q09 ==="
# Extract lines for q09 and check order.
q09_lines=()
while IFS= read -r line; do
  if [[ "$line" == *"--query_id=9"* || "$line" == *"--query_id=09"* ]]; then
    q09_lines+=("$line")
  fi
done < "$ARG_LOG"
# Expect 8 lines per query: 4 forward + 4 reverse.
q09_n="${#q09_lines[@]}"
if [[ "$q09_n" -eq 8 ]]; then
  _pass "q09 probe lines: $q09_n (expected 8)"
else
  _fail "q09 probe lines: $q09_n (expected 8)"
fi

# Check forward order: A_off, C_off, A_on, C_on.
_probe_cell() {
  local line="$1"
  if echo "$line" | grep -q 'input_source=direct'; then echo "A";
  elif echo "$line" | grep -q 'filecache_passthrough'; then echo "B";
  elif echo "$line" | grep -q 'filecache'; then echo "C"; fi
}
_probe_on() {
  if echo "$1" | grep -q 'buffered_input_perf_probe=true'; then echo "on"; else echo "off"; fi
}
if [[ ${#q09_lines[@]} -ge 4 ]]; then
  fwd_order=""
  for i in 0 1 2 3; do
    c="$(_probe_cell "${q09_lines[$i]}")"
    p="$(_probe_on "${q09_lines[$i]}")"
    fwd_order+="${c}_${p} "
  done
  fwd_order="${fwd_order% }"
  expected_fwd="A_off C_off A_on C_on"
  if [[ "$fwd_order" == "$expected_fwd" ]]; then
    _pass "q09 probe forward order: $fwd_order"
  else
    _fail "q09 probe forward order: got '$fwd_order', expected '$expected_fwd'"
  fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "=== Results: $pass_count passed, $fail_count failed ==="
if [[ $fail_count -gt 0 ]]; then
  exit 1
fi
exit 0
