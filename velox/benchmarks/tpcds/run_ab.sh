#!/usr/bin/env bash
# Two processes — one per backend — each runs 3 rounds × 99 queries.
# Cache state persists across rounds inside one process and is freshly
# constructed on the other one starting. `--clear_ram_cache` /
# `--clear_ssd_cache` are left false (their defaults) so rounds 2/3 are
# warm.
#
# Explicit `set +e` so a one-sided crash still lets the other side
# finish and the merge step still runs.
set +e

# All paths below are repo-relative; cd to the repo root so the script
# behaves the same regardless of the caller's CWD.
cd "$(git rev-parse --show-toplevel)"

BIN=./cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpcds/velox_tpcds_benchmark
OUT=docs/superpowers/results
DATA=/home/chang/test/tpds/tpcds-generated-100.0-parquet-non_partitioned
mkdir -p "$OUT"

"$BIN" --input_source=cbi --rounds=3 --num_repeats=1 --num_drivers=4 \
  --cache_gb=8 --ssd_cache_gb=50 --ssd_path=/tmp/velox_cbi_ssd \
  --data_path="$DATA" \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv"
cbi_exit=$?

"$BIN" --input_source=fscache --rounds=3 --num_repeats=1 --num_drivers=4 \
  --fscache_disk_gib=58 --fscache_root=/tmp/velox_fscache \
  --data_path="$DATA" \
  --out="$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv"
fscache_exit=$?

CBI_CSV="$OUT/2026-05-23-fscache-vs-cbi-tpcds-cbi.csv"
FSCACHE_CSV="$OUT/2026-05-23-fscache-vs-cbi-tpcds-fscache.csv"

# Refuse to merge if either CSV is missing or empty — otherwise the
# Python step raises and overwrites the .md with a truncated dump.
if [[ -s "$CBI_CSV" && -s "$FSCACHE_CSV" ]]; then
  python3 scripts/bench/merge_tpcds_ab.py "$CBI_CSV" "$FSCACHE_CSV" \
    > "$OUT/2026-05-23-fscache-vs-cbi-tpcds.md"
else
  echo "ERROR: skipping merge — missing/empty CSV (cbi=$(stat -c %s "$CBI_CSV" 2>/dev/null || echo absent), fscache=$(stat -c %s "$FSCACHE_CSV" 2>/dev/null || echo absent))" >&2
fi

[[ $cbi_exit -ne 0 || $fscache_exit -ne 0 ]] && \
  echo "WARN: partial run (cbi=$cbi_exit fscache=$fscache_exit)" >&2
