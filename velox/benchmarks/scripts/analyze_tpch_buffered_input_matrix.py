#!/usr/bin/env python3
"""
Task 018S: Validate and analyze A/B/C buffered-input matrix samples.

Usage:
  analyze_tpch_buffered_input_matrix.py
      --input-root <absolute path>
      --drivers <1|4>
      --queries 9,20,17,21,4
      [--smoke]
      [--probe-validation]

Produces:
  $input_root/validity.json
  $input_root/summary.csv
  $input_root/order_block_summary.csv

Exits nonzero on any validity failure.
"""

import argparse
import csv
import json
import pathlib
import statistics
import sys

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
EXPECTED_HEADER = (
    "round,query_id,wall_ms,rows,result_hash,result_match,bytes_read,"
    "hit_pct,cache_read_mib,predownload_mib,evict_mib,evict_count,"
    "op_p50_us,op_p95_us,error,"
    "user_ns,system_ns,voluntary_csw,involuntary_csw,"
    "storage_read_ops,storage_read_bytes,"
    "local_read_ops,local_read_bytes,"
    "prefetch_ops,prefetch_bytes,"
    "enqueue_count,enqueue_bytes,"
    "next_count,returned_bytes,"
    "seek_count,max_chunk_bytes,"
    "passthrough_read_bytes"
)
EXPECTED_FIELD_COUNT = 32

# Cell names and their expected input_source values.
CELL_TO_SOURCE = {
    "A": "direct",
    "B": "filecache_passthrough",
    "C": "filecache",
}

# Normal 3+2 block structure: (block, cell, sample) triples in launch order.
FORWARD_ORDER = [("forward", c, s) for s in (1, 2, 3) for c in ("A", "B", "C")]
REVERSE_ORDER = [("reverse", c, s) for s in (1, 2) for c in ("C", "B", "A")]

# Probe-validation block structure: probe-off=False, probe-on=True.
PROBE_FORWARD_ORDER = [
    ("forward", "A", 1, False),
    ("forward", "C", 1, False),
    ("forward", "A", 2, True),
    ("forward", "C", 2, True),
]
PROBE_REVERSE_ORDER = [
    ("reverse", "C", 1, True),
    ("reverse", "A", 1, True),
    ("reverse", "C", 2, False),
    ("reverse", "A", 2, False),
]

# ---------------------------------------------------------------------------
# CSV row helpers
# ---------------------------------------------------------------------------


def _parse_csv_row(row: dict) -> dict:
    """Convert a raw CSV row dict to typed values."""
    def _f(k):
        return float(row[k]) if row[k] not in ("", None) else None

    def _i(k):
        v = row[k]
        return int(v) if v not in ("", None) else None

    return {
        "round": _i("round"),
        "query_id": row["query_id"],
        "wall_ms": _f("wall_ms"),
        "rows": _i("rows"),
        "result_hash": _i("result_hash"),
        "result_match": _i("result_match") if row.get("result_match", "") != "" else None,
        "bytes_read": _i("bytes_read"),
        "hit_pct": _f("hit_pct"),
        "cache_read_mib": _f("cache_read_mib"),
        "predownload_mib": _f("predownload_mib"),
        "evict_mib": _f("evict_mib"),
        "evict_count": _i("evict_count"),
        "op_p50_us": _f("op_p50_us"),
        "op_p95_us": _f("op_p95_us"),
        "error": row.get("error", ""),
        "user_ns": _i("user_ns"),
        "system_ns": _i("system_ns"),
        "voluntary_csw": _i("voluntary_csw"),
        "involuntary_csw": _i("involuntary_csw"),
        "storage_read_ops": _i("storage_read_ops"),
        "storage_read_bytes": _i("storage_read_bytes"),
        "local_read_ops": _i("local_read_ops"),
        "local_read_bytes": _i("local_read_bytes"),
        "prefetch_ops": _i("prefetch_ops"),
        "prefetch_bytes": _i("prefetch_bytes"),
        "enqueue_count": _i("enqueue_count"),
        "enqueue_bytes": _i("enqueue_bytes"),
        "next_count": _i("next_count"),
        "returned_bytes": _i("returned_bytes"),
        "seek_count": _i("seek_count"),
        "max_chunk_bytes": _i("max_chunk_bytes"),
        "passthrough_read_bytes": _i("passthrough_read_bytes"),
    }


def _load_sample_csv(csv_path: pathlib.Path) -> dict:
    """
    Load a two-row sample CSV. Returns the round-2 row as a typed dict.
    Raises ValueError on structural errors.
    """
    if not csv_path.is_file():
        raise ValueError(f"Sample CSV not found: {csv_path}")
    rows = []
    with csv_path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        # Validate header.
        actual_header = ",".join(reader.fieldnames or [])
        if actual_header != EXPECTED_HEADER:
            raise ValueError(
                f"CSV header mismatch in {csv_path}:\n"
                f"  expected: {EXPECTED_HEADER}\n"
                f"  actual:   {actual_header}"
            )
        if len(reader.fieldnames) != EXPECTED_FIELD_COUNT:
            raise ValueError(
                f"CSV has {len(reader.fieldnames)} fields, expected {EXPECTED_FIELD_COUNT}: {csv_path}"
            )
        for raw in reader:
            rows.append(_parse_csv_row(raw))
    if len(rows) != 2:
        raise ValueError(f"Expected 2 rows in {csv_path}, got {len(rows)}")
    if rows[0]["round"] != 1 or rows[1]["round"] != 2:
        raise ValueError(
            f"Expected rounds 1 and 2 in {csv_path}, got {rows[0]['round']} and {rows[1]['round']}"
        )
    return rows[1]  # Retain round 2 only.


def _load_meta(meta_path: pathlib.Path) -> dict:
    if not meta_path.is_file():
        raise ValueError(f"Meta JSON not found: {meta_path}")
    with meta_path.open() as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Validity checks applied to a single warm row
# ---------------------------------------------------------------------------


def _check_row_validity(row: dict, cell: str, probe_enabled: bool, errors: list):
    """Append human-readable error strings for any row-level validity failure."""
    qid = row.get("query_id", "?")

    # Rule 2: result mismatch.
    if row.get("result_match") == 0:
        errors.append(f"{qid}/{cell}: result_match=0 (mismatch)")

    # Rule 3: nonempty error.
    if row.get("error", ""):
        errors.append(f"{qid}/{cell}: nonempty error field: {row['error']!r}")

    # Rule 4/5/6: FileCache-specific checks for cell C.
    if cell == "C":
        if row.get("hit_pct", 100.0) < 100.0:
            errors.append(f"{qid}/C: hit_pct={row['hit_pct']:.4f} < 100 (not fully warm)")
        if (row.get("predownload_mib") or 0.0) > 0.0:
            errors.append(f"{qid}/C: predownload_mib={row['predownload_mib']} > 0 (not warm)")
        if (row.get("evict_mib") or 0.0) > 0.0:
            errors.append(f"{qid}/C: evict_mib={row['evict_mib']} > 0 (warm eviction)")

    # Rule 7: nonzero FileCache counters in A or B.
    if cell in ("A", "B"):
        if (row.get("cache_read_mib") or 0.0) > 0.0:
            errors.append(
                f"{qid}/{cell}: cache_read_mib={row['cache_read_mib']} > 0 (unexpected FileCache hit)"
            )

    # Rule 8: zero passthrough bytes in B.
    if cell == "B":
        if (row.get("passthrough_read_bytes") or 0) == 0:
            errors.append(f"{qid}/B: passthrough_read_bytes=0 (no passthrough reads)")

    # Rule 9: nonzero passthrough bytes in A or C.
    if cell in ("A", "C"):
        if (row.get("passthrough_read_bytes") or 0) > 0:
            errors.append(
                f"{qid}/{cell}: passthrough_read_bytes={row['passthrough_read_bytes']} > 0 (unexpected)"
            )

    # Rule 10: zero probe metrics in any cell (when probe is enabled).
    if probe_enabled:
        for field in ("enqueue_count", "next_count", "returned_bytes", "max_chunk_bytes"):
            if (row.get(field) or 0) == 0:
                errors.append(f"{qid}/{cell}: probe field {field}=0 (probe enabled but no data)")

    # Rule 11: A or B has zero storage read ops/bytes.
    if cell in ("A", "B"):
        if (row.get("storage_read_ops") or 0) == 0:
            errors.append(f"{qid}/{cell}: storage_read_ops=0")
        if (row.get("storage_read_bytes") or 0) == 0:
            errors.append(f"{qid}/{cell}: storage_read_bytes=0")

    # Rule 12: C has zero local read ops/bytes.
    if cell == "C":
        if (row.get("local_read_ops") or 0) == 0:
            errors.append(f"{qid}/C: local_read_ops=0")
        if (row.get("local_read_bytes") or 0) == 0:
            errors.append(f"{qid}/C: local_read_bytes=0")


# ---------------------------------------------------------------------------
# Metadata validation
# ---------------------------------------------------------------------------


def _check_meta_vs_manifest(meta: dict, manifest: dict, errors: list, context: str):
    """Rule 16-19: check that sample metadata matches the manifest."""
    # Rule 18: schema_version.
    if meta.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"{context}: schema_version={meta.get('schema_version')} != {SCHEMA_VERSION}")

    # Rule 19: invariant identity fields must match the manifest exactly.
    invariant_keys = [
        "binary_realpath",
        "binary_build_id",
        "velox_head",
        "gluten_head",
        "clickhouse_head",
        "cmake_build_type",
        "arrow_lib",
        "dataset_realpath",
        "num_splits_per_file",
        "reference_num_drivers",
        "query_mem_gb",
    ]
    for key in invariant_keys:
        if key in manifest and meta.get(key) != manifest[key]:
            errors.append(
                f"{context}: {key} mismatch: sample={meta.get(key)!r} manifest={manifest[key]!r}"
            )


def _check_meta_vs_expected(
    meta: dict, drivers: int, query_id: str, cell: str,
    block: str, sample: int, warm_round: int, probe_enabled: bool,
    errors: list, context: str,
):
    """Rule 16-17: check per-sample metadata fields."""
    # Rule 16: driver/query/cell/input-source.
    if meta.get("drivers") != drivers:
        errors.append(f"{context}: drivers={meta.get('drivers')} != {drivers}")
    if meta.get("query_id") != query_id:
        errors.append(f"{context}: query_id={meta.get('query_id')!r} != {query_id!r}")
    if meta.get("cell") != cell:
        errors.append(f"{context}: cell={meta.get('cell')!r} != {cell!r}")
    expected_source = CELL_TO_SOURCE.get(cell, "")
    if meta.get("input_source") != expected_source:
        errors.append(
            f"{context}: input_source={meta.get('input_source')!r} != {expected_source!r}"
        )

    # Rule 17: block/sample/warm-round/probe-enabled.
    if meta.get("block") != block:
        errors.append(f"{context}: block={meta.get('block')!r} != {block!r}")
    if meta.get("sample") != sample:
        errors.append(f"{context}: sample={meta.get('sample')} != {sample}")
    if meta.get("warm_round") != warm_round:
        errors.append(f"{context}: warm_round={meta.get('warm_round')} != {warm_round}")
    if meta.get("probe_enabled") != probe_enabled:
        errors.append(
            f"{context}: probe_enabled={meta.get('probe_enabled')} != {probe_enabled}"
        )


# ---------------------------------------------------------------------------
# Sample loading
# ---------------------------------------------------------------------------


def _load_one_sample(
    root: pathlib.Path, drivers: int, query_id: str, cell: str,
    block: str, sample: int, manifest: dict,
    probe_enabled: bool = True,
):
    """
    Load and validate one (query, cell, block, sample) combination.
    Returns (row_dict, errors_list).
    """
    errors = []
    q_fmt = f"q{int(query_id):02d}"
    sample_dir = root / f"drivers_{drivers}" / q_fmt / cell / block / f"sample_{sample}"
    csv_path = sample_dir / "result.csv"
    meta_path = sample_dir / "meta.json"
    context = f"{q_fmt}/{cell}/{block}/sample_{sample}"

    try:
        row = _load_sample_csv(csv_path)
    except ValueError as exc:
        return None, [str(exc)]

    # Check query_id field in CSV matches expected.
    if row.get("query_id") != q_fmt:
        errors.append(
            f"{context}: CSV query_id={row['query_id']!r} != {q_fmt!r}"
        )

    try:
        meta = _load_meta(meta_path)
    except ValueError as exc:
        errors.append(str(exc))
        meta = {}

    if meta:
        _check_meta_vs_manifest(meta, manifest, errors, context)
        _check_meta_vs_expected(
            meta, drivers, q_fmt, cell, block, sample, 2, probe_enabled, errors, context
        )

    _check_row_validity(row, cell, probe_enabled, errors)
    return row, errors


# ---------------------------------------------------------------------------
# Normal mode analysis
# ---------------------------------------------------------------------------


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _pooled_median(samples_a: list, samples_b: list, samples_c: list):
    """Combine forward and reverse warm rows into per-cell pooled medians."""
    return {
        "A": statistics.median([r["wall_ms"] for r in samples_a]),
        "B": statistics.median([r["wall_ms"] for r in samples_b]),
        "C": statistics.median([r["wall_ms"] for r in samples_c]),
    }


def _block_median(samples: list):
    return statistics.median([r["wall_ms"] for r in samples])


def _analyze_normal(root: pathlib.Path, drivers: int, queries: list, manifest: dict):
    """
    Full 3+2 normal-mode analysis. Returns (all_errors, summary_rows,
    order_block_rows, validity_detail).
    """
    all_errors = []
    summary_rows = []
    order_block_rows = []

    for q in queries:
        q_fmt = f"q{int(q):02d}"
        per_block = {"forward": {"A": [], "B": [], "C": []}, "reverse": {"A": [], "B": [], "C": []}}
        sample_errors = []

        # Load forward samples: A,B,C for samples 1,2,3.
        for sample in (1, 2, 3):
            for cell in ("A", "B", "C"):
                row, errs = _load_one_sample(
                    root, drivers, str(q), cell, "forward", sample, manifest
                )
                sample_errors.extend(errs)
                if row and not errs:
                    per_block["forward"][cell].append(row)

        # Load reverse samples: C,B,A for samples 1,2.
        for sample in (1, 2):
            for cell in ("C", "B", "A"):
                row, errs = _load_one_sample(
                    root, drivers, str(q), cell, "reverse", sample, manifest
                )
                sample_errors.extend(errs)
                if row and not errs:
                    per_block["reverse"][cell].append(row)

        all_errors.extend(sample_errors)

        # Require exactly 3 forward and 2 reverse samples per cell (rule 15).
        for block, expected_count in (("forward", 3), ("reverse", 2)):
            for cell in ("A", "B", "C"):
                actual = len(per_block[block][cell])
                if actual != expected_count:
                    all_errors.append(
                        f"{q_fmt}/{cell}/{block}: expected {expected_count} samples, got {actual}"
                    )

        # Compute block medians (only when all samples present).
        fwd_ok = all(len(per_block["forward"][c]) == 3 for c in ("A", "B", "C"))
        rev_ok = all(len(per_block["reverse"][c]) == 2 for c in ("A", "B", "C"))

        if fwd_ok:
            for cell in ("A", "B", "C"):
                m = _block_median(per_block["forward"][cell])
                order_block_rows.append({
                    "query_id": q_fmt,
                    "block": "forward",
                    "cell": cell,
                    "median_ms": m,
                })

        if rev_ok:
            for cell in ("A", "B", "C"):
                m = _block_median(per_block["reverse"][cell])
                order_block_rows.append({
                    "query_id": q_fmt,
                    "block": "reverse",
                    "cell": cell,
                    "median_ms": m,
                })

        # Rule 13/14: opposite signs between blocks.
        if fwd_ok and rev_ok:
            fwd = {c: _block_median(per_block["forward"][c]) for c in ("A", "B", "C")}
            rev = {c: _block_median(per_block["reverse"][c]) for c in ("A", "B", "C")}
            b_minus_a_fwd = fwd["B"] - fwd["A"]
            b_minus_a_rev = rev["B"] - rev["A"]
            c_minus_b_fwd = fwd["C"] - fwd["B"]
            c_minus_b_rev = rev["C"] - rev["B"]
            if _sign(b_minus_a_fwd) != _sign(b_minus_a_rev) and not (
                b_minus_a_fwd == 0 or b_minus_a_rev == 0
            ):
                all_errors.append(
                    f"{q_fmt}: B-A sign flips between blocks "
                    f"(forward={b_minus_a_fwd:.3f} reverse={b_minus_a_rev:.3f})"
                )
            if _sign(c_minus_b_fwd) != _sign(c_minus_b_rev) and not (
                c_minus_b_fwd == 0 or c_minus_b_rev == 0
            ):
                all_errors.append(
                    f"{q_fmt}: C-B sign flips between blocks "
                    f"(forward={c_minus_b_fwd:.3f} reverse={c_minus_b_rev:.3f})"
                )

        # Pooled stats.
        all_a = per_block["forward"]["A"] + per_block["reverse"]["A"]
        all_b = per_block["forward"]["B"] + per_block["reverse"]["B"]
        all_c = per_block["forward"]["C"] + per_block["reverse"]["C"]

        if all_a and all_b and all_c:
            med_a = statistics.median([r["wall_ms"] for r in all_a])
            med_b = statistics.median([r["wall_ms"] for r in all_b])
            med_c = statistics.median([r["wall_ms"] for r in all_c])
            min_a = min(r["wall_ms"] for r in all_a)
            min_b = min(r["wall_ms"] for r in all_b)
            min_c = min(r["wall_ms"] for r in all_c)
            max_a = max(r["wall_ms"] for r in all_a)
            max_b = max(r["wall_ms"] for r in all_b)
            max_c = max(r["wall_ms"] for r in all_c)
            b_minus_a_ms = med_b - med_a
            c_minus_b_ms = med_c - med_b
            c_minus_a_ms = med_c - med_a
            summary_rows.append({
                "query_id": q_fmt,
                "drivers": drivers,
                "a_median_ms": med_a,
                "a_min_ms": min_a,
                "a_max_ms": max_a,
                "a_samples": len(all_a),
                "b_median_ms": med_b,
                "b_min_ms": min_b,
                "b_max_ms": max_b,
                "b_samples": len(all_b),
                "c_median_ms": med_c,
                "c_min_ms": min_c,
                "c_max_ms": max_c,
                "c_samples": len(all_c),
                "b_minus_a_ms": b_minus_a_ms,
                "b_minus_a_ratio": (b_minus_a_ms / med_a) if med_a != 0 else None,
                "c_minus_b_ms": c_minus_b_ms,
                "c_minus_b_ratio": (c_minus_b_ms / med_b) if med_b != 0 else None,
                "c_minus_a_ms": c_minus_a_ms,
                "c_minus_a_ratio": (c_minus_a_ms / med_a) if med_a != 0 else None,
                "decomposition_status": "measured",
                "next_plan_selection": "user_review_required",
            })

    return all_errors, summary_rows, order_block_rows


# ---------------------------------------------------------------------------
# Smoke mode
# ---------------------------------------------------------------------------


def _analyze_smoke(root: pathlib.Path, drivers: int, queries: list, manifest: dict):
    """
    Smoke mode: exactly one A/B/C sample per query.
    Applies all per-row validity checks; skips 3+2 sample-count and sign gates.
    """
    all_errors = []
    summary_rows = []

    for q in queries:
        q_fmt = f"q{int(q):02d}"
        rows_by_cell = {}
        for cell in ("A", "B", "C"):
            row, errs = _load_one_sample(
                root, drivers, str(q), cell, "forward", 1, manifest
            )
            all_errors.extend(errs)
            if row:
                rows_by_cell[cell] = row

        # Rule 15: missing cell.
        for cell in ("A", "B", "C"):
            if cell not in rows_by_cell:
                all_errors.append(f"{q_fmt}: missing smoke sample for cell {cell}")

        if len(rows_by_cell) == 3:
            ra, rb, rc = rows_by_cell["A"], rows_by_cell["B"], rows_by_cell["C"]
            summary_rows.append({
                "query_id": q_fmt,
                "a_ms": ra["wall_ms"],
                "b_ms": rb["wall_ms"],
                "c_ms": rc["wall_ms"],
                "b_minus_a_ms": rb["wall_ms"] - ra["wall_ms"],
                "c_minus_a_ms": rc["wall_ms"] - ra["wall_ms"],
            })

    return all_errors, summary_rows


# ---------------------------------------------------------------------------
# Probe-validation mode
# ---------------------------------------------------------------------------


def _analyze_probe_validation(root: pathlib.Path, drivers: int, queries: list, manifest: dict):
    """
    Probe-validation mode: for each query, check that C-A sign is consistent
    between probe-off and probe-on in both forward and reverse orders.
    """
    all_errors = []
    sign_rows = []

    for q in queries:
        q_fmt = f"q{int(q):02d}"
        # Forward: sample_1=A_off, sample_1=C_off, sample_2=A_on, sample_2=C_on.
        # Reverse: sample_1=C_on, sample_1=A_on, sample_2=C_off, sample_2=A_off.
        probes = {
            ("forward", "A", 1): False,
            ("forward", "C", 1): False,
            ("forward", "A", 2): True,
            ("forward", "C", 2): True,
            ("reverse", "C", 1): True,
            ("reverse", "A", 1): True,
            ("reverse", "C", 2): False,
            ("reverse", "A", 2): False,
        }
        loaded = {}
        for (block, cell, sample), probe_on in probes.items():
            row, errs = _load_one_sample(
                root, drivers, str(q), cell, block, sample, manifest,
                probe_enabled=probe_on,
            )
            all_errors.extend(errs)
            if row and not errs:
                loaded[(block, cell, sample)] = row

            # Probe-specific validity: probe-off rows must have zero probe fields.
            if not probe_on and row:
                for field in ("enqueue_count", "next_count", "returned_bytes", "max_chunk_bytes"):
                    if (row.get(field) or 0) != 0:
                        all_errors.append(
                            f"{q_fmt}/{cell}/{block}/sample_{sample}: "
                            f"probe-off but {field}={row[field]} != 0"
                        )

        # Compare C-A sign between probe-off and probe-on in each order.
        for order in ("forward", "reverse"):
            a_off_s = 1 if order == "forward" else 2
            c_off_s = 1 if order == "forward" else 2
            a_on_s = 2 if order == "forward" else 1
            c_on_s = 2 if order == "forward" else 1
            probe_off_a = loaded.get((order, "A", a_off_s))
            probe_off_c = loaded.get((order, "C", c_off_s))
            probe_on_a = loaded.get((order, "A", a_on_s))
            probe_on_c = loaded.get((order, "C", c_on_s))

            if probe_off_a and probe_off_c and probe_on_a and probe_on_c:
                c_minus_a_off = probe_off_c["wall_ms"] - probe_off_a["wall_ms"]
                c_minus_a_on = probe_on_c["wall_ms"] - probe_on_a["wall_ms"]
                if (
                    _sign(c_minus_a_off) != _sign(c_minus_a_on)
                    and c_minus_a_off != 0
                    and c_minus_a_on != 0
                ):
                    all_errors.append(
                        f"{q_fmt}/{order}: C-A sign flips between probe-off and probe-on "
                        f"(off={c_minus_a_off:.3f} on={c_minus_a_on:.3f})"
                    )
                sign_rows.append({
                    "query_id": q_fmt,
                    "order": order,
                    "c_minus_a_off_ms": c_minus_a_off,
                    "c_minus_a_on_ms": c_minus_a_on,
                    "sign_consistent": _sign(c_minus_a_off) == _sign(c_minus_a_on),
                })

    return all_errors, sign_rows


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _write_validity(out_path: pathlib.Path, errors: list, extra: dict = None):
    doc = {"valid": len(errors) == 0, "errors": errors}
    if extra:
        doc.update(extra)
    with out_path.open("w") as fh:
        json.dump(doc, fh, indent=2)


def _write_summary_csv(out_path: pathlib.Path, rows: list, fieldnames: list):
    with out_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _load_manifest(root: pathlib.Path) -> dict:
    manifest_path = root / "run_manifest.json"
    if not manifest_path.is_file():
        return {}
    with manifest_path.open() as fh:
        return json.load(fh)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate and analyze Task-018S buffered-input matrix samples."
    )
    parser.add_argument("--input-root", required=True, type=pathlib.Path)
    parser.add_argument("--drivers", required=True, type=int, choices=[1, 4])
    parser.add_argument("--queries", required=True,
                        help="Comma-separated query numbers e.g. 9,20,17,21,4")
    parser.add_argument("--smoke", action="store_true",
                        help="Smoke mode: one A/B/C sample per query")
    parser.add_argument("--probe-validation", action="store_true",
                        help="Probe-validation mode: A/C with probe off and on")
    args = parser.parse_args(argv)

    root = args.input_root
    if not root.is_dir():
        print(f"ERROR: --input-root does not exist: {root}", file=sys.stderr)
        sys.exit(1)

    queries = [int(q.strip()) for q in args.queries.split(",")]
    manifest = _load_manifest(root)

    if args.smoke:
        errors, summary_rows = _analyze_smoke(root, args.drivers, queries, manifest)
        _write_validity(root / "validity.json", errors)
        _write_summary_csv(
            root / "summary.csv", summary_rows,
            ["query_id", "a_ms", "b_ms", "c_ms", "b_minus_a_ms", "c_minus_a_ms"],
        )

    elif args.probe_validation:
        errors, sign_rows = _analyze_probe_validation(root, args.drivers, queries, manifest)
        _write_validity(root / "validity.json", errors)
        _write_summary_csv(
            root / "summary.csv", sign_rows,
            ["query_id", "order", "c_minus_a_off_ms", "c_minus_a_on_ms", "sign_consistent"],
        )

    else:
        errors, summary_rows, ob_rows = _analyze_normal(root, args.drivers, queries, manifest)
        _write_validity(root / "validity.json", errors)
        _write_summary_csv(
            root / "summary.csv", summary_rows,
            [
                "query_id", "drivers",
                "a_median_ms", "a_min_ms", "a_max_ms", "a_samples",
                "b_median_ms", "b_min_ms", "b_max_ms", "b_samples",
                "c_median_ms", "c_min_ms", "c_max_ms", "c_samples",
                "b_minus_a_ms", "b_minus_a_ratio",
                "c_minus_b_ms", "c_minus_b_ratio",
                "c_minus_a_ms", "c_minus_a_ratio",
                "decomposition_status", "next_plan_selection",
            ],
        )
        _write_summary_csv(
            root / "order_block_summary.csv", ob_rows,
            ["query_id", "block", "cell", "median_ms"],
        )

    if errors:
        print(f"INVALID: {len(errors)} error(s):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    print(f"VALID: {root}")
    sys.exit(0)


if __name__ == "__main__":
    main()
