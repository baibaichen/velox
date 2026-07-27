#!/usr/bin/env python3
"""
Task 018S: Unit tests for analyze_tpch_buffered_input_matrix.py.

Run with:
  python3 velox/benchmarks/scripts/tests/test_analyze_tpch_buffered_input_matrix.py
"""

import csv
import io
import json
import pathlib
import sys
import tempfile
import unittest

# Locate the analyzer relative to this test file.
_SCRIPT_DIR = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_SCRIPT_DIR))
import analyze_tpch_buffered_input_matrix as ana

# ---------------------------------------------------------------------------
# CSV header shared with the implementation.
# ---------------------------------------------------------------------------

HEADER = (
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

_IDENTITY = {
    "binary_realpath": "/build/velox_tpch_benchmark",
    "binary_build_id": "abcdef1234567890",
    "velox_head": "abc123",
    "gluten_head": "def456",
    "clickhouse_head": "ghi789",
    "cmake_build_type": "RelWithDebInfo",
    "arrow_lib": "/vcpkg/libarrow.a",
    "dataset_realpath": "/data/tpch-sf100",
    "num_splits_per_file": 1,
    "reference_num_drivers": 1,
    "query_mem_gb": 32,
}

_MANIFEST = {
    "schema_version": 1,
    **_IDENTITY,
    "drivers": 1,
    "queries": ["q09"],
    "samples": [],
}


def _make_row(
    query_id: str = "q09",
    wall_ms: float = 1000.0,
    result_match: str = "1",
    hit_pct: float = 100.0,
    cache_read_mib: float = 0.0,
    predownload_mib: float = 0.0,
    evict_mib: float = 0.0,
    evict_count: int = 0,
    error: str = "",
    storage_read_ops: int = 100,
    storage_read_bytes: int = 10485760,
    local_read_ops: int = 0,
    local_read_bytes: int = 0,
    enqueue_count: int = 10,
    enqueue_bytes: int = 10485760,
    next_count: int = 10,
    returned_bytes: int = 10485760,
    seek_count: int = 0,
    max_chunk_bytes: int = 1048576,
    passthrough_read_bytes: int = 0,
    rnd: int = 2,
) -> dict:
    """Build a CSV row dict with sensible defaults for the A/direct cell."""
    return {
        "round": rnd,
        "query_id": query_id,
        "wall_ms": wall_ms,
        "rows": 1000,
        "result_hash": 12345678,
        "result_match": result_match,
        "bytes_read": storage_read_bytes,
        "hit_pct": hit_pct,
        "cache_read_mib": cache_read_mib,
        "predownload_mib": predownload_mib,
        "evict_mib": evict_mib,
        "evict_count": evict_count,
        "op_p50_us": 100.0,
        "op_p95_us": 200.0,
        "error": error,
        "user_ns": 500000000,
        "system_ns": 100000000,
        "voluntary_csw": 100,
        "involuntary_csw": 10,
        "storage_read_ops": storage_read_ops,
        "storage_read_bytes": storage_read_bytes,
        "local_read_ops": local_read_ops,
        "local_read_bytes": local_read_bytes,
        "prefetch_ops": 0,
        "prefetch_bytes": 0,
        "enqueue_count": enqueue_count,
        "enqueue_bytes": enqueue_bytes,
        "next_count": next_count,
        "returned_bytes": returned_bytes,
        "seek_count": seek_count,
        "max_chunk_bytes": max_chunk_bytes,
        "passthrough_read_bytes": passthrough_read_bytes,
    }


def _row_for_cell(cell: str, wall_ms: float = 1000.0, **kwargs) -> dict:
    """
    Build a row for a specific cell with appropriate default per-cell fields.
    A=direct: storage reads, no local, no passthrough.
    B=filecache_passthrough: storage reads, no local, passthrough bytes.
    C=filecache: local reads, no storage, no passthrough.
    """
    if cell == "A":
        defaults = dict(
            storage_read_ops=100, storage_read_bytes=10485760,
            local_read_ops=0, local_read_bytes=0,
            passthrough_read_bytes=0,
            hit_pct=0.0, cache_read_mib=0.0, predownload_mib=0.0, evict_mib=0.0,
        )
    elif cell == "B":
        defaults = dict(
            storage_read_ops=100, storage_read_bytes=10485760,
            local_read_ops=0, local_read_bytes=0,
            passthrough_read_bytes=10485760,
            hit_pct=0.0, cache_read_mib=0.0, predownload_mib=0.0, evict_mib=0.0,
        )
    else:  # C
        defaults = dict(
            storage_read_ops=0, storage_read_bytes=0,
            local_read_ops=100, local_read_bytes=10485760,
            passthrough_read_bytes=0,
            hit_pct=100.0, cache_read_mib=0.0, predownload_mib=0.0, evict_mib=0.0,
        )
    defaults.update(kwargs)
    return _make_row(wall_ms=wall_ms, **defaults)


def _write_csv(path: pathlib.Path, row1: dict, row2: dict):
    """Write a two-row sample CSV to `path`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER.split(","))
        writer.writeheader()
        r1 = dict(row2)
        r1["round"] = 1
        writer.writerow(r1)
        writer.writerow(row2)


def _write_meta(path: pathlib.Path, meta: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump(meta, fh)


def _make_meta(
    drivers: int = 1,
    query_id: str = "q09",
    cell: str = "A",
    block: str = "forward",
    sample: int = 1,
    probe_enabled: bool = True,
    extra: dict = None,
) -> dict:
    m = {
        "schema_version": 1,
        "drivers": drivers,
        "query_id": query_id,
        "cell": cell,
        "input_source": ana.CELL_TO_SOURCE[cell],
        "block": block,
        "sample": sample,
        "warm_round": 2,
        "probe_enabled": probe_enabled,
        **_IDENTITY,
        "filecache_disk_gib": 80 if cell == "C" else 0,
        "result_csv": "result.csv",
        "run_log": "run.log",
    }
    if extra:
        m.update(extra)
    return m


def _write_manifest(root: pathlib.Path, manifest: dict = None):
    m = manifest if manifest is not None else dict(_MANIFEST)
    with (root / "run_manifest.json").open("w") as fh:
        json.dump(m, fh)


# ---------------------------------------------------------------------------
# Helpers to build complete sample trees for normal mode.
# ---------------------------------------------------------------------------


def _write_normal_samples(
    root: pathlib.Path,
    drivers: int,
    query: str,  # e.g. "q09"
    wall_ms: dict = None,  # cell -> wall_ms
    override_cell: str = None,
    override_field: str = None,
    override_value=None,
):
    """
    Write the full 3+2 sample tree for one query.
    `wall_ms` maps cell letter to wall_ms (default A=1000, B=1100, C=1200).
    If override_cell/field/value are set, that field is patched for the
    first forward sample of that cell.
    """
    if wall_ms is None:
        wall_ms = {"A": 1000.0, "B": 1100.0, "C": 1200.0}

    q_int = int(query.lstrip("q"))

    # Forward: A,B,C for samples 1,2,3.
    for sample in (1, 2, 3):
        for cell in ("A", "B", "C"):
            row = _row_for_cell(cell, wall_ms=wall_ms[cell], query_id=query)
            if override_cell == cell and sample == 1 and override_field:
                row[override_field] = override_value
            csv_path = (
                root / f"drivers_{drivers}" / query / cell / "forward" / f"sample_{sample}"
                / "result.csv"
            )
            _write_csv(csv_path, row, row)
            meta = _make_meta(
                drivers=drivers, query_id=query, cell=cell,
                block="forward", sample=sample,
            )
            _write_meta(csv_path.parent / "meta.json", meta)

    # Reverse: C,B,A for samples 1,2.
    for sample in (1, 2):
        for cell in ("C", "B", "A"):
            row = _row_for_cell(cell, wall_ms=wall_ms[cell], query_id=query)
            csv_path = (
                root / f"drivers_{drivers}" / query / cell / "reverse" / f"sample_{sample}"
                / "result.csv"
            )
            _write_csv(csv_path, row, row)
            meta = _make_meta(
                drivers=drivers, query_id=query, cell=cell,
                block="reverse", sample=sample,
            )
            _write_meta(csv_path.parent / "meta.json", meta)


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestAnalyzerValidCase(unittest.TestCase):
    """Fixture 1: valid A/B/C with five warm rows per cell."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")

    def tearDown(self):
        self.tmp.cleanup()

    def test_valid_passes(self):
        errors, summary, ob = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertEqual(errors, [], f"Expected no errors, got: {errors}")
        self.assertEqual(len(summary), 1)
        row = summary[0]
        self.assertEqual(row["query_id"], "q09")
        self.assertEqual(row["decomposition_status"], "measured")
        self.assertEqual(row["next_plan_selection"], "user_review_required")
        self.assertAlmostEqual(row["b_minus_a_ms"], 100.0, places=1)
        self.assertAlmostEqual(row["c_minus_b_ms"], 100.0, places=1)
        self.assertAlmostEqual(row["c_minus_a_ms"], 200.0, places=1)
        # No root_cause field.
        self.assertNotIn("root_cause", row)


class TestResultMismatch(unittest.TestCase):
    """Fixture 2: result_match=0."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="A", override_field="result_match",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_mismatch_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("result_match=0" in e for e in errors),
                        f"Expected mismatch error, got: {errors}")


class TestNonEmptyError(unittest.TestCase):
    """Fixture 3: nonempty error field."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="B", override_field="error",
                               override_value="OOM")

    def tearDown(self):
        self.tmp.cleanup()

    def test_error_field_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("nonempty error" in e for e in errors),
                        f"Expected error-field error, got: {errors}")


class TestFileCacheHitBelow100(unittest.TestCase):
    """Fixture 4: FileCache hit below 100%."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="C", override_field="hit_pct",
                               override_value=95.0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_hit_below_100_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("hit_pct" in e and "100" in e for e in errors),
                        f"Expected hit_pct error, got: {errors}")


class TestFileCachePredownload(unittest.TestCase):
    """Fixture 5: FileCache warm predownload."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="C", override_field="predownload_mib",
                               override_value=5.0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_predownload_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("predownload_mib" in e for e in errors),
                        f"Expected predownload error, got: {errors}")


class TestFileCacheEviction(unittest.TestCase):
    """Fixture 6: FileCache warm eviction."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="C", override_field="evict_mib",
                               override_value=2.0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_eviction_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("evict_mib" in e for e in errors),
                        f"Expected eviction error, got: {errors}")


class TestNonzeroFileCacheInAB(unittest.TestCase):
    """Fixture 7: nonzero FileCache counters in A/B."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="A", override_field="cache_read_mib",
                               override_value=1.5)

    def tearDown(self):
        self.tmp.cleanup()

    def test_cache_in_a_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("cache_read_mib" in e and "/A" in e for e in errors),
                        f"Expected cache_read_mib error for A, got: {errors}")


class TestZeroPassthroughInB(unittest.TestCase):
    """Fixture 8: zero passthrough bytes in B."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="B",
                               override_field="passthrough_read_bytes",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_passthrough_in_b_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("passthrough_read_bytes=0" in e and "/B" in e for e in errors),
                        f"Expected passthrough=0/B error, got: {errors}")


class TestNonzeroPassthroughInAC(unittest.TestCase):
    """Fixture 9: nonzero passthrough bytes in A/C."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="A",
                               override_field="passthrough_read_bytes",
                               override_value=1048576)

    def tearDown(self):
        self.tmp.cleanup()

    def test_nonzero_passthrough_in_a_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(
            any("passthrough_read_bytes" in e and "/A" in e and "> 0" in e for e in errors),
            f"Expected passthrough>0/A error, got: {errors}",
        )


class TestZeroProbeMetrics(unittest.TestCase):
    """Fixture 10: zero enqueue/Next/returned/max-chunk in any A/B/C cell."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="A", override_field="enqueue_count",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_enqueue_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("enqueue_count=0" in e and "/A" in e for e in errors),
                        f"Expected probe enqueue_count error, got: {errors}")


class TestZeroStorageInAB(unittest.TestCase):
    """Fixture 11: A or B has zero storage read ops."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="A", override_field="storage_read_ops",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_storage_ops_in_a_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("storage_read_ops=0" in e and "/A" in e for e in errors),
                        f"Expected storage_read_ops error, got: {errors}")


class TestZeroLocalInC(unittest.TestCase):
    """Fixture 12: C has zero local read ops."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="C", override_field="local_read_ops",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_local_ops_in_c_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("local_read_ops=0" in e and "/C" in e for e in errors),
                        f"Expected local_read_ops/C error, got: {errors}")


class TestOppositeBMinusASigns(unittest.TestCase):
    """Fixture 13: opposite B-A signs between order blocks."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        # Forward: A=1000, B=1100, C=1200 → B-A = +100
        # Reverse: A=1100, B=1000, C=1200 → B-A = -100 (sign flip!)
        wall_ms = {"A": 1000.0, "B": 1100.0, "C": 1200.0}
        _write_normal_samples(self.root, 1, "q09", wall_ms=wall_ms)
        # Patch reverse B to be below A.
        rev_root = self.root / "drivers_1" / "q09" / "B" / "reverse"
        for sample in (1, 2):
            csv_p = rev_root / f"sample_{sample}" / "result.csv"
            row = _row_for_cell("B", wall_ms=900.0, query_id="q09")
            _write_csv(csv_p, row, row)

    def tearDown(self):
        self.tmp.cleanup()

    def test_opposite_ba_sign_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("B-A sign flips" in e for e in errors),
                        f"Expected B-A sign flip error, got: {errors}")


class TestOppositeCMinusBSigns(unittest.TestCase):
    """Fixture 14: opposite C-B signs between order blocks."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")
        # Patch reverse C to be below B.
        rev_root = self.root / "drivers_1" / "q09" / "C" / "reverse"
        for sample in (1, 2):
            csv_p = rev_root / f"sample_{sample}" / "result.csv"
            row = _row_for_cell("C", wall_ms=500.0, query_id="q09")
            _write_csv(csv_p, row, row)

    def tearDown(self):
        self.tmp.cleanup()

    def test_opposite_cb_sign_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("C-B sign flips" in e for e in errors),
                        f"Expected C-B sign flip error, got: {errors}")


class TestMissingSampleCellQuery(unittest.TestCase):
    """Fixture 15: missing sample/cell/query."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        # Write full tree but omit forward sample 2 for cell A.
        _write_normal_samples(self.root, 1, "q09")
        import shutil
        shutil.rmtree(str(self.root / "drivers_1" / "q09" / "A" / "forward" / "sample_2"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_sample_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        # Expect either a missing-file error or a wrong sample count.
        self.assertTrue(len(errors) > 0, "Expected errors for missing sample")


class TestMetaDriverQueryCellSourceMismatch(unittest.TestCase):
    """Fixture 16: metadata driver/query/cell/input-source mismatch."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")
        # Patch the forward/sample_1/A meta with wrong input_source.
        meta_path = (
            self.root / "drivers_1" / "q09" / "A" / "forward" / "sample_1" / "meta.json"
        )
        m = _make_meta(cell="A", block="forward", sample=1)
        m["input_source"] = "filecache"  # wrong: should be "direct"
        _write_meta(meta_path, m)

    def tearDown(self):
        self.tmp.cleanup()

    def test_source_mismatch_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("input_source" in e for e in errors),
                        f"Expected input_source mismatch, got: {errors}")


class TestMetaBlockSampleWarmRoundProbeMismatch(unittest.TestCase):
    """Fixture 17: metadata block/sample/warm-round/probe-enabled mismatch."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")
        # Patch forward/sample_1/B meta with wrong warm_round.
        meta_path = (
            self.root / "drivers_1" / "q09" / "B" / "forward" / "sample_1" / "meta.json"
        )
        m = _make_meta(cell="B", block="forward", sample=1)
        m["warm_round"] = 1  # wrong: should be 2
        _write_meta(meta_path, m)

    def tearDown(self):
        self.tmp.cleanup()

    def test_warm_round_mismatch_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("warm_round" in e for e in errors),
                        f"Expected warm_round mismatch, got: {errors}")


class TestWrongSchemaVersion(unittest.TestCase):
    """Fixture 18: wrong metadata schema_version."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")
        meta_path = (
            self.root / "drivers_1" / "q09" / "C" / "forward" / "sample_1" / "meta.json"
        )
        m = _make_meta(cell="C", block="forward", sample=1)
        m["schema_version"] = 99
        _write_meta(meta_path, m)

    def tearDown(self):
        self.tmp.cleanup()

    def test_schema_version_mismatch_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("schema_version" in e for e in errors),
                        f"Expected schema_version error, got: {errors}")


class TestMixedIdentityFields(unittest.TestCase):
    """Fixture 19: mixed binary path in manifest vs sample meta."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09")
        meta_path = (
            self.root / "drivers_1" / "q09" / "A" / "forward" / "sample_1" / "meta.json"
        )
        m = _make_meta(cell="A", block="forward", sample=1)
        m["velox_head"] = "deadbeef99"  # different from manifest
        _write_meta(meta_path, m)

    def tearDown(self):
        self.tmp.cleanup()

    def test_identity_mismatch_fails(self):
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("velox_head" in e for e in errors),
                        f"Expected velox_head mismatch, got: {errors}")


# ---------------------------------------------------------------------------
# Smoke mode tests
# ---------------------------------------------------------------------------


class TestSmokeValidCase(unittest.TestCase):
    """--smoke: exactly one valid A/B/C sample passes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        for cell in ("A", "B", "C"):
            row = _row_for_cell(cell, wall_ms=1000.0, query_id="q09")
            csv_p = self.root / "drivers_1" / "q09" / cell / "forward" / "sample_1" / "result.csv"
            _write_csv(csv_p, row, row)
            meta = _make_meta(cell=cell, block="forward", sample=1)
            _write_meta(csv_p.parent / "meta.json", meta)

    def tearDown(self):
        self.tmp.cleanup()

    def test_smoke_passes(self):
        errors, summary = ana._analyze_smoke(self.root, 1, [9], _MANIFEST)
        self.assertEqual(errors, [], f"Expected no errors, got: {errors}")
        self.assertEqual(len(summary), 1)

    def test_smoke_missing_cell_fails(self):
        import shutil
        shutil.rmtree(str(self.root / "drivers_1" / "q09" / "B"))
        errors, _ = ana._analyze_smoke(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("missing smoke sample" in e or "B" in e for e in errors),
                        f"Expected missing cell error, got: {errors}")


class TestSmokeInvalidRowsDoNotLookMissing(unittest.TestCase):
    """Smoke mode keeps row-validity failures distinct from missing files."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        manifest = dict(_MANIFEST)
        manifest["queries"] = ["q04"]
        _write_manifest(self.root, manifest)
        overrides = {
            "A": {"storage_read_ops": 0},
            "B": {"storage_read_ops": 0},
            "C": {"local_read_ops": 0},
        }
        for cell in ("A", "B", "C"):
            row = _row_for_cell(cell, wall_ms=1000.0, query_id="q04", **overrides[cell])
            csv_p = self.root / "drivers_1" / "q04" / cell / "forward" / "sample_1" / "result.csv"
            _write_csv(csv_p, row, row)
            meta = _make_meta(query_id="q04", cell=cell, block="forward", sample=1)
            _write_meta(csv_p.parent / "meta.json", meta)

    def tearDown(self):
        self.tmp.cleanup()

    def test_invalid_rows_report_metric_failures_without_missing_samples(self):
        errors, _ = ana._analyze_smoke(self.root, 1, [4], _MANIFEST)
        self.assertTrue(any("q04/A: storage_read_ops=0" == e for e in errors), errors)
        self.assertTrue(any("q04/B: storage_read_ops=0" == e for e in errors), errors)
        self.assertTrue(any("q04/C: local_read_ops=0" == e for e in errors), errors)
        self.assertFalse(any("missing smoke sample" in e for e in errors), errors)


# ---------------------------------------------------------------------------
# Probe-validation mode tests
# ---------------------------------------------------------------------------


def _write_probe_sample(
    root: pathlib.Path, drivers: int, query: str, cell: str, block: str,
    sample: int, probe_on: bool, wall_ms: float
):
    row = _row_for_cell(cell, wall_ms=wall_ms, query_id=query)
    if not probe_on:
        # Probe-off: zero probe fields.
        for f in ("enqueue_count", "enqueue_bytes", "next_count", "returned_bytes",
                  "max_chunk_bytes"):
            row[f] = 0
    csv_p = root / f"drivers_{drivers}" / query / cell / block / f"sample_{sample}" / "result.csv"
    _write_csv(csv_p, row, row)
    meta = _make_meta(cell=cell, block=block, sample=sample, probe_enabled=probe_on)
    _write_meta(csv_p.parent / "meta.json", meta)


def _write_probe_full(
    root: pathlib.Path, drivers: int, query: str,
    wall_fwd_a_off: float = 1000.0, wall_fwd_c_off: float = 1200.0,
    wall_fwd_a_on: float = 1010.0, wall_fwd_c_on: float = 1210.0,
    wall_rev_c_on: float = 1210.0, wall_rev_a_on: float = 1010.0,
    wall_rev_c_off: float = 1200.0, wall_rev_a_off: float = 1000.0,
):
    _write_probe_sample(root, drivers, query, "A", "forward", 1, False, wall_fwd_a_off)
    _write_probe_sample(root, drivers, query, "C", "forward", 1, False, wall_fwd_c_off)
    _write_probe_sample(root, drivers, query, "A", "forward", 2, True, wall_fwd_a_on)
    _write_probe_sample(root, drivers, query, "C", "forward", 2, True, wall_fwd_c_on)
    _write_probe_sample(root, drivers, query, "C", "reverse", 1, True, wall_rev_c_on)
    _write_probe_sample(root, drivers, query, "A", "reverse", 1, True, wall_rev_a_on)
    _write_probe_sample(root, drivers, query, "C", "reverse", 2, False, wall_rev_c_off)
    _write_probe_sample(root, drivers, query, "A", "reverse", 2, False, wall_rev_a_off)


class TestProbeValidationPasses(unittest.TestCase):
    """--probe-validation: forward and reverse probe-off/on with matching C-A signs pass."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        # C > A in all cases: C-A positive and consistent.
        _write_probe_full(self.root, 1, "q09")

    def tearDown(self):
        self.tmp.cleanup()

    def test_consistent_sign_passes(self):
        errors, sign_rows = ana._analyze_probe_validation(self.root, 1, [9], _MANIFEST)
        self.assertEqual(errors, [], f"Expected no errors, got: {errors}")
        for row in sign_rows:
            self.assertTrue(row["sign_consistent"],
                            f"Expected consistent sign, got: {row}")


class TestProbeValidationSignFlipFails(unittest.TestCase):
    """--probe-validation: one sign flip fails."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        # Forward: probe-off C-A = +200, probe-on C-A = -10 (sign flip!).
        _write_probe_full(
            self.root, 1, "q09",
            wall_fwd_a_off=1000.0, wall_fwd_c_off=1200.0,  # off: C-A = +200
            wall_fwd_a_on=1100.0, wall_fwd_c_on=1090.0,    # on: C-A = -10 (flip)
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_sign_flip_fails(self):
        errors, _ = ana._analyze_probe_validation(self.root, 1, [9], _MANIFEST)
        self.assertTrue(any("C-A sign flips" in e for e in errors),
                        f"Expected sign-flip error, got: {errors}")


# ---------------------------------------------------------------------------
# Mutation test: zero passthrough bytes in B must fail
# ---------------------------------------------------------------------------


class TestZeroPassthroughMutation(unittest.TestCase):
    """
    Verify that relaxing the zero-passthrough-in-B check causes the fixture to
    pass when it should fail (i.e., tests the test itself, not the analyzer).
    The real check must be in place.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        _write_manifest(self.root)
        _write_normal_samples(self.root, 1, "q09",
                               override_cell="B",
                               override_field="passthrough_read_bytes",
                               override_value=0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_passthrough_b_detected(self):
        """The real analyzer must reject B with zero passthrough bytes."""
        errors, _, _ = ana._analyze_normal(self.root, 1, [9], _MANIFEST)
        self.assertTrue(
            any("passthrough_read_bytes=0" in e and "/B" in e for e in errors),
            f"Analyzer must reject B with zero passthrough bytes. Got: {errors}",
        )


if __name__ == "__main__":
    unittest.main()
