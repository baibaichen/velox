#!/usr/bin/env python3
"""Parse and align layer-1 hash-table benchmark output into markdown tables.

Consumes raw output from the two benchmarks (does NOT run them):
  - Velox: velox_exec_ch_hashtable_layer_benchmark -> "RESULT key=value ..." lines
           (arms: ch, velox). build_wall_ms / probe_wall_ms are already per single
           build / single full-table probe. The ch arm's hash_mode field carries
           the authoritative FixedKeyMap Type (key64/keys128/key_string/hashed/...).
  - CH native: ch_hj_bench (google-benchmark) -> "BM_ProbeL1/<suite> <Time> ns ..."
           and "BM_Build/<suite> ...". gbench Time is one full-table op; /1e6 -> ms.

Both benchmarks measure the same unit -- one full-table build and one full-table
probe over the same key set -- so the wall-clock milliseconds align directly.

Usage:
  analyze_layer1_results.py velox.txt ch.txt [velox2.txt ...]
Files may be mixed and repeated; each line is classified by its own format.
"""

import re
import sys
from collections import defaultdict

# CH gbench suite name -> (distribution, velox key_layout). The seq suite is the
# only sequential one; every other CH suite generates uniform keys.
CH_SUITE = {
    "bigint_seq": ("sequential", "bigint"),
    "bigint_uniform": ("uniform", "bigint"),
    "2xbigint": ("uniform", "2xbigint"),
    "bigint_2xint": ("uniform", "bigint_2xint"),
    "varchar_short_low": ("uniform", "varchar_short_low"),
    "varchar_short_high": ("uniform", "varchar_short_high"),
    "varchar_long_low": ("uniform", "varchar_long_low"),
    "varchar_long_high": ("uniform", "varchar_long_high"),
}

KEY_ORDER = [
    "bigint", "2xbigint", "bigint_2xint", "5xbigint", "bigint_varchar",
    "varchar_short_low", "varchar_short_high",
    "varchar_long_low", "varchar_long_high",
]
DIST_ORDER = {"sequential": 0, "uniform": 1}


def cell_key(row):
    # (distribution, rows, key_layout) identifies one benchmark configuration.
    return (row["distribution"], row["rows"], row["key_layout"])


def parse_velox_line(line):
    if not line.startswith("RESULT "):
        return None
    kv = dict(re.findall(r"(\w+)=([^\s]+)", line))
    arm = kv.get("arm")
    if arm not in ("ch", "velox"):
        return None
    return {
        "source": "velox_bench",
        "arm": arm,
        "distribution": kv["distribution"],
        "key_layout": kv["key_layout"],
        "rows": int(kv["build_rows"]),
        "build_ms": float(kv["build_wall_ms"]),
        "probe_ms": float(kv["probe_wall_ms"]),
        "peak_bytes": int(kv["peak_bytes"]),
        "hash_mode": kv.get("hash_mode", ""),
    }


# CH bench prints the suite name plus a build_n counter (the CH_HJ_N row count).
# The suite name maps 1:1 to (distribution, key_layout); build_n gives the rows.
# Together they form the same (distribution, rows, key_layout) key the Velox
# RESULT lines use, so CH native rows attach to the exact matching cell.
def parse_ch_line(line):
    m = re.match(r"(BM_(?:ProbeL1|Build))/(\S+)\s+(\d+)\s+ns\s+(\d+)\s+ns", line)
    if not m:
        return None
    kind, suite, real_ns, _cpu_ns = m.groups()
    if suite not in CH_SUITE:
        return None
    # gbench prints counters with SI suffixes, e.g. build_n=400k or build_n=4M.
    build_n = re.search(r"build_n=([\d.]+)([kMG])?", line)
    if build_n is None:
        return None
    si = {"k": 1e3, "M": 1e6, "G": 1e9, None: 1.0}[build_n.group(2)]
    dist, key_layout = CH_SUITE[suite]
    return {
        "source": "ch_native",
        "kind": kind,
        "distribution": dist,
        "key_layout": key_layout,
        "rows": round(float(build_n.group(1)) * si),
        "ms": int(real_ns) / 1e6,
    }


def collect(paths):
    # cells[config] = {"ch": row, "velox": row,
    #                  "ch_build_ms": x, "ch_probe_ms": y}
    # config = (distribution, rows, key_layout). CH native and Velox rows both
    # carry rows now, so they land in the same cell with no inference.
    cells = defaultdict(dict)
    for path in paths:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                velox = parse_velox_line(line)
                if velox is not None:
                    cells[cell_key(velox)][velox["arm"]] = velox
                    continue
                ch = parse_ch_line(line)
                if ch is not None:
                    key = (ch["distribution"], ch["rows"], ch["key_layout"])
                    field = "ch_build_ms" if ch["kind"] == "BM_Build" else "ch_probe_ms"
                    cells[key][field] = ch["ms"]
    return cells


def sort_key(config):
    dist, rows, key_layout = config
    kidx = KEY_ORDER.index(key_layout) if key_layout in KEY_ORDER else len(KEY_ORDER)
    return (DIST_ORDER.get(dist, 9), kidx, rows if rows is not None else -1)


def fmt(value, digits=1):
    return "-" if value is None else f"{value:.{digits}f}"


def ratio(value):
    # Format a Port/other speed ratio; > 1 means CH Port is faster.
    return "-" if value is None else f"{value:.2f}x"


def ch_port_ms(cell, field):
    # CH Port = the single migrated ch arm's value for this metric.
    if "ch" in cell:
        return cell["ch"].get(field)
    return None


def ch_map_type(cell):
    # Authoritative FixedKeyMap Type the ch arm selected (key64/keys128/...).
    return cell["ch"]["hash_mode"] if "ch" in cell else "-"


def render(cells):
    configs = sorted(cells.keys(), key=sort_key)

    def mb(cell):
        return cell["ch"]["peak_bytes"] / (1 << 20) if "ch" in cell else None

    def velox_mb(cell):
        return cell["velox"]["peak_bytes"] / (1 << 20) if "velox" in cell else None

    lines = []
    lines.append("## Probe wall-clock (ms, one full-table probe; lower = faster)")
    lines.append("")
    lines.append(
        "| dist | rows | key | CH map | Velox mode | CH Port | Velox | "
        "CH native | Port/Velox | Port/native |"
    )
    lines.append("|---|---:|---|---|---|---:|---:|---:|---:|---:|")
    for config in configs:
        dist, rows, key_layout = config
        cell = cells[config]
        mode = cell["velox"]["hash_mode"] if "velox" in cell else "-"
        port = ch_port_ms(cell, "probe_ms")
        velox = cell["velox"]["probe_ms"] if "velox" in cell else None
        native = cell.get("ch_probe_ms")
        vsVelox = (velox / port) if (port and velox) else None
        vsNative = (native / port) if (port and native) else None
        lines.append(
            f"| {dist} | {rows} | {key_layout} | {ch_map_type(cell)} | {mode} | "
            f"{fmt(port)} | {fmt(velox)} | {fmt(native)} | "
            f"{ratio(vsVelox)} | {ratio(vsNative)} |"
        )

    lines.append("")
    lines.append("## Build wall-clock (ms, one full build; lower = faster)")
    lines.append("")
    lines.append(
        "| dist | rows | key | CH map | CH Port | Velox | CH native | "
        "Port/Velox | Port/native |"
    )
    lines.append("|---|---:|---|---|---:|---:|---:|---:|---:|")
    for config in configs:
        dist, rows, key_layout = config
        cell = cells[config]
        port = ch_port_ms(cell, "build_ms")
        velox = cell["velox"]["build_ms"] if "velox" in cell else None
        native = cell.get("ch_build_ms")
        vsVelox = (velox / port) if (port and velox) else None
        vsNative = (native / port) if (port and native) else None
        lines.append(
            f"| {dist} | {rows} | {key_layout} | {ch_map_type(cell)} | "
            f"{fmt(port)} | {fmt(velox)} | {fmt(native)} | "
            f"{ratio(vsVelox)} | {ratio(vsNative)} |"
        )

    lines.append("")
    lines.append("## Peak memory (MB) — Velox-process arms only, NOT three-way")
    lines.append("")
    lines.append(
        "Both columns are measured inside the Velox benchmark process "
        "(pool peakBytes): CH Port = the ch arm, Velox = the velox native arm. "
        "The CH-native benchmark reports no memory, so this table does not "
        "include it."
    )
    lines.append("")
    lines.append("| dist | rows | key | CH map | CH Port | Velox |")
    lines.append("|---|---:|---|---|---:|---:|")
    for config in configs:
        dist, rows, key_layout = config
        cell = cells[config]
        lines.append(
            f"| {dist} | {rows} | {key_layout} | {ch_map_type(cell)} | "
            f"{fmt(mb(cell))} | {fmt(velox_mb(cell))} |"
        )

    lines.append("")
    lines.append(
        "> CH Port = the single ch arm; CH map is the authoritative FixedKeyMap "
        "Type it selected. All ms columns are one full-table build / one "
        "full-table probe, with data generation excluded from timing on both "
        "sides and both probes counting hits without materializing results, so "
        "the ms figures are same-unit. CH native rows come from the build_n "
        "counter the CH bench emits, so they attach to the exact matching cell."
    )
    lines.append(
        "> Cross-process caveat: the Velox arms are built with gcc -O3 (Release) "
        "and the CH-native arm with clang -O2 -g -DNDEBUG (RelWithDebInfo). "
        "Asserts are disabled on both (NDEBUG), but the differing compiler and "
        "optimization level mean CH-native absolute ms is only a trend "
        "reference, not a strict head-to-head with the Velox arms. The "
        "CH-Port-vs-Velox comparison (same process, same compiler) is the "
        "apples-to-apples one."
    )
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        sys.exit(f"usage: {sys.argv[0]} <output-file> [more-files ...]")
    cells = collect(sys.argv[1:])
    if not cells:
        sys.exit("no recognizable benchmark lines found")
    print(render(cells))


if __name__ == "__main__":
    main()
