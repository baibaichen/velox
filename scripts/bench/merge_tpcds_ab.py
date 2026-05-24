#!/usr/bin/env python3
"""Merge two CSV outputs from velox_tpcds_benchmark (--input_source=cbi vs
fscache) into a single Markdown report. See spec §3.3.

Usage:
    merge_tpcds_ab.py CBI_CSV FSCACHE_CSV > report.md
"""
import csv
import math
import platform
import socket
import subprocess
import sys


def read_csv(path):
    rows = {}  # (round, query_id) → row
    with open(path) as f:
        for row in csv.DictReader(f):
            rows[(int(row["round"]), row["query_id"])] = row
    return rows


def geomean(values):
    positive = [v for v in values if v > 0]
    if not positive:
        return float("nan")
    return math.exp(sum(math.log(v) for v in positive) / len(positive))


def pct_delta(new, base):
    return 100.0 * (new - base) / base if base else float("nan")


def mean(*values):
    return sum(values) / len(values)


def cold_table(cbi, fscache, queries):
    print("**Table A — Cold round (round=1)**\n")
    print("| query | CBI ms | FsCache ms | Δ% | CBI hit% | FsCache hit% | CBI dl MiB | FsCache dl MiB |")
    print("|-------|-------:|-----------:|---:|---------:|-------------:|-----------:|---------------:|")
    for q in queries:
        c = cbi.get((1, q))
        f = fscache.get((1, q))
        if not c or not f or c["error"] or f["error"]:
            print(f"| {q} | — | — | — | — | — | — | — |")
            continue
        c_ms = float(c["wall_ms"])
        f_ms = float(f["wall_ms"])
        print(f"| {q} | {c_ms:.1f} | {f_ms:.1f} | {pct_delta(f_ms, c_ms):+.1f}% | "
              f"{float(c['hit_pct']):.1f}% | {float(f['hit_pct']):.1f}% | "
              f"{float(c['bytes_dl_mib']):.1f} | {float(f['bytes_dl_mib']):.1f} |")


def warm_table(cbi, fscache, queries):
    print("\n**Table B — Warm mean of round 2 + round 3**\n")
    print("| query | CBI ms | FsCache ms | Δ% | CBI hit% | FsCache hit% | CBI dl MiB | FsCache dl MiB |")
    print("|-------|-------:|-----------:|---:|---------:|-------------:|-----------:|---------------:|")
    for q in queries:
        c2, c3 = cbi.get((2, q)), cbi.get((3, q))
        f2, f3 = fscache.get((2, q)), fscache.get((3, q))
        rounds = [c2, c3, f2, f3]
        if not all(rounds) or any(r["error"] for r in rounds):
            print(f"| {q} | — | — | — | — | — | — | — |")
            continue
        c_ms = mean(float(c2["wall_ms"]), float(c3["wall_ms"]))
        f_ms = mean(float(f2["wall_ms"]), float(f3["wall_ms"]))
        # Average every column over rounds 2+3, not just wall_ms — otherwise
        # the "warm mean" label silently lies on the auxiliary columns and
        # a reader can't tell which side a hit% / dl MiB number came from.
        c_hit = mean(float(c2["hit_pct"]), float(c3["hit_pct"]))
        f_hit = mean(float(f2["hit_pct"]), float(f3["hit_pct"]))
        c_dl = mean(float(c2["bytes_dl_mib"]), float(c3["bytes_dl_mib"]))
        f_dl = mean(float(f2["bytes_dl_mib"]), float(f3["bytes_dl_mib"]))
        print(f"| {q} | {c_ms:.1f} | {f_ms:.1f} | {pct_delta(f_ms, c_ms):+.1f}% | "
              f"{c_hit:.1f}% | {f_hit:.1f}% | "
              f"{c_dl:.1f} | {f_dl:.1f} |")


def summary_table(cbi, fscache, queries):
    cbi_cold = [float(cbi[(1, q)]["wall_ms"]) for q in queries
                if (1, q) in cbi and not cbi[(1, q)]["error"]]
    fscache_cold = [float(fscache[(1, q)]["wall_ms"]) for q in queries
                    if (1, q) in fscache and not fscache[(1, q)]["error"]]
    cbi_warm, fscache_warm, deltas = [], [], []
    wins = losses = regressions = failed = 0
    regression_qs = []
    for q in queries:
        cset = [cbi.get((r, q)) for r in (2, 3)]
        fset = [fscache.get((r, q)) for r in (2, 3)]
        if not all(cset) or not all(fset) or any(r["error"] for r in cset + fset):
            failed += 1
            continue
        c_ms = mean(*(float(r["wall_ms"]) for r in cset))
        f_ms = mean(*(float(r["wall_ms"]) for r in fset))
        cbi_warm.append(c_ms)
        fscache_warm.append(f_ms)
        delta = pct_delta(f_ms, c_ms) if c_ms else 0.0
        deltas.append(delta)
        if delta <= -5.0:
            wins += 1
        elif delta >= 5.0:
            losses += 1
        if delta >= 20.0:
            regressions += 1
            regression_qs.append(q)

    print("\n**Table C — Summary**\n")
    print("|                  | CBI  | FsCache | Δ |")
    print("|------------------|-----:|--------:|---|")
    c_geo = geomean(cbi_cold)
    f_geo = geomean(fscache_cold)
    print(f"| cold geomean ms  | {c_geo:.0f} | {f_geo:.0f} | {pct_delta(f_geo, c_geo):+.1f}% |")
    cw_geo = geomean(cbi_warm)
    fw_geo = geomean(fscache_warm)
    print(f"| warm geomean ms  | {cw_geo:.0f} | {fw_geo:.0f} | {pct_delta(fw_geo, cw_geo):+.1f}% |")
    print(f"| wins  (FsCache ≥5% faster) | — | {wins} / {len(deltas)} | |")
    print(f"| losses (FsCache ≥5% slower)| — | {losses} / {len(deltas)} | |")
    print(f"| regressions > 20%          | — | {regressions} / {len(deltas)} | "
          f"({', '.join(regression_qs)}) |")
    print(f"| failed (excluded)          | — | {failed} / {len(deltas) + failed} | |")


def header(cbi_csv, fscache_csv):
    print("# FsCache vs CBI — TPC-DS scale 100 A/B\n")
    print("**Spec:** `docs/superpowers/specs/2026-05-23-fscache-vs-cbi-tpcds-design.md`\n")
    # Spec §3.4 requires the header to capture the dataset path. The merge
    # script only sees the post-bench CSVs, so log those as the proxy; the
    # raw `--data_path` is captured in run_ab.sh itself.
    print(f"**Inputs:** `{cbi_csv}` (CBI), `{fscache_csv}` (FsCache)\n")
    print(f"**Host:** {socket.gethostname()}, kernel {platform.release()}")
    try:
        cpu = subprocess.check_output(
            ["bash", "-c", "lscpu | grep 'Model name' | head -1"]).decode().strip()
        print(f"**CPU:** {cpu}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    print("**Build:** RelWithDebInfo, GCC-13\n")
    print("**Δ% convention:** negative ⇒ FsCache faster, positive ⇒ slower.\n")
    print("**Caveat (spec §3.2):** `bytes_dl_mib` semantics differ between sides. "
          "CBI = remote→RAM (SsdCache hits not counted); FsCache = remote→disk only. "
          "Do not compare absolute download volumes across sides without this context.\n")


def main():
    if len(sys.argv) != 3:
        sys.exit(f"Usage: {sys.argv[0]} CBI_CSV FSCACHE_CSV")
    cbi = read_csv(sys.argv[1])
    fscache = read_csv(sys.argv[2])
    queries = sorted({q for (_, q) in cbi.keys() | fscache.keys()})

    header(sys.argv[1], sys.argv[2])
    cold_table(cbi, fscache, queries)
    warm_table(cbi, fscache, queries)
    summary_table(cbi, fscache, queries)


if __name__ == "__main__":
    main()
