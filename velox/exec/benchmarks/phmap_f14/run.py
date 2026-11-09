#!/usr/bin/env python3
"""Run paired Folly benchmarks with a wall-clock deadline; preserve raw evidence."""

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import resource
import statistics
import subprocess
import time

KINDS = ("i64_seq", "i64_random", "str8", "str24", "str128")
SIZES = (16, 300, 4096, 32768, 90000, 100000)


def sha256(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def capture(*command):
    return subprocess.check_output(command, text=True).strip()


def comparison(f14, phmap):
    assert len(f14) == len(phmap) and f14
    assert all(value > 0 for value in f14 + phmap)
    ratios = [b / a for a, b in zip(f14, phmap)]
    winner = "incomplete" if len(ratios) < 7 else (
        "phmap" if max(ratios) < 0.95 else
        "F14" if min(ratios) > 1.05 else "no_stable_5pct_winner")
    return {
        "rounds": len(ratios),
        "f14_ns_median": statistics.median(f14),
        "f14_ns_min": min(f14),
        "f14_ns_max": max(f14),
        "phmap_ns_median": statistics.median(phmap),
        "phmap_ns_min": min(phmap),
        "phmap_ns_max": max(phmap),
        "phmap_over_f14_median": statistics.median(ratios),
        "phmap_over_f14_min": min(ratios),
        "phmap_over_f14_max": max(ratios),
        "winner": winner,
    }


def self_check():
    assert comparison([10] * 7, [5] * 7)["winner"] == "phmap"
    assert comparison([10] * 7, [20] * 7)["winner"] == "F14"
    assert comparison([10] * 7, [10.1] * 7)["winner"] == "no_stable_5pct_winner"
    assert comparison([10] * 7, [5, 5, 5, 5, 5, 5, 20])["winner"] == "no_stable_5pct_winner"
    assert comparison([10], [5])["winner"] == "incomplete"
    assert comparison([10, 100], [20, 50])["phmap_over_f14_median"] == 1.25
    assert unconverged("  \x1b[1mF14/grow\x1b[0m: p50.00=12ns\n") == ["F14/grow"]
    assert unconverged("F14/grow 12ns\n") == []


def unconverged(stderr):
    plain = re.sub(r"\x1b\[[0-9;]*m", "", stderr)
    return re.findall(r"^  ((?:F14|phmap)/\S+): p", plain, re.MULTILINE)


def summarize(output):
    samples = {}
    rows = []
    for path in sorted((output / "raw").glob("round-*.json")):
        _, round_id, kind, size = path.stem.split("-")
        quality_path = output / "quality" / path.name
        if not quality_path.exists():
            continue  # A killed process may leave JSON without a completion record.
        results = json.loads(path.read_text())
        quality = json.loads(quality_path.read_text())
        assert len(results) == 24, (path, len(results))
        names = set()
        for _, name, nanos, *unused in results:
            assert name not in names and nanos > 0, (path, name, nanos)
            names.add(name)
            container, case = name.split("/", 1)
            assert container in ("F14", "phmap")
            key = (kind, int(size), case)
            samples.setdefault(key, {}).setdefault(int(round_id), {})[container] = (
                nanos, name not in quality["unconverged"])
            rows.append(dict(round=int(round_id), kind=kind, ndv=int(size),
                             case=case, container=container, ns_per_op=nanos,
                             converged=name not in quality["unconverged"]))
    if not rows:
        return
    write_csv(output / "samples.csv", rows)
    summary = []
    for (kind, size, case), rounds in sorted(samples.items()):
        assert all(set(pair) == {"F14", "phmap"} for pair in rounds.values())
        ordered = [rounds[i] for i in sorted(rounds)]
        stress = kind.startswith("str") and int(kind[3:]) > 8 and int(kind[3:]) * size > 1 << 20
        stats = comparison([p["F14"][0] for p in ordered],
                           [p["phmap"][0] for p in ordered])
        count = sum(not pair[c][1] for pair in ordered for c in ("F14", "phmap"))
        if count:
            stats["winner"] = "unconverged"
        summary.append(dict(kind=kind, ndv=size, case=case,
                            scope="container_stress" if stress else "typical_range",
                            unconverged_samples=count, **stats))
    write_csv(output / "summary.csv", summary)


def write_csv(path, rows):
    with path.open("w", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.summarize_only:
        summarize(output)
        return
    if not args.binary or not args.phmap_root or not args.deadline:
        raise ValueError("--binary, --phmap-root and --deadline are required")
    if (output / "metadata.json").exists():
        raise ValueError("Use a new output directory to avoid mixing experiments")
    binary = args.binary.resolve()
    deadline = dt.datetime.fromisoformat(args.deadline)
    if deadline.tzinfo is None:
        raise ValueError("Deadline must include a timezone")
    deadline = deadline.timestamp()
    if args.cpu not in os.sched_getaffinity(0):
        raise ValueError("CPU is outside the allowed affinity mask")
    tracer = int(next(line.split()[1] for line in Path("/proc/self/status").read_text().splitlines()
                      if line.startswith("TracerPid:")))
    if tracer:
        raise ValueError("Run timings outside ptrace/debugger supervision")
    (output / "raw").mkdir()
    (output / "quality").mkdir()
    source = Path(__file__).resolve().parent
    flags = ["--bm_mode=adaptive", "--bm_target_percentile=50",
             "--bm_target_precision_pct=3", "--bm_min_secs=0.05",
             "--bm_min_samples=20", "--bm_max_secs=1",
             "--bm_slice_usec=1000", "--bm_warm_up_iteration"]
    metadata = {
        "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "deadline": args.deadline, "cpu": args.cpu, "rounds_requested": 7,
        "velox_commit": capture("git", "rev-parse", "HEAD"),
        "phmap_commit": capture("git", "-C", str(args.phmap_root), "rev-parse", "HEAD"),
        "binary_sha256": sha256(binary),
        "benchmark_sha256": sha256(source / "UniqueValueSetBenchmark.cpp"),
        "runner_sha256": sha256(source / "run.py"),
        "VectorHasher.h_sha256": sha256(source.parents[3] / "velox/exec/VectorHasher.h"),
        "compiler": capture("c++", "--version"),
        "kernel": capture("uname", "-srvm"),
        "tracer_pid": tracer,
        "cpu_details": capture("lscpu"),
        "dynamic_libraries": capture("ldd", str(binary)),
        "load_start": list(os.getloadavg()),
        "folly_flags": flags, "status": "running", "completed_processes": 0,
    }
    cpu_path = Path(f"/sys/devices/system/cpu/cpu{args.cpu}")
    for label, path in {
        "governor": cpu_path / "cpufreq/scaling_governor",
        "smt_siblings": cpu_path / "topology/thread_siblings_list",
    }.items():
        metadata[label] = path.read_text().strip() if path.exists() else "unknown"
    for name in ("F14Set.h", "detail/F14Table.h", "detail/F14Policy.h"):
        metadata["folly/" + name] = sha256(Path("/usr/local/include/folly/container") / name)
    metadata["folly/Benchmark.h"] = sha256("/usr/local/include/folly/Benchmark.h")
    metadata["libfolly.a"] = sha256("/usr/local/lib/libfolly.a")
    metadata["libfollybenchmark.a"] = sha256("/usr/local/lib/libfollybenchmark.a")
    metadata_path = output / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    combinations = [(kind, size) for kind in KINDS for size in SIZES]
    memory_rows = []
    with (output / "run.log").open("w", buffering=1) as log:
        def execute(command):
            remaining = deadline - time.time()
            if remaining <= 0:
                raise TimeoutError("Experiment deadline reached")
            log.write(json.dumps(command) + "\n")
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=min(remaining, 180))
            log.write(result.stdout + result.stderr)
            result.check_returncode()
            metadata["completed_processes"] += 1
            metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
            return result

        def command(kind, size):
            return ["taskset", "-c", str(args.cpu), str(binary),
                    "--uv_kind=" + kind, "--uv_size=" + str(size)]

        try:
            for kind, size in combinations:
                result = execute(command(kind, size) + ["--uv_memory"])
                rows = list(csv.DictReader(io.StringIO(result.stdout)))
                assert len(rows) == 4
                memory_rows.extend(rows)
                write_csv(output / "memory.csv", memory_rows)
            for round_id in range(7):
                random.Random(20261109 + round_id).shuffle(combinations)
                for kind, size in combinations:
                    name = f"round-{round_id}-{kind}-{size}"
                    print(name, flush=True)
                    result = execute(command(kind, size) + flags + [
                        "--uv_reverse=" + str(bool(round_id % 2)).lower(),
                        "--bm_json_verbose=" + str(output / "raw" / (name + ".json"))])
                    (output / "quality" / (name + ".json")).write_text(json.dumps({
                        "unconverged": unconverged(result.stderr)}) + "\n")
                summarize(output)
            metadata["status"] = "complete"
        except (subprocess.TimeoutExpired, TimeoutError) as error:
            metadata["status"] = "deadline_or_process_timeout"
            metadata["error"] = str(error)
        except KeyboardInterrupt:
            metadata["status"] = "interrupted"
            raise
        except Exception as error:
            metadata["status"] = "failed"
            metadata["error"] = str(error)
            raise
        finally:
            metadata["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
            metadata["load_end"] = list(os.getloadavg())
            metadata["max_child_process_rss_kib"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
            metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
            summarize(output)


if __name__ == "__main__":
    self_check()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--phmap-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=8)
    parser.add_argument("--deadline", help="ISO 8601 timestamp, including timezone")
    parser.add_argument("--summarize-only", action="store_true")
    run(parser.parse_args())
