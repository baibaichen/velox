#!/usr/bin/env python3
"""Mechanical identifier-consistency scan for fscache plan/spec.

Origin: round-6 introduced the third instance of the same bug class —
plan code snippets reference identifiers that other steps delete/rename
(N2 holder.empty(), O1 cache.totalSize(), Q2 counters_.bytesOnDisk).
User asked for a 5-minute mechanical defence so this class of bug stops
re-appearing every round.

Run: python3 docs/superpowers/notes/plan-identifier-scan.py
Exit 0 on clean, 1 on any flagged identifier (so a pre-commit hook can
key off it later).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PLAN = Path("docs/superpowers/plans/2026-05-26-fscache-ch-aligned-redesign.md")
SPEC = Path("docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md")

# Patterns that indicate the Shape α FsCacheStats contract was violated
# somewhere in plan/spec. Add new patterns when a future round-N finds a
# similar mismatch; each addition is a permanent guard.
PATTERNS: list[tuple[str, str]] = [
    (r"\bs\.snapshot\(\)", "snapshot() on FsCacheStats — Shape α removed"),
    (r"cache->stats\(\)\.snapshot\(\)", "cache->stats().snapshot() — Shape α removed"),
    (r"FsCacheStatsSnapshot", "FsCacheStatsSnapshot — Shape α removed"),
    (r"\.recordHit\(IsPrefetch", "instance.recordHit(IsPrefetch) — Shape α made free fn"),
    (r"\.recordMiss\(IsPrefetch", "instance.recordMiss(IsPrefetch) — Shape α made free fn"),
    (r"\.prefetchRatio\(\)", ".prefetchRatio() method — Shape α made free fn"),
    (r"counters_\.hits\b", "counters_.hits — Shape α renamed to prefetchHits/demandHits"),
    (r"counters_\.misses\b", "counters_.misses — Shape α renamed to prefetchMisses/demandMisses"),
    (r"\bstats_\.", "stats_ member — Shape α kept name counters_"),
    (r"FsCacheStats\.h", "FsCacheStats.h — Shape α did NOT create this header"),
    (r"\bsnapshot\.hits\b", "snapshot.hits — Shape α removed unsplit hits/misses"),
    (r"\bsnapshot\.misses\b", "snapshot.misses — Shape α removed unsplit hits/misses"),
    # Round-7 R6: POD snapshot fields treated as atomic — Shape α residue
    (
        r"\b(?:s|stats|snap|warmStats|finalStats|baseline\w*|snapshot)\.(?:prefetchHits|prefetchMisses|demandHits|demandMisses|evictions|bytesOnDisk)\.(?:load|fetch_add|fetch_sub|store)\b",
        "POD FsCacheStats field accessed as atomic — Shape β snapshot is plain uint64_t",
    ),
    # Round-7 R6: free-fn recordHit/recordMiss on FsCacheStats (spec §6.3
    # only declares prefetchHitRate / prefetchMissShare free fns).
    (
        r"\brecord(?:Hit|Miss)\s*\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*&\s*)?(?:s|stats|counters_)\b\s*,",
        "recordHit/recordMiss free-fn on FsCacheStats — spec §6.3 only declares prefetchHitRate / prefetchMissShare free fns",
    ),
    # Round-7 R6: stale "non-copyable / contains atomic" prose contradicting
    # §6.3 design note.
    (
        r"FsCacheStats is non-copyable|contains std::atomic members?",
        "prose says FsCacheStats contains atomic — contradicts §6.3 design note (POD snapshot)",
    ),
    # Round-7 R6: prefetchRatio name removed (split into prefetchHitRate +
    # prefetchMissShare per spec §6.3 to disambiguate the two metrics).
    (
        r"\bprefetchRatio\b",
        "prefetchRatio — renamed in Round-7 to prefetchHitRate (§9.2 perf gate) and prefetchMissShare (§3 quantitative target)",
    ),
]

# 2-arg recordHit/recordMiss callsites are valid INSIDE Task 8's getOrSet
# pre-Task-14 body, but must carry an inline annotation pointing forward
# to Task 14 step 5 so a future reader sees the upgrade plan.
ARITY_PATTERNS: list[tuple[str, str]] = [
    (r"recordHit\(seg\.get\(\)\);", "2-arg recordHit without Task 14 annotation"),
    (
        r"recordMiss\(seg\.get\(\),\s*seg->key\(\)\.size\);",
        "2-arg recordMiss without Task 14 annotation",
    ),
]

ARITY_TOLERANCE = "Task 14 step 5 extends"


def extract_cpp_blocks(text: str) -> list[tuple[int, int, str]]:
    """Returns list of (start_line, end_line, body) for each ```cpp block."""
    blocks: list[tuple[int, int, str]] = []
    cur_lang: str | None = None
    cur_start = 0
    cur_lines: list[str] = []
    for i, line in enumerate(text.splitlines(), 1):
        m = re.match(r"^```(\w*)\s*$", line)
        if m:
            if cur_lang is not None:
                if cur_lang == "cpp":
                    blocks.append((cur_start, i, "\n".join(cur_lines)))
                cur_lang = None
                cur_lines = []
            else:
                cur_lang = m.group(1) or "plain"
                cur_start = i
                cur_lines = []
        elif cur_lang is not None:
            cur_lines.append(line)
    return blocks


def scan(path: Path) -> list[tuple[int, str, str]]:
    text = path.read_text()
    problems: list[tuple[int, str, str]] = []
    cpp_blocks = extract_cpp_blocks(text)
    for start, _end, body in cpp_blocks:
        body_lines = body.splitlines()
        for pat, desc in PATTERNS:
            for m in re.finditer(pat, body):
                line_in_block = body[: m.start()].count("\n") + 1
                ctx = body_lines[line_in_block - 1].strip()
                problems.append((start + line_in_block, desc, ctx))
        for line_in_block, ln in enumerate(body_lines, 1):
            stripped = ln.strip()
            if ARITY_TOLERANCE in stripped:
                continue
            for pat, desc in ARITY_PATTERNS:
                if re.search(pat, stripped):
                    problems.append((start + line_in_block, desc, stripped))
    # NOTE: only cpp blocks are scanned. Prose paragraphs that describe
    # the upgrade itself ("flip counters_.hits.fetch_add to ...") legitimately
    # name the old identifier inside backticks — that is documentation, not
    # code that has to compile. Block scope keeps the signal high.
    return sorted(set(problems))


def main() -> int:
    rc = 0
    for path in (PLAN, SPEC):
        problems = scan(path)
        if not problems:
            print(f"{path}: clean")
            continue
        rc = 1
        print(f"{path}: {len(problems)} flagged identifier(s)")
        for ln, desc, ctx in problems:
            print(f"  L{ln:5d}  {desc}")
            print(f"          context: {ctx}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
