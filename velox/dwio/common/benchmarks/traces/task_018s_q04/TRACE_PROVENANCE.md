# Task 018S q04 `BufferedInput` trace provenance

The full event trace is intentionally not tracked by git:

```text
external artifact:
  /root/oss/velox/tmp/tpch_buffered_input_trace_q04_watchdog_1/capture_trace/events.jsonl
size:
  2041347 bytes
```

The file exceeds the repository's 1 MiB artifact limit. `manifest.json` and
`SHA256SUMS` pin its identity without committing the large JSONL file.

## Capture identity

```text
query: q04
drivers: 1
files: 240
events: 10320
dataset:
  /root/oss/test-data/tpch-sf100-parquet-double
capture binary build ID:
  7e6d90db6ea6ac99fe023bcb7fd287779eae4ad5
Velox HEAD:
  0c5b5918eb8374f39b09248be091da94bf4d72f0
Gluten HEAD:
  c44409a7c3d8fab17ac5369b7cad8b3c80f5a437
ClickHouse HEAD:
  dedc90e3ee1bc34cdb619252e4937240704ffaee
```

## What the trace records

This is a logical `BufferedInput` trace, not a physical `pread` trace. Each
JSONL row has a monotonically increasing global `seq` and carries the relevant
`file_id`, `input_id`, `stream_id`, offset, length, lifecycle operation,
capture TID, and stable capture thread index.

The trace records input and stream lifecycle plus logical operations such as
create, clone, reset, preload, stream read, load, consume, skip, seek, EOF, and
close. `manifest.json` maps each `file_id` to a dataset-relative Parquet path
and exact file size.

## Exact calculations

```text
event_count = number of JSONL rows
            = 10320

file_count = length(manifest.files)
           = 240

logical_bytes = sum(event.length for event where event.op == "consume")
              = 5625132188
```

Only `consume.length` contributes to `logical_bytes`. `stream_read`, enqueue,
load, preload, seek, skip, EOF, and close do not contribute. If the workload
consumes the same source range more than once, each consumption is counted
because it is logical work performed by the reader.

Recompute with:

```bash
wc -l \
  /root/oss/velox/tmp/tpch_buffered_input_trace_q04_watchdog_1/capture_trace/events.jsonl
jq '.files | length' manifest.json
jq -s 'map(select(.op=="consume") | .length) | add' \
  /root/oss/velox/tmp/tpch_buffered_input_trace_q04_watchdog_1/capture_trace/events.jsonl
sha256sum manifest.json \
  /root/oss/velox/tmp/tpch_buffered_input_trace_q04_watchdog_1/capture_trace/events.jsonl
sha256sum -c SHA256SUMS
```

## Replay contract and limitations

Replay processes events in global `seq` order and verifies consumed bytes
against the same source files. The current deterministic replay uses one
thread. It preserves the logical access sequence and byte contents, but does
not reproduce capture-thread concurrency, scheduling gaps, physical I/O
overlap, or the original `pread` timing.

FileCache work generated while replaying the trace is not part of
`logical_bytes`. For example, synchronous foreground predownload may read and
cache gap bytes that have no `consume` event. Consequently FileCache source
reads and writes can exceed `5625132188` even though every backend returns the
same logical bytes.
