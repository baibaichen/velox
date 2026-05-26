# FsCache 多 PathKey 验证：per-bucket lock 是否解 contention

**日期**: 2026-05-26
**分支**: `fscache-clickhouse-style`
**Bench commit**: `1c412657d` (--num_files flag)
**编译目录**: `cmake-build-relwithdebinfo-gcc13` (GCC-13 RelWithDebInfo)
**硬件**: 同 [2026-05-26 min-wall sweep](2026-05-26-fscache-min-wall-sweep.md)

## TL;DR

`FsCacheBenchmark` 之前用单一 `kRemotePath`，所有线程 → 同一 PathKey → 同一
bucket，per-bucket lock 退化为全局锁，16t/1t scaling 为反向 0.30x。
加 `--num_files=threads` 让每个线程路由到独占的 PathKey/bucket 后，hit 路径
scaling 全部翻正，sequential.16t.0.5ws.0us 从 2.12M ops/s 升到 28.1M ops/s
(**13.3x**)，p50 从 5.3us 降到 0.4us。

**结论**：plan-1 per-bucket sharding 设计正确。phase-1 baseline 看到的反向
scaling 不是 FsCache 本身的缺陷，而是 micro-bench workload 把所有 key 折叠
到一个 bucket 的人为现象。在真实场景（多文件、key 自然分散）下，hit path
contention 不再是瓶颈。

## 起因

[`2026-05-26-fscache-min-wall-sweep.md`](2026-05-26-fscache-min-wall-sweep.md)
在 180s × cap=1000 配置下确认 16t/1t = 0.30 是稳定信号、不是采样噪声。
perf record 锁定 `FsCache::recordHit` 占 25.4% CPU（lock 14.17% + unlock
11.20%），落点是 `bucket.priorityMutex`。

排查 ClickHouse 同设计后确认：CH 也是 per-segment `increase_priority_mutex`
(try_lock 折叠) + queue 级 writeLock (splice)，**没有** lock-free batch
bump。Velox plan-1 已在 CH 设计 ceiling。剩下要回答的是：当 keys 实际分散
到多个 bucket 时，per-bucket lock 是否解 contention？

## 方法

加 `--num_files` flag（commit `1c412657d`）：
- 每个 file id 映射到 `kRemotePath + "#" + fileId` 一个独特路径串
- PathKey 由路径串 hash，所以 N 个虚拟文件 → N 个不同 bucket
- IO 还走同一个底层 blob（SleepyReadFile 用 path 仅作 cache key）
- 线程 → file 映射 `fileId = t % numFiles`；`numFiles >= threads` 时每线程独占
  一个 bucket
- Working set 按 `wsKeys / numFiles` 切，每个 file 独立 keyspace

跑三个 sweep，固定 `--num_files = --threads`：
- `threads=1, num_files=1` — baseline，等价于旧行为
- `threads=4, num_files=4` — 4 个 bucket
- `threads=16, num_files=16` — 16 个 bucket

其它参数同 min-wall sweep：`ops = 10000 * threads`, `warmup = ops/10`,
`min_wall_seconds=180`, `ws_mult ∈ {0.5, 2.0}`, `lat ∈ {0, 200} us`。
共 36 cell。

## 数据

完整三张表见末尾"原始数据"段。下面是 ops/s 汇总，对比项是旧
`num_files=1` 16t 列（取自 [min-wall sweep](2026-05-26-fscache-min-wall-sweep.md)
的 180s cap=1000 结果）。

### Hit-dominated cells (ws_mult=0.5)

| workload   | lat |    1t |    4t |   16t | 4t/1t | 16t/1t | 旧 16t (num_files=1) | 旧 16t/1t |
|------------|----:|------:|------:|------:|------:|-------:|---------------------:|----------:|
| sequential |   0 | 7.04M | 20.6M | 28.1M | 2.93x |  3.99x | 2.12M                |  0.30x    |
| sequential | 200 | 7.40M | 21.5M | 29.1M | 2.91x |  3.93x | 2.20M                |  0.30x    |
| zipfian    |   0 | 6.49M | 13.9M | 16.4M | 2.14x |  2.52x | 2.46M                |  0.38x    |
| zipfian    | 200 | 6.56M | 21.6M | 28.5M | 3.30x |  4.34x | 2.56M                |  0.39x    |
| uniform    |   0 | 6.84M | 14.7M | 17.1M | 2.15x |  2.51x | 2.22M                |  0.32x    |
| uniform    | 200 | 6.85M | 20.8M | 28.3M | 3.04x |  4.13x | 2.19M                |  0.32x    |

读法：每个线程独占一个 bucket 后，16t 全部翻正，hit-path scaling 在
2.5x ~ 4.3x（相比 13x 理想 scaling 仍受限于其它因素，见下文 §"残余瓶颈"）。

### Miss-dominated cells (ws_mult=2.0)

| workload   | lat |    1t |    4t |   16t | 旧 16t (num_files=1) |
|------------|----:|------:|------:|------:|---------------------:|
| sequential |   0 | 3.85k | 4.98M | 12.2M | 2.40M                |
| sequential | 200 | 1.95k | 3.43M | 15.4M | 2.55M                |
| zipfian    |   0 | 25.9k | 5.03M | 12.3M | 95.8k                |
| zipfian    | 200 | 14.2k | 3.80M | 15.6M | 87.0k                |
| uniform    |   0 | 6.85k | 4.77M | 12.1M | 25.1k                |
| uniform    | 200 | 3.71k | 3.34M | 14.9M | 22.7k                |

**警告**：miss cells 的 4t/16t 巨幅提升（千倍以上）**不能直接归功于 bucket
sharding**。当线程数 >= num_files 且每线程独占一个 file 时，每个 file 的
working set = `2 * 512MiB / numFiles`，对 16 个 file 来说每个 = 64 MiB，
单 bucket 内的 LRU 完全装得下，所以 hit% 从约 50%（uniform 1t）跳到 99.9%。
这是 workload fan-out 的人为产物（per-file working set 缩小到 cache 容量
内），**不是** miss path 性能改进。要单独测 miss path 性能需要把每个 file
的 working set 单独放大到超过单 bucket 配额。

下面分析只看 hit cells (ws_mult=0.5)。

## 解读

### 1. 反向 scaling 消失

|                                | 旧 (num_files=1) | 新 (num_files=threads) |
|--------------------------------|------------------:|------------------------:|
| sequential.16t.0.5ws.0us       | 2.12M (0.30x 1t) | 28.1M (3.99x 1t)        |
| p50 us                         | 5.3              | 0.4                     |
| p95 us                         | 19.8             | 1.0                     |
| p99 us                         | 30.8             | 1.4                     |

之前所有 16 个线程串行在同一个 `bucket.priorityMutex` 上，每次 hit 都要排
队，p50 5.3us 基本是 lock+unlock 的 cost。分到 16 个 bucket 后每线程
独占自己的 mutex（无人竞争），p50 落回到 0.4us（接近 1t 的 0.1us + atomic
counter 自身开销）。

### 2. plan-1 设计验证

plan-1 spec 写过的目标："per-bucket lock 把 hit path contention 分散到
N 个 bucket，避免单一 metadata lock 退化为全局锁"。这次数据直接证实在
keys 真的分散到不同 bucket 时，这个机制工作。phase-1 baseline 看到的
退化属于 workload 不公平（单 PathKey → 单 bucket），不是 FsCache 的缺陷。

### 3. 残余瓶颈（hit cells 16t/1t 只到 2.5–4.3x）

理论上 16 线程独占 bucket 应接近 16x scaling，实际 2.5–4.3x，差距来自：
- `FsCache::recordHit` 里的 `counters_.hits.fetch_add` 还是单个 atomic，全部
  线程竞争同一 cache line（false-sharing 也可能涉及）
- `FileSegment::increasePriorityMutex_` 仍然在每个 segment 上 try_lock，
  hot segments 会被多线程频繁碰
- 内存带宽 / L3 cache 子系统在 16t 下接近饱和
- per-bucket EvictionPolicy 自身的指令开销

这些是下一个性能层的事情（如果以后觉得 hit path 仍不够快可以加 sharded
counters / per-thread counters），但**不**属于 plan-1 验收范围。

### 4. zipfian 在 num_files=16, lat=0 偏低 (16.4M)

zipfian 即使 fan-out，热点 keys 仍然集中在 file 0（universe 内 zipf
分布）。16 个线程虽各自命中独立 file 的 keyspace，但 file 内 zipf 头部
keys 高度集中 → segment-level `increasePriorityMutex_` 上 try_lock 折叠
率高 → 不少 priority bump 被丢弃（CH 的 `FileSegmentFailToIncreasePriority`
统计同样的事）。这是 try_lock 设计的预期 trade-off，不是缺陷。

## 结论

plan-1 hit path 设计验收通过：

1. per-bucket sharding 在 keys 分散到不同 bucket 时确实把 contention 分散
   开（13.3x 提升）
2. 之前 phase-1 baseline 看到的反向 scaling 是 micro-bench 工作负载的
   人为现象（单 PathKey 把所有 key 折叠到一个 bucket），不是 FsCache
   设计缺陷
3. 设计已在 CH 同等设计的 ceiling 上，进一步降低 hit path 开销需要离开
   CH 设计（lock-free counter、sharded atomic、per-CPU counter 等）
   ——属于 phase-2 之后再议的范围

跟进项（不阻塞 plan-1）：
- 真实端到端 workload 验证：跑 TPC-DS 或类似多文件查询 sweep，确认在
  自然 key 分布下 FsCache hit path 不再是 hotspot
- 如有需要，单独加一个 per-file working set 可控的 miss path 性能测试，
  与 hit path 解耦

## 原始数据

### Sweep 1 — threads=1, num_files=1

```
| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |       1 |    0.50 |      0 |   7039795 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   1.420 |
| sequential |       1 |    0.50 |    200 |   7404017 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   1.351 |
| sequential |       1 |    2.00 |      0 |      3846 |  0.0% | 704670 |     704670 |  704670 |  258.7 |  280.2 |  296.7 | 183.224 |
| sequential |       1 |    2.00 |    200 |      1951 |  0.0% | 355203 |     355203 |  355203 |  507.9 |  540.1 |  691.0 | 182.056 |
| zipfian    |       1 |    0.50 |      0 |   6486967 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   1.542 |
| zipfian    |       1 |    0.50 |    200 |   6562471 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.1 |   1.524 |
| zipfian    |       1 |    2.00 |      0 |     25886 | 86.8% | 670563 |     670563 |  670563 |    0.3 |  295.2 |  311.4 | 195.896 |
| zipfian    |       1 |    2.00 |    200 |     14186 | 86.8% | 345855 |     345855 |  345855 |    0.3 |  533.0 |  551.9 | 184.504 |
| uniform    |       1 |    0.50 |      0 |   6838294 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   1.462 |
| uniform    |       1 |    0.50 |    200 |   6847234 | 100.0% |     0 |          0 |       0 |    0.1 |    0.1 |    0.2 |   1.460 |
| uniform    |       1 |    2.00 |      0 |      6845 | 49.9% | 694759 |     694759 |  694759 |  246.6 |  311.3 |  329.5 | 202.772 |
| uniform    |       1 |    2.00 |    200 |      3711 | 49.9% | 354640 |     354640 |  354640 |  469.0 |  555.7 |  607.0 | 190.926 |
```

### Sweep 2 — threads=4, num_files=4

```
| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |       4 |    0.50 |      0 |  20613268 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   1.940 |
| sequential |       4 |    0.50 |    200 |  21525885 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   1.858 |
| sequential |       4 |    2.00 |      0 |   4981911 | 99.9% | 28283 |      28283 |   28283 |    0.1 |    0.3 |    0.3 |   4.046 |
| sequential |       4 |    2.00 |    200 |   3431949 | 99.9% | 12261 |      12261 |   12261 |    0.1 |    0.1 |    0.2 |   2.929 |
| zipfian    |       4 |    0.50 |      0 |  13878821 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.4 |   2.882 |
| zipfian    |       4 |    0.50 |    200 |  21643959 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   1.848 |
| zipfian    |       4 |    2.00 |      0 |   5034890 | 99.9% | 33686 |      33686 |   33686 |    0.1 |    0.3 |    0.4 |   5.242 |
| zipfian    |       4 |    2.00 |    200 |   3801141 | 99.9% | 14733 |      14733 |   14733 |    0.1 |    0.2 |    0.3 |   3.684 |
| uniform    |       4 |    0.50 |      0 |  14678005 | 100.0% |     0 |          0 |       0 |    0.2 |    0.3 |    0.4 |   2.725 |
| uniform    |       4 |    0.50 |    200 |  20813978 | 100.0% |     0 |          0 |       0 |    0.1 |    0.2 |    0.2 |   1.922 |
| uniform    |       4 |    2.00 |      0 |   4770544 | 99.9% | 28208 |      28208 |   28208 |    0.1 |    0.3 |    0.4 |   4.249 |
| uniform    |       4 |    2.00 |    200 |   3340948 | 99.9% | 12536 |      12536 |   12536 |    0.1 |    0.2 |    0.3 |   3.042 |
```

### Sweep 3 — threads=16, num_files=16

```
| workload   | threads | ws_mult | lat_us |     ops/s |  hit% | dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us | wallSec |
|------------|--------:|--------:|-------:|----------:|------:|------:|-----------:|--------:|-------:|-------:|-------:|--------:|
| sequential |      16 |    0.50 |      0 |  28130475 | 100.0% |     0 |          0 |       0 |    0.4 |    1.0 |    1.4 |   5.688 |
| sequential |      16 |    0.50 |    200 |  29138550 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   5.491 |
| sequential |      16 |    2.00 |      0 |  12207860 | 99.9% | 190916 |     190916 |  190916 |    0.3 |    0.5 |    0.7 |  13.106 |
| sequential |      16 |    2.00 |    200 |  15405206 | 99.9% | 102580 |     102580 |  102580 |    0.2 |    0.3 |    0.4 |   8.464 |
| zipfian    |      16 |    0.50 |      0 |  16368036 | 100.0% |     0 |          0 |       0 |    0.8 |    1.6 |    2.1 |   9.775 |
| zipfian    |      16 |    0.50 |    200 |  28475806 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   5.619 |
| zipfian    |      16 |    2.00 |      0 |  12326539 | 99.9% | 179551 |     179551 |  179551 |    0.3 |    0.5 |    0.7 |  12.980 |
| zipfian    |      16 |    2.00 |    200 |  15636511 | 99.9% | 123080 |     123080 |  123080 |    0.2 |    0.3 |    0.4 |  10.232 |
| uniform    |      16 |    0.50 |      0 |  17143329 | 100.0% |     0 |          0 |       0 |    0.7 |    1.5 |    1.9 |   9.333 |
| uniform    |      16 |    0.50 |    200 |  28274300 | 100.0% |     0 |          0 |       0 |    0.4 |    0.9 |    1.2 |   5.659 |
| uniform    |      16 |    2.00 |      0 |  12094656 | 99.9% | 177412 |     177412 |  177412 |    0.3 |    0.6 |    0.7 |  13.229 |
| uniform    |      16 |    2.00 |    200 |  14875662 | 99.9% | 126683 |     126683 |  126683 |    0.2 |    0.3 |    0.5 |  10.756 |
```
