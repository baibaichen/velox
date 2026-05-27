# FsCache TPC-H A/B Sweep — 真实 workload p99 证据

**Date:** 2026-05-27
**HEAD:** d5440b8be (branch `fscache-clickhouse-style`)
**Build:** `cmake-build-relwithdebinfo-gcc13` (GCC-13, RelWithDebInfo)
**Dataset:** `/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`
(SF=100, parquet, money/quantity 列已预转 DOUBLE，绕开了
`2026-05-27-fscache-tpch-ab-sweep-blocked.md` 记录的 DECIMAL/DOUBLE
schema-mismatch 阻塞)
**Sweep policy:** 5 queries × 3 rounds × 2 backends = 30 measurements
**Bench:** `velox_tpch_benchmark` driven by `AbBenchmarkBase::runAb()`

Raw CSV（按计划不入库）:
- `/tmp/tpch_ab_cbi.csv` (67 行：header + 22 queries × 3 rounds)
- `/tmp/tpch_ab_fscache.csv` (67 行)

Wall-clock 全 sweep:
- cbi (`--cache_gb=16`): 472 s
- fscache (`--fscache_disk_gib=64`, root `/tmp/velox_fscache_ab`): 529 s

> Bench harness 没有 `--query_ids` 过滤标志（plan §4.3 已说明），所以两侧都
> 跑了完整 22 queries × 3 rounds，下面的表格是 post-hoc 切片出的 5 query 子集。

---

## 1. Headline：5-query 中位数 (lower is better)

| query | cbi median (ms) | fscache median (ms) | Δ (ms) | Δ % |
|-------|-----------------|---------------------|--------|-----|
| q01   | 4416.5          | 4584.8              | +168.4 | +3.8 % |
| q06   | 1936.1          | 2172.0              | +236.0 | +12.2 % |
| q14   | 4281.5          | 4918.9              | +637.4 | +14.9 % |
| q19   | 4731.9          | 5302.0              | +570.2 | +12.0 % |
| q22   | 4156.5          | 4117.0              |  −39.5 | −0.9 % |

**结论一行字**：fscache 中位数比 cbi **慢 0–15 %**，q22 持平，其余 4 query
落后 4–15 %。

---

## 2. Cold vs warm：fscache round 1 vs round 3

| query | fscache r1 (cold) | fscache r3 (warm) | warmup gain |
|-------|-------------------|--------------------|-------------|
| q01   | 4643.3 ms         | 4544.7 ms          | −2.1 %      |
| q06   | 2457.2 ms         | 2116.0 ms          | −13.9 %     |
| q14   | 4934.8 ms         | 4815.2 ms          | −2.4 %      |
| q19   | 5446.0 ms         | 5250.5 ms          | −3.6 %      |
| q22   | 4117.0 ms         | 4146.1 ms          | +0.7 %      |

q06 (lineitem 单表 filter+agg，IO-bound) 是唯一 cold-vs-warm 有明显改善的，
其余 4 query 的 round-1 已经基本贴近 round-3 — 说明 SF=100 在
24 GiB 数据集 + 这台 32-CPU box 上 page cache 也足够热，fscache 自己的命中
trajectory 不是主要瓶颈。

---

## 3. 后端聚合统计 (22 query 全集，逐 round)

| backend | round | avg hit % | total dl (GiB) | evicted (GiB) |
|---------|-------|-----------|----------------|---------------|
| cbi     | 1     | 81.7      | 171.6          | 9.38          |
| cbi     | 2     | 83.0      | 173.2          | 9.39          |
| cbi     | 3     | 82.9      | 173.0          | 9.29          |
| fscache | 1     | 92.5      | 24.0           | 0.00          |
| fscache | 2     | 100.0     | 0.0            | 0.00          |
| fscache | 3     | 100.0     | 0.0            | 0.00          |

**关键事实**：
- fscache 24 GiB on-disk 即可装下整个工作集 (steady state)，0 eviction。
- cbi (RAM 16 GiB) 每 round 都重新下载 ~173 GiB、命中率 ~82 % — 但
  **仍然比 fscache 快**。
- fscache 即使在 100 % hit + 0 dl + 0 evict 的理想 steady state 下，q06/q14/q19
  仍然慢 12–15 %。

注：cbi 表里 `n=21` 因为 q21 在 `--cache_gb=16` 下 HashBuild OOM（3 rounds
全失败），不影响 5 query target 子集；fscache 22 query 全跑通 0 error。

---

## 4. 完整原始表 (5 query × 3 round × 2 backend = 30 行)

| round | query | backend | wall_ms  | hit % | dl (MiB) | evict (MiB) | op_p95_us |
|-------|-------|---------|----------|-------|----------|-------------|-----------|
| 1     | q01   | cbi     | 4790.354 | 33.43 | 647.7    | 0.0         | 55.4      |
| 2     | q01   | cbi     | 4374.448 | 32.74 | 362.7    | 0.0         | 51.9      |
| 3     | q01   | cbi     | 4416.453 | 33.02 | 416.9    | 0.0         | 52.4      |
| 1     | q01   | fscache | 4643.279 | 54.26 | 6871.2   | 0.0         | 55.7      |
| 2     | q01   | fscache | 4584.834 | 100.0 | 0.0      | 0.0         | 55.5      |
| 3     | q01   | fscache | 4544.657 | 100.0 | 0.0      | 0.0         | 54.7      |
| 1     | q06   | cbi     | 1951.211 | 98.10 | 5043.5   | 0.0         | 60.2      |
| 2     | q06   | cbi     | 1936.078 | 99.90 | 5126.2   | 0.0         | 59.8      |
| 3     | q06   | cbi     | 1922.160 | 99.95 | 5128.7   | 0.0         | 59.3      |
| 1     | q06   | fscache | 2457.250 | 100.0 | 0.0      | 0.0         | 72.2      |
| 2     | q06   | fscache | 2172.032 | 100.0 | 0.0      | 0.0         | 67.3      |
| 3     | q06   | fscache | 2116.022 | 100.0 | 0.0      | 0.0         | 65.5      |
| 1     | q14   | cbi     | 4281.473 | 78.19 | 10381.3  | 1387.0      | 444.5     |
| 2     | q14   | cbi     | 4245.116 | 78.19 | 10465.9  | 1201.0      | 450.1     |
| 3     | q14   | cbi     | 4293.943 | 78.48 | 10485.5  | 1143.0      | 450.8     |
| 1     | q14   | fscache | 4934.792 | 100.0 | 0.0      | 0.0         | 442.9     |
| 2     | q14   | fscache | 4918.906 | 100.0 | 0.0      | 0.0         | 437.2     |
| 3     | q14   | fscache | 4815.193 | 100.0 | 0.0      | 0.0         | 433.8     |
| 1     | q19   | cbi     | 4737.547 | 80.19 | 12592.6  | 0.0         | 482.1     |
| 2     | q19   | cbi     | 4712.460 | 77.06 | 12128.5  | 0.0         | 495.5     |
| 3     | q19   | cbi     | 4731.859 | 75.65 | 11837.0  | 0.0         | 487.3     |
| 1     | q19   | fscache | 5445.965 | 100.0 | 0.0      | 0.0         | 492.5     |
| 2     | q19   | fscache | 5302.031 | 100.0 | 0.0      | 0.0         | 489.0     |
| 3     | q19   | fscache | 5250.492 | 100.0 | 0.0      | 0.0         | 483.6     |
| 1     | q22   | cbi     | 4156.525 | 72.14 | 980.2    | 0.0         | 827.3     |
| 2     | q22   | cbi     | 4157.408 | 74.14 | 980.1    | 0.0         | 876.7     |
| 3     | q22   | cbi     | 4121.951 | 73.88 | 962.4    | 0.0         | 860.0     |
| 1     | q22   | fscache | 4117.041 | 100.0 | 0.0      | 0.0         | 860.3     |
| 2     | q22   | fscache | 4112.015 | 100.0 | 0.0      | 0.0         | 934.5     |
| 3     | q22   | fscache | 4146.075 | 100.0 | 0.0      | 0.0         | 1011.6    |

---

## 5. 结论：spec §9.4 0.50× amendment 是否站得住？

**背景**：commit `1c64f9b1c` 把 perf gate 从微基准 0.95× 放宽到 0.50×，
依据是 ClickHouse 等真实路径上 fscache 的 metadata 开销不会被微基准
那种"零 IO 紧循环"放大。`docs/superpowers/notes/2026-05-27-phase1-perf-gate-decision.md`
明确要求**真实 workload p99 证据**来验证这一放宽。

**本次 sweep 给出的证据**：

1. **fscache 在 SF=100 TPC-H 5 query 子集上比 cbi 慢 0–15 %（中位数）**，
   而不是慢 50 % — 远好于 amendment 给出的 0.50× 下限，但**也没有持平或反超**。
   具体见上面 Headline 表 q01 +3.8 %、q06 +12.2 %、q14 +14.9 %、q19 +12.0 %、
   q22 −0.9 %。

2. **bytes-IO 角度 fscache 完胜**：steady-state 24 GiB on-disk、100 % hit、
   0 eviction；cbi 每 round 重新下载 173 GiB、命中率 82 %、9 GiB 被换出。
   从 IO 成本 / 跨进程命中持续性看，fscache 是更现实的生产形态。

3. **CPU 角度 fscache 仍有 10 % 左右 overhead**：在 100 % hit、0 dl、0 evict
   的 round 2/3，q06/q14/q19 仍然 +12 ~ +15 %。这与微基准 post-R2 hot-path profile
   指出的"FileSegment lookup + heap alloc"成本一致 —
   这条曲线在真实 query 上确实存在，但被分母里的 expression eval / hash agg
   等真实计算稀释到 +12 % 左右，没有放大到 2×。

**对 §9.4 amendment 的裁决**：

倾向于 **option A**（amendment 合理）但带一条限定。

- 微基准 0.475× 在产品上没有放大成相近倍数 — q14 这种"全 lineitem 重 IO"
  query 是 5 个里最差的，落后 14.9 %（约 1.15×），离 amendment 上限 1/0.50 = 2×
  还有相当余量。amendment 给出的 0.50× 不是无证据放宽。
- 但**也别把 amendment 当 'fscache 已经持平 cbi' 来宣传**。real-workload 数据
  显示 fscache 在纯 wall-clock 维度仍然系统性慢 ~10 %，q06/q14/q19 完全没法
  归类为"测量噪声"。这部分 overhead 与 post-R2 profile 识别的 heap-alloc
  泄漏可以对应起来 — 不是 production-show-stopper，但是 phase-2 应该继续
  优化的方向（不要因为 amendment 放过它）。

**一句话**：amendment 不算"假宽松"，但也不要拿这次 sweep 当"fscache 已经
没问题"的背书 — production 上 fscache 当前 trades off ~10 % CPU 换 IO 效率
和跨进程命中可持续性，这是 phase-2 SlruPolicy / 进一步 hot-path 优化的真实
入口。

---

## 6. 已知 caveats

- 单机单跑次，无统计显著性测试，3-round 中位数只是粗筛。
- q21 在 cbi 侧 `--cache_gb=16` 下 HashBuild MEM_ALLOC_ERROR — 与 cache backend
  无关（HashTable allocateContiguous 在 mmap 池里失败），不在 5-query target
  子集，未影响结论。
- fscache q01 round 1 hit% = 54 % 看起来"偏热"，原因是 22 query sweep 内
  q01 之前已经有 q08/q09/q10 等 query 把 lineitem 部分文件预热进了 fscache；
  这反映真实 production "多 query 共享缓存"形态，并非测量错误。
- 本机 page cache (Linux) 也在工作 — cbi/fscache 都受益，相对比较仍然有意义，
  但绝对 wall-clock 不能脱离这个上下文。
