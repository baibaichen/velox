# #179 TPC-H A/B Sweep — Blocked (DECIMAL vs DOUBLE Schema Mismatch)

> **Historical — #179 unblocked via DOUBLE dataset.** This document is kept
> for context only. The sweep was successfully completed using
> `/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`;
> see `docs/superpowers/results/2026-05-27-fscache-tpch-ab-sweep.md` and the
> post-memset re-run `docs/superpowers/results/2026-05-28-fscache-tpch-ab-post-memset-fix.md`.

**Status:** BLOCKED — not solvable in-session (historical).
**Branch:** `fscache-clickhouse-style` @ `125975649`.
**Plan:** [`docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md`](../plans/2026-05-26-fscache-tpch-ab.md).
**Decision context:** [`docs/superpowers/notes/2026-05-27-phase1-perf-gate-decision.md`](../notes/2026-05-27-phase1-perf-gate-decision.md).

## 1. 尝试内容

按 plan §Task 4 执行 SF-100 TPC-H A/B sweep（5-query subset × 3 rounds × {cbi, fscache}）。
CBI 侧命令（plan 第 557-565 行）：

```
cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark \
  --data_path=/home/chang/test/tpch/tpch-generated-100.0-parquet \
  --data_format=parquet \
  --input_source=cbi \
  --cache_gb=16 \
  --rounds=3 \
  --num_repeats=1 \
  --out=/tmp/tpch_ab_cbi.csv
```

数据集：`/home/chang/test/tpch/tpch-generated-100.0-parquet`（SF-100，parquet，8 张表，lineitem 切成 ~101 个 part file；总大小 34 GB，lineitem 占 22 GB；由 Spark 4.1.1 生成，schema-metadata 中 `org.apache.spark.version` 字段确认）。
预期产物：`/tmp/tpch_ab_cbi.csv`（67 行 = 1 header + 22 queries × 3 rounds），随后 fscache 侧同样产物，最终 q01/q06/q14/q19/q22 五条 query 的 wall_ms / hit% 对比表。

## 2. 阻塞点：q1 plan 构造抛 VeloxUserError

`/tmp/tpch_ab_cbi.log` 实测错误（原文逐字）：

```
terminate called after throwing an instance of 'facebook::velox::VeloxUserError'
  what():  Exception: VeloxUserError
Reason: Scalar function signature is not supported: minus(DOUBLE, DECIMAL(12, 2)).
Supported signatures: ... (double,double) -> double, (real,real) -> real, ...
(decimal(i1,i5),decimal(i2,i6)) -> decimal(i3,i7), ...
```

stack 顶层（同 log）：

```
TpchQueryBuilder::getQ1Plan
  velox/exec/tests/utils/TpchQueryBuilder.cpp:230
TpchQueryBuilder::getQueryPlan(i)
  velox/exec/tests/utils/TpchQueryBuilder.cpp:158
TpchBenchmark::buildPlan(i)
  velox/benchmarks/tpch/TpchBenchmark.h:40
AbBenchmarkBase::runAb
  velox/benchmarks/AbBenchmarkBase.cpp:201
```

### 2.1 数据集 schema（pyarrow 实测）

`/home/chang/test/tpch/tpch-generated-100.0-parquet/lineitem/part-00000-...-c000.snappy.parquet`：

```
l_orderkey:       int64
l_partkey:        int64
l_suppkey:        int64
l_linenumber:     int32
l_quantity:       decimal128(12, 2)
l_extendedprice:  decimal128(12, 2)
l_discount:       decimal128(12, 2)
l_tax:            decimal128(12, 2)
l_returnflag:     string
l_linestatus:     string
l_commitdate:     date32[day]
l_receiptdate:    date32[day]
l_shipinstruct:   string
l_shipmode:       string
l_comment:        string
l_shipdate:       date32[day]
```

`l_extendedprice / l_discount / l_tax / l_quantity` 全部是 `DECIMAL(12,2)`——这是 TPC-H spec 规定的类型（Clause 1.3.1 / Table column datatype: Decimal(12,2)）。

### 2.2 q1 plan 构造点

`velox/exec/tests/utils/TpchQueryBuilder.cpp:230-237`（实际行号，已 `Read` 过）：

```cpp
.project(
    {"l_returnflag",
     "l_linestatus",
     "l_quantity",
     "l_extendedprice",
     "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
     "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
     "l_discount"})
```

`1.0` 解析为 DOUBLE，`l_discount` 是 DECIMAL(12,2) → 寻找 `minus(DOUBLE, DECIMAL(12,2))` signature → Velox PrestoSQL 注册表无此 signature（只有 `(double,double)`、`(decimal,decimal)` 两支同质 signature）。

### 2.3 引爆点：runAb 一次性构造全部 22 个 plan

`velox/benchmarks/AbBenchmarkBase.cpp:201`（plan §1.2 的设计：plan 在 round 循环外预构）：第一个 plan（q1）就抛，整个 sweep 在进入任何一次实际执行前 abort，因此没有任何 CSV 产物，没有任何 wall_ms / hit% 数据。

## 3. 两条 unblock 路径——为何都不在 #179 范围

### 路径 A：重新生成 DOUBLE schema 的数据集

- 数据工作，不是 fscache 工作：和 cache 行为完全无关。
- write-amp 实测 ~34 GB（`du -sh /home/chang/test/tpch/tpch-generated-100.0-parquet/` 显示 34 G；改 schema 重写一遍意味着至少一份 34 GB 的写盘）。
- 路径偏离 TPC-H spec（spec 指定 Decimal(12,2)，DOUBLE 版本不是合规 TPC-H 数据）。

### 路径 B：在 TpchQueryBuilder 给 DECIMAL 列加 cast-to-DOUBLE projection

- 改的是 `velox/exec/tests/utils/TpchQueryBuilder.cpp`——production-adjacent code（被 TPC-H 集成测试、benchmark、equivalence test 共用，不只是 #179 自己用），不是 #179 的 scope。
- 正确性验证负担：q1 是 `sum(l_extendedprice * (1 - l_discount))`。把两个 DECIMAL(12,2) 转 DOUBLE 再相乘再 sum，和原本 DECIMAL 精确算再 sum，浮点 rounding 路径完全不同。需要重新生成 reference output（用 DuckDB 或 Presto 在同一数据集上跑 q1-q22）做 bit-level / tolerance 对比，否则后续所有用这个 builder 的等价性测试都会变成"和自己比"——失去外部参考价值。
- 设计决策需用户确认（八荣八耻 #3）：DECIMAL→DOUBLE coercion 是改 builder（影响所有调用者）还是只改 benchmark fork（增加代码重复），是单独的 design 题。

## 4. 对 spec §9.4 amendment 决策的影响

cross-ref [`2026-05-27-phase1-perf-gate-decision.md`](../notes/2026-05-27-phase1-perf-gate-decision.md)：

- §9.4 t=16 efficiency gate 从 0.80× 改到 0.50×（commit `ea9998cdf`）的论证基于 "real-workload p99 evidence will drive the final answer"——decision-doc 第 122-125 行明确把"跑 #179 TPC-H A/B sweep on SF-100"列为决策依据。
- #179 现在阻塞在数据 schema，证据无法采集。
- 因此 amendment 既不能被 #179 **PASS-确认**（"真实 workload 没退化"），也不能被 #179 **FAIL-推翻**（"真实 workload 确实退化"）。
- 决策状态：**pending real-workload evidence**——和 decision-doc 第 95 行 "由用户复审决定，当前 HEAD 是选项 A，但未经用户确认" 完全一致。Amendment 不变更地留在 HEAD，**显式标注 awaiting real-workload validation**。

## 5. 本 session 实际已 green 的 gate（不为 #179 阻塞 改变这部分结论）

| Gate | 来源 | 结果 |
|---|---|---|
| TPC-H q1-q22 SF=0.01 等价性 | Task 15 `FsCacheTpchEquivalenceTest`，commit `374a6bfd3` | **22/22 PASS** |
| 全套 UT（caching / dwio / exec / fscache 层） | Tasks 1-14 每 task 自 5-phase 验证 | 全绿（每个 task commit 自带 UT 通过记录） |
| Microbench `sequential / ws_mult=0.5 / lat=0 / num_files=16` | Round-11+ 优化路径，decision-doc 表 1 | t=1 hot 8.47-8.64 M ops/s（≥7.0 M PASS），t=16 efficiency **0.475-0.509×**（amended ≥0.50× gate PASS / 原 ≥0.80× MISS -38%） |

Microbench 数字是 decision-doc 已记录的实测；**未在本 session 重跑**。

## 6. 决断

- #179 标 **deferred**（task #218 的 perf gate 已 deferred；#179 同形 option B-deferred 处理）。
- spec §9.4 amendment 留在 HEAD，**显式标注 "awaiting real workload validation"**（同时更新 decision-doc Next-step §）。
- Phase-1 acceptance 基于：spec §9.4 amended gates（t=1 / t=16 amended PASS）+ Task 15 SF=0.01 22/22 + 所有 UT 层绿。
- Phase-2 启动时自然复审：要么先做数据（生 DOUBLE schema），要么先做 TpchQueryBuilder DECIMAL coercion（带 reference-output 验证），再回头跑 #179 sweep。
