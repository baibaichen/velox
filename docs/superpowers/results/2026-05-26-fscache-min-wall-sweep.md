# FsCache Phase-2 Sweep — `--min_wall_seconds` 去噪 — 2026-05-26

承接 `2026-05-25-fscache-phase2-pathkey-fix.md`。后者每 cell 仅
`--ops=200000`，高吞吐 hit cell 实测 wall ≤ 0.1 s — 远低于稳定采样所需
窗口。这次在 `velox_fscache_benchmark` 引入 `--min_wall_seconds`：当某
cell 首轮 wall < 阈值时，按比例放大 ops 重跑一次（同 driver、保留
warmup-后状态），把 hit cell 拉到稳态再测。

代码：`velox/common/caching/fscache/benchmarks/FsCacheBenchmark.cpp` —
`runCell()` 末尾自适应分支；scale = `min(cap, minWallSeconds / wall)`。

---

## 1. 短样本（200 k ops）的噪声有多大

对比同二进制（commit `c63541c45`）三次 sweep：

- A：`--ops=200000`，无 `--min_wall_seconds`（即 2026-05-25 那次）
- B：`--ops=200000 --min_wall_seconds=3`，cap = 100×
- C：`--ops=200000 --min_wall_seconds=180`，cap = 100×

只看 ops/s（M ops/s），同 cell 三组并列：

| cell                              |   A   |   B   |   C   | C/A   |
|-----------------------------------|------:|------:|------:|------:|
| sequential.1t.0.5ws.0us           | 6.57  | 7.17  | 7.15  | +8.8% |
| sequential.1t.0.5ws.200us         | 6.12  | 7.19  | 7.23  | +18.1% |
| sequential.4t.0.5ws.0us           | 3.21  | 3.57  | 3.66  | +13.9% |
| sequential.16t.0.5ws.0us          | 2.23  | 2.31  | 2.56  | +14.9% |
| sequential.16t.2.0ws.0us          | 1.45  | 2.24  | 2.51  | +73.6% |
| zipfian.4t.0.5ws.0us              | 2.81  | 3.38  | 3.54  | +25.7% |
| zipfian.16t.2.0ws.0us             | 0.098 | 0.098 | 0.107 | +9.0% |

**结论：** 短样本（cell wall ≤ 0.1 s）相对 180 s 真值偏差最大达
**+73.6%**（cell sequential.16t.2.0ws.0us：1.45 → 2.51 M ops/s）。原 sweep
中所有 wall < 1 s 的 hit cell 的绝对数都不可单独引用。趋势可以看，比较
绝对值不行。

---

## 2. 180 s sweep 的反向缩放表

C 跑（180 s，cap = 100×；仅 1-thread 真到 180 s，4-/16-thread hit cell 因
cap 卡到 ~5-9 s）：

```
workload    lat       1t          4t         16t    4t/1t   16t/1t
─────────────────────────────────────────────────────────────────
sequential    0    7.15M     3.66M      2.56M     0.51     0.36
sequential  200    7.23M     3.57M      2.21M     0.49     0.31
zipfian       0    5.94M     3.54M      2.70M     0.60     0.46
zipfian     200    6.32M     3.53M      2.60M     0.56     0.41
uniform       0    6.57M     3.24M      2.25M     0.49     0.34
uniform     200    6.28M     3.27M      2.23M     0.52     0.36
```

反向缩放是真的，不是噪声：4 线程总吞吐只有 1 线程的 0.49-0.60 倍；16
线程降到 0.31-0.46 倍。所有 workload / latency 组合一致。

cap = 100 对 4-/16-thread hit cell 仍偏小（wall 5-9 s），后续追加 cap =
1000 重跑结果到 §3。

---

## 3. cap = 1000 重跑 — 待追加

`--min_wall_seconds=180`，cap 提到 1000×，预估 ~110 min。完成后追加完整
表与最终 4t/16t hit cell 真值；若 16-thread 仍 ±10% 浮动，再讨论是否进
一步加 ops 或换 perf 路径。

---

## 4. 数据文件

- `/tmp/phase2_min_wall_sweep.md` — B (3 s, cap 100)
- `/tmp/phase2_min_wall_180s.md` — C (180 s, cap 100)
- `/tmp/phase2_min_wall_180s_cap1000.md` — D (180 s, cap 1000) — 跑完追加
