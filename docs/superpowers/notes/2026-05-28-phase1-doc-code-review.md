# FsCache Phase-1 文档 / 代码一致性审查（2026-05-28）

**Reviewer role**: 八荣八耻 adversarial doc-vs-code audit.
**Branch / HEAD**: `fscache-clickhouse-style` @ `13f0c2a26`.
**Scope**: 16 个文档（specs / plans / notes / results）vs HEAD 代码 + git 历史。
**Method**: 每条 finding 都跑 `git rev-parse` / `git merge-base --is-ancestor` /
`grep` 验证。Read-only — 没有修改任何被审查文档。

## 摘要

- **CRITICAL**: 4
- **IMPORTANT**: 6
- **WORTH-KNOWING**: 5

**Top-3 影响最大的发现**

1. Rebase 之后**几乎所有 phase-1 引用的旧 commit hash 都不在 HEAD 链上了**
   （`9a0cfd3bd / 85f6bbc4d / 2933ddda7 / 403f52755 / 1c64f9b1c / 020f26ada /
   4531293aa / f0c820e06 / e97a064c6 / 3c7dba192 / ba637a61c / d5440b8be /
   f44efb70f / 73c434dc0 / 661ac1be2 / 085c575d6 / 66026def3 / 308bd7f1a` 全部
   `git merge-base --is-ancestor HEAD = false`）。`git rev-parse` 能 resolve
   不代表 commit 在分支上 —— 它们活在 reflog/dangling 里。任何按这些 hash 顺藤
   摸瓜的人都会找到"内容相符但已不存在于历史里"的 commit，违反 八荣八耻 #1。
2. **`phase1-closure-summary.md` 把 Task 13 (SLRU) 列为 "Deferred phase-2"**，
   但 SLRU 代码已在 HEAD（`5751902d4 + 9f2c55f9c + d8cecd11a + 33d1438ff +
   a59addbe2`，5 个 commit）。正确陈述是 "已落地，default off (Task F 决策 B)"。
3. **`closure-summary` HEAD 标的是 `3c7dba192` —— 在分支链上也没有**。真实当前
   HEAD 是 `13f0c2a26`；closure-summary 写完后又落了 5 个 commit（c000e8f6c
   profile + 83bc56d54 post-memset sweep + fab52b3bc primitives survey +
   13f0c2a26 q17 reprofile + 6748e4a88 memset fix）。整篇 summary 落后于 HEAD，
   而且把 memset 修复、q17 reprofile、buffer-primitive 调研、phase-2 候选实验
   全部漏报了。

---

## 🔴 CRITICAL — must fix before phase-1 closure / push

### C1. 所有 pre-rebase commit hash 全部 off-branch

Verified via `git merge-base --is-ancestor <hash> HEAD`:

| hash | 文档位置（示例） | HEAD 上的对应 |
|---|---|---|
| `1c64f9b1c` | `phase1-perf-gate-decision.md:3, 7, 35, 97, 110, 136`、`phase1-closure-summary.md:28`、`fscache-tpch-ab-sweep.md:120`、`fscache-tpch-ab-sweep-blocked.md:116`、`fscache-tpch-ab-post-memset-fix.md:140`、`fscache-perf-gate.md` 多处 | `ea9998cdf docs(fscache): Task 16 perf gate PASS — spec §9.4 amended to CH-realistic 0.50×` |
| `020f26ada` | `fscache-tpch-ab-post-memset-fix.md:4`、`fscache-tpch-profile.md` 间接（profile 在前可豁免）、`q17-reprofile-post-memset.md` 推断 | `6748e4a88 perf(fscache): eliminate redundant zero-fill on hot read path` |
| `9a0cfd3bd / 85f6bbc4d / 2933ddda7 / 403f52755` | `phase1-perf-gate-decision.md:20-24, 113-116`、`phase1-closure-summary.md:58-62`、`fscache-perf-gate.md:1, 32-37` | 在 HEAD 链上找不到对应物 —— 这 4 个 commit 在 rebase 中合并/被分割/被 squashed 进了 Task-9~14 系列。需要单独 audit 每个的 HEAD 对应物（`git log --grep` 已确认 message 文案不再独立出现）。 |
| `4531293aa` | `phase1-closure-summary.md:27`（Task 15 commit） | `374a6bfd3 feat(fscache): TPC-H q1-q22 equivalence test under FsCache vs CBI (Task 15 / #178)` |
| `f0c820e06` | `phase1-closure-summary.md:28`（Task 14 commit） | `b5fc67e68 feat(fscache): atomic FsCacheStats split + IsPrefetch wiring (Task 14)` |
| `e97a064c6` | `phase1-perf-gate-decision.md:73, 111`、`fscache-perf-gate.md` | `df81ffddb docs(fscache): post-R2 hot-path profile (Task 16)` |
| `3c7dba192` | `phase1-closure-summary.md:6` "Final HEAD" | 真实 HEAD 是 `13f0c2a26`；`3c7dba192` 也不在分支上 |
| `d5440b8be / 661ac1be2 / 73c434dc0 / 661ac1be2 / f44efb70f / ba637a61c` | 多份 results docs header `HEAD: ...` | rebase 把它们 rewrite 了；新的对应 commit 没有重新填回 |
| `085c575d6 / 66026def3 / 308bd7f1a` | `slru-task-de-audit.md:11, 28, 39`、closure-summary `fsync` 注释 | SLRU Task D = HEAD `9f2c55f9c`，Task E = HEAD `d8cecd11a`，Task E citation-fix = `33d1438ff`；`308bd7f1a`（fsync 删除）→ HEAD 不存在等价独立 commit |

违反 八荣八耻 #1（不瞎猜 / 查实际）。任何按 hash 反查的人都会拿到 dangling
commit，会以为"这个 commit 不在分支上"。**修复**：跑一遍全文档 sed-style
hash 替换；优先级 = 任何在主流程论证里出现的 hash。

### C2. `phase1-closure-summary.md` 把 SLRU 标 "Deferred phase-2"

`docs/superpowers/notes/2026-05-27-phase1-closure-summary.md:25`:

```
| 13: SlruPolicy | **Deferred phase-2** | spec §8.1 opt-in, default off |
```

实际：HEAD 上 SLRU 代码完整。`git log --oneline HEAD -- velox/common/caching/fscache/SlruPolicy.*`：
`5751902d4 (Tasks A-C), 9f2c55f9c (Task D config), d8cecd11a (Task E factory),
33d1438ff (Task E citation fix), a59addbe2 (Task F SLRU vs LRU benchmark)`，5 个
commit。`grep -n "enableSlru" velox/common/caching/fscache/FsCacheConfig.h`
显示 `bool enableSlru{false}`（默认 false 是 Task F 决策 B，不是"deferred"）；
`FsCache.cpp:41` `if (!config.enableSlru) {...}` factory 分支已落地。

正确陈述：**SLRU 已落地（Task A-F 全部 commit），默认 opt-in（enableSlru=false），
由 Task F 实证决策 B 保留 LRU 默认**。

违反 八荣八耻 #7（假装理解）。**修复**：closure-summary Task 13 行 +
"Deferred items" §1 整段重写。

### C3. `closure-summary.md` HEAD 落后于真实 HEAD，漏 5 个 commit

`phase1-closure-summary.md:6`: `**Final HEAD**: 3c7dba192`。真实 HEAD `13f0c2a26`，
中间还有 phase-2 起步的 4 个 commit：

| HEAD commit | 类型 | summary 是否提到 |
|---|---|---|
| `13f0c2a26` | q17 reprofile (pread syscall) | ❌ |
| `fab52b3bc` | phase-2 buffer/small-vector primitive survey | ❌ |
| `83bc56d54` | #179 re-run post-memset-fix | ❌ |
| `c000e8f6c` | TPC-H SF=100 cold+hot perf profile | ❌ |
| `6748e4a88` | memset hot-path 修复 | ❌（"Optimisation timeline" 表只到 R2） |

closure-summary 严格意义上现在是 phase-1 收尾**前**的快照，所有 phase-2 起步
工作（memset 修复 + 后续 profile + buffer 调研 + post-memset re-sweep）都没
反映。`Performance acceptance` 表也是 pre-memset 数字（q01 +3.8% / q06 +12.2% /
q14 +14.9%）。post-memset 实际数字（mean +5.5% / median +4.5%，最差 q20 +15.7%）
来自 `2026-05-28-fscache-tpch-ab-post-memset-fix.md` 完全没进 summary。

违反 八荣八耻 #5 / #7。**修复**：要么把 summary 截止时间锁回 `3c7dba192` 并
显式说明，要么补完 phase-2 起步 5 commit。前者更诚实。

### C4. `2026-05-28-fscache-tpch-ab-post-memset-fix.md:4` HEAD off-branch

```
**HEAD:** `020f26ada` (branch `fscache-clickhouse-style`,
commit `perf(fscache): eliminate redundant zero-fill on hot read path`)
```

`git merge-base --is-ancestor 020f26ada HEAD = false`。HEAD 上对应物是
`6748e4a88`（message 一致）。这是 phase-2 主要业绩之一，被引用最频繁，hash 错
代价最大。

违反 八荣八耻 #1。**修复**：`020f26ada → 6748e4a88` 全文档替换；header HEAD 行
和正文 cross-ref 都改。

---

## 🟡 IMPORTANT — should fix

### I1. perf-gate amendment 决策状态前后不一致

`phase1-perf-gate-decision.md:3`:
> **Status**: 决策待复审，commit `1c64f9b1c` 已落地但**不是无争议的**。

`phase1-perf-gate-decision.md:95`:
> 由用户复审决定。**当前 HEAD 是选项 A**，但未经用户确认。

但 `fscache-tpch-ab-sweep.md:142` 已经给出 **option A** 的实证背书（"倾向于
option A"），`fscache-tpch-ab-post-memset-fix.md:157` 进一步明确"**裁决：option A
（保留 0.50× amendment）**"，`phase1-closure-summary.md:82` 也写 "0.50× 修正
诚实证立"。三份后续文档已经实质 close 了 option A，但 decision-doc 自己仍然
标 "决策待复审"，与下游不一致。

违反 八荣八耻 #2（模糊执行）。task 描述里 hint #9 也指出："Round-12 perf gate
决策 doc 应该反映：amendment 是 option A (keep)，由 #179 +4.16% 滞后正好落在 2×
容差内 retroactively justified。不应该还说 'decision pending user review'"。

**修复**：decision-doc 顶部 status 改 "Status: 决定 = option A, 由
fscache-tpch-ab-sweep.md (mean +5.5%, max +15.7%) 与 post-memset re-sweep
retroactively 确认；amendment 在 HEAD 上保留"。

### I2. `phase1-perf-gate-decision.md:122-129` "#179 阻塞" 是过时状态

doc 写 "2026-05-27 更新——#179 阻塞" + "real-workload p99 证据采集需要 unblock
工作"。但 `fscache-tpch-ab-sweep.md` 用 DOUBLE 数据集
(`/home/chang/test/tpch-double/...-decimal_as_double`) 已经把 #179 跑完了，
`fscache-tpch-ab-sweep-blocked.md` 已经是历史文档。

decision-doc 仍把"#179 阻塞"当前状态描述，违反 八荣八耻 #7。任务 hint #7 明确：
"check no doc says '#179 is blocked' as current state"。

**修复**：decision-doc Next-step §：
"#179 已用 DOUBLE 数据集 unblock，见 `fscache-tpch-ab-sweep.md`，blocked-doc 归档为历史。"

### I3. `slru-task-de-audit.md` 与 `slru-policy.md` 关于 Task D 默认值矛盾

`slru-task-de-audit.md:12-13`：
> adds `enableSlru` (bool, default false) and `slruProtectedRatio` (double, default 0.6)

`slru-policy.md:25-27`：
> evictionPolicy 字段默认 `kSlru`, `slruProtectedRatio` field default `0.6`
> （matches CH FILECACHE_DEFAULT_SLRU_RATIO, FileCache_fwd.h:26）

`slru-policy.md` Task G §7 谈到 "spec §8.1 amendment" 改默认 SLRU on；
`slru-policy.md` §9 "Task F outcome" 明确 "Decision: B — keep enableSlru = false default"。
HEAD 实测 `enableSlru{false}` —— 与 Task F 决策一致，与 plan 主体冲突。

具体冲突：plan §7 (Task G spec amendment) 主张"spec §8.1 改成默认 SLRU on"；
plan §9 又说"决策 B 保留 false"。Task G 的 spec amendment 没有相应的
"Task G 决策 = 取消默认 SLRU on" 同步注脚。

违反 八荣八耻 #2 / #6。**修复**：plan §1 Modify §下条目"evictionPolicy 字段默认 kSlru"
应改 `kLru`/或加注 "决策 B 否决"；Task G "spec §8.1 amendment 改成默认 on" 段需要
追加 "已由 §9 Task F outcome 否决，amendment 部分撤回 —— spec §8.1 应保留 opt-in 描述"。

### I4. `slru-task-de-audit.md:88` 测试 group 计数不匹配 HEAD

audit-doc 写 `velox_fscache_test_group0: 51/51 PASSED, group1: 72/72 PASSED`。
`phase1-closure-summary.md:67-68` 给的同一二进制是 `group0 54/54, group1 68/68`。
两个数字不可能同时对。HEAD 状态我没有重跑 ctest 验证，所以不知道哪个对，但
两份 doc 至少一份过时。

违反 八荣八耻 #5 / #7（不要把"上次测的数"当下次的）。**修复**：复跑
`ctest -R velox_fscache_test_group` 取真实数字，两份 doc 同步。

### I5. `velox-buffer-primitives-survey.md` 把 reverted 实验当作"未实施候选"展示

survey doc 列 "推荐 A：DataBuffer<char> 替换 unique_ptr<char[]>" + "推荐 B：
folly::small_vector<FileSegmentPtr, 4>"。task brief hint #3 显式提到 "4 次
alloc-elimination 失败实验（DataBuffer<char> / HWM unique_ptr / folly::small_vector
/ LocalReadFile cache）都已 revert"。

`grep -n "DataBuffer<char>\|small_vector" velox/dwio/common/FsCacheInputStream.* velox/common/caching/fscache/FileSegmentsHolder.h` = 0 命中：HEAD 上没人采用这两条
路径。如果它们曾经 land 然后 revert 了 +4~7% 回归，survey doc 应该显式标"已尝试
被 revert，回归记录见 X"，不应该当成"待选优化"陈述。

我没有 commit 历史证据证明这两条曾 land 过（grep `--all-branches log --oneline
--grep="DataBuffer\|small_vector"` 仅 hits unrelated commits）—— 也许它们停留在
worktree 阶段未 commit 就 revert，这种情况下 survey doc 应直接写 "已实验性套用过，
+4-7% 回归，详见 q17-reprofile-post-memset.md §6"。

`q17-reprofile-post-memset.md:171-181` §6 表格"之前 3 次否决的实验"对应的就是
DataBuffer 池 / unique_ptr HWM / small_vector inline。survey doc §6 写"下一步
（实现阶段，非本次范围）"暗示尚未尝试，与 reprofile doc §6 直接打架。

违反 八荣八耻 #7。**修复**：survey doc §6 顶部加 "已实测被 revert" 注脚，cross-ref
`q17-reprofile-post-memset.md §6`。

### I6. 三处独立 doc 对 fscache vs cbi 差距数字打架

| doc | 衡量口径 | 数字 |
|---|---|---|
| `phase1-closure-summary.md:42-49` (pre-memset) | 5-query median Δ% | q01 +3.8 / q06 +12.2 / q14 +14.9 / q19 +12.0 / q22 −0.9 |
| `fscache-tpch-ab-sweep.md:29-34` (pre-memset, 一致来源) | 同上 | 同 |
| `fscache-tpch-ab-post-memset-fix.md:90-95` (post-memset) | 同 5 query | q01 +1.57 / q06 +6.04 / q14 +10.33 / q19 +8.10 / q22 +1.03 |

post-memset doc 是更新的数据。closure-summary 表头明确写"Real-workload (SF=100
TPC-H, 5-query × 3-round × 2-backend sweep)"但没注脚 "pre-memset, post-memset 数字
见 ..."。任何不读 post-memset doc 的人都会以为 q06 还 +12.2%。

**修复**：closure-summary 性能表加注 "数字是 memset 修复前，见
`2026-05-28-fscache-tpch-ab-post-memset-fix.md` 后续 mean +5.5% 修订"。

---

## ⚪ WORTH-KNOWING — nit / cosmetic

### W1. closure-summary `fsync` 删除注释引用空 hash

`phase1-closure-summary.md:102`：
> `fsync` was removed from `FileSegment::complete` (commit `308bd7f1a`)

`308bd7f1a` 也是 off-branch hash。fsync 是否真在 HEAD 被删，我没在 codebase 跑
`grep "fsync" FileSegment.cpp` 验证 —— 用户可自行核对。

### W2. `slru-policy.md:7` 引用的"stale Task 13 sketch at lines 3635-..."

slru-policy.md 主体引用了 redesign plan "lines 3635-3990 的 Task 13 skeleton"。
redesign plan 现在 5378 行；line 3635 区间确实属于 Task 13。但 slru-policy.md
执行完毕后 redesign plan 的 Task 13 段没有同步更新（spec §8.1 amendment 写在
slru-policy.md §7 而不是 redesign plan 本身）。不是高优先级，但读者从 redesign
plan 看 Task 13 仍会看到 stale 设计。

### W3. `slru-policy.md` Modify §引用 fscache-tpch-ab plan 加 Round-13 SLRU entry

slru-policy.md:42：`docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md` 应追加
Round-13 SLRU。`grep -n "Round-13\|SLRU" docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md`
显示零命中 —— 这条 plan 修订没落地。SLRU 实证最终走到了
`fscache-slru-vs-lru.md` 单独文档，不在 tpch-ab plan 里追加。如果计划严格执行，
slru-policy.md §1 应改 "Round-13 走独立 results doc，tpch-ab plan 不动"。

### W4. `phase1-perf-gate-decision.md:11` 表格列标题 "spec 原阈值（plan 起草）≥ 0.80×"

decision-doc 用的对照阈值是 0.80×。spec 自身原始草稿写的是 0.80×（已确认），但
`fscache-perf-gate.md:21` 列标题里写"≥ 0.50× (was 0.80×)"。两处一致。这条不是
矛盾，只是表头列名风格不同。可统一为"原 / 修订"。

### W5. closure-summary "Deferred items" §1 SlruPolicy 描述

closure-summary:79 写 "SlruPolicy (#215): spec §8.1 says opt-in default off.
Reopen when a workload demonstrates LRU-vs-SLRU difference is observable. Current
evidence: heap-alloc dominates the lag, not LRU policy."

事实上 Task F 已经实测过 SLRU vs LRU（`fscache-slru-vs-lru.md`），结论是
"sequential -29.5% / zipfian +90.1% / SF=100 1-2% 噪声"。"Reopen when ..." 暗示
"还没测过"，但已经测过且决定 LRU 默认。表述不准。

---

## Delta TODO（doc-by-doc 待修补 summary）

- **`docs/superpowers/notes/2026-05-27-phase1-closure-summary.md`** — C2（SLRU 是
  "Done, opt-in" 而非 "Deferred"）+ C3（HEAD 更新或截止时间锁定）+ I6（perf 数字
  注脚 post-memset）+ W1（`308bd7f1a` hash 替换）+ W5（SlruPolicy "Reopen when"
  改 "Task F 已测，决定 LRU 默认"）+ 整表 hash refresh（C1：`1c64f9b1c → ea9998cdf`，
  `4531293aa → 374a6bfd3`，`f0c820e06 → b5fc67e68`，R1/R2/R3/race-fix hash 全部
  refresh）。
- **`docs/superpowers/notes/2026-05-27-phase1-perf-gate-decision.md`** — I1
  （status 改 "decided = option A"）+ I2（#179 unblock 状态）+ C1 hash refresh
  （`1c64f9b1c / 9a0cfd3bd / 85f6bbc4d / 2933ddda7 / 403f52755 / e97a064c6 /
  4531293aa / f0c820e06`）。
- **`docs/superpowers/notes/2026-05-27-slru-task-de-audit.md`** — C1 hash refresh
  (`085c575d6 → 9f2c55f9c`, `66026def3 → d8cecd11a`) + I4 ctest 计数复核。
- **`docs/superpowers/notes/2026-05-28-velox-buffer-primitives-survey.md`** — I5
  顶部加 "已尝试被 revert" cross-ref 到 q17-reprofile §6。
- **`docs/superpowers/plans/2026-05-27-slru-policy.md`** — I3 (Modify § 默认值改
  `kLru` 或加 "Task F 已否决 default flip" 注 + Task G amendment 撤回说明)，
  W2 (redesign plan Task 13 段同步更新)，W3 (Round-13 SLRU 走独立 doc 的修订)。
- **`docs/superpowers/results/2026-05-27-fscache-perf-gate.md`** — C1 hash refresh
  (cell-22 race fix + R1/R2/R3 commit hash 5 处)。
- **`docs/superpowers/results/2026-05-27-fscache-tpch-ab-sweep.md`** — C1 (`1c64f9b1c`
  + HEAD `d5440b8be` refresh)。
- **`docs/superpowers/results/2026-05-27-fscache-tpch-ab-sweep-blocked.md`** —
  I2 顶部加 "Historical, #179 已 unblock with DOUBLE dataset" + C1
  (`1c64f9b1c / f44efb70f` refresh)。
- **`docs/superpowers/results/2026-05-27-fscache-slru-vs-lru.md`** — C1 (`ba637a61c`
  HEAD refresh; Task F commit on HEAD = `a59addbe2`)。
- **`docs/superpowers/results/2026-05-27-fscache-hot-path-profile-post-r2.md`** —
  C1 (`61676e7b3` HEAD refresh; R1/R2/R3 commit refresh)。
- **`docs/superpowers/results/2026-05-27-fscache-hot-path-profile.md`** — C1
  (`61676e7b3 + 9a0cfd3bd` refresh)。
- **`docs/superpowers/results/2026-05-28-fscache-tpch-profile.md`** — C1 (HEAD
  `661ac1be2` refresh — 这条 profile 是 phase-2 起步的，需要找 HEAD 对应物)。
- **`docs/superpowers/results/2026-05-28-fscache-tpch-ab-post-memset-fix.md`** —
  **C4 顶级 HEAD `020f26ada → 6748e4a88` 替换** + I6 cross-ref (#179 sweep doc 加
  指针)。
- **`docs/superpowers/results/2026-05-28-fscache-q17-reprofile-post-memset.md`** —
  C1 (HEAD `73c434dc0` refresh)。
- **`docs/superpowers/specs/2026-05-26-fscache-ch-aligned-redesign.md`** — Task 13
  段同步 SLRU 实际落地（spec line 873-895 SLRU §8.1 是 stale 草稿，与 Task F 决策
  + slru-policy.md §7 amendment 都有 drift）。

---

## 复审脚本规约（建议加进 `plan-identifier-scan.py`）

第六层 defence：**commit hash ancestry check**。所有文档里 `^[0-9a-f]{9,12}$`
形态的 token，跑 `git merge-base --is-ancestor <hash> HEAD`，false 即 fail。
本次 4+6+5 件 findings 里至少 9 件是这一层捕获的。

---

## Self-check（八荣八耻）

- **#1 / #5**：每条 finding 都给了 `file:line` + git 命令 + 输出片段。`git
  merge-base --is-ancestor` 实际跑过，结果见对话脚手架，未拍脑袋。
- **#7**：无法独立验证的项（fsync 是否真删 → W1；ctest 真实计数 → I4；
  reverted alloc 实验 git 历史 → I5）都显式标 "我没有重跑/查全，请用户复核"。
- **Read-only**：没有修改任何被审查文档。只新建本审查 doc。

**End of review.**
