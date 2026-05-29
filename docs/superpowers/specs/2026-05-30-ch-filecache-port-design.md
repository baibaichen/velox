# ClickHouse FileCache → Velox 移植设计

**状态**: Draft 2（已按 review 修订）
**日期**: 2026-05-30
**分支**: `ch-filecache`（基于 `main`，工作区 `/home/chang/OpenSource/velox`）
**基准**: ClickHouse 源码 `/home/chang/SourceCode/ClickHouse/src/Interpreters/FileCache/`（唯一基准）
**CH 基准 commit**: `ac3f86486abcde9bd6bf66556d20d78a569897df`（2026-05-29）—— 本文所有 CH 行号引用以此 commit 为准。
**参考（仅参考，不复制）**: `/home/chang/OpenSource/velox2` 分支 `fscache-clickhouse-style` 的已有实现与文档；其 test/benchmark 代码可复用。

---

## 1. 目标

把 ClickHouse 的 `FileCache` 子系统**逐类移植**到 Velox，最终对接 Velox 的
`BufferedInput` 接口，使其成为 `CachedBufferedInput`（AsyncDataCache 后端）之外的
另一种文件缓存 BufferedInput 实现。

**核心要求**
1. **算法层面 exactly 模拟** CH 的 FileCache 语义（区间查找、6 态状态机、
   partial-readable、SLRU 淘汰、QueryLimit、bypass 阈值）。
2. **非必要不发明新数据结构**：保留 CH 的数据结构与控制流。
3. **基础设施采用 Velox 实现**：日志、断言、文件读写、容器等用 Velox/folly 等价物。

## 2. 统一约定（Ground Rules）

- **目录**: `velox/common/caching/filecache/`
- **命名空间**: `facebook::velox::ch`（保留 CH 原类名，用命名空间隔离避免与 Velox
  通用符号 `FileCache`/`Metadata`/`FileSegment` 冲突）。**决策**：明知 `ch` 把 CH 出处
  带进公共 API，仍刻意保留，以便逐行对照 CH 源码、明确标注移植来源；后续若要作为长期
  生产后端再评估改名为 `filecache`（此为已知可逆决定，不阻塞移植）。
- **类名**: 保留 CH 原名，便于逐行对照 CH 源码。
- **执行方式**: 一个类（一个文件）一个迁移单位，按依赖**自底向上**（从无依赖的
  叶子开始）。每个单位：忠实移植 → **每个单位都要带 UT（可写则必写）**；纯/叶子类
  (L0–L1) 必须有单测；状态机/Metadata (L2–L3) 必须至少有针对 mock 依赖的状态转换测试；
  完整集成测试与 benchmark 在功能闭合后统一补（参考 velox2）。
- **分支/提交策略**: 单分支 `ch-filecache`，**逐单位 commit**，每个 commit 自带该单位
  的 UT（便于二分定位 bug）；不把测试整体推迟到最后。
- **每单位编译门禁**：移植后必须能编译（目标库随依赖逐步可链接）。

### 2.1 基础设施替换表

| CH | Velox 替换 | 说明 |
|----|-----------|------|
| `chassert(x)` | `VELOX_DCHECK(x)` | **debug/sanitizer-only，release no-op 且不求值**（`velox/common/base/Exceptions.h:455-470`：`NDEBUG` 下 `VELOX_CHECK(true)`，否则 `VELOX_CHECK(expr)`）。**不是** `VELOX_CHECK`。 |
| `throw Exception(LOGICAL_ERROR,…)` | `VELOX_FAIL` / `VELOX_CHECK` | always-on 的逻辑错误。 |
| `LOG_*` / `Poco::Logger` | `LOG`/`VLOG`（`velox/common/base/...`） | |
| `ReadBufferFromFileBase`（远端读） | `velox::ReadFile`（`velox/common/file/File.h`） | 缓存后端数据源。 |
| `WriteBufferFromFile`（本地段写） | `velox::LocalWriteFile` | 本地段文件写。 |
| `ReadBufferFromFile`（本地段读） | `velox::LocalReadFile` | 本地段文件读。 |
| `UInt128`（`KeyHash`） | `__uint128_t` | 128 位 key。固定用 `__uint128_t`（现有 `FileCacheKey.h:30` 即此选择），不用 `velox::int128_t`。 |
| `sipHash128(path)` | folly `SpookyHashV2::Hash128`（`folly/hash/SpookyHashV2.h`） | **决策**：固定用 SpookyHashV2（环境无 folly SipHash）。`fromPath` 是 key 身份函数，更换哈希族会让不同进程/构建产生不同 key —— 故**明确声明：缓存目录仅进程内有效，跨构建/重启不可复用**（与 §8 不互通磁盘格式一致）。<br>**TODO(hash-stability)**：若将来需 key 跨运行稳定，忠实移植 CH 的 `SipHash128`。**代码侧**已在 `FileCacheKey.cpp::fromPath` 留同名 `// TODO(hash-stability)` 注释反向指回本决策。 |
| `UUIDHelpers::generateV4` | folly/boost UUID 或随机 128bit | `FileCacheKey::random()`。 |
| `getHexUIntLowercase` / `unhexUInt` | velox/folly hex 工具 | `toString`/`fromKeyString`。 |
| `WriteBufferToFileSegment` | **不移植** | CH WriteBuffer 适配器；其 reserve/write/complete 序列在填充路径中直接复现。 |

> 哈希已在上表固定为 SpookyHashV2；UUID 的具体实现可在 `FileCacheKey` 移植时最终敲定，不影响缓存算法正确性。

### 2.2 数据结构选型策略（三档）

1. **CH 用标准库** → 移植**照用 std**（`std::map`/`std::list`/`std::unordered_map`/
   `std::vector`/`std::mutex`/`std::condition_variable`/`std::shared_mutex` 等），
   **不**替换为 folly F14 / Velox 容器。保持容器语义与迭代器失效行为与 CH 完全一致。
2. **CH 用自定义数据结构/工具件**（CH 专有，非 std，如 `ThreadPool` /
   `ThreadFromGlobalPool` / `BackgroundSchedulePool` / `Stopwatch` / `SipHash` /
   `Exception` / `ProfileEvents` 等）→ 找 **Velox/folly 对应物**替换（如线程池→folly
   executor、计时→Velox time、异常→`VELOX_*`）。
   > **已知决策点（线程原语，落到具体单位时一行确认）**：CH 的 `ThreadPool`（即时任务）
   > → folly `CPUThreadPoolExecutor`/`IOThreadPoolExecutor`；CH 的 `BackgroundSchedulePool`
   > （延迟/周期任务，语义≠普通 executor）→ folly `FunctionScheduler` 或 `HHWheelTimer`，
   > **不可**简单换成 `IOThreadPoolExecutor`（会改变定时语义）。在 L3（Metadata 后台清理）/
   > L6（FileCache 淘汰 worker）需要时按此确认，避免错配。
3. **纯观测量（metric / ProfileEvents / CurrentMetrics）** → 移植时先删成 **no-op**
   并在每处留 `// TODO(metric): <原 CH 指标名>` 标记；**收尾阶段**把有价值的接到
   Velox `RECORD_METRIC_VALUE`（`StatsReporter.h`）。**算法状态计数器**（cache
   size/elements、段 downloaded_size/reserved_size、hits_count、ref_count、
   `CacheStateGuard` 计数等是数据结构字段，非 ProfileEvents）→ **原样移植**。
   > **判定边界（解决跨界计数器歧义）**：只要某计数器在**算法代码任何地方被读取**用于
   > 决策（如 `current_size`/`current_elements` 既是状态又被导出），即归**第 1 类（tier-1，
   > 照搬移植）**；**仅写给 metrics、算法从不读**的计数器才归第 3 类 no-op。

## 3. CH FileCache 类依赖分析

分层（自底向上）：

- **L0 叶子**: `FileCacheKey`、`FileCache_fwd*`、`FileCacheUtils`、`FileSegmentInfo`
  (6 态)、`FileSegmentKeyType`、`FileCacheOriginInfo`、`CacheUsage`、`Guards`、
  `FileCacheSettings`
- **L1**: `IFileCachePriority`（抽象接口 + 嵌套类型 Entry/Iterator/HoldSpace/…）
- **L2**: `FileSegment` + `FileSegmentsHolder`
- **L3**: `Metadata`（`CacheMetadata`/`MetadataBucket`/`KeyMetadata`/`LockedKey`/
  `FileSegmentMetadata`）
- **L4**: `LRU`/`SLRU`/`Split` `FileCachePriority`、`EvictionCandidates`
- **L5**: `FileCacheQueryLimit`
- **L6**: `FileCache`
- **L7**: `FileCacheFactory`、BufferedInput 集成

### 3.1 关键：四者循环依赖"结"

```
FileSegment ──holds IteratorPtr──▶ IFileCachePriority
     ▲                                    │ iterate(LockedKey&, FileSegmentMetadataPtr)
     │ owns                               ▼
FileSegmentMetadata ◀──────────── Metadata / KeyMetadata
     ▲
     └──── EvictionCandidates ───────────┘ (依赖以上全部)
```

CH 用**前向声明 + `FileCache_fwd_internal.h` + `weak_ptr<KeyMetadata>`** 打破 include 环。
移植时同样以前向声明 + fwd 头打破环。

### 3.2 锁序（`Guards.h`）

`CachePriorityGuard > CacheMetadataGuard > KeyGuard > FileSegmentGuard`，外加
`CacheStateGuard`（总 size/elements 计数）。**`CacheStateGuard` 的定位（消歧）**：它是
**短临界区的叶子锁**——只在更新总量计数那一小段持有，**不在持有上述四把锁中任何一把时
再去拿它**，也不在持有它时去拿其余四把（即与四把锁互不嵌套）。移植时严格保持同一锁序与
该约束。

## 4. 迁移顺序

分类列：**REWRITE**=无 Velox 等价、忠实移植；**REUSE**=已有 Velox/folly 等价直接用；
**SKIP**=不移植（裁剪或内联复现）。逐类明细见 §4.1。

| # | 单位 | CH 源 | 层 | 分类 |
|---|------|-------|----|----|
| 1 | FileCacheKey | FileCacheKey.{h,cpp} | L0 | **REUSE/已写**（worktree 已存在 `ch::FileCacheKey`） |
| 2 | fwd 头（破循环用） | FileCache_fwd*.h | L0 | SKIP-as-unit（无独立类型，按需建 fwd 头破 include 环） |
| 3 | FileCacheUtils | FileCacheUtils.h | L0 | REWRITE（`roundUp`→`bits::roundUp`；`roundDown` 手写） |
| 4 | FileSegmentInfo / KeyType | FileSegmentInfo.h, FileSegmentKeyType.{h,cpp} | L0 | REWRITE（枚举→string 手写，Velox 无 enum 反射） |
| 5 | OriginInfo / CacheUsage | FileCacheOriginInfo.h, CacheUsage.h | L0 | REWRITE |
| 6 | Guards | Guards.h | L0 | REWRITE（纯 std 锁包装，平凡） |
| 7 | FileCacheSettings | FileCacheSettings.{h,cpp} | L0 | REWRITE（仅 settings 结构）+ 解析方法 SKIP。**默认：先最小移植，step 18 需要再扩** |
| 8 | IFileCachePriority | IFileCachePriority.{h,cpp} | L1 | REWRITE |
| 9 | FileSegment + Holder | FileSegment.{h,cpp} | L2 | REWRITE（I/O→`ReadFile`/`Local{Read,Write}File`） |
| 10 | Metadata 全家 | Metadata.{h,cpp} | L3 | REWRITE（后台线程→folly executor+scheduler，见 §4.1） |
| 11 | EvictionCandidates | EvictionCandidates.{h,cpp} | L4 | REWRITE（`absl::flat_hash_map` 保留，环境可用） |
| 12 | LRUFileCachePriority | LRUFileCachePriority.{h,cpp} | L4 | REWRITE（保留 `std::list`+迭代器） |
| 13 | SLRUFileCachePriority | SLRUFileCachePriority.{h,cpp} | L4 | REWRITE |
| 14 | (可选) SplitFileCachePriority | SplitFileCachePriority.{h,cpp} | L4 | REWRITE |
| 15 | QueryLimit | QueryLimit.{h,cpp} | L5 | REWRITE（TLS→显式上下文，见 §5） |
| 16 | FileCache | FileCache.{h,cpp} | L6 | REWRITE |
| 17 | FileCacheFactory | FileCacheFactory.{h,cpp} | L7 | SKIP（全局注册表，Velox 改为直接/注入式持有）。**默认：先不移植，确有多缓存注册需求再加** |
| 18 | BufferedInput 集成（**net-new glue，非移植**，见 §6.1） | 新建 velox 侧文件 | L7 | NEW（独立子 spec） |
| 19 | 测试 + benchmark | — | 全部完成后 | — |

### 4.1 逐类清单与跨切面替换（并行核查结果）

**SKIP / 内联复现**
- `WriteBufferToFileSegment` → 不移植；其 reserve/write/complete 序列在 FileSegment 写路径内联复现。
- `FileCacheFactory` / `FileCacheData` → 全局单例注册表，默认不移植（见 step 17）。
- `FileCacheSettings::loadFromConfig/loadFromCollection/validate` → CH 专有配置管线
  （`Poco::Util::AbstractConfiguration`/`NamedCollection`/`ServerSettings`），裁剪。
- `FileCache_fwd.h` / `FileCache_fwd_internal.h` → 无独立类型，仅按需保留破循环的 fwd 头。

**跨切面基础设施（已核查环境可用性）**
- **后台线程**：CH `ThreadFromGlobalPool`/`BackgroundSchedulePool` → 即时任务用
  `folly::CPUThreadPoolExecutor`/`IOThreadPoolExecutor`；**延迟/周期**任务（metadata cleanup、
  download 重试）用 `folly::FunctionScheduler`（`folly/executors/FunctionScheduler.h`）。
  *已核查*：本仓未发现可用的 `velox::ThreadPool`（`velox/common/concurrency/ThreadPool.h` 不存在），
  故统一走 folly。
- **`absl::flat_hash_map`**（EvictionCandidates/EvictionInfo）→ **保留 absl**：环境已装
  `/usr/local/include/absl/container/flat_hash_map.h`，最忠实；不强行换 std/F14。
- **枚举→字符串**（FileSegmentKeyType 用 `magic_enum`）→ **手写** switch/映射：本仓 `common/base`
  无 enum 反射工具。
- **`roundUpToMultiple`** → `bits::roundUp`（`velox/common/base/BitUtil.h:127`）；
  **`roundDownToMultiple`** → 手写（无现成等价）。
- **std 容器**（`std::map`/`std::list`/`std::unordered_map`/`std::shared_mutex`/`std::condition_variable`）
  → 全部照搬 std，不换 folly/F14（保持迭代器失效与语义一致）。

## 5. 需裁剪 / 适配的 CH 专有依赖（遇到时处理）

- `FileCacheSettings` 的 CH 配置解析（`Poco::Util::AbstractConfiguration`、
  `ServerSettings`）→ 精简为普通 settings 结构 + 默认值。**默认：先最小移植，step 18 需要再扩。**
- `FileCacheFactory` 的 ClickHouse 全局注册表 → **默认不移植**；Velox 侧改为直接/注入式持有
  缓存实例，确有"按名/路径多缓存注册"需求再加。
- `FileCacheOriginInfo` 的 `user_id` 等 CH 概念 → 用 Velox 等价物或精简。
- `QueryLimit` 依赖的 CH `QueryContext`/`CurrentThread`（TLS 读取）→ Velox 侧以**显式
  上下文传递**替代：**形态在 step 15 落地时最终敲定**，候选是给 `getOrSet`/`get` 等入口
  增加一个 `QueryLimitContext*`（或 `const QueryLimitContext&`）参数并向下穿透，替代 CH
  在任意调用点读 `CurrentThread::getQueryContext()` 的 TLS 行为。此为已知的非平凡 API 改动，
  标记为"step 15 决策点"。
- ProfileEvents / 指标 → 见 §2.2 第 3 档（移植先 no-op + TODO，收尾接 StatsReporter）。

## 6. getOrSet 核心算法（已精读，移植锚点）

`FileCache::getOrSet`（`FileCache.cpp:800-966`）流程：boundary 对齐 →
`metadata.lockKeyMetadata(CREATE_EMPTY)` → `getImpl` 区间查找（`getImpl:461-557`，
lower_bound + 前一段相交判断）→ 处理 uncovered prefix/suffix（再 `getImpl` 探边）→
空则 `splitRange`(`559-604`)+`createFileSegmentsFromRanges`(`606-623`)；非空则
`fillHolesWithEmptyFileSegments`(`625-751`) → 返回 `FileSegmentsHolder`。
`get`（`968-1009`）：未命中返回 DETACHED 占位段，holes 用 detached 填充。

### 6.1 BufferedInput 集成（step 18，net-new glue，开放问题）

step 1–17 是对 CH 的**忠实移植**；step 18 是**全新的 Velox 胶水层**，不是移植，需独立设计。
**硬阻塞**：step 18 启动前，本 §6.1 必须先扩写为一份独立子 spec 并经 approved，方可动手编码
（不得在子 spec 缺位时直接实现）。已知开放问题：

- **范围翻译**：Velox `BufferedInput::read(offset, length)` / `enqueue` / `load` 如何映射到
  CH `getOrSet(key, offset, size, …)` 的对齐区间与 `FileSegmentsHolder`。
- **prefetch 交互**：Velox 的 enqueue/prefetch 语义与 CH 段级 download 的协调。
- **`SeekableInputStream` 语义**：holder 内多段如何拼成连续可 seek 的流。
- **cache-miss 回退**：未命中/写入失败时回退到底层 `velox::ReadFile` 的路径。
- **参照物**：现有 `CachedBufferedInput`（AsyncDataCache 后端）作为接口实现范例；选择点在
  `connector::hive::createBufferedInput`（`HiveConnectorUtil.cpp`），非插件注册表。

## 7. 验证策略

- **每个单位都必须带 UT**（可写则必写），随该单位 commit 一起提交：
  - L0–L1（叶子/纯类，如 Key/Utils/SegmentInfo/Settings/IPriority）→ 直接单测。
  - L2–L3（`FileSegment` 状态机 / `Metadata`）→ **至少**针对 mock 依赖的**状态转换测试**
    （6 态转换、download 生命周期、LockedKey 行为），不允许只"编译通过"。
  - L4–L6（淘汰/QueryLimit/FileCache）→ 针对该单位可独立验证的行为写测试；需要完整依赖
    闭合的端到端用例留到终局。
- **终局**：功能闭合后统一补**完整集成单测 + benchmark**（复用 velox2 的 test/bench 作
  参考），对照 CH 语义与 velox2 行为做回归。
- **回归定位**：逐单位 commit + 自带 UT，使任一单位引入的 bug 可通过 `git bisect` 定位，
  避免"bug 压在 15 个未验证单位之后"。

## 8. 非目标

- 不与 CH 的磁盘缓存格式互通（不需要读 CH 已有 cache 目录）。
- 不移植 CH 临时数据写缓存的 WriteBuffer 路径。
- 跨后端 microbenchmark（FsCache vs AsyncDataCache+SsdCache）、RAM 层 page cache、
  S3/HTTP 真实远端 backend —— 后续 spec。
