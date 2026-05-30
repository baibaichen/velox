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
- **类名**: 保留 CH 原名（已是 PascalCase：`FileCache`/`FileSegment`/`Metadata`），便于逐行对照 CH 源码。
- **命名规范（遵循 `.claude/CLAUDE.md:145-147`，高于"贴近 CH"目标）**：
  - **方法/函数**：camelCase。CH 多数方法已是 camelCase（`getOrSet`/`lockKeyMetadata`），原样保留。
  - **成员变量**：camelCase（私有/受保护带尾下划线 `camelCase_`）。CH 的 snake_case 成员
    （`max_size`/`current_size`/`downloaded_size`）**必须改成** camelCase（`maxSize`/`currentSize`/`downloadedSize`）。
    这是与 CH 唯一刻意偏离的命名点——CLAUDE.md 明文优先；逐行对照时按"snake→camel"机械映射即可。
  - **命名空间/构建目标**：snake_case（`facebook::velox::ch`、`velox_ch_file_cache`）。
  - **宏**：UPPER_SNAKE_CASE。
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
| `ReadBufferFromFileBase`（远端读 / `FileSegment::remoteFileReader`） | `ByteInputStream`（抽象，`velox/common/memory/ByteStream.h:163`，`facebook::velox`）；具体用 `FileInputStream`（`velox/common/file/FileInputStream.h:28`，`facebook::velox::common`，包一个 `ReadFile` + bufferSize≈`max_read_buffer_size`） | **缓存后端数据源（流式读）**。CH 的 `ReadBufferFromFileBase` 是带内部缓冲的**流式** ReadBuffer，`downloadImpl`（`Metadata.cpp:861-935`）按 `set/seek/eof/available/position` + `file_segment.write(buf->position(),…)` 协议消费它——这套语义对应 Velox 的 `ByteInputStream::nextView/seekp/atEnd/remainingSize`，**不是**随机访问的 `velox::ReadFile`（pread 无 next/eof/position 游标）。`FileSegment::RemoteFileReaderPtr` 应从前向声明的 `ReadBufferFromFileBase` stub 改为 `std::shared_ptr<ByteInputStream>`；胶水层在 miss 时 `setRemoteFileReader(FileInputStream over 源 ReadFile)`。（`velox::ReadFile` 仍用于 bypass 回退的整段 pread，见 §6.1。） |
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

### 2.3 基础类映射表（FileCache 目录之外的 CH 通用依赖，全表，环境已核查）

按 §2.2 三档处理；"处置"列：**REUSE**=直接用 Velox/folly；**HAND**=手写小适配；
**NO-OP**=先空实现 + `// TODO(metric)`；**SKIP**=不需要。

| CH 基础类 / 头 | 用途 | Velox/folly 对应 | 处置 |
|---|---|---|---|
| `Common/logger_useful.h`（`LOG_*`） | 日志 | glog `LOG()`/`VLOG()`（仓内 `velox/common/caching/*.cpp` 已用） | REUSE |
| `Common/Exception.h` / `ErrnoException.h` | 异常 | `VELOX_FAIL`/`VELOX_CHECK*`/`VELOX_USER_FAIL`（`common/base/Exceptions.h`） | REUSE |
| `Common/ThreadPool.h` / `_fwd.h` | 线程池（即时任务） | `folly::CPUThreadPoolExecutor`/`IOThreadPoolExecutor` | REUSE |
| （CH `BackgroundSchedulePool`） | 延迟/周期任务 | `folly::FunctionScheduler`（`folly/executors/FunctionScheduler.h`） | REUSE |
| `Common/SharedMutex.h` | 读写锁 | **`std::shared_mutex`**（同目录 `velox/common/caching/SsdFile.{h,cpp}` 既有惯例；本目录无 `folly::SharedMutex` 使用） | REUSE |
| `Common/callOnce.h` | 一次性初始化 | `folly::call_once` / `std::call_once` | REUSE |
| `Common/CurrentThread.h` / `Interpreters/Context.h` | TLS query context | **沿用 Velox 非 TLS 设计取向，强制显式上下文传递**（QueryLimit，step 15）。注：Velox 有 `folly::ThreadLocal`（`velox/common/process/ThreadLocalRegistry.h`），但本移植刻意不用 TLS，改为显式下穿 `QueryLimitContext*`。 | HAND |
| `base/getThreadId.h` | 线程 id | `folly::getCurrentThreadID()` | REUSE |
| `Common/CurrentMetrics.h` / ProfileEvents / `ElapsedTimeProfileEventIncrement.h` | 观测指标 | 收尾接 `RECORD_METRIC_VALUE`（`StatsReporter.h`） | NO-OP |
| `Common/randomSeed.h` / `<random>` / `pcg_random.hpp` / `Core/UUID.h` | 随机/UUID | `folly::Random`（`folly/Random.h`） | REUSE |
| `Common/FailPoint.h` | 故障注入 | `velox/common/testutil/TestValue.h` | REUSE |
| `Common/assert_cast.h` | 下行转换断言 | `static_cast` + `VELOX_DCHECK`（手写小工具） | HAND |
| `base/scope_guard.h`（`SCOPE_EXIT`） | RAII 退出 | `folly::makeGuard` / `SCOPE_EXIT`（`folly/ScopeGuard.h`） | REUSE |
| `base/EnumReflection.h`（`magic_enum`） | 枚举↔串 | **本仓无反射 → 手写 switch/映射** | HAND |
| `base/hex.h`（`getHexUIntLowercase`/`unhexUInt`） | 十六进制 | 手写（`FileCacheKey` 已实现） | HAND |
| `boost/noncopyable.hpp` | 禁拷贝 | `= delete` 拷贝构造/赋值 | HAND |
| `Core/Types.h`（`UInt128`） | 128 位整型 | `__uint128_t` | REUSE |
| `Core/SettingsEnums.h` | settings 枚举 | 手写枚举 | HAND |
| `IO/ReadBufferFromFileBase.h`（远端流式读 / `remoteFileReader`） | 流式 ReadBuffer | `ByteInputStream` / `FileInputStream`（`common/memory/ByteStream.h`、`common/file/FileInputStream.h`） | REUSE（见 §2.1：流式 `set/seek/eof/available/position` → `nextView/seekp/atEnd`） |
| `IO/ReadBufferFromFile.h`（本地段读） | 本地缓存段文件读 | `velox::LocalReadFile`（`common/file/File.h`） | REUSE |
| `IO/WriteBufferFromFile.h` 等 | 文件写 | `velox::LocalWriteFile` | REUSE |
| `IO/ReadSettings.h` / WriteSettings | 读写参数 | 手写最小结构 | HAND |
| `IO/Operators.h` / `WriteBufferFromString.h` / `ReadHelpers.h` | 串格式化 | `fmt` / `folly` 串工具 | REUSE |
| `fmt/format.h` / `fmt/ranges.h` | 格式化 | `fmt`（仓内可用） | REUSE |
| `Poco/Util/AbstractConfiguration.h` / `NamedCollection` / `ServerSettings` | 配置解析 | —（FileCacheSettings 解析裁剪） | SKIP |
| `Storages/ColumnsDescription.h` / `DataTypeString.h` / `MutableColumnsAndConstraints.h` | system 表自省 | — | SKIP |
| `Disks/IO/CachedOnDiskWriteBufferFromFile.h` | CH 写缓存路径 | —（见 §8 非目标） | SKIP |
| `<memory>`/`<mutex>`/`<list>`/`<unordered_map>`/`<atomic>`/`<shared_mutex>`/… | std 容器/同步 | 照搬 std | REUSE |

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
**PORTED**=本目录已移植完成的代码（非 Velox/folly 等价物）；**SKIP**=不移植（裁剪或内联复现）。逐类明细见 §4.1。

| # | 单位 | CH 源 | 层 | 分类 |
|---|------|-------|----|----|
| 1 | FileCacheKey | FileCacheKey.{h,cpp} | L0 | **PORTED**（worktree 已写 `ch::FileCacheKey`，未提交，待 review） |
| 2 | fwd 头（破循环用） | FileCache_fwd*.h | L0 | SKIP-as-unit（无独立类型，按需建 fwd 头破 include 环） |
| 3 | FileCacheUtils | FileCacheUtils.h | L0 | REWRITE（`roundUp`→`bits::roundUp`；`roundDown` 手写） |
| 4 | FileSegmentInfo / KeyType | FileSegmentInfo.h, FileSegmentKeyType.{h,cpp} | L0 | REWRITE（枚举→string 手写，Velox 无 enum 反射） |
| 5 | OriginInfo / CacheUsage | FileCacheOriginInfo.h, CacheUsage.h | L0 | REWRITE |
| 6 | Guards | Guards.h | L0 | REWRITE（纯 std 锁包装，平凡） |
| 7 | FileCacheSettings | FileCacheSettings.{h,cpp} | L0 | 数据 struct + `Setting<T>`（默认值对齐 CH）+ `validate()` 忠实移植；解析/自省 **不实现**（`// TODO(config)`，见 §5.1） |
| 8 | IFileCachePriority | IFileCachePriority.{h,cpp} | L1 | REWRITE |
| 9 | FileSegment + Holder | FileSegment.{h,cpp} | L2 | REWRITE（本地段 I/O→`Local{Read,Write}File`；`remoteFileReader` 槽位类型→`std::shared_ptr<ByteInputStream>`，见 §2.1） |
| 10 | Metadata 全家 | Metadata.{h,cpp} | L3 | REWRITE（后台线程→folly executor+scheduler，见 §4.1） |
| 11 | EvictionCandidates | EvictionCandidates.{h,cpp} | L4 | REWRITE（`absl::flat_hash_map` → `folly::F14FastMap`，见 §4.1） |
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
- **`absl::flat_hash_map`**（EvictionCandidates/EvictionInfo）→ **改用 `folly::F14FastMap`**
  （`folly/container/F14Map.h`）。*已核查*：`absl` 不是 Velox 主库的直接依赖（仅作 Spark query
  runner / s2geometry / protobuf / re2 的间接依赖被解析），在本目录引 `absl/` 会给主库新增直接
  依赖（PR 阻塞点）；而 `folly::F14FastMap` 在主库已用 167 处。F14 同为开放寻址快表，语义最接近
  absl::flat_hash_map（注意：迭代器在 rehash 时失效，与 absl 一致，移植时无需改动调用逻辑）。
- **枚举→字符串**（FileSegmentKeyType 用 `magic_enum`）→ **手写** switch/映射：本仓 `common/base`
  无 enum 反射工具。
- **`roundUpToMultiple`** → `bits::roundUp`（`velox/common/base/BitUtil.h:127`）；
  **`roundDownToMultiple`** → 手写（无现成等价）。
- **std 容器**（`std::map`/`std::list`/`std::unordered_map`/`std::shared_mutex`/`std::condition_variable`）
  → 全部照搬 std，不换 folly/F14（保持迭代器失效与语义一致）。

## 5. 需裁剪 / 适配的 CH 专有依赖（遇到时处理）

- `FileCacheSettings` → 见 §5.1 专项方案。
- `FileCacheFactory` 的 ClickHouse 全局注册表 → **默认不移植**；Velox 侧改为直接/注入式持有
  缓存实例，确有"按名/路径多缓存注册"需求再加。
- `FileCacheOriginInfo` 的 `user_id` 等 CH 概念 → 用 Velox 等价物或精简。
- `QueryLimit` 依赖的 CH `QueryContext`/`CurrentThread`（TLS 读取）→ Velox 侧以**显式
  上下文传递**替代：**形态在 step 15 落地时最终敲定**，候选是给 `getOrSet`/`get` 等入口
  增加一个 `QueryLimitContext*`（或 `const QueryLimitContext&`）参数并向下穿透，替代 CH
  在任意调用点读 `CurrentThread::getQueryContext()` 的 TLS 行为。此为已知的非平凡 API 改动，
  标记为"step 15 决策点"。
- ProfileEvents / 指标 → 见 §2.2 第 3 档（移植先 no-op + TODO，收尾接 StatsReporter）。

### 5.1 FileCacheSettings 迁移方案（unit #7，定稿）

CH 现状拆 4 块及处置：

1. **数据（28 具名设置 + 默认值）** → 全保留。FileCache 算法读了其中 ~25 个，几乎都相关。
   **默认值逐一对齐 CH 缺省配置**：每个 `Setting<T>` 成员初始化器的默认值 = CH
   `LIST_OF_FILE_CACHE_SETTINGS`（`FileCacheSettings.cpp:35-64`）里该字段的字面量 / 默认常量，
   不得自拟。默认常量（`FILECACHE_DEFAULT_*`、`FILECACHE_BYPASS_THRESHOLD`）与 `FileCachePolicy`
   枚举来自 `FileCache_fwd.h`（unit #2，1:1 移植），故 #7 依赖 #2。例如
   `max_elements=FILECACHE_DEFAULT_MAX_ELEMENTS(10000000)`、
   `max_file_segment_size=FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE(32Mi)`、
   `boundary_alignment=FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT(4Mi)`、
   `cache_policy=SLRU`、`slru_size_ratio=0.6`、`background_download_threads=5`、
   `cache_on_write_operations=false`、`bypass_cache_threshold=256Mi`、
   `path=""`、`max_size=0` 等，全部照搬。
2. **机制（`BaseSettings` 宏 / pimpl `FileCacheSettingsImpl` / 下标算子 / `.changed`）** →
   换成**普通 struct**，每字段用轻量包装 `Setting<T>` 复刻 `{value, changed}`：

   ```cpp
   template <typename T>
   struct Setting {
     T value{};
     bool changed{false};
     Setting() = default;
     Setting(T v) : value(std::move(v)) {}        // 成员初始化器给默认值，changed=false
     Setting& operator=(T v) { value = std::move(v); changed = true; return *this; }
     operator const T&() const { return value; } // 隐式取值
   };
   struct FileCacheSettings {
     Setting<std::string> path;
     Setting<uint64_t>    maxSize{0};
     // …共 28 个字段，camelCase（CLAUDE.md），默认值逐一对齐 LIST_OF_FILE_CACHE_SETTINGS …
     void validate();
   };
   ```
   **字段命名**：CH `LIST_OF_FILE_CACHE_SETTINGS` 是 snake_case（`max_size`/`max_file_segment_size`
   /`max_size_ratio_to_total_space`），本移植按 §2 命名规范统一改 camelCase
   （`maxSize`/`maxFileSegmentSize`/`maxSizeRatioToTotalSpace`），与 CH 唯一刻意偏离点，机械映射。
   **读取迁移规则**：CH `settings[FileCacheSetting::max_size].value/.changed` → `settings.maxSize.value/.changed`
   （`[枚举]`→`.camelCase字段`；`.value`/`.changed`/隐式转换语义不变）。
   默认值经成员初始化器 → `changed=false`；显式赋值经 `operator=` → `changed=true`，忠实复刻。
3. **解析（`loadFromConfig`/Poco、`loadFromCollection`/NamedCollection）** → **不实现**。
   Velox 侧由 step 18 集成时从 Velox 配置/`HiveConfig` **程序化构造**（直接给字段赋值）。
4. **自省（`getColumnsDescription`/`dumpToSystemSettingsColumns`/`ColumnsDescription`）** → **不实现**
   （CH system 表专用）。

**`validate()` 保留并忠实移植**（算法相关 fail-fast）：`path` 必填且须为绝对路径、`maxSize`
与 `maxSizeRatioToTotalSpace` 互斥且至少一个、`maxSize!=0`、`overcommitEvictionEvictStep!=0`、
`boundaryAlignment ≤ maxFileSegmentSize`、ratio 分支用 `statvfs`+`std::filesystem` 由占比算出
`maxSize`（保留该计算，日志改 glog `LOG(INFO)`，异常改 `VELOX_USER_FAIL`/`VELOX_CHECK`）。
`.changed` 判断由上面的 `Setting<T>` 提供。对照 `FileCacheSettings.cpp:224-272`。
> **TODO(config)**：长期看配置校验宜迁到 Velox 自身配置方案；现阶段先忠实移植 validate() 保证语义。
> 即 unit #7 落地范围 = 28 字段数据持有（默认值对齐 CH）+ `Setting<T>` 包装 + `validate()` 忠实移植；
> 解析/自省/系统表相关方法**均不实现**，以 `// TODO(config)` 指回 CH 来源，后续接 Velox 配置方案。

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
