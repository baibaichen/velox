# ClickHouse FileCache → Velox 移植设计

**状态**: Draft 1
**日期**: 2026-05-30
**分支**: `ch-filecache`（基于 `main`，工作区 `/home/chang/OpenSource/velox`）
**基准**: ClickHouse 源码 `/home/chang/SourceCode/ClickHouse/src/Interpreters/FileCache/`（唯一基准）
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
  通用符号 `FileCache`/`Metadata`/`FileSegment` 冲突）
- **类名**: 保留 CH 原名，便于逐行对照 CH 源码。
- **执行方式**: 一个类（一个文件）一个迁移单位，按依赖**自底向上**（从无依赖的
  叶子开始）。每个单位：忠实移植 → **能写 UT 就写、不能就保证编译通过** →
  全部功能完成后再统一补测试 + benchmark（参考 velox2）。
- **每单位编译门禁**：移植后必须能编译（目标库随依赖逐步可链接）。

### 2.1 基础设施替换表

| CH | Velox 替换 | 说明 |
|----|-----------|------|
| `chassert(x)` | `VELOX_DCHECK(x)` | **debug/sanitizer-only，release no-op 且不求值**（`base/base/defines.h:22-52`）。**不是** `VELOX_CHECK`。 |
| `throw Exception(LOGICAL_ERROR,…)` | `VELOX_FAIL` / `VELOX_CHECK` | always-on 的逻辑错误。 |
| `LOG_*` / `Poco::Logger` | `LOG`/`VLOG`（`velox/common/base/...`） | |
| `ReadBufferFromFileBase`（远端读） | `velox::ReadFile`（`velox/common/file/File.h`） | 缓存后端数据源。 |
| `WriteBufferFromFile`（本地段写） | `velox::LocalWriteFile` | 本地段文件写。 |
| `ReadBufferFromFile`（本地段读） | `velox::LocalReadFile` | 本地段文件读。 |
| `UInt128`（`KeyHash`） | `__uint128_t` / `velox::int128_t` | 128 位 key。 |
| `sipHash128(path)` | folly 128-bit 哈希（`SpookyHashV2::Hash128`）或 XXH3-128 | 仅影响 key 身份，不影响算法；不需与 CH 磁盘格式互通。 |
| `UUIDHelpers::generateV4` | folly/boost UUID 或随机 128bit | `FileCacheKey::random()`。 |
| `getHexUIntLowercase` / `unhexUInt` | velox/folly hex 工具 | `toString`/`fromKeyString`。 |
| `WriteBufferToFileSegment` | **不移植** | CH WriteBuffer 适配器；其 reserve/write/complete 序列在填充路径中直接复现。 |

> 替换的具体哈希/UUID 实现可在对应文件移植时最终敲定，不影响缓存算法正确性。

### 2.2 数据结构选型策略（三档）

1. **CH 用标准库** → 移植**照用 std**（`std::map`/`std::list`/`std::unordered_map`/
   `std::vector`/`std::mutex`/`std::condition_variable`/`std::shared_mutex` 等），
   **不**替换为 folly F14 / Velox 容器。保持容器语义与迭代器失效行为与 CH 完全一致。
2. **CH 用自定义数据结构/工具件**（CH 专有，非 std，如 `ThreadPool` /
   `ThreadFromGlobalPool` / `BackgroundSchedulePool` / `Stopwatch` / `SipHash` /
   `Exception` / `ProfileEvents` 等）→ 找 **Velox/folly 对应物**替换（如线程池→folly
   executor、计时→Velox time、异常→`VELOX_*`）。
3. **纯观测量（metric / ProfileEvents / CurrentMetrics）** → 移植时先删成 **no-op**
   并在每处留 `// TODO(metric): <原 CH 指标名>` 标记；**收尾阶段**把有价值的接到
   Velox `RECORD_METRIC_VALUE`（`StatsReporter.h`）。**算法状态计数器**（cache
   size/elements、段 downloaded_size/reserved_size、hits_count、ref_count、
   `CacheStateGuard` 计数等是数据结构字段，非 ProfileEvents）→ **原样移植**。

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

`CachePriorityGuard > CacheMetadataGuard > KeyGuard > FileSegmentGuard`，外加独立的
`CacheStateGuard`（总 size/elements 计数）。移植时严格保持同一锁序。

## 4. 迁移顺序

| # | 单位 | CH 源 | 层 |
|---|------|-------|----|
| 1 | FileCacheKey | FileCacheKey.{h,cpp} | L0 |
| 2 | fwd + 默认值 | FileCache_fwd*.h | L0 |
| 3 | FileCacheUtils | FileCacheUtils.h | L0 |
| 4 | FileSegmentInfo / KeyType | FileSegmentInfo.h, FileSegmentKeyType.{h,cpp} | L0 |
| 5 | OriginInfo / CacheUsage | FileCacheOriginInfo.h, CacheUsage.h | L0 |
| 6 | Guards | Guards.h | L0 |
| 7 | FileCacheSettings | FileCacheSettings.{h,cpp} | L0 |
| 8 | IFileCachePriority | IFileCachePriority.{h,cpp} | L1 |
| 9 | FileSegment + Holder | FileSegment.{h,cpp} | L2 |
| 10 | Metadata 全家 | Metadata.{h,cpp} | L3 |
| 11 | EvictionCandidates | EvictionCandidates.{h,cpp} | L4 |
| 12 | LRUFileCachePriority | LRUFileCachePriority.{h,cpp} | L4 |
| 13 | SLRUFileCachePriority | SLRUFileCachePriority.{h,cpp} | L4 |
| 14 | (可选) SplitFileCachePriority | SplitFileCachePriority.{h,cpp} | L4 |
| 15 | QueryLimit | QueryLimit.{h,cpp} | L5 |
| 16 | FileCache | FileCache.{h,cpp} | L6 |
| 17 | (可选) FileCacheFactory | FileCacheFactory.{h,cpp} | L7 |
| 18 | BufferedInput 集成 | 新建 velox 侧文件 | L7 |
| 19 | 测试 + benchmark | — | 全部完成后 |

## 5. 需裁剪 / 适配的 CH 专有依赖（遇到时处理）

- `FileCacheSettings` 的 CH 配置解析（`Poco::Util::AbstractConfiguration`、
  `ServerSettings`）→ 精简为普通 settings 结构 + 默认值。
- `FileCacheFactory` 的 ClickHouse 全局注册表 → 视 Velox 需要决定是否移植/简化。
- `FileCacheOriginInfo` 的 `user_id` 等 CH 概念 → 用 Velox 等价物或精简。
- `QueryLimit` 依赖的 CH `QueryContext`/`CurrentThread` → Velox 侧以显式上下文传递替代。
- ProfileEvents / 指标 → 用 Velox 指标或暂略。

## 6. getOrSet 核心算法（已精读，移植锚点）

`FileCache::getOrSet`（`FileCache.cpp:800-966`）流程：boundary 对齐 →
`metadata.lockKeyMetadata(CREATE_EMPTY)` → `getImpl` 区间查找（`getImpl:461-557`，
lower_bound + 前一段相交判断）→ 处理 uncovered prefix/suffix（再 `getImpl` 探边）→
空则 `splitRange`(`559-604`)+`createFileSegmentsFromRanges`(`606-623`)；非空则
`fillHolesWithEmptyFileSegments`(`625-751`) → 返回 `FileSegmentsHolder`。
`get`（`968-1009`）：未命中返回 DETACHED 占位段，holes 用 detached 填充。

## 7. 验证策略

- 单位级：能写 UT 就写（叶子类如 Key/Utils/SegmentInfo 易测）；底层互相依赖的
  状态机/Metadata 在依赖闭合前可能只能编译门禁。
- 终局：功能闭合后统一补**完整单测 + benchmark**（复用 velox2 的 test/bench 作参考），
  对照 CH 语义与 velox2 行为做回归。

## 8. 非目标

- 不与 CH 的磁盘缓存格式互通（不需要读 CH 已有 cache 目录）。
- 不移植 CH 临时数据写缓存的 WriteBuffer 路径。
- 跨后端 microbenchmark（FsCache vs AsyncDataCache+SsdCache）、RAM 层 page cache、
  S3/HTTP 真实远端 backend —— 后续 spec。
