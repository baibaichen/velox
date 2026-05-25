# FsCache Prefetch 集成调研：CH ↔ Velox 映射

> 调研笔记，2026-05-25。服务于 Phase-2 spec 的"预取集成"章节决策。
> **不是设计文档**——结论待 Phase-2 brainstorming 确认。

## 调研缘起

Phase-1 baseline benchmark 暴露 16 线程 hit-path 从 7M ops/s 降到 2.1-2.3M ops/s
的回归。根因诊断完成（单全局 metadata mutex + per-hit 两锁 + LRU 链表 ping-pong）后，
Phase-2 spec 已基本成型，剩"预取如何与 FsCache 集成"未定。本笔记回答：

> Velox 现有的预取机制能否让 FsCache 达到 CH 的预取效果或更好？

## CH 预取实现 (源码核对)

源码根：`/home/chang/SourceCode/ClickHouse/src/Interpreters/FileCache/` 与
`src/Disks/IO/`。

### 三条关键路径

1. **Reader 层异步派发**：
   `AsynchronousBoundedReadBuffer::nextImpl()` (Disks/IO) 把下一段读丢到
   `ThreadPoolRemoteFSReader`（Pool B），自己不阻塞。

2. **Reader 线程亲自 download**：
   miss 时由 `CachedOnDiskReadBufferFromFile::predownloadForFileSegment()`
   (`CachedOnDiskReadBufferFromFile.cpp:687-866`) 在 reader 线程里完成下载，
   写盘走 `writeCache()` (line 814)。

3. **`PARTIALLY_DOWNLOADED` 边下边读**：
   `FileSegment` 状态机覆盖部分下载状态，前台 reader 通过
   `canStartFromCache()` 消费已落盘的前 N 字节，剩余部分由 `download_threads`
   线程池（默认 5，`FileCache_fwd.h`）续传。

### 配套观测与配额

- `FilesystemReadPrefetchesLog` 在读 buffer 层记录 SUBMITTED / USED /
  CANCELLED_WITH_SEEK / UNNEEDED。
- `bypass_cache_threshold` (`FileCache.cpp:466`) — 大读绕过整个 cache，
  与预取正交。
- `FileCacheQueryLimit` 没有为预取单独配额，按 query 统一计。

## Velox 预取实现 (源码核对)

### 三层

| 层 | 入口 | 默认 |
|---|---|---|
| L1 算子调度 | `TableScan::preload` + `AsyncSource` + `connector->ioExecutor()` | `maxSplitPreloadPerDriver = 2` |
| L2 reader 调度 | Parquet `scheduleRowGroups`、DWRF `shouldPrefetchStripes`，统一走 `BufferedInput::enqueue` + `load(LogType)` | 各 reader 自定节奏 |
| L3 cache 内 quantum read-ahead | `CacheInputStream::Next()` → `bufferedInput_->prefetch(Region)` | `prefetchPct_ = 200`（**关**） |

### 两条 prefetch 路径的实际重要性 (本次调研订正)

| 路径 | 入口 | 谁调 | 默认 | 重要性 |
|---|---|---|---|---|
| A | `BufferedInput::enqueue` + `load()` 或 `CachedBufferedInput::scheduleLoads` | reader (Parquet/DWRF) | 开 | **主路径** |
| B | `CachedBufferedInput::prefetch(Region)` | 仅 `CacheInputStream::Next()` 一处 (`CacheInputStream.cpp:118`)，受 `prefetchPct_ < 100` 守门 | **关** | 生产几乎不走 |

生产代码里 **`setPrefetchPct` 仅被一个测试调过** (`dwrf/test/CacheInputTest.cpp:770`)。
所以"FsCacheBufferedInput 没 override `prefetch(Region)`" 并不致命——上游
主预取根本不通过这条路径。

### 关键事实

- `BufferedInput` 基类 **没有** `virtual prefetch(Region)`
  (`velox/dwio/common/BufferedInput.h:100-241`)。
- `CachedBufferedInput::prefetch(Region)` (`CachedBufferedInput.h:142`) 是
  **类自加**的 non-virtual 方法。
- `FsCacheBufferedInput : public BufferedInput`，**直接**继承基类，未继承
  `CachedBufferedInput`。

## 真正的漏接点 (本次调研主要发现)

`FsCacheBufferedInput::load()` 是**同步**的
(`FsCacheBufferedInput.cpp:147-158`)：

```cpp
void FsCacheBufferedInput::load(LogType /*unused*/) {
  for (auto& enqueued : enqueuedRegions_) {
    if (!enqueued.segments.empty()) continue;
    enqueued.segments = fsCache_->getOrSet(
        input_->getName(),
        enqueued.region.offset,
        enqueued.region.length,
        *input_->getReadFile());   // ← 阻塞 download
  }
}
```

**后果**：reader 想要的"异步把下个 stripe 的 region 拉到 cache"被压成同步串行
下载。Velox L1/L2 预取信号都通过 `load()` 进 FsCache，但 FsCache 把它们退化
为前台 demand-fetch，并行预取**完全丢失**。

## CH ↔ Velox 三桶分类

**(a)** Velox 已有，FsCache 直接接 |
**(b)** Velox 缺，FsCache 补 |
**(c)** 架构差异，需要专门 phase

| CH 机制 | 桶 | 落点 |
|---|---|---|
| `AsynchronousBoundedReadBuffer::nextImpl` 异步读 | (a) | Velox L2/L3 已有，FsCache 接 `load()` 异步化 |
| `ThreadPoolRemoteFSReader` (Pool B) | (a) | Velox `connector->ioExecutor()` |
| Reader 亲自 download 写 cache | (a) → 漏接 | `FsCache::lookupOrCreate` 已实现，但 `FsCacheBufferedInput::load` 是同步 |
| `download_threads` (Pool A，后台续传) | (c) → (b) | Phase 2.5（依赖 PARTIALLY_DOWNLOADED） |
| `PARTIALLY_DOWNLOADED` + `canStartFromCache` | (c) | Phase 2.5 独立做 |
| `FilesystemReadPrefetchesLog` | (a) | Velox `IoStatistics::prefetch{,Bytes,Hits}` + `AsyncDataCacheEntry::isPrefetch_` 已等价 |
| `bypass_cache_threshold` | (b) | Phase-2 umbrella 已含 |
| `FileCacheQueryLimit` | (b) | Phase-2 umbrella 已含 |

## "能否达到 CH 或更好" 按维度结论

| 维度 | 结论 |
|---|---|
| 算子-level split preload | **Velox 更好**（CH 无对应） |
| Reader-level stripe/RG prefetch 节奏 | **Velox ≥ CH**（按 file format 分开调，更细） |
| 预取 ↔ cache 集成（hit/partial/miss download） | **接好后 ≈ CH（不含 partial）；不补 partial 仍差一档** |
| 后台 download 池 | Phase-2 引入 download 池可拉平**调度部分**；partial-readable 留 Phase 2.5 |
| 预取观测 | **持平** |
| 大读 bypass | Phase-2 umbrella 已含 |

## 三种补救方案

| 方案 | 改动 | 评价 |
|---|---|---|
| **R0** | `FsCacheBufferedInput::load()` 异步化：miss segment 丢 download 池，reader 立即返回；真正读时 `DeferredStream::ensureWithData` 同步等待 | **推荐**。改 1 个文件，无新接口，复用现有 lazy 同步点 (`DeferredStream`)，恰好把 reader-driven async download 完整搬过来。比 CH 更干净——CH 是在 `AsynchronousBoundedReadBuffer` 里搞异步，Velox 在 `BufferedInput::load` 这一层就异步了。 |
| **R1** | `BufferedInput` 基类加 `virtual bool prefetch(Region)`，FsCacheBufferedInput override | **降级**。生产没人调 `prefetch(Region)`，改基类收益小；且与 R0 解决的不是同一类问题（R0 解决 A 路径，R1 修 B 路径）。 |
| **R3** | 另加显式 `enqueue → asyncLoad → wait` API | **不推荐**。reader 必须感知 cache 类型，破坏 BufferedInput 抽象。 |

## R0 待决问题（留给 Phase-2 brainstorming）

1. **lazy wait 锚点**：`DeferredStream::ensureWithData()` (`FsCacheBufferedInput.cpp:104-120`)
   目前 `VELOX_CHECK(!slot_->segments.empty(), "Stream used before load()")`，
   假设 `load()` 完成时 segments 已 ready。R0 把 segments 改为 future/状态，
   `ensureWithData` 改为同步等待——这个等待是否会阻塞 reader 线程到不可接受的
   程度？需要确认 reader 是否能容忍"`enqueue + load` 返回后，第一次 `Next()`
   仍可能阻塞数百 ms"。
2. **download 池并发模型**：池大小、与 `connector->ioExecutor()` 关系
   （复用还是独立池）、per-segment 还是 per-region 调度、和 reader 端
   `maxSplitPreloadPerDriver` 的乘法效应。

这两点是 Phase-2 spec 必须答的。

## 引用

- `velox/dwio/common/FsCacheBufferedInput.cpp:147-158` — 同步 load 现状
- `velox/dwio/common/FsCacheBufferedInput.cpp:104-120` — `DeferredStream` lazy 同步点
- `velox/dwio/common/BufferedInput.h:100-241` — virtual 接口枚举
- `velox/dwio/common/CachedBufferedInput.h:142` — `prefetch(Region)` 非 virtual
- `velox/dwio/common/CacheInputStream.cpp:106-121` — quantum read-ahead 触发条件
- `ClickHouse/src/Interpreters/FileCache/FileSegment.cpp:1196-1223` — `increasePriority` try_lock
- `ClickHouse/src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:687-866` — `predownloadForFileSegment`
- `ClickHouse/src/Disks/IO/AsynchronousBoundedReadBuffer.cpp:233-308` — `nextImpl` 异步派发
