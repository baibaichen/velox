# pull-core（阶段1）设计 —— FCBI 下载从 load() 挪到消费

> 状态：**已实现**（plan `plans/2026-06-01-pull-core.md`）。`velox_ch_file_cache_test` 全绿（83 tests）。提交 `cace38488`（核心改写）+ `7c2f8c00a`（守护测试）。
> 前置决策见 `2026-06-01-f16-serve-from-memory-design.md` 顶部横幅（F16/push 废弃、改 pull）。
> 本 spec 只覆盖**阶段1 结构重构**：把下载从 `load()` 挪到 `FileCacheInputStream` 消费时。
> **不含**：serve-from-memory（省 re-pread）、phase-2 异步预取、`async-ssd-writeback`——均为后续 TODO。
> 八荣八耻：所有引用带 file:line；语义经用户逐项确认。

## 1. 目标与非目标
- 目标：`load()` 退化为 planner（解析 holder + DETACHED bypass）；下载在消费时按段懒触发，对齐 DBI/CH。
- 非目标（明确推迟）：serve-from-memory；phase-2 `ScanTracker`+executor 预取；异步写回 SSD。
- 验收：行为/计数语义与 CH-faithful 契约（`FileCache.h:253-259`，消费时按段在 consumer 计数）一致；
  bench 命中率与现状一致（纯前向消费，见下）。

## 2. 现状（push，已提交，内存有界）
- `enqueue` 记 `EnqueuedRegion` 返回 `DeferredStream`（`FileCacheBufferedInput.cpp:295-301`）。
- `load()` 三趟：Pass1 解析 holder + DETACHED bypass；Pass2 命中计数 + coalesce miss target；
  Pass3 同步下载每个 miss 段到盘（`FileCacheBufferedInput.cpp:303-406`）。
- 消费：`FileCacheInputStream::loadSegment` 只**等**下载（60s gated loop）再 pread 盘
  （`FileCacheInputStream.cpp:99-163`）。
- `downloadSegmentPrefix`（`FileCacheBufferedInput.cpp:157-273`）：单下载者/前向续传/handoff，已为跨线程共享段设计。

## 3. 改动

### 3.1 `load()` → 纯 planner
- 保留 Pass1：`getOrSet` 解析 holder + DETACHED → bypass pread（`FileCacheBufferedInput.cpp:303-345`）。
- **删除 Pass2、Pass3**（不再下载、不在 load() 记命中/未命中）。
- 更新类注释（`FileCacheBufferedInput.h:35-42`）：load() 不再同步下载 miss。

### 3.2 搬移 `downloadSegmentPrefix`
- 从 `FileCacheBufferedInput.cpp` 匿名 namespace **移入** `FileCacheInputStream.cpp`（它现在是流的下载者）。
  逻辑不变（单下载者/前向/handoff/terminal）。

### 3.3 `FileCacheInputStream` 自包含化（拿源句柄 + 自持 holder）
对齐 CH：`CachedOnDiskReadBufferFromFile` 自己持有 holder（`ReadInfo::file_segments`，析构
`completeAndPopFront` 释放）——**内层 reader 自持 holder**。故：
- 构造新增 3 个句柄：`std::shared_ptr<ReadFile> readFile`（共担源文件生命周期）、`FileCache* cache`、
  `std::shared_ptr<io::IoStatistics> ioStats`。
- **构造改为接管 holder**：把 `FileSegmentsHolderPtr` 从 `EnqueuedRegion` `std::move` 进流（替换原来
  copy 出 `std::vector<FileSegmentPtr> segments` 的做法）；流持有 holder 为成员，端到端掌控段生命周期。
  `EnqueuedRegion` 只保留 `region` + bypass 信息。
- `DeferredStream::ensureWithData`（`FileCacheBufferedInput.cpp:121-148`）：捕获并转发 3 句柄 +
  `std::move(slot_->holder)` 给流；move 后 slot 的 holder 置空（bypass 路径不走此处，不受影响）。
- 内层 `FileCacheInputStream` 自持 holder ⇒ 段生命周期（含背景 tail-fill）跟随该流读完，而非 FCBI 析构
  （CH-faithful）；内层流也不再回查 `EnqueuedRegion` 取段。**注意**：外层 `DeferredStream` 仍持有
  `slot_` 裸指针（bypass 还从 `slot_->bypassBuffer` 取字节），故**不主张"流可活过父 FCBI"**——消费期间
  FCBI 必须存活。phase-1 仅把内层 holder 所有权对齐 CH（rubber-duck non-blocking #2，范围收敛）。
- **不变量（pull 语义）**：`SkipInt64`/`Skip`（`FileCacheInputStream.cpp:216-227`）只动 `regionCursor_`，
  **不得触发 `loadPosition()`/下载**；新增的 lazy `loadSegment` 只能从 `loadPosition()` 调用。否则"跳过段
  不下载"语义被破。注意 `DeferredStream::ensureWithData` 的 skip 重放（`FileCacheBufferedInput.cpp:145`）：
  pull 把"构造 inner"与"触发下载"合一后，**首次 `ensureWithData`（即首个 `Next()`）就会触发下载**，故
  "先 Skip 整段 + 后 Next() 一字节" 仍会下载整段——符合预期。

### 3.4 `loadSegment(index)` 成为下载驱动（`FileCacheInputStream.cpp:99-163`）
- `needed = sliceOffsetInSegment_[index] + sliceLen`；`segStart = segment->range().left`。
- 若 `segment->getDownloadedSize() >= needed`（缓存已覆盖本 slice，对齐 CH `canStartFromCache` =
  `current_write_offset > offset`，**覆盖判据而非 `state()==DOWNLOADED`**）：
  `cache->recordHit()` + `ioStats->ssdRead(sliceLen)`，直接 pread。
- 否则：调 `downloadSegmentPrefix(segment, readFile_, cache_, ioStats_, segStart + needed)`
  （miss/下载字节在其内部记一次）；返回后 `VELOX_CHECK(getDownloadedSize() >= needed, "writer abandoned")`。
- 然后 pread slice 到 `segData_`/`tinyData_`（**读路径不变**，阶段1 保留 re-pread）。
- **删除**原 60s wait-gate loop：仅因 `downloadSegmentPrefix` 成为**唯一同步下载驱动**，其内部
  holder/wait/recheck 循环已承担 EMPTY/PARTIAL/DOWNLOADING 的退避（要么本线程当 downloader，要么
  wait 别人当，循环到 terminal 才返回），外层 `VELOX_CHECK(getDownloadedSize() >= needed)` 才足够。
  **不变量**：任何不经此路径进入 `loadSegment` 的入口（若将来出现）将立刻破坏此前提。
- **durability 不变量**：`getDownloadedSize() >= needed` ⇒ slice 字节已 pread 可见——因 `append()`
  写入 page cache 后 `downloadedSize` 才前进，同主机后续 pread 经 kernel page cache 立即可见，无需 fsync
  （filecache 已 drop fsync，commit 44056d39c）。**仅在 same-host local disk 下成立**。

## 4. 计数语义（决策 B，已确认，CH 源码佐证）
- 契约：`FileCache.h:253-259`——消费时、在 consumer 计数。CH `readFromFileSegment` 实证：
  hit/miss 按 read_type **每次缓冲填充（`buf->next()`）记一次**（CACHED→Hits/ReadFromCacheBytes，
  REMOTE_FS_READ_AND_PUT_IN_CACHE→Misses/ReadFromSourceBytes），**不按段去重** → 决策 B 即 CH 语义。
- **不引入 `accounted_`**：`loadSegment` 每次按"是否已覆盖"分类（§3.4 覆盖判据）。命中：每次覆盖记一次；
  未命中：`downloadSegmentPrefix` 内部记（已下/已覆盖的段回读走命中路径，不重下、不重记 miss）。
- 覆盖判据顺带补上 partial-但-覆盖 段的命中计数（CH 按 CACHED 记，旧 push 漏记）；bench 无 partial 段、数字不变。
- 取舍：回退重读会按读事件累加 hit/miss——更贴 CH；bench 纯前向消费不触发（见 §5）。

## 5. bench 影响
- 三个 harness 消费均为纯前向 drain（`CacheReadHarness.cpp:177` `while(copied<readSize && Next())`），
  无 BackUp/回退 seek → A、B 计数相同，命中率与现状一致。
- 已知回退点：阶段1 把首 row group 在 split-preload 线程的 IO 挪进 driver 线程消费 → 重叠变弱，
  可能小幅回退；phase-2 预取找回。**接受**。

## 6. 测试影响
- 两个 `FileCacheBufferedInputTest.cpp`（`velox/dwio/common/tests/`、
  `velox/common/caching/filecache/tests/`）：凡断言"load() 后段已 DOWNLOADED / downloadedBytes>0"的，
  改为先驱动一次消费再断言。命中/未命中断言改为消费后读取。
- 新增：**纯 Skip 不下载**——只 `Skip` 跳过整段、不跟 `Next()` → 该段不下载、不计数（pull 语义）。
- 新增：**Skip 后 Next() 会下载**——`Skip` 跳过整段后再 `Next()` 一字节 → 经 `ensureWithData` skip 重放
  触发下载（区别于上一条，显式分两个用例）。
- 新增：同段两个 DeferredStream 共享 → 每流各记。
- bypass 回归：设极小 `bypassCacheThreshold` 触发 DETACHED → 走 `bypassBuffer` 直读，字节正确且
  行为与 pull 重构前一致（见 §7）。

## 7. 背景：DETACHED 段的三种来源（bypass 路径，测试需覆盖）
Pass1 对 `getOrSet` 返回的 DETACHED 段走 **bypass 直读**（`FileCacheBufferedInput.cpp:324-343`：
pread 整段进 `EnqueuedRegion::bypassBuffer`，不进缓存、不计命中率）。DETACHED 由
`getImpl`/`get` 产生（`FileCache.cpp:526-560,1068-1069`），三种场景：
1. **请求超过 `bypassCacheThreshold`**（`FileCache.cpp:531-536`）：`range.size() > bypassCacheThreshold`
   → 整范围做成一个 DETACHED 段（读太大不值得缓存，直读源，如一次几十/上百 MB 大读）。
2. **段正在被淘汰/删除**（`FileCache.cpp:548-554` `isEvictingOrRemoved`）：命中段恰好正被 SLRU/LRU
   驱逐或 `removeIfExists` → 返回只读 DETACHED 副本，本次直读。
3. **缓存兜底失败**（`FileCache.cpp:1068-1069`）：分配/锁失败等 → 返回覆盖整 range 的 DETACHED 段。

官方语义（`FileSegment.h:184-197`）：DETACHED = 此段不再归缓存管理，持有者只读、自行直读源。
阶段1 **不改 bypass 路径**；§6 保留一条回归用例确认 pull 重构后 bypass 行为不变
（场景 1 最易构造：设极小 `bypassCacheThreshold` 触发 DETACHED）。

## 8. 风险与开放项
- 风险：首 row group 重叠变弱（§5）；`downloadSegmentPrefix` 调用点从单线程 load 挪到消费线程——
  其跨线程语义本就支持，无新增危害（待并发单测覆盖，见 deferred `c5-drop-executor`）。
- 生命周期：holder 已移进流（§3.3），流自包含、不再依赖父 FCBI 存活；`FileCache*`/pool 仍需长于消费（caller 持有，恒成立）。
- 已解决（CH 佐证）：partial-但-覆盖 段的命中计数（§4 覆盖判据）。
- 开放：DETACHED bypass 仍在 `load()` 整段 pread（`bypassBuffer` 占整 region 内存）——阶段1 不动，记为后续。
- 开放（已知粗粒度）：miss 段读 = 已有缓存前缀 + 新下载后缀，CH 按字节拆分（前缀 cache、后缀 source），
  本 port 维持现状粗粒度（整 slice 一次 pread，后缀字节归 `downloadSegmentPrefix` 的 read/prefetch），不细拆。
