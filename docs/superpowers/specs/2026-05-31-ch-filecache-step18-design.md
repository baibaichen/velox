# Step 18 子 spec：FileCacheBufferedInput / FileCacheInputStream 集成（CH 忠实下载）

> 父 spec：`docs/superpowers/specs/2026-05-30-ch-filecache-port-design.md` §6.1（硬阻塞：step 18
> 编码前本子 spec 必须 approved）。本文档解决 §6.1 列出的全部开放问题，并把 cache-miss
> 下载路径从"急切整段下载"改为**忠实模拟 CH 的 partial-readable 流式下载 + 背景补尾**。

## 0. 工作纪律（八荣八耻）

本设计与后续实现遵循八荣八耻：
1. **认真查询**：所有 API / 字段 / 行号经 grep+view 核实，不臆测（下文引用均带 file:line）。
2. **寻求确认**：范围/语义不清先 `ask_user`，不默默猜测。
3. **人类拍板**：业务/命名/取舍由用户确认（Option 1 已确认）。
4. **复用现有**：复用 `ByteInputStream`/`FileInputStream`/`ParallelUnitLoader`/已移植
   `ch::FileSegment` 状态机，不重复造轮子。
5. **主动测试**：每个改动带 UT，改完跑 build + 相关测试。
6. **遵循规范**：遵守父 spec 模块边界与 velox dwio 既有 BufferedInput 接口。
7. **诚实无知**：不确定项在 §9 显式标注（holder 生命周期、所有权适配）。
8. **谨慎重构**：`RemoteFileReaderPtr` 改类型前确认全部调用方（当前 `setRemoteFileReader`
   零调用点，retype 安全）。

## 1. 背景与问题

### 1.1 step 18 是什么
step 1–17 是对 ClickHouse `FileCache` 的忠实移植（核心已落地，含 6 态状态机和
`PARTIALLY_DOWNLOADED`）。step 18 是**全新的 Velox 胶水层**：把已移植的 `ch::FileCache`
接到 Velox 的 `dwio::common::BufferedInput` 接口上，供 Parquet/DWIO reader 使用。涉及：

- `velox/dwio/common/FileCacheBufferedInput.{h,cpp}` —— 实现 `BufferedInput`，`enqueue/load`。
- `velox/dwio/common/FileCacheInputStream.{h,cpp}` —— 实现 `SeekableInputStream`，`Next/Seek`。

### 1.2 当前实现的偏差（要修的核心）
`FileCacheBufferedInput::load`（`FileCacheBufferedInput.cpp:244-294`）在 cache-miss 时，向
executor 提交一个任务，**急切地把整个 segment（4MB）下满**：

```cpp
constexpr uint64_t kChunk = 1u << 20;                       // 1MB
while (segment->getDownloadedSize() < segment->range().size()) {  // 直到整段 4MB
    readFile->pread(cursor, toRead, buffer.data());         // 直接 pread 本地源
    segment->write(buffer.data(), toRead, cursor);          // 循环 4 次
}
```

这**偏离 CH**：CH 是消费者驱动的**增量**下载——`CachedOnDiskReadBufferFromFile::nextImplStep`
每次 `next()` 只读一个 buffer（≤ `max_read_buffer_size`=1MB）并 `writeCache`，读多少下多少；
段尾由**背景下载池**在 holder 释放后补到 4MB（`getSizeForBackgroundDownloadUnlocked`，
`FileSegment.cpp:704-732`）。当前实现用"前台下满整段"把这件事做掉了，同时掩盖了两个未完成项：

- `CacheMetadata::downloadImpl`（`Metadata.cpp:887-918`）背景下载体是 **`VELOX_NYI`**（line 916）。
- `FileSegment::setRemoteFileReader`（`FileSegment.h:271`）**零调用点**；`RemoteFileReaderPtr =
  shared_ptr<ReadBufferFromFileBase>`（`FileSegment.h:57`）是前向声明的 stub。

## 2. 目标与非目标

**目标**
- cache-miss 前台下载只取**请求前缀** `[segStart, rangeEnd)`，段尾留 `PARTIALLY_DOWNLOADED`。
- 背景下载池把段尾补到 4MB（忠实 CH，不减少总字节，只挪到关键路径外）。
- 源端下载统一走 `ByteInputStream` 流式协议（兼容 S3/HTTP 远端），消除直接 `pread` 特判。
- 实现 NYI 的 `downloadImpl`；`RemoteFileReaderPtr` retype；胶水层 wire `setRemoteFileReader`。
- 保留 Velox 既有的 unit 级异步 I/O 重叠（`ParallelUnitLoader`）。
- 砍掉 glue 前台 `FileCacheDownloadExecutor`，前缀同步下载（CH 忠实，见 §3.1）。

**非目标**
- 不引入 cbi 式的 `coalesceIo` 跨 region 合并（见 §7，忠实 CH 即无此机制）。
- 不引入 cbi 式的 loadQuantum 滑窗预取（见 §7）。
- 不做 S3/HTTP 真实远端 backend 的端到端联调（接口留好，留后续 spec）。

## 3. 决策：Option 1（CH 忠实，已确认）

经 Option 1 vs Option 2 对抗式 PK，确认 **Option 1**：前台下载请求前缀 + 背景补尾，保留
`ParallelUnitLoader` 的 unit 级异步重叠。
（PK 关键证据：`ParallelUnitLoader::load`，`ParallelUnitLoader.cpp:138-159`，把 `unit->load()`
经 AsyncSource 提交到 ioExecutor，先于 decode 执行——异步重叠真实存在；Option 2 把下载移进
`Next()` 会回退该重叠，故否决。）

### 3.1 线程模型（已确认 Option A：砍前台执行器，前缀同步下载）

CH 的前缀下载是**同步在 read/query 线程上**（consumer-driven `next()`），唯一的异步池是**背景
补尾池**（CH `DownloadQueue`）。据此采用 **Option A**：

- **前缀下载 = 同步**，直接在 `FileCacheBufferedInput::load()` 调用线程上完成；**不**再经
  `FileCacheDownloadExecutor` 前台池。
- **unit 间 overlap 不丢**：生产路径下 `BufferedInput::load()` 由 `ParallelUnitLoader` 提交到
  `ioExecutor` 执行（`ParallelUnitLoader.cpp:147-152`），`load()` 同步阻塞也只阻塞 IO 线程，
  解码线程照常处理上一个 unit。砍前台池仅失去"**单个 unit 内多段并行下载**"——这正是 cbi
  风格、CH 没有的增强，砍掉更忠实。
- **背景补尾池**复用 `FileCache` 自带的 `downloadExecutor_`（`FileCache.cpp:486-487`，大小
  = `backgroundDownloadThreads_`，源自 `settings.backgroundDownloadThreads`，line 279）。
- **删除** glue ctor 的 `FileCacheDownloadExecutor* executor` 参数与非空断言
  （现 `FileCacheBufferedInput.cpp:157,172`）。
- **删除** benchmark flag `--fcbi_download_threads`；**新增** `--fcbi_background_download_threads`
  （默认 5 = CH `FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS`，`FileCache_fwd.h:27`），映射到
  已存在的 `settings.backgroundDownloadThreads`。

> **关于 `FileCacheDownloadExecutor` 的两个实例（澄清）**：该类有两处使用——(a) 生产
> `FileCache::downloadExecutor_`（背景池，`FileCache.cpp:487`）；(b) benchmark harness 注入给
> glue 的前台池（`--fcbi_download_threads`）。Option A 只删除 (b)；(a) 保留作为背景补尾池。

## 4. 前缀语义（"前台下请求前缀"是什么）

segment **顺序从前往后填充**，`downloadedSize` 单调递增、中间**无空洞**（CH `FileSegment`
不变量；移植版同样）。因此要读到请求区间的**末尾**字节，必须把 `[segStart, rangeEnd)`
整个前缀下下来——它 = **predownload 空隙** + **请求数据**。

```
段边界（4MB 对齐）:  segStart                                   segEnd
                     |                                          |
请求区间:                      rangeStart........rangeEnd
                     |        |                 |              |
                     |<-空隙->|<--- 请求数据 --->|<-- 段尾 ---->|
                     |<======= 前台下载前缀 =====>|<- 背景补尾 ->|
                       [segStart, rangeEnd)         [rangeEnd, segEnd)
                       关键路径（reader 等待）       关键路径外（背景池）
```

- **predownload 空隙** `[segStart, rangeStart)`：请求不需要，但因顺序填充约束必须先下。
- **请求数据** `[rangeStart, rangeEnd)`：reader 真正要的。
- **段尾** `[rangeEnd, segEnd)`：前台不下；holder 释放后由背景池补到 4MB。

若请求恰好对齐段头（`rangeStart == segStart`），空隙为 0，前缀 = 请求数据。

## 5. 下载粒度与配置旋钮

### 5.1 配置旋钮
| 旋钮 | 默认 | 含义 | CH 对应 |
|---|---|---|---|
| `--fcbi_segment_mb` | 4MB | **缓存/对齐单位**（一个 cache 段大小、背景补尾目标） | `FILE_SEGMENT_ALIGNMENT` / bgMaxSize |
| `--fcbi_read_buffer_mb` | 1MB | **源下载的流式 step**（`FileSegment::downloadFromReader` 的 `kDownloadChunk`） | `max_read_buffer_size` / `DBMS_DEFAULT_BUFFER_SIZE` |
| `--fcbi_background_download_threads` | 5 | **背景补尾池**线程数（映射 `settings.backgroundDownloadThreads`） | `BACKGROUND_DOWNLOAD_THREADS` |

> **移除** `--fcbi_download_threads`（Option A 砍掉前台执行器，见 §3.1）。

`--fcbi_segment_mb` 与 `--fcbi_read_buffer_mb` 正交：段大小决定缓存条目与背景补尾目标；read
buffer 决定每次从源流式取多少。`--fcbi_read_buffer_mb` ≥ 段大小时 → 每段一次取完（"一次读完"）。

> **关于默认值与当前实现**：设计阶段曾设想 step 由 `FileInputStream` 的 `bufferSize`（必填参数，
> `FileInputStream.h:30-32`）承载。实现最终改用 `ReadFileByteInputStream`（无 bufferSize 参数，每次
> `readBytes` 直接 `pread`），流式 step 落在共享静态 `FileSegment::downloadFromReader` 内的常量
> `kDownloadChunk = 1MB`（前台胶水与背景池**共用**该原语）。因此 `--fcbi_read_buffer_mb` 目前**固定为
> 1MB**：benchmark 校验非 1MB 即报错（不静默忽略），把它做成真正可配需要给该共享原语加 chunk 参数 +
> `FileCacheSettings` 字段，会改动生产缓存行为，故按最小改动原则推迟（见 `FileSegment.cpp` kDownloadChunk
> 处 TODO）。

### 5.2 与 cbi loadQuantum 的对位（仅供对比理解，非实现耦合）
- cbi `loadQuantum`（默认 8MB，`Options.h:65`）≈ fcbi `--fcbi_segment_mb`（缓存条目/加载单位）。
- cbi 的下载没有独立 step 概念（一次 readv 拉满一个 quantum）；fcbi 的 step 来自 CH 的流式
  read buffer 语义，是 fcbi 特有的、更细的下载粒度。

## 6. 接口与数据流

### 6.1 §6.1 开放问题的解决
| 开放问题 | 解决 |
|---|---|
| **范围翻译** | `BufferedInput::enqueue(region)` 记录待读区间；`load()` 对每个 region 调
`getOrSet(key, region.offset, region.length, …)`，得到对齐到 4MB 的 `FileSegmentsHolder`。前台
按 §4 只下 `[segStart, rangeEnd)` 前缀。 |
| **prefetch 交互** | enqueue→load：`load()` 同步下前缀（Option A，§3.1），unit 间 overlap 由
`ParallelUnitLoader` 把 `load()` 跑在 IO 线程提供；不实现 cbi 式 quantum 滑窗预取（§7）。 |
| **SeekableInputStream 语义** | `FileCacheInputStream` 在 holder 的多段之上提供连续可 seek 流；
`loadCurrentSegmentBuffer`（`FileCacheInputStream.cpp:40-96`）按 `segmentOffset+length` 的交集
等待并读取，已是 partial-aware。 |
| **cache-miss 回退** | 源下载失败→`setDownloadFailed`+终止态，reader 观察到放弃（现有
`FileCacheBufferedInput.cpp:267-273` 的 TODO 路径，保留）；DETACHED bypass 整段 pread 走
`velox::ReadFile`（现有 199-219 路径）。 |
| **参照物** | 接口实现照 `CachedBufferedInput`；选择点在
`connector::hive::createBufferedInput`（`HiveConnectorUtil.cpp`）。 |

### 6.2 两条读路径（关键区分）
1. **源下载**（miss 前台前缀 + 背景补尾）：源**可能是远端 S3/HTTP**，故**统一用
   `ByteInputStream` 流式**（`FileInputStream` 包 `ReadFile`+bufferSize），**不**特判本地 pread。
2. **cache-hit 读**：读本地 cache 段文件，**永远本地磁盘**，一次性 `pread`（`LocalReadFile`）即可，
   保持现状（`FileCacheInputStream.cpp:91-94`）。

### 6.3 remoteFileReader 接线与 downloadImpl
- **retype**：`FileSegment::RemoteFileReaderPtr`（`FileSegment.h:57`）从
  `shared_ptr<ReadBufferFromFileBase>` 改为 `shared_ptr<ByteInputStream>`（父 spec §2.1 已定）。
  确认安全：`setRemoteFileReader`（`FileSegment.h:271`）当前零调用点。
- **glue ctor 简化（Option A）**：删除 `FileCacheBufferedInput` ctor 的
  `FileCacheDownloadExecutor* executor` 参数及其非空断言（现 `FileCacheBufferedInput.cpp:157,172`）。
  前缀下载不再经前台池（见 §3.1）；背景补尾由 `FileCache::downloadExecutor_` 承载。
- **wire**：胶水层在 miss 时构造 `FileInputStream(over 源 ReadFile, bufferSize=--fcbi_read_buffer_mb)`
  并 `segment->setRemoteFileReader(...)`，供前台前缀下载与背景补尾共用同一流式 reader。
- **实现 downloadImpl**：`CacheMetadata::downloadImpl`（`Metadata.cpp:887-918`，现 `VELOX_NYI`）
  用 `ByteInputStream` 的 `nextView/remainingSize/atEnd/seekp/tellp` 驱动
  `file_segment.write(...)`，对位 CH 的 `set/seek/eof/available/position` + `write` 协议。
- **读游标 vs 写游标（勿混用）**：`ByteInputStream::tellp/seekp`（`ByteStream.h:173-177`，文档
  "current position … from the start"）是**源端读位置**游标——对位 CH `ReadBuffer::position/seek`，
  反映"已从源读到哪"。**段内已写入进度**则由 `FileSegment::write` 内部的 cursor 推进、由
  `getDownloadedSize()` 反映。实现时务必区分：源读进度看 `tellp/remainingSize`，缓存写进度看
  `getDownloadedSize`，两者不要互相代入（二者通常一致，但失败重试/部分写时会分叉）。

### 6.4 前台下载循环（改造后，Option A：同步）
`load()` 在**调用线程上同步**完成前缀下载（无 `executor_->submit`）：只把光标推进到 `rangeEnd`
（前缀），每步 ≤ `--fcbi_read_buffer_mb`，经 `ByteInputStream`：

```
# 在 FileCacheBufferedInput::load() 调用线程上同步执行
cursor = segStart
while (downloadedSize < (rangeEnd - segStart)):
    view = remoteFileReader->nextView(min(read_buffer, rangeEnd - cursor))
    reserve(view.size); segment->write(view.data(), view.size(), cursor)
    cursor += view.size()
completePart(allow_background_download = true)   # 触发段尾背景补到 4MB（背景池）
```

unit 间 overlap 由 `ParallelUnitLoader` 在生产路径提供（§3.1），故同步 `load()` 不挡解码线程。

`complete(..., allow_background_download=true)`（`FileSegment.cpp:759-830`）+
`getSizeForBackgroundDownloadUnlocked`（704-732，配置下补满 4MB）由
`FileCache::downloadExecutor_` 背景池负责段尾。

**`reserve` 失败语义（前缀循环新增路径，需明确）**：前缀循环里 `reserve(view.size)` 失败
（cache 满 / 无法腾出空间）时，沿用现有 miss 任务的终止语义——`setDownloadFailed` +
`completePartAndResetDownloader`（`FileCacheBufferedInput.cpp:267-273`），使等待的 reader 观察到
**放弃**而非空转；该 region 的本次读由上层走 DETACHED bypass 整段 `pread`（§6.1 表"cache-miss
回退"行）兜底。**不**在前缀循环里抛异常穿透到调用线程。（CH 在此处会改走远端读尾，属后续增强，
见现有 TODO(bypass-on-reserve-failure)。）

## 7. 已知架构差异（忠实 CH 的代价，非 bug）

> 这两条是 cbi 有、fcbi（忠实 CH）无的机制，在 benchmark 报告中应作为**已知架构差异**标注，
> 避免误读对比数字。均与本 step-18 正交，不阻塞。

1. **无 coalesceIo 跨 region 合并**：cbi 在 `load()` 把相邻条目按间隙≤512KB、累计≤128MB 合并成
   一次 `preadv`（`CachedBufferedInput.cpp:291-332`）；fcbi 逐 region/逐 4MB 段独立 `getOrSet`，
   源端 IO 更碎。CH 自身也不在 FileCache 层做此合并，靠 reader 层 readahead + 段顺序。
   → 见 TODO `fcbi-coalesce-io`（deferred，本 step 不做）。
2. **无 loadQuantum 滑窗预取**：cbi 在读越过当前 quantum 的 `prefetchPct_%` 时异步预取下一个
   quantum（`CacheInputStream.cpp:106-122`）；fcbi/CH 走 reader 层
   `AsynchronousBoundedReadBuffer` + 段顺序 + 背景补尾，不走此滑窗。

## 8. 验证策略

- **downloadImpl / 前缀循环**：UT 覆盖 (a) 请求对齐段头（空隙=0）；(b) 请求跨段中部（空隙>0）；
  (c) 请求跨多段；(d) `--fcbi_read_buffer_mb` ≥ 段大小（一次读完）；(e) read buffer < 请求（多步）。
- **背景补尾**：UT 验证 holder 释放后段从 `PARTIALLY_DOWNLOADED` 补到 4MB（`getDownloadedSize`
  == range size），且总下载字节 = 4MB（前缀 + 背景）。
- **背景补尾（holder 生命周期，关键正确性前提）**：UT 显式"读完一个 region 后 `drop` 其
  `FileSegmentsHolder`"，断言段尾在**有限时间内**（带超时轮询，非无限等待）补到 4MB。这是 Option 1
  正确性的硬前提（§9 风险 1），必须直接用 UT 验证，**不能**只靠 benchmark 间接观察。
- **流式协议**：用 mock `ByteInputStream` 验证 `downloadImpl` 的 `nextView/atEnd/write` 序列对位
  CH 语义。
- **同步 load() + unit 级 overlap（Option A）**：UT/断言确认 `load()` 在调用线程上**同步**完成前缀
  下载（无前台 executor 提交）；并在生产路径下 `ParallelUnitLoader` 把 `load()` 跑在 IO 线程，
  unit 间 overlap 不退化。
- **benchmark 公平性（Option A 必处理）**：harness 直接调 `load()`、**不走** `ParallelUnitLoader`，
  故 fcbi 同步 load() 与 cbi 异步 load() 不可直接比。处理：给 fcbi 的 `runBatch` 把 `load()` 包一层
  IO executor 异步（等价 ParallelUnitLoader 的 unit 级 overlap），或统一以 end-to-end wall time
  度量。报告中注明该处理。
- **端到端**：`velox_bufferedinput_wrapper_benchmark` 跑 fcbi vs cbi，核对前缀语义下的字节数与
  hit-rate，与 §7 架构差异一致。
- 构建：`cmake --build cmake-build-relwithdebinfo-gcc13 --target velox_bufferedinput_wrapper_benchmark -j$(nproc)`。

## 9. 开放问题 / 风险（诚实标注）

- **holder 生命周期**：背景补尾在 `FileSegmentsHolder` 释放时触发——需确认 Parquet 生命周期中
  `EnqueuedRegion::holder` 被及时析构，否则补尾延迟。（pk-option1 agent 标注的不确定项，待核实。）
- **所有权适配**：`FileInputStream` 持 `unique_ptr<ReadFile>`，而胶水层携带
  `shared_ptr<ReadFile>`；可能需小适配器（持 shared_ptr 的 ReadFile 包装）解决所有权。
- **read buffer 与段对齐的交互**：`--fcbi_read_buffer_mb` 不整除段大小时最后一步截断到 `rangeEnd`，
  需 UT 覆盖边界。

## 10. 实施单位（建议 commit 粒度）

1. `RemoteFileReaderPtr` retype → `shared_ptr<ByteInputStream>`（+编译修复，零行为变化）。
2. 实现 `downloadImpl`（流式协议）+ UT（mock ByteInputStream）。
3. **3a** ReadFile 所有权适配器（shared_ptr<ReadFile> → FileInputStream 所需 unique_ptr 语义的
   小包装，独立可测）。
4. **3b** 胶水层 `setRemoteFileReader`（用 3a 的适配器包源 ReadFile）+ 前台前缀循环改造
   （**同步**替换整段 while，含 reserve 失败语义）+ UT。
5. **Option A 砍前台执行器**：删 `FileCacheBufferedInput` ctor 的 `executor` 参数 + 断言，
   `load()` 改同步直跑（依赖 3b）；删 benchmark flag `--fcbi_download_threads`；benchmark harness
   给 fcbi `runBatch` 包一层 IO executor 异步（公平性）+ UT/断言。
6. 背景补尾接通（`complete(allow_background_download=true)`，用 `FileCache::downloadExecutor_`）+
   UT（含 holder-drop 补尾 UT）。
7. 配置旋钮 `--fcbi_read_buffer_mb` 接入 + 新增 `--fcbi_background_download_threads`（映射
   `settings.backgroundDownloadThreads`）+ benchmark 报告标注 §7 架构差异。
