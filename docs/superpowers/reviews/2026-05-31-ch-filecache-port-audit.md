# ClickHouse FileCache → Velox 移植审计报告

- **日期**: 2026-05-31
- **分支**: `ch-filecache`
- **被审对象**: `facebook::velox::ch` 下的 CH FileCache 移植
  - 核心子系统: `velox/common/caching/filecache/`(应逐行忠实于 CH）
  - 集成读/下载层: `velox/dwio/common/FileCache*`（按 spec §7 有意改写）
- **CH 参照**: `/home/chang/SourceCode/ClickHouse`
  - `src/Interpreters/FileCache/`
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp`
- **设计 spec**: `docs/superpowers/specs/2026-05-31-ch-filecache-step18-design.md`（§7 记录有意偏离：无 RAM 层、无 coalesceIo、前台同步下载 + 后台 tail-fill）

> 本文件是 **ground truth**：每条都带双边 `file:line` 引用。配套 HTML（`2026-05-31-ch-filecache-port-audit.html`）面向人阅读，贴真实代码片段、不含行号。

---

## 1. 方法

三个**独立**的只读审计代理并行逐函数对照 CH 与 Velox，外加作者本人对最高价值项的逐行复核：

| 代理 | 模型 | 提示词 | 特点 |
|---|---|---|---|
| `ch-review-detailed` | claude-opus-4.8 | 完整（文件配对 + 10 点清单） | 亲自逐行通读核心状态机/tryReserve，子代理深读其余 |
| `ch-review-detailed-gpt` | gpt-5.5 | 完整（同上） | 跨模型交叉验证 |
| `ch-review-independent` | claude-opus-4.8 | 精简（仅目录/方法/格式/纪律） | 自行配对文件，无先验；**显式剔除假阳性** |

> 注：另有一个 `ch-port-review` 代理在未获批准下被误启动，其输出**未采用**，不计入本报告。

纪律：只读不改、双边 `file:line` 证据、严重度分级（CRITICAL/HIGH/MEDIUM/LOW）、区分「核心层算法不一致（更可疑）」与「集成层有意改写」。

---

## 2. 三方交叉汇总（按共识强度）

严重度取三方中位/作者复核后的最终判定。`H/M/L` = 该代理的评级，`–` = 未提及。

| ID | 主题 | 层 | GPT-5.5 | Claude-d | 独立 | 作者复核 | 最终 |
|---|---|---|:--:|:--:|:--:|:--:|:--:|
| F1 | 读路径缓存写/预留失败未降级为 bypass → 硬失败 | 集成 | H | H | H | 确认 | **HIGH** |
| F2 | `boundaryAlignment==0` 时 `roundUp` 除零崩溃（丢 0 保护） | **核心** | – | – | H | **确认（真坑）** | **HIGH** |
| F3 | `FileSegment::write` 部分写失败后缺 ENOSPC 处理/元数据修复 | **核心** | M | M | H | 确认 | **HIGH** |
| F4 | per-query 缓存配额事实禁用（query context NYI） | 核心(NYI) | H | M | M | – | MEDIUM |
| F5 | keep-free-space 后台任务 NYI，配置开启即抛 | 核心(NYI) | H | – | M | – | MEDIUM |
| F6 | reserve 锁等待超时硬编码 10s（应可配，默认 1s） | 集成 | M | – | M | – | MEDIUM |
| F7 | `FileCacheKey` 哈希不同（SpookyHashV2 vs SipHash128） | 已文档化 | M | L | (折叠) | – | LOW※ |
| F8 | `getCallerId` 丢 `query_id`/线程名 | 核心 | – | L | M | – | LOW |
| F9 | 短对象 EOF 被当作下载失败 | 集成 | – | L | M | – | LOW |
| F10 | DETACHED bypass 粒度：region 级 vs CH per-segment | 集成 | – | – | M | – | MEDIUM |
| F11 | 段完成/空间释放时机：持有到 BufferedInput 析构 | 集成 | – | – | M | – | MEDIUM |
| F12 | origin user 每进程随机 vs CH ServerUUID | 核心 | – | – | L | – | LOW |
| F13 | write 打开 flags 丢 `O_APPEND`/`O_CLOEXEC`（功能仍正确） | 集成 | – | – | L | – | LOW |
| F14 | 删文件后 `OpenedFileCache` fd 清理未移植（当前无 fd 缓存） | 核心 | – | M | – | – | LOW |
| F15 | 可观测性 metrics/ProfileEvents/failpoint 成片 TODO | 全局 | L | L | L | – | LOW |

※ F7 虽两方提及，但代码已注释说明为有意替换，实质为「等价适配/已文档化」，非 bug。

**共识强度**：
- 🔴 三方一致：F1、F3、F4、F15
- 🟡 双方一致：F6、F7、F8、F9
- ⭐ 单方 + 作者逐行复核确认：F2（最高价值，另两方漏掉）
- 单方：F5（双方但其一未提）、F10、F11、F12、F13、F14

---

## 3. 逐条详情

### F1 — 读路径缓存写/预留失败未降级为 bypass（HIGH，集成层）

- **CH**：缓存盘写不进（ENOSPC/EDQUOT）或预留失败时，切 `ReadType::REMOTE_FS_READ_BYPASS_CACHE`，剩余字节直读远端，**本次读继续成功**。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:523-533`（切 bypass）
  - `:880-886`（predownload 失败后 `completePartAndResetDownloader()` 并切 bypass）
  - `:977-981`（`write()` 抛 ENOSPC/EDQUOT 时 `writeCache` 返回 false）
  - `:1315-1316`（reserve 失败转 `PARTIALLY_DOWNLOADED_NO_CONTINUATION` 并 bypass）
- **Velox**：前台下载无进展/预留失败/异常 → `setDownloadFailed()` 后返回，无 bypass 重试；读到终态且字节不足直接 `VELOX_FAIL`。
  - `velox/dwio/common/FileCacheInputStream.cpp:92-99`（`VELOX_FAIL`）
  - `velox/dwio/common/FileCacheBufferedInput.cpp:245-258`（`setDownloadFailed()`+return）
  - `:320-339`（bypass 仅覆盖 `getOrSet` 直接返回 DETACHED 的段，**不覆盖**下载中途失败）
- **差异/影响**：缓存盘满、配额超限或缓存盘 IO 错误时，CH 查询降级成功，Velox 查询**硬失败**。生产可用性实质回归。
- **判定**：待人确认。集成读路径按 spec §7 有意改写，但「缓存失败→查询失败」是否可接受需拍板；若要对齐 CH 健壮性，需补 bypass 远端读降级。

### F2 — `boundaryAlignment==0` 除零崩溃（HIGH，核心层，作者已逐行复核确认）

- **CH**：两个对齐工具都防 0。
  - `src/Interpreters/FileCache/FileCacheUtils.h:9-20`（`roundUpToMultiple`/`roundDownToMultiple` 均 `if (!multiple) return num;`）
  - `src/Interpreters/FileCache/FileCache.cpp:822-825`（对齐调用）
- **Velox**：`roundDownToMultiple` 保留 0 保护，但 round-up 改用无 0 保护的 `bits::roundUp`。
  - `velox/common/caching/filecache/FileCache.cpp:71-74`（本地 `roundDownToMultiple` 有 0 保护）
  - `velox/common/caching/filecache/FileCache.cpp:890-891`（`bits::roundUp(initial_range.right + 1, alignment)`）
  - `velox/common/base/BitUtil.h:126-128`（`roundUp = (value + (factor-1)) / factor * factor` → `factor==0` 时 `/0`）
- **可达性（作者复核）**：
  - settings 校验只禁 `boundaryAlignment > maxFileSegmentSize`，**不禁 0**：`velox/common/caching/filecache/FileCacheSettings.cpp:64`
  - CH 默认 per-read `filesystem_cache_boundary_alignment = 0`，且 CH 用 0 保护正确处理：`src/Core/Settings.cpp:6502`（CH 明确支持 alignment=0）
  - 当前 dwio 集成对 `getOrSet` 的 `boundary_alignment_` 传 `std::nullopt` → 走非 0 默认，**当前路径安全**：`velox/dwio/common/FileCacheBufferedInput.cpp:316,318`
  - 但用户把 settings `boundaryAlignment` 配成 0（校验允许、CH 支持）即触发 `getOrSet` 内 `SIGFPE`。
- **判定**：**疑似 bug（核心层潜在崩溃）**。一行修复：round-up 改回带 0 保护的 `roundUpToMultiple`。

### F3 — `FileSegment::write` 部分写失败后缺元数据修复（HIGH，核心层）

- **CH**：分类 `catch`：`ErrnoException` 对 ENOSPC(28)/EDQUOT(122) 特判；文件存在且 `downloaded_size==0` 删空文件；否则把 `downloaded_size` 校正为实际 `file_size` 以保留已落盘字节；`fs::filesystem_error` 转 `ErrnoException`；均在 `setDownloadFailedUnlocked` 后 rethrow。
  - `src/Interpreters/FileCache/FileSegment.cpp:451-498`
- **Velox**：单一 `catch(const std::exception&)` → `setDownloadFailedUnlocked()` 后 `VELOX_FAIL`，无 ENOSPC 特判、无文件清理、无 `downloaded_size` 校正。
  - `velox/common/caching/filecache/FileSegment.cpp:439-448`
- **缓解（Claude-d 复核）**：后续 `complete` 时 `shrinkFileSegmentToDownloadedSize` 按 `downloadedSize` 截断 → 最终磁盘一致；**不会数据损坏**，但比 CH 少保留可复用的部分字节，且 0 字节失败文件可能残留。
- **判定**：疑似 bug（核心层）。建议补 ENOSPC 处理与空文件清理以对齐 CH。

### F4 — per-query 缓存配额事实禁用（MEDIUM，核心层 NYI）

- **CH**：`tryGetQueryContext` 经 TLS（`CurrentThread::getQueryId`）取查询上下文；`tryReserve` 据此做 per-query 限额。
  - `src/Interpreters/FileCache/QueryLimit.cpp:22-28`
  - `src/Interpreters/FileCache/FileCache.cpp:2560-2568`（`getQueryContextHolder`）
- **Velox**：`tryGetQueryContext` 恒返回 `nullptr`（TODO）；`getQueryContextHolder` 为 `VELOX_NYI`；集成层不传 query id。
  - `velox/common/caching/filecache/QueryLimit.cpp:25-30`
  - `velox/common/caching/filecache/FileCache.cpp:2655-2659`
  - `velox/dwio/common/FileCacheBufferedInput.h:43-50`、`FileCacheBufferedInput.cpp:273-289`
- **影响**：per-query 限额逻辑完整保留但永不触发；CH 默认也禁用此特性，故**默认行为一致**；若依赖该特性则缺失。
- **判定**：待人确认（已知 TODO 设计选择）。

### F5 — keep-free-space 后台任务 NYI（MEDIUM，核心层 NYI）

- **CH**：非 0 free-space ratio 时创建并调度后台任务反复 `scheduleAfter/schedule`。
  - `src/Interpreters/FileCache/FileCache.cpp:446-449`、`1398-1530`（`freeSpaceRatioKeepingThreadFunc`）
- **Velox**：`BackgroundSchedulePoolTaskHolder::schedule/scheduleAfter` 均 `VELOX_NYI`；当 `keepCurrentSizeToMaxRatio != 1 || keepCurrentElementsToMaxRatio != 1` 时初始化仍会调 `schedule()` → 抛 NYI。
  - `velox/common/caching/filecache/FileCache.cpp:176-177`、`512-515`
- **影响**：默认 ratio=1 不触发；一旦配置 keep-free-space 比例，init **显式抛错**（非静默）。
- **判定**：待人确认（已知未移植特性）。

### F6 — reserve 锁等待超时硬编码（MEDIUM，集成/下载）

- **CH**：默认 `reserve_space_wait_lock_timeout_milliseconds = 1000`，前台/后台下载均从 settings 读。
  - `src/IO/ReadSettings.h:112-114`、`CachedOnDiskReadBufferFromFile.cpp:1279-1283`、`Metadata.cpp:888-889`
- **Velox**：定义了同名默认 1000 的字段但下载路径不用，前台/后台均硬编码 10000。
  - `velox/common/caching/filecache/FilesystemCacheSettings.h:33-35`（字段）
  - `velox/dwio/common/FileCacheBufferedInput.cpp:31,233-235`（`kReserveTimeoutMs{10000}`）
  - `velox/common/caching/filecache/Metadata.cpp:918-922`（硬编码 10000）
- **影响**：锁竞争/缓存满时放弃时机与 CH 不等价，配置不可控。仅时序，无数据损坏。
- **判定**：待人确认。

### F7 — `FileCacheKey` 哈希不同（LOW，已文档化）

- **CH**：`fromPath()` 用 `sipHash128(path.data(), path.size())`。
  - `src/Interpreters/FileCache/FileCacheKey.cpp:31-34`
- **Velox**：`fromPath()` 用 `folly::hash::SpookyHashV2::Hash128`，代码注释已说明不同于 CH。
  - `velox/common/caching/filecache/FileCacheKey.cpp:66-75`
- **影响**：同一路径生成的 128-bit key 与 CH 不一致 → 缓存目录不可与 CH 互用。
- **判定**：等价适配（已文档化）；若目标是「缓存目录与 CH 互通」才算 bug。

### F8 — `getCallerId` 丢 query_id（LOW，核心层等价）

- **CH**：`src/Interpreters/FileCache/FileSegment.cpp:222-228`（`query_id:thread_id`，无 query 时 `None:thread_name:thread_id`）
- **Velox**：`velox/common/caching/filecache/FileSegment.cpp:230-235`（始终 `None:<thread id>`）
- **影响**：进程内 downloader 选举靠唯一 thread id 仍正确；主要影响 per-query 归因与诊断（与 F4 一致）。
- **判定**：等价适配。

### F9 — 短对象 EOF 被当作下载失败（LOW，集成层）

- **CH**：远端对象实际大小 == 当前 offset 时视为合法 EOF（`setDownloadFinishedWithoutContinuation()`）。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:1368-1398`
- **Velox**：源端无前进即 `setDownloadFailed()`；字节不足且非 DOWNLOADING 直接 `VELOX_FAIL`。
  - `velox/dwio/common/FileCacheBufferedInput.cpp:245-252`、`FileCacheInputStream.cpp:92-99`
- **影响**：两者最终都把段置为 `PARTIALLY_DOWNLOADED_NO_CONTINUATION`，状态机结果等价；DWIO 读 region 源于已知 `fileSize_`，正常不越界，触发概率低；远端被并发截断时 Velox 会异常中断。
- **判定**：待人确认（语义精度略降）。

### F10 — DETACHED bypass 粒度（MEDIUM，集成层）

- **CH**：仅当前 DETACHED 段走 bypass，其余段照常走缓存。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:439-443`
- **Velox**：region 内任一 segment 为 DETACHED，整个 enqueued region 走 bypass 全量 pread。
  - `velox/dwio/common/FileCacheBufferedInput.cpp:320-338`
- **影响**：同一 region 中其他已缓存段被一并绕过，命中率/性能下降（数据不损坏）。
- **判定**：待人确认。

### F11 — 段完成/空间释放时机（MEDIUM，集成层）

- **CH**：边读越过段边界即 `completeFileSegmentAndGetNext()`，逐段及时收尾。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:1158-1160`
- **Velox**：holder 保存在 `enqueuedRegions_`，前台只 `completePartAndResetDownloader()`；段的 `completeAndPopFront` 仅在 holder 析构（BufferedInput 生命周期结束）时触发。
  - `velox/dwio/common/FileCacheBufferedInput.h:85-100`、`velox/common/caching/filecache/FileSegment.cpp:1297+`
- **影响**：后台 tail-fill、shrink、空间释放、优先级推进时机推迟；长生命周期输入延迟缓存收尾、占用更久。
- **判定**：待人确认（BufferedInput 模型的合理重构，但偏离 CH 时序不变量）。

### F12 — origin user 每进程随机（LOW，核心层语义）

- **CH**：用配置 user 或 `ServerUUID`，跨重启稳定。
  - `src/Interpreters/FileCache/FileCache.cpp:112-116`、`309-312`
- **Velox**：用随机 `FileCacheKey`，每进程随机。
  - `velox/common/caching/filecache/FileCache.cpp:66-68`（`getCommonUserID`）、`373-376`
- **影响**：per-user 目录/归属每进程变化；若依赖按 user 划分目录则跨重启不忠实。
- **判定**：待人确认。

### F13 — write 打开 flags 丢失（LOW，集成层等价）

- **CH**：续写时 `O_WRONLY|O_APPEND|O_CLOEXEC`。
  - `src/Interpreters/FileCache/FileSegment.cpp:431-434`
- **Velox**：计算 flags 后 `unused(flags)`；`LocalWriteFile(path,false,false)` → `O_WRONLY|O_CREAT`（`bufferIo` 默认 true 无 `O_DIRECT`）+ `lseek(SEEK_END)` 续写不截断。
  - `velox/common/caching/filecache/FileSegment.cpp:426-432`、`velox/common/file/File.cpp`（`LocalWriteFile` open）
- **影响**：功能正确（续写、不截断）；仅损失 append 原子性与 close-on-exec。单 downloader 顺序写无实际问题。
- **判定**：等价适配（纯语义弱化）。

### F14 — 删文件后 `OpenedFileCache` fd 清理未移植（LOW，核心层，当前无风险）

- **CH**：删段文件后清除 `OpenedFileCache` 中该 path 的 fd 缓存（含 O_DIRECT）。
  - `src/Interpreters/FileCache/Metadata.cpp:1176-1180`
- **Velox**：仅 TODO 注释，未实现；但读路径每次 `new LocalReadFile`，无 fd 缓存，故无实际风险。
  - `velox/common/caching/filecache/Metadata.cpp:1155-1156`、`velox/dwio/common/FileCacheInputStream.cpp:105`
- **判定**：当前安全；若将来引入 fd 复用缓存需补齐。

### F15 — 可观测性成片缺失（LOW，全局，非功能）

- **CH**：各文件 `ProfileEvents`/`CurrentMetrics`/`fiu_do_on`（如 `Guards.h:88-97`、`EvictionCandidates.cpp:295-297`、`Metadata.cpp` 多处）。
- **Velox**：对应位置均 `// TODO(metric)` / `// TODO(failpoint)` 占位（如 `LRUFileCachePriority.cpp:42/48/60/67`、`EvictionCandidates.cpp:286/327/356/372`、`Guards.h:95-102/121-128`）。
- **影响**：监控、容量诊断、队列积压排查、测试故障注入能力下降；**不影响核心算法账目**。
- **判定**：功能缺失（非 bug）。

---

## 4. 已核验为「一致」/ 剔除的假阳性

> 由独立代理逐行核验后剔除，避免误报：

- **优先级/淘汰核心**：`LRUFileCachePriority::canFit` 两边逐字一致（CH `LRUFileCachePriority.cpp:343-356` ↔ Velox 同名函数）；LRU/SLRU/Split/EvictionCandidates 队列移动、候选账目、边界比较未发现可证实的算法不一致。
- **后台下载未显式 reset reader/downloader**：Velox `downloadImpl` 末尾虽未像 CH（`Metadata.cpp:934-935`）显式调用，但其后 `completeAndPopFront → complete()` 已在 `FileSegment.cpp:1195,1203-1204` reset `remoteFileReader` 与 `downloaderId`；remote reader 跨轮复用是 Velox 有意设计（`FileSegment.cpp:477-484` 内部 `seekp` 对齐写偏移）。**候选不成立**。
- **`WriteBufferFromFile` 截断风险**：`LocalWriteFile` 默认 `bufferIo=true`（无 O_DIRECT）、`O_WRONLY|O_CREAT` 无 `O_TRUNC`、`lseek` 到末尾续写，**不截断**，续传语义正确。仅降级为 F13。
- **错误码语义合并**（`BAD_ARGUMENTS`/`LOGICAL_ERROR`/`ACCESS_DENIED` → `VELOX_FAIL`）：属 Velox 适配层正常折叠，非算法不一致。

**三方都确认逐行/逐项忠实（无差异）**：`FileSegmentInfo.h` 枚举/结构体、`FileSegment` 状态机全部转换（`getOrSetDownloader`/`resetDownloadingStateUnlocked`/`complete`/`shrink`/`setDetachedState`/`assertCorrectnessUnlocked`）、`FileCache::tryReserve/doTryReserve`（含 HoldSpace/EvictionCandidates 提交回滚、`catch(...)` 僵尸条目 invalidate 保护、锁顺序 `cacheStateGuard → cacheGuard.writeLock`）、LRU/SLRU 全部核心算法（淘汰顺序、protected/probationary 升降级、size_ratio、计数对称）、`FileCacheUtils::roundUp/roundDown` 数学一致、所有默认常量（32MiB 段、4MiB 对齐、SLRU 0.6、max_elements 1e7 等）、`range()` 闭区间语义（`right` inclusive，`+1` 转开区间无 off-by-one）、`downloadFromReader` 的 reserve-先于-consume 不变量（`FileSegment.cpp:496→512`，无丢字节风险）。

---

## 5. 评估与优先级

### 忠实度评分

| 来源 | 评分 |
|---|---|
| GPT-5.5 | 82 / 100 |
| Claude-detailed | 核心 9.5/10，集成 8/10 |
| 独立 | 8/10（算法 8.5，实现 7.5） |

三方一致：**核心状态机/tryReserve/LRU-SLRU 淘汰/Metadata 锁顺序/range 闭区间近乎逐行忠实；未发现 CRITICAL、无数据损坏、无死锁、无 off-by-one、无整数回绕。**

### 优先级

按「核心层应忠实、集成层有意改写」原则区分：

**应修的核心层 bug（核心层却偏离 → 最可疑）：**
1. **F2 对齐除零** — 核心 `FileCache.cpp`，一行修复（`bits::roundUp` 改回带 0 保护的 `roundUpToMultiple`）。
2. **F3 write 失败 ENOSPC 处理** — 核心 `FileSegment.cpp`，补 `downloaded_size` 校正与空文件清理。

**需拍板的集成层改写后果（spec §7 已声明读路径重写）：**
3. **F1 无 bypass 降级** — 「盘满/IO 错 → 查询硬失败」是可用性回归，是否补 bypass 远端读由产品决定。其余 F9/F10/F11 同属读路径重写的衍生差异。

**已知 NYI（显式未移植，非静默）：** F4 query 配额、F5 free-space scheduler — 默认不触发，配置开启即显式抛错。

**等价适配 / 非功能（非 bug）：** F7 哈希（已文档化）、F8 callerId、F12 user、F13 flags、F14 fd 清理、F15 可观测性。

### 未完全覆盖（建议如需 100% 置信再补一轮逐行核验）

写路径 `CachedObjectStorage`/`CachedOnDiskWriteBufferFromFile`/`WriteBufferToFileSegment`、`FileCacheFactory`、`FileCacheSettings` 全量字段映射、`SLRUFileCachePriority` 升降级账目细节、`FileCache.cpp` 中 `getOrSet`/`set`/`doEviction`/动态 resize 的剩余约 2000 行 —— 三方报告均为「忠实/等价」，未发现 CRITICAL。
