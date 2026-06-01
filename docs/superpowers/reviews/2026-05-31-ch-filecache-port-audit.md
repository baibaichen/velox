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
| F2 | `boundaryAlignment==0` 时 `roundUp` 除零崩溃（丢 0 保护） | **核心** | – | – | H | **确认（真坑）** | **HIGH** ✅ 已修复 |
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
| F16 | 刚下载的字节一律读回缓存文件，未复用内存 buffer（CH 复用 working_buffer） | 集成 | – | – | – | **后补确认** | **MEDIUM** ⚠ 原审计漏报 |
| F17 | 无 `canStartFromCache` 流式前缀快路：非下载方读卡满整段 needed 而非边写边读 | 集成 | – | – | – | **数据流再审** | **MEDIUM** |
| F18 | 供数前对整 slice 单次 pread（CH 按 buffer 块边读边给） | 集成 | – | – | – | **数据流再审** | LOW |
| F19 | DETACHED bypass 在 load() 线程上 eager 把整 region 读进 RAM（CH 逐 buffer 流式） | 集成 | – | – | – | **数据流再审** | **MEDIUM** |
| F20 | 新段首写不带 `O_TRUNC`（lseek SEEK_END 续写）；缺 CH 的截断防御 | 集成 | – | – | – | **数据流再审** | LOW |
| F21 | `LocalWriteFile::append` 单次 `::write`，无 EINTR/部分写重试循环 | 集成 | – | – | – | **数据流再审** | LOW |

※ F7 虽两方提及，但代码已注释说明为有意替换，实质为「等价适配/已文档化」，非 bug。

※ F16 为审计后补（2026-06-01，经冷写性能调查发现）：三个审计代理均未提及，详见 §3 F16 与 §6「为何漏报」。

※ F17–F21 为 2026-06-01「数据流再审」新发现（因 F16 漏报触发，专查数据搬运路径，用「追字节：从哪来→到哪去→拷贝几次→同步/异步→与 CH 同路径逐跳对齐」方法学）。F20/F21 经作者复核代码后由 MEDIUM 下调为 LOW（详见 §3）。同批对 reserve/淘汰/SLRU 账目流做了第二遍逐跳核验，**确认账目完全守恒、忠实**，无新发现（正向印证原审计核心层「逐行忠实」结论）。

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

### F2 — `boundaryAlignment==0` 除零崩溃（HIGH，核心层，作者已逐行复核确认）✅ 已修复

> **已修复**：`FileCache.cpp:890-891` 的 `bits::roundUp` 改回带 0 保护的
> `FileCacheUtils::roundUpToMultiple`（复用现有、已单测、忠于 CH 的工具函数），
> 并移除随之未使用的 `#include "velox/common/base/BitUtil.h"`。
> 新增回归测试 `FileCacheBufferedInputTest.zeroBoundaryAlignmentDoesNotDivideByZero`
> 揭示该坑（改前 `SIGFPE`/exit 136，改后 74/74 通过）。

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

### F7 — `FileCacheKey` 哈希不同（LOW，已文档化）（影响更正 2026-05-31）

- **CH**：`fromPath()` 用 `sipHash128(path.data(), path.size())`（固定零 key）。
  - `src/Interpreters/FileCache/FileCacheKey.cpp:31-34`
- **Velox**：`fromPath()` 用 `folly::hash::SpookyHashV2::Hash128`（固定种子 `0,0`）。
  - `velox/common/caching/filecache/FileCacheKey.cpp:66-75`
- **确定性（关键更正）**：SipHash128 与 SpookyHashV2 在固定 key/seed 下**都是确定性哈希** ——
  同一路径、同一二进制每次产出同一 128-bit key，**跨进程重启稳定可复用**。配合
  `FileCache::initialize()`→`loadMetadata()` 重扫缓存目录（`FileCache.cpp:449,500`），
  重启后即可复用已落盘段（前提：不 wipe 目录、文件路径不变）。
  - ⚠️ `FileCacheKey.cpp` 原注释「cache directory is NOT reusable across builds/restarts」
    **措辞过宽**：跨「重启」实际可复用；仅跨「folly 改了 SpookyHashV2 实现的构建」或
    「与真实 ClickHouse 互通」才不可用。该注释将在 direct-read baseline 工作中修正。
- **影响（更正后）**：唯一真实限制是**算法不同（SpookyHashV2 ≠ SipHash128）→ 缓存目录无法与
  真实 ClickHouse 互通**；进程内 / 跨重启复用不受影响。抗 DoS / 加密强度对缓存身份无实际意义
  （key 源自可信文件路径，非对手可控输入）。
- **判定**：等价适配（已文档化）；仅当目标为「缓存目录与 CH 互通」时才需移植 SipHash128
  （见 HANDOFF §4 siphash-port TODO）。

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

### F16 — 刚下载的字节一律读回缓存文件，未复用内存 buffer（MEDIUM，集成层，审计后补）⚠ 原审计漏报

- **CH**：下载路径从远端把字节读进 `state.buf`（working buffer），`writeCache(state.buf...)` 同步写盘后，**把同一个 `state.buf` 直接作为 `working_buffer` 返回给消费者**——刚下载的字节从内存供数，**不重新 pread 缓存文件**；只有后续命中（其他 reader、字节已在盘）才走 `cache_file_reader` 读文件。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:1287-1303`（`writeCache(state.buf->buffer().begin(), size, offset, ...)` 后复用 `state.buf`，注释明示 "Later reads for this file segment can reuse it"）
  - `:1124,1151`（`state.buf->set(internal_buffer...)` / `working_buffer = Buffer(internal_buffer...)`，working_buffer 即源读 buffer）
  - `writeCache → file_segment.write`（`:971`）同步，但供数字节取自内存 buffer，非回读
- **Velox**：下载与供数完全解耦。`downloadFromReader` 把每 1MB chunk 读进局部 `scratch`、`write` 进缓存文件后**丢弃 scratch**；消费者侧 `FileCacheInputStream::loadSegment` **无论命中还是刚下载，一律**打开缓存文件 `preadv` 读回。
  - `velox/common/caching/filecache/FileSegment.cpp:497-530`（`scratch` 局部变量，循环用完即弃）
  - `velox/dwio/common/FileCacheInputStream.cpp:116-159`（先 `while(getDownloadedSize()<needed)` 等写落盘，再 `LocalReadFile(...).preadv(...)` 读回）
- **差异/影响**：
  1. **每字节多一次拷贝**：源→scratch→`::write`(page cache)→`preadv`→segData_，比 CH 多一趟 page-cache→用户态 memcpy（冷读 20GB 即多一遍全量拷贝）。
  2. **下载方 reader 被耦合到写**：velox 消费者卡在 `getDownloadedSize()`（写落盘后才推进）→ 才能 pread；CH 下载方从 working_buffer 取字节，与回读无关。这是冷写关键路径上"写挡读"的结构性来源（详见 `docs`/session 冷写性能调查）。
  3. 数据**不损坏**、语义正确——纯性能/架构偏差。
- **判定**：待人确认。属集成读路径重写的衍生差异，但 spec **未将其列为已知偏差**（§7 只列 coalesceIo / loadQuantum）。修复方向 = serve-from-memory（回归 CH，Option 1）+ 可选异步写（超出 CH，Option 2）。
- **状态（2026-06-01）：已解决 ✅（引用语义）**。`FileSegment::downloadFromReader` 新增可选 `folly::Range<char*> out` 输出 buffer：窗口那段 chunk 直接 `readBytes` 进消费者复用 buffer（`ReadFileByteInputStream` 的 readBytes 就是一次定位 pread，等价 CH "远端→working_buffer"），并从同一份字节写盘——**零额外拷贝**，对齐 CH 把写好的 working_buffer 直接交回。`FileCacheInputStream::fillBuffer` 在下载前沿（CASE B）先把前缀 gap 下进 throwaway scratch 让前沿对齐窗口起点，再把窗口直读进 `buf_` 供数；竞态败者（窗口被他人写过）回退到从缓存文件 pread。引用语义仅在**同步写**下成立（字节落盘后才交出 `buf_`，单块复用 buffer 不会在写盘未完成时被覆盖）；未来异步写需池化 + 引用计数管理 buffer。冷读保持 `ssdRead==0`（从 `buf_` 内存供数）。提交 `8e3f3c800`（流式 core 雏形）、`04499e449`（集成流式）、引用语义重构（本次提交）。

### F17 — 无 `canStartFromCache` 流式前缀快路径（MEDIUM，集成层，数据流再审）

- **CH**：非下载方 reader 命中正在下载/部分下载的段时，只要 `current_write_offset > current_offset` 即 `canStartFromCache` 为真，**立即从缓存文件读已写入的前缀**，不等整段下载完；若暂不可读则 `file_segment.wait(offset)` 在写偏移越过 `offset`（FileSegment chunk 粒度）即返回，边写边读流式推进。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:375-386`（`canStartFromCache`）、`:445-478`（DOWNLOADING/PARTIALLY_DOWNLOADED 走 `ReadType::CACHED`）、`:909-923`（追上写偏移后切远端）
- **Velox**：`FileCacheInputStream::loadSegment` 卡在 `while(getDownloadedSize() < needed)`，`needed = segmentOffset + length` 是**本 stream 整个 slice 的末端**，落盘后才一次性读；`downloadSegmentPrefix` 输给下载方的 reader 同样 `wait(targetEnd-1)` 等满整段 coalesced 目标。**全港无 `canStartFromCache` 等价物**。
  - `velox/dwio/common/FileCacheInputStream.cpp:113,116-142`（`needed=segmentOffset+length`；阻塞等满）、`velox/dwio/common/FileCacheBufferedInput.cpp:182-200`（`downloadSegmentPrefix` 等满）
- **差异/影响**：场景（b）部分命中、（h）第二并发 reader。CH 第二 reader 在写偏移越过其位置的瞬间即开始消费盘上前缀，与下载流水线重叠；Velox 第二 reader 被完全串行化——阻塞到整段 needed 落盘后才 pread。首字节延迟、并发读吞吐回退；**数据不损坏**。与 F16 不同角度：F16 是*下载方*回读自己刚下的字节，F17 是*非下载方*丢了流式前缀快路。
- **判定**：集成层下载/供数解耦的衍生差异，待人确认。

### F18 — 供数前对整 slice 单次 pread（LOW，集成层，数据流再审）

- **CH**：`nextImplStep` 把 `internal_buffer` 调成 `min(local_fs_buffer_size, remaining)`，读一个 buffer 即把 `working_buffer` 交给消费者，余量后续 `nextImplStep` 再流式给。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:1095-1110,1148-1151,1243`
- **Velox**：`loadSegment` 对整个 slice（`length`）单次 `preadv` 后 `Next()` 才能给出第 0 字节；整 slice 在 stream 生命周期内常驻 `segData_`。
  - `velox/dwio/common/FileCacheInputStream.cpp:152-159,192-207`
- **差异/影响**：多 MB 段首字节延迟升至整 slice 读、峰值常驻内存为整 slice；消费者只读前缀后 seek 走也已读全。纯性能/内存。**与已接受的 loadQuantum 偏差部分重叠**（此处是已定读内的供数/读取粒度，非预取大小），故记 LOW 备查。
- **判定**：集成层重写衍生，LOW。

### F19 — DETACHED bypass 在 load() 线程上 eager 整 region 读进 RAM（MEDIUM，集成层，数据流再审）

- **CH**：`REMOTE_FS_READ_BYPASS_CACHE` 经 `working_buffer` 逐 buffer 流式拉取，整 region 从不常驻内存。
  - `src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:350-364,1243`
- **Velox**：`FileCacheBufferedInput::load` 命中 bypass 时 `bypassBuffer.resize(region.length)` + 对**整 region 单次同步 `pread`，且在 load() 线程上、消费者拉取前**完成；之后 `DeferredStream::Next` 从这块全常驻 buffer 切片供数。
  - `velox/dwio/common/FileCacheBufferedInput.cpp:330-343`、`:41-56`
- **差异/影响**：场景（d）。峰值内存=每 stream 整个 bypass region（非一个 buffer），且即使消费者只读一部分也整段预取。属已知 F10/F11（region 级 bypass）经端到端追踪暴露的**内存足迹/无谓预取**新角度，非独立 bug。
- **判定**：F10 region 级 bypass 设计的衍生后果，MEDIUM（内存足迹）。

### F20 — 新段首写不带 `O_TRUNC`（LOW，集成层，数据流再审）

- **CH**：首写（`downloaded_size==0`）以截断方式打开陈旧文件，保证从零开始（是对 F3 残留半成品文件的兜底防御）。
- **Velox**：`WriteBufferFromFile` 用 `LocalWriteFile(path, false, false)` → `O_WRONLY|O_CREAT` + `lseek(SEEK_END)` 续写，计算出的 `flags` 被 `unused(flags)` 丢弃，**无 `O_TRUNC`**。
  - `velox/common/caching/filecache/FileSegment.cpp:47-48`、`velox/common/file/File.cpp:345-383,395-406`
- **差异/影响**：超出 F13（仅枚举 `O_APPEND`/`O_CLOEXEC`）的一条*正确性*防御。**复核后下调为 LOW**：正常运行下淘汰即 `fs::remove(path)`、且 `downloadedSize==0` 时断言文件不存在（`Metadata.cpp:1148-1153,1150-1151`），启动 `loadMetadata` 又以 `file_size()` 为准对账，故陈旧文件基本被前置清理；仅当 F3 半写失败残留、或崩溃后陈旧文件在对账前路径被复用时，`lseek SEEK_END` 续写会信任陈旧字节。条件性、概率低。
- **判定**：纵深防御缺口，LOW，依赖 F3/崩溃场景。

### F21 — `LocalWriteFile::append` 单次 `::write`、无 EINTR/部分写重试（LOW，集成层，数据流再审）

- **CH**：`WriteBufferFromFileDescriptor::nextImpl` 用 `::write` 循环，重试 `EINTR`、累加短写。
- **Velox**：`LocalWriteFile::append` 单次 `::write` + `VELOX_CHECK_EQ`，把可恢复瞬态（`EINTR`/部分写）变为永久段失败。
  - `velox/common/file/File.cpp:395-406`
- **差异/影响**：机制不同于 F3（F3 是抛后 ENOSPC 修复）。**复核后下调为 LOW**：Linux 上对普通文件的缓冲 `::write` 几乎不被信号中断（EINTR 主要见于管道/终端/套接字等慢设备），普通文件短写主要源于 ENOSPC——而 ENOSPC 属 F3 范畴。故实际触发概率低。
- **判定**：忠实度偏差，LOW，与 F3 部分重叠。

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
1. **F2 对齐除零** ✅ 已修复 — 核心 `FileCache.cpp`，一行修复（`bits::roundUp` 改回带 0 保护的 `roundUpToMultiple`），并补回归测试。
2. **F3 write 失败 ENOSPC 处理** — 核心 `FileSegment.cpp`，补 `downloaded_size` 校正与空文件清理。

**需拍板的集成层改写后果（spec §7 已声明读路径重写）：**
3. **F1 无 bypass 降级** — 「盘满/IO 错 → 查询硬失败」是可用性回归，是否补 bypass 远端读由产品决定。其余 F9/F10/F11 同属读路径重写的衍生差异。
4. **F16 刚下载字节读回文件（审计后补）** — serve-from-memory 偏离 CH，导致每字节多一趟 memcpy + 下载方读被耦合到写落盘。是冷写/冷读性能差距的结构性来源。修复 = 回归 CH 的 working_buffer 复用（Option 1），可叠加异步写（Option 2，超出 CH）。
5. **F17 无流式前缀快路（数据流再审）** — 非下载方第二 reader 卡满整段 needed 才读。建议与 F16/Option 1 的 serve-from-memory 改造一并评估（同属"下载/供数解耦"代价）。
6. **F19 bypass eager 整 region 入 RAM（数据流再审）** — 内存足迹随 region 线性增长，F10/F11 的衍生后果，是否改逐 buffer 流式由产品/内存预算决定。

**低风险 / 纵深防御 / 性能微调（数据流再审，复核后 LOW）：** F18 整 slice 单次 pread（与 loadQuantum 重叠）、F20 新段首写缺 `O_TRUNC`（淘汰即 `fs::remove`+启动对账已前置兜底，仅 F3/崩溃残留时才咬）、F21 `append` 无 EINTR/部分写重试（普通文件缓冲写极少 EINTR，短写归 F3）。

**已知 NYI（显式未移植，非静默）：** F4 query 配额、F5 free-space scheduler — 默认不触发，配置开启即显式抛错。

**等价适配 / 非功能（非 bug）：** F7 哈希（已文档化）、F8 callerId、F12 user、F13 flags、F14 fd 清理、F15 可观测性。

### 为何原审计漏报 F16 / F17–F21（流程教训）

1. **偏差被 spec 设计进去 → 审计继承了 spec 的盲点**：spec §6.2 明写「cache-hit 读…保持现状（`FileCacheInputStream.cpp:91-94` 一律 pread）」，把"下载只写文件"与"消费者一律读回文件"当成两件独立的事，**从未建模 CH 的 `working_buffer` 复用**。审计以「实现 vs spec + 实现 vs CH 正确性」为基准，既然 spec 声明 pread 有意，这条就没进偏差清单。§7「已知差异」只列 coalesceIo / loadQuantum，未列 serve-from-memory。
2. **审计为正确性/严重度视角**：F16 是「正确但更慢 + 多一次拷贝」，不丢数据、不死锁、不硬失败，落在 CRITICAL/HIGH 雷达之外。
3. **`downloadFromReader` 只被当「写路径」核对**：§4 还把它认证为「逐行忠实」（reserve-先于-consume 不变量）。三方都未回答「消费者从哪拿刚下载的字节」，也就没和 CH `nextImplStep` 返回 `working_buffer` 做对照。
4. **教训**：跨实现移植审计除「逐函数对照」外，应补一条「**端到端数据流对照**」——对每条读/写路径，追问「字节从哪来、到哪去、中间拷贝几次」，并与参照实现的同一路径逐跳对齐，而非只比单个函数。

### 未完全覆盖（建议如需 100% 置信再补一轮逐行核验）

2026-06-01 数据流再审已补齐：**写数据流**（FileSegment::write / WriteBufferFromFile / downloadFromReader，R1）与 **reserve/淘汰/SLRU 账目流**（R3）经第二遍逐跳核验——写路径除 F20/F21 外**无额外拷贝、忠实**，账目**完全守恒**。

仍未逐行覆盖：`CachedObjectStorage` 写直通、`FileCacheFactory`、`FileCacheSettings` 全量字段映射、`FileCache.cpp` 中 `getOrSet`/`set`/动态 resize 的剩余约 2000 行 —— 三方报告均为「忠实/等价」，未发现 CRITICAL。
