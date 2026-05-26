# FsCache CH-Aligned Redesign — Design

**Status**: Drafted 2026-05-26
**Supersedes**: `2026-05-25-fscache-phase2-superseded.md` (whole document)
**Scope**: 完整对齐 ClickHouse `FileCache` 的语义，覆盖六类工作 ——
(1) metadata 区间查找 + `fillHolesWithEmptyFileSegments`，
(2) `FileSegment` 6 态状态机 + partial-readable 边写边读，
(3) `FsCache::getOrSet` 返回 `FileSegmentsHolderPtr` + caller-driven 推进 + stream 层重写，
(4) per-bucket + per-key 锁实例化 + atomic stats，
(5) `DownloadThreadPool` + R0 async load，
(6) SLRU + `FileCacheQueryLimit` + `bypass_cache_threshold`。

**Not in scope**: phase-3 cross-backend microbenchmark (`FsCache` vs
`AsyncDataCache+SsdCache`)、Userspace Page Cache RAM 层 (phase 4)、
S3/HTTP 真实远端 backend。这些落在后续 spec。

---

## 0. 本 spec 解决什么

Phase-1 落地的 `FsCache` 在三个维度上偏离了 ClickHouse 设计，已知问题：

1. **metadata 点查 vs 区间查的设计裂缝**（已复现 bug）：`FsCacheMetadata::lookup`
   只用 `key.offset` 查 `map<uint64_t, FileSegmentPtr>`，**忽略 `key.size`**。
   但 `FsCache::splitRange` 会按请求形状产生 **可变长度 segment**（commit
   `6ff3eb40c`：`Internal cuts ... may have sub-alignment tail size (matches
   ClickHouse)`）。同一 offset 早期小请求生成 4 MiB segment，后续大请求
   查表拿到 4 MiB 段以为覆盖全区间，触发 `FsCacheInputStream::Next`
   "Reading past end" 异常。已用 dwio TPC-H q1 复现，详见 §1.1。

2. **同步 all-or-nothing 下载 vs CH partial-readable**：`FileSegment::download`
   一次性写完 `.tmp` 再 rename，下载中 reader 必须等整段完成。CH 用
   `PARTIALLY_DOWNLOADED` 状态 + byte-level cv 通知，writer 每写一段就
   notify，reader 等到自己需要的字节数即可继续。phase-1 commit `6ff3eb40c`
   message 已承认"3 active states vs ClickHouse's 6"，"the partial-download
   progression has no consumer yet; revisits in phase 2"。

3. **16 线程 hit-path 退化到单线程的 0.3×**：phase-1 baseline
   (`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`)
   ws_mult=0.5 全命中 workload 上 16 线程吞吐从 7.6 M ops/s 退化到 2.1
   M ops/s。根因：metadata / priority / state 三把全局锁串行。

本 spec 通过 (a) 把 metadata 升级到 CH 区间查找 + fillHoles 语义，
(b) 引入 6 态状态机 + partial-readable cv，(c) `getOrSet` 改返回
`FileSegmentsHolderPtr` 让 caller 驱动推进，(d) per-bucket / per-key 锁
实例化 + atomic stats，(e) `DownloadThreadPool` 让 `load()` 异步派发，
(f) SLRU + QueryLimit + bypass —— 把三个维度的差距一次性补齐。

### 0.1 为什么六类必须一起做（不能拆 phase）

CH 的 partial-readable 语义、caller-driven holder、区间查找三者构成**不可
拆分的接口契约**：

- holder 里允许 `EMPTY / DOWNLOADING / DOWNLOADED` 三态混合 ⇒
  `getOrSet` 签名必须改为 `FileSegmentsHolderPtr`（API 硬切）。
- 区间查找返回的 segment 列表可能不连续 ⇒ 必须 `fillHolesWithEmptyFileSegments`
  补成连续段。
- `EMPTY` 段被 caller 推到 `DOWNLOADING` ⇒ 必须 caller-driven
  `reserve/write/complete` 三段式。
- partial-readable ⇒ 需要 byte-level `cv_` 而非 segment-level。
- caller-driven + async ⇒ `DownloadThreadPool` 是必然伴生项。

把 (1)(2)(3) 拆开做意味着接口要改两次（先点查改区间查、再 holder 化），
违反 "no backwards-compat hacks" 原则（CLAUDE.md）。一次性硬切是
最小成本路径。

### 0.2 与 phase-1 现有 commits 的关系

Q2.A 决策：**新增 commit 累加**，不 rebase 重写 phase-1 历史。phase-1 共
74 commits 全部保留，CH 对齐工作以新 commits 形式追加在分支末端
(`fscache-clickhouse-style` 当前 HEAD 是 `8bf7d1d74`)。phase-1 commit
`6ff3eb40c` 已在 message 里把"6 态状态机"、"partial-download progression"
列为延后工作，本 spec 是这些延后工作的兑现。

---

## 1. 背景与触发点

### 1.1 metadata 设计裂缝的复现路径

dwio TPC-H q1 在 fscache 模式下抛 `VeloxRuntimeError: Reading past end of
FsCacheInputStream`。LOG 探针确认：

- `FsCache::getOrSet(path, 45'862'771, 20'111'191)` 期待返回覆盖
  `[45'862'771, 65'973'962)` 的 segment 列表。
- 但 metadata 里同 `pathKey` 早先已写入一个 `(offset=41'943'040, size=4'194'304)`
  的 segment（来自前一次较小请求）。
- 新请求 splitRange 出来的第一段 (offset=41'943'040, ...) 调
  `metadata_->lookup`，命中那个 **4 MiB** 段，但本次期望 size 远不止 4 MiB。
- `FsCacheInputStream` 拿着 4 MiB 段去读 20 MiB region，读到段尾抛
  "Reading past end"。

phase-1 metadata 的 `map<uint64_t offset, FileSegmentPtr>` 数据结构
**前提条件**是"每个 offset 至多一个 segment"。`splitRange` 不保证这一点
（ClickHouse 也不保证）。CH 的对应做法：metadata 用 `map<size_t offset,
FileSegmentMetadataPtr>` 但 `lookup` 是**区间扫描** —— `lower_bound(range.left)`
配合 `prev` 检查相邻段相交，再用 `fillHolesWithEmptyFileSegments` 把
未覆盖区间补成 EMPTY 段。详见 `/tmp/ch_filecache_api_extract.md` §A.1-A.4
（subagent 抽取的 verbatim CH 源码，引自 CH `master` 分支
`src/Interpreters/FileCache/FileCache.cpp`）。

### 1.2 phase-1 baseline 性能瓶颈

`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` 显示：

| 配置 | 1 线程 ops/s | 16 线程 ops/s | 扩展系数 |
|---|---:|---:|---:|
| sequential, ws_mult=0.5, lat=0 | 7.6 M | 2.1 M | 0.28× |
| zipfian, ws_mult=0.5, lat=0 | 7.4 M | 2.3 M | 0.31× |
| uniform, ws_mult=0.5, lat=0 | 7.5 M | 2.2 M | 0.29× |

ws_mult=0.5 + lat=0 是**全命中纯 hit-path**，理想扩展系数应 ≥ 0.8。
当前 0.28× 证实串行锁瓶颈。根因诊断：`metadata_->lookup` 串
`CacheMetadataMutex`（一把全局）；`recordHit` 串 `priorityMutex_` (LRU `splice`)
+ `stateMutex_` (`++hits`)。

### 1.3 dwio prefetch 失效

`FsCacheBufferedInput::load()` 当前同步串行下载所有 enqueue 的区间，
违反 `BufferedInput::load(LogType)` 的 async-prefetch 语义（CBI 走
`AsyncDataCache` 的 prefetch 路径）。上游所有并行预取
（`TableScan::preload` / Parquet `scheduleRowGroups` / DWRF）退化为
前台 demand fetch。

---

## 2. 范围与非范围

### In scope

- **metadata 区间查找**：`FsCacheMetadata` 新 API `lookupRange(path, [lo,
  hi])`，CH 风格 `lower_bound + prev` 相交扫描。
- **`fillHolesWithEmptyFileSegments`**：把相交段列表补成覆盖整个请求区间
  的连续段序列；hole 按 `maxSegmentSize` 切多段。
- **6 态状态机**：`FileSegment::State` 扩到 `EMPTY / DOWNLOADING /
  DOWNLOADED / PARTIALLY_DOWNLOADED / PARTIALLY_DOWNLOADED_NO_CONTINUATION
  / DETACHED`。
- **partial-readable**：`FileSegment::cv_` 升级为 byte-level 通知；
  reader 等到自己需要的 `downloadedSize` 即唤醒。
- **reserve/write/complete API**：`FileSegment::download` 拆三段；on-disk
  原地写（**非** `.tmp+rename`），落盘时按 chunk fsync。
- **`getOrSet` 硬切签名**：返回 `FileSegmentsHolderPtr`，调用方
  （`FsCacheBufferedInput` / `FsCacheInputStream` / 所有现有 phase-1
  测试）同 commit chain 改完。
- **caller-driven holder 推进**：`FsCacheBufferedInput::load()` 拿
  holder 后，对每段 EMPTY 自己调 `getOrSetDownloader → reserve → 读 remote
  → write → complete`；对 DOWNLOADING 调 `wait(needed_bytes)`。
- **per-bucket / per-key 锁实例化**：phase-1 已把类型名按 CH 拓扑建好
  (5 把锁)，本 spec 把**实例化粒度**从 whole-cache 切到 per-bucket +
  per-key。`KeyMetadata` 引入 `LockedKey` RAII（phase-1 已有原型，本
  spec 跟 CH `Metadata::lockKeyMetadata` 行为对齐，含 `KeyNotFoundPolicy`
  4 态枚举）。
- **atomic stats + try_lock LRU bump**：`hits/misses/evictions/bytesOnDisk`
  全部 `std::atomic` 操作；`recordHit` 用 `try_to_lock` 拿 LRU 锁，
  失败即跳过 LRU 提升（best-effort）。
- **`DownloadThreadPool` + async load**：`load()` 把每段 EMPTY 投递到
  下载池，`FsCacheBufferedInput::load()` 立即返回；reader 第一次 `Next()`
  时 wait 对应 segment 的 `downloadedSize` 达到自己需要的字节。
- **SLRU 淘汰**：`EvictionPolicy` 实例追加 `SlruPolicy`（probation +
  protected list，promote on second hit）。
- **`FileCacheQueryLimit`**：per-query bytes quota；超出后 query 内续
  miss 走 bypass 或返回部分数据（具体策略见 §8）。
- **`bypass_cache_threshold`**：单次请求 size ≥ 阈值时跳过 cache，直接
  从 remote 读取。

### Out of scope (硬约束)

- **跨 backend microbenchmark**（FsCache vs AsyncDataCache+SsdCache）：
  phase-3 独立 spec。当前 microbench 只测 FsCache self-cost。
- **Userspace Page Cache RAM 层**：phase-4。
- **S3/HTTP remote backend**：使用 `LocalReadFile` 模拟远端；真实云端
  在后续 spec。
- **on-disk crash recovery 完整设计**：partial 文件 crash 后丢失即可
  （回到 EMPTY 重下），不做续传级 recovery。`loadFromDisk` 保持
  phase-1 行为：保留 size-match 文件、删 size-mismatch + .tmp。partial
  文件用文件名 schema 中的 size 字段 + 实际 stat size 区分（**详见 §5.5**）。
- **6 态状态机的所有 wait 中断 / cancellation**：phase-1 没有
  cancellation，本 spec 也不加。
- **Crash recovery 重启后 PARTIALLY_DOWNLOADED 续传**：partial 文件
  crash 即丢，不续传。

---

## 3. 目标与非目标

### 量化目标

- **正确性**：dwio TPC-H q1-q22 全部 fscache 模式通过；
  `FsCacheEquivalenceTest` 对所有 chunk size 字节级一致。
- **接口契约**：`getOrSet` 返回的 holder 里 segment 按 offset 排序、
  连续无洞、并 caller 推进后全部 `DOWNLOADED` 或在 `DOWNLOADING` 中。
- **性能**：
  - ws_mult=0.5 全命中 16 线程扩展系数 ≥ 0.80×（phase-1 baseline 0.28×）。
  - dwio TPC-H prefetch 路径不应被 miss 主导。**接口契约**：
    `FsCacheStats` 拆 4 个 atomic 字段
    `prefetchHits / prefetchMisses / demandHits / demandMisses`；
    每次 `FsCache::getOrSet` 调用方传入 `IsPrefetch` 枚举
    `kPrefetch | kDemand` 决定计入哪对计数器（计数发生在 getOrSet
    内、lookupRange + fillHoles 之后；lookupRange 自身不接收
    IsPrefetch，见 §5.2 / §6.3）。UT 在
    `FsCacheStatsTest.cpp` 精确覆盖 4 个字段递增（§9.1）。
  - **E2E 验证**：`FsCacheBufferedInputTest::prefetchRatio` 用 mock
    workload（先 enqueue 多个 region 触发 prefetch，再随机 read 触发
    demand）验证 `prefetchMisses / (prefetchMisses + demandMisses)
    ≥ 0.80`（§9.2）。TPC-H 端到端只用来旁证趋势，不作硬 gate（workload
    本身可能 prefetch-friendly 程度不同）。
- **测试覆盖**：每个新公共 API 至少 1 个 unit test；新增 prod 文件 ≥
  对应 test 文件；3 层 UT 全部落地（§9）。

### 非目标

- 性能不退化 phase-1 单线程 hit-path（≥ 7.0 M ops/s）。**不要求**单线程
  ops/s 提升（CH 对齐的复杂度可能轻微抬升单线程 overhead，可接受
  10% 内退化）。
- 不要求 16 线程超过 CH 实测扩展系数（CH 自身约 0.85× 上限）。

---

## 4. 架构总览

CH 对齐后的 `FsCache` 接口契约如下（伪 C++）：

```cpp
class FsCache {
 public:
  // 唯一公共入口；返回 holder 持有 segments 序列。
  // - segments 按 offset 排序、覆盖 [offset, min(offset+size, fileSize))
  //   连续无洞。
  // - 状态混合 EMPTY / DOWNLOADING / DOWNLOADED。caller 负责推进。
  // - settings 决定 hole 切段策略（max_size, alignment）。
  // - isPrefetch 决定本次调用的命中/未命中计入 FsCacheStats 的
  //   prefetch* 还是 demand* 字段（见 §6.3）。Prefetch 路径来自
  //   FsCacheBufferedInput::load（异步 enqueue），demand 路径来自
  //   FsCacheInputStream 的同步 read。
  FileSegmentsHolderPtr getOrSet(
      const std::string& path,
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& settings,
      ReadFile& remote,
      IsPrefetch isPrefetch);

  // bypass_cache_threshold：单次 size >= threshold 时返回 empty
  // holder（FileSegmentsHolder 无 segments），caller 看到 empty 时
  // fallback 到直接读 remote。`getOrSet` 入口调用本方法做短路判断。
  bool shouldBypass(uint64_t size) const;
};

class FileSegment {
 public:
  enum class State : uint8_t {
    kEmpty,                                  // 元数据建立、未开始下载
    kDownloading,                            // 当前有 writer，downloadedSize 推进中
    kDownloaded,                             // 全段已落盘
    kPartiallyDownloaded,                    // writer 异常退出，downloadedSize > 0 但未完成
    kPartiallyDownloadedNoContinuation,      // 上面那个加"不允许续传"
    kDetached,                               // 已从 metadata 移除，仅为正在持有的 reader 续命
  };

  bool reserve(uint64_t bytes);              // 单 writer：CAS State::kEmpty -> kDownloading
                                             // 成功者把 downloader_ 原子写为
                                             // std::this_thread::get_id()。
  void write(const char* buf, uint64_t len); // 推进 downloadedSize_，写盘，notify cv_
  void complete();                           // CAS State::kDownloading -> kDownloaded，notify
  void abandon();                            // writer 异常退出：state -> kPartiallyDownloaded，notify

  // 当前 writer 线程身份；non-writer 线程或无 writer 时返回默认构造的 id。
  // 唯一用途：FileSegmentsHolder 析构时判断"当前线程是否是 writer"，决
  // 定要不要替 caller 调 abandon()（见 §5.6）。
  std::thread::id getDownloader() const noexcept;

  // reader-side
  uint64_t waitForDownloadedSize(uint64_t needed);  // 阻塞到 downloadedSize_ >= needed
};

class FileSegmentsHolder {
 public:
  FileSegmentsHolder(std::vector<FileSegmentPtr>);
  ~FileSegmentsHolder();                     // 对每个非 complete segment 调 detach()
  std::vector<FileSegmentPtr>& segments();
};
```

### 4.1 一次 `getOrSet` 的端到端时序

```
caller: FsCache::getOrSet(path, off, size, settings, remote, isPrefetch)
   |
   v
1. clamp size to remote.size() - off                          (§5.1)
2. compute alignedRange = [floor(off/alignment), ceil((off+size)/alignment))
3. lockKeyMetadata(path)                                       (§6.1, per-key)
4. found = metadata.lookupRange(path, alignedRange)             (§5.2, lower_bound+prev)
5. holder = fillHolesWithEmptyFileSegments(found, alignedRange) (§5.3)
6. 对 holder 内每段按当前 state 计入 stats（hot-path 不加锁，4 字段均
   为 atomic，fetch_add(1, relaxed)）：
     state == kDownloaded                  → prefetchHits  / demandHits
     state == kEmpty 或 kDownloading       → prefetchMisses / demandMisses
   prefetch* vs demand* 由参数 isPrefetch 决定。计数完成后再 unlock
   keyMetadata（计数本身无锁，但要在 caller 看到 holder 之前完成以保
   证 §9.2 prefetchRatio 测试的 happens-before）。
7. unlock keyMetadata
8. return holder

----- caller-driven advancement (in FsCacheBufferedInput::load()) -----

for seg in holder.segments():
  switch (seg->state()):
    case kDownloaded: continue;
    case kDownloading:
      // 不做事，reader 真读到时 wait
      continue;
    case kEmpty:
      if (seg->reserve(seg->size())) {                         // 当前线程是 writer
        try {
          for chunk in chunked_read(remote, seg->range()):
            seg->write(chunk.data(), chunk.size());
          seg->complete();
        } catch (...) {
          seg->abandon();
          throw;
        }
      } else {
        // 别人抢到 writer，跳过
      }
```

### 4.2 partial-readable 时序

```
Writer thread A (in seg->write loop):
  state = kDownloading, downloadedSize = 0
  write(buf, 64KB) -> downloadedSize = 64KB, cv.notify_all()
  write(buf, 64KB) -> downloadedSize = 128KB, cv.notify_all()
  ...
  complete()       -> state = kDownloaded, cv.notify_all()

Reader thread B (FsCacheInputStream::Next, needs bytes [0, 50KB)):
  seg->state() == kDownloading
  needed = 50KB
  seg->waitForDownloadedSize(needed)
    while downloadedSize_ < needed:
      cv.wait(lock)
  // wake at downloadedSize >= 50KB (i.e., at writer's first notify, since 64KB >= 50KB)
  read bytes from local file [0, 50KB)
  return

Reader thread C (FsCacheInputStream::Next, needs bytes [0, 200KB)):
  // 同上但 needed=200KB，等到 writer 第四次 notify (downloadedSize=256KB) 才唤醒
```

`cv.wait(lock, predicate)` 配 `downloadedSize_.load() >= needed`。**关键
不变量**：`downloadedSize_` 必须在 `notify_all` 之前 release-store；reader
side acquire-load。

---

## 5. 数据结构

### 5.1 `FsCacheKey` & `PathKey`（phase-1 已就位）

phase-1 commit `5b9c7b...`（PathKey 拆分）已经把 `FsCacheKey` 改成
`{PathKey path, uint64_t offset, uint64_t size}`，`PathKey` 是 16 字节
SpookyHash。本 spec **不改 `FsCacheKey` 字段**。但 `hash()` 行为已经是
"path-only"（不参与 offset/size），区间查找天然兼容（同一 path 下所有
segment 都在同一 bucket）。

### 5.2 `FsCacheMetadata::lookupRange`

新 API：

```cpp
// Returns all segments under `path` whose range intersects [lo, hi).
// Acquires per-key mutex via lockKeyMetadata; releases before return
// (segments are shared_ptr-stable). Returns in offset-ascending order.
//
// 不接收 IsPrefetch：stats 4 字段计数统一发生在 getOrSet step 6（见 §4.1
// / §6.3），lookupRange 只负责返回相交段。fillHoles 新建的 kEmpty 段
// lookupRange 看不到但同样要计入 miss，所以计数点只能在 getOrSet 内
// lookupRange + fillHoles 之后。
std::vector<FileSegmentPtr> lookupRange(
    const PathKey& path,
    uint64_t lo,
    uint64_t hi) const;
```

实现照搬 CH `FileCache::getImpl`（`/tmp/ch_filecache_api_extract.md` §A.2）：

```cpp
auto locked = lockKeyMetadata(path, KeyNotFoundPolicy::kReturnNull);
if (locked == nullptr) return {};

auto& segments = locked->segments;  // std::map<uint64_t offset, FileSegmentMetadataPtr>
auto it = segments.lower_bound(lo);

// Check previous segment for overlap into [lo, ...)
if (it != segments.begin()) {
  auto prev = std::prev(it);
  if (prev->second->range().right >= lo) {  // CH-style closed range
    it = prev;
  }
}

std::vector<FileSegmentPtr> result;
while (it != segments.end() && it->first < hi) {
  // §5.4 kDetached 描述：evict() 把 segment 转 kDetached 后从 metadata 移
  // 除。所以 metadata.segments 里不会出现 kDetached 段，lookupRange 直接
  // 推回 fileSegment 即可。CH 的 detachedCopy / isEvicting 中间状态在本
  // spec 暂不引入（phase 3 若需要 evict 期间仍可读再加 kEvicting 状态 +
  // 对应 API，见 §10 R6 相邻论述）。
  result.push_back(it->second->fileSegment());
  ++it;
}
return result;
```

**关键 divergence vs phase-1 `lookup`**：
- phase-1 `lookup(key)` 调用 `segments.find(key.offset)`：O(log n) 点查，
  忽略 size。
- 本 spec `lookupRange(path, lo, hi)`：O(log n + k) 区间扫描，k 为相交
  段数。

phase-1 `lookup(key)` **保留**作为内部接口（commit chain 中 deprecated），
最后一个使用点删除时同 commit 移除（"no backwards-compat hacks"）。

### 5.3 `fillHolesWithEmptyFileSegments`

新 API（作为 `FsCache` 私有静态方法或 `FsCacheMetadata` 的 free function）：

```cpp
// Given the segments returned by lookupRange (may be empty), produces a
// continuous segment list covering [lo, hi). Holes are sliced into
// kEmpty FileSegments by maxSegmentSize. Inserts new kEmpty segments
// into metadata under the same per-key lock as lookupRange (CALLER
// HOLDS THE LOCK).
//
// Algorithm (CH-aligned, see /tmp/ch_filecache_api_extract.md §A.4):
//   1. If `found` is empty: hole spans [lo, hi); slice and insert.
//   2. Else:
//      a. Leading hole: [lo, found.front().offset) -> slice + insert.
//      b. Middle holes: between consecutive segments -> slice + insert.
//      c. Trailing hole: [found.back().end(), hi) -> slice + insert.
//   3. Concatenate found + new in offset-ascending order.
std::vector<FileSegmentPtr> fillHolesWithEmptyFileSegments(
    std::vector<FileSegmentPtr> found,
    uint64_t lo,
    uint64_t hi,
    const PathKey& path,
    const std::string& remotePath,
    LockedKey& lockedKey,        // 强制 caller 持锁
    const FsCacheConfig& cfg);
```

**hole 切段策略**：每段 size = `min(maxSegmentSize, alignedEnd - cursor)`，
跟 phase-1 `splitRange` 保持一致（commit `6ff3eb40c` 行为）。
**边界**：新段的 `(offset, size)` 跟既有段保证不重叠（因为只填洞）；
**不允许** caller 请求一个跟既有段部分重叠的范围 —— 但 caller 不需要
关心，因为 `getOrSet` 自己用 `alignedRange` 扩展请求边界，对齐后区间
要么完全包含既有段，要么完全在其外侧。

### 5.4 `FileSegment` 6 态状态机

```
                  reserve()
       kEmpty ─────────────────> kDownloading
         ▲                            │
         │                            │ write() x N
         │                            │ downloadedSize_ += N (notify cv_)
         │                            │
         │                       ┌────┴────┐
         │           complete()  │         │ abandon()
         │           (success)   │         │ (writer exception)
         │                       ▼         ▼
         │                  kDownloaded  kPartiallyDownloaded
         │                       │              │
         │                       │              │ retried reserve() fails
         │                       │              │ if state == kPartiallyDownloadedNoContinuation
         │                       │              │
         │                       │              ▼
         │                       │     kPartiallyDownloadedNoContinuation
         │                       │              │
         │                       │              │
         │   evict() while reader│ active       │
         └───────────────────────┴──────────────┘ ──> kDetached
                                                (last reader drop refCount → ~FileSegment)
```

**状态转移规则**（CAS-protected）：

| 起始 | 触发 | 终止 | 备注 |
|---|---|---|---|
| kEmpty | `reserve()` | kDownloading | 单 writer，CAS 成功者拥有 writer 角色；`downloader_` 原子写入 `std::this_thread::get_id()` |
| kDownloading | `write()` | kDownloading | `downloadedSize_` 累加，notify cv_ |
| kDownloading | `complete()` | kDownloaded | CAS 转移；notify 唤醒所有 reader |
| kDownloading | `abandon()` | kPartiallyDownloaded | writer 异常退出；保留已下载字节 |
| kPartiallyDownloaded | `reserve()`（新 writer） | kDownloading | 续传：从 `downloadedSize_` 继续 |
| kPartiallyDownloaded | `markNoContinuation()` | kPartiallyDownloadedNoContinuation | metadata 决策不续传 |
| kPartiallyDownloadedNoContinuation | `reserve()` | （失败，state 不变） | 抛 `VeloxRuntimeError`；state **不**自动 reset，必须由 metadata layer 显式 evict 该 segment 后由后续 caller 重新 `getOrSet` 触发 kEmpty 重建（与 R6 一致——phase-2 不存在指向该状态的转移） |
| Any | `detach()` (evict 期间有 reader) | kDetached | metadata 移除，segment 由 reader 持有 |

**phase-1 vs phase-2 状态机映射**：

| phase-1 状态 | 本 spec 状态 |
|---|---|
| kEmpty | kEmpty |
| kDownloading | kDownloading |
| kDownloaded | kDownloaded |
| kDetached | kDetached |
| (none) | kPartiallyDownloaded |
| (none) | kPartiallyDownloadedNoContinuation |

phase-1 commit `6ff3eb40c` 的代码（download 一次写完 .tmp + rename）
本 spec 改为 reserve/write/complete + 原地写。

### 5.5 On-disk layout（Q3 决策：原地写）

文件路径 schema 不变：`<cacheRoot>/<hex[0:2]>/<hex[2:4]>/<hash>.<offset>.<size>`。
但**没有 `.tmp` 后缀**。

**Writer 协议**（原地写 + 完成时 ftruncate；目的是让 partial 文件
stat_size < claimed_size，loadFromDisk 才能识别并删除）：

1. `reserve()` 成功 → `open(path, O_CREAT|O_WRONLY, 0644)`。
   **不调 ftruncate**：文件物理 size 保持 0，pwrite 写的 chunk 之间
   是 sparse holes（cheap，不占盘）。writer 持有 fd 直到
   complete()/abandon()。
2. `write(buf, n)` → `pwrite(fd, buf, n, downloadedSize_)`；fsync 该 chunk
   范围（`sync_file_range` 优先，fallback `fsync`）；`downloadedSize_ += n`
   release-store；`cv_.notify_all()`。
3. `complete()` → `ftruncate(fd, size)`（chunk 都已 pwrite 过，ftruncate
   声明物理 size 为 N，让 loadFromDisk 把它当 full）；最后一次 fsync；
   state CAS 到 `kDownloaded`；释放 fd。
4. `abandon()` → **不调 ftruncate**；state CAS 到 `kPartiallyDownloaded`；
   释放 fd。reader 想读 partial 段时自己重新 open（writer 不再持锁）。

**partial 文件的恢复识别**（`loadFromDisk`）：

- complete() 走过：`stat_size == claimed_size`（ftruncate 到 N）→ 接受
  为 full。
- writer 进程 SIGKILL 等异常退出，abandon() 没机会调用：fd 关闭，文件
  留在盘上但 `stat_size < claimed_size`（最多到最后一次 pwrite 之后的
  最高 offset+len，且未 ftruncate）→ **`loadFromDisk` 删除该文件**。
- 这是冷启动安全恢复的关键。writer 进程走过 abandon()：stat_size 仍
  < claimed_size，同样被 loadFromDisk 删除（暂不支持续传，phase 3）。

**测试覆盖**：`FsCachePartialRecoveryTest`（新文件）—— writer 写一半
SIGKILL，重启后 `loadFromDisk` 删除 partial 文件，下次 `getOrSet` 重新
下载。

### 5.6 `FileSegmentsHolder`

```cpp
class FileSegmentsHolder {
 public:
  explicit FileSegmentsHolder(std::vector<FileSegmentPtr> segments)
      : segments_{std::move(segments)} {}

  ~FileSegmentsHolder() {
    for (auto& seg : segments_) {
      // 如果 reader 在持有 holder 期间没把 EMPTY 推进到 DOWNLOADED，
      // 但也没显式 abandon，析构时把它们留在 EMPTY（不调 abandon）。
      // 其他线程可以接手 reserve()。
      //
      // 已 DOWNLOADING 但被本 caller 抛弃的：notify_all 让 wait 中的
      // reader 不至于死等。
      if (seg->state() == FileSegment::State::kDownloading &&
          seg->getDownloader() == std::this_thread::get_id()) {
        // 当前线程是 writer 但没 complete/abandon ⇒ 视为异常退出。
        seg->abandon();
      }
    }
  }

  std::vector<FileSegmentPtr>& segments() { return segments_; }

  // Non-copyable, movable.
  FileSegmentsHolder(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder& operator=(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder(FileSegmentsHolder&&) = default;
  FileSegmentsHolder& operator=(FileSegmentsHolder&&) = default;

 private:
  std::vector<FileSegmentPtr> segments_;
};
using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;
```

---

## 6. 锁拓扑

### 6.1 `LockedKey` RAII + per-key mutex

phase-1 已经有 `KeyMetadata::lock()` 返回 `LockedKey` 的雏形
（`velox/common/caching/fscache/KeyMetadata.h:38-76`）。本 spec 把行为
对齐 CH `Metadata::lockKeyMetadata`，含 `KeyNotFoundPolicy`：

```cpp
enum class KeyNotFoundPolicy {
  kThrow,           // 抛 VeloxRuntimeError
  kThrowLogical,    // 抛 VELOX_FAIL（assertion fail）
  kCreateEmpty,     // 不存在则创建空 KeyMetadata
  kReturnNull,      // 不存在则返回 nullptr
};

LockedKey FsCacheMetadata::lockKeyMetadata(
    const PathKey& path,
    KeyNotFoundPolicy policy);
```

CH 的 `kCreateEmpty` 在遇到 `REMOVED` 标记时会递归 retry —— 本 spec
照搬，因为 evict() 异步移除 segment 时可能短暂留下 REMOVED 占位。

### 6.2 锁顺序（4 把锁）

```
rank 1: FsCache::evictionMutex_                  (whole-cache, serializes evict() passes)
rank 2: FsCacheMetadata::Bucket::guard            (per-bucket, holds bucket.keys map)
rank 3: KeyMetadata::mutex_ (via LockedKey)       (per-key, holds segments map)
rank 4: FileSegment::mutex_                       (per-segment, state CAS / cv_ wait)
```

**严格 forward 顺序**：1 → 2 → 3 → 4。phase-1 的 `RankedMutex`
基础设施（`FsCacheGuards.h`）已经能在 debug build 检测违规。

### 6.3 Atomic stats

`FsCacheStats` 内部字段全部改 `std::atomic<uint64_t>`，按 prefetch/demand
拆 4 个字段：

```cpp
struct FsCacheStats {
  std::atomic<uint64_t> prefetchHits{0};
  std::atomic<uint64_t> prefetchMisses{0};
  std::atomic<uint64_t> demandHits{0};
  std::atomic<uint64_t> demandMisses{0};
  std::atomic<uint64_t> evictions{0};
  // `bytesOnDisk` 留作 phase-1 既有字段保留：eviction loop 靠它判停
  // (`FsCache.h:146` 的 `bytesOnDisk + bytesNeeded <= maxBytes`)；
  // `FsCache::totalSize()` 是它的 public accessor，对齐 CH
  // `FileCache::getUsedCacheSize()` (FileCache.h:199, 实现 cpp:2132 也是
  // 取 priority 队列的 approximate 大小)。
  std::atomic<uint64_t> bytesOnDisk{0};
};

enum class IsPrefetch : uint8_t { kPrefetch, kDemand };
```

`FsCache::getOrSet` 在公共入口处接收 `IsPrefetch isPrefetch`（§4 唯一
带该参数的 API）；`fetch_add(1, std::memory_order_relaxed)`
（observability 不要求 seq_cst）。端到端的 prefetch ratio 由 caller
自己用 4 字段算（见 §3 量化目标 + §9.2 `prefetchRatio` 测试）。

`recordHit` 当前路径：
```
hot path:
  metadata_->lookup(key)        // 1 把全局锁 ← 改 per-key
  recordHit(segment):
    LRU.splice(it, head)        // 1 把全局锁 ← 改 try_lock per-bucket
    ++stats.hits                // 改 atomic
```

改造后（与 §4.1 step 6 严格一致；计数在 `getOrSet` 内、
`lookupRange + fillHoles` 之后、unlock keyMetadata 之前）：
```
hot path (inside FsCache::getOrSet, isPrefetch from caller):
  locked = lockKeyMetadata(path)            # §6.1 per-key
  found = metadata_->lookupRange(path, range)
  holder = fillHolesWithEmptyFileSegments(found, range, ...)
  for seg in holder.segments():             # 含 lookupRange + 新建 kEmpty 段
    if (try_lock seg.bucket.priority):      # LRU bump，best-effort
      LRU.splice(it, head); unlock
    seg->hits_.fetch_add(1, relaxed)        # per-segment 计数（SLRU 用）
    if (seg.state == kDownloaded):
      (isPrefetch == kPrefetch ? counters_.prefetchHits : counters_.demandHits)
          .fetch_add(1, relaxed)
    else:                                   # kEmpty / kDownloading
      (isPrefetch == kPrefetch ? counters_.prefetchMisses : counters_.demandMisses)
          .fetch_add(1, relaxed)
  unlock keyMetadata
  return holder
```

### 6.4 Per-bucket LRU + cross-bucket evict 轮询

phase-1 `LruPolicy` 已经是 per-bucket 实例化（每个
`FsCacheMetadata::Bucket` 有自己的 `priority` 字段）。本 spec **不改
LRU 数据结构**，只把 `evict()` 的 candidate 选择改成跨 bucket 轮询：

```cpp
void FsCache::evict(uint64_t bytesNeeded) {
  std::lock_guard<std::mutex> g{evictionMutex_};
  // ... [phase-1 算法保持不变，已经是 cross-bucket round-robin] ...
}
```

phase-1 commit 已经实现 cross-bucket round-robin
（`FsCache.cpp:232-...`），本 spec 复用。

---

## 7. 后台下载

### 7.1 `DownloadThreadPool`

新组件，phase-1 没有。接口：

```cpp
class DownloadThreadPool {
 public:
  explicit DownloadThreadPool(size_t numThreads);

  // 提交一个 segment 下载任务。返回 future 供 wait 用。
  // task 内部执行：reserve / read remote / write / complete。
  std::future<void> submit(
      FileSegmentPtr seg,
      std::shared_ptr<ReadFile> remote);

  void shutdown();  // join all threads

 private:
  // IO-bound: download 任务做 remote pread + 本地 pwrite，没有 CPU
  // 重计算。用 IOThreadPoolExecutor 避免占用 CPU pool 拖慢 query 线程
  // （详见 §10 R4 mitigation）。
  folly::IOThreadPoolExecutor executor_;
};
```

`FsCache` 持有一个 `DownloadThreadPool` 实例，默认 `numThreads = 8`
（CH 默认值；可配）。**不复用 IO executor**（Velox query executor）
避免 query 线程被 I/O 阻塞。

### 7.2 R0 async load

`FsCacheBufferedInput::load(LogType)` 改造：

```cpp
void FsCacheBufferedInput::load(LogType) {
  for (auto& enq : enqueuedRegions_) {
    if (enq.holder != nullptr) continue;
    enq.holder = fsCache_->getOrSet(
        path_,
        enq.region.offset,
        enq.region.length,
        settings_,
        *remote_,
        IsPrefetch::kPrefetch);  // load() 走 prefetch 路径，§4 §6.3

    // Async: 把 EMPTY 段投递给 download pool；DOWNLOADING / DOWNLOADED 跳过
    for (auto& seg : enq.holder->segments()) {
      if (seg->state() == FileSegment::State::kEmpty) {
        if (seg->reserve(seg->size())) {  // CAS 成功 → 当前线程是 writer
          // 实际 download 异步派发
          downloadPool_->submit(seg, remote_);
        }
      }
    }
  }
  // 立即返回；read-time 用 waitForDownloadedSize 同步
}
```

### 7.3 isBuffered 仍然返回 false

phase-1 已经修过这个 bug（commit `929b398db`）。CH 对齐后 isBuffered
仍然返回 `false` —— 即使 caller "已经 load() 过"，segment 仍可能在
`kDownloading`，跳过 load() 的快速路径会绕过 caller 推进逻辑。

---

## 8. SLRU / QueryLimit / bypass

### 8.1 `SlruPolicy`

CH 默认策略：每个 cache 维护 probation list 和 protected list，新插入
段进 probation；命中两次（hits_ >= 2）promote 到 protected。protected
满了 demote 到 probation。两个 list 各自 LRU。

`EvictionPolicy` 接口扩展：

```cpp
class EvictionPolicy {
 public:
  virtual void onInsert(FileSegment*) = 0;
  virtual void onHit(FileSegment*) = 0;            // SLRU 在这里判断是否 promote
  virtual void onRemove(FileSegment*) = 0;
  virtual std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) = 0;
};
```

`SlruPolicy::onHit` 检查 `seg->hits_.load()`：
- == 1：第一次 hit，留在 probation，但 LRU bump 到 probation head
- >= 2：promote 到 protected head

`FsCacheConfig` 加 `slruProtectedRatio`（默认 0.7）。`LruPolicy` 保留
作为可选（`FsCacheConfig::evictionPolicy`：`kLru | kSlru`）。

### 8.2 `FileCacheQueryLimit`

per-query bytes quota。`QueryCtx` 拿到一个 `QueryLimitToken`：

```cpp
class FileCacheQueryLimit {
 public:
  // 每个 query 在执行开始时调用，token 析构时释放配额。
  std::unique_ptr<QueryLimitToken> reserveQuery(uint64_t maxBytesPerQuery);
};

class QueryLimitToken {
 public:
  // 在 getOrSet miss 路径上调用。返回 false 表示配额耗尽，caller
  // 应该走 bypass（直接读 remote 不缓存）。
  bool tryReserve(uint64_t bytes);
  void release(uint64_t bytes);
};
```

> **Phase-1 status (TODO: phase-3)**：本节描述的 `FileCacheQueryLimit`
> + `QueryLimitToken` 在 phase-1 实现并落库（plan Task 12），但
> **没有任何 caller** 在 phase-1 调用 `tryReserve` —— `HiveConnector` /
> `ConnectorQueryCtx` 的 token 透传（mint at `beginQuery`, store on
> ctx, 透传到 `FsCacheBufferedInput::enqueue`）推迟到 phase-3 一起
> 设计（同时决策是否换回 CH 的 thread-local query_id 模型）。phase-1
> 的 QueryLimit 类是**预留**实现，等 caller wiring 落地后才生效。

quota 与 `getOrSet` 的耦合关系：`getOrSet` 本身**不感知 quota** ——
它永远返回合法的连续段列表，caller 走正常推进路径。配额检查全部
在 caller 侧：在调 `getOrSet` 之前先 `token.tryReserve(size)`，
返回 `false` 时 caller 跳过 `getOrSet` 直接读 remote 不缓存。
`getOrSet` 不需要知道是否有 token、token 是否耗尽。

### 8.3 `bypass_cache_threshold`

`FsCacheConfig::bypassThresholdBytes`（默认 **0 = disabled**，与
ClickHouse 一致）。`FsCache::getOrSet` 入口调 `shouldBypass(size)`，
命中时返回 empty holder（无 segments）。`FsCacheBufferedInput` 看到
empty holder fallback 到直接 `remote.pread`。

**为什么默认禁用：** CH 把这条路径标为 "Undocumented. Not recommended
for use"（`src/Interpreters/FileCache/FileCacheSettings.cpp:55`），
默认 0；启用还需要把 `enable_bypass_cache_with_threshold` 翻成 `true`
（`FileCache.cpp:200`）。CH 默认靠 per-query quota
（`filesystem_cache_max_download_size`）+ LRU 自身的"新数据 evict 最冷"
来防大 scan 污染。Phase-1 沿用 CH 的默认值，承认这条路径是 safety
valve，**不是**热数据污染的主要防线。

**phase-1 现状的诚实陈述：** §8.2 的 `QueryLimitToken` 是 caller-side
dead code，phase-3 才接 wiring。phase-1 唯一能挡大 scan 的就是
`bypassThresholdBytes`。但默认值改 0 之后，phase-1 **没有任何活动的
大-scan 防护**，跟 CH 默认配置一致；运维需要时显式调高
`bypassThresholdBytes`（典型起点 256 MiB），或等 phase-3 接 query
quota。这条权衡的实际影响（默认禁用是否会让某些 workload 在
phase-1 下出现热数据被 evict）放到 phase-3 接 caller-side
QueryLimitToken wiring 之后用 TPC-H + microbench 重新验证；
phase-1 commit 不做行为验证，只保证语义和 CH 对齐。

---

## 9. 测试策略

UT 三层 + TDD-first（用户决策）。每个新公共 API 至少 1 个 unit test；
每个 prod 文件 ≥ 一个对应 test 文件；TDD-first = 每个 task 第一步是
写 failing test。

### 9.1 第 1 层：每个新 CH 概念一份独立 unit test

| 新概念 | 测试文件 (新) | 必测覆盖 |
|---|---|---|
| `FileSegment::State` 6 态 + 转移 | `FileSegmentStateTest.cpp` | 每条合法转移走一遍；每条非法转移 `EXPECT_THROW` |
| `FileSegment::reserve/write/complete` | `FileSegmentWriteTest.cpp` | happy path；reserve 后未 complete 析构应回滚；多次 write 累加 `downloadedSize_` 正确；write 越界（超 size）`EXPECT_THROW` |
| 边写边可读 | `FileSegmentPartialReadTest.cpp` | writer 写 64 KB notify，reader wait 64 KB 后能读；reader 等的字节数超过当前 `downloadedSize_` 时 block；writer abandon 时 reader wake 并 throw |
| `FsCacheMetadata::lookupRange` | `FsCacheMetadataTest.cpp` 扩展 | 空、单段全包含、单段部分相交、多段相交、相交段被 evict 后从 metadata 移除（下一次 lookupRange 看不到该段） |
| `fillHolesWithEmptyFileSegments` | `FillHolesTest.cpp` (新) | 区间无洞、头洞、尾洞、中间洞、多个洞混合、洞跨越 `maxSegmentSize` 切多段 |
| `FileSegmentsHolder` | `FileSegmentsHolderTest.cpp` (新) | 析构时所有非 complete segment 走 detach；移动语义；空 holder |
| `getOrSet` 回归 case（**原 bug**） | `FsCacheTest.cpp` 加 case | 同一 path 先 `(0, 4 MiB)` 再 `(0, 20 MiB)`，第二次返回的 holder 必须覆盖 `[0, 20 MiB)` 且 segment 划分跟 `splitRange` 一致 |
| `KeyNotFoundPolicy` 4 态 | `KeyMetadataTest.cpp` 扩展 | `kThrow` / `kThrowLogical` / `kCreateEmpty` / `kReturnNull` 每个枚举 1 个 case；`kCreateEmpty` 遇到 REMOVED 占位递归 retry |
| `SlruPolicy` | `SlruPolicyTest.cpp` (新) | onInsert → probation；onHit 第一次留 probation；onHit 第二次 promote；protected 满 demote |
| `FileCacheQueryLimit` | `FileCacheQueryLimitTest.cpp` (新) | tryReserve / release；耗尽返回 false；release 后又能 reserve |
| `DownloadThreadPool` | `DownloadThreadPoolTest.cpp` (新) | submit 一个 future、wait 完成；shutdown 后再 submit `EXPECT_THROW` |
| `FsCacheStats` 4 字段 + `IsPrefetch` 计数 | `FsCacheStatsTest.cpp` (新) | 调用 `getOrSet(kPrefetch)` miss/hit 走 `prefetchMisses/Hits`；`getOrSet(kDemand)` miss/hit 走 `demandMisses/Hits`；并发 fetch_add 计数不丢 |

### 9.2 第 2 层：集成 UT — stream 重写后的回归

| 文件 | 必测 |
|---|---|
| `FsCacheBufferedInputTest.cpp` 扩展 | load() 后所有 segment 都是 DOWNLOADED 或 DOWNLOADING；同 path 多次 enqueue 重叠区间正确；caller 推进 EMPTY → DOWNLOADED 路径；isBuffered 仍然 false |
| `FsCacheInputStreamTest.cpp` (新) | 边写边可读语义专门测试跨 partial 段的 Next() 行为；read 等到 downloadedSize 推进；writer abandon 时 read throw |
| `FsCacheEquivalenceTest.cpp` 扩展 | 跟现有 LocalReadFile 字节级一致性 — CH 对齐后必须仍然 pass，否则证明改动破坏了语义 |
| `FsCacheBypassTest.cpp` (新) | `size >= bypassThresholdBytes` 走 direct pread；不进 metadata；不计入 hits/misses |
| `FsCacheBufferedInputTest::prefetchRatio` (新增 case) | mock workload：先 enqueue N 个 region 走 `IsPrefetch::kPrefetch` 触发 prefetch；再随机 read 走 `IsPrefetch::kDemand` 触发 demand miss；断言 `prefetchMisses / (prefetchMisses + demandMisses) ≥ 0.80`，覆盖 §3 量化目标的 E2E 验证手段 |

### 9.3 第 3 层：concurrency UT

| 文件 | 必测 |
|---|---|
| `FsCacheConcurrencyTest.cpp` 扩展 | N 线程同时 getOrSet 同区间 / 不同区间 / 部分重叠区间；writer 异常时所有 waiter 都 wake；single-writer-per-segment 不变 |
| `FsCachePartialRecoveryTest.cpp` (新) | writer 写一半模拟 abandon，重启 loadFromDisk 删除 partial 文件 |
| `FsCacheAsyncLoadTest.cpp` (新) | FsCacheBufferedInput::load() 返回立即；read 第一次时阻塞 wait；多次 enqueue 不同区间并行 |

### 9.4 TDD-first 强制

**每个 task 第一步必须是写 failing test**：

1. 写 failing test → 跑 → 见红
2. 写 minimal impl → 跑 → 见绿
3. 写下一个 case → 见红 → impl → 见绿
4. ... 直到该 task 的所有 case 全绿
5. commit

phase-1 部分 commit 是 "impl + test 一起"（用户回顾时认定为
"phase-1 没严格做到 TDD-first"）。CH 对齐这波**严格 TDD**：每 commit
里 test 一定先于 impl 在 git diff 中出现，并且 test commit message 标
注 "RED:" 或 "GREEN:" 前缀（或在 commit body 里说明）。

### 9.5 老 UT 的处理

`FsCacheTest.cpp` / `FsCacheConcurrencyTest.cpp` / `FsCacheRecoveryTest.cpp`
/ `FsCachePersistenceTest.cpp` 共 10+ test case 是基于
`vector<FileSegmentPtr>` 旧签名写的。**Q4 决策硬切**，这些测试必须
在 holder 引入的同一 commit 里改成新签名：

1. 在改 prod 代码的同一 commit 里改测试
2. 改完跑一遍确认意图不变（同样的输入下，同样的预期产出，只是 holder
   解包一下）
3. 任何"老测试改完没法通过"的情况 = 真 bug，不是测试问题

### 9.6 性能 gate

新增测试 `FsCachePerfGate.cpp`（**非 ctest，需手动运行**）：

```bash
./velox_fscache_test --gtest_filter='FsCachePerfGate.*' \
    --gtest_repeat=3 --gtest_recreate_environments_when_repeating_tests
```

测试用例：
- `singleThreadHitOpsPerSec`：单线程纯命中 ops/s ≥ 7.0 M
- `sixteenThreadHitOpsPerSec`：16 线程纯命中扩展系数 ≥ 0.80×

性能 gate 不进 ctest（perf 测试有抖动），但每个 plan 结束的 verification
步骤必须手动跑一次。

### 9.7 死锁 / race 测试

`FsCacheDeadlockTest.cpp` (新)：
- 用 TSAN build 跑全部 concurrency UT 至少 30 分钟 (`--gtest_repeat=100`)
- `RankedMutex` 在 debug build 应阻止逆向锁顺序

---

## 10. 风险与已接受的复杂度

### R1：API 硬切的爆炸半径

`getOrSet` 签名改 `vector<FileSegmentPtr> → FileSegmentsHolderPtr` 会让
所有 phase-1 测试（4 个测试文件、20+ 测试函数）和 `FsCacheBufferedInput`
都在同一 commit 改。**爆炸半径估计 1000+ LOC**。

**Mitigation**：plan 拆 task 时，把"holder 引入 + 所有 caller 改造"做成
一个**单独 task**（不跟其他改动混合），review 这一 task 时专注 API
迁移正确性而非其他逻辑。

### R2：partial-readable 的 cv 风暴

writer 每写 64 KB notify_all，1 MiB segment 有 16 次 notify。N 个 reader
等同一 segment 时每次 notify 都唤醒全部。

**Mitigation**：用 `notify_all` 而非 `notify_one`（reader 等的字节数不同，
不能任选一个唤醒）；但 batch size 调大（每 256 KB notify 一次而非每
64 KB），把通知频率降到合理水平。FsCacheConfig 加 `notifyBatchBytes`
（默认 256 KiB）。

### R3：原地写 + crash 后 partial 文件被误认为 full

§5.5 已述：解决方案是 writer 完成时才 ftruncate；partial 文件 stat_size
< claimed_size，loadFromDisk 删除。**风险**：writer 进程异常退出时
`abandon()` 没机会调用，文件留在磁盘但 stat_size < N。**这是预期行为**：
重启后 loadFromDisk 删除即可。

### R4：DownloadThreadPool 跟 query executor 争资源

8 个下载线程 + Velox 默认 query executor 几十个线程，磁盘 / 网络 IO
可能成为新瓶颈。

**Mitigation**：DownloadThreadPool 用 `IOThreadPoolExecutor` 而非
`CPUThreadPoolExecutor`，并 cap concurrency（默认 8 ≤ NVMe 队列深度的
1/4）。phase-3 microbench 测量 IO 饱和度。

### R5：SLRU 在 cold-scan workload 退化

CH 默认 SLRU 在 zipfian/skewed 上比 LRU 好 10-20%，但 cold sequential
scan（每个段只命中一次）时 protected list 长期空。

**Mitigation**：保留 `LruPolicy` 作为 fallback；`FsCacheConfig::evictionPolicy`
默认 `kSlru`，benchmark 测出 cold scan 退化时可切回 `kLru`。

### R6：6 态状态机的 dead-code 风险

`kPartiallyDownloadedNoContinuation` 只在 metadata 层决定"不允许续传"
时使用；当前 phase-2 没有这种决策路径。phase-1 commit `6ff3eb40c` message
已经提示这是"phase 3 work"。

**Mitigation**：本 spec **保留 6 态枚举值**（CH 对齐），但
`kPartiallyDownloadedNoContinuation` 暂不进入任何转移路径。代码里加
`VELOX_UNREACHABLE("phase 3 only")` 占位，UT 不覆盖该状态。

---

## 11. 附录 A：实施切片

按依赖关系拆 plan 内 task（详细 plan 文件
`docs/superpowers/plans/2026-05-26-fscache-ch-aligned-redesign.md`，
本 spec 给出**高层 task 列表**）：

| Task | 内容 | 依赖 |
|---|---|---|
| 1 | `FileSegment::State` 扩 6 态 + 转移测试（**dead code，没人调用**） | — |
| 2 | `FileSegment::reserve/write/complete` 三段式 + write 单元测试 | 1 |
| 3 | partial-readable cv + downloadedSize_ + 单元测试 | 2 |
| 4 | On-disk 原地写 + `loadFromDisk` partial 识别 + recovery 测试 | 2 |
| 5 | `FileSegmentsHolder` + dtor 行为 + 测试（依赖 `getDownloader()` 接口，由 Task 2 引入） | 1, 2 |
| 6 | `FsCacheMetadata::lookupRange` + `KeyNotFoundPolicy` + 单元测试 | — |
| 7 | `fillHolesWithEmptyFileSegments` + 单元测试 | 6 |
| 8 | **API 硬切**：`FsCache::getOrSet` 改返回 `FileSegmentsHolderPtr`；所有 phase-1 测试同 commit 改完 | 5, 7 |
| 9 | `FsCacheBufferedInput::load()` caller-driven 推进 + isBuffered 仍 false | 8 |
| 10 | `FsCacheInputStream` 适配 partial-readable read | 3, 8 |
| 11 | `DownloadThreadPool` + async load + 测试 | 9 |
| 12 | `FileCacheQueryLimit` + `bypass_cache_threshold` + 测试 | 11 |
| 13 | `SlruPolicy` + 测试 | — |
| 14 | atomic stats（4 字段 `prefetchHits/Misses + demandHits/Misses` + `IsPrefetch` 枚举）+ try_lock LRU bump；**同 commit 内**在 `FsCacheBufferedInput::load` 传 `kPrefetch`、在 `FsCacheInputStream` 同步 read 路径传 `kDemand`，并跑通 §9.2 `FsCacheBufferedInputTest::prefetchRatio` | 8, 9, 10 |
| 15 | `FsCacheEquivalenceTest` / TPC-H q1-q22 端到端验证 | 12, 13, 14 |
| 16 | 性能 gate 跑通（≥ 7.0 M / ≥ 0.80×） | 15 |

依赖图：

```
1 ─┬─> 2 ─┬─> 3 ─┐
   │     │      │
   │     │      │
   │     ├─> 4  │
   │     │      │
   │     └─> 5 ─┐  (Task 5 holder dtor 依赖 Task 2 的 getDownloader())
   └────────┘  │
6 ─> 7 ────────┼─> 8 ─┬─> 9 ──┬─> 11 ─> 12 ─┐
                │      │       │             │
                │      └─> 10 ─┘             │
                │                            │
                └─> 14 <─── 9, 10            │
                  (IsPrefetch wiring 必须    │
                   等 load/stream caller     │
                   就位才能落地)             │
                                             │
13 ──────────────────────────────────────────┴─> 15 ─> 16
```

可并行：
- 1 / 5 / 6 / 13 同时启动
- 6+7 与 1-5 并行
- 11 / 14 / 13 收敛到 15 前可并行

预计 16 个 task，每个 task 5 阶段（实施 + 正确性 review + 简化 +
简化后 review + 提交），按之前节奏估计 30-50 commits 之间。

---

## 12. 附录 B：与现有计划的关系

| 现有文档 | 现状 | 处理 |
|---|---|---|
| `docs/superpowers/specs/2026-05-25-fscache-phase2-superseded.md` | 已重命名 `-superseded` | 内容废弃；本 spec 接手所有工作 |
| `docs/superpowers/specs/2026-05-23-fscache-microbench-design.md` | 仍 active | **本 spec 落地后需重写**，反映新 API（`getOrSet` 返回 holder 影响 runCell 推进逻辑） |
| `docs/superpowers/specs/2026-05-23-fscache-vs-cbi-tpcds-design.md` | 仍 active | 不动；CH 对齐对 A/B 测试 spec 透明（A/B 都是 BufferedInput 接口） |
| `docs/superpowers/plans/2026-05-23-fscache-microbench.md` | 9-task plan，已 commit | **本 spec 落地后重写**，与 microbench spec 同步 |
| `docs/superpowers/plans/2026-05-26-fscache-tpch-ab.md` | 641 行已写好，untracked | 不动；CH 对齐落地后执行 |

---

## 13. 附录 C：CH 源码参考

所有 CH 行为参考的 verbatim 源码在
`/tmp/ch_filecache_api_extract.md`（subagent 抽取，1161 行）。主要参考
点：

- §A.1: `FileCache::getOrSet` (CH `FileCache.cpp:799-966`)
- §A.2: `FileCache::getImpl` (CH `FileCache.cpp:461-557`) — `lookupRange`
  设计依据
- §A.3: `FileCache::splitRange` (CH `FileCache.cpp:559-604`)
- §A.4: `FileCache::fillHolesWithEmptyFileSegments` (CH `FileCache.cpp:625-751`)
- §B.1: `FileSegment::State` 6 态（CH 在 `FileSegmentInfo.h`，subagent
  未找到本地副本，但通过其他代码 verbatim 引用确认 5 + 1 状态命名）
- §B.2: `FileSegment::Range` 闭区间 `[left, right]`
- §C.1: `KeyMetadata` = `private std::map<size_t, FileSegmentMetadataPtr>`
- §C.4: `KeyNotFoundPolicy` 4 态 + CREATE_EMPTY 遇 REMOVED 递归 retry

---

## Self-review checklist

- [x] **Placeholder scan**：无 TBD / TODO / 待补
- [x] **Internal consistency**：架构 (§4) 跟数据结构 (§5) / 锁 (§6) /
  测试 (§9) 互相对应；6 态状态机在 §5.4 和 §10 R6 一致（dead state
  保留枚举但不进转移）；§7.1 `IOThreadPoolExecutor` 跟 §10 R4 mitigation
  一致；**stats 计数协议唯一**：§4.1 step 6、§5.2 注释、§6.3 改造后
  hot-path 三处都收口到"计数发生在 `FsCache::getOrSet` 内、
  `lookupRange + fillHoles` 之后、unlock keyMetadata 之前"；`IsPrefetch`
  只 `getOrSet` 公共入口接收，`lookupRange` 不传
- [x] **Impl skeleton sub-check**：扫描所有 impl skeleton / 伪代码块
  里出现的方法调用，逐一对照 §4 / §5.x 的 public 接口列表是否声明。
  已确认无隐式新 API：§5.2 lookupRange 不再调用未声明的
  `isEvicting()` / `detachedCopy()`（按 §5.4 状态机 kDetached 已从
  metadata 移除，metadata.segments 看不到 evicting 段，CH 的中间状态
  phase-2 暂不引入，phase-3 若需要再加 kEvicting + 配套 API）；§6.3
  hot-path 用到的 `bucket.priority` / `hits_` / `prefetchHits` 等都在
  对应小节有定义
- [x] **Scope check**：6 类工作一次性做完是用户决策 Q1B；hard cut
  签名是 Q4；新 spec 接手全部是用户决策
- [x] **Ambiguity check**：每个新 API 都给了签名 + 算法骨架；on-disk
  layout (§5.5) 给出唯一 writer 协议（open 不 ftruncate / complete 时
  ftruncate / abandon 不 ftruncate，partial 文件 stat_size < N 被
  loadFromDisk 识别删除）；`getDownloader()` 已在 §4 FileSegment 接口
  和 §5.4 状态转移表显式声明；`kPartiallyDownloadedNoContinuation →
  reserve()` 明确为"抛 VeloxRuntimeError + state 不变"；
  `KeyNotFoundPolicy` 4 个枚举统一 k 前缀 camelCase；prefetch ratio
  目标拆为 "接口契约 (4-atomic FsCacheStats) + E2E `prefetchRatio`
  test" 双轨；**`IsPrefetch` 只在 §4 `FsCache::getOrSet` 公共入口出现**
  （`lookupRange` 不接收，避免 stats 计数点多处冲突）；§4.1 时序步骤
  6 显式给出 stats 4 字段计数点（segment-level，atomic 不加锁）；§11
  Task 14 owns IsPrefetch wiring 并显式依赖 Task 9/10 防止 Task 14
  提前完成时 caller 端仍 hardcode 默认值

---

**End of design.**
