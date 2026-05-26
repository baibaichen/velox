# FsCache Phase-2 — Design

**Status**: Drafted 2026-05-25
**Scope**: phase-2 主体五项落地——锁层次完整实例化（per-bucket / per-key）、
后台下载线程池、SLRU 淘汰策略、`FileCacheQueryLimit` per-query 配额、
`bypass_cache_threshold` 大读绕过
**Not in scope**: `PARTIALLY_DOWNLOADED` 续传与 `canStartFromCache`
（phase 2.5，见 §11）；benchmark 三方对比（phase 3）；Userspace Page Cache
RAM 层（phase 4）

## 0. 本 spec 解决什么

Phase-1 落地后，FsCache 在两个维度上明显落后于 ClickHouse：

1. **16 线程 hit-path 退化到单线程的 0.3×**（2.1 M vs 7.6 M ops/s）——
   命中路径串了 3 把全局锁（metadata、priority、state），并发越高越
   排队。
2. **Reader prefetch 信号在 FsCache 这一层被压成同步**——
   `FsCacheBufferedInput::load()` 同步串行下载，上游所有并行预取
   （`TableScan::preload` / Parquet `scheduleRowGroups` / DWRF 等）
   都退化为前台 demand fetch。

本 spec 通过 (a) 细化锁实例化到 per-bucket + per-key + atomic stats +
try_lock LRU bump，把 16 线程 hit 扩展系数拉回 ≥ 0.80×；(b) 把
`load()` 异步派发到下载池，恢复 prefetch 有效性，miss→hit 比例
≥ 80%。

SLRU 默认策略、`FileCacheQueryLimit` per-query 配额、
`bypass_cache_threshold` 大读绕过——这三项是和 CH parity 的配套
（默认行为对齐 + 多 query 抗压 + 大扫描抗污染），不是上面两个性能
回归的直接修复。

## 1. 背景与触发点

Phase-1 baseline (`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md`)
在 ws_mult=0.5 全命中 workload 上量出 16 线程吞吐从 7.6 M ops/s 退化到 2.1-2.3
M ops/s。根因诊断（详见
`~/SourceCode/.ai/share_data/local-cache/claude/03-clickhouse.md` §A 与
`docs/superpowers/notes/2026-05-25-prefetch-mapping.md`）：

1. **hit-path 串行三把全局锁**：`metadata_->lookup` 走唯一 `CacheMetadataMutex`；
   `recordHit` 串 `priorityMutex_` (LRU `splice`) + `stateMutex_` (`++hits`)。
2. **prefetch 路径失效**：reader-driven prefetch 信号经 `BufferedInput::load()`
   进 `FsCache`，但 `FsCacheBufferedInput::load()` 是同步实现，把并行预取退化
   为前台 demand fetch。

Phase-1 design (`velox/docs/designs/fscache-clickhouse-style.md`) 已显式预留
phase-2 升级点：5 把锁的**类型和命名**按 CH 拓扑建好，但**实例化粒度**仍是
whole-cache 共享；`bucket` 数组数量从 phase-1 起就是 1024，正是为本 spec 的
per-bucket 锁分配铺挂载点。

## 2. 范围与非范围

### In scope

1. **锁拓扑完整实例化 + FsCacheKey 拆分**（plan-1）：per-bucket
   `CacheMetadataGuard`、per-key `KeyGuard`、per-segment 已有
   `FileSegmentGuard`、原子化 stats 计数器、`try_lock` 化的 LRU bump；
   `FsCacheKey` 拆 `PathKey + offset`（§4.4）；per-bucket LRU 实例化
   （§4.6）。
2. **后台下载线程池**（plan-2）：`FsCacheBufferedInput::load()` 异步化；
   `FsCacheConfig.downloadExecutor` hook，nullptr 时 fallback 到
   `connector->ioExecutor()`；per-segment 任务粒度。
3. **SLRU 淘汰策略**（plan-3）：`EvictionPolicy` 抽象（phase-1 已有）保持
   接口名不变；`SlruPolicy` probationary + protected 二段实例化；配置项
   `cache_policy={SLRU, LRU}`，默认 SLRU。
4. **FileCacheQueryLimit per-query 配额**（plan-4）：每 query 独立 LRU 子队列；
   `tryReserve` 阶段检查配额；hook 进 `QueryCtx`。
5. **bypass_cache_threshold**（plan-4）：`FsCacheConfig` 加 `bypassThreshold`
   字段；`getOrSet` 入口对超阈值请求短路直读远端，不入 cache。

### Out of scope (硬约束)

- **不修改操作符层任何 prefetch 调用方**。Velox 已有 4 处 prefetch 调用方
  （`TableScan::preload` / `TaskStructs::getSplit` / `ParquetReader::scheduleRowGroups`
  / `DwrfReaderBase` / `CacheInputStream`），它们在 phase-1 期间因
  `FsCacheBufferedInput::load()` 同步实现而 prefetch 退化；phase-2 之后由
  R0 自动生效。**任何"给某操作符 X 加 prefetch hint"的提议属于另一个 spec**。
- **`PARTIALLY_DOWNLOADED` 续传**：phase 2.5 独立 spec（§11）。
- **Benchmark 三方对比**：phase 3。
- **UPC RAM 层**：phase 4。
- **`cache cleanup` 后台线程**：CH 的 `cleanup_thread` 处理 REMOVING/REMOVED
  状态的 key 异步清理。本 spec 沿用 phase-1 的同步删除（`evict` 立刻 unlink），
  不引入 cleanup 线程。

## 3. 目标与非目标

### 量化目标

| 指标 | Phase-1 baseline | Phase-2 目标 |
|---|---|---|
| 16 线程纯 hit 吞吐 (ws=0.5, sequential) | 2.27 M ops/s | ≥ 6 M ops/s（接近 1-thread 7.55 M 的 0.8×） |
| 16 线程 hit 扩展系数 (t16 / t1) | 0.30× | ≥ 0.80× |
| Prefetch miss → hit 比例 | 0% (prefetch 退化为同步) | ≥ 80% (R0 异步化生效后) |
| 并发回归 gate | `FsCacheConcurrencyTest` PASS | `FsCacheConcurrencyTest` PASS（全套不变） |

### 非目标

- 与 `AsyncDataCache+SsdCache` 或 CacheLib 的吞吐对比——phase 3 做。
- SSD 写放大数据——phase 3 做。
- TPC-DS 端到端 wall_ms 改善——phase 3 做（需 phase-2 + plan-dump 完成）。
- 减少每 segment 元数据开销（KeyMetadata 引入会**增加**元数据开销，是用空间
  换并发；详见 §10 R5）。

## 4. 锁拓扑（plan-1）

### 4.1 桶锁实例化 (C1)

Phase-1 `FsCacheMetadata` 持有单个 `CacheMetadataMutex`，1024 个 bucket 共享。
Phase-2 每 bucket 自带 `CacheMetadataGuard guard`（实例字段，inline 在
`Bucket` struct 中，见 §5.3）。`numBuckets` 必须为 2 的幂（phase-1 已是
1024），按 `bucketIndex(pathKey) = hash(pathKey) & (numBuckets - 1)` 选择。

公共 API 签名不变：`lookup(key)`、`insert(key, segment)`、`erase(key)`
内部各自锁对应 bucket。跨 bucket 操作（`forEach`、`size`、`clear`）需要按
bucket 顺序逐个锁——本 spec 不引入跨 bucket 的原子快照操作，避免锁顺序爆炸。

### 4.2 Atomic stats + try_lock LRU bump (C2)

**Phase-1 现状**：`FsCacheStats` 字段是裸 `uint64_t`（`FsCache.h:41`），
由 `stateMutex_`（`FsCache.h:183` 邻近）保护；每次 `recordHit` /
`recordMiss`（`FsCache.cpp:296-313`）都获取一次 `CacheStateGuard`。

**Phase-2 改动（结构性，列出所有受影响点）**：
1. `FsCacheStats` 字段改为 `std::atomic<uint64_t>`（`FsCache.h:41-46`）。
   `bytesOnDisk` 也变 atomic——`evict()` 里 `bytesOnDisk -= freed`
   （`FsCache.cpp:287`）改为 `fetch_sub`。
2. `recordHit` / `recordMiss` 的 `CacheStateGuard guard{stateMutex_}`
   全部删除；hit/miss/eviction 计数走
   `fetch_add(1, std::memory_order_relaxed)`。
3. `stats()`（`FsCache.cpp:292-294`）用每字段 relaxed load 装配快照
   （非原子但每字段独立原子，观测用途，弱一致性可接受）。
4. **`stateMutex_` 字段保留**——`evict()` 里 `current = stats_.bytesOnDisk`
   附近（`FsCache.cpp:241`）的临界区可考虑保留以串行化"reserve→evict"
   决策；这条由 plan-1 实现时决定（保留或删除均不影响 hit-path）。
5. **Phase-2 新增** `FileSegment::increasePriorityMutex_`（`std::mutex`）
   字段——phase-1 `FileSegment.h` **没有**此字段。仅供 LRU bump
   合并并发 hit 使用，不参与下载/状态机协议。

LRU bump 通过 `try_lock` 合并：

```cpp
void FsCache::recordHit(FileSegment* segment) {
  hits_.fetch_add(1, std::memory_order_relaxed);
  // increasePriorityMutex_ 是 phase-2 新增的 per-segment 锁
  std::unique_lock<std::mutex> lk{segment->increasePriorityMutex_, std::try_to_lock};
  if (lk.owns_lock()) {
    // 同 segment 并发 hit 时只一个线程做 LRU splice，其余直接返回
    auto& bucket = bucketOf(segment);
    CachePriorityGuard::Lock pg{bucket.priorityMutex};
    bucket.priority->onHit(segment);
  }
}
```

CH 同款机制：`FileSegment.cpp:1196-1223` `increasePriority` 用
`increase_priority_mutex.try_lock()` 合并并发 hit 的 LRU 更新。

### 4.3 KeyMetadata + LockedKey RAII (C3)

引入 `KeyMetadata`：

```cpp
class KeyMetadata {
 public:
  KeyGuard::Lock lock() const;
  // segments 按 offset 排序，便于范围查询；同 path 多 offset 不需要散列遍历。
  std::map<uint64_t, FileSegmentPtr> segments;
 private:
  mutable KeyGuard guard_;
};
using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;
```

`FsCacheMetadata` 的 bucket 改为 `unordered_map<PathKey, KeyMetadataPtr>`。
`lockKeyMetadata(pathKey)` 流程严格对应 CH `Metadata.cpp:247-287`：

1. `bucket.lock()` 取 per-bucket mutex
2. `bucket.find(pathKey)` 或 `insert` 拿到 `KeyMetadataPtr`
3. **立即释放 bucket lock**（出作用域）
4. `keyMetadata->lock()` 取 per-key mutex，返回 `LockedKey` RAII

`LockedKey` 持有期间可以增删该 key 下的 segments、读写 segment state，但
**不允许跨 key 操作**（必须释放当前 LockedKey 才能取下一个）。这条约束由
debug-only `LockOrderChecker` 验证（phase-1 已有，扩展到 KeyGuard rank）。

### 4.4 FsCacheKey 拆 PathKey + offset

Phase-1 `FsCacheKey{path, offset, size}` 作为复合 hash key，phase-2 拆为：

```cpp
struct PathKey {
  // 16 位 hex string（保留 phase-1 disk format 兼容），由 path 的 64 位
  // FNV-1a hash 渲染。
  std::array<char, 16> hash;
};

struct FsCacheKey {
  PathKey path;
  uint64_t offset;
  uint64_t size;
};
```

`PathKey` 用于 bucket 索引（`hash(PathKey) & (numBuckets - 1)`）与 `KeyMetadata`
查找；`(offset, size)` 进入 `KeyMetadata::segments` 的 `std::map` key（实际
key 是 `offset`，`size` 存在 `FileSegment` 内部）。

**Disk 文件名格式 schema 不变，但 hash 值会变**：phase-1 `FsCacheKey.cpp:28-35`
的 `combinedHash` 把 `(path, offset, size)` 三者混入 64-bit hash，渲染为
`<16-hex>.<offset>.<size>`。phase-2 改为 PathKey-only hash（path 单独
hash），渲染格式不变，但同一 `(path, offset, size)` 在 phase-1 / phase-2
计算出的 hex 前缀**不一样**。后果：phase-1 留下的 cache 文件在 phase-2
启动时，`loadFromDisk` 解析出来的 PathKey 与新 hash 函数算出的 PathKey
不一致 → 这些文件成为孤儿。

**接受弃用既有缓存**：phase-2 首次启动如检测到 `cacheRoot` 非空，
**整体清空** 后再进入正常 `loadFromDisk` 流程（disk-walk 阶段无 path
可用于 phase-1 vs phase-2 甄别，盲扫清空是唯一可行的"接受弃用"实现）。
release notes 中标注 "phase-2 upgrade 首次启动会清空所有 phase-1 写入
的 cache 文件"。详见 §10 R7。

### 4.5 锁顺序更新

Phase-1 锁顺序（`FsCacheGuards.h` 头注释）：
```
CachePriorityGuard > CacheStateGuard > CacheMetadataGuard > KeyGuard > FileSegmentGuard
```

Phase-2 实例化后，每把锁不再唯一，但**rank** 不变。
`LockOrderChecker` 的 `thread_local rank stack` 仅检查 rank 顺序，不检查实例
身份——这是 phase-1 设计预留的扩展点。

新增的两把锁实例位置：
- `KeyGuard` 实例驻留 `KeyMetadata::guard_`（per-key）。
- `CacheMetadataGuard` 实例驻留 `bucket.guard_`（per-bucket）。

**禁止跨 bucket 持有 `CacheMetadataGuard`**——任何需要遍历多 bucket 的操作
（如 `clear()`）必须按 bucket 序列化、每次只持一把。这条由 review 把关，
不引入静态检查（CH 也是 review-based）。

**Debug 增强（可选）**：`LockOrderChecker` 现仅按 rank 检查，不区分实例
身份。两个 bucket 的 `CacheMetadataGuard` 同 rank，所以"同时持两把"
不会触发现有断言。phase-2 实现 plan-1 时建议给 LockOrderChecker 增加
`thread_local` "当前持有的 bucket index" 字段，在持第 2 把 bucket
guard 时断言——这是 belt-and-suspenders，spec 不强制（W1）。

### 4.6 Per-bucket LRU + cross-bucket evict 轮询

Phase-1 单条全局 LRU 链表 + 单 `CachePriorityGuard`。Phase-2 每 bucket 持有
独立的 `EvictionPolicy` 实例（默认 `SlruPolicy`，可配 `LruPolicy`），
受该 bucket 的 `CachePriorityGuard` 保护。

`evict(bytesNeeded)` 跨 bucket 轮询选 victim：

```cpp
void FsCache::evict(uint64_t bytesNeeded) {
  // 1. 计算总 bytesOnDisk（atomic load，无锁）
  // 2. 跨 bucket 轮询：每 bucket 各取 try_lock 收一批 victim 候选；
  //    锁不到的 bucket 跳过（下一轮再来）
  // 3. 全局聚合 freed 计数，达到 bytesNeeded 即止
  // 4. 实际 fs::remove + onRemove 在收完 candidates 之后执行
  //    （phase-1 同款两阶段提交，避免 LRU 中 evict 一半磁盘失败的孤儿）
}
```

`try_lock` 跳过繁忙 bucket 是关键——单 evict 调用不阻塞其他 bucket 的 hit
路径。**重试语义**：**单次 `evict()` 调用内**尝试 `N = numBuckets` 轮
（按 round-robin 顺序遍历），每轮对当前 bucket 做 `try_lock`；N 轮跑完
仍未达 `bytesNeeded`，对剩余字节数对每个仍繁忙的 bucket 做 blocking
`lock()` 兜底，按 round-robin 顺序逐个收 victim 直到补齐 `bytesNeeded`
或全部 bucket 都查过。**不**跨多次 `evict()` 调用累计计数。

**Blocking fallback 的最坏等待**：兜底阶段对每个 bucket 串行 blocking
`lock()`，单次 `evict()` 最坏等待 ≈ `numBuckets` × per-bucket 持锁时长。
phase-1 measurements 显示单 bucket 持锁内只做 `unordered_map` 插入/删除
与 LRU list splice，典型 < 10 μs；phase-2 numBuckets = 1024 时最坏
~10 ms，pathological 写盘风暴下可达数百 ms。spec **接受**该上限：
`evict` 是后台/低频路径（仅在 `reserve` 超 quota 时触发），不在 hit
fast-path 上；若 microbench 显示 `reserve → evict` 成为瓶颈，phase-3
可改 background evict thread 异步化。

## 5. 数据结构（plan-1）

### 5.1 PathKey & FsCacheKey

见 §4.4。`PathKey` 是 `std::array<char, 16>` 的 trivially copyable 类型，
带 `operator==` + `std::hash<PathKey>` 特化（直接取前 8 字节 reinterpret 为
`uint64_t`，因为 hex 渲染已分散）。

### 5.2 KeyMetadata 字段

```cpp
class KeyMetadata {
 public:
  KeyGuard::Lock lock() const { return guard_.lock(); }

  // 同 path 下的 segments，按 offset 排序。size 存 FileSegment 内部。
  std::map<uint64_t, FileSegmentPtr> segments;

  // segment count 缓存（避免 segments.size() 在持锁外被调用）；
  // 仅 LockedKey 持有期间可读写。
  size_t numSegments{0};

 private:
  mutable KeyGuard guard_;
};
```

**不缓存 path 字符串**——磁盘 fileName 是 hash，反查 path 是 admin 问题，
不在本 spec 范围（phase-1 design doc §498 风险 3 已显式接受丢失反查）。

### 5.3 Metadata bucket 数组改造

```cpp
struct FsCacheMetadata::Bucket {
  std::unordered_map<PathKey, KeyMetadataPtr> keys;
  mutable CacheMetadataGuard guard;
  // per-bucket LRU 实例，默认 SlruPolicy，构造时由 FsCache 注入
  std::unique_ptr<EvictionPolicy> priority;
  mutable CachePriorityGuard priorityMutex;
};
std::vector<Bucket> buckets_;  // 长度 numBuckets，构造后不变
```

Bucket 数组**构造后不可 resize**——`buckets_.size()` 进入 const 状态，
`bucketIndex(key)` 是无锁查表。

### 5.4 LRU 容器迁移到 per-bucket

`LruPolicy` / `SlruPolicy` 都满足 `EvictionPolicy`，不再被 `FsCache`
直接持有，而是每 bucket 一份。`onHit` / `onInsert` / `onRemove` /
`selectVictims` 的 segment 仅限本 bucket。

跨 bucket evict 由 `FsCache::evict` 协调（§4.6）。

### 5.5 fileName() schema 不变 / hash 值变更

`FsCacheKey::fileName()` 返回 `<pathkey-hex>.<offset>.<size>`，schema 不变；
但 `<pathkey-hex>` 的计算方式从 phase-1 的 `combinedHash(path, offset, size)`
（`FsCacheKey.cpp:28-35`）改为 PathKey-only hash（仅 path）。

phase-1 写盘的 cache 文件在 phase-2 启动时**无法区分**（fileName schema
一致，且 disk-walk 阶段无 path 可重算比对）。phase-2 首次启动如检测到
`cacheRoot` 非空，**整体清空后再进入正常 `loadFromDisk` 流程**——既有
phase-1 文件全部弃用。详见 §10 R7。

**Crash recovery 测试要回归**：phase-1 的 `FsCacheRecoveryTest` 必须在
phase-2 代码下不修改即通过——但前提是 fixture 在 phase-2 代码下用
phase-2 hash 函数写盘 + 重启 + reload，**不是**跨版本 recovery。后者
是有意 break 的（见 R7）。

## 6. 后台下载与 R0 异步 load（plan-2）

### 6.1 总览

Phase-1 的 `FsCacheBufferedInput::load()` 同步串行下载（`FsCacheBufferedInput.cpp:147-158`），
phase-2 改为异步派发到下载池：每个 miss segment 一个 task，reader 立刻
返回；真正读取时 `DeferredStream::ensureWithData()` 同步等待对应 segment
的下载完成。

R0 选项已在 `docs/superpowers/notes/2026-05-25-prefetch-mapping.md` 三选一
里被选中，其优于 R1（修 `prefetch(Region)` 路径，生产不通）与 R3（新加
显式 enqueue/wait API，破坏抽象）。

### 6.2 DownloadThreadPool 接口与池所有权

新增 hook：

```cpp
struct FsCacheConfig {
  // ...既有字段...

  // 可选自定义下载池。nullptr 时 fallback 到 connector->ioExecutor()。
  // 由调用方持有生命周期；FsCache 仅持有原始指针。
  folly::Executor* downloadExecutor{nullptr};
};
```

`FsCacheBufferedInput` 构造时若 `config.downloadExecutor` 为 nullptr，
**回退发生在调用点**（`HiveConnectorUtil::createBufferedInput` 那一侧
或调用方主入口），由调用点把 `connector->ioExecutor()` 注入
`FsCacheConfig`，再构造 `FsCacheBufferedInput`。
`FsCacheBufferedInput` 自身只持有 `folly::Executor*`，不依赖 `Connector*`。
phase-1 ctor 签名 `(shared_ptr<ReadFile>, MemoryPool&, FsCache*)` 在
phase-2 末尾**追加** `folly::Executor*` 一个参数，变为
`(shared_ptr<ReadFile>, MemoryPool&, FsCache*, folly::Executor*)`；
原 3 个参数语义与顺序保持不变。本 spec 不引入"默认独立池"或
"helper 工厂"，保持与 phase-1 同款的 zero-defaults 策略
（OQ #2-c 决策）。

**Cross-spec impact**：TPC-DS A/B spec
（`docs/superpowers/specs/2026-05-23-fscache-vs-cbi-tpcds-design.md`
§2.5）的 `createBufferedInput` 新分支构造 `FsCacheBufferedInput` 时
需同步补传 `executor` 参数。plan-2 落地 R0 时一并更新该调用点；
本 spec 与 TPC-DS A/B spec 之间不形成接口循环依赖（TPC-DS spec
只 consume ctor，不 export）。

池大小与限流策略**完全由调用方控制**，FsCache 不引入 semaphore / token
bucket / 自适应限流。CH 默认 5 线程是其工程经验值，Velox 这边 reuse
`ioExecutor` 已经天然受 connector 配置约束。

### 6.3 任务粒度：per-segment

`FsCacheBufferedInput::load()` 异步改造后伪码。**Phase-2 新增方法**
`FileSegment::isDownloadOwnedByCurrentThread()` 和
`FileSegment::waitForDownload()`——`FileSegment.h:114-116` 注释已显式
预留这两个 helper 的引入点（"FileSegment can expose waitForDownload()
/ notifyAll() helpers and drop the public mutex_/cv_"）。

`input_->getReadFile()` 返回 `const std::shared_ptr<ReadFile>&`
（`BufferedInput.h:186`），lambda 按值捕获 shared_ptr 安全持有
remote file 生命周期，覆盖 download 期间 FsCacheBufferedInput 析构
的场景。

```cpp
void FsCacheBufferedInput::load(LogType /*unused*/) {
  for (auto& enqueued : enqueuedRegions_) {
    if (!enqueued.segments.empty()) {
      continue;
    }
    // 1. metadata lookup：拿到（或创建）该 region 覆盖的 segments；
    //    miss 的 segment 标记为 kDownloading（state machine 已有）。
    //    PathKey 由 input_->getName() 经 FNV-1a hash 渲染为 16-hex；
    //    fsCache_ 内部封装 path → PathKey 转换。
    enqueued.segments = fsCache_->lookupOrCreate(
        input_->getName(),
        enqueued.region.offset,
        enqueued.region.length);

    // 2. per-segment 派发：每个 miss segment 一个 task 进池。
    //    isDownloadOwnedByCurrentThread() 是 phase-2 新增——
    //    lookupOrCreate 内部的 beginDownload CAS 把 ownerThread 写到
    //    FileSegment 的 phase-2 新增字段里，胜者派发下载，败者跳过
    //    （后续在 ensureWithData 里 waitForDownload）。
    for (auto& seg : enqueued.segments) {
      if (seg->state() == FileSegment::State::kDownloading
          && seg->isDownloadOwnedByCurrentThread()) {
        // readFile 按值捕获 shared_ptr，保 download 期间生命周期
        auto readFile = input_->getReadFile();
        executor_->add([seg, readFile = std::move(readFile)] {
          seg->download(*readFile);   // 负责 read+writeCache+markDownloaded
        });
      }
    }
  }
  // 立刻返回，reader 继续 enqueue 下一组 region
}
```

per-segment 而非 per-region 的理由（OQ #2-b）：
- Velox region 可能跨多个 segment（region.length 通常 = stripe，segment 默认 32 MiB）；
- per-segment 派发让 pool 在更细粒度上调度，避免单个 8 segment region 占住一个 worker；
- segment 是 cache 内部的天然粒度，writer-collapse（§6.5）也是 per-segment 的。

### 6.4 ensureWithData 改为可中断同步等待

`DeferredStream::ensureWithData()`（`FsCacheBufferedInput.cpp:104-120`）当前
assert `slot_->segments` 已就绪；phase-2 改为：

```cpp
void ensureWithData() {
  if (inner_ != nullptr) {
    return;
  }
  VELOX_CHECK(
      !slot_->segments.empty(),
      "Stream used before FsCacheBufferedInput::load()");

  // 等待所有 segment 进入 kDownloaded（或失败）
  for (auto& seg : slot_->segments) {
    seg->waitForDownload();  // phase-2 新增 helper，见下方注解
    if (seg->state() == FileSegment::State::kFailed) {
      VELOX_FAIL("FsCache segment download failed: {}", seg->describe());
    }
  }

  inner_ = std::make_unique<FsCacheInputStream>(/* ... */);
  // ...既有 SkipInt64 replay 逻辑不变...
}
```

**`FileSegment::waitForDownload()` 是 phase-2 新增 helper**——phase-1
现状是 `FsCache.cpp:209` 手写 `segment->cv_.wait(lock, [&]{ ... })`
直接消费 `mutex_/cv_` 公开字段；`FileSegment.h:114-116` 注释明确预留
此 helper 引入点，并配合 `notifyAll()` helper 把 `mutex_/cv_` 转为 private。
新 helper 内部封装 cancel-check 循环（下文）。

**reader 阻塞在 Next() 第一次访问是 phase-2 接受的**（OQ #1 决策）——
hit-path（异步 hits）和 miss-prefetched（提前 enqueue 多 region，第一次
访问时已下完）两条主路径都不退化；只有"reader 串行读一个新文件"这种
cold-start case 会等待，与 phase-1 行为等价（phase-1 是同步 load 等待，
phase-2 是 lazy wait，总耗时相同）。

**可中断性**：`waitForDownload()` 接受 cancel token——Velox 算子取消
（`Driver::isTerminate()` 或 future 取消）时，wait 必须能尽快返回，
避免泄漏线程。具体实现：`cv_.wait_for(100ms)` 循环 + check cancel
（沿用 `AsyncSource` 模式）。

### 6.5 写者合并：beginDownload CAS + insert under lock

避免两个 reader 同时 miss 同一 segment 后都启动下载（OQ #2-d）。
`lookupOrCreate` 对外提供 `(std::string_view path, ...)` 重载（path 内部
hash 为 PathKey），核心实现签名走 PathKey，覆盖一段连续
`[offset, offset+length)` 字节，返回按 segment 边界切分的
`FileSegmentPtr` 列表：

```cpp
// FsCache::lookupOrCreate (新接口，替换 phase-1 getOrSet 的纯 lookup 部分)
std::vector<FileSegmentPtr> FsCache::lookupOrCreate(
    PathKey path, uint64_t offset, uint64_t length) {
  std::vector<FileSegmentPtr> result;
  LockedKey lockedKey = lockKeyMetadata(path);   // §4.3 RAII
  // 按 maxSegmentSize 切片，逐 segment lookup or insert
  for (auto [segOffset, segSize] : segmentRanges(offset, length)) {
    if (auto seg = lockedKey.findSegment(segOffset)) {
      result.push_back(std::move(seg));          // hit: 直接返回
      continue;
    }
    // miss: 新建 kDownloading segment，**插入 metadata in same lock**
    auto seg = std::make_shared<FileSegment>(
        path, segOffset, segSize, FileSegment::State::kDownloading);
    lockedKey.insertSegment(seg);
    // CAS-style ownership：beginDownload 在 segment_guard 内 CAS 把
    // currentThreadId 写到 phase-2 新增字段 downloadOwnerThread_。
    // 胜者：beginDownload() 返回 true；isDownloadOwnedByCurrentThread()
    // 后续也返回 true。败者（其他 thread 在 lookupOrCreate 看到
    // kDownloading）：在 ensureWithData 走 waitForDownload 等通知。
    seg->beginDownload(/*ownerThread=*/std::this_thread::get_id());
    result.push_back(std::move(seg));
  }
  return result;
}
```

`beginDownload` 内部用 `segment_guard` 持锁 + atomic state 检查，保证
只有第一个 reach kDownloading 的 caller 拿到 ownership。其他后到者
`isDownloadOwnedByCurrentThread()` 返回 false，跳过派发，直接 wait。

### 6.6 同步 miss 路径（caller-thread download）

`FsCache::getOrSet()` 老接口在 R0 之外仍可能被同步路径调用（如 SSD
recovery、benchmark 直接构造 FsCache 调用）。这种情况下，调用线程
亲自下载——本 spec 不引入"sync 接口走异步池等结果"的复杂化：

```cpp
// 仍保留同步 API，给 non-BufferedInput caller 用
std::vector<FileSegmentPtr> FsCache::getOrSet(
    std::string_view path, uint64_t offset, uint64_t length,
    ReadFile& readFile) {
  auto segments = lookupOrCreate(/* ... */);
  for (auto& seg : segments) {
    if (seg->state() == FileSegment::State::kDownloading
        && seg->isDownloadOwnedByCurrentThread()) {
      seg->download(readFile);              // caller 亲自下，写盘，notify
    } else {
      seg->waitForDownload();               // 别人在下，等
    }
  }
  return segments;
}
```

`FsCacheBufferedInput` 之外的调用方（如 `loadFromDisk` 启动期、test
fixtures）继续走 sync 路径。

### 6.7 isBuffered 诚实化

Phase-1 `FsCacheBufferedInput::isBuffered()` 无条件返回 true
（`FsCacheBufferedInput.cpp:160-166`），是为了让 reader 走 `enqueue+load`
而不是直接 pread。Phase-2 保持返回 true，但**注释更新**：phase-2 之后
`load()` 是异步的，"buffered" 的语义从"同步已经在内存"变为"将走 cache
路径（异步 prefetched 或 lazy fetched）"。

不改成"按 metadata 查询是否真的在 cache 里"——这会引入 hit-path 多余
metadata lookup，得不偿失。

## 7. SLRU 淘汰策略（plan-3）

### 7.1 EvictionPolicy 实例追加 SlruPolicy

Phase-1 已有 `EvictionPolicy` 接口（`LruPolicy` 实现）。Phase-2 接口
不变，新增 `SlruPolicy` 实现：

```cpp
class SlruPolicy : public EvictionPolicy {
 public:
  SlruPolicy(uint64_t probationaryBytes, uint64_t protectedBytes);

  void onInsert(FileSegment* seg) override;   // → probationary 队尾
  void onHit(FileSegment* seg) override;      // probationary→protected 升级
  void onRemove(FileSegment* seg) override;
  std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) override;

 private:
  // 两条独立 LRU 链表 + 各自 index_
  LruList probationary_;
  LruList protected_;
};
```

CH `SLRUFileCachePriority` (`SLRUFileCachePriority.h:10-14`) 同款双段
LRU：第一次访问入 probationary，第二次访问升 protected；protected 满
时降级队尾元素回 probationary。

### 7.2 配置项

```cpp
struct FsCacheConfig {
  // ...
  enum class Policy { LRU, SLRU };
  Policy policy{Policy::SLRU};                  // 默认 SLRU，对齐 CH
  double protectedRatio{0.5};                   // protected 段占比；
                                                // probationary = 1 - ratio
};
```

`FsCache` 构造时按 `config.policy` 实例化每 bucket 的
`EvictionPolicy`。

### 7.3 升级时机

`onHit` 内部判断 segment 当前在 probationary 还是 protected：
- probationary 命中 → erase from probationary，push_front protected；
  若 protected 超容量，pop_back protected → push_front probationary
  （降级，不直接淘汰）。
- protected 命中 → erase + push_front protected（普通 LRU bump）。

这条逻辑全在 per-bucket priorityMutex 保护下完成。`recordHit` 路径仍
走 §4.2 的 try_lock 合并并发 hit，所以 SLRU 升级也享受 try_lock 收益。

### 7.4 跨 bucket evict 公平性

§4.6 的轮询 evict 在 SLRU 下行为：每 bucket 各自先打 probationary，
collected bytes 不够再回头打 protected。spec 不规定"全局先打 prob 再
打 protected"的复杂排序——CH 自己也是 per-instance 决定。

## 8. FileCacheQueryLimit + bypass_cache_threshold（plan-4）

### 8.1 Per-query 配额数据结构

```cpp
// 挂在 QueryCtx 上的 per-query cache 配额
class FileCacheQueryLimit {
 public:
  explicit FileCacheQueryLimit(uint64_t maxBytes);

  // tryReserve 阶段调用：当前 query 已占 cache bytes + needed 是否超限
  bool tryReserve(uint64_t needed);
  void release(uint64_t bytes);

 private:
  uint64_t maxBytes_;
  std::atomic<uint64_t> currentBytes_{0};
};
```

接入点：`FsCache::tryReserveBytes(needed, queryLimit*)` 内部先调
`queryLimit->tryReserve(needed)`（per-query），通过后再走原 phase-1 全局
水位检查。`queryLimit` 由 caller（`FsCacheBufferedInput` 通过 connector
chain 拿到 `QueryCtx`）传入；nullptr 时仅做全局检查（phase-1 行为）。

### 8.2 配额耗尽行为

当 `tryReserve` 因 query 配额耗尽返回 false，**当前 download 失败，
不入 cache，但读盘直读远端继续返回**。这等价于"该 segment 走 cache
miss-no-store"——reader 拿到数据但下次仍 miss。CH 同款行为
（`FileCache.cpp` `tryReserveImpl` 失败后 caller 直读 fallback）。

### 8.3 bypass_cache_threshold

```cpp
struct FsCacheConfig {
  // ...
  // 单次 region 字节数超此阈值时绕过 cache，直读远端，结果也不写盘。
  // 默认 0 = 不绕过（保持 phase-1 行为）。
  uint64_t bypassThreshold{0};
};
```

接入点：`FsCacheBufferedInput::load()` 入口检查每 enqueued region：

```cpp
if (cfg.bypassThreshold > 0 && enqueued.region.length > cfg.bypassThreshold) {
  // 不走 fsCache_->lookupOrCreate；标记 enqueued 为 bypass，
  // DeferredStream::ensureWithData 改走 readFile->pread 直读
  enqueued.bypass = true;
  continue;
}
```

`DeferredStream` 需要新字段表示 bypass 路径——读盘逻辑直接复用
`DirectBufferedInput`（已有）。这相当于一次 region 粒度上的"是否走
cache" 决策，零 metadata 开销，对大读 friendly。

### 8.4 QueryLimit 与 bypass 优先级

bypass 在 `load()` 入口短路，优先于 queryLimit 检查——因为 bypass 的
本意就是"这种读根本不应该污染 cache"。queryLimit 只在真正要写入
cache 的 reserve 阶段生效。

## 9. 测试策略

### 9.1 单元测试新增

| 测试 | 覆盖 |
|---|---|
| `LockOrderCheckerPerBucketTest` | per-bucket / per-key 实例化后 rank stack 仍然正确 |
| `KeyMetadataLockedKeyTest` | LockedKey RAII 释放、跨 key 切换语义 |
| `SlruPolicyTest` | probationary/protected 升降级、selectVictims |
| `DownloadPoolFallbackTest` | downloadExecutor=nullptr 时 fallback 到 ioExecutor |
| `WriterCollapseTest` | 多线程同 segment miss 只下载一次 |
| `EnsureWithDataCancelTest` | 算子取消时 waitForDownload 及时返回 |
| `QueryLimitReserveTest` | per-query 配额耗尽时 miss-no-store 行为 |
| `BypassThresholdTest` | 超阈值 region 走 pread fallback、不入 cache |

### 9.2 回归测试

phase-1 测试**全部保持通过**，无修改。重点 gate：
- `FsCacheConcurrencyTest`（含 `sameSegmentMultipleReadersExactlyOneDownload`、
  `evictionUnderConcurrentLoadIsRaceFree`，phase-1 关键回归保护）
- `FsCacheRecoveryTest`（crash recovery + fileName 兼容性保护；见 I4 的
  cache 文件迁移说明）

### 9.3 性能 gate

phase-1 microbench 直接 re-run：
- 16 线程 ws=0.5 sequential hit ≥ 6 M ops/s（§3 量化目标）
- 1 线程 hit ≥ 7.0 M ops/s（不退化）

新增 microbench cell：
- Mixed hit/miss with prefetch enabled（验证 R0 异步化生效，
  miss→hit 比例 ≥ 80%）。
- Scan-resistant workload：80 % working-set 命中 + 20 % cold scan 流过
  （验证 SLRU 不被 scan 污染：protected hot 部分命中率 ≥ LRU baseline 的
  1.1×）。
- bypass_cache_threshold 触发场景：region 大小跨阈值 ±10 % 两组对比
  （验证大读不入 cache、metadata 不增长、读穿透延迟与 pread 等价）。

### 9.4 死锁与 race 测试

`ThreadSanitizer` 跑全套 fscache 测试；CI 跑 ASAN/TSAN 两套。
LockOrderChecker debug 断言全开。

## 10. 风险与已接受的复杂度

### R1：锁实例化引入的元数据开销

per-bucket mutex × 1024 buckets ≈ 1024 × 40 B = 40 KB；per-key
KeyMetadata 开销随 keys 数量线性增（每 key ≈ shared_ptr 控制块 + map
header + KeyGuard ≈ 120 B）。

**接受**：10 K 活跃 keys 时 ≈ 1.2 MB，相比 cache 容量（默认 GiB 级）
忽略不计。这是用空间换并发的有意识 trade-off。

### R2：跨 bucket evict 在极端场景下退化

§4.6 try_lock 跳过繁忙 bucket，N=numBuckets 轮后 fallback 到 blocking。
极端 case（所有 bucket 同时被 hit 路径压住）evict 可能等待较久。

**缓解**：fallback 仅在 fallback 周期生效；监控 `eviction.fallback.count`
metrics，CI 上 stress test 触发率 < 1 % 即可。

### R3：waitForDownload cancellation 死锁风险

reader 在 `cv.wait_for(100ms)` 循环 + check cancel。如果 downloader
线程因池 saturate 永远不调度，reader 卡 cancel poll 直到 cancel
触发——不死锁但延迟最大 100 ms。

**接受**：100 ms cancel 延迟可接受；若不可接受，加 future-based 取消
（plan-2 within scope，但本 spec 不强制）。

### R4：writer-collapse 漏跑导致重复下载

`beginDownload` 在 segment lock 内做 CAS，理论上不会漏；但 segment
本身可能被 evict 重建（segment ptr 复用 path+offset，但内存对象不
同）。`WriterCollapseTest` 必须覆盖 evict-then-recreate 场景。

**接受**：测试覆盖即可，无额外架构改动。

### R5：KeyMetadata 持有 std::map 而非 unordered_map

`std::map<offset, FileSegmentPtr>` 用红黑树，单 key 多 segments 时常数
比 unordered_map 大。

**接受**：单 key 下 segments 数量通常 < 100（4 MiB segment × 100 ≈
400 MiB / file），红黑树常数差异可忽略；range-scan friendly
（按 offset 排序）反而是收益。

### R6：bypassThreshold 没有 hit/miss 智能判断

bypassThreshold 是粗暴 size gate（同 CH `bypass_cache_threshold`）。
"大但热"的 region（如 fact table dim join 的小维表 stripe）会被错过。

**接受**：本 spec 不实现 StarRocks `datacache_skip_read_factor` 那种
hit/miss 反馈式 bypass。**若实测发现 bypassThreshold 误伤明显**，
作为 phase-3 拓展独立 spec。

### R7：phase-1 → phase-2 cache 文件不兼容

`FsCacheKey.cpp:28` 的 `combinedHash(path, offset, size)` 把 3 元组混入
一个 64-bit hash。phase-2 改为 PathKey-only hash 后，同一 `(path, offset,
size)` 在 phase-1 / phase-2 生成的 fileName 前缀**不同**。phase-2 启动
扫到 phase-1 的 cache 文件无法重建索引（因为反解只能拿到 hex 前缀
而非 path 本身）。

**关键约束**：phase-2 启动时盘上 fileName 只含 `<16-hex>.<offset>.<size>`
（见 `FsCache.cpp:325` `parseFileName`），**没有 path 字符串**——所以
"phase-1 hash vs phase-2 hash" 在 disk-walk 阶段根本无法区分（两版
fileName schema 完全一致，且无 path 可重算比对）。

**接受弃用，落地方案**：phase-2 启动时若检测到 `cacheRoot` 非空，
**整体清空** 后再进入正常 `loadFromDisk` 流程（盲扫清空，与"接受弃用 +
不引入兼容代码"立场一致）。release notes 显式标注 "phase-2 upgrade
首次启动会清空所有 phase-1 写入的 cache 文件"。

可选替代方案（本 spec 不选）：fileName 加 `v2-` 版本前缀以便甄别。
代价：破坏 §5.5 "schema 不变"承诺，且未来每次 hash 函数变更都要
推进一次版本号——复杂度不值。

### R8：per-bucket SLRU 稀释 scan-resistance

经典 SLRU 靠**全局** probationary 抵御扫描：scan 进 prob，hot 进 prot。
本 spec §7 分散到 1024 个 per-bucket micro-SLRU，**单 bucket 内
probationary 窗口很小**（容量 = 全局 / 1024），扫描项可能在 bucket 内
被错误升级到 protected。

**缓解**：§9.3 加了 scan-resistance microbench（80% WS + 20% cold scan）
作为经验 gate。若实测 SLRU 收益不显著，phase-3 可改 hybrid
（per-bucket prob + global protected），但本 spec 不预先实现。

## 11. Out of scope：Phase 2.5（`PARTIALLY_DOWNLOADED` + 续传）

CH 的 `FileSegment::State::PARTIALLY_DOWNLOADED` + `canStartFromCache`
允许 reader 在 segment 还在下载中就消费已落盘的前 N 字节，剩余由
`download_threads` 后台续传。这是 CH 预取效果的关键拼图，但
phase-2 不实现，理由：

1. **状态机改动大**：`FileSegment` 现在只有 `kDownloading` / `kDownloaded`
   / `kFailed`；引入 PARTIALLY_DOWNLOADED 需要新状态、新 cv 信号粒度
   （从 segment 级到 byte-offset 级）、partial-write 的 atomic visibility
   保证。
2. **续传线程池语义**：CH 单独有 `download_threads = 5` 后台续传池；
   Velox 这边要么再开一个 executor，要么和 §6.2 的 downloadExecutor 复用
   并区分任务类型——任何一种都引入新的调度复杂度。
3. **Crash recovery 影响**：partial-downloaded 文件在 phase-1
   `loadFromDisk` 中是按 size mismatch 删除的；phase-2.5 要改为保留
   并续传，需要扩展 fileName 编码或 sidecar metadata。
4. **测试矩阵翻倍**：每条并发路径都要测 "segment in PARTIAL" 的子状态。

**Phase 2.5 独立 spec 待写**：在 phase-2 落地稳定后启动 brainstorming，
届时再分配日期与文件名（`docs/superpowers/specs/<YYYY-MM-DD>-fscache-partial-downloaded.md`）。
phase 2 的 §6 已经把 80 % CH 预取效果搬过来（reader-driven async download），
phase 2.5 是补最后那 20 %（partial read-through）。

---

## 附录 A：实施切片

按用户决策（S1：按 CH 拓扑分层），本 spec 落地为 4 个 plan：

| Plan | 覆盖 | 大致范围 |
|---|---|---|
| plan-1 | §4 + §5（锁拓扑 + 数据结构 + FsCacheKey 拆分） | C1 + C2 + C3 + PathKey/offset 拆分 |
| plan-2 | §6（后台下载 + R0 异步 load） | DownloadThreadPool hook + load() async + waitForDownload + writer-collapse |
| plan-3 | §7（SLRU） | SlruPolicy + config wire-up |
| plan-4 | §8（QueryLimit + bypass） | FileCacheQueryLimit + bypassThreshold |

plan-1 是其他 plan 的基础（所有锁/数据结构改动）；plan-2/3/4 在
plan-1 落地之后可以并行展开，**但本仓库实际推进顺序仍由 user 决定**
（推荐 plan-1 → plan-2 → plan-3 → plan-4，性能收益曲线最陡）。

## 附录 B：Deferred work（跨 plan）

下列工作不属于任何已立项 plan 的硬性范围，但会作为后续 plan 的伴生项落地。
列在这里以便相关 PR 在落地时回头取消对应的 TODO / 重新打开被关掉的测试。

### B.1 In-flight reservation accounting（在途字节预留）

**Why deferred**：phase-1 / plan-1 的 `recordMiss()` 在 `download()` 完成后
才把 segment 大小记入 `counters_.bytesOnDisk`。N 个并发 miss-path writer 因此
都能在自己的 `evict()` 里看到 `bytesOnDisk == 0`、各自跳过淘汰，再一起把计数
器抬过 `maxBytes`。下一次 miss 的 `evict()` 会观察到真实总量并把上限拉回，
所以超量是瞬时且自愈的（量级：`maxBytes + N * segmentSize`，在生产 100 GiB
cache 上 64 个 writer × 8 MiB = 512 MiB 顶到 cap 之上）。

**修复方案**：引入 `counters_.reservedBytes` 原子计数器；miss-path writer 在
`download()` 之前 `fetch_add(segmentSize)` 预占，`download()` 失败回滚、成功
则把同等字节从 `reservedBytes` 转移到 `bytesOnDisk`。`evict()` 判定条件改为
`bytesOnDisk + reservedBytes + bytesNeeded > maxBytes`。warm-restart 时
`loadFromDisk` 留下的 orphan 文件也走同一个计数器（由 plan-1 Task 10
`.fscache_version` sentinel 触发盲清后，phase-2 不再有 orphan，但接口对齐
方便后续 phase 2.5 续传）。

**承载位置**：plan-2（后台下载线程池）的自然伴生项——writer-collapse 与
async dispatch 都要重新进出 reservation 路径，一起做改动范围最小。

**TODOs to retire when this lands**：

- `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp`
  `DISABLED_roundRobinEvictUnderConcurrentInserts`：去掉 `DISABLED_` 前缀
  并删除顶部的 deferred 注释。该测试的最终断言
  `bytesOnDisk <= maxBytes` 在 reservation 落地之后才成立。
- `evictionUnderConcurrentLoadIsRaceFree` 中的 CAVEAT block + 放宽过的上界
  （`maxBytes + kThreads * kSegmentSize`）：收紧为
  `<= maxBytes`，删除 CAVEAT 段。
- `FsCache::evict()` 注释里的 "CAVEAT: only single-writer tight" 段
  （`FsCache.h` private 区）：删除。
- `FsCache::loadFromDisk()` 注释里关于 orphan-bytes 计数器的 phase-2 TODO
  （`FsCache.h` public 区）：与本计数器一起落地。
