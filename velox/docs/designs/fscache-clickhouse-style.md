# ClickHouse 风格 FileSystem Cache（FsCache）设计

**日期**：2026-05-22
**作者**：Chang Chen
**状态**：Draft（待评审）

## 摘要

在 Velox 引入一套全新的本地缓存模块 `velox/common/caching/fscache/`，参照
ClickHouse FileSystem Cache 的整体设计哲学，通过新的 `BufferedInput` 子类
接入读路径，**完全旁路**现有的 `AsyncDataCache + SsdCache`。第一阶段只做
SSD 层（FS cache 形态），第二阶段补 Userspace Page Cache 形态作为 RAM 层。

本设计**不替换**现有 Velox 缓存代码 —— 旧路径默认行为不变，通过 ReaderFactory
配置项选择新旧后端。

## 背景与动机

### CacheLib PoC 的教训（必须复述）

仓库 `~/SourceCode/.ai/share_data/local-cache/claude/poc-cachelib/2026-05-20-stage-audit.md`
对 CacheLib PoC 的审计结论：

- **集成代价过高**：CachePin 的 friend gate 导致无法从外部 manufacture pin，
  全量 caller 迁移被判定为「not worth 1500 lines of rewrite」（Phase D-γ commit）
- **E2E 验证缺失**：CacheLib 后端**从未在真实 query path 跑过**，全部数据来自
  合成 workload，违反「跳过验证为耻」原则
- **多线程下优势消失**：AsyncDataCache 的 `findOrCreate` 内置 loading map 在
  zipfian 多线程下命中率反而高 3–5pp

本设计**不重蹈这两条覆辙**：
1. **接入点**：用 `BufferedInput` 子类，绕开 `CachePin` friend gate
2. **正确性 gate**：第一阶段最后一个 commit 是「`FsCacheBufferedInput` vs
   `CachedBufferedInput` 字节对比等价测试」

### 为什么选 ClickHouse FileSystem Cache 设计

参考材料：`~/SourceCode/.ai/share_data/local-cache/claude/03-clickhouse.md`、
`~/SourceCode/ClickHouse/src/Interpreters/FileCache/`（约 11853 行 C++）。

CH FileCache 是工业验证最完整的「单节点本地 SSD cache」实现，相比 Velox 现状
的优势：

| 维度 | Velox `SsdFile` | ClickHouse FileCache |
|---|---|---|
| 淘汰策略 | 修改版时钟，无扫描抗性 | **SLRU 默认**，扫描抗性强 |
| 锁粒度 | 4 shard × `std::mutex` | **5 把锁层次** + per-key + per-segment |
| 磁盘布局 | 单文件 + 64 MiB region 整体淘汰 | per-segment 文件，单文件淘汰 |
| 续传 | 无 | `PARTIALLY_DOWNLOADED` 状态机 + cv |
| Per-query 配额 | 无 | `FileCacheQueryLimit` |
| 崩溃恢复 | CPT1/CPT2 + eviction log（复杂） | 目录扫描 + size 判完整（简单）|

## 设计决策记录

所有决策来自与用户的 brainstorm（10 个 Q），记录如下：

| Q | 决策 | 备注 |
|---|---|---|
| Q1 | "Userspace page cache" = ClickHouse UPC 概念 | `https://clickhouse.com/docs/operations/userspace-page-cache` |
| Q2 | 完全替换路线 + BufferedInput 子类 | 绕开 CachePin friend gate |
| Q3 | RAM + SSD 分层；第一阶段只做 SSD | UPC 留第二阶段 |
| Q4 | CH 完整 background download（含状态机/cv/后台线程池）| 第一阶段实现简化为同步全写 |
| Q5 | per-segment 文件 + 两级目录 | hash 前 4 字符切目录 |
| Q6 | CH 风格变长 + splitRange | 4 MiB align + ≤32 MiB max |
| Q7 | 无 file_version（path + offset + size）| 对齐 Velox 现状语义，不动 `ReadFile` |
| Q8 | SLRU 默认 | 第一阶段先填 LRU，接口预留 |
| Q9 | CH 完整 5 把锁层次 | 第一阶段简化实现（共享 mutex），第二阶段细分 |
| Q10 | 无 manifest，目录扫描重建 | `.tmp` 后缀清理 + size 校验 |
| **路径** | **路径 3（渐进 + 接口完整）** | **全部完成前不做性能测试，只做 UT，依次 commit** |

**关键铁规**：
- **不做性能测试，直到第二阶段全部完成**
  - 原因：部分实现的性能数字会被误判为「方向不行」（CacheLib PoC 的教训反例）
  - 约束：第一阶段任何 commit 不发 benchmark 数字、不写性能 commit message
- **依次 commit，每个 commit UT 全绿**
  - 第一阶段切 10 个 commit（见下方「实施计划」）

## 顶层架构

```
                     ┌──────────────────────────┐
                     │   ReaderFactory          │
                     │   按配置项选 BufferedInput│
                     └──────────┬───────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │                   │                   │
   ┌────────▼──────┐  ┌─────────▼───────────┐  ┌───▼────────────┐
   │CachedBuffered │  │ FsCacheBufferedInput│  │DirectBuffered  │
   │Input          │  │ (NEW)               │  │Input           │
   │(unchanged)    │  └─────────┬───────────┘  │(unchanged)     │
   └────────┬──────┘            │              └───┬────────────┘
            │                   │                  │
   ┌────────▼──────┐  ┌─────────▼───────────┐  ┌───▼────────────┐
   │AsyncDataCache │  │ FsCache             │  │(直接读远端)    │
   │+ SsdCache     │  │ (NEW)               │  └────────────────┘
   │(unchanged)    │  │ ┌────────────────┐  │
   └───────────────┘  │ │ FsCacheMetadata│  │
                      │ ├────────────────┤  │
                      │ │ FileSegment    │  │
                      │ ├────────────────┤  │
                      │ │ SlruPriority   │  │
                      │ ├────────────────┤  │
                      │ │ FsCacheGuards  │  │
                      │ ├────────────────┤  │
                      │ │ DownloadPool   │  │
                      │ │ (Phase 2)      │  │
                      │ └────────────────┘  │
                      └─────────────────────┘
```

**关键边界**：
- `velox/common/caching/fscache/` 不依赖 `AsyncDataCache.h` 或 `SsdCache.h`
- `velox/dwio/common/FsCacheBufferedInput.{h,cpp}` 是唯一接入点
- 不动 `BufferedInput` 基类（17 个 virtual 已经够用）
- 不动 `ReadFile` 抽象（Q7=A 决定不加 `fileVersion()`）

## 文件组织

```
velox/common/caching/fscache/
├── FsCache.{h,cpp}                # 顶层入口（≈ CH FileCache）
├── FsCacheMetadata.{h,cpp}        # 索引（bucket → key → segments）
├── FsCacheKey.{h,cpp}             # path + offset + size hash
├── FileSegment.{h,cpp}            # 状态机 + cv
├── FsCacheGuards.h                # 5 把锁层次类型 + 锁顺序文档
├── EvictionPolicy.h               # 淘汰策略接口
├── LruPolicy.{h,cpp}              # 第一阶段实现
├── SlruPolicy.{h,cpp}             # 第二阶段补
├── DownloadThreadPool.{h,cpp}     # 第二阶段补
├── FsCacheConfig.h                # 配置项
└── tests/
    ├── FsCacheKeyTest.cpp
    ├── FsCacheMetadataTest.cpp
    ├── EvictionPolicyTest.cpp
    ├── FileSegmentTest.cpp
    ├── FsCacheConcurrencyTest.cpp
    ├── FsCacheBufferedInputTest.cpp
    ├── FsCacheRecoveryTest.cpp
    ├── FsCachePersistenceTest.cpp
    ├── FsCacheSplitRangeTest.cpp
    ├── FsCacheEvictionTest.cpp
    └── FsCacheEquivalenceTest.cpp  # 关键：vs CachedBufferedInput

velox/dwio/common/
├── FsCacheBufferedInput.{h,cpp}   # BufferedInput 子类
└── FsCacheInputStream.{h,cpp}     # 配套 SeekableInputStream
```

## 核心数据结构

### FsCacheKey（Q7=A：无版本）

```cpp
struct FsCacheKey {
  std::string path;     // 远端文件路径
  uint64_t offset;      // segment 起始字节
  uint64_t size;        // segment 字节数

  // hash = SipHash(path) ^ hash(offset) ^ hash(size)
  uint64_t hash() const;
};
```

未来扩展 `(path, file_version)` 不需要重构 —— 添加可选字段即可。

### 五把锁层次（Q9=A）

锁顺序（最外层先获取）：

```
CachePriorityGuard > CacheStateGuard > CacheMetadataGuard > KeyGuard > FileSegmentGuard
```

| 锁 | 保护对象 | 持有时长 |
|---|---|---|
| `CachePriorityGuard` | SLRU 两条队列链表结构 | 极短 |
| `CacheStateGuard` | total bytes / total elements 计数器 | 极短 |
| `CacheMetadataGuard` | bucket 数组 | 短 |
| `KeyGuard` | 单 key 的 `std::map<offset, FileSegmentPtr>` | 中 |
| `FileSegmentGuard` | 单 segment 状态字段 + cv | 长（跨远端 IO）|

**第一阶段简化**：5 把锁的**类型和命名**严格按 CH 拓扑建好（`FsCacheGuards.h`），
每把锁是独立的 `class` 包装独立的 `std::mutex` 实例（不共享底层 mutex），但
不细分到 per-bucket / per-key 实例 —— 例如 `CacheMetadataGuard` 是一个全局
`std::mutex` 而非每 bucket 一个。第二阶段升级到 per-bucket / per-key 实例化
时只改 `FsCacheGuards.h` 内部和 `FsCacheMetadata` 持锁逻辑，调用方代码不动。

锁顺序在 `FsCacheGuards.h` 头注释中文档化，第一阶段通过 `LockOrderChecker`
RAII（debug build only）静态检查持锁顺序，避免死锁。

### FsCacheMetadata（两层索引）

```
bucket[N]  →  unordered_map<FsCacheKey, KeyMetadataPtr>
                                ↓
                       std::map<offset, FileSegmentPtr>
```

- N 默认 1024（对齐 CH `Metadata.h:236-252`）
- bucket 编号 = `hash(key) % N`
- 第一阶段 bucket 共享一把 `MetadataGuard`，第二阶段细分到 per-bucket

### FileSegment 状态机（第一阶段简化版）

```
              ┌──────────┐
              │   EMPTY  │
              └────┬─────┘
                   │ download trigger
                   ↓
              ┌──────────┐
              │DOWNLOADING│
              └────┬─────┘
                   │ download complete + rename atomic
                   ↓
              ┌──────────┐
              │DOWNLOADED│
              └──────────┘

              ┌──────────┐
              │ DETACHED │  (eviction 时 refCount > 0)
              └──────────┘
```

第一阶段不进入：`PARTIALLY_DOWNLOADED`（第二阶段补，用于续传）。

字段（接口对齐 CH `FileSegment.h:281-299`）：

```cpp
class FileSegment {
  FsCacheKey key_;
  std::atomic<State> downloadState_;
  std::atomic<size_t> hits_;             // SLRU 升段用，第一阶段未消费但字段在
  std::atomic<size_t> refCount_;
  std::atomic<size_t> downloadedSize_;
  std::condition_variable cv_;           // 第一阶段 wait/notify_all 已使用
  FsCacheGuards::FileSegmentMutex mutex_;
};
```

### EvictionPolicy 接口（Q8=A）

```cpp
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;
  virtual void onInsert(FileSegment*) = 0;
  virtual void onHit(FileSegment*) = 0;
  virtual std::vector<FileSegment*> selectVictims(size_t bytesNeeded) = 0;
  virtual void onRemove(FileSegment*) = 0;
};
```

第一阶段实现 `LruPolicy`；第二阶段实现 `SlruPolicy`（probationary + protected）。
调用方代码只依赖接口。

## 读路径

```
caller (Reader)
  ↓
FsCacheBufferedInput::enqueue(region)
  ↓
FsCacheInputStream（lazy）
  ↓ stream->Next() 触发
FsCache::getOrSet(path, range)
  ↓
splitRange(range)  →  List<Range>
  ↓
对每个 sub-range：
  ↓
lookupOrCreate(key)  →  FileSegmentPtr
  ↓
hit ? readFromLocalFile : download
```

### splitRange（Q6=A，CH 风格变长）

- 入参 `(offset, size)`
- 向外按 4 MiB align（`FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT`）
- hole 部分按 ≤32 MiB max（`FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE`）切多段
- 返回 `List<Range>`，逐个走 lookup/download

参考 CH `FileCache.cpp:559` `splitRange` 实现思路。

### 多 reader 同 segment 协调（第一阶段同步全写）

```
reader A 到 → KeyGuard → 创建 EMPTY segment → FileSegmentGuard →
  CAS EMPTY→DOWNLOADING → 释放 KeyGuard → 持 FileSegmentGuard download

reader B 到 → KeyGuard → 找到 segment → 试 FileSegmentGuard（阻塞）

reader A download 完 → CAS DOWNLOADING→DOWNLOADED → notify_all →
  释放 FileSegmentGuard

reader B 拿到 FileSegmentGuard → 看 DOWNLOADED → 读本地文件
```

语义等同 `AsyncDataCache` 的 loading map（防止窗口期重复 miss），同时是 CH 的
子集。

## 写路径（第一阶段同步全写）

### 磁盘布局（Q5=A）

```
<cache_root>/<hash[0:2]>/<hash[2:4]>/<full_hash>_<offset>_<size>
```

- `full_hash` = SipHash(path) 的 hex（32 字符）
- 两级目录每级 256 子目录 → 单目录文件数控制在万级
- 不用 path 做文件名（含 `/` `:` 字符且长度不可控）

### 写流程

```
FileSegment::download(remote_reader):
  1. FileSegmentGuard
  2. compute local_path
  3. mkdir -p 两级目录
  4. open(local_path + ".tmp", O_WRONLY|O_CREAT|O_TRUNC)
  5. while not done:
       buf = remote_reader.pread(...)
       write(fd, buf)
       downloadedSize_ += chunk
  6. fsync + close
  7. rename(.tmp, final)        ← atomic publish
  8. CAS state EMPTY → DOWNLOADED
  9. cv.notify_all
  10. release FileSegmentGuard
```

**`.tmp + rename` 模式的优势**（比 CH 的 size-only 判完整更稳）：
- 崩溃后看到 `.tmp` 即未完成，unlink 即可
- 不存在「文件 size 到位但 metadata 未刷盘」的中间态
- 是 Linux fs 上 atomic publish 的标准模式

### 失败处理

- 远端 pread 失败 → unlink `.tmp` → state 回 EMPTY → throw
- 本地 write 失败（磁盘满） → 同上 + 触发紧急 eviction
- fsync 失败 → 同上 + metrics

### Eviction 与 unlink

1. EvictionPolicy 选 victim
2. FileSegmentGuard → CAS DOWNLOADED → DETACHED
3. 若 refCount_ == 0：unlink 文件 + 从 metadata 删除
4. 若 refCount_ > 0：保留 DETACHED（有 reader 持引用），最后引用释放时 unlink

per-segment 文件 + unlink 模式**天然解决**了 Velox `SsdFile` 单文件复用导致的
SSD-WA 问题（候选项 `SSD-1` 提议的 `fallocate(PUNCH_HOLE)` 不再需要）。

## 持久化与暖启（Q10=A）

### 启动期目录扫描

```
FsCache::loadFromDisk(cache_root):
  for top_dir in iter(cache_root):
    for sub_dir in iter(top_dir):
      for file in iter(sub_dir):
        if file.endswith(".tmp"):
          unlink(file)                     # 未完成写
          continue
        try:
          (full_hash, offset, size) = parseFileName(file)
        except:
          log.warn(...); continue
        if stat(file).st_size != size:
          unlink(file)                     # size mismatch
          continue
        segment = FileSegment::recoverFromDisk(...)
        metadata.insert(segment)
        evictionPolicy.onInsert(segment)
```

不做 checksum（CH 也不做，path immutable 假设）。无 `file_version` 校验。

### path 反查

文件名只含 `hash(path)`，重启后无法从文件名反推 path。**这不影响 cache 工作**
—— 下次 reader 用 `hash(path)` 查表命中即可。仅 admin tool 看不到原始 path。
第二阶段如需，可在每个 cache 文件旁加 `<hash>.path` 小文件。

### SLRU 顺序恢复

第一阶段 LRU：所有 entry 重启进队尾（目录扫描顺序），用 `shuffle()` 打乱避免
「目录顺序 = 淘汰顺序」（借鉴 CH `FileCache.cpp:1603`）。

第二阶段 SLRU：所有 entry 重启进 probationary，第二次命中再升 protected。

### 崩溃模型

| 崩溃时机 | 行为 |
|---|---|
| download 中（写 `.tmp`） | 重启 unlink，等同 cache miss |
| rename 后 | 重启识别 DOWNLOADED，下次命中 |
| eviction unlink 前 | 重启目录扫描自然不含该文件 |
| eviction unlink 后 metadata 删前 | 重启目录扫描自然不含 |
| eviction metadata 删后 unlink 前 | 重启目录扫描重新登记（over-recovery，cache 行为正确）|

## 单元测试策略

第一阶段唯一的正确性 gate（不做性能测试）。

### UT 文件清单

| 文件 | 测试内容 |
|---|---|
| `FsCacheKeyTest.cpp` | hash 稳定性、collision 边界 |
| `FsCacheMetadataTest.cpp` | bucket / per-key map 增删查 |
| `EvictionPolicyTest.cpp` | LruPolicy 行为；第二阶段填 SLRU 行为 |
| `FileSegmentTest.cpp` | 状态机转换 |
| `FsCacheConcurrencyTest.cpp` | 多 reader 5 把锁正确性 |
| `FsCacheBufferedInputTest.cpp` | enqueue / load 行为 |
| `FsCacheRecoveryTest.cpp` | .tmp 清理、size mismatch |
| `FsCachePersistenceTest.cpp` | 多次启停可用 |
| `FsCacheSplitRangeTest.cpp` | CH splitRange 边界 |
| `FsCacheEvictionTest.cpp` | LRU 选 victim、DETACHED、并发 unlink |
| `FsCacheEquivalenceTest.cpp` | **vs CachedBufferedInput 字节对比** |

### 必须覆盖的边界

1. **splitRange 边界**：完全对齐 / 完全未对齐 / 半对齐 / hole 跨 max_size /
   hole 在文件尾不足 4 MiB
2. **并发 lookup 同 segment**：2/4/16 reader 同 key 同 offset → 只有一个 download
3. **并发 lookup 不同 segment**：不同 key 完全无阻塞
4. **崩溃恢复**：`.tmp` 残留 / rename 完成 / size mismatch / metadata 与 fs 漂移
5. **Eviction**：满容量触发 / victim 被读时 DETACHED / 并发 eviction 不重复 unlink
6. **等价测试**：同一 ReadFile 跑两次 query，新旧 BufferedInput 字节相同

### Velox 集成

- 接入 `velox_add_grouped_tests`，加进 `velox_caching_test` group
- BufferedInput 子类 UT 加进 `velox/dwio/common/tests/`

## 实施计划（第一阶段 10 个 commit）

| # | Commit subject | 主要文件 | 增量行数 | 验证 |
|---|---|---|---|---|
| 1 | `feat(fscache): scaffold module skeleton` | `fscache/CMakeLists.txt` + 各 `.h` 文件含 class 前向声明、namespace、include guard，无方法实现 | ~200 | 编译过 |
| 2 | `feat(fscache): FsCacheKey + hash` | `FsCacheKey.{h,cpp}` + UT | ~300 | KeyTest |
| 3 | `feat(fscache): FsCacheGuards (5 lock types)` | `FsCacheGuards.h` | ~100 | 编译过 |
| 4 | `feat(fscache): EvictionPolicy interface + LruPolicy` | `EvictionPolicy.h` + `LruPolicy.{h,cpp}` + UT | ~400 | EvictionPolicyTest |
| 5 | `feat(fscache): FileSegment 3-state machine + splitRange` | `FileSegment.{h,cpp}` + `FsCache::splitRange` + UT | ~700 | FileSegment + SplitRange |
| 6 | `feat(fscache): FsCacheMetadata + top-level FsCache` | `FsCache.{h,cpp}` + `FsCacheMetadata.{h,cpp}` + UT | ~800 | Metadata + Eviction |
| 7 | `feat(dwio): FsCacheBufferedInput` | `dwio/common/FsCache*.{h,cpp}` + UT | ~600 | BufferedInputTest |
| 8 | `feat(fscache): crash recovery via directory scan` | `FsCache::loadFromDisk` + UT | ~400 | Recovery + Persistence |
| 9 | `test(fscache): concurrent stress test` | UT only | ~300 | ConcurrencyTest |
| 10 | `test(fscache): equivalence vs CachedBufferedInput` | UT only | ~300 | EquivalenceTest |

**第一阶段总增量**：~4100 行（UT 占约 40%）

### Commit gates

- 每 commit `make debug && make unittest` 全绿
- commit 7 是第一次能跑 dwio test 的节点
- commit 10 通过 = 第一阶段完成的标志
- **任何 commit 不发性能数字、不写性能 commit message**

### 第二阶段（接口预留，不在本设计实施范围）

| 项 | 增量行数 |
|---|---|
| SlruPolicy 实现（替换 LruPolicy） | ~500 |
| DownloadThreadPool 后台下载 | ~600 |
| PARTIALLY_DOWNLOADED 续传 | ~400 |
| Per-query quota（FileCacheQueryLimit） | ~300 |
| 5 把锁物理细分（拆 bucket / key 锁） | ~200 |
| bypass_cache_threshold 配置 | ~50 |

第二阶段总增量预估：~2050 行。两阶段合计 ~6150 行（含 UT），低于 CH 原版
11853 行 —— 去掉了 Poco / Settings / ProfileEvents 等 CH 特有依赖适配。

### 第三阶段（性能验证）

第二阶段所有功能完成后，再启动 benchmark。届时与 `AsyncDataCache + SsdCache`
和 CacheLib PoC 数据三方对比。**不在本设计文档范围内**。

## 风险与显式接受的复杂度

### 风险 1：5 把锁层次的死锁面

锁顺序写错就死锁。CH 自己在 `Guards.h:52` 都要专门注释。本设计第一阶段
共享 mutex 缓和此风险，但第二阶段必须建立锁顺序的静态检查或文档化 review。

### 风险 2：CH 完整 background download 状态机的复杂度

`PARTIALLY_DOWNLOADED` + cv wait + 后台线程池 + 队列限额是 CH FileCache
最复杂的部分。第二阶段才进入，第一阶段接口预留但不实现。

**显式接受**：用户已认可此复杂度为「直接实现」的成本，理由是部分实现
PoC 容易被误判为「方向不行」。

### 风险 3：path 反查丢失

文件名只含 `hash(path)`，重启后丢失 path 反查能力。第一阶段不解决。admin
tool 可能受影响，但 cache 工作正确性不受影响。

### 风险 4：与 Velox 现有缓存的长期并存

`AsyncDataCache + SsdCache`、`CacheLibBackend`、`FsCache` 三套并存增加维护
成本。本设计第一阶段不解决并存策略 —— 通过 ReaderFactory 配置项让用户选。
若第三阶段性能验证 `FsCache` 显著优于现有方案，再讨论是否废弃旧路径。

## 不在本设计范围

- Userspace Page Cache（RAM 层）—— 第二阶段或更后续
- `ReadFile.fileVersion()` 扩展 —— Q7=A 决定第一阶段不动
- 性能测试与 benchmark 数据 —— 第三阶段
- 分布式 cache / peer cache —— 跨节点设计，不在本次范围
- CacheLib PoC 路径的废弃 —— 由第三阶段数据决定

## 参考

- ClickHouse FileCache：`~/SourceCode/ClickHouse/src/Interpreters/FileCache/`
  （约 11853 行 C++）
- ClickHouse Userspace Page Cache：`~/SourceCode/ClickHouse/src/Common/PageCache.{h,cpp}`
  （约 450 行 + reader 接入 497 行）
- Velox 现状分析：`~/SourceCode/.ai/share_data/local-cache/claude/01-velox.md`
- ClickHouse 对比：`~/SourceCode/.ai/share_data/local-cache/claude/03-clickhouse.md`
- Velox 候选项汇总：`~/SourceCode/.ai/share_data/local-cache/claude/06-gap-and-candidates.md`
- 综合 review：`~/SourceCode/.ai/share_data/local-cache/claude/07-review.md`
- CacheLib PoC 审计：
  `~/SourceCode/.ai/share_data/local-cache/claude/poc-cachelib/2026-05-20-stage-audit.md`
- CH Userspace Page Cache 文档：
  `https://clickhouse.com/docs/operations/userspace-page-cache`
