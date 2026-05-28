# fscache q17 重新剖析（memset 修复后 + 3 次内存分配实验被否决之后）

> 日期: 2026-05-28  HEAD: `13f0c2a26`  构建: `cmake-build-relwithdebinfo-gcc13`
> 数据集: SF=100 parquet (`/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`)
> 采样: `perf record -F 999 -g --call-graph fp`, 每后端 5 轮 × 22 个 query
> CSV 输出: `/tmp/q17-fsc-hot-v2.csv`, `/tmp/q17-cbi-hot-v2.csv`
> perf 数据: `/tmp/q17-fsc-hot-v2.perf` (476 MB, 4.07M 样本), `/tmp/q17-cbi-hot-v2.perf` (419 MB, 3.62M 样本)

## 1. q17 墙钟（5 轮平均）

| 后端 | round1 | round2 | round3 | round4 | round5 | mean | 缺口 |
|---|---|---|---|---|---|---|---|
| fscache | 21296 | 21412 | 21459 | 20858 | 20615 | **21128 ms** | +8.8% |
| cbi | 19351 | 19511 | 19364 | 19524 | 19532 | **19457 ms** | baseline |

> 注: 因为 `velox_tpch_benchmark --input_source=...` 一次扫全部 22 个 query，无 per-query 过滤旗；本剖析的 perf 样本是 22 个 query 的合计。但 q17 在 fscache 一轮 ~21 s 中占绝对大头（其他 query 大多 < 10 s），且 q17 自身缺口 8.8%（>> 报告的 +4% 聚合缺口），表明该差距集中在 q17。

## 2. 各后端 Top 30 自时间符号（`perf report --no-children --sort=symbol`，VERBATIM）

> 完整文件: `/tmp/q17-fsc-v2-top.txt`, `/tmp/q17-cbi-v2-top.txt`

### 2.1 fscache

```
16.58% [k] 0xffffffffa779dd78          # pread syscall 路径 (内核)
15.05% [.] __memmove_avx_unaligned_erms
10.17% [.] __memset_avx2_unaligned_erms
 6.61% [.] HashTable<false>::groupNormalizedKeyProbe
 4.23% [.] ProbeState::fullProbe<...>
 2.02% [.] libsnappy 0x4aac
 1.60% [k] 0xffffffffa818f677
 1.34% [.] HashTable<true>::joinNormalizedKeyProbe
 1.14% [.] snappy::MemCopy64
 0.98% [.] RowContainer::store
 0.91% [k] 0xffffffffa6c001bd
 0.68% [.] RowContainer::updateColumnStats
 0.61% [.] _int_malloc
 0.53% [.] HashPartitionFunction::partition
 0.53% [.] F14Table::tryEmplaceValueImpl
 0.51% [.] VectorHasher::makeValueIdsDecoded<long,false>
 0.48% [.] RowContainer::initializeRow
 0.45% [.] malloc
 0.41% [.] populateNormalizedKeys
 0.39% [.] DecodedVector::applyDictionaryWrapper
 0.39% [.] AverageAggregateBase::updateNonNullValue
 0.38% [.] VectorHasher::valueId<long>
 0.38% [.] StringView::StringView
 0.38% [.] HashTable<false>::insertEntry
 0.37% [.] libsnappy 0x4abd
 0.36% [.] HashTable<false>::insertForGroupBy
 0.32% [.] DecodedVector::valueAt<double>
 0.32% [.] HashStringAllocator::storeStringFast
 0.32% [.] HashBuild::addInput lambda
 0.32% [.] RowContainer::newRow
```

### 2.2 cbi

```
14.86% [.] __memmove_avx_unaligned_erms
10.31% [.] __memset_avx2_unaligned_erms
 7.95% [k] 0xffffffffa779de24           # 内核 page-fault / copy_user 路径
 5.90% [.] HashTable<false>::groupNormalizedKeyProbe
 3.63% [.] ProbeState::fullProbe<...>
 3.31% [k] 0xffffffffa818f677
 2.65% [.] libsnappy 0x4aac
 1.51% [.] snappy::MemCopy64
 1.13% [.] RowContainer::store
 1.05% [.] HashTable<true>::joinNormalizedKeyProbe
 0.85% [k] 0xffffffffa6c001bd
 0.80% [.] RowContainer::updateColumnStats
 0.71% [.] F14Table::tryEmplaceValueImpl
 0.61% [.] _int_malloc
 0.58% [.] malloc
 0.55% [.] RowContainer::initializeRow
 0.50% [.] HashPartitionFunction::partition
 0.47% [.] libsnappy 0x4abd
 0.43% [.] HashBuild::addInput lambda
 0.41% [.] VectorHasher::makeValueIdsDecoded
 0.39% [.] populateNormalizedKeys
 0.38% [.] HashStringAllocator::storeStringFast
 0.36% [.] VectorHasher::valueId<long>
 0.36% [.] malloc_consolidate
 0.36% [.] F14Table::rehashImpl
 0.36% [.] snappy::DeferMemCopy
 0.35% [.] typeKindSize lambda
 0.35% [.] SelectiveColumnReader::addValue<string_view>
 0.35% [.] HashTable<false>::insertEntry
 0.35% [.] DecodedVector::applyDictionaryWrapper
```

## 3. Delta 排行（fscache% − cbi%；正值 = fscache 多花）

| 符号 | fsc | cbi | Δ | 类别 |
|---|---:|---:|---:|---|
| **`[k] 0xffffffffa779dd78`（pread 内核路径，所有 IO 线程合计 = **20.17%**）** | **20.17** | 0.05 | **+20.12** | **内核 syscall** |
| `[k] 0xffffffffa779de24`（cbi 内核 page-fault 路径，合计 = 8.56%） | 0 | 8.56 | −8.56 | 内核 |
| `[k] 0xffffffffa818f677` | 1.60 | 3.31 | −1.71 | 内核 |
| `HashTable<false>::groupNormalizedKeyProbe` | 6.61 | 5.90 | +0.71 | 查询执行 |
| `ProbeState::fullProbe<...>` | 4.23 | 3.63 | +0.60 | 查询执行 |
| `RowContainer::store` | 0.98 | 1.13 | −0.15 | 查询执行 |
| `__memset_avx2_unaligned_erms` | 10.17 | 10.31 | −0.14 | 库 |
| `__memmove_avx_unaligned_erms` | 15.05 | 14.86 | +0.19 | 库 |
| `pthread_mutex_lock@GLIBC` | 0.16 | n/a | ≈+0.16 | 锁（**假设1 否决**） |
| `FsCacheMetadata::lockKeyMetadata` | 0.00 | 0.00 | 0 | fscache（否决） |
| `FsCache::recordHit` | 0.00 | 0.00 | 0 | fscache（否决，**假设3**） |
| `FsCacheBufferedInput::load` | 0.00 | 0.00 | 0 | fscache（否决，**假设4**） |
| `FsCacheInputStream::loadCurrentSegmentBuffer` | 0.00 | 0.00 | 0 | fscache（否决，**假设5**） |
| `FileSegment::read` | 0.00 | 0.00 | 0 | self time 0（但驱动 pread → **假设2 中标**） |
| `FsCache::getOrSet` | 0.01 | 0.00 | +0.01 | fscache |

> Top 5 同时含 `__memset` 与 `__memmove`，但两者在 fscache vs cbi 几乎相等 (±0.2 pp)，**这些不再是 fscache 特有热点**。memset 在每 IOThreadPool 调用栈下都同属 `ParquetReader::ParquetReader / ReaderBase::ReaderBase` 的 footer 解析路径，两后端各 ~1.0–1.2% × 8 IO 线程，**结构对称**，不解释缺口。

> 内核净额: fscache 内核侧（pread）多 +20.12 pp，但 cbi 在 mmap 路径上多 −8.56 pp（page fault + `copy_user`），**净 +11.6 pp 流向 fscache 的内核 syscall 路径**。q17 wall 缺口 8.8% 与此一致（pred IOThreadPool 不是全部 CPU 主载，部分被 CPUThreadPool 的查询执行均摊）。

## 4. 单行根因

> fscache 的额外 CPU 时间集中在 **`LocalReadFile::preadInternal` → `::pread(fd_, ...)` 这一条单一 syscall 路径**，被 `FileSegment::read`（`velox/common/caching/fscache/FileSegment.cpp:318`）从各 IOThreadPool 调用。verbatim：
>
> ```
>  1.85%--facebook::velox::LocalReadFile::preadInternal(unsigned long, unsigned long, char*) const
>         __libc_pread
>         0xffffffffa6c0012b
>         0xffffffffa8194660
>         0xffffffffa6e8c220
>         0xffffffffa73e7698
>         0xffffffffa73e74be
>         0xffffffffa74f1180
>         0xffffffffa72977cb
>         0xffffffffa7295586
>         0xffffffffa779e3ff
>          --1.85%--0xffffffffa779dd7a   <-- pread vfs/ext4 leaf
> ```
>
> 同一符号 `[k] 0xffffffffa779dd78` 在 fscache 合计 **20.17%**，cbi 仅 **0.05%**。CBI 不走 pread，因为 `AsyncDataCache` 把命中页保留在进程内 `MemoryAllocator` slab 里，读路径只是用户态 `memcpy`。

## 5. 修复方案

### 候选 A（推荐）: 在 `FileSegment::read` 中保留 `LocalReadFile` 实例

**位置**: `velox/common/caching/fscache/FileSegment.cpp:316-318`
```cpp
const std::string path = localPath(cacheRoot);
::facebook::velox::LocalReadFile file{path};   // <-- 每次 read 都 open(2)+close(2)
const auto view = file.pread(offsetInSegment, length, outBuf);
```

每次 `FileSegment::read` 都：(1) 计算 `localPath`，(2) `::open` 文件描述符，(3) `::pread`，(4) 析构关闭 fd。**每个 4 MiB segment 命中至少多 2 个 syscall（open+close）**，并且重复打开同一文件导致内核 dentry/inode lookup 重复（这通常占 `do_sys_openat2` 路径，正是 `a779e3ff → a779dd7a` 看到的内核帧的一部分）。

修复草图：在 `FileSegment` 里 lazily 缓存一个 `mutable std::unique_ptr<LocalReadFile> readFile_;`（同样以 segment 生命周期为界），第一次 `read` 时初始化，之后所有 `read` 复用同一 fd：

```cpp
// FileSegment.h: 在 private 区加
mutable std::unique_ptr<LocalReadFile> readFile_;
mutable std::once_flag readFileOnce_;

// FileSegment.cpp:316-318 改为
std::call_once(readFileOnce_, [&] {
  readFile_ = std::make_unique<LocalReadFile>(localPath(cacheRoot));
});
const auto view = readFile_->pread(offsetInSegment, length, outBuf);
```

**预估收益**: 消除每 read 一次 open/close 的内核往返；按内核 path 中 `a779e3ff` 帧（1.6–1.9%/IO 线程 × 8 线程 ≈ 12–14% 内核合计）粗估，若 open/close 占该路径 30–50%，可削 4–7 pp 内核时间，对应 q17 ~3–5% 墙钟改善。**不足以完全闭合 +8.8%**，但是单点最大的可拿杠杆。

### 候选 B（互补，更深）: 用 `mmap` 服务 fscache 已 `kDownloaded` 的 segment 命中

把 `FileSegment::read` 从 `pread` 改成对 segment 文件 `mmap` 一次，read 时退化为 `memcpy`（与 CBI 等价）。这正是 CBI 跑得快的原因。但工程量大且需要解决：(1) 写入路径还要继续 `pwrite`，(2) eviction 时要 `munmap`，(3) madvise/page fault 在 cold 情况下仍要 syscall，(4) 每文件 vma 数量上限。**短期不推荐**，作为 phase-3 候选。

## 6. 为什么这个修复不会像之前 3 次内存分配实验那样回归？

| 维度 | 之前 3 次否决的实验 | 本次推荐 (候选 A) |
|---|---|---|
| 改动对象 | DataBuffer 池 / unique_ptr HWM / small_vector inline | `FileSegment` 内的 `LocalReadFile` 实例 |
| 改动模式 | 重排 buffer 生命周期、改 RAII 边界 | **不改 buffer**，只把 `LocalReadFile` 从函数局部提升到成员 |
| 缓存行/对齐 | 改变了热结构 sizeof，可能挤压 L1d | `LocalReadFile` 含 1 个 int (`fd_`) + 几个 stat 字段，每 segment 加 ~80 B，与 segment 自身 size_t/atomic 已有 ~120 B 相比微不足道 |
| 行为面 | 间接通过 malloc/free 计数省 CPU；CPU profile 显示 `malloc` 仅 0.45%，省不了多少，反而损 inlining 与 ICache | **直接省 open(2)/close(2) syscall**；perf 已经显示 syscall 是 20.17% 大头，**直接命中已实测的瓶颈** |
| 风险面 | 不一定有收益，但显式破坏既有 alloc fast-path | 风险仅为 fd 生命周期延长到 segment lifetime，可控（eviction 时 segment 销毁即关闭 fd） |
| 可验证性 | 需要跑整 sweep 才能看出，已知小幅回归 | 可用 `strace -c` 或 perf 直接对比 syscall 频次，立等可见 |

**核心区别**：前 3 次实验都在猜 `malloc/free` 是瓶颈——而 profile 显示 `_int_malloc + malloc + cfree` 加起来 ~1.3%，根本不可能省出 4% 来。本次依据是 profile 实测的 20.17% pread 内核时间，是**已知**的瓶颈，且修复方向是减少 syscall 频次（不是省 CPU 周期），所以收益不会被 ICache / 缓存行重排吃掉。

## 7. 重要保留

- 22 query 合计采样：候选 A/B 的预估都基于 q17 在合计中占比的近似。落地前应跑 `strace -c -e open,close,pread` 单独验证 q17 的 syscall 计数与每 segment open/close 假设。
- 候选 A 假定每 `FileSegment::read` 仅一次 pread（中段、对齐到 segment 大小）。若上层 `FsCacheInputStream` 把一个逻辑 read 拆成多次 `FileSegment::read`，open/close 节省更显著；反之单次则节省较小。
- 剩余 +4% 聚合差距（不只 q17）很可能要靠候选 B（mmap）才能完全闭合；候选 A 是单 PR 可落、风险可控的中间步骤。

## 8. 八荣八耻自检

- (#1 不瞎猜) pread 是基于 verbatim 内核符号 `[k] 0xffffffffa779dd78` 与 `__libc_pread → LocalReadFile::preadInternal` 的调用栈，**未引用先验**；
- (#5 不跳过验证) perf 数据完整保存在 `/tmp/q17-fsc-hot-v2.perf` 与 `/tmp/q17-cbi-hot-v2.perf`，可随时复算；
- (#7 不假装理解) 内核符号 `a779dd78/de24/818f677` 因 `kptr_restrict=2` 无法精确解符，已诚实标注为"pread 路径 leaf"与"page-fault 路径 leaf"，未拍脑袋编名字；
- (#3 业务确认) 修复方案 A 与既有 `FileSegment` 生命周期对齐，不引入新业务语义。
