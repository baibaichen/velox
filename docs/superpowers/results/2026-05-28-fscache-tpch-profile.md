# FsCache vs CBI: TPC-H SF=100 perf profile — cold + hot

- 日期: 2026-05-28
- 分支: `fscache-clickhouse-style` @ `661ac1be2`
- 数据集: `/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`
- 二进制: `cmake-build-relwithdebinfo-gcc13/velox/benchmarks/tpch/velox_tpch_benchmark`
- 采样: `perf record -F 999 -g --call-graph fp`，`perf_event_paranoid=1`，`kptr_restrict=1` (内核符号未解析，仅显示十六进制地址)

## 1. 跑法 (verbatim recipes)

冷 (rounds=1, 22 个 query):

```
perf record -F 999 -g --call-graph fp -o /tmp/q-fsc-cold.perf -- \
  velox_tpch_benchmark --input_source=fscache --rounds=1 \
  --fscache_root=/tmp/velox_fscache_cold --fscache_disk_gib=64 --out=/tmp/q-fsc-cold.csv
perf record -F 999 -g --call-graph fp -o /tmp/q-cbi-cold.perf -- \
  velox_tpch_benchmark --input_source=cbi --rounds=1 --cache_gb=16 --out=/tmp/q-cbi-cold.csv
```

热 (rounds=5, 22 个 query × 5 轮，覆盖 q17 多次)：

```
perf record ... -o /tmp/q17-fsc-hot.perf -- \
  velox_tpch_benchmark --input_source=fscache --rounds=5 \
  --fscache_root=/tmp/velox_fscache_hot_q17 --fscache_disk_gib=64 --out=/tmp/q17-fsc-hot.csv
perf record ... -o /tmp/q17-cbi-hot.perf -- \
  velox_tpch_benchmark --input_source=cbi --rounds=5 --cache_gb=16 --out=/tmp/q17-cbi-hot.csv
```

`runAb()` 没有 `--query_ids`，所以热路径用 5 轮全 22 query，q17 占总采样的约 1/8。

## 2. 文件存在性

```
-rw-------  99.2 MB /tmp/q-fsc-cold.perf   (849,525 samples)
-rw-------  85.1 MB /tmp/q-cbi-cold.perf   (730,037 samples)
-rw------- 474.3 MB /tmp/q17-fsc-hot.perf  (4,082,818 samples)
-rw------- 422.8 MB /tmp/q17-cbi-hot.perf  (3,643,999 samples)
```

`exit=0` for all four; 22 行 CSV (cold) 与 110 行 CSV (hot)；无 `task failed` 标记。

## 3. Wall-time ground truth

| Pass | Backend | Σ wall_ms (22 q × N round) | q17 平均 wall (ms) |
|---|---|---|---|
| cold (rounds=1) | fsc | 174,026 | 21,083 |
| cold (rounds=1) | cbi | 151,730 | 19,548 |
| hot (rounds=5) | fsc | 168,114 ~ 171,083 / 轮 | 21,254 (5 轮均值) |
| hot (rounds=5) | cbi | 150,773 ~ 152,768 / 轮 | 19,938 (5 轮均值) |

`(174026-151730)/151730 = +14.7%` (cold) ; `(170029-151179)/151179 = +12.5%` (hot mean of 5 rounds).
两组实测都落在 task 给的 +12~15% 区间内。

注意：fsc 的 round 1 (171,083 ms) 与 round 5 (168,114 ms) 几乎相同，CSV 里 `hit_pct=100%, bytes_dl_mib=0` 全轮一致 → **fsc 没有"冷启动一次然后变快"的形态**。下面的 cold/hot 分析其实是「初次填盘」与「稳态命中」的差，但 wall 上看不到这个 gap。+12% 主要不是下载成本，而是 **稳态 CPU overhead**。

## 4. Top-20 self by symbol (聚合所有线程)

### 4.1 fsc cold (cycles total ≈ 3.86e12)

```
 21.32 [.] __memset_avx2_unaligned_erms
 18.77 [k] 0xffffffffa779dd78                          ← 内核 syscall / copy_user
 17.99 [.] __memmove_avx_unaligned_erms
 10.33 [.] HashTable<false>::groupNormalizedKeyProbe
  8.31 [.] HashTable<true>::joinNormalizedKeyProbe
  6.43 [.] ProbeState::fullProbe
  6.38 [.] snappy::MemCopy64
  3.88 [k] 0xffffffffa818f677
  2.80 [.] 0x0000000000004aac                          ← libsnappy (Snappy 解压块)
  2.34 [.] HashTable<true>::insertForJoin
  1.87 [.] RowContainer::store
  1.55 [.] HashTable<false>::insertForGroupBy
  1.49 [.] HashPartitionFunction::partition
  1.48 [.] RowContainer::updateColumnStats
  1.47 [.] snappy::DeferMemCopy
  ... (其余 < 1.5%)
```

### 4.2 cbi cold (cycles total ≈ 3.38e12)

```
 19.66 [.] __memmove_avx_unaligned_erms
 12.11 [.] HashTable<false>::groupNormalizedKeyProbe
 10.99 [.] __memset_avx2_unaligned_erms
  8.00 [.] ProbeState::fullProbe
  7.81 [k] 0xffffffffa779de24
  7.53 [.] HashTable<true>::joinNormalizedKeyProbe
  7.28 [.] snappy::MemCopy64
  4.71 [k] 0xffffffffa818f677
  3.52 [.] 0x0000000000004aac
  2.28 [.] RowContainer::store
  1.85 [.] HashTable<false>::insertForGroupBy
  1.78 [.] RowContainer::updateColumnStats
  1.74 [.] HashTable<true>::insertForJoin
  1.68 [.] snappy::DeferMemCopy
  1.56 [.] 0x0000000000004a87
  1.53 [.] HashPartitionFunction::partition
  1.41 [.] memmove@plt
  ...
```

### 4.3 fsc hot (cycles total ≈ 1.897e13)

```
 22.41 [.] __memset_avx2_unaligned_erms
 18.93 [.] __memmove_avx_unaligned_erms
 16.56 [k] 0xffffffffa779dd78
 11.06 [.] HashTable<false>::groupNormalizedKeyProbe
  8.04 [.] HashTable<true>::joinNormalizedKeyProbe
  6.92 [.] ProbeState::fullProbe
  6.48 [.] snappy::MemCopy64
  2.85 [.] 0x0000000000004aac
  2.34 [.] HashTable<true>::insertForJoin
  1.94 [.] RowContainer::store
  1.84 [k] 0xffffffffa818f677
  ...
```

### 4.4 cbi hot (cycles total ≈ 1.695e13)

```
 19.88 [.] __memmove_avx_unaligned_erms
 13.27 [.] HashTable<false>::groupNormalizedKeyProbe
 11.36 [.] __memset_avx2_unaligned_erms
  8.27 [.] ProbeState::fullProbe
  8.17 [.] HashTable<true>::joinNormalizedKeyProbe
  7.57 [k] 0xffffffffa779de24
  7.17 [.] snappy::MemCopy64
  3.24 [k] 0xffffffffa818f677
  3.24 [.] 0x0000000000004aac
  2.06 [.] RowContainer::store
  ...
```

## 5. Delta 排行 (绝对 cycle 差，正数=fsc 比 cbi 多)

### 5.1 Cold delta (Δ cycles ×1e9, sorted)

| 符号 | fsc% × 3860 | cbi% × 3384 | Δ ×1e9 |
|---|---|---|---|
| `0xffffffffa779dd78` (kernel) | 724.5 | 264.5 (用 a779de24) | **+460** |
| `__memset_avx2_unaligned_erms` | 822.9 | 371.9 | **+451** |
| `__memmove_avx_unaligned_erms` | 694.4 | 665.3 | +29 |
| `0xffffffffa818f677` (kernel) | 149.7 | 159.4 | -10 |
| `HashTable<false>::groupNormalizedKeyProbe` | 398.7 | 409.8 | -11 |
| `snappy::MemCopy64` | 246.5 | 246.4 | +0 |
| `HashTable<true>::joinNormalizedKeyProbe` | 320.8 | 254.8 | +66 |

总差 = 3860 - 3384 = **+476**；memset + kernel I/O 加起来 **+911**，被 HashTable/snappy/ProbeState 等 -435 部分抵消。

### 5.2 Hot delta (Δ cycles ×1e9)

| 符号 | fsc% × 18970 | cbi% × 16950 | Δ ×1e9 |
|---|---|---|---|
| `__memset_avx2_unaligned_erms` | 4252 | 1925 | **+2327** |
| `0xffffffffa779dd78` (kernel) | 3142 | 1283 (a779de24) | **+1859** |
| `__memmove_avx_unaligned_erms` | 3592 | 3370 | +222 |
| `HashTable<false>::groupNormalizedKeyProbe` | 2098 | 2249 | -151 |
| `HashTable<true>::joinNormalizedKeyProbe` | 1525 | 1384 | +141 |
| `0xffffffffa818f677` (kernel) | 349 | 549 | -200 |
| `snappy::MemCopy64` | 1229 | 1215 | +14 |

总差 = 18970 - 16950 = **+2020**；memset 一项 **+2327** 就已经超过总差，加上 +1859 内核 I/O，被其它符号 -2166 部分抵消。

**结论**：cold 和 hot 两个 pass 里，「fsc 多出来的 cycle」几乎都集中在 `__memset_avx2_unaligned_erms`（约 2.2× 于 cbi 的绝对量）与对应的内核 page-zero/syscall 路径。

## 6. Root cause — verbatim call-graph evidence

### 6.1 Hot root cause: `FsCacheInputStream::loadCurrentSegmentBuffer`

`/home/chang/OpenSource/velox2/velox/dwio/common/FsCacheInputStream.cpp:57`

```cpp
void FsCacheInputStream::loadCurrentSegmentBuffer() {
  const auto& segment = segments_[index_];
  ...
  const uint64_t length = rangeEnd - rangeStart;
  segment->waitForDownloadedSize(needed);
  buffer_.assign(length, '\0');                   // ← line 57, 整段 zero-fill
  segment->read(rangeStart - segStart, length, buffer_.data(), cacheRoot_);
  cursor_ = 0;
}
```

`buffer_` 是 `std::vector<char>`。`assign(length, '\0')` 把整段 segment 大小（默认 ~4 MiB，实测 cache 文件 6 MiB+ 常见）memset 成 0，**下一行 `segment->read()` 立刻用 pread 全量覆盖** — 这次 memset 是纯粹浪费。

对应 cbi 路径 `CacheInputStream::loadPosition` (`velox/dwio/common/CacheInputStream.cpp:326`) 不做 zero-fill；它直接把 pread 写进 SsdPin 拥有的缓冲。

Perf graph (fsc hot) 把 `__memset_avx2_unaligned_erms` 跟 `0xffffffffa6c00ef0`（内核 clear_page / 缺页 zeroing）成对显示，提示一半的 memset cost 是用户态 AVX2 store，另一半是首次 touch 时的内核 page-zero — 两者都源自这一行。

CSV 数据佐证: q17 hot 5 轮 wall 几乎相等（21.30 / 20.63 / 22.75 / 20.68 / 20.91 秒）；`hit_pct=100%` 全轮、`bytes_dl_mib=0` 全轮 → 第二轮起完全是稳态命中盘。这条 memset 路径就是稳态 +12% 的主因。

### 6.2 Cold root cause: 同上 + 额外的 `FsCacheDownload` 写盘

冷路径 perf 里 fsc 多了一个 `FsCacheDownload` 命名线程，自时占总 cycle 约 7.2%：

```
2.80%  FsCacheDownload  [k] 0xffffffffa779dd78   __libc_pread (向 LocalReadFile 读远程文件)
2.53%  FsCacheDownload  [k] 0xffffffffa818f677   __libc_pwrite
             ↑ 顶上是
             facebook::velox::cache::fs::FileSegment::write(char const*, unsigned long)
0.66%  FsCacheDownload  [k] 0xffffffffa779fda3 → FileSegment::write
0.36%  FsCacheDownload  __memset_avx2_unaligned_erms
```

这是 `FsCacheBufferedInput::load` 下沉到 `DownloadThreadPool` 后，把 1 MiB chunk 从远程 pread → memcpy → pwrite 到 SSD 的开销。cbi 无对应路径（`--cache_gb=16` 只有 RAM 一层）。

但 cold delta 表上「`__memset_avx2_unaligned_erms` +451」与「kernel I/O +460」量级几乎相同 → 冷路径里 hot-path 那条 `loadCurrentSegmentBuffer::assign(length, 0)` **也同样存在**（一旦 segment 写盘完成，读端立即走同一条 zero-fill）。也就是说 6.1 节那个 hot root cause 在冷 pass 里同样占主导。

补充：`FsCacheBufferedInput::load` 里还有两处大块 `std::vector<char>` 构造：

- `velox/dwio/common/FsCacheBufferedInput.cpp:225` `enqueued.bypassBuffer.assign(enqueued.region.length, '\0');`
- `velox/dwio/common/FsCacheBufferedInput.cpp:277-278` `std::vector<char> buf(std::min<uint64_t>(kChunk, segCapture->key().size));` 在 download 闭包里

这两条只在冷路径或 bypass 时命中，但都是同款「先 zero-fill 再 overwrite」反模式，建议一起改。

## 7. 修复 sketch（**未实施**，仅给出补丁草案）

### 7.1 Hot 路径 — 把 `buffer_` 换成不初始化的存储 (主修)

`velox/dwio/common/FsCacheInputStream.cpp:57` 与配套 header `FsCacheInputStream.h` 里 `buffer_` 的类型。

```cpp
// 草案 A — 使用 std::unique_ptr<char[]> 与 make_unique_for_overwrite (C++20):
//   header: std::unique_ptr<char[]> buffer_; size_t bufferSize_{0};
//   cpp  loadCurrentSegmentBuffer():
if (bufferSize_ < length) {
  buffer_ = std::make_unique_for_overwrite<char[]>(length);  // 无 zero-fill
  bufferSize_ = length;
}
segment->read(rangeStart - segStart, length, buffer_.get(), cacheRoot_);
// 之后 Next() 用 buffer_.get() 替代 buffer_.data()，用 length 替代 buffer_.size()
```

或者沿用 Velox 既有的 `dwio::common::DataBuffer<char>` (`reserve` 不 zero-fill)：

```cpp
// 草案 B — DataBuffer：
buffer_.reserve(length);          // 不 zero
buffer_.unsafeSetSize(length);    // 调整逻辑长度，不写
segment->read(..., buffer_.data(), cacheRoot_);
```

**预期收益**: hot pass 总 cycle 中 memset 占 22.4%，其中约一半来自该行（剩余来自 ParquetReader::ReaderBase 路径，与 cbi 一致）。乐观估计 hot wall 改善 **~10%**，把 +12% 差距收窄到 ~2% 量级。

### 7.2 Cold 路径补丁 (次修)

- `FsCacheBufferedInput.cpp:225`：`bypassBuffer.assign(length, '\0')` → 同上换 `resize_for_overwrite`。
- `FsCacheBufferedInput.cpp:278`：download 闭包里 `std::vector<char> buf(N)` → 换 `auto buf = std::make_unique_for_overwrite<char[]>(N)`。

这两处只在冷/bypass 命中，整体收益较小，但和 hot 修一起做语义对齐。

### 7.3 `FsCacheDownload` 写盘 (不动)

冷路径 7% 的 pwrite/pread 是 fsc 设计层面的固有成本 — 数据必须落盘以支持热路径命中。除非引入 `O_DIRECT` 或合并小 chunk，否则没法压。先不动；如果 7.1 落地后仍有 gap，再回来评估。

## 8. 八荣八耻 self-check

- #1 (查实际签名): 已查 `FsCacheInputStream.cpp:25-75`、`FsCacheBufferedInput.cpp:200-280`、`ParquetReader.cpp:253-319`、`Options.h:578-581`。perf 行原文已贴。
- #5 (主动测试): 4 个 perf record 都成功，无 fail；CSV/wall 都被独立校验。
- #7 (诚实无知)：内核地址 `0xffffffffa779dd78` 等因为 `kptr_restrict=1` 无法解析；只能从用户态 callstack 反推它是 sys_read/copy_user 系，未给确切函数名。
- #7 hypothesis 校验: task spec 推测 leak 在 `FsCacheBufferedInput`；实测主体在 **`FsCacheInputStream`** (loadCurrentSegmentBuffer)。两者都在 fscache 模块，但具体源文件与原假设不同 — 已显式列出。

## 9. 没有修改任何 production 代码

仅产生本 doc。原始 perf 数据在 `/tmp/q-{fsc,cbi}-cold.perf`、`/tmp/q17-{fsc,cbi}-hot.perf`（未提交，约 1.1 GiB）。
