# FsCache Phase-2 Plan-1 单线程性能回退诊断

**日期**：2026-05-25
**Phase-1 baseline commit**：`8dfe97c38`（plan-1 实施前最后一个 docs commit）
**Phase-2 measured commit**：`4a3ec350b`（plan-1 task 10 完成后）
**测试床**：GCC-13 RelWithDebInfo，single CPU full-load，`perf_event_paranoid=1`

## TL;DR

Plan-1 在单线程 100% hit 场景出现 **~23 % 吞吐回退**（7.33 → 5.62 M ops/s）。Root cause = `PathKey::fromPath` 用 `fmt::format_to_n("{:016x}", combined)` 把 128-bit hash 渲染成 16-byte hex string 当 hash key，**每次 `getOrSet` 调用都跑一次完整 fmt 格式化**。Hot path `fmt::v12::*` 自时间占比 phase-2 ≈ 15 %（phase-1 = 0 %）。

## 测试 cell 与 36-cell 概览

A/B 都跑 `sequential.1t.0.5ws.0us`（100% hit、single thread、ws_mult=0.5、no remote latency），1.5B ops + 200k warmup，单 cell ~3.5-4.5 分钟，足够把 warmup/setup 噪声摊薄。

36-cell sweep 数据（`docs/superpowers/results/2026-05-23-fscache-phase1-baseline.md` vs `/tmp/phase2-plan1-baseline.md`）显示：
- 单线程 hit-only cells 普遍回退到 **0.71–0.83×**
- 16-thread `zipfian.16t.0.5ws` 改善到 1.29×（plan-1 预期收益），但 `sequential.16t.*` / `uniform.16t.*` 略退（0.88–0.98×）
- Miss-heavy cells ±5 %（噪声范围）

下文专攻 **`sequential.1t.0.5ws.0us`** 这一格——回退最显著且最容易隔离根因。

## A: `perf stat` A/B（long single-cell run）

| 指标 | Phase-1 (`8dfe97c38`) | Phase-2 (`4a3ec350b`) | Δ (B vs A) |
|---|---:|---:|---:|
| **Throughput** | **7.33 M ops/s** | **5.62 M ops/s** | **−23.4 %** |
| Wall sec | 204.60 | 266.99 | +30.5 % |
| Task-clock (s) | 220.30 | 266.97 | +21.2 % |
| User time (s) | 214.66 | 261.36 | +21.8 % |
| Sys time (s) | 5.64 | 5.61 | −0.5 % |
| **cpu_core cycles** | **1.187 T** | **1.411 T** | **+18.8 %** |
| cpu_core GHz | 5.388 | 5.285 | −1.9 % |
| **cpu_core instructions** | **3.436 T** | **4.134 T** | **+20.3 %** |
| Instr / op | 2 291 | 2 756 | **+20.3 %** |
| **IPC (cpu_core)** | **2.89** | **2.93** | +1.4 % |
| cpu_core branches | 636.5 G | 784.5 G | +23.2 % |
| Branches / op | 424 | 523 | +23.4 % |
| **cpu_core branch-miss rate** | **0.37 %** | **0.55 %** | +48.6 % |
| Branch-miss total | 2.378 G | 4.299 G | +80.8 % |
| **cpu_core cache-refs** | **2.24 G** | **2.00 G** | −10.9 % |
| cpu_core cache-miss rate | 71.76 % | 70.47 % | −1.3 % |
| Cache-miss total | 1.606 G | 1.408 G | −12.4 % |
| Context-switches | 1 207 | 1 547 | +28 % |

**核心信号**：CPU 不是变慢，是**确实多干活**——每 op 多 ~470 条指令、+23 % 分支。IPC 几乎不变，cache-miss 反而稍降（per-bucket metadata 散开了工作集）。结论：回退**不是**锁竞争、不是 cacheline、是纯指令数膨胀 + branch-prediction friendliness 下降。

## B: `perf record -g` top-25 self-time symbol diff

`perf record -g --call-graph=dwarf,16384 -F 997`，sample 数 phase-1 = 219 k / phase-2 = 267 k。Symbol % 已去掉 template 噪声、按 demangle 后名字 group sum。

### Phase-1 top 25

| Self % | Symbol |
|---:|---|
| 10.70 | `__vdso_clock_gettime` |
| 8.51 | `pthread_mutex_lock` |
| 8.49 | `folly::hash::SpookyHashV2::Short` |
| 8.16 | `pthread_mutex_unlock` |
| 7.96 | `malloc` |
| 7.53 | `std::_Hashtable::_M_find_before_node` |
| 4.71 | `FsCacheMetadata::lookup` |
| 4.54 | `parallelRun lambda` |
| 3.95 | `FsCache::getOrSet` |
| 3.69 | `_int_free` |
| 2.94 | `folly::hash::SpookyHashV2::Hash128` |
| 2.90 | `cfree` |
| 1.70 | `LruPolicy::onHit` |
| 1.66 | `__memcmp_avx2_movbe` |
| 1.54 | `clock_gettime` |
| 1.40 | `operator new` |
| 1.36 | `FsCache::lookupOrCreate` |
| 1.30 | `steady_clock::now` |
| 1.09 | `__memmove_avx_unaligned_erms` |
| 1.05 | `FsCacheKey::hash` |
| 1.00 | `malloc@plt` |
| 0.99 | `FsCache::splitRange` |

**Phase-1 hot path 在 ≥0.5 % self-time 阈值下无任何 `fmt::*` symbol**（perf report top 列 + `grep '^fmt::'` 0 命中）。低于 0.5 % 阈值是否存在零星 fmt inline 进 caller 未确证，但量级上不构成 plan-1 回退源。

### Phase-2 top 25

| Self % | Symbol |
|---:|---|
| 9.49 | `FsCacheMetadata::lookup` |
| 8.96 | `pthread_mutex_lock` |
| 5.12 | `_int_free` |
| **4.42** | **`fmt::v12::detail::write_int_noinline<unsigned long>`** |
| 4.34 | `FsCache::getOrSet` |
| 4.19 | `__vdso_clock_gettime` |
| 4.18 | `cfree` |
| 3.94 | `malloc` |
| 3.75 | `__memmove_avx_unaligned_erms` |
| 3.66 | `pthread_mutex_unlock` |
| 3.00 | `FsCache::recordHit` |
| 2.59 | `folly::hash::SpookyHashV2::Short` |
| **2.49** | **`fmt::v12::detail::parse_format_string`** |
| **2.34** | **`PathKey::fromPath`** |
| **2.16** | **`fmt::v12::detail::copy_noinline`** |
| **2.06** | **`fmt::v12::detail::buffer::append`** |
| **2.04** | **`fmt::v12::detail::parse_format_specs`** |
| 1.99 | `std::_Hashtable::_M_find_before_node` |
| 1.90 | `operator new` |
| 1.66 | `pthread_mutex_trylock` |
| 1.46 | `steady_clock::now` |
| 1.40 | `LruPolicy::onHit` |
| **1.38** | **`fmt::v12::detail::vformat_to`** |
| 1.12 | `pthread_mutex_unlock@plt` |

### `fmt::*` self-time tally

| 累计 | Phase-1 | Phase-2 |
|---|---:|---:|
| 全部 `fmt::v12::*` symbols 自时间累加 | **0.00 %** | **≈ 15.0 %** |
| + `PathKey::fromPath` 本身 | n/a | + 2.34 % |
| + `__memmove_avx_unaligned_erms` (phase-1 1.09 → phase-2 3.75) | — | +2.66 % |
| + `malloc/_int_free/cfree/new` 涨幅 (15.95 → 15.14, 实际持平) | — | ±0 % |
| **关 fmt 后预期回收** | — | **~17–18 %** |

## C: 调用栈定位

Phase-2 `fmt::*` top sample 的调用链稳定指向同一处：

```
fmt::detail::write_int_noinline
  ← fmt::detail::do_format_base2e
  ← fmt::detail::write<unsigned long long>
  ← fmt::detail::arg_formatter::operator()
  ← fmt::detail::vformat_to
  ← fmt::format_to_n
  ← facebook::velox::cache::fs::PathKey::fromPath
  ← facebook::velox::cache::fs::FsCache::getOrSet
  ← parallelRun lambda
```

源码 `velox/common/caching/fscache/FsCacheKey.cpp:27-35`：

```cpp
PathKey PathKey::fromPath(std::string_view path) {
  uint64_t hash1{0};
  uint64_t hash2{0};
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &hash1, &hash2);
  const uint64_t combined = hash1 ^ hash2;
  PathKey key{};
  fmt::format_to_n(key.chars.data(), key.chars.size(), "{:016x}", combined);
  return key;
}
```

每次 `FsCache::getOrSet` 都会构造一个 `FsCacheKey { PathKey, offset, size }`，`PathKey` 又由 `PathKey::fromPath(path)` 生成 —— **每 op 一次完整 fmt 格式化**（parse format string + parse specs + write int + buffer copy）。

## 根因总结

**`PathKey` 把 128-bit hash 渲染成 16-byte hex string 当 hash key** 是 plan-1 task 1 的设计选择，目的是让 on-disk 文件名前缀（`aa/bb/<filename>`）和内存 key 共用一份 hex 字节序，节省一次"内存 binary key ↔ 磁盘 hex 字符串"转换。代价是：
- **每 op 一次 `fmt::format_to_n` 调用** —— hot path 上完全多余的字符串渲染。
- Hash key 比较从 8-byte `memcmp` 变成 16-byte `memcmp`，cycle 上没差，但失去了"key 本身就是 hash"的零成本属性。
- 派生 alloc/memmove（fmt 内部 buffer）让 `__memmove_avx_unaligned_erms` 从 1.09 % 飙到 3.75 %。

## 建议修复

`PathKey` 改成 POD `std::array<uint8_t, 16>`（或 `struct { uint64_t lo, hi; }`），**直接存原始 128-bit hash bytes，不做 stringify**：

- `fromPath()`：只调 `SpookyHashV2::Hash128`，把 `hash1/hash2` `memcpy` 进 `std::array<uint8_t,16>`。
- `hash()`：返回 `hash1`（无需 memcpy，可直接取 `uint64_t` first8）。
- `operator==`：`memcmp` 16 字节（不变）。
- **`fileName()` 必须显式 hex 渲染**：当前 `fileName()` 实现是 `fmt::format("{}.{}.{}", string_view{path.chars}, offset, size)`，把 `chars` 当 ASCII 喂出去；改 POD 后 `chars` 是 raw bytes（含不可打印字符、可能含 NUL），若不改 `fileName()` 会直接破坏 on-disk 文件名格式，`FsCache.cpp:473` 的 `parseFileName` 严格校验 "exactly 16 lowercase hex chars + `.`" 会全部 reject。因此**契约要求**：`fileName()` 改成 `fmt::format("{:016x}{:016x}.{}.{}", hi, lo, offset, size)` 或等价 hex 渲染，**output 必须与 plan-1 现状字节级一致**（16 lowercase hex + `.<offset>.<size>`），这样 on-disk layout 不变、parser 不动、crash recovery 兼容。
- Hex 化只在 `fileName()`（disk write + crash recovery 路径）发生，hit path 完全无 fmt。

预计效果：
- `fmt::*` self-time 0 %（净减 ~15 %）
- `PathKey::fromPath` self-time 从 2.34 % 降到 ~0.5 %（只剩 SpookyHash）
- `__memmove_avx_unaligned_erms` 回落到 ~1 %（净减 ~2.5 %）
- alloc/free 略降（~1 %）
- **总计 ~17–18 % 吞吐回升**：phase-2 单线程从 5.62 推回 ~6.6–6.8 M ops/s，与 phase-1 7.33 M 的差距收窄到 ~7 %。

**Hypothesis**：剩余 ~7 % 单线程差由 per-bucket 多锁层（KeyMetadata 间接 + 额外 mutex_lock/unlock pair）+ try_lock LRU bump + atomic stats 共同贡献，属于 plan-1 设计代价；但本诊断**未隔离**各项贡献。下一步验证流程：PathKey 修复 commit 后重跑 36-cell + 二次 `perf record`，若 fmt::* 自时间归零、`pthread_mutex_lock/unlock` 占比与残差吞吐回退量吻合，方可下结论；否则需进一步切片（如临时 stub atomic stats、关 try_lock 路径）。Plan-2 的 bg download / in-flight reservation 会在 miss path 摊薄锁开销；plan-3 SLRU 让 hit path 受益更多，但这些都不是本次回退的对策。

## 复现命令

Phase-1 binary（与 phase-2 build 完整对齐的 cmake invocation）：
```bash
cd /home/chang/OpenSource/velox && git checkout 8dfe97c38
cmake -S . -B cmake-build-relwithdebinfo-gcc13 -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER=/usr/bin/cc -DCMAKE_CXX_COMPILER=/usr/bin/c++ \
  -DVELOX_ENABLE_BENCHMARKS=ON -DVELOX_ENABLE_BENCHMARKS_BASIC=ON \
  -DVELOX_BUILD_TESTING=ON -DVELOX_BUILD_RUNNER=ON \
  -DVELOX_MONO_LIBRARY=ON -DVELOX_ENABLE_GROUPED_TESTS=ON \
  -DVELOX_TESTS_PER_GROUP=10 -DVELOX_ENABLE_HIVE_CONNECTOR=ON \
  -DVELOX_ENABLE_PARQUET=ON -DVELOX_ENABLE_TPCDS_CONNECTOR=ON \
  -DVELOX_ENABLE_TPCH_CONNECTOR=ON -DVELOX_ENABLE_ICEBERG_FUNCTIONS=ON \
  -DVELOX_ENABLE_PRESTO_FUNCTIONS=ON -DVELOX_ENABLE_SPARK_FUNCTIONS=ON \
  -DVELOX_ENABLE_AGGREGATES=ON -DVELOX_ENABLE_GEO=ON \
  -DVELOX_ENABLE_CCACHE=ON -DVELOX_ENABLE_ABFS=OFF \
  -DVELOX_ENABLE_ARROW=OFF -DVELOX_ENABLE_GCS=OFF \
  -DVELOX_ENABLE_HDFS=OFF -DVELOX_ENABLE_REMOTE_FUNCTIONS=OFF \
  -DVELOX_ENABLE_FAISS=OFF -DVELOX_ENABLE_EXAMPLES=OFF \
  -DVELOX_ENABLE_TORCHWAVE=OFF -DVELOX_BUILD_PYTHON_PACKAGE=OFF \
  -DTREAT_WARNINGS_AS_ERRORS=OFF
ninja -C cmake-build-relwithdebinfo-gcc13 velox_fscache_benchmark
```
Frame-pointer 策略：本诊断 `perf record --call-graph=dwarf,16384` 不依赖 fp，未额外加 `-fno-omit-frame-pointer`；若改用 `--call-graph=fp` 需重 build 加 flag。

Phase-2 binary（当前分支已 build）：
```bash
cmake-build-relwithdebinfo-gcc13/velox/common/caching/fscache/benchmarks/velox_fscache_benchmark
```

Long-run sample（任一 binary）：
```bash
perf stat -e task-clock,cycles,instructions,branches,branch-misses,\
cache-references,cache-misses,context-switches,cpu-migrations \
  <binary> --ops=1500000000 --warmup_ops=200000 \
  --workloads=sequential --threads_list=1 --ws_mult_list=0.5 \
  --remote_latency_us_list=0 --out=/tmp/probe.md

perf record -g --call-graph=dwarf,16384 -F 997 -o /tmp/perf.data \
  <binary> --ops=1500000000 --warmup_ops=200000 \
  --workloads=sequential --threads_list=1 --ws_mult_list=0.5 \
  --remote_latency_us_list=0 --out=/tmp/probe.md

perf report -i /tmp/perf.data --no-children --stdio --percent-limit=0.5 \
  -F overhead,symbol -g none
```

## 下游 follow-up

- Commit B：`perf(fscache): PathKey as POD 128-bit hash, drop fmt::format_to_n`
- Commit C：`docs(fscache): plan-1 36-cell rerun after PathKey fix`（重跑 36-cell sweep 验证）
