# Direct-read 基线 设计（SP1）

- 日期：2026-05-31
- 分支：`ch-filecache`
- 命名空间：`facebook::velox::ch`
- 状态：设计已与用户确认，待转 implementation plan

## 0. 背景与阶段定位

下一阶段目标是**优化 hot run 的读性能**，并与 Velox 做深度集成。整阶段拆为两个独立子项目，各自
spec → plan → 实现：

- **SP1（本文档）**：给 TPCH 端到端基准与 micro 基准各加入 `DirectBufferedInput` 路径，跑
  `cbi` / `filecache` / `direct` **三方 hot-read 基线**，确认 `direct` 是否更快，为后续优化定标。
- **SP2（后续，依赖 SP1 实测）**：若基线证明 `DirectBufferedInput` 的 coalesced-load / `DirectInputStream`
  机制在 hot 读上优于现 `FileCacheBufferedInput`，则以它为蓝本重写 FileCache 命中读路径。

SP2 的设计**完全依赖** SP1 的测量结果，本文档不展开 SP2。

## 1. 目标 / 非目标

**目标**
- TPCH 基准支持第三种后端 `--input_source=direct`（纯 Velox 直读，无应用层缓存）。
- micro 基准支持第三个 wrapper `dbi`（`DirectBufferedInput`）。
- micro 基准支持**两阶段持久方法论**：prime 一个进程灌缓存并落盘持久、退出不删；measure 重启另一个
  进程，不 wipe、重载持久缓存、只跑 hot 测量轮。
- **一次性正确性闸门**：独立二进制 `velox_cache_verify` —— prime（cold）之后跑一次，把整个工作集经
  各 wrapper 从持久缓存读回、逐区间字节级对比源文件 + 段级 checksum，pass 才放行 measure；失败则
  非零退出、不进入 hot。校验逻辑抽成**共享组件**，bench 与 verify 工具同链接（保证通用）。
- 产出三方 hot-read 基线报告（md ground-truth + html for humans，随 repo 走）。

**非目标（明确排除）**
- 不在 SP1 重写任何读路径（那是 SP2）。
- 不移植 SipHash128（siphash-port 是独立 TODO，仅在“与真实 ClickHouse 共享缓存目录”时才需要；
  见 §5）。
- 不改 `createBufferedInput` 连接器选择逻辑（direct 通过“不装任何缓存”自然 fallback）。

## 2. 架构 / 改动范围

### A. TPCH 端：`--input_source=direct`

改动点：`velox/benchmarks/AbBenchmarkMain.cpp`（`dispatchAbMain`）。

- 现有 `else` 分支（`AbBenchmarkMain.cpp:111-114`，`dispatchAbMain` 内）对未知 `input_source` 调
  `VELOX_USER_FAIL`。新增
  `else if (FLAGS_input_source == "direct")`：强制 `FLAGS_cache_gb = 0`，不装 `ch::FileCache`，
  不装 `AsyncDataCache`。**`FLAGS_cache_gb = 0` 必须在 `ab.initialize()` 之前设置**（与现有
  `filecache` 分支同序，否则缓存已按非零值装好）。
- 结果：`connectorQueryCtx->cache() == nullptr` 且 `ch::FileCache::getInstance() == nullptr`
  → `createBufferedInput`（`velox/connectors/hive/HiveConnectorUtil.cpp:655`）自然 fallback 到
  `DirectBufferedInput`（同文件 line 709 的 `return std::make_unique<DirectBufferedInput>`；parquet
  非 NIMBLE 分支）。**无需改连接器代码。**
- `coldResetFn`：direct 无应用缓存，设为 no-op；在 flag 说明里写明“direct 的‘冷’仅指 OS page
  cache，harness 不主动 drop”。
- flag 文案与校验：`--input_source` 帮助串补充 `direct`；`direct` 不要求 `--cache_gb > 0`。

### B. micro 端：`dbi` harness + 两阶段持久

改动点：`velox/dwio/common/benchmarks/BufferedInputWrapperBenchmark.cpp`。

- **新增 `DbiHarness`**：仿现有 `CbiHarness`，但不建 `AsyncDataCache`/`SsdCache`，直接构造
  `DirectBufferedInput`（复用同一份合成 blob / `--data_dir` 与 `ScanTracker` 机制）。
- **`--wrappers`** 接受 `dbi` 和 `all`（`all` = cbi+fcbi+dbi）。
- **新 flag `--reuse_cache`（默认 false）**：
  - false：保持现有行为（启动/退出都 `remove_all` 缓存目录）。
  - true：启动**不** wipe、退出**不**删；且 cbi 的 `SsdCache` 用 `checkpointIntervalBytes > 0`
    构造，使 SSD 层 durable，退出前 `checkpoint()` + 等待写完。**cbi 的 SSD 写必须
    `--velox_ssd_odirect=false`**（避免 checkpoint 001 的 O_DIRECT 损坏前科，TPCH 矩阵已带此项，
    micro 矩阵此处补齐）。
- **新 flag `--phase=full|prime|measure`（默认 full）**：
  - `full`：现状单进程（wipe → warm → measure）。
  - `prime`：只灌缓存 + 持久 + 退出**不删**，不出测量数。
  - `measure`：**不**灌缓存，重载持久缓存，只跑 hot 测量轮并出数。
    **若缓存目录不存在或 metadata 重建失败 → 立即 loud fail（非零退出），严禁静默回落到 cold**，
    否则 hot 数会被污染且不可察。由各 harness 在 measure 入口自检并 throw（具体判据放 plan：
    filecache 看 `initialize()` 抛异常 / `loadMetadata()` 后段表为空但目录非空；cbi 看 SsdCache
    checkpoint 文件缺失或 magic 校验失败）。
- **并发约定**：prime/verify/measure 三步**约定单进程独占 cache 目录，不加锁**；若需防误并发，
  plan 阶段可选加 lockfile（本 SP1 默认不加，文档显式声明独占即可）。
- 持久化底层能力（已逐行核实，见 §5）：cbi 靠 SsdCache checkpoint；fcbi 靠
  `ch::FileCache::initialize()` → `loadMetadata()` 扫盘重建段；dbi 靠 OS page cache。

### C. 正确性校验：共享组件 + 独立二进制 `velox_cache_verify`

理由：本 codebase 有缓存损坏前科（checkpoint 001 SSD O_DIRECT corruption）；`src_MB≈0` 只证明
“没回源”，不证明“字节正确”。需在 prime（cold）之后、measure（hot）之前做**一次性**字节级校验，
之后所有 hot run 复读同一份已验证缓存即可信，不必每趟都验（省时且不污染热读计时）。

- **共享校验组件**（新文件，建议 `velox/dwio/common/benchmarks/CacheVerify.{h,cpp}` 或同级）：
  - 把现 `BufferedInputWrapperBenchmark.cpp` 匿名命名空间内的 `CbiHarness`/`FcbiHarness`/`DbiHarness`
    与源文件/工作集逻辑**抽到头文件**，bench 与 verify 工具同链接（八荣八耻 #4 复用，避免重复 wiring）。
  - **命名**：避免泛泛 `verify()` 与禁止的 `*Utils/*Helpers/*Common`（CLAUDE.md 规约）。给具名
    `class CacheVerifier { static Result verifySegmentAgainstSource(...); }` 或具名自由函数
    （如 `verifySegmentAgainstSource`、`verifyWorkingSet`）。
  - `verifySegmentAgainstSource(...)`：经 wrapper 读 [offset,len] → 与源文件直接 `pread` 同区间
    `memcmp`，并累计段级 checksum；不符则报 offset + 失败计数 + loud fail。
- **独立二进制 `velox_cache_verify`**：
  - 安装指定后端（`cbi`/`filecache`）指向**已持久**的缓存目录（不 wipe，复用 `--reuse_cache` 同义），
    重载缓存；遍历整个工作集经 wrapper 读回、逐区间校验源文件；全过 → 退出 0，任一不符 → 非零退出。
  - `dbi` 无应用缓存，读即源，校验退化为 sanity（可选跳过）。
  - 可脱离基准单跑，便于将来接 CI。

## 3. 运行矩阵

**micro（两阶段，同一 shell 顺序起两个进程）**
```
# Phase 1: prime（灌缓存、落盘、不删）
velox_bufferedinput_wrapper_benchmark --phase=prime --reuse_cache --wrappers=all \
  --target_ws_gb=32 --workloads=sequential,zipfian --read_sizes_kib=1024,8192 --batch=64

# Gate: 一次性正确性校验（cbi + filecache，逐区间字节对比源）。
# 用 && 链：前一个 verify pass（退出 0）才跑后一个；任一非零即中止，绝不进 measure。
velox_cache_verify --backend=filecache --reuse_cache --target_ws_gb=32 ... \
  && velox_cache_verify --backend=cbi --reuse_cache --target_ws_gb=32 ...

# Phase 2: measure（重启、不 wipe、重载、只测热）
velox_bufferedinput_wrapper_benchmark --phase=measure --reuse_cache --wrappers=all \
  --target_ws_gb=32 --workloads=sequential,zipfian --read_sizes_kib=1024,8192 --batch=64 \
  --measure_passes=3 --out=<report.md>
```

**TPCH（三个进程各自 `--rounds=3`；round1 冷灌，round2/3 热）**
- 数据集：`/home/chang/test/tpch-double/tpch-generated-100.0-parquet-decimal_as_double`（SF100）
- `cbi`：`--input_source=cbi --cache_gb=64 --cache_mem_gb=4 --cache_num_shards=4
  --ssd_path=/tmp/velox_ssd_cache --ssd_cache_gb=50 --velox_ssd_odirect=false --rounds=3 --out=...`
- `filecache`：`--input_source=filecache --cache_gb=64 --filecache_disk_gib=50
  --filecache_root=/tmp/velox_filecache --rounds=3 --out=...`
- `direct`：`--input_source=direct --rounds=3 --out=...`（不装任何应用缓存）
- 不开 `--cold_each_round`，使每个后端自身 round2/3 为可比的热轮。

## 4. 指标与缓存复用校验

- **主指标**：hot 读吞吐 / 时延。
  - micro：measure 阶段每个 (pattern × read_size × wrapper) 的 `MB/s`、`wall_ms` 中位。
  - TPCH：round2/3 的 `wall_ms`（每 query）。
- **缓存复用实证（关键）**：
  - micro measure 阶段 `src_MB ≈ 0`（无回源下载）且 cbi/fcbi `ssd_MB > 0` → 证明 SSD/磁盘有数据、
    读走了持久缓存。这也是 F7“跨重启可复用”的实证（key 不匹配会回源 → `src_MB` 暴涨立刻暴露）。
  - TPCH round2/3 `bytes_dl_mib ≈ 0`（filecache）/ 高 `hit_pct`（cbi）→ 证明热。
- **SP2 放行判定（量化门槛）**：满足以下任一即放行 SP2，以 `DirectBufferedInput` 为蓝本重写
  FileCache 热读路径（**两项同时不满足才否决**，勿只看吞吐而忽略 CPU 旁证）：
  - **吞吐**：`direct` hot 读 p50 MB/s ≥ `filecache` 的 **1.1×**，且两者信赖区间（measure 多趟）
    **不重叠**；或
  - **CPU**：相同热读负载下 `filecache` 单核 CPU 占比比 `direct` 高 **≥ 15%**（坐实 read-path 开销）。
  - 与早先 micro 数据 fcbi 慢 2–3× 的观测一致时，上述门槛应轻松满足；若**不**满足（direct 并不更快），
    则 SP2 暂缓，回头重新审视瓶颈定位。

## 5. F7（哈希）对本阶段的影响 — 不阻碍

逐行核实结论：**F7 不阻碍跨重启复用。**

- `FileCacheKey::fromPath`（`velox/common/caching/filecache/FileCacheKey.cpp:66`）用固定种子 `0,0`
  调 `SpookyHashV2::Hash128`，确定性哈希 → 同一路径同一二进制产出同一 key，跨重启不变。
- `ch::FileCache::initialize()`（`velox/common/caching/filecache/FileCache.cpp:449`）：
  `need_to_load_metadata = fs::exists(basePath)`，目录在即 `loadMetadata()`（:500）重建段。
  只要不 wipe，重启即复用。
- F7 唯一真实限制：算法不同（SpookyHashV2 ≠ SipHash128）→ 缓存目录无法与**真实 ClickHouse** 互通；
  与本阶段无关。
- **附带修正**：`velox/common/caching/filecache/FileCacheKey.cpp:69-71` 现注释
  “cache keys are process-local and the cache directory is NOT reusable across builds/restarts”
  **双重过严**。已逐行核实：种子硬编码 `h1=0,h2=0`（:67-68），`SpookyHashV2::Hash128` 算法稳定、
  无运行时盐、无 `__DATE__/__TIME__`/版本宏参与 → **跨重启与跨 build 都稳定**。订正注释直接写实：
  - 跨重启（同一二进制）：✅ 稳定。
  - 跨 build（不同编译产物）：✅ 稳定（固定零种子 + 稳定算法）。
  - 唯一不可互通：**与真实 ClickHouse**（算法不同：SipHash128 ≠ SpookyHashV2）。
  - 建议注释文案：`SpookyHashV2 with fixed zero seed: keys are stable across restarts AND builds.
    Only incompatibility is with real ClickHouse (SipHash128 vs SpookyHashV2).`
  审计文档 `docs/superpowers/reviews/2026-05-31-ch-filecache-port-audit.{md,html}`
  的 F7 影响已同步更正（2026-05-31）。

## 6. 交付物

- 代码：A（TPCH `direct`）+ B（micro `dbi` + 两阶段持久 flag）+ C（共享校验组件 + `velox_cache_verify`
  独立二进制 + harness 抽头文件）+ `FileCacheKey.cpp` 注释更正。
- flag 帮助串：`--phase`、`--reuse_cache`、`--wrappers=dbi|all`、`--input_source=direct` 的 help 文案；
  README/运行说明同步两阶段用法（非阻塞，与代码同 PR）。
- 报告：三方 hot-read 基线 `docs/superpowers/reviews/2026-05-31-direct-read-baseline.{md,html}`
  （md 为 ground truth，html for humans，随 repo 走）。
- 结论：写明 SP2 放行 / 不放行判定及依据数字。

## 7. 风险 / 注意

- micro `--reuse_cache` 误用（忘了它会保留旧缓存）可能污染下一次实验 → 报告里记录每次运行的
  effective config（现有 harness 已支持）。
- cbi RAM tier 不跨重启持久属其自身设计，本阶段不处理；只校验 SSD 有数据（用户明确）。
- direct 的“hot”依赖 OS page cache 驻留；32GiB 工作集需机器有足够空闲内存，否则 measure 会掺入
  磁盘读（属真实结果，报告中标注）。**measure 前打印 `/proc/meminfo` 的 `MemAvailable` 进报告**，
  作为“hot 是否真热”的旁证。
