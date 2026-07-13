# HANDOFF — 顺序 key hash 表 probe 32% 差距的 perf 调查

> 目的：容器里 perf 无权限（`perf_event_open` 被挡），把这个性能谜题打包到真机用 perf 查。分支已推：`https://github.com/baibaichen/velox` 分支 **`ch-hashtable-layer-stage7`**（commit `d4e5e8ce8`，含完整实现 + benchmark）。

## 一句话问题

第 1 层哈希表公平 benchmark（纯 hash 表 build/probe，排除 payload/输出物化）里，**顺序 4M int64 key、hash 表对 hash 表**这一档，CH 坐标式 hash 表的 probe 吞吐只有 Velox 的 **0.68×**（CH 31.46 vs Velox 46.12 M key/s）。**已用读代码排除一圈候选，剩余 32% 差距的真因需 perf 实测。**

## 背景：这是什么 benchmark

- **两层模型**：第 1 层 = hash 表本身（hash + bucket 探测 + 核对 key 确认命中）；第 2 层 = 展开多匹配 + 坐标解引用 + 输出物化。本 benchmark 只测第 1 层。
- **两臂**：`ChEngine`（CH 坐标式 `ch::HashMap` = `HashMapAll_key64`，开放寻址 + saved_hash 先筛 + cell 内联比 key）vs `VeloxEngine`（Velox `HashTable`，bucket + SIMD tag + 回 RowContainer 比 key，**自适应 hash mode**）。
- **公平口径**：build 都计到"表可查"（CH 已 reserve、Velox prepareJoinTable 一次分配）；probe 都含 hash + key 核对、都不展开多匹配、不物化；fanout=1 全命中；单 key64。CH probe 已加滚动软件预取（look-ahead 16，8MiB 门控）。
- benchmark 文件：`velox/exec/ch/benchmarks/ChHashTableLayerBenchmark.cpp`。结果文档：`work/hashjoin/ch/14_HASHTABLE_LAYER_BENCHMARK.md`。设计：`work/hashjoin/ch/14_HASHTABLE_LAYER_DESIGN.md`。

## 完整数字（reserve + 预取后，排除 array 档）

**probe（hash 表对 hash 表，M key/s）**：
| 分布 | keys | Velox mode | CH | Velox | CH/Velox |
|---|---:|---|---:|---:|---:|
| 顺序 | 4M | normalized_key | 31.46 | 46.12 | **0.68×** ← 本调查目标 |
| 随机 | 400K | hash | 65.37 | 40.03 | 1.63× |
| 随机 | 4M | hash | 24.17 | 15.44 | 1.57× |

**关键对照**：随机 key（Velox 落 `hash` 模式）时 CH 反超 1.57-1.63×；只有顺序 key（Velox 落 `normalized_key`）时 CH 输。**所以差距与 Velox 的 hash mode 强相关。** 顺序 40K/400K Velox 落 `array`（直接数组寻址、非 hash 表），已排除、不在调查范围。

## 已排除的候选（附代码依据，别重复排查）

1. **normalized_key 编码本身** — 对单 int64 key 是空操作（normalized = key - min，仍是一个 int64）。且 build 时 `mixNormalizedKey`（`HashTable.cpp:442` = `folly::hasher`）把 normalized key 又打散一遍才定位 bucket，所以顺序 key **不会**连续聚进同一 bucket。排除"编码省事"和"bucket 聚集局部性"。
2. **内存布局单跳/双跳** — CH cell 内联（key+坐标+saved_hash 一跳），Velox bucket→行两跳。CH 布局反而更省一跳，不是差距来源。
3. **软件预取缺失** — 已在 task23 补上（CH `ChHashProbe.cpp` 滚动预取），大表 probe 提升 31-73%；顺序 4M 从 0.47×→0.65×→0.68×。预取补了大头，剩余 32% 不是预取缺失。
4. **rehash / reserve artifact** — task24 给 CH 加 reserve（rehash 8 次→0-1 次，内存 480→192 MiB）。顺序 4M probe 仅 0.65→0.68×，**reserve 不显著改善 probe**。排除"rehash 后布局连累 probe"。

## 待验证假设（perf 要区分的）

顺序 4M、Velox=normalized_key vs CH=saved_hash，CH probe 慢 32%。候选：
- **H1 比较路径指令数**：Velox normalized_key 命中后比一个 8 字节 word（`RowContainer::normalizedKey(group)==keys[row]`）；CH 比 saved_hash（先筛）+ 原始 key。指令数/分支差异？
- **H2 SIMD tag 并行**：Velox 一次 SIMD load 16 个 tag（128B bucket）并行筛（`loadTags`+`TagVector`）；CH 开放寻址逐 cell 比 saved_hash。分支预测失败率 / 每命中指令数差异？
- **H3 探测链长度 / 装载因子**：CH 装载因子 0.5（`ch/HashTable.h:67`）；Velox bucket 装填不同。顺序 key（经各自 hash 打散后）平均探测步数是否 CH 更长？
- **H4 cache 行为**：两边都预取了，但 miss 数 / LLC 命中率是否仍有差（顺序 key 的行在 Velox RowContainer 里连续，probe 顺序访问命中热 line；CH cell 散在 buffer_）？

## 真机复现步骤

```bash
# 1. clone 分支
git clone https://github.com/baibaichen/velox.git
cd velox && git checkout ch-hashtable-layer-stage7   # commit d4e5e8ce8

# 2. Release 编 benchmark（按 velox 标准 Release 构建）
#    target: velox_exec_ch_hashtable_layer_benchmark
#    产物: _build/release/velox/exec/ch/benchmarks/velox_exec_ch_hashtable_layer_benchmark

# 3. 先复现 0.68×（确认真机也有这个差距）
./velox_exec_ch_hashtable_layer_benchmark   # 跑全矩阵,看 sequential 4M ch vs velox probe

# 4. perf：只测顺序 4M probe。benchmark 目前把 build+probe 一起跑,
#    真机上建议加一个只跑单档的 flag,或用 perf record 采样后按符号过滤 probe 热函数:
#    CH probe 热函数:  ch::joinProbe / HashMapTable::find / findCell
#    Velox probe 热函数: joinNormalizedKeyProbe / ProbeState::joinNormalizedKeyFullProbe
perf stat -e cycles,instructions,branch-misses,\
cache-references,cache-misses,LLC-load-misses,\
L1-dcache-load-misses,dTLB-load-misses \
  ./velox_exec_ch_hashtable_layer_benchmark   # 分别对两臂,或 perf record+report 按符号拆
```

## 该看什么（把结果对到假设）

- **instructions / cycle（IPC）**：CH 低 → 更多 stall（指向 H4 cache 或 H3 探测链）；指令数 CH 高 → 比较路径更重（H1/H2）。
- **branch-misses**：CH 高 → 逐 cell 标量探测的分支预测差（H2，Velox SIMD 无分支筛 tag）。
- **LLC-load-misses / cache-misses**：CH 高 → cell 散布 + 预取没完全藏住（H4）；两边相近 → 差距不在 cache，在计算路径。
- **dTLB-load-misses**：大表跨页访问差异。
- 关键判别：**若 miss 数两边相近但 CH cycles 高 → 差距在计算（H1/H2/H3）；若 CH miss 显著高 → 差距在 cache/访存（H4），可能预取深度/门控要调。**

## 关键源码定位（真机对照读）

- CH probe：`velox/exec/ch/ChHashProbe.cpp`（`joinProbe` + 滚动预取）；`velox/exec/ch/HashMap.h`（cell 结构）；`velox/exec/ch/HashTable.h`（`findCell` :411-437,`grower.place` :77,装载因子 :67）。
- Velox probe：`velox/exec/HashTable.cpp`（`joinNormalizedKeyProbe` :697-720,`joinProbe` 分派 :610,`mixNormalizedKey` :442,`compareKeys` :359）；`velox/exec/HashTable.h`（Bucket 16-slot :824-861,`bucketOffset` :1130）。

## 期望产出

perf 数据指向 H1/H2/H3/H4 中哪个（或组合）是顺序 key 32% 差距的主因。这决定甲方向对顺序/低基数 key 的优化方向：若是 H2（SIMD tag）→ 考虑给 CH 加 SIMD 批量筛；若是 H3（探测链）→ 调装载因子；若是 H4（cache）→ 调预取深度/门控。**注：真实 join key 常是顺序/低基数（自增 ID、维度编号），所以这 32% 值得查清。**
