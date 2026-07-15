# HANDOFF — rebase 后待办

> 本地 `ch-hashjoin` 于本次会话 rebase 到远程 `baibaichen/ch-hashjoin` 的
> `d5fabecd`（= 分叉点 `930e2b24d` + 远程 `75c61a98c` TrivialHash + `d5fabecdc`
> test round1）之上。本地这次会话的 21 个 commit 重放其上，冲突已解，测试全过。
> 备份 tag：`ch-backup-2` → rebase 前的 `ec11a039a`。未 push。

## TODO 1: hashed 路径 TrivialHash 对齐 CH 后 probe 变慢（根因已查明，现状保持对齐）

**背景**：rebase 并入远程 `75c61a98c`——hashed map 哈希函子从 `HashWide<UInt128>`
（对 128-bit digest 逐 word 再 CRC32）改成 `UInt128TrivialHash`（直接取低 64 位
`words[0]`），对齐 CH `HashMap<UInt128, UInt128TrivialHash>`（HashJoin.h）。

**现象（release, gcc -O3, 4M uniform, 本机 ChangDev）**：

| 档 | 指标 | HashWide（旧）| TrivialHash（现，对齐 CH）| SipHash digest 实验 |
|---|---|---:|---:|---:|
| bigint_varchar | probe | ~310 ms | ~486 ms（+57%）| ~570 ms（更慢）|
| 5xbigint | probe | ~569 ms | ~594 ms | ~724 ms（更慢）|

**根因（已用 perf + 微基准 + 受控实验查实，非假设）**：

1. **不是哈希质量/探测链**：独立实验证 XXH3 digest 的 `words[0]` 与 SipHash 的
   探测链逐项相同（avg 1.4558、p99=6、23.8% 链>1）。桶分布不是问题。
2. **不是内存访问次数**：L3 miss 两版相近（TrivialHash 甚至略多）。
3. **真因是内存级并行（MLP）差异**（perf 铁证）：
   - `l1d_pend_miss.fb_full`：HashWide 451M（fill buffer 更满）vs TrivialHash 393M。
   - 平均在途 miss：HashWide 2.07 vs TrivialHash 1.55。
   - perf annotate：probeLoop 里 `mov 0x8(%r12)`（读哈希桶 cell）占 **87.6%** 周期
     —— 瓶颈就是读桶那次内存 load 的延迟。
   - HashWide 的两次 CRC32 是**便宜指令**，填了循环调度气泡、提高 MLP，把读桶延迟
     藏住了；TrivialHash 零指令，藏不住，串行等 load。

**关键辨析（digest 计算 vs 哈希函子是两回事）**：
- HashWide 快 = digest 仍是便宜的 XXH3（没变），只在**函子端**加两条便宜 CRC32
  填气泡 → 净赚。
- 换 **SipHash digest** 实验（改 HashedKey.h::update 用 SipHash128）**更慢**（570/724）：
  把 **digest 计算本身**换成昂贵的 SipHash，probe 每行重算完整 SipHash 轮次，
  计算成本暴涨盖过 MLP 收益 → 净亏。**所以"重哈希→快"是错的规律**，对的是
  "便宜 digest + 函子端便宜指令填气泡→快"。

**CH 为什么用 SipHash digest 却不慢**：CH hashed map 有 `saved_hash` 缓存
（HashMapWithSavedHash），digest 只在 build 算一次存进 cell，**probe 不重算**。
Port 的 hashed map（`HashMapAll`，无 saved_hash）**probe 每行重算 digest**——
这才是 Port 换 SipHash 暴慢、且现状（XXH3 每行重算）也偏慢的结构性原因。

**现状对齐 CH 的结论**：现状（XXH3 digest + TrivialHash 函子 + 无 saved_hash）在
**哈希函子层已对齐 CH**（都 TrivialHash）。慢是对齐的代价，非 bug。**保持现状**
（用户明确"对齐 CH 不发挥"）。

**若未来要提速且仍对齐 CH**：正解是给 hashed map 加 `saved_hash` 缓存（对齐 CH
`HashMapWithSavedHash`，probe 不重算 digest），而**非**换 SipHash、也非退回 HashWide
函子（后者偏离 CH 的 TrivialHash）。这是独立工程，非本轮范围。

**未锤死的边界**：微基准（纯查表 / +XXH3 / +24字节cell）三次都复现不出真实的
TrivialHash-慢翻转——真实翻转来自完整 probeLoop 里命中处理 + 哈希函子的**编译器整体
调度/寄存器分配交互**，微基准的小循环优化方式不同。要再往下需反汇编对比两版
probeLoop 寄存器分配，收益递减，未做。

## TODO 1b: hashed digest 生成端未对齐 CH（XXH3 vs SipHash）

**现状**：hashed 路径的两个"哈希"，对齐情况不同：

| 层 | Port | CH | 对齐 |
|---|---|---|---|
| 哈希函子（digest→桶号）| `UInt128TrivialHash`（取 `words[0]`）| `UInt128TrivialHash`（同）| ✅ 已对齐（`75c61a98c`）|
| digest 生成（key→128bit）| **XXH3-128**（`HashedKey.h:95`）| **SipHash128**（`hash128`→`SipHash::get128()`，`ColumnsHashing/HashMethod.h:20-29`）| ❌ **未对齐** |

**为什么 digest 端未对齐**：XXH3 是 Port 早先自选（velox 自带 xxhash），非移植 CH。
CH 用 SipHash128。

**不能单换 SipHash**：本轮实测单换 SipHash digest（保持无 saved_hash）probe **更慢**
（bigint_varchar 486→570 ms），因为 Port hashed map 无 saved_hash 缓存、**probe 每行
重算 digest**，SipHash 比 XXH3 贵得多。见 TODO 1。

**完整对齐 CH digest 端 = 成套改，非单点**：需**同时**
1. digest 生成换 SipHash128（对齐 CH `hash128`），且
2. 给 hashed map 加 `saved_hash` 缓存（对齐 CH `HashMapWithSavedHash`），build 算一次
   存 cell、probe 不重算——这是 CH 用 SipHash 却不慢的关键。
单做 (1) 只会更慢；(2) 本身也能让现状 XXH3 提速（不必换 SipHash）。属独立工程，非本轮范围。

## TODO 2: 远程 3 个未并入的对齐-CH commit（在 d5fabecd 之后）


review 已确认这 3 个是"对齐 CH"方向、但与本地改动硬冲突，需手动整合：

- `2b5fdf2dc` test：pin hashed digest 字面值 —— **并入时需按本地 HEAD 实际 hash 重算
  字面值**（本地 key_string 改 CRC32、hashed 改 TrivialHash 后，pin 的 digest 值会变）。
- `fefc88a55` feat：定长 key 裸指针快路径 —— 重写 `FixedKey.h` 的 pack API +
  build/probe。与本地独立建的 `FixedKeyDecoder`（`packAll`/`packedAt`）结构性冲突，需手动
  重贴到本地 decoder。CH 有此快路径。
- `621aeb774` feat：自适应 L2 门控预取（新增 `Prefetching.h`/`.cpp` + 测试）。对齐 CH 的
  `getL2CacheSize()` + `PrefetchingHelper`（自适应 look-ahead [4,32]），**应取代**本地这次
  加的固定 8MiB 门控 + 固定 look-ahead 16 预取（本地 commit `add rolling build-side
  prefetch`）。并入时：引入 `Prefetching.h`/`.cpp` 作新文件，再把本地**所有**预取点
  （定长 + 字符串、build + probe + `probeLoop`）都改走 `JoinPrefetcher`。依赖 `fefc88a55`。

## 本地这次会话已做的对齐-CH 改动（供整合时区分，勿回退）

这些本身就是**对齐 CH**、不是偏离，整合远程时应保留：
- key_string 哈希 `bits::hashBytes` → CRC32（`simd::crc32U64`）：对齐 CH 的 key_string CRC32。
- key_string build 双 find → 单次 emplace：对齐 CH `insertAll`（emplaceKey 一次查或插）。
- build 加软件预取：对齐 CH build 有预取（但门控是固定 8MiB，见 TODO 2，应被远程自适应取代）。
- `FixedDirectMap`（key8/key16 直接寻址）：对齐 CH `FixedHashMap`。
- `Map32`（key32/keys32 用 UInt32 表）：对齐 CH `key32`。
- `FixedKeyMap::Type` enum + chooseType：对齐 CH `chooseMethod` 的显式 Type 派发。

**偏离 CH、待远程取代的**：固定 8MiB 预取门控 + 固定 look-ahead 16（应换 `621aeb774` 的自适应 L2）。

## 已知遗留（非本次 rebase 引入）

- varchar_short probe/build 仍慢于 CH native ~15-30%：CRC32 修了 hash 分支后残余差距，
  疑似字符串路径其他细节 + 编译器 gcc-O3 vs clang-O2 差异。见三方 benchmark。
