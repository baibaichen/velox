# HANDOFF — rebase 后待办

> 本地 `ch-hashjoin` 于本次会话 rebase 到远程 `baibaichen/ch-hashjoin` 的
> `d5fabecd`（= 分叉点 `930e2b24d` + 远程 `75c61a98c` TrivialHash + `d5fabecdc`
> test round1）之上。本地这次会话的 21 个 commit 重放其上，冲突已解，测试全过。
> 备份 tag：`ch-backup-2` → rebase 前的 `ec11a039a`。未 push。

## TODO 1: hashed 路径 TrivialHash 对齐 CH 后 probe 变慢（待查 digest 生成端）

**背景**：rebase 并入了远程 `75c61a98c`——hashed map 的哈希函子从本地的
`HashWide<UInt128>`（对 128-bit digest 逐 word 再做 CRC32，双 CRC32）改成
`UInt128TrivialHash`（直接取 digest 低 64 位 `words[0]`），对齐 CH 的
`HashMap<UInt128, UInt128TrivialHash>`（ClickHouse HashJoin.h）。

**现象（release, gcc -O3, 4M rows, uniform, 本机 ChangDev；hashed 档 = 5xbigint 与
bigint_varchar，均 hash_mode=hashed）**：TrivialHash 对齐 CH 后 **probe 反而变慢**：

| 档 | 指标 | HashWide（双 CRC32，旧）| UInt128TrivialHash（对齐 CH，现）|
|---|---|---:|---:|
| bigint_varchar | probe | ~319 ms | ~493 ms（+55%）|
| 5xbigint | probe | ~574 ms | ~598 ms |
| bigint_varchar | build | ~539 ms | ~550 ms |
| 5xbigint | build | ~630 ms | ~634 ms |

**根因假设（未证实）**：TrivialHash 直接用 digest 低 64 位当开放寻址表的哈希。Port 的
digest 由 `HashedKeyDecoder`（XXH3-128，见 `velox/exec/ch/HashedKey.h`）生成，其低 64 位
在开放寻址（线性探测）下的分布可能不如"再过一遍 CRC32"均匀 → 桶冲突多、探测链长 → probe 慢。
CH 用 TrivialHash 能快，是因为 CH 的 digest 生成端保证了低位质量。

**待查**：Port 的 digest 生成（`HashedKeyDecoder::hash`，XXH3-128）是否与 CH 的 digest
生成一致。若要真对齐 CH 的 hashed 路径，可能连 **digest 怎么算**都要对齐（而非只对齐哈希
函子 TrivialHash）。用 perf 确认 TrivialHash 下 hashed 档的 branch-miss / 探测链是否变长。

**当前决定**：保留 `UInt128TrivialHash`（移植保真度对齐 CH 优先，性能次要——用户明确
"不要改进要对齐 CH"）。性能差距记此待查。

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
