# HANDOFF — CH→Velox coordinate hash join (stage1-10 状态 + 后续)

> 这份 handoff 让一个没有上下文的工程师能接手。项目：把 ClickHouse 风格的**坐标式 hash join**（payload 不转行、留列式保活、cell 存 8 字节坐标字、wrapInDictionary 延迟物化）移植进 Velox，走自己的 CH 算子（不复用 Velox 原生算子），跟 Velox 原生 join 公平对比 + 逐项修移植保真度。

## 代码位置

- **Velox 移植**（主体）：fork `https://github.com/baibaichen/velox` 分支 **`ch-hashjoin`**。容器 `gluten41` 内 `/root/oss/velox/velox/exec/ch/`（HEAD = task34，tag `ch-full-reviewed` 在 stage8 收官点 `cdfc327ef`，之后 stage10 task33/34 在其上）。
- **ClickHouse 原生 benchmark 脚手架**：fork `https://github.com/baibaichen/ClickHouse` 分支 **`ch-hashjoin`**。容器内 `/root/oss/clickhouse/`（分支 `ch-native-layer1-bench` == `ch-hashjoin`）。**本地测量脚手架，不回上游 CH。**
- 设计/结果文档在 host `C:\Users\changchen\MS\kandor\work\hashjoin\ch\`（编号 01-17 + tasks/ 派发 + HANDOFF_*）。

## 编译参数

### Velox

#### 本机（ChangDev）复现步骤

```bash
# 代码位置
1. 当前文档位置: ../../../../ <=> ~/OpenSource/velox
2. cmake 已经好了 => /usr/bin/cmake --build /home/chang/OpenSource/velox/cmake-build-release-gcc13 --target velox_exec_ch_hashtable_layer_benchmark -j 30
```

#### 容器复现步骤

封装脚本 `/root/oss/velox-help/build.sh`（自动 source `env.sh` 带全套 VCPKG env）：
```
build.sh config             # 配 _build/debug
build.sh <target>           # 增量编 debug target
build.sh config-release     # 配 _build/release (-O2 -DNDEBUG)
build.sh release <target>   # 增量编 release target
```
configure flag：`-DCMAKE_BUILD_TYPE=Debug/Release -DVELOX_GFLAGS_TYPE=static -DVELOX_BUILD_TESTING=ON -DVELOX_ENABLE_BENCHMARKS=ON -DVELOX_ENABLE_EXEC=ON -DVELOX_ENABLE_GROUPED_TESTS=OFF -DVELOX_MONO_LIBRARY=ON -DVELOX_BUILD_RUNNER=OFF`
- gtest：`velox_exec_ch_test`（Debug）；benchmark：`velox_exec_ch_hashtable_layer_benchmark`（Release）
- 日志 `/tmp/velox-build-logs/`。链接慢（`libvelox.a` ~4.7G mono）——后台编。

### ClickHouse（原生 benchmark）

#### 本机（ChangDev）复现步骤
```bash
# debug
/home/chang/.local/share/JetBrains/Toolbox/apps/clion/bin/cmake/linux/x64/bin/cmake --build /home/chang/SourceCode/ClickHouse/cmake-build-debug-clang.21 --target ch_hj_bench -j 30

# release
/home/chang/.local/share/JetBrains/Toolbox/apps/clion/bin/cmake/linux/x64/bin/cmake --build /home/chang/SourceCode/ClickHouse/cmake-build-relwithdebinfo-clang.21 --target ch_hj_bench -j 30
```

#### 容器复现步骤

| | Debug (`build_debug`) | Release (`build_release`) |
|---|---|---|
| BUILD_TYPE | Debug | Release |
| C/C++ 编译器 | `/usr/bin/clang(++)` | **`/usr/local/bin/clang(++)-21`** |
| ENABLE_RUST | **OFF** | **OFF**（必须，见 memory `ch-native-build-env`）|
| ENABLE_TESTS | ON | OFF |
| ENABLE_EXAMPLES / BENCHMARKS | OFF / OFF | OFF / OFF |
| SANITIZE | OFF | OFF |
- benchmark 编：`cd /root/oss/clickhouse/build_release && ninja ch_hj_bench`。首次全量编 dbms ~90min，之后增量（ccache）。
- ⚠ debug 用默认 clang、release 用 clang-21（两 build 编译器版本不同）。`ch_hj_bench` 是**无条件 add_executable target**（`src/Interpreters/CMakeLists.txt`），绕过 ENABLE_EXAMPLES/BENCHMARKS=OFF，直接链 `ch_contrib::gbenchmark_all` + `dbms`。

## 已完成（stage1-9 + stage10 部分）

- **stage1-5**：坐标底座（RowRef/Arena/RowRefList）+ 保活容器 + 真 CH map + probe + 输出物化 EmitGather + 接进 Velox pipeline（ChHashJoinNode/算子/bridge），端到端 == 原生。tag `ch-full-reviewed`。
- **stage6**：窄 payload 甜区门槛（`13_*`）。结论：速度门槛 N≤1、内存才是真交叉点。
- **stage7**：哈希表分层（joinProbe/listJoinResults、prepareJoinTable/addRowReferences）+ 第 1 层公平 benchmark + 滚动预取 + build reserve（`14_*`）。
- **stage8**：任意 key 组合——packFixed 多列 keys128/256、serialized 字符串、hashed XXH3-128 摘要（`15_*`）。stage8 整体审过。
- **stage9**：CH 原生第 1 层对齐 benchmark（不走 query 手工驱动 HashJoin + `template<bool layer1_only>` if constexpr 零成本插桩 + gbench）三方对比（`16_CH_NATIVE_ALIGNMENT_*`）。
- **stage10 task33**（`ch-stage3task33-reviewed`）：**定长 key 去 saved_hash**（移植 bug：定长 key 误用 `HashMapCellWithSavedHash`，CH 用 plain cell）。cell key64 24→16B、内存 -33%、随机 4M probe +14.3%。
- **stage10 task34**（`ch-stage3task34-reviewed`，**BLOCKED**）：packFixedBatch——实现正确但**复合 key probe 回退 -23~58%**（batch buffer 多一层内存往返，probe 反而慢；build +17~52%）。**不合入 probe 路径**；假设「复合慢=缺 packFixedBatch」被实测推翻。

## ⚠ 两个 benchmark BUG（关键，影响结论可信度）

1. **[已修 task33] 定长 key 错存 saved_hash**——cell 撑大 50%、每 probe 多一次 hash 比较。白盒审查（`16_MIGRATION_FIDELITY_REVIEW.md`）抓出，黑盒 benchmark 看不见。
2. **[未修] CH 原生 benchmark 第 1 层臂 probe 被编译器优化掉（dead-code）**——`16_CH_NATIVE_ALIGNMENT_BENCHMARK.md` 里 **CH-native 那整列无效**。证据：ns/probe 从 3MB 到 256MB 表**平坦在 6-7ns**（真标量开放寻址超 L3 必飙到 ~100ns）。根因：第 1 层臂跳过 processMatch、`find_result` 无可观察用途 → 编译器删掉 bucket 访问。**修法（只改 benchmark，不动 CH 算法）**：让第 1 层臂把命中 row 引用写进 sink buffer（对齐 Velox `hits`）+ `benchmark::DoNotOptimize`，逼编译器真跑 probe。文件：`clickhouse/src/Interpreters/examples/ch_hj_bench.cpp`（runProbe/probeOnce）+ 第 1 层插桩 `HashJoinMethodsImpl.h:653-666`。Velox 参照 `ChHashTableLayerBenchmark.cpp` 的 `doNotOptimizeAway`。

## 方法论教训（重要）

黑盒 benchmark（三方对比）**看不见实现级 bug**——saved_hash 错配、probe 被优化删，都靠**白盒读代码审查**抓；反过来审查的「应该更快」判断（packFixedBatch 可补、hashed 双 CRC32 可补）多次被实测推翻/打折。**两法必须合流：benchmark 量差距、审查查机理，任一单用都会得错结论。** 数字反直觉（如 CH 5ns/probe、15-23× 差距）要深挖，别用「框架差异」打发。

## 后续待办（优先级）

1. **🔴 先修 CH benchmark dead-code**（上面 bug 2）——否则所有「对照 CH 原生」的三方结论不可信，stage10 后续移植修复无真基线可依。修完重跑三方，重新校准优先级。
2. **重估 task34**：packFixedBatch build-only（build +17~52% 真收益）是否单独值得；probe 路径不用它。
3. **stage10 剩余可补移植项**（`17_MIGRATION_FIX_DESIGN.md`，**待真 CH 基线后重排**）：hashed 去二次 CRC32（`HashMap.h:47-58`，摘要低 64 位直接定位，审查 §3.1）；定长 raw-pointer fast path（审查 §2.2）；自适应预取 [4,32]+L2 门控+build 预取（审查 §4）。**每项逐项 A/B、实测验证，别信读代码的「应该更快」。**
4. **算法差距（非移植、另立新能力项目）**：array/normalized hash 模式（顺序/低基数）、长字符串流式 hash、two-level 并发 map、key8/16 direct map、range conversion、LowCardinality cache。这些是 CH 系相对 Velox 的固有差距或未搬能力，补移植追不上，是独立工程。
5. **顺序 4M 剩余 32%**：task33 后 port/Velox 0.52→0.57，saved_hash 只值 ~5pp，主体是 normalized-key 算法能力——归入第 4 点。另有 `HANDOFF_seqkey_probe_perf_investigation.md`（真机 perf 待查）。

## 核心结论（截至 task34，注意 CH 基线待修）

CH 移植哈希表：**随机 key64 反超 Velox**（4M 1.14×、build 1.17×、内存省）；**顺序/低基数、长字符串输**（主体算法差距）；**复合 fixed 输**（部分移植、部分算法，packFixedBatch 对 probe 无效）。移植真能补的：saved_hash（已修）、可能的 hashed 双 CRC32 / raw-pointer / 预取（待真基线 A/B）。**是否值得进 Gluten 集成，取决于修完可补项后 port 相对 Velox 的真实竞争力 + 真实 Spark workload 的 key 分布（多为顺序/低基数/复合——恰是 CH 系弱项）。**
