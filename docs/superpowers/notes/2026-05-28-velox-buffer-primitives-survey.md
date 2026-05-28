# Velox 缓冲与小向量原语调研（FsCache phase-2 选型）

> **已尝试被 revert（2026-05-28）。** 本 doc §4/§5 列出的两条候选
> （`DataBuffer<char>` 替换 `unique_ptr<char[]>`、`folly::small_vector<,4>` +
> HWM unique_ptr + LocalReadFile cache）已实验性套用，全部因 q17 +4–7%
> 回归被 revert。回归详情见
> `docs/superpowers/results/2026-05-28-fscache-q17-reprofile-post-memset.md` §6。
> 本 doc 保留作为"候选评估"留底；§6 "下一步"不再适用。

目标：为 FsCache 热路径两项优化挑选合适的原语。

1. `FsCacheInputStream::loadCurrentSegmentBuffer` 每次 `std::make_unique_for_overwrite<char[]>(length)`，希望容量足够时复用既有分配。
2. `FsCache::getOrSet` 路径上的三处 `std::vector` 堆分配（`lookupRangeUnlocked`、`fillHolesWithEmptyFileSegments` 返回值；`FsCacheBufferedInput::load` 下载缓冲）。TPC-H 主流为单段（N=1），希望小 N 时栈内联，N 增长时再回退到堆。

调研为只读，所有引用均给出 `file:line`。

---

## 1. Velox 中可用的缓冲类型

| 名称 | 头文件 | 用途 | 生命周期 | 初始化 | 内存源 |
| --- | --- | --- | --- | --- | --- |
| `Buffer` | `velox/buffer/Buffer.h:54` | RC 基类，对接 `BufferPtr` / Vectors 借用 | `shared_ptr` 引用计数 | 由派生类决定 | `MemoryPool` |
| `AlignedBuffer` | `velox/buffer/Buffer.h:350` | 16B 对齐的 POD 缓冲，Vectors 主体 | RC（`BufferPtr`） | 默认零填充，`allocate<T>(pool, size, /*initialize=*/false)` 可跳过 | `MemoryPool` |
| `NonPODAlignedBuffer` | `velox/buffer/Buffer.h:657` | 非 POD 元素的对齐缓冲 | RC | 默认构造每个元素 | `MemoryPool` |
| `BufferView` | `velox/buffer/Buffer.h:776` | 借用外部内存，零拷贝 | RC | 不拥有 | 外部 |
| `dwio::common::DataBuffer<T>` | `velox/dwio/common/DataBuffer.h:29` | dwio 路径专用的可增长 trivial 缓冲（首次 `allocateZeroFilled`，`reserve` 改用 `allocate` 不清零；具备容量复用语义） | move-only，独占所有权 | 首次构造零填充；扩容路径**不**清零 | `MemoryPool` |
| `raw_vector<T>` | `velox/common/memory/RawVector.h:34` | 带 SIMD padding 的无默认构造容器，要求 trivial | move-only | 不清零 | 可选 `MemoryPool` 或全局 `malloc` |
| `ScratchPtr<T, inlineSize>` | `velox/common/memory/Scratch.h:112` | 表达式求值临时区，`<=inlineSize` 不分配 | RAII 借出归还 | 不清零 | `Scratch` 内部池 |
| `AllocationPool` | `velox/common/memory/AllocationPool.h` | bump-pointer arena，适合短期多次小分配 | arena 整体释放 | 不清零 | `MemoryPool` |

是否适合 phase-2 的"短期单缓冲、复用容量"诉求：

- `AlignedBuffer` / `Buffer`：RC、需要 `BufferPtr`，对当前只在 `FsCacheInputStream` 内部用的私有缓冲来说 API 太重；`AlignedBuffer::allocate(..., initialize=false)` 才能跳过零填充（默认零填充会抵消现有的 `make_unique_for_overwrite` 优化）。
- `DataBuffer<char>`：dwio 既有惯用 idiom（`velox/dwio/common/SeekableInputStream.h:135` 的 `buffer_`；`velox/dwio/common/OutputStream.h:126`；`velox/dwio/common/DataBufferHolder.h:60`）。`reserve` 在容量足够时直接返回（`DataBuffer.h:96-101`），不足时 `reallocate`（`DataBuffer.h:109-111`），扩容路径不清零（不调用 `allocateZeroFilled`），完美贴合"容量足够直接复用、不足才扩"的需求。代价：首次构造会触发一次 `allocateZeroFilled`（`DataBuffer.h:36`），但这只发生一次。
- `raw_vector<char>`：可以，但其设计目标是 SIMD padding；FsCacheInputStream 不需要这点，而且 `raw_vector` 既支持 `MemoryPool` 又支持裸 `malloc`，比 `DataBuffer` 多一层路径开销，且未在 dwio 输入流场景使用过。
- `ScratchPtr` / `AllocationPool`：是表达式/算子执行期的短期借出区，不属于跨 `Next()` 调用、跨多次 IO 复用的所有权模型。
- `AlignedBuffer` 的 no-init 形式（`Buffer.h:632`-起的特化）：仍然要 `BufferPtr` 包装，API 复杂度比 `DataBuffer` 高，无明显收益。

## 2. Velox 中已被使用的 folly 容器

- `folly/container/F14Map.h` / `F14Set.h`：在 `velox/functions/` 下出现数十处（如 `velox/functions/lib/KHyperLogLog.h:21`、`velox/functions/prestosql/ArraySum.cpp:17`），是 hash 容器的事实标准。
- `folly/container/small_vector.h`：当前仅 1 处使用——`velox/functions/prestosql/aggregates/ArrayAggAggregate.cpp:17` 引用，`ArrayAggAggregate.cpp:42` 声明 `folly::small_vector<SourceRange, 2> sources;`。即引入新的 `small_vector` 调用点不属于"创造接口"，但属于较少见的 idiom——必须明确头文件路径 `folly/container/small_vector.h`。
- 没有 `folly::fbvector` 用例。

## 3. FsCache `getOrSet` 三处 vector 的形态确认

1. `lookupRangeUnlocked` —— `velox/common/caching/fscache/FsCache.cpp:170-192`：返回 `std::vector<FileSegmentPtr>`，调用方 `FsCache::getOrSet` 在同一作用域内 `std::move` 进 `fillHolesWithEmptyFileSegments`（`FsCache.cpp:290-291`），不导出。
2. `fillHolesWithEmptyFileSegments` —— `velox/common/caching/fscache/FsCache.cpp:196-243`：返回 `std::vector<FileSegmentPtr>`，结果落到 `slots`（`FsCache.cpp:284`），最终 `std::make_unique<FileSegmentsHolder>(...)` 入 holder。
3. `FsCacheBufferedInput::load` —— `velox/dwio/common/FsCacheBufferedInput.cpp:276-287`：`std::vector<char> buf(min(1MiB, segSize))`，在后台下载线程 lambda 内，**单个 segment 一个 buf，循环 1MiB chunks**。和 `getOrSet` 热路径不在同一帧，是后台下载路径。

`FileSegmentsHolder` 的存储——`velox/common/caching/fscache/FileSegmentsHolder.h:90`：`std::vector<FileSegmentPtr> segments_`。构造（`FileSegmentsHolder.h:34`）接受 `std::vector<FileSegmentPtr>`，公共 getter `segments()`（`FileSegmentsHolder.h:67,72`）返回 `std::vector<FileSegmentPtr>&`。

`segments()` 的消费者：

- `velox/dwio/common/FsCacheBufferedInput.cpp:162` —— 传给 `FsCacheInputStream` 构造器（`FsCacheInputStream.h:41` 也是 `std::vector<cache::fs::FileSegmentPtr> segments`）。
- `velox/dwio/common/FsCacheBufferedInput.cpp:233` —— `for (auto& seg : enqueued.holder->segments())`。
- `velox/common/caching/fscache/tests/FsCacheConcurrencyTest.cpp:140,360`、`FsCacheBufferedInputTest.cpp:277,281,399`、`FsCacheTest.cpp:122,141,177` —— 均为 `for (auto& seg : segments)` / `.empty()` / `.size()` / `.front()`。

→ 若把 holder 内部存储改成 `folly::small_vector<FileSegmentPtr, 4>`，调用方代码不需要改：range-for、`.empty()`、`.size()`、`.front()`、`.back()`、operator[] 都被 small_vector 支持。**唯一硬约束**：`FsCacheInputStream` 构造函数（`FsCacheInputStream.h:41,59`）签名是 `std::vector<cache::fs::FileSegmentPtr>` —— 这条引用点会形成 ripple：要么改成接受 `folly::small_vector<…, 4>`（连同成员 `segments_` 一起改），要么 holder 的 `segments()` 仍然暴露 `std::vector` 接口（即 holder 内部存储类型保持 `std::vector`，仅把 `getOrSet` 流水线中的中间局部变量换 `small_vector`，最后一步再 move 进 `vector` 给 holder）。

## 4. 推荐 A —— `FsCacheInputStream` 内的 buffer 复用

**采用 `dwio::common::DataBuffer<char>` 替换 `std::unique_ptr<char[]> buffer_ + size_t bufferSize_`。**

- 文件：`velox/dwio/common/DataBuffer.h:29`。
- 语义匹配：`reserve(capacity)` 在 `capacity <= capacity_` 时直接返回（`DataBuffer.h:96-101`），扩容时调用 `pool_->reallocate` 且不清零（`DataBuffer.h:109-111`）。这正是想要的"容量足够直接复用、不足才扩、且不要 zero-fill 抵消现有的 `make_unique_for_overwrite` 收益"语义。
- 既有惯例：`SeekableInputStream.h:135` 的 `DataBuffer<char> buffer_;` 是同类输入流的事实标准；`OutputStream.h:126` 同样。FsCacheInputStream 采纳此模式属于"遵循规范"而不是"创造接口"。
- 代价：构造函数需要一个 `MemoryPool&`。`FsCacheBufferedInput` 已经持有 pool（`FsCacheBufferedInput` 继承自 `BufferedInput`，后者 ctor 需要 `MemoryPool*`），把 pool 透传给 `FsCacheInputStream` 构造函数即可。
- 首次构造的 `allocateZeroFilled`（`DataBuffer.h:36`）：默认 `size=0`，零字节分配，无开销；首次 `reserve` 走 `allocate` 不清零分支（`DataBuffer.h:107`）。
- 字段调整：`buffer_` 改 `DataBuffer<char>`，`bufferSize_` 字段可删（已被 `buffer_.capacity()` 覆盖；逻辑大小用一个新 `payloadSize_` 或同样的 `bufferSize_` 保留为"本次有效数据长度"，区别于 capacity）。

考虑过但**未采用**：

- `AlignedBuffer` + `BufferPtr`：RC 包装；要拿到 raw 指针还得 `buffer->asMutable<char>()`；no-init 版本（`Buffer.h:632`）只有 `allocate<T>(pool, n, /*initialize=*/false)` 这条路径，调用方对它的"何时清零"心智模型不如 `DataBuffer` 直接，且 dwio 输入流没有先例。
- `raw_vector<char>`：可以，但其卖点是 SIMD padding，FsCacheInputStream 不需要；并且它在 dwio 输入流路径上没有先例。
- `std::vector<char>`：`resize(n)` 会 zero-fill，抵消现有的 `make_unique_for_overwrite` 优化；改进点反而退步。
- 朴素的"`unique_ptr<char[]>` + 容量字段 + 手写 grow"：相比 `DataBuffer` 没有任何新增能力，反而要在 fscache 里维护一份本质上等价的小工具，违反"复用现有"。

## 5. 推荐 B —— `getOrSet` 路径的 small-vector

**采用 `folly::small_vector<FileSegmentPtr, 4>` 作为 `lookupRangeUnlocked` 与 `fillHolesWithEmptyFileSegments` 的返回类型，同时把 `FileSegmentsHolder` 内部存储改成 `folly::small_vector<FileSegmentPtr, 4>`。**

- 头文件：`folly/container/small_vector.h`（与 `ArrayAggAggregate.cpp:17` 一致，属于既有依赖）。
- 容量阈值 4：TPC-H 主流是 N=1；选 4 兼顾偶发的少量多段拼接，仍保持 `small_vector` 节点的 cacheline 友好（`shared_ptr` 是 16B，4*16=64B 一个 cacheline）。
- ripple 范围：
  - `FileSegmentsHolder` ctor 参数和 `segments()` 返回类型从 `std::vector<FileSegmentPtr>` 改成 `folly::small_vector<FileSegmentPtr, 4>` —— 影响 `FileSegmentsHolder.h:34,67,72,90`。
  - `FsCacheInputStream` ctor 形参 `std::vector<cache::fs::FileSegmentPtr> segments`（`FsCacheInputStream.h:41` / 成员 `segments_` 在 `.h:59`）需要改成同一 small_vector 类型；调用点 `FsCacheBufferedInput.cpp:162` 自动跟随。
  - 测试中所有 range-for / `.empty()` / `.size()` / 索引访问无需改动（small_vector API 兼容）。
  - `FsCache.cpp:170-192` `lookupRangeUnlocked` / `:196-243` `fillHolesWithEmptyFileSegments` / `:284` `slots` 的局部变量类型同步改为 `small_vector<…, 4>`。
- 为什么选 `folly::small_vector` 而不是其他：
  - Velox 已有先例（虽然只一处）；引入不算"新依赖"；且 folly 已是 Velox 一级依赖。
  - `small_vector` 在 `size <= N` 时不分配，超出时回退到堆，无需调用方判断。
  - 兼容 `std::vector` 的接口（迭代器、`reserve`、`push_back`、`emplace_back`、operator[]、`front`/`back`、`empty`/`size`），改造面小。

第 3 处——`FsCacheBufferedInput.cpp:276` 的 `std::vector<char> buf(min(1MiB, segSize))`：**不建议改为 small_vector**。

- 它在后台下载线程里，一个 segment 一次分配（生命周期=一次下载），不在 `getOrSet` 热路径帧上。
- 它的大小最大 1MiB，不适合栈内联。
- 真要优化下载缓冲，应该走 buffer pool / `MemoryPool::allocate`（与 phase-2 主线无关），可以作为单独跟踪项。

考虑过但**未采用**：

- `folly::fbvector`：增长策略略不同但不解决"小 N 不分配"问题。
- `boost::container::small_vector`：boost 未必在 Velox 依赖图里；与已用的 folly 比无优势。
- 手写"inline 4 + heap"小容器：违反"复用现有"。
- 把 `std::vector` 改成 `boost::container::static_vector`：N 上限是固定的，但 fscache 允许 N>4（大读跨多个 segment），需要可溢出方案。
- 重用 `raw_vector<FileSegmentPtr>`：`raw_vector` `static_assert` 要求 `is_trivially_copyable`（`RawVector.h:36-37`），而 `shared_ptr<FileSegment>` 非 trivially copyable，**编译失败**，直接排除。

## 6. 总结建议

| 优化点 | 推荐类型 | 头文件 |
| --- | --- | --- |
| FsCacheInputStream 缓冲复用 | `dwio::common::DataBuffer<char>` | `velox/dwio/common/DataBuffer.h:29` |
| getOrSet 小向量内联 | `folly::small_vector<FileSegmentPtr, 4>` | `folly/container/small_vector.h` |

下一步（实现阶段，非本次范围）：

1. 推荐 A：改 `FsCacheInputStream::buffer_` 类型 + 透传 `MemoryPool&` + `loadCurrentSegmentBuffer` 改为 `buffer_.reserve(length); buffer_.resize(length)` 之前先确认 `resize` 不再 zero-fill 已分配范围——`DataBuffer.h:123-129` 显示 `resize` 仅对 `size > size_` 的尾部 memset，需要另用 `buffer_.reserve(length)` + 私有访问 `data()`，避免 zero-fill；或直接用 `unsafeAppend` 类的不清零 API。验证细节在实现 PR 里给基准回归。
2. 推荐 B：先把 holder 内部存储改成 small_vector，再回头把 `lookupRangeUnlocked` / `fillHolesWithEmptyFileSegments` / `FsCacheInputStream::segments_` 同步换型，最后跑 TPC-H A/B 验证 p99。
