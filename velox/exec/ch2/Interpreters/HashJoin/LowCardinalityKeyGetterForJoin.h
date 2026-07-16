/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch2/Common/ColumnsHashing/ColumnsHashingImpl.h"
#include "velox/vector/BaseVector.h"

#include <folly/Portability.h>

#include <cstddef>
#include <vector>

namespace facebook::velox::exec::ch2 {

// ============================================================================
// LowCardinalityKeyGetterForJoin — exactly 搬自 CH
// Interpreters/HashJoin/KeyGetter.h:51-197 的 **join 版**
// LowCardinalityKeyGetterForJoin(不是 aggregation 版
// HashMethodSingleLowCardinalityColumn)。
//
// 本 task(O4):
//   (a) 块内去重 visit_cache 算法搬 CH —— 低基数字典列 probe 同一 dict index
//       只查一次表(visit_cache 三态 + mapped_cache 缓存命中 cell 裸指针)。
//   (b) 跨-block 字典级 saved_hash 只留接口 stub —— Velox DictionaryVector 缺
//       字典身份 / 持久 hash infra,saved_hash 恒 nullptr、走"现算 hash"回退
//       (base method 的 data.find(key) 里现算),**不用 Velox hashAll 顶替**。
//       钩子留好,task10 补 Velox 字典 hash 缓存 infra。
//
// 铁律(MIGRATION_PRINCIPLE.md):visit_cache 三态去重 + mapped_cache 复用逻辑
//   逐字搬 CH;DictionaryVector 只当数据承载(indices → row→dict index,
//   valueVector → 字典 base 列)。saved_hash 缺 infra → stub、不顶替。
//
// infra 边界:
//   CH `ColumnLowCardinality::getIndexes()`/`getSizeOfIndexType()`
//     → Velox `DictionaryVector::wrapInfo()`(= indices 缓冲,vector_size_t)。
//       Velox dict indices 统一是 vector_size_t(int32),所以没有 CH 的
//       size_of_index_type 多分派;getIndexAt 直接读 vector_size_t 数组。
//   CH `getDictionary().getNestedNotNullableColumn()`(字典 base 列)
//     → Velox `DictionaryVector::valueVector()`(base 向量)。base method
//       (BaseMethod)在此 base 向量上跑(如 HashMethodString on nested)。
//   CH `dictionary.getNestedNotNullableColumn()->size()`(dictionary_size)
//     → Velox base 向量 size(visit_cache/mapped_cache 数组大小)。
//   CH `saved_hash = dictionary.tryGetSavedHash()` → **stub 为 nullptr**。
//
// plain 列回退(CH KeyGetter.h:90-92):probe/left key 可以是 plain(非
//   DictionaryVector)—— joins 允许 plain T vs LowCardinality(T)。此时无字典,
//   positions 保持 null,base method 直接在列本身上跑(无 dict 间接、无去重)。
//   map 存 key 值,plain probe 与 dict build 产生兼容 key。
// ============================================================================
template <typename BaseMethod, typename Mapped>
struct LowCardinalityKeyGetterForJoin {
  // CH: using MappedNonConst = std::remove_const_t<Mapped>;
  using MappedNonConst = std::remove_const_t<Mapped>;
  static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
  // CH: using EmplaceResult / FindResult = BaseMethod::EmplaceResult/FindResult;
  using EmplaceResult = typename BaseMethod::EmplaceResult;
  using FindResult = typename BaseMethod::FindResult;

  // CH 原文 (KeyGetter.h:59):has_cheap_key_calculation = false —— 解析 key 要
  // 查字典 index,不当"便宜",从而关掉 probe-loop 软件预取(会跟 per-dict 缓存
  // 打架)。ch2 照抄保留。
  static constexpr bool has_cheap_key_calculation = false;

  // CH 原文 (KeyGetter.h:62-66)。
  BaseMethod base;
  // infra 边界:CH `const IColumn * positions`(字典 positions 列)→ Velox dict
  // indices 裸指针(vector_size_t,统一 int32,无 size_of_index_type 多分派)。
  const vector_size_t* positions = nullptr;
  // O4(b) stub:CH `const UInt64 * saved_hash`。Velox DictionaryVector 缺字典级
  // 持久 hash / 字典身份 infra → 恒 nullptr(走现算 hash 回退,见 findKey)。
  // 钩子留好,task10 补 Velox 字典 hash 缓存 infra。**不用 hashAll 顶替。**
  const UInt64* saved_hash = nullptr;
  // infra 边界:CH `ColumnPtr dictionary_holder`(持有字典生命周期)→ Velox
  // 持有 DictionaryVector(indices/valueVector 的生命周期锚)。
  VectorPtr dictionary_holder;

  // CH 原文 (KeyGetter.h:69-77)。per-dict-index probe 缓存:缓存进 hash 表 cell
  // 的**指针**(对 immutable probe 阶段稳定),不是 mapped 值的拷贝(拷贝会悬垂)。
  // infra:PaddedPODArray → std::vector。
  std::vector<UInt8> visit_cache;      // 0 = 未访问, 1 = 找到, 2 = 没找到
  std::vector<Mapped*> mapped_cache;
  std::vector<size_t> offset_cache;

  // ------------------------------------------------------------------------
  // 块内去重生效验证钩子(非 CH 成员):记录 findKey 里真正调 data.find 的次数。
  // visit_cache 命中(同一 dict index 第二次起)不查表 → find_calls 只 ≈ 命中
  // 的 unique dict index 数,远小于行数。测试据此证明去重真生效(不是靠结果对)。
  // ------------------------------------------------------------------------
  size_t find_calls = 0;

  // CH 原文 getBaseColumn (KeyGetter.h:80-85):LowCardinality → 字典 nested 列;
  // plain → 列本身。infra 边界:CH typeid_cast<ColumnLowCardinality> +
  // getNestedNotNullableColumn → Velox 判 DictionaryVector 取 valueVector。
  static VectorPtr getBaseColumn(const VectorPtr& column) {
    if (column->encoding() == VectorEncoding::Simple::DICTIONARY) {
      return column->valueVector();
    }
    return column;
  }

  // CH 原文构造 (KeyGetter.h:87-108)。
  LowCardinalityKeyGetterForJoin(
      const std::vector<VectorPtr>& key_columns,
      const Sizes& key_sizes,
      const HashMethodContextPtr& context)
      // CH: base({getBaseColumn(key_columns[0])}, key_sizes, nullptr)
      : base({getBaseColumn(key_columns[0])}, key_sizes, context) {
    // CH 原文注释 (KeyGetter.h:89-93):build/right key 总是 LowCardinality(所以
    // 才选中此 map),但 probe/left key 可以是 plain 列(joins 允许 plain T vs
    // LowCardinality(T) 无需 cast)。plain 无字典 → positions 保持 null、base
    // method 直接用(无 per-dict 去重)。map 存 key 值,plain probe 与
    // dict-encoded build 产生兼容 key。
    const auto& column = key_columns[0];
    if (column->encoding() != VectorEncoding::Simple::DICTIONARY) {
      return;
    }

    // CH: dictionary_holder = low_cardinality_column->getDictionaryPtr();
    //     saved_hash = dictionary.tryGetSavedHash();
    //     size_of_index_type = low_cardinality_column->getSizeOfIndexType();
    //     positions = low_cardinality_column->getIndexesPtr().get();
    dictionary_holder = column;
    // O4(b) stub:saved_hash 恒 nullptr(见成员注释)。不接 Velox hashAll。
    saved_hash = nullptr;
    // infra 边界:CH getIndexesPtr()(positions 列,size_of_index_type 多分派)→
    // Velox wrapInfo()(dict indices 缓冲,统一 vector_size_t,无多分派)。
    positions = column->wrapInfo()->template as<vector_size_t>();

    // CH: dictionary_size = dictionary.getNestedNotNullableColumn()->size();
    //     visit_cache/mapped_cache/offset_cache.assign(dictionary_size, ...);
    // infra 边界:CH 字典 nested 列 size → Velox valueVector() size。
    const size_t dictionary_size = column->valueVector()->size();
    visit_cache.assign(dictionary_size, static_cast<UInt8>(0));
    mapped_cache.assign(dictionary_size, static_cast<Mapped*>(nullptr));
    offset_cache.assign(dictionary_size, static_cast<size_t>(0));
  }

  // CH 原文 (KeyGetter.h:111):positions != nullptr 表示当前列是 LowCardinality
  // (字典路径);plain 列为 false。
  FOLLY_ALWAYS_INLINE bool isLowCardinality() const {
    return positions != nullptr;
  }

  // CH 原文 getIndexAt (KeyGetter.h:112-122):从字典 positions 拿 row→dict index。
  // infra 边界:CH 按 size_of_index_type 多分派(UInt8/16/32/64)→ Velox dict
  // indices 统一 vector_size_t,直接读一种类型(无多分派)。语义等价:拿这行的
  // 字典下标。
  FOLLY_ALWAYS_INLINE size_t getIndexAt(size_t row) const {
    return static_cast<size_t>(positions[row]);
  }

  // CH 原文 getKeyHolder (KeyGetter.h:124-127):LowCardinality 用 dict index、
  // plain 用 row 直接喂 base method。
  FOLLY_ALWAYS_INLINE auto getKeyHolder(size_t row, ch::Arena& pool) const {
    return base.getKeyHolder(isLowCardinality() ? getIndexAt(row) : row, pool);
  }

  // CH 原文 getHash (KeyGetter.h:129-140):ConcurrentHashJoin 分片用。plain →
  // base.getHash;LowCardinality → saved_hash[index](若有)否则 base 现算。
  // O4(b) stub:saved_hash 恒 nullptr → 恒走 base.getHash(现算 hash 回退)。
  template <typename Data>
  FOLLY_ALWAYS_INLINE size_t getHash(const Data& data, size_t row, ch::Arena& pool) {
    if (!isLowCardinality()) {
      return base.getHash(data, row, pool);
    }
    const size_t index = getIndexAt(row);
    if (saved_hash) {
      return saved_hash[index];
    }
    return base.getHash(data, index, pool);
  }

  // CH 原文 emplaceKey (KeyGetter.h:142-166)。build 侧:每行都要真插入进 cell,
  // 无 per-dict 去重(mapped RowRefList 住在 cell 里)。字典加速只在 probe 侧。
  template <typename Data>
  FOLLY_ALWAYS_INLINE EmplaceResult
  emplaceKey(Data& data, size_t row_, ch::Arena& pool) {
    // CH:plain key(无字典)→ base method 直接处理。build 侧总是 LowCardinality,
    // 所以这个分支只在 plain key 到达 build 时走到。
    if (!isLowCardinality()) {
      return base.emplaceKey(data, row_, pool);
    }

    const size_t row = getIndexAt(row_);

    auto key_holder = base.getKeyHolder(row, pool);

    typename Data::LookupResult it;
    bool inserted = false;
    // O4(b) stub:saved_hash 恒 nullptr → 走 data.emplace(现算 hash)分支。
    if (saved_hash) {
      data.emplace(key_holder, it, inserted, saved_hash[row]);
    } else {
      data.emplace(key_holder, it, inserted);
    }

    auto& mapped = it->getMapped();
    if (inserted) {
      new (&mapped) MappedNonConst();
    }
    return EmplaceResult(mapped, mapped, inserted);
  }

  // CH 原文 findKey (KeyGetter.h:168-197)。**O4(a) 块内去重核心。**
  template <typename Data>
  FOLLY_ALWAYS_INLINE FindResult
  findKey(Data& data, size_t row_, ch::Arena& pool) {
    // CH:plain probe key(无字典)→ base method 直接查。map 存 key 值,所以这
    // 会找到从 dict-encoded build 侧插入的行。plain 路径无去重。
    if (!isLowCardinality()) {
      return base.findKey(data, row_, pool);
    }

    const size_t row = getIndexAt(row_);

    // CH 原文 (KeyGetter.h:180-181):visit_cache 三态命中 → 直接返回缓存的
    // mapped_cache 指针 + found 位,**同一 dict index 零表查**(块内去重核心)。
    if (visit_cache[row] != 0) {
      return FindResult(mapped_cache[row], visit_cache[row] == 1, offset_cache[row]);
    }

    // CH 原文 (KeyGetter.h:183-184):cache miss → 算一次 key、查一次表。
    auto key_holder = base.getKeyHolder(row, pool);
    const auto key = keyHolderGetKey(key_holder);

    // CH 原文 (KeyGetter.h:186):saved_hash ? find(key, saved_hash[row]) : find(key)。
    // O4(b) stub:saved_hash 恒 nullptr → 走 data.find(key)(现算 hash 回退,
    // 非 hashAll 顶替)。task10 补字典 hash 缓存后接上 find(key, saved_hash[row])。
    ++find_calls; // 去重验证钩子:只在真查表时 +1。
    auto it = data.find(key);

    // CH 原文 (KeyGetter.h:188-196):写三个 cache、返回 FindResult。
    const bool found = it;
    Mapped* mapped = found ? &it->getMapped() : nullptr;
    // O4(b) stub:offset 相关(need_offset)本 task 不启用,置 0。CH 走
    // data.offsetInternal(it)(JoinUsedFlags 按 offset 索引),ch2 最小版 map
    // 不带 offsetInternal,need_offset=false 时 FindResult 忽略此值。
    const size_t offset = 0;

    visit_cache[row] = found ? 1 : 2;
    mapped_cache[row] = mapped;
    offset_cache[row] = offset;
    return FindResult(mapped, found, offset);
  }
};

} // namespace facebook::velox::exec::ch2
