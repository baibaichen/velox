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

#include "velox/exec/ch/Common/ColumnsHashing/SerializedKey.h" // ch::StringRef / StringRefHash
#include "velox/exec/ch/Common/HashTable/HashMap.h" // ch::HashMapAll_key_string
#include "velox/exec/ch/Common/HashTable/HashTableKeyHolder.h"

#include <folly/Portability.h>

#include <string_view>

namespace facebook::velox::exec::ch {

// ============================================================================
// StringHashMapAdapter — ch 变长字符串 key 的 `Data`(HashMethodBase 看到的
// map 抽象)。它包住 port 的 `ch::HashMapAll_key_string`(StringRef key +
// HashMapCellWithSavedHash + StringRefHash + CRC32),只在这一层把 CH 的
// **ArenaKeyHolder persist 协议**逐字搬进来。
//
// 为什么要这个适配层:
//   CH `HashTable::emplaceNonZeroImpl`(HashTable.h:1035-1049)在插入成功那一步
//   调 `keyHolderPersistKey(key_holder)`(把 key 字节拷进 Arena、view 指向
//   Arena),命中既有 cell 则调 `keyHolderDiscardKey`。这是 CH 的算法。
//   port 的 `ch::HashTable::emplace` 走 `const Key& key = keyHolder;` —— 它
//   **不**跑 keyHolder persist 协议(port 采用"先插非持久 key,再由 caller
//   setKey 换成 arena 副本"的模型)。铁律要求算法搬 CH,而 ch/ 不许动,所以
//   把 CH 的 persist 协议原样搬到这个 ch 适配层里驱动 port map:
//     插入成功 → keyHolderPersistKey(拷进 ch::Arena)→ cell->setKey(持久 view)
//     命中既有 → keyHolderDiscardKey
//   —— 与 CH emplaceNonZeroImpl 逐字对应,只把底层 map 换成 port 的 infra。
// ============================================================================
class StringHashMapAdapter {
 public:
  using Impl = ch::HashMapAll_key_string;
  using LookupResult = typename Impl::LookupResult;
  using mapped_type = typename Impl::mapped_type;
  using value_type = typename Impl::cell_type::value_type;

  explicit StringHashMapAdapter(memory::MemoryPool* pool) : impl_(pool) {}

  // CH: data.hash(keyHolderGetKey(key_holder))(HashMethodBase::getHash)。
  size_t hash(std::string_view key) const {
    return ch::StringRefHash{}(toStringRef(key));
  }

  // 搬 CH HashTable::emplace(compute_hash 分支): hash 由 map 自算。
  FOLLY_ALWAYS_INLINE void
  emplace(ArenaKeyHolder& keyHolder, LookupResult& it, bool& inserted) {
    emplace(keyHolder, it, inserted, hash(keyHolderGetKey(keyHolder)));
  }

  // 搬 CH HashTable::emplace + emplaceNonZeroImpl(HashTable.h:1035-1049)的
  // persist 协议。底层 port map.emplace 先用非持久 key 建 cell;我们在这里
  // 补上 CH 的 keyHolderPersistKey / keyHolderDiscardKey 一步(算法搬 CH,
  // map 换成 port infra)。
  FOLLY_ALWAYS_INLINE void emplace(
      ArenaKeyHolder& keyHolder,
      LookupResult& it,
      bool& inserted,
      size_t hashValue) {
    // keyHolderGetKey(key_holder) —— 取非持久 view(此刻还指向原列 buffer)。
    const std::string_view& key = keyHolderGetKey(keyHolder);
    impl_.emplace(toStringRef(key), it, inserted, hashValue);

    if (inserted) {
      // CH emplaceNonZeroImpl:插入成功 → keyHolderPersistKey(拷进 Arena、
      // view 指向 Arena)→ 用持久 key 建 cell。port map 已用非持久 key 建了
      // cell,所以这里 persist 后再 setKey 把 cell 的 key 换成 Arena 副本。
      keyHolderPersistKey(keyHolder);
      const std::string_view& persisted = keyHolderGetKey(keyHolder);
      it->setKey(toStringRef(persisted));
    } else {
      // CH emplaceNonZeroImpl:命中既有 cell → keyHolderDiscardKey(no-op)。
      keyHolderDiscardKey(keyHolder);
    }
  }

  // 搬 CH HashMethodBase::findKeyImpl → data.find(key)。
  FOLLY_ALWAYS_INLINE LookupResult find(std::string_view key) {
    return impl_.find(toStringRef(key));
  }

  size_t size() const {
    return impl_.size();
  }

  Impl& impl() {
    return impl_;
  }

 private:
  static FOLLY_ALWAYS_INLINE ch::StringRef toStringRef(std::string_view key) {
    return ch::StringRef{key.data(), static_cast<uint32_t>(key.size())};
  }

  Impl impl_;
};

} // namespace facebook::velox::exec::ch
