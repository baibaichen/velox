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

#include "velox/common/base/SimdUtil.h"
#include "velox/exec/ch/FixedKey.h"
#include "velox/exec/ch/HashTable.h"
#include "velox/exec/ch/HashTableAllocatorAdapter.h"
#include "velox/exec/ch/RowRefList.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>

namespace facebook::velox::exec::ch {

using UInt32 = uint32_t;
using UInt64 = uint64_t;

template <typename T>
struct HashCRC32 {
  size_t operator()(T key) const {
    static_assert(
        std::is_integral_v<T> && std::is_unsigned_v<T> &&
        sizeof(T) <= sizeof(UInt64));
    return simd::crc32U64(0, static_cast<UInt64>(key));
  }
};

template <typename T>
struct HashWide {
  size_t operator()(const T& key) const {
    static_assert(
        std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256>);
    uint64_t hash = 0;
    for (const auto word : key.words) {
      hash = simd::crc32U64(hash, word);
    }
    return hash;
  }
};

template <typename First, typename Second>
struct PairNoInit {
  First first;
  Second second;

  PairNoInit() = default;

  explicit PairNoInit(const First& firstValue)
      : first(firstValue), second() {}

  PairNoInit(const First& firstValue, const Second& secondValue)
      : first(firstValue), second(secondValue) {}
};

template <
    typename Key,
    typename TMapped,
    typename Hash,
    typename TState = HashTableNoState,
    typename Pair = PairNoInit<Key, TMapped>>
struct HashMapCell {
  using Mapped = TMapped;
  using State = TState;
  using value_type = Pair;
  using mapped_type = Mapped;
  using key_type = Key;

  static constexpr bool need_zero_value_storage = true;

  value_type value;

  HashMapCell() = default;

  HashMapCell(const Key& key, const State&) : value(key) {}

  const Key& getKey() const {
    return value.first;
  }

  Mapped& getMapped() {
    return value.second;
  }

  const Mapped& getMapped() const {
    return value.second;
  }

  const value_type& getValue() const {
    return value;
  }

  static const Key& getKey(const value_type& value) {
    return value.first;
  }

  bool keyEquals(const Key& key) const {
    return bitEquals(value.first, key);
  }

  bool keyEquals(const Key& key, size_t) const {
    return bitEquals(value.first, key);
  }

  bool keyEquals(const Key& key, size_t, const State&) const {
    return bitEquals(value.first, key);
  }

  void setHash(size_t) {}

  size_t getHash(const Hash& hash) const {
    return hash(value.first);
  }

  bool isZero(const State& state) const {
    return isZero(value.first, state);
  }

  static bool isZero(const Key& key, const State&) {
    return ZeroTraits::check(key);
  }

  void setZero() {
    ZeroTraits::set(value.first);
  }

  void setMapped(const value_type& newValue) {
    value.second = newValue.second;
  }
};

template <
    typename Key,
    typename TMapped,
    typename Hash,
    typename TState = HashTableNoState>
struct HashMapCellWithSavedHash
    : public HashMapCell<Key, TMapped, Hash, TState> {
  using Base = HashMapCell<Key, TMapped, Hash, TState>;
  using State = typename Base::State;

  size_t saved_hash{0};

  using Base::Base;

  bool keyEquals(const Key& key) const {
    return bitEquals(this->value.first, key);
  }

  bool keyEquals(const Key& key, size_t hash) const {
    return saved_hash == hash && bitEquals(this->value.first, key);
  }

  bool keyEquals(const Key& key, size_t hash, const State&) const {
    return keyEquals(key, hash);
  }

  void setHash(size_t hash) {
    saved_hash = hash;
  }

  size_t getHash(const Hash&) const {
    return saved_hash;
  }
};

template <
    typename Key,
    typename Cell,
    typename Hash = std::hash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocatorAdapter>
class HashMapTable : public HashTable<Key, Cell, Hash, Grower, Allocator> {
 public:
  using Base = HashTable<Key, Cell, Hash, Grower, Allocator>;
  using LookupResult = typename Base::LookupResult;
  using mapped_type = typename Base::mapped_type;

  using Base::Base;
  using Base::emplace;
  using Base::find;

  mapped_type& emplace(const Key& key) {
    bool inserted;
    return emplace(key, inserted);
  }

  mapped_type& emplace(const Key& key, bool& inserted) {
    LookupResult result;
    Base::emplace(key, result, inserted);
    return result->getMapped();
  }

  mapped_type& operator[](const Key& key) {
    return emplace(key);
  }

  template <typename Func>
  void forEachValue(Func&& func) {
    for (auto& cell : *this) {
      func(cell.getKey(), cell.getMapped());
    }
  }

  template <typename Func>
  void forEachValue(Func&& func) const {
    for (const auto& cell : *this) {
      func(cell.getKey(), cell.getMapped());
    }
  }
};

template <
    typename Key,
    typename Hash = HashCRC32<Key>,
    typename Grower = HashTableGrower<>>
using HashMapAll = HashMapTable<
    Key,
    HashMapCellWithSavedHash<Key, RowRefList, Hash>,
    Hash,
    Grower,
    HashTableAllocatorAdapter>;

using HashMapAll_key32 = HashMapAll<UInt32>;
using HashMapAll_key64 = HashMapAll<UInt64>;
using HashMapAll_keys128 = HashMapAll<UInt128, HashWide<UInt128>>;
using HashMapAll_keys256 = HashMapAll<UInt256, HashWide<UInt256>>;

static_assert(std::is_trivially_copyable_v<RowRefList>);
static_assert(std::is_trivially_copyable_v<HashMapAll_key32::cell_type>);
static_assert(std::is_trivially_copyable_v<HashMapAll_key64::cell_type>);
static_assert(std::is_trivially_copyable_v<HashMapAll_keys128::cell_type>);
static_assert(std::is_trivially_copyable_v<HashMapAll_keys256::cell_type>);

} // namespace facebook::velox::exec::ch
