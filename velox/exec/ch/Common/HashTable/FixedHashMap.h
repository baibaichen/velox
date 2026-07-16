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

#include "velox/exec/ch/Common/HashTable/FixedHashTable.h"
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/exec/ch/Interpreters/RowRefList.h"

namespace facebook::velox::exec::ch {

/// Cell for FixedHashMap (full-flag variant). Ports ClickHouse FixedHashMapCell:
/// the key is not stored (it equals the cell index), only occupancy (`full`)
/// and the mapped value live in the cell. Used for mapped types such as
/// RowRefList that have no natural zero value. Algorithm 逐字搬 CH.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell {
  using Mapped = TMapped;
  using State = TState;

  using value_type = PairNoInit<Key, Mapped>;
  using mapped_type = TMapped;

  bool full;
  Mapped mapped;

  FixedHashMapCell() {}
  FixedHashMapCell(const Key&, const State&) : full(true) {}
  FixedHashMapCell(const value_type& value, const State&)
      : full(true), mapped(value.second) {}

  VoidKey getKey() const {
    return {};
  }
  Mapped& getMapped() {
    return mapped;
  }
  const Mapped& getMapped() const {
    return mapped;
  }

  bool isZero(const State&) const {
    return !full;
  }
  void setZero() {
    full = false;
  }

  /// Similar to FixedHashSetCell except that we need to contain a pointer to the
  /// Mapped field. Note that we have to assemble a continuous layout for the
  /// value_type on each call of getValue().
  struct CellExt {
    CellExt() {}
    CellExt(Key&& keyValue, const FixedHashMapCell* ptrValue)
        : key(keyValue), ptr(const_cast<FixedHashMapCell*>(ptrValue)) {}
    void update(Key&& keyValue, const FixedHashMapCell* ptrValue) {
      key = keyValue;
      ptr = const_cast<FixedHashMapCell*>(ptrValue);
    }
    Key key;
    FixedHashMapCell* ptr;

    const Key& getKey() const {
      return key;
    }
    Mapped& getMapped() {
      return ptr->mapped;
    }
    const Mapped& getMapped() const {
      return ptr->mapped;
    }
    const value_type getValue() const {
      return {key, ptr->mapped};
    }
  };
};

/// In case when we can encode empty cells with zero mapped values. Ports
/// ClickHouse FixedHashMapImplicitZeroCell.
template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapImplicitZeroCell {
  using Mapped = TMapped;
  using State = TState;

  using value_type = PairNoInit<Key, Mapped>;
  using mapped_type = TMapped;

  Mapped mapped;

  FixedHashMapImplicitZeroCell() {}
  FixedHashMapImplicitZeroCell(const Key&, const State&) {}
  FixedHashMapImplicitZeroCell(const value_type& value, const State&)
      : mapped(value.second) {}

  VoidKey getKey() const {
    return {};
  }
  Mapped& getMapped() {
    return mapped;
  }
  const Mapped& getMapped() const {
    return mapped;
  }

  bool isZero(const State&) const {
    return !mapped;
  }
  void setZero() {
    mapped = {};
  }

  struct CellExt {
    CellExt() {}
    CellExt(Key&& keyValue, const FixedHashMapImplicitZeroCell* ptrValue)
        : key(keyValue),
          ptr(const_cast<FixedHashMapImplicitZeroCell*>(ptrValue)) {}
    void update(Key&& keyValue, const FixedHashMapImplicitZeroCell* ptrValue) {
      key = keyValue;
      ptr = const_cast<FixedHashMapImplicitZeroCell*>(ptrValue);
    }
    Key key;
    FixedHashMapImplicitZeroCell* ptr;

    const Key& getKey() const {
      return key;
    }
    Mapped& getMapped() {
      return ptr->mapped;
    }
    const Mapped& getMapped() const {
      return ptr->mapped;
    }
    const value_type getValue() const {
      return {key, ptr->mapped};
    }
  };
};

template <
    typename Key,
    typename Mapped,
    typename Cell = FixedHashMapCell<Key, Mapped>,
    typename Size = FixedHashTableStoredSize<Cell>,
    typename Allocator = HashTableAllocatorAdapter,
    size_t sizeBits = sizeof(Key) * 8>
class FixedHashMap
    : public FixedHashTable<Key, Cell, Size, Allocator, sizeBits> {
 public:
  using Base = FixedHashTable<Key, Cell, Size, Allocator, sizeBits>;
  using Self = FixedHashMap;
  using LookupResult = typename Base::LookupResult;

  using Base::Base;
  using Base::emplace;

  /// mergeToViaIndexFilter is a special mergeTo function to allow `total_worker`
  /// worker to merge without race condition.
  template <typename Func>
  void mergeToViaIndexFilter(
      Self& that,
      Func&& func,
      uint32_t workerId,
      uint32_t totalWorker) {
    uint32_t minIndex = 0;
    uint32_t maxIndex = static_cast<uint32_t>(this->getBufferSizeInCells());
    if (this->canUseMinMaxOptimization()) {
      auto [minValue, maxValue] = this->getMinMaxIndex();
      minIndex = minValue;
      maxIndex = maxValue + 1;
    }
    uint32_t startIndex = (minIndex / totalWorker) * totalWorker + workerId;

    /// Increment by totalWorker to make distribution of merge evenly. We use
    /// index directly instead of iterator because we need to precisely control
    /// the cells for each worker. Iterator however would skip zero cells.
    for (uint32_t i = startIndex; i < maxIndex; i += totalWorker) {
      if (!this->buf[i].isZero(*this)) {
        typename Self::LookupResult resIt;
        bool inserted = false;
        that.emplace(static_cast<Key>(i), resIt, inserted, i);
        func(resIt->getMapped(), this->buf[i].getMapped(), inserted);
      }
    }
  }

  template <typename Func, bool>
  void mergeToViaEmplace(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      typename Self::LookupResult resIt;
      bool inserted = false;
      that.emplace(it->getKey(), resIt, inserted, it.getHash());
      func(resIt->getMapped(), it->getMapped(), inserted);
    }
  }

  template <typename Func>
  void mergeToViaFind(Self& that, Func&& func) {
    for (auto it = this->begin(), end = this->end(); it != end; ++it) {
      auto resIt = that.find(it->getKey(), it.getHash());
      if (!resIt) {
        func(it->getMapped(), it->getMapped(), false);
      } else {
        func(resIt->getMapped(), it->getMapped(), true);
      }
    }
  }

  template <typename Func>
  void forEachValue(Func&& func) {
    for (auto& v : *this) {
      func(v.getKey(), v.getMapped());
    }
  }

  template <typename Func>
  void forEachMapped(Func&& func) {
    for (auto& v : *this) {
      if constexpr (std::is_same_v<decltype(func(v.getMapped())), bool>) {
        if (!func(v.getMapped())) {
          break;
        }
      } else {
        func(v.getMapped());
      }
    }
  }

  Mapped& operator[](const Key& x) {
    LookupResult it;
    bool inserted = false;
    this->emplace(x, it, inserted);
    if (inserted) {
      new (&it->getMapped()) Mapped();
    }

    return it->getMapped();
  }

  /// Convenience emplace mirroring HashMapTable::emplace(Key): returns the
  /// mapped value, inserting a default one on first use. Used by FixedKeyMap so
  /// the direct-address maps expose the same emplace(key) -> Mapped& contract as
  /// the open-addressing maps. Not part of CH's FixedHashMap; infra glue only.
  Mapped& emplace(const Key& key) {
    return (*this)[key];
  }

  /// No-op: direct addressing never grows, so reservation is meaningless. Kept
  /// to satisfy the FixedKeyMap map interface.
  void reserve(size_t /* numElements */) {}

  /// Prefetches the cell addressed by the given identity hash. Present so the
  /// FixedKeyMap prefetch/hash template branches compile uniformly with the
  /// open-addressing maps.
  void prefetchByHash(size_t hashValue) const {
    __builtin_prefetch(&this->buf[hashValue]);
  }
};

/// Direct-address maps for 1- and 2-byte integer join keys. Mapped is
/// RowRefList, which has no natural zero value, so the full-flag FixedHashMapCell
/// variant is used (not the implicit-zero variant). Names align with CH's
/// FixedHashMap template.
using FixedHashMap_key8 = FixedHashMap<uint8_t, RowRefList>;
using FixedHashMap_key16 = FixedHashMap<uint16_t, RowRefList>;

static_assert(std::is_trivially_copyable_v<FixedHashMap_key8::cell_type>);
static_assert(std::is_trivially_copyable_v<FixedHashMap_key16::cell_type>);

} // namespace facebook::velox::exec::ch
