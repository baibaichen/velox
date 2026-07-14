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

#include "velox/exec/ch/HashTableAllocatorAdapter.h"
#include "velox/exec/ch/RowRefList.h"

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace facebook::velox::exec::ch {

/// Direct-address cell for FixedDirectMap. Ports ClickHouse's FixedHashMapCell
/// (the full-flag variant): the key is not stored, it equals the cell's index
/// in the backing array; only occupancy and the mapped value live in the cell.
/// The full-flag variant is used because RowRefList has no natural zero value.
template <typename Key, typename Mapped>
struct FixedDirectMapCell {
  using mapped_type = Mapped;

  // True once a value has been emplaced at this index.
  bool full;
  // The mapped value; only meaningful when full is true.
  Mapped mapped;

  Mapped& getMapped() {
    return mapped;
  }

  const Mapped& getMapped() const {
    return mapped;
  }
};

/// Direct-address hash map for 1- and 2-byte integer keys, faithfully porting
/// ClickHouse's FixedHashMap. The key value is the index into a flat array of
/// 1 << (8 * sizeof(Key)) cells (256 for uint8_t, 65'536 for uint16_t), so
/// there is no hashing, no probing, no rehashing, and no zero-key special case.
/// The backing buffer is allocated once from the same memory pool as the other
/// join maps so peak-bytes accounting is uniform, and it relies on
/// allocateZeroFilled leaving every cell with full == false and mapped in its
/// valid default state (RowRefList's zero representation is a valid empty list).
/// Drop-in replaceable for the
/// HashMapTable variants: it exposes the same LookupResult, cell_type,
/// mapped_type, find/emplace/reserve/size/empty and buffer-size accessors that
/// FixedKeyMap dispatches on.
template <typename Key, typename Mapped>
class FixedDirectMap {
 public:
  static_assert(
      std::is_same_v<Key, uint8_t> || std::is_same_v<Key, uint16_t>,
      "FixedDirectMap only supports uint8_t and uint16_t keys");
  static_assert(
      std::is_trivially_copyable_v<Mapped>,
      "FixedDirectMap requires a trivially copyable mapped type");

  using key_type = Key;
  using mapped_type = Mapped;
  using cell_type = FixedDirectMapCell<Key, Mapped>;
  using LookupResult = cell_type*;
  using ConstLookupResult = const cell_type*;

  // Number of directly-addressable slots: one per possible key value.
  static constexpr size_t kNumCells = size_t{1} << (8 * sizeof(Key));

  explicit FixedDirectMap(memory::MemoryPool* pool) : allocator_(pool) {
    buffer_ = static_cast<cell_type*>(
        allocator_.alloc(kNumCells * sizeof(cell_type)));
  }

  FixedDirectMap(const FixedDirectMap&) = delete;
  FixedDirectMap& operator=(const FixedDirectMap&) = delete;
  FixedDirectMap(FixedDirectMap&&) = delete;
  FixedDirectMap& operator=(FixedDirectMap&&) = delete;

  ~FixedDirectMap() {
    allocator_.free(buffer_, kNumCells * sizeof(cell_type));
  }

  /// Hashing is the identity: the key is its own slot index. Present so the
  /// FixedKeyMap prefetch/hash template branches compile uniformly with the
  /// open-addressing maps.
  size_t hash(const Key& key) const {
    return key;
  }

  /// Prefetches the cell addressed by the given identity hash.
  void prefetchByHash(size_t hashValue) const {
    __builtin_prefetch(&buffer_[hashValue]);
  }

  bool empty() const {
    return size_ == 0;
  }

  size_t size() const {
    return size_;
  }

  /// No-op: direct addressing never grows, so reservation is meaningless. Kept
  /// to satisfy the FixedKeyMap map interface.
  void reserve(size_t /* numElements */) {}

  size_t getBufferSizeInCells() const {
    return kNumCells;
  }

  size_t getBufferSizeInBytes() const {
    return kNumCells * sizeof(cell_type);
  }

  /// Returns a reference to the mapped value for the given key, inserting an
  /// empty one on first use. Mirrors HashMapTable::emplace(Key).
  Mapped& emplace(const Key& key) {
    auto& cell = buffer_[key];
    if (!cell.full) {
      // No placement-new is needed: the buffer is zero-filled at allocation, so
      // cell.mapped is already a valid default RowRefList. This relies on the
      // mapped type's zero representation matching its default-constructed state.
      cell.full = true;
      ++size_;
    }
    return cell.mapped;
  }

  LookupResult find(const Key& key) {
    return buffer_[key].full ? &buffer_[key] : nullptr;
  }

  ConstLookupResult find(const Key& key) const {
    return buffer_[key].full ? &buffer_[key] : nullptr;
  }

  /// Forward iterator over occupied cells. Skips empty slots and reconstructs
  /// the key from the slot index, matching ClickHouse's FixedHashTable
  /// iterator. Only occupied cells are yielded. Not used on any production
  /// path (build and probe go through find; FixedKeyMap::begin/end delegate to
  /// the 64-bit map); kept for port fidelity and future full-table scans.
  class Iterator {
   public:
    Iterator(cell_type* buffer, size_t index)
        : buffer_(buffer), index_(index) {}

    bool operator==(const Iterator& other) const {
      return index_ == other.index_;
    }

    bool operator!=(const Iterator& other) const {
      return index_ != other.index_;
    }

    Iterator& operator++() {
      ++index_;
      while (index_ < kNumCells && !buffer_[index_].full) {
        ++index_;
      }
      return *this;
    }

    Key getKey() const {
      return static_cast<Key>(index_);
    }

    cell_type& operator*() const {
      return buffer_[index_];
    }

    cell_type* operator->() const {
      return &buffer_[index_];
    }

   private:
    cell_type* buffer_;
    size_t index_;
  };

  Iterator begin() const {
    size_t index = 0;
    while (index < kNumCells && !buffer_[index].full) {
      ++index;
    }
    return Iterator(buffer_, index);
  }

  Iterator end() const {
    return Iterator(buffer_, kNumCells);
  }

 private:
  HashTableAllocatorAdapter allocator_;
  // Flat direct-address array; buffer_[key] is the cell for that key. Allocated
  // zero-filled so every cell starts with full == false.
  cell_type* buffer_;
  // Number of occupied cells.
  size_t size_{0};
};

using FixedDirectMap_key8 = FixedDirectMap<uint8_t, RowRefList>;
using FixedDirectMap_key16 = FixedDirectMap<uint16_t, RowRefList>;

static_assert(std::is_trivially_copyable_v<FixedDirectMap_key8::cell_type>);
static_assert(std::is_trivially_copyable_v<FixedDirectMap_key16::cell_type>);

} // namespace facebook::velox::exec::ch
