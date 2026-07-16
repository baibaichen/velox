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

#include "velox/exec/ch/Common/HashTable/HashTable.h"
#include "velox/exec/ch/Common/HashTable/HashTableAllocatorAdapter.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <utility>

namespace facebook::velox::exec::ch {

/// Cell for a FixedHashSet (Set variant). Ports ClickHouse FixedHashTableCell:
/// the key is not stored, it equals the cell index; only occupancy (`full`)
/// lives in the cell. infra unchanged, algorithm 逐字搬 CH.
template <typename Key, typename TState = HashTableNoState>
struct FixedHashTableCell {
  using State = TState;

  using value_type = Key;
  using mapped_type = VoidMapped;
  bool full;

  FixedHashTableCell() {}
  FixedHashTableCell(const Key&, const State&) : full(true) {}

  VoidKey getKey() const {
    return {};
  }
  VoidMapped getMapped() const {
    return {};
  }

  bool isZero(const State&) const {
    return !full;
  }
  void setZero() {
    full = false;
  }
  static constexpr bool need_zero_value_storage = false;

  /// This Cell is only stored inside an iterator. It is used to accommodate the
  /// fact that the iterator based API always provides a reference to a
  /// continuous memory containing the Key. As a result, we have to instantiate a
  /// real Key field.
  struct CellExt {
    Key key;

    VoidKey getKey() const {
      return {};
    }
    VoidMapped getMapped() const {
      return {};
    }
    const value_type& getValue() const {
      return key;
    }
    void update(Key&& keyValue, FixedHashTableCell*) {
      key = keyValue;
    }
  };
};

/// How to obtain the size of the table.

template <typename Cell>
struct FixedHashTableStoredSize {
  std::atomic<size_t> mSize = 0;

  size_t getSize(const Cell*, const typename Cell::State&, size_t) const {
    return mSize.load();
  }
  bool isEmpty(const Cell*, const typename Cell::State&, size_t) const {
    return mSize.load() == 0;
  }

  void increaseSize() {
    mSize.fetch_add(1);
  }
  void clearSize() {
    mSize.store(0);
  }
  void setSize(size_t to) {
    mSize.store(to);
  }
};

template <typename Cell>
struct FixedHashTableCalculatedSize {
  size_t getSize(
      const Cell* buf,
      const typename Cell::State& state,
      size_t numCells) const {
    if (!buf) {
      return 0;
    }

    size_t res = 0;
    for (const Cell* end = buf + numCells; buf != end; ++buf) {
      if (!buf->isZero(state)) {
        ++res;
      }
    }
    return res;
  }

  bool isEmpty(
      const Cell* buf,
      const typename Cell::State& state,
      size_t numCells) const {
    if (!buf) {
      return true;
    }

    for (const Cell* end = buf + numCells; buf != end; ++buf) {
      if (!buf->isZero(state)) {
        return false;
      }
    }
    return true;
  }

  void increaseSize() {}
  void clearSize() {}
  void setSize(size_t) {}
};

/** Used as a lookup table for small keys such as UInt8, UInt16. It is different
  * than a HashTable in that keys are not stored in the Cell buf, but inferred
  * inside each iterator. There are a bunch of things to make it faster than
  * using HashTable: a) It does not have a conflict chain; b) There is no key
  * comparison; c) The number of cycles for checking cell empty is halved; d)
  * Memory layout is tighter.
  *
  * NOTE: For Set variants this should always be better. For Map variants
  * however, as we need to assemble the real cell inside each iterator, there
  * might be some cases we fall short.
  */
template <
    typename Key,
    typename Cell,
    typename Size,
    typename Allocator,
    size_t sizeBits = sizeof(Key) * 8>
class FixedHashTable : protected Allocator,
                       protected Cell::State,
                       protected Size {
  static constexpr size_t kNumCells = 1ULL << sizeBits;

  /// We maintain min and max values inserted into the hash table to then limit
  /// the amount of cells to traverse to the [min; max] range. Both values could
  /// be efficiently calculated only within `emplace` calls (and not when we
  /// populate the hash table in `read` method for example), so we update them
  /// only within `emplace` and track if any other method was called.
  bool onlyEmplaceWasUsedToInsertData = true;
  bool disableMinMaxOptimizationFlag = false;
  size_t min = kNumCells - 1;
  size_t max = 0;

 protected:
  friend class const_iterator;
  friend class iterator;

  using Self = FixedHashTable;

  Cell* buf; /// A piece of memory for all elements.

  void alloc() {
    buf = reinterpret_cast<Cell*>(Allocator::alloc(kNumCells * sizeof(Cell)));
  }

  std::pair<uint32_t, uint32_t> getMinMaxIndex() const {
    return {static_cast<uint32_t>(min), static_cast<uint32_t>(max)};
  }

  void free() {
    if (buf) {
      Allocator::free(buf, getBufferSizeInBytes());
      buf = nullptr;
    }
  }

  void destroyElements() {
    if (!std::is_trivially_destructible_v<Cell>) {
      for (iterator it = begin(), itEnd = end(); it != itEnd; ++it) {
        it.ptr->~Cell();
      }
    }
  }

  template <typename Derived, bool isConst>
  class iterator_base {
    using Container = std::conditional_t<isConst, const Self, Self>;
    using cell_type = std::conditional_t<isConst, const Cell, Cell>;

    Container* container;
    cell_type* ptr;

    friend class FixedHashTable;

   public:
    iterator_base() {}
    iterator_base(Container* containerValue, cell_type* ptrValue)
        : container(containerValue), ptr(ptrValue) {
      cell.update(static_cast<Key>(ptr - container->buf), ptr);
    }

    bool operator==(const iterator_base& rhs) const {
      return ptr == rhs.ptr;
    }
    bool operator!=(const iterator_base& rhs) const {
      return ptr != rhs.ptr;
    }

    Derived& operator++() {
      ++ptr;

      /// Skip empty cells in the main buffer.
      const auto* bufEnd = container->buf + container->kNumCells;
      if (container->canUseMinMaxOptimization()) {
        bufEnd = container->buf + container->max + 1;
      }
      while (ptr < bufEnd && ptr->isZero(*container)) {
        ++ptr;
      }

      return static_cast<Derived&>(*this);
    }

    auto& operator*() {
      if (cell.key != static_cast<Key>(ptr - container->buf)) {
        cell.update(static_cast<Key>(ptr - container->buf), ptr);
      }
      return cell;
    }
    auto* operator->() {
      if (cell.key != static_cast<Key>(ptr - container->buf)) {
        cell.update(static_cast<Key>(ptr - container->buf), ptr);
      }
      return &cell;
    }

    auto getPtr() const {
      return ptr;
    }
    size_t getHash() const {
      return ptr - container->buf;
    }
    size_t getCollisionChainLength() const {
      return 0;
    }
    typename cell_type::CellExt cell;
  };

 public:
  using key_type = Key;
  using mapped_type = typename Cell::mapped_type;
  using value_type = typename Cell::value_type;
  using cell_type = Cell;

  using LookupResult = Cell*;
  using ConstLookupResult = const Cell*;

  size_t hash(const Key& x) const {
    return x;
  }

  explicit FixedHashTable(memory::MemoryPool* pool)
      : Allocator(pool), buf(nullptr) {
    alloc();
  }

  FixedHashTable(const FixedHashTable&) = delete;
  FixedHashTable& operator=(const FixedHashTable&) = delete;
  FixedHashTable(FixedHashTable&&) = delete;
  FixedHashTable& operator=(FixedHashTable&&) = delete;

  ~FixedHashTable() {
    destroyElements();
    free();
  }

  class iterator : public iterator_base<iterator, false> {
   public:
    using iterator_base<iterator, false>::iterator_base;
  };

  class const_iterator : public iterator_base<const_iterator, true> {
   public:
    using iterator_base<const_iterator, true>::iterator_base;
  };

  const_iterator begin() const {
    if (!buf) {
      return end();
    }

    return const_iterator(this, firstPopulatedCell());
  }

  const_iterator cbegin() const {
    return begin();
  }

  iterator begin() {
    if (!buf) {
      return end();
    }

    return iterator(this, const_cast<Cell*>(firstPopulatedCell()));
  }

  const_iterator end() const {
    /// Avoid UBSan warning about adding zero to nullptr.
    return const_iterator(this, buf ? lastPopulatedCell() : buf);
  }

  const_iterator cend() const {
    return end();
  }

  iterator end() {
    return iterator(this, buf ? lastPopulatedCell() : buf);
  }

  /// The last parameter is unused but exists for compatibility with HashTable
  /// interface.
  void emplace(
      const Key& x,
      LookupResult& it,
      bool& inserted,
      size_t /* hash */ = 0) {
    it = &buf[x];

    if (!buf[x].isZero(*this)) {
      inserted = false;
      return;
    }

    new (&buf[x]) Cell(x, *this);
    inserted = true;

    if (!disableMinMaxOptimizationFlag) {
      if (x < min) {
        min = x;
      }
      if (x > max) {
        max = x;
      }
    }

    this->increaseSize();
  }

  std::pair<LookupResult, bool> insert(const value_type& x) {
    std::pair<LookupResult, bool> res;
    emplace(Cell::getKey(x), res.first, res.second);
    if (res.second) {
      res.first->setMapped(x);
    }

    return res;
  }

  LookupResult find(const Key& x) {
    return !buf[x].isZero(*this) ? &buf[x] : nullptr;
  }

  ConstLookupResult find(const Key& x) const {
    return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x);
  }

  LookupResult find(const Key&, size_t hashValue) {
    return !buf[hashValue].isZero(*this) ? &buf[hashValue] : nullptr;
  }

  ConstLookupResult find(const Key& key, size_t hashValue) const {
    return const_cast<std::decay_t<decltype(*this)>*>(this)->find(
        key, hashValue);
  }

  bool has(const Key& x) const {
    return !buf[x].isZero(*this);
  }
  bool has(const Key&, size_t hashValue) const {
    return !buf[hashValue].isZero(*this);
  }

  /// Decide if we use the min/max optimization. `max < min` means the
  /// FixedHashTable is empty. The flag `onlyEmplaceWasUsedToInsertData` will
  /// check if the FixedHashTable will only use `emplace()` to insert the raw
  /// data. `disableMinMaxOptimizationFlag` means that the min/max optimization
  /// is disabled.
  bool canUseMinMaxOptimization() const {
    return (max >= min) && onlyEmplaceWasUsedToInsertData &&
        !disableMinMaxOptimizationFlag;
  }

  /// min/max optimization has to be disabled when FixedHashTable is used
  /// concurrently in certain scenarios. For example, when aggregator merges
  /// single level aggregation state in parallel.
  void disableMinMaxOptimization() {
    disableMinMaxOptimizationFlag = true;
  }

  const Cell* firstPopulatedCell() const {
    const Cell* ptr = buf;
    if (!canUseMinMaxOptimization()) {
      while (ptr < buf + kNumCells && ptr->isZero(*this)) {
        ++ptr;
      }
    } else {
      ptr = buf + min;
    }

    return ptr;
  }

  Cell* lastPopulatedCell() const {
    return canUseMinMaxOptimization() ? buf + max + 1 : buf + kNumCells;
  }

  size_t size() const {
    return this->getSize(buf, *this, kNumCells);
  }
  bool empty() const {
    return this->isEmpty(buf, *this, kNumCells);
  }

  void clear() {
    destroyElements();
    this->clearSize();

    memset(static_cast<void*>(buf), 0, kNumCells * sizeof(*buf));
  }

  /// After executing this function, the table can only be destroyed, and also
  /// you can use the methods `size`, `empty`, `begin`, `end`.
  void clearAndShrink() {
    destroyElements();
    this->clearSize();
    free();
  }

  size_t getBufferSizeInBytes() const {
    return kNumCells * sizeof(Cell);
  }

  size_t getBufferSizeInCells() const {
    return kNumCells;
  }

  /// Return offset for result in internal buffer.
  size_t offsetInternal(ConstLookupResult ptr) const {
    if (ptr->isZero(*this)) {
      return 0;
    }
    return ptr - buf + 1;
  }

  const Cell* data() const {
    return buf;
  }
  Cell* data() {
    onlyEmplaceWasUsedToInsertData = false;
    return buf;
  }
};

} // namespace facebook::velox::exec::ch
