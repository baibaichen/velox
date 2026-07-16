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

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

namespace facebook::velox::exec::ch {

struct HashTableNoState {};

/// Ports ClickHouse VoidKey/VoidMapped: placeholder key/value types used by the
/// Set-variant cells (e.g. FixedHashTableCell) that carry no real mapped value.
struct VoidKey {};
struct VoidMapped {
  template <typename T>
  auto& operator=(const T&) {
    return *this;
  }
};

template <typename T>
inline bool bitEquals(T lhs, T rhs) {
  if constexpr (std::is_floating_point_v<T>) {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
  } else {
    return lhs == rhs;
  }
}

namespace ZeroTraits {

template <typename T>
bool check(const T& value) {
  return bitEquals(value, T{});
}

template <typename T>
void set(T& value) {
  value = T{};
}

} // namespace ZeroTraits

template <size_t initialSizeDegree = 8>
struct HashTableGrower {
  uint8_t sizeDegree{initialSizeDegree};
  static constexpr auto initial_count = 1ULL << initialSizeDegree;
  static constexpr auto performs_linear_probing_with_single_step = true;
  static constexpr size_t max_size_degree = 23;

  size_t bufSize() const {
    return 1ULL << sizeDegree;
  }

  size_t maxFill() const {
    return 1ULL << (sizeDegree - 1);
  }

  size_t mask() const {
    return bufSize() - 1;
  }

  size_t place(size_t hashValue) const {
    return hashValue & mask();
  }

  size_t next(size_t position) const {
    return ++position & mask();
  }

  bool overflow(size_t elements) const {
    return elements > maxFill();
  }

  void increaseSize() {
    sizeDegree += sizeDegree >= max_size_degree ? 1 : 2;
  }

  void set(size_t numElements) {
    if (numElements <= 1) {
      sizeDegree = initialSizeDegree;
    } else if (
        initialSizeDegree >
        static_cast<size_t>(std::log2(numElements - 1)) + 2) {
      sizeDegree = initialSizeDegree;
    } else {
      sizeDegree = static_cast<uint8_t>(
          static_cast<size_t>(std::log2(numElements - 1)) + 2);
    }
  }

  void setBufSize(size_t bufferSize) {
    sizeDegree = static_cast<uint8_t>(
        static_cast<size_t>(std::log2(bufferSize - 1) + 1));
  }
};

template <bool needsZeroValueStorage, typename Cell>
struct ZeroValueStorage;

template <typename Cell>
struct ZeroValueStorage<true, Cell> {
  bool hasZero() const {
    return hasZero_;
  }

  void setHasZero() {
    VELOX_CHECK(!hasZero_);
    hasZero_ = true;
    new (zeroValue()) Cell();
    zeroValue()->setZero();
  }

  void clearHasZero() {
    if (hasZero_) {
      zeroValue()->~Cell();
      hasZero_ = false;
    }
  }

  void clearHasZeroFlag() {
    hasZero_ = false;
  }

  Cell* zeroValue() {
    return std::launder(reinterpret_cast<Cell*>(&storage_));
  }

  const Cell* zeroValue() const {
    return std::launder(reinterpret_cast<const Cell*>(&storage_));
  }

 private:
  bool hasZero_{false};
  alignas(Cell) std::byte storage_[sizeof(Cell)];
};

template <typename Cell>
struct ZeroValueStorage<false, Cell> {
  bool hasZero() const {
    return false;
  }

  void setHasZero() {
    VELOX_FAIL("HashTable cell does not support a zero key");
  }

  void clearHasZero() {}
  void clearHasZeroFlag() {}

  Cell* zeroValue() {
    return nullptr;
  }

  const Cell* zeroValue() const {
    return nullptr;
  }
};

template <
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator>
class HashTable : protected Hash,
                  protected Allocator,
                  protected Cell::State,
                  public ZeroValueStorage<Cell::need_zero_value_storage, Cell> {
 public:
  using key_type = Key;
  using grower_type = Grower;
  using mapped_type = typename Cell::mapped_type;
  using value_type = typename Cell::value_type;
  using cell_type = Cell;
  using LookupResult = Cell*;
  using ConstLookupResult = const Cell*;

  static constexpr size_t initial_buffer_bytes =
      Grower::initial_count * sizeof(Cell);

  explicit HashTable(memory::MemoryPool* pool) : Allocator(pool) {
    alloc(grower_);
  }

  HashTable(memory::MemoryPool* pool, const Grower& grower)
      : Allocator(pool), grower_(grower) {
    alloc(grower_);
  }

  HashTable(memory::MemoryPool* pool, size_t reserveForNumElements)
      : Allocator(pool) {
    grower_.set(reserveForNumElements);
    alloc(grower_);
  }

  HashTable(const HashTable&) = delete;
  HashTable& operator=(const HashTable&) = delete;
  HashTable(HashTable&&) = delete;
  HashTable& operator=(HashTable&&) = delete;

  ~HashTable() {
    destroyElements();
    free();
  }

  size_t hash(const Key& key) const {
    return Hash::operator()(key);
  }

  bool empty() const {
    return size_ == 0;
  }

  size_t size() const {
    return size_;
  }

  size_t getBufferSizeInCells() const {
    return grower_.bufSize();
  }

  size_t getBufferSizeInBytes() const {
    return checkedBufferSize(grower_.bufSize());
  }

  class iterator {
   public:
    iterator() = default;

    bool operator==(const iterator& other) const {
      return cell_ == other.cell_;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }

    iterator& operator++() {
      if (cell_ == table_->zeroValue()) {
        cell_ = table_->buffer_;
      } else {
        ++cell_;
      }
      skipEmpty();
      return *this;
    }

    Cell& operator*() const {
      return *cell_;
    }

    Cell* operator->() const {
      return cell_;
    }

    Cell* getPtr() const {
      return cell_;
    }

   private:
    friend class HashTable;

    iterator(HashTable* table, Cell* cell) : table_(table), cell_(cell) {}

    void skipEmpty() {
      auto* end = table_->buffer_ + table_->grower_.bufSize();
      while (cell_ < end && cell_->isZero(*table_)) {
        ++cell_;
      }
    }

    HashTable* table_{nullptr};
    Cell* cell_{nullptr};
  };

  class const_iterator {
   public:
    const_iterator() = default;
    const_iterator(const iterator& other)
        : table_(other.table_), cell_(other.cell_) {}

    bool operator==(const const_iterator& other) const {
      return cell_ == other.cell_;
    }

    bool operator!=(const const_iterator& other) const {
      return !(*this == other);
    }

    const_iterator& operator++() {
      if (cell_ == table_->zeroValue()) {
        cell_ = table_->buffer_;
      } else {
        ++cell_;
      }
      skipEmpty();
      return *this;
    }

    const Cell& operator*() const {
      return *cell_;
    }

    const Cell* operator->() const {
      return cell_;
    }

    const Cell* getPtr() const {
      return cell_;
    }

   private:
    friend class HashTable;

    const_iterator(const HashTable* table, const Cell* cell)
        : table_(table), cell_(cell) {}

    void skipEmpty() {
      auto* end = table_->buffer_ + table_->grower_.bufSize();
      while (cell_ < end && cell_->isZero(*table_)) {
        ++cell_;
      }
    }

    const HashTable* table_{nullptr};
    const Cell* cell_{nullptr};
  };

  iterator begin() {
    if (this->hasZero()) {
      return iterator(this, this->zeroValue());
    }
    iterator result(this, buffer_);
    result.skipEmpty();
    return result;
  }

  const_iterator begin() const {
    if (this->hasZero()) {
      return const_iterator(this, this->zeroValue());
    }
    const_iterator result(this, buffer_);
    result.skipEmpty();
    return result;
  }

  iterator end() {
    return iterator(this, buffer_ + grower_.bufSize());
  }

  const_iterator end() const {
    return const_iterator(this, buffer_ + grower_.bufSize());
  }

  void reserve(size_t numElements) {
    resize(numElements);
  }

  void prefetchByHash(size_t hashValue) const {
    __builtin_prefetch(&buffer_[grower_.place(hashValue)]);
  }

  bool isEmptyCell(size_t hashValue) const {
    return buffer_[grower_.place(hashValue)].isZero(*this);
  }

  template <typename KeyHolder>
  void emplace(
      KeyHolder&& keyHolder,
      LookupResult& result,
      bool& inserted) {
    const Key& key = keyHolder;
    emplace(std::forward<KeyHolder>(keyHolder), result, inserted, hash(key));
  }

  template <typename KeyHolder>
  void emplace(
      KeyHolder&& keyHolder,
      LookupResult& result,
      bool& inserted,
      size_t hashValue) {
    const Key& key = keyHolder;
    if (!emplaceIfZero(key, result, inserted, hashValue)) {
      emplaceNonZero(key, result, inserted, hashValue);
    }
  }

  LookupResult find(const Key& key) {
    return find(key, hash(key));
  }

  ConstLookupResult find(const Key& key) const {
    return const_cast<HashTable*>(this)->find(key);
  }

  LookupResult find(const Key& key, size_t hashValue) {
    if (Cell::isZero(key, *this)) {
      return this->hasZero() ? this->zeroValue() : nullptr;
    }

    const auto position =
        findCell(key, hashValue, grower_.place(hashValue));
    return buffer_[position].isZero(*this) ? nullptr : &buffer_[position];
  }

  ConstLookupResult find(const Key& key, size_t hashValue) const {
    return const_cast<HashTable*>(this)->find(key, hashValue);
  }

 protected:
  size_t findCell(
      const Key& key,
      size_t hashValue,
      size_t position) const {
    while (
        !buffer_[position].isZero(*this) &&
        !buffer_[position].keyEquals(key, hashValue, *this)) {
      position = grower_.next(position);
    }
    return position;
  }

  size_t findEmptyCell(size_t position) const {
    while (!buffer_[position].isZero(*this)) {
      position = grower_.next(position);
    }
    return position;
  }

  void resize(size_t forNumElements = 0, size_t forBufferSize = 0) {
    const size_t oldSize = grower_.bufSize();
    Grower newGrower = grower_;

    if (forNumElements != 0) {
      newGrower.set(forNumElements);
      if (newGrower.bufSize() <= oldSize) {
        return;
      }
    } else if (forBufferSize != 0) {
      newGrower.setBufSize(forBufferSize);
      if (newGrower.bufSize() <= oldSize) {
        return;
      }
    } else {
      newGrower.increaseSize();
    }

    const size_t oldBufferSize = getBufferSizeInBytes();
    buffer_ = reinterpret_cast<Cell*>(Allocator::realloc(
        buffer_, oldBufferSize, checkedBufferSize(newGrower.bufSize())));
    grower_ = newGrower;

    if (!empty()) {
      size_t index = 0;
      for (; index < oldSize; ++index) {
        if (!buffer_[index].isZero(*this)) {
          reinsert(buffer_[index], buffer_[index].getHash(*this));
        }
      }

      const size_t newSize = grower_.bufSize();
      for (; index < newSize && !buffer_[index].isZero(*this); ++index) {
        reinsert(buffer_[index], buffer_[index].getHash(*this));
      }
    }
  }

 private:
  static size_t checkedBufferSize(size_t cells) {
    VELOX_CHECK_LE(cells, std::numeric_limits<size_t>::max() / sizeof(Cell));
    return cells * sizeof(Cell);
  }

  void alloc(const Grower& newGrower) {
    buffer_ =
        reinterpret_cast<Cell*>(Allocator::alloc(checkedBufferSize(
            newGrower.bufSize())));
    grower_ = newGrower;
  }

  void free() {
    if (buffer_ != nullptr) {
      Allocator::free(buffer_, getBufferSizeInBytes());
      buffer_ = nullptr;
    }
  }

  bool emplaceIfZero(
      const Key& key,
      LookupResult& result,
      bool& inserted,
      size_t hashValue) {
    if constexpr (!Cell::need_zero_value_storage) {
      return false;
    }

    if (Cell::isZero(key, *this)) {
      result = this->zeroValue();
      if (!this->hasZero()) {
        ++size_;
        this->setHasZero();
        this->zeroValue()->setHash(hashValue);
        inserted = true;
      } else {
        inserted = false;
      }
      return true;
    }
    return false;
  }

  void emplaceNonZero(
      const Key& key,
      LookupResult& result,
      bool& inserted,
      size_t hashValue) {
    size_t position = findCell(key, hashValue, grower_.place(hashValue));
    result = &buffer_[position];

    if (!buffer_[position].isZero(*this)) {
      inserted = false;
      return;
    }

    new (&buffer_[position]) Cell(key, *this);
    buffer_[position].setHash(hashValue);
    inserted = true;
    ++size_;

    if (grower_.overflow(size_)) {
      try {
        resize();
      } catch (...) {
        --size_;
        buffer_[position].setZero();
        inserted = false;
        throw;
      }

      position = findCell(key, hashValue, grower_.place(hashValue));
      VELOX_CHECK(!buffer_[position].isZero(*this));
      result = &buffer_[position];
    }
  }

  void reinsert(Cell& cell, size_t hashValue) {
    size_t position = grower_.place(hashValue);
    if (&cell == &buffer_[position]) {
      return;
    }

    position =
        findCell(Cell::getKey(cell.getValue()), hashValue, position);
    if (!buffer_[position].isZero(*this)) {
      return;
    }

    cell.setHash(hashValue);
    std::memcpy(&buffer_[position], &cell, sizeof(cell));
    cell.setZero();
  }

  void destroyElements() {
    if constexpr (!std::is_trivially_destructible_v<Cell>) {
      for (auto it = begin(); it != end();) {
        auto* cell = it.getPtr();
        ++it;
        cell->~Cell();
      }
      this->clearHasZeroFlag();
    } else {
      this->clearHasZero();
    }
  }

  size_t size_{0};
  Cell* buffer_{nullptr};
  Grower grower_;
};

} // namespace facebook::velox::exec::ch
