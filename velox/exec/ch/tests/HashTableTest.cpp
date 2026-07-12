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

#include "velox/exec/ch/HashTable.h"
#include "velox/exec/ch/HashTableAllocatorAdapter.h"

#include "velox/common/memory/Memory.h"

#include <cstdint>
#include <functional>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

struct TestCell {
  using State = HashTableNoState;
  using key_type = uint64_t;
  using mapped_type = uint64_t;
  using value_type = TestCell;

  static constexpr bool need_zero_value_storage = false;

  uint64_t key{0};
  uint64_t mapped{0};

  TestCell() = default;
  TestCell(uint64_t keyValue, const State&) : key(keyValue) {}

  static const uint64_t& getKey(const value_type& value) {
    return value.key;
  }

  const uint64_t& getKey() const {
    return key;
  }

  const value_type& getValue() const {
    return *this;
  }

  uint64_t& getMapped() {
    return mapped;
  }

  void setMapped(const value_type& value) {
    mapped = value.mapped;
  }

  bool keyEquals(uint64_t other, size_t, const State&) const {
    return key == other;
  }

  bool isZero(const State&) const {
    return key == 0;
  }

  static bool isZero(uint64_t value, const State&) {
    return value == 0;
  }

  void setZero() {
    key = 0;
  }

  void setHash(size_t) {}

  size_t getHash(const std::hash<uint64_t>& hash) const {
    return hash(key);
  }
};

using TestTable = HashTable<
    uint64_t,
    TestCell,
    std::hash<uint64_t>,
    HashTableGrower<2>,
    HashTableAllocatorAdapter>;

struct ZeroKeyCell {
  using State = HashTableNoState;
  using key_type = uint64_t;
  using mapped_type = uint64_t;
  using value_type = ZeroKeyCell;

  static constexpr bool need_zero_value_storage = true;

  uint64_t key;
  uint64_t mapped;

  ZeroKeyCell() {}
  ZeroKeyCell(uint64_t keyValue, const State&) : key(keyValue), mapped(0) {}

  static const uint64_t& getKey(const value_type& value) {
    return value.key;
  }

  const uint64_t& getKey() const {
    return key;
  }

  const value_type& getValue() const {
    return *this;
  }

  uint64_t& getMapped() {
    return mapped;
  }

  bool keyEquals(uint64_t other, size_t, const State&) const {
    return key == other;
  }

  bool isZero(const State&) const {
    return key == 0;
  }

  static bool isZero(uint64_t value, const State&) {
    return value == 0;
  }

  void setZero() {
    key = 0;
  }

  void setHash(size_t) {}

  size_t getHash(const std::hash<uint64_t>& hash) const {
    return hash(key);
  }
};

using ZeroKeyTable = HashTable<
    uint64_t,
    ZeroKeyCell,
    std::hash<uint64_t>,
    HashTableGrower<2>,
    HashTableAllocatorAdapter>;

class HashTableTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("ch-hash-table-test");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(HashTableTest, allocatorZeroFillsAndPreservesReallocatedBytes) {
  HashTableAllocatorAdapter allocator(pool_.get());
  auto* bytes = static_cast<uint8_t*>(allocator.alloc(64));
  ASSERT_NE(bytes, nullptr);
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(bytes[i], 0);
    bytes[i] = static_cast<uint8_t>(i + 1);
  }

  bytes = static_cast<uint8_t*>(allocator.realloc(bytes, 64, 128));
  for (size_t i = 0; i < 64; ++i) {
    EXPECT_EQ(bytes[i], static_cast<uint8_t>(i + 1));
  }
  for (size_t i = 64; i < 128; ++i) {
    EXPECT_EQ(bytes[i], 0);
  }

  allocator.free(bytes, 128);
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(HashTableTest, insertsFindsAndRejectsDuplicates) {
  TestTable table(pool_.get());
  EXPECT_EQ(table.find(91), nullptr);

  TestTable::LookupResult cell;
  bool inserted;
  table.emplace(91, cell, inserted);
  ASSERT_TRUE(inserted);
  cell->getMapped() = 1234;

  table.emplace(91, cell, inserted);
  EXPECT_FALSE(inserted);
  ASSERT_NE(table.find(91), nullptr);
  EXPECT_EQ(table.find(91)->getMapped(), 1234);
  EXPECT_EQ(table.size(), 1);
}

TEST_F(HashTableTest, initializesSeparatelyStoredZeroKey) {
  alignas(ZeroKeyTable) std::byte storage[sizeof(ZeroKeyTable)];
  std::memset(storage, 0xa5, sizeof(storage));
  auto* table = new (storage) ZeroKeyTable(pool_.get());

  ZeroKeyTable::LookupResult cell;
  bool inserted;
  table->emplace(uint64_t{0}, cell, inserted);

  ASSERT_TRUE(inserted);
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->getKey(), 0);
  EXPECT_EQ(table->find(0), cell);

  table->~ZeroKeyTable();
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(HashTableTest, resizePreservesEveryMappedValueAndReleasesMemory) {
  EXPECT_EQ(pool_->usedBytes(), 0);
  {
    TestTable table(pool_.get());
    EXPECT_GT(pool_->usedBytes(), 0);

    for (uint64_t key = 1; key <= 100; ++key) {
      TestTable::LookupResult cell;
      bool inserted;
      table.emplace(key, cell, inserted);
      ASSERT_TRUE(inserted);
      cell->getMapped() = key * 17;
    }

    for (uint64_t key = 1; key <= 100; ++key) {
      ASSERT_NE(table.find(key), nullptr);
      EXPECT_EQ(table.find(key)->getMapped(), key * 17);
    }
  }
  EXPECT_EQ(pool_->usedBytes(), 0);
}

} // namespace
} // namespace facebook::velox::exec::ch
