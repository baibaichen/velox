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

#include "velox/exec/ch/HashMap.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Arena.h"
#include "velox/exec/ch/RowRef.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class HashMapTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    mapPool_ = memory::memoryManager()->addLeafPool("ch-hash-map-test");
    arenaPool_ =
        memory::memoryManager()->addLeafPool("ch-hash-map-arena-test");
  }

  static uint64_t refWord(uint32_t block, uint32_t row) {
    return RowRef(block, row).encode();
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;
};

TEST_F(HashMapTest, fixedWidthCellsDoNotStoreSavedHash) {
  EXPECT_TRUE((std::is_same_v<
               HashMapAll_key32::cell_type,
               HashMapCell<UInt32, RowRefList, HashCRC32<UInt32>>>));
  EXPECT_TRUE((std::is_same_v<
               HashMapAll_key64::cell_type,
               HashMapCell<UInt64, RowRefList, HashCRC32<UInt64>>>));
  EXPECT_TRUE((std::is_same_v<
               HashMapAll_keys128::cell_type,
               HashMapCell<UInt128, RowRefList, HashWide<UInt128>>>));
  EXPECT_TRUE((std::is_same_v<
               HashMapAll_keys256::cell_type,
               HashMapCell<UInt256, RowRefList, HashWide<UInt256>>>));
  EXPECT_TRUE((std::is_same_v<
               HashMapAll_serialized::cell_type,
               HashMapCellWithSavedHash<
                   StringRef,
                   RowRefList,
                   StringRefHash>>));
  EXPECT_EQ(sizeof(HashMapAll_key32::cell_type), 16);
  EXPECT_EQ(sizeof(HashMapAll_key64::cell_type), 16);
  EXPECT_EQ(sizeof(HashMapAll_keys128::cell_type), 24);
  EXPECT_EQ(sizeof(HashMapAll_keys256::cell_type), 40);
}

TEST_F(HashMapTest, emplaceReturnsLiveCoordinateMapping) {
  HashMapAll_key64 map(mapPool_.get());
  Arena arena(arenaPool_.get());
  const auto expected = refWord(7, 19);

  bool inserted = false;
  auto& mapped = map.emplace(42, inserted);
  ASSERT_TRUE(inserted);
  mapped.insert(expected, arena);

  auto* cell = map.find(42);
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->getMapped().firstWord(), expected);

  auto& existing = map.emplace(42, inserted);
  EXPECT_FALSE(inserted);
  EXPECT_EQ(&existing, &cell->getMapped());
}

TEST_F(HashMapTest, usesVeloxCrc32ForFixedWidthKeys) {
  HashMapAll_key32 map32(mapPool_.get());
  HashMapAll_key64 map64(mapPool_.get());

  EXPECT_EQ(map32.hash(123456789), 3531890030);
  EXPECT_EQ(map64.hash(123456789), 3531890030);
}

TEST_F(HashMapTest, storesZeroKeySeparately) {
  HashMapAll_key64 map(mapPool_.get());
  const auto expected = refWord(8, 23);

  bool inserted = false;
  auto& mapped = map.emplace(0, inserted);
  ASSERT_TRUE(inserted);
  mapped.word = expected;

  for (uint64_t key = 1; key <= 1000; ++key) {
    map.emplace(key).word = refWord(9, key);
  }

  auto* cell = map.find(0);
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->getKey(), 0);
  EXPECT_EQ(cell->getMapped().firstWord(), expected);
  EXPECT_EQ(map.begin()->getKey(), 0);

  map.emplace(0, inserted);
  EXPECT_FALSE(inserted);
}

TEST_F(HashMapTest, wideKeysSupportInsertFindAndZeroKey) {
  HashMapAll_keys128 map128(mapPool_.get());
  HashMapAll_keys256 map256(mapPool_.get());
  Arena arena(arenaPool_.get());

  const UInt128 key128{{11, 22}};
  const UInt256 key256{{33, 44, 55, 66}};
  map128.emplace(key128).insert(refWord(1, 2), arena);
  map128.emplace(key128).insert(refWord(1, 3), arena);
  map256.emplace(key256).insert(refWord(4, 5), arena);

  ASSERT_NE(map128.find(key128), nullptr);
  EXPECT_EQ(map128.find(key128)->getMapped().rows(), 2);
  ASSERT_NE(map256.find(key256), nullptr);
  EXPECT_EQ(map256.find(key256)->getMapped().firstWord(), refWord(4, 5));
  EXPECT_EQ(map128.find(UInt128{{99, 0}}), nullptr);
  EXPECT_EQ(map256.find(UInt256{{99, 0, 0, 0}}), nullptr);

  bool inserted = false;
  map128.emplace(UInt128{}, inserted).word = refWord(7, 8);
  ASSERT_TRUE(inserted);
  map256.emplace(UInt256{}, inserted).word = refWord(9, 10);
  ASSERT_TRUE(inserted);
  ASSERT_NE(map128.find(UInt128{}), nullptr);
  EXPECT_EQ(map128.find(UInt128{})->getMapped().firstWord(), refWord(7, 8));
  ASSERT_NE(map256.find(UInt256{}), nullptr);
  EXPECT_EQ(map256.find(UInt256{})->getMapped().firstWord(), refWord(9, 10));
}

TEST_F(HashMapTest, duplicateCoordinatesUseRowRefListChain) {
  HashMapAll_key64 map(mapPool_.get());
  Arena arena(arenaPool_.get());
  std::vector<uint64_t> expected;

  for (uint32_t row = 0; row < 20; ++row) {
    expected.push_back(refWord(3, row));
    map.emplace(9).insert(expected.back(), arena);
  }

  auto* cell = map.find(9);
  ASSERT_NE(cell, nullptr);
  EXPECT_EQ(cell->getMapped().rows(), expected.size());

  std::vector<uint64_t> actual;
  for (auto word : cell->getMapped()) {
    actual.push_back(word);
  }
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(actual, expected);
}

struct SameBucketHash {
  size_t operator()(uint64_t key) const {
    return key << 2;
  }
};

using CollisionCell =
    HashMapCellWithSavedHash<uint64_t, RowRefList, SameBucketHash>;
using CollisionMap = HashMapTable<
    uint64_t,
    CollisionCell,
    SameBucketHash,
    HashTableGrower<2>,
    HashTableAllocatorAdapter>;

TEST_F(HashMapTest, savedHashAndKeyDistinguishBucketCollisions) {
  HashTableNoState state;
  CollisionCell cell(1, state);
  cell.setHash(SameBucketHash{}(1));
  EXPECT_TRUE(cell.keyEquals(1, SameBucketHash{}(1), state));
  EXPECT_FALSE(cell.keyEquals(1, SameBucketHash{}(2), state));
  EXPECT_FALSE(cell.keyEquals(2, SameBucketHash{}(1), state));

  CollisionMap map(mapPool_.get());
  bool inserted;
  map.emplace(1, inserted).word = refWord(1, 1);
  ASSERT_TRUE(inserted);
  map.emplace(2, inserted).word = refWord(2, 2);
  ASSERT_TRUE(inserted);

  ASSERT_NE(map.find(1), nullptr);
  ASSERT_NE(map.find(2), nullptr);
  EXPECT_EQ(map.find(1)->getMapped().firstWord(), refWord(1, 1));
  EXPECT_EQ(map.find(2)->getMapped().firstWord(), refWord(2, 2));
}

TEST_F(HashMapTest, rehashPreservesEveryCoordinateChain) {
  HashMapAll_key64 map(mapPool_.get());
  Arena arena(arenaPool_.get());

  for (uint64_t key = 1; key <= 1000; ++key) {
    map.emplace(key).insert(refWord(4, key), arena);
    map.emplace(key).insert(refWord(5, key), arena);
  }

  for (uint64_t key = 1; key <= 1000; ++key) {
    auto* cell = map.find(key);
    ASSERT_NE(cell, nullptr);
    EXPECT_EQ(cell->getMapped().rows(), 2);
    std::vector<uint64_t> words;
    for (auto word : cell->getMapped()) {
      words.push_back(word);
    }
    EXPECT_EQ(
        words,
        (std::vector<uint64_t>{refWord(4, key), refWord(5, key)}));
  }
}

TEST_F(HashMapTest, iteratesValuesAndReleasesCellStorage) {
  EXPECT_EQ(mapPool_->usedBytes(), 0);
  int64_t initialBytes = 0;
  int64_t grownBytes = 0;
  {
    HashMapAll_key32 map(mapPool_.get());
    initialBytes = mapPool_->usedBytes();
    EXPECT_GT(initialBytes, 0);

    for (uint32_t key = 1; key <= 10000; ++key) {
      map.emplace(key).word = refWord(6, key);
    }
    grownBytes = mapPool_->usedBytes();
    EXPECT_GT(grownBytes, initialBytes);

    uint64_t keySum = 0;
    uint64_t rowSum = 0;
    map.forEachValue([&](uint32_t key, RowRefList& mapped) {
      keySum += key;
      rowSum += refWordRowNo(mapped.firstWord());
    });
    EXPECT_EQ(keySum, 10000ULL * 10001 / 2);
    EXPECT_EQ(rowSum, keySum);
  }
  EXPECT_GT(grownBytes, initialBytes);
  EXPECT_EQ(mapPool_->usedBytes(), 0);
}

} // namespace
} // namespace facebook::velox::exec::ch
