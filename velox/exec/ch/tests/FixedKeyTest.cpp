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

#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h"

#include "velox/common/memory/Memory.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <limits>

namespace facebook::velox::exec::ch {
namespace {

class FixedKeyTest : public testing::Test,
                     public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }
};

TEST_F(FixedKeyTest, packsColumnsInOrderAndSelectsWidth) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0x0102030405060708}),
      makeFlatVector<int32_t>({0x11121314}),
      makeFlatVector<int32_t>({0x21222324}),
  });
  SelectivityVector rows(input->size());
  FixedKeyDecoder decoder(input, {0, 1, 2}, rows);

  EXPECT_EQ(decoder.width(), FixedKeyWidth::k128);
  EXPECT_FALSE(decoder.mayHaveNulls());
  UInt128 key;
  ASSERT_TRUE(decoder.pack(0, key));
  const std::array<uint8_t, 16> expected{
      0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
      0x14, 0x13, 0x12, 0x11, 0x24, 0x23, 0x22, 0x21};
  EXPECT_EQ(std::memcmp(&key, expected.data(), expected.size()), 0);
}

TEST_F(FixedKeyTest, packsFourBigintsIntoUInt256) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1}),
      makeFlatVector<int64_t>({2}),
      makeFlatVector<int64_t>({3}),
      makeFlatVector<int64_t>({4}),
  });
  SelectivityVector rows(input->size());
  FixedKeyDecoder decoder(input, {0, 1, 2, 3}, rows);

  EXPECT_EQ(decoder.width(), FixedKeyWidth::k256);
  UInt256 key;
  ASSERT_TRUE(decoder.pack(0, key));
  EXPECT_EQ(key.words, (std::array<uint64_t, 4>{1, 2, 3, 4}));
}

TEST_F(FixedKeyTest, decodesDictionaryAndConstantAndSkipsNull) {
  auto base = makeNullableFlatVector<int64_t>({11, std::nullopt, 33});
  auto indices = makeIndices({2, 0, 1});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr, indices, 3, base);
  auto constantBase = makeFlatVector<int32_t>({7});
  auto constant = BaseVector::wrapInConstant(3, 0, constantBase);
  auto input = makeRowVector({dictionary, constant});
  SelectivityVector rows(input->size());
  FixedKeyDecoder decoder(input, {0, 1}, rows);

  UInt128 key;
  ASSERT_TRUE(decoder.pack(0, key));
  EXPECT_EQ(key.words[0], 33);
  EXPECT_EQ(key.words[1] & 0xffffffffULL, 7);
  EXPECT_TRUE(decoder.pack(1, key));
  EXPECT_EQ(key.words[0], 11);
  EXPECT_FALSE(decoder.pack(2, key));
}

TEST_F(FixedKeyTest, rawPointerFastPathMatchesPackedBytes) {
  auto bigintInput = makeRowVector({makeFlatVector<int64_t>({11, -1, 33})});
  SelectivityVector bigintRows(bigintInput->size());
  FixedKeyDecoder bigintDecoder(bigintInput, {0}, bigintRows);

  uint64_t bigintKey = 0;
  ASSERT_TRUE(bigintDecoder.packFast(1, bigintKey));
  EXPECT_EQ(bigintKey, std::numeric_limits<uint64_t>::max());
  uint64_t scalarBigintKey = 0;
  ASSERT_TRUE(bigintDecoder.pack(1, scalarBigintKey));
  EXPECT_EQ(bigintKey, scalarBigintKey);

  auto tinyintInput = makeRowVector({makeFlatVector<int8_t>({1, -1, 3})});
  SelectivityVector tinyintRows(tinyintInput->size());
  FixedKeyDecoder tinyintDecoder(tinyintInput, {0}, tinyintRows);

  uint64_t tinyintKey = 0;
  ASSERT_TRUE(tinyintDecoder.packFast(1, tinyintKey));
  EXPECT_EQ(tinyintKey, 0xff);
  uint64_t scalarTinyintKey = 0;
  ASSERT_TRUE(tinyintDecoder.pack(1, scalarTinyintKey));
  EXPECT_EQ(tinyintKey, scalarTinyintKey);
}

TEST_F(FixedKeyTest, rawPointerFastPathRejectsFallbackInputs) {
  auto base = makeFlatVector<int64_t>({11, 22, 33});
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, makeIndices({2, 0, 1}), 3, base);
  auto dictionaryInput = makeRowVector({dictionary});
  SelectivityVector dictionaryRows(dictionaryInput->size());
  FixedKeyDecoder dictionaryDecoder(dictionaryInput, {0}, dictionaryRows);
  uint64_t key = 0;
  EXPECT_FALSE(dictionaryDecoder.packFast(0, key));
  ASSERT_TRUE(dictionaryDecoder.pack(0, key));
  EXPECT_EQ(key, 33);

  auto nullableInput = makeRowVector(
      {makeNullableFlatVector<int64_t>({11, std::nullopt, 33})});
  SelectivityVector nullableRows(nullableInput->size());
  FixedKeyDecoder nullableDecoder(nullableInput, {0}, nullableRows);
  EXPECT_FALSE(nullableDecoder.packFast(0, key));
  EXPECT_FALSE(nullableDecoder.pack(1, key));

  auto compositeInput = makeRowVector({
      makeFlatVector<int64_t>({0x0102030405060708}),
      makeFlatVector<int32_t>({0x11121314}),
  });
  SelectivityVector compositeRows(compositeInput->size());
  FixedKeyDecoder compositeDecoder(compositeInput, {0, 1}, compositeRows);
  UInt128 compositeKey;
  EXPECT_FALSE(compositeDecoder.packFast(0, compositeKey));
  ASSERT_TRUE(compositeDecoder.pack(0, compositeKey));
  EXPECT_EQ(compositeKey.words[0], 0x0102030405060708);
  EXPECT_EQ(compositeKey.words[1], 0x11121314);
}

TEST_F(FixedKeyTest, batchPackingMatchesScalarPackingAndMarksNulls) {
  auto first = makeNullableFlatVector<int64_t>(
      {0x0102030405060708, 11, std::nullopt, 33});
  auto indices = makeIndices({3, 0, 2, 1});
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 4, first);
  auto second = makeFlatVector<int32_t>({7, 8, 9, 10});
  auto constantBase = makeFlatVector<int32_t>({17});
  auto constant = BaseVector::wrapInConstant(4, 0, constantBase);
  auto input = makeRowVector({dictionary, second, constant});
  SelectivityVector rows(input->size());
  FixedKeyDecoder decoder(input, {0, 1, 2}, rows);

  EXPECT_TRUE(decoder.mayHaveNulls());
  decoder.packAll<UInt128>();
  for (vector_size_t row = 0; row < input->size(); ++row) {
    UInt128 scalar;
    const bool scalarIsValid = decoder.pack(row, scalar);
    EXPECT_EQ(decoder.hasPackedKeyAt(row), scalarIsValid);
    if (scalarIsValid) {
      EXPECT_EQ(
          std::memcmp(
              &decoder.packedAt<UInt128>(row), &scalar, sizeof(UInt128)),
          0);
    }
  }

  EXPECT_FALSE(decoder.hasPackedKeyAt(2));
}

TEST_F(FixedKeyTest, rejectsUnsupportedAndOversizedKeys) {
  auto boolInput = makeRowVector({makeFlatVector<bool>({true})});
  SelectivityVector boolRows(boolInput->size());
  EXPECT_ANY_THROW(FixedKeyDecoder(boolInput, {0}, boolRows));

  std::vector<VectorPtr> columns;
  std::vector<column_index_t> channels;
  for (column_index_t i = 0; i < 5; ++i) {
    columns.push_back(makeFlatVector<int64_t>({i}));
    channels.push_back(i);
  }
  auto oversized = makeRowVector(std::move(columns));
  SelectivityVector oversizedRows(oversized->size());
  EXPECT_ANY_THROW(FixedKeyDecoder(oversized, channels, oversizedRows));
}

TEST_F(FixedKeyTest, supportsProbePrefetch) {
  EXPECT_TRUE(FixedKeyDecoder::hasCheapKeyCalculation);
}

} // namespace
} // namespace facebook::velox::exec::ch
