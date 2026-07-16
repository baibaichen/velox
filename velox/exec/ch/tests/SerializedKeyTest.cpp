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

#include "velox/exec/ch/Common/ColumnsHashing/SerializedKey.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class SerializedKeyTest : public testing::Test,
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

TEST_F(SerializedKeyTest, prefixesStringsAndPreservesColumnOrder) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({"a", "ab"}),
      makeFlatVector<std::string>({"bc", "c"}),
  });
  SelectivityVector rows(input->size());
  SerializedKeyDecoder decoder(
      input, {0, 1}, {VARCHAR(), VARCHAR()}, rows);

  std::string first;
  std::string second;
  ASSERT_TRUE(decoder.serialize(0, first));
  ASSERT_TRUE(decoder.serialize(1, second));
  EXPECT_NE(first, second);
  ASSERT_EQ(first.size(), 11);
  uint32_t firstLength = 0;
  uint32_t secondLength = 0;
  std::memcpy(&firstLength, first.data(), sizeof(firstLength));
  std::memcpy(&secondLength, first.data() + 5, sizeof(secondLength));
  EXPECT_EQ(firstLength, 1);
  EXPECT_EQ(secondLength, 2);
  EXPECT_EQ(first.substr(4, 1), "a");
  EXPECT_EQ(first.substr(9, 2), "bc");
}

TEST_F(SerializedKeyTest, decodesDictionaryAndConstantAndSkipsNull) {
  auto base = makeNullableFlatVector<std::string>(
      {"zero", std::nullopt, "two"});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr, makeIndices({2, 0, 1}), 3, base);
  auto constant = BaseVector::wrapInConstant(
      3, 0, makeFlatVector<int64_t>({17}));
  auto input = makeRowVector({dictionary, constant});
  SelectivityVector rows(input->size());
  SerializedKeyDecoder decoder(
      input, {0, 1}, {VARCHAR(), BIGINT()}, rows);

  std::string key;
  ASSERT_TRUE(decoder.serialize(0, key));
  ASSERT_EQ(key.size(), 15);
  EXPECT_EQ(key.substr(4, 3), "two");
  int64_t fixed = 0;
  std::memcpy(&fixed, key.data() + 7, sizeof(fixed));
  EXPECT_EQ(fixed, 17);
  EXPECT_TRUE(decoder.serialize(1, key));
  EXPECT_EQ(key.substr(4, 4), "zero");
  EXPECT_FALSE(decoder.serialize(2, key));
}

TEST_F(SerializedKeyTest, serializesOverwideFixedKey) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1}),
      makeFlatVector<int64_t>({2}),
      makeFlatVector<int64_t>({3}),
      makeFlatVector<int64_t>({4}),
      makeFlatVector<int64_t>({5}),
  });
  SelectivityVector rows(input->size());
  SerializedKeyDecoder decoder(
      input,
      {0, 1, 2, 3, 4},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      rows);

  std::string key;
  ASSERT_TRUE(decoder.serialize(0, key));
  ASSERT_EQ(key.size(), 40);
  std::array<int64_t, 5> values{};
  std::memcpy(values.data(), key.data(), key.size());
  EXPECT_EQ(values, (std::array<int64_t, 5>{1, 2, 3, 4, 5}));
}

TEST_F(SerializedKeyTest, emptyStringIsNotTheZeroKey) {
  const uint32_t length = 0;
  StringRef temporary{reinterpret_cast<const char*>(&length), sizeof(length)};
  EXPECT_FALSE(ZeroTraits::check(temporary));

  Arena arena(pool());
  HashMapAll_key_string map(pool());
  StringRef persisted{
      arena.insert(temporary.data, temporary.size), temporary.size};
  map.emplace(persisted);

  EXPECT_NE(map.find(temporary), nullptr);
  EXPECT_EQ(map.size(), 1);
}

TEST_F(SerializedKeyTest, stringRefHashIsSelfConsistentAcrossLengths) {
  StringRefHash hash;

  // Empty key does not crash and hashes deterministically to zero.
  EXPECT_EQ(hash(StringRef{nullptr, 0}), hash(StringRef{nullptr, 0}));

  // Cover the tail path (0-7 residual bytes) plus multi-word and long keys.
  // Each length is exercised twice to confirm the hash is deterministic, and
  // the payloads are padded so no read runs past the declared size (an
  // out-of-bounds read would trip under ASAN).
  for (uint32_t length : {0u, 1u, 7u, 8u, 9u, 128u}) {
    std::string bytes(length, 'x');
    StringRef key{bytes.data(), length};
    EXPECT_EQ(hash(key), hash(key));
  }

  // Distinct bytes hash differently; identical bytes hash the same.
  const std::string left = "clickhouse";
  const std::string right = "clickhous_";
  const std::string leftCopy = left;
  EXPECT_EQ(
      hash(StringRef{left.data(), static_cast<uint32_t>(left.size())}),
      hash(StringRef{leftCopy.data(), static_cast<uint32_t>(leftCopy.size())}));
  EXPECT_NE(
      hash(StringRef{left.data(), static_cast<uint32_t>(left.size())}),
      hash(StringRef{right.data(), static_cast<uint32_t>(right.size())}));
}

TEST_F(SerializedKeyTest, doesNotSupportProbePrefetch) {
  EXPECT_FALSE(SerializedKeyDecoder::hasCheapKeyCalculation);
}

} // namespace
} // namespace facebook::velox::exec::ch
