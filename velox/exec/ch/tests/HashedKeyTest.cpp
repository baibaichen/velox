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

#include "velox/exec/ch/HashedKey.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/HashMap.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class HashedKeyTest : public testing::Test,
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

TEST_F(HashedKeyTest, hashesColumnsWithoutAmbiguousConcatenation) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({"a", "ab"}),
      makeFlatVector<std::string>({"bc", "c"}),
  });
  SelectivityVector rows(input->size());
  HashedKeyDecoder decoder(input, {0, 1}, {VARCHAR(), VARCHAR()}, rows);

  UInt128 first;
  UInt128 second;
  ASSERT_TRUE(decoder.hash(0, first));
  ASSERT_TRUE(decoder.hash(1, second));
  EXPECT_NE(first, second);
}

TEST_F(HashedKeyTest, decodesWrappedVectorsAndSkipsNull) {
  auto base = makeNullableFlatVector<std::string>(
      {"zero", std::nullopt, "two"});
  auto dictionary = BaseVector::wrapInDictionary(
      nullptr, makeIndices({2, 0, 1}), 3, base);
  auto constant = BaseVector::wrapInConstant(
      3, 0, makeFlatVector<int64_t>({17}));
  auto input = makeRowVector({dictionary, constant});
  SelectivityVector rows(input->size());
  HashedKeyDecoder decoder(input, {0, 1}, {VARCHAR(), BIGINT()}, rows);

  UInt128 first;
  UInt128 second;
  UInt128 nullKey;
  EXPECT_TRUE(decoder.hash(0, first));
  EXPECT_TRUE(decoder.hash(1, second));
  EXPECT_NE(first, second);
  EXPECT_FALSE(decoder.hash(2, nullKey));
}

TEST_F(HashedKeyTest, hashesEmptyStringAndOverwideFixedKey) {
  auto strings = makeRowVector({makeFlatVector<std::string>({"", "x"})});
  SelectivityVector stringRows(strings->size());
  HashedKeyDecoder stringDecoder(strings, {0}, {VARCHAR()}, stringRows);
  UInt128 empty;
  UInt128 nonEmpty;
  EXPECT_TRUE(stringDecoder.hash(0, empty));
  EXPECT_TRUE(stringDecoder.hash(1, nonEmpty));
  EXPECT_NE(empty, nonEmpty);

  auto wide = makeRowVector({
      makeFlatVector<int64_t>({1}),
      makeFlatVector<int64_t>({2}),
      makeFlatVector<int64_t>({3}),
      makeFlatVector<int64_t>({4}),
      makeFlatVector<int64_t>({5}),
  });
  SelectivityVector wideRows(wide->size());
  HashedKeyDecoder wideDecoder(
      wide,
      {0, 1, 2, 3, 4},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      wideRows);
  UInt128 digest;
  EXPECT_TRUE(wideDecoder.hash(0, digest));
}

TEST_F(HashedKeyTest, mapStoresDigestWithoutSavedHash) {
  EXPECT_EQ(
      sizeof(HashMapAll_hashed::cell_type),
      sizeof(HashMapAll_keys128::cell_type));
  EXPECT_FALSE((std::is_same_v<HashMapAll_hashed, HashMapAll_keys128>));
  EXPECT_FALSE(HashedKeyDecoder::hasCheapKeyCalculation);
}

TEST_F(HashedKeyTest, pinsDigestForFixedKey) {
  // Characterization test: pins the actual 128-bit digest of a concrete
  // single-column VARCHAR key ("x") so any change to the hash
  // function/seed/word-extraction is caught. The distinctness tests above
  // only prove different keys map to different digests; this locks the
  // exact mapping. Update these literals if the hash function/seed
  // intentionally changes.
  auto strings = makeRowVector({makeFlatVector<std::string>({"x"})});
  SelectivityVector rows(strings->size());
  HashedKeyDecoder decoder(strings, {0}, {VARCHAR()}, rows);
  UInt128 digest;
  ASSERT_TRUE(decoder.hash(0, digest));
  EXPECT_EQ(digest.words[0], 0x305d1ecd260d8a7dULL);
  EXPECT_EQ(digest.words[1], 0x4ff80a75f079d4dfULL);
}

} // namespace
} // namespace facebook::velox::exec::ch
