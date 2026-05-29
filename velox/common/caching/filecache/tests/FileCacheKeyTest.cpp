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
#include "velox/common/caching/filecache/FileCacheKey.h"

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(FileCacheKeyTest, fromPathDeterministic) {
  const auto a = FileCacheKey::fromPath("/a/b/c");
  const auto b = FileCacheKey::fromPath("/a/b/c");
  const auto c = FileCacheKey::fromPath("/a/b/d");

  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a == c);
}

TEST(FileCacheKeyTest, toStringIs32LowercaseHex) {
  const auto key = FileCacheKey::fromPath("/some/path");
  const auto keyString = key.toString();

  EXPECT_EQ(keyString.size(), 32);
  for (const auto ch : keyString) {
    const bool isLowerHex =
        (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
    EXPECT_TRUE(isLowerHex) << "unexpected char: " << ch;
  }
}

TEST(FileCacheKeyTest, toStringUsesClickHouseHexOrder) {
  EXPECT_EQ(
      FileCacheKey::fromKey(static_cast<UInt128>(1)).toString(),
      "00000000000000000000000000000001");
  EXPECT_EQ(
      FileCacheKey::fromKey(
          (static_cast<UInt128>(0x0123456789abcdefULL) << 64) |
          static_cast<UInt128>(0xfedcba9876543210ULL))
          .toString(),
      "0123456789abcdeffedcba9876543210");
}

TEST(FileCacheKeyTest, toStringFromKeyStringRoundTrip) {
  const auto key = FileCacheKey::fromPath("/round/trip");
  const auto keyString = key.toString();
  const auto restored = FileCacheKey::fromKeyString(keyString);

  EXPECT_TRUE(key == restored);
  EXPECT_EQ(keyString, restored.toString());
}

TEST(FileCacheKeyTest, fromKeyAndFromKeyStringAgree) {
  const auto key = FileCacheKey::fromPath("/abc");
  const auto viaKey = FileCacheKey::fromKey(key.key);

  EXPECT_TRUE(key == viaKey);
}

TEST(FileCacheKeyTest, fromKeyStringRejectsBadInput) {
  EXPECT_THROW(FileCacheKey::fromKeyString("tooshort"), std::exception);
}

TEST(FileCacheKeyTest, randomKeysDiffer) {
  const auto a = FileCacheKey::random();
  const auto b = FileCacheKey::random();

  EXPECT_FALSE(a == b);
}

TEST(FileCacheKeyTest, usableAsHashMapKey) {
  std::unordered_map<FileCacheKey, int> values;
  const auto key = FileCacheKey::fromPath("/x");

  values[key] = 7;

  EXPECT_EQ(values.at(FileCacheKey::fromPath("/x")), 7);
}

TEST(FileCacheKeyTest, keyAndOffsetHash) {
  const FileCacheKeyAndOffsetHash hasher;
  const auto key = FileCacheKey::fromPath("/y");
  const auto otherKey = FileCacheKey::fromPath("/z");
  constexpr size_t offset = 10;

  EXPECT_EQ(
      hasher({key, offset}),
      std::hash<FileCacheKey>()(key) ^
          std::hash<uint64_t>()(static_cast<uint64_t>(offset)));
  EXPECT_EQ(
      hasher({otherKey, offset}),
      std::hash<FileCacheKey>()(otherKey) ^
          std::hash<uint64_t>()(static_cast<uint64_t>(offset)));
  EXPECT_EQ(
      hasher({key, offset + 1}),
      std::hash<FileCacheKey>()(key) ^
          std::hash<uint64_t>()(static_cast<uint64_t>(offset + 1)));
}
