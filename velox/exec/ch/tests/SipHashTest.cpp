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

#include "velox/exec/ch/Common/SipHash.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace facebook::velox::exec::ch {
namespace {

TEST(SipHashTest, matchesClickHouseKnownVectorsByteForByte) {
  struct TestCase {
    std::string_view input;
    std::array<uint8_t, 16> expected;
  };

  const std::array<TestCase, 2> cases{{
      {"",
       {0x32,
        0xb5,
        0xc1,
        0xdb,
        0x56,
        0xa6,
        0x83,
        0xe9,
        0xe5,
        0xb5,
        0xb6,
        0xa8,
        0xcb,
        0xed,
        0x11,
        0xf7}},
      {"hello",
       {0x54,
        0xf0,
        0xc4,
        0x90,
        0x05,
        0x81,
        0xe0,
        0x97,
        0xed,
        0xa2,
        0x33,
        0x22,
        0x58,
        0xdc,
        0x21,
        0x1b}},
  }};

  for (const auto& test : cases) {
    SipHash hash;
    hash.update(test.input);
    const auto actual = hash.get128();
    EXPECT_EQ(
        std::memcmp(&actual, test.expected.data(), test.expected.size()), 0);
  }
}

struct ByteSwapTransform {
  uint64_t operator()(uint64_t value) const {
    return __builtin_bswap64(value);
  }
};

TEST(SipHashTest, preservesClickHouseTransformTemplateApi) {
  constexpr uint64_t value = 0x0102030405060708ULL;
  SipHash baseline;
  baseline.update(value);

  SipHash transformed;
  transformed.update<ByteSwapTransform>(value);

  EXPECT_EQ(transformed.get128(), baseline.get128());
}

} // namespace
} // namespace facebook::velox::exec::ch
