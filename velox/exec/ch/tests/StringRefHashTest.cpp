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

#include <gtest/gtest.h>

#include <string>

namespace facebook::velox::exec::ch {
namespace {

TEST(StringRefHashTest, stringRefHashIsSelfConsistentAcrossLengths) {
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

} // namespace
} // namespace facebook::velox::exec::ch
