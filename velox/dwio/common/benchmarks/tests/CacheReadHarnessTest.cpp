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

#include "velox/dwio/common/benchmarks/CacheReadHarness.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::dwio::common::bench {
namespace {

constexpr uint64_t kKiB = 1024;

TEST(DataLayoutTest, singleFileBlocks) {
  // 4 KiB file, 1 KiB reads -> 4 blocks, all in file 0.
  const std::vector<SourceFile> files{{"f0", 4 * kKiB}};
  const DataLayout layout{files, kKiB, /*maxBytes=*/0};
  ASSERT_EQ(layout.totalKeys(), 4);
  for (uint64_t k = 0; k < 4; ++k) {
    const auto loc = layout.resolve(k);
    EXPECT_EQ(loc.fileIdx, 0u);
    EXPECT_EQ(loc.offset, k * kKiB);
  }
}

TEST(DataLayoutTest, multiFileContiguousMapping) {
  // f0: 1 block, f1: 1 block -> key 0 in f0, key 1 in f1.
  const std::vector<SourceFile> files{{"f0", kKiB}, {"f1", kKiB}};
  const DataLayout layout{files, kKiB, /*maxBytes=*/0};
  ASSERT_EQ(layout.totalKeys(), 2);
  EXPECT_EQ(layout.resolve(0).fileIdx, 0u);
  EXPECT_EQ(layout.resolve(0).offset, 0u);
  EXPECT_EQ(layout.resolve(1).fileIdx, 1u);
  EXPECT_EQ(layout.resolve(1).offset, 0u);
}

// Locks the fileIndices_ fix: a leading file that contributes zero blocks (it
// is smaller than the read size) must NOT shift the resolved index of the
// later files. resolve() must return the ORIGINAL source-file index.
TEST(DataLayoutTest, skippedLeadingFileKeepsOriginalIndex) {
  const std::vector<SourceFile> files{
      {"f0", 512}, // 0 blocks (< readSize), skipped
      {"f1", 2 * kKiB}, // 2 blocks
      {"f2", kKiB}}; // 1 block
  const DataLayout layout{files, kKiB, /*maxBytes=*/0};
  ASSERT_EQ(layout.totalKeys(), 3);

  // Keys 0,1 belong to the original file index 1, not 0.
  EXPECT_EQ(layout.resolve(0).fileIdx, 1u);
  EXPECT_EQ(layout.resolve(0).offset, 0u);
  EXPECT_EQ(layout.resolve(1).fileIdx, 1u);
  EXPECT_EQ(layout.resolve(1).offset, kKiB);
  // Key 2 belongs to the original file index 2.
  EXPECT_EQ(layout.resolve(2).fileIdx, 2u);
  EXPECT_EQ(layout.resolve(2).offset, 0u);
}

TEST(DataLayoutTest, skippedMiddleFileKeepsOriginalIndex) {
  const std::vector<SourceFile> files{
      {"f0", kKiB}, // 1 block
      {"f1", 100}, // 0 blocks, skipped
      {"f2", kKiB}}; // 1 block
  const DataLayout layout{files, kKiB, /*maxBytes=*/0};
  ASSERT_EQ(layout.totalKeys(), 2);
  EXPECT_EQ(layout.resolve(0).fileIdx, 0u);
  EXPECT_EQ(layout.resolve(1).fileIdx, 2u);
}

TEST(DataLayoutTest, maxBytesCaps) {
  // 4 blocks available but cap to 2 blocks.
  const std::vector<SourceFile> files{{"f0", 4 * kKiB}};
  const DataLayout layout{files, kKiB, /*maxBytes=*/2 * kKiB};
  ASSERT_EQ(layout.totalKeys(), 2);
  EXPECT_EQ(layout.resolve(1).fileIdx, 0u);
  EXPECT_EQ(layout.resolve(1).offset, kKiB);
}

TEST(DataLayoutTest, maxBytesCapSpansFiles) {
  // Cap of 3 blocks: 2 from f0, 1 from f1.
  const std::vector<SourceFile> files{{"f0", 2 * kKiB}, {"f1", 2 * kKiB}};
  const DataLayout layout{files, kKiB, /*maxBytes=*/3 * kKiB};
  ASSERT_EQ(layout.totalKeys(), 3);
  EXPECT_EQ(layout.resolve(2).fileIdx, 1u);
  EXPECT_EQ(layout.resolve(2).offset, 0u);
}

TEST(DataLayoutTest, noReadableBlocksThrows) {
  const std::vector<SourceFile> files{{"f0", 512}};
  VELOX_ASSERT_THROW(
      (DataLayout{files, kKiB, /*maxBytes=*/0}), "No readable blocks");
}

} // namespace
} // namespace facebook::velox::dwio::common::bench
