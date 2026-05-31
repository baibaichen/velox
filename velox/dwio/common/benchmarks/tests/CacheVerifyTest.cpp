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

#include "velox/dwio/common/benchmarks/CacheVerify.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

#include <gtest/gtest.h>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"

namespace facebook::velox::dwio::common::bench {
namespace {

constexpr uint64_t kKiB = 1024;

// Writes `bytes` of deterministic content to `path`; byte i = (i*seed) mod 251
// so two files with different seeds differ at every position.
void writeFile(const std::string& path, uint64_t bytes, uint8_t seed) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  for (uint64_t i = 0; i < bytes; ++i) {
    const char byte = static_cast<char>((i * seed + 7) % 251);
    out.write(&byte, 1);
  }
}

class CacheVerifyTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    filesystems::registerLocalFileSystem();
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    }
  }

  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
        ("velox_cache_verify_test_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::string path(const std::string& name) const {
    return (dir_ / name).string();
  }

  std::filesystem::path dir_;
};

// A dbi harness reading the source it was built from, verified against that same
// source, passes: every segment matches and the working set is fully covered.
TEST_F(CacheVerifyTest, matchingSourcePasses) {
  const uint64_t bytes = 8 * kKiB;
  const std::string file = path("data.bin");
  writeFile(file, bytes, /*seed=*/13);
  const std::vector<SourceFile> sources{{file, bytes}};

  HarnessConfig config;
  DbiHarness harness(config, sources);
  const DataLayout layout{sources, kKiB, /*maxBytes=*/0};

  CacheVerifier verifier(sources);
  const auto result =
      verifier.verifyWorkingSet(harness, layout, kKiB, /*batch=*/4);

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.segmentsChecked, 8u);
  EXPECT_EQ(result.bytesChecked, bytes);
  EXPECT_EQ(result.mismatches, 0u);
}

// When the verifier's reference source differs from what the harness reads, the
// mismatch is detected and the first offending segment is reported. This is the
// corruption-detection path the gate relies on.
TEST_F(CacheVerifyTest, mismatchingSourceIsDetected) {
  const uint64_t bytes = 8 * kKiB;
  const std::string served = path("served.bin");
  const std::string reference = path("reference.bin");
  // Same size, every byte differs (different seed) -> first segment mismatches.
  writeFile(served, bytes, /*seed=*/13);
  writeFile(reference, bytes, /*seed=*/29);

  const std::vector<SourceFile> served_set{{served, bytes}};
  const std::vector<SourceFile> reference_set{{reference, bytes}};

  HarnessConfig config;
  DbiHarness harness(config, served_set);
  const DataLayout layout{served_set, kKiB, /*maxBytes=*/0};

  CacheVerifier verifier(reference_set);
  const auto result =
      verifier.verifyWorkingSet(harness, layout, kKiB, /*batch=*/4);

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.segmentsChecked, 8u);
  EXPECT_GT(result.mismatches, 0u);
  EXPECT_EQ(result.firstMismatchFileIdx, 0u);
  EXPECT_EQ(result.firstMismatchOffset, 0u);
}

// A harness that returns fewer bytes than requested for each segment, used to
// lock the truncation-detection path: a short cache read must fail verification
// instead of having its prefix silently match the source.
struct ShortReadHarness {
  std::string payload; // returned for every segment (shorter than readSize)

  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& /*ioStats*/,
      const StreamConsumer& consume) {
    for (const auto& [fileIdx, offsets] : byFile) {
      for (const auto offset : offsets) {
        SeekableArrayInputStream stream(payload.data(), payload.size());
        consume(stream, readSize, fileIdx, offset);
      }
    }
  }
};

// A wrapper that serves a truncated segment (fewer bytes than readSize) must be
// reported as a mismatch even when the bytes it does return match the source
// prefix.
TEST_F(CacheVerifyTest, truncatedReadIsDetected) {
  const uint64_t bytes = 2 * kKiB;
  const std::string file = path("data.bin");
  writeFile(file, bytes, /*seed=*/13);
  const std::vector<SourceFile> sources{{file, bytes}};

  // The harness returns only the first half of each requested 1 KiB block; those
  // bytes match the source prefix, so only the length check can catch it.
  ShortReadHarness harness;
  const auto reader = std::make_shared<LocalReadFile>(file);
  const std::string fullBlock =
      static_cast<ReadFile&>(*reader).pread(0, kKiB);
  harness.payload = fullBlock.substr(0, kKiB / 2);

  const DataLayout layout{sources, kKiB, /*maxBytes=*/0};
  CacheVerifier verifier(sources);
  const auto result =
      verifier.verifyWorkingSet(harness, layout, kKiB, /*batch=*/4);

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.segmentsChecked, 2u);
  EXPECT_EQ(result.mismatches, 2u);
}

} // namespace
} // namespace facebook::velox::dwio::common::bench
