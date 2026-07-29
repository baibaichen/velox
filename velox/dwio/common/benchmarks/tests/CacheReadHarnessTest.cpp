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

#include <filesystem>
#include <fstream>
#include <string>

#include <atomic>
#include <unistd.h>

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::dwio::common::bench {
namespace {

namespace fs = std::filesystem;

// Creates a unique, deterministic scratch directory name for the cache-root
// clear tests. Keeping the name unique per test avoids cross-test interference
// and makes cleanup independent.
std::string uniqueName(const std::string& tag) {
  static std::atomic<uint64_t> counter{0};
  return "clearcacheroot_" + tag + "_" +
      std::to_string(static_cast<uint64_t>(::getpid())) + "_" +
      std::to_string(counter.fetch_add(1));
}

// Writes a small file, creating parent directories as needed.
void writeFile(const fs::path& path, const std::string& content) {
  fs::create_directories(path.parent_path());
  std::ofstream out{path, std::ios::binary | std::ios::trunc};
  out << content;
}

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

// A cache root strictly under the current working directory's tmp/ subtree may
// be cleared without a sentinel, and its payload is fully removed.
TEST(ClearCacheRootTest, tmpSubtreeClearsWithoutSentinel) {
  const fs::path root = fs::current_path() / "tmp" / uniqueName("tmpchild");
  writeFile(root / "payload.bin", "cached-bytes");
  writeFile(root / "nested" / "more.bin", "more");
  ASSERT_TRUE(fs::exists(root / "payload.bin"));

  clearBenchmarkCacheRoot(root.string());

  // The whole root is removed (no sentinel to preserve).
  EXPECT_FALSE(fs::exists(root));

  std::error_code ec;
  fs::remove_all(root, ec);
}

// A cache root outside the cwd/tmp subtree with no sentinel is rejected before
// anything is deleted; its payload must survive.
TEST(ClearCacheRootTest, externalRootWithoutSentinelIsRejected) {
  // Sibling of tmp/ under cwd -> not in the exempt tmp/ subtree -> "external".
  const fs::path root = fs::current_path() / uniqueName("external");
  const fs::path payload = root / "payload.bin";
  writeFile(payload, "must-survive");
  ASSERT_TRUE(fs::exists(payload));

  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot(root.string()), "without the sentinel file");

  // Nothing was deleted.
  EXPECT_TRUE(fs::exists(payload));

  std::error_code ec;
  fs::remove_all(root, ec);
}

// A sentineled cache root (even outside the tmp/ subtree) clears its payload but
// preserves the sentinel so an 018-D trap can still authenticate the directory.
TEST(ClearCacheRootTest, sentineledRootClearsPayloadPreservesSentinel) {
  const fs::path root = fs::current_path() / uniqueName("sentineled");
  const fs::path sentinel = root / kCacheSentinelName;
  writeFile(sentinel, "");
  writeFile(root / "payload.bin", "cached-bytes");
  writeFile(root / "nested" / "more.bin", "more");
  ASSERT_TRUE(fs::exists(sentinel));
  ASSERT_TRUE(fs::exists(root / "payload.bin"));

  clearBenchmarkCacheRoot(root.string());

  // Payload gone, sentinel and root preserved.
  EXPECT_TRUE(fs::exists(root));
  EXPECT_TRUE(fs::exists(sentinel));
  EXPECT_FALSE(fs::exists(root / "payload.bin"));
  EXPECT_FALSE(fs::exists(root / "nested"));

  std::error_code ec;
  fs::remove_all(root, ec);
}

// Dangerous roots are rejected outright: empty, the filesystem root, the current
// working directory, and the cwd/tmp parent itself. Crucially, trailing-
// separator and dot spellings ("tmp/", "tmp//", "tmp/.", "./tmp/", "./", and an
// absolute cwd/tmp with a trailing slash) must normalize to the same protected
// path and be rejected identically -- otherwise libstdc++'s lexically_normal
// trailing empty filename would let them slip past as a "strict child" and wipe
// the shared cwd or cwd/tmp parent. To prove no destructive call can reach the
// shared cwd/tmp directory, a payload placed directly under cwd/tmp must survive
// every rejected call.
TEST(ClearCacheRootTest, dangerousRootsAreRejected) {
  // Guard payload directly under the shared cwd/tmp parent. If any spelling
  // below were mistreated as a strict child of cwd/tmp and cleared, this file
  // (a direct child of cwd/tmp) would be deleted.
  const fs::path guard =
      fs::current_path() / "tmp" / (uniqueName("guard") + ".bin");
  writeFile(guard, "must-survive-every-dangerous-call");
  ASSERT_TRUE(fs::exists(guard));

  VELOX_ASSERT_THROW(clearBenchmarkCacheRoot(""), "empty root");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("/"), "refusing to clear the filesystem root");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot(fs::current_path().string()),
      "refusing to clear the current working directory");
  // The cwd itself via a "./" and a "." spelling -> must resolve to cwd.
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("./"),
      "refusing to clear the current working directory");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("."),
      "refusing to clear the current working directory");
  // The cwd/tmp parent via every trailing-separator / dot spelling.
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot((fs::current_path() / "tmp").string()),
      "refusing to clear the tmp/ parent directory");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("tmp/"),
      "refusing to clear the tmp/ parent directory");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("tmp//"),
      "refusing to clear the tmp/ parent directory");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("tmp/."),
      "refusing to clear the tmp/ parent directory");
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot("./tmp/"),
      "refusing to clear the tmp/ parent directory");
  // Absolute cwd/tmp with a trailing slash must also normalize to the parent.
  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot((fs::current_path() / "tmp").string() + "/"),
      "refusing to clear the tmp/ parent directory");

  // The payload under the shared cwd/tmp survived every rejected call.
  EXPECT_TRUE(fs::exists(guard));

  std::error_code ec;
  fs::remove(guard, ec);
}

// A sentinel entry that is NOT a regular file (here: a directory named exactly
// kCacheSentinelName) must not authorize a destructive clear of an external
// root. The forged directory-sentinel is authenticated by symlink status, so it
// is rejected and the payload survives.
TEST(ClearCacheRootTest, directorySentinelIsRejected) {
  const fs::path root = fs::current_path() / uniqueName("dirsentinel");
  const fs::path forgedSentinel = root / kCacheSentinelName;
  const fs::path payload = root / "payload.bin";
  fs::create_directories(forgedSentinel); // sentinel name, but a directory
  writeFile(payload, "must-survive");
  ASSERT_TRUE(fs::is_directory(forgedSentinel));
  ASSERT_TRUE(fs::exists(payload));

  VELOX_ASSERT_THROW(
      clearBenchmarkCacheRoot(root.string()), "without the sentinel file");

  // Nothing was deleted: the forged directory-sentinel did not authenticate.
  EXPECT_TRUE(fs::exists(payload));
  EXPECT_TRUE(fs::is_directory(forgedSentinel));

  std::error_code ec;
  fs::remove_all(root, ec);
}

TEST(ReuseCacheRootTest, tmpPayloadIsAcceptedWithoutMutation)
{
    const fs::path root =
        fs::current_path() / "tmp" / uniqueName("reuse-valid");
    const fs::path payload = root / "regular" / "segment.bin";
    writeFile(payload, "cached-bytes");

    const auto normalized = validateBenchmarkCacheRootForReuse(root.string());

    EXPECT_EQ(normalized, fs::absolute(root).lexically_normal().string());
    EXPECT_TRUE(fs::exists(payload));
    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST(ReuseCacheRootTest, missingEmptyAndStatusOnlyRootsAreRejected)
{
    const fs::path missing =
        fs::current_path() / "tmp" / uniqueName("reuse-missing");
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(missing.string()),
        "does not exist");

    const fs::path empty =
        fs::current_path() / "tmp" / uniqueName("reuse-empty");
    fs::create_directories(empty);
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(empty.string()),
        "no cache payload");

    writeFile(empty / "status", "stale status");
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(empty.string()),
        "no cache payload");
    std::error_code ec;
    fs::remove_all(empty, ec);
}

TEST(ReuseCacheRootTest, externalRootRequiresRegularSentinel)
{
    const fs::path root = fs::current_path() / uniqueName("reuse-external");
    writeFile(root / "regular" / "segment.bin", "cached-bytes");
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(root.string()),
        "without the sentinel file");

    writeFile(root / kCacheSentinelName, "");
    EXPECT_NO_THROW(validateBenchmarkCacheRootForReuse(root.string()));
    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST(ReuseCacheRootTest, dangerousAndNonDirectoryRootsAreRejected)
{
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse("/"),
        "filesystem root");
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(fs::current_path().string()),
        "current working directory");
    VELOX_ASSERT_THROW(
        validateBenchmarkCacheRootForReuse(
            (fs::current_path() / "tmp").string()),
        "tmp/ parent directory");

    const fs::path file =
        fs::current_path() / "tmp" / uniqueName("reuse-file");
    writeFile(file, "not-a-directory");
    EXPECT_THROW(
        validateBenchmarkCacheRootForReuse(file.string()),
        VeloxUserError);
    std::error_code ec;
    fs::remove(file, ec);
}

} // namespace
} // namespace facebook::velox::dwio::common::bench
