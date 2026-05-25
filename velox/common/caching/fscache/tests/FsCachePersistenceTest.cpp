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

#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

// Stand-in for a remote ReadFile that ASSERTS if any read is issued. Used to
// prove the warm-restart short-circuit short-circuited --- if download() ever
// touches the remote, pread() fails the test immediately rather than silently
// succeeding.
class FailIfReadCalled final : public ::facebook::velox::ReadFile {
 public:
  std::string_view pread(
      uint64_t /*offset*/,
      uint64_t /*length*/,
      void* /*buf*/,
      const ::facebook::velox::FileIoContext& /*context*/ = {}) const override {
    ADD_FAILURE() << "pread() called on FailIfReadCalled: warm-restart "
                  << "short-circuit was not taken";
    return {};
  }
  bool shouldCoalesce() const override {
    return false;
  }
  // Must report the actual file size: FsCache::getOrSet clamps the requested
  // range to size() so the warm-restart short-circuit code path can run on a
  // key whose size matches what is on disk. Returning 0 would clamp the
  // request to empty and bypass lookupOrCreate entirely.
  uint64_t size() const override {
    return 4UL * 1'024 * 1'024;
  }
  uint64_t memoryUsage() const override {
    return 0;
  }
  std::string getName() const override {
    return "FailIfReadCalled";
  }
  uint64_t getNaturalReadSize() const override {
    return 0;
  }
};

TEST(FsCachePersistenceTest, dataSurvivesRestart) {
  auto tempDir = TempDirectoryPath::create();
  const std::string remotePath = tempDir->getPath() + "/remote.bin";
  const std::string cacheRoot = tempDir->getPath() + "/cache";
  std::filesystem::create_directories(cacheRoot);
  // The first-run FsCache does not call loadFromDisk(), so the sentinel
  // would never be written. Pre-write it so the second-run loadFromDisk()
  // takes the survivor-preserving path rather than blind-clearing.
  std::ofstream{
      std::filesystem::path{cacheRoot} / kFsCacheVersionSentinelName}
      << kFsCacheCurrentVersion;
  {
    std::ofstream out{remotePath, std::ios::binary};
    const std::string blob(4UL * 1'024 * 1'024, 'z');
    out.write(blob.data(), blob.size());
  }

  FsCacheConfig cfg;
  cfg.cacheRoot = cacheRoot;
  cfg.maxBytes = 16UL * 1'024 * 1'024;

  uint64_t firstRunBytesOnDisk = 0;
  {
    FsCache cache{cfg};
    ::facebook::velox::LocalReadFile remote{remotePath};
    cache.getOrSet(remotePath, 0, 4UL * 1'024 * 1'024, remote);
    firstRunBytesOnDisk = cache.stats().bytesOnDisk;
    EXPECT_GT(firstRunBytesOnDisk, 0);
  }

  // Capture actual on-disk bytes from the first run before constructing the
  // second FsCache --- stats.bytesOnDisk starts at zero in a fresh cache so
  // we need the filesystem snapshot to compare against.
  uint64_t diskBytesBetweenRuns = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot}) {
    if (p.is_regular_file() &&
        p.path().filename() != kFsCacheVersionSentinelName) {
      diskBytesBetweenRuns += p.file_size();
    }
  }
  EXPECT_EQ(diskBytesBetweenRuns, firstRunBytesOnDisk);

  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    // Hand the second-run getOrSet a remote that ASSERTS if pread() runs.
    // This is what proves the warm-restart short-circuit in
    // FileSegment::download() is actually taken --- without it, the
    // bytesOnDisk and miss-count assertions below would also hold if
    // download() re-fetched and rename-overwrote the existing file, so
    // they alone would not distinguish "short-circuited" from "re-downloaded".
    FailIfReadCalled remote;
    const auto before = cache.stats();
    cache.getOrSet(remotePath, 0, 4UL * 1'024 * 1'024, remote);
    const auto after = cache.stats();
    // Recovery does not pre-populate metadata_ so this counts as a miss, but
    // FileSegment::download() short-circuits because the file already exists
    // with the expected size --- bytesOnDisk only reflects the recordMiss
    // accounting bump (which equals the segment size, matching what was
    // already on disk).
    EXPECT_EQ(after.misses, before.misses + 1);
    EXPECT_EQ(after.hits, before.hits);
    EXPECT_EQ(after.bytesOnDisk, diskBytesBetweenRuns);
  }
}

} // namespace facebook::velox::cache::fs::test
