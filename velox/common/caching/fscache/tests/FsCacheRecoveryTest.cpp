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

class FsCacheRecoveryTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;
  std::string remotePath_;
  FsCacheConfig config_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    std::filesystem::create_directories(cacheRoot_);

    std::ofstream out{remotePath_, std::ios::binary};
    const std::string blob(8UL * 1'024 * 1'024, 'x');
    out.write(blob.data(), blob.size());

    config_.cacheRoot = cacheRoot_;
    config_.maxBytes = 64UL * 1'024 * 1'024;
  }

  void warmCache(uint64_t offset, uint64_t size) {
    FsCache cache{config_};
    ::facebook::velox::LocalReadFile remote{remotePath_};
    cache.getOrSet(remotePath_, offset, size, remote);
  }
};

TEST_F(FsCacheRecoveryTest, tmpFilesRemovedOnLoad) {
  warmCache(0, 4'096);
  const std::string stray = cacheRoot_ + "/aa/bb/deadbeef00000000.0.1024.tmp";
  std::filesystem::create_directories(
      std::filesystem::path{stray}.parent_path());
  std::ofstream{stray} << "junk";

  // Snapshot survivor count so we can prove loadFromDisk only deletes the
  // .tmp, not the legitimate cache file warmCache() just produced.
  size_t survivorsBefore = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot_}) {
    if (p.is_regular_file() && p.path() != stray) {
      ++survivorsBefore;
    }
  }
  ASSERT_GT(survivorsBefore, 0);

  FsCache cache{config_};
  cache.loadFromDisk();
  EXPECT_FALSE(std::filesystem::exists(stray));
  size_t survivorsAfter = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot_}) {
    if (p.is_regular_file()) {
      ++survivorsAfter;
    }
  }
  EXPECT_EQ(survivorsAfter, survivorsBefore);
}

TEST_F(FsCacheRecoveryTest, sizeMismatchFilesRemoved) {
  // Synthetic file: declared size 1024 in name, actual size 500.
  const std::string fake = cacheRoot_ + "/aa/bb/deadbeef00000001.0.1024";
  std::filesystem::create_directories(
      std::filesystem::path{fake}.parent_path());
  std::ofstream out{fake, std::ios::binary};
  out << std::string(500, 'y');
  out.close();

  FsCache cache{config_};
  cache.loadFromDisk();
  EXPECT_FALSE(std::filesystem::exists(fake));
}

TEST_F(FsCacheRecoveryTest, unparsableNamesIgnored) {
  const std::string bad = cacheRoot_ + "/aa/bb/not_a_valid_name";
  std::filesystem::create_directories(
      std::filesystem::path{bad}.parent_path());
  std::ofstream{bad} << "x";

  FsCache cache{config_};
  cache.loadFromDisk();
  // Unrecognised files are left for admin tooling; metadata is empty so
  // bytesOnDisk stays zero.
  EXPECT_TRUE(std::filesystem::exists(bad));
  EXPECT_EQ(cache.stats().bytesOnDisk, 0);
}

TEST_F(FsCacheRecoveryTest, validFilesSurvive) {
  warmCache(0, 4UL * 1'024 * 1'024);
  uint64_t bytesOnDiskBefore = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot_}) {
    if (p.is_regular_file()) {
      bytesOnDiskBefore += p.file_size();
    }
  }
  EXPECT_GT(bytesOnDiskBefore, 0);

  FsCache cache{config_};
  cache.loadFromDisk();
  // Survivors are NOT pre-credited to bytesOnDisk by design --- see the
  // loadFromDisk doc-comment for why. Counter stays at zero until a
  // matching getOrSet() triggers the writer path.
  EXPECT_EQ(cache.stats().bytesOnDisk, 0);
  uint64_t bytesOnDiskAfter = 0;
  for (auto& p :
       std::filesystem::recursive_directory_iterator{cacheRoot_}) {
    if (p.is_regular_file()) {
      bytesOnDiskAfter += p.file_size();
    }
  }
  EXPECT_EQ(bytesOnDiskAfter, bytesOnDiskBefore);
}

} // namespace facebook::velox::cache::fs::test
