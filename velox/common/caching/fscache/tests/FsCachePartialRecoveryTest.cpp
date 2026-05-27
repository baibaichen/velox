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

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

TEST(FsCachePartialRecoveryTest, abandonedPartialDeletedOnRestart) {
  auto tempDir = TempDirectoryPath::create();
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir->getPath() + "/cache";
  cfg.maxBytes = 64UL * 1'024 * 1'024;
  std::filesystem::create_directories(cfg.cacheRoot);

  std::filesystem::path partialPath;
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    FsCacheKey key{PathKey::fromPath("/remote/abandoned"), 0, 1UL << 20};
    FileSegment seg{key, "/remote/abandoned"};
    ASSERT_EQ(seg.reserve(1UL << 20, cfg.cacheRoot), FileSegment::ReserveResult::kReserved);
    std::string chunk(256UL << 10, 'X');
    seg.write(chunk.data(), chunk.size());
    seg.abandon();
    partialPath = seg.localPath(cfg.cacheRoot);
    ASSERT_TRUE(std::filesystem::exists(partialPath));
    ASSERT_EQ(std::filesystem::file_size(partialPath), 256UL << 10);
  }

  // Fresh FsCache reloads; partial must be deleted because stat_size
  // (256 KiB) does not match parsed key size (1 MiB).
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    EXPECT_FALSE(std::filesystem::exists(partialPath));
  }
}

TEST(FsCachePartialRecoveryTest, completedSegmentSurvivesRestart) {
  auto tempDir = TempDirectoryPath::create();
  FsCacheConfig cfg;
  cfg.cacheRoot = tempDir->getPath() + "/cache";
  cfg.maxBytes = 64UL * 1'024 * 1'024;
  std::filesystem::create_directories(cfg.cacheRoot);

  std::filesystem::path completedPath;
  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    FsCacheKey key{PathKey::fromPath("/remote/full"), 0, 4'096};
    FileSegment seg{key, "/remote/full"};
    ASSERT_EQ(seg.reserve(4'096, cfg.cacheRoot), FileSegment::ReserveResult::kReserved);
    std::string payload(4'096, 'Y');
    seg.write(payload.data(), payload.size());
    seg.complete();
    completedPath = seg.localPath(cfg.cacheRoot);
    ASSERT_EQ(std::filesystem::file_size(completedPath), 4'096UL);
  }

  {
    FsCache cache{cfg};
    cache.loadFromDisk();
    EXPECT_TRUE(std::filesystem::exists(completedPath));
    EXPECT_EQ(std::filesystem::file_size(completedPath), 4'096UL);
  }
}

} // namespace facebook::velox::cache::fs::test
