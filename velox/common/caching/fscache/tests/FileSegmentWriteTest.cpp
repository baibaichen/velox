/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentWriteTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentWriteTest, reserveTransitionsEmptyToDownloading) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.state(), FileSegment::State::kEmpty);
  ASSERT_EQ(seg.reserve(1024, cacheRoot_), FileSegment::ReserveResult::kReserved);
  EXPECT_EQ(seg.state(), FileSegment::State::kDownloading);
  EXPECT_EQ(seg.getDownloader(), std::this_thread::get_id());
}

TEST_F(FileSegmentWriteTest, writeThenCompleteProducesFullFile) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 8};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.reserve(8, cacheRoot_), FileSegment::ReserveResult::kReserved);
  const char payload[] = "ABCDEFGH";
  seg.write(payload, 8);
  seg.complete();
  EXPECT_EQ(seg.state(), FileSegment::State::kDownloaded);
  std::ifstream in{seg.localPath(cacheRoot_), std::ios::binary};
  std::string disk((std::istreambuf_iterator<char>(in)), {});
  EXPECT_EQ(disk, std::string(payload, 8));
}

TEST_F(FileSegmentWriteTest, abandonLeavesPartialFileWithoutFtruncate) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.reserve(1024, cacheRoot_), FileSegment::ReserveResult::kReserved);
  std::string chunk(256, 'A');
  seg.write(chunk.data(), chunk.size());
  seg.abandon();
  EXPECT_EQ(seg.state(), FileSegment::State::kPartiallyDownloaded);
  // stat_size must equal what was actually pwritten (256), NOT claimed 1024.
  // This is what lets loadFromDisk recognise & delete partials (spec §5.5).
  EXPECT_EQ(std::filesystem::file_size(seg.localPath(cacheRoot_)), 256UL);
}

TEST_F(FileSegmentWriteTest, reserveRejectsConcurrentWriter) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 1024};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.reserve(1024, cacheRoot_), FileSegment::ReserveResult::kReserved);
  EXPECT_EQ(seg.reserve(1024, cacheRoot_), FileSegment::ReserveResult::kLostRace);
}

TEST_F(FileSegmentWriteTest, writeBeyondReservedSizeThrows) {
  FsCacheKey key{PathKey::fromPath("/remote/x"), 0, 8};
  FileSegment seg{key, "/remote/x"};
  ASSERT_EQ(seg.reserve(8, cacheRoot_), FileSegment::ReserveResult::kReserved);
  std::string oversized(16, 'A');
  EXPECT_THROW(
      seg.write(oversized.data(), oversized.size()),
      ::facebook::velox::VeloxException);
}

} // namespace facebook::velox::cache::fs::test
