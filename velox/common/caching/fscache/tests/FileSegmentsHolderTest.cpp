/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegmentsHolder.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentsHolderTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentsHolderTest, emptyHolderDestructsCleanly) {
  FileSegmentsHolder holder{{}};
  EXPECT_TRUE(holder.empty());
  EXPECT_TRUE(holder.segments().empty());
}

TEST_F(FileSegmentsHolderTest, emptyReturnsFalseWhenSegmentsHeld) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();
  FileSegmentsHolder holder{{seg}};
  EXPECT_FALSE(holder.empty());
}

TEST_F(FileSegmentsHolderTest, kDownloadedSegmentSurvivesDestructor) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloaded);
  {
    FileSegmentsHolder holder{{seg}};
  }
  // Destructor is a no-op for kDownloaded segments.
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
}

TEST_F(FileSegmentsHolderTest, holderAbandonsDownloadingSegmentOwnedByThisThread) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(1024, cacheRoot_));
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloading);
  ASSERT_EQ(seg->getDownloader(), std::this_thread::get_id());
  {
    FileSegmentsHolder holder{{seg}};
  }
  EXPECT_EQ(seg->state(), FileSegment::State::kPartiallyDownloaded);
}

TEST_F(FileSegmentsHolderTest, holderDoesNotAbandonSegmentOwnedByOtherThread) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");

  std::thread other{[&]() {
    ASSERT_TRUE(seg->reserve(1024, cacheRoot_));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    seg->complete();
  }};
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  // Segment is kDownloading, but downloader_ is `other` thread.
  ASSERT_EQ(seg->state(), FileSegment::State::kDownloading);
  ASSERT_NE(seg->getDownloader(), std::this_thread::get_id());

  {
    FileSegmentsHolder holder{{seg}};
  }
  // We did NOT touch state because downloader is not this thread.
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloading);

  // Note: writing 0 bytes is fine because complete() ftruncates to declared
  // size; the segment ends at kDownloaded with all-zero contents.
  other.join();
  EXPECT_EQ(seg->state(), FileSegment::State::kDownloaded);
}

TEST_F(FileSegmentsHolderTest, moveSemantics) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 8};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_TRUE(seg->reserve(8, cacheRoot_));
  std::string payload(8, 'A');
  seg->write(payload.data(), payload.size());
  seg->complete();

  FileSegmentsHolder a{{seg}};
  FileSegmentsHolder b{std::move(a)};
  EXPECT_EQ(b.segments().size(), 1UL);
  EXPECT_TRUE(a.segments().empty());
}

} // namespace facebook::velox::cache::fs::test
