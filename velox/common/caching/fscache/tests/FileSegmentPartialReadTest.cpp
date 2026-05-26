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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentPartialReadTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

TEST_F(FileSegmentPartialReadTest, readerWakesAfterEnoughBytesWritten) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(256);
    readerDone.store(true);
  }};

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  std::string chunk(64, 'A');
  seg.write(chunk.data(), chunk.size());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  seg.write(chunk.data(), chunk.size());
  seg.write(chunk.data(), chunk.size());
  seg.write(chunk.data(), chunk.size());
  reader.join();
  EXPECT_TRUE(readerDone.load());

  seg.complete();
}

TEST_F(FileSegmentPartialReadTest, readerWokenByCompleteWhenNeededEqualsSize) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 128};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(128, cacheRoot_));

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(128);
    readerDone.store(true);
  }};

  std::string payload(128, 'B');
  seg.write(payload.data(), payload.size());
  seg.complete();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

TEST_F(FileSegmentPartialReadTest, readerThrowsWhenWriterAbandonsShortOfNeeded) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::thread reader{[&]() {
    EXPECT_THROW(
        seg.waitForDownloadedSize(512),
        ::facebook::velox::VeloxException);
  }};

  std::string chunk(128, 'C');
  seg.write(chunk.data(), chunk.size());
  seg.abandon();
  reader.join();
}

TEST_F(FileSegmentPartialReadTest, readerWokenByAbandonWhenDownloadedExceedsNeeded) {
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, 1024};
  FileSegment seg{key, "/r/x"};
  ASSERT_TRUE(seg.reserve(1024, cacheRoot_));

  std::string chunk(256, 'D');
  seg.write(chunk.data(), chunk.size());
  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    seg.waitForDownloadedSize(128);
    readerDone.store(true);
  }};
  seg.abandon();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

} // namespace facebook::velox::cache::fs::test
