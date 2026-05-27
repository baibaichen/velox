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

#include "velox/dwio/common/FsCacheInputStream.h"

#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FileSegment;
using ::facebook::velox::cache::fs::FileSegmentPtr;
using ::facebook::velox::cache::fs::FsCacheKey;
using ::facebook::velox::cache::fs::PathKey;
using ::facebook::velox::common::testutil::TempDirectoryPath;

class FsCacheInputStreamTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath() + "/cache";
    std::filesystem::create_directories(cacheRoot_);
  }
};

// Reader entering Next() while the writer is mid-download must block in
// loadCurrentSegmentBuffer() until the writer publishes enough bytes (or
// transitions out of kDownloading). Without the wait, FileSegment::read()
// would throw because state != kDownloaded.
TEST_F(FsCacheInputStreamTest, readerBlocksUntilWriterCompletesDownloadingSegment) {
  constexpr uint64_t kSize = 1024;
  FsCacheKey key{PathKey::fromPath("/r/x"), 0, kSize};
  auto seg = std::make_shared<FileSegment>(key, "/r/x");
  ASSERT_EQ(seg->reserve(kSize, cacheRoot_), FileSegment::ReserveResult::kReserved);
  std::vector<FileSegmentPtr> segs{seg};

  std::atomic<bool> readerDone{false};
  std::thread reader{[&]() {
    FsCacheInputStream stream{segs, 0, kSize, cacheRoot_};
    const void* data;
    int32_t len;
    while (stream.Next(&data, &len)) {
    }
    readerDone.store(true);
  }};

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(readerDone.load());

  std::string payload(kSize, 'Z');
  seg->write(payload.data(), payload.size());
  seg->complete();
  reader.join();
  EXPECT_TRUE(readerDone.load());
}

} // namespace facebook::velox::dwio::common::test
