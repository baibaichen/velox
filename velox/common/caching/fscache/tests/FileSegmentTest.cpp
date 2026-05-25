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

#include "velox/common/caching/fscache/FsCacheKey.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::cache::fs::test {

using ::facebook::velox::common::testutil::TempDirectoryPath;

class FileSegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::string cacheRoot_;

  void SetUp() override {
    tempDir_ = TempDirectoryPath::create();
    cacheRoot_ = tempDir_->getPath();

    // Deterministic 8 KiB blob so reads from any offset/size below 8 KiB
    // produce verifiable bytes. 251 is prime so the mod pattern doesn't
    // align with any power-of-two read size.
    remotePath_ = cacheRoot_ + "/remote.bin";
    std::ofstream remote{remotePath_, std::ios::binary};
    for (uint64_t i = 0; i < 8'192; ++i) {
      const char c = static_cast<char>(i % 251);
      remote.write(&c, 1);
    }
  }
};

TEST_F(FileSegmentTest, freshSegmentStartsEmpty) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, 4'096};
  FileSegment segment{key, remotePath_};
  EXPECT_EQ(segment.state(), FileSegment::State::kEmpty);
  EXPECT_EQ(segment.downloadedSize(), 0);
}

TEST_F(FileSegmentTest, downloadFromLocalFileSucceeds) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, 4'096};
  FileSegment segment{key, remotePath_};

  LocalReadFile remote{remotePath_};
  ASSERT_TRUE(segment.beginDownload());
  segment.download(remote, cacheRoot_);

  EXPECT_EQ(segment.state(), FileSegment::State::kDownloaded);
  EXPECT_EQ(segment.downloadedSize(), 4'096);

  const std::string path = segment.localPath(cacheRoot_);
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(std::filesystem::file_size(path), 4'096);
}

TEST_F(FileSegmentTest, readReturnsExactBytes) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 256, 1'024};
  FileSegment segment{key, remotePath_};
  LocalReadFile remote{remotePath_};
  ASSERT_TRUE(segment.beginDownload());
  segment.download(remote, cacheRoot_);

  std::string buf(1'024, '\0');
  segment.read(0, 1'024, buf.data(), cacheRoot_);
  for (uint64_t i = 0; i < 1'024; ++i) {
    EXPECT_EQ(
        static_cast<unsigned char>(buf[i]),
        static_cast<unsigned char>((256 + i) % 251));
  }
}

TEST_F(FileSegmentTest, downloadFailureLeavesEmpty) {
  // Ask for more bytes than the remote file contains. LocalReadFile::pread
  // itself throws on a short read at EOF; the exception handler in
  // FileSegment::download is supposed to roll state back to kEmpty and
  // remove any .tmp.
  constexpr uint64_t kTooLarge = 1UL << 20;
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, kTooLarge};
  FileSegment segment{key, remotePath_};
  ASSERT_TRUE(segment.beginDownload());
  LocalReadFile remote{remotePath_};
  EXPECT_THROW(segment.download(remote, cacheRoot_), std::exception);
  EXPECT_EQ(segment.state(), FileSegment::State::kEmpty);
  EXPECT_FALSE(std::filesystem::exists(segment.localPath(cacheRoot_)));
  EXPECT_FALSE(std::filesystem::exists(segment.localPath(cacheRoot_) + ".tmp"));
}

TEST_F(FileSegmentTest, downloadAtomicityViaTmpRename) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, 4'096};
  FileSegment segment{key, remotePath_};
  LocalReadFile remote{remotePath_};
  ASSERT_TRUE(segment.beginDownload());
  segment.download(remote, cacheRoot_);
  // After a successful download, the .tmp file must have been renamed away.
  EXPECT_FALSE(std::filesystem::exists(segment.localPath(cacheRoot_) + ".tmp"));
}

TEST_F(FileSegmentTest, beginDownloadIsOneShot) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, 4'096};
  FileSegment segment{key, remotePath_};
  EXPECT_TRUE(segment.beginDownload());
  // Subsequent callers must lose the CAS race; only the first wins.
  EXPECT_FALSE(segment.beginDownload());
  EXPECT_FALSE(segment.beginDownload());
}

TEST_F(FileSegmentTest, localPathFollowsTwoLevelLayout) {
  FsCacheKey key{PathKey::fromPath(remotePath_), 0, 4'096};
  FileSegment segment{key, remotePath_};
  const std::string path = segment.localPath(cacheRoot_);
  // <cacheRoot>/<hex[0:2]>/<hex[2:4]>/<fileName> — verify the prefix and
  // that the leaf name matches the key.
  const std::string fileName = key.fileName();
  ASSERT_GE(fileName.size(), 4U);
  const std::string expected = cacheRoot_ + "/" + fileName.substr(0, 2) + "/" +
      fileName.substr(2, 2) + "/" + fileName;
  EXPECT_EQ(path, expected);
}

TEST_F(FileSegmentTest, increasePriorityMutexIsAccessible) {
  FsCacheKey key{PathKey::fromPath("/x"), 0, 4'096};
  FileSegment segment{key, "/x"};
  std::unique_lock<std::mutex> lk{
      segment.increasePriorityMutex_, std::try_to_lock};
  EXPECT_TRUE(lk.owns_lock());
}

TEST_F(FileSegmentTest, increasePriorityMutexExcludesConcurrentTryLock) {
  FsCacheKey key{PathKey::fromPath("/x"), 0, 4'096};
  FileSegment segment{key, "/x"};
  std::unique_lock<std::mutex> first{segment.increasePriorityMutex_};
  std::unique_lock<std::mutex> second{
      segment.increasePriorityMutex_, std::try_to_lock};
  EXPECT_FALSE(second.owns_lock());
}

} // namespace facebook::velox::cache::fs::test
