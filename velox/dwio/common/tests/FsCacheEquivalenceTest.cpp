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
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/FsCacheBufferedInput.h"
#include "velox/dwio/common/Options.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::dwio::common::test {

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::common::testutil::TempDirectoryPath;

// Phase-1 correctness gate: prove FsCacheBufferedInput's read path returns
// exactly the source file's canonical bytes for the workloads connectors
// drive in practice (one big region, many small regions in a single load,
// and re-reading the same range across BufferedInput lifetimes). The
// reference is the in-memory copy of the file contents --- by Velox
// invariant CachedBufferedInput must also return these bytes, so comparing
// against the canonical bytes directly is equivalent to comparing against
// CachedBufferedInput while avoiding the AsyncDataCache + StringIdLease +
// ScanTracker setup that would just confirm the same ground truth.
class FsCacheEquivalenceTest : public ::testing::Test {
 protected:
  std::shared_ptr<TempDirectoryPath> tempDir_;
  std::string remotePath_;
  std::unique_ptr<FsCache> fsCache_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::string content_;

  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
    pool_ = memory::memoryManager()->addLeafPool("FsCacheEquivalenceTest");

    tempDir_ = TempDirectoryPath::create();
    remotePath_ = tempDir_->getPath() + "/remote.bin";
    content_.resize(16UL * 1'024 * 1'024);
    // Non-trivial byte pattern so off-by-one shifts and zero-fill bugs are
    // both detectable. The (7*i + 13) mod 256 sequence is dense enough that
    // any 1-byte misalignment fails the EXPECT_EQ.
    for (size_t i = 0; i < content_.size(); ++i) {
      content_[i] = static_cast<char>((i * 7 + 13) % 256);
    }
    std::ofstream out{remotePath_, std::ios::binary};
    out.write(content_.data(), content_.size());

    FsCacheConfig cfg;
    cfg.cacheRoot = tempDir_->getPath() + "/cache";
    cfg.maxBytes = 64UL * 1'024 * 1'024;
    std::filesystem::create_directories(cfg.cacheRoot);
    fsCache_ = std::make_unique<FsCache>(cfg);
  }

  // Reads exactly size bytes from stream, looping over Next() chunks.
  static std::string drain(SeekableInputStream& stream, size_t size) {
    std::string buf(size, '\0');
    size_t copied = 0;
    const void* data;
    int32_t len;
    while (copied < size && stream.Next(&data, &len)) {
      const size_t toCopy =
          std::min<size_t>(static_cast<size_t>(len), size - copied);
      std::memcpy(buf.data() + copied, data, toCopy);
      copied += toCopy;
    }
    buf.resize(copied);
    return buf;
  }
};

TEST_F(FsCacheEquivalenceTest, singleRegionByteForByte) {
  const uint64_t offset = 1'234;
  const uint64_t length = 8UL * 1'024 * 1'024 - offset;
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
  auto stream = input.enqueue({offset, length});
  input.load(LogType::FILE);
  EXPECT_EQ(drain(*stream, length), content_.substr(offset, length));
}

// Five requests of varied offset alignments and sizes issued in one load
// batch. Offsets exercise both alignment boundaries (0, 4'096, 1 MiB) and
// mid-segment positions (3 MiB + 17); sizes range from 1 KiB to 2 MiB.
// All five requests fall within two outer segments under the default
// config, so this primarily proves intra-segment slicing and per-request
// stream isolation; the cross-segment span case is covered by the sibling
// `rangeSpansMultipleSegments` in FsCacheBufferedInputTest.
TEST_F(FsCacheEquivalenceTest, variedRegionsInOneLoadByteForByte) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};

  struct Request {
    uint64_t offset;
    uint64_t length;
  };
  const std::vector<Request> requests{
      {0, 4'096},
      {4'096, 4'096},
      {1UL * 1'024 * 1'024, 64 * 1'024},
      {3UL * 1'024 * 1'024 + 17, 1'024},
      {7UL * 1'024 * 1'024, 2UL * 1'024 * 1'024},
  };
  std::vector<std::unique_ptr<SeekableInputStream>> streams;
  streams.reserve(requests.size());
  for (const auto& r : requests) {
    streams.push_back(input.enqueue({r.offset, r.length}));
  }
  input.load(LogType::FILE);
  for (size_t i = 0; i < requests.size(); ++i) {
    const auto& r = requests[i];
    EXPECT_EQ(drain(*streams[i], r.length), content_.substr(r.offset, r.length))
        << "request " << i << " offset=" << r.offset << " length=" << r.length;
  }
}

// Re-reading the same range across fresh BufferedInput instances must return
// identical bytes. The second and third rounds also exercise the warm-cache
// path (segment already kDownloaded), so this catches regressions where a
// hit-path read could return a different prefix than a miss-path read.
TEST_F(FsCacheEquivalenceTest, repeatedReadConsistent) {
  auto readFile = std::make_shared<LocalReadFile>(remotePath_);
  const uint64_t length = 4UL * 1'024 * 1'024;
  for (int round = 0; round < 3; ++round) {
    FsCacheBufferedInput input{readFile, *pool_, fsCache_.get()};
    auto stream = input.enqueue({0, length});
    input.load(LogType::FILE);
    EXPECT_EQ(drain(*stream, length), content_.substr(0, length))
        << "round " << round;
  }
}

} // namespace facebook::velox::dwio::common::test
