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

#include "velox/common/caching/fscache/KeyMetadata.h"

#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace facebook::velox::cache::fs::test {

namespace {
FileSegmentPtr
makeSegment(std::string_view path, uint64_t offset, uint64_t size) {
  FsCacheKey key{PathKey::fromPath(path), offset, size};
  return std::make_shared<FileSegment>(key, std::string{path});
}
} // namespace

TEST(KeyMetadataTest, lockedKeyOwnsMutex) {
  KeyMetadata meta;
  auto locked = meta.lock();
  EXPECT_NE(locked.get(), nullptr);
}

TEST(KeyMetadataTest, segmentsMapStoresByOffset) {
  KeyMetadata meta;
  auto locked = meta.lock();
  locked->segments.emplace(0, makeSegment("/x", 0, 4'096));
  locked->segments.emplace(4'096, makeSegment("/x", 4'096, 4'096));
  ++locked->numSegments;
  ++locked->numSegments;
  EXPECT_EQ(locked->segments.size(), 2u);
  EXPECT_EQ(locked->numSegments, 2u);
  EXPECT_EQ(locked->segments.begin()->first, 0u);
  EXPECT_EQ(std::next(locked->segments.begin())->first, 4'096u);
}

TEST(KeyMetadataTest, secondLockBlocksUntilFirstReleased) {
  KeyMetadata meta;
  std::atomic<bool> secondAcquired{false};
  auto first = meta.lock();
  std::thread t{[&] {
    auto second = meta.lock();
    secondAcquired = true;
  }};
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(secondAcquired)
      << "Second lock must block while first is held";
  // Release first by move-assigning an empty LockedKey.
  first = LockedKey{};
  t.join();
  EXPECT_TRUE(secondAcquired);
}

} // namespace facebook::velox::cache::fs::test
