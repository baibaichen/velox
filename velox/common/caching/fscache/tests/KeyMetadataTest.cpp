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

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"
#include "velox/common/caching/fscache/FsCacheMetadata.h"

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

TEST(KeyMetadataTest, lockKeyMetadataThrowsWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  EXPECT_THROW(
      md.lockKeyMetadata(path, KeyNotFoundPolicy::kThrow),
      ::facebook::velox::VeloxException);
}

TEST(KeyMetadataTest, lockKeyMetadataReturnsEmptyLockedKeyWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  auto locked = md.lockKeyMetadata(path, KeyNotFoundPolicy::kReturnNull);
  EXPECT_EQ(locked.get(), nullptr);
}

TEST(KeyMetadataTest, lockKeyMetadataCreatesEmptyWhenAbsent) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/new");
  {
    auto locked = md.lockKeyMetadata(path, KeyNotFoundPolicy::kCreateEmpty);
    ASSERT_NE(locked.get(), nullptr);
    EXPECT_TRUE(locked->segments.empty());
    // LockedKey dtor releases the per-key lock here; second call below
    // must not deadlock.
  }
  auto again = md.lockKeyMetadata(path, KeyNotFoundPolicy::kReturnNull);
  EXPECT_NE(again.get(), nullptr);
}

TEST(KeyMetadataTest, lockKeyMetadataThrowLogicalAlsoThrows) {
  FsCacheMetadata md{4};
  PathKey path = PathKey::fromPath("/nope");
  EXPECT_THROW(
      md.lockKeyMetadata(path, KeyNotFoundPolicy::kThrowLogical),
      ::facebook::velox::VeloxException);
}

} // namespace facebook::velox::cache::fs::test
