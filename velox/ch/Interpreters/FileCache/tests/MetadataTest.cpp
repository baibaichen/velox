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

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/tests/FileCacheTestResources.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;
namespace fs = std::filesystem;

FileCacheSettings metaSettings(const std::string & path)
{
    FileCacheSettings s;
    s.path = path;
    s.maxSize = 16 * 1024 * 1024;
    s.maxElements = 100;
    s.maxFileSegmentSize = 1024 * 1024;
    s.boundaryAlignment = 1;
    s.cachePolicy = FileCachePolicy::LRU;
    s.useSplitCache = false;
    s.backgroundDownloadThreads = 0;
    s.loadMetadataAsynchronously = false;
    s.keepFreeSpaceSizeRatio = 0.0;
    s.keepFreeSpaceElementsRatio = 0.0;
    return s;
}

class MetadataTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        cache_ = res_.makeFileCache("m", metaSettings((fs::path(temp_->getPath()) / "cache").string()), "user-A");
        cache_->initialize();
    }

    void TearDown() override
    {
        if (cache_)
            cache_->deactivateBackgroundOperations();
    }

    std::shared_ptr<TempDirectoryPath> temp_;
    facebook::velox::ch::test::FileCacheTestResources res_;
    std::unique_ptr<FileCache> cache_;
};

/// Path layout invariants (production `CacheMetadata::getFileNameForFileSegment`, reached
/// through the public `FileCache::getFileSegmentPath`). The trailing filename encodes the
/// segment kind/state exactly as ClickHouse does.
TEST_F(MetadataTest, RegularDownloadingFilename)
{
    auto key = FileCacheKey::random();
    auto path = cache_->getFileSegmentPath(key, 100, FileSegmentKind::Regular, cache_->getCommonOrigin(), std::nullopt);
    EXPECT_EQ(fs::path(path).filename().string(), "100");
}

TEST_F(MetadataTest, RegularDownloadedFilename)
{
    auto key = FileCacheKey::random();
    auto path = cache_->getFileSegmentPath(key, 100, FileSegmentKind::Regular, cache_->getCommonOrigin(), 512);
    EXPECT_EQ(fs::path(path).filename().string(), "100_512");
}

TEST_F(MetadataTest, EphemeralFilename)
{
    auto key = FileCacheKey::random();
    auto path = cache_->getFileSegmentPath(key, 0, FileSegmentKind::Ephemeral, cache_->getCommonOrigin(), std::nullopt);
    EXPECT_EQ(fs::path(path).filename().string(), "0_temporary");
}

/// Key-path layout: the segment file lives under the key directory, whose last two
/// components are the first-3-key-chars prefix directory and the full key string.
TEST_F(MetadataTest, KeyPathLayout)
{
    auto key = FileCacheKey::random();
    auto key_str = key.toString();
    auto key_path = cache_->getKeyPath(key, cache_->getCommonOrigin());
    EXPECT_EQ(fs::path(key_path).filename().string(), key_str);
    EXPECT_EQ(fs::path(key_path).parent_path().filename().string(), key_str.substr(0, 3));
    // The segment path is the key path joined with the filename.
    auto seg_path = cache_->getFileSegmentPath(key, 7, FileSegmentKind::Regular, cache_->getCommonOrigin(), std::nullopt);
    EXPECT_EQ(fs::path(seg_path).parent_path().string(), key_path);
}

/// LockedKey destruction-order safety (production RAII): a LockedKey acquired via the real
/// metadata path must release its key lock in its destructor so the key can be locked again.
/// If the member destruction order were wrong (lock outliving the metadata reference), or if
/// the lock were not released, this re-lock would deadlock/throw. It returns cleanly instead.
TEST_F(MetadataTest, LockedKeyReleasesLockOnDestruction)
{
    auto key = FileCacheKey::random();
    CreateFileSegmentSettings create_settings;
    // Create metadata for the key so a KeyMetadata exists to lock.
    auto holder = cache_->getOrSet(key, 0, 4096, 4096, create_settings, 0, cache_->getCommonOrigin());
    ASSERT_TRUE(holder);
    ASSERT_FALSE(holder->empty());

    auto key_metadata = holder->front().getKeyMetadata();
    ASSERT_TRUE(key_metadata);

    {
        LockedKeyPtr locked = key_metadata->lock();
        ASSERT_TRUE(locked);
        EXPECT_EQ(locked->getKey(), key);
        EXPECT_EQ(locked->getKeyState(), KeyMetadata::KeyState::ACTIVE);
    } // LockedKey destroyed here: lock must be released before key_metadata ref drops.

    // Re-locking succeeds, proving the previous lock was released in the destructor.
    LockedKeyPtr again = key_metadata->lock();
    EXPECT_TRUE(again);
    EXPECT_EQ(again->getKey(), key);
}

} // namespace
} // namespace facebook::velox::ch
