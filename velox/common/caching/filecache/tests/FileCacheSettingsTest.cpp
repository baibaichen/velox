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
#include "velox/common/caching/filecache/FileCacheSettings.h"

#include <filesystem>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox::ch;

namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;

std::filesystem::path testDirectory(const char* name) {
  return std::filesystem::current_path() / name;
}

FileCacheSettings validSettings() {
  FileCacheSettings settings;
  settings.path = testDirectory("filecache_settings_valid").string();
  settings.maxSize = 128 * kMiB;
  return settings;
}

} // namespace

TEST(FileCacheSettingsTest, defaultValuesMatchClickHouseListOfSettings) {
  FileCacheSettings settings;

  EXPECT_EQ(settings.path.value, "");
  EXPECT_FALSE(settings.path.changed);
  EXPECT_EQ(settings.maxSize.value, 0);
  EXPECT_EQ(settings.maxElements.value, FILECACHE_DEFAULT_MAX_ELEMENTS);
  EXPECT_EQ(
      settings.maxFileSegmentSize.value, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE);
  EXPECT_EQ(
      settings.boundaryAlignment.value, FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT);
  EXPECT_FALSE(settings.cacheOnWriteOperations.value);
  EXPECT_EQ(settings.cachePolicy.value, FILECACHE_DEFAULT_CACHE_POLICY);
  EXPECT_EQ(settings.slruSizeRatio.value, FILECACHE_DEFAULT_SLRU_RATIO);
  EXPECT_EQ(
      settings.backgroundDownloadThreads.value,
      FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS);
  EXPECT_EQ(
      settings.backgroundDownloadQueueSizeLimit.value,
      FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT);
  EXPECT_EQ(
      settings.backgroundDownloadMaxFileSegmentSize.value,
      FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD);
  EXPECT_EQ(
      settings.loadMetadataThreads.value,
      FILECACHE_DEFAULT_LOAD_METADATA_THREADS);
  EXPECT_FALSE(settings.loadMetadataAsynchronously.value);
  EXPECT_EQ(
      settings.keepFreeSpaceSizeRatio.value,
      FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO);
  EXPECT_EQ(
      settings.keepFreeSpaceElementsRatio.value,
      FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO);
  EXPECT_EQ(
      settings.keepFreeSpaceRemoveBatch.value,
      FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH);
  EXPECT_FALSE(settings.enableFilesystemQueryCacheLimit.value);
  EXPECT_EQ(settings.cacheHitsThreshold.value, 0);
  EXPECT_FALSE(settings.enableBypassCacheWithThreshold.value);
  EXPECT_EQ(settings.bypassCacheThreshold.value, FILECACHE_BYPASS_THRESHOLD);
  EXPECT_FALSE(settings.writeCachePerUserIdDirectory.value);
  EXPECT_FALSE(settings.allowDynamicCacheResize.value);
  EXPECT_EQ(settings.dynamicResizeLockWaitMs.value, 1000);
  EXPECT_EQ(settings.maxSizeRatioToTotalSpace.value, 0);
  EXPECT_FALSE(settings.skipCacheOnDiskFailure.value);
  EXPECT_FALSE(settings.useSplitCache.value);
  EXPECT_EQ(settings.splitCacheRatio.value, 0.1);
  EXPECT_EQ(settings.overcommitEvictionEvictStep.value, 10 * kMiB);
  EXPECT_EQ(settings.checkCacheProbability.value, 0.001);
}

TEST(FileCacheSettingsTest, assignmentSetsValueAndChangedFlag) {
  FileCacheSettings settings;

  settings.maxSize = 42;
  settings.path = std::string("/cache");

  EXPECT_EQ(settings.maxSize.value, 42);
  EXPECT_TRUE(settings.maxSize.changed);
  EXPECT_EQ(settings.path.value, "/cache");
  EXPECT_TRUE(settings.path.changed);
}

TEST(FileCacheSettingsTest, settingImplicitlyConvertsToConstValue) {
  FileCacheSettings settings;
  settings.maxSize = 42;

  const uint64_t value = settings.maxSize;

  EXPECT_EQ(value, 42);
}

TEST(FileCacheSettingsTest, validateAcceptsAbsolutePathAndMaxSize) {
  auto settings = validSettings();

  EXPECT_NO_THROW(settings.validate());
  EXPECT_EQ(settings.maxSize.value, 128 * kMiB);
}

TEST(FileCacheSettingsTest, validateRequiresPathToBeChanged) {
  FileCacheSettings settings;
  settings.maxSize = 128 * kMiB;

  VELOX_ASSERT_THROW(
      settings.validate(), "`path` is required parameter of cache configuration");
}

TEST(FileCacheSettingsTest, validateRejectsRelativePath) {
  FileCacheSettings settings;
  settings.path = std::string("relative/cache");
  settings.maxSize = 128 * kMiB;

  VELOX_ASSERT_THROW(
      settings.validate(), "`path` was not normalized to absolute");
}

TEST(FileCacheSettingsTest, validateRequiresMaxSizeOrRatio) {
  FileCacheSettings settings;
  settings.path = testDirectory("filecache_settings_requires_size").string();

  VELOX_ASSERT_THROW(
      settings.validate(),
      "Either `max_size` or `max_size_ratio_to_total_space` must be defined in cache configuration");
}

TEST(FileCacheSettingsTest, validateRejectsMaxSizeAndRatioTogether) {
  auto settings = validSettings();
  settings.maxSizeRatioToTotalSpace = 0.5;

  VELOX_ASSERT_THROW(
      settings.validate(),
      "`max_size` and `max_size_ratio_to_total_space` cannot be specified at the same time");
}

TEST(FileCacheSettingsTest, validateRejectsZeroMaxSizeWhenChanged) {
  FileCacheSettings settings;
  settings.path = testDirectory("filecache_settings_zero_max_size").string();
  settings.maxSize = 0;

  VELOX_ASSERT_THROW(settings.validate(), "`max_size` cannot be 0");
}

TEST(FileCacheSettingsTest, validateRejectsZeroOvercommitEvictionEvictStep) {
  auto settings = validSettings();
  settings.overcommitEvictionEvictStep = 0;

  VELOX_ASSERT_THROW(
      settings.validate(), "`overcommit_eviction_evict_step` cannot be zero");
}

TEST(
    FileCacheSettingsTest,
    validateRejectsBoundaryAlignmentAboveMaxFileSegmentSize) {
  auto settings = validSettings();
  settings.maxFileSegmentSize = 4 * kMiB;
  settings.boundaryAlignment = 8 * kMiB;

  VELOX_ASSERT_THROW(
      settings.validate(),
      "`boundary_alignment` (8388608) must not exceed "
      "`max_file_segment_size` (4194304)");
}

TEST(FileCacheSettingsTest, validateRejectsZeroRatio) {
  FileCacheSettings settings;
  settings.path = testDirectory("filecache_settings_bad_ratio").string();
  settings.maxSizeRatioToTotalSpace = 0.0;

  VELOX_ASSERT_THROW(
      settings.validate(), "`max_size_ratio_to_total_space` must be in range (0, 1]");
}

TEST(FileCacheSettingsTest, validateRejectsRatioAboveOne) {
  FileCacheSettings settings;
  settings.path = testDirectory("filecache_settings_bad_ratio_high").string();
  settings.maxSizeRatioToTotalSpace = 1.1;

  VELOX_ASSERT_THROW(
      settings.validate(),
      "`max_size_ratio_to_total_space` must be in range (0, 1]");
}

TEST(FileCacheSettingsTest, validateComputesMaxSizeFromRatio) {
  const auto path = testDirectory("filecache_settings_ratio");
  std::filesystem::remove_all(path);

  FileCacheSettings settings;
  settings.path = path.string();
  settings.maxSizeRatioToTotalSpace = 1.0;

  EXPECT_NO_THROW(settings.validate());
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_TRUE(settings.maxSize.changed);
  EXPECT_GT(settings.maxSize.value, 0);

  std::filesystem::remove_all(path);
}
