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
#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "velox/common/caching/filecache/FileCache_fwd.h"

namespace facebook::velox::ch {

template <typename T>
struct Setting {
  T value{};
  bool changed{false};

  Setting() = default;
  Setting(T v) : value(std::move(v)) {}

  Setting& operator=(T v) {
    value = std::move(v);
    changed = true;
    return *this;
  }

  operator const T&() const {
    return value;
  }
};

struct FileCacheSettings {
  Setting<std::string> path{""};
  Setting<uint64_t> maxSize{0};
  Setting<uint64_t> maxElements{FILECACHE_DEFAULT_MAX_ELEMENTS};
  Setting<uint64_t> maxFileSegmentSize{FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE};
  Setting<uint64_t> boundaryAlignment{FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT};
  Setting<bool> cacheOnWriteOperations{false};
  Setting<FileCachePolicy> cachePolicy{FILECACHE_DEFAULT_CACHE_POLICY};
  Setting<double> slruSizeRatio{FILECACHE_DEFAULT_SLRU_RATIO};
  Setting<uint64_t> backgroundDownloadThreads{
      FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS};
  Setting<uint64_t> backgroundDownloadQueueSizeLimit{
      FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT};
  Setting<uint64_t> backgroundDownloadMaxFileSegmentSize{
      FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD};
  Setting<uint64_t> loadMetadataThreads{FILECACHE_DEFAULT_LOAD_METADATA_THREADS};
  Setting<bool> loadMetadataAsynchronously{false};
  Setting<double> keepFreeSpaceSizeRatio{FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO};
  Setting<double> keepFreeSpaceElementsRatio{
      FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO};
  Setting<uint64_t> keepFreeSpaceRemoveBatch{
      FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH};
  Setting<bool> enableFilesystemQueryCacheLimit{false};
  Setting<uint64_t> cacheHitsThreshold{0};
  Setting<bool> enableBypassCacheWithThreshold{false};
  Setting<uint64_t> bypassCacheThreshold{FILECACHE_BYPASS_THRESHOLD};
  Setting<bool> writeCachePerUserIdDirectory{false};
  Setting<bool> allowDynamicCacheResize{false};
  Setting<uint64_t> dynamicResizeLockWaitMs{1000};
  Setting<double> maxSizeRatioToTotalSpace{0};
  Setting<bool> skipCacheOnDiskFailure{false};
  Setting<bool> useSplitCache{false};
  Setting<double> splitCacheRatio{0.1};
  Setting<uint64_t> overcommitEvictionEvictStep{10 * 1024ULL * 1024ULL};
  Setting<double> checkCacheProbability{0.001};

  void validate();
};

// TODO(config): CH FileCacheSettings also implements loadFromConfig(Poco),
// loadFromCollection(NamedCollection), getColumnsDescription(), and
// dumpToSystemSettingsColumns(); this Velox port intentionally omits that CH
// BaseSettings/pimpl/system-table infrastructure until the Velox configuration
// integration is designed.

} // namespace facebook::velox::ch
