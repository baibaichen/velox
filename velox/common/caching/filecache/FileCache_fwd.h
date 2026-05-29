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

#include <cstddef>
#include <cstdint>
#include <memory>

namespace facebook::velox::ch {

static constexpr int FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 32 * 1024 * 1024; /// 32Mi
static constexpr int FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT = 4 * 1024 * 1024; /// 4Mi
static constexpr int FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD = 4 * 1024 * 1024; /// 4Mi
static constexpr int FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS = 5;
static constexpr int FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT = 5000;
static constexpr int FILECACHE_DEFAULT_LOAD_METADATA_THREADS = 16;
static constexpr int FILECACHE_DEFAULT_MAX_ELEMENTS = 10000000;
static constexpr size_t FILECACHE_BYPASS_THRESHOLD = 256 * 1024 * 1024;
static constexpr double FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO = 0; /// Disabled.
static constexpr double FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO = 0; /// Disabled.
static constexpr int FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH = 100;
static constexpr auto FILECACHE_DEFAULT_CONFIG_PATH = "filesystem_caches";

enum class FileCachePolicy : uint8_t {
  LRU,
  SLRU,
  SLRU_OVERCOMMIT,
  LRU_OVERCOMMIT,
};

static constexpr auto FILECACHE_DEFAULT_CACHE_POLICY = FileCachePolicy::SLRU;

/// SLRU ratio of 0.6 means:
/// 60% of cache for protected elements.
/// 40% of cache for probationary elements.
static constexpr double FILECACHE_DEFAULT_SLRU_RATIO = 0.6;

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheSettings;
struct FileCacheKey;
struct FileCacheUserInfo;

} // namespace facebook::velox::ch
