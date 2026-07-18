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
#include <memory>

namespace facebook::velox::ch
{

enum class FileCachePolicy : uint8_t { LRU, SLRU, SLRU_OVERCOMMIT, LRU_OVERCOMMIT };

inline constexpr uint64_t FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE =
    32ULL * 1024 * 1024;
inline constexpr uint64_t FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT =
    4ULL * 1024 * 1024;
inline constexpr uint64_t FILECACHE_DEFAULT_RESERVE_GRANULARITY =
    4ULL * 1024 * 1024;
// Typo preserved from CH identifier to minimise algorithm-file diff.
inline constexpr uint64_t FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD =
    4ULL * 1024 * 1024;
inline constexpr uint64_t FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS = 5;
inline constexpr uint64_t FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT = 5000;
inline constexpr uint64_t FILECACHE_DEFAULT_LOAD_METADATA_THREADS = 16;
inline constexpr uint64_t FILECACHE_DEFAULT_MAX_ELEMENTS = 10'000'000;
inline constexpr uint64_t FILECACHE_BYPASS_THRESHOLD = 256ULL * 1024 * 1024;
inline constexpr double FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO = 0.0;
inline constexpr double FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO = 0.0;
inline constexpr uint64_t FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH = 250;
inline constexpr uint64_t FILECACHE_DEFAULT_FREE_SPACE_EVICTION_THREADS = 1;
inline constexpr FileCachePolicy FILECACHE_DEFAULT_CACHE_POLICY =
    FileCachePolicy::SLRU;
inline constexpr double FILECACHE_DEFAULT_SLRU_RATIO = 0.6;

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

struct FileCacheConfig;
/// Algorithm files use the CH name `FileCacheSettings`.
using FileCacheSettings = FileCacheConfig;

struct FileCacheKey;

} // namespace facebook::velox::ch
