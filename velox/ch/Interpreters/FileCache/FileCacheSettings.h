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

#include "velox/ch/Interpreters/FileCache/FileCache_fwd.h"
#include "velox/common/config/Config.h"

#include <cstdint>
#include <string>

namespace facebook::velox::ch
{

/// Effective cache-instance configuration. All field defaults reference the
/// constants in `FileCache_fwd.h` to keep a single source of truth.
///
/// This is a plain value type: compiler-generated copy/move/equality.
/// `FileCacheSettings` is a type alias for this struct so that algorithm
/// files compiled from ClickHouse source change as little as possible.
struct FileCacheConfig
{
    std::string path;

    uint64_t maxSize = 0;
    uint64_t maxElements = FILECACHE_DEFAULT_MAX_ELEMENTS;
    uint64_t maxFileSegmentSize = FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    uint64_t boundaryAlignment = FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT;
    uint64_t reserveGranularity = FILECACHE_DEFAULT_RESERVE_GRANULARITY;

    bool cacheOnWriteOperations = false;
    FileCachePolicy cachePolicy = FILECACHE_DEFAULT_CACHE_POLICY;
    double slruSizeRatio = FILECACHE_DEFAULT_SLRU_RATIO;

    uint64_t backgroundDownloadThreads =
        FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS;
    uint64_t backgroundDownloadQueueSizeLimit =
        FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT;
    uint64_t backgroundDownloadMaxFileSegmentSize =
        FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD;

    uint64_t loadMetadataThreads = FILECACHE_DEFAULT_LOAD_METADATA_THREADS;
    bool loadMetadataAsynchronously = false;

    double keepFreeSpaceSizeRatio = FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO;
    double keepFreeSpaceElementsRatio =
        FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO;
    uint64_t keepFreeSpaceRemoveBatch = FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH;
    uint64_t keepFreeSpaceEvictionThreads =
        FILECACHE_DEFAULT_FREE_SPACE_EVICTION_THREADS;

    uint64_t invalidatedEntriesCleanupIntervalMs = 10'000;
    uint64_t invalidatedEntriesCleanupThreshold = 1'000;
    uint64_t invalidatedEntriesCleanupRemoveBatch =
        FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH;

    bool enableFilesystemQueryCacheLimit = false;
    uint64_t cacheHitsThreshold = 0;
    bool enableBypassCacheWithThreshold = false;
    uint64_t bypassCacheThreshold = FILECACHE_BYPASS_THRESHOLD;

    bool writeCachePerUserIdDirectory = false;
    bool allowDynamicCacheResize = false;
    uint64_t dynamicResizeLockWaitMs = 1'000;

    double maxSizeRatioToTotalSpace = 0;
    bool skipCacheOnDiskFailure = false;

    bool useSplitCache = false;
    double splitCacheRatio = 0.1;
    uint64_t overcommitEvictionEvictStep = 10ULL * 1024 * 1024;

    double checkCacheProbability = 0.001;

    uint64_t idleClientTtlSec = 7 * 24 * 60 * 60;
    uint64_t idleClientCheckIntervalSec = 0;
    uint64_t idleClientEvictionThreads = 4;

    bool exposePrometheusEvictionMetrics = false;
    bool exposePrometheusEvictionMetricsPerUser = false;

    /// B2a: injected into `CacheMetadata` for the background-download reserve path,
    /// replacing CH's read from the global `Context`
    /// (`filesystem_cache_settings.reserve_space_wait_lock_timeout_milliseconds`).
    /// CH default is 1000 ms.
    uint64_t reserveSpaceWaitLockTimeoutMilliseconds = 1000;

    bool operator==(const FileCacheConfig &) const = default;
};

/// Algorithm files use the CH name `FileCacheSettings`; the real type is
/// `FileCacheConfig`. This alias lets algorithm files compile with minimal diff.
/// (Also declared in `FileCache_fwd.h`; redeclaring the same alias is legal.)
using FileCacheSettings = FileCacheConfig;

/// Loads and validates a `FileCacheConfig` from a Velox `ConfigBase`.
///
/// Config key layout:
///   canonical:  file-cache.<name>.<key>
///   default:    file-cache.<key>
///
/// `cachePrefix` is the full prefix (e.g. "file-cache" or "file-cache.mystore").
/// `cachePathPrefix` is the directory prepended to relative paths.
/// `allowedCacheRoot` restricts the resolved absolute path; an exception is
/// thrown if the resolved path does not lie under this root.
///
/// Path resolution order:
///   missing path      -> exception
///   relative path     -> cachePathPrefix / path, then lexically normalised
///   verify absolute
///   verify lies under allowedCacheRoot (canonicalized component-prefix check)
///
/// Max-size source: exactly one of max-size or max-size-ratio-to-total-space
/// must be present. If the ratio form is used, `std::filesystem::space(path)`
/// is called after the path is authorised and the directory may be created if
/// needed, to obtain total space for derivation.
struct FileCacheSettingsLoader
{
    static FileCacheConfig load(
        const config::ConfigBase & config,
        const std::string & cachePrefix,
        const std::string & cachePathPrefix,
        const std::string & allowedCacheRoot);
};

} // namespace facebook::velox::ch
