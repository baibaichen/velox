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
#include <optional>

namespace facebook::velox::ch
{

/// Request-scoped read parameters for a single read operation. Unlike
/// `FileCacheConfig` (a cache-instance value owned by the manager/factory), a
/// `FileCacheReadOptions` value is owned by the caller and consumed by one read
/// (`FileCacheInputStream`). It is a plain value type with compiler-generated
/// copy/move so each read carries an independent copy.
///
/// Defaults mirror ClickHouse `ReadSettings` so a read behaves identically to
/// ClickHouse when the host does not override anything. Task 014 adds request
/// and file identity; it does not redefine this type.
struct FileCacheReadOptions
{
    /// Cache-only read: a miss is an error; the remote source is not read.
    bool tempCacheOnly = false;
    /// Read only if already cached; on a miss, bypass the cache and read the
    /// remote source without creating a segment.
    bool readIfExistsOtherwiseBypass = false;
    /// Whether the holder, on completion, may let unfinished segments continue
    /// downloading in the background.
    bool allowBackgroundDownload = true;
    /// Whether packed-storage metadata files may be downloaded in the background.
    bool allowBackgroundDownloadForMetadataFilesInPackedStorage = true;
    /// Whether background download is allowed during a fetch.
    bool allowBackgroundDownloadDuringFetch = true;
    /// When the filesystem cache is active, prefer a larger remote read buffer to
    /// reduce cache fragmentation.
    bool preferBiggerBufferSize = true;

    /// Maximum number of file segments held by one read.
    uint64_t segmentsBatchSize = 20;
    /// Per-read boundary alignment override (validated against the target
    /// cache's `maxFileSegmentSize`).
    std::optional<uint64_t> boundaryAlignment;

    /// Read buffer size for the remote source file.
    uint64_t remoteFsBufferSize = 0;
    /// Read buffer size for a local cache segment.
    uint64_t localFsBufferSize = 0;
    /// Timeout, in milliseconds, to wait for the reserve-space lock.
    uint64_t reserveSpaceWaitLockTimeoutMs = 0;
    /// Maximum size a single query may download into the cache.
    uint64_t maxDownloadSizePerQuery = 0;
    /// Whether to skip downloading when the per-query cache write limit is
    /// exceeded.
    bool skipDownloadIfExceedsPerQueryCacheWriteLimit = true;

    /// Whether to record the filesystem cache log.
    bool enableFilesystemCacheLog = false;
};

} // namespace facebook::velox::ch
