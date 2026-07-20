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

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"

#include <cstdint>
#include <ctime>
#include <string>

namespace facebook::velox::ch
{

/// File-segment state machine values. Order and underlying integer values are
/// preserved exactly from ClickHouse `src/Interpreters/FileCache/FileSegmentInfo.h`.
enum class FileSegmentState : uint8_t
{
    DOWNLOADED = 0,
    /// When a file segment is first created and returned to a user, it has
    /// state EMPTY. EMPTY becomes DOWNLOADING when getOrSetDownloader is called
    /// successfully by any owner of an EMPTY-state file segment.
    EMPTY = 1,
    /// A newly created file segment never has DOWNLOADING state until a call to
    /// getOrSetDownloader, because each cache user might acquire multiple file
    /// segments and read them one by one; only the user which actually needs to
    /// read this segment earlier than others becomes a downloader.
    DOWNLOADING = 2,
    /// Space reservation is incremental: the downloader reads buffer_size bytes
    /// from remote fs, tries to reserve buffer_size bytes, and writes to cache
    /// on successful reservation, stopping otherwise. Waiters read the
    /// downloaded part from cache and the remaining part directly from remote fs.
    PARTIALLY_DOWNLOADED_NO_CONTINUATION = 3,
    /// If the downloader did not finish downloading for any reason apart from
    /// running out of cache space, the download can be continued by other owners.
    PARTIALLY_DOWNLOADED = 4,
    /// If the file segment cannot possibly be downloaded (first space
    /// reservation attempt failed), mark it as out of cache scope.
    DETACHED = 5,
};

enum class FileSegmentKind : uint8_t
{
    /// Data cached from S3 or other backing storage. Kept in the cache after
    /// usage and can be evicted on demand, unless there are holders.
    Regular = 0,

    /// Temporary data without backing storage, written to the cache from
    /// outside. Ephemeral segments are kept while in use, then removed
    /// immediately after releasing. Corresponding files are removed during
    /// cache loading. Ephemeral segments have no bound and a single segment can
    /// have an arbitrary size.
    Ephemeral = 1,
};

/// Defined in FileSegment.cpp (no separate FileSegmentInfo.cpp created).
std::string toString(FileSegmentKind kind);

struct FileSegmentInfo
{
    FileCacheKey key;
    uint64_t offset = 0;
    std::string path;
    uint64_t range_left = 0;
    uint64_t range_right = 0;
    FileSegmentKind kind = FileSegmentKind::Regular;
    FileSegmentState state = FileSegmentState::EMPTY;
    uint64_t size = 0;
    uint64_t downloaded_size = 0;
    /// CH uses `time_t` (wall-clock seconds); the FileSegment member and
    /// `getInfo()` assign it straight through. Must not be a steady_clock
    /// time_point (monotonic) — see CH FileSegment.cpp getInfo.
    time_t download_finished_time = 0;
    uint64_t cache_hits = 0;
    uint64_t references = 0;
    bool is_unbound = false;
    IFileCachePriority::QueueEntryType queue_entry_type
        = IFileCachePriority::QueueEntryType::None;
    FileCacheOriginInfo origin;
};

} // namespace facebook::velox::ch
