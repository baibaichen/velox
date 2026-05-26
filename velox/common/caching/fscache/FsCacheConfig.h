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
#include <string>
#include <string_view>

namespace facebook::velox::cache::fs {

/// On-disk version sentinel file, written under cacheRoot to mark the
/// directory layout produced by this build. loadFromDisk() blind-clears
/// cacheRoot when the sentinel is absent or its content does not match
/// kFsCacheCurrentVersion, since phase-1 hashes are not re-keyable.
inline constexpr std::string_view kFsCacheVersionSentinelName =
    ".fscache_version";

/// Current on-disk layout version written into kFsCacheVersionSentinelName.
/// Bump when the on-disk filename format or directory layout changes in a
/// way that older survivors would mis-key against the new hasher.
inline constexpr std::string_view kFsCacheCurrentVersion = "2";

/// Holds configuration for an FsCache instance. All fields are immutable after
/// the FsCache is constructed.
struct FsCacheConfig {
  /// Filesystem root under which cache files live.
  std::string cacheRoot;

  /// Maximum total bytes of cached data on disk. Eviction triggers when usage
  /// would exceed this value.
  uint64_t maxBytes{0};

  /// Segment start/end alignment in bytes. Matches ClickHouse default of
  /// 4 MiB so that adjacent reads share a segment.
  uint64_t alignment{4UL * 1024 * 1024};

  /// Maximum size of a single segment in bytes. Holes larger than this are
  /// split into multiple segments.
  uint64_t maxSegmentSize{32UL * 1024 * 1024};

  /// Number of buckets in the top-level metadata array. A larger value reduces
  /// per-bucket contention at the cost of memory.
  ///
  /// Tuning guidance: target ~16-64 live FileSegments per bucket at peak. With
  /// 4 MiB align and 32 MiB max segment size, 1 TiB of warm working set is
  /// ~32 K-256 K segments, so 1024 buckets keeps the per-bucket chain short
  /// without blowing up the metadata array. Bucket count should be a power of
  /// two for the hash → bucket modulo to fold cleanly.
  size_t numBuckets{1024};

  /// Number of threads dedicated to asynchronous segment downloads. IO-bound;
  /// must not share with Velox's CPU executor (spec §7.1 / §10 R4). Capped at
  /// 32 inside DownloadThreadPool to bound per-remote-pread contention.
  size_t downloadThreads{8};

  /// Skip the cache for any single getOrSet request whose `size` is greater
  /// than or equal to this value. Set to 0 to disable the bypass entirely
  /// (every request enters the cache regardless of size).
  ///
  /// Default 0 (disabled), matching ClickHouse's `FILECACHE_BYPASS_THRESHOLD`
  /// behaviour (src/Interpreters/FileCache/FileCacheSettings.cpp declares
  /// `bypass_cache_threshold` as "Undocumented. Not recommended for use" with
  /// default 0). Rationale: CH relies on per-query
  /// `filesystem_cache_max_download_size` quotas + LRU itself to keep large
  /// scans from evicting the warm working set; bypass is a safety valve, not
  /// the primary defence. Phase-1 ships the mechanism but defaults it off
  /// until the caller-side QueryLimitToken wiring lands in phase-3 and
  /// effectiveness can be re-validated with TPC-H and microbench (spec §8.3).
  uint64_t bypassThresholdBytes{0};
};

} // namespace facebook::velox::cache::fs
