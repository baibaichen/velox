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

namespace facebook::velox::cache::fs {

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
};

} // namespace facebook::velox::cache::fs
