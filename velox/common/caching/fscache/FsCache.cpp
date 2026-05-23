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

#include "velox/common/caching/fscache/FsCache.h"

#include "velox/common/base/Exceptions.h"

#include <algorithm>

namespace facebook::velox::cache::fs {

FsCache::FsCache(FsCacheConfig config) : config_{std::move(config)} {}

FsCache::~FsCache() = default;

std::vector<std::pair<uint64_t, uint64_t>> FsCache::splitRange(
    uint64_t offset,
    uint64_t size,
    const FsCacheConfig& config) {
  std::vector<std::pair<uint64_t, uint64_t>> result;
  if (size == 0) {
    return result;
  }
  // Only the outer boundaries [alignedStart, alignedEnd) are snapped to
  // alignment. The internal cuts produced by the maxSegmentSize chunk loop
  // are NOT re-aligned, so e.g. alignment=4MiB, maxSegmentSize=32MiB,
  // size=40MiB yields [0, 32MiB), [32MiB, 40MiB) -- the second chunk's size
  // is unaligned (its start is still aligned because 32MiB is a multiple of
  // 4MiB, guaranteed by the contract that maxSegmentSize % alignment == 0).
  // ClickHouse behaves the same way: alignment is a sharing key for adjacent
  // reads, not a strict invariant on every segment.
  const uint64_t alignedStart = (offset / config.alignment) * config.alignment;
  const uint64_t end = offset + size;
  const uint64_t alignedEnd =
      ((end + config.alignment - 1) / config.alignment) * config.alignment;
  // The size>0 early-return plus outward alignment guarantees
  // alignedStart < alignedEnd, so at least one chunk is produced; the
  // VELOX_CHECK_GT guards against a future refactor accidentally producing
  // a zero-size chunk (which would create a "<hash>.<offset>.0" cache file
  // that crash recovery would silently accept).
  uint64_t cursor = alignedStart;
  while (cursor < alignedEnd) {
    const uint64_t chunkSize =
        std::min(config.maxSegmentSize, alignedEnd - cursor);
    VELOX_CHECK_GT(chunkSize, 0);
    result.emplace_back(cursor, chunkSize);
    cursor += chunkSize;
  }
  return result;
}

} // namespace facebook::velox::cache::fs
