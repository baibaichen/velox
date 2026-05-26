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

#include "velox/dwio/common/FsCacheInputStream.h"

#include "velox/common/base/Exceptions.h"

#include <algorithm>

namespace facebook::velox::dwio::common {

FsCacheInputStream::FsCacheInputStream(
    std::vector<cache::fs::FileSegmentPtr> segments,
    uint64_t regionOffset,
    uint64_t regionLength,
    std::string cacheRoot)
    : segments_{std::move(segments)},
      regionOffset_{regionOffset},
      regionLength_{regionLength},
      cacheRoot_{std::move(cacheRoot)} {
  VELOX_CHECK(!segments_.empty(), "FsCacheInputStream requires >=1 segment");
  loadCurrentSegmentBuffer();
}

void FsCacheInputStream::loadCurrentSegmentBuffer() {
  const auto& segment = segments_[index_];
  const uint64_t segStart = segment->key().offset;
  const uint64_t segSize = segment->key().size;
  // Intersection of the requested region with the current segment. Both
  // endpoints are clipped because the outer segments produced by
  // FsCache::splitRange are outward-aligned and may extend past the region.
  const uint64_t rangeStart = std::max(regionOffset_, segStart);
  const uint64_t rangeEnd =
      std::min(regionOffset_ + regionLength_, segStart + segSize);
  VELOX_CHECK_LT(rangeStart, rangeEnd);
  const uint64_t length = rangeEnd - rangeStart;
  // Block until the writer (another thread driving the same shared segment
  // through reserve/write/complete) has made enough bytes durable. `needed`
  // is measured from segStart and equals the highest byte offset this read
  // touches. If the writer abandons, waitForDownloadedSize() throws so the
  // reader surfaces the failure instead of reading past the partial boundary.
  const uint64_t needed = (rangeStart - segStart) + length;
  segment->waitForDownloadedSize(needed);
  buffer_.assign(length, '\0');
  segment->read(rangeStart - segStart, length, buffer_.data(), cacheRoot_);
  cursor_ = 0;
}

bool FsCacheInputStream::Next(const void** data, int32_t* size) {
  if (cursor_ >= buffer_.size()) {
    if (index_ + 1 >= segments_.size()) {
      return false;
    }
    ++index_;
    loadCurrentSegmentBuffer();
  }
  *data = buffer_.data() + cursor_;
  *size = static_cast<int32_t>(buffer_.size() - cursor_);
  byteCount_ += *size;
  cursor_ = buffer_.size();
  return true;
}

void FsCacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  VELOX_CHECK_LE(static_cast<size_t>(count), cursor_);
  cursor_ -= count;
  byteCount_ -= count;
}

bool FsCacheInputStream::SkipInt64(int64_t count) {
  VELOX_CHECK_GE(count, 0);
  while (count > 0) {
    const int64_t available =
        static_cast<int64_t>(buffer_.size()) - static_cast<int64_t>(cursor_);
    if (count <= available) {
      cursor_ += count;
      byteCount_ += count;
      return true;
    }
    cursor_ = buffer_.size();
    byteCount_ += available;
    count -= available;
    if (index_ + 1 >= segments_.size()) {
      return false;
    }
    ++index_;
    loadCurrentSegmentBuffer();
  }
  return true;
}

int64_t FsCacheInputStream::ByteCount() const {
  return static_cast<int64_t>(byteCount_);
}

void FsCacheInputStream::seekToPosition(PositionProvider& position) {
  // Phase 1: linear rewind + forward Skip. Phase 2 can index segments by
  // cumulative offset for O(log N) seek.
  const uint64_t target = position.next();
  index_ = 0;
  byteCount_ = 0;
  loadCurrentSegmentBuffer();
  SkipInt64(static_cast<int64_t>(target));
}

std::string FsCacheInputStream::getName() const {
  return "FsCacheInputStream";
}

size_t FsCacheInputStream::positionSize() const {
  return 1;
}

} // namespace facebook::velox::dwio::common
