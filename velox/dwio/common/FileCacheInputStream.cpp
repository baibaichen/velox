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

#include "velox/dwio/common/FileCacheInputStream.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/file/File.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace facebook::velox::ch {

FileCacheInputStream::FileCacheInputStream(
    std::vector<FileSegmentPtr> segments,
    uint64_t regionOffset,
    uint64_t regionLength)
    : segments_{std::move(segments)},
      regionOffset_{regionOffset},
      regionLength_{regionLength} {
  VELOX_CHECK(!segments_.empty(), "FileCacheInputStream requires >=1 segment");
  loadCurrentSegmentBuffer();
}

void FileCacheInputStream::loadCurrentSegmentBuffer() {
  const auto& segment = segments_[index_];
  const uint64_t segStart = segment->range().left;
  const uint64_t segEnd = segment->range().right + 1;
  // Intersect the requested region with the current segment. Both endpoints are
  // clipped because outer segments may be aligned beyond the requested region.
  const uint64_t rangeStart = std::max(regionOffset_, segStart);
  const uint64_t rangeEnd = std::min(regionOffset_ + regionLength_, segEnd);
  VELOX_CHECK_LT(rangeStart, rangeEnd);
  const uint64_t length = rangeEnd - rangeStart;
  // Offset of the first requested byte within the segment file.
  const uint64_t segmentOffset = rangeStart - segStart;

  // Wait until the writer has persisted the last byte this read needs. Treat
  // DOWNLOADING as a retryable wait, and EMPTY as the brief window before the
  // async download task submitted by load() has claimed the downloader
  // (EMPTY -> DOWNLOADING). wait() returns immediately while the segment is
  // EMPTY, so back off to avoid busy-spinning, but bound the wait so a task
  // that never starts surfaces an error instead of spinning forever. Abandoned
  // downloads land in a terminal non-EMPTY state, so EMPTY here only ever means
  // "not started yet". Fail once the segment reaches a terminal state without
  // enough durable bytes.
  const uint64_t needed = segmentOffset + length;
  const auto emptyDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(60);
  while (segment->getDownloadedSize() < needed) {
    const auto state = segment->wait(segStart + needed - 1);
    const auto downloadedSize = segment->getDownloadedSize();
    if (downloadedSize >= needed) {
      break;
    }
    if (state == FileSegment::State::EMPTY) {
      VELOX_CHECK(
          std::chrono::steady_clock::now() < emptyDeadline,
          "FileCacheInputStream: async download task did not start; segment "
          "still EMPTY after timeout (needed: {})",
          needed);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    if (state != FileSegment::State::DOWNLOADING) {
      VELOX_FAIL(
          "FileCacheInputStream: segment has fewer downloaded bytes than "
          "required; writer abandoned the download (downloadedSize: {}, "
          "needed: {})",
          downloadedSize,
          needed);
    }
  }

  // Skip zero-fill because LocalReadFile::pread overwrites every byte.
  buffer_ = std::make_unique_for_overwrite<char[]>(length);
  bufferSize_ = length;
  LocalReadFile file(segment->getPath());
  file.pread(segmentOffset, length, buffer_.get());
  cursor_ = 0;
}

bool FileCacheInputStream::Next(const void** data, int32_t* size) {
  if (cursor_ >= bufferSize_) {
    if (index_ + 1 >= segments_.size()) {
      return false;
    }
    ++index_;
    loadCurrentSegmentBuffer();
  }
  *data = buffer_.get() + cursor_;
  *size = static_cast<int32_t>(bufferSize_ - cursor_);
  byteCount_ += *size;
  cursor_ = bufferSize_;
  return true;
}

void FileCacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  VELOX_CHECK_LE(static_cast<size_t>(count), cursor_);
  cursor_ -= count;
  byteCount_ -= count;
}

bool FileCacheInputStream::SkipInt64(int64_t count) {
  VELOX_CHECK_GE(count, 0);
  while (count > 0) {
    const int64_t available =
        static_cast<int64_t>(bufferSize_) - static_cast<int64_t>(cursor_);
    if (count <= available) {
      cursor_ += count;
      byteCount_ += count;
      return true;
    }
    cursor_ = bufferSize_;
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

int64_t FileCacheInputStream::ByteCount() const {
  return static_cast<int64_t>(byteCount_);
}

void FileCacheInputStream::seekToPosition(
    facebook::velox::dwio::common::PositionProvider& position) {
  // Linear rewind plus forward skip. A later version can index cumulative
  // segment offsets for logarithmic seek.
  const uint64_t target = position.next();
  index_ = 0;
  byteCount_ = 0;
  loadCurrentSegmentBuffer();
  SkipInt64(static_cast<int64_t>(target));
}

std::string FileCacheInputStream::getName() const {
  return "FileCacheInputStream";
}

size_t FileCacheInputStream::positionSize() const {
  return 1;
}

} // namespace facebook::velox::ch
