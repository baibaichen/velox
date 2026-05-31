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
#include "velox/common/memory/Allocation.h"

#include <folly/Range.h>

#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

namespace facebook::velox::ch {

namespace {
// Slices below this size are read into a std::string instead of a pooled
// Allocation, mirroring DirectInputStream's tiny-data path.
constexpr uint64_t kTinySize = 4 * 1024;

// Builds ranges covering EXACTLY `length` bytes from the front of `data`,
// stopping as soon as `length` is reached. The reused allocation may be larger
// than the current slice, so this must not emit surplus ranges.
std::vector<folly::Range<char*>> makeCappedRanges(
    uint64_t length,
    memory::Allocation& data) {
  std::vector<folly::Range<char*>> buffers;
  uint64_t remaining = length;
  for (int32_t i = 0; i < data.numRuns() && remaining > 0; ++i) {
    auto run = data.runAt(i);
    const uint64_t runBytes =
        memory::AllocationTraits::pageBytes(run.numPages());
    const uint64_t take = std::min(runBytes, remaining);
    buffers.push_back(folly::Range<char*>(run.data<char>(), take));
    remaining -= take;
  }
  VELOX_CHECK_EQ(remaining, 0, "Allocation too small for segment slice");
  return buffers;
}
} // namespace

FileCacheInputStream::FileCacheInputStream(
    std::vector<FileSegmentPtr> segments,
    uint64_t regionOffset,
    uint64_t regionLength,
    memory::MemoryPool& pool)
    : segments_{std::move(segments)},
      regionOffset_{regionOffset},
      regionLength_{regionLength},
      pool_{pool} {
  VELOX_CHECK(!segments_.empty(), "FileCacheInputStream requires >=1 segment");
  // Precompute, for each segment, the slice of the requested region it covers:
  // its length (as a prefix sum) and its offset within the on-disk segment
  // file. Both endpoints are clipped because outer segments may be aligned
  // beyond the requested region. Done once so Next()/Skip()/seek() can locate
  // and load segments lazily by absolute region cursor.
  sliceStartInRegion_.reserve(segments_.size() + 1);
  sliceOffsetInSegment_.reserve(segments_.size());
  uint64_t acc = 0;
  for (const auto& segment : segments_) {
    const uint64_t segStart = segment->range().left;
    const uint64_t segEnd = segment->range().right + 1;
    const uint64_t rangeStart = std::max(regionOffset_, segStart);
    const uint64_t rangeEnd = std::min(regionOffset_ + regionLength_, segEnd);
    VELOX_CHECK_LT(rangeStart, rangeEnd);
    sliceStartInRegion_.push_back(acc);
    sliceOffsetInSegment_.push_back(rangeStart - segStart);
    acc += rangeEnd - rangeStart;
  }
  sliceStartInRegion_.push_back(acc);
  VELOX_CHECK_EQ(
      acc, regionLength_, "segments do not fully cover requested region");
}

size_t FileCacheInputStream::segmentIndexFor(uint64_t cursor) const {
  // Largest i with sliceStartInRegion_[i] <= cursor (cursor < regionLength_).
  const auto it = std::upper_bound(
      sliceStartInRegion_.begin(), sliceStartInRegion_.end(), cursor);
  return static_cast<size_t>(it - sliceStartInRegion_.begin()) - 1;
}

void FileCacheInputStream::loadSegment(size_t index) {
  const auto& segment = segments_[index];
  const uint64_t length =
      sliceStartInRegion_[index + 1] - sliceStartInRegion_[index];
  const uint64_t segmentOffset = sliceOffsetInSegment_[index];

  // Wait until the writer has persisted the last byte this slice needs. Treat
  // DOWNLOADING as a retryable wait. EMPTY and (resumable) PARTIALLY_DOWNLOADED
  // are also transient: a download task is pending or a later load() resumes the
  // gap, so back off briefly (bounded) rather than failing. Only the terminal
  // states (PARTIALLY_DOWNLOADED_NO_CONTINUATION / DETACHED) mean the writer
  // truly abandoned the download; fail once the segment reaches one of those
  // without enough durable bytes.
  const uint64_t segStart = segment->range().left;
  const uint64_t needed = segmentOffset + length;
  const auto resumeDeadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(60);
  while (segment->getDownloadedSize() < needed) {
    const auto state = segment->wait(segStart + needed - 1);
    const auto downloadedSize = segment->getDownloadedSize();
    if (downloadedSize >= needed) {
      break;
    }
    if (state == FileSegment::State::EMPTY ||
        state == FileSegment::State::PARTIALLY_DOWNLOADED) {
      VELOX_CHECK(
          std::chrono::steady_clock::now() < resumeDeadline,
          "FileCacheInputStream: download did not progress; segment still "
          "resumable ({}) after timeout (downloadedSize: {}, needed: {})",
          FileSegment::stateToString(state),
          downloadedSize,
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

  // Read the slice into reused storage. Pooled non-contiguous allocation lets
  // the MmapAllocator reuse resident pages across reads (no per-read
  // zero-fill page faults); tiny slices use a small std::string instead.
  LocalReadFile file(segment->getPath());
  if (length < kTinySize) {
    tinyData_.resize(length);
    const auto got = file.pread(segmentOffset, length, tinyData_.data());
    VELOX_CHECK_EQ(got.size(), length, "short pread of cache segment slice");
  } else {
    const auto numPages = memory::AllocationTraits::numPages(length);
    if (numPages > segData_.numPages()) {
      pool_.allocateNonContiguous(numPages, segData_); // grow-only
    }
    auto ranges = makeCappedRanges(length, segData_);
    const uint64_t read = file.preadv(segmentOffset, ranges);
    VELOX_CHECK_EQ(read, length, "short preadv of cache segment slice");
  }
  loadedIndex_ = index;
  loadedSliceLen_ = length;
}

void FileCacheInputStream::loadPosition() {
  const size_t index = segmentIndexFor(regionCursor_);
  if (index != loadedIndex_) {
    loadSegment(index);
  }
  const uint64_t offsetInSlice = regionCursor_ - sliceStartInRegion_[index];
  if (loadedSliceLen_ < kTinySize) {
    run_ = reinterpret_cast<uint8_t*>(tinyData_.data());
    runSize_ = static_cast<uint32_t>(loadedSliceLen_);
    offsetInRun_ = offsetInSlice;
  } else {
    int32_t runIndex = 0;
    int32_t inRun = 0;
    segData_.findRun(offsetInSlice, &runIndex, &inRun);
    auto run = segData_.runAt(runIndex);
    run_ = run.data();
    runSize_ = static_cast<uint32_t>(
        memory::AllocationTraits::pageBytes(run.numPages()));
    const uint64_t offsetOfRun = offsetInSlice - static_cast<uint64_t>(inRun);
    if (offsetOfRun + runSize_ > loadedSliceLen_) {
      runSize_ = static_cast<uint32_t>(loadedSliceLen_ - offsetOfRun);
    }
    offsetInRun_ = static_cast<uint64_t>(inRun);
  }
  VELOX_CHECK_LT(offsetInRun_, runSize_);
}

bool FileCacheInputStream::Next(const void** data, int32_t* size) {
  if (regionCursor_ >= regionLength_) {
    *size = 0;
    return false;
  }
  loadPosition();
  uint64_t chunk = runSize_ - offsetInRun_;
  if (regionCursor_ + chunk > regionLength_) {
    chunk = regionLength_ - regionCursor_;
  }
  *data = run_ + offsetInRun_;
  *size = static_cast<int32_t>(chunk);
  offsetInRun_ += chunk;
  regionCursor_ += chunk;
  return true;
}

void FileCacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  VELOX_CHECK_LE(unsignedCount, offsetInRun_, "Can't backup that much!");
  regionCursor_ -= unsignedCount;
}

bool FileCacheInputStream::SkipInt64(int64_t count) {
  if (count < 0) {
    return false;
  }
  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  if (unsignedCount + regionCursor_ <= regionLength_) {
    regionCursor_ += unsignedCount;
    return true;
  }
  regionCursor_ = regionLength_;
  return false;
}

int64_t FileCacheInputStream::ByteCount() const {
  return static_cast<int64_t>(regionCursor_);
}

void FileCacheInputStream::seekToPosition(
    facebook::velox::dwio::common::PositionProvider& position) {
  const uint64_t target = position.next();
  VELOX_CHECK_LE(target, regionLength_);
  regionCursor_ = target;
}

std::string FileCacheInputStream::getName() const {
  return "FileCacheInputStream";
}

size_t FileCacheInputStream::positionSize() const {
  return 1;
}

} // namespace facebook::velox::ch
