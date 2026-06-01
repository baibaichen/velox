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
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Allocation.h"
#include "velox/dwio/common/ReadFileByteInputStream.h"

#include <folly/Range.h>

#include <algorithm>
#include <vector>

namespace facebook::velox::ch {

namespace {
// Slices below this size are read into a std::string instead of a pooled
// Allocation, mirroring DirectInputStream's tiny-data path.
constexpr uint64_t kTinySize = 4 * 1024;

constexpr size_t kReserveTimeoutMs{10000};

// Downloads the prefix [getCurrentWriteOffset, targetEnd) of `segment` from
// `readFile`, guaranteeing the segment durably covers `targetEnd` before
// returning (or that it has reached a terminal, non-resumable state).
//
// Several readers across threads can share one segment while requesting
// different prefixes, and FileCacheInputStream only waits for the download --
// it never drives it itself. A caller that loses the downloader race to another
// thread must therefore still ensure its own `targetEnd` is satisfied: a
// concurrent download that captured a shallower target could otherwise complete
// first and strand a deeper reader (which the old whole-segment download never
// did). Hence the retry loop: become the downloader and resume the gap, or wait
// for whoever holds it and re-check, until `targetEnd` is met or the segment is
// abandoned.
void downloadSegmentPrefix(
    const FileSegmentPtr& segment,
    const std::shared_ptr<ReadFile>& readFile,
    FileCache* cache,
    const std::shared_ptr<facebook::velox::io::IoStatistics>& ioStats,
    uint64_t targetEnd) {
  const uint64_t segStart = segment->range().left;
  VELOX_CHECK_GT(targetEnd, segStart);
  // Required downloadedSize (range-relative) for the segment to cover
  // targetEnd.
  const uint64_t needBytes = targetEnd - segStart;
  bool recordedMiss = false;

  while (segment->getDownloadedSize() < needBytes) {
    const auto downloaderId = segment->getOrSetDownloader();
    if (downloaderId != FileSegment::getCallerId()) {
      // Lost the race: another task holds the downloader. Wait for it to make
      // progress, then re-check whether it covered our target or gave up.
      const auto state = segment->wait(targetEnd - 1);
      if (segment->getDownloadedSize() >= needBytes) {
        return;
      }
      if (state == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION ||
          state == FileSegment::State::DETACHED ||
          state == FileSegment::State::DOWNLOADED) {
        // Terminal (or shrunk DOWNLOADED) without enough bytes: give up; the
        // waiting reader surfaces the shortfall.
        return;
      }
      // EMPTY / DOWNLOADING / PARTIALLY_DOWNLOADED: loop to acquire and resume.
      continue;
    }

    // Won the downloader. Another task may have satisfied the target between
    // the loop test and the acquisition; re-check before counting a miss or
    // writing.
    if (segment->getDownloadedSize() >= needBytes) {
      segment->completePartAndResetDownloader();
      return;
    }
    if (!recordedMiss) {
      // A real source download for this segment (a cross-load partial resume
      // counts as a further miss, mirroring "segments downloaded from source").
      cache->recordMiss();
      recordedMiss = true;
    }

    const uint64_t before = segment->getDownloadedSize();
    try {
      // Download only the requested prefix [currentWriteOffset, targetEnd);
      // downloadFromReader resumes from the current write offset, so a
      // partially downloaded segment continues rather than restarting. On a
      // reserve failure it stops after moving the segment to a terminal state.
      const uint64_t writeOffset = segment->getCurrentWriteOffset();
      if (targetEnd > writeOffset) {
        // Install a positioned reader on the segment (only the first downloader
        // does; a later resume reuses it). The same reader serves this
        // foreground prefix download and, after the holder is released, the
        // background tail-fill that completes the segment to its background
        // target size. It holds a shared_ptr to the source file, so it safely
        // outlives this BufferedInput; downloader ownership serializes its use
        // between the foreground and the background pool. Reaching a terminal
        // or fully-downloaded state resets it inside FileSegment.
        auto reader = segment->getRemoteFileReader();
        if (reader == nullptr) {
          reader = std::make_shared<ReadFileByteInputStream>(readFile);
          segment->setRemoteFileReader(reader);
        }
        std::vector<char> scratch;
        const size_t written = FileSegment::downloadFromReader(
            *segment,
            *reader,
            targetEnd - writeOffset,
            scratch,
            kReserveTimeoutMs);
        // Bytes fetched from source and written into the cache (Layer B
        // cumulative + Layer A: read() = source bytes, prefetch() = bytes
        // pulled ahead into the cache, mirroring CachedBufferedInput).
        cache->recordDownloadedBytes(written);
        if (ioStats != nullptr) {
          ioStats->read().increment(written);
          ioStats->prefetch().increment(written);
        }
      }
      if (segment->getDownloadedSize() == before &&
          segment->getDownloadedSize() < needBytes) {
        // No forward progress while we held the downloader (e.g. the source is
        // shorter than targetEnd). Mark the segment failed so the reader stops
        // waiting, instead of spinning on a target that can never be reached.
        segment->setDownloadFailed();
        segment->completePartAndResetDownloader();
        return;
      }
      segment->completePartAndResetDownloader();
    } catch (...) {
      segment->setDownloadFailed();
      segment->completePartAndResetDownloader();
      return;
    }

    const auto state = segment->state();
    if (state == FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION ||
        state == FileSegment::State::DETACHED) {
      // Reserve failed terminally; the segment cannot be continued.
      return;
    }
    // Otherwise loop: re-check downloadedSize >= needBytes.
  }
}

// Builds the in-region segment views from the holder, in list order.
std::vector<FileSegmentPtr> collectSegments(
    const FileSegmentsHolderPtr& holder) {
  VELOX_CHECK_NOT_NULL(holder, "FileCacheInputStream requires a holder");
  std::vector<FileSegmentPtr> segments;
  for (const auto& segment : *holder) {
    segments.push_back(segment);
  }
  return segments;
}

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
    FileSegmentsHolderPtr holder,
    uint64_t regionOffset,
    uint64_t regionLength,
    memory::MemoryPool& pool,
    std::shared_ptr<ReadFile> readFile,
    FileCache* cache,
    std::shared_ptr<facebook::velox::io::IoStatistics> ioStats)
    : holder_{std::move(holder)},
      segments_{collectSegments(holder_)},
      regionOffset_{regionOffset},
      regionLength_{regionLength},
      pool_{pool},
      readFile_{std::move(readFile)},
      cache_{cache},
      ioStats_{std::move(ioStats)} {
  VELOX_CHECK(!segments_.empty(), "FileCacheInputStream requires >=1 segment");
  VELOX_CHECK_NOT_NULL(cache_, "FileCacheInputStream requires a FileCache");
  VELOX_CHECK_NOT_NULL(readFile_, "FileCacheInputStream requires a ReadFile");
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
  const uint64_t segStart = segment->range().left;
  // Range-relative bytes the cache must hold to cover this slice, and its
  // absolute end offset in the source file.
  const uint64_t needed = segmentOffset + length;
  const uint64_t targetEnd = segStart + needed;

  if (segment->getDownloadedSize() >= needed) {
    // Coverage-based cache hit (CH canStartFromCache = current_write_offset >
    // offset): the slice is already on disk even if the segment is only
    // PARTIALLY_DOWNLOADED. Count one hit plus the slice bytes served from the
    // local cache file (Layer A ssdRead -- FileCache is disk-backed).
    cache_->recordHit();
    if (ioStats_ != nullptr) {
      ioStats_->ssdRead().increment(length);
    }
  } else {
    // Miss: drive the prefix download synchronously. downloadSegmentPrefix is
    // the sole synchronous driver -- its holder/wait/recheck loop handles
    // EMPTY/PARTIAL/DOWNLOADING back-off (become downloader and resume, or wait
    // for whoever holds it), so no separate wait-gate is needed here. It
    // records the miss + downloaded bytes + Layer A read/prefetch internally.
    downloadSegmentPrefix(segment, readFile_, cache_, ioStats_, targetEnd);
    VELOX_CHECK_GE(
        segment->getDownloadedSize(),
        needed,
        "FileCacheInputStream: segment has fewer downloaded bytes than "
        "required; writer abandoned the download (downloadedSize: {}, "
        "needed: {})",
        segment->getDownloadedSize(),
        needed);
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
