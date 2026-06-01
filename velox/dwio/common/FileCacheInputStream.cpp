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
#include "velox/dwio/common/ReadFileByteInputStream.h"

#include <algorithm>
#include <memory>
#include <vector>

namespace facebook::velox::ch {

namespace {
// CH-style working-buffer cap: the stream serves at most this many bytes per
// Next() and fills its reusable buffer in units of this size.
constexpr uint64_t kBufferSize = 1ULL << 20;

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
    uint64_t targetEnd,
    FileSegment::DownloadTee* tee = nullptr,
    bool* missGuard = nullptr) {
  const uint64_t segStart = segment->range().left;
  VELOX_CHECK_GT(targetEnd, segStart);
  // Required downloadedSize (range-relative) for the segment to cover
  // targetEnd.
  const uint64_t needBytes = targetEnd - segStart;
  // A slice served across several working buffers must count as a single miss;
  // missGuard carries that "already recorded" state across the per-buffer calls
  // (a null guard falls back to once-per-call dedup, used by callers that drive
  // a whole slice in one shot).
  bool recordedMiss = (missGuard != nullptr) ? (*missGuard != 0) : false;

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
      if (missGuard != nullptr) {
        *missGuard = 1;
      }
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
            kReserveTimeoutMs,
            tee);
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
  sliceHitRecorded_.assign(segments_.size(), 0);
  sliceMissRecorded_.assign(segments_.size(), 0);
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

LocalReadFile& FileCacheInputStream::cacheFileFor(size_t index) {
  if (cacheFileIndex_ != index) {
    cacheFile_ = std::make_unique<LocalReadFile>(segments_[index]->getPath());
    cacheFileIndex_ = index;
  }
  return *cacheFile_;
}

void FileCacheInputStream::fillBuffer(uint64_t cursor) {
  const size_t index = segmentIndexFor(cursor);
  const auto& segment = segments_[index];
  const uint64_t segStart = segment->range().left;
  const uint64_t sliceStart = sliceStartInRegion_[index];
  const uint64_t sliceLen = sliceStartInRegion_[index + 1] - sliceStart;
  const uint64_t offsetInSlice = cursor - sliceStart;
  // The cache file stores a segment's bytes at range-relative offsets, so a
  // slice byte at offsetInSlice lives at cache-file/range offset
  // segmentOffset + offsetInSlice.
  const uint64_t offsetInSegment = sliceOffsetInSegment_[index] + offsetInSlice;

  // One working buffer never crosses the 1MiB cap or the slice boundary; it is
  // further capped below at the downloaded/frontier boundary so its bytes come
  // from a single source (all cache file, or all just-downloaded memory).
  uint64_t step = std::min<uint64_t>(kBufferSize, sliceLen - offsetInSlice);

  if (buf_ == nullptr) {
    const uint64_t capacity = std::min<uint64_t>(kBufferSize, regionLength_);
    buf_ = AlignedBuffer::allocate<char>(capacity, &pool_);
  }
  char* dst = buf_->asMutable<char>();

  const uint64_t downloaded = segment->getDownloadedSize();
  if (offsetInSegment < downloaded) {
    // Already-downloaded prefix: serve this buffer from the durable cache file
    // (Layer A ssdRead). Cap the buffer at the frontier so it stays single
    // source; the next fill picks up at the frontier.
    step = std::min<uint64_t>(step, downloaded - offsetInSegment);
    if (sliceHitRecorded_[index] == 0) {
      // Coverage-based cache hit (CH canStartFromCache): the slice prefix is on
      // disk even if the segment is only PARTIALLY_DOWNLOADED. Count it once per
      // slice in this stream.
      cache_->recordHit();
      sliceHitRecorded_[index] = 1;
    }
    const auto got = cacheFileFor(index).pread(offsetInSegment, step, dst);
    VELOX_CHECK_EQ(got.size(), step, "short pread of cache segment slice");
    if (ioStats_ != nullptr) {
      ioStats_->ssdRead().increment(step);
    }
  } else {
    // At or beyond the download frontier: drive the prefix download to cover
    // [offsetInSegment, offsetInSegment + step), teeing the requested window
    // straight into the working buffer so the just-downloaded bytes are served
    // from memory (CH hands the written working_buffer back) rather than
    // re-read from the cache file. A forward seek past the frontier downloads
    // the intervening gap too; the tee copies only the requested window. The
    // download accounting (miss + downloaded/read/prefetch) happens inside
    // downloadSegmentPrefix.
    const uint64_t targetEnd = segStart + offsetInSegment + step;
    FileSegment::DownloadTee tee{segStart + offsetInSegment, step, dst, 0};
    bool missGuard = sliceMissRecorded_[index] != 0;
    downloadSegmentPrefix(
        segment, readFile_, cache_, ioStats_, targetEnd, &tee, &missGuard);
    sliceMissRecorded_[index] = missGuard ? 1 : 0;
    VELOX_CHECK_GE(
        segment->getDownloadedSize(),
        offsetInSegment + step,
        "FileCacheInputStream: segment has fewer downloaded bytes than "
        "required; writer abandoned the download (downloadedSize: {}, "
        "needed: {})",
        segment->getDownloadedSize(),
        offsetInSegment + step);
    if (tee.copied < step) {
      // Race-loser fallback: another thread wrote (part of) this window while we
      // waited, so it was not teed into memory. The bytes are now durable; read
      // them from the cache file instead (Layer A ssdRead).
      const auto got = cacheFileFor(index).pread(offsetInSegment, step, dst);
      VELOX_CHECK_EQ(got.size(), step, "short pread of cache segment slice");
      if (ioStats_ != nullptr) {
        ioStats_->ssdRead().increment(step);
      }
    }
  }

  bufStartInRegion_ = cursor;
  bufLen_ = step;
}

bool FileCacheInputStream::Next(const void** data, int32_t* size) {
  if (regionCursor_ >= regionLength_) {
    *size = 0;
    return false;
  }
  if (regionCursor_ < bufStartInRegion_ ||
      regionCursor_ >= bufStartInRegion_ + bufLen_) {
    fillBuffer(regionCursor_);
  }
  const uint64_t posInBuf = regionCursor_ - bufStartInRegion_;
  const uint64_t chunk = bufLen_ - posInBuf;
  *data = buf_->as<char>() + posInBuf;
  *size = static_cast<int32_t>(chunk);
  regionCursor_ += chunk;
  return true;
}

void FileCacheInputStream::BackUp(int32_t count) {
  VELOX_CHECK_GE(count, 0);
  const uint64_t unsignedCount = static_cast<uint64_t>(count);
  // Backing up is bounded to the bytes served from the current working buffer
  // (everything between its start and the cursor), mirroring the
  // ZeroCopyInputStream contract that BackUp follows a single Next().
  VELOX_CHECK_LE(
      unsignedCount,
      regionCursor_ - bufStartInRegion_,
      "Can't backup that much!");
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
