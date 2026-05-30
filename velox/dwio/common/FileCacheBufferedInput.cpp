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

#include "velox/dwio/common/FileCacheBufferedInput.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/FileCacheInputStream.h"
#include "velox/dwio/common/ReadFileByteInputStream.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace facebook::velox::ch {

namespace {

constexpr size_t kReserveTimeoutMs{10000};

class DeferredStream final
    : public facebook::velox::dwio::common::SeekableInputStream {
 public:
  explicit DeferredStream(FileCacheBufferedInput::EnqueuedRegion* slot)
      : slot_{slot} {}

  bool Next(const void** data, int32_t* size) override {
    ensureWithData();
    if (bypassActive_) {
      if (bytesConsumed_ >= slot_->bypassBuffer.size()) {
        return false;
      }
      const uint64_t remaining = slot_->bypassBuffer.size() - bytesConsumed_;
      const uint64_t chunk = std::min<uint64_t>(
          remaining, static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
      *data = slot_->bypassBuffer.data() + bytesConsumed_;
      *size = static_cast<int32_t>(chunk);
      bytesConsumed_ += chunk;
      return true;
    }
    return inner_->Next(data, size);
  }

  void BackUp(int32_t count) override {
    if (bypassActive_) {
      VELOX_CHECK_GE(count, 0);
      VELOX_CHECK_LE(static_cast<uint64_t>(count), bytesConsumed_);
      bytesConsumed_ -= static_cast<uint64_t>(count);
      return;
    }
    VELOX_CHECK_NOT_NULL(
        inner_, "BackUp called before any Next() -- no buffer to back up");
    inner_->BackUp(count);
  }

  bool SkipInt64(int64_t count) override {
    if (count < 0) {
      return false;
    }
    const auto unsignedCount = static_cast<uint64_t>(count);
    if (bypassActive_) {
      return advanceClamped(slot_->bypassBuffer.size(), unsignedCount);
    }
    if (inner_ != nullptr) {
      return inner_->SkipInt64(count);
    }
    return advanceClamped(slot_->region.length, unsignedCount);
  }

  int64_t ByteCount() const override {
    if (bypassActive_ || inner_ == nullptr) {
      return static_cast<int64_t>(bytesConsumed_);
    }
    return inner_->ByteCount();
  }

  void seekToPosition(
      facebook::velox::dwio::common::PositionProvider& position) override {
    ensureWithData();
    if (bypassActive_) {
      const uint64_t target = position.next();
      VELOX_CHECK_LE(target, slot_->bypassBuffer.size());
      bytesConsumed_ = target;
      return;
    }
    inner_->seekToPosition(position);
  }

  std::string getName() const override {
    return "FileCacheBufferedInput::DeferredStream";
  }

  size_t positionSize() const override {
    return 1;
  }

 private:
  // Advances bytesConsumed_ by count, clamped to limit; returns whether the
  // full count fit before reaching limit.
  bool advanceClamped(uint64_t limit, uint64_t count) {
    const uint64_t newPos = std::min<uint64_t>(limit, bytesConsumed_ + count);
    const bool fits = newPos == bytesConsumed_ + count;
    bytesConsumed_ = newPos;
    return fits;
  }

  void ensureWithData() {
    if (inner_ != nullptr || bypassActive_) {
      return;
    }
    VELOX_CHECK(
        slot_->holder != nullptr,
        "Stream used before FileCacheBufferedInput::load()");
    if (slot_->bypass) {
      bypassActive_ = true;
      VELOX_CHECK_EQ(
          slot_->bypassBuffer.size(),
          slot_->region.length,
          "Bypass buffer size must match region length");
      return;
    }

    std::vector<FileSegmentPtr> segments;
    for (const auto& segment : *slot_->holder) {
      segments.push_back(segment);
    }
    inner_ = std::make_unique<FileCacheInputStream>(
        std::move(segments), slot_->region.offset, slot_->region.length);
    if (bytesConsumed_ > 0) {
      const bool ok = inner_->SkipInt64(static_cast<int64_t>(bytesConsumed_));
      VELOX_CHECK(ok, "Replaying pre-load skip past region end");
    }
  }

  FileCacheBufferedInput::EnqueuedRegion* const slot_;
  std::unique_ptr<FileCacheInputStream> inner_;
  uint64_t bytesConsumed_{0};
  bool bypassActive_{false};
};

// Downloads the prefix [getCurrentWriteOffset, targetEnd) of `segment` from
// `readFile`, guaranteeing the segment durably covers `targetEnd` before
// returning (or that it has reached a terminal, non-resumable state).
//
// Several readers can share one segment while requesting different prefixes, and
// FileCacheInputStream only waits for this pre-download task -- it never drives
// the download itself. A task that loses the downloader race must therefore
// still ensure its own `targetEnd` is satisfied: an in-flight task that captured
// a shallower target could otherwise complete first and strand a deeper reader
// (which the old whole-segment download never did). Hence the retry loop:
// become the downloader and resume the gap, or wait for whoever holds it and
// re-check, until `targetEnd` is met or the segment is abandoned.
void downloadSegmentPrefix(
    const FileSegmentPtr& segment,
    const std::shared_ptr<ReadFile>& readFile,
    FileCache* cache,
    const std::shared_ptr<facebook::velox::io::IoStatistics>& ioStats,
    uint64_t targetEnd) {
  const uint64_t segStart = segment->range().left;
  VELOX_CHECK_GT(targetEnd, segStart);
  // Required downloadedSize (range-relative) for the segment to cover targetEnd.
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

    // Won the downloader. Another task may have satisfied the target between the
    // loop test and the acquisition; re-check before counting a miss or writing.
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
      // downloadFromReader resumes from the current write offset, so a partially
      // downloaded segment continues rather than restarting. On a reserve
      // failure it stops after moving the segment to a terminal state.
      const uint64_t writeOffset = segment->getCurrentWriteOffset();
      if (targetEnd > writeOffset) {
        auto reader = std::make_shared<ReadFileByteInputStream>(readFile);
        std::vector<char> scratch;
        const size_t written = FileSegment::downloadFromReader(
            *segment, *reader, targetEnd - writeOffset, scratch,
            kReserveTimeoutMs);
        // Bytes fetched from source and written into the cache (Layer B
        // cumulative + Layer A: read() = source bytes, prefetch() = bytes pulled
        // ahead into the cache, mirroring CachedBufferedInput).
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

} // namespace

FileCacheBufferedInput::FileCacheBufferedInput(
    std::shared_ptr<ReadFile> readFile,
    memory::MemoryPool& pool,
    FileCache* fileCache,
    FileCacheDownloadExecutor* executor,
    FileCacheKey key,
    FileCache::OriginInfo origin,
    CreateFileSegmentSettings createSettings,
    std::shared_ptr<facebook::velox::io::IoStatistics> ioStats)
    : BufferedInput(std::move(readFile), pool),
      fileCache_{fileCache},
      executor_{executor},
      key_{key},
      origin_{std::move(origin)},
      createSettings_{createSettings},
      ioStats_{std::move(ioStats)},
      fileSize_{input_->getLength()} {
  VELOX_CHECK_NOT_NULL(fileCache_, "FileCacheBufferedInput requires FileCache");
  VELOX_CHECK_NOT_NULL(
      executor_, "FileCacheBufferedInput requires FileCacheDownloadExecutor");
}

std::unique_ptr<facebook::velox::dwio::common::SeekableInputStream>
FileCacheBufferedInput::enqueue(
    facebook::velox::common::Region region,
    const facebook::velox::dwio::common::StreamIdentifier* /*sid*/) {
  enqueuedRegions_.push_back(EnqueuedRegion{region, nullptr, false, {}});
  return std::make_unique<DeferredStream>(&enqueuedRegions_.back());
}

void FileCacheBufferedInput::load(
    facebook::velox::dwio::common::LogType /*logType*/) {
  // Pass 1: resolve holders for newly enqueued regions and handle DETACHED
  // bypass. Collect the regions resolved by THIS call so a repeated load() does
  // not re-submit downloads for regions that were already resolved earlier.
  std::vector<EnqueuedRegion*> pending;
  for (auto& enqueued : enqueuedRegions_) {
    if (enqueued.holder != nullptr) {
      continue;
    }

    enqueued.holder = fileCache_->getOrSet(
        key_,
        enqueued.region.offset,
        enqueued.region.length,
        fileSize_,
        createSettings_,
        0,
        origin_,
        std::nullopt);

    for (const auto& segment : *enqueued.holder) {
      if (segment->state() == FileSegment::State::DETACHED) {
        enqueued.bypass = true;
        break;
      }
    }
    if (enqueued.bypass) {
      // Region-level bypass for DETACHED segments returned by getOrSet().
      enqueued.bypassBuffer.resize(enqueued.region.length);
      input_->getReadFile()->pread(
          enqueued.region.offset,
          enqueued.region.length,
          enqueued.bypassBuffer.data());
      // Bypass is intentionally non-cacheable, so it is excluded from the
      // cache hit/miss ratio; only surface its source bytes (Layer A).
      if (ioStats_ != nullptr) {
        ioStats_->read().increment(enqueued.region.length);
      }
      continue;
    }
    pending.push_back(&enqueued);
  }

  // Pass 2: classify each newly resolved region's segments. Already-downloaded
  // segments are cache hits; every other segment is folded into one coalesced
  // download target.
  //
  // Coalescing: a segment can be shared by several regions; the foreground
  // download must cover the furthest byte any of them needs, because
  // FileCacheInputStream only waits for this pre-download task and fails if the
  // segment reaches a terminal state with too few bytes -- it never drives the
  // download itself. The target is the absolute end offset min(segmentEnd,
  // regionEnd), maximized across the regions sharing the segment. Keep the
  // FileSegmentPtr alongside the target so the async task owns it (the task may
  // outlive the holder).
  struct SegmentDownload {
    FileSegmentPtr segment;
    uint64_t targetEnd{0};
  };
  std::unordered_map<FileSegment*, SegmentDownload> segmentDownloads;
  for (auto* enqueued : pending) {
    const uint64_t regStart = enqueued->region.offset;
    const uint64_t regEnd = regStart + enqueued->region.length;
    for (const auto& segment : *enqueued->holder) {
      const uint64_t segStart = segment->range().left;
      const uint64_t segEnd = segStart + segment->range().size();
      if (segment->state() == FileSegment::State::DOWNLOADED) {
        // Cache hit: bytes are already on disk and will be served from the local
        // cache file. Record one hit (Layer B) plus the overlapping bytes served
        // from cache (Layer A, ssdRead -- FileCache is disk-backed). Hit
        // accounting is per (region, segment) because a hit means the bytes a
        // given region needs are already served from the local cache file.
        fileCache_->recordHit();
        if (ioStats_ != nullptr) {
          const uint64_t lo = std::max(segStart, regStart);
          const uint64_t hi = std::min(segEnd, regEnd);
          if (hi > lo) {
            ioStats_->ssdRead().increment(hi - lo);
          }
        }
        continue;
      }
      auto& entry = segmentDownloads[segment.get()];
      entry.segment = segment;
      entry.targetEnd = std::max(entry.targetEnd, std::min(segEnd, regEnd));
    }
  }

  // Pass 3: submit one coalesced prefix download per distinct miss segment.
  // Submitting a single task per segment (rather than one per region) means no
  // two tasks from this load() contend for the same segment's downloader.
  auto readFile = input_->getReadFile();
  auto* cache = fileCache_;
  auto ioStats = ioStats_;
  for (auto& entry : segmentDownloads) {
    auto segment = entry.second.segment;
    const uint64_t targetEnd = entry.second.targetEnd;
    (void)executor_->submit(
        [segment, readFile, cache, ioStats, targetEnd]() mutable {
          downloadSegmentPrefix(segment, readFile, cache, ioStats, targetEnd);
        });
  }
}

bool FileCacheBufferedInput::isBuffered(
    uint64_t /*offset*/,
    uint64_t /*length*/) const {
  return false;
}

std::unique_ptr<facebook::velox::dwio::common::BufferedInput>
FileCacheBufferedInput::clone() const {
  return std::make_unique<FileCacheBufferedInput>(
      input_->getReadFile(),
      *pool_,
      fileCache_,
      executor_,
      key_,
      origin_,
      createSettings_,
      ioStats_);
}

void FileCacheBufferedInput::cacheRegion(
    uint64_t /*offset*/,
    uint64_t /*length*/,
    std::string_view /*data*/) {
  VELOX_UNSUPPORTED(
      "FileCacheBufferedInput::cacheRegion: phase-1 read-path only; "
      "external write-through/lookup not implemented");
}

void FileCacheBufferedInput::cacheRegion(
    uint64_t /*offset*/,
    uint64_t /*length*/,
    const folly::IOBuf& /*buffer*/,
    uint64_t /*bufferOffset*/) {
  VELOX_UNSUPPORTED(
      "FileCacheBufferedInput::cacheRegion(IOBuf): phase-1 read-path only; "
      "external write-through/lookup not implemented");
}

std::optional<facebook::velox::dwio::common::CachedRegion>
FileCacheBufferedInput::findCachedRegion(uint64_t /*offset*/) const {
  VELOX_UNSUPPORTED(
      "FileCacheBufferedInput::findCachedRegion: phase-1 read-path only; "
      "external write-through/lookup not implemented");
}

} // namespace facebook::velox::ch
