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

#include <algorithm>
#include <limits>

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

} // namespace

FileCacheBufferedInput::FileCacheBufferedInput(
    std::shared_ptr<ReadFile> readFile,
    memory::MemoryPool& pool,
    FileCache* fileCache,
    FileCacheDownloadExecutor* executor,
    FileCacheKey key,
    FileCache::OriginInfo origin,
    CreateFileSegmentSettings createSettings)
    : BufferedInput(std::move(readFile), pool),
      fileCache_{fileCache},
      executor_{executor},
      key_{key},
      origin_{std::move(origin)},
      createSettings_{createSettings},
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
      continue;
    }

    for (const auto& segment : *enqueued.holder) {
      if (segment->state() == FileSegment::State::DOWNLOADED) {
        continue;
      }

      auto readFile = input_->getReadFile();
      (void)executor_->submit([segment, readFile]() mutable {
        const auto downloaderId = segment->getOrSetDownloader();
        if (downloaderId != FileSegment::getCallerId()) {
          return;
        }

        try {
          constexpr uint64_t kChunk = 1u << 20;
          std::vector<char> buffer;
          while (segment->getDownloadedSize() < segment->range().size()) {
            const uint64_t already = segment->getDownloadedSize();
            // Continue partial downloads at the absolute file offset expected by write().
            const uint64_t cursor = segment->range().left + already;
            const uint64_t remaining = segment->range().size() - already;
            const uint64_t toRead = std::min<uint64_t>(kChunk, remaining);
            std::string failureReason;
            if (!segment->reserve(
                    toRead, kReserveTimeoutMs, failureReason, nullptr)) {
              // TODO(bypass-on-reserve-failure): CH reads the tail remotely.
              segment->completePartAndResetDownloader();
              return;
            }
            if (buffer.size() < toRead) {
              buffer.resize(toRead);
            }
            readFile->pread(cursor, toRead, buffer.data());
            segment->write(buffer.data(), toRead, cursor);
          }
          segment->completePartAndResetDownloader();
        } catch (...) {
          segment->setDownloadFailed();
          segment->completePartAndResetDownloader();
        }
      });
    }
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
      createSettings_);
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
