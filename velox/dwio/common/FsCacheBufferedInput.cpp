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

#include "velox/dwio/common/FsCacheBufferedInput.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/FsCacheInputStream.h"

#include <algorithm>
#include <limits>
#include <vector>

namespace facebook::velox::dwio::common {

namespace {

// SeekableInputStream returned by FsCacheBufferedInput::enqueue() BEFORE the
// caller has invoked load(). Real bytes are not available yet because the
// FileSegments are populated during load(); DeferredStream therefore wraps an
// inner FsCacheInputStream that is constructed lazily.
//
// Pre-load behaviour (no inner_ yet):
//   - SkipInt64 accumulates into bytesConsumed_, clamped to region length so
//     ByteCount() stays monotonic and bounded.
//   - ByteCount() reports bytesConsumed_.
//   - BackUp is invalid (no buffer to back up over) and asserts.
//   - Any call that needs real bytes (Next, seekToPosition) materializes
//     inner_ and replays bytesConsumed_ via SkipInt64.
//
// DeferredStream is NOT thread-safe; the SeekableInputStream contract assumes
// single-threaded access. slot_ is shared with the owning FsCacheBufferedInput
// only via the happens-before of load() being called on the reader thread
// before any stream method.
class DeferredStream final : public SeekableInputStream {
 public:
  DeferredStream(
      FsCacheBufferedInput::EnqueuedRegion* slot,
      cache::fs::FsCache* cache)
      : slot_{slot}, cache_{cache} {}

  bool Next(const void** data, int32_t* size) override {
    ensureWithData();
    if (bypassActive_) {
      if (bytesConsumed_ >= slot_->bypassBuffer.size()) {
        return false;
      }
      const uint64_t remaining =
          slot_->bypassBuffer.size() - bytesConsumed_;
      // Clamp to INT32_MAX so callers drain via multiple Next() calls if
      // a bypass region exceeds 2 GiB. The default disabled threshold
      // makes this unreachable in practice, but a misconfigured threshold
      // would otherwise silently truncate the returned size.
      const uint64_t chunk = std::min<uint64_t>(
          remaining,
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
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
    if (bypassActive_) {
      const uint64_t newPos = std::min<uint64_t>(
          slot_->bypassBuffer.size(),
          bytesConsumed_ + static_cast<uint64_t>(count));
      const bool fits =
          newPos == bytesConsumed_ + static_cast<uint64_t>(count);
      bytesConsumed_ = newPos;
      return fits;
    }
    if (inner_ != nullptr) {
      return inner_->SkipInt64(count);
    }
    const auto unsignedCount = static_cast<uint64_t>(count);
    // Clamp at region length so post-load ByteCount() matches inner_'s bounds.
    const uint64_t newPos = std::min<uint64_t>(
        slot_->region.length, bytesConsumed_ + unsignedCount);
    const bool fits = newPos == bytesConsumed_ + unsignedCount;
    bytesConsumed_ = newPos;
    return fits;
  }

  int64_t ByteCount() const override {
    if (bypassActive_ || inner_ == nullptr) {
      return static_cast<int64_t>(bytesConsumed_);
    }
    return inner_->ByteCount();
  }

  void seekToPosition(PositionProvider& position) override {
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
    return "FsCacheBufferedInput::DeferredStream";
  }

  size_t positionSize() const override {
    return 1;
  }

 private:
  // Materializes inner_ from slot_->holder->segments() (load() must have run)
  // and replays any pre-load SkipInt64 so inner_'s position matches what
  // ByteCount() has been reporting. If the cache bypassed this region
  // (holder->empty()), instead activates the bypass branch which serves
  // bytes directly from slot_->bypassBuffer populated by load().
  void ensureWithData() {
    if (inner_ != nullptr || bypassActive_) {
      return;
    }
    VELOX_CHECK(
        slot_->holder != nullptr,
        "Stream used before FsCacheBufferedInput::load()");
    if (slot_->holder->empty()) {
      bypassActive_ = true;
      VELOX_CHECK_EQ(
          slot_->bypassBuffer.size(),
          slot_->region.length,
          "Bypass buffer size must match region length");
      return;
    }
    inner_ = std::make_unique<FsCacheInputStream>(
        slot_->holder->segments(),
        slot_->region.offset,
        slot_->region.length,
        cache_->config().cacheRoot);
    if (bytesConsumed_ > 0) {
      const bool ok = inner_->SkipInt64(static_cast<int64_t>(bytesConsumed_));
      VELOX_CHECK(ok, "Replaying pre-load skip past region end");
    }
  }

  FsCacheBufferedInput::EnqueuedRegion* const slot_;
  cache::fs::FsCache* const cache_;
  std::unique_ptr<FsCacheInputStream> inner_;
  // Skip distance accumulated while inner_ is still null; replayed onto
  // inner_ the first time we need real bytes. On the bypass path this is
  // the cursor into slot_->bypassBuffer.
  uint64_t bytesConsumed_{0};
  // Set once ensureWithData() observes an empty holder; switches every
  // override to serve bytes from slot_->bypassBuffer instead of inner_.
  bool bypassActive_{false};
};

} // namespace

FsCacheBufferedInput::FsCacheBufferedInput(
    std::shared_ptr<ReadFile> readFile,
    memory::MemoryPool& pool,
    cache::fs::FsCache* fsCache)
    : BufferedInput(std::move(readFile), pool), fsCache_{fsCache} {
  VELOX_CHECK_NOT_NULL(fsCache_, "FsCacheBufferedInput requires an FsCache");
}

std::unique_ptr<SeekableInputStream> FsCacheBufferedInput::enqueue(
    velox::common::Region region,
    const StreamIdentifier* /*sid*/) {
  enqueuedRegions_.push_back(EnqueuedRegion{region, nullptr, {}});
  return std::make_unique<DeferredStream>(&enqueuedRegions_.back(), fsCache_);
}

void FsCacheBufferedInput::load(LogType /*unused*/) {
  for (auto& enqueued : enqueuedRegions_) {
    if (enqueued.holder != nullptr) {
      continue;
    }
    enqueued.holder = fsCache_->getOrSet(
        input_->getName(),
        enqueued.region.offset,
        enqueued.region.length,
        fsCache_->config(),
        *input_->getReadFile(),
        // load() submits the actual download to DownloadThreadPool and
        // returns immediately, so from the cache's accounting perspective
        // this work is asynchronous prefetch. The reader synchronises
        // through FsCacheInputStream::waitForDownloadedSize, not here.
        cache::fs::IsPrefetch::kPrefetch);

    if (enqueued.holder->empty()) {
      // Cache bypassed this region (size >= bypassThresholdBytes). Read
      // the full region directly from remote into the slot-owned buffer;
      // DeferredStream serves bytes from there. One pread keeps the
      // network cost at one HTTP range per region, matching CH's
      // setReadUntilPosition(file_segment.range().right + 1) path
      // (src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp).
      enqueued.bypassBuffer.assign(enqueued.region.length, '\0');
      input_->getReadFile()->pread(
          enqueued.region.offset,
          enqueued.region.length,
          enqueued.bypassBuffer.data());
      continue;
    }

    for (auto& seg : enqueued.holder->segments()) {
      if (seg->state() == cache::fs::FileSegment::State::kDownloaded) {
        fsCache_->recordHit(seg.get(), cache::fs::IsPrefetch::kPrefetch);
        continue;
      }
      if (seg->state() != cache::fs::FileSegment::State::kEmpty) {
        // Another driver is mid-download. The reader will block in
        // FsCacheInputStream::loadCurrentSegmentBuffer via
        // waitForDownloadedSize, so load() does not need to wait here.
        fsCache_->recordHit(seg.get(), cache::fs::IsPrefetch::kPrefetch);
        continue;
      }
      fsCache_->evict(seg->key().size);
      if (!seg->reserve(seg->key().size, fsCache_->config().cacheRoot)) {
        if (seg->state() == cache::fs::FileSegment::State::kDownloaded) {
          // Warm-restart short-circuit inside reserve() found the file on
          // disk and published it directly.
          fsCache_->recordMiss(
              seg.get(),
              seg->key().size,
              cache::fs::IsPrefetch::kPrefetch);
        } else {
          // Lost the reserve() race. The reader will sync via
          // waitForDownloadedSize; just count this as a hit on the
          // in-flight bytes.
          fsCache_->recordHit(seg.get(), cache::fs::IsPrefetch::kPrefetch);
        }
        continue;
      }
      // Move the per-segment work onto the download pool. The closure
      // captures shared_ptrs (segCapture, readFile) and the FsCache raw
      // pointer (its lifetime exceeds this BufferedInput, see
      // QueryCtx::fsCache_). load() returns as soon as the reserve loop
      // finishes; readers synchronise via FileSegment::waitForDownloadedSize.
      auto segCapture = seg;
      auto readFile = input_->getReadFile();
      auto* cache = fsCache_;
      fsCache_->downloadPool().submit(
          [segCapture, readFile, cache]() mutable {
            try {
              constexpr uint64_t kChunk = 1UL << 20;
              std::vector<char> buf(
                  std::min<uint64_t>(kChunk, segCapture->key().size));
              uint64_t remaining = segCapture->key().size;
              uint64_t cursor = segCapture->key().offset;
              while (remaining > 0) {
                const uint64_t toRead =
                    std::min<uint64_t>(buf.size(), remaining);
                readFile->pread(cursor, toRead, buf.data());
                segCapture->write(buf.data(), toRead);
                cursor += toRead;
                remaining -= toRead;
              }
              segCapture->complete();
              cache->recordMiss(
                  segCapture.get(),
                  segCapture->key().size,
                  cache::fs::IsPrefetch::kPrefetch);
            } catch (...) {
              // No re-throw: the task runs detached on the pool. The
              // abandon() call moves the segment to kPartiallyDownloaded
              // and notifies cv_, so a reader blocked in
              // waitForDownloadedSize wakes up and surfaces the failure
              // to its query.
              segCapture->abandon();
            }
          });
    }
  }
}

bool FsCacheBufferedInput::isBuffered(
    uint64_t /*offset*/,
    uint64_t /*length*/) const {
  // Must return false: the contract of isBuffered() is "the region is already
  // resident and can be read without a subsequent load()". FsCache's enqueue()
  // only registers the region; load() is what populates segments. Returning
  // true would let StructColumnReader::loadRowGroup take the fast path that
  // skips load(), and DeferredStream would then fail with "Stream used before
  // FsCacheBufferedInput::load()".
  return false;
}

std::unique_ptr<BufferedInput> FsCacheBufferedInput::clone() const {
  return std::make_unique<FsCacheBufferedInput>(
      input_->getReadFile(), *pool_, fsCache_);
}

void FsCacheBufferedInput::cacheRegion(
    uint64_t /*offset*/,
    uint64_t /*length*/,
    std::string_view /*data*/) {
  VELOX_UNSUPPORTED(
      "FsCacheBufferedInput::cacheRegion: phase 1 is read-path only; "
      "external write-through into FsCache is not implemented yet");
}

void FsCacheBufferedInput::cacheRegion(
    uint64_t /*offset*/,
    uint64_t /*length*/,
    const folly::IOBuf& /*buffer*/,
    uint64_t /*bufferOffset*/) {
  VELOX_UNSUPPORTED(
      "FsCacheBufferedInput::cacheRegion(IOBuf): phase 1 is read-path only; "
      "external write-through into FsCache is not implemented yet");
}

std::optional<CachedRegion> FsCacheBufferedInput::findCachedRegion(
    uint64_t /*offset*/) const {
  VELOX_UNSUPPORTED(
      "FsCacheBufferedInput::findCachedRegion: phase 1 is read-path only; "
      "external cache lookup against FsCache is not implemented yet");
}

} // namespace facebook::velox::dwio::common
