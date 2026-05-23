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
    return inner_->Next(data, size);
  }

  void BackUp(int32_t count) override {
    VELOX_CHECK_NOT_NULL(
        inner_, "BackUp called before any Next() -- no buffer to back up");
    inner_->BackUp(count);
  }

  bool SkipInt64(int64_t count) override {
    // Skip is allowed pre-load: the caller is positioning the stream without
    // needing bytes yet. Once inner_ exists, forward unchanged.
    if (count < 0) {
      return false;
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
    return inner_ != nullptr
        ? inner_->ByteCount()
        : static_cast<int64_t>(bytesConsumed_);
  }

  void seekToPosition(PositionProvider& position) override {
    ensureWithData();
    inner_->seekToPosition(position);
  }

  std::string getName() const override {
    return "FsCacheBufferedInput::DeferredStream";
  }

  size_t positionSize() const override {
    return 1;
  }

 private:
  // Materializes inner_ from slot_->segments (load() must have run) and
  // replays any pre-load SkipInt64 so inner_'s position matches what
  // ByteCount() has been reporting.
  void ensureWithData() {
    if (inner_ != nullptr) {
      return;
    }
    VELOX_CHECK(
        !slot_->segments.empty(),
        "Stream used before FsCacheBufferedInput::load()");
    inner_ = std::make_unique<FsCacheInputStream>(
        slot_->segments,
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
  // inner_ the first time we need real bytes.
  uint64_t bytesConsumed_{0};
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
  enqueuedRegions_.push_back(EnqueuedRegion{region, {}});
  return std::make_unique<DeferredStream>(&enqueuedRegions_.back(), fsCache_);
}

void FsCacheBufferedInput::load(LogType /*unused*/) {
  for (auto& enqueued : enqueuedRegions_) {
    if (!enqueued.segments.empty()) {
      continue;
    }
    enqueued.segments = fsCache_->getOrSet(
        input_->getName(),
        enqueued.region.offset,
        enqueued.region.length,
        *input_->getReadFile());
  }
}

bool FsCacheBufferedInput::isBuffered(
    uint64_t /*offset*/,
    uint64_t /*length*/) const {
  // Phase 1: trust enqueue + load to bring the segment in. Phase 2 could
  // probe metadata for already-resident segments.
  return true;
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
