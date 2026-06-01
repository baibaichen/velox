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

#pragma once

#include "velox/buffer/Buffer.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch {

class FileCache;

class FileCacheInputStream final
    : public facebook::velox::dwio::common::SeekableInputStream {
 public:
  FileCacheInputStream(
      FileSegmentsHolderPtr holder,
      uint64_t regionOffset,
      uint64_t regionLength,
      memory::MemoryPool& pool,
      std::shared_ptr<ReadFile> readFile,
      FileCache* cache,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStats);

  bool Next(const void** data, int32_t* size) override;
  void BackUp(int32_t count) override;
  bool SkipInt64(int64_t count) override;
  int64_t ByteCount() const override;
  void seekToPosition(
      facebook::velox::dwio::common::PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() const override;

 private:
  // Ensures the working buffer covers absolute region offset `cursor` (which
  // must be < regionLength_): fills buf_ with up to kBufferSize bytes starting
  // at `cursor`, either from the durable cache file (already-downloaded prefix)
  // or, at the download frontier, straight from the just-downloaded bytes.
  void fillBuffer(uint64_t cursor);

  // Index of the segment slice covering absolute region offset `cursor`
  // (cursor must be < regionLength_).
  size_t segmentIndexFor(uint64_t cursor) const;

  // Lazily opens (and caches) the on-disk cache file backing segment `index`.
  LocalReadFile& cacheFileFor(size_t index);

  // Owns the segments holder so the stream is self-contained and may outlive
  // the FileCacheBufferedInput that created it (mirrors CH
  // ReadInfo::file_segments). Destruction runs completeAndPopFront / background
  // tail-fill at end of read.
  const FileSegmentsHolderPtr holder_;
  // Views into holder_, in region order; sized/indexed by the slice tables
  // below.
  const std::vector<FileSegmentPtr> segments_;
  const uint64_t regionOffset_;
  const uint64_t regionLength_;
  memory::MemoryPool& pool_;
  // Source file + cache + per-query IO counters, used by fillBuffer to drive
  // the lazy download and classify hits/misses (consume-time accounting).
  const std::shared_ptr<ReadFile> readFile_;
  FileCache* const cache_;
  const std::shared_ptr<facebook::velox::io::IoStatistics> ioStats_;

  // Prefix sums of per-segment slice lengths within the region; size
  // segments_.size()+1, front()==0, back()==regionLength_.
  std::vector<uint64_t> sliceStartInRegion_;
  // Offset of each segment's slice within its on-disk segment file.
  std::vector<uint64_t> sliceOffsetInSegment_;
  // Per-slice "already recorded a hit/miss in this stream" guards, so a slice
  // served across several working buffers still counts as one hit/one miss
  // (matches the one-event-per-segment-slice accounting of the old whole-slice
  // load). Sized segments_.size().
  std::vector<uint8_t> sliceHitRecorded_;
  std::vector<uint8_t> sliceMissRecorded_;

  // Single reused contiguous working buffer (CH working_buffer): holds up to
  // kBufferSize bytes of the region, describing the absolute region window
  // [bufStartInRegion_, bufStartInRegion_ + bufLen_). Next() serves a pointer
  // into it; the next fill overwrites it (ZeroCopyInputStream contract).
  BufferPtr buf_;
  uint64_t bufStartInRegion_{0};
  uint64_t bufLen_{0};

  // Lazily opened cache file for the slice currently backing buf_ reads.
  std::unique_ptr<LocalReadFile> cacheFile_;
  size_t cacheFileIndex_{std::numeric_limits<size_t>::max()};

  // Absolute consumed position within the region: [0, regionLength_].
  uint64_t regionCursor_{0};
};

} // namespace facebook::velox::ch
