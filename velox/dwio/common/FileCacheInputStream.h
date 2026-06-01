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

#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Allocation.h"
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
  // Loads the segment slice at `index` into reused storage (segData_/tinyData_),
  // waiting for the download to cover the slice first.
  void loadSegment(size_t index);

  // Ensures the byte at regionCursor_ is resident and sets the run_/runSize_/
  // offsetInRun_ view onto it, loading the owning segment if needed.
  void loadPosition();

  // Index of the segment slice covering absolute region offset `cursor`
  // (cursor must be < regionLength_).
  size_t segmentIndexFor(uint64_t cursor) const;

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
  // Source file + cache + per-query IO counters, used by loadSegment to drive
  // the lazy download and classify hits/misses (consume-time accounting).
  const std::shared_ptr<ReadFile> readFile_;
  FileCache* const cache_;
  const std::shared_ptr<facebook::velox::io::IoStatistics> ioStats_;

  // Prefix sums of per-segment slice lengths within the region; size
  // segments_.size()+1, front()==0, back()==regionLength_.
  std::vector<uint64_t> sliceStartInRegion_;
  // Offset of each segment's slice within its on-disk segment file.
  std::vector<uint64_t> sliceOffsetInSegment_;

  // Reused destination for the currently loaded segment slice. segData_ holds
  // slices >= kTinySize; tinyData_ holds smaller ones.
  memory::Allocation segData_;
  std::string tinyData_;
  size_t loadedIndex_{std::numeric_limits<size_t>::max()};
  uint64_t loadedSliceLen_{0};

  // Absolute consumed position within the region: [0, regionLength_].
  uint64_t regionCursor_{0};

  // Run view of the loaded slice for the current position.
  uint8_t* run_{nullptr};
  uint32_t runSize_{0};
  uint64_t offsetInRun_{0};
};

} // namespace facebook::velox::ch
