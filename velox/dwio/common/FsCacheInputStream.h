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

#include "velox/common/caching/fscache/FsCacheMetadata.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace facebook::velox::dwio::common {

/// Reads bytes from a sequence of kDownloaded FileSegments covering a
/// contiguous file region. Implements the SeekableInputStream contract with a
/// per-segment in-memory buffer: each Next() call returns the remaining slice
/// of the current segment's buffer, advancing to the next segment when the
/// current one is exhausted.
class FsCacheInputStream final : public SeekableInputStream {
 public:
  /// All segments must be in state kDownloaded and together cover
  /// [regionOffset, regionOffset + regionLength). cacheRoot is the same root
  /// the segments were downloaded into.
  FsCacheInputStream(
      std::vector<cache::fs::FileSegmentPtr> segments,
      uint64_t regionOffset,
      uint64_t regionLength,
      std::string cacheRoot);

  bool Next(const void** data, int32_t* size) override;
  void BackUp(int32_t count) override;
  bool SkipInt64(int64_t count) override;
  int64_t ByteCount() const override;
  void seekToPosition(PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() const override;

 private:
  // Reads the intersection of [regionOffset_, regionOffset_+regionLength_)
  // with segments_[index_] into buffer_ and resets cursor_ to 0.
  void loadCurrentSegmentBuffer();

  const std::vector<cache::fs::FileSegmentPtr> segments_;
  const uint64_t regionOffset_;
  const uint64_t regionLength_;
  const std::string cacheRoot_;

  // Index of the segment whose bytes currently sit in buffer_.
  size_t index_{0};
  // Total bytes returned to callers via Next() (less any BackUp / pre-Next
  // SkipInt64). Reported by ByteCount().
  uint64_t byteCount_{0};
  // Buffer holding the slice of the current segment that intersects the
  // requested region.
  std::string buffer_;
  // Offset within buffer_ of the next byte to deliver from Next().
  size_t cursor_{0};
};

} // namespace facebook::velox::dwio::common
