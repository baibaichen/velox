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

#include "velox/common/caching/filecache/FileCache_fwd_internal.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch {

class FileCacheInputStream final
    : public facebook::velox::dwio::common::SeekableInputStream {
 public:
  FileCacheInputStream(
      std::vector<FileSegmentPtr> segments,
      uint64_t regionOffset,
      uint64_t regionLength);

  bool Next(const void** data, int32_t* size) override;
  void BackUp(int32_t count) override;
  bool SkipInt64(int64_t count) override;
  int64_t ByteCount() const override;
  void seekToPosition(
      facebook::velox::dwio::common::PositionProvider& position) override;
  std::string getName() const override;
  size_t positionSize() const override;

 private:
  void loadCurrentSegmentBuffer();

  const std::vector<FileSegmentPtr> segments_;
  const uint64_t regionOffset_;
  const uint64_t regionLength_;

  size_t index_{0};
  uint64_t byteCount_{0};
  std::unique_ptr<char[]> buffer_;
  size_t bufferSize_{0};
  size_t cursor_{0};
};

} // namespace facebook::velox::ch
