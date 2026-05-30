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

#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheDownloadExecutor.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/dwio/common/BufferedInput.h"

#include <list>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace facebook::velox::ch {

class FileCacheBufferedInput final
    : public facebook::velox::dwio::common::BufferedInput {
 public:
  /// fileCache, executor, and readFile must outlive this BufferedInput and all
  /// download tasks submitted by load(); ownership stays with the caller.
  FileCacheBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      FileCache* fileCache,
      FileCacheDownloadExecutor* executor,
      FileCacheKey key,
      FileCache::OriginInfo origin = FileCache::getInternalOrigin(),
      CreateFileSegmentSettings createSettings = {});

  std::unique_ptr<facebook::velox::dwio::common::SeekableInputStream> enqueue(
      facebook::velox::common::Region region,
      const facebook::velox::dwio::common::StreamIdentifier* sid = nullptr)
      override;

  void load(facebook::velox::dwio::common::LogType logType) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone()
      const override;

  /// Returns true so callers know raw bytes are already cache-backed (by the
  /// ch::FileCache engine) and need not duplicate caching. Note cacheRegion()
  /// and findCachedRegion() still throw VELOX_UNSUPPORTED: this class is a
  /// phase-1 read path only and does not expose Velox's write-through cache
  /// region API.
  bool hasCache() const override {
    return true;
  }

  void cacheRegion(uint64_t offset, uint64_t length, std::string_view data)
      override;

  void cacheRegion(
      uint64_t offset,
      uint64_t length,
      const folly::IOBuf& buffer,
      uint64_t bufferOffset) override;

  std::optional<facebook::velox::dwio::common::CachedRegion> findCachedRegion(
      uint64_t offset) const override;

  struct EnqueuedRegion {
    facebook::velox::common::Region region;
    FileSegmentsHolderPtr holder;
    bool bypass{false};
    std::vector<char> bypassBuffer;
  };

 private:
  FileCache* fileCache_;
  FileCacheDownloadExecutor* executor_;
  FileCacheKey key_;
  FileCache::OriginInfo origin_;
  CreateFileSegmentSettings createSettings_;
  uint64_t fileSize_;
  std::list<EnqueuedRegion> enqueuedRegions_;
};

} // namespace facebook::velox::ch
