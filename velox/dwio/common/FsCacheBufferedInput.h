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

#include "velox/common/caching/fscache/FileSegmentsHolder.h"
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/dwio/common/BufferedInput.h"

#include <list>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace facebook::velox::dwio::common {

/// BufferedInput subclass backed by FsCache. Bypasses AsyncDataCache /
/// SsdCache entirely.
///
/// Phase 1 implements the read path only (enqueue + load + clone).
/// hasCache() returns true so callers (e.g. MetadataCache) know not to
/// duplicate raw-byte caching; however cacheRegion() / findCachedRegion()
/// are explicitly overridden to throw VELOX_UNSUPPORTED with a phase-1
/// message. Inheriting the base default would also throw, but its message
/// ("requires a backing cache") is misleading once hasCache()==true.
/// A later phase will implement these so external pre-fetchers can populate
/// FsCache.
class FsCacheBufferedInput final : public BufferedInput {
 public:
  /// fsCache must outlive this BufferedInput; ownership is held by the caller
  /// (typically the connector / reader factory).
  FsCacheBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool,
      cache::fs::FsCache* fsCache);

  std::unique_ptr<SeekableInputStream> enqueue(
      velox::common::Region region,
      const StreamIdentifier* sid = nullptr) override;

  void load(LogType logType) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  std::unique_ptr<BufferedInput> clone() const override;

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

  std::optional<CachedRegion> findCachedRegion(uint64_t offset) const override;

  // Public so anonymous-namespace helpers in the .cpp (e.g. DeferredStream)
  // can name the type without befriending the class. Not part of the API
  // surface clients should rely on.
  struct EnqueuedRegion {
    velox::common::Region region;
    // Populated by load(); nullptr before load() runs.
    cache::fs::FileSegmentsHolderPtr holder;
    // Populated by Task 12's load() when getOrSet returns an empty holder
    // (size >= bypassThresholdBytes). In Tasks 8-11 the holder is always
    // non-empty, so this field stays unused/empty.
    std::vector<char> bypassBuffer;
  };

 private:
  cache::fs::FsCache* const fsCache_;
  // std::list (not std::vector) so DeferredStream's EnqueuedRegion* stays
  // valid across subsequent enqueue() calls -- vector reallocation on
  // push_back would invalidate pointers held by previously-returned streams.
  std::list<EnqueuedRegion> enqueuedRegions_;
};

} // namespace facebook::velox::dwio::common
