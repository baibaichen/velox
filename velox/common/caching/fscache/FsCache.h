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

#include "velox/common/caching/fscache/FsCacheConfig.h"

#include <cstdint>
#include <utility>
#include <vector>

namespace facebook::velox::cache::fs {

class FsCacheKey;
class FileSegment;
class FsCacheMetadata;
class EvictionPolicy;

/// Top-level entry point of the FsCache module. Methods are added in later
/// commits (FsCacheKey first, then EvictionPolicy, then FileSegment, then
/// the public getOrSet API in commit 6).
class FsCache {
 public:
  explicit FsCache(FsCacheConfig config);
  ~FsCache();

  /// Splits an arbitrary [offset, offset + size) range into aligned cache
  /// segments. The outer boundaries are snapped to config.alignment; internal
  /// cuts produced by the maxSegmentSize chunk loop preserve aligned starts
  /// but may have sub-alignment tail size. Each returned (offset, size) is
  /// at most config.maxSegmentSize bytes. Returns an empty vector if size
  /// is zero.
  static std::vector<std::pair<uint64_t, uint64_t>> splitRange(
      uint64_t offset,
      uint64_t size,
      const FsCacheConfig& config);

 private:
  const FsCacheConfig config_;
};

} // namespace facebook::velox::cache::fs
