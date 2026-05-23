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

#include "velox/common/caching/fscache/EvictionPolicy.h"

#include <list>
#include <unordered_map>

namespace facebook::velox::cache::fs {

/// LRU eviction policy. Front of the list is most recently used; back is
/// least recently used and is evicted first. All operations are O(1) via an
/// iterator side index.
///
/// Thread safety: not internally synchronized. Callers must hold the
/// appropriate FsCacheGuards lock (typically CacheMetadataGuard) before
/// invoking any method.
class LruPolicy final : public EvictionPolicy {
 public:
  void onInsert(FileSegment* segment) override;
  void onHit(FileSegment* segment) override;
  void onRemove(FileSegment* segment) override;
  std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) override;

 private:
  std::list<FileSegment*> mruToLru_;
  std::unordered_map<FileSegment*, std::list<FileSegment*>::iterator> index_;
};

} // namespace facebook::velox::cache::fs
