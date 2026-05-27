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
#include "velox/common/caching/fscache/LruPolicy.h"

#include <cstdint>
#include <unordered_map>

namespace facebook::velox::cache::fs {

/// Segmented LRU eviction policy, CH-aligned. Holds two LRU sub-queues —
/// probationary (new insertions) and protected (entries that have been hit
/// at least once since download). Capacity is size-split by `protectedRatio`
/// (mirrors CH SLRUFileCachePriority.cpp:45-52; default ratio 0.6 matches
/// CH FileCache_fwd.h:26 FILECACHE_DEFAULT_SLRU_RATIO).
///
/// Promotion semantics (CH SLRUFileCachePriority.cpp:586-611): the first hit
/// on a probationary entry promotes it to protected. There is no hit counter.
/// If protected has no room, the LRU end of protected is demoted back to
/// probationary MRU (CH SLRUFileCachePriority.cpp:398-584) — demotion moves,
/// it does not evict.
///
/// Eviction (CH SLRUFileCachePriority.cpp:213-247, is_total_space_cleanup
/// ordering): selectVictims drains probationary LRU first; if more bytes are
/// still needed, drains protected LRU next.
///
/// Thread safety: not internally synchronized. Callers must hold the
/// appropriate FsCacheGuards lock — same contract as LruPolicy.
class SlruPolicy final : public EvictionPolicy {
 public:
  /// `capacity` is the per-bucket byte budget; protected gets
  /// `capacity * protectedRatio`, probationary gets the remainder.
  SlruPolicy(uint64_t capacity, double protectedRatio);

  void onInsert(FileSegment* segment) override;
  void onHit(FileSegment* segment) override;
  void onRemove(FileSegment* segment) override;
  std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) override;

  /// Observability — mirrors CH's getProtectedSize / getProbationarySize
  /// at SLRUFileCachePriority.h:35-38. Used by tests and stats.
  uint64_t probationaryBytes() const {
    return probationaryBytes_;
  }
  uint64_t protectedBytes() const {
    return protectedBytes_;
  }

 private:
  // Two LRU sub-queues, mirroring CH's nested LRUFileCachePriority members
  // at SLRUFileCachePriority.h:138-139. Each LruPolicy already implements
  // the full EvictionPolicy contract; SlruPolicy dispatches based on
  // isProtected_.
  LruPolicy probationary_;
  LruPolicy protected_;

  // Side index recording which sub-queue holds each segment. CH stores this
  // on the iterator inside the entry; we keep a parallel map because
  // LruPolicy does not expose its index.
  std::unordered_map<FileSegment*, bool> isProtected_;

  // Per-sub-queue byte counters. Used to decide when to demote on promotion
  // (mirrors CH's size accounting at SLRUFileCachePriority.cpp:398-584).
  uint64_t probationaryBytes_{0};
  uint64_t protectedBytes_{0};

  // Per-sub-queue capacity caps derived from `capacity * ratio`. Only the
  // protected cap drives behavior — exceeding it triggers demotion on
  // promotion. Probationary cap is informational; eviction is driven by
  // FsCache asking for `bytesNeeded`, not by the cap.
  const uint64_t protectedCap_;
  const uint64_t probationaryCap_;
};

} // namespace facebook::velox::cache::fs
