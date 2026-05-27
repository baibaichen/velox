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

#include "velox/common/caching/fscache/SlruPolicy.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"

namespace facebook::velox::cache::fs {

SlruPolicy::SlruPolicy(uint64_t capacity, double protectedRatio)
    : protectedCap_{static_cast<uint64_t>(capacity * protectedRatio)},
      probationaryCap_{capacity - static_cast<uint64_t>(capacity * protectedRatio)} {
  // Ratio must be in (0, 1). CH validates the same range at
  // SLRUFileCachePriority.cpp:45-52.
  VELOX_CHECK_GT(protectedRatio, 0.0);
  VELOX_CHECK_LT(protectedRatio, 1.0);
}

void SlruPolicy::onInsert(FileSegment* segment) {
  VELOX_CHECK_NOT_NULL(segment);
  // New entries always go to probationary in the steady state — mirrors CH
  // SLRUFileCachePriority.cpp:126-161 default branch. LruPolicy::onInsert
  // enforces the kDownloaded precondition for us.
  probationary_.onInsert(segment);
  isProtected_[segment] = false;
  probationaryBytes_ += segment->size();
}

void SlruPolicy::onHit(FileSegment* segment) {
  auto it = isProtected_.find(segment);
  if (it == isProtected_.end()) {
    // Race tolerance: segment was evicted but caller did not notice yet.
    // Mirrors LruPolicy::onHit's silent no-op on unknown segments.
    return;
  }
  if (it->second) {
    // Already in protected — reorder within protected. Mirrors CH
    // SLRUFileCachePriority.cpp:597-601.
    protected_.onHit(segment);
    return;
  }
  // In probationary — promote to protected on this hit. Mirrors CH
  // SLRUFileCachePriority.cpp:613 onward; no hit counter.
  const uint64_t segmentSize = segment->size();
  probationary_.onRemove(segment);
  probationaryBytes_ -= segmentSize;

  // If protected has no room, demote its LRU end back to probationary MRU
  // until the new entry fits. Demotion is a move, not an evict —
  // CH SLRUFileCachePriority.cpp:398-584 collectCandidatesForEvictionInProtected.
  while (protectedBytes_ + segmentSize > protectedCap_) {
    auto victims = protected_.selectVictims(1);
    if (victims.empty()) {
      break;
    }
    FileSegment* demoted = victims.front();
    const uint64_t demotedSize = demoted->size();
    protected_.onRemove(demoted);
    protectedBytes_ -= demotedSize;
    probationary_.onInsert(demoted);
    isProtected_[demoted] = false;
    probationaryBytes_ += demotedSize;
  }

  protected_.onInsert(segment);
  it->second = true;
  protectedBytes_ += segmentSize;
}

void SlruPolicy::onRemove(FileSegment* segment) {
  auto it = isProtected_.find(segment);
  if (it == isProtected_.end()) {
    return;
  }
  const uint64_t segmentSize = segment->size();
  if (it->second) {
    protected_.onRemove(segment);
    protectedBytes_ -= segmentSize;
  } else {
    probationary_.onRemove(segment);
    probationaryBytes_ -= segmentSize;
  }
  isProtected_.erase(it);
}

std::vector<FileSegment*> SlruPolicy::selectVictims(uint64_t bytesNeeded) {
  // Drain probationary LRU first, then protected — mirrors CH's
  // is_total_space_cleanup ordering at SLRUFileCachePriority.cpp:213-247.
  std::vector<FileSegment*> victims = probationary_.selectVictims(bytesNeeded);
  uint64_t accumulated{0};
  for (auto* victim : victims) {
    accumulated += victim->size();
  }
  if (accumulated >= bytesNeeded) {
    return victims;
  }
  auto protectedVictims = protected_.selectVictims(bytesNeeded - accumulated);
  victims.insert(
      victims.end(), protectedVictims.begin(), protectedVictims.end());
  return victims;
}

} // namespace facebook::velox::cache::fs
