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

#include "velox/common/caching/fscache/LruPolicy.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FileSegment.h"

namespace facebook::velox::cache::fs {

void LruPolicy::onInsert(FileSegment* segment) {
  VELOX_CHECK_NOT_NULL(segment);
  // Only kDownloaded segments may be tracked; otherwise selectVictims could
  // return a segment whose download is still in-flight and the writer would
  // race with eviction over the on-disk file.
  VELOX_CHECK(
      segment->state() == FileSegment::State::kDownloaded,
      "LruPolicy tracks only kDownloaded segments, state={}",
      static_cast<int>(segment->state()));
  VELOX_CHECK_EQ(index_.count(segment), 0, "Segment already tracked");
  mruToLru_.push_front(segment);
  index_[segment] = mruToLru_.begin();
}

void LruPolicy::onHit(FileSegment* segment) {
  auto it = index_.find(segment);
  if (it == index_.end()) {
    return;
  }
  mruToLru_.erase(it->second);
  mruToLru_.push_front(segment);
  it->second = mruToLru_.begin();
}

void LruPolicy::onRemove(FileSegment* segment) {
  auto it = index_.find(segment);
  if (it == index_.end()) {
    return;
  }
  mruToLru_.erase(it->second);
  index_.erase(it);
}

std::vector<FileSegment*> LruPolicy::selectVictims(uint64_t bytesNeeded) {
  std::vector<FileSegment*> victims;
  uint64_t accumulated{0};
  for (auto rit = mruToLru_.rbegin(); rit != mruToLru_.rend(); ++rit) {
    if (accumulated >= bytesNeeded) {
      break;
    }
    FileSegment* victim = *rit;
    victims.push_back(victim);
    accumulated += victim->size();
  }
  return victims;
}

} // namespace facebook::velox::cache::fs
