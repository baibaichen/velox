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

#include <cstdint>
#include <vector>

namespace facebook::velox::cache::fs {

class FileSegment;

/// Abstract eviction policy. FsCache invokes onInsert when a new segment
/// becomes kDownloaded, onHit when an existing segment is read, onRemove
/// when a segment is permanently dropped, and selectVictims to pick segments
/// to evict when bytes are needed.
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  /// Starts tracking a newly downloaded segment. The segment pointer must
  /// remain valid until a matching onRemove call.
  virtual void onInsert(FileSegment* segment) = 0;

  /// Records a read against an already-tracked segment. No-op if the segment
  /// is unknown to the policy.
  virtual void onHit(FileSegment* segment) = 0;

  /// Stops tracking a segment. No-op if the segment is unknown.
  virtual void onRemove(FileSegment* segment) = 0;

  /// Returns segments whose combined size is at least bytesNeeded, ordered
  /// least valuable first (the order in which they should be evicted).
  /// Returns fewer segments — possibly empty — if the policy holds less than
  /// bytesNeeded total.
  virtual std::vector<FileSegment*> selectVictims(uint64_t bytesNeeded) = 0;
};

} // namespace facebook::velox::cache::fs
