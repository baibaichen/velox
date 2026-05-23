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

#include "velox/common/caching/fscache/FsCacheKey.h"

#include <cstdint>
#include <utility>

namespace facebook::velox::cache::fs {

/// Single cache segment. Commit 4 introduces only the key/size/state surface
/// used by EvictionPolicy. Commit 5 extends this class with the full download
/// state machine (atomic transitions, beginDownload, download, read).
class FileSegment {
 public:
  /// Download lifecycle state. kEmpty -> kDownloading -> kDownloaded is the
  /// happy path; kDetached marks a segment that has been removed from the
  /// metadata index but may still be in use by readers.
  enum class State : uint8_t {
    kEmpty = 0,
    kDownloading = 1,
    kDownloaded = 2,
    kDetached = 3,
  };

  /// Constructs a fresh segment. The starting state is kEmpty in commit 5's
  /// extended version; in commit 4 we accept an explicit state so the
  /// EvictionPolicy tests can exercise LruPolicy::onInsert's invariant
  /// (only kDownloaded segments are tracked) without depending on the
  /// download machinery that commit 5 introduces.
  explicit FileSegment(FsCacheKey key, State state = State::kDownloaded)
      : key_{std::move(key)}, state_{state} {}

  /// Returns the cache key (path/offset/size) that identifies this segment.
  const FsCacheKey& key() const {
    return key_;
  }

  /// Returns the segment size in bytes; equal to key().size.
  uint64_t size() const {
    return key_.size;
  }

  /// Returns the current lifecycle state.
  State state() const {
    return state_;
  }

 private:
  FsCacheKey key_;
  State state_;
};

} // namespace facebook::velox::cache::fs
