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

#include "velox/common/caching/fscache/FsCacheGuards.h"

#include "velox/common/base/Exceptions.h"

#include <vector>

namespace facebook::velox::cache::fs {

#ifndef NDEBUG
namespace {
thread_local std::vector<LockRank> heldRanks;
} // namespace

void LockOrderChecker::onAcquire(LockRank rank) {
  if (!heldRanks.empty()) {
    VELOX_CHECK_GT(
        static_cast<int>(rank),
        static_cast<int>(heldRanks.back()),
        "FsCache lock order violation");
  }
  heldRanks.push_back(rank);
}

void LockOrderChecker::onRelease(LockRank rank) {
  VELOX_CHECK(!heldRanks.empty(), "Releasing lock with no held lock");
  VELOX_CHECK_EQ(
      static_cast<int>(heldRanks.back()),
      static_cast<int>(rank),
      "FsCache lock release out of order");
  heldRanks.pop_back();
}
#endif

} // namespace facebook::velox::cache::fs
