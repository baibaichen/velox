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
#include "velox/common/caching/filecache/IFileCachePriority.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/EvictionCandidates.h"

namespace facebook::velox::ch {

namespace {

std::string_view typeName(IFileCachePriority::Type type) {
  switch (type) {
    case IFileCachePriority::Type::LRU:
      return "LRU";
    case IFileCachePriority::Type::SLRU:
      return "SLRU";
    case IFileCachePriority::Type::LRU_OVERCOMMIT:
      return "LRU_OVERCOMMIT";
    case IFileCachePriority::Type::SLRU_OVERCOMMIT:
      return "SLRU_OVERCOMMIT";
  }
  return {};
}

std::string_view entryStateName(IFileCachePriority::Entry::State state) {
  switch (state) {
    case IFileCachePriority::Entry::State::Active:
      return "Active";
    case IFileCachePriority::Entry::State::PreActive:
      return "PreActive";
    case IFileCachePriority::Entry::State::Evicting:
      return "Evicting";
    case IFileCachePriority::Entry::State::Moving:
      return "Moving";
    case IFileCachePriority::Entry::State::Invalidated:
      return "Invalidated";
    case IFileCachePriority::Entry::State::Removed:
      return "Removed";
  }
  return {};
}

} // namespace

IFileCachePriority::IFileCachePriority(size_t maxSize_, size_t maxElements_)
    : maxSize(maxSize_), maxElements(maxElements_) {}

IFileCachePriority::Entry::Entry(
    const Key& key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr keyMetadata_,
    State initialState)
    : key(key_),
      offset(offset_),
      keyMetadata(keyMetadata_),
      size(size_),
      state(initialState) {}

IFileCachePriority::Entry::Entry(const Entry& other)
    : key(other.key),
      offset(other.offset),
      keyMetadata(other.keyMetadata),
      size(other.size.load()),
      hits(other.hits.load()) {}

std::string IFileCachePriority::Entry::toString(
    const std::string& prefix) const {
  return fmt::format(
      "{}{}:{}:{} (state: {})",
      prefix,
      key,
      offset,
      size.load(),
      stateName(state.load(std::memory_order_relaxed)));
}

void IFileCachePriority::check(const CacheStateGuard::Lock& lock) const {
  if ((maxSize != 0 && getSize(lock) > maxSize) ||
      (maxElements != 0 && getElementsCount(lock) > maxElements)) {
    VELOX_FAIL("Cache limits violated. {}", getStateInfoForLog(lock));
  }

  if (getSize(lock) > (1ull << 63) ||
      getElementsCount(lock) > (1ull << 63)) {
    VELOX_FAIL("Cache became inconsistent. There must be a bug");
  }
}

std::unordered_map<std::string, IFileCachePriority::UsageStat>
IFileCachePriority::getUsageStatPerClient() {
  VELOX_FAIL(
      "getUsageStatPerClient() is not implemented for {} policy",
      typeName(getType()));
}

void IFileCachePriority::removeEntries(
    const std::vector<InvalidatedEntryInfo>& entries,
    const CachePriorityGuard::WriteLock& lock) {
  if (entries.empty()) {
    return;
  }

  for (const auto& [entry, it] : entries) {
    /// We store `entry` shared pointer in addition to `it`
    /// (which is an iterator pointing to the same entry)
    /// because `it` could become invalid,
    /// so we use `entry` to check validity of the iterator.
    const auto entry_state = entry->getState();
    VELOX_DCHECK(
        entry_state == Entry::State::Invalidated ||
            entry_state == Entry::State::Removed,
        fmt::format("Unexpected state: {}", entryStateName(entry_state)));
    if (entry_state != Entry::State::Removed) {
      it->remove(lock);
    }
  }
}

} // namespace facebook::velox::ch
