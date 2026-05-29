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
#include "velox/common/caching/filecache/EvictionCandidates.h"
#include "velox/common/caching/filecache/Metadata.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <exception>
#include <sstream>
#include <string_view>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch {

namespace {

std::string_view keyStateName(KeyMetadata::KeyState state) {
  // TODO(ch-port): CH formats KeyState through its native formatter/log path;
  // keep the same state value visible in Velox diagnostics.
  switch (state) {
    case KeyMetadata::KeyState::ACTIVE:
      return "ACTIVE";
    case KeyMetadata::KeyState::REMOVING:
      return "REMOVING";
    case KeyMetadata::KeyState::REMOVED:
      return "REMOVED";
  }
  return {};
}

std::string getCurrentExceptionMessage(bool /*with_stacktrace*/) {
  // TODO(ch-port): CH getCurrentExceptionMessage(true) includes ClickHouse
  // stack-trace formatting; Velox equivalent keeps the active exception text.
  try {
    if (auto exception = std::current_exception()) {
      std::rethrow_exception(exception);
    }
  } catch (const VeloxException& e) {
    return e.message();
  } catch (const std::exception& e) {
    return e.what();
  } catch (...) {
    return "Unknown exception";
  }
  return "";
}

} // namespace

void QueueEvictionInfo::releaseHoldSpace(const CacheStateGuard::Lock& lock) {
  if (holdSpace) {
    holdSpace->release(lock);
    holdSpace = {};
  }
}

std::string QueueEvictionInfo::toString() const {
  // TODO(ch-port): CH WriteBufferFromOwnString is represented with
  // std::ostringstream in this Velox port.
  std::ostringstream wb;
  wb << "description: " << description;
  wb << ", "
     << "user: " << userId;
  wb << ", "
     << "size to evict: " << sizeToEvict;
  wb << ", "
     << "elements to evict: " << elementsToEvict;
  if (holdSpace) {
    wb << ", "
       << "hold space size: " << holdSpace->getSize();
    wb << ", "
       << "hold space elements: " << holdSpace->getElements();
  }
  return wb.str();
}

void QueueEvictionInfo::merge(QueueEvictionInfoPtr other) {
  sizeToEvict += other->sizeToEvict;
  elementsToEvict += other->elementsToEvict;
  if (other->holdSpace) {
    if (holdSpace) {
      holdSpace->merge(std::move(other->holdSpace));
    } else {
      holdSpace = std::move(other->holdSpace);
    }
  }
}

EvictionInfo::EvictionInfo(QueueID queue_id, QueueEvictionInfoPtr info) {
  addImpl(queue_id, std::move(info), /* merge_if_exists */ false);
}

std::string EvictionInfo::toString() const {
  // TODO(ch-port): CH WriteBufferFromOwnString is represented with
  // std::ostringstream in this Velox port.
  std::ostringstream wb;
  bool first = true;
  for (const auto& [queue_id, info] : *this) {
    if (!first) {
      wb << ", ";
    }
    first = false;
    wb << "[queue id " << queue_id << ", " << info->toString() << "]";
  }
  return wb.str();
}

bool EvictionInfo::hasHoldSpace() const {
  for (const auto& [_, elem] : *this) {
    if (elem->hasHoldSpace()) {
      return true;
    }
  }
  return false;
}

void EvictionInfo::releaseHoldSpace(const CacheStateGuard::Lock& lock) {
  for (auto& [_, elem] : *this) {
    elem->releaseHoldSpace(lock);
  }
}

void EvictionInfo::add(EvictionInfoPtr&& info) {
  for (auto&& [queue_id, info_] : *info) {
    addImpl(queue_id, std::move(info_), /* merge_if_exists */ false);
  }
}

void EvictionInfo::addOrUpdate(EvictionInfoPtr&& info) {
  for (auto&& [queue_id, info_] : *info) {
    addImpl(queue_id, std::move(info_), /* merge_if_exists */ true);
  }
}

void EvictionInfo::addImpl(
    const QueueID& queue_id,
    QueueEvictionInfoPtr info,
    bool merge_if_exists) {
  sizeToEvict += info->sizeToEvict;
  elementsToEvict += info->elementsToEvict;
  auto [it, inserted] = try_emplace(queue_id, std::move(info));
  if (!inserted) {
    if (!merge_if_exists) {
      VELOX_FAIL("Queue with id {} already exists", queue_id);
    }

    if (it->second) {
      it->second->merge(std::move(info));
    }
  }
}

const QueueEvictionInfo& EvictionInfo::get(const QueueID& queue_id) const {
  if (auto it = find(queue_id); it != end()) {
    return *it->second;
  } else {
    VELOX_FAIL(
        "Eviction info for queue  with id {} does not exist ({})",
        queue_id,
        toString());
  }
}

std::string EvictionCandidates::FailedCandidates::getFirstErrorMessage()
    const {
  if (failedCandidatesPerKey.empty()) {
    return "";
  }

  const auto& first_failed = failedCandidatesPerKey[0];
  if (!first_failed.errorMessages.empty()) {
    return first_failed.errorMessages[0];
  }

  VELOX_DCHECK(false);
  return "";
}

EvictionCandidates::EvictionCandidates() {}

EvictionCandidates::~EvictionCandidates() {
  /// Here `queue_entries_to_invalidate` contains queue entries
  /// for file segments which were successfully removed in evict().
  /// This set is non-empty in destructor only if there was
  /// an exception before we called finalize() or in the middle of finalize().
  VLOG(1) << fmt::format(
      "Will invalidate {} queue entries", queueEntriesToInvalidate.size());
  for (const auto& iterator : queueEntriesToInvalidate) {
    /// In this case we need to finalize the state of queue entries
    /// which correspond to removed files segments to make sure
    /// consistent state of cache.
    iterator->invalidate();
  }

  /// We cannot reset evicting flag if we already removed queue entries.
  if (removedQueueEntries) {
    return;
  }

  /// Here `candidates` contain only those file segments
  /// which failed to be removed during evict()
  /// because there was some exception before evict()
  /// or in the middle of evict().
  for (const auto& [key, key_candidates] : candidates) {
    // Reset the evicting state
    // (as the corresponding file segments were not yet removed).
    for (const auto& candidate : key_candidates.candidates) {
      candidate->resetEvictingFlag();
    }
  }
}

void EvictionCandidates::add(
    const FileSegmentMetadataPtr& candidate,
    LockedKey& locked_key) {
  auto [it, inserted] = candidates.emplace(locked_key.getKey(), KeyCandidates{});
  if (inserted) {
    it->second.keyMetadata = locked_key.getKeyMetadata();
  }

  it->second.candidates.push_back(candidate);
  candidate->setEvictingFlag(locked_key);
  ++candidatesSize;
  candidatesBytes += candidate->size();
}

void EvictionCandidates::removeQueueEntries(
    const CachePriorityGuard::WriteLock& lock) {
  /// Remove queue entries of eviction candidates.
  /// This will release space we consider to be hold for them.

  VLOG(1) << fmt::format("Will remove {} eviction candidates", size());

  for (const auto& [key, key_candidates] : candidates) {
    auto locked_key = key_candidates.keyMetadata->lock();
    for (const auto& candidate : key_candidates.candidates) {
      auto queue_iterator = candidate->getQueueIterator();

      /// Save the inner queue type before invalidation so we can
      /// restore entries to their original queue if eviction fails.
      /// Use getNestedOrThis() to see through SplitIterator and get
      /// the SLRU_Protected/SLRU_Probationary type, not SplitCache_Data/System.
      originalQueueTypes[candidate.get()] =
          queue_iterator->getNestedOrThis()->getType();

      queue_iterator->invalidate();

      VELOX_DCHECK(candidate->releasable());
      candidate->fileSegment->markDelayedRemovalAndResetQueueIterator();

      /// We need to set removed flag in file segment metadata,
      /// because in dynamic cache resize we first remove queue entries,
      /// then evict which also removes file segment metadata,
      /// but we need to make sure that this file segment is not requested from cache in the meantime.
      /// In ordinary eviction we use `evicting` flag for this purpose,
      /// but here we cannot, because `evicting` is a property of a queue entry,
      /// but at this point for dynamic cache resize we have already deleted all queue entries.
      candidate->setRemovedFlag(*locked_key);

      queue_iterator->remove(lock);
    }
  }
  removedQueueEntries = true;
}

void EvictionCandidates::evict() {
  if (candidates.empty()) {
    return;
  }

  // TODO(metric): CH records FilesystemCacheEvictMicroseconds timer here.

  /// If queue entries are already removed, then nothing to invalidate.
  if (!removedQueueEntries) {
    queueEntriesToInvalidate.reserve(candidatesSize);
  }

  for (auto& [key, key_candidates] : candidates) {
    auto locked_key = key_candidates.keyMetadata->tryLock();
    if (!locked_key) {
      /// A key cannot be removed while eviction candidates are still there.
      VELOX_DCHECK(
          false,
          "Failed to lock key {} (state: {}), but had {} eviction candidates from it",
          key,
          keyStateName(key_candidates.keyMetadata->getState()),
          key_candidates.candidates.size());
      continue;
    }

    KeyCandidates failed_key_candidates;
    failed_key_candidates.keyMetadata = key_candidates.keyMetadata;

    while (!key_candidates.candidates.empty()) {
      auto& candidate = key_candidates.candidates.back();
      try {
        if (!candidate->releasable()) {
          VELOX_FAIL(
              "Eviction candidate is not releasable: {} (evicting or removed flag: {})",
              candidate->fileSegment->getInfoForLog(),
              candidate->isEvictingOrRemoved(*locked_key));
        }

        const auto segment = candidate->fileSegment;

        IFileCachePriority::IteratorPtr iterator;
        if (!removedQueueEntries) {
          iterator = segment->getQueueIterator();
          VELOX_DCHECK(iterator);
        }

        // TODO(failpoint): CH FailPoints::file_cache_dynamic_resize_fail_to_evict
        // is not ported in this Velox file-cache subset.

        locked_key->removeFileSegment(
            segment->offset(),
            segment->lock(),
            false /* can_be_broken */,
            false /* invalidate_queue_entry */);

        /// We set invalidate_queue_entry = false in removeFileSegment() above, because:
        ///   evict() is done without a cache priority lock while finalize() is done under the lock.
        ///   In evict() we:
        ///     - remove file segment from filesystem
        ///     - remove it from cache metadata
        ///   In finalize() we:
        ///     - remove corresponding queue entry from priority queue
        ///
        ///   We do not invalidate queue entry now in evict(),
        ///   because invalidation of queue entries needs to be done under cache lock.
        ///   Why? Firstly, as long as queue entry exists,
        ///   the corresponding space in cache is considered to be hold,
        ///   and once queue entry is removed/invalidated - the space is released.
        ///   Secondly, after evict() and finalize() stages we will also add back the
        ///   "reserved size" (<= actually released size),
        ///   but until we do this - we cannot allow other threads to think that
        ///   this released space is free to take, as it is not -
        ///   it was freed in favour of some reserver, so we can make it visibly
        ///   free only for that particular reserver.

        // TODO(metric): CH increments FilesystemCacheEvictedFileSegments here.
        // TODO(metric): CH increments FilesystemCacheEvictedBytes by segment->range().size() here.

        if (iterator) {
          queueEntriesToInvalidate.push_back(iterator);
        }
      } catch (...) {
        /// Sum up reserved size, which is the queue entry size.
        failedCandidates.totalCacheSize += candidate->size();
        failedCandidates.totalCacheElements += 1;
        failed_key_candidates.candidates.push_back(candidate);

        const auto error_message = getCurrentExceptionMessage(true);
        failed_key_candidates.errorMessages.push_back(error_message);

        // TODO(metric): CH increments FilesystemCacheFailedEvictionCandidates here.

        LOG(ERROR) << fmt::format(
            "Failed to evict file segment ({}): {}",
            candidate->fileSegment->getInfoForLog(),
            error_message);
      }

      key_candidates.candidates.pop_back();
    }

    if (!failed_key_candidates.candidates.empty()) {
      failedCandidates.failedCandidatesPerKey.push_back(failed_key_candidates);
    }
  }
}

void EvictionCandidates::afterEvictWrite(
    const CachePriorityGuard::WriteLock& lock) {
  if (afterEvictWriteFunc) {
    afterEvictWriteFunc(lock);
    afterEvictWriteFunc = {};
  }
}

void EvictionCandidates::afterEvictState(const CacheStateGuard::Lock& lock) {
  /// We invalidate queue entries under state lock,
  /// because this space will be replaced by reserver,
  /// so we need to make sure this is done atomically.
  ///
  /// Note: this step is not needed in case of dynamic cache resize,
  ///       because we remove queue entries in advance, before actual eviction.
  while (!queueEntriesToInvalidate.empty()) {
    auto iterator = queueEntriesToInvalidate.back();
    iterator->invalidate();
    iterator->check(lock);
    queueEntriesToInvalidate.pop_back();
  }

  if (afterEvictStateFunc) {
    afterEvictStateFunc(lock);
    afterEvictStateFunc = {};
  }
}

} // namespace facebook::velox::ch
