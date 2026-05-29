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

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/FileCacheOriginInfo.h"
#include "velox/common/caching/filecache/FileCache_fwd_internal.h"
#include "velox/common/caching/filecache/FileSegmentInfo.h"
#include "velox/common/caching/filecache/Guards.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>

namespace facebook::velox::ch {
struct FileCacheReserveStat;
class EvictionCandidates;
class EvictionInfo;
using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;
struct CacheUsageStatGuard;

class IFileCachePriority : private boost::noncopyable {
 public:
  using Key = FileCacheKey;
  using QueueEntryType = FileCacheQueueEntryType;
  using OriginInfo = FileCacheOriginInfo;
  using UserID = OriginInfo::UserID;

  struct Entry {
    const Key key;
    const size_t offset;
    const KeyMetadataPtr keyMetadata;

    std::atomic<size_t> size;
    std::atomic<size_t> hits = 0;

    std::string toString(const std::string& prefix = "") const;

    enum class State {
      Active,
      /// Temporary state used only in SLRU during queue transitions (downgrade from
      /// protected to probationary, or upgrade from probationary to protected).
      /// The new entry is added to the destination queue with this state and transitions
      /// to Active atomically together with the SLRUIterator's inner pointer update in
      /// `SLRUIterator::setIterator`. This ensures that `iterateImpl`, which checks entry
      /// state before evicting, never sees the entry as evictable until the SLRUIterator
      /// already points to it: a concurrent iteration that observes Active state will
      /// always find this entry (not the previous queue entry) when it calls getEntry().
      PreActive,
      /// Entry is collected for eviction via IFileCachePriority::collectEvictionCandidates
      /// and is being or soon will be removed from filesystem.
      Evicting,
      /// Can only be set in SLRU eviciton policy during moves
      /// in between protected/probationary queues.
      Moving,
      /// Has size 0, will never get non-zero size and must soon be removed from queue.
      Invalidated,
      /// Removed from queue completely.
      Removed,
    };

    Entry(
        const Key& key_,
        size_t offset_,
        size_t size_,
        KeyMetadataPtr keyMetadata_,
        State initialState = State::Active);
    Entry(const Entry& other);

    State getState() const {
      return state.load();
    }

    /// Transitions PreActive → Active. Must be called inside `SLRUIterator::entry_mutex`
    /// together with the pointer update so that `getEntry()` and state visibility are atomic.
    void setActiveFlag(const CacheStateGuard::Lock&) {
      [[maybe_unused]] auto prev = state.exchange(State::Active);
      VELOX_DCHECK(
          prev == State::PreActive,
          printUnexpectedState(prev, "PreActive", "Active"));
    }

    void setEvictingFlag(const LockedKey&) {
      [[maybe_unused]] auto prev = state.exchange(State::Evicting);
      VELOX_DCHECK(
          prev == State::Active,
          printUnexpectedState(prev, "Active", "Evicting"));
    }

    void setMovingFlag(const LockedKey&) {
      [[maybe_unused]] auto prev = state.exchange(State::Moving);
      VELOX_DCHECK(
          prev == State::Active,
          printUnexpectedState(prev, "Active", "Moving"));
    }

    void setRemoved(const CachePriorityGuard::WriteLock&) {
      [[maybe_unused]] auto prev = state.exchange(State::Removed);
      VELOX_DCHECK(
          prev == State::Active || prev == State::Evicting ||
              prev == State::Invalidated,
          printUnexpectedState(
              prev, "Active or Evicting or Invalidated", "Removed"));
    }

    void setInvalidatedFlag() {
      [[maybe_unused]] auto prev = state.exchange(State::Invalidated);
      /// Active in case of FileCache::remove
      /// Evicting in case of FileCache::tryReserve
      /// Moving in case of SLRU queue moves
      /// PreActive in case of exception during SLRU queue transition
      VELOX_DCHECK(
          prev == State::Active || prev == State::Evicting ||
              prev == State::Moving || prev == State::PreActive,
          printUnexpectedState(
              prev,
              "Active or Moving or Evicting or PreActive",
              "Invalidated"));
    }

    void resetFlag(State fromState, State toState = State::Active) {
      [[maybe_unused]] auto prev = state.exchange(toState);
      VELOX_DCHECK(
          prev == fromState,
          printUnexpectedState(
              prev, stateName(fromState), fmt::format("{}", stateName(toState))));
    }

   private:
    static std::string_view stateName(State state) {
      switch (state) {
        case State::Active:
          return "Active";
        case State::PreActive:
          return "PreActive";
        case State::Evicting:
          return "Evicting";
        case State::Moving:
          return "Moving";
        case State::Invalidated:
          return "Invalidated";
        case State::Removed:
          return "Removed";
      }
      return {};
    }

    std::string printUnexpectedState(
        State prevState,
        std::string_view expectedState,
        std::string type) const {
      return fmt::format(
          "Previous state is {}, but expected state to be {} while setting {} flag for {}",
          stateName(prevState),
          expectedState,
          type,
          toString());
    }

    std::atomic<State> state = State::Active;
  };
  using EntryPtr = std::shared_ptr<Entry>;

  class Iterator {
   public:
    virtual ~Iterator() = default;

    virtual EntryPtr getEntry() const = 0;

    /// Note: IncrementSize unlike decrementSize requires a cache lock, because
    /// it requires more consistency guarantees for eviction.

    virtual void incrementSize(size_t size, const CacheStateGuard::Lock&) = 0;

    virtual void decrementSize(size_t size) = 0;

    virtual bool isValid(const CachePriorityGuard::WriteLock&) const = 0;

    virtual void remove(const CachePriorityGuard::WriteLock&) = 0;

    virtual void invalidate() = 0;

    virtual QueueEntryType getType() const = 0;

    virtual const Iterator* getNestedOrThis() const {
      return this;
    }
    virtual Iterator* getNestedOrThis() {
      return this;
    }

    virtual void check(const CacheStateGuard::Lock&) const {}
  };
  using IteratorPtr = std::shared_ptr<Iterator>;

  struct InvalidatedEntryInfo {
    /// Iterator becomes invalid when entry is removed
    /// so we also save the entry here to be able to check validity of the iterator.
    IFileCachePriority::EntryPtr entry;
    IFileCachePriority::IteratorPtr iterator;
  };
  using InvalidatedEntriesInfos = std::vector<InvalidatedEntryInfo>;

  virtual ~IFileCachePriority() = default;

  enum class Type {
    LRU,
    SLRU,
    LRU_OVERCOMMIT,
    SLRU_OVERCOMMIT,
  };
  virtual Type getType() const = 0;

  size_t getSizeLimit(const CacheStateGuard::Lock&) const {
    return maxSize;
  }
  size_t getSizeLimitApprox() const {
    return maxSize.load(std::memory_order_relaxed);
  }

  size_t getElementsLimit(const CacheStateGuard::Lock&) const {
    return maxElements;
  }
  size_t getElementsLimitApprox() const {
    return maxElements.load(std::memory_order_relaxed);
  }

  virtual size_t getSize(const CacheStateGuard::Lock&) const = 0;
  virtual size_t getSizeApprox() const = 0;

  virtual size_t getElementsCount(const CacheStateGuard::Lock&) const = 0;
  virtual size_t getElementsCountApprox() const = 0;

  virtual bool isOvercommitEviction() const {
    return false;
  }
  virtual double getSLRUSizeRatio() const {
    return 0;
  }

  virtual std::string getStateInfoForLog(const CacheStateGuard::Lock&) const = 0;
  /// Check correctness of cache state.
  virtual void check(const CacheStateGuard::Lock&) const;

  virtual EvictionInfoPtr collectEvictionInfo(
      size_t size,
      size_t elements,
      IFileCachePriority::Iterator* reservee,
      bool isTotalSpaceCleanup,
      const IFileCachePriority::OriginInfo& origin,
      const CacheStateGuard::Lock&) = 0;

  enum class IterationResult : uint8_t {
    BREAK,
    CONTINUE,
  };

  using IterateFunc =
      std::function<IterationResult(LockedKey&, const FileSegmentMetadataPtr&)>;
  virtual void iterate(
      IterateFunc func,
      FileCacheReserveStat& stat,
      const CachePriorityGuard::ReadLock&) = 0;

  /// Throws exception if there is not enough size to fit it.
  virtual IteratorPtr add( /// NOLINT
      KeyMetadataPtr keyMetadata,
      size_t offset,
      size_t size,
      const CachePriorityGuard::WriteLock&,
      const CacheStateGuard::Lock*,
      bool isInitialLoad = false) = 0;

  /// Restore a previously removed entry back to the queue it came from.
  /// `original_queue_type` is the `QueueEntryType` the entry had before removal.
  /// Default implementation ignores the hint and delegates to `add`.
  /// SLRU overrides this to route protected entries back to the protected queue.
  virtual IteratorPtr addForRestore( /// NOLINT
      KeyMetadataPtr keyMetadata,
      size_t offset,
      size_t size,
      QueueEntryType /* originalQueueType */,
      const CachePriorityGuard::WriteLock& lock,
      const CacheStateGuard::Lock* stateLock) {
    return add(keyMetadata, offset, size, lock, stateLock, false);
  }

  /// `reservee` is the entry for which are reserving now.
  /// It does not exist, if it is the first space reservation attempt
  /// for the corresponding file segment.
  virtual bool canFit( /// NOLINT
      size_t size,
      size_t elements,
      const CacheStateGuard::Lock&,
      IteratorPtr reservee = nullptr,
      const OriginInfo& originInfo = {},
      bool isInitialLoad = false) const = 0;

  virtual bool tryIncreasePriority(
      Iterator& iterator,
      bool isSpaceReservationComplete,
      CachePriorityGuard& queueGuard,
      CacheStateGuard& stateGuard) = 0;

  virtual void shuffle(const CachePriorityGuard::WriteLock&) = 0;

  struct IPriorityDump {
    std::vector<FileSegmentInfo> infos;
    IPriorityDump() = default;
    explicit IPriorityDump(const std::vector<FileSegmentInfo>& infos_)
        : infos(infos_) {}
    void merge(const IPriorityDump& other) {
      infos.insert(infos.end(), other.infos.begin(), other.infos.end());
    }
    virtual ~IPriorityDump() = default;
  };

  using PriorityDumpPtr = std::shared_ptr<IPriorityDump>;

  virtual PriorityDumpPtr dump(const CachePriorityGuard::ReadLock&) = 0;

  /// Collect eviction candidates sufficient to free `size` bytes
  /// and `elements` elements from cache.
  virtual bool collectCandidatesForEviction(
      const EvictionInfo& evictionInfo,
      FileCacheReserveStat& stat,
      EvictionCandidates& res,
      InvalidatedEntriesInfos& invalidatedEntries,
      IteratorPtr reservee,
      bool continueFromLastEvictionPos,
      size_t maxCandidatesSize,
      bool isTotalSpaceCleanup,
      const OriginInfo& originInfo,
      CachePriorityGuard&,
      CacheStateGuard&) = 0;

  /// Collect eviction candidates sufficient to have `desired_size`
  /// and `desired_elements_num` as current cache state.
  /// Collect no more than `max_candidates_to_evict` elements.
  /// Return SUCCESS status if the first condition is satisfied.
  enum class CollectStatus {
    SUCCESS,
    CANNOT_EVICT,
    REACHED_MAX_CANDIDATES_LIMIT,
  };

  virtual bool modifySizeLimits(
      size_t maxSize_,
      size_t maxElements_,
      double sizeRatio_,
      const CacheStateGuard::Lock&) = 0;

  /// Compute eviction info needed to resize the cache to the given limits.
  /// Unlike collectEvictionInfo which takes total amounts to evict,
  /// this method takes desired limits and computes per-sub-queue eviction
  /// correctly for priority types with internal structure (e.g., SLRU).
  virtual EvictionInfoPtr collectEvictionInfoForResize(
      size_t desiredMaxSize,
      size_t desiredMaxElements,
      const OriginInfo& originInfo,
      const CacheStateGuard::Lock& lock) = 0;

  virtual void resetEvictionPos() = 0;

  /// Remove given queue entries for the queue.
  /// Used to cleanup invalidated queue entries.
  static void removeEntries(
      const std::vector<InvalidatedEntryInfo>& entries,
      const CachePriorityGuard::WriteLock&);

  struct UsageStat {
    size_t size;
    size_t elements;
  };
  virtual std::unordered_map<std::string, UsageStat> getUsageStatPerClient();

  class HoldSpace;
  using HoldSpacePtr = std::unique_ptr<HoldSpace>;
  /// A space holder implementation, which allows to take hold of
  /// some space in cache given that this space was freed.
  /// Takes hold of the space in constructor and releases it in destructor.
  class HoldSpace : private boost::noncopyable {
   public:
    HoldSpace(
        size_t size_,
        size_t elements_,
        IFileCachePriority& priority_,
        const CacheStateGuard::Lock& lock)
        : size(size_), elements(elements_), priority(priority_) {
      priority.holdImpl(size, elements, lock);
    }

    size_t getSize() const {
      return size;
    }

    size_t getElements() const {
      return elements;
    }

    void release(const CacheStateGuard::Lock&) {
      releaseUnlocked();
    }

    void merge(HoldSpacePtr other) {
      size += other->size;
      elements += other->elements;
      other->size = other->elements = 0;
    }

    ~HoldSpace() {
      if (!released)
        releaseUnlocked();
    }

   private:
    size_t size;
    size_t elements;
    IFileCachePriority& priority;
    bool released = false;

    void releaseUnlocked() {
      if (released || (!size && !elements))
        return;
      released = true;
      priority.releaseImpl(size, elements);
      size = elements = 0;
    }
  };

  virtual size_t getHoldSize() = 0;

  virtual size_t getHoldElements() = 0;

  virtual void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard>) {}

 protected:
  IFileCachePriority(size_t maxSize_, size_t maxElements_);

  virtual void holdImpl(
      size_t /* size */,
      size_t /* elements */,
      const CacheStateGuard::Lock&) {}
  /// No lock is required in releaseImpl unlike holdImpl,
  /// because for releasing hold space we do not need strong guarantees.
  virtual void releaseImpl(size_t /* size */, size_t /* elements */) {}

  std::atomic<size_t> maxSize = 0;
  std::atomic<size_t> maxElements = 0;
};

using IFileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;

} // namespace facebook::velox::ch
