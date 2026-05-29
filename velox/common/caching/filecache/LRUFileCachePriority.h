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

#include <list>
#include "velox/common/caching/filecache/IFileCachePriority.h"
#include "velox/common/caching/filecache/CacheUsage.h"
#include "velox/common/caching/filecache/Guards.h"


namespace facebook::velox::ch
{

/// Based on the LRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class LRUFileCachePriority : public IFileCachePriority
{
protected:
    class State
    {
    public:
        State() = default;

        size_t getSize(const CacheStateGuard::Lock &) const { return size; }
        size_t getSizeApprox() const { return size.load(std::memory_order_relaxed); }

        size_t getElementsCount(const CacheStateGuard::Lock &) const { return elementsNum; }
        size_t getElementsCountApprox() const { return elementsNum.load(std::memory_order_relaxed); }

        void add(uint64_t size_, uint64_t elements_, const CacheStateGuard::Lock &);
        void sub(uint64_t size_, uint64_t elements_);

    private:
        std::atomic<size_t> size = 0;
        std::atomic<size_t> elementsNum = 0;
    };
    using StatePtr = std::shared_ptr<State>;

public:
    LRUFileCachePriority(
        size_t max_size_,
        size_t max_elements_,
        const std::string & description_ = "none",
        StatePtr state_ = nullptr);

    Type getType() const override { return Type::LRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override { return state->getSize(lock); }
    size_t getSizeApprox() const override { return state->getSizeApprox(); }

    size_t getElementsCount(const CacheStateGuard::Lock & lock) const override { return state->getElementsCount(lock); }
    size_t getElementsCountApprox() const override { return state->getElementsCountApprox(); }

    size_t getQueueID() const { return queueId; }

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin,
        const CacheStateGuard::Lock &) override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        const OriginInfo & origin_info = {},
        bool is_initial_load = false) const override;

    /// Create a queue entry for given key and offset.
    /// Write priority lock is required.
    /// State lock is required only if non-zero size entry is being added.
    /// In most cases, we first add a zero-size queue entry with write priority lock,
    /// then release that lock and take cache state lock
    /// with which we increase size of the newly added zero-size queue entry.
    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool is_initial_load = false) override;

    bool collectCandidatesForEviction(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CachePriorityGuard &,
        CacheStateGuard &) override;

    bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

    void pop(const CachePriorityGuard::WriteLock & lock) { remove(queue.begin(), lock); } // NOLINT

    bool modifySizeLimits(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const CacheStateGuard::Lock &) override;

    EvictionInfoPtr collectEvictionInfoForResize(
        size_t desired_max_size,
        size_t desired_max_elements,
        const OriginInfo & origin_info,
        const CacheStateGuard::Lock & lock) override;

    FileCachePriorityPtr copy() const { return std::make_unique<LRUFileCachePriority>(maxSize, maxElements, description, state); }

    /// See a comment near evictionPos.
    void resetEvictionPos() override
    {
        std::lock_guard lock(evictionPosMutex);
        evictionPos = LRUQueue::iterator{};
    }

    /// Used only for unit test.
    size_t getEvictionPosCount()
    {
        std::lock_guard lock(evictionPosMutex);
        if (evictionPos == LRUQueue::iterator{})
            return 0;
        return std::distance(queue.begin(), evictionPos);
    }

protected:
    void holdImpl(
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock & lock) override;

    void releaseImpl(size_t size, size_t elements) override;

    size_t getHoldSize() override { return totalHoldSize; }

    size_t getHoldElements() override { return totalHoldElements; }

    /// Used to collect stats for a system table (in private).
    void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard> guard) override
    {
        cacheUsageStatGuard = guard;
    }

private:
    class LRUIterator;
    using LRUQueue = std::list<EntryPtr>;
    friend class SLRUFileCachePriority;

    LRUQueue queue;
    const std::string description;
    // TODO(logging): CH LoggerPtr was used only by LOG_* call sites; map .cpp logs to glog.
    StatePtr state;
    /// Eviction position is a pointer used in collectCandidatesForEviction
    /// to track where the last collectCandidatesForEviction stopped.
    /// This is an optimization for concurrently made eviction attempts,
    /// which allows us not to iterate the queue from scratch,
    /// skipping elements which are likely in non-evictable state.
    LRUQueue::iterator evictionPos;
    mutable std::mutex evictionPosMutex;
    /// Id of the current priority queue.
    /// Used to find its eviction info in collected eviction info map
    /// (which contains eviction info for several priority queues).
    const size_t queueId;

    /// Total "hold" size by "IFileCachePriority::HoldSpace"
    /// (updated in holdImpl, releaseImpl).
    std::atomic<size_t> totalHoldSize = 0;
    std::atomic<size_t> totalHoldElements = 0;
    std::shared_ptr<CacheUsageStatGuard> cacheUsageStatGuard;

    bool canFit(
        size_t size,
        size_t elements,
        size_t released_size_assumption,
        size_t released_elements_assumption,
        const CacheStateGuard::Lock &,
        const size_t * max_size_ = nullptr,
        const size_t * max_elements_ = nullptr) const;

    LRUQueue::iterator remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) override;

    LRUQueue::iterator iterateImpl(
        LRUQueue::iterator start_pos,
        IterateFunc func,
        FileCacheReserveStat & stat,
        InvalidatedEntriesInfos & invalidated_entries,
        const CachePriorityGuard::ReadLock &);

    LRUIterator add(
        EntryPtr entry,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *);

    /// Move a queue element from one queue to another.
    /// Used in SLRU eviction policy to upgrade/downgrade queue entries.
    LRUIterator move(
        LRUIterator & it,
        LRUFileCachePriority & other,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock &);

    std::string getApproxStateInfoForLog() const;

    LRUQueue::iterator getEvictionPos(const CachePriorityGuard::ReadLock &) const;
    void setEvictionPos(LRUQueue::iterator it, const CachePriorityGuard::ReadLock &);
    void moveEvictionPosIfEqual(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &);
};

class LRUFileCachePriority::LRUIterator : public IFileCachePriority::Iterator
{
    friend class LRUFileCachePriority;
    friend class SLRUFileCachePriority;

public:
    LRUIterator(LRUFileCachePriority * cache_priority_, LRUQueue::iterator iterator_);

    LRUIterator(const LRUIterator & other);
    LRUIterator & operator =(const LRUIterator & other);
    bool operator ==(const LRUIterator & other) const;

    EntryPtr getEntry() const override;

    bool isValid(const CachePriorityGuard::WriteLock &) const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override { return QueueEntryType::LRU; }

    LRUQueue::iterator get() const { return iterator; }

private:
    bool assertValid() const;

    LRUFileCachePriority * cachePriority;

    LRUQueue::iterator iterator;
    /// We store entry separately from iterator,
    /// because we want to be able to change its atomic state
    /// without any queue lock - both shared and unique locks - (in invalidate() method).
    /// A non-zero size entry will always stay in the queue by the same iterator
    /// until its state becomes Invalidated and it is removed.
    std::weak_ptr<Entry> entry;
};

} // namespace facebook::velox::ch
