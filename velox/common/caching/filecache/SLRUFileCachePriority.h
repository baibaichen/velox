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

#include "velox/common/caching/filecache/LRUFileCachePriority.h"


namespace facebook::velox::ch
{

/// Based on the SLRU algorithm implementation.
/// There are two queues: "protected" and "probationary".
/// All cache entries which have been accessed only once, would lie in probationary queue.
/// When entry is accessed more than once, it would go to the protected queue.
class SLRUFileCachePriority : public IFileCachePriority
{
public:
    class SLRUIterator;

    SLRUFileCachePriority(
        size_t max_size_,
        size_t max_elements_,
        double size_ratio_,
        const std::string & description_ = "none",
        LRUFileCachePriority::StatePtr probationary_state_ = nullptr,
        LRUFileCachePriority::StatePtr protected_state_ = nullptr);

    Type getType() const override { return Type::SLRU; }

    size_t getSize(const CacheStateGuard::Lock & lock) const override;
    size_t getSizeApprox() const override;

    size_t getElementsCount(const CacheStateGuard::Lock &) const override;
    size_t getElementsCountApprox() const override;

    size_t getProtectedSize(const CacheStateGuard::Lock & lock) const { return protectedQueue.getSize(lock); }
    size_t getProtectedElementsCount(const CacheStateGuard::Lock & lock) const { return protectedQueue.getElementsCount(lock); }
    size_t getProbationarySize(const CacheStateGuard::Lock & lock) const { return probationaryQueue.getSize(lock); }
    size_t getProbationaryElementsCount(const CacheStateGuard::Lock & lock) const { return probationaryQueue.getElementsCount(lock); }

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;
    void check(const CacheStateGuard::Lock &) const override;

    double getSLRUSizeRatio() const override { return sizeRatio; }

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin_info,
        const CacheStateGuard::Lock &) override;

    bool canFit( /// NOLINT
        size_t size,
        size_t elements,
        const CacheStateGuard::Lock &,
        IteratorPtr reservee = nullptr,
        const OriginInfo & origin_info = {},
        bool is_initial_load = false) const override;

    IteratorPtr add( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *,
        bool is_initial_load = false) override;

    IteratorPtr addForRestore( /// NOLINT
        KeyMetadataPtr key_metadata,
        size_t offset,
        size_t size,
        QueueEntryType original_queue_type,
        const CachePriorityGuard::WriteLock &,
        const CacheStateGuard::Lock *) override;

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

    void iterate(
        IterateFunc func,
        FileCacheReserveStat & stat,
        const CachePriorityGuard::ReadLock &) override;

    bool tryIncreasePriority(
        Iterator & iterator_,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

    void resetEvictionPos() override;

    PriorityDumpPtr dump(const CachePriorityGuard::ReadLock &) override;

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

    FileCachePriorityPtr copy() const;

protected:
    size_t getHoldSize() override { return protectedQueue.getHoldSize() + probationaryQueue.getHoldSize(); }

    size_t getHoldElements() override { return protectedQueue.getHoldElements() + probationaryQueue.getHoldElements(); }

    void setCacheUsageStatGuard(std::shared_ptr<CacheUsageStatGuard> guard) override
    {
        probationaryQueue.setCacheUsageStatGuard(guard);
        protectedQueue.setCacheUsageStatGuard(guard);
    }

private:
    using LRUIterator = LRUFileCachePriority::LRUIterator;
    using LRUQueue = std::list<Entry>;

    std::string description;
    double sizeRatio;
    LRUFileCachePriority protectedQueue;
    LRUFileCachePriority probationaryQueue;
    // TODO(logging): CH LoggerPtr was used only by LOG_* call sites; map .cpp logs to glog.

    void increasePriority(SLRUIterator & iterator, const CachePriorityGuard::WriteLock & lock);

    bool collectCandidatesForEvictionInProtected(
        const EvictionInfo & eviction_info,
        FileCacheReserveStat & stat,
        EvictionCandidates & res,
        InvalidatedEntriesInfos & invalidated_entries,
        IFileCachePriority::IteratorPtr reservee,
        bool continue_from_last_eviction_pos,
        size_t max_candidates_size,
        bool is_total_space_cleanup,
        const OriginInfo & origin_info,
        CachePriorityGuard & cache_guard,
        CacheStateGuard & state_guard);

    LRUFileCachePriority::LRUIterator addOrThrow(
        EntryPtr entry,
        LRUFileCachePriority & queue,
        const CachePriorityGuard::WriteLock & lock,
        const CacheStateGuard::Lock &);
};

class SLRUFileCachePriority::SLRUIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;
public:
    SLRUIterator(
        SLRUFileCachePriority * cache_priority_,
        LRUFileCachePriority::LRUIterator && lru_iterator_,
        bool is_protected_);

    QueueEntryType getType() const override { return isProtected ? QueueEntryType::SLRU_Protected : QueueEntryType::SLRU_Probationary; }

    EntryPtr getEntry() const override;

    bool isValid(const CachePriorityGuard::WriteLock &) const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

private:
    bool assertValid() const;

    void setIterator(LRUIterator && iterator_, bool is_protected_, const CacheStateGuard::Lock &);

    SLRUFileCachePriority * cachePriority;
    LRUIterator lruIterator;
    /// Entry itself is stored by lruIterator.entry.
    /// We have it as a separate field to use entry without requiring priority write lock
    /// (which will be required if we wanted to get entry from lruIterator.getEntry()).
    std::weak_ptr<Entry> entry;
    mutable std::mutex entryMutex;
    /// Atomic,
    /// but needed only in order to do FileSegment::getInfo() without priority write lock,
    /// which is done for system tables and logging.
    std::atomic<bool> isProtected;
};

} // namespace facebook::velox::ch
