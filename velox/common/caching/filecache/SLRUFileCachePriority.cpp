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
#include "velox/common/caching/filecache/SLRUFileCachePriority.h"

#include <algorithm>
#include <cmath>
#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/EvictionCandidates.h"
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/IFileCachePriority.h"

namespace facebook::velox::ch {

namespace {

template <typename To, typename From>
To assertCast(From* from)
{
    auto* res = dynamic_cast<std::remove_pointer_t<To>*>(from);
    // TODO(ch-port): ClickHouse assert_cast has no Velox equivalent; dynamic_cast preserves the runtime type check.
    VELOX_DCHECK_NOT_NULL(res, "Unexpected pointer type");
    return res;
}

std::string currentExceptionMessage()
{
    // TODO(ch-port): ClickHouse getCurrentExceptionMessage(true) includes CH exception details/stack if available.
    try
    {
        throw;
    }
    catch (const std::exception & e)
    {
        return e.what();
    }
    catch (...)
    {
        return "Unknown exception";
    }
}

    size_t getRatio(size_t total, double ratio, bool ceil = false)
    {
        if (ceil)
            return static_cast<size_t>(std::ceil(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0)));
        return std::lround(static_cast<double>(total) * std::clamp(ratio, 0.0, 1.0));
    }
} // namespace

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    double size_ratio_,
    const std::string & description_,
    LRUFileCachePriority::StatePtr probationary_state_,
    LRUFileCachePriority::StatePtr protected_state_)
    : IFileCachePriority(max_size_, max_elements_)
    , description(description_)
    , sizeRatio(size_ratio_)
    , protectedQueue(LRUFileCachePriority(getRatio(max_size_, size_ratio_),
                                           getRatio(max_elements_, size_ratio_),
                                           description_ + ", protected",
                                           protected_state_))
    , probationaryQueue(LRUFileCachePriority(getRatio(max_size_, 1 - size_ratio_),
                                              getRatio(max_elements_, 1 - size_ratio_),
                                              description_ + ", probationary",
                                              probationary_state_))
{
    LOG(INFO) << fmt::format(
        "Probationary queue {} in size and {} in elements. "
        "Protected queue {} in size and {} in elements",
        probationaryQueue.maxSize.load(), probationaryQueue.maxElements.load(),
        protectedQueue.maxSize.load(), protectedQueue.maxElements.load());

    if (probationaryQueue.maxSize == 0 || protectedQueue.maxSize == 0)
    {
        VELOX_FAIL(
            "Incorrect max size cache configuration. Max size: {}, size ratio: {}. "
            "Cannot have zero max size after ratio is applied.",
            max_size_, size_ratio_);
    }
    if (probationaryQueue.maxElements == 0 || protectedQueue.maxElements == 0)
    {
        VELOX_FAIL(
            "Incorrect max elements cache configuration. Max size: {}, size ratio: {}. "
            "Cannot have zero max elements after ratio is applied.",
            max_elements_, size_ratio_);
    }
}

FileCachePriorityPtr SLRUFileCachePriority::copy() const
{
    return std::make_unique<SLRUFileCachePriority>(
        maxSize, maxElements, sizeRatio, description, probationaryQueue.state, protectedQueue.state);
}

size_t SLRUFileCachePriority::getSize(const CacheStateGuard::Lock & lock) const
{
    return protectedQueue.getSize(lock) + probationaryQueue.getSize(lock);
}

size_t SLRUFileCachePriority::getElementsCount(const CacheStateGuard::Lock & lock) const
{
    return protectedQueue.getElementsCount(lock) + probationaryQueue.getElementsCount(lock);
}

size_t SLRUFileCachePriority::getSizeApprox() const
{
    return protectedQueue.getSizeApprox() + probationaryQueue.getSizeApprox();
}

size_t SLRUFileCachePriority::getElementsCountApprox() const
{
    return protectedQueue.getElementsCountApprox() + probationaryQueue.getElementsCountApprox();
}

bool SLRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock,
    IteratorPtr reservee,
    const OriginInfo &,
    bool is_initial_load) const
{
    if (is_initial_load)
        return probationaryQueue.canFit(size, elements, lock) || protectedQueue.canFit(size, elements, lock);

    if (reservee)
    {
        const auto * slru_iterator = assertCast<SLRUIterator *>(reservee->getNestedOrThis());
        if (slru_iterator->isProtected)
            return protectedQueue.canFit(size, elements, lock);
        return probationaryQueue.canFit(size, elements, lock);
    }
    return probationaryQueue.canFit(size, elements, lock);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock,
    bool is_initial_load)
{
    bool is_protected = false;
    if (is_initial_load)
    {
        VELOX_DCHECK(size);
        if (!state_lock)
            VELOX_FAIL("Startup initialization requires state lock");

        /// If it is server startup, we put entries in any queue it will fit in,
        /// but with preference for probationary queue,
        /// because we do not know the distribution between queues after server restart.
        is_protected = !probationaryQueue.canFit(size, /* elements */1, *state_lock);
    }
    else if (size && !state_lock)
    {
        VELOX_FAIL(
            "Adding non-zero size entry without state lock "
            "(key: {}, offset: {})", key_metadata->key, offset);
    }

    auto entry = std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata);
    return std::make_shared<SLRUIterator>(
        this,
        is_protected
            ? protectedQueue.add(std::move(entry), lock, state_lock)
            : probationaryQueue.add(std::move(entry), lock, state_lock),
        is_protected);
}

IFileCachePriority::IteratorPtr SLRUFileCachePriority::addForRestore( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    QueueEntryType original_queue_type,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock)
{
    /// Restore to the original queue: protected entries go back to protected,
    /// everything else goes to probationary.
    bool is_protected = (original_queue_type == QueueEntryType::SLRU_Protected);

    auto entry = std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata);
    return std::make_shared<SLRUIterator>(
        this,
        is_protected
            ? protectedQueue.add(std::move(entry), lock, state_lock)
            : probationaryQueue.add(std::move(entry), lock, state_lock),
        is_protected);
}

void SLRUFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & lock)
{
    protectedQueue.iterate(func, stat, lock);
    probationaryQueue.iterate(func, stat, lock);
}

void SLRUFileCachePriority::resetEvictionPos()
{
    protectedQueue.resetEvictionPos();
    probationaryQueue.resetEvictionPos();
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator * reservee,
    bool is_total_space_cleanup,
    const OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    if (!size && !elements)
        return std::make_unique<EvictionInfo>();

    /// Total space cleanup is for keep_free_space_size(elements)_ratio feature.
    if (is_total_space_cleanup)
    {
        /// Remove everything from probationary first
        /// and only if it's empty - remove from protected as well.
        size_t evict_size_from_probationary = std::min(size, probationaryQueue.getSize(lock));
        size_t evict_elements_from_probationary = std::min(elements, probationaryQueue.getElementsCount(lock));

        /// It is valid for the probationary queue to be empty here while the protected queue still
        /// has entries -- e.g. when `keep_free_space_size(elements)_ratio` is high enough for the
        /// background thread to want to evict everything, but all entries have already been
        /// promoted to the protected queue. The downstream code below correctly handles this case
        /// by passing zeroes to `probationaryQueue.collectEvictionInfo` and routing the full
        /// requested amount to the protected queue.
        size -= evict_size_from_probationary;
        elements -= evict_elements_from_probationary;

        auto info = probationaryQueue.collectEvictionInfo(
            evict_size_from_probationary,
            evict_elements_from_probationary,
            reservee,
            is_total_space_cleanup,
            origin_info,
            lock);

        size_t evict_size_from_protected = size ? std::min(size, protectedQueue.getSize(lock)) : 0;
        size_t evict_elements_from_protected = elements ? std::min(elements, protectedQueue.getElementsCount(lock)) : 0;

        info->add(
            protectedQueue.collectEvictionInfo(
                evict_size_from_protected,
                evict_elements_from_protected,
                reservee,
                is_total_space_cleanup,
                origin_info,
                lock));
        return info;
    }

    bool evict_in_protected = false;
    SLRUIterator * slru_iterator = nullptr;
    if (reservee)
    {
        slru_iterator = assertCast<SLRUIterator *>(reservee->getNestedOrThis());
        evict_in_protected = slru_iterator->isProtected;

        VELOX_DCHECK(evict_in_protected
                 ? slru_iterator->lruIterator.cachePriority == &protectedQueue
                 : slru_iterator->lruIterator.cachePriority == &probationaryQueue);
    }

    if (evict_in_protected)
    {
        /// If protected queue required eviction, we need to "downgrade"
        /// its eviction candidates into probationary queue
        /// (to make sure we have space in probationary queue for the downgrade).
        /// But we cannot do it here, as we do not know in advance the exact size to downgrade.
        /// So we will do it in collectCandidatesForEviction.
        return protectedQueue.collectEvictionInfo(
            size, elements, reservee, is_total_space_cleanup, origin_info, lock);
    }

    return probationaryQueue.collectEvictionInfo(
        size, elements, reservee, is_total_space_cleanup, origin_info, lock);
}

bool SLRUFileCachePriority::collectCandidatesForEviction(
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
    CacheStateGuard & state_guard)
{
    if (is_total_space_cleanup)
    {
        /// Use per-queue local stat objects so that each sub-queue's
        /// stopping condition sees only its own accumulated releasable bytes,
        /// not the cumulative total from both passes.
        FileCacheReserveStat probationary_stat;
        bool success_probationary = probationaryQueue.collectCandidatesForEviction(
            eviction_info,
            probationary_stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
        /// We do not quit here if !success_probationary,
        /// because in case of keep_up_free_space_ratio it is ok to evict at least something
        /// (so we will check res.size() instead of returned bool value).

        /// We do not use collectCandidatesForEvictionInProtected method,
        /// because it will "downgrade" instead of remove,
        /// but for total space cleanup we need remove.
        FileCacheReserveStat protected_stat;
        bool success_protected = protectedQueue.collectCandidatesForEviction(
            eviction_info,
            protected_stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);

        stat += probationary_stat;
        stat += protected_stat;

        return success_probationary && success_protected;
    }

    /// If `reservee` is nullptr, then it is the first space reservation attempt
    /// for a corresponding file segment, so it will be directly put into probationary queue.
    if (!reservee)
    {
        return probationaryQueue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
    }

    auto * slru_iterator = assertCast<SLRUIterator *>(reservee->getNestedOrThis());
    bool success = false;

    /// If `reservee` is not nullptr (e.g. is already in some queue),
    /// we need to check in which queue (protected/probationary) it currently is
    /// (in order to know where we need to free space).
    if (slru_iterator->isProtected)
    {
        VELOX_DCHECK(slru_iterator->lruIterator.cachePriority == &protectedQueue);
        /// Entry is in protected queue.
        /// Check if we have enough space in protected queue to fit a new size of entry.
        success = collectCandidatesForEvictionInProtected(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
    }
    else
    {
        VELOX_DCHECK(slru_iterator->lruIterator.cachePriority == &probationaryQueue);
        success = probationaryQueue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard);
    }

    return success;
}

/// TODO: currently this will find only releasable entries,
/// but since we are only downgrading, then it does not matter.
bool SLRUFileCachePriority::collectCandidatesForEvictionInProtected(
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
    CacheStateGuard & state_guard)
{
    auto downgrade_candidates = std::make_shared<EvictionCandidates>();
    FileCacheReserveStat downgrade_stat;
    if (!protectedQueue.collectCandidatesForEviction(
        eviction_info,
        downgrade_stat,
        *downgrade_candidates,
        invalidated_entries,
        reservee,
        continue_from_last_eviction_pos,
        max_candidates_size,
        is_total_space_cleanup,
        origin_info,
        cache_guard,
        state_guard))
    {
        return false;
    }

    /// We can have no downgrade candidates because cache size could
    /// reduce concurrently because of lock-free cache entries invalidation.
    if (downgrade_candidates->size() == 0)
    {
        return true;
    }

    /// We did not collect eviction info for probationary queue in advance
    /// (when doing so for protected queue),
    /// because we could not know how much space we will need to downgrade
    /// (because downgraded space >= space to reserve in protected).
    /// So we do it now.
    auto probationary_eviction_info = probationaryQueue.collectEvictionInfo(
        downgrade_stat.totalStat.releasableSize,
        downgrade_stat.totalStat.releasableCount,
        /* reservee */nullptr,
        /* is_total_space_cleanup */false,
        origin_info,
        state_guard.lock());

    const bool requires_eviction = probationary_eviction_info->requiresEviction();
    /// FIXME: const_cast is a bad practice.
    const_cast<EvictionInfo &>(eviction_info).add(std::move(probationary_eviction_info));
    if (requires_eviction)
    {
        /// If not enough space - we need to "downgrade" lowest priority entries
        /// from protected queue to probationary queue,
        /// so collect eviction candidates in probationary now.
        if (!probationaryQueue.collectCandidatesForEviction(
            eviction_info,
            stat,
            res,
            invalidated_entries,
            reservee,
            continue_from_last_eviction_pos,
            max_candidates_size,
            is_total_space_cleanup,
            origin_info,
            cache_guard,
            state_guard))
        {
            return false;
        }
    }

    struct DowngradedEntryInfo
    {
        IteratorPtr slru_iterator;
        /// Entry size as it was in protected queue.
        size_t entry_size = 0;
        /// Previous iterator to entry in protected queue.
        LRUIterator prev_nested_iterator;
        /// New iterator to entry in probationary queue.
        LRUIterator new_nested_iterator;
    };
    /// RAII wrapper to protect against the case when afterEvictState callback
    /// is not called because of some unexpected exception.
    struct DowngradedEntriesInfos : private std::vector<DowngradedEntryInfo>
    {
        explicit DowngradedEntriesInfos(size_t size) { reserve(size); }

        void add(DowngradedEntryInfo && info) { push_back(info); }

        [[maybe_unused]] size_t getSize() const { return size(); }

        std::optional<DowngradedEntryInfo> next()
        {
            if (empty())
                return std::nullopt;
            auto info = std::move(back());
            pop_back();
            return info;
        }

        ~DowngradedEntriesInfos()
        {
            /// Invalidate new unused iterators.
            /// If entries number is non-zero here, it must mean there was
            /// some exception because of which we failed to process new iterators.
            for (auto & entry : *this)
                entry.new_nested_iterator.invalidate();
        }
    };
    auto downgraded_entries = std::make_shared<DowngradedEntriesInfos>(downgrade_candidates->size());

    /// Set callback to execute the "downgrade".
    /// As PriorityGuard::WriteLock allows to only move elements,
    /// but not increment size of any of the queues,
    /// we move elements with zero size and increase the size later in a separate callback.
    res.setAfterEvictWriteFunc([=, this](const CachePriorityGuard::WriteLock & lk) mutable
    {
        for (auto & [key, key_candidates] : *downgrade_candidates)
        {
            while (!key_candidates.candidates.empty())
            {
                auto iterator = key_candidates.candidates.back()->getQueueIterator();
                auto * slru_iterator = assertCast<SLRUIterator *>(iterator->getNestedOrThis());
                auto entry = slru_iterator->getEntry();
                VELOX_DCHECK(entry->size > 0);

                /// Add a new empty queue entry,
                /// save pointers to a new empty entry and old non-empty entry.
                /// Once we have state lock, we will increment size for new entry
                /// and reset size for the old entry,
                /// thus size will be transferred from one entry to another.
                /// PreActive: iterateImpl skips this entry until setIterator atomically transitions it to Active.
                auto empty_entry = std::make_shared<Entry>(entry->key, entry->offset, /* size */0, entry->keyMetadata, Entry::State::PreActive);
                auto new_iterator = probationaryQueue.add(std::move(empty_entry), lk, /* state_lock */nullptr);
                downgraded_entries->add(DowngradedEntryInfo{
                    .slru_iterator = iterator,
                    .entry_size = entry->size,
                    .prev_nested_iterator = slru_iterator->lruIterator,
                    .new_nested_iterator = new_iterator
                });
                key_candidates.candidates.pop_back();
            }
        }
    });

    /// Set incrementing size callback, as explained in the previous comment.
    res.setAfterEvictStateFunc([=, this](const CacheStateGuard::Lock & lk)
    {
        VELOX_DCHECK(downgraded_entries->getSize() > 0);
        while (true)
        {
            auto info = downgraded_entries->next();
            if (!info.has_value())
                break;

            auto * iterator = assertCast<SLRUIterator *>(info->slru_iterator->getNestedOrThis());
            VELOX_DCHECK(iterator);
            try
            {
                info->new_nested_iterator.incrementSize(info->entry_size, lk);
            }
            catch (...)
            {
                info->new_nested_iterator.invalidate();
                throw;
            }
            iterator->setIterator(std::move(info->new_nested_iterator), /* is_protected */false, lk);
            info->prev_nested_iterator.invalidate();
            check(lk);
            info->slru_iterator->check(lk);
        }
    });

    VLOG(1) << fmt::format(
        "Eviction info: {}. "
        "Downgrading {} elements from protected to probationary. "
        "Total size: {}",
        eviction_info.toString(), downgrade_candidates->size(), downgrade_stat.totalStat.releasableSize);

    return true;
}

bool SLRUFileCachePriority::tryIncreasePriority(
    Iterator & iterator_,
    bool is_space_reservation_complete,
    CachePriorityGuard & queue_guard,
    CacheStateGuard & state_guard)
{
    auto & iterator = dynamic_cast<SLRUFileCachePriority::SLRUIterator &>(iterator_);
    VELOX_DCHECK(iterator.assertValid());

    /// If entry is already in protected queue,
    /// we only need to increase its priority within the protected queue.
    if (iterator.isProtected)
    {
        return protectedQueue.tryIncreasePriority(
            iterator.lruIterator, is_space_reservation_complete, queue_guard, state_guard);
    }
    else if (!is_space_reservation_complete)
    {
        /// `is_space_reservation_complete` means that file segment is fully downloaded.
        /// This is a limitation of current implementation
        /// that we opt to upgrade only those entries which have already completed space reservation.
        /// Because otherwise it becomes too complex to handle concurrent
        /// space reservation and priority increase (because of the granular locking).
        return probationaryQueue.tryIncreasePriority(
            iterator.lruIterator, is_space_reservation_complete, queue_guard, state_guard);
    }

    VELOX_DCHECK(iterator.lruIterator.cachePriority == &probationaryQueue);

    EntryPtr prev_entry = iterator.getEntry();

    {
        auto locked_key = prev_entry->keyMetadata->lock();
        const auto entry_state = prev_entry->getState();
        VELOX_DCHECK(entry_state == Entry::State::Active || entry_state == Entry::State::Evicting);
        if (entry_state != Entry::State::Active)
            return false;

        /// As we are in progress now of moving this queue entry to a protected queue,
        /// then we need to make sure no one tries to concurrently evict this entry from cache.
        /// So we set "moving flag" to make sure no one touches this entry in the meantime.
        /// And we do not reuse "evicting flag" because we want queries to be able
        /// to use this file segment in the meantime.
        prev_entry->setMovingFlag(*locked_key);
    }

    bool reset_evicting_flag_for_prev_entry = true;
    auto reset_evicting_flag_for_prev_entry_guard = folly::makeGuard([&] {
        if (reset_evicting_flag_for_prev_entry)
            prev_entry->resetFlag(/* from_state */Entry::State::Moving);
    });

    /// Entry is in probationary queue.
    /// Check if there is enough space in protected queue to move entry there.
    /// If not - we need to "downgrade" lowest priority entries from protected
    /// queue to probationary queue.
    std::unique_ptr<EvictionInfo> downgrade_info;
    {
        auto lock = state_guard.lock();
        downgrade_info = protectedQueue.collectEvictionInfo(
            prev_entry->size,
            /* elements */1,
            /* reservee */nullptr,
            /* is_total_space_cleanup */false,
            FileCache::getInternalOrigin(),
            lock);

#ifdef DEBUG_OR_SANITIZER_BUILD
        VLOG(1) << fmt::format(
            "Entry: {}. Downgrade info: {} ({})",
            prev_entry->toString(), downgrade_info->toString(), getStateInfoForLog(lock));
#endif
    }

    EvictionCandidates downgrade_candidates;
    FileCacheReserveStat downgrade_stat;
    InvalidatedEntriesInfos invalidated_entries;

    if (!collectCandidatesForEvictionInProtected(
        *downgrade_info,
        downgrade_stat,
        downgrade_candidates,
        invalidated_entries,
        /* reservee */nullptr,
        /* continue_from_last_eviction_pos */false,
        /* max_candidates_size */0,
        /* is_total_space_cleanup */false,
        FileCache::getInternalOrigin(),
        queue_guard,
        state_guard))
    {
        reset_evicting_flag_for_prev_entry = false;
        prev_entry->resetFlag(/* from_state */Entry::State::Moving);

        return probationaryQueue.tryIncreasePriority(
            iterator.lruIterator, is_space_reservation_complete, queue_guard, state_guard);
    }

    downgrade_candidates.evict();

    /// Count how much we evict,
    /// because it could affect performance if we have to do this often.
    // TODO(metric): CH ProfileEvents::increment(FilesystemCacheEvictedFileSegmentsDuringPriorityIncrease) is observational.

    auto new_iterator = [&]{
        auto lock = queue_guard.writeLock();
        downgrade_candidates.afterEvictWrite(lock);
        removeEntries(invalidated_entries, lock);

        /// PreActive: iterateImpl skips this entry until setIterator atomically transitions it to Active.
        auto empty_entry = std::make_shared<Entry>(
            prev_entry->key,
            prev_entry->offset,
            /* size */0,
            prev_entry->keyMetadata,
            Entry::State::PreActive);

        return protectedQueue.add(
            std::move(empty_entry), lock, /* state_lock */nullptr);
    }();

    auto prev_iterator = iterator.lruIterator;
    {
        auto lock = state_guard.lock();
        try
        {
            downgrade_info->releaseHoldSpace(lock);
            downgrade_candidates.afterEvictState(lock);
            new_iterator.incrementSize(prev_entry->size, lock);
        }
        catch (...)
        {
            new_iterator.invalidate();
            throw;
        }
        iterator.setIterator(std::move(new_iterator), /* is_protected */true, lock);
        prev_iterator.invalidate();
        reset_evicting_flag_for_prev_entry = false;
        check(lock);
    }

    return true;
}

LRUFileCachePriority::LRUIterator SLRUFileCachePriority::addOrThrow(
    EntryPtr entry,
    LRUFileCachePriority & queue,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock & state_lock)
{
    try
    {
        return queue.add(entry, lock, &state_lock);
    }
    catch (...)
    {
        const auto initial_exception = currentExceptionMessage();
        try
        {
            /// We cannot allow a situation that a file exists on filesystem, but
            /// there is no corresponding entry in priority queue for it,
            /// because it will mean that cache became inconsistent.
            /// So let's try to fix the situation.
            auto metadata = entry->keyMetadata->tryLock();
            VELOX_DCHECK(metadata);
            if (metadata)
            {
                auto segment_metadata = metadata->tryGetByOffset(entry->offset);
                metadata->removeFileSegment(entry->offset, segment_metadata->fileSegment->lock());
            }
        }
        catch (...)
        {
            VELOX_FAIL(
                "Unexpected exception: {} (Initial exception: {}). Cache will become inconsistent",
                currentExceptionMessage(), initial_exception);
        }

        VELOX_FAIL(
            "Failed to create queue entry: {}", currentExceptionMessage());
    }
}

IFileCachePriority::PriorityDumpPtr SLRUFileCachePriority::dump(const CachePriorityGuard::ReadLock & lock)
{
    auto res = probationaryQueue.dump(lock);
    auto part_res = protectedQueue.dump(lock);
    res->merge(*part_res);
    return res;
}

void SLRUFileCachePriority::shuffle(const CachePriorityGuard::WriteLock & lock)
{
    protectedQueue.shuffle(lock);
    probationaryQueue.shuffle(lock);
}

bool SLRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double size_ratio_, const CacheStateGuard::Lock & lock)
{
    if (maxSize == max_size_ && maxElements == max_elements_ && sizeRatio == size_ratio_)
        return false; /// Nothing to change.

    protectedQueue.modifySizeLimits(getRatio(max_size_, size_ratio_), getRatio(max_elements_, size_ratio_), 0, lock);
    probationaryQueue.modifySizeLimits(getRatio(max_size_, 1 - size_ratio_), getRatio(max_elements_, 1 - size_ratio_), 0, lock);

    maxSize = max_size_;
    maxElements = max_elements_;
    sizeRatio = size_ratio_;
    return true;
}

EvictionInfoPtr SLRUFileCachePriority::collectEvictionInfoForResize(
    size_t desired_max_size,
    size_t desired_max_elements,
    const IFileCachePriority::OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    /// Delegate to each sub-queue's collectEvictionInfoForResize with per-sub-queue
    /// desired limits derived from the desired total and ratio.
    /// This is needed because the total cache size might be under the desired total,
    /// but one sub-queue (e.g. protected) might exceed its new sub-limit.

    auto info = protectedQueue.collectEvictionInfoForResize(
        getRatio(desired_max_size, sizeRatio),
        getRatio(desired_max_elements, sizeRatio),
        origin_info, lock);

    info->add(probationaryQueue.collectEvictionInfoForResize(
        getRatio(desired_max_size, 1 - sizeRatio),
        getRatio(desired_max_elements, 1 - sizeRatio),
        origin_info, lock));

    return info;
}

SLRUFileCachePriority::SLRUIterator::SLRUIterator(
    SLRUFileCachePriority * cache_priority_,
    LRUFileCachePriority::LRUIterator && lru_iterator_,
    bool is_protected_)
    : cachePriority(cache_priority_)
    , lruIterator(lru_iterator_)
    , entry(lru_iterator_.getEntry())
    , isProtected(is_protected_)
{
}

SLRUFileCachePriority::EntryPtr SLRUFileCachePriority::SLRUIterator::getEntry() const
{
    std::lock_guard lock(entryMutex);
    auto entry_ptr = entry.lock();
    if (!entry_ptr)
        VELOX_FAIL("Entry pointer expired");
    return entry_ptr;
}

void SLRUFileCachePriority::SLRUIterator::setIterator(
    LRUIterator && iterator_,
    bool is_protected_,
    const CacheStateGuard::Lock & state_lock)
{
    auto new_entry = iterator_.getEntry();
    VELOX_DCHECK(new_entry->size > 0);

    lruIterator = iterator_;
    isProtected = is_protected_;

    std::lock_guard lock(entryMutex);
    entry = new_entry;
    /// Atomically activate the new entry together with the pointer update.
    /// `iterateImpl` reads entry state without holding `entryMutex`, so it sees either:
    ///   - PreActive  → skips the entry (not evictable yet), or
    ///   - Active     → calls getEntry() under entryMutex and is guaranteed to see new_entry.
    /// This eliminates the race where iterateImpl finds new_entry as evictable but
    /// getEntry() still returns the previous queue entry, causing setEvictingFlag
    /// to be called on an entry that is not in Active state.
    new_entry->setActiveFlag(state_lock);
}

void SLRUFileCachePriority::SLRUIterator::incrementSize(size_t size, const CacheStateGuard::Lock & lock)
{
    assertValid();
    lruIterator.incrementSize(size, lock);
    check(lock);
}

void SLRUFileCachePriority::SLRUIterator::decrementSize(size_t size)
{
    assertValid();
    lruIterator.decrementSize(size);
}

void SLRUFileCachePriority::SLRUIterator::invalidate()
{
    assertValid();
    lruIterator.invalidate();
}

bool SLRUFileCachePriority::SLRUIterator::isValid(const CachePriorityGuard::WriteLock & lock) const
{
    return lruIterator.isValid(lock);
}

void SLRUFileCachePriority::SLRUIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    assertValid();
    lruIterator.remove(lock);
}

bool SLRUFileCachePriority::SLRUIterator::assertValid() const
{
    lruIterator.assertValid();
    return true;
}

std::string SLRUFileCachePriority::getStateInfoForLog(const CacheStateGuard::Lock & lock) const
{
    return fmt::format("total size {}/{}, elements {}/{}, "
                       "probationary queue size {}/{}, elements {}/{}, "
                       "protected queue size {}/{}, elements {}/{}",
                       getSize(lock), maxSize.load(), getElementsCount(lock), maxElements.load(),
                       probationaryQueue.getSize(lock), probationaryQueue.maxSize.load(),
                       probationaryQueue.getElementsCount(lock), probationaryQueue.maxElements.load(),
                       protectedQueue.getSize(lock), protectedQueue.maxSize.load(),
                       protectedQueue.getElementsCount(lock), protectedQueue.maxElements.load());
}

void SLRUFileCachePriority::check(const CacheStateGuard::Lock & lock) const
{
    probationaryQueue.check(lock);
    protectedQueue.check(lock);
    IFileCachePriority::check(lock);
}

} // namespace facebook::velox::ch
