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
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/LRUFileCachePriority.h"

#include <algorithm>
#include <optional>
#include <random>
#include <vector>

#include <folly/Random.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch
{

void LRUFileCachePriority::State::add(uint64_t size_, uint64_t elements_, const CacheStateGuard::Lock &)
{
    VELOX_DCHECK(size_ || elements_);

    VLOG(1) << fmt::format("Updating size with {}, current is {}", size_, size.load(std::memory_order_relaxed));

    if (size_)
    {
        size.fetch_add(size_, std::memory_order_relaxed);
        // TODO(metric): CH CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size_).
    }

    if (elements_)
    {
        elementsNum.fetch_add(elements_, std::memory_order_relaxed);
        // TODO(metric): CH CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, elements_).
    }
}

void LRUFileCachePriority::State::sub(uint64_t size_, uint64_t elements_)
{
    VELOX_DCHECK(size_ || elements_);

    if (size_)
    {
        VELOX_DCHECK(size >= size_);
        size -= size_;
        // TODO(metric): CH CurrentMetrics::sub(CurrentMetrics::FilesystemCacheSize, size_).
    }

    if (elements_)
    {
        VELOX_DCHECK(elementsNum >= elements_);
        elementsNum -= elements_;
        // TODO(metric): CH CurrentMetrics::sub(CurrentMetrics::FilesystemCacheElements, elements_).
    }
}

LRUFileCachePriority::LRUFileCachePriority(
    size_t max_size_,
    size_t max_elements_,
    const std::string & description_,
    StatePtr state_)
    : IFileCachePriority(max_size_, max_elements_)
    , description(description_)
    , evictionPos(queue.end())
    // TODO(random): CH uses randomSeed(); Velox port uses folly::Random::rand64().
    , queueId(folly::Random::rand64())
{
    if (state_)
        state = state_;
    else
        state = std::make_shared<State>();
}

IFileCachePriority::IteratorPtr LRUFileCachePriority::add( /// NOLINT
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock * state_lock,
    bool)
{
    return std::make_shared<LRUIterator>(add(
        std::make_shared<Entry>(key_metadata->key, offset, size, key_metadata),
        lock,
        state_lock));
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::add(
    EntryPtr entry,
    const CachePriorityGuard::WriteLock &,
    const CacheStateGuard::Lock * state_lock)
{
    if (entry->size && !state_lock)
    {
        VELOX_FAIL(
            "Adding non-zero size entry without state lock "
            "(key: {}, offset: {})", entry->key.toString(), entry->offset);
    }

#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        if (queue_entry->getState() == Entry::State::Active
            && queue_entry->key == entry->key && queue_entry->offset == entry->offset)
        {
            VELOX_FAIL(
                "Attempt to add duplicate queue entry to queue: {}",
                entry->toString());
        }
    }
#endif

    if (entry->size && !canFit(entry->size, /* elements */1, *state_lock))
    {
        VELOX_FAIL(
            "Not enough space to add a new entry {}. Current state: {}",
            entry->toString(), getStateInfoForLog(*state_lock));
    }

    auto iterator = queue.insert(queue.end(), entry);

    if (entry->size)
        state->add(entry->size, /* elements */1, *state_lock);

    VLOG(1) << fmt::format(
        "Added entry into LRU queue, key: {}, offset: {}, size: {}",
        entry->key.toString(), entry->offset, entry->size.load());

    return LRUIterator(this, iterator);
}

LRUFileCachePriority::LRUQueue::iterator
LRUFileCachePriority::remove(LRUQueue::iterator it, const CachePriorityGuard::WriteLock & lock)
{
    /// If size is 0, entry is invalidated, current_elements_num was already updated.
    auto & entry = **it;
    if (entry.size)
        state->sub(entry.size, /* elements */1);

    entry.setRemoved(lock);

    VLOG(1) << fmt::format(
        "Removed entry from LRU queue, key: {}, offset: {}, size: {}",
        entry.key.toString(), entry.offset, entry.size.load());

    moveEvictionPosIfEqual(it, lock);
    return queue.erase(it);
}

LRUFileCachePriority::LRUIterator::LRUIterator(
    LRUFileCachePriority * cache_priority_,
    LRUQueue::iterator iterator_)
    : cachePriority(cache_priority_)
    , iterator(iterator_)
    , entry(*iterator)
{
    assertValid();
}

LRUFileCachePriority::LRUIterator::LRUIterator(const LRUIterator & other)
{
    *this = other;
}

LRUFileCachePriority::LRUIterator &
LRUFileCachePriority::LRUIterator::operator =(const LRUIterator & other)
{
    if (this == &other)
        return *this;

    cachePriority = other.cachePriority;
    iterator = other.iterator;
    entry = other.entry;
    return *this;
}

bool LRUFileCachePriority::LRUIterator::operator ==(const LRUIterator & other) const
{
    return cachePriority == other.cachePriority && iterator == other.iterator;
}

void LRUFileCachePriority::iterate(
    IterateFunc func,
    FileCacheReserveStat & stat,
    const CachePriorityGuard::ReadLock & lock)
{
    InvalidatedEntriesInfos invalidated_entries;
    iterateImpl(queue.begin(), func, stat, invalidated_entries, lock);
}

LRUFileCachePriority::LRUQueue::iterator
LRUFileCachePriority::iterateImpl(
    LRUQueue::iterator start_pos,
    IterateFunc func,
    FileCacheReserveStat & stat,
    InvalidatedEntriesInfos & invalidated_entries,
    const CachePriorityGuard::ReadLock &)
{
    const size_t max_elements_to_iterate = queue.size();
    auto it = start_pos;

    for (size_t iterated_elements = 0; iterated_elements < max_elements_to_iterate; ++iterated_elements)
    {
        if (it == queue.end())
            it = queue.begin();

        const auto & entry = **it;

        //VLOG(1) << fmt::format("Entry: {}", entry.toString());

        auto is_evictable_state = [&]() -> bool
        {
            switch (entry.getState())
            {
                case Entry::State::Active:
                {
                    /// A newly added entry may have size 0 before the first
                    /// space reservation completes. It is not yet evictable.
                    return entry.size > 0;
                }
                case Entry::State::PreActive:
                {
                    /// Entry is being moved between SLRU queues. Size may already be
                    /// non-zero, but the entry is not evictable until `SLRUIterator::setIterator`
                    /// transitions it to Active atomically with the iterator pointer update.
                    /// For SLRU transitions `size > 0` alone is not a sufficient guard, because
                    /// the SLRUIterator might still point to the old entry.
                    return false;
                }
                case Entry::State::Invalidated:
                {
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
                    invalidated_entries.emplace_back(*it, std::make_shared<LRUIterator>(this, it));
                    return false;
                }
                case Entry::State::Evicting:
                {
                    /// Skip queue entries which are in evicting state.
                    /// We threat them the same way as deleted entries.
                    // TODO(metric): CH ProfileEvents::FilesystemCacheEvictionSkippedEvictingFileSegments.
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Evicting);
                    return false;
                }
                case Entry::State::Moving:
                {
                    // TODO(metric): CH ProfileEvents::FilesystemCacheEvictionSkippedMovingFileSegments.
                    stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Moving);
                    return false;
                }
                case Entry::State::Removed:
                {
                    /// As we iterate under priority read lock and removed flag is
                    /// set under priority write lock right before it is removed from queue,
                    /// then the entry must have been removed from queue
                    /// before we acquired read lock.
                    VELOX_DCHECK(false, "Entry should have been removed from queue: {}", entry.toString());
                    return false;
                }
            }
        };

        /// Check state without locked key as an optimization.
        if (!is_evictable_state())
        {
            ++it;
            continue;
        }

        auto locked_key = entry.keyMetadata->tryLock();
        if (!locked_key)
        {
            /// locked_key == nullptr means that the cache key of
            /// the file segment of this queue entry no longer exists.
            /// This is normal if the key was removed from metadata,
            /// while queue entries can be removed lazily (with delay).
            stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
            ++it;
            continue;
        }

        /// Reread entry state under locked key.
        if (!is_evictable_state())
        {
            ++it;
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(entry.offset);
        if (!metadata)
        {
            stat.update(entry.size, FileSegmentKind::Regular, FileCacheReserveStat::State::Invalidated);
            ++it;
            /// We should have quit earlier in is_evictable_state under locked key.
            VELOX_DCHECK(false);
            continue;
        }

        auto result = func(*locked_key, metadata);
        switch (result)
        {
            case IterationResult::BREAK:
            {
                return it;
            }
            case IterationResult::CONTINUE:
            {
                ++it;
                break;
            }
        }
    }
    return queue.end();
}

bool LRUFileCachePriority::canFit( /// NOLINT
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock,
    IteratorPtr,
    const OriginInfo &,
    bool) const
{
    return canFit(size, elements, 0, 0, lock);
}

bool LRUFileCachePriority::canFit(
    size_t size,
    size_t elements,
    size_t released_size_assumption,
    size_t released_elements_assumption,
    const CacheStateGuard::Lock & lock,
    const size_t * max_size_,
    const size_t * max_elements_) const
{
    const size_t current_size = state->getSize(lock);
    const size_t current_elements_num = state->getElementsCount(lock);
    return (maxSize == 0
            || (current_size + size - released_size_assumption <= (max_size_ ? *max_size_ : maxSize.load())))
        && (maxElements == 0
            || current_elements_num + elements - released_elements_assumption <= (max_elements_ ? *max_elements_ : maxElements.load()));
}

EvictionInfoPtr LRUFileCachePriority::collectEvictionInfo(
    size_t size,
    size_t elements,
    IFileCachePriority::Iterator *,
    bool is_total_space_cleanup,
    const IFileCachePriority::OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    auto info = std::make_unique<QueueEvictionInfo>(description, origin_info.userId);
    if (!size && !elements)
        return std::make_unique<EvictionInfo>(queueId, std::move(info));

    /// Total space cleanup is for keep_free_space_size(elements)_ratio feature.
    if (is_total_space_cleanup)
    {
        info->sizeToEvict = std::min(size, getSize(lock));
        info->elementsToEvict = std::min(elements, getElementsCount(lock));
        return std::make_unique<EvictionInfo>(queueId, std::move(info));
    }

    /// max_size == 0 => unlimitted size
    const size_t available_size = maxSize ? maxSize - state->getSize(lock) : size;
    if (available_size < size)
        info->sizeToEvict = size - available_size;

    /// max_elements == 0 => unlimitted elements
    const size_t available_elements = maxElements ? maxElements - state->getElementsCount(lock) : elements;
    if (available_elements < elements)
        info->elementsToEvict = elements - available_elements;

    /// As eviction is done without a cache priority lock,
    /// then if some space was partially available and some needed
    /// to be freed via eviction, we need to make sure that this
    /// partially available space is still available
    /// after we finish with eviction for non-available space.
    /// So we create a space holder for the currently available part
    /// of the required space for the duration of eviction of the other
    /// currently non-available part of the space.
    size_t size_to_hold = info->sizeToEvict ? available_size : size;
    size_t elements_to_hold = info->elementsToEvict ? available_elements : elements;

    if (size_to_hold || elements_to_hold)
    {
        info->holdSpace = std::make_unique<IFileCachePriority::HoldSpace>(
            size_to_hold,
            elements_to_hold,
            *this,
            lock);
    }
    return std::make_unique<EvictionInfo>(queueId, std::move(info));
}

bool LRUFileCachePriority::collectCandidatesForEviction(
    const EvictionInfo & eviction_info,
    FileCacheReserveStat & stat,
    EvictionCandidates & res,
    InvalidatedEntriesInfos & invalidated_entries,
    IFileCachePriority::IteratorPtr /* reservee */,
    bool continue_from_last_eviction_pos,
    size_t max_candidates_size,
    bool /* is_total_space_cleanup */,
    const OriginInfo &,
    CachePriorityGuard & cache_guard,
    CacheStateGuard &)
{
    const auto & info = eviction_info.get(queueId);
    size_t size = info.sizeToEvict;
    size_t elements = info.elementsToEvict;

    if (!size && !elements)
        return true;

    // TODO(metric): CH ProfileEvents::FilesystemCacheEvictionTries.

    auto get_iteration_result = [&]()
    {
        if ((!size || stat.totalStat.releasableSize >= size)
            && (!elements || stat.totalStat.releasableCount >= elements))
            return IterationResult::BREAK;

        if (max_candidates_size && res.size() >= max_candidates_size)
            return IterationResult::BREAK;

        return IterationResult::CONTINUE;
    };

    auto lock = cache_guard.readLock();

    auto start_pos = queue.begin();
    auto current_eviction_pos = getEvictionPos(lock);
    if (continue_from_last_eviction_pos
        && current_eviction_pos != LRUQueue::iterator{}
        && current_eviction_pos != queue.end()
        && start_pos != current_eviction_pos)
    {
        // TODO(metric): CH ProfileEvents::FilesystemCacheEvictionReusedIterator.
        start_pos = current_eviction_pos;
    }

    auto iteration_pos = iterateImpl(
        start_pos,
        [&](LockedKey & locked_key, const FileSegmentMetadataPtr & segment_metadata)
    {
        if (get_iteration_result() == IterationResult::BREAK)
            return IterationResult::BREAK;

        const auto & file_segment = segment_metadata->fileSegment;
        VELOX_DCHECK(file_segment->assertCorrectness());

        if (segment_metadata->releasable())
        {
            res.add(segment_metadata, locked_key);
            stat.update(
                segment_metadata->size(),
                file_segment->getKind(),
                FileCacheReserveStat::State::Releasable);

            return get_iteration_result();
        }

        // TODO(metric): CH ProfileEvents::FilesystemCacheEvictionSkippedFileSegments.
        stat.update(
            segment_metadata->size(),
            file_segment->getKind(),
            FileCacheReserveStat::State::NonReleasable);

        return IterationResult::CONTINUE;
    },
    stat, invalidated_entries, lock);

    if (continue_from_last_eviction_pos)
        setEvictionPos(iteration_pos, lock);

    lock.unlock();

    const bool success = (max_candidates_size && res.size() >= max_candidates_size)
        || ((!size || stat.totalStat.releasableSize >= size)
            && (!elements || stat.totalStat.releasableCount >= elements));

    if (!success)
    {
        VLOG(1) << fmt::format(
            "Failed to collect eviction candidates "
            "(for size: {}, elements: {}, current size: {}, current elements: {}): {}",
            size, elements, getSizeApprox(), getElementsCountApprox(), stat.totalStat.toString());
    }
    return success;
}

LRUFileCachePriority::LRUIterator LRUFileCachePriority::move(
    LRUIterator & it,
    LRUFileCachePriority & other,
    const CachePriorityGuard::WriteLock & lock,
    const CacheStateGuard::Lock & state_lock)
{
    const auto & entry = *it.getEntry();
    if (entry.size == 0)
    {
        VELOX_FAIL(
            "Adding zero size entries to LRU queue is not allowed "
            "(key: {}, offset: {})", entry.key.toString(), entry.offset);
    }
#ifndef NDEBUG
    for (const auto & queue_entry : queue)
    {
        /// entry.size == 0 means entry was invalidated.
        if (queue_entry->size != 0 && queue_entry->key == entry.key && queue_entry->offset == entry.offset)
            VELOX_FAIL(
                "Attempt to add duplicate queue entry to queue: {}",
                entry.toString());
    }
#endif

    moveEvictionPosIfEqual(it.iterator, lock);
    queue.splice(queue.end(), other.queue, it.iterator);

    state->add(entry.size, /* elements */1, state_lock);
    other.state->sub(entry.size, /* elements */1);

    return LRUIterator(this, it.iterator);
}

IFileCachePriority::PriorityDumpPtr LRUFileCachePriority::dump(const CachePriorityGuard::ReadLock & lock)
{
    std::vector<FileSegmentInfo> res;
    FileCacheReserveStat stat{};
    iterate([&](LockedKey &, const FileSegmentMetadataPtr & segment_metadata)
    {
        res.emplace_back(FileSegment::getInfo(segment_metadata->fileSegment));
        return IterationResult::CONTINUE;
    }, stat, lock);
    return std::make_shared<IPriorityDump>(res);
}

bool LRUFileCachePriority::modifySizeLimits(
    size_t max_size_, size_t max_elements_, double /* size_ratio_ */, const CacheStateGuard::Lock & lock)
{
    if (maxSize == max_size_ && maxElements == max_elements_)
        return false; /// Nothing to change.

    if (state->getSize(lock) > max_size_ || state->getElementsCount(lock) > max_elements_)
    {
        VELOX_FAIL("Cannot modify size limits to {} in size and {} in elements: "
                   "not enough space freed. Current size: {}/{}, elements: {}/{} ({})",
                   max_size_, max_elements_, state->getSize(lock), maxSize.load(),
                   state->getElementsCount(lock), maxElements.load(), description);
    }

    LOG(INFO) << fmt::format("Modifying size limits from {} to {} in size, "
                             "from {} to {} in elements count",
                             maxSize.load(), max_size_, maxElements.load(), max_elements_);

    maxSize = max_size_;
    maxElements = max_elements_;
    return true;
}

EvictionInfoPtr LRUFileCachePriority::collectEvictionInfoForResize(
    size_t desired_max_size,
    size_t desired_max_elements,
    const OriginInfo & origin_info,
    const CacheStateGuard::Lock & lock)
{
    size_t current_size = getSize(lock);
    size_t current_elements = getElementsCount(lock);
    size_t size_to_evict = current_size > desired_max_size ? current_size - desired_max_size : 0;
    size_t elements_to_evict = current_elements > desired_max_elements ? current_elements - desired_max_elements : 0;
    return collectEvictionInfo(
        size_to_evict, elements_to_evict,
        /* reservee */ nullptr,
        /* is_total_space_cleanup */ true,
        origin_info, lock);
}

bool LRUFileCachePriority::tryIncreasePriority(
    Iterator & iterator,
    bool /* is_space_reservation_complete */,
    CachePriorityGuard & queue_guard,
    CacheStateGuard &)
{
    auto lock = queue_guard.writeLock();
    const auto & entry = iterator.getEntry();
    VELOX_DCHECK(entry->getState() == Entry::State::Active);
    entry->hits += 1;

    auto it = dynamic_cast<const LRUFileCachePriority::LRUIterator &>(iterator).get();
    moveEvictionPosIfEqual(it, lock);
    queue.splice(queue.end(), queue, it);
    return true;
}

IFileCachePriority::EntryPtr LRUFileCachePriority::LRUIterator::getEntry() const
{
    assertValid();
    return entry.lock();
}

bool LRUFileCachePriority::LRUIterator::isValid(const CachePriorityGuard::WriteLock &) const
{
    return entry.lock() != nullptr && iterator != LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::remove(const CachePriorityGuard::WriteLock & lock)
{
    assertValid();
    cachePriority->remove(iterator, lock);
    iterator = LRUQueue::iterator{};
}

void LRUFileCachePriority::LRUIterator::invalidate()
{
    auto entry_ptr = entry.lock();
    VELOX_DCHECK(entry_ptr);

    VLOG(1) << fmt::format(
        "Invalidating entry in LRU queue {}: {}",
        entry_ptr->toString(), cachePriority->getApproxStateInfoForLog());

    size_t entry_size = entry_ptr->size;
    entry_ptr->size = 0;
    entry_ptr->setInvalidatedFlag();

    if (entry_size)
        cachePriority->state->sub(entry_size, 1);
}

void LRUFileCachePriority::LRUIterator::incrementSize(
    size_t size,
    const CacheStateGuard::Lock & lock)
{
    VELOX_DCHECK(size);
    assertValid();

    auto entry_ptr = entry.lock();
    VELOX_DCHECK(entry_ptr);

    size_t elements = entry_ptr->size > 0 ? 0 : 1;

    if (!cachePriority->canFit(size, elements, lock))
    {
        VELOX_FAIL("Cannot increment size by {} for entry {}. Current state: {}",
                   size, entry_ptr->toString(), cachePriority->getStateInfoForLog(lock));
    }

    VLOG(1) << fmt::format(
        "Incrementing size with {} in LRU queue for entry {}",
        size, entry_ptr->toString());

    cachePriority->state->add(size, elements, lock);
    entry_ptr->size += size;

    cachePriority->check(lock);
}

void LRUFileCachePriority::LRUIterator::decrementSize(size_t size)
{
    assertValid();

    auto entry_ptr = entry.lock();
    VELOX_DCHECK(entry_ptr);
    VELOX_DCHECK(entry_ptr->size >= 0);
    VELOX_DCHECK(entry_ptr->size >= size);

    VLOG(1) << fmt::format(
        "Decrement size with {} in LRU queue entry {}",
        size, entry_ptr->toString());

    cachePriority->state->sub(size, 0);
    entry_ptr->size -= size;
}

bool LRUFileCachePriority::LRUIterator::assertValid() const
{
    const bool is_iterator_valid = iterator != LRUQueue::iterator{};
    auto entry_ptr = entry.lock();
    if (!entry_ptr || !is_iterator_valid)
    {
        VELOX_FAIL(
            "Attempt to use invalid iterator (entry: {}, iterator: {})",
            bool(entry_ptr), is_iterator_valid);
    }
    VELOX_DCHECK(entry_ptr == *iterator);
    return true;
}

void LRUFileCachePriority::shuffle(const CachePriorityGuard::WriteLock &)
{
    // TODO(thread-safety): CH uses TSA_SUPPRESS_WARNING_FOR_READ(eviction_pos).
    VELOX_DCHECK(evictionPos == queue.end());
    std::vector<LRUQueue::iterator> its;
    its.reserve(queue.size());
    for (auto it = queue.begin(); it != queue.end(); ++it)
        its.push_back(it);
    // TODO(random): CH uses pcg64 generator(randomSeed()).
    std::mt19937_64 generator(folly::Random::rand64());
    std::shuffle(its.begin(), its.end(), generator);
    for (auto & it : its)
        queue.splice(queue.end(), queue, it);
}

std::string LRUFileCachePriority::getStateInfoForLog(const CacheStateGuard::Lock & lock) const
{
    return fmt::format(
        "size: {}/{}, elements: {}/{}, hold size: {}, hold elements: {}, description: {}",
        getSize(lock), maxSize.load(),
        getElementsCount(lock), maxElements.load(),
        totalHoldSize.load(), totalHoldElements.load(), description);
}

std::string LRUFileCachePriority::getApproxStateInfoForLog() const
{
    return fmt::format("size: {}/{}, elements: {}/{} (description: {})",
                       getSizeApprox(), maxSize.load(), getElementsCountApprox(), maxElements.load(), description);
}

void LRUFileCachePriority::holdImpl(
    size_t size,
    size_t elements,
    const CacheStateGuard::Lock & lock)
{
    VELOX_DCHECK(size || elements);

    if (!canFit(size, elements, lock))
    {
        VELOX_FAIL("Cannot take space {} in size and {} in elements. "
                   "({})", size, elements, getStateInfoForLog(lock));
    }

    state->add(size, elements, lock);

    totalHoldSize += size;
    totalHoldElements += elements;

    //VLOG(1) << fmt::format("Hold {} by size and {} by elements", size, elements);
}

void LRUFileCachePriority::releaseImpl(size_t size, size_t elements)
{
    auto lock = cacheUsageStatGuard
        ? std::optional<CacheUsageStatGuard::Lock>(cacheUsageStatGuard->lock())
        : std::nullopt;

    state->sub(size, elements);

    totalHoldSize -= size;
    totalHoldElements -= elements;

    //VLOG(1) << fmt::format("Released {} by size and {} by elements", size, elements);
}

LRUFileCachePriority::LRUQueue::iterator LRUFileCachePriority::getEvictionPos(const CachePriorityGuard::ReadLock &) const
{
    std::lock_guard lk(evictionPosMutex);
    return evictionPos;
}

void LRUFileCachePriority::setEvictionPos(LRUQueue::iterator it, const CachePriorityGuard::ReadLock &)
{
    std::lock_guard lk(evictionPosMutex);
    evictionPos = it;
}

void LRUFileCachePriority::moveEvictionPosIfEqual(LRUQueue::iterator it, const CachePriorityGuard::WriteLock &)
{
    std::lock_guard lk(evictionPosMutex);
    if (evictionPos != LRUQueue::iterator{} && evictionPos == it)
        evictionPos = std::next(it);
}
}
