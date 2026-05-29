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

#include <memory>
#include "velox/common/caching/filecache/Guards.h"
#include "velox/common/caching/filecache/IFileCachePriority.h"


namespace facebook::velox::ch {

/**
  * Wrapper for IFilecachePriority that keeps two IFilecachePriority inside:
  * Data: for `.bin`, `.mrk` and etc files
  * System: for indexes, `.json`, `.txt` files.
  * Such separations might be performance-useful.
  */
class SplitFileCachePriority : public IFileCachePriority
{
public:
    class SplitIterator;
    using IFileCachePriorityPtr = std::unique_ptr<IFileCachePriority>;
    using CachePriorityCreatorFunction
        = std::function<IFileCachePriorityPtr(size_t max_size, size_t max_elements, double size_ratio, size_t overcommit_eviction_evict_step, std::string description)>;
    using SegmentType = FileSegmentKeyType;
    using PriorityPerType = std::array<IFileCachePriorityPtr, 3>;

    SplitFileCachePriority(
        CachePriorityCreatorFunction creator_function,
        size_t max_size_,
        size_t max_elements_,
        double size_ratio,
        double system_segment_size_ratio_,
        const std::string & description_ = "none");

    Type getType() const override { return getPriority(SegmentType::Data).getType(); }

    size_t getSize(const CacheStateGuard::Lock &) const override;
    size_t getSizeApprox() const override;

    size_t getElementsCount(const CacheStateGuard::Lock &) const override;
    size_t getElementsCountApprox() const override;

    std::string getStateInfoForLog(const CacheStateGuard::Lock & lock) const override;

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

    bool tryIncreasePriority(
        Iterator & iterator,
        bool is_space_reservation_complete,
        CachePriorityGuard & queue_guard,
        CacheStateGuard & state_guard) override;

    EvictionInfoPtr collectEvictionInfo(
        size_t size,
        size_t elements,
        IFileCachePriority::Iterator * reservee,
        bool is_total_space_cleanup,
        const IFileCachePriority::OriginInfo & origin,
        const CacheStateGuard::Lock &) override;

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
        const CachePriorityGuard::ReadLock & lock) override;

    void shuffle(const CachePriorityGuard::WriteLock &) override;

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

    void resetEvictionPos() override;

protected:
    size_t getHoldSize() override;

    size_t getHoldElements() override;

private:
    SegmentType getPriorityType(const SegmentType & segment_type) const;

    IFileCachePriority & getPriority(SegmentType type) { return *prioritiesHolder[static_cast<uint8_t>(type)]; }
    const IFileCachePriority & getPriority(SegmentType type) const { return *prioritiesHolder[static_cast<uint8_t>(type)]; }

    PriorityPerType prioritiesHolder;
    double systemSegmentSizeRatio;
    size_t maxDataSegmentSize;
    size_t maxDataSegmentElements;
    size_t maxSystemSegmentSize;
    size_t maxSystemSegmentElements;

    // TODO(logging): CH LoggerPtr was used only by LOG_* call sites; map .cpp logs to glog.
};


class SplitFileCachePriority::SplitIterator : public IFileCachePriority::Iterator
{
    friend class SLRUFileCachePriority;

public:
    SplitIterator(
        IFileCachePriority * inner_cache_priority,
        IteratorPtr iterator_,
        FileSegmentKeyType type);

    EntryPtr getEntry() const override;

    void remove(const CachePriorityGuard::WriteLock &) override;

    void invalidate() override;

    void incrementSize(size_t size, const CacheStateGuard::Lock &) override;

    void decrementSize(size_t size) override;

    QueueEntryType getType() const override
    {
        return type == FileSegmentKeyType::Data
            ? QueueEntryType::SplitCache_Data
            : QueueEntryType::SplitCache_System;
    }

    const Iterator * getNestedOrThis() const override { return iterator->getNestedOrThis(); }
    Iterator * getNestedOrThis() override { return iterator->getNestedOrThis(); }

    const FileSegmentKeyType type;

private:
    void assertValid() const;

    IFileCachePriority * cachePriority;
    IFileCachePriority::IteratorPtr iterator;
    const std::weak_ptr<Entry> entry;
};

} // namespace facebook::velox::ch
