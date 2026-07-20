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

#include "velox/ch/Interpreters/FileCache/EvictionCandidates.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/LRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SLRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SplitFileCachePriority.h"

#include <gtest/gtest.h>

#include <memory>

namespace facebook::velox::ch
{
namespace
{

using QueueType = IFileCachePriority::QueueType;

/// Creator used by SplitFileCachePriority to build its two inner LRU queues.
IFileCachePriorityPtr makeLru(
    QueueType queue_type, size_t max_size, size_t max_elements, double /*ratio*/, size_t /*step*/, String desc)
{
    return std::make_unique<LRUFileCachePriority>(queue_type, max_size, max_elements, desc);
}

/// LRU limits and empty-state accounting (production getters under real cache locks).
TEST(PriorityEvictionTest, LruLimitsAndEmptyState)
{
    LRUFileCachePriority lru(QueueType::Main, /*max_size*/ 1000, /*max_elements*/ 10);
    CacheStateGuard state_guard;
    auto lock = state_guard.lock();

    EXPECT_EQ(lru.getType(), IFileCachePriority::Type::LRU);
    EXPECT_EQ(lru.getQueueType(), QueueType::Main);
    EXPECT_EQ(lru.getSizeLimit(lock), 1000u);
    EXPECT_EQ(lru.getElementsLimit(lock), 10u);
    EXPECT_EQ(lru.getSize(lock), 0u);
    EXPECT_EQ(lru.getElementsCount(lock), 0u);
    EXPECT_EQ(lru.getSizeApprox(), 0u);
    EXPECT_EQ(lru.getElementsCountApprox(), 0u);
}

/// canFit reflects the configured limits on an empty queue (fits within, rejects beyond).
TEST(PriorityEvictionTest, LruCanFitRespectsLimits)
{
    LRUFileCachePriority lru(QueueType::Main, /*max_size*/ 1000, /*max_elements*/ 10);
    CacheStateGuard state_guard;
    auto lock = state_guard.lock();

    EXPECT_TRUE(lru.canFit(/*size*/ 1000, /*elements*/ 10, lock));
    EXPECT_FALSE(lru.canFit(/*size*/ 1001, /*elements*/ 1, lock));
    EXPECT_FALSE(lru.canFit(/*size*/ 1, /*elements*/ 11, lock));
}

/// HoldSpace reserves and releases hold accounting (RAII, production holdImpl/releaseImpl).
/// Held space is observed through the public canFit (getHoldSize/Elements are protected).
TEST(PriorityEvictionTest, LruHoldSpaceAccounting)
{
    LRUFileCachePriority lru(QueueType::Main, 1000, 10);
    CacheStateGuard state_guard;
    {
        auto lock = state_guard.lock();
        // Before holding, the whole capacity is free.
        EXPECT_TRUE(lru.canFit(/*size*/ 1000, /*elements*/ 1, lock));
        IFileCachePriority::HoldSpace hold(/*size*/ 100, /*elements*/ 1, lru, lock);
        EXPECT_EQ(hold.getSize(), 100u);
        EXPECT_EQ(hold.getElements(), 1u);
        // Holding reduces the space canFit sees as free: 100 bytes + 1 element are held.
        EXPECT_FALSE(lru.canFit(/*size*/ 1000, /*elements*/ 1, lock));
        EXPECT_TRUE(lru.canFit(/*size*/ 900, /*elements*/ 1, lock));
    } // hold released in destructor
    {
        auto lock = state_guard.lock();
        // Space returns after release.
        EXPECT_TRUE(lru.canFit(1000, 1, lock));
    }
}

/// SLRU exposes its two-queue structure with the configured size ratio.
TEST(PriorityEvictionTest, SlruTypeAndRatio)
{
    SLRUFileCachePriority slru(QueueType::Main, /*max_size*/ 1000, /*max_elements*/ 10, /*ratio*/ 0.6);
    CacheStateGuard state_guard;
    auto lock = state_guard.lock();

    EXPECT_EQ(slru.getType(), IFileCachePriority::Type::SLRU);
    EXPECT_DOUBLE_EQ(slru.getSLRUSizeRatio(), 0.6);
    EXPECT_EQ(slru.getSize(lock), 0u);
    EXPECT_EQ(slru.getElementsCount(lock), 0u);
    // The protected queue's size limit is the ratio of the total (0.6 * 1000).
    EXPECT_EQ(slru.getProtectedSizeLimit(lock), 600u);
    // The probationary queue holds the remainder.
    EXPECT_EQ(slru.getProbationarySizeLimit(lock), 400u);
}

/// Split partitions the total size/elements between its Data and System sub-priorities;
/// the aggregate limit still equals the configured maximum.
TEST(PriorityEvictionTest, SplitPartitionsAndAggregates)
{
    SplitFileCachePriority split(
        QueueType::Main, makeLru, /*max_size*/ 1000, /*max_elements*/ 100,
        /*size_ratio*/ 0.0, /*system_segment_size_ratio*/ 0.25);
    CacheStateGuard state_guard;
    auto lock = state_guard.lock();

    // The Split reports the Data sub-priority's algorithm type.
    EXPECT_EQ(split.getType(), IFileCachePriority::Type::LRU);
    // Aggregate limit and empty state.
    EXPECT_EQ(split.getSizeLimitApprox(), 1000u);
    EXPECT_EQ(split.getSize(lock), 0u);
    EXPECT_EQ(split.getElementsCount(lock), 0u);
    // The System partition takes 25% and Data the rest, so neither alone equals the total,
    // but the aggregate canFit tolerates the full 1000 across both partitions of size only if
    // each partition individually can hold its share. A single request beyond the total is rejected.
    EXPECT_FALSE(split.canFit(/*size*/ 1001, /*elements*/ 1, lock));
}

/// EvictionCandidates bookkeeping: empty on construction; getOriginalQueueType returns None
/// before removeQueueEntries is ever called.
TEST(PriorityEvictionTest, EvictionCandidatesEmptyBookkeeping)
{
    EvictionCandidates candidates(/*on_evict_callback*/ nullptr);
    EXPECT_EQ(candidates.size(), 0u);
    EXPECT_EQ(candidates.bytes(), 0u);
    EXPECT_FALSE(candidates.requiresAfterEvictWrite());
    EXPECT_FALSE(candidates.requiresAfterEvictState());
    // No candidate recorded, so its original queue type is None.
    EXPECT_EQ(
        candidates.getOriginalQueueType(reinterpret_cast<const FileSegmentMetadata *>(0x1)),
        IFileCachePriority::QueueEntryType::None);
    EXPECT_EQ(candidates.getFailedCandidates().size(), 0u);
}

/// EvictionInfo keeps separate QueueEvictionInfo per QueueID and aggregates targets;
/// takeKeptAliveCacheUsage merges usage pins with shared_ptr dedup (the B1 portability path).
TEST(PriorityEvictionTest, EvictionInfoPerQueueAndUsagePins)
{
    auto q0 = std::make_unique<QueueEvictionInfo>("q0", "user-A");
    q0->size_to_evict = 100;
    q0->elements_to_evict = 1;
    EvictionInfo info(/*queue_id*/ 0, std::move(q0));

    EXPECT_EQ(info.getSizeToEvict(), 100u);
    EXPECT_EQ(info.getElementsToEvict(), 1u);
    EXPECT_TRUE(info.requiresEviction());
    EXPECT_EQ(info.get(0).size_to_evict, 100u);

    // Add a second queue's info; totals aggregate across queue ids.
    auto q1 = std::make_unique<QueueEvictionInfo>("q1", "user-B");
    q1->size_to_evict = 50;
    info.add(std::make_unique<EvictionInfo>(1, std::move(q1)));
    EXPECT_EQ(info.getSizeToEvict(), 150u);
    EXPECT_EQ(info.get(1).size_to_evict, 50u);

    // takeKeptAliveCacheUsage merges the source's usage pins into ours and clears the source
    // (B1 portability path: insert-range loop instead of folly F14FastSet::merge). Exercised here
    // with an empty source so it runs the loop + clear without throwing.
    EvictionInfo source;
    info.takeKeptAliveCacheUsage(source);
    SUCCEED();
}

} // namespace
} // namespace facebook::velox::ch
