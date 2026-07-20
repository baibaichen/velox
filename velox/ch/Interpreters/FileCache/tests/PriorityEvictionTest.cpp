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
#include "velox/ch/Interpreters/FileCache/LRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SLRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SplitFileCachePriority.h"

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using velox::common::testutil::TempDirectoryPath;

// The Task-011 priority/eviction sources are exercised against real KeyMetadata
// (minted through a standalone, manager-injected CacheMetadata) and the real
// cache guards. These cases are the priority half of the amended Task-012
// mandatory rows (LRU/SLRU/Split behaviour); releasable reserve eviction against
// the full FileCache lives in FileCacheTest.
class PriorityEvictionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool("priority-test");
        cacheDir_ = TempDirectoryPath::create();
        metadata_ = std::make_unique<CacheMetadata>(
            cacheDir_->getPath(),
            /*background_download_queue_size_limit*/ 0,
            /*background_download_threads*/ 0,
            /*write_cache_per_user_directory*/ false,
            workerPool_,
            pool_.get(),
            /*reserve_space_wait_lock_timeout_ms*/ 1000,
            [](const std::string &) {},
            /*common_user_id*/ std::string("common-user"));
    }

    void TearDown() override
    {
        metadata_->shutdown();
        metadata_.reset();
    }

    // Mint a real KeyMetadata for a fresh key with the given segment type.
    KeyMetadataPtr makeKeyMetadata(FileSegmentKeyType type = FileSegmentKeyType::General)
    {
        const auto key = FileCacheKey::random();
        FileCacheOriginInfo origin("user", /*weight*/ 100, type);
        return metadata_->getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin);
    }

    FileCacheWorkerPool workerPool_{4, 1, "prio-test"};
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<TempDirectoryPath> cacheDir_;
    std::unique_ptr<CacheMetadata> metadata_;
    CachePriorityGuard queue_guard_;
    CacheStateGuard state_guard_;
};

// -- LRU add/remove and byte/element accounting -----------------------------

TEST_F(PriorityEvictionTest, LRUAddRemoveAndStableIterator)
{
    LRUFileCachePriority lru(IFileCachePriority::QueueType::Main, /*max_size*/ 1 << 20, /*max_elements*/ 100);

    auto km1 = makeKeyMetadata();
    auto km2 = makeKeyMetadata();

    IFileCachePriority::IteratorPtr it1;
    IFileCachePriority::IteratorPtr it2;
    {
        auto write_lock = queue_guard_.writeLock();
        auto state_lock = state_guard_.lock();
        it1 = lru.add(km1, /*offset*/ 0, /*size*/ 100, write_lock, &state_lock);
        it2 = lru.add(km2, /*offset*/ 0, /*size*/ 200, write_lock, &state_lock);
    }

    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(lru.getSize(state_lock), 300u);
        EXPECT_EQ(lru.getElementsCount(state_lock), 2u);
    }

    // The iterator's entry is stable across further adds (same key/offset/size).
    EXPECT_EQ(it1->getEntry()->key, km1->key);
    EXPECT_EQ(it1->getEntry()->offset, 0u);
    EXPECT_EQ(it1->getEntry()->size.load(), 100u);

    // Removing one entry decrements both bytes and element count.
    {
        auto write_lock = queue_guard_.writeLock();
        it1->remove(write_lock);
    }
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(lru.getSize(state_lock), 200u);
        EXPECT_EQ(lru.getElementsCount(state_lock), 1u);
    }
}

// -- decrementing an entry's size to zero drops it from the element count ----

TEST_F(PriorityEvictionTest, DecrementSizeToZeroDropsElement)
{
    LRUFileCachePriority lru(IFileCachePriority::QueueType::Main, 1 << 20, 100);
    auto km = makeKeyMetadata();

    IFileCachePriority::IteratorPtr it;
    {
        auto write_lock = queue_guard_.writeLock();
        auto state_lock = state_guard_.lock();
        it = lru.add(km, 0, 100, write_lock, &state_lock);
    }
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(lru.getSize(state_lock), 100u);
        EXPECT_EQ(lru.getElementsCount(state_lock), 1u);
    }

    // Shrinking to zero bytes leaves neither bytes nor an element counted.
    it->decrementSize(100);
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(lru.getSize(state_lock), 0u);
        EXPECT_EQ(lru.getElementsCount(state_lock), 0u);
    }
}

// -- SLRU second access promotes a probationary entry to protected ----------

TEST_F(PriorityEvictionTest, SLRUSecondAccessPromotesProbationaryToProtected)
{
    SLRUFileCachePriority slru(
        IFileCachePriority::QueueType::Main, /*max_size*/ 1 << 20, /*max_elements*/ 100, /*size_ratio*/ 0.5);

    auto km = makeKeyMetadata();
    IFileCachePriority::IteratorPtr it;
    {
        auto write_lock = queue_guard_.writeLock();
        auto state_lock = state_guard_.lock();
        it = slru.add(km, 0, 100, write_lock, &state_lock);
    }

    // A newly added entry lands in the probationary queue.
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(slru.getProbationarySize(state_lock), 100u);
        EXPECT_EQ(slru.getProtectedSize(state_lock), 0u);
    }

    // A second access (space reservation complete) promotes it to protected.
    EXPECT_TRUE(slru.tryIncreasePriority(*it, /*is_space_reservation_complete*/ true, queue_guard_, state_guard_));
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(slru.getProbationarySize(state_lock), 0u);
        EXPECT_EQ(slru.getProtectedSize(state_lock), 100u);
    }
}

// -- Split partitions bytes/elements across its per-type inner priorities ----

TEST_F(PriorityEvictionTest, SplitPartitionsBytesAndElements)
{
    // A Split cache wrapping LRU inner queues, routing by segment key type.
    auto creator = [](IFileCachePriority::QueueType queue_type,
                      size_t max_size,
                      size_t max_elements,
                      double /*size_ratio*/,
                      size_t /*overcommit_eviction_evict_step*/,
                      String description) -> SplitFileCachePriority::IFileCachePriorityPtr
    {
        return std::make_unique<LRUFileCachePriority>(queue_type, max_size, max_elements, description);
    };

    SplitFileCachePriority split(
        IFileCachePriority::QueueType::Main,
        creator,
        /*max_size*/ 1 << 20,
        /*max_elements*/ 100,
        /*size_ratio*/ 0.5,
        /*system_segment_size_ratio*/ 0.5);

    auto dataKey = makeKeyMetadata(FileSegmentKeyType::Data);
    auto systemKey = makeKeyMetadata(FileSegmentKeyType::System);

    {
        auto write_lock = queue_guard_.writeLock();
        auto state_lock = state_guard_.lock();
        split.add(dataKey, 0, 100, write_lock, &state_lock);
        split.add(systemKey, 0, 200, write_lock, &state_lock);
    }

    // Both entries are accounted in the aggregate totals regardless of routing.
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(split.getSize(state_lock), 300u);
        EXPECT_EQ(split.getElementsCount(state_lock), 2u);
    }
}

// -- modifySizeLimits resizes the LRU limits --------------------------------

TEST_F(PriorityEvictionTest, ModifySizeLimits)
{
    LRUFileCachePriority lru(IFileCachePriority::QueueType::Main, /*max_size*/ 1000, /*max_elements*/ 10);
    {
        auto state_lock = state_guard_.lock();
        EXPECT_EQ(lru.getSizeLimit(state_lock), 1000u);
        EXPECT_EQ(lru.getElementsLimit(state_lock), 10u);

        EXPECT_TRUE(lru.modifySizeLimits(/*max_size*/ 2000, /*max_elements*/ 20, /*size_ratio*/ 0, state_lock));
        EXPECT_EQ(lru.getSizeLimit(state_lock), 2000u);
        EXPECT_EQ(lru.getElementsLimit(state_lock), 20u);
    }
}

// -- SLRU modifySizeLimits is all-or-nothing: a throw rolls back ------------
//    (ClickHouse gtest_filecache.cpp SLRUModifySizeLimitsRollbackOnThrow)

TEST_F(PriorityEvictionTest, SLRUModifySizeLimitsRollbackOnThrow)
{
    // modifySizeLimits updates the protected sub-queue first, then the
    // probationary sub-queue. If the probationary update throws, the already
    // applied protected limit must be rolled back to its previous value. The
    // throw is injected at the production failpoint (armed via TestValue) exactly
    // between the two sub-queue updates; nothing else in the resize is invalid.
    const size_t max_size = 30;
    const size_t max_elements = 6;
    const double slru_size_ratio = 0.5; // protected 15/3, probationary 15/3
    SLRUFileCachePriority priority(
        IFileCachePriority::QueueType::Main, max_size, max_elements, slru_size_ratio, "test_slru_modify_rollback");

    // One small 5-byte entry in each sub-queue, fitting under old and new limits.
    auto km = makeKeyMetadata();
    {
        auto write_lock = queue_guard_.writeLock();
        auto state_lock = state_guard_.lock();
        priority.addForRestore(km, 0, 5, IFileCachePriority::QueueEntryType::SLRU_Protected, write_lock, &state_lock);
        priority.addForRestore(km, 100, 5, IFileCachePriority::QueueEntryType::SLRU_Probationary, write_lock, &state_lock);
    }
    EXPECT_EQ(priority.getProtectedSize(state_guard_.lock()), 5u);
    EXPECT_EQ(priority.getProbationarySize(state_guard_.lock()), 5u);
    EXPECT_EQ(priority.getProtectedSizeLimit(state_guard_.lock()), 15u);

    facebook::velox::common::testutil::TestValue::enable();
    SCOPED_TESTVALUE_SET(
        "facebook::velox::ch::filecache::failpoint::file_cache_modify_size_limits_fail",
        std::function<void(void *)>([](void *) { VELOX_FAIL("Injected fault in modifySizeLimits"); }));

    {
        // Resize total to 20 (ratio 0.5 -> protected limit 10). The resize is valid
        // by itself (current 5/1 fit 10/3); only the injected failpoint throws.
        auto state_lock = state_guard_.lock();
        EXPECT_ANY_THROW(priority.modifySizeLimits(20, max_elements, slru_size_ratio, state_lock));
    }

    // With the rollback bug the protected limit was left shrunk to 10; with the fix
    // it is restored to the original 15.
    EXPECT_EQ(priority.getProtectedSizeLimit(state_guard_.lock()), 15u);
}

} // namespace
} // namespace facebook::velox::ch
