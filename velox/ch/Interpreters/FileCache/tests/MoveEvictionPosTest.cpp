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

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"

#include <gtest/gtest.h>

#include <iterator>
#include <memory>
#include <string>

// Port of ClickHouse `TEST_F(FileCacheTest, MoveEvictionPos)`
// (src/Interpreters/tests/gtest_filecache.cpp:2421). It proves that when
// `LRUFileCachePriority::move` splices an entry out of one queue, every eviction
// cursor that pointed at the spliced node advances to the next surviving node
// instead of dangling on the moved-away node (`moveEvictionPosIfEqual`).
//
// This test is DELIBERATELY at global scope (no enclosing namespace) and lives in
// its own standalone executable, `velox_ch_filecache_priority_cursor_test`:
//
//  * Global lexical scope is REQUIRED. `LRUFileCachePriority.h` grants
//    `friend class ::FileCacheTest_MoveEvictionPos_Test`, a global-namespace
//    class name. gtest's `TEST(FileCacheTest, MoveEvictionPos)` generates a class
//    named exactly `FileCacheTest_MoveEvictionPos_Test` in the namespace this
//    macro is lexically written in. At global scope it is
//    `::FileCacheTest_MoveEvictionPos_Test`, matching the friend; written inside
//    `facebook::velox::ch::(anonymous)` it would be a different class that the
//    friend does not name, and the private cursor calls below would not compile.
//
//  * A SEPARATE executable is also REQUIRED. gtest identifies a test suite by its
//    string name alone ("FileCacheTest"). A global `TEST(FileCacheTest, ...)`
//    linked into the same binary as `FileCacheTest.cpp`'s own
//    `TEST_F(FileCacheTest, ...)` fixture cases is a fatal test-suite collision
//    at `RUN_ALL_TESTS`. Keeping this one case in its own binary — never linked
//    with `FileCacheTest.cpp` — is the only correct fix.
//
// The existing `friend` is sufficient once the test is at the right scope; no new
// friend or public cursor accessor is added.
TEST(FileCacheTest, MoveEvictionPos)
{
    using namespace facebook::velox::ch;
    using facebook::velox::common::testutil::TempDirectoryPath;
    using Entry = IFileCachePriority::Entry;
    using EvictionCursor = IFileCachePriority::EvictionCursor;

    // Two independent LRU queues, modelling SLRU's protected/probationary
    // sub-queues between which `LRUFileCachePriority::move` transfers entries.
    LRUFileCachePriority src(IFileCachePriority::QueueType::Main, /*max_size*/ 100, /*max_elements*/ 10, "src");
    LRUFileCachePriority dst(IFileCachePriority::QueueType::Main, /*max_size*/ 100, /*max_elements*/ 10, "dst");

    // A standalone, manager-injected CacheMetadata mints a real KeyMetadata,
    // exactly the way PriorityEvictionTest::SetUp/makeKeyMetadata does. A plain
    // global-scope TEST has no fixture SetUp to share it with, so it is inlined
    // here (authorized by the Task-011 corrective dependency pre-check).
    auto pool = facebook::velox::memory::deprecatedAddDefaultLeafMemoryPool("move-eviction-pos-test");
    auto cacheDir = TempDirectoryPath::create();
    FileCacheWorkerPool workerPool{4, 1, "move-pos-test"};
    CacheMetadata metadata(
        cacheDir->getPath(),
        /*background_download_queue_size_limit*/ 0,
        /*background_download_threads*/ 0,
        /*write_cache_per_user_directory*/ false,
        workerPool,
        pool.get(),
        /*reserve_space_wait_lock_timeout_ms*/ 1000,
        [](const std::string &) {},
        /*common_user_id*/ std::string("common-user"));

    const auto key = FileCacheKey::random();
    FileCacheOriginInfo origin("user", /*weight*/ 100, FileSegmentKeyType::General);
    auto key_metadata = metadata.getKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin);

    CacheStateGuard state_guard;
    CachePriorityGuard cache_guard;

    // add() here is the PRIVATE add(EntryPtr, WriteLock, StateLock*) overload,
    // reachable only through the granted friend `::FileCacheTest_MoveEvictionPos_Test`
    // (exactly as the CH test uses the private overload). It returns an
    // LRUIterator by value.
    auto add_to_src = [&](size_t offset, size_t size) {
        auto write_lock = cache_guard.writeLock();
        auto state_lock = state_guard.lock();
        return src.add(std::make_shared<Entry>(key, offset, size, key_metadata), write_lock, &state_lock);
    };

    // src queue: [offset 0, offset 10, offset 20].
    add_to_src(0, 10);
    auto it_middle = add_to_src(10, 10);
    add_to_src(20, 10);

    // Point BOTH eviction cursors at the middle entry -- the one about to move out.
    {
        auto read_lock = cache_guard.readLock();
        src.setEvictionPos(EvictionCursor::Reserve, it_middle.get(), read_lock);
        src.setEvictionPos(EvictionCursor::Background, it_middle.get(), read_lock);
    }
    ASSERT_EQ((*src.getEvictionPos(EvictionCursor::Reserve, cache_guard.readLock()))->offset, 10u);
    ASSERT_EQ((*src.getEvictionPos(EvictionCursor::Background, cache_guard.readLock()))->offset, 10u);

    // Move the middle entry out of src into dst (as an SLRU upgrade/downgrade
    // would). move() is called on the destination queue; src is the source.
    {
        auto write_lock = cache_guard.writeLock();
        auto state_lock = state_guard.lock();
        dst.move(it_middle, src, write_lock, state_lock);
    }

    // The moved node was spliced out of src, so src's eviction position must
    // advance to the next surviving src entry (offset 20). Before the fix it kept
    // pointing at the moved node, which now lives in dst (offset 10) -- a dangling
    // cross-queue eviction position. Both cursors were set at the moved node, so
    // moveEvictionPosIfEqual must advance BOTH: a regression advancing only one
    // would leave the other dangling.
    ASSERT_EQ((*src.getEvictionPos(EvictionCursor::Reserve, cache_guard.readLock()))->offset, 20u);
    ASSERT_EQ((*src.getEvictionPos(EvictionCursor::Background, cache_guard.readLock()))->offset, 20u);

    // The two cursors are independent: resetting one must not disturb the other.
    // Put them at different positions, reset only Reserve, and check Background is
    // untouched. A regression where resetEvictionPos(Reserve) also cleared
    // Background would be caught here.
    {
        auto read_lock = cache_guard.readLock();
        src.setEvictionPos(EvictionCursor::Reserve, src.queue.begin(), read_lock);
        src.setEvictionPos(EvictionCursor::Background, std::next(src.queue.begin()), read_lock);
    }
    src.resetEvictionPos(EvictionCursor::Reserve);
    ASSERT_EQ(src.getEvictionPosCount(EvictionCursor::Reserve), 0u);
    ASSERT_EQ(src.getEvictionPosCount(EvictionCursor::Background), 1u);

    metadata.shutdown();
}
