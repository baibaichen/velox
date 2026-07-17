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

#include "velox/ch/Common/ThreadPool.h"
#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// FileCacheWorkerPool helpers
// ---------------------------------------------------------------------------

static FileCacheWorkerPool makePool(
    size_t maxThreads = 4,
    size_t minThreads = 1,
    const std::string & prefix = "test-pool")
{
    return FileCacheWorkerPool(maxThreads, minThreads, prefix);
}

// ---------------------------------------------------------------------------
// FileCacheWorker tests
// ---------------------------------------------------------------------------

TEST(FileCacheWorkerTest, AliasIsFileCacheWorker)
{
    static_assert(std::is_same_v<ThreadFromGlobalPool, FileCacheWorker>);
}

TEST(FileCacheWorkerTest, WorkerRunsFunctionAndIsJoinable)
{
    auto pool = makePool();
    std::promise<void> done;
    auto doneFuture = done.get_future();

    FileCacheWorker worker(pool, [&done]
    {
        done.set_value();
    });

    EXPECT_TRUE(worker.joinable());
    ASSERT_EQ(doneFuture.wait_for(5s), std::future_status::ready);
    worker.join();
    EXPECT_FALSE(worker.joinable());
}

TEST(FileCacheWorkerTest, JoinPropagatesException)
{
    auto pool = makePool();
    std::promise<void> started;
    auto startedFuture = started.get_future();

    FileCacheWorker worker(pool, [&started]
    {
        started.set_value();
        throw std::runtime_error("worker error");
    });

    startedFuture.get();
    EXPECT_THROW(worker.join(), std::runtime_error);
}

TEST(FileCacheWorkerTest, MoveTransfersOwnership)
{
    auto pool = makePool();
    std::promise<void> done;
    auto doneFuture = done.get_future();

    FileCacheWorker w1(pool, [&done] { done.set_value(); });
    FileCacheWorker w2 = std::move(w1);

    EXPECT_FALSE(w1.joinable()); // NOLINT: intentional move-from use
    EXPECT_TRUE(w2.joinable());

    doneFuture.get();
    w2.join();
}

TEST(FileCacheWorkerTest, DestructorOnUnjoinedWorkerChecks)
{
    auto pool = makePool();
    std::promise<void> done;
    auto doneFuture = done.get_future();

    auto * worker = new FileCacheWorker(pool, [&done] { done.set_value(); });
    doneFuture.get(); // let function finish so the issue is only about join

    // The destructor is noexcept like std::thread; an unjoined worker is fatal.
    EXPECT_DEATH({ delete worker; }, "");
}

TEST(FileCacheWorkerTest, MoveAssignmentIntoNonJoinableTargetSucceeds)
{
    auto pool = makePool();
    std::promise<void> done1;
    auto doneFuture1 = done1.get_future();

    // A default-constructed worker is never joinable, so moving into it must
    // succeed and simply transfer ownership.
    FileCacheWorker target;
    EXPECT_FALSE(target.joinable());

    FileCacheWorker source(pool, [&done1] { done1.set_value(); });
    target = std::move(source);

    EXPECT_FALSE(source.joinable()); // NOLINT: intentional move-from use
    EXPECT_TRUE(target.joinable());

    doneFuture1.get();
    target.join();
    EXPECT_FALSE(target.joinable());

    // An already-joined (but still constructed) target is also not joinable,
    // so moving into it again must succeed too.
    std::promise<void> done2;
    auto doneFuture2 = done2.get_future();
    FileCacheWorker source2(pool, [&done2] { done2.set_value(); });
    target = std::move(source2);

    EXPECT_TRUE(target.joinable());
    doneFuture2.get();
    target.join();
}

TEST(FileCacheWorkerTest, MoveAssignmentOverJoinableTargetChecks)
{
    auto pool = makePool();
    std::promise<void> targetDone;
    auto targetDoneFuture = targetDone.get_future();
    std::promise<void> sourceDone;
    auto sourceDoneFuture = sourceDone.get_future();

    FileCacheWorker target(pool, [&targetDone] { targetDone.set_value(); });
    targetDoneFuture.get(); // let the function finish; only join() is missing

    FileCacheWorker source(pool, [&sourceDone] { sourceDone.set_value(); });
    sourceDoneFuture.get();

    // Move-assigning into a still-joinable target must be fatal, exactly
    // like destroying a joinable worker without join() first: it would
    // silently drop the only handle able to join target's background task.
    EXPECT_DEATH({ target = std::move(source); }, "");

    // The statement above ran (and aborted) only inside a forked child
    // process; target and source are unaffected here. Join both so their
    // destructors don't trip the same invariant when this scope ends.
    target.join();
    source.join();
}

// ---------------------------------------------------------------------------
// FileCacheWorkerPool tests
// ---------------------------------------------------------------------------

TEST(FileCacheWorkerPoolTest, DynamicPoolStartsWorkersOnDemand)
{
    FileCacheWorkerPool pool(4, 1, "dynamic-pool");
    std::atomic<int> counter{0};
    std::vector<FileCacheWorker> workers;

    for (int i = 0; i < 4; ++i)
    {
        workers.emplace_back(pool, [&counter]
        {
            counter.fetch_add(1, std::memory_order_relaxed);
        });
    }

    for (auto & w : workers)
        w.join();

    EXPECT_EQ(counter.load(), 4);
}

TEST(FileCacheWorkerPoolTest, SetNumThreadsGrows)
{
    FileCacheWorkerPool pool(2, 1, "resize-pool");
    pool.setNumThreads(8);

    std::atomic<int> counter{0};
    std::vector<FileCacheWorker> workers;
    for (int i = 0; i < 8; ++i)
        workers.emplace_back(pool, [&counter] { counter.fetch_add(1); });
    for (auto & w : workers)
        w.join();
    EXPECT_EQ(counter.load(), 8);
}

// ---------------------------------------------------------------------------
// FileCacheThreadPool tests
// ---------------------------------------------------------------------------

TEST(FileCacheThreadPoolTest, AliasIsFileCacheThreadPool)
{
    static_assert(std::is_same_v<ThreadPool, FileCacheThreadPool>);
}

TEST(FileCacheThreadPoolTest, ScheduleAndWaitCompletesAllTasks)
{
    FileCacheWorkerPool workerPool(4, 1, "logical-pool");
    FileCacheThreadPool pool(workerPool, /*maxThreads=*/4, /*queueSize=*/16);

    std::atomic<int> counter{0};
    for (int i = 0; i < 8; ++i)
    {
        pool.scheduleOrThrowOnError([&counter]
        {
            counter.fetch_add(1, std::memory_order_relaxed);
        });
    }

    pool.wait();
    EXPECT_EQ(counter.load(), 8);
}

TEST(FileCacheThreadPoolTest, WaitRethrowsFirstException)
{
    FileCacheWorkerPool workerPool(4, 1, "exc-pool");
    FileCacheThreadPool pool(workerPool, 4, 16);

    pool.scheduleOrThrowOnError([]
    {
        throw std::runtime_error("pool task error");
    });

    EXPECT_THROW(pool.wait(), std::runtime_error);
}

TEST(FileCacheThreadPoolTest, LocalConcurrencyRespectedByMetadataWorkers)
{
    // Simulate metadata listing: up to maxThreads workers blocked on a barrier.
    // All must run concurrently (no starvation) when pool is large enough.
    constexpr int kMetadataThreads = 4;
    FileCacheWorkerPool workerPool(kMetadataThreads + 2, 1, "meta-pool");
    FileCacheThreadPool pool(workerPool, kMetadataThreads, kMetadataThreads * 2);

    std::promise<void> barrier;
    auto barrierFuture = barrier.get_future().share();
    std::atomic<int> atBarrier{0};
    std::promise<void> allAtBarrier;
    auto allAtBarrierFuture = allAtBarrier.get_future();

    for (int i = 0; i < kMetadataThreads; ++i)
    {
        pool.scheduleOrThrowOnError([&atBarrier, &allAtBarrier, barrierFuture, kMetadataThreads]() mutable
        {
            if (atBarrier.fetch_add(1) + 1 == kMetadataThreads)
                allAtBarrier.set_value();
            barrierFuture.wait();
        });
    }

    ASSERT_EQ(allAtBarrierFuture.wait_for(5s), std::future_status::ready);
    barrier.set_value();
    pool.wait();
    EXPECT_EQ(atBarrier.load(), kMetadataThreads);
}

TEST(FileCacheThreadPoolTest, LocalMaxThreadsCapsConcurrencyWithoutBlockingSharedPool)
{
    // The shared physical pool has strictly more capacity than this logical
    // pool's maxThreads. FileCacheThreadPool itself must still cap its own
    // in-flight tasks at maxThreads, so another cache sharing the same
    // physical pool is never starved by this instance.
    constexpr size_t kMaxThreads = 2;
    FileCacheWorkerPool workerPool(kMaxThreads + 4, 1, "cap-pool");
    FileCacheThreadPool pool(workerPool, kMaxThreads, /*queueSize=*/16);

    std::promise<void> gate;
    auto gateFuture = gate.get_future().share();
    std::atomic<size_t> atGate{0};
    std::promise<void> allAtGate;
    auto allAtGateFuture = allAtGate.get_future();

    // Occupy exactly kMaxThreads slots with tasks blocked at a gate.
    for (size_t i = 0; i < kMaxThreads; ++i)
    {
        pool.scheduleOrThrowOnError([&atGate, &allAtGate, gateFuture]() mutable
        {
            if (atGate.fetch_add(1) + 1 == kMaxThreads)
                allAtGate.set_value();
            gateFuture.wait();
        });
    }
    ASSERT_EQ(allAtGateFuture.wait_for(5s), std::future_status::ready);

    // Submit one more task beyond maxThreads. Even though the shared
    // physical pool has spare worker capacity, this extra task must not
    // start while all of this instance's maxThreads slots remain occupied.
    std::promise<void> extraStarted;
    auto extraStartedFuture = extraStarted.get_future();
    pool.scheduleOrThrowOnError([&extraStarted]
    {
        extraStarted.set_value();
    });

    EXPECT_EQ(extraStartedFuture.wait_for(200ms), std::future_status::timeout);
    EXPECT_EQ(atGate.load(), kMaxThreads);

    // Releasing the gate frees a slot; the extra task must now be able to
    // start and complete, and wait() must observe every task done.
    gate.set_value();
    ASSERT_EQ(extraStartedFuture.wait_for(5s), std::future_status::ready);
    pool.wait();
}

TEST(FileCacheThreadPoolTest, SafeShrinkPreconditionDocumented)
{
    // shrink (setNumThreads on worker pool) must only happen after all tasks
    // using the old capacity have been joined / wait()ed.  This test verifies
    // that setNumThreads on the backing FileCacheWorkerPool is safe to call
    // after wait().
    FileCacheWorkerPool workerPool(8, 1, "shrink-pool");
    FileCacheThreadPool pool(workerPool, 8, 32);

    std::atomic<int> counter{0};
    for (int i = 0; i < 8; ++i)
        pool.scheduleOrThrowOnError([&counter] { counter.fetch_add(1); });
    pool.wait();

    // Safe to shrink the backing executor now that all tasks are done.
    workerPool.setNumThreads(2);
    EXPECT_EQ(counter.load(), 8);
}

} // namespace
} // namespace facebook::velox::ch
