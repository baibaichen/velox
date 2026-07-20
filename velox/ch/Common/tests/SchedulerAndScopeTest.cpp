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

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/common/base/Exceptions.h"

#include <folly/futures/ManualTimekeeper.h>
#include <folly/system/ThreadId.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

using namespace std::chrono_literals;

// Construct a scheduler backed by a ManualTimekeeper and a small worker pool.
struct TestScheduler
{
    std::shared_ptr<folly::ManualTimekeeper> tk =
        std::make_shared<folly::ManualTimekeeper>();
    FileCacheWorkerPool pool{4, 1, "sched-test"};
    FileCacheScheduler scheduler{tk, pool};
};

// ---------------------------------------------------------------------------
// Alias tests
// ---------------------------------------------------------------------------

TEST(FileCacheSchedulerTest, AliasesAreCorrect)
{
    static_assert(std::is_same_v<BackgroundSchedulePool, FileCacheScheduler>);
    static_assert(
        std::is_same_v<
            BackgroundSchedulePoolTaskHolder,
            FileCacheScheduledTaskHolder>);
}

// ---------------------------------------------------------------------------
// schedule / scheduleAfter tests
// ---------------------------------------------------------------------------

TEST(FileCacheSchedulerTest, ScheduleDispatchesExactlyOneCallback)
{
    TestScheduler ts;
    auto holder = ts.scheduler.createTask("test-task", [] {});
    ASSERT_TRUE(static_cast<bool>(holder));

    std::promise<void> ran;
    auto ranFuture = ran.get_future();

    holder->setCallback([&ran]
    {
        ran.set_value();
    });

    holder->schedule();

    ASSERT_EQ(ranFuture.wait_for(5s), std::future_status::ready);
}

TEST(FileCacheSchedulerTest, ScheduleAfterRunsAfterTimekeeperAdvance)
{
    TestScheduler ts;

    std::promise<void> ran;
    auto ranFuture = ran.get_future();

    auto holder = ts.scheduler.createTask("delayed-task", [&ran]
    {
        ran.set_value();
    });

    holder->scheduleAfter(100);  // 100 ms

    // Not yet fired.
    EXPECT_EQ(ranFuture.wait_for(0ms), std::future_status::timeout);

    ts.tk->advance(200ms);

    ASSERT_EQ(ranFuture.wait_for(5s), std::future_status::ready);
}

TEST(FileCacheSchedulerTest, ScheduleAdvancesDelayedTask)
{
    TestScheduler ts;

    std::atomic<int> count{0};
    std::promise<void> firstRan;
    auto firstFuture = firstRan.get_future();

    auto holder = ts.scheduler.createTask("advance-task", [&count, &firstRan]
    {
        if (count.fetch_add(1) == 0)
            firstRan.set_value();
    });

    holder->scheduleAfter(10000); // far future
    // Calling schedule() cancels the pending timer and queues immediately.
    holder->schedule();

    ASSERT_EQ(firstFuture.wait_for(5s), std::future_status::ready);
    EXPECT_GE(count.load(), 1);
}

// Corrective Task 006 regression: while a callback is Running with an immediate
// re-run already pending, scheduleAfter() must NOT downgrade that immediate
// request to a delayed one. This mirrors CH
// `BackgroundSchedulePoolTaskInfo::scheduleAfter`, which returns false and
// records no delayed run whenever an immediate run is already scheduled
// (`if (deactivated || scheduled) return false;`) — immediate work has priority.
// Real callers depend on this: `FileCache::backgroundCleanupTaskFunc` ends by
// calling `scheduleAfter(interval)` from inside the running callback, while the
// invalidated-entries notifier and `applySettingsChanges` may concurrently call
// `schedule()`; the pending immediate cleanup must survive so newly invalidated
// entries are processed promptly instead of after a full interval.
TEST(FileCacheSchedulerTest, ScheduleAfterWhileRunningDoesNotReplacePendingImmediate)
{
    TestScheduler ts;

    std::atomic<int> runCount{0};
    // Must be observed false: scheduleAfter reports it did not replace the
    // pending immediate request.
    std::atomic<bool> scheduleAfterReturn{true};

    std::promise<void> firstRunning;
    auto firstRunningFuture = firstRunning.get_future();
    std::promise<void> releaseFirst;
    auto releaseFirstFuture = releaseFirst.get_future().share();
    std::promise<void> scheduleAfterDone;
    auto scheduleAfterDoneFuture = scheduleAfterDone.get_future();
    std::promise<void> secondRan;
    auto secondRanFuture = secondRan.get_future();

    FileCacheScheduledTask * rawTask = nullptr;
    auto holder = ts.scheduler.createTask(
        "schedule-after-priority",
        [&]
        {
            const int run = runCount.fetch_add(1);
            if (run == 0)
            {
                // Hold the first run with a barrier so the test can request an
                // immediate re-run from another thread while we stay Running.
                firstRunning.set_value();
                releaseFirstFuture.get();

                // An immediate run is now pending. A far-future delay must be
                // rejected because immediate work has priority; the
                // ManualTimekeeper is never advanced, so any wrongly-armed
                // timer could never fire on its own.
                scheduleAfterReturn.store(rawTask->scheduleAfter(1000000));
                scheduleAfterDone.set_value();
                // Returning now must re-queue the pending immediate run.
            }
            else if (run == 1)
            {
                secondRan.set_value();
            }
        });
    rawTask = holder.get();

    holder->schedule(); // first run
    ASSERT_EQ(firstRunningFuture.wait_for(5s), std::future_status::ready);

    // Request an immediate re-run from another thread while the first run is
    // held. schedule() returns true in the Running state and records exactly one
    // pending-immediate request.
    ASSERT_TRUE(holder->schedule());

    releaseFirst.set_value(); // let the callback call scheduleAfter and return

    // scheduleAfter must have reported that it did NOT replace the pending
    // immediate request.
    ASSERT_EQ(scheduleAfterDoneFuture.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(scheduleAfterReturn.load());

    // The pending immediate run must fire the next callback WITHOUT advancing
    // the ManualTimekeeper. On the divergent implementation scheduleAfter()
    // overwrote the immediate request with a far-future delayed one, so this
    // wait would time out.
    ASSERT_EQ(secondRanFuture.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(runCount.load(), 2);
}

TEST(FileCacheSchedulerTest, MultipleScheduleCallsCoalesceWhileQueued)
{
    TestScheduler ts;

    // Use a gate to hold the first callback while we fire extra schedule() calls.
    // Track the coalesced second run via a separate promise so we can wait on it.
    std::promise<void> gate;
    auto gateFuture = gate.get_future().share();
    std::atomic<int> count{0};
    std::promise<void> firstRan;
    auto firstFuture = firstRan.get_future();
    std::promise<void> coalescedRan;
    auto coalescedFuture = coalescedRan.get_future();

    auto holder = ts.scheduler.createTask(
        "coalesce-task",
        [&count, &gateFuture, &firstRan, &coalescedRan]() mutable
        {
            const int n = count.fetch_add(1);
            if (n == 0)
            {
                firstRan.set_value();
                gateFuture.get(); // block first run
            }
            else if (n == 1)
            {
                coalescedRan.set_value(); // coalesced second run
            }
        });

    holder->schedule(); // → Queued → Running (first)
    ASSERT_EQ(firstFuture.wait_for(5s), std::future_status::ready);

    // First execution is blocked.  Queue two more schedule() calls; they must
    // coalesce into exactly one pending next-run.
    holder->schedule();
    holder->schedule();

    gate.set_value(); // release first run

    // The coalesced run must fire exactly once.
    ASSERT_EQ(coalescedFuture.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(count.load(), 2);
}

TEST(FileCacheSchedulerTest, SameTaskNeverRunsConcurrently)
{
    TestScheduler ts;

    // Gate the first execution to keep it alive while we fire more schedule()
    // calls; then release it and verify the second run starts only after.
    std::promise<void> firstRunning;
    auto firstRunningFuture = firstRunning.get_future();
    std::promise<void> releaseFirst;
    auto releaseFirstFuture = releaseFirst.get_future().share();
    std::atomic<int> concurrent{0};
    std::atomic<int> maxConcurrent{0};
    std::atomic<int> count{0};
    std::promise<void> secondDone;
    auto secondFuture = secondDone.get_future();

    auto holder = ts.scheduler.createTask("no-concurrent", [&]
    {
        const int cur = concurrent.fetch_add(1, std::memory_order_acq_rel) + 1;
        int expected = maxConcurrent.load();
        while (cur > expected && !maxConcurrent.compare_exchange_weak(expected, cur))
        {
        }
        const int run = count.fetch_add(1);
        if (run == 0)
        {
            firstRunning.set_value();
            releaseFirstFuture.get(); // hold until released
        }
        else if (run == 1)
        {
            secondDone.set_value();
        }
        concurrent.fetch_sub(1, std::memory_order_acq_rel);
    });

    holder->schedule();
    ASSERT_EQ(firstRunningFuture.wait_for(5s), std::future_status::ready);

    // First run is alive.  A concurrent execution is impossible by contract;
    // schedule() now queues a pending next-run.
    holder->schedule();

    // Release the first run.
    releaseFirst.set_value();

    // Wait for the second (coalesced) run to complete.
    ASSERT_EQ(secondFuture.wait_for(5s), std::future_status::ready);

    // The two runs never overlapped.
    EXPECT_EQ(maxConcurrent.load(), 1);
}

TEST(FileCacheSchedulerTest, CallbackSelfReschedulesAfter)
{
    TestScheduler ts;
    std::atomic<int> count{0};
    std::promise<void> reachedTwo;
    auto twoFuture = reachedTwo.get_future();

    FileCacheScheduledTask * rawTask = nullptr;
    auto holder = ts.scheduler.createTask("self-reschedule", [&]
    {
        if (count.fetch_add(1) + 1 < 2)
            rawTask->scheduleAfter(10);
        else
            reachedTwo.set_value();
    });
    rawTask = holder.get();

    holder->schedule();

    // Wait until the first invocation's reentrant scheduleAfter(10) has
    // actually registered its timer with the ManualTimekeeper before
    // advancing the clock. `ManualTimekeeper::advance()` only fires (and
    // erases) entries already present in its internal schedule map at the
    // moment it runs a single upper_bound/erase pass over the map guarded by
    // its lock; it does not keep watching for later insertions. The
    // reentrant registration happens asynchronously on a worker-pool thread
    // (inside runCallback(), after the callback returns), so calling
    // advance() without first confirming the timer is registered would race:
    // if advance() takes the schedule lock first, the timer registered a
    // moment later would target an already-passed clock value and this
    // single advance() call would never observe it, hanging the test.
    // Polling numScheduled() (which, like advance(), is documented as safe
    // to call only from a single thread — here, this test's main thread) is
    // a precise, non-sleep-based synchronization point for "the timer is now
    // in the schedule".
    while (ts.tk->numScheduled() == 0)
        std::this_thread::yield();
    ts.tk->advance(200ms);

    ASSERT_EQ(twoFuture.wait_for(5s), std::future_status::ready);
    EXPECT_GE(count.load(), 2);
}

TEST(FileCacheSchedulerTest, CallbackExceptionDoesNotLeaveTaskRunning)
{
    TestScheduler ts;

    std::promise<void> afterThrow;
    auto afterFuture = afterThrow.get_future();
    std::atomic<int> count{0};
    std::promise<void> firstStarted;
    auto firstStartedFuture = firstStarted.get_future();

    auto holder = ts.scheduler.createTask("exc-task", [&]
    {
        const int n = count.fetch_add(1);
        if (n == 0)
        {
            firstStarted.set_value(); // signal before throw
            throw std::runtime_error("test exception");
        }
        afterThrow.set_value();
    });

    holder->schedule();

    // Wait until the first invocation has at least started, then schedule again.
    // The task may be in Running (exception not yet caught) or back in Idle;
    // either way a second execution must eventually happen.
    ASSERT_EQ(firstStartedFuture.wait_for(5s), std::future_status::ready);
    holder->schedule();

    ASSERT_EQ(afterFuture.wait_for(5s), std::future_status::ready);
    EXPECT_GE(count.load(), 2);
}

TEST(FileCacheSchedulerTest, DeactivatePreventsQueuedCallbackFromRunning)
{
    TestScheduler ts;

    std::atomic<bool> ran{false};
    auto holder = ts.scheduler.createTask("deactivate-queued", [&ran]
    {
        ran.store(true);
    });

    // Block the pool so the task sits in Queued state.
    std::promise<void> unblock;
    auto unblockFuture = unblock.get_future().share();
    std::vector<FileCacheWorker> blockers;
    for (int i = 0; i < 4; ++i)
        blockers.emplace_back(ts.pool, [unblockFuture]() mutable { unblockFuture.get(); });

    holder->schedule(); // sits in queue since all workers are busy

    holder->deactivate(); // cancel before execution

    unblock.set_value();
    for (auto & w : blockers)
        w.join();

    // Draining the worker pool guarantees the stale, generation-invalidated
    // closure (if it was dispatched) is actually dequeued and runs its no-op
    // path *before* we assert, and while the task is still alive. Joining the
    // blockers alone only proves the blocker functions finished, not that the
    // task closure queued behind them was dequeued; without this drain
    // EXPECT_FALSE could pass merely because the closure had not run yet, and
    // the closure could later dereference the freed task during pool teardown.
    ts.pool.shutdown();

    EXPECT_FALSE(ran.load());
}

TEST(FileCacheSchedulerTest, DeactivateWaitsForRunningCallback)
{
    TestScheduler ts;

    std::promise<void> running;
    auto runningFuture = running.get_future();
    std::promise<void> unblock;
    auto unblockFuture = unblock.get_future();
    std::atomic<bool> completed{false};

    auto holder = ts.scheduler.createTask("deactivate-running", [&]
    {
        running.set_value();
        unblockFuture.wait();
        completed.store(true);
    });

    holder->schedule();
    runningFuture.get(); // callback is now executing

    // deactivate blocks until the running callback returns.
    std::thread deactivator([&holder]
    {
        holder->deactivate();
    });

    unblock.set_value();
    deactivator.join();

    // After deactivate() returns the callback must have completed — deactivate
    // guarantees it waited for the running execution to finish.
    EXPECT_TRUE(completed.load());
}

TEST(FileCacheSchedulerTest, HolderDestructorDeactivatesTask)
{
    TestScheduler ts;

    std::atomic<int> count{0};
    std::promise<void> running;
    auto runningFuture = running.get_future();
    std::promise<void> gate;
    auto gateFuture = gate.get_future();

    auto holderPtr = std::make_unique<FileCacheScheduledTaskHolder>(
        ts.scheduler.createTask("dtor-deactivate", [&]
        {
            running.set_value();
            gateFuture.wait();
            count.fetch_add(1);
        }));

    (*holderPtr)->schedule();
    ASSERT_EQ(runningFuture.wait_for(5s), std::future_status::ready);

    // Destroy the holder from a background thread.  The destructor calls
    // deactivate() which must block until the running callback returns.
    std::thread destroyer([h = std::move(holderPtr)] {});

    gate.set_value(); // release callback
    destroyer.join();

    // Callback ran exactly once; subsequent schedules are impossible.
    EXPECT_EQ(count.load(), 1);
}

TEST(FileCacheSchedulerTest, QueuedHolderDestructionMakesCallbackSafeNoOp)
{
    TestScheduler ts;

    std::atomic<bool> ran{false};
    auto holder = std::make_unique<FileCacheScheduledTaskHolder>(
        ts.scheduler.createTask("queued-holder-dtor", [&ran] { ran.store(true); }));

    // Saturate the worker pool so the scheduled task cannot start; its closure
    // sits in the pool behind the blockers while the task stays Queued.
    std::promise<void> unblock;
    auto unblockFuture = unblock.get_future().share();
    std::vector<FileCacheWorker> blockers;
    for (int i = 0; i < 4; ++i)
        blockers.emplace_back(ts.pool, [unblockFuture]() mutable { unblockFuture.get(); });

    (*holder)->schedule(); // Queued behind the saturated pool.

    // Destroy the holder while the task is still Queued and BEFORE the workers
    // are released. The holder owns the only strong reference, so this frees the
    // task; the queued closure holds just a weak_ptr and must become a safe
    // no-op (a raw `this` capture would dereference the freed task here).
    holder.reset();

    // Release the workers and drain the pool so the stale closure is actually
    // dequeued and runs its no-op path before we assert, while using no sleep.
    unblock.set_value();
    for (auto & w : blockers)
        w.join();
    ts.pool.shutdown();

    EXPECT_FALSE(ran.load());
}

TEST(FileCacheSchedulerTest, DelayedHolderDestructionMakesCallbackSafeNoOp)
{
    TestScheduler ts;

    std::atomic<bool> ran{false};
    auto holder = std::make_unique<FileCacheScheduledTaskHolder>(
        ts.scheduler.createTask("delayed-holder-dtor", [&ran] { ran.store(true); }));

    (*holder)->scheduleAfter(100); // Delayed; timer not yet fired.

    // Destroy the holder while the task is still Delayed and BEFORE advancing the
    // ManualTimekeeper. The holder owns the only strong reference, so this frees
    // the task; the timer continuation holds just a weak_ptr and must become a
    // safe no-op (a raw `this` capture would dereference the freed task once the
    // clock advances past the deadline).
    holder.reset();

    // Advancing past the deadline must fire no callback: deactivate() cancelled
    // the timer and bumped the generation, and the task is freed regardless.
    // Draining the pool makes any wrongly-queued closure observable before we
    // assert. No sleep is used.
    ts.tk->advance(200ms);
    ts.pool.shutdown();

    EXPECT_FALSE(ran.load());
}

TEST(FileCacheSchedulerTest, ShutdownCancelsAllTimersAndWaitsCallbacks)
{
    TestScheduler ts;
    std::atomic<int> count{0};
    std::vector<FileCacheScheduledTaskHolder> holders;

    for (int i = 0; i < 3; ++i)
    {
        holders.push_back(ts.scheduler.createTask(
            "shutdown-task-" + std::to_string(i),
            [&count] { count.fetch_add(1); }));
        holders.back()->scheduleAfter(10000); // far-future timers
    }

    // shutdown must cancel pending timers and return promptly.
    ts.scheduler.shutdown();

    // Advancing the clock past every far-future timer must still fire no
    // callback: shutdown() cancelled each task's timer and bumped its
    // generation, so the ManualTimekeeper continuations are stale no-ops.
    // Draining the pool afterwards makes any wrongly-queued callback observable
    // before we assert, so a regression that failed to cancel/deactivate the
    // timers would be caught here instead of passing vacuously (the original
    // assertion never advanced the clock, so it proved nothing about
    // cancellation).
    ts.tk->advance(20000ms);
    ts.pool.shutdown();

    EXPECT_EQ(count.load(), 0);
}

TEST(FileCacheSchedulerTest, TriggerNowViaScheduleOnDelayedTask)
{
    TestScheduler ts;

    std::promise<void> ran;
    auto ranFuture = ran.get_future();

    auto holder = ts.scheduler.createTask("trigger-now", [&ran]
    {
        ran.set_value();
    });

    holder->scheduleAfter(9999999); // far future
    holder->schedule();             // triggerNow: cancel timer, run immediately

    ASSERT_EQ(ranFuture.wait_for(5s), std::future_status::ready);
}

// ---------------------------------------------------------------------------
// FileCacheQueryIdScope tests
// ---------------------------------------------------------------------------

TEST(FileCacheQueryIdScopeTest, ScopeSetAndRestoresQueryId)
{
    EXPECT_TRUE(FileCacheQueryIdScope::currentQueryId().empty());

    {
        FileCacheQueryIdScope scope("query-abc");
        EXPECT_EQ(FileCacheQueryIdScope::currentQueryId(), "query-abc");

        {
            FileCacheQueryIdScope inner("query-xyz");
            EXPECT_EQ(FileCacheQueryIdScope::currentQueryId(), "query-xyz");
        }

        // inner scope exited; outer restored.
        EXPECT_EQ(FileCacheQueryIdScope::currentQueryId(), "query-abc");
    }

    EXPECT_TRUE(FileCacheQueryIdScope::currentQueryId().empty());
}

TEST(FileCacheQueryIdScopeTest, PhysicalTidChangeMakesCallerIdDiffer)
{
    // Two threads with the same query id must produce different caller ids.
    const std::string queryId = "shared-query";

    std::string tid1, tid2;

    std::thread t1([&]
    {
        FileCacheQueryIdScope scope(queryId);
        tid1 = FileCacheQueryIdScope::getCallerId();
    });

    std::thread t2([&]
    {
        FileCacheQueryIdScope scope(queryId);
        tid2 = FileCacheQueryIdScope::getCallerId();
    });

    t1.join();
    t2.join();

    EXPECT_NE(tid1, tid2);
    // Both must start with the shared query id.
    EXPECT_EQ(tid1.substr(0, queryId.size()), queryId);
    EXPECT_EQ(tid2.substr(0, queryId.size()), queryId);
}

TEST(FileCacheQueryIdScopeTest, NoScopeProducesExactNoneFormat)
{
    // F-CALLERID (Task 017): without a scope, the caller id uses the exact CH
    // diagnostic format `None:<threadname>:<tid>`. This is an EXACT-format check,
    // not a prefix check: the string must have exactly three colon-separated
    // fields, the first literally "None", and the last a decimal OS thread id
    // matching this thread's id.
    const std::string callerId = FileCacheQueryIdScope::getCallerId();

    const auto firstColon = callerId.find(':');
    ASSERT_NE(firstColon, std::string::npos);
    const auto lastColon = callerId.rfind(':');
    ASSERT_NE(lastColon, std::string::npos);
    // Three fields => two distinct colons (threadname may be empty, but both
    // colons must be present and distinct).
    ASSERT_NE(firstColon, lastColon)
        << "caller id must be None:<threadname>:<tid> with two colons: " << callerId;

    const std::string prefix = callerId.substr(0, firstColon);
    EXPECT_EQ(prefix, "None");

    const std::string tidField = callerId.substr(lastColon + 1);
    ASSERT_FALSE(tidField.empty());
    for (char c : tidField)
        EXPECT_TRUE(std::isdigit(static_cast<unsigned char>(c)))
            << "tid field must be decimal: " << callerId;
    EXPECT_EQ(tidField, std::to_string(folly::getOSThreadID()));

    // False-green probe: the previous prefix-only assertion (substr(0,5)=="None:")
    // would also pass for the OLD `None:<tid>` format, which lacks the threadname
    // field. Assert the format is NOT the two-field form, so a regression back to
    // `None:<tid>` fails here.
    const std::string twoFieldForm = "None:" + std::to_string(folly::getOSThreadID());
    EXPECT_NE(callerId, twoFieldForm)
        << "caller id regressed to the two-field None:<tid> form";
}

// SD8 (Task 017): the scheduler keeps a `std::recursive_mutex` because a timer
// continuation can run INLINE on the thread that is attaching it while that
// thread already holds the task mutex. `scheduleAfter(0)` triggers exactly this:
// `ManualTimekeeper::after(0)` fulfils its promise immediately, so
// `armTimerLocked`'s `.thenValue()` runs the continuation inline on the current
// thread (which holds `mutex_` via `scheduleAfter`). The continuation re-locks
// `mutex_` to call `queueImmediateLocked`. With a NON-recursive mutex this
// self-deadlocks and this test hangs (RED for resolution option 2); with the
// retained `std::recursive_mutex` the re-entry is safe and the callback runs.
TEST(FileCacheSchedulerTest, ZeroDelayInlineContinuationDoesNotSelfDeadlock)
{
    TestScheduler ts;

    std::promise<void> ran;
    auto ranFuture = ran.get_future();

    auto holder = ts.scheduler.createTask("inline-reentry-task", [&ran]
    {
        ran.set_value();
    });
    ASSERT_TRUE(static_cast<bool>(holder));

    // This call must return (not deadlock) even though it re-enters the task
    // mutex inline via the immediately-ready timer continuation.
    const bool scheduled = holder->scheduleAfter(0);
    EXPECT_TRUE(scheduled);

    // The inline continuation queued the immediate run on the worker pool.
    ASSERT_EQ(ranFuture.wait_for(5s), std::future_status::ready);
}

TEST(FileCacheQueryIdScopeTest, SameQueryDifferentResumeProducesDifferentCallerId)
{
    // Simulates a Velox driver resuming on a new OS thread after the previous
    // execution released the downloader lease.  The new thread must get a
    // different caller id even though the query id is the same.
    const std::string queryId = "resume-query";

    std::string before, after;

    std::thread t1([&]
    {
        FileCacheQueryIdScope scope(queryId);
        before = FileCacheQueryIdScope::getCallerId();
    });
    t1.join();

    std::thread t2([&]
    {
        FileCacheQueryIdScope scope(queryId);
        after = FileCacheQueryIdScope::getCallerId();
    });
    t2.join();

    // Different physical threads → different os-tid component.
    EXPECT_NE(before, after);
}

} // namespace
} // namespace facebook::velox::ch
