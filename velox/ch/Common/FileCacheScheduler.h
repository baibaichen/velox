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

#include "velox/ch/Common/ThreadPool.h"

#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace folly
{
class Timekeeper;
} // namespace folly

namespace facebook::velox::ch
{

class FileCacheScheduledTask;
class FileCacheScheduler;

/// RAII holder for a `FileCacheScheduledTask`.
///
/// Default-constructible; move-only.  Destructor calls `deactivate()` if the
/// task is still active.  Mirrors `BackgroundSchedulePoolTaskHolder`.
class FileCacheScheduledTaskHolder
{
public:
    FileCacheScheduledTaskHolder() = default;
    explicit FileCacheScheduledTaskHolder(
        std::shared_ptr<FileCacheScheduledTask> task);

    FileCacheScheduledTaskHolder(const FileCacheScheduledTaskHolder &) = delete;
    FileCacheScheduledTaskHolder &
    operator=(const FileCacheScheduledTaskHolder &) = delete;

    FileCacheScheduledTaskHolder(FileCacheScheduledTaskHolder &&) noexcept;
    FileCacheScheduledTaskHolder &
    operator=(FileCacheScheduledTaskHolder &&) noexcept;

    ~FileCacheScheduledTaskHolder();

    explicit operator bool() const;
    FileCacheScheduledTask * operator->();
    const FileCacheScheduledTask * operator->() const;

    FileCacheScheduledTask * get();

private:
    std::shared_ptr<FileCacheScheduledTask> task_;
};

/// A single named task managed by `FileCacheScheduler`.
///
/// State machine:
///   Idle → schedule() → Queued → [running] → Running → [done] → Idle
///   Any → scheduleAfter(ms) → Delayed → [timer fires] → Queued → Running → Idle
///   Any → deactivate() → Deactivated (waits for running callback to return)
///
/// Invariants:
/// - The same task never executes concurrently with itself.
/// - Multiple `schedule()` calls while Queued or Running coalesce into one
///   next-run request.
/// - `deactivate()` blocks until any in-flight callback returns, then prevents
///   all future executions.
/// - Destroying or move-assigning a holder while the task is Delayed or Queued
///   is safe on its own: the asynchronous worker-pool closure and timer
///   continuation hold a `weak_ptr` to the task (never a raw pointer) and become
///   safe no-ops once the holder (the sole strong owner) is gone. Holder
///   teardown does not require draining the shared `FileCacheWorkerPool` first.
class FileCacheScheduledTask
    : public std::enable_shared_from_this<FileCacheScheduledTask>
{
public:
    /// Replace the callback (used in tests that need to set it after createTask).
    void setCallback(std::function<void()> callback);

    /// Dispatch for immediate execution.
    /// Returns false if already Deactivated or if the scheduler is shut down.
    bool schedule();

    /// Dispatch for execution after `delayMs` milliseconds.
    /// Returns false if already Deactivated or shut down, or if an immediate
    /// run is already pending while the callback is Running (immediate work has
    /// priority and is never downgraded to a delayed run, matching CH
    /// `BackgroundSchedulePoolTaskInfo::scheduleAfter`).
    bool scheduleAfter(uint64_t delayMs);

    /// Prevent all future executions; block until any running callback returns.
    void deactivate();

    const std::string & name() const;

private:
    friend class FileCacheScheduler;

    enum class State : uint8_t
    {
        Idle,
        Delayed,
        Queued,
        Running,
        Deactivated,
    };

    FileCacheScheduledTask(
        std::string name,
        std::function<void()> callback,
        FileCacheScheduler & scheduler);

    // Called by the worker closure when it actually starts executing.
    void runCallback();

    // Cancel the current timer future (if any).  Must be called under
    // `scheduleMutex_`.
    void cancelTimerLocked();

    // Queue one immediate execution on the worker pool.
    // Must be called under `scheduleMutex_`.  Transitions state to Queued.
    void queueImmediateLocked();

    // Arm a one-shot delayed timer via the scheduler's Timekeeper.
    // Must be called with `scheduleMutex_` held (passed in as `lock`).
    // Transitions state to Delayed and returns with `lock` still HELD.
    //
    // The continuation captures a `weak_ptr` to this task (obtained via
    // `weak_from_this()`), never a raw `this`: the holder that owns the only
    // strong reference may be destroyed before the timer fires. The continuation
    // locks the `weak_ptr` to a `shared_ptr` and no-ops if the task has already
    // been freed, so it never dereferences a destroyed task.
    //
    // The continuation is attached to a `folly::Future` (via
    // `toUnsafeFuture()`), not a bare `folly::SemiFuture`: a deferred
    // continuation on a `SemiFuture` only runs once something drives it with
    // `.via(executor)`/`.get()`/`.wait()`, which this scheduler never does for
    // `timerFuture_`. `Future::thenValue` runs inline on whichever thread
    // fulfils the antecedent promise (the thread that calls
    // `folly::Timekeeper::advance()` in tests, or the Timekeeper's own timer
    // thread in production). If that promise is already fulfilled when
    // `.thenValue()` attaches (e.g. a zero delay or a concurrent advance), folly
    // runs the continuation INLINE on the attaching thread. To make that safe
    // under two plain mutexes, `scheduleMutex_` is released BEFORE the
    // continuation is attached: an inline run therefore re-locks a *free*
    // `scheduleMutex_` and cannot self-deadlock (no recursive mutex is needed).
    // The `weak_ptr` + `generation_` snapshot still guard lifetime and staleness,
    // and the timer handle is published only if this timer is still the current
    // one (see the implementation), so a stale completed future never overwrites
    // a newer live timer handle.
    // (A `cancel()`-driven completion cannot hit this path: it is skipped
    // entirely rather than re-entering the continuation — see `cancelTimerLocked`.)
    void armTimerLocked(std::unique_lock<std::mutex> & lock, uint64_t delayMs);

    // Two plain locks (CH BackgroundSchedulePool structure):
    //   execMutex_     - serializes callback execution; deactivate() acquires it
    //                    to drain (block until) any running callback.
    //   scheduleMutex_ - protects state_/pending*/generation_/timerFuture_.
    // Lock order when both are needed: execMutex_ THEN scheduleMutex_.
    mutable std::mutex execMutex_;
    mutable std::mutex scheduleMutex_;

    std::string name_;
    std::function<void()> callback_;
    FileCacheScheduler & scheduler_;

    State state_{State::Idle};
    uint64_t generation_{0}; // incremented on cancel/deactivate

    // Pending next-run request accumulated while Running.
    bool pendingImmediate_{false};
    bool pendingDelayed_{false};
    uint64_t pendingDelayMs_{0};

    // Handle for the outstanding delayed future (may be cancelled). A plain
    // `Future` (not `SemiFuture`) so that the attached continuation runs
    // without requiring an executor; see `armTimerLocked`.
    folly::Future<folly::Unit> timerFuture_{
        folly::Future<folly::Unit>::makeEmpty()};
};

/// Scheduler that maps `BackgroundSchedulePool` semantics onto
/// `folly::Timekeeper` (timer) + `FileCacheWorkerPool` (execution).
///
/// All scheduled callbacks execute on the shared worker pool, so long-running
/// callbacks do not block timer delivery.
///
/// Shutdown procedure:
///   1. `scheduler.shutdown()` — deactivates all live tasks and waits.
///   2. `worker_pool.shutdown()` — stops the executor.
class FileCacheScheduler
{
public:
    FileCacheScheduler(
        std::shared_ptr<folly::Timekeeper> timekeeper,
        FileCacheWorkerPool & workerPool);

    ~FileCacheScheduler();

    /// Create a named task.  Returns an empty holder if already shut down.
    FileCacheScheduledTaskHolder createTask(
        std::string name,
        std::function<void()> callback);

    /// Deactivate all live tasks and wait for running callbacks to return.
    /// Subsequent `createTask` calls return empty holders.
    void shutdown();

private:
    friend class FileCacheScheduledTask;

    std::shared_ptr<folly::Timekeeper> timekeeper_;
    FileCacheWorkerPool & workerPool_;

    mutable std::mutex mutex_;
    bool shutdown_{false};
    std::vector<std::weak_ptr<FileCacheScheduledTask>> tasks_;
};

/// CH alias: `using BackgroundSchedulePool = FileCacheScheduler`
using BackgroundSchedulePool = FileCacheScheduler;

/// CH alias: `using BackgroundSchedulePoolTaskHolder = FileCacheScheduledTaskHolder`
using BackgroundSchedulePoolTaskHolder = FileCacheScheduledTaskHolder;

} // namespace facebook::velox::ch
