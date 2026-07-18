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

#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/logger_useful.h"

#include <folly/futures/Future.h>

#include <utility>

namespace facebook::velox::ch
{

// ---------------------------------------------------------------------------
// FileCacheScheduledTaskHolder
// ---------------------------------------------------------------------------

FileCacheScheduledTaskHolder::FileCacheScheduledTaskHolder(
    std::shared_ptr<FileCacheScheduledTask> task)
    : task_(std::move(task))
{
}

FileCacheScheduledTaskHolder::FileCacheScheduledTaskHolder(
    FileCacheScheduledTaskHolder && other) noexcept
    : task_(std::move(other.task_))
{
}

FileCacheScheduledTaskHolder & FileCacheScheduledTaskHolder::operator=(
    FileCacheScheduledTaskHolder && other) noexcept
{
    if (this != &other)
    {
        if (task_)
            task_->deactivate();
        task_ = std::move(other.task_);
    }
    return *this;
}

FileCacheScheduledTaskHolder::~FileCacheScheduledTaskHolder()
{
    if (task_)
        task_->deactivate();
}

FileCacheScheduledTaskHolder::operator bool() const
{
    return static_cast<bool>(task_);
}

FileCacheScheduledTask * FileCacheScheduledTaskHolder::operator->()
{
    return task_.get();
}

const FileCacheScheduledTask * FileCacheScheduledTaskHolder::operator->() const
{
    return task_.get();
}

FileCacheScheduledTask * FileCacheScheduledTaskHolder::get()
{
    return task_.get();
}

// ---------------------------------------------------------------------------
// FileCacheScheduledTask
// ---------------------------------------------------------------------------

FileCacheScheduledTask::FileCacheScheduledTask(
    std::string name,
    std::function<void()> callback,
    FileCacheScheduler & scheduler)
    : name_(std::move(name)), callback_(std::move(callback)), scheduler_(scheduler)
{
}

void FileCacheScheduledTask::setCallback(std::function<void()> callback)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    callback_ = std::move(callback);
}

const std::string & FileCacheScheduledTask::name() const
{
    return name_;
}

bool FileCacheScheduledTask::schedule()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (state_ == State::Deactivated)
        return false;

    if (state_ == State::Idle || state_ == State::Delayed)
    {
        cancelTimerLocked();
        queueImmediateLocked();
        return true;
    }

    if (state_ == State::Queued)
        return false; // already queued; multiple schedule() calls coalesce

    // state_ == State::Running: remember exactly one immediate next-run
    // request. Immediate requests take priority over delayed ones.
    pendingImmediate_ = true;
    pendingDelayed_ = false;
    return true;
}

bool FileCacheScheduledTask::scheduleAfter(uint64_t delayMs)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (state_ == State::Deactivated)
        return false;

    if (state_ == State::Idle || state_ == State::Delayed)
    {
        cancelTimerLocked();
        armTimerLocked(delayMs);
        return true;
    }

    if (state_ == State::Queued)
        return false; // already queued to run imminently; keep that request

    // state_ == State::Running: remember/overwrite one delayed next-run
    // request. A delayed request made after an immediate one supersedes it,
    // matching schedule()'s Running branch, which does the reverse.
    pendingDelayed_ = true;
    pendingImmediate_ = false;
    pendingDelayMs_ = delayMs;
    return true;
}

void FileCacheScheduledTask::deactivate()
{
    std::unique_lock<std::recursive_mutex> lock(mutex_);

    if (state_ == State::Deactivated)
        return;

    cancelTimerLocked();
    const bool wasRunning = (state_ == State::Running);
    state_ = State::Deactivated;
    pendingImmediate_ = false;
    pendingDelayed_ = false;

    // Lifetime safety: a Queued closure already handed to the worker pool, and a
    // Delayed timer continuation still held by `timerFuture_`, both capture a
    // `weak_ptr` to this task (never a raw pointer). Each locks it to a
    // `shared_ptr` before touching any member and no-ops if the lock fails, so
    // they never dereference a destroyed task. Destroying (or move-assigning) the
    // holder while the task is Delayed or Queued is therefore safe on its own and
    // does NOT require draining the shared `FileCacheWorkerPool` first: after this
    // `deactivate()` runs, each closure either observes the bumped
    // `generation_`/`Deactivated` state and no-ops, or finds the task already
    // freed and no-ops. Only a Running callback is waited for below, because only
    // it may still be dereferencing caller-supplied captured state.
    if (wasRunning)
        cv_.wait(lock, [this] { return !callbackInFlight_; });
}

void FileCacheScheduledTask::runCallback()
{
    std::unique_lock<std::recursive_mutex> lock(mutex_);

    // The task may have been deactivated between dispatch (the stale check in
    // the worker closure created by queueImmediateLocked) and this call.
    if (state_ == State::Deactivated)
        return;

    state_ = State::Running;
    callbackInFlight_ = true;
    pendingImmediate_ = false;
    pendingDelayed_ = false;
    lock.unlock();

    try
    {
        callback_();
    }
    catch (...)
    {
        LOG_ERROR(
            getLogger("FileCacheScheduler"),
            "Task '{}' callback threw: {}",
            name_,
            getCurrentExceptionMessage(/* with_stacktrace */ true));
    }

    lock.lock();
    callbackInFlight_ = false;
    cv_.notify_all(); // wake any deactivate() waiting for this run to finish

    if (state_ == State::Deactivated)
        return;

    if (pendingImmediate_)
    {
        pendingImmediate_ = false;
        pendingDelayed_ = false;
        queueImmediateLocked();
    }
    else if (pendingDelayed_)
    {
        pendingDelayed_ = false;
        const uint64_t delayMs = pendingDelayMs_;
        armTimerLocked(delayMs);
    }
    else
    {
        state_ = State::Idle;
    }
}

void FileCacheScheduledTask::cancelTimerLocked()
{
    if (timerFuture_.valid())
    {
        timerFuture_.cancel();
        timerFuture_ = folly::Future<folly::Unit>::makeEmpty();
    }
    ++generation_;
}

void FileCacheScheduledTask::queueImmediateLocked()
{
    state_ = State::Queued;
    const uint64_t gen = generation_;
    // Capture a weak_ptr, never a raw `this`: the holder (the sole strong owner)
    // may be destroyed while this closure still sits in the worker pool. The
    // closure locks the task to a shared_ptr and no-ops if it has been freed.
    std::weak_ptr<FileCacheScheduledTask> weakSelf = weak_from_this();
    scheduler_.workerPool_.schedule(
        [weakSelf, gen]
        {
            auto self = weakSelf.lock();
            if (!self)
                return; // task destroyed → safe no-op
            {
                std::lock_guard<std::recursive_mutex> lock(self->mutex_);
                if (gen != self->generation_ || self->state_ != State::Queued)
                    return; // stale: cancelled/deactivated/superseded already
            }
            self->runCallback();
        });
}

void FileCacheScheduledTask::armTimerLocked(uint64_t delayMs)
{
    state_ = State::Delayed;
    const uint64_t gen = generation_;
    // Capture a weak_ptr, never a raw `this`: the holder may be destroyed before
    // the timer fires. The continuation locks the task to a shared_ptr and
    // no-ops if it has been freed.
    std::weak_ptr<FileCacheScheduledTask> weakSelf = weak_from_this();
    timerFuture_ = scheduler_.timekeeper_->after(std::chrono::milliseconds(delayMs))
                       .toUnsafeFuture()
                       .thenValue(
                           [weakSelf, gen](folly::Unit)
                           {
                               auto self = weakSelf.lock();
                               if (!self)
                                   return; // task destroyed → safe no-op
                               std::lock_guard<std::recursive_mutex> lock(self->mutex_);
                               if (gen != self->generation_ || self->state_ != State::Delayed)
                                   return; // stale: cancelled/rescheduled/deactivated
                               self->queueImmediateLocked();
                           });
}

// ---------------------------------------------------------------------------
// FileCacheScheduler
// ---------------------------------------------------------------------------

FileCacheScheduler::FileCacheScheduler(
    std::shared_ptr<folly::Timekeeper> timekeeper,
    FileCacheWorkerPool & workerPool)
    : timekeeper_(std::move(timekeeper)), workerPool_(workerPool)
{
}

FileCacheScheduler::~FileCacheScheduler()
{
    shutdown();
}

FileCacheScheduledTaskHolder FileCacheScheduler::createTask(
    std::string name,
    std::function<void()> callback)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (shutdown_)
        return FileCacheScheduledTaskHolder();

    // FileCacheScheduledTask's constructor is private; FileCacheScheduler is
    // a friend, so a direct `new` from here is allowed even though
    // `std::make_shared` (which constructs from outside this class) is not.
    std::shared_ptr<FileCacheScheduledTask> task(
        new FileCacheScheduledTask(std::move(name), std::move(callback), *this));
    tasks_.push_back(task);
    return FileCacheScheduledTaskHolder(task);
}

void FileCacheScheduler::shutdown()
{
    std::vector<std::shared_ptr<FileCacheScheduledTask>> liveTasks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_)
            return;
        shutdown_ = true;

        liveTasks.reserve(tasks_.size());
        for (auto & weakTask : tasks_)
            if (auto task = weakTask.lock())
                liveTasks.push_back(std::move(task));
        tasks_.clear();
    }

    for (auto & task : liveTasks)
        task->deactivate();
}

} // namespace facebook::velox::ch
