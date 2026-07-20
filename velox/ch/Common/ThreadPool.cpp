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
#include "velox/ch/Common/FileCacheException.h"

#include <folly/futures/Future.h>

#include <exception>
#include <utility>

namespace facebook::velox::ch
{

// ---------------------------------------------------------------------------
// FileCacheWorker
// ---------------------------------------------------------------------------

FileCacheWorker::FileCacheWorker(FileCacheWorkerPool & pool, Function function)
    : state_(std::make_shared<State>())
{
    auto state = state_;
    pool.executor_.add(
        [state, func = std::move(function)]() mutable
        {
            try
            {
                func();
                state->finished.setValue(folly::Unit{});
            }
            catch (...)
            {
                state->exception = std::current_exception();
                state->finished.setValue(folly::Unit{});
            }
        });
}

FileCacheWorker::FileCacheWorker(FileCacheWorker && other) noexcept
    : state_(std::move(other.state_))
{
}

FileCacheWorker & FileCacheWorker::operator=(FileCacheWorker && other) noexcept
{
    if (this != &other)
    {
        VELOX_CHECK(
            !joinable(),
            "FileCacheWorker move-assigned over a joinable target; "
            "this would leak a background task. "
            "Call join() before move-assigning into the worker.");
        state_ = std::move(other.state_);
    }
    return *this;
}

FileCacheWorker::~FileCacheWorker()
{
    VELOX_CHECK(
        !joinable(),
        "FileCacheWorker destroyed without join(); "
        "this leaks a background task. "
        "Call join() before destroying the worker.");
}

void FileCacheWorker::join()
{
    if (!state_ || state_->joined.exchange(true, std::memory_order_acq_rel))
        return;

    std::move(state_->future).get();

    if (state_->exception)
        std::rethrow_exception(state_->exception);

    state_.reset();
}

bool FileCacheWorker::joinable() const
{
    return state_ && !state_->joined.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// FileCacheWorkerPool
// ---------------------------------------------------------------------------

FileCacheWorkerPool::FileCacheWorkerPool(
    size_t maxThreads,
    size_t minThreads,
    std::string threadNamePrefix)
    : executor_(
          std::make_pair(maxThreads, minThreads),
          std::make_shared<folly::NamedThreadFactory>(std::move(threadNamePrefix)))
{
}

FileCacheWorker FileCacheWorkerPool::startThread(FileCacheWorker::Function function)
{
    return FileCacheWorker(*this, std::move(function));
}

folly::SemiFuture<folly::Unit> FileCacheWorkerPool::schedule(
    std::function<void()> task)
{
    auto [promise, future] = folly::makePromiseContract<folly::Unit>();
    auto sharedPromise = std::make_shared<folly::Promise<folly::Unit>>(
        std::move(promise));

    executor_.add(
        [t = std::move(task), p = std::move(sharedPromise)]() mutable
        {
            try
            {
                t();
                p->setValue(folly::Unit{});
            }
            catch (...)
            {
                // folly::Promise<T>::setException only accepts a
                // folly::exception_wrapper (or a concrete std::exception
                // subtype), not a raw std::exception_ptr; wrap it.
                p->setException(folly::current_exception_wrapper());
            }
        });

    return std::move(future);
}

void FileCacheWorkerPool::shutdown()
{
    executor_.stop();
    executor_.join();
}

void FileCacheWorkerPool::setNumThreads(size_t threads)
{
    executor_.setNumThreads(threads);
}

size_t FileCacheWorkerPool::numThreads() const
{
    return executor_.numThreads();
}

// ---------------------------------------------------------------------------
// FileCacheThreadPool
// ---------------------------------------------------------------------------

FileCacheThreadPool::FileCacheThreadPool(
    FileCacheWorkerPool & workerPool,
    size_t maxThreads,
    size_t queueSize)
    : workerPool_(workerPool)
    , maxThreads_(maxThreads)
    , queueSize_(queueSize)
{
    // Non-owning conduit to the shared physical executor. Admission is
    // additionally self-gated below (inFlight_ / backlog_); see the class
    // comment in ThreadPool.h for why maxInQueue alone is not sufficient.
    folly::MeteredExecutor::Options options;
    options.maxInQueue = static_cast<uint32_t>(maxThreads_);
    meteredExecutor_ = std::make_unique<folly::MeteredExecutor>(
        folly::getKeepAliveToken(workerPool_.executor_), options);
}

void FileCacheThreadPool::scheduleOrThrowOnError(std::function<void()> task)
{
    PendingTask pendingTask;
    bool shouldDispatch = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pending_.size() >= queueSize_)
            throwFileCacheException(
                "FileCacheThreadPool queue full ({} / {})", pending_.size(), queueSize_);

        auto [promise, future] = folly::makePromiseContract<folly::Unit>();
        pendingTask.task = std::move(task);
        pendingTask.promise = std::make_shared<folly::Promise<folly::Unit>>(std::move(promise));
        pending_.push_back(std::move(future));

        if (inFlight_ < maxThreads_)
        {
            ++inFlight_;
            shouldDispatch = true;
        }
        else
        {
            backlog_.push_back(std::move(pendingTask));
        }
    }

    // Dispatch outside the lock: the metered executor's add() only enqueues
    // the closure, it does not run it synchronously, but there is no reason
    // to hold mutex_ across it.
    if (shouldDispatch)
        dispatch(std::move(pendingTask));
}

void FileCacheThreadPool::dispatch(PendingTask pendingTask)
{
    meteredExecutor_->add(
        [this, t = std::move(pendingTask.task), p = std::move(pendingTask.promise)]() mutable
        {
            try
            {
                t();
                p->setValue(folly::Unit{});
            }
            catch (...)
            {
                // folly::Promise<T>::setException only accepts a
                // folly::exception_wrapper (or a concrete std::exception
                // subtype), not a raw std::exception_ptr; wrap it.
                p->setException(folly::current_exception_wrapper());
            }

            // Only now that the task's own function has actually finished do
            // we admit the next backlogged task (or release the in-flight
            // slot), preserving the maxThreads_ concurrency cap regardless
            // of the shared pool's physical capacity.
            PendingTask next;
            bool hasNext = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!backlog_.empty())
                {
                    next = std::move(backlog_.front());
                    backlog_.pop_front();
                    hasNext = true;
                }
                else
                {
                    --inFlight_;
                }
            }
            if (hasNext)
                dispatch(std::move(next));
        });
}

void FileCacheThreadPool::wait()
{
    std::vector<folly::SemiFuture<folly::Unit>> toWait;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        toWait.swap(pending_);
    }

    std::exception_ptr firstException;
    for (auto & future : toWait)
    {
        try
        {
            std::move(future).get();
        }
        catch (...)
        {
            if (!firstException)
                firstException = std::current_exception();
        }
    }

    if (firstException)
        std::rethrow_exception(firstException);
}

} // namespace facebook::velox::ch
