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

#include "velox/common/base/Exceptions.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/MeteredExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>

#include <atomic>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace facebook::velox::ch
{

class FileCacheWorkerPool;

/// Handle for a long-running task submitted to a `FileCacheWorkerPool`.
///
/// Semantics mirror `ThreadFromGlobalPool` in ClickHouse:
/// - Constructed with a pool reference and a callable; the callable is
///   dispatched immediately to the pool's executor.
/// - `join` blocks until the callable returns and rethrows any exception.
/// - Destroying a joinable worker triggers `VELOX_CHECK` to prevent silent
///   background-task leaks.
/// - Move-only; the moved-from object reports `joinable() == false`.
/// - Move-assigning into a still-joinable target triggers the same
///   `VELOX_CHECK` as the destructor: it would silently drop the only handle
///   able to join the target's background task.
class FileCacheWorker
{
public:
    using Function = std::function<void()>;

    FileCacheWorker() = default;

    FileCacheWorker(FileCacheWorkerPool & pool, Function function);

    FileCacheWorker(FileCacheWorker && other) noexcept;

    /// Moves `other` into `*this`. Triggers VELOX_CHECK if `*this` is still
    /// joinable, since that would overwrite the only handle able to join the
    /// target's background task.
    FileCacheWorker & operator=(FileCacheWorker && other) noexcept;

    FileCacheWorker(const FileCacheWorker &) = delete;
    FileCacheWorker & operator=(const FileCacheWorker &) = delete;

    /// Destroys the handle. Triggers VELOX_CHECK if still joinable.
    ~FileCacheWorker();

    /// Blocks until the function returns; rethrows any exception.
    /// Calling join on an unjoinable worker is a no-op.
    void join();

    /// True until join() has been called (or the worker was move-constructed).
    bool joinable() const;

private:
    struct State
    {
        folly::Promise<folly::Unit> finished;
        folly::SemiFuture<folly::Unit> future = finished.getSemiFuture();
        std::exception_ptr exception;
        std::atomic_bool joined{false};
    };

    std::shared_ptr<State> state_;
};

/// CH alias: `using ThreadFromGlobalPool = FileCacheWorker`
using ThreadFromGlobalPool = FileCacheWorker;

/// Process-level shared dynamic executor (analogous to `GlobalThreadPool` in
/// ClickHouse).  Owned by `FileCacheManager`; shared across all `FileCache`
/// instances and their logical `FileCacheThreadPool`s.
///
/// Capacity contract:
/// - `maxThreads` encodes the conservative budget computed by
///   `FileCacheManager` from all registered caches.
/// - Idle workers retire after the Folly default timeout (≈ 60 s).
/// - `setNumThreads` grows or shrinks; must only shrink after all tasks
///   needing the current capacity have joined or wait()ed.
class FileCacheWorkerPool
{
public:
    FileCacheWorkerPool(
        size_t maxThreads,
        size_t minThreads,
        std::string threadNamePrefix);

    FileCacheWorkerPool(const FileCacheWorkerPool &) = delete;
    FileCacheWorkerPool & operator=(const FileCacheWorkerPool &) = delete;

    /// Dispatch a long-running callable and return its handle.
    FileCacheWorker startThread(FileCacheWorker::Function function);

    /// Dispatch a short task; returns a SemiFuture that resolves on completion.
    folly::SemiFuture<folly::Unit> schedule(std::function<void()> task);

    /// Stop the underlying executor (waits for running tasks to finish).
    void shutdown();

    /// Adjust the maximum number of threads (thread-safe per Folly contract).
    void setNumThreads(size_t threads);

    /// Current number of threads in the underlying executor. Used by
    /// FileCache::loadMetadataImpl as a fail-close capacity precondition: the
    /// shared pool must already provide enough threads for the concurrent
    /// listing/loading workers (the manager budgets it), and FileCache must never
    /// resize the shared pool itself.
    size_t numThreads() const;

private:
    folly::CPUThreadPoolExecutor executor_;

    friend class FileCacheWorker;
    friend class FileCacheThreadPool;
};

/// Per-cache logical pool (analogous to `ThreadPoolImpl<ThreadFromGlobalPool>`
/// in ClickHouse).  Submits bounded short tasks to the shared
/// `FileCacheWorkerPool`, tracks pending futures, and rethrows the first
/// exception on `wait`.
///
/// No OS threads are owned; all physical execution is via the shared pool.
/// `maxThreads` is enforced locally and self-gated: this instance tracks how
/// many of its own tasks are currently in flight (fed to the shared pool)
/// and only ever feeds up to `maxThreads` of them concurrently, via a
/// `folly::MeteredExecutor` wrapping the shared pool's executor. Tasks
/// beyond that limit wait in an internal backlog queue that never touches
/// the shared executor, so they cannot occupy or starve a shared physical
/// worker thread. The next backlogged task is fed in only when a
/// previously in-flight task's function has actually finished, not merely
/// been dispatched: `folly::MeteredExecutor`'s own admission count is
/// evaluated when a slot is dispatched rather than when its task body
/// completes, so relying on it alone does not cap concurrency once the
/// shared pool has spare physical threads. `queueSize` still bounds every
/// task owned by this logical pool, whether in flight or backlogged.
class FileCacheThreadPool
{
public:
    FileCacheThreadPool(
        FileCacheWorkerPool & workerPool,
        size_t maxThreads,
        size_t queueSize);

    FileCacheThreadPool(const FileCacheThreadPool &) = delete;
    FileCacheThreadPool & operator=(const FileCacheThreadPool &) = delete;

    /// Dispatch a task. Throws `VeloxRuntimeError` immediately if the queue is
    /// at `queueSize` capacity.
    void scheduleOrThrowOnError(std::function<void()> task);

    /// Block until all previously submitted tasks complete.
    /// Rethrows the first collected exception (if any).
    void wait();

private:
    struct PendingTask
    {
        std::function<void()> task;
        std::shared_ptr<folly::Promise<folly::Unit>> promise;
    };

    /// Feeds one task to the shared pool via the metered executor. On
    /// completion, hands the next backlogged task (if any) to the shared
    /// pool, or releases its in-flight slot.
    void dispatch(PendingTask pendingTask);

    FileCacheWorkerPool & workerPool_;
    const size_t maxThreads_;
    const size_t queueSize_;

    // Declaration (and therefore destruction) order matters here: members
    // are destroyed in reverse declaration order. mutex_/inFlight_/backlog_/
    // pending_ are declared before meteredExecutor_ so that
    // meteredExecutor_'s destructor -- which blocks until every dispatched
    // task's completion lambda (including its tail bookkeeping that locks
    // mutex_ and touches backlog_/inFlight_) has fully finished -- runs
    // first. If meteredExecutor_ were destroyed after those members, a
    // background completion lambda still finishing its tail bookkeeping
    // could race the destruction of the very state it touches, even though
    // the caller already called wait() first.
    std::mutex mutex_;
    size_t inFlight_ = 0;
    std::deque<PendingTask> backlog_;
    std::vector<folly::SemiFuture<folly::Unit>> pending_;

    /// Non-blocking conduit to the shared physical pool; see the class
    /// comment above for why admission is additionally self-gated by
    /// `inFlight_` / `backlog_` rather than left to
    /// `folly::MeteredExecutor::Options::maxInQueue` alone. Must be declared
    /// last so it is destroyed first (see comment on `mutex_` above).
    std::unique_ptr<folly::MeteredExecutor> meteredExecutor_;
};

/// CH alias: `using ThreadPool = FileCacheThreadPool`
using ThreadPool = FileCacheThreadPool;

} // namespace facebook::velox::ch
