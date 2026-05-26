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

#include "velox/common/caching/fscache/DownloadThreadPool.h"

#include "velox/common/base/Exceptions.h"

#include <folly/executors/thread_factory/NamedThreadFactory.h>

#include <algorithm>

namespace facebook::velox::cache::fs {

namespace {
// Bound the worker count. Beyond ~32 threads per remote, S3 / HDFS start
// returning 503s faster than additional parallelism helps; misconfigured
// downloadThreads would otherwise silently regress p99 fetch latency. If
// 32 turns out to be wrong, raise after Task 16 perf measurement.
constexpr size_t kMaxThreads{32};

size_t checkedNumThreads(size_t numThreads) {
  VELOX_CHECK_GT(numThreads, 0, "DownloadThreadPool needs at least 1 thread");
  return std::min(numThreads, kMaxThreads);
}
} // namespace

DownloadThreadPool::DownloadThreadPool(size_t numThreads)
    : executor_{
          checkedNumThreads(numThreads),
          std::make_shared<folly::NamedThreadFactory>("FsCacheDownload")} {}

DownloadThreadPool::~DownloadThreadPool() {
  executor_.join();
}

folly::SemiFuture<folly::Unit> DownloadThreadPool::submit(
    folly::Function<void()> task) {
  return folly::via(&executor_, std::move(task));
}

} // namespace facebook::velox::cache::fs
