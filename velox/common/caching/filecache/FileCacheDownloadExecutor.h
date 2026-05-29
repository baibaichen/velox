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

#include <folly/Function.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include <cstddef>

namespace facebook::velox::ch {

/// Thin wrapper over folly::IOThreadPoolExecutor for asynchronous segment
/// downloads. IO-bound by design; must not be shared with the Velox CPU
/// executor so slow remote reads do not stall CPU work.
class FileCacheDownloadExecutor {
 public:
  /// Constructs an executor with `numThreads` worker threads. Caps at 32
  /// internally to bound per-remote-pread contention; S3 and HDFS can start
  /// throttling beyond that point and lose more to contention than they gain
  /// in parallelism. Throws VeloxRuntimeError if numThreads is zero.
  explicit FileCacheDownloadExecutor(size_t numThreads);
  ~FileCacheDownloadExecutor();

  /// Submits a no-arg callable; returns a SemiFuture that becomes ready when
  /// the task finishes successfully or via exception.
  folly::SemiFuture<folly::Unit> submit(folly::Function<void()> task);

 private:
  folly::IOThreadPoolExecutor executor_;
};

} // namespace facebook::velox::ch
