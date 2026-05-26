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

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include <cstddef>

namespace facebook::velox::cache::fs {

/// Thin wrapper over folly::IOThreadPoolExecutor for asynchronous segment
/// downloads. IO-bound by design; must NOT be shared with the Velox CPU
/// executor (spec §7.1 / §10 R4) so a slow remote does not stall CPU work.
class DownloadThreadPool {
 public:
  /// Constructs a pool with `numThreads` worker threads. Caps at 32 internally
  /// to bound per-remote-pread contention (S3 / HDFS start throttling beyond
  /// that point and we would lose more to contention than gain in parallelism).
  /// Throws VeloxRuntimeError if numThreads is zero.
  explicit DownloadThreadPool(size_t numThreads);
  ~DownloadThreadPool();

  /// Submits a no-arg callable; returns a SemiFuture that becomes ready when
  /// the task finishes (successfully or via exception).
  folly::SemiFuture<folly::Unit> submit(folly::Function<void()> task);

 private:
  folly::IOThreadPoolExecutor executor_;
};

} // namespace facebook::velox::cache::fs
