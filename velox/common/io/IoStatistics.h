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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/dynamic.h>
#include "velox/common/base/IoCounter.h"

namespace facebook::velox::io {

struct OperationCounters {
  uint64_t resourceThrottleCount{0};
  uint64_t localThrottleCount{0};
  uint64_t networkThrottleCount{0};
  uint64_t globalThrottleCount{0};
  uint64_t fullThrottleCount{0};
  uint64_t partialThrottleCount{0};
  uint64_t retryCount{0};
  uint64_t latencyInMs{0};
  uint64_t requestCount{0};
  uint64_t delayInjectedInSecs{0};

  void merge(const OperationCounters& other);
};

/// Point-in-time snapshot of per-query BufferedInput probe statistics.
/// All fields are zero when the probe is disabled (the default).
struct BufferedInputProbeSnapshot
{
  uint64_t enqueueCount{0};   ///< Number of `enqueue()` calls.
  uint64_t enqueueBytes{0};   ///< Total bytes requested via `enqueue()`.
  uint64_t nextCount{0};      ///< Number of `Next()` calls.
  uint64_t returnedBytes{0};  ///< Total bytes returned by `Next()`.
  uint64_t seekCount{0};      ///< Number of `seekToPosition()` calls.
  uint64_t maxChunkBytes{0};  ///< Largest single `Next()` chunk seen.

  bool operator==(const BufferedInputProbeSnapshot&) const = default;
};

class IoStatistics {
 public:
  uint64_t rawBytesRead() const;
  uint64_t rawOverreadBytes() const;
  uint64_t rawBytesWritten() const;
  uint64_t inputBatchSize() const;
  uint64_t outputBatchSize() const;
  uint64_t totalScanTimeNs() const;
  uint64_t writeIOTimeUs() const;
  uint64_t duplicateReadRegions() const;
  uint64_t duplicateReadBytes() const;

  uint64_t incRawBytesRead(int64_t);
  uint64_t incRawOverreadBytes(int64_t);
  uint64_t incRawBytesWritten(int64_t);
  uint64_t incInputBatchSize(int64_t);
  uint64_t incOutputBatchSize(int64_t);
  uint64_t incTotalScanTimeNs(int64_t);
  uint64_t incWriteIOTimeUs(int64_t);
  void incDuplicateRead(int64_t regions, int64_t bytes);

  IoCounter& prefetch() {
    return prefetch_;
  }

  IoCounter& read() {
    return read_;
  }

  IoCounter& ssdRead() {
    return ssdRead_;
  }

  IoCounter& ramHit() {
    return ramHit_;
  }

  IoCounter& queryThreadIoLatencyUs() {
    return queryThreadIoLatencyUs_;
  }

  IoCounter& storageReadLatencyUs() {
    return storageReadLatencyUs_;
  }

  IoCounter& ssdCacheReadLatencyUs() {
    return ssdCacheReadLatencyUs_;
  }

  IoCounter& cacheWaitLatencyUs() {
    return cacheWaitLatencyUs_;
  }

  IoCounter& coalescedSsdLoadLatencyUs() {
    return coalescedSsdLoadLatencyUs_;
  }

  IoCounter& coalescedStorageLoadLatencyUs() {
    return coalescedStorageLoadLatencyUs_;
  }

  /// Distribution of gaps (in bytes) between consecutive read regions
  /// before coalescing. Measures data locality on disk.
  IoCounter& readGap() {
    return readGap_;
  }

  const IoCounter& readGap() const {
    return readGap_;
  }

  void incOperationCounters(
      const std::string& operation,
      const uint64_t resourceThrottleCount,
      const uint64_t localThrottleCount,
      const uint64_t networkThrottleCount,
      const uint64_t globalThrottleCount,
      const uint64_t retryCount,
      const uint64_t latencyInMs,
      const uint64_t delayInjectedInSecs,
      const uint64_t fullThrottleCount = 0,
      const uint64_t partialThrottleCount = 0);

  std::unordered_map<std::string, OperationCounters> operationStats() const;

  void merge(const IoStatistics& other);

  folly::dynamic getOperationStatsSnapshot() const;

  // ---------------------------------------------------------------------------
  // BufferedInput probe — lightweight per-query I/O accounting.
  // Probe methods are no-ops when disabled (default) so they can be called
  // unconditionally on every I/O path without branch overhead in steady state.
  // ---------------------------------------------------------------------------

  /// Enables the probe for this statistics instance. Not thread-safe; must be
  /// called before the first I/O operation on the associated stream.
  void enableBufferedInputProbe()
  {
    probeEnabled_.store(true, std::memory_order_release);
  }

  /// Records one `enqueue()` call requesting `bytes` bytes.
  void recordBufferedInputEnqueue(uint64_t bytes)
  {
    if (!probeEnabled_.load(std::memory_order_acquire))
    {
      return;
    }
    probeEnqueueCount_.fetch_add(1, std::memory_order_relaxed);
    probeEnqueueBytes_.fetch_add(bytes, std::memory_order_relaxed);
  }

  /// Records one `Next()` call returning `chunkBytes` bytes and updates the
  /// running maximum via a CAS loop.
  void recordBufferedInputNext(uint64_t chunkBytes)
  {
    if (!probeEnabled_.load(std::memory_order_acquire))
    {
      return;
    }
    probeNextCount_.fetch_add(1, std::memory_order_relaxed);
    probeReturnedBytes_.fetch_add(chunkBytes, std::memory_order_relaxed);
    uint64_t current = probeMaxChunkBytes_.load(std::memory_order_relaxed);
    while (chunkBytes > current)
    {
      if (probeMaxChunkBytes_.compare_exchange_weak(
              current, chunkBytes, std::memory_order_relaxed))
      {
        break;
      }
    }
  }

  /// Records one `seekToPosition()` call.
  void recordBufferedInputSeek()
  {
    if (!probeEnabled_.load(std::memory_order_acquire))
    {
      return;
    }
    probeSeekCount_.fetch_add(1, std::memory_order_relaxed);
  }

  /// Returns a consistent snapshot of the probe counters. Returns an all-zero
  /// struct when the probe is disabled.
  BufferedInputProbeSnapshot bufferedInputProbeSnapshot() const
  {
    if (!probeEnabled_.load(std::memory_order_acquire))
    {
      return {};
    }
    BufferedInputProbeSnapshot s;
    s.enqueueCount = probeEnqueueCount_.load(std::memory_order_relaxed);
    s.enqueueBytes = probeEnqueueBytes_.load(std::memory_order_relaxed);
    s.nextCount = probeNextCount_.load(std::memory_order_relaxed);
    s.returnedBytes = probeReturnedBytes_.load(std::memory_order_relaxed);
    s.seekCount = probeSeekCount_.load(std::memory_order_relaxed);
    s.maxChunkBytes = probeMaxChunkBytes_.load(std::memory_order_relaxed);
    return s;
  }

 private:
  std::atomic_uint64_t rawBytesRead_{0};
  std::atomic_uint64_t rawBytesWritten_{0};
  std::atomic_uint64_t inputBatchSize_{0};
  std::atomic_uint64_t outputBatchSize_{0};
  std::atomic_uint64_t rawOverreadBytes_{0};
  std::atomic_uint64_t totalScanTimeNs_{0};
  std::atomic_uint64_t writeIOTimeUs_{0};
  std::atomic_uint64_t duplicateReadRegions_{0};
  std::atomic_uint64_t duplicateReadBytes_{0};

  // Planned read from storage or SSD.
  IoCounter prefetch_;

  // Read from storage, for sparsely accessed columns.
  IoCounter read_;

  // Hits from RAM cache. Does not include first use of prefetched data.
  IoCounter ramHit_;

  // Read from SSD cache instead of storage. Includes both random and planned
  // reads.
  IoCounter ssdRead_;

  // Time spent by a query processing thread waiting for synchronously issued IO
  // or for an in-progress read-ahead to finish.
  IoCounter queryThreadIoLatencyUs_;

  // Breakdown of queryThreadIoLatencyUs_ by I/O type:

  // Time spent waiting for remote storage reads (S3, HDFS, etc.)
  IoCounter storageReadLatencyUs_;

  // Time spent waiting for SSD cache reads
  IoCounter ssdCacheReadLatencyUs_;

  // Time spent waiting for EXCLUSIVE cache entries (another thread is loading)
  IoCounter cacheWaitLatencyUs_;

  // Time spent waiting for coalesced loads from SSD cache
  IoCounter coalescedSsdLoadLatencyUs_;

  // Time spent waiting for coalesced loads from remote storage
  IoCounter coalescedStorageLoadLatencyUs_;

  // Gap between consecutive read regions before coalescing.
  IoCounter readGap_;

  std::unordered_map<std::string, OperationCounters> operationStats_;
  mutable std::mutex operationStatsMutex_;

  // --- BufferedInput probe ---
  std::atomic<bool> probeEnabled_{false};
  std::atomic_uint64_t probeEnqueueCount_{0};
  std::atomic_uint64_t probeEnqueueBytes_{0};
  std::atomic_uint64_t probeNextCount_{0};
  std::atomic_uint64_t probeReturnedBytes_{0};
  std::atomic_uint64_t probeSeekCount_{0};
  std::atomic_uint64_t probeMaxChunkBytes_{0};
};

} // namespace facebook::velox::io
