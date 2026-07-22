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

#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"

#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"

#include <folly/container/F14Map.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>
#include <vector>

namespace facebook::velox::ch
{

class FileCacheInputStream;

/// Velox `BufferedInput` that drives the ClickHouse `FileCache` scan read path.
///
/// It is not an `AsyncDataCache` subclass and does not inherit
/// `CachedBufferedInput`; it directly subclasses `BufferedInput` and creates
/// `FileCacheInputStream` state machines (ported from CH
/// `CachedOnDiskReadBufferFromFile`). This object only stores immutable context
/// and creates streams; the per-region streaming state machine lives in
/// `FileCacheInputStream`.
///
/// Lifetime contract: every `SeekableInputStream` returned by `enqueue` or
/// `read` holds a non-owning back-pointer to the `FileCacheBufferedInput` that
/// created it and reads its context (cache, key, origin, options, source file,
/// memory pool) on every `Next`/`seek`. The owner must therefore outlive every
/// stream it hands out; destroying it while a stream is still alive is undefined
/// behavior. This matches the Velox `BufferedInput` convention (streams borrow
/// their owner) and is intentional -- there is no shared-ownership redesign.
///
/// One object created here can outlive both the stream and this owner: a remote
/// reader handed off to a `FileSegment` for background download. That reader is
/// stripped of its (query-pool-charged) owned buffer at handoff -- it only holds
/// its source `shared_ptr` and reads into an externally supplied buffer -- so a
/// background worker on another thread can use and destroy it safely after this
/// owner and its query pool are gone.
class FileCacheBufferedInput : public dwio::common::BufferedInput
{
public:
    FileCacheBufferedInput(
        std::shared_ptr<ReadFile> readFile,
        FileCachePtr cache,
        FileCacheKey cacheKey,
        FileCacheOriginInfo origin,
        FileCacheReadOptions cacheOptions,
        FileCacheRequestContext requestContext,
        const dwio::common::MetricsLogPtr & metricsLog,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        std::shared_ptr<velox::IoStats> ioStats,
        folly::Executor * executor,
        const dwio::common::ReaderOptions & readerOptions,
        folly::F14FastMap<std::string, std::string> fileReadOps = {});

    // BufferedInput overrides. The returned stream borrows `*this` (see the
    // class-level lifetime contract) and must not outlive this owner.
    std::unique_ptr<dwio::common::SeekableInputStream> enqueue(
        velox::common::Region region,
        const dwio::common::StreamIdentifier * sid = nullptr) override;

    void load(dwio::common::LogType logType) override;

    // The returned stream borrows `*this` and must not outlive this owner.
    std::unique_ptr<dwio::common::SeekableInputStream>
    read(uint64_t offset, uint64_t length, dwio::common::LogType logType)
        const override;

    bool isBuffered(uint64_t offset, uint64_t length) const override;

    void preload() override {}
    bool preloaded() const override { return false; }
    bool shouldPreload(int32_t numPages = 0) override { return false; }

    // Must return false: prevents DWRF StripeMetadataCache from hard-casting
    // FileCacheInputStream to CacheInputStream (undefined behavior).
    bool shouldPrefetchStripes() const override { return false; }

    std::unique_ptr<dwio::common::BufferedInput> clone() const override;

    // Returns the injected executor; never a Manager-owned pool.
    folly::Executor * executor() const override { return executor_; }

    // Returns false: FileCacheInputStream is not CachePin-compatible, so the
    // cacheRegion/findCachedRegion APIs must not be advertised.
    bool hasCache() const override { return false; }

    // Accessors for FileCacheInputStream.
    FileCache & fileCache() const { return *cache_; }
    const std::shared_ptr<ReadFile> & sourceReadFile() const
    {
        return sourceReadFile_;
    }
    const FileCacheKey & cacheKey() const { return cacheKey_; }
    const FileCacheOriginInfo & origin() const { return origin_; }
    const FileCacheReadOptions & cacheOptions() const { return cacheOptions_; }
    const FileCacheRequestContext & requestContext() const { return requestContext_; }
    uint64_t fileSize() const { return fileSize_; }
    velox::memory::MemoryPool * memoryPool() const { return memoryPool_; }
    io::IoStatistics * ioStatistics() const { return ioStatistics_.get(); }
    velox::IoStats * ioStats() const { return ioStats_.get(); }

private:
    struct Request
    {
        velox::common::Region region;
        const dwio::common::StreamIdentifier * sid = nullptr;
    };

    std::shared_ptr<ReadFile> sourceReadFile_;
    FileCachePtr cache_;
    FileCacheKey cacheKey_;
    FileCacheOriginInfo origin_;
    FileCacheReadOptions cacheOptions_;
    FileCacheRequestContext requestContext_;
    std::shared_ptr<io::IoStatistics> ioStatistics_;
    std::shared_ptr<velox::IoStats> ioStats_;
    folly::Executor * executor_;
    dwio::common::ReaderOptions readerOptions_;
    velox::memory::MemoryPool * memoryPool_;
    uint64_t fileSize_;

    // Copied region values only; never stream pointers. `load` operates on these
    // copies so a caller that discards an `enqueue` result before `load` cannot
    // cause a use-after-free.
    std::vector<Request> requests_;
};

} // namespace facebook::velox::ch
