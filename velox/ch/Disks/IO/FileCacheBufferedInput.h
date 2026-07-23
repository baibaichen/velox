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
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"

#include <folly/container/F14Map.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>
#include <vector>

namespace facebook::velox::ch
{

class FileCacheInputStream;

/// `BufferedInput` subclass that routes Velox scan/DWIO reads through the
/// ClickHouse `FileCache`. It stores the immutable per-file read context and
/// creates one `FileCacheInputStream` per region; it does not itself drive the
/// segment state machine. See `port/3-consumers/03-filecache-buffered-input-design.md`.
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

    // BufferedInput overrides.
    std::unique_ptr<dwio::common::SeekableInputStream> enqueue(
        velox::common::Region region,
        const dwio::common::StreamIdentifier * sid = nullptr) override;

    void load(dwio::common::LogType logType) override;

    std::unique_ptr<dwio::common::SeekableInputStream>
    read(uint64_t offset, uint64_t length, dwio::common::LogType logType)
        const override;

    bool isBuffered(uint64_t offset, uint64_t length) const override;

    void preload() override {}
    bool preloaded() const override { return false; }
    bool shouldPreload(int32_t numPages = 0) override
    {
        (void)numPages;
        return false;
    }

    // Must return false: prevents DWRF StripeMetadataCache from hard-casting
    // FileCacheInputStream to CacheInputStream.
    bool shouldPrefetchStripes() const override { return false; }

    std::unique_ptr<dwio::common::BufferedInput> clone() const override;

    // Returns the injected executor; does not return a Manager-owned pool.
    folly::Executor * executor() const override { return executor_; }

    // Returns false: FileCacheInputStream is not CachePin-compatible.
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
    velox::memory::MemoryPool * memoryPool() const { return &readerOptions_.memoryPool(); }
    uint64_t fileSize() const { return fileSize_; }
    // Per-split IoStatistics from the connector (may be null). Operator-level
    // hit/miss byte attribution is recorded here so it reaches OperatorStats.
    io::IoStatistics * ioStatistics() const { return ioStatistics_.get(); }

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
    uint64_t fileSize_;

    // Copied region values only; never stream pointers.
    std::vector<Request> requests_;
};

} // namespace facebook::velox::ch
