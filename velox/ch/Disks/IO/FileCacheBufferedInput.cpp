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

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"

#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"

#include <algorithm>

namespace facebook::velox::ch
{

FileCacheBufferedInput::FileCacheBufferedInput(
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
    folly::F14FastMap<std::string, std::string> fileReadOps,
    folly::CancellationToken cancellationToken)
    : dwio::common::BufferedInput(
          readFile,
          readerOptions.memoryPool(),
          metricsLog,
          ioStatistics.get(),
          ioStats.get(),
          dwio::common::BufferedInput::kMaxMergeDistance,
          std::nullopt,
          std::move(fileReadOps),
          requestContext.cacheable),
      sourceReadFile_(std::move(readFile)),
      cache_(std::move(cache)),
      cacheKey_(std::move(cacheKey)),
      origin_(std::move(origin)),
      cacheOptions_(std::move(cacheOptions)),
      requestContext_(std::move(requestContext)),
      ioStatistics_(std::move(ioStatistics)),
      ioStats_(std::move(ioStats)),
      executor_(executor),
      readerOptions_(readerOptions),
      memoryPool_(&readerOptions.memoryPool()),
      fileSize_(sourceReadFile_ ? sourceReadFile_->size() : 0),
      cancellationToken_(std::move(cancellationToken))
{
    VELOX_CHECK_NOT_NULL(sourceReadFile_, "FileCacheBufferedInput requires a source ReadFile");
    VELOX_CHECK_NOT_NULL(cache_, "FileCacheBufferedInput requires a FileCache");
    VELOX_CHECK_NOT_NULL(memoryPool_, "ReaderOptions::memoryPool must be non-null");
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::enqueue(
    velox::common::Region region,
    const dwio::common::StreamIdentifier * sid)
{
    // Record only the copied region value (and non-owning stream-identifier
    // metadata); never a stream pointer. `load` operates on these copies.
    requests_.push_back({region, sid});
    return std::make_unique<FileCacheInputStream>(
        this, region, requestContext_, dwio::common::LogType::STREAM);
}

void FileCacheBufferedInput::load(dwio::common::LogType /*logType*/)
{
    // First version: a no-op planning barrier over the copied request values. It
    // must not store or dereference any SeekableInputStream pointer -- doing so
    // would use-after-free when a caller discards an enqueue result before load.
    // The FileSegmentsHolder is acquired lazily inside Next -> initializeIfNeeded,
    // preserving ClickHouse's on-demand downloader semantics.
    requests_.clear();
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    dwio::common::LogType logType) const
{
    // Unplanned reads still go through the FileCache state machine; never fall
    // back to a raw SeekableFileInputStream, which would bypass the cache.
    return std::make_unique<FileCacheInputStream>(
        const_cast<FileCacheBufferedInput *>(this),
        velox::common::Region{offset, length},
        requestContext_,
        logType);
}

bool FileCacheBufferedInput::isBuffered(uint64_t offset, uint64_t length) const
{
    if (length == 0)
        return false;

    // Absolute last byte of the probed range. checkedAdd rejects an overflowing
    // offset + length up front -- before it reaches FileCache::get or is used as
    // a loop bound below -- instead of wrapping to a spurious in-range value.
    const uint64_t right =
        FileCacheUtils::checkedAdd(offset, length, "FileCacheBufferedInput::isBuffered range") - 1;

    // No-create probe: FileCache::get never creates metadata, acquires a
    // downloader, or reserves space (unlike getOrSet).
    auto holder = cache_->get(cacheKey_, offset, length, cacheOptions_.segmentsBatchSize, origin_.user_id);
    if (holder->empty())
        return false;

    if (!holder->front().range().contains(offset))
        return false;

    for (const auto & segmentPtr : *holder)
    {
        const auto & segment = *segmentPtr;
        if (segment.range().left > right)
            break;

        if (right <= segment.range().right)
        {
            // The last segment we need: a downloaded prefix covering `right`, or a
            // fully-downloaded segment, satisfies the probe.
            return right < segment.getCurrentWriteOffset()
                || segment.state() == FileSegment::State::DOWNLOADED;
        }

        // An interior segment must be fully downloaded.
        if (segment.state() != FileSegment::State::DOWNLOADED)
            return false;
    }

    return false;
}

std::unique_ptr<dwio::common::BufferedInput> FileCacheBufferedInput::clone() const
{
    // A clean instance sharing the same source file, cache, and context. Enqueued
    // regions are not copied (BufferedInput contract).
    return std::make_unique<FileCacheBufferedInput>(
        sourceReadFile_,
        cache_,
        cacheKey_,
        origin_,
        cacheOptions_,
        requestContext_,
        dwio::common::MetricsLog::voidLog(),
        ioStatistics_,
        ioStats_,
        executor_,
        readerOptions_,
        folly::F14FastMap<std::string, std::string>{},
        cancellationToken_);
}

} // namespace facebook::velox::ch
