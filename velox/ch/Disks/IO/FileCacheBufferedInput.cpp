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
    folly::F14FastMap<std::string, std::string> fileReadOps)
    : dwio::common::BufferedInput(
        readFile,
        readerOptions.memoryPool(),
        metricsLog,
        ioStatistics.get(),
        ioStats.get(),
        dwio::common::BufferedInput::kMaxMergeDistance,
        std::nullopt,
        std::move(fileReadOps),
        requestContext.cacheable)
    , sourceReadFile_(std::move(readFile))
    , cache_(std::move(cache))
    , cacheKey_(cacheKey)
    , origin_(std::move(origin))
    , cacheOptions_(cacheOptions)
    , requestContext_(std::move(requestContext))
    , ioStatistics_(std::move(ioStatistics))
    , ioStats_(std::move(ioStats))
    , executor_(executor)
    , readerOptions_(readerOptions)
    , fileSize_(sourceReadFile_ ? sourceReadFile_->size() : 0)
{
    VELOX_CHECK_NOT_NULL(cache_, "FileCacheBufferedInput requires a FileCache");
    VELOX_CHECK_NOT_NULL(
        &readerOptions_.memoryPool(),
        "FileCacheBufferedInput requires a non-null memory pool");
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::enqueue(
    velox::common::Region region,
    const dwio::common::StreamIdentifier * sid)
{
    // Record the copied region value only; never store the stream pointer.
    requests_.push_back(Request{region, sid});
    return std::make_unique<FileCacheInputStream>(
        this, region, requestContext_, dwio::common::LogType::STREAM);
}

void FileCacheBufferedInput::load(dwio::common::LogType /*logType*/)
{
    // First version: a no-op planning barrier. It must not dereference any
    // SeekableInputStream pointer (enqueue transferred ownership to the caller,
    // which may already have discarded it). The FileSegmentsHolder is acquired
    // lazily inside FileCacheInputStream::Next, preserving CH on-demand
    // downloader semantics. A future prefetch extension may submit copied region
    // values from requests_ to executor_, but never a stream pointer.
    requests_.clear();
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    dwio::common::LogType logType) const
{
    // Unplanned reads still go through the FileCache state machine; do not fall
    // back to a raw SeekableFileInputStream (that bypasses the cache).
    return std::make_unique<FileCacheInputStream>(
        const_cast<FileCacheBufferedInput *>(this),
        velox::common::Region{offset, length},
        requestContext_,
        logType);
}

bool FileCacheBufferedInput::isBuffered(uint64_t offset, uint64_t length) const
{
    if (length == 0)
        return true;

    // No-create probe: FileCache::get never creates metadata, acquires a
    // downloader, or reserves space (unlike getOrSet). EMPTY placeholders in the
    // returned holder are detached and do not persist.
    auto holder = cache_->get(
        cacheKey_,
        offset,
        length,
        cacheOptions_.segmentsBatchSize,
        origin_.user_id);

    if (!holder || holder->empty())
        return false;

    const uint64_t requestedRight =
        FileCacheUtils::checkedAdd(offset, length, "isBuffered range") - 1;

    // Every byte of [offset, requestedRight] must be backed by a downloaded
    // prefix of a contiguous chain of segments.
    uint64_t expectedLeft = offset;
    for (const auto & segmentPtr : *holder)
    {
        const auto & segment = *segmentPtr;
        const auto range = segment.range();
        if (range.left > expectedLeft)
            return false; // hole before this segment

        const uint64_t neededRight = std::min<uint64_t>(requestedRight, range.right);
        // The downloaded prefix must cover up to neededRight (exclusive write
        // offset must exceed neededRight), matching CH isRangeContainedInSegments.
        if (segment.getCurrentWriteOffset() <= neededRight)
        {
            if (!(neededRight == range.right
                  && segment.state() == FileSegment::State::DOWNLOADED))
                return false;
        }

        if (range.right >= requestedRight)
            return true;

        expectedLeft = range.right + 1;
    }
    return false;
}

std::unique_ptr<dwio::common::BufferedInput> FileCacheBufferedInput::clone() const
{
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
        readerOptions_);
}

} // namespace facebook::velox::ch
