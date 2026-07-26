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
#include "velox/ch/Disks/IO/FileCacheCoalescedLoad.h"

#include "velox/ch/Common/FileCacheQueryIdScope.h"

#include <folly/ScopeGuard.h>

#include <algorithm>
#include <cstring>

namespace facebook::velox::ch
{

namespace
{
/// Byte length of the interval-union of a set of requested regions. Overlapping
/// or duplicate regions count each physical byte once, so a prefetch group with
/// two identical (or overlapping) requested ranges reports the real number of
/// useful prefetched bytes rather than a per-range sum.
uint64_t requestedUnionLength(const std::vector<FileCacheLoadRequest> & requests)
{
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    intervals.reserve(requests.size());
    for (const auto & request : requests)
    {
        if (request.region.length > 0)
            intervals.emplace_back(
                request.region.offset,
                request.region.offset + request.region.length);
    }
    std::sort(intervals.begin(), intervals.end());

    uint64_t total = 0;
    uint64_t curBegin = 0;
    uint64_t curEnd = 0;
    bool have = false;
    for (const auto & [begin, end] : intervals)
    {
        if (!have)
        {
            curBegin = begin;
            curEnd = end;
            have = true;
            continue;
        }
        if (begin > curEnd)
        {
            total += curEnd - curBegin;
            curBegin = begin;
            curEnd = end;
        }
        else
        {
            curEnd = std::max(curEnd, end);
        }
    }
    if (have)
        total += curEnd - curBegin;
    return total;
}
} // namespace

FileCacheCoalescedLoad::FileCacheCoalescedLoad(
    Context context,
    uint64_t groupOffset,
    uint64_t groupLength,
    std::vector<FileCacheLoadRequest> requests)
    : cache::CoalescedLoad({}, {})
    , context_(std::move(context))
    , requests_(std::move(requests))
    , groupOffset_(groupOffset)
    , groupLength_(groupLength)
{
}

std::vector<cache::CachePin> FileCacheCoalescedLoad::loadData(bool prefetch)
{
    VELOX_CHECK_NOT_NULL(context_.readContext);

    // Stabilise the caller identity for this load so the internal streams'
    // getOrSetDownloader / reserve / write all agree on "<queryId>:<os-tid>".
    // loadData runs on the prefetch executor thread (for prefetch groups) or the
    // query thread (for demand groups); in neither case is the thread-local query
    // id set for us. Without this scope, FileCacheQueryLimit::isQueryInitialized
    // returns false on the executor thread, tryGetQueryContext returns null, and
    // the per-query download limit (maxDownloadSizePerQuery) is never enforced.
    // The Context's queryContextHolder keeps the query account alive in the map
    // for the duration of the load; this scope is what makes reserve find it.
    FileCacheQueryIdScope scope(context_.readContext->requestContext.queryId);

    // Take the group's bounding-range segment holder. This pins the group's
    // FileSegment metadata (including any EMPTY gap segments) for the duration of
    // the load. It is released on ALL exit paths (success or exception) by the
    // scope guard below, but only AFTER every internal stream has taken its own
    // precise per-request holder -- so it is never released before the sub-reads
    // are done.
    groupSegments_ = getFileSegmentsForRead(
        *context_.readContext, groupOffset_, groupLength_);

    // fileSegments -> reset on every exit path (success + exception). The internal
    // streams below each acquire their own precise holders inside their first
    // Next, so the group holder is only released here at the very end.
    SCOPE_EXIT
    {
        groupSegments_.reset();
    };

    // Materialise each request's region through an internal FileCacheInputStream.
    // A duplicate region (same offset+length as an already-read request) is NOT
    // re-read from the source or the local FileSegment: it copies the bytes of the
    // already-materialised buffers into fresh independent allocations (mirrors
    // Direct duplicateRegion / copyDuplicateRegion).
    std::vector<std::vector<FileCachePreparedBuffer>> materialised(requests_.size());

    // Map an already-materialised region -> the index in requests_ that produced
    // it, so a later request with the same region copies instead of re-reading.
    std::vector<std::pair<velox::common::Region, size_t>> seen;

    for (size_t i = 0; i < requests_.size(); ++i)
    {
        const auto & region = requests_[i].region;

        size_t sourceIndex = requests_.size();
        for (const auto & [seenRegion, seenIndex] : seen)
        {
            if (seenRegion.offset == region.offset
                && seenRegion.length == region.length)
            {
                sourceIndex = seenIndex;
                break;
            }
        }

        if (sourceIndex != requests_.size())
        {
            // Duplicate region: allocate independent buffers and copy only the
            // valid region bytes; never create a second internal stream and never
            // read the local FileSegment.
            const auto & sourceBuffers = materialised[sourceIndex];
            std::vector<FileCachePreparedBuffer> copies;
            copies.reserve(sourceBuffers.size());
            for (const auto & src : sourceBuffers)
            {
                const size_t validBytes = src.region.length;
                velox::BufferPtr copy = velox::AlignedBuffer::allocate<char>(
                    validBytes, context_.readContext->pool.get());
                std::memcpy(
                    copy->asMutable<char>(),
                    src.data->as<char>(),
                    validBytes);
                copies.push_back(FileCachePreparedBuffer{std::move(copy), src.region});
            }
            materialised[i] = std::move(copies);
            continue;
        }

        // First (real) read of this region: drive an internal stream to completion.
        // Downloader election / reserve / write / errno all live in the stream; the
        // load layer never touches a FileSegment directly.
        auto stream = FileCacheInputStream::createCoalescedInternal(
            context_.readContext, region, dwio::common::LogType::FILE);

        std::vector<FileCachePreparedBuffer> buffers;
        const void * data = nullptr;
        int32_t size = 0;
        while (stream->Next(&data, &size))
        {
            auto prepared = stream->takeLastOutputBuffer();
            if (prepared.has_value())
                buffers.push_back(std::move(prepared.value()));
        }

        seen.emplace_back(region, i);
        materialised[i] = std::move(buffers);
    }

    // Publish all requests atomically: on success every request becomes ready in
    // one critical section. On an exception above we never reach here, so no
    // partial success-shaped payload is published; already-written FileSegment
    // bytes are retained by the internal streams' state machine.
    {
        std::lock_guard<std::mutex> lock(requestMutex_);
        for (size_t i = 0; i < requests_.size(); ++i)
        {
            requests_[i].buffers = std::move(materialised[i]);
            requests_[i].ready = true;
        }
    }

    // Useful-prefetch accounting. Only a prefetch load prepares bytes ahead of a
    // demand read, so only it contributes to prefetch(). The internal streams
    // already recorded source read()/ssdRead()/rawBytesRead as they ran; the
    // coalesced model reads exactly the requested regions (no gap over-read), so
    // the useful prefetch bytes are the interval-union of the requested regions --
    // duplicate/overlapping regions counted once, matching the source bytes read.
    if (prefetch)
    {
        if (auto * ioStatistics = context_.readContext->ioStatistics.get())
            ioStatistics->prefetch().increment(requestedUnionLength(requests_));
    }

    return {};
}

int64_t FileCacheCoalescedLoad::size() const
{
    int64_t total = 0;
    for (const auto & request : requests_)
    {
        total += request.region.length;
    }
    return total;
}

std::optional<std::vector<FileCachePreparedBuffer>>
FileCacheCoalescedLoad::getData(const std::vector<size_t> & requestIndices)
{
    std::lock_guard<std::mutex> lock(requestMutex_);

    // All-or-nothing: every requested business request must be ready and not yet
    // consumed. Otherwise deliver nothing (the caller falls back to the plain
    // demand path). This never publishes a partial success-shaped payload.
    for (const size_t requestIndex : requestIndices)
    {
        bool found = false;
        for (const auto & request : requests_)
        {
            if (request.requestIndex != requestIndex)
                continue;
            found = true;
            if (!request.ready || request.consumed)
                return std::nullopt;
        }
        if (!found)
            return std::nullopt;
    }

    // Move out the buffers of every matching request and mark them consumed.
    std::vector<FileCachePreparedBuffer> result;
    for (const size_t requestIndex : requestIndices)
    {
        for (auto & request : requests_)
        {
            if (request.requestIndex != requestIndex)
                continue;
            for (auto & buffer : request.buffers)
                result.push_back(std::move(buffer));
            request.buffers.clear();
            request.consumed = true;
        }
    }
    return result;
}

} // namespace facebook::velox::ch
