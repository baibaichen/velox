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

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Disks/IO/FileCacheCoalescedLoad.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/base/CoalesceIo.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/time/Timer.h"
#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/StreamIdentifier.h"

#include <folly/ScopeGuard.h>

#include <glog/logging.h>

#include <algorithm>
#include <vector>

DECLARE_int32(cache_prefetch_min_pct);

namespace facebook::velox::ch
{

namespace
{
// True if the read percentage is high enough to warrant prefetch. Mirrors the
// anonymous-namespace helper in DirectBufferedInput.cpp so the FileCache
// planner uses the identical threshold (the cache_prefetch_min_pct gflag).
bool isPrefetchablePct(int32_t pct)
{
    return pct >= FLAGS_cache_prefetch_min_pct;
}

} // namespace

FileCacheBufferedInput::FileCacheBufferedInput(
    std::shared_ptr<ReadFile> readFile,
    FileCachePtr cache,
    FileCacheKey cacheKey,
    FileCacheOriginInfo origin,
    FileCacheReadOptions cacheOptions,
    FileCacheRequestContext requestContext,
    QueryStatus queryStatus,
    const dwio::common::MetricsLogPtr & metricsLog,
    velox::StringIdLease fileNum,
    velox::StringIdLease groupId,
    std::shared_ptr<velox::cache::ScanTracker> tracker,
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
    , queryStatus_(std::move(queryStatus))
    , fileNum_(std::move(fileNum))
    , groupId_(std::move(groupId))
    , tracker_(std::move(tracker))
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
    readContext_ = makeReadContext();
}

FileCacheBufferedInput::~FileCacheBufferedInput()
{
    // R2-4: cancel planned coalesced loads (mirrors DirectBufferedInput's
    // destructor). A running load is kept alive by its shared context and
    // completes safely after this input is gone; cancel only affects planned.
    for (auto & load : coalescedLoads_)
    {
        if (load)
            load->cancel();
    }
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::enqueue(
    velox::common::Region region,
    const dwio::common::StreamIdentifier * sid)
{
    // Build a stable tracking id from the stream identifier and record the
    // reference on the ScanTracker (mirrors DirectBufferedInput::enqueue, which
    // records the reference unconditionally, even when preloaded). The tracker
    // is optional (may be null in tests / uninstrumented paths).
    velox::cache::TrackingId trackingId;
    if (sid != nullptr)
    {
        trackingId = velox::cache::TrackingId(sid->getId());
    }
    if (tracker_)
    {
        tracker_->recordReference(
            trackingId, region.length, fileNum_.id(), groupId_.id());
    }

    // A4: after whole-file preload, serve directly from RAM. Do not enqueue a
    // request and do not touch the FileCache state machine; the reference was
    // already recorded above, and the preload stream records delivered bytes
    // (recordRead) itself via its tracking id (mirrors DirectBufferedInput,
    // where the preloaded stream still tracks reference and read).
    if (preloaded())
    {
        return makePreloadedStream(region.offset, region.length, trackingId);
    }

    // Record the copied region value only; never store the stream pointer.

    const size_t requestIndex = requests_.size();
    requests_.push_back(Request{region, trackingId});
    requests_.back().requestIndex = requestIndex;
    auto stream = std::make_unique<FileCacheInputStream>(
        this,
        readContext_,
        region,
        dwio::common::LogType::STREAM,
        trackingId);
    // Record the business stream pointer as a stable map key for
    // streamToCoalescedLoads_ (filled in load()). It is never dereferenced.
    requests_.back().stream = stream.get();
    return stream;
}

std::shared_ptr<const FileCacheReadContext> FileCacheBufferedInput::makeReadContext() const
{
    // Assemble the immutable per-file read context from this input's accessors.
    // Called exactly once from the constructor body; every FileCacheInputStream
    // created by this input shares the resulting readContext_ instance.
    auto context = std::make_shared<FileCacheReadContext>();
    context->cache = cache_;
    context->ioStatistics = ioStatistics_;
    context->ioStats = ioStats_;
    context->source = getInputStream();
    context->pool = readerOptions_.memoryPool().shared_from_this();
    context->key = cacheKey_;
    context->origin = origin_;
    context->cacheOptions = cacheOptions_;
    context->requestContext = requestContext_;
    context->queryStatus = queryStatus_;
    context->tracker = tracker_;
    context->fileNum = fileNum_;
    context->groupId = groupId_;
    context->fileSize = fileSize_;
    return context;
}

void FileCacheBufferedInput::load(dwio::common::LogType /*logType*/)
{
    // B2: build read-planning chunks by splitting each enqueued region at
    // loadQuantum. This is pure planning: it does NOT start any IO and does NOT
    // change what Next() reads (the stream still reads lazily and the plan is
    // consumed only in stage B5). The FileSegmentsHolder is still acquired
    // lazily inside FileCacheInputStream::Next, preserving CH on-demand
    // downloader semantics.
    plan_.clear();
    sourceGroups_.clear();
    const uint64_t loadQuantum = static_cast<uint64_t>(readerOptions_.loadQuantum());
    for (size_t k = 0; k < requests_.size(); ++k)
    {
        const auto & request = requests_[k];
        const velox::cache::TrackingId trackingId = request.trackingId;
        const bool prefetch = classifyPrefetch(trackingId);
        const uint64_t regionEnd = request.region.offset + request.region.length;
        for (uint64_t off = request.region.offset; off < regionEnd; off += loadQuantum)
        {
            const uint64_t chunkLen = std::min(loadQuantum, regionEnd - off);
            PlanChunk chunk{off, chunkLen, trackingId};
            chunk.state = classifyChunk(off, chunkLen);
            chunk.prefetch = prefetch;
            chunk.requestIndex = k;
            plan_.push_back(chunk);
        }
    }

    // B4: coalesce only the kMiss chunks into candidate source groups. Hit
    // chunks are served locally and never enter a source group; downloading
    // chunks go through the wait path (stage B5) and are not coalesced here.
    // Prefetch and demand misses are grouped separately, mirroring the two
    // storageLoad buckets in DirectBufferedInput::load. This produces data
    // structures only -- no IO is started.
    std::vector<size_t> missPrefetch;
    std::vector<size_t> missDemand;
    for (size_t i = 0; i < plan_.size(); ++i)
    {
        if (plan_[i].state != ChunkCacheState::kMiss)
        {
            continue;
        }
        (plan_[i].prefetch ? missPrefetch : missDemand).push_back(i);
    }
    // plan_ is built in ascending offset order per region, but different
    // requests may interleave; sort by offset to give coalesceIo monotonic
    // input (it assumes ascending offsets).
    const auto byOffset = [&](size_t a, size_t b) { return plan_[a].offset < plan_[b].offset; };
    std::sort(missPrefetch.begin(), missPrefetch.end(), byOffset);
    std::sort(missDemand.begin(), missDemand.end(), byOffset);
    groupMissChunks(missPrefetch, true);
    groupMissChunks(missDemand, false);

    // R2-4: build one FileCacheCoalescedLoad per miss group (prefetch AND
    // demand). The stable mapping group -> memberChunks[j] -> plan_[idx]
    // .requestIndex -> requests_[k] gives each load its FileCacheLoadRequests
    // (keyed by the stable business requestIndex) and records the per-stream
    // bindings. Prefetch groups are submitted to the executor to run
    // immediately; demand groups stay kPlanned and are triggered by the first
    // Next of a bound stream. A null executor leaves prefetch kPlanned too.
    for (const auto & group : sourceGroups_)
    {
        std::vector<FileCacheLoadRequest> loadRequests;
        loadRequests.reserve(group.memberChunks.size());
        // Per business stream: the stable requestIndices that this load covers
        // for that stream.
        folly::F14FastMap<dwio::common::SeekableInputStream *, std::vector<size_t>> streamRequestIndices;
        for (const size_t chunkIdx : group.memberChunks)
        {
            const PlanChunk & chunk = plan_[chunkIdx];
            const Request & request = requests_[chunk.requestIndex];
            loadRequests.push_back(FileCacheLoadRequest{
                request.requestIndex,
                velox::common::Region{chunk.offset, chunk.length},
                chunk.trackingId,
                {}});
            if (request.stream != nullptr)
                streamRequestIndices[request.stream].push_back(request.requestIndex);
        }

        FileCacheCoalescedLoad::Context context{
            readContext_,
            cache_->getQueryContextHolder(requestContext_.queryId, cacheOptions_)};
        auto load = std::make_shared<FileCacheCoalescedLoad>(
            std::move(context),
            group.offset,
            group.length,
            std::move(loadRequests));
        coalescedLoads_.push_back(load);

        // Record per-stream bindings (dedupe the requestIndices per stream).
        streamToCoalescedLoads_.withWLock(
            [&](auto & loads)
            {
                for (auto & [streamPtr, indices] : streamRequestIndices)
                {
                    std::sort(indices.begin(), indices.end());
                    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
                    loads[streamPtr].push_back(LoadBinding{load, std::move(indices)});
                }
            });

        // Prefetch groups execute immediately on the executor (by-value capture
        // of the shared load keeps its pool alive). Demand groups stay kPlanned.
        if (group.prefetch && executor_ != nullptr)
        {
            executor_->add([load]() { load->loadOrFuture(nullptr); });
        }
    }

    // The enqueued stream owns its own read; the planner only mirrors region
    // values, never a stream pointer.
    requests_.clear();
}

FileCacheBufferedInput::ChunkCacheState
FileCacheBufferedInput::classifyChunk(uint64_t offset, uint64_t length) const
{
    // B3 read-only cache-state probe. We deliberately use FileCache::get, NOT
    // getOrSet: get is side-effect free. It creates no metadata, elects no
    // downloader and reserves no space; missing ranges come back as a detached
    // EMPTY holder that is destroyed with the holder (no persistent change). Any
    // access-time bump inside get is get's own inherent behavior and acceptable.
    // Using getOrSet here would create segments / grab a downloader and pollute
    // the lazy on-demand read path driven later by FileCacheInputStream::Next.
    if (length == 0)
    {
        return ChunkCacheState::kMiss;
    }

    auto holder = cache_->get(
        cacheKey_,
        offset,
        length,
        cacheOptions_.segmentsBatchSize,
        origin_.user_id);

    // No holder / no segments covering the chunk => absent.
    if (!holder || holder->empty())
    {
        return ChunkCacheState::kMiss;
    }

    // Design 6.4: unlike makePreloadedStream (an external-reachable boundary),
    // classifyChunk's offset/length are internal: load splits each enqueued
    // region at loadQuantum with off < regionEnd and
    // chunkLen = min(loadQuantum, regionEnd - off), so
    // off + chunkLen <= regionEnd = region.offset + region.length and the sum
    // cannot overflow here. The DCHECK documents and guards that invariant.
    VELOX_DCHECK_GE(
        std::numeric_limits<uint64_t>::max() - offset,
        length,
        "classifyChunk offset+length overflow (internal invariant violated)");
    const uint64_t chunkEnd = offset + length;

    // §11.4: a single chunk can span MANY smaller FileSegments, so we must
    // examine EVERY overlapping segment, not just front(). get returns segments
    // in ascending, hole-free order covering [offset, offset+length). For each
    // segment we look only at the sub-range it shares with the chunk and decide
    // whether that sub-range is fully resident, in-progress, or fillable-absent.
    // Aggregate with the minimal rule (§11.4 CH state semantics):
    //   any fillable EMPTY/DETACHED sub-range        -> kMiss
    //   PARTIALLY_DOWNLOADED with insufficient prefix -> kMiss (continuable)
    //   else any DOWNLOADING / not-fully-covered     -> kDownloading
    //   else (every overlapping byte downloaded)     -> kHit
    // Marking such a mixed chunk kMiss is acceptable this round: warm execution
    // skips already-downloaded segments and only fills the gaps (§11.4).
    bool anyDownloading = false;
    // 'covered' tracks GEOMETRIC coverage: the largest end offset within the
    // chunk that overlapping segments physically cover so far -- independent of
    // whether those bytes are already resident. A DOWNLOADING segment (a
    // downloader is producing its bytes) covers its whole range even though it is
    // not yet resident. Because segments are hole-free and ascending, a gap
    // (segment.left > covered) means an uncovered range before this segment; a
    // gap past the last segment (covered < chunkEnd at the end) means a
    // fillable-absent tail. Both are treated as kMiss below. Residency vs
    // in-progress is decided separately via anyDownloading.
    uint64_t covered = offset;
    for (auto & segPtr : *holder)
    {
        const FileSegment & segment = *segPtr;
        const FileSegment::Range range = segment.range();
        // The segment's inclusive [left, right] intersected with the chunk's
        // half-open [offset, chunkEnd). segStart/segEnd are the half-open bounds
        // of this segment's sub-range within the chunk.
        const uint64_t segStart = std::max<uint64_t>(range.left, offset);
        const uint64_t segRightExclusive = static_cast<uint64_t>(range.right) + 1;
        const uint64_t segEnd = std::min<uint64_t>(segRightExclusive, chunkEnd);
        if (segStart >= segEnd)
        {
            // No overlap with the chunk (should not happen for a hole-free
            // holder covering the chunk, but be defensive).
            continue;
        }

        // A hole before this segment's sub-range: bytes in [covered, segStart)
        // are not covered by any (resident) segment => fillable-absent.
        if (segStart > covered)
        {
            return ChunkCacheState::kMiss;
        }

        switch (segment.state())
        {
            case FileSegment::State::DOWNLOADED:
            {
                // Fully downloaded segment: its whole inclusive range (right edge
                // + 1) is resident, so it geometrically covers its sub-range up to
                // segEnd. A DOWNLOADED segment's written prefix always spans its
                // full range, so segEnd is always covered here.
                covered = std::max(covered, segEnd);
                break;
            }
            case FileSegment::State::DOWNLOADING:
            {
                // A downloader is actively producing this segment's bytes: the
                // whole sub-range is geometrically covered (someone will fill it),
                // even though it is not yet resident. Advance 'covered' so a
                // DOWNLOADING segment fully covering the chunk classifies as
                // kDownloading, not a spurious kMiss (§11.4 reviewer fix).
                anyDownloading = true;
                covered = std::max(covered, segEnd);
                break;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                // A committed-but-incomplete segment whose remainder can be
                // continued by another owner. Only its written prefix
                // (getCurrentWriteOffset) is resident; if that prefix does not
                // cover this sub-range, the remainder is fillable-absent and the
                // chunk is a miss that warm execution can continue-fill (§11.4).
                const uint64_t writeOffset = segment.getCurrentWriteOffset();
                if (writeOffset >= segEnd)
                {
                    covered = std::max(covered, segEnd);
                    break;
                }
                return ChunkCacheState::kMiss;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                // Like PARTIALLY_DOWNLOADED but the tail can NOT be continued: the
                // written prefix is read from cache and the remainder is served by
                // bypass (direct source read), never re-elected for download. Its
                // sub-range is geometrically covered (bypass will serve the tail),
                // so advance 'covered' and mark in-progress but DO NOT treat it as
                // a fillable miss / warm candidate (§11.4).
                const uint64_t writeOffset = segment.getCurrentWriteOffset();
                if (writeOffset >= segEnd)
                {
                    covered = std::max(covered, segEnd);
                }
                else
                {
                    anyDownloading = true;
                    covered = std::max(covered, segEnd);
                }
                break;
            }
            case FileSegment::State::EMPTY:
            case FileSegment::State::DETACHED:
            {
                // A fillable-absent sub-range: the whole chunk is a miss.
                return ChunkCacheState::kMiss;
            }
        }
    }

    // A gap past the last segment's right edge: the tail [covered, chunkEnd) is
    // uncovered => fillable-absent.
    if (covered < chunkEnd)
    {
        return ChunkCacheState::kMiss;
    }

    return anyDownloading ? ChunkCacheState::kDownloading : ChunkCacheState::kHit;
}

bool FileCacheBufferedInput::classifyPrefetch(velox::cache::TrackingId trackingId) const
{
    // Mirror DirectBufferedInput::load: an empty trackingId or the special
    // sequentialFile id is always prefetched. Otherwise consult the tracker's
    // adjusted read percentage. With no tracker there is no history, so a
    // non-"prefetch anyway" chunk stays demand (conservative: avoids
    // speculative overread when the access pattern is unknown).
    const bool prefetchAnyway = trackingId.empty()
        || trackingId.id() == dwio::common::StreamIdentifier::sequentialFile().getId();
    if (prefetchAnyway)
    {
        return true;
    }
    if (!tracker_)
    {
        return false;
    }
    const velox::cache::TrackingData trackingData = tracker_->trackingData(trackingId);
    return isPrefetchablePct(adjustedReadPct(trackingData));
}

void FileCacheBufferedInput::groupMissChunks(
    const std::vector<size_t> & chunkIndices, bool prefetch)
{
    // A single demand chunk has nothing to coalesce with; a single prefetch
    // chunk is still eligible to be issued on its own. Mirrors
    // DirectBufferedInput::groupRequests.
    if (chunkIndices.empty() || (chunkIndices.size() < 2 && !prefetch))
    {
        return;
    }

    const int32_t maxDistance = readerOptions_.maxCoalesceDistance();
    const uint64_t loadQuantum = static_cast<uint64_t>(readerOptions_.loadQuantum());
    // Dense (prefetch) access coalesces up to maxCoalesceBytes for throughput;
    // sparse (demand) access coalesces only to loadQuantum to limit overread.
    const int64_t maxCoalesceBytes
        = prefetch ? readerOptions_.maxCoalesceBytes() : static_cast<int64_t>(loadQuantum);

    int64_t coalescedBytes = 0;
    velox::coalesceIo<size_t, char>(
        chunkIndices,
        maxDistance,
        std::numeric_limits<int32_t>::max(), // limit by bytes, not count
        [&](int32_t i) { return plan_[chunkIndices[i]].offset; },
        [&](int32_t i) -> int32_t
        {
            const uint64_t size = plan_[chunkIndices[i]].length;
            if (size > loadQuantum)
            {
                coalescedBytes += loadQuantum;
                return static_cast<int32_t>(loadQuantum);
            }
            coalescedBytes += static_cast<int64_t>(size);
            return static_cast<int32_t>(size);
        },
        [&](int32_t /*i*/)
        {
            if (coalescedBytes > maxCoalesceBytes)
            {
                coalescedBytes = 0;
                return kNoCoalesce;
            }
            return 1;
        },
        [&](size_t /*item*/, std::vector<char> & ranges)
        {
            // ranges.size() participates in coalesceIo bookkeeping, so it must
            // not stay empty.
            ranges.push_back(0);
        },
        [&](int32_t /*gap*/, std::vector<char> /*ranges*/) { /* no op */ },
        [&](const std::vector<size_t> & items,
            int32_t begin,
            int32_t end,
            uint64_t offset,
            const std::vector<char> & /*ranges*/)
        {
            // Record the group boundary only -- no IO. Byte extent spans from
            // the first chunk's offset to the last chunk's end. The `ranges`
            // list keeps every requested chunk's exact {offset, length} so warm
            // can write only those bytes (design 5.7); the box may span gaps.
            const PlanChunk & lastChunk = plan_[items[end - 1]];
            const uint64_t coveredEnd = lastChunk.offset + lastChunk.length;
            std::vector<std::pair<uint64_t, uint64_t>> ranges;
            ranges.reserve(static_cast<size_t>(end - begin));
            // R2-4: record the exact plan_ index members of this group (the
            // discrete items[begin..end) after the offset sort), so the stable
            // group -> chunk -> business request/stream mapping never has to
            // reverse-infer membership from the [begin,end) bounding box.
            std::vector<size_t> memberChunks;
            memberChunks.reserve(static_cast<size_t>(end - begin));
            for (int32_t i = begin; i < end; ++i)
            {
                const PlanChunk & chunk = plan_[items[i]];
                ranges.emplace_back(chunk.offset, chunk.length);
                memberChunks.push_back(items[i]);
            }
            sourceGroups_.push_back(CoalescedGroup{
                offset,
                coveredEnd - offset,
                prefetch,
                std::move(ranges),
                std::move(memberChunks)});
        });
}

std::vector<FileCacheBufferedInput::LoadBinding>
FileCacheBufferedInput::coalescedLoads(const dwio::common::SeekableInputStream * stream)
{
    // Move out and erase this stream's bindings (mirrors
    // DirectBufferedInput::coalescedLoad move+erase). A stream with no group
    // returns an empty vector and the caller falls back to the plain demand path.
    return streamToCoalescedLoads_.withWLock(
        [&](auto & loads) -> std::vector<LoadBinding>
        {
            auto it = loads.find(stream);
            if (it == loads.end())
                return {};
            auto bindings = std::move(it->second);
            loads.erase(it);
            return bindings;
        });
}

std::unique_ptr<dwio::common::SeekableInputStream> FileCacheBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    dwio::common::LogType logType) const
{
    // A4: after whole-file preload, serve directly from RAM.
    if (preloaded())
    {
        return makePreloadedStream(offset, length);
    }

    // Unplanned reads still go through the FileCache state machine; do not fall
    // back to a raw SeekableFileInputStream (that bypasses the cache).
    return std::make_unique<FileCacheInputStream>(
        const_cast<FileCacheBufferedInput *>(this),
        readContext_,
        velox::common::Region{offset, length},
        logType);
}

bool FileCacheBufferedInput::isBuffered(uint64_t /*offset*/, uint64_t /*length*/) const
{
    // Aligned with DirectBufferedInput: isBuffered reports only whole-file
    // in-memory preload, NOT a persistent on-disk FileSegment hit. A disk cache
    // hit still goes through the normal clone/enqueue/load path (the reader does
    // not skip it), so returning true here for a disk hit would wrongly tell the
    // upper layer the bytes are already resident in memory.
    return preloaded();
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
        queryStatus_,
        dwio::common::MetricsLog::voidLog(),
        fileNum_,
        groupId_,
        tracker_,
        ioStatistics_,
        ioStats_,
        executor_,
        readerOptions_);
}

void FileCacheBufferedInput::reset()
{
    // Base clears regions_/offsets_/buffers_/allocPool_. We additionally drop the
    // planner request list. No coalesced loads exist yet (planning lands in a
    // later stage), and persistent FileSegments are never touched here.
    dwio::common::BufferedInput::reset();
    // R2-4: cancel any planned/running coalesced loads and drop the bindings
    // (mirrors DirectBufferedInput::reset). A running load's shared context keeps
    // it alive to completion; cancel only flips a planned load to kCancelled.
    for (auto & load : coalescedLoads_)
    {
        load->cancel();
    }
    coalescedLoads_.clear();
    streamToCoalescedLoads_.wlock()->clear();
    requests_.clear();
    plan_.clear();
    sourceGroups_.clear();
}

void FileCacheBufferedInput::preload()
{
    VELOX_CHECK(!preloadData_.has_value(), "preload() called more than once");
    VELOX_CHECK(requests_.empty(), "preload() must be called before enqueue()");

    // Fail fast on an oversized file. preload() reads the WHOLE file into RAM;
    // without this bound a large file would trigger an unbounded whole-file
    // allocation. We never silently skip: an oversized preload is a caller error
    // (shouldPreload() is false, so preload() is only reached by an explicit
    // caller decision).
    VELOX_CHECK_LE(
        fileSize_,
        readerOptions_.filePreloadThreshold(),
        "preload() file size {} exceeds filePreloadThreshold {}",
        fileSize_,
        readerOptions_.filePreloadThreshold());

    // Read into a LOCAL PreloadData first. Only after every source read succeeds
    // do we commit it to preloadData_, so a mid-read exception leaves preloaded()
    // returning false (no "half preload" observed as success).
    PreloadData localData;
    localData.size = fileSize_;

    // Time the whole source-read phase (Design 7.6). The source read is a real
    // storage-traffic event: it must be accounted the instant it completes,
    // BEFORE fillFileSegmentsFromPreload. Otherwise a strict on-disk write
    // failure inside the fill (skipCacheOnDiskFailure=false) throws out of
    // preload() and the already-executed source read is reported as zero.
    uint64_t storageReadUs{0};
    {
        velox::MicrosecondWallTimer timer(&storageReadUs);
        if (fileSize_ <= static_cast<uint64_t>(dwio::common::DirectBufferedInput::kTinySize))
        {
            localData.tinyData.resize(fileSize_);
            if (fileSize_ > 0)
            {
                getInputStream()->read(
                    localData.tinyData.data(), fileSize_, 0, dwio::common::LogType::FILE);
            }
        }
        else
        {
            // Large file: non-contiguous MemoryPool-backed allocation. pread each
            // run separately; the source file offset for run i is the sum of the
            // byte lengths of all preceding runs.
            const auto numPages = velox::memory::AllocationTraits::numPages(fileSize_);
            memoryPool()->allocateNonContiguous(numPages, localData.data);

            const auto & allocation = localData.data;
            uint64_t fileOffset = 0;
            for (int32_t i = 0; i < allocation.numRuns(); ++i)
            {
                auto run = allocation.runAt(i);
                const uint64_t runBytes = velox::memory::AllocationTraits::pageBytes(run.numPages());
                const uint64_t readSize = std::min(runBytes, fileSize_ - fileOffset);
                getInputStream()->read(
                    run.data<char>(), readSize, fileOffset, dwio::common::LogType::FILE);
                fileOffset += readSize;
                if (fileOffset >= fileSize_)
                {
                    break;
                }
            }
        }
    }

    // The source read fully succeeded: account for it NOW, before the fill.
    // Whether the subsequent fill succeeds (bypass) or throws (strict) does not
    // change the fact that these bytes were read from source. Mirrors
    // DirectBufferedInput::preload for the IoStatistics side and the demand path
    // (warm) for the ProfileEvents side.
    if (ioStatistics_)
    {
        ioStatistics_->read().increment(fileSize_);
        ioStatistics_->queryThreadIoLatencyUs().increment(storageReadUs);
        ioStatistics_->storageReadLatencyUs().increment(storageReadUs);
    }
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, fileSize_);

    // A4b: best-effort fill of the FileCache with the resident bytes, using the
    // SAME source read that just populated RAM (no second read from source).
    // Bypass-eligible fill failures (reserve failure, or a disk write failure
    // when skipCacheOnDiskFailure=true) are absorbed inside the fill and leave
    // the RAM preload committable. But a strict disk write failure
    // (skipCacheOnDiskFailure=false) or any other exception propagates out of
    // here, so preloadData_ below is NOT committed and preloaded() stays false
    // (Design 6.7).
    fillFileSegmentsFromPreload(localData);

    // RAM read fully succeeded: commit. Only now does preloaded() report true.
    preloadData_ = std::move(localData);
}

void FileCacheBufferedInput::fillFileSegmentsFromPreload(const PreloadData & localData)
{
    if (fileSize_ == 0)
    {
        return;
    }

    // Design 5.8: the resident preload bytes already live in a MemoryPool-backed
    // buffer -- `tinyData` (a std::string) for tiny files, or the non-contiguous
    // `Allocation` runs for larger ones. Each run's PageRun is physically
    // contiguous, so we hand the run pointer straight to reserveAndWriteSegmentChunk
    // and split writes at run boundaries (FileSegment supports many sequential
    // writes). No intermediate scratch copy and no extra un-metered allocation.
    //
    // Returns a pointer to the contiguous resident block containing `offset` and,
    // via `avail`, how many bytes are contiguously available from there.
    // reserveAndWriteSegmentChunk takes `char*` (it only reads `data`, never writes
    // it); the resident bytes are logically const here, so const_cast the run /
    // tinyData pointer to match the signature. This does not mutate the preload.
    const auto residentAt = [&](uint64_t offset, uint64_t & avail) -> char *
    {
        if (localData.data.numPages() == 0)
        {
            avail = localData.tinyData.size() - offset;
            return const_cast<char *>(localData.tinyData.data()) + offset;
        }
        int32_t runIndex = 0;
        int32_t offsetInRun = 0;
        localData.data.findRun(offset, &runIndex, &offsetInRun);
        const auto run = localData.data.runAt(runIndex);
        avail = velox::memory::AllocationTraits::pageBytes(run.numPages())
            - static_cast<uint64_t>(offsetInRun);
        // run.data<char>() already yields a char*; keep it explicit for clarity.
        return const_cast<char *>(run.data<char>()) + offsetInRun;
    };

    // Design 6.7: NO broad catch here. reserveAndWriteSegmentChunk already turns
    // every bypass-eligible failure into `return false` (reserve failure, or a
    // disk write failure when skipCacheOnDiskFailure=true) -- those `break` out
    // of the segment and leave the RAM preload intact. A strict disk write
    // failure (skipCacheOnDiskFailure=false), or any non-disk logic exception
    // (e.g. getOrSet), must propagate: it unwinds out of preload() so
    // preloadData_ is never committed and preloaded() stays false. Each elected
    // segment's SCOPE_EXIT releases its downloader lease during unwinding.
    {
        // Hold our own QueryContextHolder so the per-query download limit
        // (maxDownloadSizePerQuery) is enforced during the fill: reserves inside
        // reserveAndWriteSegmentChunk look the query account up by thread-local id,
        // which is only pinned while some holder references it. Empty when the
        // limit is disabled -- harmless.
        FileCache::QueryContextHolderPtr holder =
            cache_->getQueryContextHolder(requestContext_.queryId, cacheOptions_);

        // Stabilise the caller identity so getOrSetDownloader / reserve / write /
        // completePart all agree on "<queryId>:<os-tid>".
        FileCacheQueryIdScope scope(requestContext_.queryId);

        CreateFileSegmentSettings createSettings(FileSegmentKind::Regular);
        std::optional<size_t> alignment;
        if (cacheOptions_.boundaryAlignment.has_value())
        {
            alignment = cacheOptions_.boundaryAlignment.value();
        }

        auto segments = cache_->getOrSet(
            cacheKey_,
            0,
            fileSize_,
            fileSize_,
            createSettings,
            cacheOptions_.segmentsBatchSize,
            origin_,
            alignment);
        if (!segments)
        {
            return;
        }

        const bool skipOnDiskFailure = cache_->skipCacheOnDiskFailure();

        for (auto & segPtr : *segments)
        {
            FileSegment & seg = *segPtr;
            const auto state = seg.state();
            if (state != FileSegment::State::EMPTY
                && state != FileSegment::State::PARTIALLY_DOWNLOADED)
            {
                continue;
            }

            // Election: skip a segment owned by someone else; we hold no lease.
            const auto elected = seg.getOrSetDownloader();
            if (elected != FileSegment::getCallerId())
            {
                continue;
            }

            // Release the elected lease on every exit path.
            SCOPE_EXIT
            {
                seg.resetRemoteFileReader();
                seg.completePartAndResetDownloader();
            };

            const auto range = seg.range();
            const uint64_t segEndExclusive = std::min<uint64_t>(range.right + 1, fileSize_);

            while (true)
            {
                const uint64_t writeOffset = seg.getCurrentWriteOffset();
                if (writeOffset >= segEndExclusive)
                {
                    break;
                }
                const uint64_t remaining = segEndExclusive - writeOffset;

                // Contiguous resident block at writeOffset. chunkSize is bounded
                // by that block, the segment tail, and kDefaultBufferSize; a run
                // boundary simply ends this chunk and the next iteration resumes
                // in the following run. Pass the run pointer directly -- no copy.
                uint64_t availContig = 0;
                char * const src = residentAt(writeOffset, availContig);
                const size_t chunkSize = static_cast<size_t>(std::min<uint64_t>(
                    {remaining, availContig, ReadBufferFromVeloxReadFile::kDefaultBufferSize}));

                if (!reserveAndWriteSegmentChunk(
                        seg,
                        src,
                        chunkSize,
                        writeOffset,
                        cacheOptions_.reserveSpaceWaitLockTimeoutMs,
                        /*reserveHint=*/remaining,
                        skipOnDiskFailure))
                {
                    // Reserve / on-disk write failed: stop filling THIS segment.
                    // The RAM preload is unaffected.
                    break;
                }
            }
        }
    }
}

bool FileCacheBufferedInput::addressInPreloadData(const void * ptr) const
{
    if (!preloadData_.has_value())
    {
        return false;
    }
    const auto * p = static_cast<const char *>(ptr);
    const auto & pd = *preloadData_;
    if (pd.data.numPages() == 0)
    {
        const char * base = pd.tinyData.data();
        return p >= base && p < base + pd.tinyData.size();
    }
    for (int32_t i = 0; i < pd.data.numRuns(); ++i)
    {
        const auto run = pd.data.runAt(i);
        const char * base = run.data<const char>();
        const uint64_t runBytes = velox::memory::AllocationTraits::pageBytes(run.numPages());
        if (p >= base && p < base + runBytes)
        {
            return true;
        }
    }
    return false;
}

folly::Range<const char *>
FileCacheBufferedInput::preloadedData(uint64_t offset, uint64_t length) const
{
    VELOX_CHECK(preloadData_.has_value(), "preloadedData() called without preload");
    // Overflow-safe range check (see makePreloadedStream for the rationale).
    VELOX_CHECK_LE(offset, preloadData_->size, "Preloaded read offset out of range");
    VELOX_CHECK_LE(
        length, preloadData_->size - offset, "Preloaded read length out of range");
    if (length == 0)
        return folly::Range<const char *>(static_cast<const char *>(nullptr), size_t{0});

    const auto & pd = *preloadData_;
    if (pd.data.numPages() == 0)
    {
        // Tiny (contiguous) file: the whole remainder is one run; return up to
        // `length` bytes starting at `offset`.
        const char * base = pd.tinyData.data() + offset;
        return folly::Range<const char *>(base, length);
    }

    // Non-contiguous Allocation: locate the run holding `offset` and return the
    // contiguous slice from `offset` to the end of that run, capped by `length`.
    // The caller loops across run boundaries using the same findRun/runAt
    // traversal as the preload storage.
    int32_t runIndex = 0;
    int32_t offsetInRun = 0;
    pd.data.findRun(offset, &runIndex, &offsetInRun);
    const auto run = pd.data.runAt(runIndex);
    const uint64_t runBytes = velox::memory::AllocationTraits::pageBytes(run.numPages());
    const uint64_t contiguous =
        std::min<uint64_t>(length, runBytes - static_cast<uint64_t>(offsetInRun));
    const char * base = run.data<const char>() + offsetInRun;
    return folly::Range<const char *>(base, contiguous);
}

std::unique_ptr<dwio::common::SeekableInputStream>
FileCacheBufferedInput::makePreloadedStream(
    uint64_t offset, uint64_t length, velox::cache::TrackingId trackingId) const
{
    VELOX_CHECK(preloadData_.has_value(), "makePreloadedStream() called without preload");
    // Overflow-safe range check. Do NOT compute `offset + length` first: an
    // offset near UINT64_MAX makes the sum wrap to a small value that passes a
    // naive `offset + length <= size` check, after which the real huge offset is
    // used for pointer arithmetic / Allocation::findRun and reads out of bounds.
    // Check `offset <= size` first (so `size - offset` does not underflow), then
    // `length <= size - offset`.
    VELOX_CHECK_LE(offset, preloadData_->size, "Preloaded read offset out of range");
    VELOX_CHECK_LE(
        length, preloadData_->size - offset, "Preloaded read length out of range");

    // C6: build a business-role FileCacheInputStream. Its Next serves zero-copy
    // slices out of preloadData_ (via preloadedData) whenever bufferedInput_ is
    // preloaded(), never touching the FileSegment state machine.
    return std::make_unique<FileCacheInputStream>(
        const_cast<FileCacheBufferedInput *>(this),
        readContext_,
        velox::common::Region{offset, length},
        dwio::common::LogType::STREAM,
        trackingId);
}

} // namespace facebook::velox::ch
