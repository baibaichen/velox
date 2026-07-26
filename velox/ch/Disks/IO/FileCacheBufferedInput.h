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
#include "velox/ch/Common/QueryStatus.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"

#include <folly/Range.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <memory>
#include <vector>

namespace facebook::velox::ch
{

class FileCacheInputStream;
struct FileCacheReadContext;
class FileCacheCoalescedLoad;

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
        QueryStatus queryStatus,
        const dwio::common::MetricsLogPtr & metricsLog,
        velox::StringIdLease fileNum,
        velox::StringIdLease groupId,
        std::shared_ptr<velox::cache::ScanTracker> tracker,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        std::shared_ptr<velox::IoStats> ioStats,
        folly::Executor * executor,
        const dwio::common::ReaderOptions & readerOptions,
        folly::F14FastMap<std::string, std::string> fileReadOps = {});

    // R2-4: cancel any planned coalesced loads still owned here (mirrors
    // DirectBufferedInput's destructor). A running load stays alive via its
    // shared context and completes; a planned one flips to kCancelled.
    ~FileCacheBufferedInput() override;

    // BufferedInput overrides.
    std::unique_ptr<dwio::common::SeekableInputStream> enqueue(
        velox::common::Region region,
        const dwio::common::StreamIdentifier * sid = nullptr) override;

    void load(dwio::common::LogType logType) override;

    std::unique_ptr<dwio::common::SeekableInputStream>
    read(uint64_t offset, uint64_t length, dwio::common::LogType logType)
        const override;

    bool isBuffered(uint64_t offset, uint64_t length) const override;

    // A4: whole-file synchronous in-memory preload. Reads the entire source
    // file into MemoryPool-backed memory. Must be called exactly once, before
    // any enqueue()/read(). This stage does NOT populate any FileSegment (that
    // is A4b); it only serves subsequent reads from RAM, bypassing the
    // FileCache state machine. Mirrors DirectBufferedInput::preload.
    void preload() override;
    bool preloaded() const override { return preloadData_.has_value(); }
    bool shouldPreload(int32_t numPages = 0) override
    {
        (void)numPages;
        return false;
    }

    // Must return false: prevents DWRF StripeMetadataCache from hard-casting
    // FileCacheInputStream to CacheInputStream.
    bool shouldPrefetchStripes() const override { return false; }

    std::unique_ptr<dwio::common::BufferedInput> clone() const override;

    // Cancels not-yet-started planning state and releases the planner's request
    // list, without deleting any persistent FileSegment. Mirrors
    // DirectBufferedInput::reset (base reset + clear own requests_).
    void reset() override;

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
    // Base ReadFileInputStream (built by BufferedInput with the populated
    // FileIoContext = ioStats + fileReadOps + cacheable). Source reads route
    // through this so ReadFile::pread receives the real context.
    const std::shared_ptr<dwio::common::ReadFileInputStream> & sourceInputStream() const
    {
        return getInputStream();
    }
    const FileCacheKey & cacheKey() const { return cacheKey_; }
    const FileCacheOriginInfo & origin() const { return origin_; }
    const FileCacheReadOptions & cacheOptions() const { return cacheOptions_; }
    velox::memory::MemoryPool * memoryPool() const { return &readerOptions_.memoryPool(); }
    uint64_t fileSize() const { return fileSize_; }
    // Per-split IoStatistics from the connector (may be null). Operator-level
    // hit/miss byte attribution is recorded here so it reaches OperatorStats.
    io::IoStatistics * ioStatistics() const { return ioStatistics_.get(); }

    // A1: upstream context held for later planning/prefetch stages (stored, not
    // yet used to drive logic).
    const std::shared_ptr<velox::cache::ScanTracker> & tracker() const { return tracker_; }
    const velox::StringIdLease & fileNum() const { return fileNum_; }
    const velox::StringIdLease & groupId() const { return groupId_; }

    // B1: record actually-delivered bytes on the ScanTracker (no-op if no
    // tracker or an empty tracking id). Called by FileCacheInputStream on the
    // demand read path only.
    void recordReadBytes(velox::cache::TrackingId trackingId, uint64_t bytes)
    {
        if (tracker_ && !trackingId.empty())
        {
            tracker_->recordRead(trackingId, bytes, fileNum_.id(), groupId_.id());
        }
    }

    // C6 (design 11.6): zero-copy accessor into the whole-file preload buffer.
    // Requires preloaded(). Returns a contiguous slice starting at absolute file
    // offset `offset`, spanning at most `length` bytes but never crossing a
    // preload-storage run boundary (tinyData is one run; a non-contiguous
    // Allocation is one run per allocation run). The returned Range therefore may
    // be shorter than `length`; callers loop, advancing `offset` by the slice
    // length, until they have consumed `length`. The referenced memory is owned by
    // this FileCacheBufferedInput (preloadData_) and outlives every stream, so the
    // returned pointer is non-owning. The zero-copy RAM slice semantics live in
    // FileCacheInputStream::Next, which
    // must never fall back to a FileSegment disk read while preloaded().
    folly::Range<const char *> preloadedData(uint64_t offset, uint64_t length) const;

    // Number of enqueued-but-not-yet-loaded planner requests (test observability
    // for reset/clone lifecycle).
    size_t numRequests() const { return requests_.size(); }

    // R2-4: binding of a business stream to one coalesced load and the stable
    // business request indices within it that belong to that stream. A single
    // stream may map to several bindings (its region spans multiple groups), so
    // the map value is a vector. Mirrors DirectBufferedInput's
    // stream->coalescedLoad relation, generalised to the multi-group case.
    struct LoadBinding
    {
        std::shared_ptr<FileCacheCoalescedLoad> load;
        std::vector<size_t> requestIndices;
    };

    // R2-4: move out and erase all bindings recorded for `stream`. Called by the
    // business FileCacheInputStream on its first Next to obtain its coalesced
    // loads (mirrors DirectBufferedInput::coalescedLoad move+erase). Returns an
    // empty vector when the stream has no bindings (standalone / demand-only with
    // no group).
    std::vector<LoadBinding> coalescedLoads(const dwio::common::SeekableInputStream * stream);

    // B2: read-planning chunk. Each enqueued region is split at loadQuantum into
    // chunks; the planner classifies and coalesces these in later stages. B2
    // fills only the geometry + trackingId; state (B3) and prefetch/demand (B4)
    // are placeholders here.
    enum class ChunkCacheState : uint8_t
    {
        kUnknown, // B3 not yet run
        kHit, // fully downloaded
        kMiss, // absent / detached
        kDownloading, // being written by some downloader
    };
    struct PlanChunk
    {
        uint64_t offset = 0; // absolute file offset
        uint64_t length = 0;
        velox::cache::TrackingId trackingId;
        ChunkCacheState state = ChunkCacheState::kUnknown;
        bool prefetch = false; // B4: prefetch vs demand
        // R2-4: stable owner index into requests_ (the business request this
        // chunk was split from). Internal planning carrier only; never leaks into
        // any public FileCacheCoalescedLoad signature. Used to build the stable
        // group -> chunk -> business request/stream mapping.
        size_t requestIndex = 0;
    };

    // B4: a candidate coalesced source group produced by grouping adjacent
    // kMiss chunks (prefetch and demand grouped separately). This is a pure
    // planning artifact: it records the byte extent of the grouped chunks and
    // performs NO IO. Execution (getOrSet / pread / writeCache / executor
    // submission) is stage B5.
    struct CoalescedGroup
    {
        uint64_t offset = 0; // absolute file offset of the first chunk
        uint64_t length = 0; // covered byte extent (last chunk end - offset)
        bool prefetch = false; // group is prefetch (true) or demand (false)
        // Design 5.7: the exact requested chunk ranges {absolute offset, length}
        // that this group covers, in ascending offset order. The bounding box
        // [offset, offset+length) may span unrequested gaps BETWEEN these ranges;
        // the coalesced load caches ONLY these ranges (plus the sequential-write
        // prefix each segment needs) into FileSegments -- gap bytes are read for
        // one coalesced source IO but never cached.
        std::vector<std::pair<uint64_t, uint64_t>> ranges;
        // R2-4: exact plan_ index set of the members of this group (the discrete
        // items[begin..end) selected by coalesceIo after the offset sort), NOT a
        // contiguous plan_ range -- the bounding byte extent above may include
        // chunks of other requests/groups. All group -> business stream/request
        // mapping walks these indices.
        std::vector<size_t> memberChunks;
    };

    // Test observability: true if `ptr` points inside the preloaded data buffer
    // (tinyData or one of the non-contiguous allocation runs). Used to prove
    // makePreloadedStream returns zero-copy slices into the preload buffer rather
    // than freshly allocated per-stream copies.
    bool addressInPreloadData(const void * ptr) const;

    // Test observability for the planning stages (B2-B4).
    size_t numPlanChunks() const { return plan_.size(); }
    const PlanChunk & planChunkAt(size_t i) const { return plan_.at(i); }
    size_t numSourceGroups() const { return sourceGroups_.size(); }
    const CoalescedGroup & sourceGroupAt(size_t i) const { return sourceGroups_.at(i); }

private:
    struct Request
    {
        velox::common::Region region;
        // Tracking id copied by value at enqueue() time. We must NOT retain the
        // StreamIdentifier* itself: in production (Parquet reader) the sid is a
        // stack temporary destroyed once enqueue() returns, so a later load()
        // that dereferenced it would be a use-after-free. Mirrors
        // DirectBufferedInput/CachedBufferedInput, which store the TrackingId.
        velox::cache::TrackingId trackingId;
        // R2-4: the business stream this request produced, used ONLY as a stable
        // map key for streamToCoalescedLoads_ (never dereferenced -- the stream
        // may be destroyed before its bindings are looked up). Filled at enqueue
        // after the stream is constructed.
        dwio::common::SeekableInputStream * stream = nullptr;
        // R2-4: stable business request id, stable across the whole input (not a
        // 0..N-1 counter inside a load). Used as the FileCacheLoadRequest key.
        size_t requestIndex = 0;
    };

    // A4: whole-file preload buffer. Exactly one of 'tinyData' / 'data' is
    // populated: 'tinyData' when fileSize_ <= DirectBufferedInput::kTinySize,
    // 'data' (non-contiguous allocation) otherwise. 'size' is the file size.
    struct PreloadData
    {
        velox::memory::Allocation data;
        std::string tinyData;
        uint64_t size{0};
    };

    // C6: serve an enqueue()/read() from the whole-file RAM preload. Requires
    // preloaded(). Returns a business-role FileCacheInputStream whose Next serves
    // zero-copy slices out of preloadData_ (via preloadedData) and never touches
    // the FileSegment state machine. No per-stream copy of the region is made.
    std::unique_ptr<dwio::common::SeekableInputStream>
    makePreloadedStream(
        uint64_t offset,
        uint64_t length,
        velox::cache::TrackingId trackingId = {}) const;

    // A4b: best-effort fill of the FileCache with the bytes preload() just read
    // into RAM. For each EMPTY / PARTIALLY_DOWNLOADED segment covering
    // [0, fileSize_) the caller elects itself downloader and writes the resident
    // bytes via reserveAndWriteSegmentChunk. Bound by QueryLimit (a
    // QueryContextHolder + FileCacheQueryIdScope), disk-failure skipping and
    // downloader-lease atomicity (SCOPE_EXIT). Any failure here is swallowed: it
    // must not break the RAM preload. `data` points at the resident whole-file
    // bytes accessor.
    void fillFileSegmentsFromPreload(const PreloadData & localData);

    // B3: read-only cache-state probe for a single planning chunk. Uses
    // FileCache::get (never getOrSet) so it creates no segment, elects no
    // downloader and reserves no space; the probe is side-effect free on the
    // FileCache's persistent state. See load() for the classification rules.
    ChunkCacheState classifyChunk(uint64_t offset, uint64_t length) const;

    // B4: classify a chunk as prefetch (true) vs demand (false), mirroring
    // DirectBufferedInput::load. A chunk with an empty trackingId or the
    // sequentialFile id is always prefetch; otherwise, with a tracker, the
    // adjusted read percentage decides. With no tracker (no history) and no
    // "prefetch anyway" reason, we conservatively return demand.
    bool classifyPrefetch(velox::cache::TrackingId trackingId) const;

    // B4: group adjacent kMiss chunks (from indices in 'chunkIndices', already
    // sorted by offset) into candidate CoalescedGroups using coalesceIo. Pure
    // planning: ioFunc only records group boundaries, no IO is performed.
    void groupMissChunks(const std::vector<size_t> & chunkIndices, bool prefetch);

    // Assemble the immutable per-file FileCacheReadContext shared with each
    // FileCacheInputStream this input creates. Called exactly once from the
    // constructor body to populate readContext_; every stream/load created by
    // this input reuses that single cached instance instead of building one
    // per enqueue.
    std::shared_ptr<const FileCacheReadContext> makeReadContext() const;

    std::shared_ptr<ReadFile> sourceReadFile_;
    FileCachePtr cache_;
    FileCacheKey cacheKey_;
    FileCacheOriginInfo origin_;
    FileCacheReadOptions cacheOptions_;
    FileCacheRequestContext requestContext_;
    QueryStatus queryStatus_;
    velox::StringIdLease fileNum_;
    velox::StringIdLease groupId_;
    std::shared_ptr<velox::cache::ScanTracker> tracker_;
    std::shared_ptr<io::IoStatistics> ioStatistics_;
    std::shared_ptr<velox::IoStats> ioStats_;
    folly::Executor * executor_;

    dwio::common::ReaderOptions readerOptions_;
    uint64_t fileSize_;

    // R2-4: the immutable per-file read context shared by every
    // FileCacheInputStream/FileCacheCoalescedLoad this input creates. Built
    // once (via makeReadContext) in the constructor body after validation, so
    // enqueue/load/read/makePreloadedStream all reuse the same instance rather
    // than allocating an equivalent one each time. Declared after the
    // immutable source/options/stat members it is built from and before the
    // mutable request state below. Reverse member destruction order therefore
    // tears down request/load state first, then the cached context, then the
    // input's backing owners.
    std::shared_ptr<const FileCacheReadContext> readContext_;

    // Copied region values only; never stream pointers.
    std::vector<Request> requests_;

    // B2: read-planning chunks built by load(). Not consumed by Next() in the
    // B1-B4 stages (stream still reads lazily); becomes the prefetch/demand
    // execution plan in stage B5.
    std::vector<PlanChunk> plan_;

    // B4: candidate coalesced source groups over the kMiss chunks of plan_
    // (prefetch and demand grouped separately). Pure planning artifact; not
    // consumed by Next() and never triggers IO (execution is stage B5).
    std::vector<CoalescedGroup> sourceGroups_;

    // R2-4: coalesced loads built by load() for the miss groups (prefetch AND
    // demand). Owned here so reset()/destruction can cancel planned loads.
    // Mirrors DirectBufferedInput::coalescedLoads_.
    std::vector<std::shared_ptr<FileCacheCoalescedLoad>> coalescedLoads_;

    // R2-4: per-business-stream bindings to the coalesced loads that cover it.
    // Keyed by the stream pointer (never dereferenced -- only a stable key).
    // Mirrors DirectBufferedInput::streamToCoalescedLoad_, generalised to a
    // vector value because one stream may span several groups.
    folly::Synchronized<
        folly::F14FastMap<const dwio::common::SeekableInputStream *, std::vector<LoadBinding>>>
        streamToCoalescedLoads_;

    // A4: populated by preload(); std::nullopt until then.
    std::optional<PreloadData> preloadData_;
};

} // namespace facebook::velox::ch
