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

#include "velox/ch/Common/QueryStatus.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/QueryLimit.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <memory>
#include <optional>

namespace facebook::velox::ch
{

class FileCacheBufferedInput;

/// Immutable per-file read context shared between a `FileCacheBufferedInput`,
/// the `FileCacheInputStream`s it creates, and the `FileCacheCoalescedLoad`s
/// that execute their group reads. Produced by the buffered input and consumed
/// by streams/loads; a stream/load never holds a raw pointer back to the input.
///
/// Member declaration order == reverse destruction order and is load-bearing
/// for the UAF contract: `source` (which holds raw `IoStats`/`IoStatistics`
/// pointers) is declared AFTER `ioStats`/`ioStatistics`, so it is destroyed
/// first; `cache` is declared first and destroyed last.
struct FileCacheReadContext
{
    FileCachePtr cache;
    std::shared_ptr<io::IoStatistics> ioStatistics;
    std::shared_ptr<velox::IoStats> ioStats;
    std::shared_ptr<dwio::common::ReadFileInputStream> source;
    std::shared_ptr<velox::memory::MemoryPool> pool;
    FileCacheKey key;
    FileCacheOriginInfo origin;
    FileCacheReadOptions cacheOptions;
    FileCacheRequestContext requestContext;
    QueryStatus queryStatus;
    std::shared_ptr<velox::cache::ScanTracker> tracker;
    velox::StringIdLease fileNum;
    velox::StringIdLease groupId;
    uint64_t fileSize;
};

/// A RAM buffer prepared by a `FileCacheInputStream` and handed off to a
/// consumer, paired with the absolute file region it covers.
struct FileCachePreparedBuffer
{
    velox::BufferPtr data;
    velox::common::Region region;
};

/// How a cache-write `FileCacheErrnoException` should be handled by a consumer.
enum class CacheWriteErrorAction
{
    Bypass,
    Rethrow,
};

/// Pure, side-effect-free classification of a `FileCacheErrnoException` raised
/// while writing a chunk into a cache segment. `FileSegment::write` has already
/// reconciled the downloaded size and moved the segment to
/// PARTIALLY_DOWNLOADED_NO_CONTINUATION before the exception surfaced; this only
/// decides whether the caller bypasses the cache (`Bypass`) or rethrows.
///
///   ENOSPC / EDQUOT           -> Bypass (out-of-space is always a bypass,
///                                regardless of skipOnDiskFailure; mirrors CH
///                                `writeCache`, which records the shortage and
///                                bypasses even in strict mode)
///   other errno, skip=true    -> Bypass
///   other errno, skip=false   -> Rethrow (CH throws CACHE_CANNOT_WRITE_TO_CACHE_DISK)
///
/// A non-`FileCacheErrnoException` (program logic error) never reaches here: the
/// consumers catch only the typed errno exception, so a logic error propagates
/// naturally even when skipOnDiskFailure is true (mirrors CH catching only
/// `ErrnoException`).
CacheWriteErrorAction classifyCacheWriteError(int error, bool skipOnDiskFailure) noexcept;

/// Write one already-read chunk into a FileSegment the caller has been elected
/// downloader of, then classify any physical write error. Space MUST already be
/// reserved (see reserveAndWriteSegmentChunk for the reserve+write combination).
/// Returns true on success; false when a disk write failed and skipOnDiskFailure
/// is set (the segment has already been moved to
/// PARTIALLY_DOWNLOADED_NO_CONTINUATION by FileSegment::write). Rethrows the
/// write failure when skipOnDiskFailure is false. A non-errno (logic) exception
/// is intentionally NOT caught here: a program bug propagates even when
/// skipOnDiskFailure is true (mirrors CH writeCache catching only ErrnoException).
/// The caller must already hold the downloader lease and pass
/// offset == fileSegment.getCurrentWriteOffset().
bool writeSegmentChunk(
    FileSegment & segment,
    char * data,
    size_t size,
    uint64_t offset,
    bool skipOnDiskFailure);

/// Reserve (with `reserveHint` bounding the reserve-ahead to the remaining read
/// horizon, see FileSegment::reserve) then write one chunk. Returns false if the
/// reservation failed OR the write bypassed; true on success. Rethrows a strict
/// (skipOnDiskFailure == false) non-space write error.
bool reserveAndWriteSegmentChunk(
    FileSegment & segment,
    char * data,
    size_t size,
    uint64_t offset,
    uint64_t reserveTimeoutMs,
    size_t reserveHint,
    bool skipOnDiskFailure);

/// Shared IO-layer cache lookup for a read of [absPos, absPos + size) using the
/// three mode-dispatch branches, so there is exactly one copy of the mode policy:
///   tempCacheOnly              -> getDownloadedContiguousOrEmpty; an empty batch
///                                 is a hard error (mirrors CH
///                                 throwTemporaryDataNotInCache)
///   readIfExistsOtherwiseBypass-> cache.get (read-only probe, no metadata)
///   otherwise                  -> cache.getOrSet (creates metadata)
/// Used by both `FileCacheInputStream::nextFileSegmentsBatch` and
/// `FileCacheCoalescedLoad::loadData`.
FileSegmentsHolderPtr getFileSegmentsForRead(
    const FileCacheReadContext & ctx,
    uint64_t absPos,
    uint64_t size);

/// Streaming read state machine for a single region, ported from ClickHouse
/// `CachedOnDiskReadBufferFromFile`. All position-related stream API is
/// region-relative; all `FileCache`/`FileSegment`/`ReadFile` calls use absolute
/// file offsets (`absolutePosition() = region_.offset + position_`).
class FileCacheInputStream : public dwio::common::SeekableInputStream
{
public:
    /// Business role: a stream created for an enqueued business read. Keeps a
    /// back-pointer to its `FileCacheBufferedInput` (mirrors
    /// `DirectInputStream::bufferedInput_`), used on the first `Next` to fetch
    /// coalesced-load bindings. The stream must not outlive its buffered input.
    /// All per-file identity/resources come from the shared immutable
    /// `FileCacheReadContext`.
    FileCacheInputStream(
        FileCacheBufferedInput * bufferedInput,
        std::shared_ptr<const FileCacheReadContext> context,
        velox::common::Region region,
        dwio::common::LogType logType,
        velox::cache::TrackingId trackingId = {});

    /// Internal role: a stream created by a `FileCacheCoalescedLoad` to execute
    /// one request's IO. Holds no back-pointer to the buffered input and looks up
    /// no bindings, so it may outlive the input that spawned the load. Uses an
    /// empty `TrackingId` so its reads are never accounted as business delivery.
    static std::unique_ptr<FileCacheInputStream> createCoalescedInternal(
        std::shared_ptr<const FileCacheReadContext> context,
        velox::common::Region region,
        dwio::common::LogType logType);

    ~FileCacheInputStream() override;

    /// Move out the last published output buffer (owned, pool-backed) together
    /// with the absolute file region it covers, for zero-copy handoff to a
    /// consumer. Contract (see design "takeLastOutputBuffer buffer 生命周期契约"):
    /// the current window must already have been fully delivered to the consumer;
    /// after this call `BackUp` must NOT be called on this stream. Returns
    /// `std::nullopt` when the window is empty (`outputBufferSize_ == 0`). The
    /// window metadata is cleared so the next `Next` allocates a fresh buffer and
    /// never reads the moved-out one.
    std::optional<FileCachePreparedBuffer> takeLastOutputBuffer();

    /// Install RAM buffers materialised by a `FileCacheCoalescedLoad` for this
    /// business stream. The buffers are sorted by absolute offset and served by
    /// `Next` whenever the current absolute position falls inside one of them,
    /// before falling back to the segment state machine. Business-role only.
    void installCoalescedBuffers(std::vector<FileCachePreparedBuffer> buffers);

    bool Next(const void ** data, int32_t * size) override;
    void BackUp(int32_t count) override;
    bool SkipInt64(int64_t count) override;
    int64_t ByteCount() const override;
    void seekToPosition(dwio::common::PositionProvider & position) override;
    std::string getName() const override;
    size_t positionSize() const override;

private:
    /// `RemoteFileReaderPtr` (== shared_ptr<ReadBufferFromFileBase>) is the type
    /// `FileSegment` stores for handoff, so the readers here use the same base to
    /// interoperate with `getRemoteFileReader` / `setRemoteFileReader`.
    using ReaderPtr = FileSegment::RemoteFileReaderPtr;

    struct ReadInfo
    {
        FileSegmentsHolderPtr fileSegments;
        ReaderPtr remoteReader;
        ReaderPtr cacheReader;
        // Absolute position: region.offset + region.length.
        uint64_t readUntilPosition = 0;

        void reset();
    };

    enum class ReadType : uint8_t
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
        NONE,
    };

    struct ReadFromFileSegmentState
    {
        ReaderPtr reader;
        ReadType readType = ReadType::NONE;
        uint64_t bytesToPredownload = 0;
        // For a CACHED reader over a still-DOWNLOADING segment, the absolute
        // offset where the reader's downloaded prefix ends (== range.left +
        // getDownloadedSize() captured at prepare time). The local cache file
        // only holds this many bytes right now, so the reader stops here. Once
        // the read cursor reaches it, updateReadStateIfNeeded re-prepares to pick
        // up bytes the concurrent downloader has since flushed (the reader's own
        // ReadFile caches its size at open and cannot see the growth). Zero when
        // not a CACHED read of an incomplete segment (no re-prepare on this axis).
        uint64_t cachedPrefixEndAbsolute = 0;
        // Owned scratch used when the reader's own internal buffer is too small
        // for predownload (mirrors CH `predownload_memory`).
        velox::BufferPtr predownloadBuffer;
    };

    static std::string toString(ReadType type);

    void initializeIfNeeded();
    // R2-4 business-role first-Next coalesced-load trigger (see .cpp).
    void triggerCoalescedLoadIfNeeded();
    // R2-4: if a coalesced RAM window covers the current absolute position,
    // publish it via *data/*size, advance, and return true. Otherwise false.
    bool serveCoalescedWindow(const void ** data, int32_t * size);
    // C6: publish the next zero-copy slice of the whole-file RAM preload for the
    // current position (business-role + preloaded() only), advance, and return
    // true. Never touches the FileSegment state machine.
    bool servePreloadWindow(const void ** data, int32_t * size);
    // Base pointer of the currently published window: the non-owning preload slice
    // when preloadWindow_ is set, otherwise the owned outputBuffer_ payload.
    const char * currentWindowBase() const;
    bool nextFileSegmentsBatch();
    uint64_t getRemainingSizeToRead() const;

    std::unique_ptr<ReadFromFileSegmentState> prepareReadFromFileSegmentState(
        FileSegment & fileSegment,
        uint64_t offset);

    std::unique_ptr<ReadFromFileSegmentState> createReadFromFileSegmentState(
        FileSegment & fileSegment,
        uint64_t offset);

    ReaderPtr getCacheReadBuffer(const FileSegment & fileSegment);

    ReaderPtr getRemoteReadBuffer(
        FileSegment & fileSegment,
        uint64_t offset,
        ReadType readType);

    ReaderPtr createRemoteReadBuffer();

    bool canStartFromCache(uint64_t offset, const FileSegment & fileSegment) const;

    bool updateCurrentReaderIfNeeded();
    void updateReadStateIfNeeded(FileSegment & fileSegment, uint64_t offset);

    size_t readFromCurrentSegment(
        FileSegment & fileSegment,
        uint64_t offset,
        char * outputBuffer,
        size_t outputCapacity,
        bool & readerCanBeReused);
    bool predownloadForCurrentSegment(FileSegment & fileSegment, uint64_t offset);
    bool completeCurrentSegmentAndAdvance();
    void releaseDownloaderIfNeeded(FileSegment & fileSegment, bool readerCanBeReused);

    // Drop all held segment/reader/downloader/output-buffer state and set the
    // region-relative logical position to `newPosition`, so the next Next
    // re-derives the correct segment/reader from scratch. Shared by the seek and
    // skip slow paths. Does NOT reset queryContextHolder_.
    void invalidateAndReposition(uint64_t newPosition);

    // Ensure the owned output buffer holds at least `bytes` usable bytes.
    char * ensureOutputBuffer(size_t bytes);

    uint64_t absolutePosition() const;

    // Business role only: back-pointer to the buffered input for first-Next
    // binding lookup (aligns with DirectInputStream::bufferedInput_). Null for
    // internal (coalesced) streams, which never look up bindings and may outlive
    // the input. Distinct from context_ (identity/resources); never used for
    // cache/source/pool/options access.
    FileCacheBufferedInput * bufferedInput_ = nullptr;
    // Immutable per-file identity and resources shared with the buffered input
    // and any coalesced loads. Owns cache/source/pool/stats via shared_ptr, so an
    // internal stream stays valid after its input is gone.
    std::shared_ptr<const FileCacheReadContext> context_;
    velox::common::Region region_;
    // Task 017: query cancellation. Default-constructed = never cancels. Checked
    // only at safe points where no downloader lease is held (see Next / Step 7).
    QueryStatus queryStatus_;
    // B1: per-stream tracking id for ScanTracker::recordRead when bytes are
    // delivered. Empty (id_ == -1) for read()/untracked streams.
    velox::cache::TrackingId trackingId_;
    // Acquired once in the constructor; never reset by seekToPosition.
    FileCache::QueryContextHolderPtr queryContextHolder_;
    dwio::common::LogType logType_;
    const bool skipCacheOnDiskFailure_;

    // All positions below are region-relative.
    uint64_t position_ = 0;
    uint64_t outputBufferStart_ = 0;
    velox::BufferPtr outputBuffer_;
    size_t offsetInOutputBuffer_ = 0;
    size_t outputBufferSize_ = 0;
    // C6 (design 11.6): when the buffered input is preloaded(), Next publishes a
    // zero-copy, non-owning slice straight out of the whole-file RAM preload
    // (FileCacheBufferedInput::preloadedData) instead of allocating outputBuffer_.
    // When non-null it is the base of the currently published window and
    // outputBufferStart_/offsetInOutputBuffer_/outputBufferSize_ describe it just
    // like the owned buffer, so pending-serve / BackUp work uniformly. Because the
    // memory is owned by the buffered input (not this stream), it must never be
    // moved out via takeLastOutputBuffer.
    const char * preloadWindow_ = nullptr;

    ReadInfo readInfo_;
    std::unique_ptr<ReadFromFileSegmentState> state_;
    bool initialized_ = false;

    // R2-4: coalesced RAM windows installed by a FileCacheCoalescedLoad for this
    // business stream, sorted ascending by absolute offset. Next serves bytes
    // from the window covering the current absolute position before falling back
    // to the segment state machine. Empty for internal streams.
    std::vector<FileCachePreparedBuffer> coalescedWindows_;
    // R2-4: guards the one-time first-Next binding lookup + coalesced-load wait.
    // Business-role only; internal streams never look up bindings.
    bool coalescedLoadTriggered_ = false;
};

} // namespace facebook::velox::ch
