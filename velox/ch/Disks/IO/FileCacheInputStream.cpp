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
#include "velox/ch/Disks/IO/FileCacheInputStream.h"

#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheCoalescedLoad.h"
#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/dwio/common/PositionProvider.h"

#include <algorithm>
#include <cerrno>

namespace facebook::velox::ch
{

using FileCacheUtils::checkedAdd;

namespace
{
/// Default owned output-buffer size, matching CH's remote read buffer default.
constexpr size_t kDefaultOutputBufferSize = 1u << 20; // 1 MiB
} // namespace

CacheWriteErrorAction classifyCacheWriteError(int error, bool skipOnDiskFailure) noexcept
{
    if (error == ENOSPC || error == EDQUOT)
        return CacheWriteErrorAction::Bypass;
    return skipOnDiskFailure ? CacheWriteErrorAction::Bypass : CacheWriteErrorAction::Rethrow;
}

void FileCacheInputStream::ReadInfo::reset()
{
    remoteReader.reset();
    cacheReader.reset();
    fileSegments = {};
}

std::string FileCacheInputStream::toString(ReadType type)
{
    switch (type)
    {
        case ReadType::CACHED:
            return "CACHED";
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
            return "REMOTE_FS_READ_BYPASS_CACHE";
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            return "REMOTE_FS_READ_AND_PUT_IN_CACHE";
        case ReadType::NONE:
            return "NONE";
    }
    return "NONE";
}

FileCacheInputStream::FileCacheInputStream(
    FileCacheBufferedInput * bufferedInput,
    std::shared_ptr<const FileCacheReadContext> context,
    velox::common::Region region,
    dwio::common::LogType logType,
    velox::cache::TrackingId trackingId)
    : bufferedInput_(bufferedInput)
    , context_(std::move(context))
    , region_(region)
    , queryStatus_(context_->queryStatus)
    , trackingId_(trackingId)
    , logType_(logType)
    , skipCacheOnDiskFailure_(context_->cache->skipCacheOnDiskFailure())
{
    VELOX_CHECK_NOT_NULL(context_);
    // Validate region against the source file size up front; all cache/file
    // offsets are absolute = region.offset + relative position.
    const uint64_t absEnd =
        checkedAdd(region_.offset, region_.length, "region.offset + region.length");
    VELOX_CHECK_LE(
        absEnd,
        context_->fileSize,
        "FileCacheInputStream region [{}, {}) exceeds file size {}",
        region_.offset,
        absEnd,
        context_->fileSize);

    // Acquire the query context holder once; it lives until destruction and is
    // never reset by seekToPosition.
    queryContextHolder_ = context_->cache->getQueryContextHolder(
        context_->requestContext.queryId, context_->cacheOptions);
}

std::unique_ptr<FileCacheInputStream> FileCacheInputStream::createCoalescedInternal(
    std::shared_ptr<const FileCacheReadContext> context,
    velox::common::Region region,
    dwio::common::LogType logType)
{
    // Internal role: no back-pointer to the buffered input, empty TrackingId so
    // its reads are never accounted as business delivery.
    return std::make_unique<FileCacheInputStream>(
        /*bufferedInput=*/nullptr,
        std::move(context),
        region,
        logType,
        velox::cache::TrackingId{});
}

FileCacheInputStream::~FileCacheInputStream()
{
    try
    {
        if (readInfo_.fileSegments && !readInfo_.fileSegments->empty())
        {
            auto & front = readInfo_.fileSegments->front();
            releaseDownloaderIfNeeded(front, /*readerCanBeReused=*/false);
        }
        // completeAndPopFront all remaining segments while queryContextHolder_ is
        // still alive (it is declared before readInfo_ so it outlives this reset).
        readInfo_.reset();
    }
    catch (...)
    {
        // Never throw from a destructor.
    }
}

uint64_t FileCacheInputStream::absolutePosition() const
{
    return checkedAdd(region_.offset, position_, "region.offset + position");
}

// ============================ reader helpers ============================

FileCacheInputStream::ReaderPtr FileCacheInputStream::createRemoteReadBuffer()
{
    // Remote reader over the source ReadFile, routed through the base
    // ReadFileInputStream so ReadFile::pread receives the populated
    // FileIoContext (ioStats + fileOpts + cacheable) instead of a bare context.
    return std::make_shared<ReadBufferFromVeloxReadFile>(
        context_->source, context_->pool.get(), logType_);
}

FileCacheInputStream::ReaderPtr FileCacheInputStream::getCacheReadBuffer(
    const FileSegment & fileSegment)
{
    const auto path = fileSegment.getPath();

    if (readInfo_.cacheReader)
    {
        // A fully downloaded segment's file is renamed <offset> -> <offset>_<size>,
        // so a reader opened while downloading carries the old name. Reopen under
        // the current name in that case; the caller seeks the returned buffer.
        if (readInfo_.cacheReader->getFileName() == path)
            return readInfo_.cacheReader;
        readInfo_.cacheReader.reset();
    }

    // Open the local cache segment file through the local filesystem. This uses
    // the same primitive (filesystems::FileSystem::openFileForRead) as the D1
    // OpenedFileCache; the local scheme is registered by the Manager/tests.
    //
    // Read-while-downloading rename race: a still-DOWNLOADING segment's file is
    // named <offset>; on download completion `renameToIncludeSizeInNameUnlocked`
    // renames it to <offset>_<size>. `getPath()` samples the name without a lock,
    // so a concurrent downloader (another driver) can complete the rename between
    // sampling `path` above and the open here, making the sampled <offset> path
    // vanish (FILE_NOT_FOUND). CH does not hit this because it opens the fd once
    // and the open descriptor survives the rename. We cannot pre-open, so we
    // re-sample `getPath()` (now the renamed path) and retry once. Re-sampling,
    // not blind suffix-guessing, keeps this correct for either name.
    auto openCacheFile = [&](const std::string & p)
    { return filesystems::getFileSystem(p, nullptr)->openFileForRead(p); };

    std::shared_ptr<velox::ReadFile> localFile;
    std::string openedPath = path;
    try
    {
        localFile = openCacheFile(openedPath);
    }
    catch (const std::exception &)
    {
        const auto renamedPath = fileSegment.getPath();
        if (renamedPath == openedPath)
            throw; // The name did not change; this is a real open failure.
        openedPath = renamedPath;
        localFile = openCacheFile(openedPath);
    }
    readInfo_.cacheReader = std::make_shared<ReadBufferFromVeloxReadFile>(
        std::move(localFile), context_->pool.get());

    // Self-heal on external truncation (ported from CH `getCacheReadBuffer`,
    // `CachedOnDiskReadBufferFromFile.cpp:448-477`). A fully downloaded regular
    // segment encodes its size in the file name (`<offset>_<size>`) and startup
    // metadata loading trusts that size without a `stat`. If such a file was
    // truncated outside ClickHouse, the segment is restored as fully DOWNLOADED
    // but the on-disk file is shorter than recorded. The file is already open,
    // so reading its size needs no extra `stat`.
    //
    // Observe the terminal state FIRST, then read the on-disk size: a
    // size-in-filename DOWNLOADED/DETACHED segment's file is immutable at
    // `getDownloadedSize()` bytes, so the size read next is final and a shorter
    // value can only mean an external truncation. `getDownloadedSize()` is final
    // for both DOWNLOADED and DETACHED (not reset on detach).
    const auto downloadState = fileSegment.state();
    const bool trustSizeFromFilename = fileSegment.hasSizeInFileName()
        && (downloadState == FileSegment::State::DOWNLOADED
            || downloadState == FileSegment::State::DETACHED);

    const size_t cacheFileSize =
        readInfo_.cacheReader->tryGetFileSize().value_or(0);

    if (trustSizeFromFilename && cacheFileSize < fileSegment.getDownloadedSize())
    {
        // Return nullptr so the caller bypasses the cache and re-fetches from the
        // source, rather than failing the read. Throwing here (as
        // CANNOT_READ_ALL_DATA) would be misinterpreted as a broken part during
        // `MergeTree` part loading when the truncated file backs a mark/metadata
        // file, wrongly detaching the part instead of self-healing. Covers the
        // empty-file case too (`cacheFileSize == 0 < downloadedSize`).
        readInfo_.cacheReader.reset();
        return nullptr;
    }

    if (cacheFileSize == 0)
        VELOX_FAIL("Attempt to read from an empty cache file: {}", path);

    return readInfo_.cacheReader;
}

FileCacheInputStream::ReaderPtr FileCacheInputStream::getRemoteReadBuffer(
    FileSegment & fileSegment,
    uint64_t offset,
    ReadType readType)
{
    switch (readType)
    {
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            // Each downloader downloads at most one buffer chunk, then any other
            // can continue reusing the reader left in the FileSegment.
            auto remoteReader = fileSegment.getRemoteFileReader();
            if (!remoteReader)
            {
                remoteReader = createRemoteReadBuffer();
                fileSegment.setRemoteFileReader(remoteReader);
            }
            else
            {
                VELOX_CHECK_EQ(
                    remoteReader->getFileOffsetOfBufferEnd(),
                    fileSegment.getCurrentWriteOffset());
            }
            return remoteReader;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            if (readInfo_.remoteReader
                && offset == readInfo_.remoteReader->getFileOffsetOfBufferEnd())
                return readInfo_.remoteReader;

            // Result buffer is owned only by this stream (not shareable). We
            // cannot read info.remoteReader directly because of a possible race
            // with a background downloader.
            auto reader = fileSegment.extractRemoteFileReader();
            if (reader && offset == reader->getFileOffsetOfBufferEnd())
                readInfo_.remoteReader = reader;
            else
                readInfo_.remoteReader = createRemoteReadBuffer();
            return readInfo_.remoteReader;
        }
        default:
            VELOX_FAIL(
                "Cannot use remote filesystem reader with read type: {}",
                toString(readType));
    }
}

bool FileCacheInputStream::canStartFromCache(
    uint64_t offset,
    const FileSegment & fileSegment) const
{
    return fileSegment.getCurrentWriteOffset() > offset;
}

// ============================ initialization ============================

uint64_t FileCacheInputStream::getRemainingSizeToRead() const
{
    const uint64_t absPos = absolutePosition();
    VELOX_CHECK_LE(
        absPos,
        readInfo_.readUntilPosition,
        "Read boundaries mismatch: {} > {}",
        absPos,
        readInfo_.readUntilPosition);
    return readInfo_.readUntilPosition - absPos;
}

bool FileCacheInputStream::nextFileSegmentsBatch()
{
    // Step 7 safe point 2: before any cache lookup. No downloader lease is held
    // here (we have not yet elected a downloader for the new batch).
    queryStatus_.throwIfKilled();

    VELOX_CHECK(!readInfo_.fileSegments || readInfo_.fileSegments->empty());
    const uint64_t size = getRemainingSizeToRead();
    if (size == 0)
        return false;

    const uint64_t absPos = absolutePosition();
    readInfo_.fileSegments = getFileSegmentsForRead(*context_, absPos, size);

    return readInfo_.fileSegments && !readInfo_.fileSegments->empty();
}

FileSegmentsHolderPtr getFileSegmentsForRead(
    const FileCacheReadContext & ctx,
    uint64_t absPos,
    uint64_t size)
{
    auto & cache = (*ctx.cache);
    const auto & options = ctx.cacheOptions;

    if (options.tempCacheOnly)
    {
        auto fileSegments = cache.getDownloadedContiguousOrEmpty(
            ctx.key, absPos, size, ctx.origin.user_id);
        // Throw, not return false: an empty batch is a hard error for cache-only
        // reads (mirrors CH throwTemporaryDataNotInCache).
        VELOX_CHECK(
            fileSegments && !fileSegments->empty(),
            "Temporary data is no longer present in the cache "
            "(cache-only read of [{}, {}) for key {})",
            absPos,
            absPos + size,
            ctx.key.toString());
        return fileSegments;
    }

    if (options.readIfExistsOtherwiseBypass)
    {
        return cache.get(
            ctx.key,
            absPos,
            size,
            options.segmentsBatchSize,
            ctx.origin.user_id);
    }

    CreateFileSegmentSettings createSettings(FileSegmentKind::Regular);
    std::optional<size_t> alignment;
    if (options.boundaryAlignment.has_value())
        alignment = options.boundaryAlignment.value();
    return cache.getOrSet(
        ctx.key,
        absPos,
        size,
        ctx.fileSize,
        createSettings,
        options.segmentsBatchSize,
        ctx.origin,
        alignment);
}

void FileCacheInputStream::initializeIfNeeded()
{
    if (initialized_)
        return;

    // Step 7 safe point 1: before FileCache::getOrSet / get. No downloader lease
    // is held before initialization.
    queryStatus_.throwIfKilled();

    state_.reset();
    // Absolute region end; readInfo_.readUntilPosition is absolute, not file size.
    readInfo_.readUntilPosition =
        checkedAdd(region_.offset, region_.length, "region.offset + region.length");

    if (!nextFileSegmentsBatch())
        VELOX_FAIL("List of file segments cannot be empty");

    initialized_ = true;
}

// ==================== per-segment read state ====================

std::unique_ptr<FileCacheInputStream::ReadFromFileSegmentState>
FileCacheInputStream::createReadFromFileSegmentState(
    FileSegment & fileSegment,
    uint64_t offset)
{
    auto create = [&](ReadType type, uint64_t bytesToPredownload = 0)
    {
        ReaderPtr buf;
        switch (type)
        {
            case ReadType::CACHED:
                buf = getCacheReadBuffer(fileSegment);
                if (!buf)
                {
                    // The local cache file was truncated outside ClickHouse
                    // (see `getCacheReadBuffer` self-heal). Bypass the cache and
                    // re-fetch from the source instead of failing the read.
                    type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                    buf = getRemoteReadBuffer(fileSegment, offset, type);
                }
                break;
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
                buf = getRemoteReadBuffer(fileSegment, offset, type);
                break;
            case ReadType::NONE:
                VELOX_UNREACHABLE();
        }
        auto s = std::make_unique<ReadFromFileSegmentState>();
        s->reader = std::move(buf);
        s->readType = type;
        s->bytesToPredownload = bytesToPredownload;
        return s;
    };

    const auto & options = context_->cacheOptions;
    auto downloadState = fileSegment.state();

    if (options.tempCacheOnly)
        return create(ReadType::CACHED);

    if (options.readIfExistsOtherwiseBypass)
    {
        if (downloadState == FileSegment::State::DOWNLOADED)
            return create(ReadType::CACHED);
        return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
    }

    while (true)
    {
        switch (downloadState)
        {
            case FileSegment::State::DETACHED:
                return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            case FileSegment::State::DOWNLOADING:
            {
                if (canStartFromCache(offset, fileSegment))
                    return create(ReadType::CACHED);
                downloadState = fileSegment.wait(offset, &queryStatus_);
                // Step 7 safe point 4: after FileSegment::wait() returns. wait()
                // may block, but this stream holds no downloader lease while
                // waiting on another downloader, so it is safe to abort here.
                queryStatus_.throwIfKilled();
                continue;
            }
            case FileSegment::State::DOWNLOADED:
                return create(ReadType::CACHED);
            case FileSegment::State::EMPTY:
            case FileSegment::State::PARTIALLY_DOWNLOADED:
            {
                if (canStartFromCache(offset, fileSegment))
                    return create(ReadType::CACHED);

                auto downloaderId = fileSegment.getOrSetDownloader();
                if (downloaderId == FileSegment::getCallerId())
                {
                    if (canStartFromCache(offset, fileSegment))
                    {
                        fileSegment.resetDownloader();
                        return create(ReadType::CACHED);
                    }

                    const uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
                    uint64_t bytesToPredownload = 0;
                    if (currentWriteOffset < offset)
                        bytesToPredownload = offset - currentWriteOffset;

                    return create(ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE, bytesToPredownload);
                }

                downloadState = fileSegment.state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            {
                if (canStartFromCache(offset, fileSegment))
                    return create(ReadType::CACHED);
                return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            }
        }
    }
}

std::unique_ptr<FileCacheInputStream::ReadFromFileSegmentState>
FileCacheInputStream::prepareReadFromFileSegmentState(
    FileSegment & fileSegment,
    uint64_t offset)
{
    const auto range = fileSegment.range();
    VELOX_CHECK(!fileSegment.isDownloader());
    VELOX_CHECK(offset >= range.left && offset <= range.right);

    auto state = createReadFromFileSegmentState(fileSegment, offset);

    switch (state->readType)
    {
        case ReadType::NONE:
            VELOX_FAIL("Read type not set");
        case ReadType::CACHED:
        {
            // The local cache file holds the segment's downloaded prefix in a
            // SEGMENT-RELATIVE coordinate space [0, downloadedSize). Bound and
            // seek relatively so we never over-read past what is on disk: the
            // reader wraps a local ReadFile that throws on a short pread (no
            // implicit EOF clamp), and its size is captured at open, so it cannot
            // see bytes a concurrent downloader flushes later.
            VELOX_CHECK_GE(
                offset, range.left, "current offset < file segment start offset");
            const uint64_t downloadedSize = fileSegment.getDownloadedSize();
            state->reader->setReadUntilPosition(downloadedSize);
            const uint64_t seekOffset = offset - range.left;
            state->reader->seek(static_cast<off_t>(seekOffset), SEEK_SET);

            // If the segment is still incomplete, remember where this reader's
            // downloaded prefix ends (absolute). When the read cursor reaches it,
            // updateReadStateIfNeeded re-prepares to open a fresh reader over the
            // grown cache file (or wait for more download) instead of returning a
            // spurious zero-byte read and reporting premature end of region. A
            // fully DOWNLOADED segment has downloadedSize == range.size(), so its
            // bound already covers the whole segment and no re-prepare is needed.
            if (fileSegment.state() != FileSegment::State::DOWNLOADED)
                state->cachedPrefixEndAbsolute = range.left + downloadedSize;
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            // Remote readers use ABSOLUTE source-file offsets; bound to the
            // segment's absolute end (clamped to the file size).
            state->reader->setReadUntilPosition(
                std::min<uint64_t>(range.right + 1, context_->fileSize));
            state->reader->seek(static_cast<off_t>(offset), SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            state->reader->setReadUntilPosition(
                std::min<uint64_t>(range.right + 1, context_->fileSize));
            VELOX_CHECK(fileSegment.isDownloader());
            if (state->bytesToPredownload)
            {
                const uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
                state->reader->seek(static_cast<off_t>(currentWriteOffset), SEEK_SET);
            }
            else
            {
                state->reader->seek(static_cast<off_t>(offset), SEEK_SET);
            }

            const uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
            VELOX_CHECK_EQ(
                currentWriteOffset,
                static_cast<uint64_t>(state->reader->getPosition()),
                "Buffer offsets mismatch: current_write_offset {} != reader position {}",
                currentWriteOffset,
                state->reader->getPosition());
            break;
        }
    }

    VELOX_CHECK(!state->reader->hasPendingData());
    return state;
}

// ==================== cache write / predownload ====================

bool writeSegmentChunk(
    FileSegment & segment,
    char * data,
    size_t size,
    uint64_t offset,
    bool skipOnDiskFailure)
{
    try
    {
        segment.write(data, size, offset);
    }
    catch (const FileCacheErrnoException & e)
    {
        // FileSegment::write already transitioned the segment to
        // PARTIALLY_DOWNLOADED_NO_CONTINUATION on a physical write failure.
        if (classifyCacheWriteError(e.getErrno(), skipOnDiskFailure) == CacheWriteErrorAction::Bypass)
            return false;
        throw;
    }
    // Non-errno (logic) exceptions are intentionally NOT caught here: a program
    // bug (bad offset/state/reserve invariant, a VELOX_CHECK failure) propagates
    // even when skipOnDiskFailure is true, so it is never silently swallowed into
    // a cache bypass (mirrors CH catching only ErrnoException).
    // Cache-write attribution (CH `CachedReadBufferCacheWriteBytes`, incremented
    // inside CH's `writeCache`, `CachedOnDiskReadBufferFromFile.cpp:1298`). Placed
    // here so ALL cache-fill paths — demand download, predownload, warm and
    // preload — count the `size` bytes actually written into the cache segment.
    ProfileEvents::increment(ProfileEvents::CachedReadBufferCacheWriteBytes, size);
    return true;
}

bool reserveAndWriteSegmentChunk(
    FileSegment & segment,
    char * data,
    size_t size,
    uint64_t offset,
    uint64_t reserveTimeoutMs,
    size_t reserveHint,
    bool skipOnDiskFailure)
{
    std::string reason;
    if (!segment.reserve(size, reserveTimeoutMs, reason, /*reserve_stat=*/nullptr, reserveHint))
        return false;
    VELOX_CHECK_EQ(segment.getCurrentWriteOffset(), offset);
    return writeSegmentChunk(segment, data, size, offset, skipOnDiskFailure);
}

bool FileCacheInputStream::predownloadForCurrentSegment(
    FileSegment & fileSegment,
    uint64_t offset)
{
    auto & state = *state_;
    if (state.bytesToPredownload == 0)
        return true;

    // Predownload [current_write_offset, offset) into the cache using owned
    // scratch memory (never the query output buffer), then the read resumes at
    // `offset`. Mirrors CH predownloadForFileSegment.
    uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
    // Invariant shared with prepareReadFromFileSegmentState and the demand write
    // below: on a cache-write path the reader position must equal the segment's
    // current write offset. Expressed identically at every site so a mismatch
    // reports the same way wherever it is first observed.
    VELOX_CHECK_EQ(
        currentWriteOffset,
        static_cast<uint64_t>(state.reader->getPosition()),
        "Buffer offsets mismatch: current_write_offset {} != reader position {}",
        currentWriteOffset,
        state.reader->getPosition());

    const size_t scratchSize =
        std::min<size_t>(state.bytesToPredownload, kDefaultOutputBufferSize);
    state.predownloadBuffer =
        velox::AlignedBuffer::allocate<char>(scratchSize, context_->pool.get());
    char * scratch = state.predownloadBuffer->asMutable<char>();

    while (state.bytesToPredownload > 0)
    {
        const size_t chunk =
            std::min<size_t>(scratchSize, state.bytesToPredownload);
        state.reader->set(scratch, chunk);
        // Predownload always reads from the source (it fills the segment prefix
        // that no reader/cache has yet). Time the real physical read so the
        // per-split IoStatistics carries the source read latency; the base
        // ReadFileInputStream::read already records rawBytes/totalScanTimeNs, so
        // those are not touched here (no double-count).
        uint64_t predownloadReadUs = 0;
        bool hasMore = false;
        {
            velox::MicrosecondWallTimer timer(&predownloadReadUs);
            hasMore = !state.reader->eof();
        }
        if (!hasMore)
        {
            // Source exhausted before predownload finished: release the segment
            // so waiters can take over, then fail.
            if (fileSegment.isDownloader())
            {
                fileSegment.resetRemoteFileReader();
                fileSegment.setDownloadFinishedWithoutContinuation();
            }
            VELOX_FAIL(
                "Failed to predownload remaining {} bytes for segment {}",
                state.bytesToPredownload,
                fileSegment.range().toString());
        }

        const size_t got = state.reader->buffer().size();
        VELOX_CHECK_LE(got, state.bytesToPredownload);

        // Source attribution for predownload (CH increments
        // `CachedReadBufferReadFromSourceBytes` for predownloaded chunks,
        // `CachedOnDiskReadBufferFromFile.cpp:1108`). These `got` bytes were read
        // from the source to fill the segment prefix. CH's predownload-specific
        // counters have no port enum, so only the source-bytes total is recorded.
        ProfileEvents::increment(
            ProfileEvents::CachedReadBufferReadFromSourceBytes, got);

        // Operator-level: predownloaded source bytes are a real remote read.
        if (auto * ioStats = context_->ioStatistics.get())
        {
            ioStats->read().increment(got);
            // Source read latency for the predownload physical read. Mirrors the
            // demand path and DirectInputStream; totalScanTimeNs stays with the
            // base ReadFileInputStream::read to avoid double-counting.
            ioStats->queryThreadIoLatencyUs().increment(predownloadReadUs);
            ioStats->storageReadLatencyUs().increment(predownloadReadUs);
        }

        // reserve_hint = the bytes this predownload still has to fill (remaining
        // read horizon for the predownloaded prefix), so the reserve-ahead is
        // bounded to that horizon rather than the reserve granularity. This is
        // the PRE-decrement value of `state.bytesToPredownload`: the subtraction
        // of `got` happens only at the end of this iteration (below), so the hint
        // passed here still includes the `got` bytes being written this pass.
        const bool cont = reserveAndWriteSegmentChunk(
            fileSegment,
            state.reader->buffer().begin(),
            got,
            currentWriteOffset,
            context_->cacheOptions.reserveSpaceWaitLockTimeoutMs,
            /*reserveHint=*/state.bytesToPredownload,
            skipCacheOnDiskFailure_);

        if (!cont)
        {
            // Reservation or cache write failed: bypass the cache for this
            // segment. The failed path already withdrew the reader; withdraw
            // again defensively and release the downloader.
            state.bytesToPredownload = 0;
            fileSegment.resetRemoteFileReader();
            fileSegment.completePartAndResetDownloader();
            state.readType = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
            return false;
        }

        currentWriteOffset += got;
        state.reader->position() += got;
        state.bytesToPredownload -= got;
    }
    return true;
}

// ==================== segment advance / downloader release ====================

void FileCacheInputStream::releaseDownloaderIfNeeded(
    FileSegment & fileSegment, bool readerCanBeReused)
{
    if (fileSegment.isDownloader())
    {
        if (!readerCanBeReused)
            fileSegment.resetRemoteFileReader();
        fileSegment.completePartAndResetDownloader();
    }
}

bool FileCacheInputStream::completeCurrentSegmentAndAdvance()
{
    // Drop the current read state (and thus our reference to the segment's remote
    // reader) before any potentially-throwing work.
    state_.reset();
    readInfo_.cacheReader.reset();
    readInfo_.remoteReader.reset();

    readInfo_.fileSegments->completeAndPopFront(
        context_->cacheOptions.allowBackgroundDownload,
        /*force_shrink_to_downloaded_size=*/false);

    if (readInfo_.fileSegments->empty() && !nextFileSegmentsBatch())
        return false;

    auto & next = readInfo_.fileSegments->front();
    next.increasePriority();
    state_ = prepareReadFromFileSegmentState(next, absolutePosition());
    return true;
}

bool FileCacheInputStream::updateCurrentReaderIfNeeded()
{
    VELOX_CHECK(!readInfo_.fileSegments->empty());
    auto & fileSegment = readInfo_.fileSegments->front();
    const auto range = fileSegment.range();
    const uint64_t absPos = absolutePosition();

    if (absPos > range.right)
        return completeCurrentSegmentAndAdvance();

    updateReadStateIfNeeded(fileSegment, absPos);
    return true;
}

void FileCacheInputStream::updateReadStateIfNeeded(
    FileSegment & fileSegment, uint64_t offset)
{
    if (state_->readType == ReadType::CACHED)
    {
        // A CACHED reader over a still-incomplete segment can only serve its
        // downloaded prefix [range.left, cachedPrefixEndAbsolute); its wrapped
        // ReadFile cached that size at open and cannot see bytes the concurrent
        // downloader has flushed since (nor a later rename to <offset>_<size>).
        // Re-prepare once the cursor reaches the recorded prefix end so a fresh
        // reader observes the grown/renamed cache file (or the DOWNLOADING branch
        // waits for more, or we switch to a remote read). Without this the reader
        // would freeze at the first flushed chunk (e.g. 1 MiB) and report a
        // premature end of region. `cachedPrefixEndAbsolute == 0` means the
        // segment was fully DOWNLOADED at prepare time (bound already covers the
        // whole segment), so no re-prepare is needed on this axis.
        //
        // Keep CH's original `offset >= getCurrentWriteOffset()` trigger as well
        // for the not-yet-DOWNLOADED case.
        const bool prefixExhausted = state_->cachedPrefixEndAbsolute != 0
            && offset >= state_->cachedPrefixEndAbsolute;
        const bool caughtUpToWrite =
            fileSegment.state() != FileSegment::State::DOWNLOADED
            && offset >= fileSegment.getCurrentWriteOffset();
        if (prefixExhausted || caughtUpToWrite)
            state_ = prepareReadFromFileSegmentState(fileSegment, offset);
    }
    else if (state_->readType == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
    {
        // Downloader term lasts one Next call; re-elect a downloader each time.
        state_.reset();
        state_ = prepareReadFromFileSegmentState(fileSegment, offset);
    }
}

// ==================== per-segment read ====================

size_t FileCacheInputStream::readFromCurrentSegment(
    FileSegment & fileSegment,
    uint64_t offset,
    char * outputBuffer,
    size_t outputCapacity,
    bool & readerCanBeReused)
{
    auto & state = *state_;

    if (state.bytesToPredownload)
    {
        const bool predownloaded = predownloadForCurrentSegment(fileSegment, offset);
        if (!predownloaded)
        {
            // Predownload failed and switched us to bypass: rebuild a bypass
            // reader at `offset` (the old reader may still borrow the output
            // buffer; get a fresh reader instead).
            auto buf = getRemoteReadBuffer(
                fileSegment, offset, ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            buf->setReadUntilPosition(fileSegment.range().right + 1);
            buf->seek(static_cast<off_t>(offset), SEEK_SET);
            state.reader = buf;
        }
        // In both cases the reader's working buffer was left pointing at the
        // predownload scratch (or is a brand-new bypass reader). Re-install the
        // caller output buffer before the real read so the read lands in it
        // (mirrors CH's predownload SCOPE_EXIT restoring the internal buffer).
        state.reader->set(outputBuffer, outputCapacity);
    }

    const bool doDownload = state.readType == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;

    // Single chunk read into the reader's currently installed buffer (the query
    // output buffer, installed by Next before this call). Time the physical read
    // so a source (miss) read records its latency below; a CACHED (hit) read
    // reuses the same timing but is attributed to the local-cache path, not the
    // source latency counters.
    uint64_t readUs = 0;
    bool result = false;
    {
        velox::MicrosecondWallTimer timer(&readUs);
        result = state.reader->next();
    }

    // actualBytes = physical bytes the source/cache read produced (untrimmed).
    // Everything about what physically moved (reserve, writeCache, source-IO
    // accounting) uses actualBytes. deliveredBytes is the trimmed size handed
    // to the caller and consumed by the ScanTracker; it starts equal to
    // actualBytes and is trimmed below for the last held segment. CH records
    // actual source/cache bytes BEFORE the final region trim (§11.9).
    const size_t actualBytes = result ? state.reader->buffer().size() : 0;
    size_t deliveredBytes = actualBytes;

    // Classify where these bytes were served FROM, before the readType can be
    // reassigned below on a cache-write failure. A CACHED read served the bytes
    // from a local cache segment file (a hit); any remote read type served them
    // from the source (a miss / refetch). This mirrors CH's read-path split in
    // `CachedOnDiskReadBufferFromFile::nextImplStep` (`ProfileEvents.cpp`
    // `CachedReadBufferReadFromCacheBytes` / `CachedReadBufferReadFromSourceBytes`).
    const bool servedFromCache = state.readType == ReadType::CACHED;

    if (actualBytes && doDownload)
    {
        VELOX_CHECK_LE(offset + actualBytes - 1, fileSegment.range().right);
        // Same reader-position vs current-write-offset invariant as
        // prepareReadFromFileSegmentState and predownloadForCurrentSegment,
        // expressed identically here at the demand write site.
        VELOX_CHECK_EQ(
            fileSegment.getCurrentWriteOffset(),
            static_cast<uint64_t>(state.reader->getPosition()),
            "Buffer offsets mismatch: current_write_offset {} != reader position {}",
            fileSegment.getCurrentWriteOffset(),
            state.reader->getPosition());
        // reserve_hint = the bytes still to read to the region end from the
        // current write offset (== offset here), so the reserve-ahead never
        // balloons past what this read will consume. readUntilPosition is the
        // absolute region end, not the whole file.
        const size_t reserveHint = readInfo_.readUntilPosition - offset;
        const bool success = reserveAndWriteSegmentChunk(
            fileSegment,
            state.reader->buffer().begin(),
            actualBytes,
            offset,
            context_->cacheOptions.reserveSpaceWaitLockTimeoutMs,
            reserveHint,
            skipCacheOnDiskFailure_);
        if (success)
            readerCanBeReused = true;
        else
            state.readType = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
    }

    if (actualBytes)
    {
        // For the last held segment, trim the DELIVERED size to what the region
        // actually needs. actualBytes (the physical read) is left intact so the
        // cache stored the whole chunk and source-IO accounting reflects it.
        if (readInfo_.fileSegments->size() == 1)
        {
            const uint64_t currentRight =
                std::min<uint64_t>(
                    fileSegment.range().right, readInfo_.readUntilPosition - 1);
            const size_t remaining = currentRight - offset + 1;
            if (deliveredBytes > remaining)
            {
                deliveredBytes = remaining;
                state.reader->buffer().resize(deliveredBytes);
            }
        }
        VELOX_CHECK_LE(offset + deliveredBytes, readInfo_.readUntilPosition);
    }

    if (actualBytes)
    {
        // Hit/source byte attribution over the ACTUAL (physical) bytes served,
        // matching CH which records source/cache bytes before the final trim.
        // Uses the existing `ReadType` decision, no new branching.
        // Operator-level attribution: mirror the global counter into the
        // per-split IoStatistics so it reaches OperatorStats. Local cache hits
        // map to ssdRead (customStats "localReadBytes"); source reads map to
        // read (customStats "storageReadBytes").
        if (servedFromCache)
            ProfileEvents::increment(
                ProfileEvents::CachedReadBufferReadFromCacheBytes, actualBytes);
        else
            ProfileEvents::increment(
                ProfileEvents::CachedReadBufferReadFromSourceBytes, actualBytes);

        if (auto * ioStats = context_->ioStatistics.get())
        {
            if (servedFromCache)
            {
                ioStats->ssdRead().increment(actualBytes);
                // Cache hits read through the local cache reader (bare-pread
                // ctor, no auto-accounting), so incRawBytesRead here over the
                // physical bytes. Source reads go through the base
                // ReadFileInputStream::read, which already increments raw bytes
                // for the actual physical read -- do not double-count them.
                ioStats->incRawBytesRead(static_cast<int64_t>(actualBytes));
            }
            else
            {
                ioStats->read().increment(actualBytes);
                // Source (miss) read latency. Recorded only on the source path so
                // a local cache hit is not miscounted as a storage read. The base
                // ReadFileInputStream::read already records rawBytes and
                // totalScanTimeNs for this physical read -- do not add those here.
                ioStats->queryThreadIoLatencyUs().increment(readUs);
                ioStats->storageReadLatencyUs().increment(readUs);
            }
        }

        // B1: record the actually-DELIVERED (trimmed) bytes on the ScanTracker
        // so future read-percentage decisions reflect real consumption. This is
        // the demand read path (bytes handed to the caller), never a background
        // download. No-op for internal (coalesced) streams: they carry an empty
        // TrackingId, so their reads are never accounted as business delivery.
        if (context_->tracker && !trackingId_.empty())
        {
            context_->tracker->recordRead(
                trackingId_,
                deliveredBytes,
                context_->fileNum.id(),
                context_->groupId.id());
        }
    }

    return deliveredBytes;
}

char * FileCacheInputStream::ensureOutputBuffer(size_t bytes)
{
    if (!outputBuffer_ || outputBuffer_->capacity() < bytes)
        outputBuffer_ =
            velox::AlignedBuffer::allocate<char>(bytes, context_->pool.get());
    return outputBuffer_->asMutable<char>();
}

std::optional<FileCachePreparedBuffer> FileCacheInputStream::takeLastOutputBuffer()
{
    // Empty window: nothing to hand off.
    if (outputBufferSize_ == 0)
        return std::nullopt;

    // A non-owning preload slice can never be moved out: its memory is owned by
    // the buffered input, not this stream. Preloaded business streams are never
    // used as coalesced-load internal streams, so this must not happen.
    VELOX_CHECK(
        preloadWindow_ == nullptr,
        "takeLastOutputBuffer must not move a non-owning preload slice");

    // Absolute region covered by the published window. outputBufferStart_ is the
    // region-relative position where the window began; its byte length is
    // outputBufferSize_.
    const velox::common::Region region{
        region_.offset + outputBufferStart_, outputBufferSize_};

    FileCachePreparedBuffer prepared{std::move(outputBuffer_), region};

    // Clear the window metadata so the next Next allocates/reuses a fresh buffer
    // (outputBuffer_ was just moved out) and never reads the moved-out one. Per
    // contract, BackUp must not be called after this.
    outputBufferStart_ = 0;
    offsetInOutputBuffer_ = 0;
    outputBufferSize_ = 0;

    return prepared;
}

void FileCacheInputStream::installCoalescedBuffers(
    std::vector<FileCachePreparedBuffer> buffers)
{
    coalescedWindows_ = std::move(buffers);
    std::sort(
        coalescedWindows_.begin(),
        coalescedWindows_.end(),
        [](const FileCachePreparedBuffer & a, const FileCachePreparedBuffer & b)
        { return a.region.offset < b.region.offset; });
}

// R2-4: business-role first-Next trigger. On the first Next, fetch the coalesced
// load bindings for this stream, drive/await each load, and install the RAM
// buffers it prepared for this stream's requests. Internal streams (no
// bufferedInput_) never do this. Returns without installing anything when there
// are no bindings or getData yields nullopt -- the plain demand path takes over.
void FileCacheInputStream::triggerCoalescedLoadIfNeeded()
{
    if (coalescedLoadTriggered_ || bufferedInput_ == nullptr)
        return;
    coalescedLoadTriggered_ = true;

    auto bindings = bufferedInput_->coalescedLoads(this);
    if (bindings.empty())
        return;

    std::vector<FileCachePreparedBuffer> installed;
    for (auto & binding : bindings)
    {
        auto & load = binding.load;
        if (load == nullptr)
            continue;
        folly::SemiFuture<bool> wait(false);
        if (!load->loadOrFuture(&wait))
            wait.wait();
        auto data = load->getData(binding.requestIndices);
        if (data.has_value())
        {
            for (auto & buffer : data.value())
                installed.push_back(std::move(buffer));
        }
    }
    if (!installed.empty())
        installCoalescedBuffers(std::move(installed));
}

bool FileCacheInputStream::serveCoalescedWindow(const void ** data, int32_t * size)
{
    if (coalescedWindows_.empty())
        return false;

    const uint64_t absPos = absolutePosition();
    for (auto & window : coalescedWindows_)
    {
        const uint64_t winStart = window.region.offset;
        const uint64_t winEnd = winStart + window.region.length;
        if (absPos < winStart || absPos >= winEnd)
            continue;

        const uint64_t within = absPos - winStart;
        const uint64_t avail = winEnd - absPos;
        const char * const slice = window.data->as<char>() + within;
        *data = slice;
        *size = static_cast<int32_t>(avail);
        // B1: publish the coalesced slice as a non-owning window (mirrors
        // servePreloadWindow) so BackUp / SkipInt64 / a subsequent pending-window
        // Next all operate on it uniformly. Without this the window metadata stays
        // stale (offsetInOutputBuffer_ == 0) and a decoder BackUp inside a
        // coalesced RAM window throws "BackUp beyond output buffer".
        preloadWindow_ = slice;
        outputBufferStart_ = position_;
        outputBufferSize_ = avail;
        offsetInOutputBuffer_ = avail;
        position_ += avail;
        // Business delivered-bytes accounting (no-op for empty tracking id /
        // internal streams). The coalesced payload is delivered exactly once here;
        // any bytes replayed after a BackUp are re-counted by the pending-window
        // fast path in Next.
        if (bufferedInput_ != nullptr)
            bufferedInput_->recordReadBytes(trackingId_, avail);
        return true;
    }
    return false;
}

const char * FileCacheInputStream::currentWindowBase() const
{
    if (preloadWindow_ != nullptr)
        return preloadWindow_;
    return outputBuffer_ ? outputBuffer_->as<char>() : nullptr;
}

// C6: publish the next zero-copy slice of the whole-file RAM preload covering the
// current absolute position. Reuses the outputBufferStart_/offsetInOutputBuffer_/
// outputBufferSize_ window metadata (so pending-serve + BackUp work uniformly)
// but points them at a non-owning slice into preloadData_ rather than an owned
// outputBuffer_. Never touches the FileSegment state machine, so no source /
// local / ssd read is incurred: the bytes are already resident in RAM.
bool FileCacheInputStream::servePreloadWindow(const void ** data, int32_t * size)
{
    const uint64_t remaining = region_.length - position_;
    if (remaining == 0)
        return false;

    // Contiguous slice from the current absolute position to (at most) the end of
    // the current preload-storage run. May be shorter than `remaining`; the next
    // Next resumes at the following run.
    const folly::Range<const char *> slice =
        bufferedInput_->preloadedData(absolutePosition(), remaining);
    const size_t avail = slice.size();
    VELOX_CHECK_GT(avail, 0, "preloadedData returned an empty slice");

    // Publish as a non-owning window: outputBuffer_ stays whatever it was, but
    // currentWindowBase() now returns preloadWindow_.
    preloadWindow_ = slice.data();
    outputBufferStart_ = position_;
    outputBufferSize_ = avail;
    offsetInOutputBuffer_ = avail;
    position_ += avail;

    // Business delivered-bytes accounting (no-op for empty tracking id). Preload
    // slices are delivered exactly once, mirroring the coalesced/demand paths.
    bufferedInput_->recordReadBytes(trackingId_, avail);

    *data = slice.data();
    *size = static_cast<int32_t>(avail);
    return true;
}

// ============================ Next ============================

bool FileCacheInputStream::Next(const void ** data, int32_t * size)
{
    // Serve any bytes still pending in the published window first (BackUp /
    // SkipInt64 leave the window in place). The window may be the owned
    // outputBuffer_ or a non-owning preload slice (currentWindowBase()).
    if (offsetInOutputBuffer_ < outputBufferSize_)
    {
        const size_t avail = outputBufferSize_ - offsetInOutputBuffer_;
        *data = currentWindowBase() + offsetInOutputBuffer_;
        *size = static_cast<int32_t>(avail);
        position_ += avail;
        offsetInOutputBuffer_ = outputBufferSize_;
        // Business delivered-bytes accounting for the REPLAY path. On the initial
        // publish the whole window was recorded at once (offsetInOutputBuffer_ was
        // set to the full size), so this branch only fires after a BackUp / local
        // seek reopened part of the window. Those bytes are delivered again and
        // must be re-counted, mirroring DirectInputStream::Next which records
        // recordRead on every delivery, including replays (no-op for empty
        // tracking id / internal streams).
        if (bufferedInput_ != nullptr)
            bufferedInput_->recordReadBytes(trackingId_, avail);
        return true;
    }

    if (position_ >= region_.length)
        return false;

    // C6: after a whole-file preload, serve zero-copy slices straight out of RAM.
    // Business-role streams built by makePreloadedStream take this path; it never
    // touches the FileSegment / coalesced state machine. Preloaded inputs never
    // build coalesced loads for their streams, so bindings are always empty here.
    if (bufferedInput_ != nullptr && bufferedInput_->preloaded())
    {
        return servePreloadWindow(data, size);
    }

    // R2-4: business-role first-Next coalesced-load trigger. Runs at most once;
    // installs RAM windows this stream can serve from before touching the segment
    // state machine. Internal streams and streams without bindings are no-ops.
    triggerCoalescedLoadIfNeeded();

    // Serve from a coalesced RAM window covering the current position, if any.
    if (serveCoalescedWindow(data, size))
        return true;

    // Step 7 safe point 3: at the outer Next() iteration boundary, before
    // starting (or advancing to) a segment. Any previous segment's downloader
    // lease was already released at the end of the prior Next() call, so no
    // lease is held here.
    queryStatus_.throwIfKilled();

    initializeIfNeeded();

    if (readInfo_.fileSegments->empty() && !nextFileSegmentsBatch())
        return false;

    bool readerCanBeReused = false;
    // Honor the configured remote buffer size when set (mirrors CH, which passes
    // remote_fs_buffer_size to the reader); otherwise use the default. A smaller
    // configured size limits how much one downloader term writes per Next.
    const size_t bufCapacity = context_->cacheOptions.remoteFsBufferSize > 0
        ? context_->cacheOptions.remoteFsBufferSize
        : kDefaultOutputBufferSize;
    char * out = ensureOutputBuffer(bufCapacity);

    if (state_ && state_->reader)
    {
        if (!updateCurrentReaderIfNeeded())
            return false;
    }
    else
    {
        state_ = prepareReadFromFileSegmentState(
            readInfo_.fileSegments->front(), absolutePosition());
        readInfo_.fileSegments->front().increasePriority();
    }

    auto & fileSegment = readInfo_.fileSegments->front();
    const auto range = fileSegment.range();
    const uint64_t absOffset = absolutePosition();

    size_t got = 0;
    try
    {
        // Install the query output buffer as the reader target (zero-copy handoff).
        state_->reader->set(out, bufCapacity);
        got = readFromCurrentSegment(fileSegment, absOffset, out, bufCapacity, readerCanBeReused);
        // The read wrote directly into `out`. Copy is not needed: the working
        // buffer begins at `out`.
        VELOX_CHECK(got == 0 || state_->reader->buffer().begin() == out);
        // Detach the reader from the output buffer before handoff.
        state_->reader->set(nullptr, 0);
    }
    catch (...)
    {
        // On any read/write exception, do not return the canceled reader to the
        // FileSegment: release downloader state in CH order, then propagate.
        if (state_)
        {
            releaseDownloaderIfNeeded(fileSegment, /*readerCanBeReused=*/false);
            state_.reset();
        }
        throw;
    }

    if (got == 0)
        return false;

    // Publish the output buffer window to the caller.
    preloadWindow_ = nullptr; // owned buffer window; not a preload slice
    outputBufferStart_ = position_;
    outputBufferSize_ = got;
    offsetInOutputBuffer_ = got;
    position_ += got;

    // Release the downloader term for the just-read segment (one buffer chunk per
    // downloader) before advancing.
    if (state_ && state_->readType != ReadType::CACHED)
        releaseDownloaderIfNeeded(fileSegment, readerCanBeReused);

    // Advance past a fully consumed segment. This may prepare the next segment
    // and (for a remote read) elect it as downloader.
    FileSegment * beforeAdvance =
        readInfo_.fileSegments->empty() ? nullptr : &readInfo_.fileSegments->front();
    if (absolutePosition() > range.right)
        completeCurrentSegmentAndAdvance();

    // Mirror CH's nextImplStep SCOPE_EXIT: if the advance prepared a DIFFERENT
    // front segment as downloader, release it so no downloader is ever left held
    // across Next calls. Only fires when the front actually changed, so the
    // just-read segment (already released above with the correct reuse flag) is
    // never touched twice with a conflicting flag.
    if (readInfo_.fileSegments && !readInfo_.fileSegments->empty())
    {
        auto & front = readInfo_.fileSegments->front();
        if (&front != beforeAdvance)
        {
            const bool couldBeDownloader = !state_ || state_->readType != ReadType::CACHED;
            if (couldBeDownloader)
                releaseDownloaderIfNeeded(front, /*readerCanBeReused=*/true);
        }
    }

    *data = out;
    *size = static_cast<int32_t>(got);
    return true;
}

// ==================== SeekableInputStream surface ====================

void FileCacheInputStream::BackUp(int32_t count)
{
    VELOX_CHECK_GE(count, 0);
    const size_t back = static_cast<size_t>(count);
    // Only bytes already returned from the current output buffer may be backed up.
    VELOX_CHECK_LE(back, offsetInOutputBuffer_, "BackUp beyond output buffer");
    position_ -= back;
    offsetInOutputBuffer_ -= back;
    // Does NOT reset FileCache state.
}

bool FileCacheInputStream::SkipInt64(int64_t count)
{
    VELOX_CHECK_GE(count, 0);
    const uint64_t toSkip = static_cast<uint64_t>(count);
    if (toSkip == 0)
        return true;

    // Fast path: the skip target stays within the already-published output
    // buffer. Just advance the region-relative cursors (mirrors CH seek nudging
    // the pointer inside the current buffer; keeps BackUp semantics intact).
    if (offsetInOutputBuffer_ < outputBufferSize_)
    {
        const size_t avail = outputBufferSize_ - offsetInOutputBuffer_;
        if (toSkip <= avail)
        {
            offsetInOutputBuffer_ += toSkip;
            position_ += toSkip;
            return true;
        }
    }

    // Slow path: the target is outside the current buffer (including crossing a
    // segment boundary). Do NOT call Next()/completeCurrentSegmentAndAdvance
    // here: that advance is irreversible and would desync position_ from the
    // held segment. Instead invalidate all held state (as CH seek does with
    // info.reset()/state.reset()/initialized=false) and set position_ to the
    // absolute skip target, so the next real Next re-derives the correct
    // segment/reader from position_.
    const uint64_t target = position_ + toSkip;
    VELOX_CHECK_LE(target, region_.length, "skip beyond region");
    invalidateAndReposition(target);
    return true;
}

int64_t FileCacheInputStream::ByteCount() const
{
    // Region-relative logical position.
    return static_cast<int64_t>(position_);
}

void FileCacheInputStream::seekToPosition(dwio::common::PositionProvider & position)
{
    const uint64_t newPosition = position.next(); // region-relative
    VELOX_CHECK_LE(newPosition, region_.length, "seek beyond region");

    if (outputBufferSize_ > 0 && outputBufferStart_ <= newPosition
        && newPosition < outputBufferStart_ + outputBufferSize_)
    {
        // Fast path: land inside the already-filled output buffer. O(1); no
        // holder/downloader/state change.
        position_ = newPosition;
        offsetInOutputBuffer_ = newPosition - outputBufferStart_;
        return;
    }

    // Slow path: release everything except queryContextHolder_.
    invalidateAndReposition(newPosition);
}

void FileCacheInputStream::invalidateAndReposition(uint64_t newPosition)
{
    if (readInfo_.fileSegments && !readInfo_.fileSegments->empty())
        releaseDownloaderIfNeeded(
            readInfo_.fileSegments->front(), /*readerCanBeReused=*/false);
    readInfo_.reset();
    state_.reset();
    position_ = newPosition;
    outputBufferStart_ = 0;
    outputBufferSize_ = 0;
    offsetInOutputBuffer_ = 0;
    preloadWindow_ = nullptr;
    initialized_ = false;
    // queryContextHolder_ is intentionally NOT reset.
}

std::string FileCacheInputStream::getName() const
{
    return fmt::format(
        "FileCacheInputStream(key={}, region=[{}, {}))",
        context_->key.toString(),
        region_.offset,
        region_.offset + region_.length);
}

size_t FileCacheInputStream::positionSize() const
{
    // Single position component (the region-relative offset).
    return 1;
}

} // namespace facebook::velox::ch
