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

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/PositionProvider.h"

#include <algorithm>

namespace facebook::velox::ch
{

using FileCacheUtils::checkedAdd;

namespace
{
/// Default owned output-buffer size, matching CH's remote read buffer default.
constexpr size_t kDefaultOutputBufferSize = 1u << 20; // 1 MiB
} // namespace

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
    FileCacheBufferedInput * owner,
    velox::common::Region region,
    FileCacheRequestContext cacheContext,
    dwio::common::LogType logType,
    QueryStatus queryStatus)
    : owner_(owner)
    , region_(region)
    , cacheContext_(std::move(cacheContext))
    , queryStatus_(std::move(queryStatus))
    , logType_(logType)
    , skipCacheOnDiskFailure_(owner_->fileCache().skipCacheOnDiskFailure())
{
    VELOX_CHECK_NOT_NULL(owner_);
    // Validate region against the source file size up front; all cache/file
    // offsets are absolute = region.offset + relative position.
    const uint64_t absEnd =
        checkedAdd(region_.offset, region_.length, "region.offset + region.length");
    VELOX_CHECK_LE(
        absEnd,
        owner_->fileSize(),
        "FileCacheInputStream region [{}, {}) exceeds file size {}",
        region_.offset,
        absEnd,
        owner_->fileSize());

    // Acquire the query context holder once; it lives until destruction and is
    // never reset by seekToPosition.
    queryContextHolder_ = owner_->fileCache().getQueryContextHolder(
        cacheContext_.queryId, owner_->cacheOptions());
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
    // Remote reader over the source ReadFile (non-owning: the BufferedInput owns
    // the shared source file for the stream's lifetime).
    return std::make_shared<ReadBufferFromVeloxReadFile>(
        owner_->sourceReadFile(), owner_->memoryPool());
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
    auto fs = filesystems::getFileSystem(path, nullptr);
    std::shared_ptr<velox::ReadFile> localFile = fs->openFileForRead(path);
    readInfo_.cacheReader = std::make_shared<ReadBufferFromVeloxReadFile>(
        std::move(localFile), owner_->memoryPool());

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

    auto & cache = owner_->fileCache();
    const uint64_t absPos = absolutePosition();
    const auto & options = owner_->cacheOptions();

    if (options.tempCacheOnly)
    {
        readInfo_.fileSegments = cache.getDownloadedContiguousOrEmpty(
            owner_->cacheKey(), absPos, size, owner_->origin().user_id);
        // Throw, not return false: an empty batch is a hard error for cache-only
        // reads (mirrors CH throwTemporaryDataNotInCache).
        VELOX_CHECK(
            readInfo_.fileSegments && !readInfo_.fileSegments->empty(),
            "Temporary data is no longer present in the cache "
            "(cache-only read of [{}, {}) for key {})",
            absPos,
            absPos + size,
            owner_->cacheKey().toString());
        return true;
    }

    if (options.readIfExistsOtherwiseBypass)
    {
        readInfo_.fileSegments = cache.get(
            owner_->cacheKey(),
            absPos,
            size,
            options.segmentsBatchSize,
            owner_->origin().user_id);
    }
    else
    {
        CreateFileSegmentSettings createSettings(FileSegmentKind::Regular);
        std::optional<size_t> alignment;
        if (options.boundaryAlignment.has_value())
            alignment = options.boundaryAlignment.value();
        readInfo_.fileSegments = cache.getOrSet(
            owner_->cacheKey(),
            absPos,
            size,
            owner_->fileSize(),
            createSettings,
            options.segmentsBatchSize,
            owner_->origin(),
            alignment);
    }

    return readInfo_.fileSegments && !readInfo_.fileSegments->empty();
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

    const auto & options = owner_->cacheOptions();
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
                downloadState = fileSegment.wait(offset);
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
            // The local cache file holds the segment's bytes in a SEGMENT-RELATIVE
            // coordinate space [0, downloadedSize). Bound and seek relatively so we
            // never over-read past the downloaded prefix.
            VELOX_CHECK_GE(
                offset, range.left, "current offset < file segment start offset");
            const uint64_t downloadedSize = fileSegment.getDownloadedSize();
            state->reader->setReadUntilPosition(downloadedSize);
            const uint64_t seekOffset = offset - range.left;
            state->reader->seek(static_cast<off_t>(seekOffset), SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            // Remote readers use ABSOLUTE source-file offsets; bound to the
            // segment's absolute end (clamped to the file size).
            state->reader->setReadUntilPosition(
                std::min<uint64_t>(range.right + 1, owner_->fileSize()));
            state->reader->seek(static_cast<off_t>(offset), SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            state->reader->setReadUntilPosition(
                std::min<uint64_t>(range.right + 1, owner_->fileSize()));
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

bool FileCacheInputStream::writeCache(
    char * data,
    size_t size,
    uint64_t offset,
    FileSegment & fileSegment)
{
    try
    {
        fileSegment.write(data, size, offset);
    }
    catch (const std::exception &)
    {
        // The Velox FileSegment::write already transitioned the segment to
        // PARTIALLY_DOWNLOADED_NO_CONTINUATION on a physical write failure.
        if (skipCacheOnDiskFailure_)
            return false;
        throw;
    }
    return true;
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
    VELOX_CHECK_EQ(
        static_cast<uint64_t>(state.reader->getPosition()), currentWriteOffset);

    const size_t scratchSize =
        std::min<size_t>(state.bytesToPredownload, kDefaultOutputBufferSize);
    state.predownloadBuffer =
        velox::AlignedBuffer::allocate<char>(scratchSize, owner_->memoryPool());
    char * scratch = state.predownloadBuffer->asMutable<char>();

    while (state.bytesToPredownload > 0)
    {
        const size_t chunk =
            std::min<size_t>(scratchSize, state.bytesToPredownload);
        state.reader->set(scratch, chunk);
        const bool hasMore = !state.reader->eof();
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

        std::string reason;
        const bool reserved = fileSegment.reserve(
            got,
            owner_->cacheOptions().reserveSpaceWaitLockTimeoutMs,
            reason);

        bool cont = reserved;
        if (reserved)
            cont = writeCache(
                state.reader->buffer().begin(), got, currentWriteOffset, fileSegment);

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
        owner_->cacheOptions().allowBackgroundDownload,
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
    if (state_->readType == ReadType::CACHED
        && fileSegment.state() != FileSegment::State::DOWNLOADED)
    {
        // We started from cache but the segment is no longer fully downloaded;
        // if we caught up to the write offset, re-prepare (may switch to remote).
        if (offset >= fileSegment.getCurrentWriteOffset())
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
    // output buffer, installed by Next before this call).
    const bool result = state.reader->next();
    size_t size = result ? state.reader->buffer().size() : 0;

    if (size && doDownload)
    {
        VELOX_CHECK_LE(offset + size - 1, fileSegment.range().right);
        std::string reason;
        bool success = fileSegment.reserve(
            size, owner_->cacheOptions().reserveSpaceWaitLockTimeoutMs, reason);
        if (success)
        {
            VELOX_CHECK_EQ(
                fileSegment.getCurrentWriteOffset(),
                static_cast<uint64_t>(state.reader->getPosition()));
            success = writeCache(
                state.reader->buffer().begin(), size, offset, fileSegment);
            if (success)
                readerCanBeReused = true;
        }
        if (!success)
            state.readType = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
    }

    if (size)
    {
        // For the last held segment, trim to what the region actually needs.
        if (readInfo_.fileSegments->size() == 1)
        {
            const uint64_t currentRight =
                std::min<uint64_t>(
                    fileSegment.range().right, readInfo_.readUntilPosition - 1);
            const size_t remaining = currentRight - offset + 1;
            if (size > remaining)
            {
                size = remaining;
                state.reader->buffer().resize(size);
            }
        }
        VELOX_CHECK_LE(offset + size, readInfo_.readUntilPosition);
    }

    return size;
}

char * FileCacheInputStream::ensureOutputBuffer(size_t bytes)
{
    if (!outputBuffer_ || outputBuffer_->capacity() < bytes)
        outputBuffer_ =
            velox::AlignedBuffer::allocate<char>(bytes, owner_->memoryPool());
    return outputBuffer_->asMutable<char>();
}

// ============================ Next ============================

bool FileCacheInputStream::Next(const void ** data, int32_t * size)
{
    // Serve any bytes still pending in the published output buffer first
    // (BackUp / SkipInt64 leave the buffer in place).
    if (offsetInOutputBuffer_ < outputBufferSize_)
    {
        const size_t avail = outputBufferSize_ - offsetInOutputBuffer_;
        *data = outputBuffer_->as<char>() + offsetInOutputBuffer_;
        *size = static_cast<int32_t>(avail);
        position_ += avail;
        offsetInOutputBuffer_ = outputBufferSize_;
        return true;
    }

    if (position_ >= region_.length)
        return false;

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
    const size_t bufCapacity = owner_->cacheOptions().remoteFsBufferSize > 0
        ? owner_->cacheOptions().remoteFsBufferSize
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
    uint64_t toSkip = static_cast<uint64_t>(count);
    while (toSkip > 0)
    {
        // Consume from the published output buffer first.
        if (offsetInOutputBuffer_ < outputBufferSize_)
        {
            const size_t avail = outputBufferSize_ - offsetInOutputBuffer_;
            const size_t step = std::min<uint64_t>(avail, toSkip);
            offsetInOutputBuffer_ += step;
            position_ += step;
            toSkip -= step;
            continue;
        }
        // Otherwise read and discard the next chunk.
        const void * data = nullptr;
        int32_t size = 0;
        if (!Next(&data, &size))
            return toSkip == 0;
        // Next advanced position_ by `size`; walk it back so the loop consumes
        // exactly `toSkip` and leaves the remainder in the output buffer.
        const size_t produced = static_cast<size_t>(size);
        const size_t consume = std::min<uint64_t>(produced, toSkip);
        // Undo the full-buffer advance done by Next, then re-consume `consume`.
        position_ -= produced;
        offsetInOutputBuffer_ -= produced;
        offsetInOutputBuffer_ += consume;
        position_ += consume;
        toSkip -= consume;
    }
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
    if (readInfo_.fileSegments && !readInfo_.fileSegments->empty())
        releaseDownloaderIfNeeded(
            readInfo_.fileSegments->front(), /*readerCanBeReused=*/false);
    readInfo_.reset();
    state_.reset();
    position_ = newPosition;
    outputBufferStart_ = 0;
    outputBufferSize_ = 0;
    offsetInOutputBuffer_ = 0;
    initialized_ = false;
    // queryContextHolder_ is intentionally NOT reset.
}

std::string FileCacheInputStream::getName() const
{
    return fmt::format(
        "FileCacheInputStream(key={}, region=[{}, {}))",
        owner_->cacheKey().toString(),
        region_.offset,
        region_.offset + region_.length);
}

size_t FileCacheInputStream::positionSize() const
{
    // Single position component (the region-relative offset).
    return 1;
}

} // namespace facebook::velox::ch
