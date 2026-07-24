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

#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/testutil/TestValue.h"

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>

namespace facebook::velox::ch
{

namespace
{
// Default output-buffer size when neither remote nor local buffer size is set.
constexpr size_t kDefaultOutputBufferSize = 1u << 20; // 1 MiB
} // namespace

void FileCacheInputStream::ReadInfo::reset()
{
    remoteReader.reset();
    cacheReader.reset();
    // Resetting the holder completes any remaining segments (FileSegmentsHolder
    // destructor -> completeAndPopFront for each segment).
    fileSegments = {};
}

FileCacheInputStream::FileCacheInputStream(
    FileCacheBufferedInput * owner,
    velox::common::Region region,
    FileCacheRequestContext cacheContext,
    dwio::common::LogType logType)
    : owner_(owner),
      region_(region),
      cacheContext_(std::move(cacheContext)),
      logType_(logType),
      pool_(owner->memoryPool())
{
    VELOX_CHECK_NOT_NULL(owner_, "FileCacheInputStream requires an owner");
    VELOX_CHECK_NOT_NULL(pool_, "FileCacheInputStream requires a MemoryPool");

    // Direct-IO alignment required by the source file; the output buffer handed
    // to the reader via set() must satisfy it.
    uint64_t alignment = 1;
    owner_->sourceReadFile()->directIo(alignment);
    directIoAlignment_ = alignment > 0 ? alignment : 1;

    // Acquire the query context holder once. It lives until destruction and is
    // never reset by seekToPosition.
    queryContextHolder_ =
        owner_->fileCache().getQueryContextHolder(cacheContext_.queryId, owner_->cacheOptions());

    // Capture the per-query ledgers from the owner. They are updated on every I/O
    // fact independently of the process-wide ProfileEvents ledger.
    ioStatistics_ = owner_->ioStatistics();
    ioStats_ = owner_->ioStats();

    // Copy the cancellation token by value from the owner. It is passed to
    // FileSegment::wait and checked at the segment-batch safe points.
    cancellationToken_ = owner_->cancellationToken();
}

FileCacheInputStream::~FileCacheInputStream()
{
    try
    {
        // completePartAndResetDownloader if still held, then complete the held
        // segments -- all while queryContextHolder_ is still alive (it is declared
        // before readInfo_, so it is destroyed after it).
        releaseDownloaderIfNeeded();
        readInfo_.reset();
    }
    catch (...)
    {
        // A destructor must not throw.
    }
}

uint64_t FileCacheInputStream::absolutePosition() const
{
    return FileCacheUtils::checkedAdd(region_.offset, position_, "file cache absolute position");
}

void FileCacheInputStream::allocateOutputBufferIfNeeded()
{
    if (outputBuffer_)
        return;

    size_t bufferSize = owner_->cacheOptions().remoteFsBufferSize;
    if (bufferSize == 0)
        bufferSize = owner_->cacheOptions().localFsBufferSize;
    if (bufferSize == 0)
        bufferSize = kDefaultOutputBufferSize;

    if (directIoAlignment_ <= 1)
    {
        outputBuffer_ = velox::AlignedBuffer::allocate<char>(bufferSize, pool_);
        outputBufferData_ = outputBuffer_->asMutable<char>();
        outputBufferCapacity_ = bufferSize;
        return;
    }

    // Direct IO needs the destination address and length aligned. A pool
    // allocation only guarantees its own (smaller) alignment, so over-allocate
    // and align the usable range up.
    const size_t rounded = ((bufferSize + directIoAlignment_ - 1) / directIoAlignment_) * directIoAlignment_;
    const size_t allocSize = rounded + directIoAlignment_;
    outputBuffer_ = velox::AlignedBuffer::allocate<char>(allocSize, pool_);
    auto * raw = outputBuffer_->asMutable<char>();
    const auto addr = reinterpret_cast<uintptr_t>(raw);
    const auto alignedAddr = (addr + directIoAlignment_ - 1) & ~(static_cast<uintptr_t>(directIoAlignment_) - 1);
    outputBufferData_ = reinterpret_cast<char *>(alignedAddr);
    outputBufferCapacity_ = rounded;
}

std::string FileCacheInputStream::getName() const
{
    return fmt::format(
        "FileCacheInputStream({}, region offset {} length {})",
        owner_->sourceReadFile()->getName(),
        region_.offset,
        region_.length);
}

size_t FileCacheInputStream::positionSize() const
{
    // Uncompressed stream: a single (byte offset) position value.
    return 1;
}

int64_t FileCacheInputStream::ByteCount() const
{
    // Region-relative.
    return static_cast<int64_t>(position_);
}

void FileCacheInputStream::BackUp(int count)
{
    VELOX_CHECK_GE(count, 0, "BackUp count must be non-negative");
    const size_t c = static_cast<size_t>(count);
    VELOX_CHECK_LE(c, offsetInOutputBuffer_, "BackUp beyond the current output buffer");
    position_ -= c;
    offsetInOutputBuffer_ -= c;
}

bool FileCacheInputStream::Next(const void ** data, int * size)
{
    // Serve remaining bytes from the current output buffer (post-BackUp or a
    // partially-consumed chunk) before refilling.
    if (offsetInOutputBuffer_ < outputBufferSize_)
    {
        const size_t avail = outputBufferSize_ - offsetInOutputBuffer_;
        *data = outputBufferData_ + offsetInOutputBuffer_;
        *size = static_cast<int>(avail);
        position_ += avail;
        offsetInOutputBuffer_ = outputBufferSize_;
        return true;
    }

    const size_t got = readNextChunk();
    if (got == 0)
    {
        *data = nullptr;
        *size = 0;
        return false;
    }

    *data = outputBufferData_;
    *size = static_cast<int>(got);
    position_ += got;
    offsetInOutputBuffer_ = got;
    return true;
}

bool FileCacheInputStream::SkipInt64(int64_t count)
{
    if (count < 0)
        return false;

    uint64_t remaining = static_cast<uint64_t>(count);
    while (remaining > 0)
    {
        if (offsetInOutputBuffer_ < outputBufferSize_)
        {
            const uint64_t avail = outputBufferSize_ - offsetInOutputBuffer_;
            const uint64_t take = std::min<uint64_t>(remaining, avail);
            offsetInOutputBuffer_ += take;
            position_ += take;
            remaining -= take;
        }
        else if (readNextChunk() == 0)
        {
            return false;
        }
    }
    return true;
}

void FileCacheInputStream::seekToPosition(dwio::common::PositionProvider & position)
{
    const uint64_t newPosition = position.next(); // region-relative
    VELOX_CHECK_LE(newPosition, region_.length, "seek position exceeds region length");

    if (outputBufferStart_ <= newPosition && newPosition < outputBufferStart_ + outputBufferSize_)
    {
        // Fast path: O(1). The FileSegmentsHolder, downloader, and state_ are all
        // preserved.
        position_ = newPosition;
        offsetInOutputBuffer_ = newPosition - outputBufferStart_;
        return;
    }

    // Slow path: release the held downloader and read state, then rebuild on the
    // next read. queryContextHolder_ is NEVER reset here.
    releaseDownloaderIfNeeded();
    readInfo_.reset();
    state_.reset();
    position_ = newPosition;
    outputBufferStart_ = newPosition;
    outputBufferSize_ = 0;
    offsetInOutputBuffer_ = 0;
    initialized_ = false;
}

void FileCacheInputStream::initializeIfNeeded(uint64_t offset)
{
    if (initialized_)
        return;

    // Absolute end of the region.
    readInfo_.readUntilPosition = FileCacheUtils::checkedAdd(region_.offset, region_.length, "file cache read-until");
    if (!nextFileSegmentsBatch(offset))
        VELOX_FAIL("FileCacheInputStream: the list of file segments cannot be empty");
    initialized_ = true;
}

bool FileCacheInputStream::nextFileSegmentsBatch(uint64_t offset)
{
    // Safe cancellation point: this runs before the first FileCache lookup and
    // between completed segment batches, never while a downloader lease or a
    // reserve/write is held (design 4.2).
    if (cancellationToken_.isCancellationRequested())
        VELOX_FAIL("FileCache read cancelled before segment batch lookup");

    VELOX_CHECK_LE(offset, readInfo_.readUntilPosition, "read offset past the region end");
    const uint64_t remaining = readInfo_.readUntilPosition - offset;
    if (remaining == 0)
        return false;

    const auto & opts = owner_->cacheOptions();
    if (opts.tempCacheOnly)
    {
        readInfo_.fileSegments = owner_->fileCache().getDownloadedContiguousOrEmpty(
            owner_->cacheKey(), offset, remaining, owner_->origin().user_id);
        if (readInfo_.fileSegments->empty())
            VELOX_FAIL(
                "Temporary data is no longer present in the cache for [{}, {})", offset, offset + remaining);
        return true;
    }

    if (opts.readIfExistsOtherwiseBypass)
    {
        readInfo_.fileSegments = owner_->fileCache().get(
            owner_->cacheKey(), offset, remaining, opts.segmentsBatchSize, owner_->origin().user_id);
    }
    else
    {
        CreateFileSegmentSettings createSettings(FileSegmentKind::Regular);
        readInfo_.fileSegments = owner_->fileCache().getOrSet(
            owner_->cacheKey(),
            offset,
            remaining,
            owner_->fileSize(),
            createSettings,
            opts.segmentsBatchSize,
            owner_->origin(),
            opts.boundaryAlignment);
    }

    return !readInfo_.fileSegments->empty();
}

bool FileCacheInputStream::canStartFromCache(uint64_t offset, const FileSegment & fileSegment) const
{
    return fileSegment.getCurrentWriteOffset() > offset;
}

std::shared_ptr<ReadBufferFromVeloxReadFile> FileCacheInputStream::getCacheReadBuffer(
    const FileSegment & fileSegment)
{
    auto path = fileSegment.getPath();
    if (readInfo_.cacheReader)
    {
        if (readInfo_.cacheReader->getFileName() == path)
            return readInfo_.cacheReader;
        readInfo_.cacheReader.reset();
    }

    // Test-only injection point: fires after the path is captured and before
    // the first open attempt, so a test can interpose a concurrent rename and
    // verify the retry logic below. Has no effect in production builds
    // (TestValue::adjust is a no-op unless TestValue::enable() was called).
    common::testutil::TestValue::adjust(
        "facebook::velox::ch::FileCacheInputStream::beforeCacheFileOpen", this);

    // A size-suffixed segment file is renamed from `<offset>` to
    // `<offset>_<size>` by setDownloadedUnlocked while we may still be holding
    // a path computed from getPath() before the rename. The open then fails
    // with FILE_NOT_FOUND because the old name is gone. Recompute the path
    // while holding the segment lock — the rename runs under the same lock, so
    // this serialises against it and observes the final name — and retry once.
    // If the path is unchanged, the missing file is not explained by a rename,
    // so rethrow. Any other open error is unrelated to the rename and is
    // propagated immediately. Matches CachedOnDiskReadBufferFromFile.cpp:366-395.
    try
    {
        // Open the local cache segment file through the Manager-injected cache
        // factory (never through FileCacheBufferedInput, which would re-enter
        // the cache).
        readInfo_.cacheReader = owner_->fileCache().createCacheReadBuffer(path);
    }
    catch (const velox::VeloxException & e)
    {
        if (e.errorCode() != velox::error_code::kFileNotFound)
            throw;
        std::string newPath;
        {
            auto lk = fileSegment.lock();
            newPath = fileSegment.getPath();
        }
        if (newPath == path)
            throw;
        path = std::move(newPath);
        readInfo_.cacheReader = owner_->fileCache().createCacheReadBuffer(path);
    }

    // CH source of truth: src/Disks/IO/CachedOnDiskReadBufferFromFile.cpp:448-472.
    //
    // Observe state and hasSizeInFileName AFTER opening the file, matching CH
    // exactly. The invariant that matters is: state must be observed before the
    // physical size is sampled. setDownloadedUnlocked does: (1) write final
    // bytes, (2) rename + size_in_filename=true, (3) download_state=DOWNLOADED.
    // Observing DOWNLOADED means the rename already completed (happens-before),
    // so the on-disk file is at its final size; a mismatch can only mean an
    // external truncation. Observing DOWNLOADING/PARTIALLY_DOWNLOADED keeps
    // trustSizeFromFilename false, avoiding spurious warnings during ordinary
    // in-progress reads. Placing the observation after the rename-race retry
    // ensures it reflects the state of the file that was actually opened: after
    // a retry the segment is DOWNLOADED and the truncation check correctly fires
    // if the renamed file was externally shortened.
    const auto downloadState = fileSegment.state();
    const bool trustSizeFromFilename =
        fileSegment.hasSizeInFileName()
        && (downloadState == FileSegment::State::DOWNLOADED
            || downloadState == FileSegment::State::DETACHED);

    if (trustSizeFromFilename)
    {
        const auto physicalSize = readInfo_.cacheReader->tryGetFileSize();
        if (physicalSize.has_value() && *physicalSize < fileSegment.getDownloadedSize())
        {
            // The segment is shorter than its recorded downloaded size.
            // Bypass the broken cache file so the caller re-fetches the data
            // from the remote source. The segment is intentionally left in
            // place: removing it from this read path would invalidate its
            // priority-queue entry without holding the cache priority lock,
            // which can race tryIncreasePriority (see the detailed comment in
            // CH CachedOnDiskReadBufferFromFile.cpp getCacheReadBuffer).
            LOG_WARNING(
                getLogger("FileCacheInputStream"),
                "Cache file {} is shorter than its recorded size ({} < {}); "
                "it was likely truncated outside ClickHouse. Bypassing the "
                "cache; the data will be re-fetched from the source",
                path, *physicalSize, fileSegment.getDownloadedSize());
            readInfo_.cacheReader.reset();
            return nullptr;
        }
    }

    return readInfo_.cacheReader;
}

std::shared_ptr<ReadBufferFromVeloxReadFile> FileCacheInputStream::getRemoteReadBuffer(
    FileSegment & fileSegment,
    uint64_t offset,
    ReadType readType)
{
    const size_t bufferSize = outputBufferCapacity_ > 0 ? outputBufferCapacity_ : kDefaultOutputBufferSize;
    auto makeReader = [&]() {
        return std::make_shared<ReadBufferFromVeloxReadFile>(owner_->sourceReadFile(), pool_, bufferSize);
    };

    switch (readType)
    {
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            // Each downloader downloads at most one buffer chunk and then hands
            // off. A continuing downloader reuses the reader left in the segment.
            auto reader = fileSegment.getRemoteFileReader();
            if (!reader)
            {
                reader = makeReader();
                fileSegment.setRemoteFileReader(reader);
            }
            else
            {
                VELOX_CHECK_EQ(
                    reader->getFileOffsetOfBufferEnd(),
                    fileSegment.getCurrentWriteOffset(),
                    "Reused remote reader is not positioned at the segment's current write offset");
            }
            return reader;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            // A bypass reader is owned only by this stream, so it is not shareable.
            if (readInfo_.remoteReader && offset == readInfo_.remoteReader->getFileOffsetOfBufferEnd())
                return readInfo_.remoteReader;

            auto reader = fileSegment.extractRemoteFileReader();
            if (reader && offset == reader->getFileOffsetOfBufferEnd())
                readInfo_.remoteReader = reader;
            else
                readInfo_.remoteReader = makeReader();
            return readInfo_.remoteReader;
        }
        default:
            VELOX_FAIL("Cannot use a remote reader with read type NONE/CACHED");
    }
}

std::unique_ptr<FileCacheInputStream::ReadFromFileSegmentState>
FileCacheInputStream::createReadFromFileSegmentState(FileSegment & fileSegment, uint64_t offset)
{
    auto create = [&](ReadType type, uint64_t bytesToPredownload = 0) {
        std::shared_ptr<ReadBufferFromVeloxReadFile> reader;
        switch (type)
        {
            case ReadType::CACHED:
                reader = getCacheReadBuffer(fileSegment);
                if (!reader)
                {
                    // getCacheReadBuffer detected a size-suffixed segment
                    // whose physical file is shorter than its recorded
                    // downloaded size (external truncation). Switch to bypass
                    // so the source re-fetches the data. A state with
                    // reader == nullptr and readType == CACHED would dereference
                    // null in prepareReadFromFileSegmentState.
                    type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
                    reader = getRemoteReadBuffer(fileSegment, offset, type);
                }
                break;
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
                reader = getRemoteReadBuffer(fileSegment, offset, type);
                break;
            case ReadType::NONE:
                VELOX_UNREACHABLE();
        }
        auto state = std::make_unique<ReadFromFileSegmentState>();
        state->reader = std::move(reader);
        state->readType = type;
        state->bytesToPredownload = bytesToPredownload;
        return state;
    };

    auto downloadState = fileSegment.state();
    const auto & opts = owner_->cacheOptions();

    if (opts.tempCacheOnly)
    {
        if (downloadState == FileSegment::State::DETACHED || !canStartFromCache(offset, fileSegment))
            VELOX_FAIL("Temporary data is no longer present in the cache (offset {})", offset);
        return create(ReadType::CACHED);
    }

    if (opts.readIfExistsOtherwiseBypass)
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
                if (canStartFromCache(offset, fileSegment))
                    return create(ReadType::CACHED);
                // Safe cancellation point: the caller is a pure waiter holding no
                // downloader lease. The hook lets a test observe that this stream
                // is about to wait; FileSegment::wait itself checks the token in
                // short slices and throws on cancellation.
                common::testutil::TestValue::adjust(
                    "facebook::velox::ch::FileCacheInputStream::beforeSegmentWait", this);
                downloadState = fileSegment.wait(offset, cancellationToken_);
                continue;
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
                    // This stream just won the downloader lease. No cancellation is
                    // checked between election and release (design 4.2); the hook
                    // only lets a test request cancellation mid-transaction to prove
                    // it is deferred to the next safe boundary, never interrupting a
                    // reserve/write.
                    common::testutil::TestValue::adjust(
                        "facebook::velox::ch::FileCacheInputStream::afterDownloaderElected", this);

                    if (canStartFromCache(offset, fileSegment))
                    {
                        fileSegment.resetDownloader();
                        return create(ReadType::CACHED);
                    }

                    const uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
                    uint64_t bytesToPredownload = 0;
                    if (currentWriteOffset < offset)
                        bytesToPredownload = offset - currentWriteOffset;

                    // Direct IO requires aligned source reads. The predownload of
                    // the gap [currentWriteOffset, offset) both seeks to
                    // currentWriteOffset and reads gap-sized chunks; when the
                    // source alignment cannot be satisfied there, skip the
                    // optional predownload optimization. Release the downloader and
                    // read this segment through the normal aligned bypass path at
                    // `offset` (still alignment-validated by the reader). This
                    // never silently degrades direct IO to buffered IO and never
                    // fabricates a remote size.
                    if (bytesToPredownload != 0 && directIoAlignment_ > 1
                        && (currentWriteOffset % directIoAlignment_ != 0
                            || bytesToPredownload % directIoAlignment_ != 0))
                    {
                        fileSegment.resetDownloader();
                        return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
                    }

                    return create(ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE, bytesToPredownload);
                }

                downloadState = fileSegment.state();
                continue;
            }
            case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
                if (canStartFromCache(offset, fileSegment))
                    return create(ReadType::CACHED);
                return create(ReadType::REMOTE_FS_READ_BYPASS_CACHE);
        }
    }
}

std::unique_ptr<FileCacheInputStream::ReadFromFileSegmentState>
FileCacheInputStream::prepareReadFromFileSegmentState(FileSegment & fileSegment, uint64_t offset)
{
    const auto & range = fileSegment.range();
    VELOX_CHECK(
        offset >= range.left && offset <= range.right,
        "offset {} is not within file segment {}",
        offset,
        range.toString());

    auto state = createReadFromFileSegmentState(fileSegment, offset);

    switch (state->readType)
    {
        case ReadType::CACHED:
        {
            // The cache file is local and per-segment; bound the read to the
            // bytes actually downloaded so far (in cache-file-local coordinates,
            // where offset 0 == range.left). This must NOT be the absolute segment
            // end -- a strict LocalReadFile pread would over-read a partial prefix
            // (e.g. reading 20 bytes from a 4-byte partially-downloaded segment).
            // The bound is refreshed on each chunk by updateReadStateIfNeeded so a
            // segment grown by a concurrent downloader becomes readable. The region
            // end is enforced by the last-segment resize in readFromCurrentSegment.
            state->reader->setReadUntilPosition(fileSegment.getDownloadedSize());
            state->reader->seek(static_cast<off_t>(offset - range.left), SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
        {
            // Bound the source read to the segment (min with the whole-file size
            // mirrors CH) so a multi-segment region keeps one cache entry per
            // segment.
            state->reader->setReadUntilPosition(std::min<uint64_t>(range.right + 1, owner_->fileSize()));
            state->reader->seek(static_cast<off_t>(offset), SEEK_SET);
            break;
        }
        case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
        {
            state->reader->setReadUntilPosition(std::min<uint64_t>(range.right + 1, owner_->fileSize()));
            if (state->bytesToPredownload)
                state->reader->seek(static_cast<off_t>(fileSegment.getCurrentWriteOffset()), SEEK_SET);
            else
                state->reader->seek(static_cast<off_t>(offset), SEEK_SET);
            break;
        }
        case ReadType::NONE:
            VELOX_FAIL("Read type not set");
    }

    return state;
}

void FileCacheInputStream::updateReadStateIfNeeded(FileSegment & fileSegment, uint64_t offset)
{
    if (state_->readType == ReadType::CACHED)
    {
        // Re-prepare on every chunk so the cache reader's read bound tracks the
        // downloaded prefix: a segment grown or completed by a concurrent
        // downloader after this reader opened would otherwise keep a stale frozen
        // bound and report a premature EOF for bytes now present in the cache
        // file. Re-preparing also re-opens the reader if the completed segment was
        // renamed to include its size, and switches to a remote read once the
        // reader passes the downloaded prefix (canStartFromCache becomes false at
        // offset == currentWriteOffset). getCacheReadBuffer reuses the open reader
        // by path, so a same-segment re-prepare is just a re-seek and a bound
        // refresh.
        state_ = prepareReadFromFileSegmentState(fileSegment, offset);
    }
    else if (state_->readType == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE)
    {
        // The downloader term lasts a single chunk; re-elect a downloader.
        state_.reset();
        state_ = prepareReadFromFileSegmentState(fileSegment, offset);
    }
}

bool FileCacheInputStream::writeCache(char * data, size_t size, uint64_t offset, FileSegment & fileSegment)
{
    try
    {
        {
            ProfileEventTimeIncrement<Microseconds> writeTimer(ProfileEvents::CachedReadBufferCacheWriteMicroseconds);
            fileSegment.write(data, size, offset);
        }
        // The write succeeded: one cache-write fact updates the global ledger and
        // the query IoStats free-form counter independently.
        ProfileEvents::increment(ProfileEvents::CachedReadBufferCacheWriteBytes, size);
        if (ioStats_)
            ioStats_->addCounter(
                kFileCacheWriteBytes, RuntimeCounter(static_cast<int64_t>(size), RuntimeCounter::Unit::kBytes));
    }
    catch (const FileCacheErrnoException & e)
    {
        // Space-exhaustion errors are always skipped (bypass), matching CH.
        const int code = e.getErrno();
        if (code == ENOSPC || code == EDQUOT)
            return false;
        if (owner_->fileCache().skipCacheOnDiskFailure())
            return false;
        throw;
    }
    catch (...)
    {
        if (owner_->fileCache().skipCacheOnDiskFailure())
            return false;
        throw;
    }
    return true;
}

bool FileCacheInputStream::predownloadForCurrentSegment(FileSegment & fileSegment, uint64_t /*offset*/)
{
    if (state_->bytesToPredownload == 0)
        return true;

    // Predownload the gap [currentWriteOffset, offset) into cache using the
    // already-aligned output buffer as scratch (CH shares its internal buffer for
    // predownload when it is large enough). The gap bytes are written to cache
    // before the actual read overwrites the buffer, so sharing it is safe, and it
    // satisfies any direct-IO alignment the source reader requires -- a
    // separately pool-allocated scratch would only be 64-byte aligned and could
    // violate a larger direct-IO alignment. Under direct IO the gap start and
    // length are alignment-multiples here: createReadFromFileSegmentState already
    // diverted a predownload whose alignment could not be satisfied to the bypass
    // path, so every read below stays aligned. The output buffer is allocated by
    // readNextChunk before readFromCurrentSegment, so it is always present here.
    char * const scratch = outputBufferData_;
    const size_t scratchCap = outputBufferCapacity_;

    // Restore the output buffer as the reader target when leaving predownload so
    // the actual read fills the caller-visible buffer.
    auto restore = folly::makeGuard([&] { state_->reader->set(outputBufferData_, outputBufferCapacity_); });

    while (state_->bytesToPredownload > 0)
    {
        const size_t chunk = std::min<size_t>(scratchCap, state_->bytesToPredownload);
        state_->reader->set(scratch, chunk);
        // The actual source read happens inside eof() (it calls next()/pread when
        // the buffer is empty). Time exactly that source read into the predownload
        // source-read latency counter.
        bool hasData;
        {
            ProfileEventTimeIncrement<Microseconds> predownloadTimer(
                ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds);
            hasData = !state_->reader->eof();
        }
        if (!hasData)
        {
            // EOF before the gap was filled: release the segment for waiting
            // readers with the shared reader withdrawn, then fail.
            const auto metadata = getRemoteFileMetadata();
            const uint64_t writeOffset = fileSegment.getCurrentWriteOffset();
            if (fileSegment.isDownloader())
            {
                fileSegment.resetRemoteFileReader();
                fileSegment.setDownloadFinishedWithoutContinuation();
            }
            if (isRemoteTruncationConfirmed(metadata, writeOffset))
                VELOX_FAIL(
                    "Remote object was truncated between listing and reading: size {} at offset {}",
                    metadata->size,
                    writeOffset);
            VELOX_FAIL(
                "Failed to predownload the remaining {} bytes for segment {} "
                "(remote metadata unavailable to confirm truncation)",
                state_->bytesToPredownload,
                fileSegment.getInfoForLog());
        }

        const size_t got = state_->reader->available();

        // Predownloaded gap bytes were just fetched from source. Being physical
        // source bytes, they update the global source-read ledger
        // (CachedReadBufferReadFromSourceBytes) just like an ordinary source read,
        // plus BOTH the global predownload counters and the query read()/prefetch()
        // counters, but NEVER rawBytesRead: predownload fills the cache and is not
        // returned to the caller, so counting it as raw input bytes would
        // double-count the gap. The logical returned bytes are accounted exactly
        // once on the cache/source return in readFromCurrentSegment.
        ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, got);
        ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedBytes, got);
        ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedFromSourceBytes, got);
        if (ioStatistics_)
        {
            ioStatistics_->read().increment(got);
            ioStatistics_->prefetch().increment(got);
        }

        const uint64_t currentWriteOffset = fileSegment.getCurrentWriteOffset();
        std::string failureReason;
        const uint64_t reserveHint = readInfo_.readUntilPosition - currentWriteOffset;
        bool ok = fileSegment.reserve(
            got, owner_->cacheOptions().reserveSpaceWaitLockTimeoutMs, failureReason, nullptr, reserveHint);
        if (ok)
            ok = writeCache(state_->reader->buffer().begin(), got, currentWriteOffset, fileSegment);

        if (!ok)
        {
            // Reservation or write failed: bypass the cache for this segment.
            state_->bytesToPredownload = 0;
            fileSegment.resetRemoteFileReader();
            fileSegment.completePartAndResetDownloader();
            state_->readType = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
            return false;
        }

        state_->reader->position() += got;
        state_->bytesToPredownload -= got;
    }

    return true;
}

size_t FileCacheInputStream::readFromCurrentSegment(
    FileSegment & fileSegment,
    uint64_t offset,
    bool & readerCanBeReused)
{
    const auto & range = fileSegment.range();
    size_t size = 0;

    if (state_->bytesToPredownload)
    {
        if (!predownloadForCurrentSegment(fileSegment, offset))
        {
            // Predownload switched us to bypass; rebuild the reader as a bypass
            // reader borrowing the output buffer.
            auto reader = getRemoteReadBuffer(fileSegment, offset, ReadType::REMOTE_FS_READ_BYPASS_CACHE);
            reader->setReadUntilPosition(std::min<uint64_t>(range.right + 1, owner_->fileSize()));
            reader->seek(static_cast<off_t>(offset), SEEK_SET);
            state_->reader = reader;
            state_->reader->set(outputBufferData_, outputBufferCapacity_);
        }
    }

    const bool doDownload = state_->readType == ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE;
    const bool isCacheRead = state_->readType == ReadType::CACHED;

    if (isCacheRead)
    {
        ProfileEventTimeIncrement<Microseconds> cacheTimer(ProfileEvents::CachedReadBufferReadFromCacheMicroseconds);
        if (state_->reader->next())
            size = state_->reader->available();
    }
    else
    {
        ProfileEventTimeIncrement<Microseconds> sourceTimer(ProfileEvents::CachedReadBufferReadFromSourceMicroseconds);
        if (state_->reader->next())
            size = state_->reader->available();
        if (ioStatistics_)
            ioStatistics_->incTotalScanTimeNs(static_cast<int64_t>(sourceTimer.elapsed()) * 1000);
    }

    // Physical I/O accounting, matching ClickHouse: recorded immediately after
    // next() determines the physical `size`, before the cache write and before
    // the final last-segment clamp. Hit/miss and physical bytes reflect what was
    // actually read from the local cache or from the source -- not the (possibly
    // smaller, post-clamp) logical bytes returned to the caller. A cache hit maps
    // to ssdRead; a source read (miss) maps to read(). Hit/miss is counted per
    // physical read, including a zero-byte EOF read, and never at reader
    // construction, so a reused bypass reader records one miss per physical read.
    if (isCacheRead)
    {
        ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheHits);
        ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheBytes, size);
        if (ioStatistics_)
            ioStatistics_->ssdRead().increment(size);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheMisses);
        ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, size);
        if (ioStatistics_)
            ioStatistics_->read().increment(size);
    }

    if (size)
    {
        if (doDownload)
        {
            std::string failureReason;
            const uint64_t reserveHint = readInfo_.readUntilPosition - offset;
            bool success = fileSegment.reserve(
                size, owner_->cacheOptions().reserveSpaceWaitLockTimeoutMs, failureReason, nullptr, reserveHint);
            if (success)
                success = writeCache(state_->reader->buffer().begin(), size, offset, fileSegment);

            if (success)
                readerCanBeReused = true;
            else
                state_->readType = ReadType::REMOTE_FS_READ_BYPASS_CACHE;
        }

        // Last segment: clamp the returned size to the region end. The full
        // physical read is still accounted (above) and written to cache (above);
        // only the logical bytes returned to the caller are clamped here.
        if (readInfo_.fileSegments->size() == 1)
        {
            const uint64_t remaining =
                std::min<uint64_t>(range.right, readInfo_.readUntilPosition - 1) - offset + 1;
            if (size > remaining)
            {
                size = remaining;
                state_->reader->buffer().resize(size);
            }
        }

        // Logical bytes returned to the caller: after the cache write and the
        // final clamp, only rawBytesRead records the bytes actually handed back.
        // Predownload never reaches this point, so it never touches rawBytesRead.
        if (ioStatistics_)
            ioStatistics_->incRawBytesRead(static_cast<int64_t>(size));
    }

    if (size == 0 && offset < readInfo_.readUntilPosition)
    {
        // Zero bytes but the region is not finished: the source was exhausted
        // before the end of the requested range. Release the segment (so waiting
        // readers can take over) with the shared reader withdrawn, then fail.
        const auto metadata = getRemoteFileMetadata();
        if (fileSegment.isDownloader())
        {
            fileSegment.resetRemoteFileReader();
            fileSegment.setDownloadFinishedWithoutContinuation();
        }
        if (isRemoteTruncationConfirmed(metadata, offset))
            VELOX_FAIL(
                "Remote object was truncated between listing and reading: size {} at offset {}",
                metadata->size,
                offset);
        VELOX_FAIL(
            "Cannot read all data: the source was exhausted at offset {} before the end of the "
            "requested range {} (remote metadata unavailable to confirm truncation). Segment: {}",
            offset,
            readInfo_.readUntilPosition,
            fileSegment.getInfoForLog());
    }

    return size;
}

bool FileCacheInputStream::completeCurrentSegmentAndAdvance(uint64_t nextOffset)
{
    state_.reset();
    readInfo_.cacheReader.reset();
    readInfo_.remoteReader.reset();

    readInfo_.fileSegments->completeAndPopFront(
        owner_->cacheOptions().allowBackgroundDownload, /*force_shrink_to_downloaded_size=*/false);

    // Safe cancellation point: the just-read segment is completed and its
    // downloader was already released in readNextChunk, so no lease or in-flight
    // reserve/write is held (design 4.2).
    if (cancellationToken_.isCancellationRequested())
        VELOX_FAIL("FileCache read cancelled after completing a segment");

    if (readInfo_.fileSegments->empty() && !nextFileSegmentsBatch(nextOffset))
        return false;

    // Do NOT eagerly elect a downloader for the next segment here. The next chunk
    // read prepares the front segment, avoiding an elect-then-release cycle that
    // would leave a segment DOWNLOADING and trip `wait()`'s downloader assertion.
    return true;
}

void FileCacheInputStream::releaseDownloaderIfNeeded()
{
    if (readInfo_.fileSegments && !readInfo_.fileSegments->empty())
    {
        auto & fileSegment = readInfo_.fileSegments->front();
        if (fileSegment.isDownloader())
        {
            fileSegment.resetRemoteFileReader();
            fileSegment.completePartAndResetDownloader();
        }
    }
}

std::optional<FileCacheInputStream::RemoteFileMetadata> FileCacheInputStream::getRemoteFileMetadata() const
{
    // This Velox port has no per-request remote-object metadata provider (unlike
    // CH's ReadBufferFromS3). The source ReadFile::size() is the size discovered
    // when the reader was opened, not a fresh re-stat, so it cannot serve as
    // truncation evidence. Return nullopt to avoid inventing a size/boundary.
    return std::nullopt;
}

bool FileCacheInputStream::isRemoteTruncationConfirmed(
    const std::optional<RemoteFileMetadata> & metadata,
    uint64_t offset)
{
    // A truncation boundary is known only when metadata is present and the
    // object's size equals the failing offset. A nullopt (this port's default)
    // is not evidence of a real or fabricated file size.
    return metadata.has_value() && metadata->size == offset;
}

size_t FileCacheInputStream::readNextChunk()
{
    if (region_.length == 0)
        return 0;

    const uint64_t offset = absolutePosition();
    const uint64_t readUntil = FileCacheUtils::checkedAdd(region_.offset, region_.length, "file cache read-until");
    if (offset >= readUntil)
        return 0;

    initializeIfNeeded(offset);

    if ((!readInfo_.fileSegments || readInfo_.fileSegments->empty()) && !nextFileSegmentsBatch(offset))
        return 0;

    allocateOutputBufferIfNeeded();

    // Advance past an exhausted segment and (re)prepare the read state at
    // `offset`.
    if (state_ && state_->reader)
    {
        FileSegment & front = readInfo_.fileSegments->front();
        if (offset > front.range().right)
        {
            if (!completeCurrentSegmentAndAdvance(offset))
                return 0;
        }
        else
        {
            updateReadStateIfNeeded(front, offset);
        }
    }
    if (!state_ || !state_->reader)
    {
        FileSegment & front = readInfo_.fileSegments->front();
        front.increasePriority();
        state_ = prepareReadFromFileSegmentState(front, offset);
    }

    FileSegment & fileSegment = readInfo_.fileSegments->front();
    bool readerCanBeReused = false;
    size_t size = 0;
    try
    {
        state_->reader->set(outputBufferData_, outputBufferCapacity_);
        size = readFromCurrentSegment(fileSegment, offset, readerCanBeReused);
        // Normal path: un-borrow the output buffer from the reader (handoff).
        state_->reader->set(nullptr, 0);
    }
    catch (...)
    {
        // Never return the canceled reader to the FileSegment. Drop our reference
        // to the (possibly shared) reader, then release the downloader with the
        // reader withdrawn, and propagate the exception.
        state_.reset();
        if (fileSegment.isDownloader())
        {
            fileSegment.resetRemoteFileReader();
            fileSegment.completePartAndResetDownloader();
        }
        throw;
    }

    // Release the just-read segment's downloader after its single-chunk term
    // (CH nextImplStep releases before advancing). On a successful download the
    // reader is left in the segment for the next downloader (the handoff); on a
    // non-reusable read it is withdrawn.
    if (state_ && state_->readType != ReadType::CACHED && fileSegment.isDownloader())
    {
        if (!readerCanBeReused)
            fileSegment.resetRemoteFileReader();
        else if (state_->reader)
            // The reader stays in the segment for reuse -- possibly by an
            // asynchronous background-download worker that outlives this query.
            // Free its owned buffer (never used: reads always target an external
            // buffer) so it retains no memory charged to the query-scoped pool,
            // whose teardown would otherwise race the worker's reader destruction.
            state_->reader->releaseOwnedBuffer();
        fileSegment.completePartAndResetDownloader();
    }

    // Publish the chunk window (region-relative).
    outputBufferStart_ = position_;
    outputBufferSize_ = size;
    offsetInOutputBuffer_ = 0;

    // Advance across the segment boundary if this read consumed it.
    if (size > 0)
    {
        const uint64_t newEnd = FileCacheUtils::checkedAdd(offset, size, "file cache buffer end");
        if (newEnd > fileSegment.range().right)
            completeCurrentSegmentAndAdvance(newEnd);
    }

    return size;
}

} // namespace facebook::velox::ch
