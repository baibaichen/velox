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

#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/QueryLimit.h"

#include "velox/common/file/Region.h"
#include "velox/common/memory/Memory.h"
#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace facebook::velox::ch
{

class FileCacheBufferedInput;

/// Streaming read state machine for one region of the ClickHouse `FileCache`,
/// ported from CH `CachedOnDiskReadBufferFromFile`. It is the Velox
/// `SeekableInputStream` returned by `FileCacheBufferedInput::enqueue`/`read`.
///
/// Coordinate invariant: `ByteCount`/`BackUp`/`SkipInt64`/`seekToPosition`
/// operate in region-relative coordinates; every `FileCache`/`FileSegment`/
/// `ReadFile` offset is absolute (`region.offset + relative`), converted with
/// the shared `FileCacheUtils::checkedAdd`.
///
/// Owner lifetime: the stream keeps a non-owning `owner_` pointer to the
/// `FileCacheBufferedInput` that created it and dereferences it on every
/// `Next`/`seekToPosition` for the cache, key, origin, options, source file, and
/// memory pool. The owner must outlive the stream; the caller (Velox reader
/// stack) is responsible for that ordering, exactly as for other
/// `BufferedInput` streams. The stream never extends the owner's lifetime.
class FileCacheInputStream : public dwio::common::SeekableInputStream
{
public:
    FileCacheInputStream(
        FileCacheBufferedInput * owner,
        velox::common::Region region,
        FileCacheRequestContext cacheContext,
        dwio::common::LogType logType);

    ~FileCacheInputStream() override;

    bool Next(const void ** data, int * size) override;
    void BackUp(int count) override;
    bool SkipInt64(int64_t count) override;
    int64_t ByteCount() const override;
    void seekToPosition(dwio::common::PositionProvider & position) override;
    std::string getName() const override;
    size_t positionSize() const override;

    /// Remote-object metadata equivalent to CH's `getRemoteFileMetadata`. In this
    /// Velox port there is no per-request remote-object metadata provider (unlike
    /// CH's `ReadBufferFromS3`), so a boundary is "known" only when a metadata
    /// value is supplied; otherwise the truncation boundary is unknown.
    struct RemoteFileMetadata
    {
        uint64_t size = 0;
    };

    /// Decision helper for a zero-byte read that did not reach the end of the
    /// requested range. Returns true only when remote metadata is present and the
    /// object's size equals the failing offset (a known truncation boundary).
    /// A `std::nullopt` metadata (the default in this port) is not evidence of a
    /// real or fabricated file size, so it returns false. Exposed as a static so
    /// both the metadata-present and metadata-absent branches can be verified.
    static bool isRemoteTruncationConfirmed(
        const std::optional<RemoteFileMetadata> & metadata,
        uint64_t offset);

private:
    struct ReadInfo
    {
        FileSegmentsHolderPtr fileSegments;
        std::shared_ptr<ReadBufferFromVeloxReadFile> remoteReader;
        std::shared_ptr<ReadBufferFromVeloxReadFile> cacheReader;
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
        std::shared_ptr<ReadBufferFromVeloxReadFile> reader;
        ReadType readType = ReadType::NONE;
        uint64_t bytesToPredownload = 0;
    };

    void initializeIfNeeded(uint64_t offset);
    bool nextFileSegmentsBatch(uint64_t offset);

    // The core of a single Next refill: reads at most one output-buffer chunk
    // through the FileCache state machine. Returns the number of bytes placed at
    // the front of `outputBuffer_` (0 at end of region).
    size_t readNextChunk();

    std::unique_ptr<ReadFromFileSegmentState> prepareReadFromFileSegmentState(
        FileSegment & fileSegment,
        uint64_t offset);

    std::unique_ptr<ReadFromFileSegmentState> createReadFromFileSegmentState(
        FileSegment & fileSegment,
        uint64_t offset);

    std::shared_ptr<ReadBufferFromVeloxReadFile> getCacheReadBuffer(
        const FileSegment & fileSegment);

    std::shared_ptr<ReadBufferFromVeloxReadFile> getRemoteReadBuffer(
        FileSegment & fileSegment,
        uint64_t offset,
        ReadType readType);

    bool canStartFromCache(
        uint64_t offset,
        const FileSegment & fileSegment) const;

    // CH updateImplementationBufferIfNeeded: re-elect the downloader/re-prepare the
    // reader when the current segment must continue past its downloaded prefix.
    void updateReadStateIfNeeded(FileSegment & fileSegment, uint64_t offset);

    size_t readFromCurrentSegment(
        FileSegment & fileSegment,
        uint64_t offset,
        bool & readerCanBeReused);
    bool predownloadForCurrentSegment(FileSegment & fileSegment, uint64_t offset);
    bool writeCache(char * data, size_t size, uint64_t offset, FileSegment & fileSegment);

    // CH completeFileSegmentAndGetNext: complete the current segment and prepare
    // the next one at `nextOffset`. Returns false at the end of the batch/region.
    bool completeCurrentSegmentAndAdvance(uint64_t nextOffset);
    void releaseDownloaderIfNeeded();

    // Remote-object metadata provider (see RemoteFileMetadata). Returns nullopt:
    // this port has no per-request remote-object metadata source, so truncation
    // must never be inferred from a fabricated size.
    std::optional<RemoteFileMetadata> getRemoteFileMetadata() const;

    uint64_t absolutePosition() const;

    // Non-owning back-pointer to the creating FileCacheBufferedInput; it supplies
    // the cache, key, origin, options, source file, and pool on every read. The
    // owner must outlive this stream (see the class-level lifetime contract).
    FileCacheBufferedInput * owner_;
    velox::common::Region region_;
    FileCacheRequestContext cacheContext_;
    // Acquired once in the constructor; never reset by seekToPosition; destroyed
    // after readInfo_ so segment completions during teardown still see it alive.
    FileCache::QueryContextHolderPtr queryContextHolder_;
    dwio::common::LogType logType_;

    velox::memory::MemoryPool * pool_;
    // Power-of-two alignment required by a direct-IO source (1 = none). The
    // output buffer handed to the reader via `set` must satisfy it.
    uint64_t directIoAlignment_ = 1;

    void allocateOutputBufferIfNeeded();

    // All positions below are region-relative.
    uint64_t position_ = 0;
    uint64_t outputBufferStart_ = 0;
    velox::BufferPtr outputBuffer_;
    // Aligned usable start within outputBuffer_ (== outputBuffer_ data when no
    // direct-IO over-alignment is required).
    char * outputBufferData_ = nullptr;
    size_t outputBufferCapacity_ = 0;
    size_t offsetInOutputBuffer_ = 0;
    size_t outputBufferSize_ = 0;

    ReadInfo readInfo_;
    std::unique_ptr<ReadFromFileSegmentState> state_;
    bool initialized_ = false;
};

} // namespace facebook::velox::ch
