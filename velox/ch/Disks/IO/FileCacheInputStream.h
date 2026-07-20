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
#include "velox/common/file/Region.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/SeekableInputStream.h"

#include <memory>

namespace facebook::velox::ch
{

class FileCacheBufferedInput;

/// Streaming read state machine for a single region, ported from ClickHouse
/// `CachedOnDiskReadBufferFromFile`. All position-related stream API is
/// region-relative; all `FileCache`/`FileSegment`/`ReadFile` calls use absolute
/// file offsets (`absolutePosition() = region_.offset + position_`).
class FileCacheInputStream : public dwio::common::SeekableInputStream
{
public:
    FileCacheInputStream(
        FileCacheBufferedInput * owner,
        velox::common::Region region,
        FileCacheRequestContext cacheContext,
        dwio::common::LogType logType,
        QueryStatus queryStatus = {});

    ~FileCacheInputStream() override;

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
        // Owned scratch used when the reader's own internal buffer is too small
        // for predownload (mirrors CH `predownload_memory`).
        velox::BufferPtr predownloadBuffer;
    };

    static std::string toString(ReadType type);

    void initializeIfNeeded();
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
    bool writeCache(char * data, size_t size, uint64_t offset, FileSegment & fileSegment);
    bool completeCurrentSegmentAndAdvance();
    void releaseDownloaderIfNeeded(FileSegment & fileSegment, bool readerCanBeReused);

    // Ensure the owned output buffer holds at least `bytes` usable bytes.
    char * ensureOutputBuffer(size_t bytes);

    uint64_t absolutePosition() const;

    FileCacheBufferedInput * owner_;
    velox::common::Region region_;
    FileCacheRequestContext cacheContext_;
    // Task 017: query cancellation. Default-constructed = never cancels. Checked
    // only at safe points where no downloader lease is held (see Next / Step 7).
    QueryStatus queryStatus_;
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

    ReadInfo readInfo_;
    std::unique_ptr<ReadFromFileSegmentState> state_;
    bool initialized_ = false;
};

} // namespace facebook::velox::ch
