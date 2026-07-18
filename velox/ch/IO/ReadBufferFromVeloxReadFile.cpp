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

#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"

#include "velox/ch/Common/FileCacheException.h"

#include <algorithm>
#include <limits>

namespace facebook::velox::ch
{

ReadBufferFromVeloxReadFile::ReadBufferFromVeloxReadFile(
    std::shared_ptr<velox::ReadFile> readFile,
    size_t bufferSize)
    : ownedReadFile_(std::move(readFile))
{
    readFile_ = ownedReadFile_.get();
    VELOX_CHECK_NOT_NULL(readFile_);
    VELOX_CHECK_GT(bufferSize, 0u, "ReadBuffer size must be > 0");
    initBuffer(bufferSize);
    readUntil_ = readFile_->size();
}

ReadBufferFromVeloxReadFile::ReadBufferFromVeloxReadFile(
    velox::ReadFile * readFile,
    size_t bufferSize)
    : readFile_(readFile)
{
    VELOX_CHECK_NOT_NULL(readFile_);
    VELOX_CHECK_GT(bufferSize, 0u, "ReadBuffer size must be > 0");
    initBuffer(bufferSize);
    readUntil_ = readFile_->size();
}

void ReadBufferFromVeloxReadFile::initBuffer(size_t bufferSize)
{
    internalBuffer_.resize(bufferSize);
    bufData_ = internalBuffer_.data();
    bufCapacity_ = bufferSize;
    pos_ = bufData_;
    bufEnd_ = bufData_;
    externalBuffer_ = false;
}

bool ReadBufferFromVeloxReadFile::next()
{
    if (atEof_)
    {
        resetToInternalWindow();
        return false;
    }
    if (currentOffset_ >= readUntil_)
    {
        atEof_ = true;
        resetToInternalWindow();
        return false;
    }

    // Select the destination for this read cycle without committing it yet: the
    // caller-owned buffer installed by set() (used exactly once) or the internal
    // allocation. Only a successful read publishes it as the active window, so
    // every non-success path can restore a coherent internal window below.
    char * const dest = externalBuffer_ ? bufData_ : internalBuffer_.data();
    const size_t destCapacity =
        externalBuffer_ ? bufCapacity_ : internalBuffer_.size();

    const size_t startOffset = currentOffset_;
    // The capacity bounds the read so pread always stays within dest.
    const size_t toRead = std::min(destCapacity, readUntil_ - currentOffset_);

    std::string_view chunk;
    try
    {
        chunk = readFile_->pread(startOffset, toRead, dest);
    }
    catch (...)
    {
        // A failed read ends the external-buffer lifetime like any other next()
        // attempt. Restore a coherent internal window before rethrowing so a
        // retry never reads into caller memory that may already be released.
        resetToInternalWindow();
        throw;
    }

    if (chunk.empty())
    {
        atEof_ = true;
        resetToInternalWindow();
        return false;
    }

    // Success: publish dest as the active window.
    bufData_ = dest;
    bufCapacity_ = destCapacity;
    bufStartOffset_ = startOffset;
    pos_ = dest;
    bufEnd_ = dest + chunk.size();
    // The external buffer, if any, has now been consumed by this read cycle.
    externalBuffer_ = false;
    return true;
}

void ReadBufferFromVeloxReadFile::resetToInternalWindow()
{
    // Disarm any external buffer and present a coherent empty internal window at
    // the current offset. currentOffset_ is preserved so the next successful
    // next() reloads from the same position.
    bufData_ = internalBuffer_.data();
    bufCapacity_ = internalBuffer_.size();
    pos_ = bufData_;
    bufEnd_ = bufData_;
    bufStartOffset_ = currentOffset_;
    externalBuffer_ = false;
}

void ReadBufferFromVeloxReadFile::advance(ptrdiff_t n)
{
    VELOX_CHECK_GE(n, 0, "Cannot advance by a negative amount");
    // Compare the signed count against the remaining window before forming
    // pos_ + n, so an oversized or extreme count never builds an out-of-range
    // pointer.
    const ptrdiff_t available = bufEnd_ - pos_;
    VELOX_CHECK_LE(n, available, "advance past buffer end");
    pos_ += n;
    currentOffset_ = bufStartOffset_ + static_cast<size_t>(pos_ - bufData_);
}

off_t ReadBufferFromVeloxReadFile::getPosition() const
{
    return static_cast<off_t>(currentOffset_);
}

off_t ReadBufferFromVeloxReadFile::getFileOffsetOfBufferEnd() const
{
    return static_cast<off_t>(
        bufStartOffset_ + static_cast<size_t>(bufEnd_ - bufData_));
}

void ReadBufferFromVeloxReadFile::seek(off_t offset, int whence)
{
    off_t newPosition{0};
    if (whence == SEEK_SET)
    {
        VELOX_CHECK_GE(offset, 0, "Cannot seek to a negative offset");
        newPosition = offset;
    }
    else if (whence == SEEK_CUR)
    {
        const off_t current = getPosition();
        // Reject positive overflow before the addition. A negative offset added
        // to a non-negative position can never overflow, so it only needs the
        // "before start" check below.
        if (offset >= 0)
            VELOX_CHECK_LE(
                offset,
                std::numeric_limits<off_t>::max() - current,
                "Seek offset overflows the maximum file position");
        newPosition = current + offset;
        VELOX_CHECK_GE(
            newPosition, 0, "Cannot seek before the start of the file");
    }
    else
        throwFileCacheException("Unsupported seek whence: {}", whence);

    currentOffset_ = static_cast<size_t>(newPosition);
    // A seek ends any armed external-buffer cycle (see set()'s contract): revert
    // to the internal buffer so the next next() never reads into caller-owned
    // storage the caller may already have released.
    bufData_ = internalBuffer_.data();
    bufCapacity_ = internalBuffer_.size();
    externalBuffer_ = false;
    // Invalidate the buffer so the next next() reads from currentOffset_.
    pos_ = bufData_;
    bufEnd_ = bufData_;
    bufStartOffset_ = currentOffset_;
    atEof_ = false;
}

void ReadBufferFromVeloxReadFile::setReadUntilPosition(size_t filePos)
{
    readUntil_ = filePos;
    // Update EOF in both directions: shrinking the boundary to at or below the
    // current offset marks EOF, while extending it beyond the current offset
    // clears EOF so next() can resume from currentOffset_.
    atEof_ = currentOffset_ >= readUntil_;

    // Constrain an already-loaded window so bufferEnd never exposes bytes at or
    // beyond the new exclusive boundary. Work in file-offset space to avoid
    // forming out-of-range pointers.
    const size_t windowEndOffset =
        bufStartOffset_ + static_cast<size_t>(bufEnd_ - bufData_);
    if (atEof_)
        // Boundary at or behind the current position: expose an empty window at
        // the current offset (pos_ already tracks currentOffset_).
        bufEnd_ = pos_;
    else if (readUntil_ < windowEndOffset)
        // Boundary inside the loaded window: clamp bufEnd_ to it.
        bufEnd_ = bufData_ + (readUntil_ - bufStartOffset_);
}

void ReadBufferFromVeloxReadFile::set(char * data, size_t size)
{
    VELOX_CHECK_NOT_NULL(data, "External buffer must not be null");
    VELOX_CHECK_GT(size, 0u, "External buffer capacity must be > 0");
    bufData_ = data;
    bufCapacity_ = size;
    pos_ = data;
    // Not yet filled; the next next() fills the external buffer.
    bufEnd_ = data;
    bufStartOffset_ = currentOffset_;
    externalBuffer_ = true;
}

std::string ReadBufferFromVeloxReadFile::getFileName() const
{
    return readFile_ ? readFile_->getName() : std::string{};
}

} // namespace facebook::velox::ch
