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
#include <cstdint>
#include <limits>

namespace facebook::velox::ch
{

// ---------------------------------------------------------------------------
// FileCacheBufferState
// ---------------------------------------------------------------------------

void FileCacheBufferState::allocateOwned(
    velox::memory::MemoryPool * pool,
    size_t size,
    size_t alignment)
{
    VELOX_CHECK_NOT_NULL(pool);
    VELOX_CHECK_GT(size, 0u, "Owned buffer size must be > 0");
    ownedPool_ = pool;

    if (alignment <= 1)
    {
        ownedBuffer_ = AlignedBuffer::allocate<char>(size, pool);
        ownedBegin_ = ownedBuffer_->asMutable<char>();
        ownedCapacity_ = size;
    }
    else
    {
        // Direct IO needs the address and the length aligned, but a pool
        // allocation only guarantees its own (smaller) alignment. Over-allocate
        // so the usable range can be aligned up and its length rounded up to a
        // multiple of `alignment`.
        const size_t rounded = ((size + alignment - 1) / alignment) * alignment;
        const size_t allocSize = rounded + alignment;
        ownedBuffer_ = AlignedBuffer::allocate<char>(allocSize, pool);
        auto * raw = ownedBuffer_->asMutable<char>();
        const auto addr = reinterpret_cast<uintptr_t>(raw);
        const auto alignedAddr =
            (addr + alignment - 1) & ~(static_cast<uintptr_t>(alignment) - 1);
        ownedBegin_ = reinterpret_cast<char *>(alignedAddr);
        ownedCapacity_ = rounded;
    }

    // Install the full owned range as both the internal and working buffer with
    // the cursor at the start, mirroring BufferBase(ptr, size, 0). Read adapters
    // then empty the working view; write adapters keep it full to write into.
    internal_ = CacheBuffer(ownedBegin_, ownedBegin_ + ownedCapacity_);
    working_ = CacheBuffer(ownedBegin_, ownedBegin_ + ownedCapacity_);
    position_ = ownedBegin_;
}

void FileCacheBufferState::set(char * ptr, size_t size, size_t offset)
{
    if (ptr == nullptr)
    {
        // A null buffer is only the detach signal; it never carries a nonzero
        // extent or cursor. Represent it as a coherent empty state without any
        // null-pointer arithmetic.
        VELOX_CHECK_EQ(size, 0u, "A null buffer requires size 0");
        VELOX_CHECK_EQ(offset, 0u, "A null buffer requires offset 0");
        internal_ = CacheBuffer();
        working_ = CacheBuffer();
        position_ = nullptr;
        return;
    }

    // Reject an out-of-range cursor before computing `ptr + offset`.
    VELOX_CHECK_LE(offset, size, "set() offset exceeds the buffer size");
    internal_ = CacheBuffer(ptr, ptr + size);
    working_ = CacheBuffer(ptr, ptr + size);
    position_ = ptr + offset;
}

void FileCacheBufferState::restoreOwnedWindow()
{
    if (ownedBuffer_ != nullptr)
    {
        internal_ = CacheBuffer(ownedBegin_, ownedBegin_ + ownedCapacity_);
        working_ = CacheBuffer(ownedBegin_, ownedBegin_);
        position_ = ownedBegin_;
    }
    else
    {
        internal_ = CacheBuffer();
        working_ = CacheBuffer();
        position_ = nullptr;
    }
}

void FileCacheBufferState::detach()
{
    // Drop every working/internal view and cursor into a coherent empty state
    // that references no caller memory. The owned BufferPtr, if any, stays
    // charged to the pool until this state is destroyed, but is no longer
    // exposed through the working view.
    internal_ = CacheBuffer();
    working_ = CacheBuffer();
    position_ = nullptr;
}

void FileCacheBufferState::resetWorkingView()
{
    working_ = CacheBuffer(internal_.begin(), internal_.begin());
    position_ = internal_.begin();
}

// ---------------------------------------------------------------------------
// ReadBufferFromFileBase
// ---------------------------------------------------------------------------

bool ReadBufferFromFileBase::next()
{
    VELOX_CHECK(!hasPendingData(), "next() called while the buffer still has pending data");
    VELOX_CHECK(!canceled_, "Cannot read from a canceled ReadBuffer");

    // Settle the bytes consumed from the previous working buffer before loading
    // the next chunk, mirroring ReadBuffer::next (bytes += offset()).
    state_.settleConsumed();

    bool res = false;
    try
    {
        res = nextImpl();
    }
    catch (...)
    {
        // A read failure cancels the reader; the canceled state is terminal so a
        // second next() is rejected above rather than silently retried. The
        // original exception propagates unchanged.
        cancel();
        throw;
    }

    if (!res)
    {
        // Publish an empty working view at the current position.
        char * const p = position();
        buffer() = CacheBuffer(p, p);
    }
    else
    {
        position() = buffer().begin();
        VELOX_CHECK(
            position() < buffer().end(),
            "nextImpl reported success but published an empty working buffer");
    }
    return res;
}

bool ReadBufferFromFileBase::nextImpl()
{
    char * const dest = internalBuffer().begin();
    const size_t destCapacity = internalBuffer().size();
    if (dest == nullptr || destCapacity == 0)
        // The reader is detached (no read target); there is nothing to load.
        return false;

    const size_t startOffset = fileOffsetOfBufferEnd_;
    if (startOffset >= readUntil_)
        // The right boundary has been reached.
        return false;

    const size_t toRead = std::min(destCapacity, readUntil_ - startOffset);
    // Fail closed before issuing an actual pread when direct IO is required.
    checkDirectIoRead(dest, startOffset, toRead);
    const size_t bytesRead = readInto(startOffset, dest, toRead);
    if (bytesRead == 0)
        // Physical end of input.
        return false;

    fileOffsetOfBufferEnd_ = startOffset + bytesRead;
    buffer() = CacheBuffer(dest, dest + bytesRead);
    return true;
}

void ReadBufferFromFileBase::checkDirectIoRead(
    const char * dest,
    size_t startOffset,
    size_t length) const
{
    if (directIoAlignment_ <= 1)
        return;

    const size_t alignment = directIoAlignment_;
    VELOX_CHECK_EQ(
        reinterpret_cast<uintptr_t>(dest) % alignment,
        0u,
        "Direct-IO read destination address violates the required alignment");
    VELOX_CHECK_EQ(
        startOffset % alignment,
        0u,
        "Direct-IO read offset violates the required alignment");
    VELOX_CHECK_EQ(
        length % alignment,
        0u,
        "Direct-IO read length violates the required alignment "
        "(unaligned file tail or right bound)");
}

void ReadBufferFromFileBase::set(char * ptr, size_t size)
{
    if (ptr == nullptr)
    {
        VELOX_CHECK_EQ(size, 0u, "Detaching set() requires a size of 0");
        // Detach every caller pointer and restore a coherent empty window over
        // the owned buffer, preserving the current file offset.
        state_.restoreOwnedWindow();
        return;
    }

    if (directIoAlignment_ > 1)
    {
        VELOX_CHECK_EQ(
            reinterpret_cast<uintptr_t>(ptr) % directIoAlignment_,
            0u,
            "External buffer address violates the direct-IO alignment");
        VELOX_CHECK_EQ(
            size % directIoAlignment_,
            0u,
            "External buffer length violates the direct-IO alignment");
        // Validate the offset the next read will actually use (the buffer end),
        // which equals getPosition() once the working view is drained.
        VELOX_CHECK_EQ(
            fileOffsetOfBufferEnd_ % directIoAlignment_,
            0u,
            "Read offset violates the direct-IO alignment");
    }

    // Install caller memory as both the internal and (empty) working buffer so
    // the next read fills it directly. The target persists until another set().
    state_.set(ptr, size, 0);
    state_.workingBuffer().resize(0);
}

off_t ReadBufferFromFileBase::seek(off_t offset, int whence)
{
    off_t newPosition = 0;
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
        VELOX_CHECK_GE(newPosition, 0, "Cannot seek before the start of the file");
    }
    else
    {
        throwFileCacheException("Unsupported seek whence: {}", whence);
    }

    // Fail closed on an unaligned direct-IO target before mutating any reader
    // state, so a rejected seek leaves the position and loaded window untouched.
    if (directIoAlignment_ > 1)
        VELOX_CHECK_EQ(
            static_cast<size_t>(newPosition) % directIoAlignment_,
            0u,
            "Direct-IO seek offset violates the required alignment");

    fileOffsetOfBufferEnd_ = static_cast<size_t>(newPosition);
    // Discard the loaded window but keep the current read target so a set()
    // target persists across a seek, then the next read starts from here.
    state_.resetWorkingView();
    return newPosition;
}

void ReadBufferFromFileBase::setReadUntilPosition(size_t position)
{
    readUntil_ = position;

    // File offset that corresponds to the cursor.
    const size_t currentPosition = fileOffsetOfBufferEnd_ - available();

    if (readUntil_ <= currentPosition)
    {
        // Boundary at or behind the cursor: expose an empty window at the cursor
        // and drop everything from the cursor onwards.
        buffer().resize(offset());
        fileOffsetOfBufferEnd_ = currentPosition;
    }
    else if (readUntil_ < fileOffsetOfBufferEnd_)
    {
        // Boundary inside the loaded window past the cursor: clamp the window
        // end so it never exposes bytes at or beyond the boundary.
        const size_t shrink = fileOffsetOfBufferEnd_ - readUntil_;
        buffer().resize(buffer().size() - shrink);
        fileOffsetOfBufferEnd_ = readUntil_;
    }
    // Otherwise the boundary is at or beyond the loaded window; extending it
    // lets a later next() resume, and nextImpl re-derives the real end of input.
}

// ---------------------------------------------------------------------------
// ReadBufferFromVeloxReadFile
// ---------------------------------------------------------------------------

ReadBufferFromVeloxReadFile::ReadBufferFromVeloxReadFile(
    std::shared_ptr<velox::ReadFile> readFile,
    velox::memory::MemoryPool * pool,
    size_t bufferSize)
    : ownedReadFile_(std::move(readFile))
{
    readFile_ = ownedReadFile_.get();
    initialize(pool, bufferSize);
}

ReadBufferFromVeloxReadFile::ReadBufferFromVeloxReadFile(
    velox::ReadFile * readFile,
    velox::memory::MemoryPool * pool,
    size_t bufferSize)
    : readFile_(readFile)
{
    initialize(pool, bufferSize);
}

ReadBufferFromVeloxReadFile::ReadBufferFromVeloxReadFile(
    std::shared_ptr<dwio::common::ReadFileInputStream> input,
    velox::memory::MemoryPool * pool,
    dwio::common::LogType logType,
    size_t bufferSize)
    : sourceInput_(std::move(input)), logType_(logType)
{
    VELOX_CHECK_NOT_NULL(sourceInput_, "A source ReadFileInputStream is required");
    // `initialize` only queries directIo/size on the underlying file (both
    // context-free); actual data reads go through `sourceInput_->read` so the
    // populated FileIoContext is used.
    readFile_ = sourceInput_->getReadFile().get();
    initialize(pool, bufferSize);
}

void ReadBufferFromVeloxReadFile::initialize(
    velox::memory::MemoryPool * pool,
    size_t bufferSize)
{
    VELOX_CHECK_NOT_NULL(readFile_);
    VELOX_CHECK_NOT_NULL(pool, "A MemoryPool is required for the owned buffer");
    VELOX_CHECK_GT(bufferSize, 0u, "ReadBuffer size must be > 0");

    uint64_t alignment = 1;
    readFile_->directIo(alignment);
    VELOX_CHECK_GT(alignment, 0u, "Direct-IO alignment must be positive");
    VELOX_CHECK_EQ(
        alignment & (alignment - 1),
        0u,
        "Direct-IO alignment must be a power of two");
    directIoAlignment_ = alignment;

    fileSize_ = readFile_->size();
    readUntil_ = fileSize_;
    fileOffsetOfBufferEnd_ = 0;

    state_.allocateOwned(pool, bufferSize, directIoAlignment_);
    // ReadBuffer convention: start with an empty working view so the first
    // next() loads data.
    state_.workingBuffer().resize(0);
}

size_t ReadBufferFromVeloxReadFile::readInto(
    size_t startOffset,
    char * dest,
    size_t destCapacity)
{
    if (sourceInput_)
    {
        // Source-input mode: route through the base ReadFileInputStream so
        // ReadFile::pread receives the populated FileIoContext (ioStats +
        // fileOpts + cacheable). The scalar read reads exactly destCapacity
        // bytes (the caller bounds destCapacity by readUntil_/file size).
        sourceInput_->read(dest, destCapacity, startOffset, logType_);
        return destCapacity;
    }
    const std::string_view chunk = readFile_->pread(startOffset, destCapacity, dest);
    return chunk.size();
}

} // namespace facebook::velox::ch
