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

#include "velox/ch/IO/WriteBufferFromVeloxWriteFile.h"

#include "velox/ch/Common/FileCacheException.h"

#include <algorithm>
#include <cstring>

namespace facebook::velox::ch
{

// ---------------------------------------------------------------------------
// WriteBufferFromFileBase
// ---------------------------------------------------------------------------

void WriteBufferFromFileBase::write(const char * from, size_t n)
{
    VELOX_CHECK(!finalized_, "Cannot write to a finalized buffer");
    VELOX_CHECK(!canceled_, "Cannot write to a canceled buffer");
    VELOX_CHECK(
        !buffer().empty(),
        "Cannot write through an empty (external-only) buffer");

    size_t copied = 0;
    while (copied < n)
    {
        if (!hasPendingData())
            next();
        const size_t toCopy = std::min(available(), n - copied);
        std::memcpy(position(), from + copied, toCopy);
        position() += toCopy;
        copied += toCopy;
    }
}

void WriteBufferFromFileBase::set(char * ptr, size_t size, size_t offset)
{
    // A pure state operation: install a non-owning working view. It never
    // touches the underlying file, so it stays safe even after cancel() (the
    // detaching set(nullptr, 0) runs on the write path's cleanup even when a
    // preceding next() failed).
    state_.set(ptr, size, offset);
}

void WriteBufferFromFileBase::advance(size_t n)
{
    VELOX_CHECK(!finalized_, "Cannot advance a finalized buffer");
    VELOX_CHECK(!canceled_, "Cannot advance a canceled buffer");
    VELOX_CHECK_LE(n, available(), "advance past the buffer capacity");
    position() += n;
}

void WriteBufferFromFileBase::next()
{
    if (canceled_ || finalized_)
        return;
    if (offset() == 0)
        return;

    const size_t bytesInBuffer = offset();
    try
    {
        nextImpl();
    }
    catch (...)
    {
        // CH WriteBuffer::next settles bytes += bytes_in_buffer before
        // cancel/rethrow (WriteBuffer.h:69). Settle here so count()/getPosition()
        // report the prior committed total plus the attempted chunk, allowing the
        // caller to reconcile from the physical file size.
        state_.addBytes(bytesInBuffer);
        cancel();
        throw;
    }
    state_.addBytes(bytesInBuffer);
    position() = buffer().begin();
}

void WriteBufferFromFileBase::sync()
{
    if (canceled_ || finalized_)
        return;
    next();
    syncImpl();
}

void WriteBufferFromFileBase::finalize()
{
    if (finalized_)
        return;
    if (canceled_)
        throwFileCacheException("Cannot finalize a canceled writer");

    try
    {
        finalizeImpl();
        finalized_ = true;
    }
    catch (...)
    {
        // A failed finalize is terminal, exactly like a failed next().
        cancel();
        throw;
    }
}

void WriteBufferFromFileBase::cancel() noexcept
{
    if (canceled_ || finalized_)
        return;
    // Append nothing and release the underlying file through its non-throwing
    // destruction path...
    cancelImpl();
    // ...then discard the pending cursor and detach caller memory so no caller
    // pointer is retained and offset() becomes 0.
    state_.detach();
    canceled_ = true;
}

// ---------------------------------------------------------------------------
// WriteBufferFromVeloxWriteFile
// ---------------------------------------------------------------------------

WriteBufferFromVeloxWriteFile::WriteBufferFromVeloxWriteFile(
    std::unique_ptr<velox::WriteFile> writeFile,
    velox::memory::MemoryPool * pool,
    size_t bufferSize)
    : writeFile_(std::move(writeFile))
{
    VELOX_CHECK_NOT_NULL(writeFile_);
    fileName_ = writeFile_->getName();

    if (bufferSize > 0)
    {
        VELOX_CHECK_NOT_NULL(
            pool, "A MemoryPool is required for a non-zero owned buffer");
        // Owned staging buffer; the working view stays full so write() can copy
        // into it. Writers do not use direct-IO alignment (WriteFile does not
        // expose it), so the default 64-byte pool alignment is sufficient.
        state_.allocateOwned(pool, bufferSize, 1);
    }
    // bufferSize == 0: external-only writer, no owned BufferPtr; the working
    // view stays empty and the caller drives it with set(from, size, offset).
}

void WriteBufferFromVeloxWriteFile::nextImpl()
{
    // Append exactly the pending bytes directly from the working view; for an
    // external buffer this is the caller's own memory (application-level zero
    // copy). Do not flush here.
    writeFile_->append(std::string_view(buffer().begin(), offset()));
}

void WriteBufferFromVeloxWriteFile::finalizeImpl()
{
    // Append any pending bytes, then close. No extra flush so a normal close is
    // not turned into a per-segment fsync.
    next();
    writeFile_->close();
}

void WriteBufferFromVeloxWriteFile::syncImpl()
{
    writeFile_->flush();
}

void WriteBufferFromVeloxWriteFile::cancelImpl() noexcept
{
    // Append nothing; release the file so its non-throwing destructor closes the
    // handle. Any bytes already appended stay on disk for the caller to
    // reconcile; buffered-but-unappended bytes are discarded.
    writeFile_.reset();
}

} // namespace facebook::velox::ch
