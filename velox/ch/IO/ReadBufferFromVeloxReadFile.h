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

#include "velox/buffer/Buffer.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace facebook::velox::ch
{

/// A reference to a contiguous [begin, end) range of bytes, mirroring the
/// nested `BufferBase::Buffer` in ClickHouse. `resize` only moves `end`, so the
/// working view can shrink to the number of valid bytes without reallocating.
class CacheBuffer
{
public:
    CacheBuffer() = default;
    CacheBuffer(char * begin, char * end) : begin_(begin), end_(end) {}

    char * begin() const { return begin_; }
    char * end() const { return end_; }
    size_t size() const
    {
        // Null-safe: an empty/detached buffer has both pointers null and a size
        // of 0, computed without subtracting null pointers.
        return begin_ == nullptr ? 0 : static_cast<size_t>(end_ - begin_);
    }
    bool empty() const { return end_ == begin_; }
    void resize(size_t size) { end_ = begin_ + size; }

    void swap(CacheBuffer & other) noexcept
    {
        std::swap(begin_, other.begin_);
        std::swap(end_, other.end_);
    }

private:
    char * begin_{nullptr};
    char * end_{nullptr};
};

/// Flattened `FileCache` buffer state shared by the read and write adapters.
///
/// It captures exactly the `BufferBase` semantics the `FileCache` state machine
/// relies on -- an internal buffer, a working view, a mutable cursor, and a
/// settled byte counter -- while owning its memory as a pool-charged
/// `BufferPtr` (never an unaccounted `std::vector<char>`). A caller-supplied
/// external buffer is represented as a non-owning working view; the state never
/// extends the lifetime of caller memory.
class FileCacheBufferState
{
public:
    FileCacheBufferState() = default;

    /// Allocates `size` usable bytes from `pool`, aligned to `alignment` (a
    /// power of two; 1 means no direct-IO requirement), and installs them as the
    /// internal buffer with an empty working view positioned at the start.
    void allocateOwned(velox::memory::MemoryPool * pool, size_t size, size_t alignment);

    /// Installs a non-owning [ptr, ptr + size) view and moves the cursor to
    /// `ptr + offset`, mirroring `BufferBase::set`. A null pointer requires
    /// `size == 0` and `offset == 0` and detaches all caller memory into a
    /// coherent empty window; a non-null pointer rejects `offset > size` before
    /// any pointer arithmetic.
    void set(char * ptr, size_t size, size_t offset);

    /// Restores the internal buffer to the owned pool allocation (if any) and
    /// exposes an empty working window at its start. Used when a caller detaches
    /// an external buffer via `set(nullptr, 0)`.
    void restoreOwnedWindow();

    /// Discards the working view and drops any external/owned working pointer,
    /// leaving a coherent empty state that references no caller memory. The owned
    /// `BufferPtr` (if any) is kept but no longer exposed. Used by the writer's
    /// terminal cancel/append-failure path.
    void detach();

    /// Empties the working view at the current cursor without touching the
    /// internal buffer, mirroring `BufferBase::resetWorkingBuffer`.
    void resetWorkingView();

    CacheBuffer & internalBuffer() { return internal_; }
    CacheBuffer & workingBuffer() { return working_; }
    char *& position() { return position_; }
    const char * position() const { return position_; }

    size_t offset() const
    {
        // Null-safe: a detached working view yields offset 0 without subtracting
        // null pointers.
        return (position_ == nullptr || working_.begin() == nullptr)
            ? 0
            : static_cast<size_t>(position_ - working_.begin());
    }
    size_t available() const
    {
        // Null-safe: a detached working view yields 0 without subtracting null
        // pointers.
        return (position_ == nullptr || working_.end() == nullptr)
            ? 0
            : static_cast<size_t>(working_.end() - position_);
    }
    bool hasPendingData() const { return available() > 0; }
    size_t count() const { return bytes_ + offset(); }

    size_t bytes() const { return bytes_; }
    void settleConsumed() { bytes_ += offset(); }
    void addBytes(size_t n) { bytes_ += n; }

    bool isPadded() const { return padded_; }
    bool hasOwnedBuffer() const { return ownedBuffer_ != nullptr; }
    velox::memory::MemoryPool * ownedPool() const { return ownedPool_; }

    /// Swaps the internal/working views and cursor with another state, leaving
    /// each object's owned buffer and settled byte counter untouched. Used by
    /// the post-MVP writer swap guard; provided here so both adapters share one
    /// representation.
    void swapWorkingState(FileCacheBufferState & other) noexcept
    {
        internal_.swap(other.internal_);
        working_.swap(other.working_);
        std::swap(position_, other.position_);
    }

private:
    // Pool-charged owned memory. Null for an external-only writer.
    BufferPtr ownedBuffer_;
    velox::memory::MemoryPool * ownedPool_{nullptr};
    // Aligned usable range within ownedBuffer_.
    char * ownedBegin_{nullptr};
    size_t ownedCapacity_{0};

    CacheBuffer internal_;
    CacheBuffer working_;
    char * position_{nullptr};
    size_t bytes_{0};
    bool padded_{false};
};

/// Flattened compatibility base for the exact `FileCache` read contract.
///
/// It exposes the behavior `FileCache` uses from CH `BufferBase`, `ReadBuffer`,
/// `SeekableReadBuffer`, and `ReadBufferFromFileBase` without porting the full
/// general IO hierarchy. `FileSegment::RemoteFileReaderPtr` stores this base so
/// later stages hold a remote reader polymorphically; the concrete Velox
/// implementation only provides `nextImpl`, the file identity, and the owned
/// memory allocation.
class ReadBufferFromFileBase
{
public:
    virtual ~ReadBufferFromFileBase() = default;

    // --- BufferBase surface -------------------------------------------------
    CacheBuffer & internalBuffer() { return state_.internalBuffer(); }
    CacheBuffer & buffer() { return state_.workingBuffer(); }
    char *& position() { return state_.position(); }
    size_t offset() const { return state_.offset(); }
    size_t available() const { return state_.available(); }
    bool hasPendingData() const { return state_.hasPendingData(); }
    size_t count() const { return state_.count(); }
    bool isPadded() const { return state_.isPadded(); }

    // --- ReadBuffer surface -------------------------------------------------
    /// Settles the consumed cursor, loads the next chunk via `nextImpl`, and
    /// republishes the working view. Requires no pending data. On a `nextImpl`
    /// exception the reader is canceled and the original exception is rethrown;
    /// the canceled state is terminal and not retryable.
    bool next();

    /// True once the input is exhausted. Loads the next chunk if the working
    /// view is empty, matching `ReadBuffer::eof`.
    bool eof() { return !hasPendingData() && !next(); }

    void cancel() { canceled_ = true; }
    bool isCanceled() const { return canceled_; }
    void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }

    /// Installs caller-owned memory as the read target, mirroring
    /// `ReadBuffer::set`: the target persists across reads until another
    /// explicit `set`. `set(nullptr, 0)` detaches every caller pointer and
    /// restores the owned internal buffer as a coherent empty window.
    void set(char * ptr, size_t size);

    /// This reader reads directly into the memory installed by `set`, so
    /// zero-copy external reads are supported.
    virtual bool supportsExternalBufferMode() const { return true; }

    // --- SeekableReadBuffer surface -----------------------------------------
    /// Repositions the logical file offset (`SEEK_SET` or `SEEK_CUR`) and clears
    /// the working view so the next `next` reads from there. Returns the new
    /// absolute offset.
    off_t seek(off_t offset, int whence = SEEK_SET);

    off_t getPosition() const
    {
        return static_cast<off_t>(fileOffsetOfBufferEnd_ - available());
    }
    size_t getFileOffsetOfBufferEnd() const { return fileOffsetOfBufferEnd_; }

    /// `setReadUntilPosition` guarantees eof at the boundary, so right-bounded
    /// reads are supported.
    virtual bool supportsRightBoundedReads() const { return true; }

    // --- ReadBuffer bounded-range surface -----------------------------------
    /// Limits reads to [current offset, position). Shrinking the boundary also
    /// constrains an already-loaded window; extending it lets a later `next`
    /// resume.
    void setReadUntilPosition(size_t position);
    void setReadUntilEnd() { setReadUntilPosition(fileSize_); }

    // --- ReadBufferFromFileBase surface -------------------------------------
    virtual std::string getFileName() const = 0;
    std::optional<size_t> tryGetFileSize() { return fileSize_; }

protected:
    FileCacheBufferState state_;

    // File offset that corresponds to the end of the current working view.
    size_t fileOffsetOfBufferEnd_{0};
    // Exclusive upper bound on reads.
    size_t readUntil_{static_cast<size_t>(-1)};
    // Total size of the underlying file.
    size_t fileSize_{0};
    // Power-of-two alignment required by direct IO; 1 means no requirement.
    size_t directIoAlignment_{1};
    bool canceled_{false};

    /// Reads the next chunk starting at `startOffset` into `dest` (capacity
    /// `destCapacity`, already bounded by `readUntil_`), returning the number of
    /// bytes read (0 at end of input). Implemented by the concrete adapter.
    virtual size_t readInto(size_t startOffset, char * dest, size_t destCapacity) = 0;

private:
    // Loads the next chunk into the internal buffer and republishes the working
    // view. Returns false at end of input or the right boundary.
    bool nextImpl();

    // Fails closed before an actual `pread` when direct IO is required: the
    // destination address, file offset, and requested length must all satisfy
    // `directIoAlignment_`. Rejects an unaligned file tail or right bound instead
    // of silently rounding, over-reading, or falling back to buffered IO.
    void checkDirectIoRead(const char * dest, size_t startOffset, size_t length) const;
};

/// CH-style streaming read adapter over a Velox `ReadFile`.
///
/// Implements the `FileCache` read contract with `ReadFile::pread` and owns its
/// buffer as a pool-charged `BufferPtr`. Direct IO is queried at construction;
/// when required, the owned buffer and any external buffer must satisfy the
/// reported power-of-two alignment.
class ReadBufferFromVeloxReadFile final : public ReadBufferFromFileBase
{
public:
    /// Default internal buffer size when the caller does not specify one.
    static constexpr size_t kDefaultBufferSize = 1u << 20; // 1 MiB

    /// Wraps `readFile` (taking shared ownership) and allocates the owned buffer
    /// from `pool`.
    ReadBufferFromVeloxReadFile(
        std::shared_ptr<velox::ReadFile> readFile,
        velox::memory::MemoryPool * pool,
        size_t bufferSize = kDefaultBufferSize);

    /// Wraps `readFile` without owning it. The caller must keep `readFile` alive
    /// for the lifetime of this reader.
    ReadBufferFromVeloxReadFile(
        velox::ReadFile * readFile,
        velox::memory::MemoryPool * pool,
        size_t bufferSize = kDefaultBufferSize);

    ReadBufferFromVeloxReadFile(const ReadBufferFromVeloxReadFile &) = delete;
    ReadBufferFromVeloxReadFile &
    operator=(const ReadBufferFromVeloxReadFile &) = delete;

    ~ReadBufferFromVeloxReadFile() override = default;

    std::string getFileName() const override
    {
        return readFile_ ? readFile_->getName() : std::string{};
    }

protected:
    size_t readInto(size_t startOffset, char * dest, size_t destCapacity) override;

private:
    void initialize(velox::memory::MemoryPool * pool, size_t bufferSize);

    // Non-owning view of the wrapped file; always valid after construction.
    velox::ReadFile * readFile_{nullptr};
    // Holds ownership when constructed from a shared_ptr; empty otherwise.
    std::shared_ptr<velox::ReadFile> ownedReadFile_;
};

} // namespace facebook::velox::ch
