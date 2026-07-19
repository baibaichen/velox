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
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace facebook::velox::ch
{

/// Flattened compatibility base for the exact `FileCache` write contract.
///
/// It exposes the behavior `FileCache` uses from CH `BufferBase`, `WriteBuffer`,
/// and `WriteBufferFromFileBase`: attach a working view, append exactly the
/// pending bytes on `next`, and drive the finalize/sync/cancel lifecycle. Owned
/// memory lives in the shared `FileCacheBufferState` as a pool-charged
/// `BufferPtr`; a caller-supplied external buffer is a non-owning working view
/// so the external write path copies nothing.
class WriteBufferFromFileBase
{
public:
    /// Destroying an un-finalized, un-canceled writer must not append data, so
    /// this base never finalizes in its destructor.
    virtual ~WriteBufferFromFileBase() = default;

    // --- BufferBase surface -------------------------------------------------
    CacheBuffer & internalBuffer() { return state_.internalBuffer(); }
    CacheBuffer & buffer() { return state_.workingBuffer(); }
    char *& position() { return state_.position(); }
    size_t offset() const { return state_.offset(); }
    size_t available() const { return state_.available(); }
    bool hasPendingData() const { return state_.hasPendingData(); }
    size_t count() const { return state_.count(); }

    /// Total number of bytes written so far, including any buffered but not yet
    /// appended bytes.
    size_t getPosition() const { return state_.count(); }

    // --- WriteBuffer surface ------------------------------------------------
    /// Copies `n` bytes from `from` through the owned buffer, appending whenever
    /// it fills. Rejected once the writer is finalized or canceled.
    void write(const char * from, size_t n);

    /// Installs a non-owning working view [ptr, ptr + size) with the cursor at
    /// `ptr + offset`, mirroring `BufferBase::set`. `set(from, size, size)`
    /// stages `size` external bytes for a zero-copy append; `set(nullptr, 0)`
    /// detaches every caller pointer.
    void set(char * ptr, size_t size, size_t offset = 0);

    /// Advances the cursor by `n` bytes within the working view. Rejected once
    /// the writer is finalized or canceled.
    void advance(size_t n);

    /// Appends exactly the pending bytes synchronously and resets the cursor. A
    /// no-op when canceled, finalized, or with nothing pending. On an append
    /// exception the writer is canceled and the exception is rethrown.
    void next();

    /// Appends pending bytes, then requests durability; the writer stays active.
    void sync();

    /// Idempotent: appends any pending bytes, closes the file, and transitions
    /// to finalized. On failure the writer is canceled before the exception
    /// propagates. Rejected after cancel.
    void finalize();

    /// `noexcept` and idempotent: discards pending bytes, appends nothing, and
    /// releases the file through a non-throwing destruction path. A no-op once
    /// finalized.
    void cancel() noexcept;

    bool isFinalized() const { return finalized_; }
    bool isCanceled() const { return canceled_; }

    virtual std::string getFileName() const = 0;

protected:
    FileCacheBufferState state_;
    bool finalized_{false};
    bool canceled_{false};

    /// Appends [buffer().begin(), buffer().begin() + offset()) to the file
    /// without flushing.
    virtual void nextImpl() = 0;
    /// Appends pending bytes and closes the file, without an extra flush.
    virtual void finalizeImpl() = 0;
    /// Flushes the underlying file (used by `sync`).
    virtual void syncImpl() = 0;
    /// Releases the underlying file without throwing (used by `cancel`).
    virtual void cancelImpl() noexcept = 0;
};

/// CH-style streaming write adapter over a Velox `WriteFile`.
///
/// Owns exactly one `unique_ptr<velox::WriteFile>`. Constructed with buffer size
/// 0 it is external-only and appends caller memory directly (application-level
/// zero copy); with a positive size it allocates a pool-charged `BufferPtr` to
/// stage writes. `nextImpl` maps to `WriteFile::append`, `sync` to `flush`, and
/// `finalize` to `close`.
class WriteBufferFromVeloxWriteFile final : public WriteBufferFromFileBase
{
public:
    /// Default owned buffer size when the caller does not specify one.
    static constexpr size_t kDefaultBufferSize = 1u << 20; // 1 MiB

    /// Wraps `writeFile`, taking exclusive ownership. A `bufferSize` of 0 makes
    /// the writer external-only (no owned `BufferPtr`); a positive size
    /// allocates the owned staging buffer from `pool`.
    explicit WriteBufferFromVeloxWriteFile(
        std::unique_ptr<velox::WriteFile> writeFile,
        velox::memory::MemoryPool * pool = nullptr,
        size_t bufferSize = 0);

    WriteBufferFromVeloxWriteFile(const WriteBufferFromVeloxWriteFile &) = delete;
    WriteBufferFromVeloxWriteFile &
    operator=(const WriteBufferFromVeloxWriteFile &) = delete;

    ~WriteBufferFromVeloxWriteFile() override = default;

    std::string getFileName() const override
    {
        return writeFile_ ? writeFile_->getName() : fileName_;
    }

protected:
    void nextImpl() override;
    void finalizeImpl() override;
    void syncImpl() override;
    void cancelImpl() noexcept override;

private:
    // The single owner of the underlying file handle.
    std::unique_ptr<velox::WriteFile> writeFile_;
    // Cached name so getFileName() still works after cancel() releases the file.
    std::string fileName_;
};

} // namespace facebook::velox::ch
