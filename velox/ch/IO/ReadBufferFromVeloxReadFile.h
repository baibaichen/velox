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

#include "velox/ch/Common/FileCacheException.h"
#include "velox/common/file/File.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch
{

/// CH-style streaming read buffer over a Velox `ReadFile`.
///
/// The buffer window mirrors `ReadBufferFromFileBase` in ClickHouse:
///
///   [bufData_  ...  pos_  ...  bufEnd_]
///                active allocation
///
/// - `next` reads the next chunk from `ReadFile::pread` starting at the current
///   file offset and returns true when any bytes were loaded.
/// - Reads are bounded by `readUntil_` (set via `setReadUntilPosition`); they
///   stop at or before that offset.
/// - `seek` repositions the logical file offset and clears the buffer so the
///   next `next` reads from the new position.
/// - `set` points the reader at caller-owned memory for a single read cycle,
///   allowing `FileCacheInputStream` to fill a cache segment without a copy.
///
/// The reader may own the `ReadFile` (shared_ptr constructor) or hold it
/// non-owning (raw pointer constructor); with the raw form the caller must
/// keep the `ReadFile` alive for the lifetime of this reader.
class ReadBufferFromVeloxReadFile
{
public:
    /// Default internal buffer size when the caller does not specify one.
    static constexpr size_t kDefaultBufferSize = 1u << 20; // 1 MiB

    /// Wraps `readFile` and takes shared ownership of it.
    explicit ReadBufferFromVeloxReadFile(
        std::shared_ptr<velox::ReadFile> readFile,
        size_t bufferSize = kDefaultBufferSize);

    /// Wraps `readFile` without owning it. The caller must keep `readFile`
    /// alive for the lifetime of this reader.
    explicit ReadBufferFromVeloxReadFile(
        velox::ReadFile * readFile,
        size_t bufferSize = kDefaultBufferSize);

    ReadBufferFromVeloxReadFile(const ReadBufferFromVeloxReadFile &) = delete;
    ReadBufferFromVeloxReadFile &
    operator=(const ReadBufferFromVeloxReadFile &) = delete;

    ~ReadBufferFromVeloxReadFile() = default;

    /// Loads the next chunk into the buffer. Returns true when at least one
    /// byte was loaded, or false on EOF or once the `readUntil_` boundary has
    /// been reached.
    bool next();

    /// Returns a pointer to the first unread byte in the current buffer.
    const char * position() const { return pos_; }

    /// Returns a pointer one past the last available byte in the current
    /// buffer.
    const char * bufferEnd() const { return bufEnd_; }

    /// Returns true once the buffer is exhausted and `next` returned false.
    bool eof() const { return atEof_; }

    /// Advances the read pointer by `n` bytes within the current buffer.
    void advance(ptrdiff_t n);

    /// Returns the absolute file offset that corresponds to `position`.
    off_t getPosition() const;

    /// Returns the absolute file offset that corresponds to `bufferEnd`.
    off_t getFileOffsetOfBufferEnd() const;

    /// Seeks to `offset` interpreted relative to `whence` (`SEEK_SET` or
    /// `SEEK_CUR`) and clears the buffer so the next `next` reads from there.
    void seek(off_t offset, int whence = SEEK_SET);

    /// Limits reads to the range [current offset, filePos). Reads stop once the
    /// current offset reaches `filePos`. Pass the file size to clear the limit.
    void setReadUntilPosition(size_t filePos);

    /// Switches to caller-owned memory for the next read cycle. `data` must
    /// stay valid until the next `next`, `seek`, or `set` call.
    void set(char * data, size_t size);

    /// Returns the name of the underlying `ReadFile`.
    std::string getFileName() const;

private:
    // Non-owning view of the wrapped file; always valid after construction.
    velox::ReadFile * readFile_{nullptr};
    // Holds ownership when constructed from a shared_ptr; empty otherwise.
    std::shared_ptr<velox::ReadFile> ownedReadFile_;

    // Internally allocated buffer used unless an external buffer is installed.
    std::vector<char> internalBuffer_;

    // Start of the active buffer (internal or external).
    char * bufData_{nullptr};
    // First unread byte in the active buffer.
    char * pos_{nullptr};
    // One past the last valid byte in the active buffer.
    char * bufEnd_{nullptr};
    // Capacity of the active buffer, in bytes.
    size_t bufCapacity_{0};

    // File offset at bufData_, updated on every next() and seek().
    size_t bufStartOffset_{0};
    // File offset at pos_.
    size_t currentOffset_{0};
    // Exclusive upper bound on reads.
    size_t readUntil_{static_cast<size_t>(-1)};
    // Set once next() reaches EOF or the readUntil_ boundary.
    bool atEof_{false};
    // True while bufData_ points at caller-owned memory installed by set().
    bool externalBuffer_{false};

    void initBuffer(size_t bufferSize);

    // Disarms any external buffer and restores a coherent empty internal window
    // at currentOffset_. Called on every non-success next() exit.
    void resetToInternalWindow();
};

} // namespace facebook::velox::ch
