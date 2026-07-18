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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch
{

/// CH-style buffered streaming writer over a Velox `WriteFile`.
///
/// Data accumulates in an internal buffer and is flushed to
/// `WriteFile::append` either explicitly (`flush` / `finalize`) or
/// automatically once the buffer is full.
///
/// Short-write reconciliation contract:
///   `next` exposes the remaining capacity of the internal buffer. When the
///   caller writes fewer than `size` bytes it must call `advance(n)` with the
///   actual count before calling `next` or `flush` again. `flush` commits only
///   `writePos_` bytes, not the whole capacity.
///
/// Lifecycle:
///   `flush`    commits buffered bytes and leaves the buffer ready for more.
///   `finalize` flushes and closes the `WriteFile`; further writes throw.
///   `cancel`   discards buffered bytes without appending or closing.
class WriteBufferFromVeloxWriteFile
{
public:
    /// Default internal buffer size when the caller does not specify one.
    static constexpr size_t kDefaultBufferSize = 1u << 20; // 1 MiB

    /// Wraps `writeFile` and takes shared ownership of it.
    explicit WriteBufferFromVeloxWriteFile(
        std::shared_ptr<velox::WriteFile> writeFile,
        size_t bufferSize = kDefaultBufferSize);

    WriteBufferFromVeloxWriteFile(const WriteBufferFromVeloxWriteFile &) = delete;
    WriteBufferFromVeloxWriteFile &
    operator=(const WriteBufferFromVeloxWriteFile &) = delete;

    ~WriteBufferFromVeloxWriteFile() = default;

    /// Writes `len` bytes from `buf` into the buffer, auto-flushing when full.
    void write(const char * buf, size_t len);

    /// Exposes the remaining write capacity: sets `data` to the first unused
    /// byte and `size` to the number of bytes available. The caller must call
    /// `advance(n)` with the bytes it actually wrote.
    void next(char *& data, int64_t & size);

    /// Advances the write position by `n` bytes after a `next` call.
    void advance(size_t n);

    /// Appends all pending buffered bytes to the `WriteFile`, flushes it, and
    /// resets the write position to 0.
    void flush();

    /// Flushes pending bytes and closes the `WriteFile`. Further writes throw.
    void finalize();

    /// Discards buffered bytes without flushing. The `WriteFile` is not closed.
    void cancel();

    /// Returns the total number of bytes written so far, including bytes that
    /// have already been flushed.
    size_t getPosition() const { return totalWritten_; }

private:
    // Owned destination file.
    std::shared_ptr<velox::WriteFile> writeFile_;
    // Staging buffer for bytes pending a flush.
    std::vector<char> buffer_;
    // Number of buffered bytes not yet appended to writeFile_.
    size_t writePos_{0};
    // Cumulative number of bytes handed to write()/advance().
    size_t totalWritten_{0};
    // Set once finalize() has closed the file.
    bool finalized_{false};
    // Set once cancel() has discarded the buffer.
    bool cancelled_{false};

    void flushInternal();
};

} // namespace facebook::velox::ch
