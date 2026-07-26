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

#include "velox/common/file/File.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace facebook::velox::ch
{

/// FileCache-owned local `velox::WriteFile` whose failure path produces a typed
/// `FileCacheErrnoException` carrying the structured POSIX `errno`, instead of
/// the untyped `VeloxRuntimeError` that `velox::LocalWriteFile` throws.
///
/// The Design 7.5 errno *consumers* (`reserveAndWriteSegmentChunk` and the demand
/// `writeCache`) only catch `FileCacheErrnoException` and classify by errno
/// (ENOSPC/EDQUOT -> unconditional bypass, other errno -> skip-dependent,
/// non-errno -> propagate). But the production *producer* was still
/// `LocalWriteFile`, which reports a failed `::write`/`open`/`lseek`/`fsync`/
/// `::close` via `VELOX_CHECK` -> `VeloxRuntimeError` with no typed errno. That
/// exception slips past the errno consumers, so on a real disk fault the query
/// fails even with `skipCacheOnDiskFailure=true`. This writer closes that gap
/// (Design 8.3): every syscall failure point saves the current `errno` before
/// any other call can clobber it and throws `FileCacheErrnoException`.
///
/// Production behavior is otherwise identical to `LocalWriteFile` constructed
/// with `bufferIo=true`:
///   * `open(path, O_WRONLY | O_CREAT, 0600)` (no `O_EXCL`, no `O_DIRECT`),
///   * `lseek(fd, 0, SEEK_END)` so a `PARTIALLY_DOWNLOADED` segment resumes by
///     appending to the existing content tail,
///   * each `append` writes the whole buffer with a `::write` loop that follows
///     CH's `WriteBufferFromFileDescriptor::nextImpl`: a positive short write is
///     NOT an error (the loop continues writing the unwritten suffix), `EINTR`
///     is retried, and only a genuine `-1`/`0` with `errno != EINTR` throws.
///   * `flush` is an `::fsync`, `close` is a `::close`, `size` is the running
///     byte total, `getName` is the path.
///
/// Note on state-error vs syscall-error separation (Design 9.5a): calling
/// `append`/`flush` on an already-`close`d writer is a caller ordering bug, not
/// a disk fault, so it throws a plain `VeloxRuntimeError` (via `VELOX_CHECK`),
/// NOT a `FileCacheErrnoException`. Only real `open`/`lseek`/`write`/`fsync`/
/// `close` syscall failures produce the typed errno exception, so a program bug
/// can never be misclassified by an errno consumer as a bypassable disk fault.
class FileCacheLocalWriteFile final : public velox::WriteFile
{
public:
    explicit FileCacheLocalWriteFile(std::string_view path);

    /// Non-throwing cleanup: if the caller never called `close`, close the fd on
    /// a best-effort basis and log a warning. Never throws (Design 8.3).
    ~FileCacheLocalWriteFile() override;

    void append(std::string_view data) override;

    void flush() override;

    void close() override;

    uint64_t size() const override { return size_; }

    const std::string getName() const override { return path_; }

private:
    int fd_{-1};
    std::string path_;
    uint64_t size_{0};
    bool closed_{false};
};

}
