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

#include "velox/ch/IO/FileCacheLocalWriteFile.h"

#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/common/base/Exceptions.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>

namespace facebook::velox::ch
{

FileCacheLocalWriteFile::FileCacheLocalWriteFile(std::string_view path)
    : path_(path)
{
    /// Match LocalWriteFile(bufferIo=true): write-only, create if absent, no
    /// O_EXCL, no O_DIRECT. Mode 0600 (S_IRUSR | S_IWUSR) is required with
    /// O_CREAT.
    const int flags = O_WRONLY | O_CREAT;
    const mode_t mode = S_IRUSR | S_IWUSR;

    fd_ = ::open(path_.c_str(), flags, mode);
    if (fd_ < 0)
    {
        const int saved = errno;
        throw FileCacheErrnoException(saved, "FileCacheLocalWriteFile: open failed for {}", path_);
    }

    /// Seek to end so a PARTIALLY_DOWNLOADED segment resumes by appending to the
    /// existing content tail (FileSegment resume relies on this).
    const off_t end = ::lseek(fd_, 0, SEEK_END);
    if (end < 0)
    {
        const int saved = errno;
        ::close(fd_);
        fd_ = -1;
        throw FileCacheErrnoException(saved, "FileCacheLocalWriteFile: lseek to end failed for {}", path_);
    }
    size_ = static_cast<uint64_t>(end);
}

FileCacheLocalWriteFile::~FileCacheLocalWriteFile()
{
    /// Never throw from the destructor. If the caller never called close, close
    /// the fd best-effort and warn.
    if (!closed_ && fd_ >= 0)
    {
        if (::close(fd_) != 0)
        {
            const int saved = errno;
            LOG(WARNING) << "FileCacheLocalWriteFile: close failed in destructor for " << path_ << ": "
                         << std::strerror(saved);
        }
        fd_ = -1;
    }
}

void FileCacheLocalWriteFile::append(std::string_view data)
{
    /// append-after-close is a caller ordering bug, not a disk fault: throw a
    /// plain VeloxRuntimeError so no errno consumer can misclassify it as a
    /// bypassable disk failure (Design 9.5a).
    VELOX_CHECK(!closed_, "FileCacheLocalWriteFile: append after close for {}", path_);

    if (data.empty())
        return;

    /// CH's WriteBufferFromFileDescriptor::nextImpl: loop until the whole buffer
    /// is written. A positive short write is NOT an error (write the unwritten
    /// suffix next iteration); EINTR is retried; only a genuine -1/0 with
    /// errno != EINTR throws. `size_` advances only after the whole buffer lands
    /// so book-keeping never claims more than what physically hit the file.
    ///
    /// A positive short write and a failing `::close` are syscall-level
    /// phenomena that cannot be produced through the interface-level standard
    /// `FaultyWriteFile` seam (its hook fires at the `append`/`write` boundary,
    /// not at the underlying syscall, and it has no `close` injection). The
    /// correctness of this loop is therefore guaranteed by line-by-line
    /// alignment with CH `WriteBufferFromFileDescriptor::nextImpl` plus review,
    /// not by a syscall-level test.
    const char * buf = data.data();
    const size_t total = data.size();
    size_t bytes_written = 0;
    while (bytes_written != total)
    {
        const size_t to_write = total - bytes_written;

        const ssize_t res = ::write(fd_, buf + bytes_written, to_write);
        if ((res == -1 || res == 0) && errno != EINTR)
        {
            const int saved = errno;
            throw FileCacheErrnoException(saved, "FileCacheLocalWriteFile: write failed for {}", path_);
        }
        if (res > 0)
            bytes_written += static_cast<size_t>(res);
    }
    size_ += static_cast<uint64_t>(total);
}

void FileCacheLocalWriteFile::flush()
{
    /// flush-after-close is a caller ordering bug (Design 9.5a): plain VeloxRuntimeError.
    VELOX_CHECK(!closed_, "FileCacheLocalWriteFile: flush after close for {}", path_);

    if (::fsync(fd_) != 0)
    {
        const int saved = errno;
        throw FileCacheErrnoException(saved, "FileCacheLocalWriteFile: fsync failed for {}", path_);
    }
}

void FileCacheLocalWriteFile::close()
{
    if (closed_)
        return;

    bool close_failed = false;
    int saved = 0;
    if (::close(fd_) != 0)
    {
        saved = errno;
        close_failed = true;
    }

    /// Both the success and failure paths clear fd_/closed_ (CH's
    /// WriteBufferFromFile::close). On Linux the fd is released even when
    /// `::close` reports EINTR/EIO/ENOSPC/EDQUOT, so retrying the same fd number
    /// (e.g. from the destructor) could close an unrelated file another thread
    /// reused it for. Design 9.4: never retry a released fd.
    fd_ = -1;
    closed_ = true;
    if (close_failed)
        throw FileCacheErrnoException(saved, "FileCacheLocalWriteFile: close failed for {}", path_);
}

}
