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

WriteBufferFromVeloxWriteFile::WriteBufferFromVeloxWriteFile(
    std::shared_ptr<velox::WriteFile> writeFile,
    size_t bufferSize)
    : writeFile_(std::move(writeFile))
    , buffer_(bufferSize)
{
    VELOX_CHECK_NOT_NULL(writeFile_);
    VELOX_CHECK_GT(bufferSize, 0u, "WriteBuffer size must be > 0");
}

void WriteBufferFromVeloxWriteFile::write(const char * buf, size_t len)
{
    VELOX_CHECK(!finalized_, "write after finalize");
    VELOX_CHECK(!cancelled_, "write after cancel");

    size_t written = 0;
    while (written < len)
    {
        const size_t capacity = buffer_.size();
        if (writePos_ == capacity)
            flushInternal();

        const size_t available = capacity - writePos_;
        const size_t chunk = std::min(available, len - written);
        std::memcpy(buffer_.data() + writePos_, buf + written, chunk);
        writePos_ += chunk;
        written += chunk;
    }
    totalWritten_ += len;
}

void WriteBufferFromVeloxWriteFile::next(char *& data, int64_t & size)
{
    VELOX_CHECK(!finalized_, "next after finalize");
    VELOX_CHECK(!cancelled_, "next after cancel");

    if (writePos_ == buffer_.size())
        flushInternal();

    data = buffer_.data() + writePos_;
    size = static_cast<int64_t>(buffer_.size() - writePos_);
}

void WriteBufferFromVeloxWriteFile::advance(size_t n)
{
    // advance stages bytes into the buffer just like write()/next(); reject it
    // after a terminal transition so no later flush can commit orphaned bytes.
    VELOX_CHECK(!finalized_, "advance after finalize");
    VELOX_CHECK(!cancelled_, "advance after cancel");
    // Compare against the remaining capacity so the validation itself cannot
    // overflow (writePos_ <= buffer_.size() always holds).
    VELOX_CHECK_LE(
        n,
        buffer_.size() - writePos_,
        "advance past buffer capacity");
    writePos_ += n;
    totalWritten_ += n;
}

void WriteBufferFromVeloxWriteFile::flush()
{
    if (!finalized_ && !cancelled_)
    {
        flushInternal();
        writeFile_->flush();
    }
}

void WriteBufferFromVeloxWriteFile::finalize()
{
    VELOX_CHECK(!finalized_, "finalize called twice");
    VELOX_CHECK(!cancelled_, "finalize after cancel");
    flushInternal();
    writeFile_->flush();
    writeFile_->close();
    finalized_ = true;
}

void WriteBufferFromVeloxWriteFile::cancel()
{
    VELOX_CHECK(!finalized_, "cancel after finalize");
    writePos_ = 0;
    cancelled_ = true;
}

void WriteBufferFromVeloxWriteFile::flushInternal()
{
    if (writePos_ == 0)
        return;
    writeFile_->append(std::string_view(buffer_.data(), writePos_));
    writePos_ = 0;
}

} // namespace facebook::velox::ch
