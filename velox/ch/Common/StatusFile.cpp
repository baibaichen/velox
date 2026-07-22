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

#include "velox/ch/Common/StatusFile.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/logger_useful.h"
#include "VeloxBuildRevision.h"

#include <folly/FileUtil.h>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <string>
#include <string_view>

namespace facebook::velox::ch
{
namespace
{

void writeAll(int fd, std::string_view contents)
{
    if (folly::writeFull(fd, contents.data(), contents.size()) == -1)
    {
        const int error = errno;
        VELOX_FAIL(
            "Cannot write StatusFile contents: error code {} ({})",
            error,
            std::strerror(error));
    }
}

std::string localTimestamp()
{
    const auto now = std::chrono::system_clock::now();
    const std::time_t time = std::chrono::system_clock::to_time_t(now);
    std::tm localTime{};
    if (::localtime_r(&time, &localTime) == nullptr)
        VELOX_FAIL("Cannot format StatusFile timestamp: localtime_r failed");

    char buffer[20];
    if (std::strftime(
            buffer,
            sizeof(buffer),
            "%Y-%m-%d %H:%M:%S",
            &localTime)
        == 0)
        VELOX_FAIL("Cannot format StatusFile timestamp: strftime failed");

    return buffer;
}

}

StatusFile::FillFunction StatusFile::writePid()
{
    return [](int fd)
    {
        const std::string pid = std::to_string(static_cast<long>(::getpid()));
        writeAll(fd, pid);
    };
}

StatusFile::FillFunction StatusFile::writeFullInfo()
{
    return [](int fd)
    {
        const std::string contents =
            "PID: " + std::to_string(static_cast<long>(::getpid())) + "\n"
            + "Started at: " + localTimestamp() + "\n"
            + "Revision: " + std::string(detail::kVeloxBuildRevision) + "\n";
        writeAll(fd, contents);
    };
}

StatusFile::StatusFile(std::string path, FillFunction fill)
    : path_(std::move(path))
{
    // If the file already exists, an earlier instance did not remove it on
    // shutdown, which indicates an unclean restart. Read its old contents and
    // log them for diagnostics before we truncate the file below.
    // NOTE Minor race condition.
    {
        const int oldFd = ::open(path_.c_str(), O_RDONLY | O_CLOEXEC);
        if (oldFd != -1)
        {
            folly::File oldFile(oldFd, /*ownsFd=*/true);
            std::string contents;
            contents.resize(1024);
            const ssize_t bytesRead =
                folly::readFull(oldFile.fd(), contents.data(), contents.size());
            if (bytesRead > 0)
                contents.resize(static_cast<size_t>(bytesRead));
            else
                contents.clear();

            if (!contents.empty())
                LOG_INFO(
                    getLogger("StatusFile"),
                    "Status file {} already exists - unclean restart. Contents:\n{}",
                    path_,
                    contents);
            else
                LOG_INFO(
                    getLogger("StatusFile"),
                    "Status file {} already exists and is empty - probably unclean hardware restart.",
                    path_);
        }
    }

    // Open or create the status file.
    const int rawFd = ::open(
        path_.c_str(),
        O_WRONLY | O_CREAT | O_CLOEXEC,
        0666);

    if (rawFd == -1)
        throwFileCacheException(
            "StatusFile: cannot open '{}': {}",
            path_,
            std::strerror(errno));

    // Transfer ownership to folly::File so the fd is closed on any exception.
    file_ = folly::File(rawFd, /*ownsFd=*/true);

    // Acquire exclusive flock (non-blocking).  Two separate open() calls in the
    // same or different processes each get a distinct open-file-description; the
    // kernel will deny the second try_lock.
    if (!file_.try_lock())
        throwFileCacheException(
            "StatusFile: cannot lock '{}'. "
            "Another cache instance using the same path is already running.",
            path_);

    // Truncate and seek to the beginning before writing content.
    if (::ftruncate(file_.fd(), 0) != 0)
        throwFileCacheException(
            "StatusFile: cannot truncate '{}': {}", path_, std::strerror(errno));

    if (::lseek(file_.fd(), 0, SEEK_SET) == static_cast<off_t>(-1))
        throwFileCacheException(
            "StatusFile: cannot seek '{}': {}", path_, std::strerror(errno));

    if (fill)
        fill(file_.fd());
}

StatusFile::~StatusFile()
{
    // Explicitly close before unlink so the flock is released before the path
    // disappears.  Use closeNoThrow (not close) because ~StatusFile must never
    // throw: close() throws SystemError on a close(2) failure, which would
    // escape this implicitly noexcept destructor and terminate the process.
    // closeNoThrow() swallows the error and still sets the fd to -1,
    // preventing the member destructor from double-closing.
    file_.closeNoThrow();

    // Best-effort removal; ignore errors (destructor must not throw).
    (void)::unlink(path_.c_str());
}

} // namespace facebook::velox::ch
