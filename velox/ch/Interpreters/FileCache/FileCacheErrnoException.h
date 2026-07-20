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

#include <fmt/format.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace facebook::velox::ch
{

/// FileCache-owned typed exception carrying a numeric POSIX `errno`.
///
/// It replaces CH's `ErrnoException` on the `FileSegment::write` path. The
/// structured-errno contract (R6/007-2 + E1) requires that callers inspect
/// `getErrno` rather than parse exception text: `FileSegment::write` reconciles
/// the downloaded size only for `ENOSPC`/`EDQUOT` and rethrows the original
/// exception; `CacheMetadata::downloadImpl` breaks the background-download loop
/// on the same two codes.
///
/// Note (pre-release gap, per the Task-012 amendment): the production Velox
/// `LocalWriteFile::append` reports a short write via `VELOX_CHECK`, not via
/// this typed exception, so no production code path currently *produces* a
/// `FileCacheErrnoException`. That absence is a separate pre-release gap and
/// must not be papered over with a "reconcile every exception" fallback. The
/// partial-physical-append-failure test (S4) drives this path by injecting a
/// production `velox::WriteFile` that commits a strict prefix and then throws a
/// `FileCacheErrnoException`.
class FileCacheErrnoException : public std::runtime_error
{
public:
    FileCacheErrnoException(int errno_, const std::string & message)
        : std::runtime_error(fmt::format("{} (errno: {})", message, errno_)), errno_value(errno_)
    {
    }

    template <typename... Args>
    FileCacheErrnoException(int errno_, fmt::format_string<Args...> format, Args &&... args)
        : FileCacheErrnoException(errno_, fmt::format(format, std::forward<Args>(args)...))
    {
    }

    /// Numeric POSIX errno; callers must NOT parse exception text.
    int getErrno() const noexcept { return errno_value; }

private:
    int errno_value;
};

}
