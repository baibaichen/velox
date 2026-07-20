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

#include "velox/common/base/Exceptions.h"

#include <fmt/format.h>

#include <cstddef>
#include <string_view>
#include <utility>

namespace facebook::velox::ch
{

template <typename... Args>
[[noreturn]] void throwFileCacheException(
    fmt::format_string<Args...> format,
    Args &&... args)
{
    VELOX_FAIL(
        "{}",
        fmt::format(format, std::forward<Args>(args)...));
}

/// Typed errno-carrying exception, consumed by `FileSegment::write` to reconcile
/// a short physical write against the on-disk file size — but only for the
/// space-exhaustion errnos (`ENOSPC`/`EDQUOT`). It mirrors ClickHouse's
/// `ErrnoException`/`getErrno()` used by the same code path.
///
/// The ClickHouse-shaped design targets `VeloxRuntimeError`, but that class is
/// `final`; this derives from the non-final `velox::VeloxException` base
/// instead, carrying the runtime error source/type so it groups with other
/// runtime failures.
///
/// This task only defines and *consumes* the type; the concrete producer (a
/// Velox `WriteFile` that raises a structured errno on a short write) remains a
/// separate pre-release gate. Until that producer exists, the reconciliation
/// branch is exercised only by the FileSegment tests, which raise this type
/// directly from a real-file-backed throwing writer double.
class FileCacheErrnoException : public velox::VeloxException
{
public:
    FileCacheErrnoException(
        const char * file,
        size_t line,
        const char * function,
        std::string_view message,
        int errnoCode)
        : velox::VeloxException(
              file,
              line,
              function,
              /* expression */ "",
              message,
              velox::error_source::kErrorSourceRuntime,
              velox::error_code::kUnknown,
              /* isRetriable */ false,
              velox::VeloxException::Type::kSystem,
              /* exceptionName */ "FileCacheErrnoException"),
          savedErrno_(errnoCode)
    {
    }

    int getErrno() const { return savedErrno_; }

private:
    int savedErrno_;
};

}
