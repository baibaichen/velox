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

#include "velox/ch/Common/FileCacheFilesystem.h"

#include <folly/File.h>

#include <functional>
#include <string>

namespace facebook::velox::ch
{

/// Provides that no more than one cache instance uses the same cache directory.
///
/// On construction it opens or creates the status file at `path`, acquires an
/// exclusive inter-process `flock` (non-blocking), truncates the file, and
/// writes diagnostic information via `fill`.  The lock is held until the
/// destructor runs.  The destructor explicitly closes the `folly::File` before
/// calling `unlink`, preserving the invariant that the lock is released before
/// the path disappears from the filesystem.
///
/// Constructor throws `VeloxRuntimeError` if the lock cannot be acquired
/// (another instance is running) or if any syscall fails.
/// Destructor ignores close and unlink errors (cannot throw).
class StatusFile
{
public:
    /// Callback writing diagnostic content directly to the open file descriptor.
    /// May be nullptr, in which case the file is created but left empty.
    using FillFunction = std::function<void(int fd)>;

    StatusFile(std::string path, FillFunction fill);
    ~StatusFile();

    StatusFile(const StatusFile &) = delete;
    StatusFile & operator=(const StatusFile &) = delete;

    /// Returns a FillFunction that writes the current PID as a decimal string.
    static FillFunction writePid();

    static FillFunction writeFullInfo();

private:
    const std::string path_;
    folly::File file_;
};

} // namespace facebook::velox::ch
