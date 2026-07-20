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

#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"
#include "velox/common/file/FileSystems.h"

#include <folly/futures/ThreadWheelTimekeeper.h>

#include <memory>
#include <mutex>
#include <string>

namespace facebook::velox::ch::test
{

/// Register the Velox local file system exactly once for the test process. The
/// D1 `OpenedFileCache` opens cache-segment files through `filesystems::FileSystem`,
/// which requires the local scheme to be registered.
inline std::shared_ptr<filesystems::FileSystem> localFileSystemForTests()
{
    static std::once_flag flag;
    std::call_once(flag, [] { filesystems::registerLocalFileSystem(); });
    return filesystems::getFileSystem("/", nullptr);
}

/// Direct-injection helper for the D3 (Task 013) `FileCache` constructor. Owns the
/// runtime resources the Manager would otherwise own and supplies them by reference,
/// so the center-SCC tests can construct a `FileCache` without a full Manager.
///
/// Declaration order = construction order and the reverse of destruction order:
/// `fileSystem_`/`workerPool_`/`timekeeper_` are constructed before, and destroyed
/// after, the `scheduler_`/`openedFileCache_` that reference them.
struct FileCacheTestResources
{
    FileCacheTestResources()
        : fileSystem_(localFileSystemForTests())
        , timekeeper_(std::make_shared<folly::ThreadWheelTimekeeper>())
        , workerPool_(64, 1, "FileCacheTest")
        , scheduler_(timekeeper_, workerPool_)
        , openedFileCache_(*fileSystem_)
    {
    }

    std::unique_ptr<FileCache> makeFileCache(
        const std::string & name, const FileCacheSettings & settings, const std::string & commonUserId)
    {
        return std::make_unique<FileCache>(
            name, settings, workerPool_, scheduler_, openedFileCache_, *fileSystem_, commonUserId);
    }

    std::shared_ptr<filesystems::FileSystem> fileSystem_;
    std::shared_ptr<folly::Timekeeper> timekeeper_;
    FileCacheWorkerPool workerPool_;
    FileCacheScheduler scheduler_;
    OpenedFileCache openedFileCache_;
};

} // namespace facebook::velox::ch::test
