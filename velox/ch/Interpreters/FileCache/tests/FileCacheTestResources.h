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
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/OpenedFileCache.h"
#include "velox/common/file/FileSystems.h"

#include <folly/futures/ThreadWheelTimekeeper.h>

#include <filesystem>
#include <fstream>
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

/// Deterministic pseudo-random file content of `n` bytes, used by the IO
/// integration tests to fill source files and assert byte-exact round trips.
inline std::string makeContent(size_t n)
{
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i)
        s[i] = static_cast<char>((static_cast<uint32_t>(i) * 2654435761u) >> 24);
    return s;
}

/// Join a name onto a temp base directory.
inline std::string subPath(const std::string & base, const std::string & name)
{
    return (std::filesystem::path(base) / name).string();
}

/// Write `content` to `base/name` as a binary source file and return its path.
inline std::string writeSourceFile(const std::string & base, const std::string & name, const std::string & content)
{
    const auto path = subPath(base, name);
    std::ofstream(path, std::ios::binary) << content;
    return path;
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

/// Create a single-cache `FileCacheManager` with the default test configuration,
/// install it as the process instance, and return its default cache. `managerOut`
/// receives the owning manager so the caller's fixture can shut it down in its
/// teardown. Used by the IO integration fixtures, which otherwise duplicated this
/// whole config/options/install sequence verbatim.
inline FileCachePtr installManagerDefaultCache(
    std::shared_ptr<FileCacheManager> & managerOut,
    const std::string & cachePath,
    size_t seg,
    size_t align,
    size_t maxSize = 16 * 1024 * 1024)
{
    FileCacheConfig c;
    c.path = cachePath;
    c.maxSize = maxSize;
    c.maxElements = 100;
    c.maxFileSegmentSize = seg;
    c.boundaryAlignment = align;
    c.reserveGranularity = 1;
    c.cachePolicy = FileCachePolicy::LRU;
    c.useSplitCache = false;
    c.backgroundDownloadThreads = 0;
    c.loadMetadataThreads = 2;
    c.loadMetadataAsynchronously = false;
    c.keepFreeSpaceSizeRatio = 0.0;
    c.keepFreeSpaceElementsRatio = 0.0;

    FileCacheManager::Options o;
    o.commonUserId = "user-A";
    o.localFileSystem = filesystems::getFileSystem("/", nullptr);
    o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
    o.initializeOnCreate = true;
    o.defaultCacheName = "default";
    o.caches.push_back({"default", c, "conf.default"});

    managerOut = FileCacheManager::create(o);
    FileCacheManager::setInstance(managerOut.get());
    return managerOut->getDefault();
}

} // namespace facebook::velox::ch::test
