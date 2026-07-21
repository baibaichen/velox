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

#include "velox/ch/benchmarks/AbBenchmarkMain.h"

#include <filesystem>
#include <memory>
#include <system_error>

#include <folly/ScopeGuard.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <gflags/gflags.h>

#include "velox/ch/Disks/IO/FileCacheBufferedInputBuilder.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/benchmarks/AbBenchmarkBase.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"

DECLARE_string(input_source);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_int32(cache_gb);

namespace facebook::velox::ch::benchmarks
{
namespace
{

constexpr const char * kAbCacheName = "ab_benchmark";

// Wipes FLAGS_filecache_root, recreates it, then builds a FileCache-configured
// FileCacheManager sized to FLAGS_filecache_disk_gib whose default cache is
// kAbCacheName. This is the Manager-build pattern from FileCacheSeekBenchmark;
// the returned Manager owns the cache. NO bare `new ch::FileCache`, NO
// `ch::FileCache::setInstance` -- the connector reaches the cache exclusively
// through the Task 018a builder registered on this Manager.
std::shared_ptr<FileCacheManager> buildFileCacheManager()
{
    // Wipe cache root so round 1 is always a true cold miss.
    std::error_code ec;
    std::filesystem::remove_all(FLAGS_filecache_root, ec);
    // remove_all failure is benign when the dir simply did not exist; the next
    // create_directories call is the authoritative check.
    ec.clear();
    // The local file system matches on an absolute path; cache/segment files are
    // opened via filesystems::getFileSystem(path), so the cache root must be
    // absolute.
    const std::string cacheRoot = std::filesystem::absolute(FLAGS_filecache_root).string();
    std::filesystem::create_directories(cacheRoot, ec);
    VELOX_USER_CHECK(!ec, "Failed to create --filecache_root: {} ({})", cacheRoot, ec.message());

    FileCacheConfig config;
    config.path = cacheRoot;
    config.maxSize = static_cast<uint64_t>(FLAGS_filecache_disk_gib) << 30;
    config.maxElements = 10'000'000;
    config.maxFileSegmentSize = 8ULL << 20;
    config.boundaryAlignment = 1;
    config.reserveGranularity = 1;
    config.cachePolicy = FileCachePolicy::LRU;
    config.useSplitCache = false;
    config.backgroundDownloadThreads = 0;
    config.loadMetadataThreads = 2;
    config.loadMetadataAsynchronously = false;
    config.keepFreeSpaceSizeRatio = 0.0;
    config.keepFreeSpaceElementsRatio = 0.0;

    FileCacheManager::Options options;
    options.commonUserId = "ab-user";
    options.localFileSystem = filesystems::getFileSystem("/", nullptr);
    options.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
    options.initializeOnCreate = true;
    options.defaultCacheName = kAbCacheName;
    options.caches.push_back({kAbCacheName, config, "conf.ab"});
    return FileCacheManager::create(options);
}

} // namespace

int32_t dispatchAbMain(AbBenchmarkBase & ab, const std::function<void()> & runLegacy)
{
    if (FLAGS_input_source.empty())
    {
        runLegacy();
        return 0;
    }

    // Kept alive until end of dispatch; the Manager MUST outlive the registered
    // builder (which holds a FileCacheManager&).
    std::shared_ptr<FileCacheManager> fileCacheManager;
    // Tear the Manager down cleanly even if runAb() throws.
    auto managerGuard = folly::makeGuard(
        [&fileCacheManager]
        {
            if (fileCacheManager != nullptr)
            {
                fileCacheManager->shutdown();
                FileCacheManager::setInstance(nullptr);
            }
        });

    if (FLAGS_input_source == "filecache")
    {
        // QueryBenchmarkBase::initialize skips AsyncDataCache construction when
        // FLAGS_cache_gb == 0; force it so connectorQueryCtx->cache() stays
        // nullptr and the Task 018a builder selects FileCacheBufferedInput.
        FLAGS_cache_gb = 0;
        ab.setBackend(AbBackend::kFileCache);
    }
    else if (FLAGS_input_source == "cbi")
    {
        VELOX_USER_CHECK_GT(FLAGS_cache_gb, 0, "--input_source=cbi requires --cache_gb > 0");
        ab.setBackend(AbBackend::kCbi);
    }
    else if (FLAGS_input_source == "direct")
    {
        // No application-level cache: with FLAGS_cache_gb == 0 the AsyncDataCache
        // tier stays nullptr and no FileCache builder is installed, so
        // createBufferedInput() falls back to DirectBufferedInput.
        FLAGS_cache_gb = 0;
        ab.setBackend(AbBackend::kDirect);
    }
    else
    {
        VELOX_USER_FAIL("Unknown --input_source: {} (expected cbi, filecache, or direct)", FLAGS_input_source);
    }

    // Registers filesystems, the Hive connector, and the parquet reader factory
    // (and, for cbi, the AsyncDataCache singleton). Must run before we register
    // our builder, because the builder overrides the connector's buffered-input
    // selection installed here.
    ab.initialize();

    if (FLAGS_input_source == "filecache")
    {
        fileCacheManager = buildFileCacheManager();
        FileCacheManager::setInstance(fileCacheManager.get());
        // Task 018a: install the process-wide connector buffered-input builder so
        // TPCH reads route through our FileCache. Throws (not caught) if the
        // Manager has no default cache -- a build error, surfaced immediately.
        registerFileCacheBufferedInputBuilder(*fileCacheManager);
    }

    // Wire the per-backend cold-reset used by --cold_each_round.
    if (FLAGS_input_source == "filecache")
    {
        ab.setColdResetFn(
            [&fileCacheManager]
            {
                fileCacheManager->shutdown();
                FileCacheManager::setInstance(nullptr);
                fileCacheManager = buildFileCacheManager();
                FileCacheManager::setInstance(fileCacheManager.get());
                registerFileCacheBufferedInputBuilder(*fileCacheManager);
            });
    }
    else
    {
        ab.setColdResetFn([&ab] { ab.clearCbiCache(); });
    }

    const int32_t failed = ab.runAb();
    ab.shutdown();

    // Soft-cap: only signal systemic failure when more than 10 queries failed.
    return failed > 10 ? 1 : 0;
}

} // namespace facebook::velox::ch::benchmarks
