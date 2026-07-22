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

#include "velox/benchmarks/AbBenchmarkMain.h"

#include <filesystem>
#include <memory>
#include <system_error>

#include <folly/ScopeGuard.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <gflags/gflags.h>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"

DECLARE_string(input_source);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_int32(cache_gb);

namespace facebook::velox::benchmarks {

namespace {

// Process-lifetime owners for the FileCacheManager and the runtime services it
// references. Declaration order matters for destruction: the manager is dropped
// first by teardownFileCache(), then the timekeeper/fs/pool it referenced.
std::shared_ptr<memory::MemoryPool> g_benchPool;
std::shared_ptr<filesystems::FileSystem> g_benchFs;
std::shared_ptr<folly::ThreadWheelTimekeeper> g_benchTimekeeper;
std::shared_ptr<ch::FileCacheManager> g_benchManager;

// Wipes FLAGS_filecache_root, recreates it, builds a FileCacheManager sized to
// FLAGS_filecache_disk_gib and installs it as the process-wide instance. The
// owning shared_ptrs live in the g_bench* statics; teardownFileCache() drops
// them in the documented order.
void installFileCache() {
  // Wipe cache root so round 1 is always a true cold miss. The shared helper is
  // fail-close and 018-D sentinel-aware: it refuses to touch dangerous or
  // unauthorized roots and preserves a sentinel when one is present.
  dwio::common::bench::clearBenchmarkCacheRoot(FLAGS_filecache_root);
  std::error_code ec;
  std::filesystem::create_directories(FLAGS_filecache_root, ec);
  VELOX_USER_CHECK(
      !ec,
      "Failed to create --filecache_root: {} ({})",
      FLAGS_filecache_root,
      ec.message());

  // FileCacheManager cache paths must be absolute (validateOptions).
  const std::string root =
      std::filesystem::absolute(FLAGS_filecache_root).string();

  filesystems::registerLocalFileSystem();
  g_benchPool = memory::memoryManager()->addLeafPool("filecache_bench");
  g_benchFs = filesystems::getFileSystem(root, {});
  g_benchTimekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();

  ch::FileCacheConfig cfg;
  cfg.path = root;
  cfg.maxSize = static_cast<uint64_t>(FLAGS_filecache_disk_gib) << 30;

  ch::FileCacheManager::Options opts;
  opts.caches = {{.name = "default", .config = cfg, .configPath = root}};
  opts.defaultCacheName = "default";
  opts.commonUserId = "benchmark";
  opts.cachePathPrefix = root;
  opts.allowedCacheRoot = root;
  opts.localFileSystem = g_benchFs;
  opts.memoryPool = g_benchPool.get();
  opts.timekeeper = g_benchTimekeeper;
  opts.initializeOnCreate = true;

  g_benchManager = ch::FileCacheManager::create(std::move(opts));
  ch::FileCacheManager::setInstance(g_benchManager.get());
}

// Tears the installed FileCacheManager down in the documented strict order:
// shutdown() -> setInstance(nullptr) -> drop the owning shared_ptr, then release
// the timekeeper/fs/pool the manager referenced. No-op when no manager is live
// (cbi/direct backends), so it is safe to call unconditionally from the guard.
void teardownFileCache() {
  if (g_benchManager) {
    const auto snapshot = ch::takeFileCacheStatsSnapshot();
    LOG(INFO) << "FileCache teardown: cacheSize=" << snapshot.cacheSize
              << " cacheReadBytes=" << snapshot.cacheReadBytes
              << " sourceReadBytes=" << snapshot.sourceReadBytes
              << " cacheWriteBytes=" << snapshot.cacheWriteBytes
              << " hitCount=" << snapshot.cacheHitCount
              << " missCount=" << snapshot.cacheMissCount
              << " evictedBytes=" << snapshot.evictedBytes;
    g_benchManager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
    g_benchManager.reset();
    g_benchTimekeeper.reset();
    g_benchFs.reset();
    g_benchPool.reset();
  }
}

} // namespace

int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy) {
  if (FLAGS_input_source.empty()) {
    runLegacy();
    return 0;
  }

  // Tears the FileCacheManager down at end of dispatch, even if runAb() throws:
  // teardownFileCache() is a no-op unless the filecache backend installed one.
  auto instanceGuard = folly::makeGuard([] { teardownFileCache(); });
  if (FLAGS_input_source == "filecache") {
    // QueryBenchmarkBase::initialize skips AsyncDataCache construction when
    // FLAGS_cache_gb == 0; force it so the CBI tier stays nullptr.
    FLAGS_cache_gb = 0;
    installFileCache();
  } else if (FLAGS_input_source == "cbi") {
    VELOX_USER_CHECK_GT(
        FLAGS_cache_gb, 0, "--input_source=cbi requires --cache_gb > 0");
  } else if (FLAGS_input_source == "direct") {
    // No application-level cache: with FLAGS_cache_gb == 0 the AsyncDataCache
    // tier stays nullptr and no ch::FileCache singleton is installed, so
    // createBufferedInput() falls back to DirectBufferedInput (pure Velox
    // direct reads). Must be set before ab.initialize() builds the cache.
    FLAGS_cache_gb = 0;
  } else {
    VELOX_USER_FAIL(
        "Unknown --input_source: {} (expected cbi, filecache, or direct)",
        FLAGS_input_source);
  }

  ab.initialize();

  // Wire the per-backend cold-reset used by --cold_each_round. filecache tears
  // down and reinstalls its singleton (re-wiping disk + metadata via the same
  // installFileCache() path used at startup); the else branch covers cbi (clears
  // its AsyncDataCache) and direct (clearCbiCache is a no-op as no cache exists).
  if (FLAGS_input_source == "filecache") {
    ab.setColdResetFn([]() {
      teardownFileCache();
      installFileCache();
    });
  } else {
    ab.setColdResetFn([&ab]() { ab.clearCbiCache(); });
  }

  const int32_t failed = ab.runAb();
  ab.shutdown();

  // Soft-cap: only signal systemic failure to the shell when more than 10
  // queries failed. Sweep results are still in --out.
  return failed > 10 ? 1 : 0;
}

} // namespace facebook::velox::benchmarks
