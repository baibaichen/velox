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
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

DECLARE_string(input_source);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_int32(cache_gb);
DECLARE_int32(query_id);
DECLARE_int32(num_drivers);
DECLARE_int32(rounds);

DEFINE_bool(
    buffered_input_perf_probe,
    false,
    "Collect Task-018S BufferedInput probe counters. When true, sets the "
    "buffered_input_perf_probe connector session property so IoStatistics "
    "records enqueue/Next/seek facts per split.");

DEFINE_string(
    buffered_input_trace_root,
    "",
    "When nonempty, activates BufferedInput trace capture for the single "
    "selected query+round and writes a trace to this directory.  Directory "
    "must not exist.  Requires --input_source=direct, --num_drivers=1, "
    "--rounds=1, --query_id>0, and all repo/binary identity flags.");

DEFINE_string(
    buffered_input_trace_dataset_root,
    "",
    "Canonical dataset root for computing dataset-relative file paths in the "
    "trace.  Required when --buffered_input_trace_root is set.");

DEFINE_int32(
    buffered_input_trace_round,
    1,
    "Which benchmark round to capture (1-based).  Must be 1 when "
    "--rounds=1.");

DEFINE_string(
    buffered_input_trace_velox_head,
    "",
    "Velox HEAD commit hash embedded in the trace manifest.");

DEFINE_string(
    buffered_input_trace_gluten_head,
    "",
    "Gluten HEAD commit hash embedded in the trace manifest.");

DEFINE_string(
    buffered_input_trace_clickhouse_head,
    "",
    "ClickHouse HEAD commit hash embedded in the trace manifest.");

DEFINE_string(
    buffered_input_trace_binary_build_id,
    "",
    "Build identifier for the benchmark binary embedded in the trace "
    "manifest.");

DEFINE_uint64(
    buffered_input_trace_max_events,
    5'000'000,
    "Hard cap on the number of events buffered in memory during trace "
    "capture.  Capture fails closed when the limit is reached.");

DECLARE_bool(cold_each_round);

DEFINE_string(
    filecache_root_mode,
    "reset",
    "Benchmark-only FileCache root lifecycle: 'reset' wipes before startup; "
    "'reuse' requires and reloads an existing populated root.");

namespace facebook::velox::benchmarks {

namespace {

// Process-lifetime owners for the FileCacheManager and the runtime services it
// references. Declaration order matters for destruction: the manager is dropped
// first by teardownFileCache(), then the timekeeper/fs/pool it referenced.
std::shared_ptr<memory::MemoryPool> g_benchPool;
std::shared_ptr<filesystems::FileSystem> g_benchFs;
std::shared_ptr<folly::ThreadWheelTimekeeper> g_benchTimekeeper;
std::shared_ptr<ch::FileCacheManager> g_benchManager;

// Builds a FileCacheManager sized to FLAGS_filecache_disk_gib and installs it
// as the process-wide instance. The owning shared_ptrs live in the g_bench*
// statics; teardownFileCache() drops them in the documented order.
//
// rootMode == kReset: wipes and recreates FLAGS_filecache_root so round 1 is
// always a true cold miss. The shared helper is fail-close and 018-D
// sentinel-aware: it refuses to touch dangerous or unauthorized roots and
// preserves a sentinel when one is present.
//
// rootMode == kReuse: never creates or clears the root; it must already exist
// and be populated by a prior --filecache_root_mode=reset run. Validated by
// the shared, non-destructive validateBenchmarkCacheRootForReuse() helper.
void installFileCache(FileCacheRootMode rootMode) {
  std::string root;
  if (rootMode == FileCacheRootMode::kReset) {
    dwio::common::bench::clearBenchmarkCacheRoot(FLAGS_filecache_root);
    std::error_code ec;
    std::filesystem::create_directories(FLAGS_filecache_root, ec);
    VELOX_USER_CHECK(
        !ec,
        "Failed to create --filecache_root: {} ({})",
        FLAGS_filecache_root,
        ec.message());
    // FileCacheManager cache paths must be absolute (validateOptions).
    root = std::filesystem::absolute(FLAGS_filecache_root).string();
  } else {
    root = dwio::common::bench::validateBenchmarkCacheRootForReuse(
        FLAGS_filecache_root);
  }

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

  if (rootMode == FileCacheRootMode::kReuse) {
    // Metadata load is synchronous under current config: refreshStats()
    // immediately after create() reflects what was loaded from disk. Fail
    // close if the (already-validated, non-empty-directory) root turns out to
    // hold no resident FileCache bytes, so a stale/irrelevant directory can't
    // silently become a cold benchmark.
    uint64_t residentBytes = 0;
    for (const auto& [name, stats] :
         g_benchManager->refreshStats().cachesByName) {
      residentBytes += stats.usedSize;
    }
    VELOX_USER_CHECK_GT(
        residentBytes,
        0,
        "--filecache_root_mode=reuse loaded no resident FileCache bytes from "
        "'{}'; run a reset population first",
        root);
  }
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

int32_t abExitCode(int32_t failed) {
  // Any failed query must produce a nonzero process exit so shell scripts and
  // orchestrators can detect it; only a clean sweep returns 0.
  return failed > 0 ? 1 : 0;
}

AbInputSource parseAbInputSource(std::string_view token) {
  if (token == "direct")
  {
    return AbInputSource::kDirect;
  }
  if (token == "filecache_passthrough")
  {
    return AbInputSource::kFileCachePassthrough;
  }
  if (token == "filecache")
  {
    return AbInputSource::kFileCache;
  }
  if (token == "cbi")
  {
    return AbInputSource::kCbi;
  }
  VELOX_USER_FAIL(
      "Unknown --input_source: {} (expected direct, filecache_passthrough, "
      "filecache, or cbi)",
      token);
}

FileCacheRootMode parseFileCacheRootMode(
    const std::string& inputSource,
    const std::string& rootMode,
    bool coldEachRound)
{
  if (rootMode == "reset")
  {
    return FileCacheRootMode::kReset;
  }
  VELOX_USER_CHECK_EQ(
      rootMode,
      "reuse",
      "--filecache_root_mode must be 'reset' or 'reuse'");
  VELOX_USER_CHECK_EQ(
      inputSource,
      "filecache",
      "--filecache_root_mode=reuse requires --input_source=filecache");
  VELOX_USER_CHECK(
      !coldEachRound,
      "--filecache_root_mode=reuse is incompatible with "
      "--cold_each_round=true");
  return FileCacheRootMode::kReuse;
}

int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy) {
  // Validate --filecache_root_mode before any benchmark initialization, so
  // 'reuse' paired with an incompatible --input_source or
  // --cold_each_round=true fails fast. The default ('reset') is inert/
  // compatible with every --input_source, including the empty (legacy) value.
  const FileCacheRootMode rootMode = parseFileCacheRootMode(
      FLAGS_input_source, FLAGS_filecache_root_mode, FLAGS_cold_each_round);

  if (FLAGS_input_source.empty()) {
    runLegacy();
    return 0;
  }

  const AbInputSource src = parseAbInputSource(FLAGS_input_source);

  // Tears the FileCacheManager down at end of dispatch, even if runAb() throws:
  // teardownFileCache() is a no-op unless the filecache backend installed one.
  auto instanceGuard = folly::makeGuard([] { teardownFileCache(); });

  switch (src)
  {
    case AbInputSource::kFileCache:
      // QueryBenchmarkBase::initialize skips AsyncDataCache construction when
      // FLAGS_cache_gb == 0; force it so the CBI tier stays nullptr.
      FLAGS_cache_gb = 0;
      break;
    case AbInputSource::kFileCachePassthrough:
      // Passthrough needs no FileCache disk state; ensure the CBI tier also
      // stays down so createBufferedInput selects the passthrough path.
      FLAGS_cache_gb = 0;
      break;
    case AbInputSource::kCbi:
      VELOX_USER_CHECK_GT(
          FLAGS_cache_gb, 0, "--input_source=cbi requires --cache_gb > 0");
      break;
    case AbInputSource::kDirect:
      // No application-level cache: with FLAGS_cache_gb == 0 the AsyncDataCache
      // tier stays nullptr and no ch::FileCache singleton is installed, so
      // createBufferedInput() falls back to DirectBufferedInput (pure Velox
      // direct reads). Must be set before ab.initialize() builds the cache.
      FLAGS_cache_gb = 0;
      break;
  }

  ab.initialize();

  // Enable per-split BufferedInput probe statistics when requested.
  // The connector session property is read by createBufferedInput to call
  // IoStatistics::enableBufferedInputProbe() on each split's IoStatistics.
  if (FLAGS_buffered_input_perf_probe)
  {
    ab.setConnectorSessionProperty(
        exec::test::kHiveConnectorId,
        std::string(connector::hive::kBufferedInputPerfProbeSession),
        "true");
  }

  // Install FileCache after ab.initialize() since it needs the memory manager
  // that QueryBenchmarkBase::initialize sets up.
  if (src == AbInputSource::kFileCache)
  {
    installFileCache(rootMode);
  }

  // For passthrough, install the process-wide RAII guard that signals
  // createBufferedInput to skip FileCache state and read directly.
  std::optional<connector::hive::ScopedFileCachePassthroughForBenchmark>
      passthroughOverride;
  if (src == AbInputSource::kFileCachePassthrough)
  {
    passthroughOverride.emplace();
  }

  // Wire the per-backend cold-reset used by --cold_each_round and by the
  // untimed reference-query reset. filecache tears down and reinstalls its
  // singleton via the same installFileCache(rootMode) path used at startup:
  // in kReset mode this continues wiping disk + metadata on every reset; in
  // kReuse mode it releases <root>/status and reloads the same segment tree
  // without ever clearing it. The else branch covers cbi (clears its
  // AsyncDataCache) and direct (clearCbiCache is a no-op as no cache exists).
  if (src == AbInputSource::kFileCache)
  {
    ab.setColdResetFn([rootMode]() {
      teardownFileCache();
      installFileCache(rootMode);
    });
  }
  else
  {
    ab.setColdResetFn([&ab]() { ab.clearCbiCache(); });
  }

  // Wire trace capture when --buffered_input_trace_root is set.
  if (!FLAGS_buffered_input_trace_root.empty())
  {
    BufferedInputTraceRunConfig traceConfig;
    traceConfig.traceRoot = FLAGS_buffered_input_trace_root;
    traceConfig.datasetRoot = FLAGS_buffered_input_trace_dataset_root;
    traceConfig.queryId = FLAGS_query_id;
    traceConfig.round = FLAGS_buffered_input_trace_round;
    traceConfig.veloxHead = FLAGS_buffered_input_trace_velox_head;
    traceConfig.glutenHead = FLAGS_buffered_input_trace_gluten_head;
    traceConfig.clickhouseHead = FLAGS_buffered_input_trace_clickhouse_head;
    traceConfig.binaryBuildId = FLAGS_buffered_input_trace_binary_build_id;
    traceConfig.maxEvents = FLAGS_buffered_input_trace_max_events;
    validateBufferedInputTraceConfig(
        traceConfig,
        src,
        FLAGS_buffered_input_perf_probe,
        FLAGS_query_id,
        FLAGS_num_drivers,
        FLAGS_rounds);
    ab.setBufferedInputTraceRunConfig(std::move(traceConfig));
  }

  const int32_t failed = ab.runAb();
  ab.shutdown();

  // Any failed query must produce a nonzero process exit; sweep results
  // (including per-query errors) are still recorded in --out.
  return abExitCode(failed);
}

} // namespace facebook::velox::benchmarks
