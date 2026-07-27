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

/// BufferedInput trace replay main.
///
/// Validates the trace manifest (must be q04, drivers=1), then runs A, B, and
/// C (cold + warm) replay phases in order. Verifies path gates per phase.
/// Does NOT delete any files or cache paths. Phase 1 emits no timing data.

#include <filesystem>
#include <fstream>
#include <iostream>

#include <folly/ScopeGuard.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/json.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/benchmarks/BufferedInputTraceReplay.h"

DEFINE_string(trace_path, "", "Path to the trace root directory");
DEFINE_string(dataset_root, "", "Dataset root matching the trace manifest");
DEFINE_string(cache_root, "", "Cache root for the C (FileCache) backend");
DEFINE_uint64(cache_capacity_bytes, 128ULL << 20, "FileCache capacity in bytes");
DEFINE_bool(verify_bytes, true, "Compare consumed bytes against oracle source files");
DEFINE_string(output, "", "Path for JSON result output (empty = stdout)");
DEFINE_bool(
    single_thread_timing_pilot,
    false,
    "SUPERSEDED: the single-manager timing pilot. Use "
    "--cold_filecache_timing_pilot.");
DEFINE_bool(
    cold_filecache_timing_pilot,
    false,
    "Run the fixed q04 cold/warm FileCache 3+2 A/B/C timing pilot.");
DEFINE_string(
    verification_cold_cache_root,
    "",
    "Sentinel-authenticated root for the untimed verification cold B pass.");
DEFINE_string(
    timed_cold_cache_roots,
    "",
    "Exactly five comma-separated sentinel-authenticated roots, one per timed "
    "cold B slot.");
DEFINE_string(
    warm_cache_root,
    "",
    "Sentinel-authenticated root for the persistent warm FileCache.");
DEFINE_string(
    cache_parent,
    "",
    "Canonical common parent directory of all seven cold/warm cache roots.");
DEFINE_string(
    replay_binary_build_id,
    "",
    "Build ID of THIS replay executable (required for the timing pilot; "
    "recorded distinctly from the capture binary).");

namespace {
namespace fs = std::filesystem;

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

void validateManifestForReplay(const BufferedInputTraceDocument& trace)
{
    VELOX_CHECK_EQ(
        trace.manifest.queryId, 4,
        "Replay is only supported for q04 traces (got queryId={})",
        trace.manifest.queryId);
    VELOX_CHECK_EQ(
        trace.manifest.drivers, 1,
        "Replay requires a single-driver trace (got drivers={})",
        trace.manifest.drivers);
    VELOX_CHECK(!trace.manifest.binaryRealPath.empty(),
                "Trace manifest missing binaryRealPath");
    VELOX_CHECK(!trace.manifest.binaryBuildId.empty(),
                "Trace manifest missing binaryBuildId");
    VELOX_CHECK(!trace.manifest.veloxHead.empty(),
                "Trace manifest missing veloxHead");
    VELOX_CHECK(!trace.manifest.glutenHead.empty(),
                "Trace manifest missing glutenHead");
    VELOX_CHECK(!trace.manifest.clickhouseHead.empty(),
                "Trace manifest missing clickhouseHead");
}

std::string formatResult(const BufferedInputTraceReplayResult& r,
                         const std::string& phase)
{
    return "{\"phase\":\"" + phase + "\""
        ",\"totalLogicalBytes\":" + std::to_string(r.totalLogicalBytes) +
        ",\"passthroughBytes\":" + std::to_string(r.passthroughBytes) +
        ",\"nextCount\":" + std::to_string(r.nextCount) +
        ",\"seekCount\":" + std::to_string(r.seekCount) +
        ",\"cacheHitCount\":" + std::to_string(r.cacheDelta.cacheHitCount) +
        ",\"cacheMissCount\":" + std::to_string(r.cacheDelta.cacheMissCount) +
        ",\"cacheReadBytes\":" + std::to_string(r.cacheDelta.cacheReadBytes) +
        ",\"sourceReadBytes\":" + std::to_string(r.cacheDelta.sourceReadBytes) +
        ",\"warmHitPct\":" + std::to_string(r.warmHitPct) +
        "}";
}

} // namespace

int main(int argc, char** argv)
{
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    VELOX_CHECK(!FLAGS_trace_path.empty(), "--trace_path is required");
    VELOX_CHECK(!FLAGS_dataset_root.empty(), "--dataset_root is required");
    VELOX_CHECK(
        !FLAGS_single_thread_timing_pilot,
        "--single_thread_timing_pilot is superseded; use "
        "--cold_filecache_timing_pilot");

    facebook::velox::filesystems::registerLocalFileSystem();
    facebook::velox::memory::initializeMemoryManager({});

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = FLAGS_trace_path;
    cfg.datasetRoot = FLAGS_dataset_root;
    cfg.cacheRoot = FLAGS_cache_root; // empty is fine for the cold pilot
    cfg.cacheCapacityBytes = FLAGS_cache_capacity_bytes;
    cfg.verifyBytes = FLAGS_verify_bytes;

    // Construct replayer — validates trace on construction.
    BufferedInputTraceReplayer replayer{cfg};
    validateManifestForReplay(replayer.trace());

    // --------------------------------------------------------------------
    // Cold/warm timing pilot mode: fixed q04 Direct/cold-FileCache/warm-FileCache
    // 3+2 timing over seven pre-created, sentinel-authenticated cache roots.
    // --------------------------------------------------------------------
    if (FLAGS_cold_filecache_timing_pilot)
    {
        VELOX_CHECK(FLAGS_verify_bytes,
                    "--verify_bytes must be true for the cold pilot");
        VELOX_CHECK(!FLAGS_output.empty(),
                    "--output is required for the cold pilot");
        VELOX_CHECK(!fs::exists(FLAGS_output),
                    "--output path must not already exist: {}", FLAGS_output);
        std::error_code ec;
        const fs::path outParent = fs::absolute(FLAGS_output).parent_path();
        VELOX_CHECK(
            fs::is_directory(outParent, ec) && !ec,
            "--output parent directory does not exist or is not a directory: {}",
            outParent.string());
        VELOX_CHECK(!FLAGS_replay_binary_build_id.empty(),
                    "--replay_binary_build_id is required for the cold pilot");
        VELOX_CHECK(!FLAGS_cache_parent.empty(),
                    "--cache_parent is required for the cold pilot");
        VELOX_CHECK(!FLAGS_verification_cold_cache_root.empty(),
                    "--verification_cold_cache_root is required");
        VELOX_CHECK(!FLAGS_warm_cache_root.empty(),
                    "--warm_cache_root is required");
        VELOX_CHECK_GT(FLAGS_cache_capacity_bytes, 0u,
                    "--cache_capacity_bytes must be positive");

        Q04ColdWarmTimingRoots rawRoots;
        rawRoots.verificationColdRoot = FLAGS_verification_cold_cache_root;
        rawRoots.timedColdRoots =
            parseTimedColdCacheRoots(FLAGS_timed_cold_cache_roots);
        rawRoots.warmRoot = FLAGS_warm_cache_root;

        // Preflight every root before constructing any manager, and use the
        // canonical roots everywhere (factory, orchestrator, JSON) so provenance
        // never records a symlinked or non-canonical spelling.
        const Q04ColdWarmTimingRoots roots =
            canonicalizeColdWarmRoots(rawRoots, FLAGS_cache_parent);

        const fs::path replayRealPath = fs::canonical("/proc/self/exe", ec);
        VELOX_CHECK(!ec,
                    "Cannot resolve replay executable realpath from "
                    "/proc/self/exe: {}", ec.message());
        const Q04ReplayTimingIdentity replayIdentity{
            replayRealPath.string(), FLAGS_replay_binary_build_id};

        // One process-lifetime pool/filesystem/timekeeper owner outliving every
        // sequential manager the factory constructs.
        auto sharedPool =
            facebook::velox::memory::memoryManager()->addLeafPool(
                "cold-warm-pilot");
        auto sharedFs = facebook::velox::filesystems::getFileSystem(
            fs::canonical(FLAGS_cache_parent).string(), {});
        auto sharedTimekeeper =
            std::make_shared<folly::ThreadWheelTimekeeper>();
        const uint64_t capacity = FLAGS_cache_capacity_bytes;

        // Belt-and-suspenders: no global instance leak on any exception (the
        // orchestrator's RAII already hides on failure).
        SCOPE_EXIT { ch::FileCacheManager::setInstance(nullptr); };

        // The factory never creates a root or sentinel; managers are returned
        // hidden and initialized, and the orchestrator owns shutdown order.
        ReplayFileCacheManagerFactory factory =
            [sharedPool, sharedFs, sharedTimekeeper, capacity](
                const std::string& r) -> std::shared_ptr<ch::FileCacheManager>
        {
            const std::string canonRoot = fs::canonical(r).string();
            ch::FileCacheConfig fcfg;
            fcfg.path = canonRoot;
            fcfg.maxSize = capacity;
            fcfg.backgroundDownloadThreads = 0;
            ch::FileCacheManager::Options opts;
            opts.caches = {
                {.name = "default", .config = fcfg, .configPath = canonRoot}};
            opts.defaultCacheName = "default";
            opts.commonUserId = "replay";
            opts.cachePathPrefix = canonRoot;
            opts.allowedCacheRoot = canonRoot;
            opts.localFileSystem = sharedFs;
            opts.memoryPool = sharedPool.get();
            opts.timekeeper = sharedTimekeeper;
            opts.initializeOnCreate = true;
            return ch::FileCacheManager::create(std::move(opts));
        };

        Q04ReplayTimingPilotResult result;
        runQ04ColdWarmTimingPilot(replayer, roots, factory, result);

        VELOX_CHECK_NULL(
            ch::FileCacheManager::getInstance(),
            "FileCacheManager must be hidden after the cold pilot");

        const folly::dynamic json = q04ColdWarmTimingToJson(
            replayer.trace(), cfg, result, replayIdentity, roots);

        // Write only after full verification/sample/analysis validity; the path
        // must not pre-exist.
        std::ofstream out(FLAGS_output, std::ios::trunc);
        VELOX_CHECK(out.is_open(),
                    "Cannot open cold pilot output file {}", FLAGS_output);
        out << folly::toJson(json) << "\n";
        out.flush();
        VELOX_CHECK(out.good(),
                    "Failed writing cold pilot output to {}", FLAGS_output);
        LOG(INFO) << "[cold-pilot] q04 cold/warm timing pilot written to "
                  << FLAGS_output;
        return 0;
    }

    // Functional replay mode (both timing flags false): A, B, C cold+warm.
    VELOX_CHECK(!FLAGS_cache_root.empty(),
        "--cache_root is required for functional replay");
    VELOX_CHECK(FLAGS_verify_bytes,
        "--verify_bytes must be true for functional replay; "
        "byte verification is mandatory");
    // Verify cache sentinel exists and is a real regular file before setup.
    validateCacheSentinel(cfg.cacheRoot);

    // Phase A — DirectBufferedInput.
    LOG(INFO) << "[replay] Running phase A (Direct)";
    const auto resA = replayer.replay(BufferedInputReplayBackend::kDirect);
    VELOX_CHECK_EQ(resA.passthroughBytes, 0u,
                   "Phase A: expected zero passthrough bytes");
    VELOX_CHECK_EQ(resA.cacheDelta.cacheReadBytes, 0u,
                   "Phase A: expected zero FileCache reads");
    LOG(INFO) << "[replay] Phase A done: logicalBytes=" << resA.totalLogicalBytes;

    // Phase B — FileCacheBufferedInput passthrough.
    LOG(INFO) << "[replay] Running phase B (Passthrough)";
    const auto resB = replayer.replay(BufferedInputReplayBackend::kPassthrough);
    VELOX_CHECK_GT(resB.passthroughBytes, 0u,
                   "Phase B: expected positive passthrough bytes");
    VELOX_CHECK_EQ(resB.cacheDelta.cacheReadBytes, 0u,
                   "Phase B: expected zero FileCache reads");
    VELOX_CHECK_EQ(resA.totalLogicalBytes, resB.totalLogicalBytes,
                   "Phase B: logical bytes differ from phase A");
    LOG(INFO) << "[replay] Phase B done: passthroughBytes=" << resB.passthroughBytes;

    // Phase C — FileCacheBufferedInput with real FileCache.
    // Install FileCacheManager as global instance; RAII guard ensures cleanup
    // on both normal and exceptional paths.
    auto pool = facebook::velox::memory::memoryManager()->addLeafPool("replay-main");
    const std::string cachePath = fs::absolute(cfg.cacheRoot).string();
    ch::FileCacheConfig fcfg;
    fcfg.path = cachePath;
    fcfg.maxSize = cfg.cacheCapacityBytes;
    fcfg.backgroundDownloadThreads = 0;
    ch::FileCacheManager::Options opts;
    opts.caches = {{.name = "default", .config = fcfg, .configPath = cachePath}};
    opts.defaultCacheName = "default";
    opts.commonUserId = "replay";
    opts.cachePathPrefix = cachePath;
    opts.allowedCacheRoot = cachePath;
    opts.localFileSystem = facebook::velox::filesystems::getFileSystem(cachePath, {});
    opts.memoryPool = pool.get();
    opts.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
    opts.initializeOnCreate = true;
    auto manager = ch::FileCacheManager::create(std::move(opts));
    ch::FileCacheManager::setInstance(manager.get());

    SCOPE_EXIT
    {
        manager->shutdown();
        ch::FileCacheManager::setInstance(nullptr);
    };

    LOG(INFO) << "[replay] Running phase C cold";
    const auto resCold = replayer.replay(BufferedInputReplayBackend::kFileCache);
    VELOX_CHECK_EQ(resA.totalLogicalBytes, resCold.totalLogicalBytes,
                   "Phase C cold: logical bytes differ from phase A");
    LOG(INFO) << "[replay] Phase C cold done: sourceReadBytes="
              << resCold.cacheDelta.sourceReadBytes;

    LOG(INFO) << "[replay] Running phase C warm";
    const auto resWarm = replayer.replay(BufferedInputReplayBackend::kFileCache);
    VELOX_CHECK_EQ(resA.totalLogicalBytes, resWarm.totalLogicalBytes,
                   "Phase C warm: logical bytes differ from phase A");

    // Full warm-hit gate: must have lookups, all hits, no misses, cache bytes>0,
    // no source reads, no predownload, no evictions.
    const uint64_t warmLookups =
        resWarm.cacheDelta.cacheHitCount + resWarm.cacheDelta.cacheMissCount;
    VELOX_CHECK_GT(warmLookups, 0u, "Phase C warm: zero cache lookups");
    VELOX_CHECK_GT(resWarm.cacheDelta.cacheHitCount, 0u,
                   "Phase C warm: zero cache hits");
    VELOX_CHECK_EQ(resWarm.cacheDelta.cacheMissCount, 0u,
                   "Phase C warm: expected zero cache misses");
    VELOX_CHECK_GT(resWarm.cacheDelta.cacheReadBytes, 0u,
                   "Phase C warm: expected positive cacheReadBytes");
    VELOX_CHECK_EQ(resWarm.cacheDelta.sourceReadBytes, 0u,
                   "Phase C warm: expected zero source reads");
    VELOX_CHECK_EQ(resWarm.cacheDelta.predownloadedFromSourceBytes, 0u,
                   "Phase C warm: expected zero predownload-from-source bytes");
    VELOX_CHECK_EQ(resWarm.cacheDelta.predownloadedBytes, 0u,
                   "Phase C warm: expected zero predownloaded bytes");
    VELOX_CHECK_EQ(resWarm.cacheDelta.evictedBytes, 0u,
                   "Phase C warm: expected zero evicted bytes");
    VELOX_CHECK_EQ(resWarm.cacheDelta.evictedSegments, 0u,
                   "Phase C warm: expected zero evicted segments");
    VELOX_CHECK(
        resWarm.warmHitPct == 100.0,
        "Phase C warm: expected 100%% cache hit rate, got {}%%",
        resWarm.warmHitPct);
    LOG(INFO) << "[replay] Phase C warm done: hitPct=" << resWarm.warmHitPct;

    // Emit results.
    const std::string json =
        "{\"phases\":[" +
        formatResult(resA, "A") + "," +
        formatResult(resB, "B") + "," +
        formatResult(resCold, "C_cold") + "," +
        formatResult(resWarm, "C_warm") +
        "]}";

    if (FLAGS_output.empty())
    {
        std::cout << json << "\n";
    }
    else
    {
        std::ofstream out(FLAGS_output);
        VELOX_CHECK(out.is_open(), "Cannot open output file {}", FLAGS_output);
        out << json << "\n";
        LOG(INFO) << "[replay] Results written to " << FLAGS_output;
    }

    LOG(INFO) << "[replay] All phases complete";
    return 0;
}
