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

#include "velox/dwio/common/benchmarks/BufferedInputTraceReplay.h"

#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <set>
#include <vector>

#include <folly/futures/ThreadWheelTimekeeper.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/BufferedInputTrace.h"

namespace facebook::velox::dwio::common {
namespace {

namespace fs = std::filesystem;
using velox::common::testutil::TempDirectoryPath;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Writes `size` bytes of pattern `i & 0xFF` to `dir/name` and returns the
/// relative path (just the filename).
std::string writePatternFile(const std::string& dir, const std::string& name,
                             size_t size)
{
    const std::string path = dir + "/" + name;
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    VELOX_CHECK(f.is_open(), "cannot open {}", path);
    for (size_t i = 0; i < size; ++i)
        f.put(static_cast<char>(i & 0xFF));
    return name;
}

/// Build a BufferedInputTraceDocument for a single file at `relativePath`
/// (already in `canonicalDatasetRoot`) of `fileSize` bytes with the given
/// events list.
BufferedInputTraceDocument buildDocument(
    const std::string& canonicalDatasetRoot,
    const std::string& relativePath,
    uint64_t fileSize,
    std::vector<BufferedInputTraceEvent> events)
{
    BufferedInputTraceDocument doc;
    doc.manifest.queryId = 4;
    doc.manifest.drivers = 1;
    doc.manifest.datasetRoot = canonicalDatasetRoot;
    doc.manifest.binaryRealPath = "/proc/self/exe";
    doc.manifest.binaryBuildId = "test-build-id";
    doc.manifest.veloxHead = "velox-test-head";
    doc.manifest.glutenHead = "gluten-test-head";
    doc.manifest.clickhouseHead = "ch-test-head";
    doc.files.push_back({1, relativePath, fileSize});
    doc.events = std::move(events);
    doc.manifest.eventCount = doc.events.size();
    return doc;
}

/// Serialize and write a trace document to `traceRoot` directory, which must
/// not exist yet.
void writeTrace(const BufferedInputTraceDocument& doc,
                const std::string& traceRoot)
{
    fs::create_directories(traceRoot);
    const auto ser = serializeBufferedInputTrace(doc);
    {
        std::ofstream mf(traceRoot + "/manifest.json");
        mf << ser.manifest;
    }
    {
        std::ofstream ef(traceRoot + "/events.jsonl");
        ef << ser.events;
    }
}

/// Build a minimal single-region single-input trace (create, enqueue, load,
/// consume all bytes, stream_close, input_close).
std::vector<BufferedInputTraceEvent> minimalEvents(
    uint64_t fileSize,
    uint64_t enqueueOffset,
    uint64_t consumeLength)
{
    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;

    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent& {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    auto& ec = push(BufferedInputTraceOp::kInputCreate);
    ec.fileId = 1; ec.inputId = 1;

    auto& ee = push(BufferedInputTraceOp::kStreamEnqueue);
    ee.inputId = 1; ee.streamId = 1;
    ee.offset = enqueueOffset; ee.length = fileSize - enqueueOffset;

    auto& el = push(BufferedInputTraceOp::kLoad);
    el.inputId = 1; el.logType = static_cast<int32_t>(LogType::TEST);

    auto& ecn = push(BufferedInputTraceOp::kConsume);
    ecn.streamId = 1; ecn.offset = 0; ecn.length = consumeLength;

    auto& esc = push(BufferedInputTraceOp::kStreamClose);
    esc.streamId = 1; esc.inputId = 1;

    auto& eic = push(BufferedInputTraceOp::kInputClose);
    eic.inputId = 1;

    return evs;
}

/// Shared helper to build a FileCacheManager with a timekeeper and a fresh
/// cache directory.  Caller owns the returned manager and must call shutdown()
/// when done.
static std::shared_ptr<ch::FileCacheManager> makeTestFileCacheManager(
    const std::string& cachePath,
    memory::MemoryPool* pool)
{
    fs::create_directories(cachePath);
    ch::FileCacheConfig fcfg;
    fcfg.path = cachePath;
    fcfg.maxSize = 128ULL << 20;
    fcfg.backgroundDownloadThreads = 0;
    ch::FileCacheManager::Options opts;
    opts.caches = {{.name = "default", .config = fcfg, .configPath = cachePath}};
    opts.defaultCacheName = "default";
    opts.commonUserId = "test";
    opts.cachePathPrefix = cachePath;
    opts.allowedCacheRoot = cachePath;
    opts.localFileSystem = filesystems::getFileSystem(cachePath, {});
    opts.memoryPool = pool;
    opts.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
    opts.initializeOnCreate = true;
    return ch::FileCacheManager::create(std::move(opts));
}

/// Create the cache sentinel file required by replay(kFileCache).
void createSentinel(const std::string& cacheRoot)
{
    const std::string sentinel =
        cacheRoot + "/.velox_benchmark_cache_sentinel";
    std::ofstream f(sentinel);
    f << "sentinel";
}

/// Seven sentinel-authenticated cache roots (one verification-cold, five
/// timed-cold, one warm) that are strict children of one fresh cache parent.
/// Each root's top-level sentinel is created up front, before any manager: the
/// default (non-split) FileCache loader only scans the data/system/general type
/// subdirectories and skips a top-level sentinel, so pre-created sentinels are
/// supported.  In production the Controller/shell creates them before the
/// watchdog; the manager factory never creates or writes sentinels.
struct SevenRootsFixture
{
    std::shared_ptr<TempDirectoryPath> parent;
    Q04ColdWarmTimingRoots roots;
};

SevenRootsFixture makeSevenRoots()
{
    auto parent = TempDirectoryPath::create();
    const std::string base = fs::canonical(parent->getPath()).string();
    const auto mkRoot = [&](const std::string& name) -> std::string
    {
        const std::string p = base + "/" + name;
        fs::create_directories(p);
        createSentinel(p); // authenticate the root before any manager
        return p;
    };
    Q04ColdWarmTimingRoots roots;
    roots.verificationColdRoot = mkRoot("verif_cold");
    roots.timedColdRoots = {
        mkRoot("cold_f1"), mkRoot("cold_f2"), mkRoot("cold_f3"),
        mkRoot("cold_r1"), mkRoot("cold_r2")};
    roots.warmRoot = mkRoot("warm");
    return {std::move(parent), std::move(roots)};
}

/// A ReplayFileCacheManagerFactory that records each construction (root, order,
/// and initial cache emptiness).  It requires roots that are already
/// sentinel-authenticated and never creates or writes a sentinel.
/// Observability without a test-only fake backend: it counts constructions and
/// how many happened while no manager was exposed.
struct RecordingManagerFactory
{
    memory::MemoryPool* pool;
    std::vector<std::string> roots;
    std::vector<std::pair<size_t, size_t>> initialState;
    int hiddenAtConstruction = 0;

    ReplayFileCacheManagerFactory factory()
    {
        return [this](const std::string& root)
            -> std::shared_ptr<ch::FileCacheManager>
        {
            roots.push_back(root);
            if (ch::FileCacheManager::getInstance() == nullptr)
                ++hiddenAtConstruction;
            auto manager = makeTestFileCacheManager(root, pool);
            initialState.push_back(
                {manager->getDefault()->getUsedCacheSize(),
                 manager->getDefault()->getFileSegmentsNum()});
            return manager;
        };
    }
};

/// A fully-constructed single-file replayer bound to real dataset/trace temp
/// directories.  The temp directories are kept alive alongside the replayer so
/// its source/oracle handles remain valid for the fixture's lifetime.
struct SingleFileReplayFixture
{
    std::shared_ptr<TempDirectoryPath> datasetDir;
    std::shared_ptr<TempDirectoryPath> traceDir;
    BufferedInputTraceReplayer replayer;
};

class BufferedInputTraceReplayTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        memory::MemoryManager::testingSetInstance({});
        filesystems::registerLocalFileSystem();
    }

    /// Write a 4096-byte pattern file in a fresh dataset dir, build the trace
    /// document with `events`, serialize to a fresh trace dir, and return the
    /// replay config.
    BufferedInputTraceReplayConfig makeConfig(
        const std::string& datasetDir,
        const std::string& traceDir,
        std::vector<BufferedInputTraceEvent> events,
        size_t fileSize = 4096)
    {
        const auto canonicalDataset =
            fs::canonical(datasetDir).string();
        const std::string rel = writePatternFile(datasetDir, "data.bin",
                                                 fileSize);
        const auto doc = buildDocument(canonicalDataset, rel, fileSize,
                                       std::move(events));
        writeTrace(doc, traceDir);

        BufferedInputTraceReplayConfig cfg;
        cfg.tracePath = traceDir;
        cfg.datasetRoot = datasetDir;
        cfg.verifyBytes = true;
        return cfg;
    }

    /// Build a real single-file dataset/trace of `fileSize` bytes that consumes
    /// every byte, and return a constructed replayer bound to it.  Uses no
    /// mocked backend; the constructor opens real source/oracle handles.
    SingleFileReplayFixture makeSingleFileReplayFixture(size_t fileSize)
    {
        auto datasetDir = TempDirectoryPath::create();
        auto traceDir = TempDirectoryPath::create();
        auto cfg = makeConfig(
            datasetDir->getPath(),
            traceDir->getPath() + "/trace",
            minimalEvents(fileSize, 0, fileSize),
            fileSize);
        return SingleFileReplayFixture{
            std::move(datasetDir),
            std::move(traceDir),
            BufferedInputTraceReplayer{cfg}};
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Same logical bytes consumed through A, B, and C.
TEST_F(BufferedInputTraceReplayTest, ReplaysSameLogicalBytesAcrossABC)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    BufferedInputTraceReplayer replayer{cfg};

    const auto resA = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(resA.totalLogicalBytes, kSize);
    EXPECT_EQ(resA.passthroughBytes, 0u);

    const auto resB = replayer.replay(BufferedInputReplayBackend::kPassthrough);
    EXPECT_EQ(resB.totalLogicalBytes, kSize);
    EXPECT_GT(resB.passthroughBytes, 0u);

    // Set up a FileCache manager for C and install it as the global instance.
    const std::string cachePath = cacheDir->getPath() + "/fcache";
    auto cachePool = memory::memoryManager()->addLeafPool("replay-test");
    auto manager = makeTestFileCacheManager(cachePath, cachePool.get());
    createSentinel(cachePath);
    ch::FileCacheManager::setInstance(manager.get());

    cfg.cacheRoot = cachePath;
    BufferedInputTraceReplayer replayerC{cfg};

    // Cold run — populates cache.
    const auto resCold =
        replayerC.replay(BufferedInputReplayBackend::kFileCache);
    EXPECT_EQ(resCold.totalLogicalBytes, kSize);

    // Warm run — everything from cache.
    const auto resWarm =
        replayerC.replay(BufferedInputReplayBackend::kFileCache);
    EXPECT_EQ(resWarm.totalLogicalBytes, kSize);
    EXPECT_DOUBLE_EQ(resWarm.warmHitPct, 100.0);
    EXPECT_EQ(resWarm.cacheDelta.sourceReadBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedFromSourceBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.evictedBytes, 0u);

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// Two inputs alive simultaneously with interleaved consumption.
TEST_F(BufferedInputTraceReplayTest, PreservesOverlappingInputLifetimes)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    // Two inputs on the same file; second created before first is closed.
    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent& {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    // input 1: region [0, 2048]
    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = 2048;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }

    // input 2 created while input 1 still alive: region [2048, 2048]
    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 2;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 2;
        e.streamId = 2;
        e.offset = 2048;
        e.length = 2048;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 2;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }

    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 2048;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 2;
        e.offset = 0;
        e.length = 2048;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 2;
        e.inputId = 2;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 2;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = true;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(res.totalLogicalBytes, 4096u);
}

/// Backend chunk sizes may differ from the capture chunk sizes; logical byte
/// consumption must be identical.
TEST_F(BufferedInputTraceReplayTest, BackendChunkSizesMayDiffer)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    BufferedInputTraceReplayer replayer{cfg};

    const auto resA = replayer.replay(BufferedInputReplayBackend::kDirect);
    const auto resB = replayer.replay(BufferedInputReplayBackend::kPassthrough);

    // Both backends must consume exactly the same logical byte count.
    EXPECT_EQ(resA.totalLogicalBytes, resB.totalLogicalBytes);
    EXPECT_EQ(resA.totalLogicalBytes, kSize);
}

/// A consume whose length exceeds the available region bytes causes failure.
TEST_F(BufferedInputTraceReplayTest, RejectsShortReadBeforeExpectedLength)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 256;
    // Ask to consume 512 bytes from a 256-byte region — short read.
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, 512),
                          kSize);

    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(replayer.replay(BufferedInputReplayBackend::kDirect),
                       "");
}

/// A malformed trace JSON is rejected before any backend is created.
TEST_F(BufferedInputTraceReplayTest, RejectsMalformedTrace)
{
    auto traceDir = TempDirectoryPath::create();
    const std::string traceRoot = traceDir->getPath() + "/trace";
    fs::create_directories(traceRoot);
    {
        std::ofstream mf(traceRoot + "/manifest.json");
        mf << "{not valid json";
    }
    {
        std::ofstream ef(traceRoot + "/events.jsonl");
        ef << "";
    }

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = traceDir->getPath();

    // JSON parse error may be any std::exception subtype.
    ASSERT_THROW((BufferedInputTraceReplayer{cfg}), std::exception);
}

/// B backend produces positive passthrough bytes; A produces zero.
TEST_F(BufferedInputTraceReplayTest, PassthroughBytesGate)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    BufferedInputTraceReplayer replayer{cfg};

    const auto resA = replayer.replay(BufferedInputReplayBackend::kDirect);
    const auto resB = replayer.replay(BufferedInputReplayBackend::kPassthrough);

    EXPECT_EQ(resA.passthroughBytes, 0u);
    EXPECT_GT(resB.passthroughBytes, 0u);
    // FileCache stat delta should be zero for both A and B.
    EXPECT_EQ(resA.cacheDelta.cacheReadBytes, 0u);
    EXPECT_EQ(resB.cacheDelta.cacheReadBytes, 0u);
}

/// C warm replay: 100% hit rate, zero source reads, zero predownload/eviction.
TEST_F(BufferedInputTraceReplayTest, WarmCacheIsFullHit)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fcache";
    auto cachePool2 = memory::memoryManager()->addLeafPool("warmcache-test");
    auto manager = makeTestFileCacheManager(cachePath, cachePool2.get());
    createSentinel(cachePath);
    ch::FileCacheManager::setInstance(manager.get());

    cfg.cacheRoot = cachePath;
    BufferedInputTraceReplayer replayer{cfg};

    // Cold: populate cache.
    replayer.replay(BufferedInputReplayBackend::kFileCache);

    // Warm: must be 100% hits.
    const auto resWarm =
        replayer.replay(BufferedInputReplayBackend::kFileCache);
    EXPECT_DOUBLE_EQ(resWarm.warmHitPct, 100.0);
    EXPECT_EQ(resWarm.cacheDelta.sourceReadBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedFromSourceBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.evictedBytes, 0u);

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// A skip event correctly advances the stream position.
TEST_F(BufferedInputTraceReplayTest, SkipEventAdvancesPosition)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent& {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    // Consume first 128 bytes.
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 128;
    }
    // Skip 64 bytes — argument holds the count (matches recorder schema).
    {
        auto& e = push(BufferedInputTraceOp::kSkip);
        e.streamId = 1;
        e.argument = 64;
        e.result = 1;
    }
    // Consume the remaining bytes after skip.
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 192;
        e.length = kSize - 192;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false; // skip offsets make oracle non-trivial

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    // Total logical bytes = consume events only (128 + (4096-192) = 4032).
    EXPECT_EQ(res.totalLogicalBytes, 128u + (kSize - 192u));
}

/// A kInputClone event produces a live clone of the parent input.
TEST_F(BufferedInputTraceReplayTest, InputCloneProducesLiveClone)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent& {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    // Create input 1, then clone it into input 2; read from the clone.
    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClone);
        e.inputId = 2;
        e.parentInputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 2;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 2;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 2;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 2;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = true;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(res.totalLogicalBytes, kSize);
}

/// Oracle comparison fails when the recorded bytes don't match the source file
/// (simulated by providing a trace that records consumption beyond the file).
TEST_F(BufferedInputTraceReplayTest, RejectsShortReadFailPath)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    // File has 128 bytes; trace asks to consume 256 bytes → short read.
    constexpr size_t kFileSize   = 128;
    constexpr size_t kConsumeLen = 256;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kFileSize, 0, kConsumeLen),
                          kFileSize);
    // We must override eventCount for the test doc to pass schema validation.
    // The doc was already written; reload and check the replayer throws.
    VELOX_ASSERT_THROW(
        (BufferedInputTraceReplayer{cfg}.replay(
            BufferedInputReplayBackend::kDirect)),
        "");
}

// ---------------------------------------------------------------------------
// Review-fix RED tests (all should fail with the unpatched implementation)
// ---------------------------------------------------------------------------

/// kSkip uses event.argument for the count (not event.length).
/// Build a trace where event.argument=100, event.length=0 (default).
/// If the replayer wrongly uses event.length=0, SkipInt64(0) won't advance
/// the stream and the next consume's ByteCount check will fail.
TEST_F(BufferedInputTraceReplayTest, SkipUsesCapturedArgument)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 128;
    }
    // argument=100 is the skip count; length=0 (field unused for kSkip).
    {
        auto& e = push(BufferedInputTraceOp::kSkip);
        e.streamId = 1;
        e.argument = 100;
        e.length = 0;
        e.result = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 228;
        e.length = kSize - 228;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(res.totalLogicalBytes, 128u + (kSize - 228u));
}

/// kSeek uses event.result for the target position (not event.offset).
/// Build a trace where event.result=1024, event.offset=0 (default).
/// If the replayer wrongly uses event.offset=0, seek(0) won't advance to 1024
/// and the following consume's ByteCount check will fail.
TEST_F(BufferedInputTraceReplayTest, SeekUsesCapturedResult)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 512;
    }
    // result=1024 is the post-seek ByteCount; offset=0 (field unused for kSeek).
    {
        auto& e = push(BufferedInputTraceOp::kSeek);
        e.streamId = 1;
        e.result = 1024;
        e.offset = 0;
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 1024;
        e.length = kSize - 1024;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(res.totalLogicalBytes, 512u + (kSize - 1024u));
}

/// Manifest file IDs need not be 1-based; use a fileId=5 mapping.
TEST_F(BufferedInputTraceReplayTest, SparseFileIdMapping)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    // Build document with fileId=5 (not 1); fileId-1=4 would be out of bounds.
    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 5;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    BufferedInputTraceDocument doc;
    doc.manifest.queryId = 4;
    doc.manifest.drivers = 1;
    doc.manifest.datasetRoot = canonicalDataset;
    doc.manifest.binaryRealPath = "/proc/self/exe";
    doc.manifest.binaryBuildId = "test";
    doc.manifest.veloxHead = "v";
    doc.manifest.glutenHead = "g";
    doc.manifest.clickhouseHead = "c";
    doc.files.push_back({5, "data.bin", kSize}); // fileId=5
    doc.events = evs;
    doc.manifest.eventCount = evs.size();

    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = true;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(res.totalLogicalBytes, kSize);
}

/// Consume that exceeds the enqueued region bounds must be rejected.
TEST_F(BufferedInputTraceReplayTest, RejectsConsumeExceedingRegion)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    // Enqueue region [0, 256]; consume [0, 512] — 512 > 256.
    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = 256;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 512;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false;

    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kDirect), "");
}

/// validateCacheSentinel throws when the sentinel file is missing.
TEST_F(BufferedInputTraceReplayTest, ValidateCacheSentinelMissingFails)
{
    auto cacheDir = TempDirectoryPath::create();
    // No sentinel file present.
    VELOX_ASSERT_THROW(
        validateCacheSentinel(cacheDir->getPath()), "");
}

/// validateCacheSentinel throws when the sentinel is a symlink
/// (even to a real regular file).
TEST_F(BufferedInputTraceReplayTest, ValidateCacheSentinelSymlinkFails)
{
    auto cacheDir  = TempDirectoryPath::create();
    auto targetDir = TempDirectoryPath::create();

    const std::string targetFile = targetDir->getPath() + "/real_sentinel";
    { std::ofstream f(targetFile); f << "x"; }

    const std::string sentinelPath =
        cacheDir->getPath() + "/.velox_benchmark_cache_sentinel";
    fs::create_symlink(targetFile, sentinelPath);

    VELOX_ASSERT_THROW(
        validateCacheSentinel(cacheDir->getPath()), "");
}

/// replay(kFileCache) rejects a manager whose cache root differs from
/// config.cacheRoot.
TEST_F(BufferedInputTraceReplayTest, WrongManagerRootRejectedOnFileCache)
{
    auto datasetDir  = TempDirectoryPath::create();
    auto traceDir    = TempDirectoryPath::create();
    auto cacheDir1   = TempDirectoryPath::create();
    auto cacheDir2   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    // Manager installed with cacheDir1; config asks for cacheDir2.
    const std::string cachePath1 = cacheDir1->getPath() + "/fc1";
    auto pool1 = memory::memoryManager()->addLeafPool("wrongroot-test");
    auto manager = makeTestFileCacheManager(cachePath1, pool1.get());
    createSentinel(cachePath1); // sentinel present; only root mismatch fires
    ch::FileCacheManager::setInstance(manager.get());

    cfg.cacheRoot = cacheDir2->getPath() + "/fc2"; // different from cachePath1
    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kFileCache), "");

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// Consuming only part of a region; warm cache must still report 100% hits
/// even though FileCache may overread (chunk/segment boundaries).
TEST_F(BufferedInputTraceReplayTest, PartialConsumptionWarmCache100Pct)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    // Large file so the partial consume cannot happen to read whole segments.
    constexpr size_t kSize   = 2 * 1024 * 1024 + 17; // 2 MiB + 17
    constexpr size_t kConsume = 512;                   // consume only 512 bytes
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kConsume),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fcache";
    auto pool = memory::memoryManager()->addLeafPool("partial-warm-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    ch::FileCacheManager::setInstance(manager.get());

    cfg.cacheRoot = cachePath;
    BufferedInputTraceReplayer replayer{cfg};

    // Cold run.
    replayer.replay(BufferedInputReplayBackend::kFileCache);

    // Warm run: hitPct must be exactly 100.0 regardless of cacheReadBytes.
    const auto resWarm =
        replayer.replay(BufferedInputReplayBackend::kFileCache);
    EXPECT_DOUBLE_EQ(resWarm.warmHitPct, 100.0);
    EXPECT_EQ(resWarm.cacheDelta.cacheMissCount, 0u);
    EXPECT_GT(resWarm.cacheDelta.cacheHitCount, 0u);
    EXPECT_EQ(resWarm.cacheDelta.sourceReadBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedFromSourceBytes, 0u);
    EXPECT_EQ(resWarm.cacheDelta.predownloadedBytes, 0u);

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// nextCount and seekCount are tracked in the result.
TEST_F(BufferedInputTraceReplayTest, NextCountAndSeekCountTracked){
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = kSize;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 512;
    }
    // seek to 1024
    {
        auto& e = push(BufferedInputTraceOp::kSeek);
        e.streamId = 1;
        e.result = 1024;
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 1024;
        e.length = 512;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    auto doc = buildDocument(canonicalDataset, "data.bin", kSize, evs);
    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false;

    BufferedInputTraceReplayer replayer{cfg};
    const auto res = replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_GT(res.nextCount, 0u);
    EXPECT_EQ(res.seekCount, 1u);
}

/// Enqueuing a region that exceeds the manifest file size is rejected.
TEST_F(BufferedInputTraceReplayTest, RejectsRegionExceedingFileSize)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();

    // File is 256 bytes; trace says region [0, 512] — 512 > 256.
    constexpr size_t kFileSize = 256;
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    writePatternFile(datasetDir->getPath(), "data.bin", kFileSize);

    std::vector<BufferedInputTraceEvent> evs;
    uint64_t seq = 1;
    auto push = [&](BufferedInputTraceOp op) -> BufferedInputTraceEvent&
    {
        evs.push_back({});
        auto& e = evs.back();
        e.seq = seq++;
        e.captureTid = 1;
        e.threadIndex = 0;
        e.op = op;
        return e;
    };

    {
        auto& e = push(BufferedInputTraceOp::kInputCreate);
        e.fileId = 1;
        e.inputId = 1;
    }
    // Region length 512 > file size 256: absolute bound violated.
    {
        auto& e = push(BufferedInputTraceOp::kStreamEnqueue);
        e.inputId = 1;
        e.streamId = 1;
        e.offset = 0;
        e.length = 512;
    }
    {
        auto& e = push(BufferedInputTraceOp::kLoad);
        e.inputId = 1;
        e.logType = static_cast<int32_t>(LogType::TEST);
    }
    {
        auto& e = push(BufferedInputTraceOp::kConsume);
        e.streamId = 1;
        e.offset = 0;
        e.length = 256;
    }
    {
        auto& e = push(BufferedInputTraceOp::kStreamClose);
        e.streamId = 1;
        e.inputId = 1;
    }
    {
        auto& e = push(BufferedInputTraceOp::kInputClose);
        e.inputId = 1;
    }

    // Build the document with fileSize=256 but region length=512 in event.
    // validateBufferedInputTrace does not cross-check event offsets vs file sizes,
    // so we build the doc manually with the mismatching event and a valid manifest.
    BufferedInputTraceDocument doc;
    doc.manifest.queryId = 4;
    doc.manifest.drivers = 1;
    doc.manifest.datasetRoot = canonicalDataset;
    doc.manifest.binaryRealPath = "/proc/self/exe";
    doc.manifest.binaryBuildId = "test";
    doc.manifest.veloxHead = "v";
    doc.manifest.glutenHead = "g";
    doc.manifest.clickhouseHead = "c";
    doc.files.push_back({1, "data.bin", kFileSize}); // fileSize=256
    doc.events = evs;
    doc.manifest.eventCount = evs.size();

    const std::string traceRoot = traceDir->getPath() + "/trace";
    writeTrace(doc, traceRoot);

    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.verifyBytes = false;

    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kDirect), "");
}

/// replay(kDirect) rejects an installed FileCacheManager (contamination guard).
TEST_F(BufferedInputTraceReplayTest, DirectRejectsInstalledManager)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("direct-mgr-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    ch::FileCacheManager::setInstance(manager.get());

    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kDirect), "");

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// replay(kPassthrough) rejects an installed FileCacheManager.
TEST_F(BufferedInputTraceReplayTest, PassthroughRejectsInstalledManager)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("pass-mgr-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    ch::FileCacheManager::setInstance(manager.get());

    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kPassthrough), "");

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// replay(kFileCache) rejects a missing cache sentinel (library-level check).
TEST_F(BufferedInputTraceReplayTest, FileCacheRejectsMissingSentinelAtLibrary)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("missing-sentinel-lib");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    ch::FileCacheManager::setInstance(manager.get());

    cfg.cacheRoot = cachePath;
    // Intentionally NO sentinel — library replay should reject.
    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kFileCache), "");

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

/// replay(kFileCache) rejects a symlink sentinel (library-level check).
TEST_F(BufferedInputTraceReplayTest, FileCacheRejectsSymlinkSentinelAtLibrary)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir   = TempDirectoryPath::create();
    auto cacheDir   = TempDirectoryPath::create();
    auto targetDir  = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize),
                          kSize);

    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("symlink-sentinel-lib");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    ch::FileCacheManager::setInstance(manager.get());

    // Create a symlink sentinel pointing to a real file.
    const std::string target = targetDir->getPath() + "/real";
    { std::ofstream f(target); f << "x"; }
    fs::create_symlink(target, cachePath + "/.velox_benchmark_cache_sentinel");

    cfg.cacheRoot = cachePath;
    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(BufferedInputReplayBackend::kFileCache), "");

    manager->shutdown();
    ch::FileCacheManager::setInstance(nullptr);
}

// ---------------------------------------------------------------------------
// Task-018S Task-1: measured event-loop replay mode (RED before implementation)
// ---------------------------------------------------------------------------

/// The timing helper must run setup() before the clock starts, so only the
/// event-loop delta is measured.  With a fake clock where setup advances by 100
/// and the event loop advances by 20, the measured wall time must be exactly 20.
TEST(BufferedInputTraceReplayTimingTest, ExcludesSetupFromMeasuredScope)
{
    uint64_t now = 0;
    const auto wallNs = measureReplayEventLoop(
        [&] { now += 100; },
        [&] { now += 20; },
        [&] { return now; });
    EXPECT_EQ(wallNs, 20u);
}

/// A measured replay with byte verification disabled reports a positive wall
/// time, the full logical byte count, positive Next() calls, and — because the
/// oracle is disabled — zero oracle read bytes.
TEST_F(BufferedInputTraceReplayTest, MeasuredReplayCanDisableOracle)
{
    auto fixture = makeSingleFileReplayFixture(2 * 1024 * 1024 + 17);
    BufferedInputTraceReplayOptions options{
        .verifyBytes = false,
        .measureEventLoop = true};
    const auto result =
        fixture.replayer.replay(BufferedInputReplayBackend::kDirect, options);
    EXPECT_GT(result.wallNs, 0u);
    EXPECT_EQ(result.totalLogicalBytes, 2u * 1024 * 1024 + 17);
    EXPECT_GT(result.nextCount, 0u);
    EXPECT_EQ(result.oracleReadBytes, 0u);
}

/// A measured replay with byte verification enabled still measures a positive
/// wall time and proves every logical byte was checked against the oracle.
TEST_F(BufferedInputTraceReplayTest, MeasuredReplayWithOracleReadsEveryByte)
{
    auto fixture = makeSingleFileReplayFixture(2 * 1024 * 1024 + 17);
    BufferedInputTraceReplayOptions options{
        .verifyBytes = true,
        .measureEventLoop = true};
    const auto result =
        fixture.replayer.replay(BufferedInputReplayBackend::kDirect, options);
    EXPECT_GT(result.wallNs, 0u);
    EXPECT_EQ(result.totalLogicalBytes, 2u * 1024 * 1024 + 17);
    EXPECT_EQ(result.oracleReadBytes, result.totalLogicalBytes);
}

/// The one-argument replay overload preserves the historical behavior: it runs
/// unmeasured (wallNs == 0) and honors the config's verifyBytes default, so the
/// oracle reads every logical byte.
TEST_F(BufferedInputTraceReplayTest, OneArgReplayPreservesUnmeasuredOracle)
{
    auto fixture = makeSingleFileReplayFixture(4096);
    const auto result =
        fixture.replayer.replay(BufferedInputReplayBackend::kDirect);
    EXPECT_EQ(result.wallNs, 0u);
    EXPECT_EQ(result.totalLogicalBytes, 4096u);
    EXPECT_EQ(result.oracleReadBytes, result.totalLogicalBytes);
}

// ---------------------------------------------------------------------------
// Task-018S Task-2: fixed 3+2 order and summary analysis (RED first)
// ---------------------------------------------------------------------------

/// Build a timing sample carrying only the fields the analysis reads (slot and
/// wallNs).
/// Build the exact 15-slot q04 order with a caller-supplied wall time per slot.
std::vector<ReplayTimingSample> timingSamplesFrom(
    const std::function<uint64_t(const ReplayTimingSlot&)>& wallNsOf)
{
    std::vector<ReplayTimingSample> samples;
    for (const auto& slot : q04ReplayTimingOrder())
    {
        ReplayTimingSample s;
        s.slot = slot;
        s.result.wallNs = wallNsOf(slot);
        samples.push_back(s);
    }
    return samples;
}

/// Build a per-timing-cell valid timed result honoring the A/cold-B/warm-C path
/// gates.
BufferedInputTraceReplayResult validTimedResult(
    ReplayTimingCell cell,
    uint64_t logicalBytes)
{
    BufferedInputTraceReplayResult r;
    r.totalLogicalBytes = logicalBytes;
    r.wallNs = 1000;
    r.nextCount = 10;
    r.oracleReadBytes = 0;
    r.passthroughBytes = 0;
    switch (cell)
    {
    case ReplayTimingCell::kDirect:
        // All cacheDelta counters remain zero by default.
        break;
    case ReplayTimingCell::kColdFileCache:
        // Cold population: misses, source reads, and cache writes are positive.
        r.cacheDelta.cacheMissCount = 5;
        r.cacheDelta.sourceReadBytes = logicalBytes;
        r.cacheDelta.cacheWriteBytes = logicalBytes;
        break;
    case ReplayTimingCell::kWarmFileCache:
        r.cacheDelta.cacheHitCount = 5;
        r.cacheDelta.cacheReadBytes = logicalBytes;
        r.warmHitPct = 100.0;
        break;
    }
    return r;
}

/// Block and pooled medians, min/max, and pooled-median decomposition.
TEST(BufferedInputTraceReplayTimingTest, ComputesBlockAndPooledMedians)
{
    using B = ReplayTimingBlock;
    using C = ReplayTimingCell;

    // Chosen so A<B<C in both blocks (no sign flip).
    // Cell A: forward 100,110,120; reverse 130,150.
    // Cell B: forward 200,210,220; reverse 230,250.
    // Cell C: forward 300,310,320; reverse 330,350.
    const auto wallNsOf = [](const ReplayTimingSlot& s) -> uint64_t
    {
        const uint64_t cellBase = s.cell == C::kDirect
            ? 100
            : (s.cell == C::kColdFileCache ? 200 : 300);
        if (s.block == B::kForward)
            return cellBase + (s.sample - 1) * 10; // 0,10,20
        return cellBase + 30 + (s.sample - 1) * 20; // 30,50
    };
    const auto samples = timingSamplesFrom(wallNsOf);

    const auto analysis = analyzeReplayTiming(samples);

    // Cell A (index 0).
    EXPECT_EQ(analysis.cells[0].forwardMedianNs, 110u);
    EXPECT_EQ(analysis.cells[0].reverseMedianNs, 140u); // (130+150)/2
    EXPECT_EQ(analysis.cells[0].pooledMedianNs, 120u);
    EXPECT_EQ(analysis.cells[0].minNs, 100u);
    EXPECT_EQ(analysis.cells[0].maxNs, 150u);
    EXPECT_EQ(analysis.cells[0].samples, 5u);

    // Cell B (index 1).
    EXPECT_EQ(analysis.cells[1].forwardMedianNs, 210u);
    EXPECT_EQ(analysis.cells[1].reverseMedianNs, 240u);
    EXPECT_EQ(analysis.cells[1].pooledMedianNs, 220u);
    EXPECT_EQ(analysis.cells[1].minNs, 200u);
    EXPECT_EQ(analysis.cells[1].maxNs, 250u);

    // Cell C (index 2).
    EXPECT_EQ(analysis.cells[2].forwardMedianNs, 310u);
    EXPECT_EQ(analysis.cells[2].reverseMedianNs, 340u);
    EXPECT_EQ(analysis.cells[2].pooledMedianNs, 320u);
    EXPECT_EQ(analysis.cells[2].minNs, 300u);
    EXPECT_EQ(analysis.cells[2].maxNs, 350u);

    // Pooled-median decomposition.  Ratios are overhead ratios: delta/baseline.
    EXPECT_EQ(analysis.bMinusANs, 100);
    EXPECT_DOUBLE_EQ(analysis.bMinusARatio, 100.0 / 120.0); // (B-A)/A
    EXPECT_EQ(analysis.cMinusBNs, 100);
    EXPECT_DOUBLE_EQ(analysis.cMinusBRatio, 100.0 / 220.0); // (C-B)/B
    EXPECT_EQ(analysis.cMinusANs, 200);
    EXPECT_DOUBLE_EQ(analysis.cMinusARatio, 200.0 / 120.0); // (C-A)/A
}

/// Overhead ratios are delta/baseline, not backend/backend: with pooled medians
/// A=100, B=110, C=121 the ratios are (B-A)/A=0.10, (C-B)/B=0.10, (C-A)/A=0.21.
TEST(BufferedInputTraceReplayTimingTest, ComputesOverheadRatios)
{
    using C = ReplayTimingCell;
    // Five equal samples per cell so each pooled median is exact.
    const auto wallNsOf = [](const ReplayTimingSlot& s) -> uint64_t
    {
        if (s.cell == C::kDirect)
            return 100;
        if (s.cell == C::kColdFileCache)
            return 110;
        return 121;
    };
    const auto analysis = analyzeReplayTiming(timingSamplesFrom(wallNsOf));

    EXPECT_EQ(analysis.cells[0].pooledMedianNs, 100u);
    EXPECT_EQ(analysis.cells[1].pooledMedianNs, 110u);
    EXPECT_EQ(analysis.cells[2].pooledMedianNs, 121u);

    EXPECT_EQ(analysis.bMinusANs, 10);
    EXPECT_DOUBLE_EQ(analysis.bMinusARatio, 10.0 / 100.0); // (B-A)/A = 0.10
    EXPECT_EQ(analysis.cMinusBNs, 11);
    EXPECT_DOUBLE_EQ(analysis.cMinusBRatio, 11.0 / 110.0); // (C-B)/B = 0.10
    EXPECT_EQ(analysis.cMinusANs, 21);
    EXPECT_DOUBLE_EQ(analysis.cMinusARatio, 21.0 / 100.0); // (C-A)/A = 0.21
}

/// Missing, duplicate, unexpected, or wrong-count slot sets are rejected before
/// any median is computed.
TEST(BufferedInputTraceReplayTimingTest, RejectsMissingOrDuplicateSlot)
{
    const auto wallNsOf = [](const ReplayTimingSlot&) -> uint64_t
    { return 100; };

    // Duplicate: overwrite one slot with a copy of another (one slot missing).
    {
        auto samples = timingSamplesFrom(wallNsOf);
        samples[1].slot = samples[0].slot;
        VELOX_ASSERT_THROW(analyzeReplayTiming(samples), "");
    }

    // Missing: drop one slot (14 samples).
    {
        auto samples = timingSamplesFrom(wallNsOf);
        samples.pop_back();
        VELOX_ASSERT_THROW(analyzeReplayTiming(samples), "");
    }

    // Unexpected: a slot with an out-of-range sample index.
    {
        auto samples = timingSamplesFrom(wallNsOf);
        samples[0].slot.sample = 4;
        VELOX_ASSERT_THROW(analyzeReplayTiming(samples), "");
    }
}

/// A B-A block-difference sign flip between forward and reverse is invalid.
TEST(BufferedInputTraceReplayTimingTest, RejectsBMinusASignFlip)
{
    using B = ReplayTimingBlock;
    using C = ReplayTimingCell;
    // Forward: A=100, B=200 (B-A=+100). Reverse: A=300, B=200 (B-A=-100).
    // C is consistent so only B-A flips.
    const auto wallNsOf = [](const ReplayTimingSlot& s) -> uint64_t
    {
        if (s.cell == C::kDirect)
            return s.block == B::kForward ? 100 : 300;
        if (s.cell == C::kColdFileCache)
            return 200;
        return 400; // C consistent in both blocks
    };
    VELOX_ASSERT_THROW(
        analyzeReplayTiming(timingSamplesFrom(wallNsOf)), "");
}

/// A C-B block-difference sign flip between forward and reverse is invalid.
TEST(BufferedInputTraceReplayTimingTest, RejectsCMinusBSignFlip)
{
    using B = ReplayTimingBlock;
    using C = ReplayTimingCell;
    // B-A consistent (+100 both blocks). C-B: forward +100, reverse -50.
    const auto wallNsOf = [](const ReplayTimingSlot& s) -> uint64_t
    {
        if (s.cell == C::kDirect)
            return 100;
        if (s.cell == C::kColdFileCache)
            return 200;
        return s.block == B::kForward ? 300 : 150; // C-B flips
    };
    VELOX_ASSERT_THROW(
        analyzeReplayTiming(timingSamplesFrom(wallNsOf)), "");
}

/// validateTimedReplaySample accepts canonical A (Direct), cold-B, and warm-C.
TEST(BufferedInputTraceReplayTimingTest, ValidateAcceptsCanonicalABC)
{
    constexpr uint64_t kBytes = 5625132188ULL;
    using C = ReplayTimingCell;
    for (auto cell : {C::kDirect, C::kColdFileCache, C::kWarmFileCache})
    {
        ReplayTimingSample s;
        s.slot = {ReplayTimingBlock::kForward, 1, cell};
        s.result = validTimedResult(cell, kBytes);
        validateTimedReplaySample(s, kBytes); // must not throw
    }
}

/// A nonzero oracleReadBytes means the pass was verified, not timed → invalid.
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsOracleReadBytes)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kDirect};
    s.result = validTimedResult(ReplayTimingCell::kDirect, kBytes);
    s.result.oracleReadBytes = kBytes;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// A zero wall time is invalid for any timed sample.
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsZeroWallNs)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
    s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
    s.result.wallNs = 0;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// A (Direct) must have every FileCache cumulative delta zero — not only
/// cacheReadBytes.  A nonzero cacheWriteMicroseconds must be rejected.
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsDirectFileCacheActivity)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kDirect};
    s.result = validTimedResult(ReplayTimingCell::kDirect, kBytes);
    s.result.cacheDelta.cacheWriteMicroseconds = 1;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// Warm C must record zero misses.
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsFileCacheMiss)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kWarmFileCache};
    s.result = validTimedResult(ReplayTimingCell::kWarmFileCache, kBytes);
    s.result.cacheDelta.cacheMissCount = 1;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// No timing cell may run the passthrough path: any positive passthroughBytes
/// is invalid (checked here on a warm-C sample).
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsFileCachePassthroughActivity)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kWarmFileCache};
    s.result = validTimedResult(ReplayTimingCell::kWarmFileCache, kBytes);
    s.result.passthroughBytes = 1;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// A logical-byte mismatch against the accepted count is invalid.
TEST(BufferedInputTraceReplayTimingTest, ValidateRejectsLogicalByteMismatch)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kDirect};
    s.result = validTimedResult(ReplayTimingCell::kDirect, kBytes);
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes + 1), "");
}

// ---------------------------------------------------------------------------
// Task-018S Task-3: q04 timing pilot orchestration + JSON (RED first)
// ---------------------------------------------------------------------------

/// Exposing, hiding, and re-exposing the same manager (without shutdown) keeps
/// the cache warm: a re-exposed C run is 100% hits with zero source reads.
TEST_F(BufferedInputTraceReplayTest, ManagerExposureRetainsWarmCache)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/fcache";
    auto pool = memory::memoryManager()->addLeafPool("exposure-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    cfg.cacheRoot = cachePath;

    BufferedInputTraceReplayer replayer{cfg};

    // Expose + C cold (populates cache).
    {
        ScopedFileCacheManagerExposure exposure(*manager);
        replayer.replay(BufferedInputReplayBackend::kFileCache);
    }
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);

    // Hidden + A.
    replayer.replay(BufferedInputReplayBackend::kDirect);

    // Re-expose the same warm manager + C warm.
    BufferedInputTraceReplayResult warm;
    {
        ScopedFileCacheManagerExposure exposure(*manager);
        warm = replayer.replay(BufferedInputReplayBackend::kFileCache);
    }
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
    EXPECT_DOUBLE_EQ(warm.warmHitPct, 100.0);
    EXPECT_EQ(warm.cacheDelta.sourceReadBytes, 0u);

    manager->shutdown();
}

/// A pilot on a trace whose verified A pass fails must throw before appending
/// any timed sample or constructing any manager: the builder stays empty.
TEST_F(BufferedInputTraceReplayTest, TimingPilotVerifiesBeforeMeasuring)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();

    // File is 128 bytes but the trace consumes 256 → verified A fails.
    constexpr size_t kFileSize = 128;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kFileSize, 0, 256), kFileSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("pilot-verify-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    VELOX_ASSERT_THROW(
        runQ04ColdWarmTimingPilot(replayer, sr.roots, rec.factory(), result),
        "");

    // No timed sample was produced, no manager constructed, manager hidden.
    EXPECT_TRUE(result.samples.empty());
    EXPECT_TRUE(rec.roots.empty());
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

// runQ04SingleThreadTimingPilot's successful end-to-end cold pilot (formerly
// TimedSamplesDisableByteOracle) requires the cold-root lifecycle implemented in
// a later task: a single warm manager cannot produce valid cold-B samples.  Its
// invariants are preserved here by MeasuredReplayCanDisableOracle /
// MeasuredReplayWithOracleReadsEveryByte and the per-call cold/warm cell tests
// (PerCallColdFileCacheRootSatisfiesColdGate /
// PerCallWarmFileCacheRootSatisfiesWarmGate), which assert oracleReadBytes==0 on
// timed passes and oracleReadBytes==logicalBytes on verified passes.

/// Build a valid, fully-populated pilot result with monotone per-cell wall
/// times (A<B<C) so analysis and serialization succeed.
Q04ReplayTimingPilotResult buildValidPilotResult(uint64_t logicalBytes)
{
    using C = ReplayTimingCell;
    Q04ReplayTimingPilotResult result;
    result.verifiedA = validTimedResult(C::kDirect, logicalBytes);
    result.verifiedA.oracleReadBytes = logicalBytes;
    result.verifiedB = validTimedResult(C::kColdFileCache, logicalBytes);
    result.verifiedB.oracleReadBytes = logicalBytes;
    result.coldC = validTimedResult(C::kColdFileCache, logicalBytes);
    result.coldC.oracleReadBytes = logicalBytes;
    result.verifiedWarmC = validTimedResult(C::kWarmFileCache, logicalBytes);
    result.verifiedWarmC.oracleReadBytes = logicalBytes;

    const auto cellWall = [](C cell) -> uint64_t
    {
        return cell == C::kDirect ? 1000
            : (cell == C::kColdFileCache ? 1100 : 1200);
    };
    for (const auto& slot : q04ReplayTimingOrder())
    {
        ReplayTimingSample s;
        s.slot = slot;
        s.result = validTimedResult(slot.cell, logicalBytes);
        s.result.wallNs = cellWall(slot.cell);
        result.samples.push_back(s);
    }
    result.analysis = analyzeReplayTiming(result.samples);
    return result;
}

/// q04TimingPilotToJson yields exactly the required top-level keys, 15 sample
/// objects, and no root_cause/classification.
TEST_F(BufferedInputTraceReplayTest, TimingJsonContainsRawAndSummaryRows)
{
    constexpr uint64_t kBytes = 4096;
    const auto result = buildValidPilotResult(kBytes);

    auto datasetDir = TempDirectoryPath::create();
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    const auto doc = buildDocument(
        canonicalDataset, "data.bin", kBytes, minimalEvents(kBytes, 0, kBytes));
    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = "/trace/path";
    cfg.datasetRoot = canonicalDataset;
    const Q04ReplayTimingIdentity replayIdentity{
        "/replay/bin", "replay-build-id"};

    const auto json = q04TimingPilotToJson(doc, cfg, result, replayIdentity);

    std::set<std::string> keys;
    for (const auto& kv : json.items())
        keys.insert(kv.first.asString());
    const std::set<std::string> expected{
        "schema_version", "mode", "valid", "identity", "samples",
        "cell_summary", "decomposition", "decomposition_status",
        "next_plan_selection"};
    EXPECT_EQ(keys, expected);

    EXPECT_EQ(json["mode"].asString(), "single_thread_timing_pilot");
    EXPECT_TRUE(json["valid"].asBool());
    EXPECT_EQ(json["decomposition_status"].asString(), "measured");
    EXPECT_EQ(json["next_plan_selection"].asString(), "user_review_required");
    ASSERT_TRUE(json["samples"].isArray());
    EXPECT_EQ(json["samples"].size(), 15u);
    ASSERT_TRUE(json["cell_summary"].isArray());
    EXPECT_EQ(json["cell_summary"].size(), 3u);
    EXPECT_EQ(json.count("root_cause"), 0u);
    EXPECT_EQ(json["decomposition"].count("root_cause"), 0u);
}

/// Identity must distinguish the capture (TPCH) binary from the replay
/// executable that produced the measurements.
TEST_F(BufferedInputTraceReplayTest, TimingJsonSeparatesCaptureAndReplayIdentities)
{
    constexpr uint64_t kBytes = 4096;
    const auto result = buildValidPilotResult(kBytes);

    auto datasetDir = TempDirectoryPath::create();
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    const auto doc = buildDocument(
        canonicalDataset, "data.bin", kBytes, minimalEvents(kBytes, 0, kBytes));
    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = "/trace/path";
    cfg.datasetRoot = canonicalDataset;
    const Q04ReplayTimingIdentity replayIdentity{
        "/opt/replay/velox_buffered_input_trace_replay", "replay-build-abc"};

    const auto json = q04TimingPilotToJson(doc, cfg, result, replayIdentity);
    const auto& identity = json["identity"];

    // Capture identity from the trace manifest.
    EXPECT_EQ(
        identity["capture_binary_real_path"].asString(),
        doc.manifest.binaryRealPath);
    EXPECT_EQ(
        identity["capture_binary_build_id"].asString(),
        doc.manifest.binaryBuildId);
    // Replay identity from the passed struct (the measured executable).
    EXPECT_EQ(
        identity["replay_binary_real_path"].asString(),
        "/opt/replay/velox_buffered_input_trace_replay");
    EXPECT_EQ(
        identity["replay_binary_build_id"].asString(), "replay-build-abc");

    // The ambiguous single-binary keys must not exist.
    EXPECT_EQ(identity.count("binary_real_path"), 0u);
    EXPECT_EQ(identity.count("binary_build_id"), 0u);
    // Capture and replay identities are distinct.
    EXPECT_NE(
        identity["capture_binary_build_id"].asString(),
        identity["replay_binary_build_id"].asString());
    // Trace/repository identities remain present.
    EXPECT_EQ(identity["velox_head"].asString(), doc.manifest.veloxHead);
    EXPECT_EQ(identity["gluten_head"].asString(), doc.manifest.glutenHead);
    EXPECT_EQ(
        identity["clickhouse_head"].asString(), doc.manifest.clickhouseHead);
}

/// A uint64 field beyond int64 range must be rejected before folly::dynamic.
TEST_F(BufferedInputTraceReplayTest, TimingJsonRejectsOversizedInteger)
{
    constexpr uint64_t kBytes = 4096;
    auto result = buildValidPilotResult(kBytes);
    // Corrupt one sample's wall time beyond int64 range.
    result.samples[0].result.wallNs = std::numeric_limits<uint64_t>::max();

    auto datasetDir = TempDirectoryPath::create();
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    const auto doc = buildDocument(
        canonicalDataset, "data.bin", kBytes, minimalEvents(kBytes, 0, kBytes));
    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = "/trace/path";
    cfg.datasetRoot = canonicalDataset;
    const Q04ReplayTimingIdentity replayIdentity{"/replay/bin", "replay-build"};

    VELOX_ASSERT_THROW(
        q04TimingPilotToJson(doc, cfg, result, replayIdentity), "");
}

/// The pilot result builder must be fresh: a pre-existing (stale) sample is
/// rejected at entry, before any replay or manager construction.
TEST_F(BufferedInputTraceReplayTest, TimingPilotRejectsStaleResultBuilder)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("pilot-stale-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    ReplayTimingSample stale;
    stale.slot = {
        ReplayTimingBlock::kForward, 1, ReplayTimingCell::kDirect};
    result.samples.push_back(stale);

    VELOX_ASSERT_THROW(
        runQ04ColdWarmTimingPilot(replayer, sr.roots, rec.factory(), result),
        "");
    // The entry guard rejects before any replay or manager construction.
    EXPECT_EQ(result.samples.size(), 1u);
    EXPECT_TRUE(rec.roots.empty());
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

// ---------------------------------------------------------------------------
// Task-018S cold/warm: Direct/cold-FileCache/warm-FileCache timing cells
// (RED before implementation)
// ---------------------------------------------------------------------------

/// The timing order is exactly A, cold-B, warm-C forward then warm-C, cold-B, A
/// reverse, unchanged by the cold/warm relabeling.
TEST(ColdFileCacheTimingTest, UsesDirectColdWarmThreePlusTwoOrder)
{
    using B = ReplayTimingBlock;
    using Cell = ReplayTimingCell;
    const std::vector<ReplayTimingSlot> expected{
        {B::kForward, 1, Cell::kDirect},
        {B::kForward, 1, Cell::kColdFileCache},
        {B::kForward, 1, Cell::kWarmFileCache},
        {B::kForward, 2, Cell::kDirect},
        {B::kForward, 2, Cell::kColdFileCache},
        {B::kForward, 2, Cell::kWarmFileCache},
        {B::kForward, 3, Cell::kDirect},
        {B::kForward, 3, Cell::kColdFileCache},
        {B::kForward, 3, Cell::kWarmFileCache},
        {B::kReverse, 1, Cell::kWarmFileCache},
        {B::kReverse, 1, Cell::kColdFileCache},
        {B::kReverse, 1, Cell::kDirect},
        {B::kReverse, 2, Cell::kWarmFileCache},
        {B::kReverse, 2, Cell::kColdFileCache},
        {B::kReverse, 2, Cell::kDirect},
    };
    const auto order = q04ReplayTimingOrder();
    ASSERT_EQ(order.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        EXPECT_EQ(order[i].block, expected[i].block) << "slot " << i;
        EXPECT_EQ(order[i].sample, expected[i].sample) << "slot " << i;
        EXPECT_EQ(order[i].cell, expected[i].cell) << "slot " << i;
    }
}

/// Cold B accepts positive miss/source/write; within-pass hits are allowed.
TEST(ColdFileCacheTimingTest, ColdAcceptsMissSourceWriteAndOptionalHits)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
    s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
    // A cold pass may revisit and hit a segment it just populated.
    s.result.cacheDelta.cacheHitCount = 3;
    validateTimedReplaySample(s, kBytes); // must not throw
}

/// Cold B accepts zero or positive matched foreground predownload counters:
/// FileCacheInputStream::predownloadForCurrentSegment fills source gaps
/// synchronously (foreground), recording predownloadedFromSourceBytes ==
/// predownloadedBytes.
TEST(ColdFileCacheTimingTest, ColdAcceptsMatchedForegroundPredownload)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample sample;
    sample.slot = {
        ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
    sample.result =
        validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
    sample.result.cacheDelta.predownloadedFromSourceBytes = 1024;
    sample.result.cacheDelta.predownloadedBytes = 1024;

    EXPECT_NO_THROW(validateTimedReplaySample(sample, kBytes));
}

/// Cold B rejects zero misses, zero source reads, or zero cache writes.
TEST(ColdFileCacheTimingTest, ColdRejectsZeroMissSourceOrWrite)
{
    constexpr uint64_t kBytes = 4096;
    {
        ReplayTimingSample s;
        s.slot = {
            ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
        s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
        s.result.cacheDelta.cacheMissCount = 0;
        VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
    }
    {
        ReplayTimingSample s;
        s.slot = {
            ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
        s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
        s.result.cacheDelta.sourceReadBytes = 0;
        VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
    }
    {
        ReplayTimingSample s;
        s.slot = {
            ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
        s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
        s.result.cacheDelta.cacheWriteBytes = 0;
        VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
    }
}

/// Cold B rejects mismatched foreground predownload accounting or any eviction.
TEST(ColdFileCacheTimingTest, ColdRejectsMismatchedPredownloadOrEviction)
{
    constexpr uint64_t kBytes = 4096;
    const auto reject = [&](auto mutate)
    {
        ReplayTimingSample s;
        s.slot = {
            ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
        s.result = validTimedResult(ReplayTimingCell::kColdFileCache, kBytes);
        mutate(s.result.cacheDelta);
        VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
    };
    reject([](auto& d)
    {
        d.predownloadedFromSourceBytes = 2;
        d.predownloadedBytes = 1;
    });
    reject([](auto& d)
    {
        d.predownloadedFromSourceBytes = 1;
        d.predownloadedBytes = 2;
    });
    reject([](auto& d) { d.evictedBytes = 1; });
    reject([](auto& d) { d.evictedSegments = 1; });
}

/// Warm C rejects any cache write (in addition to the existing warm gates).
TEST(ColdFileCacheTimingTest, WarmRejectsCacheWrite)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kWarmFileCache};
    s.result = validTimedResult(ReplayTimingCell::kWarmFileCache, kBytes);
    s.result.cacheDelta.cacheWriteBytes = 1;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

/// A (Direct) rejects every FileCache cumulative delta.
TEST(ColdFileCacheTimingTest, DirectRejectsAnyFileCacheDelta)
{
    constexpr uint64_t kBytes = 4096;
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kDirect};
    s.result = validTimedResult(ReplayTimingCell::kDirect, kBytes);
    s.result.cacheDelta.cacheWriteMicroseconds = 1;
    VELOX_ASSERT_THROW(validateTimedReplaySample(s, kBytes), "");
}

// --- Per-call FileCache root override tests ---

/// A timed cold FileCache replay against a fresh empty root satisfies the cold
/// cell gates and disables the oracle.
TEST_F(BufferedInputTraceReplayTest, PerCallColdFileCacheRootSatisfiesColdGate)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();

    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/cold";
    auto pool = memory::memoryManager()->addLeafPool("percall-cold-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    // config.cacheRoot intentionally left empty; use the per-call override.
    cfg.cacheRoot = "";

    BufferedInputTraceReplayer replayer{cfg};
    BufferedInputTraceReplayResult cold;
    {
        ScopedFileCacheManagerExposure exposure(*manager);
        cold = replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            {.verifyBytes = false,
             .measureEventLoop = true,
             .fileCacheRoot = cachePath});
    }
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kColdFileCache};
    s.result = cold;
    validateTimedReplaySample(s, kSize); // cold gates must hold
    EXPECT_EQ(cold.oracleReadBytes, 0u);

    manager->shutdown();
}

/// A timed warm FileCache replay against a warmed per-call root satisfies the
/// warm cell gates.
TEST_F(BufferedInputTraceReplayTest, PerCallWarmFileCacheRootSatisfiesWarmGate)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();

    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/warm";
    auto pool = memory::memoryManager()->addLeafPool("percall-warm-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    cfg.cacheRoot = cachePath;

    BufferedInputTraceReplayer replayer{cfg};
    BufferedInputTraceReplayResult warm;
    {
        ScopedFileCacheManagerExposure exposure(*manager);
        // Populate (cold), then measure a warm pass via the per-call override.
        replayer.replay(BufferedInputReplayBackend::kFileCache);
        warm = replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            {.verifyBytes = false,
             .measureEventLoop = true,
             .fileCacheRoot = cachePath});
    }
    ReplayTimingSample s;
    s.slot = {ReplayTimingBlock::kForward, 1, ReplayTimingCell::kWarmFileCache};
    s.result = warm;
    validateTimedReplaySample(s, kSize); // warm gates must hold
    EXPECT_EQ(warm.oracleReadBytes, 0u);

    manager->shutdown();
}

/// A per-call FileCache root that differs from the installed manager's root is
/// rejected.
TEST_F(BufferedInputTraceReplayTest, PerCallFileCacheRootMismatchRejected)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();
    auto otherDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("percall-mismatch-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    cfg.cacheRoot = cachePath;

    // A different, sentinel-authenticated root that is not the manager's root.
    const std::string otherPath = otherDir->getPath() + "/other";
    fs::create_directories(otherPath);
    createSentinel(otherPath);

    BufferedInputTraceReplayer replayer{cfg};
    ScopedFileCacheManagerExposure exposure(*manager);
    VELOX_ASSERT_THROW(
        replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            {.verifyBytes = false,
             .measureEventLoop = true,
             .fileCacheRoot = otherPath}),
        "");
    manager->shutdown();
}

/// A per-call FileCache root without a sentinel is rejected.
TEST_F(BufferedInputTraceReplayTest, PerCallFileCacheRootMissingSentinelRejected)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("percall-nosentinel-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    // Intentionally NO sentinel at cachePath.
    cfg.cacheRoot = cachePath;

    BufferedInputTraceReplayer replayer{cfg};
    ScopedFileCacheManagerExposure exposure(*manager);
    VELOX_ASSERT_THROW(
        replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            {.verifyBytes = false,
             .measureEventLoop = true,
             .fileCacheRoot = cachePath}),
        "");
    manager->shutdown();
}

/// Direct replay rejects a nonempty per-call FileCache root.
TEST_F(BufferedInputTraceReplayTest, DirectRejectsPerCallFileCacheRoot)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    BufferedInputTraceReplayer replayer{cfg};
    VELOX_ASSERT_THROW(
        replayer.replay(
            BufferedInputReplayBackend::kDirect,
            {.verifyBytes = false,
             .measureEventLoop = true,
             .fileCacheRoot = "/some/root"}),
        "");
}

/// The one-argument functional replay continues to use config.cacheRoot (empty
/// per-call override) and is unaffected by the new field.
TEST_F(BufferedInputTraceReplayTest, OneArgReplayUsesConfigCacheRoot)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();

    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/fc";
    auto pool = memory::memoryManager()->addLeafPool("onearg-configroot-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    createSentinel(cachePath);
    cfg.cacheRoot = cachePath;

    BufferedInputTraceReplayer replayer{cfg};
    ScopedFileCacheManagerExposure exposure(*manager);
    const auto res = replayer.replay(BufferedInputReplayBackend::kFileCache);
    EXPECT_EQ(res.totalLogicalBytes, kSize);
    manager->shutdown();
}

// ---------------------------------------------------------------------------
// Task-018S cold/warm Task-2: five fresh cold managers + one warm manager
// (RED before implementation)
// ---------------------------------------------------------------------------

/// Run a full cold/warm pilot on a small real trace with a recording factory.
/// Returns the pilot result; leaves the manager hidden.
static void runColdWarmPilot(
    BufferedInputTraceReplayer& replayer,
    const SevenRootsFixture& sr,
    RecordingManagerFactory& rec,
    Q04ReplayTimingPilotResult& result)
{
    runQ04ColdWarmTimingPilot(replayer, sr.roots, rec.factory(), result);
}

/// Each fresh cold manager (and the warm manager at construction) starts empty.
TEST_F(BufferedInputTraceReplayTest, FiveColdManagersStartEmpty)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("cold-empty-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    ASSERT_EQ(result.samples.size(), 15u);
    ASSERT_EQ(rec.roots.size(), 7u);
    for (const auto& [used, segments] : rec.initialState)
    {
        EXPECT_EQ(used, 0u);
        EXPECT_EQ(segments, 0u);
    }
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

/// Each of the five B slots maps 1:1 to its unique timed cold root, in order.
TEST_F(BufferedInputTraceReplayTest, ColdRootsMapOneToOneToBSlots)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("cold-map-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    // The timed-cold roots appear, in order, exactly once each among the
    // factory calls (interleaved with verification-cold and warm).
    std::vector<std::string> timedColdUsed;
    for (const auto& r : rec.roots)
    {
        for (const auto& tc : sr.roots.timedColdRoots)
        {
            if (r == tc)
            {
                timedColdUsed.push_back(r);
                break;
            }
        }
    }
    ASSERT_EQ(timedColdUsed.size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ(timedColdUsed[i], sr.roots.timedColdRoots[i]) << "cold " << i;

    EXPECT_EQ(
        std::count(rec.roots.begin(), rec.roots.end(),
                   sr.roots.verificationColdRoot),
        1);
    EXPECT_EQ(
        std::count(rec.roots.begin(), rec.roots.end(), sr.roots.warmRoot), 1);
}

/// The one warm manager is constructed once and re-used (warm) across all five
/// C samples despite A/B interleaving.
TEST_F(BufferedInputTraceReplayTest, WarmManagerSurvivesABInterleaving)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("warm-survive-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    // Warm manager constructed exactly once (not per C sample).
    EXPECT_EQ(
        std::count(rec.roots.begin(), rec.roots.end(), sr.roots.warmRoot), 1);
    // All five warm-C timed samples are 100% hits.
    int warmSamples = 0;
    for (const auto& s : result.samples)
    {
        if (s.slot.cell == ReplayTimingCell::kWarmFileCache)
        {
            ++warmSamples;
            EXPECT_DOUBLE_EQ(s.result.warmHitPct, 100.0);
            EXPECT_EQ(s.result.cacheDelta.sourceReadBytes, 0u);
        }
    }
    EXPECT_EQ(warmSamples, 5);
}

/// Reusing one timed cold root for two B slots is rejected before any timed
/// sample or manager construction (root distinctness/freshness gate).
TEST_F(BufferedInputTraceReplayTest, ReusingColdRootIsRejected)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    // Mutation: reuse the first timed cold root for the second B slot.
    sr.roots.timedColdRoots[1] = sr.roots.timedColdRoots[0];
    auto pool = memory::memoryManager()->addLeafPool("cold-reuse-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    VELOX_ASSERT_THROW(
        runQ04ColdWarmTimingPilot(replayer, sr.roots, rec.factory(), result),
        "");
    EXPECT_TRUE(result.samples.empty());
    EXPECT_TRUE(rec.roots.empty());
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

/// Manager construction runs outside the timed scope: every construction happens
/// while no manager is exposed, and timed wallNs comes only from replay's event
/// loop (all samples positive).
TEST_F(BufferedInputTraceReplayTest, ManagerConstructionIsOutsideTimer)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("outside-timer-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    // Seven constructions (1 verification-cold + 5 timed-cold + 1 warm), each
    // while hidden — so factory time cannot be inside any exposed/timed replay.
    EXPECT_EQ(rec.roots.size(), 7u);
    EXPECT_EQ(rec.hiddenAtConstruction, static_cast<int>(rec.roots.size()));
    for (const auto& s : result.samples)
        EXPECT_GT(s.result.wallNs, 0u);
}

/// The full cold/warm pilot produces 15 validated samples, verified passes that
/// read every byte, timed passes with the oracle disabled, and an analysis.
TEST_F(BufferedInputTraceReplayTest, ColdWarmPilotProducesValidatedSamples)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 256 * 1024;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    auto pool = memory::memoryManager()->addLeafPool("coldwarm-e2e-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    ASSERT_EQ(result.samples.size(), 15u);
    EXPECT_EQ(result.verifiedA.oracleReadBytes, kSize);
    EXPECT_EQ(result.verifiedB.oracleReadBytes, kSize);
    EXPECT_EQ(result.coldC.oracleReadBytes, kSize);
    EXPECT_EQ(result.verifiedWarmC.oracleReadBytes, kSize);
    // Cold verification B populated the cache.
    EXPECT_GT(result.verifiedB.cacheDelta.cacheMissCount, 0u);
    EXPECT_GT(result.verifiedB.cacheDelta.cacheWriteBytes, 0u);
    for (const auto& s : result.samples)
    {
        EXPECT_EQ(s.result.oracleReadBytes, 0u);
        EXPECT_GT(s.result.wallNs, 0u);
        EXPECT_EQ(s.result.totalLogicalBytes, kSize);
    }
    // Analysis populated (all cells have five samples).
    for (const auto& cell : result.analysis.cells)
        EXPECT_EQ(cell.samples, 5u);
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

/// Regression proof for the verified cold-B foreground-predownload gate
/// (`gateVerifiedColdB`), driven end-to-end through production orchestration --
/// no test-only API is exposed.  The enqueued region starts partway into the
/// empty first cache segment: with the default 4 MiB boundary alignment the
/// 128 KiB file is one segment `[0, 128 KiB)`, so electing a downloader at read
/// offset 64 KiB forces `predownloadForCurrentSegment` to synchronously fill the
/// gap `[0, 64 KiB)` from source.  The verified cold-B pass therefore observes
/// matched positive predownload counters, which the corrected gate accepts and
/// the old `predownloadedFromSourceBytes == 0` gate (which halted the real
/// pilot) rejected.  The cold population fills the whole segment, so warm C stays
/// fully cached and its preserved zero-predownload gate still holds -- proven by
/// the pilot completing with fifteen validated samples.
TEST_F(BufferedInputTraceReplayTest, ColdWarmPilotVerifiedColdBFillsPredownloadGap)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 128 * 1024;
    constexpr uint64_t kGap = 64 * 1024;
    constexpr uint64_t kLogical = kSize - kGap;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, kGap, kLogical), kSize);
    auto sr = makeSevenRoots();
    auto pool =
        memory::memoryManager()->addLeafPool("coldwarm-predownload-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    runColdWarmPilot(replayer, sr, rec, result);

    // The pilot completed: all fifteen timed samples were validated and analyzed,
    // which also proves warm C's preserved zero-predownload gate held (a nonzero
    // warm predownload would have thrown inside gateVerifiedWarmC).
    ASSERT_EQ(result.samples.size(), 15u);

    // The verified cold-B pass hit the foreground predownload gap: its two
    // counters are positive and matched -- the exact production scenario the
    // corrected gateVerifiedColdB accepts and the old zero gate rejected.
    const auto& b = result.verifiedB.cacheDelta;
    EXPECT_GT(b.predownloadedFromSourceBytes, 0u);
    EXPECT_EQ(b.predownloadedFromSourceBytes, b.predownloadedBytes);
    EXPECT_GT(b.cacheMissCount, 0u);
    EXPECT_GT(b.sourceReadBytes, 0u);
    EXPECT_GT(b.cacheWriteBytes, 0u);
    EXPECT_EQ(b.evictedBytes, 0u);
    EXPECT_EQ(b.evictedSegments, 0u);
    EXPECT_EQ(result.verifiedB.oracleReadBytes, kLogical);

    // Warm C read the fully-populated segment from cache: no predownload, no
    // source read, positive hits.
    const auto& c = result.verifiedWarmC.cacheDelta;
    EXPECT_EQ(c.predownloadedFromSourceBytes, 0u);
    EXPECT_EQ(c.predownloadedBytes, 0u);
    EXPECT_EQ(c.sourceReadBytes, 0u);
    EXPECT_GT(c.cacheHitCount, 0u);

    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

/// The cold/warm JSON records the mode, cell definition, seven root identities
/// (in identity), and each B sample row's cold_cache_root.
TEST_F(BufferedInputTraceReplayTest, ColdWarmJsonRecordsCellDefinitionAndSevenRoots)
{
    constexpr uint64_t kBytes = 4096;
    const auto result = buildValidPilotResult(kBytes);

    auto datasetDir = TempDirectoryPath::create();
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    const auto doc = buildDocument(
        canonicalDataset, "data.bin", kBytes, minimalEvents(kBytes, 0, kBytes));
    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = "/trace/path";
    cfg.datasetRoot = canonicalDataset;
    const Q04ReplayTimingIdentity replayIdentity{"/replay/bin", "replay-build"};

    Q04ColdWarmTimingRoots roots;
    roots.verificationColdRoot = "/parent/verif";
    roots.timedColdRoots = {
        "/parent/c1", "/parent/c2", "/parent/c3", "/parent/c4", "/parent/c5"};
    roots.warmRoot = "/parent/warm";

    const auto json =
        q04ColdWarmTimingToJson(doc, cfg, result, replayIdentity, roots);

    EXPECT_EQ(json["mode"].asString(), "cold_filecache_timing_pilot");
    EXPECT_EQ(
        json["cell_definition"].asString(),
        "direct_cold_filecache_warm_filecache_v1");

    const auto& identity = json["identity"];
    EXPECT_EQ(
        identity["verification_cold_cache_root"].asString(), "/parent/verif");
    EXPECT_EQ(identity["warm_cache_root"].asString(), "/parent/warm");
    ASSERT_TRUE(identity["timed_cold_cache_roots"].isArray());
    ASSERT_EQ(identity["timed_cold_cache_roots"].size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ(
            identity["timed_cold_cache_roots"][i].asString(),
            roots.timedColdRoots[i]);
    // Capture and replay binary identities remain distinct.
    EXPECT_NE(
        identity["capture_binary_build_id"].asString(),
        identity["replay_binary_build_id"].asString());

    // Each B sample row carries its cold_cache_root, in cold slot order; other
    // rows do not.  Every row must also carry the gate-recomputation fields
    // (cache_write_bytes, oracle_read_bytes) so validity is recomputable from the
    // artifact alone: all timed samples read zero oracle bytes, cold B writes to
    // the cache, and Direct A / warm C perform no cache writes.
    size_t coldIdx = 0;
    for (const auto& s : json["samples"])
    {
        ASSERT_EQ(s.count("cache_write_bytes"), 1u);
        ASSERT_EQ(s.count("oracle_read_bytes"), 1u);
        EXPECT_EQ(s["oracle_read_bytes"].asInt(), 0);
        if (s["cell"].asString() == "B")
        {
            ASSERT_EQ(s.count("cold_cache_root"), 1u);
            EXPECT_EQ(
                s["cold_cache_root"].asString(), roots.timedColdRoots[coldIdx]);
            EXPECT_GT(s["cache_write_bytes"].asInt(), 0);
            ++coldIdx;
        }
        else
        {
            EXPECT_EQ(s.count("cold_cache_root"), 0u);
            EXPECT_EQ(s["cache_write_bytes"].asInt(), 0);
        }
    }
    EXPECT_EQ(coldIdx, 5u);

    EXPECT_EQ(json.count("root_cause"), 0u);
    EXPECT_EQ(json["samples"].size(), 15u);
}

/// The timed-cold-roots parser requires exactly five nonempty entries.
TEST_F(BufferedInputTraceReplayTest, ParseTimedColdRootsRequiresFive)
{
    const auto five = parseTimedColdCacheRoots("/a,/b,/c,/d,/e");
    EXPECT_EQ(five[0], "/a");
    EXPECT_EQ(five[4], "/e");
    VELOX_ASSERT_THROW(parseTimedColdCacheRoots("/a,/b,/c"), "");
    VELOX_ASSERT_THROW(parseTimedColdCacheRoots("/a,/b,/c,/d,/e,/f"), "");
    VELOX_ASSERT_THROW(parseTimedColdCacheRoots("/a,,/c,/d,/e"), "");
}

/// requireFreshCacheRoot accepts a root that holds only the sentinel.
TEST_F(BufferedInputTraceReplayTest, RequireFreshCacheRootAcceptsSentinelOnly)
{
    auto dir = TempDirectoryPath::create();
    const std::string root = dir->getPath() + "/r";
    fs::create_directories(root);
    createSentinel(root);
    requireFreshCacheRoot(root); // must not throw
}

/// requireFreshCacheRoot rejects a root with any non-sentinel/status entry.
TEST_F(BufferedInputTraceReplayTest, RequireFreshCacheRootRejectsStrayEntry)
{
    auto dir = TempDirectoryPath::create();
    const std::string root = dir->getPath() + "/r";
    fs::create_directories(root);
    createSentinel(root);
    {
        std::ofstream f(root + "/stray.bin");
        f << "x";
    }
    VELOX_ASSERT_THROW(requireFreshCacheRoot(root), "");
}

/// requireFreshCacheRoot rejects a root without a sentinel.
TEST_F(BufferedInputTraceReplayTest, RequireFreshCacheRootRejectsMissingSentinel)
{
    auto dir = TempDirectoryPath::create();
    const std::string root = dir->getPath() + "/r";
    fs::create_directories(root);
    VELOX_ASSERT_THROW(requireFreshCacheRoot(root), "");
}

/// requireFreshCacheRoot rejects a pre-existing "status" file: a truly fresh
/// root holds only the sentinel; "status" is written by manager initialization
/// and proves prior use.
TEST_F(BufferedInputTraceReplayTest, RequireFreshCacheRootRejectsPriorStatus)
{
    auto dir = TempDirectoryPath::create();
    const std::string root = dir->getPath() + "/r";
    fs::create_directories(root);
    createSentinel(root);
    {
        std::ofstream f(root + "/status");
        f << "prior";
    }
    VELOX_ASSERT_THROW(requireFreshCacheRoot(root), "");
}

/// canonicalizeColdWarmRoots validates (fresh/sentinel/common-parent/distinct)
/// and returns canonical root paths; non-canonical (symlinked) input spellings
/// are resolved, and the JSON identity plus each B cold_cache_root then use
/// canonical paths.
TEST_F(BufferedInputTraceReplayTest, CanonicalizeColdWarmRootsResolvesSymlinkSpellings)
{
    auto tmp = TempDirectoryPath::create();
    const std::string realParent = tmp->getPath() + "/real";
    fs::create_directories(realParent);
    const std::string linkParent = tmp->getPath() + "/link";
    fs::create_symlink(realParent, linkParent);

    const auto mkRoot = [&](const std::string& name)
    {
        const std::string p = realParent + "/" + name;
        fs::create_directories(p);
        createSentinel(p);
    };
    mkRoot("verif");
    mkRoot("c1");
    mkRoot("c2");
    mkRoot("c3");
    mkRoot("c4");
    mkRoot("c5");
    mkRoot("warm");

    // Non-canonical spellings via the symlinked parent.
    Q04ColdWarmTimingRoots raw;
    raw.verificationColdRoot = linkParent + "/verif";
    raw.timedColdRoots = {
        linkParent + "/c1", linkParent + "/c2", linkParent + "/c3",
        linkParent + "/c4", linkParent + "/c5"};
    raw.warmRoot = linkParent + "/warm";

    const auto roots = canonicalizeColdWarmRoots(raw, linkParent);

    // Every returned root is canonical (symlink resolved) and differs from the
    // caller spelling.
    EXPECT_EQ(
        roots.verificationColdRoot,
        fs::canonical(raw.verificationColdRoot).string());
    EXPECT_NE(roots.verificationColdRoot, raw.verificationColdRoot);
    EXPECT_EQ(roots.warmRoot, fs::canonical(raw.warmRoot).string());
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_EQ(
            roots.timedColdRoots[i],
            fs::canonical(raw.timedColdRoots[i]).string());
        EXPECT_NE(roots.timedColdRoots[i], raw.timedColdRoots[i]);
    }

    // JSON built from the canonical roots records canonical paths.
    constexpr uint64_t kBytes = 4096;
    const auto result = buildValidPilotResult(kBytes);
    auto datasetDir = TempDirectoryPath::create();
    const std::string canonicalDataset =
        fs::canonical(datasetDir->getPath()).string();
    const auto doc = buildDocument(
        canonicalDataset, "data.bin", kBytes, minimalEvents(kBytes, 0, kBytes));
    BufferedInputTraceReplayConfig cfg;
    cfg.tracePath = "/trace/path";
    cfg.datasetRoot = canonicalDataset;
    const Q04ReplayTimingIdentity replayIdentity{"/replay/bin", "replay-build"};

    const auto json =
        q04ColdWarmTimingToJson(doc, cfg, result, replayIdentity, roots);
    const auto& identity = json["identity"];
    EXPECT_EQ(
        identity["verification_cold_cache_root"].asString(),
        roots.verificationColdRoot);
    EXPECT_EQ(identity["warm_cache_root"].asString(), roots.warmRoot);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ(
            identity["timed_cold_cache_roots"][i].asString(),
            roots.timedColdRoots[i]);

    size_t coldIdx = 0;
    for (const auto& s : json["samples"])
    {
        if (s["cell"].asString() == "B")
        {
            EXPECT_EQ(
                s["cold_cache_root"].asString(), roots.timedColdRoots[coldIdx]);
            EXPECT_NE(
                s["cold_cache_root"].asString(), raw.timedColdRoots[coldIdx]);
            ++coldIdx;
        }
    }
    EXPECT_EQ(coldIdx, 5u);
}

/// The superseded single-manager pilot is fail-closed: it always throws.
TEST_F(BufferedInputTraceReplayTest, SupersededSingleThreadPilotFailsClosed)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    auto cacheDir = TempDirectoryPath::create();
    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    const std::string cachePath = cacheDir->getPath() + "/fc";
    fs::create_directories(cachePath);
    createSentinel(cachePath); // sentinel before the manager
    auto pool = memory::memoryManager()->addLeafPool("superseded-test");
    auto manager = makeTestFileCacheManager(cachePath, pool.get());
    cfg.cacheRoot = cachePath;

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    VELOX_ASSERT_THROW(
        runQ04SingleThreadTimingPilot(replayer, *manager, result), "");
    EXPECT_TRUE(result.samples.empty());
    manager->shutdown();
}

/// A missing sentinel on any root fails the pilot before any factory
/// invocation: the factory never creates a missing sentinel.
TEST_F(BufferedInputTraceReplayTest, FactoryDoesNotCreateMissingSentinel)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceDir = TempDirectoryPath::create();
    constexpr size_t kSize = 4096;
    auto cfg = makeConfig(datasetDir->getPath(),
                          traceDir->getPath() + "/trace",
                          minimalEvents(kSize, 0, kSize), kSize);
    auto sr = makeSevenRoots();
    // Remove one root's sentinel; authentication must fail before the factory.
    fs::remove(
        sr.roots.verificationColdRoot + "/.velox_benchmark_cache_sentinel");
    auto pool = memory::memoryManager()->addLeafPool("missing-sentinel-test");
    RecordingManagerFactory rec{pool.get()};

    BufferedInputTraceReplayer replayer{cfg};
    Q04ReplayTimingPilotResult result;
    VELOX_ASSERT_THROW(
        runQ04ColdWarmTimingPilot(replayer, sr.roots, rec.factory(), result),
        "");
    EXPECT_TRUE(rec.roots.empty()); // no factory invocation
    EXPECT_TRUE(result.samples.empty());
    EXPECT_EQ(ch::FileCacheManager::getInstance(), nullptr);
}

/// Constructing/initializing a real manager on a root that already holds a
/// top-level sentinel succeeds and the cache starts empty (locks the verified
/// default-loader contract: the sentinel lives outside the scanned type dirs).
TEST_F(BufferedInputTraceReplayTest, ManagerInitializesOnPreSentinelRoot)
{
    auto cacheDir = TempDirectoryPath::create();
    const std::string root = cacheDir->getPath() + "/pre_sentinel";
    fs::create_directories(root);
    createSentinel(root); // sentinel BEFORE the manager
    auto pool = memory::memoryManager()->addLeafPool("pre-sentinel-init-test");

    auto manager = makeTestFileCacheManager(root, pool.get());
    EXPECT_EQ(manager->getDefault()->getUsedCacheSize(), 0u);
    EXPECT_EQ(manager->getDefault()->getFileSegmentsNum(), 0u);
    manager->shutdown();
}

} // namespace
} // namespace facebook::velox::dwio::common
