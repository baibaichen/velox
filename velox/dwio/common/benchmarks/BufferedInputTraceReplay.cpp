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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include <folly/ScopeGuard.h>
#include <folly/dynamic.h>

#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/BufferedInputTrace.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/PositionProvider.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::dwio::common {

namespace {
namespace fs = std::filesystem;
using dwio::common::LogType;
using dwio::common::MetricsLog;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compare logical bytes consumed from the backend against the oracle file.
void checkOracleBytes(
    const ReadFile* oracle,
    uint64_t fileOffset,
    const std::vector<char>& actual,
    uint64_t length)
{
    std::vector<char> expected(length);
    const std::string_view sv =
        oracle->pread(fileOffset, length, expected.data());
    VELOX_CHECK_EQ(
        sv.size(), length,
        "Oracle pread returned {} bytes, expected {} at file offset {}",
        sv.size(), length, fileOffset);
    VELOX_CHECK(
        std::memcmp(actual.data(), expected.data(), length) == 0,
        "Byte mismatch at oracle file offset {}",
        fileOffset);
}


// ---------------------------------------------------------------------------
// InputEntry — live input tracking during replay
// ---------------------------------------------------------------------------

struct StreamState
{
    uint64_t ownerInputId{0};
    uint64_t fileIdx{0};         // index into manifest.files
    uint64_t enqueueOffset{0};   // region start recorded at enqueue
    uint64_t regionLength{0};    // region length recorded at enqueue
    uint64_t fileSize{0};        // manifest file size for absolute bounds
    std::unique_ptr<SeekableInputStream> stream;
};

struct InputEntry
{
    uint64_t fileIdx{0};
    std::unique_ptr<BufferedInput> input;
};

// ---------------------------------------------------------------------------
// makeInput — construct a backend BufferedInput for one event
// ---------------------------------------------------------------------------

std::unique_ptr<BufferedInput> makeInput(
    BufferedInputReplayBackend backend,
    const std::shared_ptr<ReadFile>& file,
    const std::string& absolutePath,
    memory::MemoryPool& pool,
    const std::shared_ptr<io::IoStatistics>& ioStatistics,
    const std::shared_ptr<velox::IoStats>& ioStats)
{
    auto& ids = fileIds();
    auto tracker = std::make_shared<cache::ScanTracker>(
        "traceReplay", nullptr, 256ULL << 10);
    StringIdLease fileNum{ids, absolutePath};
    StringIdLease groupId{ids, "traceReplayGroup"};

    io::ReaderOptions readerOptions{&pool};
    readerOptions.setDataIoStats(ioStatistics);

    if (backend == BufferedInputReplayBackend::kDirect)
    {
        return std::make_unique<DirectBufferedInput>(
            file,
            MetricsLog::voidLog(),
            fileNum,
            tracker,
            groupId,
            ioStatistics,
            ioStats,
            /*executor=*/nullptr,
            readerOptions);
    }

    // B (passthrough) or C (cache).
    const bool passthrough = backend == BufferedInputReplayBackend::kPassthrough;
    ch::FileCachePtr cache = nullptr;
    if (!passthrough)
    {
        auto* mgr = ch::FileCacheManager::getInstance();
        VELOX_CHECK_NOT_NULL(
            mgr, "FileCacheManager not installed for kFileCache replay");
        cache = mgr->getDefault();
    }

    ch::FileCacheKey cacheKey = ch::FileCacheKey::fromPath(absolutePath);

    // Align context and origin with production HiveConnectorUtil usage:
    // segmentType=Data, userId from manager (empty for passthrough), userWeight=0.
    ch::FileCacheRequestContext context;
    context.queryId = "trace-replay";
    context.segmentType = ch::FileSegmentKeyType::Data;
    if (!passthrough)
    {
        auto* mgr = ch::FileCacheManager::getInstance();
        if (mgr)
        {
            context.userId = mgr->commonUserId();
        }
    }
    context.userWeight = 0;

    ch::FileCacheOriginInfo origin(
        context.userId,
        context.userWeight,
        context.segmentType);

    dwio::common::ReaderOptions fcbiOptions{&pool};
    fcbiOptions.setDataIoStats(ioStatistics);

    return std::unique_ptr<BufferedInput>(new ch::FileCacheBufferedInput(
        file,
        cache,
        cacheKey,
        origin,
        ch::FileCacheReadOptions{},
        context,
        MetricsLog::voidLog(),
        ioStatistics,
        ioStats,
        /*executor=*/nullptr,
        fcbiOptions,
        /*fileReadOps=*/{},
        /*cancellationToken=*/{},
        passthrough
            ? ch::FileCacheBufferedInput::ReadMode::kPassthrough
            : ch::FileCacheBufferedInput::ReadMode::kCache));
}

} // namespace

// ---------------------------------------------------------------------------
// BufferedInputTraceReplayer
// ---------------------------------------------------------------------------

BufferedInputTraceReplayer::BufferedInputTraceReplayer(
    BufferedInputTraceReplayConfig config)
    : config_(std::move(config))
{
    VELOX_CHECK(!config_.tracePath.empty(), "tracePath must not be empty");
    VELOX_CHECK(!config_.datasetRoot.empty(), "datasetRoot must not be empty");

    // Canonicalize dataset root — must be an existing directory.
    std::error_code ec;
    const fs::path dsPath = fs::canonical(config_.datasetRoot, ec);
    VELOX_CHECK(
        !ec,
        "Cannot canonicalize datasetRoot '{}': {}",
        config_.datasetRoot,
        ec.message());
    VELOX_CHECK(
        fs::is_directory(dsPath, ec) && !ec,
        "datasetRoot '{}' is not a directory",
        config_.datasetRoot);
    canonicalDatasetRoot_ = dsPath.string();

    // Load and validate the trace.
    trace_ = loadBufferedInputTrace(config_.tracePath);
    validateBufferedInputTrace(trace_, canonicalDatasetRoot_);

    // Create a memory pool for backends — unique name to avoid conflicts when
    // multiple replayers exist concurrently.
    static std::atomic<uint64_t> sPoolCounter{0};
    const std::string poolName =
        "BufferedInputTraceReplayer_" +
        std::to_string(sPoolCounter.fetch_add(1, std::memory_order_relaxed));
    poolOwned_ = memory::memoryManager()->addLeafPool(poolName);
    pool_ = poolOwned_.get();

    filesystems::registerLocalFileSystem();

    // Map file IDs to manifest indices and open every source/oracle handle once,
    // after strict trace validation.  These invariants are revalidated here with
    // no fallback: a failure aborts construction.
    fileIdToIndex_.reserve(trace_.files.size());
    sourceFiles_.reserve(trace_.files.size());
    oracleFiles_.reserve(trace_.files.size());
    const std::string containmentPrefix = canonicalDatasetRoot_ + "/";
    for (size_t i = 0; i < trace_.files.size(); ++i)
    {
        const auto& f = trace_.files[i];

        // File IDs must be unique and nonzero.
        VELOX_CHECK_NE(f.fileId, 0u, "Trace file id must be nonzero (index {})", i);
        const auto [it, inserted] = fileIdToIndex_.emplace(f.fileId, i);
        VELOX_CHECK(
            inserted,
            "Duplicate trace file id {} (indices {} and {})",
            f.fileId, it->second, i);

        // Canonical containment: resolved path must remain under the dataset root.
        const std::string absPath = containmentPrefix + f.relativePath;
        std::error_code ec;
        const fs::path canonFile = fs::canonical(absPath, ec);
        VELOX_CHECK(
            !ec,
            "Cannot canonicalize trace file '{}': {}",
            absPath, ec.message());
        const std::string canonStr = canonFile.string();
        VELOX_CHECK(
            canonStr.rfind(containmentPrefix, 0) == 0,
            "Trace file '{}' escapes dataset root '{}'",
            canonStr, canonicalDatasetRoot_);

        // Open source and oracle handles; the opened size must equal the manifest.
        auto source = std::make_shared<LocalReadFile>(canonStr);
        VELOX_CHECK_EQ(
            source->size(), f.size,
            "Trace file '{}' opened size {} does not match manifest size {}",
            canonStr, source->size(), f.size);
        auto oracle = std::make_shared<LocalReadFile>(canonStr);

        sourceFiles_.push_back(std::move(source));
        oracleFiles_.push_back(std::move(oracle));
    }
}

BufferedInputTraceReplayResult BufferedInputTraceReplayer::replay(
    BufferedInputReplayBackend backend)
{
    return replay(
        backend,
        {.verifyBytes = config_.verifyBytes, .measureEventLoop = false});
}

BufferedInputTraceReplayResult BufferedInputTraceReplayer::replay(
    BufferedInputReplayBackend backend,
    const BufferedInputTraceReplayOptions& options)
{
    // Validate manager state is consistent with the requested backend.
    // Direct and Passthrough must run without an installed FileCacheManager so
    // that their IoStats are not contaminated by a concurrent kFileCache run.
    if (backend == BufferedInputReplayBackend::kDirect ||
        backend == BufferedInputReplayBackend::kPassthrough)
    {
        VELOX_CHECK_NULL(
            ch::FileCacheManager::getInstance(),
            "FileCacheManager must not be installed when replaying "
            "Direct/Passthrough (would contaminate stats)");
        VELOX_CHECK(
            options.fileCacheRoot.empty(),
            "Direct/Passthrough replay must not set a per-call FileCache root");
    }

    // Validate kFileCache manager root matches the effective root and sentinel.
    if (backend == BufferedInputReplayBackend::kFileCache)
    {
        auto* mgr = ch::FileCacheManager::getInstance();
        VELOX_CHECK_NOT_NULL(
            mgr, "FileCacheManager not installed for kFileCache replay");

        // Effective root: per-call override when nonempty, else config.cacheRoot.
        const std::string& effectiveRoot = options.fileCacheRoot.empty()
            ? config_.cacheRoot
            : options.fileCacheRoot;
        VELOX_CHECK(!effectiveRoot.empty(),
            "FileCache root must not be empty for kFileCache replay "
            "(set config.cacheRoot or options.fileCacheRoot)");

        std::error_code ec;
        const fs::path canonMgr =
            fs::canonical(mgr->getDefault()->getBasePath(), ec);
        VELOX_CHECK(!ec,
            "Cannot canonicalize manager cache root '{}': {}",
            mgr->getDefault()->getBasePath(), ec.message());
        const fs::path canonEff = fs::canonical(effectiveRoot, ec);
        VELOX_CHECK(!ec,
            "Cannot canonicalize effective FileCache root '{}': {}",
            effectiveRoot, ec.message());
        VELOX_CHECK(
            canonMgr == canonEff,
            "Installed FileCacheManager root '{}' does not match effective "
            "FileCache root '{}'",
            canonMgr.string(), canonEff.string());

        // Validate sentinel inside library so no caller can bypass this check.
        validateCacheSentinel(effectiveRoot);
    }

    // Per-pass mutable state shared between setup, the timed event loop, and
    // post-timing result construction.
    std::unordered_map<uint64_t, InputEntry> inputs;   // inputId -> entry
    std::unordered_map<uint64_t, StreamState> streams; // streamId -> state
    std::shared_ptr<io::IoStatistics> ioStatistics;
    std::shared_ptr<velox::IoStats> veloxIoStats;
    ch::FileCacheStatsSnapshot beforeCache;

    uint64_t totalLogicalBytes = 0;
    uint64_t nextCount = 0;
    uint64_t seekCount = 0;
    uint64_t oracleReadBytes = 0;

    // Setup: create per-pass maps, I/O ledgers, and the before-cache snapshot.
    // It parses no JSON and opens no files (those are constructor invariants).
    const ReplayTimingAction setup = [&]
    {
        inputs.clear();
        streams.clear();
        ioStatistics = std::make_shared<io::IoStatistics>();
        veloxIoStats = std::make_shared<velox::IoStats>();
        beforeCache = ch::takeFileCacheStatsSnapshot();
    };

    // Instrumented Next wrapper — counts each backend Next() call.
    // (The counter is captured by reference so all stream Next() calls in the
    // loop below contribute to the same per-replay total.)
    auto doConsume = [&](StreamState& ss,
                         const BufferedInputTraceEvent& ev) -> uint64_t
    {
        VELOX_CHECK_GT(ev.length, 0u,
            "kConsume: zero-length consume is invalid");

        // Stream-relative bounds (within enqueued region).
        VELOX_CHECK(
            ev.offset <= ss.regionLength &&
            ev.length <= ss.regionLength - ev.offset,
            "kConsume: offset {} + length {} exceeds regionLength {} for streamId {}",
            ev.offset, ev.length, ss.regionLength, ev.streamId);

        // Absolute file bounds: enqueueOffset + ev.offset + ev.length <= fileSize.
        // Guard against overflow first.
        VELOX_CHECK(
            ev.offset <= UINT64_MAX - ss.enqueueOffset,
            "kConsume: absolute offset overflow for streamId {}", ev.streamId);
        const uint64_t absStart = ss.enqueueOffset + ev.offset;
        VELOX_CHECK(
            ev.length <= UINT64_MAX - absStart,
            "kConsume: absolute end overflow for streamId {}", ev.streamId);
        VELOX_CHECK(
            absStart + ev.length <= ss.fileSize,
            "kConsume: absolute end {} exceeds file size {} for streamId {}",
            absStart + ev.length, ss.fileSize, ev.streamId);

        // int64_t safety for ByteCount comparison.
        VELOX_CHECK(
            ev.offset <= static_cast<uint64_t>(INT64_MAX),
            "kConsume: stream offset {} overflows int64_t for streamId {}",
            ev.offset, ev.streamId);
        VELOX_CHECK(
            ev.offset + ev.length <= static_cast<uint64_t>(INT64_MAX),
            "kConsume: stream end {} overflows int64_t for streamId {}",
            ev.offset + ev.length, ev.streamId);

        VELOX_CHECK_EQ(
            ss.stream->ByteCount(),
            static_cast<int64_t>(ev.offset),
            "Stream ByteCount mismatch before consume (streamId {})",
            ev.streamId);

        std::vector<char> accumBuf;
        std::vector<char>* accumPtr =
            options.verifyBytes ? &accumBuf : nullptr;
        if (accumPtr)
            accumPtr->resize(ev.length);

        uint64_t consumed = 0;
        while (consumed < ev.length)
        {
            const void* data;
            int size;
            VELOX_CHECK(
                ss.stream->Next(&data, &size),
                "Unexpected EOF after {} of {} bytes (streamId {}, offset {})",
                consumed, ev.length, ev.streamId, ev.offset);
            VELOX_CHECK_GT(size, 0,
                "stream.Next returned non-positive size (streamId {})",
                ev.streamId);
            ++nextCount;
            const uint64_t available = static_cast<uint64_t>(size);
            const uint64_t want = ev.length - consumed;
            const uint64_t take = std::min(available, want);
            if (accumPtr)
                std::memcpy(accumPtr->data() + consumed, data, take);
            consumed += take;
            if (take < available)
            {
                VELOX_CHECK_LE(
                    available - take, static_cast<uint64_t>(INT32_MAX),
                    "BackUp count too large (streamId {})", ev.streamId);
                ss.stream->BackUp(static_cast<int32_t>(available - take));
            }
        }

        VELOX_CHECK_EQ(
            ss.stream->ByteCount(),
            static_cast<int64_t>(ev.offset + ev.length),
            "Stream ByteCount mismatch after consume (streamId {})",
            ev.streamId);

        if (options.verifyBytes)
        {
            checkOracleBytes(
                oracleFiles_[ss.fileIdx].get(),
                ss.enqueueOffset + ev.offset,
                accumBuf,
                ev.length);
            oracleReadBytes += ev.length;
        }
        return consumed;
    };

    // Event loop: the timed region.  Includes all input/stream create, operate,
    // and close events and the final lifecycle-closure checks.
    const ReplayTimingAction eventLoop = [&]
    {
    // Execute events in seq order (already sorted by validation).
    for (const auto& ev : trace_.events)
    {
        switch (ev.op)
        {
        case BufferedInputTraceOp::kInputCreate:
        {
            auto idxIt = fileIdToIndex_.find(ev.fileId);
            VELOX_CHECK(
                idxIt != fileIdToIndex_.end(),
                "kInputCreate: fileId {} not found in manifest", ev.fileId);
            const size_t fileIdx = idxIt->second;
            const std::string absPath =
                canonicalDatasetRoot_ + "/" +
                trace_.files[fileIdx].relativePath;

            auto inp = makeInput(
                backend, sourceFiles_[fileIdx], absPath,
                *pool_, ioStatistics, veloxIoStats);
            inputs[ev.inputId] = {fileIdx, std::move(inp)};
            break;
        }
        case BufferedInputTraceOp::kInputClone:
        {
            auto it = inputs.find(ev.parentInputId);
            VELOX_CHECK(it != inputs.end(),
                        "kInputClone: parentInputId {} not found",
                        ev.parentInputId);
            auto cloned = it->second.input->clone();
            const size_t fileIdx = it->second.fileIdx;
            inputs[ev.inputId] = {fileIdx, std::move(cloned)};
            break;
        }
        case BufferedInputTraceOp::kInputReset:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kInputReset: inputId {} not found", ev.inputId);
            it->second.input->reset();
            break;
        }
        case BufferedInputTraceOp::kInputPreload:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kInputPreload: inputId {} not found", ev.inputId);
            it->second.input->preload();
            break;
        }
        case BufferedInputTraceOp::kInputSetNumStripes:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kInputSetNumStripes: inputId {} not found", ev.inputId);
            VELOX_CHECK(
                ev.argument <= static_cast<int64_t>(INT32_MAX) &&
                ev.argument >= 0,
                "kInputSetNumStripes: argument {} out of int32 range", ev.argument);
            it->second.input->setNumStripes(
                static_cast<int32_t>(ev.argument));
            break;
        }
        case BufferedInputTraceOp::kInputClose:
        {
            VELOX_CHECK(inputs.erase(ev.inputId) == 1,
                        "kInputClose: inputId {} not found", ev.inputId);
            break;
        }
        case BufferedInputTraceOp::kStreamEnqueue:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kStreamEnqueue: inputId {} not found", ev.inputId);
            auto& entry = it->second;
            const uint64_t fileSize = trace_.files[entry.fileIdx].size;
            // Absolute region bounds must fit within the manifest file.
            VELOX_CHECK(
                ev.offset <= fileSize,
                "kStreamEnqueue: offset {} exceeds file size {} for streamId {}",
                ev.offset, fileSize, ev.streamId);
            VELOX_CHECK(
                ev.length <= fileSize - ev.offset,
                "kStreamEnqueue: offset {} + length {} exceeds file size {} "
                "for streamId {}",
                ev.offset, ev.length, fileSize, ev.streamId);
            velox::common::Region region{ev.offset, ev.length};
            auto stream = entry.input->enqueue(region, nullptr);
            StreamState ss;
            ss.ownerInputId  = ev.inputId;
            ss.fileIdx       = entry.fileIdx;
            ss.enqueueOffset = ev.offset;
            ss.regionLength  = ev.length;
            ss.fileSize      = fileSize;
            ss.stream        = std::move(stream);
            streams[ev.streamId] = std::move(ss);
            break;
        }
        case BufferedInputTraceOp::kLoad:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kLoad: inputId {} not found", ev.inputId);
            it->second.input->load(
                static_cast<LogType>(ev.logType));
            break;
        }
        case BufferedInputTraceOp::kStreamRead:
        {
            auto it = inputs.find(ev.inputId);
            VELOX_CHECK(it != inputs.end(),
                        "kStreamRead: inputId {} not found", ev.inputId);
            auto& entry = it->second;
            const uint64_t fileSize = trace_.files[entry.fileIdx].size;
            // Absolute region bounds must fit within the manifest file.
            VELOX_CHECK(
                ev.offset <= fileSize,
                "kStreamRead: offset {} exceeds file size {} for streamId {}",
                ev.offset, fileSize, ev.streamId);
            VELOX_CHECK(
                ev.length <= fileSize - ev.offset,
                "kStreamRead: offset {} + length {} exceeds file size {} "
                "for streamId {}",
                ev.offset, ev.length, fileSize, ev.streamId);
            auto stream = entry.input->read(
                ev.offset, ev.length,
                static_cast<LogType>(ev.logType));
            StreamState ss;
            ss.ownerInputId  = ev.inputId;
            ss.fileIdx       = entry.fileIdx;
            ss.enqueueOffset = ev.offset;
            ss.regionLength  = ev.length;
            ss.fileSize      = fileSize;
            ss.stream        = std::move(stream);
            streams[ev.streamId] = std::move(ss);
            break;
        }
        case BufferedInputTraceOp::kConsume:
        {
            auto sit = streams.find(ev.streamId);
            VELOX_CHECK(sit != streams.end(),
                        "kConsume: streamId {} not found", ev.streamId);
            totalLogicalBytes += doConsume(sit->second, ev);
            break;
        }
        case BufferedInputTraceOp::kSkip:
        {
            auto sit = streams.find(ev.streamId);
            VELOX_CHECK(sit != streams.end(),
                        "kSkip: streamId {} not found", ev.streamId);
            // argument holds the skip count (result holds the success flag).
            VELOX_CHECK(
                ev.argument >= 0,
                "kSkip: argument {} is negative for streamId {}",
                ev.argument, ev.streamId);
            const bool ok = sit->second.stream->SkipInt64(
                static_cast<int64_t>(ev.argument));
            VELOX_CHECK_EQ(
                static_cast<int64_t>(ok),
                ev.result,
                "kSkip result mismatch for streamId {}", ev.streamId);
            break;
        }
        case BufferedInputTraceOp::kSeek:
        {
            auto sit = streams.find(ev.streamId);
            VELOX_CHECK(sit != streams.end(),
                        "kSeek: streamId {} not found", ev.streamId);
            VELOX_CHECK_EQ(
                sit->second.stream->positionSize(), 1u,
                "kSeek: positionSize != 1 for streamId {}", ev.streamId);
            // result holds the post-seek ByteCount (target position).
            VELOX_CHECK(
                ev.result >= 0,
                "kSeek: result {} is negative for streamId {}",
                ev.result, ev.streamId);
            const uint64_t seekTarget = static_cast<uint64_t>(ev.result);
            const std::vector<uint64_t> positions{seekTarget};
            PositionProvider pp{positions};
            sit->second.stream->seekToPosition(pp);
            ++seekCount;
            VELOX_CHECK_EQ(
                sit->second.stream->ByteCount(),
                static_cast<int64_t>(seekTarget),
                "kSeek: ByteCount mismatch after seek for streamId {}",
                ev.streamId);
            break;
        }
        case BufferedInputTraceOp::kStreamEof:
        {
            auto sit = streams.find(ev.streamId);
            VELOX_CHECK(sit != streams.end(),
                        "kStreamEof: streamId {} not found", ev.streamId);
            const void* data;
            int size;
            VELOX_CHECK(
                !sit->second.stream->Next(&data, &size),
                "kStreamEof: expected EOF but Next returned true for streamId {}",
                ev.streamId);
            break;
        }
        case BufferedInputTraceOp::kStreamClose:
        {
            VELOX_CHECK(streams.erase(ev.streamId) == 1,
                        "kStreamClose: streamId {} not found", ev.streamId);
            break;
        }
        default:
            VELOX_FAIL("Unknown BufferedInputTraceOp {}", static_cast<int>(ev.op));
        }
    }

    VELOX_CHECK(
        inputs.empty(),
        "Replay ended with {} live input(s); trace lifecycle error",
        inputs.size());
    VELOX_CHECK(
        streams.empty(),
        "Replay ended with {} live stream(s); trace lifecycle error",
        streams.size());
    };

    // Production clock: steady_clock nanoseconds.  A fake clock is injected only
    // through measureReplayEventLoop's nowNanos parameter, never here.
    const auto nowNanos = []
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    };

    uint64_t wallNs = 0;
    if (options.measureEventLoop)
    {
        wallNs = measureReplayEventLoop(setup, eventLoop, nowNanos);
    }
    else
    {
        setup();
        eventLoop();
    }

    // After the clock stops: snapshot subtraction and result construction.
    BufferedInputTraceReplayResult result;
    result.totalLogicalBytes = totalLogicalBytes;
    result.nextCount = nextCount;
    result.seekCount = seekCount;
    result.oracleReadBytes = oracleReadBytes;
    result.wallNs = wallNs;

    // Passthrough bytes live in velox::IoStats.
    {
        const auto statsMap = veloxIoStats->stats();
        const auto it = statsMap.find(ch::kFileCachePassthroughReadBytes);
        if (it != statsMap.end())
            result.passthroughBytes =
                static_cast<uint64_t>(it->second.sum);
    }

    // FileCache delta (meaningful only for kFileCache backend).
    const ch::FileCacheStatsSnapshot afterCache =
        ch::takeFileCacheStatsSnapshot();
    result.cacheDelta = afterCache - beforeCache;

    if (backend == BufferedInputReplayBackend::kFileCache)
    {
        const uint64_t totalLookups =
            result.cacheDelta.cacheHitCount + result.cacheDelta.cacheMissCount;
        if (totalLookups > 0)
        {
            result.warmHitPct =
                100.0 * static_cast<double>(result.cacheDelta.cacheHitCount) /
                static_cast<double>(totalLookups);
        }
    }

    return result;
}

uint64_t measureReplayEventLoop(
    const ReplayTimingAction& setup,
    const ReplayTimingAction& eventLoop,
    const ReplayNowNanos& nowNanos)
{
    setup();
    const uint64_t start = nowNanos();
    eventLoop();
    const uint64_t end = nowNanos();
    VELOX_CHECK_GE(end, start, "Replay timing clock moved backwards");
    return end - start;
}

void validateCacheSentinel(const std::string& cacheRoot)
{
    const fs::path sentinel =
        fs::path(cacheRoot) / ".velox_benchmark_cache_sentinel";
    // Use symlink_status so symlinks are NOT followed — a symlink sentinel
    // is rejected even if its target is a regular file.
    std::error_code ec;
    const fs::file_status st = fs::symlink_status(sentinel, ec);
    VELOX_CHECK(
        !ec,
        "Cannot stat cache sentinel '{}': {}",
        sentinel.string(), ec.message());
    VELOX_CHECK(
        st.type() != fs::file_type::symlink,
        "Cache sentinel '{}' must not be a symlink",
        sentinel.string());
    VELOX_CHECK(
        st.type() == fs::file_type::regular,
        "Cache sentinel '{}' is not a regular file (type={})",
        sentinel.string(), static_cast<int>(st.type()));
}

// ---------------------------------------------------------------------------
// Task-018S: fixed 3+2 timing order and summary analysis
// ---------------------------------------------------------------------------

namespace {

/// Median of a nonempty list of unsigned samples.  Odd count returns the middle
/// value; even count returns the overflow-safe integer midpoint of the two
/// middle values.
uint64_t replayMedianNs(std::vector<uint64_t> values)
{
    VELOX_CHECK(!values.empty(), "Cannot take median of zero samples");
    std::sort(values.begin(), values.end());
    const size_t n = values.size();
    if (n % 2 == 1)
    {
        return values[n / 2];
    }
    // lo <= hi after sorting; midpoint cannot overflow.
    const uint64_t lo = values[n / 2 - 1];
    const uint64_t hi = values[n / 2];
    return lo + (hi - lo) / 2;
}

/// Checked signed difference `after - before`, guarding the unsigned-to-int64
/// conversion of both operands so the subtraction cannot overflow.
int64_t replayCheckedDeltaNs(uint64_t after, uint64_t before)
{
    VELOX_CHECK_LE(
        after, static_cast<uint64_t>(INT64_MAX),
        "Timing value {} exceeds int64 range", after);
    VELOX_CHECK_LE(
        before, static_cast<uint64_t>(INT64_MAX),
        "Timing value {} exceeds int64 range", before);
    return static_cast<int64_t>(after) - static_cast<int64_t>(before);
}

/// Three-valued sign; zero is its own sign.
int replaySign(int64_t v)
{
    return (v > 0) - (v < 0);
}

bool replaySlotsEqual(const ReplayTimingSlot& a, const ReplayTimingSlot& b)
{
    return a.block == b.block && a.sample == b.sample && a.cell == b.cell;
}

/// True iff every FileCache cumulative counter delta is zero.  Gauge fields are
/// absolute in a delta snapshot (not deltas) and are intentionally not checked.
bool allFileCacheCumulativeDeltasZero(const ch::FileCacheStatsSnapshot& d)
{
    return d.cacheReadBytes == 0 && d.sourceReadBytes == 0 &&
        d.cacheWriteBytes == 0 && d.cacheHitCount == 0 &&
        d.cacheMissCount == 0 && d.predownloadedFromSourceBytes == 0 &&
        d.predownloadedBytes == 0 && d.reserveAttempts == 0 &&
        d.reserveFailures == 0 && d.evictedBytes == 0 &&
        d.evictedSegments == 0 && d.evictionTries == 0 &&
        d.waitReadBufferMicroseconds == 0 &&
        d.readFromSourceMicroseconds == 0 &&
        d.predownloadedFromSourceMicroseconds == 0 &&
        d.readFromCacheMicroseconds == 0 && d.cacheWriteMicroseconds == 0 &&
        d.createBufferMicroseconds == 0;
}

} // namespace

std::vector<ReplayTimingSlot> q04ReplayTimingOrder()
{
    using B = ReplayTimingBlock;
    using Cell = ReplayTimingCell;
    return {
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
}

void validateTimedReplaySample(
    const ReplayTimingSample& sample,
    uint64_t expectedLogicalBytes)
{
    const auto& r = sample.result;

    VELOX_CHECK_EQ(
        r.totalLogicalBytes, expectedLogicalBytes,
        "Timed sample logical bytes {} != expected {}",
        r.totalLogicalBytes, expectedLogicalBytes);
    VELOX_CHECK_GT(r.wallNs, 0u, "Timed sample wallNs must be positive");
    VELOX_CHECK_GT(r.nextCount, 0u, "Timed sample nextCount must be positive");
    VELOX_CHECK_EQ(
        r.oracleReadBytes, 0u,
        "Timed sample must not perform oracle reads (oracleReadBytes={})",
        r.oracleReadBytes);
    // No timing cell runs the passthrough path.
    VELOX_CHECK_EQ(
        r.passthroughBytes, 0u,
        "Timed sample must have zero passthrough bytes");

    const auto& d = r.cacheDelta;
    switch (sample.slot.cell)
    {
    case ReplayTimingCell::kDirect:
        VELOX_CHECK(
            allFileCacheCumulativeDeltasZero(d),
            "A (Direct) must have every FileCache cumulative delta zero");
        break;
    case ReplayTimingCell::kColdFileCache:
        // Cold population: the empty cache is missed, source-read, and written.
        VELOX_CHECK_GT(
            d.cacheMissCount, 0u,
            "B (cold FileCache) must have positive cache misses");
        VELOX_CHECK_GT(
            d.sourceReadBytes, 0u,
            "B (cold FileCache) must have positive source read bytes");
        VELOX_CHECK_GT(
            d.cacheWriteBytes, 0u,
            "B (cold FileCache) must have positive cache write bytes");
        // Within-pass hits are allowed; eviction is not.  Foreground predownload
        // (predownloadForCurrentSegment) fills source gaps synchronously, so its
        // two counters must match (both zero or matched positive).
        VELOX_CHECK_EQ(
            d.predownloadedFromSourceBytes,
            d.predownloadedBytes,
            "B (cold FileCache) foreground predownload accounting must match");
        VELOX_CHECK_EQ(
            d.evictedBytes, 0u,
            "B (cold FileCache) must have zero evicted bytes");
        VELOX_CHECK_EQ(
            d.evictedSegments, 0u,
            "B (cold FileCache) must have zero evicted segments");
        break;
    case ReplayTimingCell::kWarmFileCache:
        VELOX_CHECK_GT(
            d.cacheHitCount, 0u,
            "C (warm FileCache) must have positive cache hits");
        VELOX_CHECK_GT(
            d.cacheReadBytes, 0u,
            "C (warm FileCache) must have positive cache read bytes");
        VELOX_CHECK_EQ(
            d.cacheMissCount, 0u,
            "C (warm FileCache) must have zero cache misses");
        VELOX_CHECK_EQ(
            d.sourceReadBytes, 0u,
            "C (warm FileCache) must have zero source read bytes");
        VELOX_CHECK_EQ(
            d.cacheWriteBytes, 0u,
            "C (warm FileCache) must have zero cache write bytes");
        VELOX_CHECK_EQ(
            d.predownloadedFromSourceBytes, 0u,
            "C (warm FileCache) must have zero predownloaded-from-source bytes");
        VELOX_CHECK_EQ(
            d.predownloadedBytes, 0u,
            "C (warm FileCache) must have zero predownloaded bytes");
        VELOX_CHECK_EQ(
            d.evictedBytes, 0u,
            "C (warm FileCache) must have zero evicted bytes");
        VELOX_CHECK_EQ(
            d.evictedSegments, 0u,
            "C (warm FileCache) must have zero evicted segments");
        VELOX_CHECK(
            r.warmHitPct == 100.0,
            "C (warm FileCache) must have 100% warm-hit percentage (got {})",
            r.warmHitPct);
        break;
    default:
        VELOX_FAIL(
            "Unknown timing cell {}", static_cast<int>(sample.slot.cell));
    }
}

ReplayTimingAnalysis analyzeReplayTiming(
    const std::vector<ReplayTimingSample>& samples)
{
    // Reject missing/duplicate/unexpected slots before computing anything: each
    // of the 15 expected slots must appear exactly once.
    const auto expected = q04ReplayTimingOrder();
    VELOX_CHECK_EQ(
        samples.size(), expected.size(),
        "Expected exactly {} timed samples, got {}",
        expected.size(), samples.size());
    for (const auto& es : expected)
    {
        size_t count = 0;
        for (const auto& s : samples)
        {
            if (replaySlotsEqual(s.slot, es))
            {
                ++count;
            }
        }
        VELOX_CHECK_EQ(
            count, 1u,
            "Slot (block={}, sample={}, cell={}) must appear exactly once "
            "(found {})",
            static_cast<int>(es.block), es.sample,
            static_cast<int>(es.cell), count);
    }

    // Partition wall times by cell and block.
    std::array<std::vector<uint64_t>, 3> forwardNs;
    std::array<std::vector<uint64_t>, 3> reverseNs;
    for (const auto& s : samples)
    {
        const size_t ci = static_cast<size_t>(s.slot.cell);
        VELOX_CHECK_LT(ci, 3u, "Cell index {} out of range", ci);
        if (s.slot.block == ReplayTimingBlock::kForward)
        {
            forwardNs[ci].push_back(s.result.wallNs);
        }
        else
        {
            reverseNs[ci].push_back(s.result.wallNs);
        }
    }

    ReplayTimingAnalysis analysis;
    std::array<uint64_t, 3> pooledMedian{};
    for (size_t ci = 0; ci < 3; ++ci)
    {
        VELOX_CHECK_EQ(
            forwardNs[ci].size(), 3u,
            "Cell {} must have exactly 3 forward samples", ci);
        VELOX_CHECK_EQ(
            reverseNs[ci].size(), 2u,
            "Cell {} must have exactly 2 reverse samples", ci);

        std::vector<uint64_t> pooled = forwardNs[ci];
        pooled.insert(pooled.end(), reverseNs[ci].begin(), reverseNs[ci].end());

        auto& cell = analysis.cells[ci];
        cell.forwardMedianNs = replayMedianNs(forwardNs[ci]);
        cell.reverseMedianNs = replayMedianNs(reverseNs[ci]);
        cell.pooledMedianNs = replayMedianNs(pooled);
        cell.minNs = *std::min_element(pooled.begin(), pooled.end());
        cell.maxNs = *std::max_element(pooled.begin(), pooled.end());
        cell.samples = static_cast<uint32_t>(pooled.size());
        pooledMedian[ci] = cell.pooledMedianNs;
    }

    // Block-difference sign gates (zero is its own sign).
    const int64_t fwdBMinusA = replayCheckedDeltaNs(
        analysis.cells[1].forwardMedianNs, analysis.cells[0].forwardMedianNs);
    const int64_t revBMinusA = replayCheckedDeltaNs(
        analysis.cells[1].reverseMedianNs, analysis.cells[0].reverseMedianNs);
    VELOX_CHECK_EQ(
        replaySign(fwdBMinusA), replaySign(revBMinusA),
        "B-A sign differs between forward ({}) and reverse ({}) blocks",
        fwdBMinusA, revBMinusA);

    const int64_t fwdCMinusB = replayCheckedDeltaNs(
        analysis.cells[2].forwardMedianNs, analysis.cells[1].forwardMedianNs);
    const int64_t revCMinusB = replayCheckedDeltaNs(
        analysis.cells[2].reverseMedianNs, analysis.cells[1].reverseMedianNs);
    VELOX_CHECK_EQ(
        replaySign(fwdCMinusB), replaySign(revCMinusB),
        "C-B sign differs between forward ({}) and reverse ({}) blocks",
        fwdCMinusB, revCMinusB);

    // Pooled-median decomposition.  Ratios are overhead ratios: the checked
    // signed difference divided by the baseline pooled median (delta/baseline),
    // e.g. bMinusARatio = (B-A)/A.  Zero denominators are rejected.
    analysis.bMinusANs = replayCheckedDeltaNs(pooledMedian[1], pooledMedian[0]);
    analysis.cMinusBNs = replayCheckedDeltaNs(pooledMedian[2], pooledMedian[1]);
    analysis.cMinusANs = replayCheckedDeltaNs(pooledMedian[2], pooledMedian[0]);

    VELOX_CHECK_GT(
        pooledMedian[0], 0u,
        "A pooled median is zero; cannot form B-A or C-A ratio");
    VELOX_CHECK_GT(
        pooledMedian[1], 0u, "B pooled median is zero; cannot form C-B ratio");

    analysis.bMinusARatio = static_cast<double>(analysis.bMinusANs) /
        static_cast<double>(pooledMedian[0]);
    analysis.cMinusBRatio = static_cast<double>(analysis.cMinusBNs) /
        static_cast<double>(pooledMedian[1]);
    analysis.cMinusARatio = static_cast<double>(analysis.cMinusANs) /
        static_cast<double>(pooledMedian[0]);

    return analysis;
}

// ---------------------------------------------------------------------------
// Task-018S: q04 single-thread timing pilot orchestration and serialization
// ---------------------------------------------------------------------------

namespace {

const char* replayCellName(ReplayTimingCell cell)
{
    switch (cell)
    {
    case ReplayTimingCell::kDirect:
        return "A";
    case ReplayTimingCell::kColdFileCache:
        return "B";
    case ReplayTimingCell::kWarmFileCache:
        return "C";
    }
    return "?";
}

const char* replayBlockName(ReplayTimingBlock block)
{
    return block == ReplayTimingBlock::kForward ? "forward" : "reverse";
}

/// Range-check an unsigned value before storing it in a folly::dynamic (which
/// holds integers as int64_t).  Rejects any value that would overflow int64.
int64_t replayCheckedJsonInt(uint64_t v)
{
    VELOX_CHECK_LE(
        v, static_cast<uint64_t>(INT64_MAX),
        "JSON integer value {} exceeds int64 range", v);
    return static_cast<int64_t>(v);
}

/// Gate a byte-verified untimed pass: it must consume exactly the accepted
/// logical bytes and verify every one against the oracle.
void gateVerifiedDirect(const BufferedInputTraceReplayResult& r, uint64_t L)
{
    VELOX_CHECK_EQ(
        r.totalLogicalBytes, L, "Verified A logical bytes {} != {}",
        r.totalLogicalBytes, L);
    VELOX_CHECK_EQ(
        r.oracleReadBytes, L,
        "Verified A must read every logical byte via the oracle");
    VELOX_CHECK_EQ(
        r.passthroughBytes, 0u, "Verified A must have zero passthrough bytes");
    VELOX_CHECK(
        allFileCacheCumulativeDeltasZero(r.cacheDelta),
        "Verified A must have every FileCache cumulative delta zero");
}

void gateVerifiedColdB(const BufferedInputTraceReplayResult& r, uint64_t L)
{
    VELOX_CHECK_EQ(
        r.totalLogicalBytes, L, "Verified cold B logical bytes {} != {}",
        r.totalLogicalBytes, L);
    VELOX_CHECK_EQ(
        r.oracleReadBytes, L,
        "Verified cold B must read every logical byte via the oracle");
    VELOX_CHECK_EQ(
        r.passthroughBytes, 0u, "Verified cold B must not run passthrough");
    const auto& d = r.cacheDelta;
    VELOX_CHECK_GT(d.cacheMissCount, 0u, "Verified cold B must have misses");
    VELOX_CHECK_GT(
        d.sourceReadBytes, 0u, "Verified cold B must read from source");
    VELOX_CHECK_GT(
        d.cacheWriteBytes, 0u, "Verified cold B must write to the cache");
    VELOX_CHECK_EQ(
        d.predownloadedFromSourceBytes,
        d.predownloadedBytes,
        "Verified cold B foreground predownload accounting must match");
    VELOX_CHECK_EQ(
        d.evictedBytes, 0u, "Verified cold B must have zero evicted bytes");
    VELOX_CHECK_EQ(
        d.evictedSegments, 0u, "Verified cold B must have zero evicted segments");
}

void gateVerifiedWarmC(const BufferedInputTraceReplayResult& r, uint64_t L)
{
    VELOX_CHECK_EQ(
        r.totalLogicalBytes, L, "Warm C logical bytes {} != {}",
        r.totalLogicalBytes, L);
    VELOX_CHECK_EQ(
        r.oracleReadBytes, L,
        "Warm C must read every logical byte via the oracle");
    VELOX_CHECK_EQ(
        r.passthroughBytes, 0u, "Warm C must not run the passthrough path");
    const auto& d = r.cacheDelta;
    VELOX_CHECK_GT(d.cacheHitCount, 0u, "Warm C must have positive cache hits");
    VELOX_CHECK_GT(
        d.cacheReadBytes, 0u, "Warm C must have positive cache read bytes");
    VELOX_CHECK_EQ(d.cacheMissCount, 0u, "Warm C must have zero cache misses");
    VELOX_CHECK_EQ(
        d.sourceReadBytes, 0u, "Warm C must have zero source read bytes");
    VELOX_CHECK_EQ(
        d.predownloadedFromSourceBytes, 0u,
        "Warm C must have zero predownloaded-from-source bytes");
    VELOX_CHECK_EQ(
        d.predownloadedBytes, 0u, "Warm C must have zero predownloaded bytes");
    VELOX_CHECK_EQ(d.evictedBytes, 0u, "Warm C must have zero evicted bytes");
    VELOX_CHECK_EQ(
        d.evictedSegments, 0u, "Warm C must have zero evicted segments");
    VELOX_CHECK(
        r.warmHitPct == 100.0,
        "Warm C must have 100% warm-hit percentage (got {})", r.warmHitPct);
}

/// Validate the seven cold/warm cache roots: nonempty, canonical, pairwise
/// distinct, strict children of one common cache parent, and each authenticated
/// by a top-level sentinel — all before any manager factory is invoked.  Roots
/// must be sentinel-authenticated in advance (the Controller/shell creates the
/// sentinels before the watchdog); the factory never creates sentinels.
void validateColdWarmRoots(const Q04ColdWarmTimingRoots& roots)
{
    std::vector<std::string> all;
    all.push_back(roots.verificationColdRoot);
    for (const auto& r : roots.timedColdRoots)
    {
        all.push_back(r);
    }
    all.push_back(roots.warmRoot);
    VELOX_CHECK_EQ(
        all.size(), 7u, "Expected 7 cold/warm roots, got {}", all.size());

    std::vector<std::string> canon;
    std::optional<std::string> commonParent;
    for (const auto& r : all)
    {
        VELOX_CHECK(!r.empty(), "Cache root must not be empty");
        std::error_code ec;
        const fs::path c = fs::canonical(r, ec);
        VELOX_CHECK(
            !ec, "Cannot canonicalize cache root '{}': {}", r, ec.message());
        const fs::path parent = c.parent_path();
        VELOX_CHECK(
            c != parent && !parent.empty(),
            "Cache root '{}' must be a strict child of a parent", c.string());
        if (!commonParent.has_value())
        {
            commonParent = parent.string();
        }
        else
        {
            VELOX_CHECK(
                *commonParent == parent.string(),
                "All cache roots must share one parent ('{}' vs '{}')",
                *commonParent, parent.string());
        }
        // Authenticate the sentinel before any manager is constructed.
        validateCacheSentinel(r);
        canon.push_back(c.string());
    }
    for (size_t i = 0; i < canon.size(); ++i)
    {
        for (size_t j = i + 1; j < canon.size(); ++j)
        {
            VELOX_CHECK(
                canon[i] != canon[j],
                "Cache roots must be pairwise distinct: '{}' reused",
                canon[i]);
        }
    }
}

/// Require a freshly-constructed manager to hold no resident cache data.
void requireEmptyManager(ch::FileCacheManager& mgr, const char* label)
{
    auto cache = mgr.getDefault();
    VELOX_CHECK_NOT_NULL(cache, "{} manager has no default cache", label);
    VELOX_CHECK_EQ(
        cache->getUsedCacheSize(), 0u,
        "{} manager must start empty (usedCacheSize={})",
        label, cache->getUsedCacheSize());
    VELOX_CHECK_EQ(
        cache->getFileSegmentsNum(), 0u,
        "{} manager must start empty (fileSegmentsNum={})",
        label, cache->getFileSegmentsNum());
}

} // namespace

ScopedFileCacheManagerExposure::ScopedFileCacheManagerExposure(
    ch::FileCacheManager& manager)
{
    VELOX_CHECK_NULL(
        ch::FileCacheManager::getInstance(),
        "A FileCacheManager is already installed; exposure must not nest");
    ch::FileCacheManager::setInstance(&manager);
}

ScopedFileCacheManagerExposure::~ScopedFileCacheManagerExposure()
{
    ch::FileCacheManager::setInstance(nullptr);
}

void runQ04SingleThreadTimingPilot(
    BufferedInputTraceReplayer& /*replayer*/,
    ch::FileCacheManager& /*manager*/,
    Q04ReplayTimingPilotResult& /*result*/)
{
    // Superseded: a single manager cannot produce a valid cold-B sample (each
    // cold sample needs a fresh empty FileCache).  Fail closed so no caller can
    // silently produce an invalid cold pilot.  Use runQ04ColdWarmTimingPilot.
    VELOX_FAIL(
        "runQ04SingleThreadTimingPilot is superseded and fail-closed; "
        "use runQ04ColdWarmTimingPilot with per-cell cold/warm roots");
}

void runQ04ColdWarmTimingPilot(
    BufferedInputTraceReplayer& replayer,
    const Q04ColdWarmTimingRoots& roots,
    const ReplayFileCacheManagerFactory& managerFactory,
    Q04ReplayTimingPilotResult& result)
{
    // The result builder must be fresh: reject stale/leftover samples.
    VELOX_CHECK(
        result.samples.empty(),
        "runQ04ColdWarmTimingPilot requires a fresh result builder "
        "(result.samples must be empty, found {})",
        result.samples.size());
    VELOX_CHECK_NULL(
        ch::FileCacheManager::getInstance(),
        "runQ04ColdWarmTimingPilot requires the manager hidden at entry");
    VELOX_CHECK(
        managerFactory != nullptr, "managerFactory must not be null");

    // Reject reused/invalid roots before any replay or manager construction.
    validateColdWarmRoots(roots);

    // Per-pass options.
    const BufferedInputTraceReplayOptions verifyDirectOpts{
        .verifyBytes = true, .measureEventLoop = false, .fileCacheRoot = ""};
    const auto verifyFileCacheOpts = [](const std::string& root)
    {
        return BufferedInputTraceReplayOptions{
            .verifyBytes = true, .measureEventLoop = false, .fileCacheRoot = root};
    };
    const auto timedFileCacheOpts = [](const std::string& root)
    {
        return BufferedInputTraceReplayOptions{
            .verifyBytes = false,
            .measureEventLoop = true,
            .fileCacheRoot = root};
    };
    const BufferedInputTraceReplayOptions timedDirectOpts{
        .verifyBytes = false, .measureEventLoop = true, .fileCacheRoot = ""};

    // --- Untimed verification (nothing appended to result.samples) ---

    // 1) Verified A (manager hidden).
    result.verifiedA =
        replayer.replay(BufferedInputReplayBackend::kDirect, verifyDirectOpts);
    const uint64_t L = result.verifiedA.totalLogicalBytes;
    VELOX_CHECK_GT(L, 0u, "Pilot trace has zero logical bytes");
    gateVerifiedDirect(result.verifiedA, L);

    // 2) Verification cold B on its own fresh root; hidden + shutdown after.
    {
        auto vMgr = managerFactory(roots.verificationColdRoot);
        VELOX_CHECK_NOT_NULL(
            vMgr, "managerFactory returned null for the verification cold root");
        SCOPE_EXIT { vMgr->shutdown(); };
        requireEmptyManager(*vMgr, "verification cold");
        {
            ScopedFileCacheManagerExposure exposure(*vMgr);
            result.verifiedB = replayer.replay(
                BufferedInputReplayBackend::kFileCache,
                verifyFileCacheOpts(roots.verificationColdRoot));
        }
        gateVerifiedColdB(result.verifiedB, L);
    }
    VELOX_CHECK_NULL(
        ch::FileCacheManager::getInstance(),
        "Manager must be hidden after verification cold B");

    // 3) One warm manager: constructed once, populated, verified, then retained
    // hidden across all samples; shut down after the timer via a function guard.
    auto warmMgr = managerFactory(roots.warmRoot);
    VELOX_CHECK_NOT_NULL(
        warmMgr, "managerFactory returned null for the warm root");
    SCOPE_EXIT
    {
        if (warmMgr)
        {
            warmMgr->shutdown();
            ch::FileCacheManager::setInstance(nullptr);
        }
    };
    requireEmptyManager(*warmMgr, "warm");
    {
        ScopedFileCacheManagerExposure exposure(*warmMgr);
        result.coldC = replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            verifyFileCacheOpts(roots.warmRoot));
        gateVerifiedColdB(result.coldC, L);

        result.verifiedWarmC = replayer.replay(
            BufferedInputReplayBackend::kFileCache,
            verifyFileCacheOpts(roots.warmRoot));
        gateVerifiedWarmC(result.verifiedWarmC, L);
    }
    VELOX_CHECK_NULL(
        ch::FileCacheManager::getInstance(),
        "Warm manager must be hidden after C verification");

    // --- Timed 3+2 loop (samples appended only after per-sample validation) ---
    size_t coldIdx = 0;
    for (const auto& slot : q04ReplayTimingOrder())
    {
        BufferedInputTraceReplayResult timed;
        switch (slot.cell)
        {
        case ReplayTimingCell::kDirect:
        {
            VELOX_CHECK_NULL(
                ch::FileCacheManager::getInstance(),
                "A (Direct) timed samples require the manager hidden");
            timed = replayer.replay(
                BufferedInputReplayBackend::kDirect, timedDirectOpts);
            break;
        }
        case ReplayTimingCell::kColdFileCache:
        {
            VELOX_CHECK_LT(
                coldIdx, roots.timedColdRoots.size(),
                "More cold B slots than timed cold roots");
            const std::string& coldRoot = roots.timedColdRoots[coldIdx];
            ++coldIdx;
            // Construct outside the timer.
            auto mgr = managerFactory(coldRoot);
            VELOX_CHECK_NOT_NULL(
                mgr, "managerFactory returned null for a timed cold root");
            SCOPE_EXIT { mgr->shutdown(); };  // shutdown outside the timer
            requireEmptyManager(*mgr, "timed cold");
            {
                ScopedFileCacheManagerExposure exposure(*mgr);
                timed = replayer.replay(
                    BufferedInputReplayBackend::kFileCache,
                    timedFileCacheOpts(coldRoot));
            }
            break;
        }
        case ReplayTimingCell::kWarmFileCache:
        {
            ScopedFileCacheManagerExposure exposure(*warmMgr);
            timed = replayer.replay(
                BufferedInputReplayBackend::kFileCache,
                timedFileCacheOpts(roots.warmRoot));
            break;
        }
        default:
            VELOX_FAIL("Unknown timing cell {}", static_cast<int>(slot.cell));
        }

        ReplayTimingSample sample;
        sample.slot = slot;
        sample.result = timed;
        validateTimedReplaySample(sample, L);
        result.samples.push_back(sample);
    }

    VELOX_CHECK_EQ(
        coldIdx, roots.timedColdRoots.size(),
        "Not every timed cold root was used ({} of {})",
        coldIdx, roots.timedColdRoots.size());
    VELOX_CHECK_EQ(
        result.samples.size(), q04ReplayTimingOrder().size(),
        "Pilot must collect exactly {} timed samples",
        q04ReplayTimingOrder().size());
    VELOX_CHECK_NULL(
        ch::FileCacheManager::getInstance(),
        "Manager must be hidden after the timed loop");

    // Analyze after all 15 validated samples (warm manager shutdown is deferred
    // to the function-scope guard, i.e. after the final timer stops).
    result.analysis = analyzeReplayTiming(result.samples);
}

folly::dynamic q04TimingPilotToJson(
    const BufferedInputTraceDocument& trace,
    const BufferedInputTraceReplayConfig& config,
    const Q04ReplayTimingPilotResult& result,
    const Q04ReplayTimingIdentity& replayIdentity)
{
    const auto u = [](uint64_t v) -> folly::dynamic
    {
        return folly::dynamic(replayCheckedJsonInt(v));
    };

    folly::dynamic identity = folly::dynamic::object;
    identity["trace_path"] = config.tracePath;
    identity["dataset_root"] = config.datasetRoot;
    // Distinguish the capture (TPCH) binary from the replay executable that
    // produced the measurements.
    identity["capture_binary_real_path"] = trace.manifest.binaryRealPath;
    identity["capture_binary_build_id"] = trace.manifest.binaryBuildId;
    identity["replay_binary_real_path"] = replayIdentity.replayBinaryRealPath;
    identity["replay_binary_build_id"] = replayIdentity.replayBinaryBuildId;
    identity["velox_head"] = trace.manifest.veloxHead;
    identity["gluten_head"] = trace.manifest.glutenHead;
    identity["clickhouse_head"] = trace.manifest.clickhouseHead;
    identity["schema_version"] =
        static_cast<int64_t>(trace.manifest.schemaVersion);
    identity["event_count"] = u(trace.manifest.eventCount);
    identity["file_count"] = u(trace.files.size());
    identity["logical_bytes"] = u(result.verifiedA.totalLogicalBytes);

    folly::dynamic samples = folly::dynamic::array;
    for (const auto& s : result.samples)
    {
        const auto& r = s.result;
        folly::dynamic o = folly::dynamic::object;
        o["block"] = replayBlockName(s.slot.block);
        o["sample"] = static_cast<int64_t>(s.slot.sample);
        o["cell"] = replayCellName(s.slot.cell);
        o["wall_ns"] = u(r.wallNs);
        o["logical_bytes"] = u(r.totalLogicalBytes);
        o["next_count"] = u(r.nextCount);
        o["seek_count"] = u(r.seekCount);
        o["passthrough_bytes"] = u(r.passthroughBytes);
        o["cache_hit_count"] = u(r.cacheDelta.cacheHitCount);
        o["cache_miss_count"] = u(r.cacheDelta.cacheMissCount);
        o["cache_read_bytes"] = u(r.cacheDelta.cacheReadBytes);
        o["cache_write_bytes"] = u(r.cacheDelta.cacheWriteBytes);
        o["source_read_bytes"] = u(r.cacheDelta.sourceReadBytes);
        o["oracle_read_bytes"] = u(r.oracleReadBytes);
        o["predownloaded_from_source_bytes"] =
            u(r.cacheDelta.predownloadedFromSourceBytes);
        o["predownloaded_bytes"] = u(r.cacheDelta.predownloadedBytes);
        o["evicted_bytes"] = u(r.cacheDelta.evictedBytes);
        o["evicted_segments"] = u(r.cacheDelta.evictedSegments);
        samples.push_back(o);
    }

    const ReplayTimingCell cells[3] = {
        ReplayTimingCell::kDirect,
        ReplayTimingCell::kColdFileCache,
        ReplayTimingCell::kWarmFileCache};
    folly::dynamic cellSummary = folly::dynamic::array;
    for (size_t ci = 0; ci < 3; ++ci)
    {
        const auto& c = result.analysis.cells[ci];
        folly::dynamic o = folly::dynamic::object;
        o["cell"] = replayCellName(cells[ci]);
        o["forward_median_ns"] = u(c.forwardMedianNs);
        o["reverse_median_ns"] = u(c.reverseMedianNs);
        o["pooled_median_ns"] = u(c.pooledMedianNs);
        o["min_ns"] = u(c.minNs);
        o["max_ns"] = u(c.maxNs);
        o["samples"] = static_cast<int64_t>(c.samples);
        cellSummary.push_back(o);
    }

    folly::dynamic decomposition = folly::dynamic::object;
    decomposition["b_minus_a_ns"] =
        static_cast<int64_t>(result.analysis.bMinusANs);
    decomposition["b_minus_a_ratio"] = result.analysis.bMinusARatio;
    decomposition["c_minus_b_ns"] =
        static_cast<int64_t>(result.analysis.cMinusBNs);
    decomposition["c_minus_b_ratio"] = result.analysis.cMinusBRatio;
    decomposition["c_minus_a_ns"] =
        static_cast<int64_t>(result.analysis.cMinusANs);
    decomposition["c_minus_a_ratio"] = result.analysis.cMinusARatio;

    folly::dynamic root = folly::dynamic::object;
    root["schema_version"] = 1;
    root["mode"] = "single_thread_timing_pilot";
    root["valid"] = true;
    root["identity"] = identity;
    root["samples"] = samples;
    root["cell_summary"] = cellSummary;
    root["decomposition"] = decomposition;
    root["decomposition_status"] = "measured";
    root["next_plan_selection"] = "user_review_required";
    return root;
}

folly::dynamic q04ColdWarmTimingToJson(
    const BufferedInputTraceDocument& trace,
    const BufferedInputTraceReplayConfig& config,
    const Q04ReplayTimingPilotResult& result,
    const Q04ReplayTimingIdentity& replayIdentity,
    const Q04ColdWarmTimingRoots& roots)
{
    folly::dynamic root =
        q04TimingPilotToJson(trace, config, result, replayIdentity);
    root["mode"] = "cold_filecache_timing_pilot";
    root["cell_definition"] = "direct_cold_filecache_warm_filecache_v1";

    // Record the seven canonical root identities inside identity.
    auto& identity = root["identity"];
    identity["verification_cold_cache_root"] = roots.verificationColdRoot;
    folly::dynamic timedCold = folly::dynamic::array;
    for (const auto& r : roots.timedColdRoots)
    {
        timedCold.push_back(r);
    }
    identity["timed_cold_cache_roots"] = timedCold;
    identity["warm_cache_root"] = roots.warmRoot;

    // Each cold-B sample row carries the cold root it ran against, in cold slot
    // order (matching runQ04ColdWarmTimingPilot's timed cold-root assignment).
    size_t coldIdx = 0;
    for (auto& sampleJson : root["samples"])
    {
        if (sampleJson["cell"].asString() == "B")
        {
            VELOX_CHECK_LT(
                coldIdx, roots.timedColdRoots.size(),
                "More cold-B sample rows than timed cold roots");
            sampleJson["cold_cache_root"] = roots.timedColdRoots[coldIdx];
            ++coldIdx;
        }
    }
    VELOX_CHECK_EQ(
        coldIdx, roots.timedColdRoots.size(),
        "Expected {} cold-B sample rows, found {}",
        roots.timedColdRoots.size(), coldIdx);
    return root;
}

std::array<std::string, 5> parseTimedColdCacheRoots(const std::string& csv)
{
    std::vector<std::string> parts;
    std::string cur;
    for (const char c : csv)
    {
        if (c == ',')
        {
            parts.push_back(cur);
            cur.clear();
        }
        else
        {
            cur.push_back(c);
        }
    }
    parts.push_back(cur);

    VELOX_CHECK_EQ(
        parts.size(), 5u,
        "--timed_cold_cache_roots must list exactly 5 roots, got {}",
        parts.size());
    std::array<std::string, 5> out;
    for (size_t i = 0; i < 5; ++i)
    {
        VELOX_CHECK(!parts[i].empty(), "Timed cold cache root {} is empty", i);
        out[i] = parts[i];
    }
    return out;
}

void requireFreshCacheRoot(const std::string& root)
{
    std::error_code ec;
    const fs::path canon = fs::canonical(root, ec);
    VELOX_CHECK(
        !ec, "Cannot canonicalize cache root '{}': {}", root, ec.message());
    VELOX_CHECK(
        fs::is_directory(canon, ec) && !ec,
        "Cache root '{}' is not a directory", canon.string());
    // Authenticate the sentinel (regular, non-symlink).
    validateCacheSentinel(root);
    // A truly fresh root holds exactly the sentinel: any other entry (including
    // a manager-written "status" file) proves prior use.
    for (const auto& entry : fs::directory_iterator(canon))
    {
        const std::string name = entry.path().filename().string();
        VELOX_CHECK(
            name == ".velox_benchmark_cache_sentinel",
            "Cache root '{}' must contain only the sentinel before use "
            "(found '{}')",
            canon.string(), name);
    }
}

Q04ColdWarmTimingRoots canonicalizeColdWarmRoots(
    const Q04ColdWarmTimingRoots& roots, const std::string& cacheParent)
{
    std::error_code ec;
    const fs::path parent = fs::canonical(cacheParent, ec);
    VELOX_CHECK(
        !ec, "Cannot canonicalize cache parent '{}': {}",
        cacheParent, ec.message());
    VELOX_CHECK(
        fs::is_directory(parent, ec) && !ec,
        "cache parent '{}' is not a directory", parent.string());

    std::set<std::string> seen;
    const auto canonOne = [&](const std::string& r) -> std::string
    {
        VELOX_CHECK(!r.empty(), "cold/warm cache root must not be empty");
        requireFreshCacheRoot(r); // fresh + sentinel
        const fs::path c = fs::canonical(r, ec);
        VELOX_CHECK(
            !ec, "Cannot canonicalize cache root '{}': {}", r, ec.message());
        VELOX_CHECK(
            c.parent_path() == parent,
            "cache root '{}' is not a strict child of parent '{}'",
            c.string(), parent.string());
        VELOX_CHECK(
            seen.insert(c.string()).second,
            "cold/warm cache roots must be distinct: '{}' repeated", c.string());
        return c.string();
    };

    Q04ColdWarmTimingRoots out;
    out.verificationColdRoot = canonOne(roots.verificationColdRoot);
    for (size_t i = 0; i < roots.timedColdRoots.size(); ++i)
    {
        out.timedColdRoots[i] = canonOne(roots.timedColdRoots[i]);
    }
    out.warmRoot = canonOne(roots.warmRoot);
    return out;
}

} // namespace facebook::velox::dwio::common
