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

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/dynamic.h>

#include "velox/ch/Common/FileCacheStats.h"
#include "velox/dwio/common/BufferedInputTrace.h"

namespace facebook::velox {
class ReadFile;
} // namespace facebook::velox

namespace facebook::velox::ch {
class FileCacheManager;
} // namespace facebook::velox::ch

namespace facebook::velox::memory {
class MemoryPool;
} // namespace facebook::velox::memory

namespace facebook::velox::dwio::common {

/// Which backend to use when replaying a trace.
enum class BufferedInputReplayBackend
{
    /// DirectBufferedInput — no FileCache, no passthrough layer.
    kDirect,
    /// FileCacheBufferedInput in ReadMode::kPassthrough — cache=nullptr.
    kPassthrough,
    /// FileCacheBufferedInput in ReadMode::kCache — real FileCache required.
    kFileCache,
};

/// Per-replay statistics returned by BufferedInputTraceReplayer::replay.
struct BufferedInputTraceReplayResult
{
    /// Sum of all consume.length values across all replay events.
    uint64_t totalLogicalBytes{0};
    /// fileCachePassthroughReadBytes from velox::IoStats (B backend only).
    uint64_t passthroughBytes{0};
    /// Total backend Next() calls during this replay.
    uint64_t nextCount{0};
    /// Total seekToPosition() calls during this replay.
    uint64_t seekCount{0};
    /// Sum of logical bytes verified against the source oracle.  Positive only
    /// when byte verification is enabled; zero for measured/unverified passes.
    uint64_t oracleReadBytes{0};
    /// Wall-clock nanoseconds spent in the timed event loop.  Zero unless
    /// BufferedInputTraceReplayOptions::measureEventLoop is set.
    uint64_t wallNs{0};
    /// FileCache stats delta (C backend only; zero for A and B).
    ch::FileCacheStatsSnapshot cacheDelta;
    /// Warm-hit percentage: 100.0 * cacheHitCount / (cacheHitCount + cacheMissCount).
    /// Valid only when (cacheHitCount + cacheMissCount) > 0; 0.0 otherwise.
    double warmHitPct{0.0};
};

/// Per-call options controlling one replay pass.  Independent of the immutable
/// BufferedInputTraceReplayConfig used to construct the replayer.
struct BufferedInputTraceReplayOptions
{
    /// When true, compare every consumed byte against the source oracle.
    bool verifyBytes{true};
    /// When true, measure the event loop with a steady clock and populate
    /// BufferedInputTraceReplayResult::wallNs.  Setup and verification are
    /// always excluded from the measured interval.
    bool measureEventLoop{false};
    /// Per-call FileCache root override for kFileCache replay.  When nonempty it
    /// supersedes config.cacheRoot for this call; when empty, config.cacheRoot is
    /// used.  Must be empty for Direct/Passthrough replay.
    std::string fileCacheRoot;
};

/// Timing primitives for measuring only the replay event loop.
using ReplayTimingAction = std::function<void()>;
using ReplayNowNanos = std::function<uint64_t()>;

/// Run `setup`, start the clock, run `eventLoop`, stop the clock, and return the
/// event-loop delta in the units produced by `nowNanos`.  `setup` is always
/// excluded from the measured interval.  Throws if the clock moves backwards.
///
/// This is the single production timing helper; a fake clock is injected only
/// through `nowNanos` in tests, never by bypassing this function.
uint64_t measureReplayEventLoop(
    const ReplayTimingAction& setup,
    const ReplayTimingAction& eventLoop,
    const ReplayNowNanos& nowNanos);

/// Configuration for one replay run.
struct BufferedInputTraceReplayConfig
{
    /// Path to the trace root directory (must contain manifest.json and
    /// events.jsonl written by a capture session).
    std::string tracePath;
    /// Canonical dataset root; must resolve to a directory and match
    /// manifest.datasetRoot after canonicalization.
    std::string datasetRoot;
    /// Directory to use as the FileCache root (kFileCache backend only).
    /// The replay creates this directory; it must not exist yet.
    std::string cacheRoot;
    /// Maximum FileCache disk capacity in bytes (kFileCache backend only).
    uint64_t cacheCapacityBytes{128ULL << 20};
    /// When true, compare every consumed byte against the source file (oracle).
    bool verifyBytes{true};
};

/// Deterministic, single-threaded replay of a BufferedInput trace.
///
/// Usage:
///   BufferedInputTraceReplayer replayer{config};
///   auto resA = replayer.replay(BufferedInputReplayBackend::kDirect);
///   auto resB = replayer.replay(BufferedInputReplayBackend::kPassthrough);
///   // install FileCacheManager before kFileCache
///   auto resC = replayer.replay(BufferedInputReplayBackend::kFileCache);
///
/// Each call to replay() is independent: fresh input/stream maps, fresh stats.
class BufferedInputTraceReplayer
{
public:
    /// Opens and fully validates the trace at config.tracePath against
    /// canonical(config.datasetRoot).  Throws on validation failure.
    explicit BufferedInputTraceReplayer(BufferedInputTraceReplayConfig config);

    ~BufferedInputTraceReplayer() = default;

    /// Execute all trace events using the selected backend.
    ///
    /// For kFileCache, the caller must have installed a FileCacheManager whose
    /// cache root equals config.cacheRoot before calling replay().
    ///
    /// Does NOT delete any files or cache paths.
    ///
    /// Preserves historical behavior: unmeasured (wallNs == 0) and using the
    /// config's verifyBytes default.
    BufferedInputTraceReplayResult replay(BufferedInputReplayBackend backend);

    /// Execute all trace events using the selected backend and per-call options.
    ///
    /// When options.measureEventLoop is set, only the event loop (input/stream
    /// create, operate, and close events) is timed; per-pass setup, oracle
    /// verification, cache snapshot subtraction, and result construction are
    /// excluded.  options.verifyBytes (not the config default) decides whether
    /// oracle preads and memcmp run.
    BufferedInputTraceReplayResult replay(
        BufferedInputReplayBackend backend,
        const BufferedInputTraceReplayOptions& options);

    const BufferedInputTraceDocument& trace() const
    {
        return trace_;
    }

private:
    BufferedInputTraceReplayConfig config_;
    std::string canonicalDatasetRoot_;
    BufferedInputTraceDocument trace_;
    std::shared_ptr<memory::MemoryPool> poolOwned_;
    memory::MemoryPool* pool_{nullptr};
    /// Invariant, opened once in the constructor after strict trace validation.
    /// sourceFiles_[i]/oracleFiles_[i] correspond to trace_.files[i].
    std::vector<std::shared_ptr<ReadFile>> sourceFiles_;
    std::vector<std::shared_ptr<ReadFile>> oracleFiles_;
    std::unordered_map<uint64_t, size_t> fileIdToIndex_;
};

/// Validate the cache sentinel file at cacheRoot + "/.velox_benchmark_cache_sentinel".
/// The sentinel must exist as a regular file via symlink_status() (symlinks are rejected).
/// Throws if missing, is a symlink, or is not a regular file.
void validateCacheSentinel(const std::string& cacheRoot);

// ---------------------------------------------------------------------------
// Task-018S: fixed 3+2 timing order and summary analysis
// ---------------------------------------------------------------------------

/// Which repetition block a timed sample belongs to.
enum class ReplayTimingBlock : uint8_t
{
    /// Three A,B,C passes in forward cell order.
    kForward,
    /// Two C,B,A passes in reverse cell order.
    kReverse,
};

/// Which timing cell a sample belongs to.  Distinct from
/// BufferedInputReplayBackend: both cold and warm FileCache map to the
/// kFileCache backend, and passthrough is not part of the timing matrix.
enum class ReplayTimingCell : uint8_t
{
    /// A: DirectBufferedInput over source Parquet files.
    kDirect,
    /// B: FileCacheBufferedInput populating an empty (cold) FileCache.
    kColdFileCache,
    /// C: FileCacheBufferedInput over a fully warmed FileCache.
    kWarmFileCache,
};

/// Identity of one timed replay slot: block, 1-based sample index, and cell.
struct ReplayTimingSlot
{
    ReplayTimingBlock block;
    uint32_t sample;
    ReplayTimingCell cell;
};

/// One collected timing sample: its slot identity and the replay result.
struct ReplayTimingSample
{
    ReplayTimingSlot slot;
    BufferedInputTraceReplayResult result;
};

/// Per-cell wall-time summary over its five samples (3 forward + 2 reverse).
struct ReplayTimingCellSummary
{
    uint64_t forwardMedianNs{0};
    uint64_t reverseMedianNs{0};
    uint64_t pooledMedianNs{0};
    uint64_t minNs{0};
    uint64_t maxNs{0};
    uint32_t samples{0};
};

/// Decomposition over the three timing cells (A=Direct, B=cold FileCache,
/// C=warm FileCache), indexed by ReplayTimingCell's underlying value.
/// Differences use pooled medians.  Ratios are overhead ratios: the signed
/// difference divided by the baseline pooled median (delta/baseline) —
/// bMinusARatio=(B-A)/A, cMinusBRatio=(C-B)/B, cMinusARatio=(C-A)/A.  No layer
/// classification or root-cause label is produced.
struct ReplayTimingAnalysis
{
    std::array<ReplayTimingCellSummary, 3> cells;
    int64_t bMinusANs{0};
    double bMinusARatio{0};
    int64_t cMinusBNs{0};
    double cMinusBRatio{0};
    int64_t cMinusANs{0};
    double cMinusARatio{0};
};

/// The exact, non-configurable 15-slot q04 replay order: three forward A,B,C
/// samples followed by two reverse C,B,A samples.
std::vector<ReplayTimingSlot> q04ReplayTimingOrder();

/// Validate one timed sample against the accepted trace logical-byte count and
/// the per-cell A/B/C path gates.  Throws on any violation.  Requires
/// wallNs>0, nextCount>0, oracleReadBytes==0, passthrough==0, and:
///   A (Direct):          every FileCache cumulative delta==0
///   B (cold FileCache):  miss/source/cacheWrite>0; both predownload and both
///                        eviction counters==0 (within-pass hits allowed)
///   C (warm FileCache):  hits/cacheRead>0; miss/source/cacheWrite==0; both
///                        predownload and both eviction==0; warmHitPct==100
void validateTimedReplaySample(
    const ReplayTimingSample& sample,
    uint64_t expectedLogicalBytes);

/// Compute per-cell summaries and the B-A/C-B/C-A decomposition.  Rejects
/// missing, duplicate, unexpected, or wrong-count slots (all 15 expected slots
/// must be present exactly once, three forward and two reverse per cell) before
/// computing.  Rejects a forward/reverse block sign mismatch for B-A or C-B
/// (zero is its own sign) and a zero ratio denominator.  Does not classify
/// layers or a root cause.
ReplayTimingAnalysis analyzeReplayTiming(
    const std::vector<ReplayTimingSample>& samples);

// ---------------------------------------------------------------------------
// Task-018S: q04 single-thread timing pilot orchestration and serialization
// ---------------------------------------------------------------------------

/// RAII guard that exposes a FileCacheManager as the process-global instance for
/// the duration of a scope and hides it (setInstance(nullptr)) on destruction.
///
/// It never constructs, destroys, initializes, or clears the manager or its
/// cache — the manager stays warm while hidden.  The constructor requires that
/// no manager is currently installed, so exposure cannot nest or leak.
class ScopedFileCacheManagerExposure
{
public:
    explicit ScopedFileCacheManagerExposure(ch::FileCacheManager& manager);
    ~ScopedFileCacheManagerExposure();

    ScopedFileCacheManagerExposure(
        const ScopedFileCacheManagerExposure&) = delete;
    ScopedFileCacheManagerExposure& operator=(
        const ScopedFileCacheManagerExposure&) = delete;
};

/// Full result of one q04 single-thread timing pilot.  Acts as an incremental
/// result builder: it is populated in order, and its `samples`/`analysis` are
/// only written after every verification pass has succeeded, so a verification
/// failure leaves `samples` empty.
struct Q04ReplayTimingPilotResult
{
    BufferedInputTraceReplayResult verifiedA;
    BufferedInputTraceReplayResult verifiedB;
    BufferedInputTraceReplayResult coldC;
    BufferedInputTraceReplayResult verifiedWarmC;
    std::vector<ReplayTimingSample> samples;
    ReplayTimingAnalysis analysis;
};

/// Run the fixed q04 single-thread 3+2 A/B/C timing pilot into `result`.
///
/// SUPERSEDED and fail-closed: this single-manager orchestrator cannot produce
/// valid cold-B samples (a fresh empty FileCache per cold sample is required).
/// It now always throws; use `runQ04ColdWarmTimingPilot` instead.  Retained only
/// as a linkable symbol for callers pending migration.
void runQ04SingleThreadTimingPilot(
    BufferedInputTraceReplayer& replayer,
    ch::FileCacheManager& manager,
    Q04ReplayTimingPilotResult& result);

/// The seven sentinel-authenticated cache roots for a cold/warm timing pilot:
/// one verification-cold root, exactly five timed-cold roots (one per B slot),
/// and one warm root.  All seven must be nonempty, canonical, pairwise distinct,
/// strict children of one common cache parent, and each already carry a
/// top-level sentinel before the pilot runs.
struct Q04ColdWarmTimingRoots
{
    std::string verificationColdRoot;
    std::array<std::string, 5> timedColdRoots;
    std::string warmRoot;
};

/// Factory that constructs (and initializes) a fresh FileCacheManager rooted at
/// `root`.  The orchestrator calls it outside every replay timer.  The root is
/// required to be already sentinel-authenticated (the default FileCache loader
/// only scans the data/system/general type subdirectories, so a pre-created
/// top-level sentinel is supported); the factory must NOT create or write a
/// sentinel, and must not expose the manager (setInstance) — the orchestrator
/// owns exposure/shutdown.
using ReplayFileCacheManagerFactory =
    std::function<std::shared_ptr<ch::FileCacheManager>(
        const std::string& root)>;

/// Run the q04 cold/warm timing pilot into `result` (a fresh builder).
///
/// Untimed verification (manager hidden except where noted): verified A; a fresh
/// verification-cold manager on `roots.verificationColdRoot` (required empty,
/// verified cold B, then hidden + shutdown); one warm manager on `roots.warmRoot`
/// (required empty, C cold population + verified warm C), then hidden but
/// retained.
///
/// Timed 3+2 loop over the exact 15 slots: A replays Direct while hidden; each B
/// slot constructs a fresh manager from its unique `roots.timedColdRoots[i]`
/// outside the timer (required empty), exposes it for the timed FileCache replay,
/// hides, validates, and shuts it down outside the timer, never reusing a root;
/// each C slot re-exposes the persistent warm manager for its timed replay.
///
/// Samples are appended only after per-sample validation, and only after every
/// verification pass has succeeded, so any failure leaves `result.samples` empty.
/// The analysis runs after all 15 samples.  All managers are shut down and the
/// instance left hidden, exception-safely, with no leaks.
void runQ04ColdWarmTimingPilot(
    BufferedInputTraceReplayer& replayer,
    const Q04ColdWarmTimingRoots& roots,
    const ReplayFileCacheManagerFactory& managerFactory,
    Q04ReplayTimingPilotResult& result);

/// Identity of the replay executable that produced the timing measurements.
/// Distinct from the capture (TPCH) binary recorded in the trace manifest.
struct Q04ReplayTimingIdentity
{
    /// Canonical realpath of the replay executable (from /proc/self/exe).
    std::string replayBinaryRealPath;
    /// Build ID of the replay executable (supplied out of band).
    std::string replayBinaryBuildId;
};

/// Serialize a completed pilot to a folly::dynamic object with the fixed
/// identity/raw-sample/summary/decomposition schema.  The identity records the
/// capture binary (from the trace manifest) and the replay binary
/// (`replayIdentity`) under distinct keys.  Ratios are decimal fractions
/// (overhead (B-A)/A, (C-B)/B, (C-A)/A).  Integer fields are range-checked
/// against int64 before conversion.  Emits no root_cause or layer
/// classification.
folly::dynamic q04TimingPilotToJson(
    const BufferedInputTraceDocument& trace,
    const BufferedInputTraceReplayConfig& config,
    const Q04ReplayTimingPilotResult& result,
    const Q04ReplayTimingIdentity& replayIdentity);

/// Serialize a cold/warm pilot: the base pilot JSON plus the cell-definition
/// version ("direct_cold_filecache_warm_filecache_v1") and the seven cache roots
/// mapped to their verification/timed/warm roles under "cache_roots".  Emits no
/// root_cause or layer classification.
folly::dynamic q04ColdWarmTimingToJson(
    const BufferedInputTraceDocument& trace,
    const BufferedInputTraceReplayConfig& config,
    const Q04ReplayTimingPilotResult& result,
    const Q04ReplayTimingIdentity& replayIdentity,
    const Q04ColdWarmTimingRoots& roots);

/// Parse exactly five comma-separated timed cold cache roots.  Throws if the
/// count is not five or any entry is empty.
std::array<std::string, 5> parseTimedColdCacheRoots(const std::string& csv);

/// Preflight a pre-created cache root: it must be an existing directory whose
/// only entry is the authentic sentinel (regular non-symlink) — i.e. fresh, with
/// no resident cache data and no manager-written "status" file (which would
/// prove prior use).  Creates and writes nothing.
void requireFreshCacheRoot(const std::string& root);

/// Validate the seven cold/warm roots (each fresh + sentinel-authenticated,
/// pairwise distinct, strict children of `cacheParent`) and return a copy with
/// every root replaced by its canonical path.  The orchestrator, manager
/// factory, and JSON must use the returned canonical roots so provenance never
/// records a symlinked or non-canonical spelling.  Throws on any violation.
Q04ColdWarmTimingRoots canonicalizeColdWarmRoots(
    const Q04ColdWarmTimingRoots& roots,
    const std::string& cacheParent);

} // namespace facebook::velox::dwio::common
