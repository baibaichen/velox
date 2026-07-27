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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::dwio::common {

/// Every logical operation the Parquet reader performs on a BufferedInput
/// or one of its streams.  These values are stored in trace files; do not
/// renumber them.
enum class BufferedInputTraceOp : uint8_t
{
    kInputCreate,
    kInputClone,
    kInputReset,
    kInputPreload,
    kInputSetNumStripes,
    kInputClose,
    kStreamEnqueue,
    kStreamRead,
    kLoad,
    kConsume,
    kSkip,
    kSeek,
    kStreamEof,
    kStreamClose,
};

/// One entry in the captured file table.  relativePath is dataset-relative
/// (no leading '/', no '..' components) and must not escape the dataset root.
struct BufferedInputTraceFile
{
    uint64_t fileId{0};
    std::string relativePath;
    uint64_t size{0};
    bool operator==(const BufferedInputTraceFile&) const = default;
};

/// One captured logical event.  Fields unused by a given op are zero.
struct BufferedInputTraceEvent
{
    uint64_t seq{0};
    uint64_t captureTid{0};
    uint32_t threadIndex{0};
    BufferedInputTraceOp op{BufferedInputTraceOp::kInputCreate};
    uint64_t fileId{0};
    uint64_t inputId{0};
    uint64_t parentInputId{0};
    uint64_t streamId{0};
    uint64_t offset{0};
    uint64_t length{0};
    int64_t argument{0};
    int64_t result{0};
    int32_t logType{0};
    bool operator==(const BufferedInputTraceEvent&) const = default;
};

/// Metadata stored in manifest.json.
struct BufferedInputTraceManifest
{
    uint32_t schemaVersion{1};
    int32_t queryId{0};
    int32_t drivers{1};
    std::string datasetRoot;
    std::string binaryRealPath;
    std::string binaryBuildId;
    std::string veloxHead;
    std::string glutenHead;
    std::string clickhouseHead;
    uint64_t eventCount{0};
    bool operator==(const BufferedInputTraceManifest&) const = default;
};

/// Complete in-memory representation of a captured trace.
struct BufferedInputTraceDocument
{
    BufferedInputTraceManifest manifest;
    std::vector<BufferedInputTraceFile> files;
    std::vector<BufferedInputTraceEvent> events;
    bool operator==(const BufferedInputTraceDocument&) const = default;
};

/// Wire form produced by serializeBufferedInputTrace.
/// manifest is a single JSON object; events is a JSONL string (one object per
/// line).
struct SerializedBufferedInputTrace
{
    std::string manifest;
    std::string events;
};

/// Serialize a trace document.  The caller is responsible for setting
/// manifest.eventCount before serializing if validation is intended.
SerializedBufferedInputTrace serializeBufferedInputTrace(
    const BufferedInputTraceDocument& trace);

/// Parse and return a trace document from its wire form.  Rejects unknown
/// or missing keys and numeric overflow; does not silently default fields.
BufferedInputTraceDocument parseBufferedInputTrace(
    std::string_view manifestJson,
    std::string_view eventsJsonl);

/// Read manifest.json and events.jsonl from traceRoot and call
/// parseBufferedInputTrace.  Fails closed if either file is missing.
BufferedInputTraceDocument loadBufferedInputTrace(
    const std::string& traceRoot);

/// Validate a parsed trace document.
///
/// Checks (in order):
///   1. schemaVersion == 1
///   2. All file relativePaths are strictly dataset-relative (no '/', no '..').
///   3. If expectedDatasetRoot is non-empty, manifest.datasetRoot must match.
///   4. Event seq values are strictly increasing.
///   5. Lifecycle state machine: IDs created before use, streams close before
///      their owner, no double-create/double-close, no use-after-close, no
///      unmatched owner.
///   6. No live input or stream remains after the final event.
///   7. manifest.eventCount == events.size().
///
/// Throws VeloxRuntimeError on the first violation.
void validateBufferedInputTrace(
    const BufferedInputTraceDocument& trace,
    const std::string& expectedDatasetRoot = "");

// ---------------------------------------------------------------------------
// Task-2: Capture session, decorators, and factory
// ---------------------------------------------------------------------------

/// Configuration for a process-scoped BufferedInput trace capture.
struct BufferedInputTraceCaptureConfig
{
    /// Directory to write manifest.json and events.jsonl.  Must not exist yet.
    std::string traceRoot;
    /// Canonical dataset root used to compute dataset-relative file paths.
    std::string datasetRoot;
    int32_t queryId{0};
    int32_t drivers{1};
    std::string binaryRealPath;
    std::string binaryBuildId;
    std::string veloxHead;
    std::string glutenHead;
    std::string clickhouseHead;
    /// Maximum buffered events before capture fails closed.
    uint64_t maxEvents{5'000'000};
};

/// Internal capture session — opaque to callers outside this translation unit.
class BufferedInputTraceSession;

/// RAII process-scoped exclusive capture guard.  Only one may be active at a
/// time.  Destroying the guard calls finish() non-throwingly; the caller
/// should call finish() explicitly to observe lifecycle errors.
class ScopedBufferedInputTraceCapture
{
public:
    explicit ScopedBufferedInputTraceCapture(
        BufferedInputTraceCaptureConfig config);

    ~ScopedBufferedInputTraceCapture();

    ScopedBufferedInputTraceCapture(
        const ScopedBufferedInputTraceCapture&) = delete;
    ScopedBufferedInputTraceCapture& operator=(
        const ScopedBufferedInputTraceCapture&) = delete;

    /// Validate the buffered event sequence and write manifest.json +
    /// events.jsonl to traceRoot.  Throws on any lifecycle, overflow, or
    /// I/O error.  Idempotent only after a successful completion.
    void finish();

private:
    std::shared_ptr<BufferedInputTraceSession> session_;
};

/// Returns true iff a capture guard is currently active.
bool bufferedInputTraceCaptureEnabled();

/// Returns a tracing wrapper around `input` when a capture is active,
/// otherwise returns `input` unchanged (exact same pointer/dynamic type).
/// `sourceFile` provides the file identity (path and size) for the trace.
std::unique_ptr<BufferedInput> maybeWrapBufferedInputForTrace(
    std::unique_ptr<BufferedInput> input,
    memory::MemoryPool& pool,
    const std::shared_ptr<velox::ReadFile>& sourceFile);

// ---------------------------------------------------------------------------
// TracingSeekableInputStream
// ---------------------------------------------------------------------------

/// Wraps a SeekableInputStream and records logical consume/skip/seek/EOF
/// events into a BufferedInputTraceSession.  Next/BackUp pairs are
/// normalized: a BackUp reduces the pending consumed length so that only
/// the bytes actually consumed by the caller appear as consume events.
class TracingSeekableInputStream final : public SeekableInputStream
{
public:
    TracingSeekableInputStream(
        std::unique_ptr<SeekableInputStream> inner,
        std::shared_ptr<BufferedInputTraceSession> session,
        uint64_t inputId,
        uint64_t streamId);

    ~TracingSeekableInputStream() override;

    bool Next(const void** data, int32_t* size) override;
    void BackUp(int32_t count) override;
    bool SkipInt64(int64_t count) override;
    int64_t ByteCount() const override;
    void seekToPosition(PositionProvider& position) override;
    std::string getName() const override;
    size_t positionSize() const override;

private:
    /// Emit a consume event for any pending bytes.  When `throwing` is false
    /// (destructor path) errors are stored in the session rather than thrown.
    void flushPendingConsume(bool throwing);

    std::unique_ptr<SeekableInputStream> inner_;
    std::shared_ptr<BufferedInputTraceSession> session_;
    uint64_t inputId_;
    uint64_t streamId_;

    bool pendingActive_{false};
    uint64_t pendingOffset_{0};  // logical position at start of pending chunk
    uint64_t pendingSize_{0};    // bytes still pending (decremented by BackUp)
    bool closed_{false};
};

// ---------------------------------------------------------------------------
// TracingBufferedInput
// ---------------------------------------------------------------------------

/// Wraps a BufferedInput and records input/stream lifecycle events into a
/// BufferedInputTraceSession.  Forwards all virtual behavior to the wrapped
/// inner input unchanged so tracing cannot affect planning or I/O decisions.
class TracingBufferedInput final : public BufferedInput
{
public:
    TracingBufferedInput(
        std::unique_ptr<BufferedInput> inner,
        memory::MemoryPool& pool,
        uint64_t fileId,
        std::shared_ptr<BufferedInputTraceSession> session);

    ~TracingBufferedInput() override;

    const std::string& getName() const override;

    std::unique_ptr<SeekableInputStream> enqueue(
        velox::common::Region region,
        const StreamIdentifier* sid = nullptr) override;

    void preload() override;
    bool preloaded() const override;

    void load(const LogType logType) override;

    bool isBuffered(uint64_t offset, uint64_t length) const override;

    std::unique_ptr<SeekableInputStream>
    read(uint64_t offset, uint64_t length, LogType logType) const override;

    bool shouldPreload(int32_t numPages = 0) override;
    bool shouldPrefetchStripes() const override;
    void setNumStripes(int32_t numStripes) override;

    std::unique_ptr<BufferedInput> clone() const override;

    folly::Executor* executor() const override;
    bool hasCache() const override;

    void cacheRegion(
        uint64_t offset,
        uint64_t length,
        std::string_view data) override;

    void cacheRegion(
        uint64_t offset,
        uint64_t length,
        const folly::IOBuf& buffer,
        uint64_t bufferOffset) override;

    std::optional<CachedRegion> findCachedRegion(uint64_t offset) const override;

    uint64_t nextFetchSize() const override;

    void reset() override;

private:
    // Used by clone(): inputId is pre-assigned; the caller already emitted
    // input_clone so this constructor must NOT emit input_create.
    TracingBufferedInput(
        std::unique_ptr<BufferedInput> inner,
        memory::MemoryPool& pool,
        uint64_t fileId,
        std::shared_ptr<BufferedInputTraceSession> session,
        uint64_t inputId);

    std::unique_ptr<BufferedInput> inner_;
    std::shared_ptr<BufferedInputTraceSession> session_;
    uint64_t fileId_;
    uint64_t inputId_;
};

} // namespace facebook::velox::dwio::common
