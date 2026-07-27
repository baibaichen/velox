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

#include "velox/dwio/common/BufferedInputTrace.h"

#include <filesystem>
#include <fstream>
#include <limits>
#include <thread>

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/testutil/TempDirectoryPath.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::memory;
namespace fvcommon = facebook::velox::common;

namespace
{

// ---------------------------------------------------------------------------
// Event builder helpers
// ---------------------------------------------------------------------------

static BufferedInputTraceEvent makeEvent(
    uint64_t seq,
    BufferedInputTraceOp op,
    uint64_t fileId = 0,
    uint64_t inputId = 0,
    uint64_t streamId = 0,
    uint64_t parentInputId = 0,
    uint64_t offset = 0,
    uint64_t length = 0)
{
    BufferedInputTraceEvent e;
    e.seq = seq;
    e.captureTid = 1001;
    e.threadIndex = 0;
    e.op = op;
    e.fileId = fileId;
    e.inputId = inputId;
    e.streamId = streamId;
    e.parentInputId = parentInputId;
    e.offset = offset;
    e.length = length;
    return e;
}

static BufferedInputTraceEvent inputCreate(
    uint64_t seq,
    uint64_t inputId,
    uint64_t fileId = 1)
{
    return makeEvent(seq, BufferedInputTraceOp::kInputCreate, fileId, inputId);
}

static BufferedInputTraceEvent inputClose(uint64_t seq, uint64_t inputId)
{
    return makeEvent(seq, BufferedInputTraceOp::kInputClose, 0, inputId);
}

static BufferedInputTraceEvent inputReset(uint64_t seq, uint64_t inputId)
{
    return makeEvent(seq, BufferedInputTraceOp::kInputReset, 0, inputId);
}

static BufferedInputTraceEvent streamRead(
    uint64_t seq,
    uint64_t inputId,
    uint64_t streamId,
    uint64_t offset,
    uint64_t length)
{
    return makeEvent(
        seq,
        BufferedInputTraceOp::kStreamRead,
        0,
        inputId,
        streamId,
        0,
        offset,
        length);
}

static BufferedInputTraceEvent streamEnqueue(
    uint64_t seq,
    uint64_t inputId,
    uint64_t streamId,
    uint64_t offset = 0,
    uint64_t length = 0)
{
    return makeEvent(
        seq,
        BufferedInputTraceOp::kStreamEnqueue,
        0,
        inputId,
        streamId,
        0,
        offset,
        length);
}

static BufferedInputTraceEvent streamClose(
    uint64_t seq,
    uint64_t streamId,
    uint64_t inputId = 0)
{
    return makeEvent(
        seq, BufferedInputTraceOp::kStreamClose, 0, inputId, streamId);
}

// ---------------------------------------------------------------------------
// Minimal valid trace builder
// ---------------------------------------------------------------------------

static BufferedInputTraceDocument makeMinimalTrace()
{
    BufferedInputTraceDocument doc;
    doc.manifest.schemaVersion = 1;
    doc.manifest.queryId = 4;
    doc.manifest.drivers = 1;
    doc.manifest.datasetRoot = "/data/tpch";
    doc.manifest.eventCount = 0;
    doc.files = {{1, "orders/part-0.parquet", 4096}};
    return doc;
}

} // namespace

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST(BufferedInputTraceSchemaTest, RoundTripsManifestEventsAndThreadIdentity)
{
    BufferedInputTraceDocument trace;
    trace.manifest.schemaVersion = 1;
    trace.manifest.queryId = 4;
    trace.manifest.datasetRoot = "/data/tpch";
    trace.files.push_back({1, "orders/part-0.parquet", 4096});
    trace.events = {
        {.seq = 1,
         .captureTid = 1001,
         .threadIndex = 0,
         .op = BufferedInputTraceOp::kInputCreate,
         .fileId = 1,
         .inputId = 1},
        {.seq = 2,
         .captureTid = 1001,
         .threadIndex = 0,
         .op = BufferedInputTraceOp::kInputClose,
         .fileId = 1,
         .inputId = 1}};

    const auto text = serializeBufferedInputTrace(trace);
    const auto parsed = parseBufferedInputTrace(text.manifest, text.events);
    EXPECT_EQ(parsed, trace);
}

TEST(BufferedInputTraceSchemaTest, RejectsStreamOutlivingInput)
{
    auto trace = makeMinimalTrace();
    trace.events = {
        inputCreate(1, 1),
        streamRead(2, 1, 1, 0, 32),
        inputClose(3, 1),
        streamClose(4, 1)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace),
        "stream_id=1 is still live when input_id=1 closes");
}

TEST(BufferedInputTraceSchemaTest, RejectsUseAfterCloseAndNonMonotonicSeq)
{
    auto trace = makeMinimalTrace();
    trace.events = {
        inputCreate(1, 1),
        inputClose(3, 1),
        inputReset(2, 1)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "event seq must be strictly increasing");
}

TEST(BufferedInputTraceSchemaTest, RejectsDuplicateInputId)
{
    auto trace = makeMinimalTrace();
    trace.events = {
        inputCreate(1, 1),
        inputCreate(2, 1),
        inputClose(3, 1)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "input_id=1 is already live");
}

TEST(BufferedInputTraceSchemaTest, RejectsUnknownFileId)
{
    auto trace = makeMinimalTrace();
    // fileId=99 is not in the files table (only fileId=1 exists).
    trace.events = {inputCreate(1, 1, 99)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "file_id=99 not found");
}

TEST(BufferedInputTraceSchemaTest, RejectsDoubleClose)
{
    auto trace = makeMinimalTrace();
    trace.events = {
        inputCreate(1, 1),
        inputClose(2, 1),
        inputClose(3, 1)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "input_id=1 is not live");
}

TEST(BufferedInputTraceSchemaTest, RejectsMissingTerminalClose)
{
    auto trace = makeMinimalTrace();
    trace.events = {inputCreate(1, 1)};
    trace.manifest.eventCount = 1;
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "live inputs remain after final event");
}

TEST(BufferedInputTraceSchemaTest, RejectsStreamOwnerMismatch)
{
    auto trace = makeMinimalTrace();
    trace.files.push_back({2, "lineitem/part-0.parquet", 8192});
    trace.events = {
        inputCreate(1, 1, 1),
        inputCreate(2, 2, 2),
        streamEnqueue(3, 1, 5, 0, 64), // stream 5 owned by input 1
        streamClose(4, 5, 2),           // claimed by input 2 — wrong owner
        inputClose(5, 1),
        inputClose(6, 2)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "owner mismatch");
}

TEST(BufferedInputTraceSchemaTest, RejectsPathEscape)
{
    auto trace = makeMinimalTrace();
    trace.files[0].relativePath = "../../etc/passwd";
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "path escapes dataset root");
}

TEST(BufferedInputTraceSchemaTest, RejectsUnsupportedSchemaVersion)
{
    auto trace = makeMinimalTrace();
    trace.manifest.schemaVersion = 99;
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "unsupported schema version");
}

// ===========================================================================
// Review-fix tests (Findings 1–4)
// ===========================================================================

// ---------------------------------------------------------------------------
// Finding 1: Canonical containment under the dataset root
// ---------------------------------------------------------------------------

TEST(BufferedInputTraceSchemaTest, RejectsSymlinkOutsideRoot)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    // Two independent RAII temp directories: the dataset root and an outside
    // directory.  Each gets a unique name from mkdtemp so parallel test
    // processes cannot collide.
    auto rootDir = TempDirectoryPath::create();
    auto outsideDir = TempDirectoryPath::create();

    // A real file outside the dataset root.
    const fs::path outsideFile = fs::path(outsideDir->getPath()) / "secret.parquet";
    {
        std::ofstream f(outsideFile);
        f << "dummy";
    }
    // A symlink inside the root that resolves to the outside file.
    const fs::path symlinkPath =
        fs::path(rootDir->getPath()) / "symlink.parquet";
    fs::create_symlink(outsideFile, symlinkPath);

    auto trace = makeMinimalTrace();
    trace.manifest.datasetRoot = rootDir->getPath();
    trace.files[0].relativePath = "symlink.parquet";

    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace, rootDir->getPath()),
        "is not a strict descendant");
    // RAII dirs clean up automatically.
}

TEST(BufferedInputTraceSchemaTest, AcceptsRealFileInsideRoot)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto rootDir = TempDirectoryPath::create();
    const fs::path subdir = fs::path(rootDir->getPath()) / "orders";
    fs::create_directories(subdir);
    const fs::path dataFile = subdir / "part-0.parquet";
    {
        std::ofstream f(dataFile);
        f << "dummy";
    }

    auto trace = makeMinimalTrace();
    trace.manifest.datasetRoot = rootDir->getPath();
    trace.files[0].relativePath = "orders/part-0.parquet";
    // No events: eventCount=0 == events.size()=0.

    EXPECT_NO_THROW(validateBufferedInputTrace(trace, rootDir->getPath()));
    // RAII dir cleans up automatically.
}

// ---------------------------------------------------------------------------
// Finding 2: Serialization must reject uint64_t values above INT64_MAX
// ---------------------------------------------------------------------------

TEST(BufferedInputTraceSchemaTest, RejectsUint64OverflowInSerialization)
{
    auto trace = makeMinimalTrace();
    // UINT64_MAX cannot be losslessly encoded as a JSON signed integer.
    trace.files[0].size = std::numeric_limits<uint64_t>::max();
    VELOX_ASSERT_THROW(serializeBufferedInputTrace(trace), "overflows int64");
}

// ---------------------------------------------------------------------------
// Finding 3: Strict JSON parser — coverage tests
// ---------------------------------------------------------------------------

TEST(BufferedInputTraceSchemaTest, RejectsUnknownKeyInManifest)
{
    const std::string manifestJson =
        R"({"schema_version":1,"query_id":4,"drivers":1,)"
        R"("dataset_root":"/data","binary_real_path":"","binary_build_id":"",)"
        R"("velox_head":"","gluten_head":"","clickhouse_head":"",)"
        R"("event_count":0,"files":[],"undocumented_field":42})";
    VELOX_ASSERT_THROW(parseBufferedInputTrace(manifestJson, ""), "unknown key");
}

TEST(BufferedInputTraceSchemaTest, RejectsMissingKeyInManifest)
{
    // "query_id" is intentionally absent.
    const std::string manifestJson =
        R"({"schema_version":1,"drivers":1,)"
        R"("dataset_root":"/data","binary_real_path":"","binary_build_id":"",)"
        R"("velox_head":"","gluten_head":"","clickhouse_head":"",)"
        R"("event_count":0,"files":[]})";
    VELOX_ASSERT_THROW(
        parseBufferedInputTrace(manifestJson, ""), "missing required key");
}

TEST(BufferedInputTraceSchemaTest, RejectsNegativeToUnsignedFileId)
{
    // file_id is uint64_t; a negative JSON integer must be rejected at parse.
    const std::string manifestJson =
        R"({"schema_version":1,"query_id":4,"drivers":1,)"
        R"("dataset_root":"/data","binary_real_path":"","binary_build_id":"",)"
        R"("velox_head":"","gluten_head":"","clickhouse_head":"",)"
        R"("event_count":0,)"
        R"("files":[{"file_id":-1,"relative_path":"a.parquet","size":100}]})";
    VELOX_ASSERT_THROW(
        parseBufferedInputTrace(manifestJson, ""), "non-negative");
}

TEST(BufferedInputTraceSchemaTest, RejectsOverflowThreadIndex)
{
    // thread_index is uint32_t; 2^32 does not fit and must be rejected.
    auto trace = makeMinimalTrace();
    const auto text = serializeBufferedInputTrace(trace);
    // Event line with thread_index = UINT32_MAX + 1 = 4294967296.
    const std::string bigEvent =
        R"({"seq":1,"capture_tid":1001,"thread_index":4294967296,)"
        R"("op":"input_create","file_id":1,"input_id":1,)"
        R"("parent_input_id":0,"stream_id":0,"offset":0,)"
        R"("length":0,"argument":0,"result":0,"log_type":0})";
    VELOX_ASSERT_THROW(
        parseBufferedInputTrace(text.manifest, bigEvent + "\n"),
        "overflows target type");
}

// ---------------------------------------------------------------------------
// Finding 4: Zero / duplicate ID rejection
// ---------------------------------------------------------------------------

TEST(BufferedInputTraceSchemaTest, RejectsZeroFileId)
{
    auto trace = makeMinimalTrace();
    trace.files[0].fileId = 0;
    VELOX_ASSERT_THROW(validateBufferedInputTrace(trace), "file_id=0 is reserved");
}

TEST(BufferedInputTraceSchemaTest, RejectsDuplicateFileId)
{
    auto trace = makeMinimalTrace();
    // Second file entry shares fileId=1 with the first.
    trace.files.push_back({1, "lineitem/part-0.parquet", 8192});
    VELOX_ASSERT_THROW(validateBufferedInputTrace(trace), "duplicate file_id");
}

TEST(BufferedInputTraceSchemaTest, RejectsZeroInputIdOnCreate)
{
    auto trace = makeMinimalTrace();
    // inputId=0 is reserved and must be rejected on input_create.
    trace.events = {inputCreate(1, 0)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "input_id=0 is reserved");
}

TEST(BufferedInputTraceSchemaTest, RejectsZeroStreamIdOnCreate)
{
    auto trace = makeMinimalTrace();
    // streamId=0 is reserved and must be rejected on stream_enqueue.
    trace.events = {
        inputCreate(1, 1),
        makeEvent(2, BufferedInputTraceOp::kStreamEnqueue, 0, 1, 0),
        inputClose(3, 1)};
    VELOX_ASSERT_THROW(
        validateBufferedInputTrace(trace), "stream_id=0 is reserved");
}

// ---------------------------------------------------------------------------
// Task-2: Recorder/decorator tests
// ---------------------------------------------------------------------------

class BufferedInputTraceRecorderTest : public testing::Test
{
protected:
    static void SetUpTestCase()
    {
        MemoryManager::testingSetInstance({});
    }

    MemoryPool& pool()
    {
        return *pool_;
    }

private:
    std::shared_ptr<MemoryPool> pool_ =
        memoryManager()->addLeafPool("BufferedInputTraceRecorderTest");
};

// Helper: write a small real file and return a LocalReadFile for it.
static std::shared_ptr<facebook::velox::LocalReadFile> makeRealFile(
    const std::string& path,
    size_t size = 64)
{
    namespace fs = std::filesystem;
    fs::create_directories(fs::path(path).parent_path());
    {
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < size; ++i)
        {
            f << static_cast<char>('A' + (i % 26));
        }
    }
    return std::make_shared<facebook::velox::LocalReadFile>(path);
}

TEST_F(
    BufferedInputTraceRecorderTest,
    RecordsIdsThreadsAndCompleteLifetimes)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();

    auto readFile = makeRealFile(filePath);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    // Two threads, each creates one TracingBufferedInput.
    // Main thread acts as "thread 0" (creates first), helper as "thread 1".
    folly::Baton<> t2StartBaton;
    folly::Baton<> t2DoneBaton;

    std::thread helperThread([&]()
    {
        t2StartBaton.wait(); // wait until main thread recorded its input_create
        auto innerB =
            std::make_unique<BufferedInput>(readFile, pool());
        auto tracedB = maybeWrapBufferedInputForTrace(
            std::move(innerB), pool(), readFile);
        tracedB.reset(); // records input_close
        t2DoneBaton.post();
    });

    {
        auto innerA = std::make_unique<BufferedInput>(readFile, pool());
        auto tracedA =
            maybeWrapBufferedInputForTrace(std::move(innerA), pool(), readFile);
        t2StartBaton.post(); // signal helper after input_create is recorded
        t2DoneBaton.wait();
        tracedA.reset(); // records input_close
    }

    helperThread.join();
    capture.finish();

    auto trace = loadBufferedInputTrace(traceRoot);
    ASSERT_EQ(trace.files.size(), 1u);
    EXPECT_EQ(trace.files[0].fileId, 1u);

    std::vector<BufferedInputTraceEvent> creates;
    for (const auto& ev : trace.events)
    {
        if (ev.op == BufferedInputTraceOp::kInputCreate)
        {
            creates.push_back(ev);
        }
    }
    ASSERT_EQ(creates.size(), 2u);
    EXPECT_NE(creates[0].inputId, creates[1].inputId);
    EXPECT_EQ(creates[0].fileId, creates[1].fileId);
    // Thread identity (from brief):
    EXPECT_NE(creates[0].captureTid, creates[1].captureTid);
    EXPECT_NE(creates[0].threadIndex, creates[1].threadIndex);
    EXPECT_EQ(creates[0].threadIndex, 0u);
    EXPECT_EQ(creates[1].threadIndex, 1u);
}

TEST_F(
    BufferedInputTraceRecorderTest,
    NormalizesNextAndBackupToConsumedBytes)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    // Exactly 32 bytes so Next() returns the full region in one call.
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/32);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    {
        auto inner = std::make_unique<BufferedInput>(readFile, pool());
        auto traced =
            maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

        auto stream = traced->enqueue(fvcommon::Region{0, 32});
        traced->load(LogType::TEST);

        // First Next: backing stream returns all 32 bytes.
        const void* data = nullptr;
        int32_t size = 0;
        ASSERT_TRUE(stream->Next(&data, &size));
        ASSERT_EQ(size, 32);

        // BackUp 12 bytes: only 20 were actually consumed.
        stream->BackUp(12);

        // Next call triggers flush: emits consume(offset=0, length=20).
        ASSERT_TRUE(stream->Next(&data, &size));

        stream.reset(); // flush remaining pending consume + stream_close
        traced.reset(); // input_close
    }

    capture.finish();

    auto trace = loadBufferedInputTrace(traceRoot);
    std::vector<BufferedInputTraceEvent> consumes;
    for (const auto& ev : trace.events)
    {
        if (ev.op == BufferedInputTraceOp::kConsume)
        {
            consumes.push_back(ev);
        }
    }
    ASSERT_GE(consumes.size(), 1u);
    // First consume must be offset=0, length=20 (not the raw 32).
    EXPECT_EQ(consumes[0].offset, 0u);
    EXPECT_EQ(consumes[0].length, 20u);
    // Backend raw chunk size (32) must never appear as a consume length.
    for (const auto& c : consumes)
    {
        EXPECT_NE(c.length, 32u)
            << "raw backend chunk size appeared as consume length";
    }
}

TEST_F(
    BufferedInputTraceRecorderTest,
    RecordsSkipSeekEofCloneAndReset)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/128);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    {
        auto inner = std::make_unique<BufferedInput>(readFile, pool());
        auto traced =
            maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

        // --- clone ---
        auto cloned = traced->clone();

        // --- reset ---
        traced->reset();

        // --- skip + EOF on a 32-byte enqueue ---
        auto stream = traced->enqueue(fvcommon::Region{0, 32});
        traced->load(LogType::TEST);

        // Skip 10 bytes.
        EXPECT_TRUE(stream->SkipInt64(10));

        // Read remaining 22 bytes; then Next returns EOF.
        const void* data = nullptr;
        int32_t sz = 0;
        // Consume remaining data to reach EOF.
        while (stream->Next(&data, &sz))
        {
        }
        // The last Next returned false → stream_eof recorded.

        // --- seek on a fresh stream ---
        auto stream2 = traced->enqueue(fvcommon::Region{32, 32});
        traced->load(LogType::TEST);
        const void* d2 = nullptr;
        int32_t s2 = 0;
        ASSERT_TRUE(stream2->Next(&d2, &s2)); // position the stream first
        stream2->BackUp(s2); // back to start
        PositionProvider pos(std::vector<uint64_t>{0});
        stream2->seekToPosition(pos);

        stream2.reset();
        stream.reset();
        cloned.reset();
        traced.reset();
    }

    capture.finish();

    auto trace = loadBufferedInputTrace(traceRoot);
    // Verify key event types were recorded.
    auto hasOp = [&](BufferedInputTraceOp op)
    {
        for (const auto& ev : trace.events)
        {
            if (ev.op == op)
            {
                return true;
            }
        }
        return false;
    };

    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kInputClone));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kInputReset));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kSkip));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kStreamEof));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kSeek));
}

TEST_F(
    BufferedInputTraceRecorderTest,
    DefaultOffReturnsOriginalInput)
{
    // With no capture active, maybeWrapBufferedInputForTrace returns the
    // original pointer unchanged.
    EXPECT_FALSE(bufferedInputTraceCaptureEnabled());

    std::string dummy(64, 'Z');
    auto readFile = std::make_shared<facebook::velox::InMemoryReadFile>(dummy);
    auto original = std::make_unique<BufferedInput>(readFile, pool());
    BufferedInput* origPtr = original.get();

    auto result =
        maybeWrapBufferedInputForTrace(std::move(original), pool(), readFile);
    EXPECT_EQ(result.get(), origPtr);
}

TEST_F(
    BufferedInputTraceRecorderTest,
    RejectsSecondActiveCapture)
{
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto dir1 = TempDirectoryPath::create();
    auto dir2 = TempDirectoryPath::create();

    BufferedInputTraceCaptureConfig cfg1;
    cfg1.traceRoot =
        (std::filesystem::path(dir1->getPath()) / "t1").string();
    cfg1.datasetRoot = dir1->getPath();

    ScopedBufferedInputTraceCapture capture1(cfg1);

    BufferedInputTraceCaptureConfig cfg2;
    cfg2.traceRoot =
        (std::filesystem::path(dir2->getPath()) / "t2").string();
    cfg2.datasetRoot = dir2->getPath();

    VELOX_ASSERT_THROW(
        ScopedBufferedInputTraceCapture(cfg2),
        "another");
}

TEST_F(
    BufferedInputTraceRecorderTest,
    RejectsStreamOutlivingOwnerAtCapture)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/64);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    // Get a stream but destroy the owning input first.
    std::unique_ptr<SeekableInputStream> stream;
    {
        auto inner = std::make_unique<BufferedInput>(readFile, pool());
        auto traced =
            maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);
        stream = traced->enqueue(fvcommon::Region{0, 64});
        traced->load(LogType::TEST);
        // traced destroyed here; stream still alive → lifecycle violation.
    }

    // finish() must detect the stream-outlives-owner violation.
    VELOX_ASSERT_THROW(capture.finish(), "stream");

    stream.reset(); // clean up the dangling stream
}

// ---------------------------------------------------------------------------
// Task-2 review-fix tests (Finding 1-7)
// ---------------------------------------------------------------------------

// Finding 2: two different LocalReadFile handles for the same underlying file
// must map to a single file_id (canonical-path key).  Before fix the pointer
// key assigns two different IDs → trace.files.size() == 2, test fails.
TEST_F(
    BufferedInputTraceRecorderTest,
    CanonicalFileIdentityDeduplicatesHandles)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();

    auto readFile1 = makeRealFile(filePath);
    // Second handle: different shared_ptr, same canonical path.
    auto readFile2 =
        std::make_shared<facebook::velox::LocalReadFile>(filePath);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    {
        auto inner1 = std::make_unique<BufferedInput>(readFile1, pool());
        auto traced1 = maybeWrapBufferedInputForTrace(
            std::move(inner1), pool(), readFile1);
        traced1.reset();

        auto inner2 = std::make_unique<BufferedInput>(readFile2, pool());
        auto traced2 = maybeWrapBufferedInputForTrace(
            std::move(inner2), pool(), readFile2);
        traced2.reset();
    }

    capture.finish();

    const auto trace = loadBufferedInputTrace(traceRoot);
    // Same canonical path → exactly one file entry.
    ASSERT_EQ(trace.files.size(), 1u);
    EXPECT_EQ(trace.files[0].fileId, 1u);

    // Both input_create events must reference file_id=1.
    std::vector<uint64_t> createFileIds;
    for (const auto& ev : trace.events)
    {
        if (ev.op == BufferedInputTraceOp::kInputCreate)
        {
            createFileIds.push_back(ev.fileId);
        }
    }
    ASSERT_EQ(createFileIds.size(), 2u);
    EXPECT_EQ(createFileIds[0], 1u);
    EXPECT_EQ(createFileIds[1], 1u);
}

// Finding 2: a source file whose canonical path is outside datasetRoot must
// be rejected at wrap time.  Before fix the fallback uses the raw name → no
// throw → test fails.
TEST_F(
    BufferedInputTraceRecorderTest,
    RejectsSourceOutsideDatasetRootAtWrap)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto datasetDir = TempDirectoryPath::create();
    auto outsideDir = TempDirectoryPath::create();

    const std::string outsidePath =
        (fs::path(outsideDir->getPath()) / "outsider.parquet").string();
    auto outsideFile = makeRealFile(outsidePath);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot =
        (fs::path(datasetDir->getPath()) / "trace").string();
    config.datasetRoot = datasetDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    auto inner = std::make_unique<BufferedInput>(outsideFile, pool());
    VELOX_ASSERT_THROW(
        maybeWrapBufferedInputForTrace(
            std::move(inner), pool(), outsideFile),
        "not inside datasetRoot");
}

// Finding 4: after a successful explicit finish() the global capture must be
// deactivated so bufferedInputTraceCaptureEnabled() returns false immediately.
// Before fix finish() does not clear the global → still returns true → fails.
TEST_F(
    BufferedInputTraceRecorderTest,
    SuccessfulFinishDeactivatesCapture)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    BufferedInputTraceCaptureConfig config;
    config.traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);
    EXPECT_TRUE(bufferedInputTraceCaptureEnabled());

    capture.finish();

    // Must be false immediately after successful finish.
    EXPECT_FALSE(bufferedInputTraceCaptureEnabled());
}

// Finding 1: a wrapper must survive guard destruction without dangling pointer.
// With shared_ptr (fix) the session refcount stays > 0 via the wrapper's own
// reference; calling load() must not crash or UB.
// Before fix: raw pointer → dangling → undefined behaviour.
TEST_F(
    BufferedInputTraceRecorderTest,
    WrapperOutlivesGuardWithoutDanglingSession)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath);

    std::unique_ptr<BufferedInput> traced;
    {
        BufferedInputTraceCaptureConfig config;
        config.traceRoot =
            (fs::path(traceBaseDir->getPath()) / "trace").string();
        config.datasetRoot = dataDir->getPath();

        ScopedBufferedInputTraceCapture capture(config);
        auto inner = std::make_unique<BufferedInput>(readFile, pool());
        traced = maybeWrapBufferedInputForTrace(
            std::move(inner), pool(), readFile);
        // Guard exits here; with shared_ptr the session stays alive.
    }

    // Capture is off after guard destruction.
    EXPECT_FALSE(bufferedInputTraceCaptureEnabled());

    // With shared_ptr the wrapper still holds the session; load() must not
    // crash.  With a raw dangling pointer this would be UB.
    EXPECT_NO_THROW(traced->load(LogType::TEST));

    traced.reset(); // clean destruction; session refcount → 0
}

// Finding 5/6: preload() must use the throwing appendEvent path.
// We prove this by filling maxEvents on the load() call and then calling
// preload(), which overflows.  With appendEvent (correct) preload() throws;
// with tryAppendEvent (old path) it would silently drop the event.
TEST_F(
    BufferedInputTraceRecorderTest,
    PreloadUsesThrowingAppendEvent)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    config.datasetRoot = dataDir->getPath();
    // input_create = event 1, load = event 2; maxEvents=2 → preload overflows.
    config.maxEvents = 2;

    ScopedBufferedInputTraceCapture capture(config);
    auto inner = std::make_unique<BufferedInput>(readFile, pool());
    auto traced =
        maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

    traced->load(LogType::TEST); // fills slot 2 (slot 1 was input_create)

    // preload() uses appendEvent (throwing path); this overflows → throws.
    // With the old tryAppendEvent it would silently drop → no throw → RED.
    VELOX_ASSERT_THROW(traced->preload(), "maxEvents limit exceeded");

    traced.reset(); // destructor tryAppendEvent(input_close) → drops silently
    // capture guard exits; destructor tries finish() → logs error, no throw
}

// Finding 8: verify preload, setNumStripes, and unplanned read (read()) are
// all recorded as events in the trace.
TEST_F(
    BufferedInputTraceRecorderTest,
    RecordsPreloadSetNumStripesAndUnplannedRead)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/64);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    {
        auto inner = std::make_unique<BufferedInput>(readFile, pool());
        auto traced =
            maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

        traced->setNumStripes(3);
        traced->preload();
        // read() is the "unplanned" path (does not go through enqueue).
        auto stream = traced->read(0, 32, LogType::TEST);
        stream.reset();
        traced.reset();
    }

    capture.finish();

    const auto trace = loadBufferedInputTrace(traceRoot);
    auto hasOp = [&](BufferedInputTraceOp op)
    {
        for (const auto& ev : trace.events)
        {
            if (ev.op == op)
            {
                return true;
            }
        }
        return false;
    };
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kInputSetNumStripes));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kInputPreload));
    EXPECT_TRUE(hasOp(BufferedInputTraceOp::kStreamRead));
}

// Finding 8: maxEvents overflow must cause finish() to throw.
// (Already implemented; this is a new regression test.)
TEST_F(
    BufferedInputTraceRecorderTest,
    RejectsMaxEventsOverflow)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/64);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    config.datasetRoot = dataDir->getPath();
    // Slots: input_create (1), load (2), setNumStripes (3) = exactly maxEvents.
    // The 4th event (input_close) is emitted by the destructor via
    // tryAppendEvent → silently sets firstError → finish() then throws.
    config.maxEvents = 3;

    ScopedBufferedInputTraceCapture capture(config);

    auto inner = std::make_unique<BufferedInput>(readFile, pool());
    auto traced =
        maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

    traced->load(LogType::TEST);
    traced->setNumStripes(1);
    traced.reset(); // destructor: tryAppendEvent(input_close) overflows → setError

    VELOX_ASSERT_THROW(capture.finish(), "maxEvents");
}

// Finding 8: if traceRoot already exists finish() must throw.
// (Already implemented; this is a new regression test.)
TEST_F(
    BufferedInputTraceRecorderTest,
    RejectsExistingTraceRoot)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();

    // Pre-create the trace root.
    fs::create_directories(traceRoot);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);
    VELOX_ASSERT_THROW(capture.finish(), "already exists");
}

// Finding 7: BackUp(-1) must throw with a "non-negative" message because the
// count >= 0 guard fires before the unsigned cast.
// Before fix: count=-1 is cast to a huge uint64_t; the "exceeds pending size"
// guard fires instead → wrong message → VELOX_ASSERT_THROW fails.
TEST_F(
    BufferedInputTraceRecorderTest,
    NegativeBackupCountThrows)
{
    namespace fs = std::filesystem;
    using facebook::velox::common::testutil::TempDirectoryPath;

    auto traceBaseDir = TempDirectoryPath::create();
    auto dataDir = TempDirectoryPath::create();

    const std::string traceRoot =
        (fs::path(traceBaseDir->getPath()) / "trace").string();
    const std::string filePath =
        (fs::path(dataDir->getPath()) / "data.parquet").string();
    auto readFile = makeRealFile(filePath, /*size=*/64);

    BufferedInputTraceCaptureConfig config;
    config.traceRoot = traceRoot;
    config.datasetRoot = dataDir->getPath();

    ScopedBufferedInputTraceCapture capture(config);

    auto inner = std::make_unique<BufferedInput>(readFile, pool());
    auto traced =
        maybeWrapBufferedInputForTrace(std::move(inner), pool(), readFile);

    auto stream = traced->enqueue(fvcommon::Region{0, 64});
    traced->load(LogType::TEST);

    const void* data = nullptr;
    int32_t size = 0;
    ASSERT_TRUE(stream->Next(&data, &size));
    ASSERT_GT(size, 0);

    // BackUp with a negative count must throw with a clear "non-negative" msg.
    VELOX_ASSERT_THROW(stream->BackUp(-1), "non-negative");

    // BackUp a valid amount first to restore state, then clean up.
    // (The stream is in indeterminate state after the throw; just reset it.)
    stream.reset();
    traced.reset();
}
