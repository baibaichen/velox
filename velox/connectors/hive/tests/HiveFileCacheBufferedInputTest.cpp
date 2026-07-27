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

/// Tests for the Hive FCBI adapter: selection, mapping, mutual exclusion,
/// and real miss-fill-hit through `createBufferedInput`.

#include "velox/connectors/hive/HiveConnectorUtil.h"

#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheFileIdentity.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileHandle.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/Connector.h"
#include "velox/dwio/common/BufferedInputTrace.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/Options.h"

#include <folly/CancellationToken.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ManualTimekeeper.h>

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace facebook::velox::connector::hive {
namespace {

using velox::common::testutil::TempDirectoryPath;
using ch::FileCacheBufferedInput;
using ch::FileCacheFileIdentity;
using ch::FileCacheManager;
using ch::FileCacheConfig;
using ch::FileCacheKey;
using ch::FileCachePolicy;
using ch::FileSegmentKeyType;

/// In-memory ReadFile for deterministic source bytes.
class InMemoryReadFile : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string data, std::string name)
      : data_(std::move(data)), name_(std::move(name)) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& = {}) const override {
    if (offset >= data_.size())
      return {};
    const uint64_t n = std::min<uint64_t>(length, data_.size() - offset);
    std::memcpy(buf, data_.data() + offset, n);
    preadBytes_ += n;
    return std::string_view(static_cast<const char*>(buf), n);
  }

  uint64_t size() const override { return data_.size(); }
  uint64_t memoryUsage() const override { return data_.size(); }
  bool shouldCoalesce() const override { return false; }
  std::string getName() const override { return name_; }
  uint64_t getNaturalReadSize() const override { return 1024; }

  uint64_t preadBytes() const { return preadBytes_.load(); }

 private:
  std::string data_;
  std::string name_;
  mutable std::atomic<uint64_t> preadBytes_{0};
};

class HiveFileCacheBufferedInputTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    filesystems::registerLocalFileSystem();
    // The process-wide MemoryAllocator only allows a single AsyncDataCache to
    // ever be registered on it (MallocAllocator::registerCache VELOX_CHECKs
    // that no cache is already registered, and there is no unregister path).
    // Create exactly one real AsyncDataCache for the whole test suite and
    // share it across every test that needs a non-null, valid
    // `cache::AsyncDataCache*` (never an invalid/sentinel pointer).
    //
    // `deprecatedDefaultMemoryManager()` (unlike `memoryManager()`) lazily
    // initializes the process-global MemoryManager singleton if it has not
    // been set up yet, which is required here since SetUpTestSuite runs
    // before any test's SetUp() (where the fixture pool is normally created).
    sharedAsyncCache_ = cache::AsyncDataCache::create(
        velox::memory::deprecatedDefaultMemoryManager().allocator());
  }

  static void TearDownTestSuite() {
    if (sharedAsyncCache_) {
      sharedAsyncCache_->shutdown();
      sharedAsyncCache_.reset();
    }
  }

  void SetUp() override {
    pool_ = velox::memory::deprecatedAddDefaultLeafMemoryPool(
        "hive-fcbi-adapter-test");
    root_ = TempDirectoryPath::create();
    sourceDir_ = TempDirectoryPath::create();
    fileSystem_ = filesystems::getFileSystem(root_->getPath(), {});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  }

  void TearDown() override {
    // Strict shutdown order: manager->shutdown() first (while the manager is
    // still the installed FileCacheManager instance), then clear the global
    // instance pointer, then drop manager/cache/timekeeper/pool (the latter
    // two happen automatically via member destruction order below, since
    // streams/inputs created in each test are already scoped out by here).
    if (manager_)
      manager_->shutdown();
    FileCacheManager::setInstance(nullptr);
    manager_.reset();
    dirs_.clear();
  }

  std::string newCacheDir() {
    auto dir = TempDirectoryPath::create();
    dirs_.push_back(dir);
    return dir->getPath();
  }

  FileCacheConfig makeConfig(const std::string& path) {
    FileCacheConfig config;
    config.path = path;
    config.maxSize = 16ull << 20;
    config.maxFileSegmentSize = 1ull << 20;
    config.boundaryAlignment = 1;
    config.reserveGranularity = 0;
    config.loadMetadataThreads = 1;
    config.backgroundDownloadThreads = 0;
    config.cachePolicy = FileCachePolicy::LRU;
    return config;
  }

  FileCacheManager::Options baseOptions() {
    FileCacheManager::Options options;
    options.commonUserId = "test-user";
    options.cachePathPrefix = root_->getPath();
    options.allowedCacheRoot = root_->getPath();
    options.localFileSystem = fileSystem_;
    options.memoryPool = pool_.get();
    options.timekeeper = timekeeper_;
    options.initializeOnCreate = false;
    return options;
  }

  void installManager() {
    auto opts = baseOptions();
    opts.caches.push_back(
        {.name = "default",
         .config = makeConfig(newCacheDir()),
         .configPath = "default.cfg"});
    opts.defaultCacheName = "default";
    opts.initializeOnCreate = true;
    manager_ = FileCacheManager::create(std::move(opts));
    FileCacheManager::setInstance(manager_.get());
  }

  FileHandle makeFileHandle(std::shared_ptr<ReadFile> file) {
    FileHandle handle;
    handle.file = std::move(file);
    return handle;
  }

  std::unique_ptr<connector::ConnectorQueryCtx> makeQueryCtx(
      cache::AsyncDataCache* cache = nullptr,
      folly::CancellationToken token = {}) {
    return std::make_unique<connector::ConnectorQueryCtx>(
        pool_.get(),
        pool_.get(),
        &sessionProperties_,
        nullptr,
        common::PrefixSortConfig(),
        nullptr,
        cache,
        "query.HiveFileCacheBufferedInputTest",
        "task.HiveFileCacheBufferedInputTest",
        "planNodeId.HiveFileCacheBufferedInputTest",
        0,
        "",
        false,
        std::move(token));
  }

  static std::string readAll(dwio::common::SeekableInputStream& stream) {
    std::string out;
    const void* data = nullptr;
    int size = 0;
    while (stream.Next(&data, &size))
      out.append(static_cast<const char*>(data), static_cast<size_t>(size));
    return out;
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<folly::Timekeeper> timekeeper_ =
      std::make_shared<folly::ManualTimekeeper>();
  std::shared_ptr<filesystems::FileSystem> fileSystem_;
  std::shared_ptr<TempDirectoryPath> root_;
  std::shared_ptr<TempDirectoryPath> sourceDir_;
  std::vector<std::shared_ptr<TempDirectoryPath>> dirs_;
  std::shared_ptr<FileCacheManager> manager_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  config::ConfigBase sessionProperties_{{}};

  // One real AsyncDataCache shared across the whole test suite; see
  // SetUpTestSuite for why it cannot be per-test.
  static std::shared_ptr<cache::AsyncDataCache> sharedAsyncCache_;
};

std::shared_ptr<cache::AsyncDataCache>
    HiveFileCacheBufferedInputTest::sharedAsyncCache_;

// ===========================================================================
// Selection tests
// ===========================================================================

TEST_F(HiveFileCacheBufferedInputTest, NoManagerNoCbiSelectsDirect) {
  // No FileCacheManager installed, no AsyncDataCache -> DirectBufferedInput.
  auto source = std::make_shared<InMemoryReadFile>(
      std::string(64, 'x'), "/source/file.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  EXPECT_NE(dynamic_cast<dwio::common::DirectBufferedInput*>(input.get()),
            nullptr)
      << "Expected DirectBufferedInput when no manager and no CBI";
}

TEST_F(HiveFileCacheBufferedInputTest, NoManagerWithCbiSelectsCachedBufferedInput) {
  // No FileCacheManager installed, with AsyncDataCache -> CachedBufferedInput.
  auto source = std::make_shared<InMemoryReadFile>(
      std::string(64, 'x'), "/source/file.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());

  // Use the suite-shared real AsyncDataCache (see SetUpTestSuite): the
  // process-wide allocator only permits a single registered cache.
  auto queryCtx = makeQueryCtx(sharedAsyncCache_.get());
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  EXPECT_NE(dynamic_cast<dwio::common::CachedBufferedInput*>(input.get()),
            nullptr)
      << "Expected CachedBufferedInput when CBI is available";
}

TEST_F(HiveFileCacheBufferedInputTest, ManagerSelectsFileCacheBufferedInput) {
  // FileCacheManager installed -> must return FileCacheBufferedInput.
  installManager();

  auto source = std::make_shared<InMemoryReadFile>(
      std::string(64, 'x'), "/source/file.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  EXPECT_NE(dynamic_cast<FileCacheBufferedInput*>(input.get()), nullptr)
      << "Expected FileCacheBufferedInput when manager is installed";
}

TEST_F(HiveFileCacheBufferedInputTest, ManagerAndCbiFailClosed) {
  // Both FileCacheManager and AsyncDataCache installed -> must throw.
  installManager();

  auto source = std::make_shared<InMemoryReadFile>(
      std::string(64, 'x'), "/source/file.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());

  // We need a non-null, real AsyncDataCache* to trigger the mutual exclusion
  // check. The guard fires before any cache method is called, but the
  // pointer must still be valid (never an invalid/sentinel address) for the
  // duration of the call. The process-wide allocator only permits a single
  // registered AsyncDataCache, so this reuses the suite-shared instance
  // (see SetUpTestSuite) rather than constructing a second one.
  auto queryCtx = makeQueryCtx(sharedAsyncCache_.get());
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  EXPECT_THROW(
      createBufferedInput(
          handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {}),
      VeloxUserError)
      << "FileCache and AsyncDataCache cannot both be installed";
}

// ===========================================================================
// Mapping test
// ===========================================================================

TEST_F(HiveFileCacheBufferedInputTest, MappingTest) {
  installManager();

  const std::string sourcePath = "/source/mapping-test.orc";
  auto source = std::make_shared<InMemoryReadFile>(
      std::string(64, 'x'), sourcePath);
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  readerOpts.setCacheable(true);

  folly::CancellationSource cancellationSource;
  auto queryCtx = makeQueryCtx(nullptr, cancellationSource.getToken());
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  auto* fcbi = dynamic_cast<FileCacheBufferedInput*>(input.get());
  ASSERT_NE(fcbi, nullptr) << "Expected FileCacheBufferedInput";

  // Verify request context mapping.
  EXPECT_EQ(fcbi->requestContext().queryId,
            "query.HiveFileCacheBufferedInputTest");
  EXPECT_EQ(fcbi->requestContext().userId, manager_->commonUserId());
  EXPECT_EQ(fcbi->requestContext().userWeight, 0u);
  EXPECT_TRUE(fcbi->requestContext().cacheable);
  EXPECT_EQ(fcbi->requestContext().segmentType, FileSegmentKeyType::Data);

  // Verify origin mapping.
  EXPECT_EQ(fcbi->origin().user_id, manager_->commonUserId());
  EXPECT_EQ(fcbi->origin().weight, std::optional<uint64_t>(0));
  EXPECT_EQ(fcbi->origin().segment_type, FileSegmentKeyType::Data);

  // Verify cache key derivation.
  EXPECT_EQ(
      fcbi->cacheKey(),
      ch::FileCacheFileIdentity::deriveKey({sourcePath, ""}));

  // Verify cancellation token propagation.
  EXPECT_TRUE(fcbi->cancellationToken().canBeCancelled());
  cancellationSource.requestCancellation();
  EXPECT_TRUE(fcbi->cancellationToken().isCancellationRequested());
}

// ===========================================================================
// Real miss-fill-hit test
// ===========================================================================

TEST_F(HiveFileCacheBufferedInputTest, RealMissFillHit) {
  installManager();

  // Deterministic source data.
  std::string sourceData(4096, 0);
  for (size_t i = 0; i < sourceData.size(); ++i)
    sourceData[i] = static_cast<char>('a' + (i % 26));

  const std::string sourcePath = "/source/miss-fill-hit.orc";
  auto source = std::make_shared<InMemoryReadFile>(sourceData, sourcePath);
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);

  // First read: expect cache miss -> source read -> cache write. Assert both
  // byte-exact source instrumentation (InMemoryReadFile::preadBytes) and the
  // global FileCache counters via takeFileCacheStatsSnapshot deltas.
  const ch::FileCacheStatsSnapshot statsBeforeFirst = ch::takeFileCacheStatsSnapshot();
  {
    auto ioStats = std::make_shared<io::IoStatistics>();
    auto ioS = std::make_shared<velox::IoStats>();
    auto input = createBufferedInput(
        handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

    auto* fcbi = dynamic_cast<FileCacheBufferedInput*>(input.get());
    ASSERT_NE(fcbi, nullptr);

    auto stream = input->read(0, sourceData.size(), dwio::common::LogType::STREAM);
    ASSERT_NE(stream, nullptr);
    std::string result = readAll(*stream);
    EXPECT_EQ(result, sourceData) << "First read must return exact source data";

    // Source was actually read (miss path).
    EXPECT_GT(source->preadBytes(), 0u)
        << "First read must cause source reads (cache miss)";
  }
  const ch::FileCacheStatsSnapshot statsAfterFirst = ch::takeFileCacheStatsSnapshot();
  const ch::FileCacheStatsSnapshot firstDelta = statsAfterFirst - statsBeforeFirst;
  EXPECT_GT(firstDelta.cacheMissCount, 0u)
      << "First read must record a FileCache miss";
  EXPECT_GT(firstDelta.sourceReadBytes, 0u)
      << "First read must record FileCache source-read bytes";
  EXPECT_GT(firstDelta.cacheWriteBytes, 0u)
      << "First read must record FileCache write (fill) bytes";

  // Second read: expect cache hit -> no additional source reads.
  const uint64_t sourceBytesBefore = source->preadBytes();
  const ch::FileCacheStatsSnapshot statsBeforeSecond = ch::takeFileCacheStatsSnapshot();
  {
    auto ioStats = std::make_shared<io::IoStatistics>();
    auto ioS = std::make_shared<velox::IoStats>();
    auto input = createBufferedInput(
        handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

    auto* fcbi = dynamic_cast<FileCacheBufferedInput*>(input.get());
    ASSERT_NE(fcbi, nullptr);

    auto stream = input->read(0, sourceData.size(), dwio::common::LogType::STREAM);
    ASSERT_NE(stream, nullptr);
    std::string result = readAll(*stream);
    EXPECT_EQ(result, sourceData) << "Second read must return exact source data";

    // Source must NOT have been read again (hit path).
    EXPECT_EQ(source->preadBytes(), sourceBytesBefore)
        << "Second read must not cause additional source reads (cache hit)";
  }
  const ch::FileCacheStatsSnapshot statsAfterSecond = ch::takeFileCacheStatsSnapshot();
  const ch::FileCacheStatsSnapshot secondDelta = statsAfterSecond - statsBeforeSecond;
  EXPECT_GT(secondDelta.cacheHitCount, 0u)
      << "Second read must record a FileCache hit";
  EXPECT_GT(secondDelta.cacheReadBytes, 0u)
      << "Second read must record FileCache cache-read bytes";
}

// ===========================================================================
// Task 3: Passthrough selection and streaming tests
// ===========================================================================

// When `ScopedFileCachePassthroughForBenchmark` is live and no manager or CBI
// is installed, `createBufferedInput` must return a `FileCacheBufferedInput` in
// passthrough (`ReadMode::kPassthrough`) mode.
TEST_F(HiveFileCacheBufferedInputTest, PassthroughSelectsFileCacheBufferedInputWithoutManager)
{
  auto source = std::make_shared<InMemoryReadFile>(
      std::string(256, 'A'), "/source/passthrough_selection.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  ScopedFileCachePassthroughForBenchmark guard;

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  auto* fcbi = dynamic_cast<ch::FileCacheBufferedInput*>(input.get());
  ASSERT_NE(fcbi, nullptr) << "Expected FileCacheBufferedInput in passthrough mode";
  EXPECT_EQ(fcbi->readMode(), ch::FileCacheBufferedInput::ReadMode::kPassthrough)
      << "readMode must be kPassthrough";
}

// The `buffered_input_perf_probe` session property alone must NOT activate the
// passthrough path — it only enables probe statistics.
TEST_F(HiveFileCacheBufferedInputTest, ProbePropertyAloneDoesNotSelectPassthrough)
{
  auto source = std::make_shared<InMemoryReadFile>(
      std::string(128, 'B'), "/source/probe_only.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  config::ConfigBase sessionWithProbe{{{"buffered_input_perf_probe", "true"}}};
  auto queryCtx = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      pool_.get(),
      &sessionWithProbe,
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.probe-only",
      "task.probe-only",
      "planNodeId.probe-only",
      0,
      "",
      false,
      folly::CancellationToken{});
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  // No ScopedFileCachePassthroughForBenchmark active.
  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  // Without passthrough guard, expects Direct (no manager, no CBI).
  EXPECT_NE(dynamic_cast<dwio::common::DirectBufferedInput*>(input.get()), nullptr)
      << "Probe property alone must not select passthrough; expected DirectBufferedInput";
}

// Passthrough must throw when a FileCacheManager is installed because the
// invariant (no FileCache state) cannot be guaranteed.
TEST_F(HiveFileCacheBufferedInputTest, PassthroughRejectsInstalledFileCacheManager)
{
  installManager();

  auto source = std::make_shared<InMemoryReadFile>(
      std::string(128, 'C'), "/source/passthrough_reject.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  ScopedFileCachePassthroughForBenchmark guard;

  EXPECT_THROW(
      createBufferedInput(
          handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {}),
      velox::VeloxException)
      << "Passthrough with a live FileCacheManager must throw";
}

// Passthrough path must read the full source data correctly through
// `FileCacheInputStream::readNextPassthroughChunk`.
TEST_F(HiveFileCacheBufferedInputTest, PassthroughStreamsSourceDataCorrectly)
{
  const std::string sourceData(4096, 'D');
  auto source = std::make_shared<InMemoryReadFile>(sourceData, "/source/passthrough_stream.orc");
  auto handle = makeFileHandle(source);
  dwio::common::ReaderOptions readerOpts(pool_.get());
  auto queryCtx = makeQueryCtx(nullptr);
  auto ioStats = std::make_shared<io::IoStatistics>();
  auto ioS = std::make_shared<velox::IoStats>();

  ScopedFileCachePassthroughForBenchmark guard;

  auto input = createBufferedInput(
      handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

  auto* fcbi = dynamic_cast<ch::FileCacheBufferedInput*>(input.get());
  ASSERT_NE(fcbi, nullptr);
  ASSERT_EQ(fcbi->readMode(), ch::FileCacheBufferedInput::ReadMode::kPassthrough);

  auto stream = input->read(0, sourceData.size(), dwio::common::LogType::STREAM);
  ASSERT_NE(stream, nullptr);
  std::string result = readAll(*stream);
  EXPECT_EQ(result, sourceData) << "Passthrough stream must return exact source data";

  // Source bytes were consumed via pread.
  EXPECT_GT(source->preadBytes(), 0u) << "Passthrough must read from source";

  // IoStatistics must record source read (not cache hit).
  EXPECT_GT(ioStats->read().count(), 0u)
      << "IoStatistics::read must be incremented for passthrough reads";
  EXPECT_GT(ioStats->rawBytesRead(), 0u)
      << "rawBytesRead must be incremented for passthrough reads";
}

// ===========================================================================
// BufferedInput trace-capture integration tests
// ===========================================================================

// When no ScopedBufferedInputTraceCapture is active, createBufferedInput must
// return the exact original dynamic type (DirectBufferedInput in the no-manager
// / no-cache branch) — not a TracingBufferedInput wrapper.
TEST_F(HiveFileCacheBufferedInputTest, TraceCaptureIsDefaultOff)
{
    ASSERT_FALSE(dwio::common::bufferedInputTraceCaptureEnabled())
        << "No trace guard should be active at test start";

    const std::string sourceData(64, 'z');
    auto source = std::make_shared<InMemoryReadFile>(sourceData, "/src/off.orc");
    auto handle = makeFileHandle(source);
    dwio::common::ReaderOptions readerOpts(pool_.get());
    auto queryCtx = makeQueryCtx(nullptr);
    auto ioStats = std::make_shared<io::IoStatistics>();
    auto ioS = std::make_shared<velox::IoStats>();

    auto input = createBufferedInput(
        handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

    ASSERT_NE(input, nullptr);
    EXPECT_NE(dynamic_cast<dwio::common::DirectBufferedInput*>(input.get()), nullptr)
        << "Without an active capture guard, createBufferedInput must return "
           "DirectBufferedInput unmodified";
    EXPECT_EQ(
        dynamic_cast<dwio::common::TracingBufferedInput*>(input.get()), nullptr)
        << "TracingBufferedInput must not be returned when capture is off";
}

// When a ScopedBufferedInputTraceCapture is active, createBufferedInput must
// return a TracingBufferedInput wrapper; reading a stream emits at least one
// event visible after finish().
TEST_F(HiveFileCacheBufferedInputTest, ActiveTraceWrapsDirectInput)
{
    // Create a real local file strictly inside sourceDir_ so that canonical
    // path containment check passes.
    const std::string fileName = "active_trace_source.bin";
    const std::string filePath = sourceDir_->getPath() + "/" + fileName;
    const std::string fileData(256, 'A');
    {
        std::ofstream f(filePath, std::ios::binary);
        ASSERT_TRUE(f.is_open()) << "Could not create test file: " << filePath;
        f.write(fileData.data(), static_cast<std::streamsize>(fileData.size()));
    }

    // Fresh temp dir for the trace output.
    auto traceDir = TempDirectoryPath::create();
    const std::string traceRoot = traceDir->getPath() + "/trace_out";

    // Open the file with LocalReadFile so canonical identity is real.
    auto localFile = std::make_shared<LocalReadFile>(filePath);
    auto handle = makeFileHandle(localFile);

    dwio::common::BufferedInputTraceCaptureConfig captureConfig;
    captureConfig.traceRoot = traceRoot;
    captureConfig.datasetRoot = sourceDir_->getPath();
    captureConfig.queryId = 4;
    captureConfig.drivers = 1;
    captureConfig.binaryRealPath =
        std::filesystem::canonical("/proc/self/exe").string();
    captureConfig.binaryBuildId = "test-build-id";
    captureConfig.veloxHead = "velox-head-hash";
    captureConfig.glutenHead = "gluten-head-hash";
    captureConfig.clickhouseHead = "ch-head-hash";
    captureConfig.maxEvents = 100'000;

    dwio::common::ScopedBufferedInputTraceCapture guard(
        std::move(captureConfig));

    ASSERT_TRUE(dwio::common::bufferedInputTraceCaptureEnabled());

    dwio::common::ReaderOptions readerOpts(pool_.get());
    auto queryCtx = makeQueryCtx(nullptr);
    auto ioStats = std::make_shared<io::IoStatistics>();
    auto ioS = std::make_shared<velox::IoStats>();

    auto input = createBufferedInput(
        handle, readerOpts, queryCtx.get(), ioStats, ioS, executor_.get(), {});

    ASSERT_NE(input, nullptr);
    EXPECT_NE(
        dynamic_cast<dwio::common::TracingBufferedInput*>(input.get()), nullptr)
        << "With an active capture guard, createBufferedInput must return "
           "TracingBufferedInput";

    // Drain a stream to emit at least one event.
    auto stream = input->read(0, fileData.size(), dwio::common::LogType::STREAM);
    ASSERT_NE(stream, nullptr);
    readAll(*stream);
    stream.reset();
    input.reset();

    guard.finish();

    // Validate the written trace.
    auto doc = dwio::common::loadBufferedInputTrace(traceRoot);
    EXPECT_GE(doc.events.size(), 1u)
        << "At least one event must be captured";
    EXPECT_EQ(doc.manifest.queryId, 4);
    EXPECT_EQ(doc.manifest.drivers, 1);
    EXPECT_FALSE(doc.manifest.datasetRoot.empty());
    EXPECT_FALSE(doc.files.empty())
        << "File table must have at least one entry";
}

} // namespace
} // namespace facebook::velox::connector::hive
