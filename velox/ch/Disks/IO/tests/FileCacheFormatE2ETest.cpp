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

// Design 6.11: a REAL format-level end-to-end test that drives a real DWRF
// reader over the assembled FileCache read path. The existing UAF unit test
// (EnqueueCopiesTrackingIdSurvivingStreamIdentifierDestruction in the connector
// suite) only fakes a temporary StreamIdentifier; this suite exercises the true
// format-reader call stack:
//
//     dwrf::DwrfReader / DwrfRowReader
//       -> clones the FileCacheBufferedInput per stripe
//       -> enqueues each column stream with a stack-local StreamIdentifier
//       -> load()
//       -> ColumnReader/DecoderUtil consume the FileCacheInputStream
//
// It proves, on the real reader path:
//   * clone() carries the full FileCache / tracker / request context, so the
//     cloned per-stripe input reads through the SAME cache;
//   * many column-chunk enqueues followed by a single load() read back the
//     exact written data;
//   * the stack-local StreamIdentifier handed to enqueue() inside the reader is
//     safe (no use-after-free): the values read are correct;
//   * a cold scan (first read) goes through the source and FILLS the FileCache,
//     while a warm scan (fresh reader, second query) is served from the
//     FileCache and reads the source dramatically less (0 here).
//
// Why DWRF and not Parquet (the format the design text names): the goal is a
// real format reader driving clone -> enqueue(stack sid) -> load -> consume with
// a cold/warm split. DWRF is velox's native format; its Writer and Reader are in
// the mono `velox` library that this test target already links (via
// velox_hive_connector), so it integrates with NO new build dependency. The
// Parquet writer pulls in the bundled Arrow stack (velox_dwio_parquet_writer +
// arrow), a heavier link surface for this focused FileCache target. DWRF covers
// every 6.11 verification point identically: multiple columns, multiple stripes
// (the DWRF analogue of Parquet row groups), the real ColumnReader consumption
// path, per-stripe clone, stack-local stream identifiers, and the cold+warm
// FileCache fill/hit behaviour. So we use DWRF; the path under test (the real
// format reader over FileCacheBufferedInput) is exactly what 6.11 asks for.

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInputBuilder.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace facebook::velox::ch
{
namespace
{
namespace fs = std::filesystem;
using common::testutil::TempDirectoryPath;
using connector::ConnectorQueryCtx;
using connector::hive::BufferedInputBuilder;

// Deterministic per-row payloads. Row i holds int64 value `kIntBase + i` and a
// string "row-<i>-<hash>" where the hash mixes i, so a wrong absolute row almost
// always yields a wrong string.
constexpr int64_t kIntBase = 1'000'000;

int64_t intAt(int64_t i)
{
    return kIntBase + i;
}

std::string strAt(int64_t i)
{
    const uint32_t h = (static_cast<uint32_t>(i) * 2654435761u) >> 16;
    return fmt::format("row-{}-{}", i, h);
}

/// A `velox::ReadFile` wrapping `LocalReadFile` that counts every physical read
/// (`pread`/`preadv`). Used to compare cold vs warm source I/O: after the cold
/// scan fills the FileCache, a warm scan must serve bytes from local cache
/// segment files, so its source read count is far smaller (0 here).
class CountingReadFile : public velox::ReadFile
{
public:
    explicit CountingReadFile(const std::string & path)
        : inner_(std::make_unique<velox::LocalReadFile>(path))
    {
    }

    std::string_view pread(uint64_t offset, uint64_t length, void * buf, const velox::FileIoContext & ctx = {})
        const override
    {
        preadCount_.fetch_add(1);
        return inner_->pread(offset, length, buf, ctx);
    }

    uint64_t preadv(
        uint64_t offset,
        const std::vector<folly::Range<char *>> & buffers,
        const velox::FileIoContext & ctx = {}) const override
    {
        preadCount_.fetch_add(1);
        return inner_->preadv(offset, buffers, ctx);
    }

    bool shouldCoalesce() const override { return inner_->shouldCoalesce(); }
    uint64_t size() const override { return inner_->size(); }
    uint64_t memoryUsage() const override { return inner_->memoryUsage(); }
    std::string getName() const override { return inner_->getName(); }
    uint64_t getNaturalReadSize() const override { return inner_->getNaturalReadSize(); }

    uint64_t preadCount() const { return preadCount_.load(); }

private:
    std::unique_ptr<velox::LocalReadFile> inner_;
    mutable std::atomic_uint64_t preadCount_{0};
};

class FileCacheFormatE2ETest : public ::testing::Test
{
protected:
    static void SetUpTestCase() { filesystems::registerLocalFileSystem(); }

    void SetUp() override
    {
        temp_ = TempDirectoryPath::create();
        pool_ = memoryManager_.addLeafPool("filecache-format-e2e");
        connectorPool_ = memoryManager_.addLeafPool("filecache-format-e2e-connector");
        writerPool_ = memoryManager_.addRootPool("filecache-format-e2e-writer");
        executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
        sessionProperties_ = std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>{});
        rowType_ = ROW({"c_int", "c_str"}, {BIGINT(), VARCHAR()});
    }

    void TearDown() override
    {
        BufferedInputBuilder::registerBuilder(std::make_shared<NativeForwardingBuilder>());
        if (FileCacheManager::getInstance())
        {
            FileCacheManager::getInstance()->shutdown();
            FileCacheManager::setInstance(nullptr);
        }
        manager_.reset();
    }

    // A native-forwarding builder equivalent to the trunk's file-local default,
    // restored between tests so a leaked FileCache registration cannot leak into
    // an unrelated test.
    class NativeForwardingBuilder final : public BufferedInputBuilder
    {
    public:
        std::unique_ptr<dwio::common::BufferedInput> create(
            const FileHandle & fileHandle,
            const dwio::common::ReaderOptions & readerOpts,
            const ConnectorQueryCtx * connectorQueryCtx,
            std::shared_ptr<io::IoStatistics> ioStatistics,
            std::shared_ptr<IoStats> ioStats,
            folly::Executor * executor,
            const folly::F14FastMap<std::string, std::string> & fileReadOps) override
        {
            return connector::hive::createBufferedInput(
                fileHandle, readerOpts, connectorQueryCtx, ioStatistics, ioStats, executor, fileReadOps);
        }
    };

    std::string sub(const std::string & s) const { return (fs::path(temp_->getPath()) / s).string(); }

    // Build a two-column RowVector for absolute rows [firstRow, firstRow + n).
    RowVectorPtr makeBatch(int64_t firstRow, vector_size_t n)
    {
        auto ints = BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
        auto strs = BaseVector::create<FlatVector<StringView>>(VARCHAR(), n, pool_.get());
        for (vector_size_t i = 0; i < n; ++i)
        {
            ints->set(i, intAt(firstRow + i));
            const auto s = strAt(firstRow + i);
            strs->set(i, StringView(s));
        }
        std::vector<VectorPtr> children{ints, strs};
        return std::make_shared<RowVector>(pool_.get(), rowType_, nullptr, n, std::move(children));
    }

    // Write a DWRF file with two columns and (at least) two stripes by flushing
    // between the two written batches. Returns the file path and total row count.
    std::pair<std::string, int64_t> writeTwoStripeFile(
        const std::string & name,
        vector_size_t rowsPerStripe,
        std::optional<common::CompressionKind> compression = std::nullopt)
    {
        const auto path = sub(name);

        dwrf::WriterOptions options;
        options.schema = rowType_;
        options.memoryPool = writerPool_.get();
        if (compression.has_value())
        {
            auto config = std::make_shared<dwrf::Config>();
            config->set(dwrf::Config::COMPRESSION, compression.value());
            options.config = std::move(config);
        }

        auto sink = std::make_unique<dwio::common::LocalFileSink>(
            path, dwio::common::FileSink::Options{});
        auto writer = std::make_unique<dwrf::Writer>(std::move(sink), options);

        // Two batches with an explicit flush in between => two stripes.
        writer->write(makeBatch(0, rowsPerStripe));
        writer->flush();
        writer->write(makeBatch(rowsPerStripe, rowsPerStripe));
        writer->close();

        return {path, static_cast<int64_t>(rowsPerStripe) * 2};
    }

    // Build + install a FileCacheManager with one default cache. A small
    // maxFileSegmentSize forces many segments (multiple column chunks / stripes
    // map onto multiple segments), exercising the real multi-segment path.
    FileCachePtr makeManagerCache()
    {
        FileCacheConfig c;
        c.path = sub("cache");
        c.maxSize = 64 * 1024 * 1024;
        c.maxElements = 10000;
        c.maxFileSegmentSize = 16 * 1024;
        c.boundaryAlignment = 1;
        c.reserveGranularity = 1;
        c.cachePolicy = FileCachePolicy::LRU;
        c.useSplitCache = false;
        c.backgroundDownloadThreads = 0;
        c.loadMetadataThreads = 2;
        c.loadMetadataAsynchronously = false;
        c.keepFreeSpaceSizeRatio = 0.0;
        c.keepFreeSpaceElementsRatio = 0.0;

        FileCacheManager::Options o;
        o.commonUserId = "user-A";
        o.localFileSystem = filesystems::getFileSystem("/", nullptr);
        o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
        o.initializeOnCreate = true;
        o.defaultCacheName = "default";
        o.caches.push_back({"default", c, "conf.default"});

        manager_ = FileCacheManager::create(o);
        FileCacheManager::setInstance(manager_.get());
        return manager_->getDefault();
    }

    dwio::common::ReaderOptions readerOptions()
    {
        dwio::common::ReaderOptions opts(pool_.get());
        opts.setFileFormat(dwio::common::FileFormat::DWRF);
        // Disable whole-file preload: a small file would otherwise be slurped
        // into RAM in a single source read, bypassing the per-stream FileCache
        // demand path we are exercising. With the threshold at 0 the footer and
        // every column stream are read through FileCacheInputStream, so the cold
        // scan fills the cache segment-by-segment and the warm scan hits them.
        opts.setFilePreloadThreshold(0);
        // A small load quantum so each column stream splits into several plan
        // chunks -- the real multi-chunk enqueue/load path, not one giant read.
        opts.setLoadQuantum(8 * 1024);
        return opts;
    }

    std::unique_ptr<ConnectorQueryCtx> makeCtx()
    {
        return std::make_unique<ConnectorQueryCtx>(
            pool_.get(),
            connectorPool_.get(),
            sessionProperties_.get(),
            /*spillConfig*/ nullptr,
            common::PrefixSortConfig(),
            /*expressionEvaluator*/ nullptr,
            /*cache*/ nullptr,
            /*queryId*/ "q1",
            /*taskId*/ "task1",
            /*planNodeId*/ "plan1",
            /*driverId*/ 0,
            /*sessionTimezone*/ "");
    }

    // Open a real DwrfReader whose BufferedInput is produced by the installed
    // FileCacheBufferedInputBuilder (the exact connector extension point). The
    // reader clones this input per stripe and enqueues each column stream with a
    // stack-local StreamIdentifier -- the real path under test.
    std::unique_ptr<dwrf::DwrfReader>
    openReader(const ConnectorQueryCtx & ctx, std::shared_ptr<velox::ReadFile> file)
    {
        FileHandle handle;
        handle.file = std::move(file);
        auto input = BufferedInputBuilder::getInstance()->create(
            handle,
            readerOptions(),
            &ctx,
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get());
        // Prove the connector selected our FileCacheBufferedInput for this reader.
        EXPECT_NE(dynamic_cast<FileCacheBufferedInput *>(input.get()), nullptr)
            << "reader must be driven through FileCacheBufferedInput";
        return dwrf::DwrfReader::create(std::move(input), readerOptions());
    }

    // Read the whole file through a RowReader and return the flattened (int, str)
    // pairs in row order. Consuming the RowReader drives ColumnReader over the
    // FileCacheInputStream -- the real read stack.
    std::vector<std::pair<int64_t, std::string>> readAll(dwrf::DwrfReader & reader)
    {
        dwio::common::RowReaderOptions rowReaderOpts;
        rowReaderOpts.select(std::make_shared<dwio::common::ColumnSelector>(rowType_));
        auto rowReader = reader.createRowReader(rowReaderOpts);

        std::vector<std::pair<int64_t, std::string>> out;
        VectorPtr batch;
        while (rowReader->next(256, batch))
        {
            auto * row = batch->as<RowVector>();
            auto * ints = row->childAt(0)->asFlatVector<int64_t>();
            auto * strs = row->childAt(1)->asFlatVector<StringView>();
            for (vector_size_t i = 0; i < batch->size(); ++i)
                out.emplace_back(ints->valueAt(i), std::string(strs->valueAt(i)));
        }
        return out;
    }

    void expectContent(const std::vector<std::pair<int64_t, std::string>> & got, int64_t total)
    {
        ASSERT_EQ(static_cast<int64_t>(got.size()), total);
        for (int64_t i = 0; i < total; ++i)
        {
            EXPECT_EQ(got[i].first, intAt(i)) << "int mismatch at row " << i;
            EXPECT_EQ(got[i].second, strAt(i)) << "str mismatch at row " << i;
        }
    }

// Write a Parquet file with two columns and (at least) two row groups by
// capping rowsInRowGroup below the total row count. Returns path + total rows.
std::pair<std::string, int64_t> writeTwoRowGroupParquet(const std::string & name, vector_size_t rowsPerGroup)
{
    const auto path = sub(name);

    dwio::common::WriterOptions options;
    options.schema = rowType_;
    options.memoryPool = writerPool_.get();
    // Cap each Parquet row group at rowsPerGroup rows, so 2 * rowsPerGroup total
    // rows produce exactly two row groups (Arrow enforces the row-count cap).
    options.flushPolicyFactory = [rowsPerGroup]()
    {
        return std::make_unique<parquet::LambdaFlushPolicy>(
            /*rowsInRowGroup=*/static_cast<uint64_t>(rowsPerGroup),
            /*bytesInRowGroup=*/1024 * 1024,
            []() { return false; });
    };

    auto sink = std::make_unique<dwio::common::LocalFileSink>(path, dwio::common::FileSink::Options{});
    auto writer = std::make_unique<parquet::Writer>(std::move(sink), options, rowType_);

    writer->write(makeBatch(0, rowsPerGroup));
    writer->write(makeBatch(rowsPerGroup, rowsPerGroup));
    writer->close();

    return {path, static_cast<int64_t>(rowsPerGroup) * 2};
}

velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
    std::shared_ptr<velox::memory::MemoryPool> connectorPool_;
    std::shared_ptr<velox::memory::MemoryPool> writerPool_;
    std::shared_ptr<TempDirectoryPath> temp_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
    std::shared_ptr<config::ConfigBase> sessionProperties_;
    std::shared_ptr<FileCacheManager> manager_;
    RowTypePtr rowType_;
};

// ============================================================================
// ColdScanFillsCacheWarmScanServesFromCache: the whole 6.11 scenario.
//
// 1. Write a two-column, two-stripe DWRF file (deterministic rows).
// 2. Cold scan through a real DwrfReader whose input is the FileCacheBufferedInput
//    (built by the connector extension point). Assert:
//      * the read-back data equals the written data (correctness across the real
//        clone -> enqueue(stack sid) -> load -> ColumnReader consume path);
//      * the source was actually read (cold preadCount > 0);
//      * the FileCache was filled (segments created) -- via the DEMAND read path
//        (normal stream ids with no history classify as demand, so load() warms
//        nothing; the cache is filled synchronously by FileCacheInputStream::Next
//        during the scan, not by an async prefetch warm).
// 3. Warm scan through a FRESH DwrfReader (new instance, second query) over a
//    fresh CountingReadFile. Assert:
//      * data still correct;
//      * source reads are dramatically reduced vs cold (0 here) -- the bytes come
//        from the local cache segment files, proving the warm path.
// ============================================================================
TEST_F(FileCacheFormatE2ETest, ColdScanFillsCacheWarmScanServesFromCache)
{
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);

    // ~4000 rows across two stripes; string column makes stripes several KiB so
    // the small 16 KiB segments split the file into many cache segments.
    const auto [path, total] = writeTwoStripeFile("data.dwrf", /*rowsPerStripe*/ 2000);

    registerFileCacheBufferedInputBuilder(*manager_);
    auto ctx = makeCtx();

    // --- Cold scan: fills the cache from the source. ---
    auto coldFile = std::make_shared<CountingReadFile>(path);
    {
        auto reader = openReader(*ctx, coldFile);
        // Two stripes were written.
        EXPECT_EQ(reader->getNumberOfStripes(), 2u);
        expectContent(readAll(*reader), total);
    }
    const uint64_t coldReads = coldFile->preadCount();
    EXPECT_GT(coldReads, 0u) << "cold scan must read the source";
    EXPECT_GT(cache->getFileSegmentsNum(), 0u)
        << "cold scan must fill the FileCache (demand-path fill)";

    // --- Warm scan: fresh reader, served from the FileCache. ---
    auto warmFile = std::make_shared<CountingReadFile>(path);
    {
        auto reader = openReader(*ctx, warmFile);
        expectContent(readAll(*reader), total);
    }
    const uint64_t warmReads = warmFile->preadCount();

    // The warm scan reads the source dramatically less than the cold scan; with
    // the whole file cached after the cold scan, it reads the source not at all.
    EXPECT_LT(warmReads, coldReads)
        << "warm scan must read the source far less than the cold scan (cold="
        << coldReads << ", warm=" << warmReads << ")";
    EXPECT_EQ(warmReads, 0u)
        << "a fully-cached warm scan must not read the source at all";
}

TEST_F(FileCacheFormatE2ETest, UncompressedDwrfReadFullyBacksUpCoalescedWindow)
{
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);

    const auto [path, total] = writeTwoStripeFile(
        "uncompressed.dwrf",
        /*rowsPerStripe*/ 120'000,
        common::CompressionKind_NONE);
    ASSERT_GT(fs::file_size(path), 1ULL << 20);

    registerFileCacheBufferedInputBuilder(*manager_);
    auto ctx = makeCtx();
    auto source = std::make_shared<CountingReadFile>(path);
    auto reader = openReader(*ctx, source);

    expectContent(readAll(*reader), total);
}

// ============================================================================
// Design 7.8: a REAL Parquet format-level end-to-end test. The DWRF test above
// cannot substitute for Parquet validation because:
//   * the production UAF call site is ParquetData::enqueueRowGroup's stack-local
//     StreamIdentifier;
//   * Parquet holds per-row-group input clones through ReaderBase::inputs_;
//   * StructColumnReader::loadRowGroup has a Parquet-specific
//     isBuffered -> clone / enqueue / load branch;
//   * DWRF uses stripe / UnitLoader lifetimes and cannot prove Parquet
//     row-group scheduling.
//
// This test drives a real ParquetReader whose BufferedInput is produced by the
// installed FileCacheBufferedInputBuilder, over a file with two columns and two
// row groups. The reader clones the input per row group and enqueues each
// column chunk with a stack-local StreamIdentifier -- the true UAF-prone path.
// It asserts a cold scan fills the FileCache from the source and a warm scan
// (fresh reader) is served from the cache with dramatically fewer source reads.
// ============================================================================

TEST_F(FileCacheFormatE2ETest, ParquetColdScanFillsCacheWarmScanServesFromCache)
{
    auto cache = makeManagerCache();
    ASSERT_NE(cache, nullptr);

    // 2 row groups of 2000 rows each; the string column makes each row group
    // several KiB so the 16 KiB cache segments split the file into many.
    const auto [path, total] = writeTwoRowGroupParquet("data.parquet", /*rowsPerGroup*/ 2000);

    registerFileCacheBufferedInputBuilder(*manager_);
    auto ctx = makeCtx();

    auto parquetReaderOpts = [this]()
    {
        auto opts = readerOptions();
        opts.setFileFormat(dwio::common::FileFormat::PARQUET);
        return opts;
    };

    // Open a real ParquetReader whose input is the FileCacheBufferedInput
    // produced by the connector extension point. Assert the connector selected
    // FileCacheBufferedInput for this reader (the real path under test).
    auto openParquet = [&](std::shared_ptr<velox::ReadFile> file)
    {
        FileHandle handle;
        handle.file = std::move(file);
        auto input = BufferedInputBuilder::getInstance()->create(
            handle,
            parquetReaderOpts(),
            ctx.get(),
            std::make_shared<io::IoStatistics>(),
            std::make_shared<velox::IoStats>(),
            executor_.get());
        EXPECT_NE(dynamic_cast<FileCacheBufferedInput *>(input.get()), nullptr)
            << "Parquet reader must be driven through FileCacheBufferedInput";
        return std::make_unique<parquet::ParquetReader>(std::move(input), parquetReaderOpts());
    };

    // Read the whole file through a ParquetRowReader, driving StructColumnReader
    // over the per-row-group clone / enqueue(stack sid) / load / PageReader path.
    auto readAllParquet = [&](parquet::ParquetReader & reader)
    {
        dwio::common::RowReaderOptions rowReaderOpts;
        rowReaderOpts.select(std::make_shared<dwio::common::ColumnSelector>(rowType_));
        auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
        scanSpec->addAllChildFields(*rowType_);
        rowReaderOpts.setScanSpec(scanSpec);
        auto rowReader = reader.createRowReader(rowReaderOpts);

        std::vector<std::pair<int64_t, std::string>> out;
        // The Parquet reader requires a pre-allocated result vector (unlike
        // DWRF, its SelectiveStructColumnReader rejects a null result).
        VectorPtr batch = BaseVector::create(rowType_, 0, pool_.get());
        while (rowReader->next(256, batch))
        {
            auto * row = batch->as<RowVector>();
            // Parquet may return non-flat encodings (e.g. dictionary for the
            // string column), so decode both children rather than assuming flat.
            DecodedVector decInt(*row->childAt(0));
            DecodedVector decStr(*row->childAt(1));
            for (vector_size_t i = 0; i < batch->size(); ++i)
                out.emplace_back(decInt.valueAt<int64_t>(i), std::string(decStr.valueAt<StringView>(i)));
        }
        return out;
    };

    // --- Cold scan: fills the cache from the source. ---
    auto coldFile = std::make_shared<CountingReadFile>(path);
    {
        auto reader = openParquet(coldFile);
        // Two row groups were written (Parquet-specific metadata check).
        EXPECT_EQ(reader->fileMetaData().numRowGroups(), 2);
        expectContent(readAllParquet(*reader), total);
    }
    const uint64_t coldReads = coldFile->preadCount();
    EXPECT_GT(coldReads, 0u) << "cold scan must read the source";
    EXPECT_GT(cache->getFileSegmentsNum(), 0u)
        << "cold scan must fill the FileCache (demand-path fill)";

    // --- Warm scan: fresh reader, served from the FileCache. ---
    auto warmFile = std::make_shared<CountingReadFile>(path);
    {
        auto reader = openParquet(warmFile);
        expectContent(readAllParquet(*reader), total);
    }
    const uint64_t warmReads = warmFile->preadCount();

    EXPECT_LT(warmReads, coldReads)
        << "warm scan must read the source far less than the cold scan (cold="
        << coldReads << ", warm=" << warmReads << ")";
    EXPECT_EQ(warmReads, 0u)
        << "a fully-cached warm scan must not read the source at all";
}

} // namespace
} // namespace facebook::velox::ch
