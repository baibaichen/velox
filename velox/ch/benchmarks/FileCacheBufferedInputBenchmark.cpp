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

// Benchmark: the full FileCacheBufferedInput read()/Next() path.
//
// Unlike FileCacheSeekBenchmark (which measures only seekToPosition), this micro
// exercises the whole streaming read path: a fresh FileCacheBufferedInput per
// call, inp->read(offset, len) to obtain a stream, and readAll() draining the
// stream via repeated Next() calls.
//
// Usage:
//   velox_ch_fcbi_benchmark \
//     --bm_min_iters=5 \
//     --file_size_mb=128 \
//     --cache_dir=tmp/fc_fcbi_bench \
//     --cache_size_mb=256
//
// Benchmarks emitted:
//   FCBI_SequentialHot   — sequential region reads over a warmed key (hits)
//   FCBI_RandomHot       — random-offset region reads over a warmed key (hits)
//   FCBI_SequentialCold  — fresh random key each iteration (source fill + write)
//
// Cache directories are never wiped with remove_all: fresh reads use
// FileCacheKey::random() so a persistent cache directory can never turn a cold
// benchmark into an unintended hit, and a warm key is filled once at setup.

#include "velox/ch/Disks/IO/tests/FileCacheTestHelpers.h"

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <cstring>
#include <filesystem>
#include <random>

DEFINE_int32(file_size_mb, 128, "Synthetic file size in MiB");
DEFINE_int32(cache_size_mb, 256, "FileCache max size in MiB");
DEFINE_string(cache_dir, "tmp/fc_fcbi_bench", "FileCache directory path");
DEFINE_int32(region_size_kib, 1024, "Region size in KiB per read");
DEFINE_int32(regions_per_iter, 16, "Regions read per benchmark iteration");

namespace facebook::velox::ch
{
namespace
{

using test::CountingReadFile;
using test::FileCacheTestOptions;
using test::makeDeterministicData;
using test::makeInput;
using test::makeManager;
using test::readAll;

struct FcbiFixture
{
    std::shared_ptr<memory::MemoryPool> pool;
    std::shared_ptr<filesystems::FileSystem> fs;
    std::shared_ptr<folly::Timekeeper> timekeeper;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    std::shared_ptr<FileCacheManager> manager;
    FileCachePtr cache;
    std::shared_ptr<CountingReadFile> sourceFile;
    std::vector<char> data;
    FileCacheKey hotKey; // pre-warmed in setup for the hot benchmarks
};

static std::unique_ptr<FcbiFixture> gFixture;

uint64_t regionBytes()
{
    return static_cast<uint64_t>(FLAGS_region_size_kib) * 1024;
}

// Reads `regions` regions of `regionBytes()` starting at `startOffset` through a
// fresh FCBI over `key`; returns total bytes returned. A fresh (random) key
// yields a cold fill; the warmed hotKey yields cache hits. This is the timed
// body: it deliberately performs no verification so the measured cost is the
// pure read/Next path.
size_t readRegions(const FileCacheKey & key, uint64_t startOffset, int regions)
{
    auto & f = *gFixture;
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = regionBytes();
    opts.localFsBufferSize = regionBytes();
    auto inp = makeInput(
        *f.manager, f.cache, f.sourceFile, key, f.pool.get(), f.executor.get(), opts);
    size_t total = 0;
    uint64_t offset = startOffset;
    for (int r = 0; r < regions && offset + regionBytes() <= f.data.size(); ++r)
    {
        auto stream = inp->read(offset, regionBytes(), dwio::common::LogType::STREAM);
        total += readAll(*stream).size();
        offset += regionBytes();
    }
    return total;
}

// Verified counterpart of readRegions used only outside the timed loops. It
// asserts every region returns exactly regionBytes() and that the bytes match
// the deterministic source pattern, so the pre-timing smoke can prove the read
// path returns real, correct data rather than an empty/no-op stream. Returns the
// total bytes returned. Never call this from a BENCHMARK body: the per-byte
// memcmp would pollute the timing.
size_t readRegionsVerified(const FileCacheKey & key, uint64_t startOffset, int regions)
{
    auto & f = *gFixture;
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = regionBytes();
    opts.localFsBufferSize = regionBytes();
    auto inp = makeInput(
        *f.manager, f.cache, f.sourceFile, key, f.pool.get(), f.executor.get(), opts);
    size_t total = 0;
    uint64_t offset = startOffset;
    for (int r = 0; r < regions && offset + regionBytes() <= f.data.size(); ++r)
    {
        auto stream = inp->read(offset, regionBytes(), dwio::common::LogType::STREAM);
        auto bytes = readAll(*stream);
        VELOX_CHECK_EQ(
            bytes.size(), regionBytes(),
            "FCBI smoke: region read returned an unexpected byte count");
        VELOX_CHECK(
            std::memcmp(bytes.data(), f.data.data() + offset, bytes.size()) == 0,
            "FCBI smoke: region content mismatch vs source pattern");
        total += bytes.size();
        offset += regionBytes();
    }
    return total;
}

void warmHotKey()
{
    auto & f = *gFixture;
    const uint64_t totalRegions = f.data.size() / regionBytes();
    for (uint64_t start = 0; start < totalRegions; start += FLAGS_regions_per_iter)
        readRegions(f.hotKey, start * regionBytes(), FLAGS_regions_per_iter);
}

void setupFixture()
{
    filesystems::registerLocalFileSystem();
    gFixture = std::make_unique<FcbiFixture>();
    gFixture->pool = memory::deprecatedAddDefaultLeafMemoryPool("fcbi_bench");

    std::string cacheDir = FLAGS_cache_dir;
    if (!cacheDir.empty() && cacheDir[0] != '/')
        cacheDir = std::filesystem::absolute(cacheDir).string();
    std::filesystem::create_directories(cacheDir);

    gFixture->fs = filesystems::getFileSystem(cacheDir, {});
    gFixture->timekeeper = std::make_shared<folly::ManualTimekeeper>();
    gFixture->executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);

    const size_t fileSize = static_cast<size_t>(FLAGS_file_size_mb) * 1024 * 1024;
    gFixture->data = makeDeterministicData(fileSize);
    gFixture->sourceFile = std::make_shared<CountingReadFile>(gFixture->data);

    FileCacheTestOptions testOpts;
    testOpts.maxSize = static_cast<uint64_t>(FLAGS_cache_size_mb) * 1024 * 1024;
    testOpts.maxFileSegmentSize = regionBytes();
    gFixture->manager = makeManager(
        cacheDir, gFixture->pool.get(), gFixture->fs, gFixture->timekeeper, testOpts);
    FileCacheManager::setInstance(gFixture->manager.get());
    gFixture->cache = gFixture->manager->getDefault();

    gFixture->hotKey = FileCacheKey::random();
    warmHotKey(); // read the whole file once so hot benchmarks hit the cache
}

// Sequential read of regions_per_iter regions over the warmed key (cache hits).
BENCHMARK(FCBI_SequentialHot, n)
{
    auto & f = *gFixture;
    const uint64_t span = FLAGS_regions_per_iter * regionBytes();
    const uint64_t maxStart = f.data.size() > span ? f.data.size() - span : 0;
    for (unsigned i = 0; i < n; ++i)
    {
        uint64_t start = maxStart == 0 ? 0 : (static_cast<uint64_t>(i) * span) % maxStart;
        start -= start % regionBytes();
        folly::doNotOptimizeAway(readRegions(f.hotKey, start, FLAGS_regions_per_iter));
    }
}

// Random-offset reads over the warmed key (cache hits).
BENCHMARK(FCBI_RandomHot, n)
{
    auto & f = *gFixture;
    std::mt19937_64 gen(42);
    const uint64_t maxStart =
        f.data.size() > regionBytes() ? f.data.size() - regionBytes() : 0;
    std::uniform_int_distribution<uint64_t> dist(0, maxStart);
    for (unsigned i = 0; i < n; ++i)
    {
        for (int r = 0; r < FLAGS_regions_per_iter; ++r)
        {
            uint64_t offset = dist(gen);
            offset -= offset % regionBytes();
            folly::doNotOptimizeAway(readRegions(f.hotKey, offset, 1));
        }
    }
}

// Cold fill: a fresh random key each iteration forces a source read + cache
// write (never previously cached, even across process restarts).
BENCHMARK(FCBI_SequentialCold, n)
{
    for (unsigned i = 0; i < n; ++i)
    {
        FileCacheKey coldKey = FileCacheKey::random();
        folly::doNotOptimizeAway(readRegions(coldKey, 0, FLAGS_regions_per_iter));
    }
}

} // namespace
} // namespace facebook::velox::ch

int main(int argc, char ** argv)
{
    folly::init(&argc, &argv);
    facebook::velox::ch::setupFixture();

    // Smoke: validate the read/Next path returns the full, correct data on both a
    // warmed (cache-hit) key and a fresh (cold source-fill) key before timing.
    // The exact byte-count and content checks reject an empty/no-op read path,
    // which would otherwise make the benchmark measure nothing.
    const uint64_t expectedBytes = static_cast<uint64_t>(FLAGS_regions_per_iter)
        * facebook::velox::ch::regionBytes();
    VELOX_CHECK_EQ(
        facebook::velox::ch::readRegionsVerified(
            facebook::velox::ch::gFixture->hotKey, 0, FLAGS_regions_per_iter),
        expectedBytes,
        "FCBI smoke (hot): read returned insufficient data");
    VELOX_CHECK_EQ(
        facebook::velox::ch::readRegionsVerified(
            facebook::velox::ch::FileCacheKey::random(), 0, FLAGS_regions_per_iter),
        expectedBytes,
        "FCBI smoke (cold): read returned insufficient data");

    folly::runBenchmarks();

    if (facebook::velox::ch::gFixture && facebook::velox::ch::gFixture->manager)
    {
        facebook::velox::ch::gFixture->manager->shutdown();
        facebook::velox::ch::FileCacheManager::setInstance(nullptr);
    }
    // Destroy the fixture explicitly: manager before pool.
    facebook::velox::ch::gFixture.reset();
    return 0;
}
