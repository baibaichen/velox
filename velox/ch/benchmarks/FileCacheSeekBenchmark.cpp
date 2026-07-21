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

// Benchmark: random seekToPosition access patterns via FileCacheInputStream.
//
// Usage:
//   velox_ch_filecache_seek_benchmark \
//     --bm_min_iters=10 \
//     --file_size_mb=256 \
//     --cache_dir=tmp/fc_bench \
//     --cache_size_mb=512
//
// Metrics emitted (per iteration):
//   seek_cache_hit_ns   — seek + one Next() when segment is already DOWNLOADED
//   seek_cache_miss_ns  — seek + one Next() on first access (miss -> fill)
//   seek_bypass_ns      — seek + one Next() with readIfExistsOtherwiseBypass

#include "velox/ch/Disks/IO/tests/FileCacheTestHelpers.h"

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/PositionProvider.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <random>

DEFINE_int32(file_size_mb, 64, "Synthetic file size in MiB");
DEFINE_int32(cache_size_mb, 128, "FileCache max size in MiB");
DEFINE_string(cache_dir, "tmp/fc_seek_bench", "FileCache directory path");
DEFINE_int32(seek_count, 1000, "Number of random seeks per benchmark run");

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

struct BenchmarkFixture
{
    std::shared_ptr<memory::MemoryPool> pool;
    std::shared_ptr<filesystems::FileSystem> fs;
    std::shared_ptr<folly::Timekeeper> timekeeper;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    std::shared_ptr<FileCacheManager> manager;
    FileCachePtr cache;
    std::shared_ptr<CountingReadFile> sourceFile;
    // Per-process random key for the hit-path benchmark.  Using random() instead
    // of fromPath() guarantees fresh keys even when the cache directory is
    // reloaded from a prior run — no destructive remove_all needed.
    FileCacheKey cacheKey;
    std::vector<uint64_t> seekOffsets;
    std::vector<char> expectedPattern;
};

static std::unique_ptr<BenchmarkFixture> gFixture;

void setupFixture()
{
    filesystems::registerLocalFileSystem();

    gFixture = std::make_unique<BenchmarkFixture>();
    gFixture->pool = memory::deprecatedAddDefaultLeafMemoryPool("bench");

    // Ensure cache directory is absolute for the local filesystem.
    std::string cacheDir = FLAGS_cache_dir;
    if (!cacheDir.empty() && cacheDir[0] != '/')
    {
        cacheDir = std::filesystem::absolute(cacheDir).string();
    }
    std::filesystem::create_directories(cacheDir);

    gFixture->fs = filesystems::getFileSystem(cacheDir, {});
    gFixture->timekeeper = std::make_shared<folly::ManualTimekeeper>();
    gFixture->executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);

    const size_t fileSize = static_cast<size_t>(FLAGS_file_size_mb) * 1024 * 1024;
    gFixture->expectedPattern = makeDeterministicData(fileSize);
    gFixture->sourceFile = std::make_shared<CountingReadFile>(gFixture->expectedPattern);
    // Random key per process: even when the cache directory retains metadata
    // from prior runs, this key has never been seen before so warm/hit
    // benchmarks are genuine.
    gFixture->cacheKey = FileCacheKey::random();

    FileCacheTestOptions testOpts;
    testOpts.maxSize = static_cast<uint64_t>(FLAGS_cache_size_mb) * 1024 * 1024;
    testOpts.maxFileSegmentSize = 1ull << 20;
    gFixture->manager = makeManager(
        cacheDir, gFixture->pool.get(), gFixture->fs,
        gFixture->timekeeper, testOpts);
    FileCacheManager::setInstance(gFixture->manager.get());
    gFixture->cache = gFixture->manager->getDefault();

    // Generate deterministic seek offsets with fixed seed.
    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<uint64_t> dist(0, fileSize - 4096);
    gFixture->seekOffsets.reserve(FLAGS_seek_count);
    for (int i = 0; i < FLAGS_seek_count; ++i)
        gFixture->seekOffsets.push_back(dist(rng));
}

/// Constructs a stream over the full file, seeks to `offset` via
/// seekToPosition with PositionProvider, calls one Next, and verifies every
/// returned byte against the expected pattern. Uses bounded buffer sizes
/// (4096) so one Next is predictable.
bool seekAndVerify(uint64_t offset, bool bypass)
{
    auto & f = *gFixture;
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 4096;
    opts.localFsBufferSize = 4096;
    FileCacheKey key = f.cacheKey;
    if (bypass)
    {
        opts.readIfExistsOtherwiseBypass = true;
        // Random key guarantees uncached even when the cache dir is reloaded.
        key = FileCacheKey::random();
    }

    auto inp = makeInput(
        *f.manager, f.cache, f.sourceFile, key,
        f.pool.get(), f.executor.get(), opts);
    auto stream = inp->read(0, f.expectedPattern.size(), dwio::common::LogType::STREAM);

    // Seek using real seekToPosition with PositionProvider.
    std::vector<uint64_t> positions{offset};
    dwio::common::PositionProvider provider(positions);
    stream->seekToPosition(provider);

    // One real Next call.
    const void * data = nullptr;
    int size = 0;
    VELOX_CHECK(stream->Next(&data, &size), "seekAndVerify: Next must succeed");

    // Validate every returned byte against the pattern.
    const size_t numBytes = static_cast<size_t>(size);
    const char * bytes = static_cast<const char *>(data);
    for (size_t i = 0; i < numBytes; ++i)
    {
        if (bytes[i] != f.expectedPattern[offset + i])
            return false;
    }
    return true;
}

bool seekAndVerifyMiss(uint64_t offset)
{
    auto & f = *gFixture;
    // Random key per call: collision-free even across process runs that share
    // the same persistent cache directory.
    auto key = FileCacheKey::random();

    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 4096;
    opts.localFsBufferSize = 4096;

    auto inp = makeInput(
        *f.manager, f.cache, f.sourceFile, key,
        f.pool.get(), f.executor.get(), opts);
    auto stream = inp->read(0, f.expectedPattern.size(), dwio::common::LogType::STREAM);

    // Seek using real seekToPosition with PositionProvider.
    std::vector<uint64_t> positions{offset};
    dwio::common::PositionProvider provider(positions);
    stream->seekToPosition(provider);

    // One real Next call.
    const void * data = nullptr;
    int size = 0;
    VELOX_CHECK(stream->Next(&data, &size), "seekAndVerifyMiss: Next must succeed");

    const size_t numBytes = static_cast<size_t>(size);
    const char * bytes = static_cast<const char *>(data);
    for (size_t i = 0; i < numBytes; ++i)
    {
        if (bytes[i] != f.expectedPattern[offset + i])
            return false;
    }
    return true;
}

/// Warm the shared cacheKey at every seek offset to guarantee cache hits
/// in the timed hit-path benchmark.
void warmCacheKey()
{
    auto & f = *gFixture;
    FileCacheReadOptions opts;
    opts.remoteFsBufferSize = 4096;
    opts.localFsBufferSize = 4096;

    for (auto offset : f.seekOffsets)
    {
        auto inp = makeInput(
            *f.manager, f.cache, f.sourceFile, f.cacheKey,
            f.pool.get(), f.executor.get(), opts);
        auto stream = inp->read(offset, 4096, dwio::common::LogType::STREAM);
        auto bytes = readAll(*stream);
        VELOX_CHECK_EQ(bytes.size(), 4096u, "warm read must return 4096 bytes");
    }
}

} // namespace
} // namespace facebook::velox::ch

BENCHMARK(FileCacheSeekCacheHit)
{
    for (auto offset : facebook::velox::ch::gFixture->seekOffsets)
    {
        const bool ok = facebook::velox::ch::seekAndVerify(offset, false);
        VELOX_CHECK(ok, "cache-hit seek verification failed");
        folly::doNotOptimizeAway(ok);
    }
}

BENCHMARK(FileCacheSeekCacheMiss)
{
    for (auto offset : facebook::velox::ch::gFixture->seekOffsets)
    {
        const bool ok = facebook::velox::ch::seekAndVerifyMiss(offset);
        VELOX_CHECK(ok, "cache-miss seek verification failed");
        folly::doNotOptimizeAway(ok);
    }
}

BENCHMARK(FileCacheSeekBypass)
{
    for (auto offset : facebook::velox::ch::gFixture->seekOffsets)
    {
        const bool ok = facebook::velox::ch::seekAndVerify(offset, true);
        VELOX_CHECK(ok, "bypass seek verification failed");
        folly::doNotOptimizeAway(ok);
    }
}

int main(int argc, char ** argv)
{
    folly::init(&argc, &argv);
    facebook::velox::ch::setupFixture();

    // Warm every referenced segment/offset on the normal key; assert each warm read.
    facebook::velox::ch::warmCacheKey();

    // Smoke pass: validate miss/hit/bypass before timing.
    const auto smokeOffset = facebook::velox::ch::gFixture->seekOffsets.front();
    VELOX_CHECK(
        facebook::velox::ch::seekAndVerifyMiss(smokeOffset),
        "Smoke pass: miss path failed");
    VELOX_CHECK(
        facebook::velox::ch::seekAndVerify(smokeOffset, false),
        "Smoke pass: hit path failed");
    VELOX_CHECK(
        facebook::velox::ch::seekAndVerify(
            facebook::velox::ch::gFixture->seekOffsets.back(), true),
        "Smoke pass: bypass path failed");

    folly::runBenchmarks();

    if (facebook::velox::ch::gFixture &&
        facebook::velox::ch::gFixture->manager)
    {
        facebook::velox::ch::gFixture->manager->shutdown();
        facebook::velox::ch::FileCacheManager::setInstance(nullptr);
    }
    // Explicitly destroy the fixture before main returns to ensure proper
    // teardown ordering (manager before pool).
    facebook::velox::ch::gFixture.reset();
    return 0;
}
