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

// Benchmark: random seek access patterns via the assembled FileCache read path
// (FileCacheBufferedInput -> FileCacheInputStream -> FileCache), driven through a
// real FileCacheManager.
//
// Usage:
//   velox_ch_filecache_seek_benchmark \
//     --bm_min_iters=10 \
//     --file_size_mb=256 \
//     --cache_dir=/tmp/fc_bench \
//     --cache_size_mb=512
//
// Metrics emitted (per iteration): a batch of `seek_count` random-offset reads.
//   FileCacheSeekCacheHit   — reads over already-DOWNLOADED segments (warm cache)
//   FileCacheSeekCacheMiss  — reads over a fresh cache dir (first-touch miss->fill)
//   FileCacheSeekBypass     — reads with readIfExistsOtherwiseBypass=true (bypass)

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <atomic>
#include <cstdint>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

DEFINE_int32(file_size_mb, 64, "Synthetic file size in MiB");
DEFINE_int32(cache_size_mb, 128, "FileCache max size in MiB");
DEFINE_string(cache_dir, "/tmp/fc_seek_bench", "FileCache directory path root");
DEFINE_int32(seek_count, 1000, "Number of random seeks per benchmark run");
DEFINE_int32(read_len, 4096, "Bytes read per seek");

namespace facebook::velox::ch
{
namespace
{

struct BenchmarkFixture
{
    std::unique_ptr<velox::memory::MemoryManager> memoryManager;
    std::shared_ptr<velox::memory::MemoryPool> pool;
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
    std::shared_ptr<FileCacheManager> manager;

    std::string sourcePath;
    std::string cacheRoot;
    uint64_t fileSize{0};
    uint64_t readLen{0};
    FileCacheKey warmKey;
    std::vector<uint64_t> seekOffsets;
    int nextMissCache{0};
};

std::unique_ptr<BenchmarkFixture> gFixture;

/// A `velox::ReadFile` wrapping `LocalReadFile` that counts every physical read
/// (`pread`/`preadv`). Used by the hit benchmark to assert the "no source read
/// on a cache hit" contract: once a segment is DOWNLOADED, subsequent reads must
/// serve from the local cache segment file, so the SOURCE pread count must not
/// grow during the timed hit loop. If a hit ever degrades to reading the source
/// this counter rises and the assertion fails (guards against a silent perf
/// regression that would otherwise still "pass").
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


std::string benchContent(size_t n)
{
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i)
        s[i] = static_cast<char>(i * 31 + 7);
    return s;
}

FileCacheConfig makeConfig(const std::string & path, uint64_t maxSize, uint64_t seg)
{
    FileCacheConfig c;
    c.path = path;
    c.maxSize = maxSize;
    c.maxElements = 100000;
    c.maxFileSegmentSize = seg;
    c.boundaryAlignment = 1;
    c.reserveGranularity = 1;
    c.cachePolicy = FileCachePolicy::LRU;
    c.useSplitCache = false;
    c.backgroundDownloadThreads = 0;
    c.loadMetadataThreads = 2;
    c.loadMetadataAsynchronously = false;
    c.keepFreeSpaceSizeRatio = 0.0;
    c.keepFreeSpaceElementsRatio = 0.0;
    return c;
}

std::unique_ptr<FileCacheBufferedInput> makeInput(
    const FileCachePtr & cache,
    std::shared_ptr<velox::ReadFile> readFile,
    const FileCacheKey & key,
    FileCacheReadOptions opts = {})
{
    auto & fx = *gFixture;
    FileCacheRequestContext ctx;
    ctx.queryId = "bench";
    ctx.userId = fx.manager->commonUserId();
    return std::make_unique<FileCacheBufferedInput>(
        std::move(readFile),
        cache,
        key,
        cache->getCommonOrigin(),
        opts,
        ctx,
        dwio::common::MetricsLog::voidLog(),
        std::make_shared<io::IoStatistics>(),
        std::make_shared<velox::IoStats>(),
        fx.executor.get(),
        dwio::common::ReaderOptions(fx.pool.get()));
}

std::unique_ptr<FileCacheBufferedInput> makeInput(
    const FileCachePtr & cache, const FileCacheKey & key, FileCacheReadOptions opts = {})
{
    auto & fx = *gFixture;
    return makeInput(cache, std::make_shared<velox::LocalReadFile>(fx.sourcePath), key, opts);
}

// Drain a stream (returns the number of bytes read).
uint64_t drain(dwio::common::SeekableInputStream & stream)
{
    uint64_t total = 0;
    const void * data = nullptr;
    int32_t size = 0;
    while (stream.Next(&data, &size))
    {
        folly::doNotOptimizeAway(*static_cast<const char *>(data));
        total += static_cast<uint64_t>(size);
    }
    return total;
}

void setupFixture()
{
    if (gFixture)
        return;

    filesystems::registerLocalFileSystem();
    auto fx = std::make_unique<BenchmarkFixture>();

    fx->fileSize = static_cast<uint64_t>(FLAGS_file_size_mb) * 1024 * 1024;
    fx->readLen = static_cast<uint64_t>(FLAGS_read_len);
    // The local file system matches on an absolute path; cache/segment files are
    // opened via filesystems::getFileSystem(path), so the cache root must be
    // absolute (a relative --cache_dir would fail scheme matching at read time).
    fx->cacheRoot = std::filesystem::absolute(FLAGS_cache_dir).string();

    fx->memoryManager = std::make_unique<velox::memory::MemoryManager>(
        velox::memory::MemoryManager::Options{});
    fx->pool = fx->memoryManager->addLeafPool("filecache-seek-benchmark");
    fx->executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);

    std::filesystem::create_directories(fx->cacheRoot);
    fx->sourcePath = fx->cacheRoot + "/source.bin";
    {
        std::ofstream out(fx->sourcePath, std::ios::binary | std::ios::trunc);
        out << benchContent(fx->fileSize);
    }

    // Two caches: a "warm" cache pre-filled for the hit benchmark, and a "miss"
    // cache dir template re-created per miss run. The manager owns the warm one.
    const uint64_t maxSize = static_cast<uint64_t>(FLAGS_cache_size_mb) * 1024 * 1024;
    const uint64_t seg = 1024 * 1024;

    FileCacheManager::Options o;
    o.commonUserId = "bench-user";
    o.localFileSystem = filesystems::getFileSystem("/", nullptr);
    o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
    o.initializeOnCreate = true;
    o.defaultCacheName = "warm";
    o.caches.push_back({"warm", makeConfig(fx->cacheRoot + "/warm", maxSize, seg), "conf.warm"});
    fx->manager = FileCacheManager::create(o);
    FileCacheManager::setInstance(fx->manager.get());

    // Pre-generate random offsets aligned within [0, fileSize - readLen].
    std::mt19937_64 rng(12345);
    const uint64_t maxOff = fx->fileSize > fx->readLen ? fx->fileSize - fx->readLen : 0;
    std::uniform_int_distribution<uint64_t> dist(0, maxOff);
    fx->seekOffsets.reserve(FLAGS_seek_count);
    for (int i = 0; i < FLAGS_seek_count; ++i)
        fx->seekOffsets.push_back(dist(rng));

    fx->warmKey = FileCacheKey::fromPath(fx->sourcePath);

    // Publish the fixture BEFORE warming so makeInput (which reads gFixture) sees
    // a fully-populated fixture during the warm-up reads.
    gFixture = std::move(fx);

    // Warm the "warm" cache: read every seek window once so subsequent reads hit.
    auto warm = gFixture->manager->getDefault();
    for (uint64_t off : gFixture->seekOffsets)
    {
        auto input = makeInput(warm, gFixture->warmKey);
        auto stream = input->enqueue({off, gFixture->readLen});
        drain(*stream);
    }
}

// Fresh, independent cache for the miss benchmark (its own dir + key each run so
// nothing is pre-downloaded). Registered under a unique name in the manager.
FileCachePtr freshMissCache()
{
    auto & fx = *gFixture;
    const std::string name = "miss-" + std::to_string(fx.nextMissCache++);
    const std::string dir = fx.cacheRoot + "/" + name;
    std::filesystem::remove_all(dir);
    const uint64_t maxSize = static_cast<uint64_t>(FLAGS_cache_size_mb) * 1024 * 1024;
    auto cache = fx.manager->factory().getOrCreate(name, makeConfig(dir, maxSize, 1024 * 1024), "conf." + name);
    cache->initialize();
    return cache;
}

} // namespace
} // namespace facebook::velox::ch

using namespace facebook::velox;
using namespace facebook::velox::ch;

// Baseline: warm-cache hits. All FileCacheBufferedInput objects are constructed
// in a suspended (untimed) region so the measured cost is purely enqueue+drain
// over the local cache segments, not per-read wrapper construction. The source
// is wrapped in a CountingReadFile: after warming, a hit must serve from the
// local segment file, so the source pread count must NOT grow across the timed
// loop. If it does (a hit degraded into reading the source), the run fails.
BENCHMARK(FileCacheSeekCacheHit)
{
    setupFixture();
    auto & fx = *gFixture;
    auto cache = fx.manager->getDefault();

    std::vector<std::shared_ptr<CountingReadFile>> sources;
    std::vector<std::unique_ptr<FileCacheBufferedInput>> inputs;
    uint64_t preadBefore = 0;
    BENCHMARK_SUSPEND
    {
        sources.reserve(fx.seekOffsets.size());
        inputs.reserve(fx.seekOffsets.size());
        for (size_t i = 0; i < fx.seekOffsets.size(); ++i)
        {
            auto source = std::make_shared<CountingReadFile>(fx.sourcePath);
            inputs.push_back(makeInput(cache, source, fx.warmKey));
            sources.push_back(std::move(source));
        }
        for (const auto & s : sources)
            preadBefore += s->preadCount();
    }

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto stream = inputs[i]->enqueue({fx.seekOffsets[i], fx.readLen});
        folly::doNotOptimizeAway(drain(*stream));
    }

    BENCHMARK_SUSPEND
    {
        uint64_t preadAfter = 0;
        for (const auto & s : sources)
            preadAfter += s->preadCount();
        // Load-bearing: a warm hit reads only the local cache segment file, never
        // the wrapped source. Any increase means the hit path fell back to the
        // source, i.e. a real cache-hit regression.
        VELOX_CHECK_EQ(
            preadAfter,
            preadBefore,
            "cache hit read the source ({} extra preads): hit path degraded to a source read",
            preadAfter - preadBefore);
    }
}

BENCHMARK_RELATIVE(FileCacheSeekCacheMiss)
{
    setupFixture();
    auto & fx = *gFixture;
    FileCachePtr cache;
    std::vector<std::unique_ptr<FileCacheBufferedInput>> inputs;
    // A fresh cache per iteration guarantees every seek is a first-touch miss.
    // Both the cache setup and the wrapper construction happen untimed so the
    // measured cost is only the enqueue+drain miss->fill path.
    BENCHMARK_SUSPEND
    {
        cache = freshMissCache();
        auto key = FileCacheKey::fromPath(fx.sourcePath);
        inputs.reserve(fx.seekOffsets.size());
        for (size_t i = 0; i < fx.seekOffsets.size(); ++i)
            inputs.push_back(makeInput(cache, key));
    }
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto stream = inputs[i]->enqueue({fx.seekOffsets[i], fx.readLen});
        folly::doNotOptimizeAway(drain(*stream));
    }
    BENCHMARK_SUSPEND { cache->deactivateBackgroundOperations(); }
}

BENCHMARK_RELATIVE(FileCacheSeekBypass)
{
    setupFixture();
    auto & fx = *gFixture;
    auto cache = fx.manager->getDefault();
    FileCacheReadOptions opts;
    opts.readIfExistsOtherwiseBypass = true; // never creates a segment
    std::vector<std::unique_ptr<FileCacheBufferedInput>> inputs;
    BENCHMARK_SUSPEND
    {
        auto key = FileCacheKey::fromPath(fx.sourcePath);
        inputs.reserve(fx.seekOffsets.size());
        for (size_t i = 0; i < fx.seekOffsets.size(); ++i)
            inputs.push_back(makeInput(cache, key, opts));
    }
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto stream = inputs[i]->enqueue({fx.seekOffsets[i], fx.readLen});
        folly::doNotOptimizeAway(drain(*stream));
    }
}

int main(int argc, char ** argv)
{
    folly::Init init(&argc, &argv);
    setupFixture();
    folly::runBenchmarks();
    if (gFixture && gFixture->manager)
    {
        gFixture->manager->shutdown();
        FileCacheManager::setInstance(nullptr);
    }
    // Tear the fixture down here (not at static-destruction time) so its pool /
    // MemoryManager destruct while the runtime is still alive.
    gFixture.reset();
    return 0;
}
