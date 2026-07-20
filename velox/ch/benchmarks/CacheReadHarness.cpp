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

#include "velox/ch/benchmarks/CacheReadHarness.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <glog/logging.h>

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/LocalFile.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::ch::bench {
namespace {
using dwio::common::LogType;
using dwio::common::MetricsLog;
using dwio::common::SeekableInputStream;

constexpr uint64_t kGiB = 1ULL << 30;

// Lazily (re)builds the shared remote blob at `path`, pseudo-random with a
// fixed seed so every run sees identical source bytes.
void ensureRemoteFile(const std::string& path, uint64_t want, bool rebuild) {
  namespace fs = std::filesystem;
  if (!rebuild && fs::exists(path) && fs::file_size(path) == want) {
    return;
  }
  LOG(INFO) << "Building remote blob " << path << " (" << want << " bytes)";
  std::ofstream out{path, std::ios::binary | std::ios::trunc};
  constexpr size_t kChunk = 1 << 20;
  std::vector<char> chunk(kChunk);
  std::mt19937_64 rng{0xfeedfaceULL};
  for (uint64_t written = 0; written < want; written += kChunk) {
    for (size_t i = 0; i < kChunk; i += 8) {
      const uint64_t v = rng();
      std::memcpy(chunk.data() + i, &v, 8);
    }
    const size_t toWrite =
        static_cast<size_t>(std::min<uint64_t>(kChunk, want - written));
    out.write(chunk.data(), toWrite);
  }
  VELOX_CHECK(out.good(), "Failed to write remote blob");
}
} // namespace

// ---- KeyGenerator ----

KeyGenerator::KeyGenerator(
    Workload workload,
    uint64_t n,
    uint64_t seed,
    uint64_t seqStart,
    double zipfTheta)
    : workload_(workload), n_(n), seqPos_(seqStart % n), rng_(seed) {
  VELOX_CHECK_GT(n, 0, "KeyGenerator universe must be non-empty");
  if (workload_ == Workload::kZipfian) {
    buildZipfCdf(zipfTheta);
  }
}

uint64_t KeyGenerator::next() {
  switch (workload_) {
    case Workload::kSequential: {
      const auto k = seqPos_;
      seqPos_ = (seqPos_ + 1) % n_;
      return k;
    }
    case Workload::kZipfian: {
      std::uniform_real_distribution<double> u(0.0, 1.0);
      const double r = u(rng_);
      auto it = std::lower_bound(cdf_.begin(), cdf_.end(), r);
      return static_cast<uint64_t>(std::distance(cdf_.begin(), it));
    }
    case Workload::kUniform: {
      std::uniform_int_distribution<uint64_t> d(0, n_ - 1);
      return d(rng_);
    }
  }
  VELOX_UNREACHABLE();
}

void KeyGenerator::buildZipfCdf(double theta) {
  cdf_.resize(n_);
  double sum = 0.0;
  for (uint64_t i = 1; i <= n_; ++i) {
    sum += 1.0 / std::pow(static_cast<double>(i), theta);
    cdf_[i - 1] = sum;
  }
  for (auto& c : cdf_) {
    c /= sum;
  }
}

// ---- WorkloadDriver ----

WorkloadDriver::WorkloadDriver(
    Workload workload,
    uint64_t workingSetKeys,
    uint64_t readSizeBytes,
    uint64_t seed,
    uint64_t baseOffset)
    : readSizeBytes_{readSizeBytes},
      baseOffset_{baseOffset},
      keyGen_{workload, workingSetKeys, seed} {
  VELOX_CHECK_GT(readSizeBytes, 0, "readSizeBytes must be positive");
}

velox::common::Region WorkloadDriver::nextRegion() {
  const uint64_t key = keyGen_.next();
  return velox::common::Region{
      baseOffset_ + key * readSizeBytes_, readSizeBytes_};
}

// ---- DataLayout ----

DataLayout::DataLayout(
    const std::vector<SourceFile>& files,
    uint64_t readSize,
    uint64_t maxBytes)
    : readSize_(readSize) {
  const uint64_t maxBlocks = maxBytes / readSize;
  prefix_.push_back(0);
  uint64_t acc = 0;
  for (uint32_t idx = 0; idx < files.size(); ++idx) {
    if (maxBytes > 0 && acc >= maxBlocks) {
      break;
    }
    uint64_t blocks = files[idx].size / readSize;
    if (maxBytes > 0) {
      blocks = std::min(blocks, maxBlocks - acc);
    }
    if (blocks == 0) {
      continue;
    }
    acc += blocks;
    prefix_.push_back(acc);
    fileIndices_.push_back(idx);
  }
  total_ = acc;
  VELOX_USER_CHECK_GT(
      total_,
      0,
      "No readable blocks: read size {} exceeds the file sizes",
      readSize);
}

DataLayout::Loc DataLayout::resolve(uint64_t key) const {
  const auto it = std::upper_bound(prefix_.begin(), prefix_.end(), key);
  const auto slot =
      static_cast<size_t>(std::distance(prefix_.begin(), it) - 1);
  return Loc{fileIndices_[slot], (key - prefix_[slot]) * readSize_};
}

// ---- WorkingSet ----

WorkingSet WorkingSet::create(const WorkingSetConfig& config) {
  WorkingSet ws;
  const uint64_t blobBytes = config.remoteBytesOverride > 0
      ? config.remoteBytesOverride
      : config.targetBytes + kGiB;
  ws.remotePath_ = config.remotePath;
  ws.blobBytes_ = blobBytes;
  ws.rebuildRemote_ = config.rebuildRemote;
  ws.files_.push_back({config.remotePath, blobBytes});
  ws.targetBytes_ = config.targetBytes;
  return ws;
}

void WorkingSet::materialize() const {
  ensureRemoteFile(remotePath_, blobBytes_, rebuildRemote_);
}

uint64_t WorkingSet::rawTotalBytes() const {
  uint64_t total = 0;
  for (const auto& f : files_) {
    total += f.size;
  }
  return total;
}

// ---- read consumers ----

uint64_t drain(SeekableInputStream& stream, uint64_t readSize) {
  uint64_t copied = 0;
  const void* data = nullptr;
  int32_t size = 0;
  while (copied < readSize && stream.Next(&data, &size)) {
    copied +=
        std::min<uint64_t>(static_cast<uint64_t>(size), readSize - copied);
  }
  return copied;
}

void drainConsumer(
    SeekableInputStream& stream,
    uint64_t readSize,
    uint32_t /*fileIdx*/,
    uint64_t /*offset*/) {
  (void)drain(stream, readSize);
}

// ---- CacheHarnessBase ----

CacheHarnessBase::CacheHarnessBase(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : config_(config), sourceFiles_(files) {}

PassResult CacheHarnessBase::runSweep(
    WorkloadDriver& driver,
    uint64_t ops,
    uint64_t readSize,
    const DataLayout& layout,
    const std::shared_ptr<io::IoStatistics>& ioStats) {
  PassResult r;
  r.requestedBytes = ops * readSize;
  const auto t0 = std::chrono::steady_clock::now();
  uint64_t done = 0;
  while (done < ops) {
    const uint64_t n = std::min<uint64_t>(config_.batch, ops - done);
    std::map<uint32_t, std::vector<uint64_t>> byFile;
    for (uint64_t i = 0; i < n; ++i) {
      const auto region = driver.nextRegion();
      const auto loc = layout.resolve(region.offset / readSize);
      byFile[loc.fileIdx].push_back(loc.offset);
    }
    readBatch(byFile, readSize, ioStats, drainConsumer);
    done += n;
  }
  const auto t1 = std::chrono::steady_clock::now();
  r.wallNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
  return r;
}

PassResult CacheHarnessBase::sweep(
    WorkloadDriver& driver,
    uint64_t ops,
    uint64_t readSize,
    const DataLayout& layout) {
  const auto ioStats = std::make_shared<io::IoStatistics>();
  PassResult r = runSweep(driver, ops, readSize, layout, ioStats);
  fillTiers(r, *ioStats);
  return r;
}

PassResult CacheHarnessBase::sweepConcurrent(
    Workload workload,
    uint64_t opsPerThread,
    uint64_t readSize,
    const DataLayout& layout,
    int32_t numThreads,
    uint64_t seed) {
  VELOX_CHECK_GT(numThreads, 0, "numThreads must be positive");
  const uint64_t keys = layout.totalKeys();
  std::vector<PassResult> perThread(numThreads);
  std::vector<std::shared_ptr<io::IoStatistics>> stats(numThreads);
  std::vector<std::thread> workers;
  workers.reserve(numThreads);
  for (int32_t t = 0; t < numThreads; ++t) {
    stats[t] = std::make_shared<io::IoStatistics>();
    workers.emplace_back([&, t]() {
      // Each thread scans the full key space from a distinct sequential start
      // (partitioned scans) and its own RNG stream, so concurrent workers do
      // not fully alias the same offsets.
      WorkloadDriver driver{
          workload, keys, readSize, seed + static_cast<uint64_t>(t),
          /*baseOffset=*/0};
      perThread[t] =
          runSweep(driver, opsPerThread, readSize, layout, stats[t]);
    });
  }
  for (auto& w : workers) {
    w.join();
  }
  // Aggregate: wall is the slowest worker (true wall of the concurrent run);
  // requested + tier bytes are summed across workers.
  PassResult agg;
  for (int32_t t = 0; t < numThreads; ++t) {
    agg.wallNs = std::max(agg.wallNs, perThread[t].wallNs);
    agg.requestedBytes += perThread[t].requestedBytes;
    PassResult tierR;
    fillTiers(tierR, *stats[t]);
    agg.tiers.ramBytes += tierR.tiers.ramBytes;
    agg.tiers.ssdBytes += tierR.tiers.ssdBytes;
    agg.tiers.sourceBytes += tierR.tiers.sourceBytes;
  }
  return agg;
}

// ---- DbiHarness ----

DbiHarness::DbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : CacheHarnessBase(config, files) {
  buildCache();
}

void DbiHarness::buildCache() {
  tracker_ = std::make_shared<cache::ScanTracker>(
      "chWrapperBenchTrackerDbi", nullptr, 256UL << 10);
  pool_ = memory::memoryManager()->addLeafPool("dbiChWrapperBench");
  auto& ids = fileIds();
  for (const auto& f : sourceFiles_) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    fileIds_.emplace_back(ids, f.path);
  }
}

void DbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  auto& ids = fileIds();
  StringIdLease groupId{ids, "chWrapperBenchGroup"};
  for (const auto& [fileIdx, offsets] : byFile) {
    io::ReaderOptions readerOptions{pool_.get()};
    readerOptions.setDataIoStats(ioStats);
    if (config_.cbiReadQuantumBytes > 0) {
      readerOptions.setLoadQuantum(config_.cbiReadQuantumBytes);
    }

    dwio::common::DirectBufferedInput input(
        files_[fileIdx],
        MetricsLog::voidLog(),
        fileIds_[fileIdx],
        tracker_,
        groupId,
        ioStats,
        /*ioStats=*/nullptr,
        /*executor=*/nullptr,
        readerOptions);

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    streams.reserve(offsets.size());
    for (const auto offset : offsets) {
      streams.push_back(
          input.enqueue(velox::common::Region{offset, readSize}, nullptr));
    }
    input.load(LogType::TEST);
    for (size_t i = 0; i < streams.size(); ++i) {
      consume(*streams[i], readSize, fileIdx, offsets[i]);
    }
  }
}

void DbiHarness::fillTiers(PassResult& r, io::IoStatistics& ioStats) {
  // No cache tier: every read is a source read (served by the OS page cache
  // when hot, but still attributed to the file).
  r.tiers.ramBytes = 0;
  r.tiers.ssdBytes = 0;
  r.tiers.sourceBytes = ioStats.read().sum();
}

// ---- CbiHarness ----

CbiHarness::CbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : CacheHarnessBase(config, files) {
  tracker_ = std::make_shared<cache::ScanTracker>(
      "chWrapperBenchTracker", nullptr, 256UL << 10);
  pool_ = memory::memoryManager()->addLeafPool("cbiChWrapperBench");
  auto& ids = fileIds();
  for (const auto& f : sourceFiles_) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    fileIds_.emplace_back(ids, f.path);
  }
  buildCache();
}

void CbiHarness::buildCache() {
  namespace fs = std::filesystem;
  fs::remove_all(config_.ssdPath);
  fs::create_directories(config_.ssdPath);
  ssdExecutor_ =
      std::make_unique<folly::IOThreadPoolExecutor>(config_.ssdNumShards);
  const cache::SsdCache::Config ssdConfig(
      config_.ssdPath + "/cache",
      config_.ssdCacheBytes,
      config_.ssdNumShards,
      ssdExecutor_.get(),
      /*checkpointIntervalBytes=*/0);
  auto ssdCache = std::make_unique<cache::SsdCache>(ssdConfig);

  memory::MemoryAllocator::Options allocOptions;
  allocOptions.capacity = config_.ramCacheBytes;
  allocator_ = std::make_shared<memory::MmapAllocator>(allocOptions);

  cache::AsyncDataCache::Options cacheOptions;
  cacheOptions.numShards = config_.ramNumShards;
  ssdCache_ = ssdCache.get();
  cache_ = cache::AsyncDataCache::create(
      allocator_.get(), std::move(ssdCache), cacheOptions);

  loadExecutor_ =
      std::make_unique<folly::IOThreadPoolExecutor>(config_.ssdNumShards);
}

CbiHarness::~CbiHarness() {
  loadExecutor_.reset();
  if (cache_ != nullptr) {
    cache_->shutdown();
    cache_.reset();
  }
  ssdExecutor_.reset();
  allocator_.reset();
  std::error_code ec;
  std::filesystem::remove_all(config_.ssdPath, ec);
}

void CbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  auto& ids = fileIds();
  StringIdLease groupId{ids, "chWrapperBenchGroup"};
  for (const auto& [fileIdx, offsets] : byFile) {
    io::ReaderOptions readerOptions{pool_.get()};
    readerOptions.setDataIoStats(ioStats);
    readerOptions.setLoadQuantum(config_.cbiReadQuantumBytes);

    dwio::common::CachedBufferedInput input(
        files_[fileIdx],
        MetricsLog::voidLog(),
        fileIds_[fileIdx],
        cache_.get(),
        tracker_,
        groupId,
        ioStats,
        nullptr,
        loadExecutor_.get(),
        readerOptions);

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    streams.reserve(offsets.size());
    for (const auto offset : offsets) {
      streams.push_back(
          input.enqueue(velox::common::Region{offset, readSize}, nullptr));
    }
    input.load(LogType::TEST);
    for (size_t i = 0; i < streams.size(); ++i) {
      consume(*streams[i], readSize, fileIdx, offsets[i]);
    }
  }
}

void CbiHarness::fillTiers(PassResult& r, io::IoStatistics& ioStats) {
  r.tiers.ramBytes = ioStats.ramHit().sum();
  r.tiers.ssdBytes = ioStats.ssdRead().sum();
  r.tiers.sourceBytes = ioStats.read().sum();
}

// ---- FcbiHarness ----

FcbiHarness::FcbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : CacheHarnessBase(config, files) {
  pool_ = memory::memoryManager()->addLeafPool("fcbiChWrapperBench");
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  buildCache();
  for (const auto& f : sourceFiles_) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    keys_.push_back(FileCacheKey::fromPath(f.path));
  }
}

void FcbiHarness::buildCache() {
  namespace fs = std::filesystem;
  fs::remove_all(config_.filecacheRoot);
  fs::create_directories(config_.filecacheRoot);

  // CH-faithful construction: the FileCache is created through a real
  // FileCacheManager / FileCacheFactory (mirrors FileCacheSeekBenchmark's
  // setup). ch::FileCache cannot be bare-constructed; it needs the injected
  // workerPool / scheduler / openedFileCache / localFileSystem / commonUserId.
  FileCacheConfig c;
  c.path = config_.filecacheRoot;
  c.maxSize = config_.filecacheDiskBytes;
  c.maxElements = 1000000;
  c.maxFileSegmentSize = config_.fcbiSegmentBytes;
  c.boundaryAlignment = config_.fcbiSegmentBytes;
  c.reserveGranularity = 1;
  c.cachePolicy = FileCachePolicy::LRU;
  c.useSplitCache = false;
  c.backgroundDownloadThreads = 0;
  c.loadMetadataThreads = 2;
  c.loadMetadataAsynchronously = false;
  c.keepFreeSpaceSizeRatio = 0.0;
  c.keepFreeSpaceElementsRatio = 0.0;

  FileCacheManager::Options o;
  o.commonUserId = "bench-user";
  o.localFileSystem = filesystems::getFileSystem("/", nullptr);
  o.timekeeper = std::make_shared<folly::ThreadWheelTimekeeper>();
  o.initializeOnCreate = true;
  o.defaultCacheName = "wrapperBenchFc";
  o.caches.push_back({"wrapperBenchFc", c, "conf.wrapperBenchFc"});
  manager_ = FileCacheManager::create(o);
  cache_ = manager_->getDefault();
  VELOX_CHECK_NOT_NULL(cache_, "FileCacheManager did not produce a cache");
}

FcbiHarness::~FcbiHarness() {
  cache_.reset();
  if (manager_ != nullptr) {
    manager_->shutdown();
    manager_.reset();
  }
  std::error_code ec;
  std::filesystem::remove_all(config_.filecacheRoot, ec);
}

void FcbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  for (const auto& [fileIdx, offsets] : byFile) {
    FileCacheRequestContext ctx;
    ctx.queryId = "bench";
    ctx.userId = manager_->commonUserId();
    ch::FileCacheBufferedInput input(
        files_[fileIdx],
        cache_,
        keys_[fileIdx],
        cache_->getCommonOrigin(),
        FileCacheReadOptions{},
        ctx,
        MetricsLog::voidLog(),
        ioStats,
        std::make_shared<velox::IoStats>(),
        executor_.get(),
        dwio::common::ReaderOptions(pool_.get()));

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    streams.reserve(offsets.size());
    for (const auto offset : offsets) {
      streams.push_back(
          input.enqueue(velox::common::Region{offset, readSize}, nullptr));
    }
    input.load(LogType::TEST);
    for (size_t i = 0; i < streams.size(); ++i) {
      consume(*streams[i], readSize, fileIdx, offsets[i]);
    }
  }
}

void FcbiHarness::fillTiers(PassResult& r, io::IoStatistics& ioStats) {
  r.tiers.ramBytes = 0; // no RAM tier
  r.tiers.ssdBytes = ioStats.ssdRead().sum();
  r.tiers.sourceBytes = ioStats.read().sum();
}

} // namespace facebook::velox::ch::bench
