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

#include "velox/dwio/common/benchmarks/CacheReadHarness.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/FileCacheBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"

namespace facebook::velox::dwio::common::bench {
namespace {
using dwio::common::LogType;
using dwio::common::MetricsLog;

constexpr uint64_t kGiB = 1ULL << 30;

// Lazily (re)builds the shared remote blob at `path`, pseudo-random with a
// fixed seed so both wrappers and the verify tool see identical source bytes.
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
  namespace fs = std::filesystem;
  WorkingSet ws;
  if (!config.dataDir.empty()) {
    for (const auto& e : fs::directory_iterator(config.dataDir)) {
      if (!e.is_regular_file()) {
        continue;
      }
      const auto name = e.path().filename().string();
      if (!name.empty() && name.front() == '.') {
        continue; // skip dotfiles such as .*.crc
      }
      if (e.path().extension() != ".parquet") {
        continue;
      }
      ws.files_.push_back(
          {e.path().string(), static_cast<uint64_t>(e.file_size())});
    }
    std::sort(
        ws.files_.begin(), ws.files_.end(), [](const auto& a, const auto& b) {
          return a.path < b.path;
        });
    VELOX_USER_CHECK(
        !ws.files_.empty(),
        "No .parquet files found in --data_dir {}",
        config.dataDir);
    const uint64_t raw = ws.rawTotalBytes();
    const uint64_t cap = config.targetBytes > 0 ? config.targetBytes : raw;
    ws.targetBytes_ = std::min(cap, raw);
  } else {
    const uint64_t blobBytes = config.remoteBytesOverride > 0
        ? config.remoteBytesOverride
        : config.targetBytes + config.scrubBytes + kGiB;
    ws.synthetic_ = true;
    ws.remotePath_ = config.remotePath;
    ws.blobBytes_ = blobBytes;
    ws.rebuildRemote_ = config.rebuildRemote;
    ws.files_.push_back({config.remotePath, blobBytes});
    ws.targetBytes_ = config.targetBytes;
  }
  return ws;
}

void WorkingSet::materialize() const {
  if (synthetic_) {
    ensureRemoteFile(remotePath_, blobBytes_, rebuildRemote_);
  }
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

// ---- CbiHarness ----

CbiHarness::CbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : config_(config) {
  namespace fs = std::filesystem;
  if (config_.clearCacheOnStart) {
    fs::remove_all(config_.ssdPath);
  }
  fs::create_directories(config_.ssdPath);
  ssdExecutor_ =
      std::make_unique<folly::IOThreadPoolExecutor>(config_.ssdNumShards);
  const cache::SsdCache::Config ssdConfig(
      config_.ssdPath + "/cache",
      config_.ssdCacheBytes,
      config_.ssdNumShards,
      ssdExecutor_.get(),
      config_.ssdCheckpointIntervalBytes);
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
  tracker_ = std::make_shared<cache::ScanTracker>(
      "wrapperBenchTracker", nullptr, 256UL << 10);
  pool_ = memory::memoryManager()->addLeafPool("cbiWrapperBench");
  auto& ids = fileIds();
  for (const auto& f : files) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    fileIds_.emplace_back(ids, f.path);
  }
}

CbiHarness::~CbiHarness() {
  loadExecutor_.reset();
  if (cache_ != nullptr) {
    cache_->shutdown();
    cache_.reset();
  }
  ssdExecutor_.reset();
  allocator_.reset();
  if (config_.cleanupOnDestroy) {
    std::error_code ec;
    std::filesystem::remove_all(config_.ssdPath, ec);
  }
}

PassResult CbiHarness::sweep(
    WorkloadDriver& driver,
    uint64_t ops,
    uint64_t readSize,
    const DataLayout& layout) {
  const auto ioStats = std::make_shared<io::IoStatistics>();
  const auto ssdBefore = ssdCache_->stats();
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
  r.tiers.ramBytes = ioStats->ramHit().sum();
  r.tiers.ssdBytes = ioStats->ssdRead().sum();
  r.tiers.sourceBytes = ioStats->read().sum();
  // SsdCache backend confirmation: prefer its bytesRead when IoStatistics
  // ssdRead is not populated by the load path.
  const auto ssdDelta = ssdCache_->stats().bytesRead - ssdBefore.bytesRead;
  if (r.tiers.ssdBytes == 0 && ssdDelta > 0) {
    r.tiers.ssdBytes = ssdDelta;
  }
  return r;
}

void CbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  auto& ids = fileIds();
  StringIdLease groupId{ids, "wrapperBenchGroup"};
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

void CbiHarness::flush() {
  ssdCache_->waitForWriteToFinish();
  if (ssdCache_->startWrite()) {
    cache_->saveToSsd(/*saveAll=*/true);
    ssdCache_->waitForWriteToFinish();
  }
}

void CbiHarness::logWarmState() const {
  const auto s = ssdCache_->stats();
  LOG(INFO) << "  cbi warm: ssd bytesCached=" << (s.bytesCached >> 20)
            << "MiB entriesCached=" << s.entriesCached
            << " bytesWritten=" << (s.bytesWritten >> 20)
            << "MiB writeDropped=" << s.writeSsdDropped
            << " writeErrors=" << s.writeSsdErrors
            << " noSpace=" << s.writeSsdNoSpaceErrors
            << " regionsEvicted=" << s.regionsEvicted;
}

// ---- FcbiHarness ----

FcbiHarness::FcbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : config_(config) {
  namespace fs = std::filesystem;
  if (config_.clearCacheOnStart) {
    fs::remove_all(config_.filecacheRoot);
  }
  fs::create_directories(config_.filecacheRoot);
  ch::FileCacheSettings settings;
  settings.path = config_.filecacheRoot;
  settings.maxSize = config_.filecacheDiskBytes;
  settings.maxFileSegmentSize = config_.fcbiSegmentBytes;
  settings.boundaryAlignment = config_.fcbiSegmentBytes;
  settings.validate();
  cache_ = std::make_unique<ch::FileCache>("wrapperBenchFc", settings);
  cache_->initialize();
  pool_ = memory::memoryManager()->addLeafPool("fcbiWrapperBench");
  for (const auto& f : files) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    keys_.push_back(ch::FileCacheKey::fromPath(f.path));
  }
}

FcbiHarness::~FcbiHarness() {
  cache_.reset();
  if (config_.cleanupOnDestroy) {
    std::error_code ec;
    std::filesystem::remove_all(config_.filecacheRoot, ec);
  }
}

PassResult FcbiHarness::sweep(
    WorkloadDriver& driver,
    uint64_t ops,
    uint64_t readSize,
    const DataLayout& layout) {
  const auto ioStats = std::make_shared<io::IoStatistics>();
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
  r.tiers.ramBytes = 0; // no RAM tier
  r.tiers.ssdBytes = ioStats->ssdRead().sum();
  r.tiers.sourceBytes = ioStats->read().sum();
  {
    const auto st = cache_->stats();
    LOG(ERROR) << "[fcbi-stats] hits=" << st.hits << " misses=" << st.misses
               << " dlBytes=" << st.downloadedBytes
               << " onDisk=" << st.bytesOnDisk << " evict=" << st.evictions;
  }
  return r;
}

void FcbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  for (const auto& [fileIdx, offsets] : byFile) {
    ch::FileCacheBufferedInput input(
        files_[fileIdx],
        *pool_,
        cache_.get(),
        keys_[fileIdx],
        ch::FileCache::getCommonOrigin(),
        ch::CreateFileSegmentSettings{},
        ioStats);

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

} // namespace facebook::velox::dwio::common::bench
