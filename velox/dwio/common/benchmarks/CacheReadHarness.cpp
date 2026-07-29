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
#include <string_view>

#include <glog/logging.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>

#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/Options.h"

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

// True when `p` is strictly nested under `base` (a proper descendant). Both
// paths must be absolute and lexically-normal. Equal paths return false.
bool isStrictSubpath(
    const std::filesystem::path& base,
    const std::filesystem::path& p) {
  auto b = base.begin();
  const auto bEnd = base.end();
  auto q = p.begin();
  const auto qEnd = p.end();
  for (; b != bEnd; ++b, ++q) {
    if (q == qEnd || *q != *b) {
      return false;
    }
  }
  // Every component of `base` matched a prefix of `p`; `p` is a proper
  // descendant only if it has at least one further component.
  return q != qEnd;
}

// Outcome of the shared root-safety policy: the normalized, absolute root
// path, whether it lies strictly under the cwd's `tmp/` subtree (exempt from
// the sentinel requirement), and whether a regular-file sentinel is present.
struct BenchmarkCacheRootInfo {
  std::filesystem::path normalized;
  bool underTmp{false};
  bool hasSentinel{false};
};

// Shared root-safety policy reused by every task-owned cache-root operation
// (destructive clear and non-destructive reuse-validation alike). `operation`
// is only used to prefix error messages so callers can be identified; the
// checks themselves -- normalization, dangerous-root rejection, and
// sentinel authentication -- are identical regardless of caller. See the
// `clearBenchmarkCacheRoot` doc comment in CacheReadHarness.h for the exact
// policy this implements. This function never creates, removes, or modifies
// filesystem entries.
BenchmarkCacheRootInfo inspectBenchmarkCacheRoot(
    const std::string& root,
    std::string_view operation) {
  namespace fs = std::filesystem;
  VELOX_USER_CHECK(
      !root.empty(), "{}: refusing to clear an empty root", operation);

  std::error_code ec;
  fs::path canonical = fs::absolute(fs::path(root), ec).lexically_normal();
  VELOX_USER_CHECK(
      !ec,
      "{}: cannot resolve cache root '{}' ({})",
      operation,
      root,
      ec.message());
  // Canonicalize away trailing separators and dot components before any
  // equality/subpath check. For a path written with a trailing separator (e.g.
  // "tmp/", "tmp//", "tmp/.", "./tmp/") libstdc++'s lexically_normal() leaves a
  // trailing *empty* filename (filename().empty()), not a "." component, so
  // `canonical` would otherwise carry a trailing separator and compare unequal
  // to the real directory path -- letting "tmp/" slip past the tmp/-parent
  // guard. Strip every trailing empty-or-dot component in a loop; the
  // has_relative_path() guard preserves the filesystem root (e.g. "/") so it is
  // still caught by the root check below.
  while (canonical.has_relative_path() &&
         (canonical.filename().empty() || canonical.filename() == ".")) {
    canonical = canonical.parent_path();
  }
  const std::string normalized = canonical.string();

  const fs::path cwd = fs::current_path(ec);
  VELOX_USER_CHECK(
      !ec,
      "{}: cannot resolve current working directory ({})",
      operation,
      ec.message());
  const fs::path tmpParent = (cwd / "tmp").lexically_normal();

  // Reject dangerous roots before touching the filesystem.
  VELOX_USER_CHECK(
      canonical != canonical.root_path(),
      "{}: refusing to clear the filesystem root '{}'",
      operation,
      normalized);
  VELOX_USER_CHECK(
      canonical != cwd,
      "{}: refusing to clear the current working directory '{}'",
      operation,
      normalized);
  VELOX_USER_CHECK(
      canonical != tmpParent,
      "{}: refusing to clear the tmp/ parent directory '{}'",
      operation,
      normalized);

  const bool underTmp = isStrictSubpath(tmpParent, canonical);
  const fs::path sentinel = canonical / kCacheSentinelName;
  // Authenticate the sentinel via its symlink status (do NOT follow symlinks):
  // only a *regular file* named exactly kCacheSentinelName authorizes a
  // destructive external reset. A directory, symlink, fifo, socket or device
  // must not be accepted, otherwise an attacker (or an accidental mkdir) could
  // forge authorization for an external root. symlink_status classifies the
  // link itself, so a symlink -- even one pointing at a real regular file -- is
  // rejected.
  const fs::file_status sentinelStatus = fs::symlink_status(sentinel, ec);
  // A non-existent sentinel yields status_error/file_not_found in ec; that is an
  // expected "no sentinel" outcome, not a hard error, so only a genuine stat
  // failure on an existing entry is fatal below.
  const bool sentinelMissing = sentinelStatus.type() == fs::file_type::not_found;
  VELOX_USER_CHECK(
      !ec || sentinelMissing,
      "{}: cannot stat sentinel under '{}' ({})",
      operation,
      normalized,
      ec.message());
  const bool hasSentinel =
      sentinelStatus.type() == fs::file_type::regular;

  if (!underTmp) {
    VELOX_USER_CHECK(
        hasSentinel,
        "{}: refusing to clear external cache root '{}' "
        "without the sentinel file '{}'; create it first to authorize "
        "destructive resets (Task 018-D protocol)",
        operation,
        normalized,
        kCacheSentinelName);
  }
  return {canonical, underTmp, hasSentinel};
}
} // namespace

void clearBenchmarkCacheRoot(const std::string& root) {
  namespace fs = std::filesystem;
  const auto info =
      inspectBenchmarkCacheRoot(root, "clearBenchmarkCacheRoot");
  std::error_code ec;
  const fs::path& canonical = info.normalized;
  const std::string normalized = canonical.string();
  const bool hasSentinel = info.hasSentinel;

  if (!fs::exists(canonical, ec)) {
    VELOX_USER_CHECK(
        !ec,
        "clearBenchmarkCacheRoot: cannot stat cache root '{}' ({})",
        normalized,
        ec.message());
    return;
  }

  if (hasSentinel) {
    // Remove every child except the sentinel so an 018-D trap can still
    // authenticate and remove the run directory afterwards. Collect the paths
    // first so a mid-iteration error surfaces explicitly instead of throwing
    // from the range-based loop.
    std::vector<fs::path> children;
    for (fs::directory_iterator it(canonical, ec), end; it != end;
         it.increment(ec)) {
      VELOX_USER_CHECK(
          !ec,
          "clearBenchmarkCacheRoot: cannot enumerate cache root '{}' ({})",
          normalized,
          ec.message());
      children.push_back(it->path());
    }
    VELOX_USER_CHECK(
        !ec,
        "clearBenchmarkCacheRoot: cannot enumerate cache root '{}' ({})",
        normalized,
        ec.message());
    for (const auto& child : children) {
      if (child.filename() == kCacheSentinelName) {
        continue;
      }
      fs::remove_all(child, ec);
      VELOX_USER_CHECK(
          !ec,
          "clearBenchmarkCacheRoot: failed to remove '{}' under '{}' ({})",
          child.string(),
          normalized,
          ec.message());
    }
  } else {
    fs::remove_all(canonical, ec);
    VELOX_USER_CHECK(
        !ec,
        "clearBenchmarkCacheRoot: failed to remove cache root '{}' ({})",
        normalized,
        ec.message());
  }
}

std::string validateBenchmarkCacheRootForReuse(const std::string& root) {
  namespace fs = std::filesystem;
  const auto info =
      inspectBenchmarkCacheRoot(root, "validateBenchmarkCacheRootForReuse");

  std::error_code ec;
  const fs::file_status rootStatus = fs::symlink_status(info.normalized, ec);
  VELOX_USER_CHECK(
      !ec && rootStatus.type() != fs::file_type::not_found,
      "validateBenchmarkCacheRootForReuse: cache root '{}' does not exist",
      info.normalized.string());
  VELOX_USER_CHECK(
      rootStatus.type() == fs::file_type::directory,
      "validateBenchmarkCacheRootForReuse: cache root '{}' is not a "
      "directory",
      info.normalized.string());

  // Enumerate only the top level: any entry other than the sentinel or a
  // stale "status" file counts as cache payload. This never recurses and
  // never touches the filesystem beyond reading directory entries.
  bool hasPayload = false;
  for (fs::directory_iterator it(info.normalized, ec), end; it != end;
       it.increment(ec)) {
    VELOX_USER_CHECK(
        !ec,
        "validateBenchmarkCacheRootForReuse: cannot enumerate '{}' ({})",
        info.normalized.string(),
        ec.message());
    const auto name = it->path().filename();
    if (name != kCacheSentinelName && name != "status") {
      hasPayload = true;
      break;
    }
  }
  VELOX_USER_CHECK(
      !ec,
      "validateBenchmarkCacheRootForReuse: cannot enumerate '{}' ({})",
      info.normalized.string(),
      ec.message());
  VELOX_USER_CHECK(
      hasPayload,
      "validateBenchmarkCacheRootForReuse: cache root '{}' has no cache "
      "payload",
      info.normalized.string());
  return info.normalized.string();
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
  tracker_ = std::make_shared<cache::ScanTracker>(
      "wrapperBenchTracker", nullptr, 256UL << 10);
  pool_ = memory::memoryManager()->addLeafPool("cbiWrapperBench");
  auto& ids = fileIds();
  for (const auto& f : files) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    fileIds_.emplace_back(ids, f.path);
  }
  buildCache();
  if (config_.requireResidentCache) {
    // measure phase: the SsdCache must have reloaded a checkpoint. An empty
    // cache means the dir was missing/cold or the checkpoint failed to load;
    // fail loud instead of silently re-reading from source.
    VELOX_CHECK_GT(
        ssdCache_->stats().entriesCached,
        0,
        "--phase=hot: SSD cache at {} is empty (no checkpoint reloaded); "
        "run --phase=cold first",
        config_.ssdPath);
  }
}

void CbiHarness::buildCache() {
  namespace fs = std::filesystem;
  if (config_.clearCacheOnStart) {
    clearBenchmarkCacheRoot(config_.ssdPath);
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

  loadExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
      config_.loadThreads > 0 ? config_.loadThreads : config_.ssdNumShards);
}

void CbiHarness::clearCache() {
  // Tear down the cache stack in the same order as the destructor, then rebuild
  // it empty. buildCache() wipes ssdPath when clearCacheOnStart is set, so the
  // next sweep re-downloads from source and repopulates RAM+SSD from scratch.
  if (ssdCache_ != nullptr) {
    const auto ssd = ssdCache_->stats();
    LOG(INFO) << "[cbi-cold] wiping cache (pre-pass): ssd entriesCached="
              << ssd.entriesCached << " bytesCached=" << (ssd.bytesCached >> 20)
              << "MiB -> removing " << config_.ssdPath;
  }
  loadExecutor_.reset();
  if (cache_ != nullptr) {
    cache_->shutdown();
    cache_.reset();
  }
  ssdCache_ = nullptr;
  ssdExecutor_.reset();
  allocator_.reset();
  buildCache();
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
    // A destructor must not throw; the sentinel-aware clear can, so log and
    // continue rather than terminating during stack unwinding.
    try {
      clearBenchmarkCacheRoot(config_.ssdPath);
    } catch (const std::exception& e) {
      LOG(ERROR) << "[cbi] cache-root cleanup failed: " << e.what();
    }
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
  // Durable checkpoints for --reuse_cache are written once at destruction by
  // SsdCache::shutdown() (force-checkpoints every shard when checkpointing is
  // enabled), so flush() only persists RAM->SSD per warm chunk here. A no-op
  // when checkpointing is off (the default).
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
  pool_ = memory::memoryManager()->addLeafPool("fcbiWrapperBench");
  filesystems::registerLocalFileSystem();
  timekeeper_ = std::make_shared<folly::ThreadWheelTimekeeper>();
  executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  for (const auto& f : files) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    keys_.push_back(ch::FileCacheKey::fromPath(f.path));
  }
  buildCache();
  if (config_.requireResidentCache) {
    // measure phase: the Manager must have reloaded segment metadata. No used
    // bytes means the dir was missing/cold; fail loud. Valid because the harness
    // loads metadata synchronously (loadMetadataAsynchronously=false); an async
    // load would need awaiting here.
    VELOX_CHECK_GT(
        residentBytes(),
        0,
        "--phase=hot: FileCache at {} is empty (no metadata reloaded); "
        "run --phase=cold first",
        config_.filecacheRoot);
  }
}

uint64_t FcbiHarness::residentBytes() const {
  uint64_t used = 0;
  for (const auto& [name, stats] : manager_->refreshStats().cachesByName) {
    used += stats.usedSize;
  }
  return used;
}

void FcbiHarness::buildCache() {
  namespace fs = std::filesystem;
  if (config_.clearCacheOnStart) {
    clearBenchmarkCacheRoot(config_.filecacheRoot);
  }
  fs::create_directories(config_.filecacheRoot);
  // FileCacheManager cache paths must be absolute (validateOptions).
  const std::string root = fs::absolute(config_.filecacheRoot).string();

  ch::FileCacheConfig cfg;
  cfg.path = root;
  cfg.maxSize = config_.filecacheDiskBytes;
  if (config_.fcbiSegmentBytes > 0) {
    cfg.maxFileSegmentSize = config_.fcbiSegmentBytes;
    cfg.boundaryAlignment = config_.fcbiSegmentBytes;
  }
  // Synchronous download keeps the verify sweep deterministic (no background
  // worker races); the correctness gate reads each segment right after load().
  cfg.backgroundDownloadThreads = 0;

  ch::FileCacheManager::Options opts;
  opts.caches = {{.name = "default", .config = cfg, .configPath = root}};
  opts.defaultCacheName = "default";
  opts.commonUserId = "benchmark";
  opts.cachePathPrefix = root;
  opts.allowedCacheRoot = root;
  opts.localFileSystem =
      filesystems::getFileSystem(root, {});
  opts.memoryPool = pool_.get();
  opts.timekeeper = timekeeper_;
  opts.initializeOnCreate = true;

  fileSystem_ = opts.localFileSystem;
  manager_ = ch::FileCacheManager::create(std::move(opts));
  cache_ = manager_->getDefault();
}

void FcbiHarness::teardownCache() {
  cache_.reset();
  if (manager_ != nullptr) {
    manager_->shutdown();
    manager_.reset();
  }
}

void FcbiHarness::clearCache() {
  // Drop the live Manager and rebuild it empty. buildCache() wipes
  // filecacheRoot when clearCacheOnStart is set, so the next sweep re-downloads
  // from source and rewrites the on-disk segments from scratch.
  LOG(INFO) << "[fcbi-cold] wiping cache (pre-pass): usedBytes="
            << (residentBytes() >> 20) << "MiB -> removing "
            << config_.filecacheRoot;
  teardownCache();
  buildCache();
}

FcbiHarness::~FcbiHarness() {
  teardownCache();
  if (config_.cleanupOnDestroy) {
    // A destructor must not throw; the sentinel-aware clear can, so log and
    // continue rather than terminating during stack unwinding.
    try {
      clearBenchmarkCacheRoot(config_.filecacheRoot);
    } catch (const std::exception& e) {
      LOG(ERROR) << "[fcbi] cache-root cleanup failed: " << e.what();
    }
  }
}

PassResult FcbiHarness::sweep(
    WorkloadDriver& driver,
    uint64_t ops,
    uint64_t readSize,
    const DataLayout& layout) {
  const auto ioStats = std::make_shared<io::IoStatistics>();
  const auto before = ch::takeFileCacheStatsSnapshot();
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
    const auto delta = ch::takeFileCacheStatsSnapshot() - before;
    LOG(ERROR) << "[fcbi-stats] hits=" << delta.cacheHitCount
               << " misses=" << delta.cacheMissCount
               << " writeBytes=" << delta.cacheWriteBytes
               << " onDisk=" << residentBytes()
               << " evict=" << delta.evictedBytes;
  }
  return r;
}

void FcbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  for (const auto& [fileIdx, offsets] : byFile) {
    dwio::common::ReaderOptions readerOptions(pool_.get());
    ch::FileCacheRequestContext context;
    context.queryId = "verify";
    context.userId = manager_->commonUserId();
    ch::FileCacheOriginInfo origin(manager_->commonUserId(), context.userWeight);

    ch::FileCacheBufferedInput input(
        files_[fileIdx],
        cache_,
        keys_[fileIdx],
        origin,
        ch::FileCacheReadOptions{},
        context,
        MetricsLog::voidLog(),
        ioStats,
        nullptr,
        executor_.get(),
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

// ---- DbiHarness ----

DbiHarness::DbiHarness(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files)
    : config_(config) {
  tracker_ = std::make_shared<cache::ScanTracker>(
      "wrapperBenchTrackerDbi", nullptr, 256UL << 10);
  pool_ = memory::memoryManager()->addLeafPool("dbiWrapperBench");
  auto& ids = fileIds();
  for (const auto& f : files) {
    files_.push_back(std::make_shared<LocalReadFile>(f.path));
    fileIds_.emplace_back(ids, f.path);
  }
}

PassResult DbiHarness::sweep(
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
  // DirectBufferedInput has no cache tier: every read is a source read (served
  // by the OS page cache when hot, but still attributed to the file).
  r.tiers.ramBytes = 0;
  r.tiers.ssdBytes = 0;
  r.tiers.sourceBytes = ioStats->read().sum();
  return r;
}

void DbiHarness::readBatch(
    const std::map<uint32_t, std::vector<uint64_t>>& byFile,
    uint64_t readSize,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const StreamConsumer& consume) {
  auto& ids = fileIds();
  StringIdLease groupId{ids, "wrapperBenchGroup"};
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

} // namespace facebook::velox::dwio::common::bench
