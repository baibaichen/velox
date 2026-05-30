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

// Wrapper microbench comparing the two BufferedInput read paths head-to-head,
// with cache config aligned to the e2e TPC-H A/B benchmark:
//   * cbi  = CachedBufferedInput + AsyncDataCache (RAM, MmapAllocator) +
//            SsdCache (on-disk, O_DIRECT off so reads go through the OS page
//            cache). Default 4 GiB RAM + 50 GiB SSD.
//   * fcbi = FileCacheBufferedInput + ch::FileCache (on-disk segment cache, no
//            RAM tier). Default 50 GiB disk.
//
// To isolate the *local cache read path* (the layer where the e2e cold gap
// shows up) rather than RAM hits or cold downloads, each cell runs:
//   1. warm   : sweep the target working set into the cache in RAM-sized
//      chunks, flushing RAM->SSD after each chunk (cbi only) so a target larger
//      than the RAM tier still becomes fully SSD-resident. AsyncDataCache drops
//      RAM-evicted entries not yet written to SSD, so each chunk must be
//      persisted before later chunks evict it.
//   2. measure: read the target with the cell's workload and time it. An
//      optional RAM-scrub (--scrub_gb, off by default) can evict the target out
//      of the cbi RAM tier first; it is off by default because scrub data is
//      itself cacheable and can evict the target.
// Note: cbi shards its SSD by file id, so the single synthetic blob lands in one
// shard -- keep --ssd_num_shards=1 so it can use the full SSD.
// Metrics are tier-aware *byte* counters from IoStatistics (ramHit / ssdRead /
// read==source) plus SsdCache backend bytesRead, not a single hit%. A measure
// pass with non-zero source bytes is flagged: it means the target was not fully
// cache-resident and the number does not reflect the cache read path.
//
// All sizes are gflags; defaults honor the e2e alignment (4 GiB RAM / 50 GiB
// SSD / 50 GiB disk). For a fast smoke run pass small overrides, e.g.
//   --ram_cache_gb=1 --ssd_cache_gb=4 --filecache_disk_gb=4
//   --target_ws_gb=1.5 --read_sizes_kib=1024 --workloads=sequential
//   --measure_passes=1

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <unistd.h>

#include <folly/String.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheDownloadExecutor.h"
#include "velox/common/caching/filecache/FileCacheKey.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/FileCacheBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/benchmarks/WorkloadDriver.h"

// Defined in velox/flag_definitions/flags.cpp; global default is O_DIRECT.
DECLARE_bool(velox_ssd_odirect);

DEFINE_double(ram_cache_gb, 4.0, "AsyncDataCache RAM size (GiB) for cbi.");
DEFINE_double(ssd_cache_gb, 50.0, "SsdCache size (GiB) for cbi.");
DEFINE_int32(ram_num_shards, 4, "AsyncDataCache shard count for cbi.");
DEFINE_int32(ssd_num_shards, 1,
    "SsdCache shard count for cbi. SsdCache shards by file id, so a single "
    "synthetic blob lands entirely in one shard with capacity ssd_cache_gb / "
    "ssd_num_shards. Keep this at 1 so the whole blob can use the full SSD.");
DEFINE_double(filecache_disk_gb, 50.0, "ch::FileCache disk size (GiB) for fcbi.");
DEFINE_double(read_quantum_mb, 8.0,
    "Load quantum (cbi) and segment size/alignment (fcbi), MiB. Aligns "
    "cache granularity across the two backends.");
DEFINE_double(target_ws_gb, 32.0,
    "Measured target working set (GiB). Must exceed ram_cache_gb so the "
    "measure passes read from SSD/disk, and target_ws_gb + scrub_gb must fit "
    "in the SSD/disk cache.");
DEFINE_double(scrub_gb, 0.0,
    "Optional RAM-scrub range (GiB), read before each measure pass to evict the "
    "target out of the CBI RAM tier so measured reads hit SSD. Default 0 "
    "(disabled): scrub reads are themselves cacheable and compete with / evict "
    "the target from SSD and the ch::FileCache disk tier, so enabling it can "
    "make the target non-resident. With scrub=0 the per-tier byte split "
    "(ram_MB vs ssd_MB) already shows where CBI reads land.");
DEFINE_double(warm_chunk_gb, 0.0,
    "CBI warm chunk size (GiB). The target is warmed in chunks of this size, "
    "flushing RAM->SSD (saveToSsd + waitForWriteToFinish) after each chunk so "
    "entries reach SSD before later reads evict them from RAM. 0 = "
    "ram_cache_gb / 2.");
DEFINE_double(remote_gb, 0.0,
    "Synthetic remote blob size (GiB). 0 = target_ws_gb + scrub_gb + 1 margin.");
DEFINE_bool(rebuild_remote_file, false, "Force rebuilding the remote blob.");
DEFINE_string(workloads, "sequential,zipfian", "Workload CSV: sequential,zipfian,uniform.");
DEFINE_string(read_sizes_kib, "1024,8192", "Read-size CSV in KiB.");
DEFINE_uint64(batch, 64, "Regions enqueued per BufferedInput before load().");
DEFINE_int32(measure_passes, 3, "Measure passes per cell; median is reported.");
DEFINE_string(ssd_path, "/tmp/velox_wrapper_bench_ssd", "SsdCache root for cbi.");
DEFINE_string(filecache_root, "/tmp/velox_wrapper_bench_fc", "ch::FileCache root for fcbi.");
DEFINE_string(out, "", "Markdown output path; empty writes stdout.");

namespace facebook::velox {
namespace {

using dwio::common::LogType;
using dwio::common::MetricsLog;
using dwio::common::SeekableInputStream;
using dwio::common::bench::WorkloadDriver;

constexpr const char* kRemotePath = "/tmp/velox_wrapper_bench_remote.bin";

uint64_t gbToBytes(double gb) {
  return static_cast<uint64_t>(gb * static_cast<double>(1ULL << 30));
}

uint64_t remoteBytes() {
  if (FLAGS_remote_gb > 0.0) {
    return gbToBytes(FLAGS_remote_gb);
  }
  return gbToBytes(FLAGS_target_ws_gb + FLAGS_scrub_gb + 1.0);
}

// Lazily (re)builds the shared remote blob, pseudo-random with a fixed seed.
void ensureRemoteFile() {
  namespace fs = std::filesystem;
  const uint64_t want = remoteBytes();
  if (!FLAGS_rebuild_remote_file && fs::exists(kRemotePath) &&
      fs::file_size(kRemotePath) == want) {
    return;
  }
  LOG(INFO) << "Building remote blob " << kRemotePath << " (" << want
            << " bytes)";
  std::ofstream out{kRemotePath, std::ios::binary | std::ios::trunc};
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

// Tier-aware byte counters for one measured sweep.
struct TierBytes {
  uint64_t ramBytes{0};
  uint64_t ssdBytes{0}; // local cache (SSD for cbi / disk for fcbi)
  uint64_t sourceBytes{0}; // remote/source reads -- should be ~0 when warm
};

struct PassResult {
  uint64_t wallNs{0};
  uint64_t requestedBytes{0};
  TierBytes tiers;
};

const char* workloadName(ch::bench::Workload w) {
  switch (w) {
    case ch::bench::Workload::kSequential:
      return "seq";
    case ch::bench::Workload::kZipfian:
      return "zipf";
    case ch::bench::Workload::kUniform:
      return "uni";
  }
  VELOX_UNREACHABLE();
}

ch::bench::Workload parseWorkload(const std::string& s) {
  if (s == "sequential") {
    return ch::bench::Workload::kSequential;
  }
  if (s == "zipfian") {
    return ch::bench::Workload::kZipfian;
  }
  if (s == "uniform") {
    return ch::bench::Workload::kUniform;
  }
  VELOX_USER_FAIL("Unknown workload: {}", s);
}

template <typename T>
std::vector<T> parseCsv(const std::string& csv, T (*parse)(const std::string&)) {
  std::vector<std::string> toks;
  folly::split(',', csv, toks);
  std::vector<T> out;
  for (const auto& t : toks) {
    auto s = folly::trimWhitespace(t).str();
    if (!s.empty()) {
      out.push_back(parse(s));
    }
  }
  return out;
}

uint64_t drain(SeekableInputStream& stream, uint64_t expected) {
  uint64_t copied = 0;
  const void* data = nullptr;
  int32_t size = 0;
  while (copied < expected && stream.Next(&data, &size)) {
    copied += std::min<uint64_t>(static_cast<uint64_t>(size), expected - copied);
  }
  return copied;
}

// ---- cbi: CachedBufferedInput + AsyncDataCache(RAM) + SsdCache ----
class CbiHarness {
 public:
  CbiHarness() {
    std::filesystem::remove_all(FLAGS_ssd_path);
    std::filesystem::create_directories(FLAGS_ssd_path);
    ssdExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_ssd_num_shards);
    if (gflags::GetCommandLineFlagInfoOrDie("velox_ssd_odirect").is_default) {
      FLAGS_velox_ssd_odirect = false;
    }
    const cache::SsdCache::Config ssdConfig(
        FLAGS_ssd_path + "/cache",
        gbToBytes(FLAGS_ssd_cache_gb),
        FLAGS_ssd_num_shards,
        ssdExecutor_.get(),
        /*checkpointIntervalBytes=*/0);
    auto ssdCache = std::make_unique<cache::SsdCache>(ssdConfig);

    memory::MemoryAllocator::Options allocOptions;
    allocOptions.capacity = gbToBytes(FLAGS_ram_cache_gb);
    allocator_ = std::make_shared<memory::MmapAllocator>(allocOptions);

    cache::AsyncDataCache::Options cacheOptions;
    cacheOptions.numShards = FLAGS_ram_num_shards;
    ssdCache_ = ssdCache.get();
    cache_ = cache::AsyncDataCache::create(
        allocator_.get(), std::move(ssdCache), cacheOptions);

    loadExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_ssd_num_shards);
    tracker_ = std::make_shared<cache::ScanTracker>(
        "wrapperBenchTracker", nullptr, 256UL << 10);
    pool_ = memory::memoryManager()->addLeafPool("cbiWrapperBench");
    readFile_ = std::make_shared<LocalReadFile>(kRemotePath);
  }

  ~CbiHarness() {
    loadExecutor_.reset();
    if (cache_ != nullptr) {
      cache_->shutdown();
      cache_.reset();
    }
    ssdExecutor_.reset();
    allocator_.reset();
    std::error_code ec;
    std::filesystem::remove_all(FLAGS_ssd_path, ec);
  }

  // Runs one enqueue/load/drain sweep of `driver` for `ops` operations,
  // constructing a fresh CachedBufferedInput per `batch` regions. Returns wall
  // time, requested bytes and tier-byte deltas measured via a fresh
  // IoStatistics plus SsdCache backend bytesRead.
  PassResult sweep(WorkloadDriver& driver, uint64_t ops, uint64_t readSize) {
    const auto ioStats = std::make_shared<io::IoStatistics>();
    const auto ssdBefore = ssdCache_->stats();
    PassResult r;
    r.requestedBytes = ops * readSize;
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t done = 0;
    while (done < ops) {
      const uint64_t n = std::min<uint64_t>(FLAGS_batch, ops - done);
      runBatch(driver, n, readSize, ioStats);
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

  // Forces RAM-resident, ssd-savable entries out to SSD and blocks until the
  // writes complete. Must drive the SsdCache write state machine in the
  // startWrite() -> saveToSsd() -> waitForWriteToFinish() order: saveToSsd()
  // requires a write to be in progress and SsdCache::write() asserts every
  // shard is in the writing state.
  void flush() {
    ssdCache_->waitForWriteToFinish();
    if (ssdCache_->startWrite()) {
      cache_->saveToSsd(/*saveAll=*/true);
      ssdCache_->waitForWriteToFinish();
    }
  }

  // Logs SSD residency after warming so a failed warm (drops, no-space,
  // eviction) is visible before the measure passes run.
  void logWarmState() const {
    const auto s = ssdCache_->stats();
    LOG(INFO) << "  cbi warm: ssd bytesCached=" << (s.bytesCached >> 20)
              << "MiB entriesCached=" << s.entriesCached
              << " bytesWritten=" << (s.bytesWritten >> 20)
              << "MiB writeDropped=" << s.writeSsdDropped
              << " writeErrors=" << s.writeSsdErrors
              << " noSpace=" << s.writeSsdNoSpaceErrors
              << " regionsEvicted=" << s.regionsEvicted;
  }

 private:
  void runBatch(
      WorkloadDriver& driver,
      uint64_t n,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats) {
    auto& ids = fileIds();
    StringIdLease fileId{ids, std::string{kRemotePath}};
    StringIdLease groupId{ids, "wrapperBenchGroup"};
    io::ReaderOptions readerOptions{pool_.get()};
    readerOptions.setDataIoStats(ioStats);
    readerOptions.setLoadQuantum(
        static_cast<int32_t>(FLAGS_read_quantum_mb * (1 << 20)));

    dwio::common::CachedBufferedInput input(
        readFile_,
        MetricsLog::voidLog(),
        std::move(fileId),
        cache_.get(),
        tracker_,
        std::move(groupId),
        ioStats,
        nullptr,
        loadExecutor_.get(),
        readerOptions);

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    streams.reserve(n);
    for (uint64_t i = 0; i < n; ++i) {
      streams.push_back(input.enqueue(driver.nextRegion(), nullptr));
    }
    input.load(LogType::TEST);
    for (auto& stream : streams) {
      (void)drain(*stream, readSize);
    }
  }

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> loadExecutor_;
  std::shared_ptr<memory::MmapAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  cache::SsdCache* ssdCache_{nullptr};
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<ReadFile> readFile_;
};

// ---- fcbi: FileCacheBufferedInput + ch::FileCache(disk) ----
class FcbiHarness {
 public:
  FcbiHarness() {
    std::filesystem::remove_all(FLAGS_filecache_root);
    std::filesystem::create_directories(FLAGS_filecache_root);
    const uint64_t segBytes =
        static_cast<uint64_t>(FLAGS_read_quantum_mb * (1 << 20));
    ch::FileCacheSettings settings;
    settings.path = FLAGS_filecache_root;
    settings.maxSize = gbToBytes(FLAGS_filecache_disk_gb);
    settings.maxFileSegmentSize = segBytes;
    settings.boundaryAlignment = segBytes;
    settings.validate();
    cache_ = std::make_unique<ch::FileCache>("wrapperBenchFc", settings);
    cache_->initialize();
    executor_ = std::make_unique<ch::FileCacheDownloadExecutor>(
        FLAGS_ssd_num_shards);
    pool_ = memory::memoryManager()->addLeafPool("fcbiWrapperBench");
    readFile_ = std::make_shared<LocalReadFile>(kRemotePath);
  }

  ~FcbiHarness() {
    cache_.reset();
    std::error_code ec;
    std::filesystem::remove_all(FLAGS_filecache_root, ec);
  }

  PassResult sweep(WorkloadDriver& driver, uint64_t ops, uint64_t readSize) {
    const auto ioStats = std::make_shared<io::IoStatistics>();
    PassResult r;
    r.requestedBytes = ops * readSize;
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t done = 0;
    while (done < ops) {
      const uint64_t n = std::min<uint64_t>(FLAGS_batch, ops - done);
      runBatch(driver, n, readSize, ioStats);
      done += n;
    }
    const auto t1 = std::chrono::steady_clock::now();
    r.wallNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    r.tiers.ramBytes = 0; // no RAM tier
    r.tiers.ssdBytes = ioStats->ssdRead().sum();
    r.tiers.sourceBytes = ioStats->read().sum();
    return r;
  }

  // ch::FileCache writes to disk synchronously during load, so warmed segments
  // are already disk-resident; no RAM->disk flush step is needed.
  void flush() {}

  void logWarmState() const {}

 private:
  void runBatch(
      WorkloadDriver& driver,
      uint64_t n,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats) {
    // FileCacheBufferedInput::enqueue accumulates regions, so use a fresh
    // instance per batch.
    ch::FileCacheBufferedInput input(
        readFile_,
        *pool_,
        cache_.get(),
        executor_.get(),
        ch::FileCacheKey::fromPath(kRemotePath),
        ch::FileCache::getCommonOrigin(),
        ch::CreateFileSegmentSettings{},
        ioStats);

    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    streams.reserve(n);
    for (uint64_t i = 0; i < n; ++i) {
      streams.push_back(input.enqueue(driver.nextRegion(), nullptr));
    }
    input.load(LogType::TEST);
    for (auto& stream : streams) {
      (void)drain(*stream, readSize);
    }
  }

  std::unique_ptr<ch::FileCache> cache_;
  std::unique_ptr<ch::FileCacheDownloadExecutor> executor_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<ReadFile> readFile_;
};

struct CellSpec {
  ch::bench::Workload workload;
  uint64_t readSize;
};

struct WrapperRow {
  std::string wrapper;
  PassResult result;
};

struct CellRow {
  CellSpec spec;
  WrapperRow cbi;
  WrapperRow fcbi;
};

double wallMs(const PassResult& r) {
  return r.wallNs / 1'000'000.0;
}

double throughputMBs(const PassResult& r) {
  if (r.wallNs == 0) {
    return 0.0;
  }
  return static_cast<double>(r.requestedBytes) / (1ULL << 20) /
      (r.wallNs / 1e9);
}

PassResult medianByWall(std::vector<PassResult> runs) {
  VELOX_CHECK(!runs.empty());
  std::sort(runs.begin(), runs.end(), [](const auto& a, const auto& b) {
    return a.wallNs < b.wallNs;
  });
  return runs[runs.size() / 2];
}

// Runs chunked-warm (with RAM->SSD flush) + optional RAM-scrub + measure for
// one wrapper, returns the median measure pass.
template <typename Harness>
PassResult runWrapper(Harness& h, const CellSpec& spec) {
  const uint64_t targetBytes = gbToBytes(FLAGS_target_ws_gb);
  const uint64_t scrubBytes = gbToBytes(FLAGS_scrub_gb);
  const uint64_t targetKeys = targetBytes / spec.readSize;
  const uint64_t scrubKeys = scrubBytes / spec.readSize;
  VELOX_USER_CHECK_GT(targetKeys, 0, "target_ws_gb too small for read size");

  // Warm in RAM-sized chunks, flushing RAM->SSD after each chunk so a target
  // larger than the RAM tier still becomes fully cache-resident: AsyncDataCache
  // drops RAM-evicted entries that have not yet been written to SSD, so the
  // whole target must be persisted before later chunks evict it from RAM.
  const double chunkGb =
      FLAGS_warm_chunk_gb > 0.0 ? FLAGS_warm_chunk_gb : FLAGS_ram_cache_gb / 2.0;
  const uint64_t chunkKeys =
      std::max<uint64_t>(1, gbToBytes(chunkGb) / spec.readSize);
  uint64_t warmed = 0;
  while (warmed < targetKeys) {
    const uint64_t n = std::min<uint64_t>(chunkKeys, targetKeys - warmed);
    WorkloadDriver warm{
        ch::bench::Workload::kSequential, n, spec.readSize,
        /*seed=*/1, /*baseOffset=*/warmed * spec.readSize};
    (void)h.sweep(warm, n, spec.readSize);
    h.flush();
    warmed += n;
  }
  h.logWarmState();

  std::vector<PassResult> passes;
  passes.reserve(FLAGS_measure_passes);
  for (int p = 0; p < FLAGS_measure_passes; ++p) {
    // Optional: scrub the RAM tier with a disjoint range so measured reads come
    // from SSD rather than RAM. Disabled by default (see --scrub_gb) because the
    // scrub data is itself cacheable and can evict the target.
    if (scrubKeys > 0) {
      WorkloadDriver scrub{
          ch::bench::Workload::kSequential, scrubKeys, spec.readSize,
          /*seed=*/7, /*baseOffset=*/targetBytes};
      (void)h.sweep(scrub, scrubKeys, spec.readSize);
    }
    WorkloadDriver measure{
        spec.workload, targetKeys, spec.readSize,
        /*seed=*/static_cast<uint64_t>(1 + p),
        /*baseOffset=*/0};
    passes.push_back(h.sweep(measure, targetKeys, spec.readSize));
  }
  return medianByWall(std::move(passes));
}

std::string mibStr(uint64_t bytes) {
  return std::to_string(bytes >> 20);
}

void writeTable(std::ostream& os, const std::vector<CellRow>& rows) {
  os << "| pattern | read | wrapper | wall_ms | MB/s | ram_MB | ssd_MB"
     << " | src_MB | Δ vs cbi |\n"
     << "|---|---|---|---:|---:|---:|---:|---:|---:|\n";
  auto line = [&](const CellSpec& spec, const WrapperRow& w, double deltaPct,
                  bool hasDelta) {
    const auto& r = w.result;
    os << "| " << workloadName(spec.workload) << " | "
       << (spec.readSize >> 10) << "K | " << w.wrapper << " | " << std::fixed
       << std::setprecision(1) << wallMs(r) << " | " << std::setprecision(0)
       << throughputMBs(r) << " | " << mibStr(r.tiers.ramBytes) << " | "
       << mibStr(r.tiers.ssdBytes) << " | " << mibStr(r.tiers.sourceBytes)
       << " | ";
    if (hasDelta) {
      os << std::showpos << std::setprecision(1) << deltaPct << "%"
         << std::noshowpos;
    } else {
      os << "—";
    }
    os << " |\n";
  };
  for (const auto& row : rows) {
    line(row.spec, row.cbi, 0.0, false);
    const double cbiMs = wallMs(row.cbi.result);
    const double delta = cbiMs == 0.0
        ? 0.0
        : 100.0 * (wallMs(row.fcbi.result) - cbiMs) / cbiMs;
    line(row.spec, row.fcbi, delta, true);
  }
}

void cleanup() {
  std::error_code ec;
  std::filesystem::remove_all(FLAGS_ssd_path, ec);
  std::filesystem::remove_all(FLAGS_filecache_root, ec);
}

void onSigint(int /*signo*/) {
  cleanup();
  signal(SIGINT, SIG_DFL);
  raise(SIGINT);
}

} // namespace
} // namespace facebook::velox

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);

  using namespace facebook::velox;
  filesystems::registerLocalFileSystem();
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  VELOX_USER_CHECK_GT(
      FLAGS_target_ws_gb, FLAGS_ram_cache_gb,
      "target_ws_gb must exceed ram_cache_gb so measure passes read from the "
      "SSD/disk cache rather than RAM");
  VELOX_USER_CHECK_LE(
      FLAGS_target_ws_gb + FLAGS_scrub_gb, FLAGS_ssd_cache_gb,
      "target_ws_gb + scrub_gb must fit in ssd_cache_gb");
  VELOX_USER_CHECK_LE(
      FLAGS_target_ws_gb + FLAGS_scrub_gb, FLAGS_filecache_disk_gb,
      "target_ws_gb + scrub_gb must fit in filecache_disk_gb");

  signal(SIGINT, onSigint);
  ensureRemoteFile();

  const auto workloads = parseCsv<ch::bench::Workload>(
      FLAGS_workloads, parseWorkload);
  const auto readKib = parseCsv<uint64_t>(
      FLAGS_read_sizes_kib, [](const std::string& s) -> uint64_t {
        return std::stoull(s);
      });
  VELOX_USER_CHECK(!workloads.empty(), "--workloads is empty");
  VELOX_USER_CHECK(!readKib.empty(), "--read_sizes_kib is empty");

  std::vector<CellSpec> cells;
  for (auto w : workloads) {
    for (auto kib : readKib) {
      cells.push_back(CellSpec{w, kib << 10});
    }
  }

  std::vector<CellRow> rows;
  rows.reserve(cells.size());
  for (const auto& spec : cells) {
    LOG(INFO) << "cell workload=" << workloadName(spec.workload)
              << " read=" << (spec.readSize >> 10) << "KiB";
    CbiHarness cbi;
    const auto cbiRes = runWrapper(cbi, spec);
    FcbiHarness fcbi;
    const auto fcbiRes = runWrapper(fcbi, spec);
    rows.push_back(CellRow{
        spec, WrapperRow{"cbi", cbiRes}, WrapperRow{"fcbi", fcbiRes}});
    LOG(INFO) << "  cbi wall=" << wallMs(cbiRes) << "ms ssd_MB="
              << (cbiRes.tiers.ssdBytes >> 20)
              << " src_MB=" << (cbiRes.tiers.sourceBytes >> 20)
              << " | fcbi wall=" << wallMs(fcbiRes) << "ms ssd_MB="
              << (fcbiRes.tiers.ssdBytes >> 20)
              << " src_MB=" << (fcbiRes.tiers.sourceBytes >> 20);
    if (cbiRes.tiers.sourceBytes > 0 || fcbiRes.tiers.sourceBytes > 0) {
      LOG(WARNING) << "  non-zero source bytes: target not fully cache-resident;"
                   << " increase ssd/disk size or lower target_ws_gb";
    }
  }

  if (FLAGS_out.empty()) {
    writeTable(std::cout, rows);
  } else {
    std::ofstream out{FLAGS_out};
    VELOX_USER_CHECK(out.good(), "Failed to open --out path: {}", FLAGS_out);
    writeTable(out, rows);
    LOG(INFO) << "Wrote " << rows.size() << " cells to " << FLAGS_out;
  }

  cleanup();
  return 0;
}
