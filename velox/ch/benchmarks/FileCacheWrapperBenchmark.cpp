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

// FileCache wrapper microbenchmark: compares three BufferedInput read paths
// head-to-head over an identical synthetic working set.
//   * fcbi = ch::FileCacheBufferedInput + ch::FileCache (our port), constructed
//            through a real FileCacheManager (CH-faithful; no bare FileCache).
//   * cbi  = CachedBufferedInput + native AsyncDataCache (RAM) + SsdCache.
//   * dbi  = DirectBufferedInput, no cache layer (OS-page-cache baseline).
//
// The three engines run back-to-back in the same process per cell; results are
// printed with per-engine throughput and a relative delta against fcbi. A
// separate --num_threads variant drives each engine with that many concurrent
// reader threads.
//
// For a fast smoke run:
//   --wrappers=all --workloads=sequential,zipfian --target_ws_gb=1
//   --measure_passes=1

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <string>
#include <vector>

#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/ch/benchmarks/CacheReadHarness.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"

DEFINE_double(ram_cache_gb, 4.0, "AsyncDataCache RAM size (GiB) for cbi.");
DEFINE_double(ssd_cache_gb, 80.0, "SsdCache size (GiB) for cbi.");
DEFINE_int32(ram_num_shards, 4, "AsyncDataCache shard count for cbi.");
DEFINE_int32(
    ssd_num_shards,
    1,
    "SsdCache shard count for cbi (also sizes cbi's ssd/load IO thread pools).");
DEFINE_double(cbi_read_quantum_mb, 8.0, "cbi load quantum (cache granularity), MiB.");
DEFINE_double(
    fcbi_segment_mb,
    4.0,
    "fcbi segment size and boundary alignment (ch::FileCache "
    "maxFileSegmentSize / boundaryAlignment), MiB.");
DEFINE_double(filecache_disk_gb, 80.0, "ch::FileCache disk size (GiB) for fcbi.");
DEFINE_double(target_ws_gb, 4.0, "Measured target working set (GiB).");
DEFINE_double(
    remote_gb,
    0.0,
    "Synthetic remote blob size (GiB). 0 = target_ws_gb + 1 GiB margin.");
DEFINE_bool(rebuild_remote_file, false, "Force rebuilding the remote blob.");
DEFINE_string(workloads, "sequential,zipfian", "Workload CSV: sequential,zipfian,uniform.");
DEFINE_string(read_sizes_kib, "1024", "Read-size CSV in KiB.");
DEFINE_uint64(batch, 64, "Regions enqueued per BufferedInput before load().");
DEFINE_int32(measure_passes, 3, "Measure passes per cell; median is reported.");
DEFINE_bool(
    cold_each_pass,
    false,
    "Rebuild the cache engine before every measure pass so each pass measures "
    "the cold cache-populate path. No-op for dbi.");
DEFINE_string(
    wrappers,
    "all",
    "Which wrappers to run: 'fcbi', 'cbi', 'dbi', 'both' (fcbi+cbi) or 'all'.");
DEFINE_string(ssd_path, "/tmp/velox_ch_wrapper_bench_ssd", "SsdCache root for cbi.");
DEFINE_string(filecache_root, "/tmp/velox_ch_wrapper_bench_fc", "ch::FileCache root for fcbi.");
DEFINE_string(out, "", "Markdown output path; empty writes stdout.");
DEFINE_int32(
    num_threads,
    0,
    "If > 0, run the concurrent variant with this many reader threads per "
    "engine (each thread scans the whole working set from a distinct start). "
    "Budget <= 16 to avoid oversubscription. 0 runs the single-threaded sweep.");

namespace facebook::velox::ch::bench {
namespace {

uint64_t gbToBytes(double gb) {
  return static_cast<uint64_t>(gb * static_cast<double>(1ULL << 30));
}

const char* workloadName(Workload w) {
  switch (w) {
    case Workload::kSequential:
      return "seq";
    case Workload::kZipfian:
      return "zipf";
    case Workload::kUniform:
      return "uni";
  }
  VELOX_UNREACHABLE();
}

Workload parseWorkload(const std::string& s) {
  if (s == "sequential") {
    return Workload::kSequential;
  }
  if (s == "zipfian") {
    return Workload::kZipfian;
  }
  if (s == "uniform") {
    return Workload::kUniform;
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

struct CellSpec {
  Workload workload;
  uint64_t readSize;
};

struct WrapperRow {
  std::string wrapper;
  bool present{false};
  PassResult result;
};

struct CellRow {
  CellSpec spec;
  WrapperRow fcbi{};
  WrapperRow cbi{};
  WrapperRow dbi{};
};

double wallMs(const PassResult& r) {
  return r.wallNs / 1'000'000.0;
}

double throughputMBs(const PassResult& r) {
  if (r.wallNs == 0) {
    return 0.0;
  }
  return static_cast<double>(r.requestedBytes) / (1ULL << 20) / (r.wallNs / 1e9);
}

PassResult medianByWall(std::vector<PassResult> runs) {
  VELOX_CHECK(!runs.empty());
  std::sort(runs.begin(), runs.end(), [](const auto& a, const auto& b) {
    return a.wallNs < b.wallNs;
  });
  return runs[runs.size() / 2];
}

// Warms the working set into the cache once, then runs the measure passes and
// returns the median. --cold_each_pass rebuilds the harness before every pass
// so each pass measures the cold populate path.
template <typename Harness>
PassResult runWrapper(
    const HarnessConfig& config,
    const std::vector<SourceFile>& files,
    const CellSpec& spec,
    uint64_t targetBytes) {
  const DataLayout layout{files, spec.readSize, targetBytes};
  const uint64_t targetKeys = layout.totalKeys();
  VELOX_USER_CHECK_GT(targetKeys, 0, "target_ws_gb too small for read size");

  auto h = std::make_unique<Harness>(config, files);

  // Warm: sweep the target into the cache once.
  {
    WorkloadDriver warm{
        Workload::kSequential, targetKeys, spec.readSize, /*seed=*/1};
    (void)h->sweep(warm, targetKeys, spec.readSize, layout);
  }

  std::vector<PassResult> passes;
  passes.reserve(FLAGS_measure_passes);
  for (int p = 0; p < FLAGS_measure_passes; ++p) {
    if (FLAGS_cold_each_pass) {
      h = std::make_unique<Harness>(config, files);
      WorkloadDriver warm{
          Workload::kSequential, targetKeys, spec.readSize, /*seed=*/1};
      (void)h->sweep(warm, targetKeys, spec.readSize, layout);
    }
    if (FLAGS_num_threads > 0) {
      passes.push_back(h->sweepConcurrent(
          spec.workload,
          targetKeys,
          spec.readSize,
          layout,
          FLAGS_num_threads,
          /*seed=*/static_cast<uint64_t>(1 + p)));
    } else {
      WorkloadDriver measure{
          spec.workload,
          targetKeys,
          spec.readSize,
          /*seed=*/static_cast<uint64_t>(1 + p)};
      passes.push_back(h->sweep(measure, targetKeys, spec.readSize, layout));
    }
  }
  return medianByWall(std::move(passes));
}

std::string mibStr(uint64_t bytes) {
  return std::to_string(bytes >> 20);
}

// Prints the engines back-to-back with throughput and a relative delta against
// Classifies which cache tier actually served this engine's reads in this pass,
// derived from the per-tier byte counters. This is the key to reading the table
// honestly: a cbi "RAM" row and an fcbi "disk" row are NOT the same-layer
// comparison, so the raw delta between them is a RAM-vs-disk artifact, not a
// like-for-like result (see the footnote emitted after the table).
std::string hitLayer(const std::string& wrapper, const TierBytes& t) {
  if (wrapper == "dbi") {
    return "page-cache";
  }
  if (t.sourceBytes > 0) {
    return "source(!)"; // target not fully cache-resident; number is unreliable
  }
  if (t.ramBytes > 0) {
    return "RAM";
  }
  if (t.ssdBytes > 0) {
    return wrapper == "fcbi" ? "disk" : "SSD";
  }
  // No tier bytes recorded but no source read either: served from a local
  // segment/page whose bytes the IoStatistics load path did not attribute to a
  // tier counter (fcbi disk-segment hits land here).
  return wrapper == "fcbi" ? "disk" : "local";
}

// Prints the engines back-to-back with throughput and a relative delta against
// fcbi (fcbi is the baseline: 0% against itself). A positive delta means the
// engine's wall time is that much larger than fcbi's.
void writeTable(std::ostream& os, const std::vector<CellRow>& rows) {
  os << "| pattern | read | wrapper | hit_layer | wall_ms | MB/s | ram_MB"
     << " | ssd_MB | src_MB | delta vs fcbi |\n"
     << "|---|---|---|---|---:|---:|---:|---:|---:|---:|\n";
  auto line = [&](const CellSpec& spec, const WrapperRow& w, double deltaPct,
                  bool hasDelta) {
    const auto& r = w.result;
    os << "| " << workloadName(spec.workload) << " | " << (spec.readSize >> 10)
       << "K | " << w.wrapper << " | " << hitLayer(w.wrapper, r.tiers) << " | "
       << std::fixed << std::setprecision(1) << wallMs(r) << " | "
       << std::setprecision(0) << throughputMBs(r) << " | "
       << mibStr(r.tiers.ramBytes) << " | " << mibStr(r.tiers.ssdBytes) << " | "
       << mibStr(r.tiers.sourceBytes) << " | ";
    if (hasDelta) {
      os << std::showpos << std::setprecision(1) << deltaPct << "%"
         << std::noshowpos;
    } else {
      os << "—";
    }
    os << " |\n";
  };
  for (const auto& row : rows) {
    const double fcbiMs = wallMs(row.fcbi.result);
    const bool deltaBase = row.fcbi.present && fcbiMs != 0.0;
    auto lineVsFcbi = [&](const WrapperRow& w) {
      const double delta =
          deltaBase ? 100.0 * (wallMs(w.result) - fcbiMs) / fcbiMs : 0.0;
      line(row.spec, w, delta, deltaBase);
    };
    if (row.fcbi.present) {
      line(row.spec, row.fcbi, 0.0, false);
    }
    if (row.cbi.present) {
      lineVsFcbi(row.cbi);
    }
    if (row.dbi.present) {
      lineVsFcbi(row.dbi);
    }
  }
  os << "\n> **Read `hit_layer` before comparing engines.** The engines serve "
        "from DIFFERENT tiers: `fcbi` from its on-disk segment cache, `cbi` "
        "from its RAM tier when the working set fits `--ram_cache_gb` (spilling "
        "to `SSD` only when it does not), `dbi` from the OS page cache. When "
        "`cbi`'s hit_layer is `RAM` and `fcbi`'s is `disk`, the `delta vs fcbi` "
        "column is a RAM-vs-disk artifact and does NOT mean fcbi is slower "
        "same-for-same. For a fair local-cache comparison, force cbi onto disk "
        "by setting `--ram_cache_gb` below `--target_ws_gb` so cbi's hit_layer "
        "becomes `SSD` (disk vs disk).\n";
}


} // namespace
} // namespace facebook::velox::ch::bench

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);

  using namespace facebook::velox;
  using namespace facebook::velox::ch::bench;

  filesystems::registerLocalFileSystem();
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  VELOX_USER_CHECK_LE(
      FLAGS_num_threads, 16, "--num_threads must be <= 16 to avoid oversubscription");

  WorkingSetConfig wsConfig;
  wsConfig.remotePath = kSyntheticBlobPath;
  wsConfig.targetBytes = gbToBytes(FLAGS_target_ws_gb);
  wsConfig.remoteBytesOverride =
      FLAGS_remote_gb > 0.0 ? gbToBytes(FLAGS_remote_gb) : 0;
  wsConfig.rebuildRemote = FLAGS_rebuild_remote_file;
  const WorkingSet workingSet = WorkingSet::create(wsConfig);
  const uint64_t targetBytes = workingSet.effectiveTargetBytes();
  VELOX_USER_CHECK_GT(targetBytes, 0, "working set is empty");

  HarnessConfig config;
  // Normalize the on-disk cache roots to absolute paths. The local file system
  // matches on an absolute path; ch::FileCache and SsdCache open their segment
  // files via filesystems::getFileSystem(path), so a relative root would fail
  // scheme matching at read time ("No registered file system matched"). This
  // mirrors the same fix already in FileCacheSeekBenchmark's setupFixture.
  config.ssdPath = std::filesystem::absolute(FLAGS_ssd_path).string();
  config.ssdCacheBytes = gbToBytes(FLAGS_ssd_cache_gb);
  config.ssdNumShards = FLAGS_ssd_num_shards;
  config.ramCacheBytes = gbToBytes(FLAGS_ram_cache_gb);
  config.ramNumShards = FLAGS_ram_num_shards;
  config.cbiReadQuantumBytes =
      static_cast<int32_t>(FLAGS_cbi_read_quantum_mb * (1 << 20));
  config.filecacheRoot = std::filesystem::absolute(FLAGS_filecache_root).string();
  config.filecacheDiskBytes = gbToBytes(FLAGS_filecache_disk_gb);
  config.fcbiSegmentBytes =
      static_cast<uint64_t>(FLAGS_fcbi_segment_mb * (1 << 20));
  config.batch = FLAGS_batch;

  const bool runAll = FLAGS_wrappers == "all";
  const bool runBoth = FLAGS_wrappers == "both";
  const bool runFcbi = runAll || runBoth || FLAGS_wrappers == "fcbi";
  const bool runCbi = runAll || runBoth || FLAGS_wrappers == "cbi";
  const bool runDbi = runAll || FLAGS_wrappers == "dbi";
  VELOX_USER_CHECK(
      runFcbi || runCbi || runDbi,
      "--wrappers must be 'fcbi', 'cbi', 'dbi', 'both' or 'all' (got '{}')",
      FLAGS_wrappers);

  if (runCbi) {
    VELOX_USER_CHECK_LE(
        targetBytes, gbToBytes(FLAGS_ssd_cache_gb),
        "working set must fit in ssd_cache_gb");
  }
  if (runFcbi) {
    VELOX_USER_CHECK_LE(
        targetBytes, gbToBytes(FLAGS_filecache_disk_gb),
        "working set must fit in filecache_disk_gb");
  }

  workingSet.materialize();
  const auto& files = workingSet.files();

  const auto workloads = parseCsv<Workload>(FLAGS_workloads, parseWorkload);
  const auto readKib = parseCsv<uint64_t>(
      FLAGS_read_sizes_kib,
      [](const std::string& s) -> uint64_t { return std::stoull(s); });
  VELOX_USER_CHECK(!workloads.empty(), "--workloads is empty");
  VELOX_USER_CHECK(!readKib.empty(), "--read_sizes_kib is empty");

  std::vector<CellSpec> cells;
  for (auto w : workloads) {
    for (auto kib : readKib) {
      cells.push_back(CellSpec{w, kib << 10});
    }
  }

  if (FLAGS_num_threads > 0) {
    LOG(INFO) << "Concurrent variant: num_threads=" << FLAGS_num_threads;
  }

  std::vector<CellRow> rows;
  rows.reserve(cells.size());
  for (const auto& spec : cells) {
    LOG(INFO) << "cell workload=" << workloadName(spec.workload)
              << " read=" << (spec.readSize >> 10) << "KiB";
    CellRow row{spec};
    if (runFcbi) {
      row.fcbi = WrapperRow{
          "fcbi", true,
          runWrapper<FcbiHarness>(config, files, spec, targetBytes)};
    }
    if (runCbi) {
      row.cbi = WrapperRow{
          "cbi", true,
          runWrapper<CbiHarness>(config, files, spec, targetBytes)};
    }
    if (runDbi) {
      row.dbi = WrapperRow{
          "dbi", true,
          runWrapper<DbiHarness>(config, files, spec, targetBytes)};
    }
    rows.push_back(std::move(row));
  }

  if (FLAGS_out.empty()) {
    writeTable(std::cout, rows);
  } else {
    std::ofstream out{FLAGS_out};
    VELOX_USER_CHECK(out.good(), "Failed to open --out path: {}", FLAGS_out);
    writeTable(out, rows);
    LOG(INFO) << "Wrote " << rows.size() << " cells to " << FLAGS_out;
  }

  return 0;
}
