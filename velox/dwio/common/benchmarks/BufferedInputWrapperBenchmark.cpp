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
// All sizes are gflags; defaults honor the e2e alignment (4 GiB RAM / 80 GiB
// SSD / 80 GiB disk). For a fast smoke run pass small overrides, e.g.
//   --ram_cache_gb=1 --ssd_cache_gb=4 --filecache_disk_gb=4
//   --target_ws_gb=1.5 --read_sizes_kib=1024 --workloads=sequential
//   --measure_passes=1

#include <algorithm>
#include <chrono>
#include <ctime>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <string>
#include <vector>

#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"
#include "velox/dwio/common/benchmarks/WorkloadDriver.h"

// Defined in velox/flag_definitions/flags.cpp; global default is O_DIRECT.
DECLARE_bool(velox_ssd_odirect);

DEFINE_double(ram_cache_gb, 4.0, "AsyncDataCache RAM size (GiB) for cbi.");
DEFINE_double(ssd_cache_gb, 80.0, "SsdCache size (GiB) for cbi.");
DEFINE_int32(ram_num_shards, 4, "AsyncDataCache shard count for cbi.");
DEFINE_int32(ssd_num_shards, 1,
    "SsdCache shard count for cbi (also sizes cbi's ssd/load IO thread pools). "
    "SsdCache shards by file id, so a single synthetic blob lands entirely in "
    "one shard with capacity ssd_cache_gb / ssd_num_shards. Keep this at 1 so "
    "the whole blob can use the full SSD. This is a cbi-only knob; fcbi "
    "downloads cache misses synchronously on the calling thread (CH-faithful, "
    "no separate download pool).");
DEFINE_double(filecache_disk_gb, 80.0, "ch::FileCache disk size (GiB) for fcbi.");
DEFINE_double(cbi_read_quantum_mb, 8.0,
    "cbi load quantum (cache granularity), MiB.");
DEFINE_double(fcbi_segment_mb, 4.0,
    "fcbi segment size and boundary alignment (ch::FileCache "
    "maxFileSegmentSize / boundaryAlignment), MiB. Defaults to 4 MiB to match "
    "ch::FileCache's native boundary alignment.");
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
DEFINE_string(wrappers, "both",
    "Which wrappers to run: 'cbi', 'fcbi', or 'both'. Running a single wrapper "
    "is useful for profiling (the sampler then sees only that read path).");
DEFINE_string(ssd_path, "/tmp/velox_wrapper_bench_ssd", "SsdCache root for cbi.");
DEFINE_string(filecache_root, "/tmp/velox_wrapper_bench_fc", "ch::FileCache root for fcbi.");
DEFINE_string(out, "", "Markdown output path; empty writes stdout.");
#ifndef WRAPPER_BENCH_REPORT_DIR
#define WRAPPER_BENCH_REPORT_DIR "."
#endif
DEFINE_string(report_dir, WRAPPER_BENCH_REPORT_DIR,
    "Directory for the per-run markdown report (effective config + results). A "
    "timestamped bench_run_<YYYYMMDD_HHMMSS>.md is written here on every run. "
    "Defaults to the benchmark source dir; set empty to disable.");
DEFINE_string(data_dir, "",
    "If set, read the real *.parquet files in this directory (e.g. a TPC-H "
    "lineitem dir) instead of the synthetic blob. Each file gets its own cache "
    "key, so files distribute across SSD shards like the e2e workload. The "
    "working set is the union of the files, capped to target_ws_gb (0 = all). "
    "Reads are opaque byte ranges, mirroring the Parquet reader's IO layer.");

namespace facebook::velox {
namespace {

using dwio::common::bench::CbiHarness;
using dwio::common::bench::DataLayout;
using dwio::common::bench::FcbiHarness;
using dwio::common::bench::HarnessConfig;
using dwio::common::bench::PassResult;
using dwio::common::bench::SourceFile;
using dwio::common::bench::WorkingSet;
using dwio::common::bench::WorkingSetConfig;
using dwio::common::bench::WorkloadDriver;

constexpr const char* kRemotePath = "/tmp/velox_wrapper_bench_remote.bin";

uint64_t gbToBytes(double gb) {
  return static_cast<uint64_t>(gb * static_cast<double>(1ULL << 30));
}

// The working set and harness config are built once in main() and read through
// these file-scope pointers by the report/run helpers (which still source the
// rest of their values straight from FLAGS).
const WorkingSet* gWorkingSet = nullptr;
const HarnessConfig* gHarnessConfig = nullptr;

const std::vector<SourceFile>& dataFiles() {
  return gWorkingSet->files();
}

uint64_t rawTotalBytes() {
  return gWorkingSet->rawTotalBytes();
}

uint64_t effectiveTargetBytes() {
  return gWorkingSet->effectiveTargetBytes();
}

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
  bool hasCbi{false};
  bool hasFcbi{false};
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
  const uint64_t targetBytes = effectiveTargetBytes();
  const uint64_t scrubBytes = gbToBytes(FLAGS_scrub_gb);
  const DataLayout layout{dataFiles(), spec.readSize, targetBytes};
  const uint64_t targetKeys = layout.totalKeys();
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
    (void)h.sweep(warm, n, spec.readSize, layout);
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
      (void)h.sweep(scrub, scrubKeys, spec.readSize, layout);
    }
    WorkloadDriver measure{
        spec.workload, targetKeys, spec.readSize,
        /*seed=*/static_cast<uint64_t>(1 + p),
        /*baseOffset=*/0};
    passes.push_back(h.sweep(measure, targetKeys, spec.readSize, layout));
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
    if (row.hasCbi) {
      line(row.spec, row.cbi, 0.0, false);
    }
    if (row.hasFcbi) {
      const double cbiMs = wallMs(row.cbi.result);
      const bool hasDelta = row.hasCbi && cbiMs != 0.0;
      const double delta =
          hasDelta ? 100.0 * (wallMs(row.fcbi.result) - cbiMs) / cbiMs : 0.0;
      line(row.spec, row.fcbi, delta, hasDelta);
    }
  }
}

// Renders the effective per-run configuration (the actual FLAGS values used)
// as a markdown section, so every report is self-documenting. Mirrors the
// cbi/fcbi/experiment grouping in cbi_vs_fcbi_full_config.md.
void writeConfig(std::ostream& os) {
  const uint64_t targetBytes = effectiveTargetBytes();
  const double warmChunkGb = FLAGS_warm_chunk_gb > 0.0
      ? FLAGS_warm_chunk_gb
      : FLAGS_ram_cache_gb / 2.0;
  os << "## Effective configuration\n\n";
  os << "Data source: "
     << (FLAGS_data_dir.empty()
             ? "synthetic blob"
             : (FLAGS_data_dir + " (" + std::to_string(dataFiles().size()) +
                " file(s), " + std::to_string(rawTotalBytes() >> 30) +
                " GiB raw)"))
     << "  \n";
  os << "Working set: " << (targetBytes >> 20) << " MiB\n\n";

  os << "### CBI (AsyncDataCache RAM + SsdCache)\n\n";
  os << "| option | value |\n|---|---|\n";
  os << "| ram_cache_gb | " << FLAGS_ram_cache_gb << " |\n";
  os << "| ram_num_shards | " << FLAGS_ram_num_shards << " |\n";
  os << "| ssd_cache_gb | " << FLAGS_ssd_cache_gb << " |\n";
  os << "| ssd_num_shards | " << FLAGS_ssd_num_shards << " |\n";
  os << "| cbi_read_quantum_mb | " << FLAGS_cbi_read_quantum_mb << " |\n";
  os << "| velox_ssd_odirect | " << (FLAGS_velox_ssd_odirect ? "true" : "false")
     << " |\n";
  os << "| ssd_path | " << FLAGS_ssd_path << " |\n\n";

  os << "### FCBI (ch::FileCache + OS page cache)\n\n";
  os << "| option | value |\n|---|---|\n";
  os << "| filecache_disk_gb | " << FLAGS_filecache_disk_gb << " |\n";
  os << "| fcbi_segment_mb | " << FLAGS_fcbi_segment_mb << " |\n";
  os << "| fcbi_download | synchronous (CH-faithful, serial foreground) |\n";
  os << "| filecache_root | " << FLAGS_filecache_root << " |\n\n";

  os << "### Experiment\n\n";
  os << "| option | value |\n|---|---|\n";
  os << "| wrappers | " << FLAGS_wrappers << " |\n";
  os << "| target_ws_gb | " << FLAGS_target_ws_gb << " |\n";
  os << "| scrub_gb | " << FLAGS_scrub_gb << " |\n";
  os << "| warm_chunk_gb (effective) | " << warmChunkGb << " |\n";
  os << "| measure_passes | " << FLAGS_measure_passes << " |\n";
  os << "| workloads | " << FLAGS_workloads << " |\n";
  os << "| read_sizes_kib | " << FLAGS_read_sizes_kib << " |\n";
  os << "| batch | " << FLAGS_batch << " |\n\n";
}

// Local timestamp YYYYMMDD_HHMMSS for the report filename.
std::string runTimestamp() {
  const auto now = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  std::tm tm{};
  localtime_r(&now, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%d_%H%M%S", &tm);
  return buf;
}

// Writes the per-run report (config + results) to
// report_dir/bench_run_<timestamp>.md. No-op when --report_dir is empty.
void writeReport(const std::vector<CellRow>& rows) {
  if (FLAGS_report_dir.empty()) {
    return;
  }
  std::error_code ec;
  std::filesystem::create_directories(FLAGS_report_dir, ec);
  const std::string ts = runTimestamp();
  const std::filesystem::path path =
      std::filesystem::path(FLAGS_report_dir) / ("bench_run_" + ts + ".md");
  std::ofstream os{path};
  if (!os.good()) {
    LOG(WARNING) << "Failed to open report path: " << path.string();
    return;
  }
  os << "# velox_bufferedinput_wrapper_benchmark run " << ts << "\n\n";
  os << "See `cbi_vs_fcbi_full_config.md` for the meaning of each option and "
     << "the cbi/fcbi fairness caveats.\n\n";
  writeConfig(os);
  os << "## Results\n\n";
  writeTable(os, rows);
  LOG(INFO) << "Wrote run report to " << path.string();
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

  // O_DIRECT default: the e2e A/B benchmark runs the SsdCache with O_DIRECT off
  // so cbi reads go through the OS page cache. Apply the same default here
  // unless the user set --velox_ssd_odirect explicitly. Must run before the
  // harnesses construct their SsdCache.
  if (gflags::GetCommandLineFlagInfoOrDie("velox_ssd_odirect").is_default) {
    FLAGS_velox_ssd_odirect = false;
  }

  // Build the working set once (synthetic: a deterministic blob; data_dir: a
  // sorted scan of the real *.parquet files) and the cache config the harnesses
  // read. All sizes are converted from the gb/mb flags to bytes here.
  WorkingSetConfig wsConfig;
  wsConfig.dataDir = FLAGS_data_dir;
  wsConfig.remotePath = kRemotePath;
  wsConfig.targetBytes = gbToBytes(FLAGS_target_ws_gb);
  wsConfig.scrubBytes = gbToBytes(FLAGS_scrub_gb);
  wsConfig.remoteBytesOverride =
      FLAGS_remote_gb > 0.0 ? gbToBytes(FLAGS_remote_gb) : 0;
  wsConfig.rebuildRemote = FLAGS_rebuild_remote_file;
  const WorkingSet workingSet = WorkingSet::create(wsConfig);
  gWorkingSet = &workingSet;

  HarnessConfig harnessConfig;
  harnessConfig.ssdPath = FLAGS_ssd_path;
  harnessConfig.ssdCacheBytes = gbToBytes(FLAGS_ssd_cache_gb);
  harnessConfig.ssdNumShards = FLAGS_ssd_num_shards;
  harnessConfig.ramCacheBytes = gbToBytes(FLAGS_ram_cache_gb);
  harnessConfig.ramNumShards = FLAGS_ram_num_shards;
  harnessConfig.cbiReadQuantumBytes =
      static_cast<int32_t>(FLAGS_cbi_read_quantum_mb * (1 << 20));
  harnessConfig.ssdCheckpointIntervalBytes = 0;
  harnessConfig.filecacheRoot = FLAGS_filecache_root;
  harnessConfig.filecacheDiskBytes = gbToBytes(FLAGS_filecache_disk_gb);
  harnessConfig.fcbiSegmentBytes =
      static_cast<uint64_t>(FLAGS_fcbi_segment_mb * (1 << 20));
  harnessConfig.batch = FLAGS_batch;
  harnessConfig.clearCacheOnStart = true;
  harnessConfig.cleanupOnDestroy = true;
  gHarnessConfig = &harnessConfig;

  // Validate that the working set exceeds RAM (so measure passes hit the
  // SSD/disk tier) and fits in both caches. In data_dir mode the working set is
  // the union of the real files capped by target_ws_gb, so validate in bytes.
  const uint64_t targetBytes = effectiveTargetBytes();
  const uint64_t scrubBytes = gbToBytes(FLAGS_scrub_gb);
  VELOX_USER_CHECK_GT(
      targetBytes, gbToBytes(FLAGS_ram_cache_gb),
      "working set ({} bytes) must exceed ram_cache_gb so measure passes read "
      "from the SSD/disk cache rather than RAM", targetBytes);
  VELOX_USER_CHECK_LE(
      targetBytes + scrubBytes, gbToBytes(FLAGS_ssd_cache_gb),
      "working set + scrub_gb must fit in ssd_cache_gb");
  VELOX_USER_CHECK_LE(
      targetBytes + scrubBytes, gbToBytes(FLAGS_filecache_disk_gb),
      "working set + scrub_gb must fit in filecache_disk_gb");
  VELOX_USER_CHECK(
      FLAGS_data_dir.empty() || FLAGS_scrub_gb == 0.0,
      "--scrub_gb is not supported with --data_dir (the scrub range has no "
      "backing file)");

  signal(SIGINT, onSigint);
  // Build the synthetic blob (if any) only after the config passed validation,
  // so a misconfigured run fails fast without a wasted multi-GiB write.
  workingSet.materialize();
  if (!FLAGS_data_dir.empty()) {
    LOG(INFO) << "Reading " << dataFiles().size() << " real file(s) from "
              << FLAGS_data_dir << " (" << (rawTotalBytes() >> 30)
              << " GiB raw, working set " << (targetBytes >> 30) << " GiB)";
  }

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

  const bool runCbi = FLAGS_wrappers == "both" || FLAGS_wrappers == "cbi";
  const bool runFcbi = FLAGS_wrappers == "both" || FLAGS_wrappers == "fcbi";
  VELOX_USER_CHECK(
      runCbi || runFcbi,
      "--wrappers must be 'cbi', 'fcbi', or 'both' (got '{}')", FLAGS_wrappers);

  std::vector<CellRow> rows;
  rows.reserve(cells.size());
  for (const auto& spec : cells) {
    LOG(INFO) << "cell workload=" << workloadName(spec.workload)
              << " read=" << (spec.readSize >> 10) << "KiB";
    CellRow row{spec};
    if (runCbi) {
      CbiHarness cbi{*gHarnessConfig, dataFiles()};
      row.cbi = WrapperRow{"cbi", runWrapper(cbi, spec)};
      row.hasCbi = true;
    }
    if (runFcbi) {
      FcbiHarness fcbi{*gHarnessConfig, dataFiles()};
      row.fcbi = WrapperRow{"fcbi", runWrapper(fcbi, spec)};
      row.hasFcbi = true;
    }
    const auto& cbiRes = row.cbi.result;
    const auto& fcbiRes = row.fcbi.result;
    LOG(INFO) << "  "
              << (runCbi ? "cbi wall=" + std::to_string(wallMs(cbiRes)) +
                      "ms ssd_MB=" + std::to_string(cbiRes.tiers.ssdBytes >> 20) +
                      " src_MB=" + std::to_string(cbiRes.tiers.sourceBytes >> 20)
                         : std::string{})
              << (runCbi && runFcbi ? " | " : "")
              << (runFcbi ? "fcbi wall=" + std::to_string(wallMs(fcbiRes)) +
                      "ms ssd_MB=" + std::to_string(fcbiRes.tiers.ssdBytes >> 20) +
                      " src_MB=" + std::to_string(fcbiRes.tiers.sourceBytes >> 20)
                          : std::string{});
    if ((runCbi && cbiRes.tiers.sourceBytes > 0) ||
        (runFcbi && fcbiRes.tiers.sourceBytes > 0)) {
      LOG(WARNING) << "  non-zero source bytes: target not fully cache-resident;"
                   << " increase ssd/disk size or lower target_ws_gb";
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

  writeReport(rows);

  cleanup();
  return 0;
}
