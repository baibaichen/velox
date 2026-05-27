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

// Phase-1 microbenchmark for FsCache::getOrSet. See
// docs/superpowers/specs/2026-05-23-fscache-microbench-design.md for the
// design and the 36-cell sweep specification.

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <folly/Format.h>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/caching/fscache/benchmarks/KeyGenerator.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace {

using namespace facebook::velox;

// ReadFile that delegates to an inner LocalReadFile and optionally sleeps
// `latencyUs_` microseconds before each pread to simulate remote IO.
// Composition is required because LocalReadFile's pread/size/preadv/
// memoryUsage/shouldCoalesce are `final`. Bumps the inherited
// ReadFile::bytesRead_ counter directly so the driver can read bytesRead()
// to measure how many bytes hit the remote.
class SleepyReadFile : public ReadFile {
 public:
  SleepyReadFile(const std::string& path, uint64_t latencyUs)
      : inner_(path), latencyUs_(latencyUs) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override {
    if (latencyUs_ != 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(latencyUs_));
    }
    auto out = inner_.pread(offset, length, buf, context);
    bytesRead_ += length;
    return out;
  }

  uint64_t size() const override {
    return inner_.size();
  }

  uint64_t memoryUsage() const override {
    return inner_.memoryUsage();
  }

  bool shouldCoalesce() const override {
    return inner_.shouldCoalesce();
  }

  std::string getName() const override {
    return inner_.getName();
  }

  uint64_t getNaturalReadSize() const override {
    return inner_.getNaturalReadSize();
  }

 private:
  mutable LocalReadFile inner_;
  const uint64_t latencyUs_;
};

} // namespace

DEFINE_uint64(
    remote_file_size_gb,
    2,
    "Size of /tmp/velox_fscache_bench_remote.bin in GiB. Rebuilt if "
    "missing or size-mismatched.");
DEFINE_bool(
    rebuild_remote_file,
    false,
    "Force rebuild of the shared remote blob even if size matches.");
DEFINE_uint64(
    ops,
    200'000,
    "Measured ops per cell. Must divide by every value in --threads_list "
    "cleanly.");
DEFINE_uint64(warmup_ops, 20'000, "Warmup ops per cell (not counted).");
DEFINE_string(
    workloads,
    "sequential,zipfian,uniform",
    "Comma-separated subset of {sequential,zipfian,uniform}.");
DEFINE_string(
    threads_list,
    "1,4,16",
    "Comma-separated thread counts to sweep.");
DEFINE_string(
    ws_mult_list,
    "0.5,2.0",
    "Comma-separated working-set / maxBytes ratios.");
DEFINE_string(
    remote_latency_us_list,
    "0,200",
    "Comma-separated SleepyReadFile sleep durations (us).");
DEFINE_string(
    out,
    "",
    "If non-empty, write the Markdown table to this path instead of "
    "stdout. glog still goes to stderr.");
DEFINE_uint64(
    seed_base,
    42,
    "Base seed; per-thread seed = seed_base + tid.");
DEFINE_double(
    min_wall_seconds,
    0.0,
    "If > 0 and a cell's main-loop wall time falls below this, re-run the "
    "main loop once with ops scaled up so the wall time reaches the "
    "threshold. Used to suppress sampling noise on high-throughput hit "
    "cells where the default --ops finishes in <100 ms. Capped at "
    "1000 * --ops to bound the worst case.");
DEFINE_uint64(
    num_files,
    1,
    "Number of distinct virtual path strings the benchmark routes through. "
    "Each path string hashes to a different PathKey and therefore a "
    "different per-bucket lock in FsCacheMetadata, so raising this above 1 "
    "disperses recordHit() contention across buckets. The working set "
    "(wsKeys) is split evenly across files. All virtual paths read from the "
    "same on-disk blob via the shared SleepyReadFile, so disk IO behavior "
    "is unchanged. Default 1 preserves prior behavior (all threads hit the "
    "same bucket).");

namespace {

constexpr const char* kRemotePath = "/tmp/velox_fscache_bench_remote.bin";

std::string benchTmpRoot() {
  return "/tmp/velox_fscache_bench/" + std::to_string(::getpid());
}

// Lazily (re)builds the shared remote blob. Rebuilt only if the file is
// missing, the size disagrees with --remote_file_size_gb, or
// --rebuild_remote_file is set. Pseudo-random content is deterministic
// (seed 0xfeedface) so repeated runs reproduce.
void ensureRemoteFile() {
  namespace fs = std::filesystem;
  const uint64_t want = FLAGS_remote_file_size_gb * (1ULL << 30);
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

void cleanupBenchTmp() {
  std::error_code ec;
  std::filesystem::remove_all(benchTmpRoot(), ec);
}

void onSigint(int /*signo*/) {
  cleanupBenchTmp();
  // Re-raise with the default handler so the process exits with the
  // conventional 128+SIGINT status instead of swallowing the signal.
  signal(SIGINT, SIG_DFL);
  raise(SIGINT);
}

using ::facebook::velox::cache::fs::FsCache;
using ::facebook::velox::cache::fs::FsCacheConfig;
using ::facebook::velox::cache::fs::bench::KeyGenerator;
using ::facebook::velox::cache::fs::bench::Workload;

constexpr uint64_t kSegmentBytes = 1ULL << 20;
constexpr uint64_t kMaxCacheBytes = 512ULL * (1ULL << 20);

struct CellKey {
  Workload workload;
  uint64_t threads;
  double wsMult;
  uint64_t latencyUs;
};

struct CellResult {
  CellKey key;
  double opsPerSec{0};
  double hitRatePct{0};
  uint64_t bytesDlMB{0};
  uint64_t evicCount{0};
  uint64_t bytesEvicMB{0};
  double p50Us{0};
  double p95Us{0};
  double p99Us{0};
  double wallSec{0};
};

const char* workloadName(Workload w) {
  switch (w) {
    case Workload::kSequential:
      return "sequential";
    case Workload::kZipfian:
      return "zipfian";
    case Workload::kUniform:
      return "uniform";
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

uint64_t parseU64(const std::string& s) {
  return std::stoull(s);
}

double parseDouble(const std::string& s) {
  return std::stod(s);
}

template <typename T>
std::vector<T> parseCsv(
    const std::string& csv,
    T (*parse)(const std::string&)) {
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

class FsCacheDriver {
 public:
  FsCacheDriver(uint64_t workingSetKeys, uint64_t latencyUs, int cellIdx)
      : cacheRoot_(benchTmpRoot() + "/" + std::to_string(cellIdx)),
        workingSetKeys_(workingSetKeys),
        sleepyReadFile_(kRemotePath, latencyUs) {
    std::filesystem::create_directories(cacheRoot_);
    FsCacheConfig cfg;
    cfg.cacheRoot = cacheRoot_;
    cfg.maxBytes = kMaxCacheBytes;
    cfg.alignment = kSegmentBytes;
    cfg.maxSegmentSize = kSegmentBytes;
    fsCache_ = std::make_unique<FsCache>(cfg);
  }

  ~FsCacheDriver() {
    fsCache_.reset();
    std::error_code ec;
    std::filesystem::remove_all(cacheRoot_, ec);
  }

  FsCache& fsCache() {
    return *fsCache_;
  }
  SleepyReadFile& sleepyReadFile() {
    return sleepyReadFile_;
  }
  uint64_t workingSetKeys() const {
    return workingSetKeys_;
  }

 private:
  const std::string cacheRoot_;
  const uint64_t workingSetKeys_;
  SleepyReadFile sleepyReadFile_;
  std::unique_ptr<FsCache> fsCache_;
};

double quantileNs(std::vector<uint64_t>& v, double q) {
  if (v.empty()) {
    return 0.0;
  }
  const size_t idx =
      std::min<size_t>(v.size() - 1, static_cast<size_t>(q * v.size()));
  std::nth_element(v.begin(), v.begin() + idx, v.end());
  return static_cast<double>(v[idx]);
}

void parallelRun(
    FsCacheDriver& driver,
    Workload workload,
    uint64_t threads,
    uint64_t opsPerThread,
    uint64_t numFiles,
    bool recordLatency,
    uint64_t seedBase,
    std::vector<std::vector<uint64_t>>* perThreadLatencies) {
  if (recordLatency) {
    perThreadLatencies->assign(threads, {});
    for (auto& v : *perThreadLatencies) {
      v.reserve(opsPerThread);
    }
  }
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (uint64_t t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      // Pick a virtual file for this thread. Each fileId maps to a
      // distinct path string ("<kRemotePath>#<fileId>") whose PathKey
      // hashes to a different FsCacheMetadata bucket. With numFiles >=
      // threads, every thread routes to its own bucket and the per-bucket
      // recordHit() lock no longer serializes the hit path.
      const uint64_t fileId = t % numFiles;
      const std::string pathStr = numFiles == 1
          ? std::string{kRemotePath}
          : std::string{kRemotePath} + "#" + std::to_string(fileId);
      // Sequential: each thread owns a disjoint slice of the per-file
      // keyspace [0, wsKeys / numFiles) by giving KeyGenerator
      // universe = slice and adding keyOffset. KeyGenerator stays
      // workload-agnostic; the per-thread offset lives here in the driver.
      // Zipfian / uniform: shared per-file keyspace, no offset.
      const uint64_t threadsPerFile = (threads + numFiles - 1) / numFiles;
      const uint64_t threadIndexInFile = t / numFiles;
      const uint64_t wsKeysPerFile = driver.workingSetKeys() / numFiles;
      uint64_t universe;
      uint64_t keyOffset;
      if (workload == Workload::kSequential) {
        const uint64_t slice = wsKeysPerFile / threadsPerFile;
        universe = slice;
        keyOffset = threadIndexInFile * slice;
      } else {
        universe = wsKeysPerFile;
        keyOffset = 0;
      }
      KeyGenerator gen{workload, universe, seedBase + t};
      auto* lat = recordLatency ? &(*perThreadLatencies)[t] : nullptr;
      for (uint64_t i = 0; i < opsPerThread; ++i) {
        const uint64_t offset = (keyOffset + gen.next()) * kSegmentBytes;
        const auto start = std::chrono::steady_clock::now();
        auto segs = driver.fsCache().getOrSet(
            pathStr, offset, kSegmentBytes, driver.sleepyReadFile());
        const auto end = std::chrono::steady_clock::now();
        (void)segs;
        if (lat != nullptr) {
          lat->push_back(
              std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count());
        }
      }
    });
  }
  for (auto& th : workers) {
    th.join();
  }
}

CellResult runCell(
    const CellKey& key,
    uint64_t warmupOps,
    uint64_t ops,
    uint64_t seedBase,
    int cellIdx,
    double minWallSeconds,
    uint64_t numFiles) {
  const uint64_t wsKeys = static_cast<uint64_t>(
      key.wsMult * static_cast<double>(kMaxCacheBytes) /
      static_cast<double>(kSegmentBytes));
  VELOX_USER_CHECK_GT(wsKeys, 0, "ws_mult too small for kMaxCacheBytes");
  VELOX_USER_CHECK_EQ(
      ops % key.threads,
      0,
      "ops {} must divide cleanly by threads {}",
      ops,
      key.threads);
  VELOX_USER_CHECK_GT(numFiles, 0, "num_files must be > 0");
  VELOX_USER_CHECK_GT(
      wsKeys / numFiles,
      0,
      "wsKeys {} too small to split across num_files {}",
      wsKeys,
      numFiles);
  if (key.workload == Workload::kSequential) {
    const uint64_t threadsPerFile =
        (key.threads + numFiles - 1) / numFiles;
    VELOX_USER_CHECK_GT(
        (wsKeys / numFiles) / threadsPerFile,
        0,
        "Sequential per-thread slice is 0; wsKeysPerFile {} too small "
        "for threadsPerFile {} (threads {} / num_files {})",
        wsKeys / numFiles,
        threadsPerFile,
        key.threads,
        numFiles);
  }

  FsCacheDriver driver(wsKeys, key.latencyUs, cellIdx);

  // Warmup. recordLatency=false; SleepyReadFile bytesRead_ reset below
  // so the post-warmup main loop is the only contributor to bytesDl.
  std::vector<std::vector<uint64_t>> dummyLat;
  parallelRun(
      driver,
      key.workload,
      key.threads,
      warmupOps / key.threads,
      numFiles,
      /*recordLatency=*/false,
      seedBase,
      &dummyLat);

  // Baseline snapshot AFTER warmup so deltas exclude warmup counters.
  // Without this reset, hit% can exceed 100% because warmup misses count
  // against the main loop's op total.
  auto statsBase = driver.fsCache().stats();
  driver.sleepyReadFile().resetBytesRead();

  uint64_t effectiveOps = ops;
  std::vector<std::vector<uint64_t>> mainLat;
  auto wallStart = std::chrono::steady_clock::now();
  parallelRun(
      driver,
      key.workload,
      key.threads,
      effectiveOps / key.threads,
      numFiles,
      /*recordLatency=*/true,
      seedBase + key.threads,
      &mainLat);
  double wallSec = std::chrono::duration<double>(
                       std::chrono::steady_clock::now() - wallStart)
                       .count();

  // Adaptive re-run: if the first pass finished too fast for stable
  // quantiles (e.g. high-throughput hit cells where 200k ops complete in
  // <100 ms), scale ops up to hit `minWallSeconds` and re-run once.
  // Capped at 1000x to bound worst case. The driver / FsCache state is
  // reused; we reset the baseline so the recorded deltas correspond only
  // to the longer pass.
  if (minWallSeconds > 0.0 && wallSec > 0.0 && wallSec < minWallSeconds) {
    const double scale = std::min<double>(1000.0, minWallSeconds / wallSec);
    uint64_t scaledOps =
        static_cast<uint64_t>(static_cast<double>(effectiveOps) * scale);
    // Round up to a multiple of threads so the per-thread slice is whole.
    scaledOps =
        ((scaledOps + key.threads - 1) / key.threads) * key.threads;
    LOG(INFO) << "cell " << cellIdx << " wall=" << wallSec
              << "s < min=" << minWallSeconds << "s; re-running with ops="
              << scaledOps;
    statsBase = driver.fsCache().stats();
    driver.sleepyReadFile().resetBytesRead();
    mainLat.clear();
    effectiveOps = scaledOps;
    wallStart = std::chrono::steady_clock::now();
    parallelRun(
        driver,
        key.workload,
        key.threads,
        effectiveOps / key.threads,
        numFiles,
        /*recordLatency=*/true,
        seedBase + 2 * key.threads,
        &mainLat);
    wallSec = std::chrono::duration<double>(
                  std::chrono::steady_clock::now() - wallStart)
                  .count();
  }

  const auto statsFinal = driver.fsCache().stats();
  const uint64_t hitsDelta = (statsFinal.prefetchHits + statsFinal.demandHits) -
      (statsBase.prefetchHits + statsBase.demandHits);
  const uint64_t evictionsDelta = statsFinal.evictions - statsBase.evictions;
  const uint64_t bytesReadDelta = driver.sleepyReadFile().bytesRead();

  size_t total = 0;
  for (const auto& v : mainLat) {
    total += v.size();
  }
  std::vector<uint64_t> all;
  all.reserve(total);
  for (const auto& v : mainLat) {
    all.insert(all.end(), v.begin(), v.end());
  }

  const auto& cfg = driver.fsCache().config();
  CellResult r;
  r.key = key;
  r.opsPerSec = static_cast<double>(effectiveOps) / wallSec;
  r.hitRatePct = 100.0 * static_cast<double>(hitsDelta) /
      static_cast<double>(effectiveOps);
  r.bytesDlMB = bytesReadDelta / (1ULL << 20);
  r.evicCount = evictionsDelta;
  // stats_.evictions is a COUNT of segments, not bytes. Multiply before
  // divide so a (future) sub-MiB maxSegmentSize would not silently
  // truncate to 0.
  r.bytesEvicMB = (evictionsDelta * cfg.maxSegmentSize) / (1ULL << 20);
  r.p50Us = quantileNs(all, 0.50) / 1000.0;
  r.p95Us = quantileNs(all, 0.95) / 1000.0;
  r.p99Us = quantileNs(all, 0.99) / 1000.0;
  r.wallSec = wallSec;
  return r;
}

void printMarkdownTable(
    std::ostream& os,
    const std::vector<CellResult>& rows) {
  os << "| workload   | threads | ws_mult | lat_us |     ops/s |  hit% |"
     << " dl_MB | evic_count | evic_MB | p50_us | p95_us | p99_us |"
     << " wallSec |\n"
     << "|------------|--------:|--------:|-------:|----------:|------:|"
     << "------:|-----------:|--------:|-------:|-------:|-------:|"
     << "--------:|\n";
  for (const auto& r : rows) {
    os << folly::sformat(
        "| {:<10} | {:>7} | {:>7.2f} | {:>6} | {:>9.0f} | {:>4.1f}% |"
        " {:>5} | {:>10} | {:>7} | {:>6.1f} | {:>6.1f} | {:>6.1f} |"
        " {:>7.3f} |\n",
        workloadName(r.key.workload),
        r.key.threads,
        r.key.wsMult,
        r.key.latencyUs,
        r.opsPerSec,
        r.hitRatePct,
        r.bytesDlMB,
        r.evicCount,
        r.bytesEvicMB,
        r.p50Us,
        r.p95Us,
        r.p99Us,
        r.wallSec);
  }
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();

  signal(SIGINT, onSigint);
  std::filesystem::create_directories(benchTmpRoot());
  ensureRemoteFile();

  const auto workloads = parseCsv<Workload>(FLAGS_workloads, parseWorkload);
  const auto threadsList = parseCsv<uint64_t>(FLAGS_threads_list, parseU64);
  const auto wsMultList = parseCsv<double>(FLAGS_ws_mult_list, parseDouble);
  const auto latencyList =
      parseCsv<uint64_t>(FLAGS_remote_latency_us_list, parseU64);
  VELOX_USER_CHECK(!workloads.empty(), "--workloads is empty");
  VELOX_USER_CHECK(!threadsList.empty(), "--threads_list is empty");
  VELOX_USER_CHECK(!wsMultList.empty(), "--ws_mult_list is empty");
  VELOX_USER_CHECK(!latencyList.empty(), "--remote_latency_us_list is empty");
  VELOX_USER_CHECK_GT(FLAGS_num_files, 0, "--num_files must be > 0");
  for (auto t : threadsList) {
    VELOX_USER_CHECK_GT(t, 0, "threads must be > 0");
    VELOX_USER_CHECK_EQ(
        FLAGS_ops % t,
        0,
        "ops {} must divide cleanly by threads {}",
        FLAGS_ops,
        t);
    VELOX_USER_CHECK_EQ(
        FLAGS_warmup_ops % t,
        0,
        "warmup_ops {} must divide cleanly by threads {}",
        FLAGS_warmup_ops,
        t);
  }

  std::vector<CellResult> rows;
  int cellIdx = 0;
  for (auto w : workloads) {
    for (auto th : threadsList) {
      for (auto mult : wsMultList) {
        for (auto lat : latencyList) {
          CellKey k{w, th, mult, lat};
          LOG(INFO) << "cell " << cellIdx << " workload=" << workloadName(w)
                    << " threads=" << th << " ws_mult=" << mult
                    << " lat_us=" << lat;
          rows.push_back(runCell(
              k,
              FLAGS_warmup_ops,
              FLAGS_ops,
              FLAGS_seed_base,
              cellIdx,
              FLAGS_min_wall_seconds,
              FLAGS_num_files));
          ++cellIdx;
        }
      }
    }
  }

  if (FLAGS_out.empty()) {
    printMarkdownTable(std::cout, rows);
  } else {
    std::ofstream out{FLAGS_out};
    VELOX_USER_CHECK(out.good(), "Failed to open --out path: {}", FLAGS_out);
    printMarkdownTable(out, rows);
    LOG(INFO) << "Wrote " << rows.size() << " rows to " << FLAGS_out;
  }

  cleanupBenchTmp();
  return 0;
}
