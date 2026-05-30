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

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
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
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheKey.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/caching/filecache/benchmarks/KeyGenerator.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/FileCacheBufferedInput.h"
#include "velox/dwio/common/MetricsLog.h"
#include "velox/dwio/common/SeekableInputStream.h"

DEFINE_uint64(remote_file_size_gb, 2, "Remote blob size in GiB.");
DEFINE_bool(rebuild_remote_file, false, "Force rebuilding the remote blob.");
DEFINE_uint64(ops, 200'000, "Measured ops per cell.");
DEFINE_uint64(warmup_ops, 20'000, "Warmup ops per cell.");
DEFINE_string(workloads, "sequential,zipfian,uniform", "Workload CSV.");
DEFINE_string(threads_list, "1,4,16", "Thread-count CSV.");
DEFINE_string(ws_mult_list, "0.5,2.0", "Working-set/cache-size CSV.");
DEFINE_string(remote_latency_us_list, "0,200", "Remote latency CSV in us.");
DEFINE_uint64(seed_base, 42, "Base RNG seed.");
DEFINE_string(out, "", "Markdown output path; empty writes stdout.");

namespace {

using facebook::velox::LocalReadFile;
using facebook::velox::ReadFile;
using facebook::velox::ch::FileCache;
using facebook::velox::ch::FileCacheBufferedInput;
using facebook::velox::ch::FileCacheKey;
using facebook::velox::ch::FileCacheSettings;
using facebook::velox::ch::bench::KeyGenerator;
using facebook::velox::ch::bench::Workload;
using facebook::velox::dwio::common::LogType;
using facebook::velox::dwio::common::SeekableInputStream;
namespace memory = facebook::velox::memory;

constexpr const char* kRemotePath = "/tmp/velox_ch_filecache_bench_remote.bin";
constexpr uint64_t kSegmentBytes = 1ULL << 20;
constexpr uint64_t kMaxCacheBytes = 512ULL * (1ULL << 20);

class SleepyReadFile : public ReadFile {
 public:
  SleepyReadFile(const std::string& path, uint64_t latencyUs)
      : inner_(path), latencyUs_(latencyUs) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const facebook::velox::FileIoContext& context = {}) const override {
    if (latencyUs_ != 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(latencyUs_));
    }
    auto out = inner_.pread(offset, length, buf, context);
    bytesRead_ += length;
    return out;
  }

  uint64_t size() const override { return inner_.size(); }
  uint64_t memoryUsage() const override { return inner_.memoryUsage(); }
  bool shouldCoalesce() const override { return inner_.shouldCoalesce(); }
  std::string getName() const override { return inner_.getName(); }
  uint64_t getNaturalReadSize() const override {
    return inner_.getNaturalReadSize();
  }

 private:
  mutable LocalReadFile inner_;
  const uint64_t latencyUs_;
};

std::string benchTmpRoot() {
  return "/tmp/velox_ch_filecache_bench/" + std::to_string(::getpid());
}

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
  signal(SIGINT, SIG_DFL);
  raise(SIGINT);
}

FileCacheSettings makeSettings(const std::string& path) {
  FileCacheSettings settings;
  settings.path = path;
  settings.maxSize = kMaxCacheBytes;
  settings.maxFileSegmentSize = kSegmentBytes;
  settings.boundaryAlignment = kSegmentBytes;
  settings.validate();
  return settings;
}

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
  uint64_t usedMB{0};
  uint64_t segCount{0};
  double p50Us{0};
  double p95Us{0};
  double p99Us{0};
  double wallSec{0};
};

struct RunStats {
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t bytesDl{0};
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

uint64_t parseU64(const std::string& s) { return std::stoull(s); }
double parseDouble(const std::string& s) { return std::stod(s); }

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

double quantileNs(std::vector<uint64_t>& v, double q) {
  if (v.empty()) {
    return 0.0;
  }
  const size_t idx =
      std::min<size_t>(v.size() - 1, static_cast<size_t>(q * v.size()));
  std::nth_element(v.begin(), v.begin() + idx, v.end());
  return static_cast<double>(v[idx]);
}

size_t drain(SeekableInputStream& stream, size_t expected) {
  size_t copied = 0;
  const void* data = nullptr;
  int32_t size = 0;
  while (copied < expected && stream.Next(&data, &size)) {
    copied += std::min<size_t>(static_cast<size_t>(size), expected - copied);
  }
  VELOX_CHECK_EQ(copied, expected, "BufferedInput stream returned short read");
  return copied;
}

class BufferedInputDriver {
 public:
  BufferedInputDriver(uint64_t workingSetKeys, int cellIdx)
      : cacheRoot_(benchTmpRoot() + "/" + std::to_string(cellIdx)),
        workingSetKeys_(workingSetKeys) {
    std::filesystem::create_directories(cacheRoot_);
    cache_ = std::make_unique<FileCache>("filecache_buffered_bench", makeSettings(cacheRoot_));
    cache_->initialize();
    pool_ = memory::memoryManager()->addLeafPool("FileCacheBufferedInputBenchmark");
  }

  ~BufferedInputDriver() {
    cache_.reset();
    std::error_code ec;
    std::filesystem::remove_all(cacheRoot_, ec);
  }

  FileCache& cache() { return *cache_; }
  memory::MemoryPool& pool() { return *pool_; }
  uint64_t workingSetKeys() const { return workingSetKeys_; }

 private:
  const std::string cacheRoot_;
  const uint64_t workingSetKeys_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<FileCache> cache_;
};

void parallelRun(
    BufferedInputDriver& driver,
    Workload workload,
    uint64_t threads,
    uint64_t opsPerThread,
    uint64_t latencyUs,
    bool recordLatency,
    uint64_t seedBase,
    std::vector<std::vector<uint64_t>>* perThreadLatencies,
    std::vector<RunStats>* perThreadStats) {
  if (recordLatency) {
    perThreadLatencies->assign(threads, {});
    for (auto& v : *perThreadLatencies) {
      v.reserve(opsPerThread);
    }
  }
  perThreadStats->assign(threads, {});
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (uint64_t t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      const uint64_t slice = std::max<uint64_t>(1, driver.workingSetKeys() / threads);
      const uint64_t universe = workload == Workload::kSequential
          ? slice
          : driver.workingSetKeys();
      const uint64_t keyOffset = workload == Workload::kSequential ? t * slice : 0;
      KeyGenerator gen{workload, universe, seedBase + t};
      auto readFile = std::make_shared<SleepyReadFile>(kRemotePath, latencyUs);
      auto* lat = recordLatency ? &(*perThreadLatencies)[t] : nullptr;
      auto& stats = (*perThreadStats)[t];
      for (uint64_t i = 0; i < opsPerThread; ++i) {
        const uint64_t offset = (keyOffset + gen.next()) * kSegmentBytes;
        const auto beforeBytes = readFile->bytesRead();
        const auto start = std::chrono::steady_clock::now();
        auto input = FileCacheBufferedInput(
            readFile,
            driver.pool(),
            &driver.cache(),
            FileCacheKey::fromPath(kRemotePath),
            FileCache::getCommonOrigin());
        auto stream = input.enqueue({offset, kSegmentBytes});
        input.load(LogType::FILE);
        (void)drain(*stream, kSegmentBytes);
        const auto end = std::chrono::steady_clock::now();
        const auto afterBytes = readFile->bytesRead();
        // hit == this reader issued no remote IO for the op. Under concurrency
        // this measures "remote IO this reader avoided", not strictly cache
        // residency: when several threads race the same cold segment, only the
        // elected downloader reads remotely; the waiters observe zero IO and
        // count as hits, so hit% skews slightly high on contended cold cells.
        if (afterBytes == beforeBytes) {
          ++stats.hits;
        } else {
          ++stats.misses;
        }
        if (lat != nullptr) {
          lat->push_back(
              std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                  .count());
        }
      }
      stats.bytesDl = readFile->bytesRead();
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
    int cellIdx) {
  const uint64_t wsKeys = static_cast<uint64_t>(
      key.wsMult * static_cast<double>(kMaxCacheBytes) /
      static_cast<double>(kSegmentBytes));
  VELOX_USER_CHECK_GT(wsKeys, 0, "ws_mult too small");
  // The working set addresses offsets up to wsKeys*kSegmentBytes; keep it within
  // the remote blob so reads never run past EOF (which would abort in pread).
  VELOX_USER_CHECK_LE(
      wsKeys * kSegmentBytes,
      FLAGS_remote_file_size_gb * (1ULL << 30),
      "Working set ({} MiB) exceeds remote_file_size_gb; raise --remote_file_size_gb "
      "or lower --ws_mult_list",
      (wsKeys * kSegmentBytes) >> 20);
  VELOX_USER_CHECK_EQ(ops % key.threads, 0, "ops must divide by threads");
  VELOX_USER_CHECK_EQ(
      warmupOps % key.threads, 0, "warmup_ops must divide by threads");
  if (key.workload == Workload::kSequential) {
    VELOX_USER_CHECK_GT(wsKeys / key.threads, 0, "Sequential slice is 0");
  }

  BufferedInputDriver driver(wsKeys, cellIdx);

  std::vector<std::vector<uint64_t>> dummyLat;
  std::vector<RunStats> dummyStats;
  parallelRun(
      driver,
      key.workload,
      key.threads,
      warmupOps / key.threads,
      key.latencyUs,
      false,
      seedBase,
      &dummyLat,
      &dummyStats);

  std::vector<std::vector<uint64_t>> mainLat;
  std::vector<RunStats> mainStats;
  const auto wallStart = std::chrono::steady_clock::now();
  parallelRun(
      driver,
      key.workload,
      key.threads,
      ops / key.threads,
      key.latencyUs,
      true,
      seedBase + key.threads,
      &mainLat,
      &mainStats);
  const double wallSec = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - wallStart)
                             .count();

  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t bytesDl = 0;
  for (const auto& s : mainStats) {
    hits += s.hits;
    misses += s.misses;
    bytesDl += s.bytesDl;
  }

  size_t total = 0;
  for (const auto& v : mainLat) {
    total += v.size();
  }
  std::vector<uint64_t> all;
  all.reserve(total);
  for (const auto& v : mainLat) {
    all.insert(all.end(), v.begin(), v.end());
  }

  CellResult r;
  r.key = key;
  r.opsPerSec = static_cast<double>(ops) / wallSec;
  r.hitRatePct = 100.0 * static_cast<double>(hits) /
      static_cast<double>(std::max<uint64_t>(1, hits + misses));
  r.bytesDlMB = bytesDl / (1ULL << 20);
  r.usedMB = driver.cache().getUsedCacheSize() / (1ULL << 20);
  r.segCount = driver.cache().getFileSegmentsNum();
  r.p50Us = quantileNs(all, 0.50) / 1000.0;
  r.p95Us = quantileNs(all, 0.95) / 1000.0;
  r.p99Us = quantileNs(all, 0.99) / 1000.0;
  r.wallSec = wallSec;
  return r;
}

void printMarkdownTable(std::ostream& os, const std::vector<CellResult>& rows) {
  os << "| workload   | threads | ws_mult | lat_us |     ops/s |  hit% |"
     << " dl_MB | used_MB | seg_count | p50_us | p95_us | p99_us | wallSec |\n"
     << "|------------|--------:|--------:|-------:|----------:|------:|"
     << "------:|--------:|----------:|-------:|-------:|-------:|--------:|\n";
  for (const auto& r : rows) {
    os << folly::sformat(
        "| {:<10} | {:>7} | {:>7.2f} | {:>6} | {:>9.0f} | {:>4.1f}% |"
        " {:>5} | {:>7} | {:>9} | {:>6.1f} | {:>6.1f} | {:>6.1f} |"
        " {:>7.3f} |\n",
        workloadName(r.key.workload),
        r.key.threads,
        r.key.wsMult,
        r.key.latencyUs,
        r.opsPerSec,
        r.hitRatePct,
        r.bytesDlMB,
        r.usedMB,
        r.segCount,
        r.p50Us,
        r.p95Us,
        r.p99Us,
        r.wallSec);
  }
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

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
  for (auto t : threadsList) {
    VELOX_USER_CHECK_GT(t, 0, "threads must be > 0");
    VELOX_USER_CHECK_EQ(FLAGS_ops % t, 0, "ops must divide by threads");
    VELOX_USER_CHECK_EQ(
        FLAGS_warmup_ops % t, 0, "warmup_ops must divide by threads");
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
          rows.push_back(runCell(k, FLAGS_warmup_ops, FLAGS_ops, FLAGS_seed_base, cellIdx));
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
