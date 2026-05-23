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
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

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
      // Sequential: each thread owns a disjoint slice of
      // [0, workingSetKeys) by giving KeyGenerator universe = slice and
      // adding keyOffset to every output. KeyGenerator stays
      // workload-agnostic; the per-thread offset lives here in the driver.
      // Zipfian / uniform: shared keyspace, no offset.
      uint64_t universe;
      uint64_t keyOffset;
      if (workload == Workload::kSequential) {
        const uint64_t slice = driver.workingSetKeys() / threads;
        universe = slice;
        keyOffset = t * slice;
      } else {
        universe = driver.workingSetKeys();
        keyOffset = 0;
      }
      KeyGenerator gen{workload, universe, seedBase + t};
      auto* lat = recordLatency ? &(*perThreadLatencies)[t] : nullptr;
      for (uint64_t i = 0; i < opsPerThread; ++i) {
        const uint64_t offset = (keyOffset + gen.next()) * kSegmentBytes;
        const auto start = std::chrono::steady_clock::now();
        auto segs = driver.fsCache().getOrSet(
            kRemotePath, offset, kSegmentBytes, driver.sleepyReadFile());
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

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();

  signal(SIGINT, onSigint);
  const std::string tmpRoot = benchTmpRoot();
  std::filesystem::create_directories(tmpRoot);

  ensureRemoteFile();
  LOG(INFO) << "Setup complete. tmpRoot=" << tmpRoot
            << " remote=" << kRemotePath;

  cleanupBenchTmp();
  return 0;
}
