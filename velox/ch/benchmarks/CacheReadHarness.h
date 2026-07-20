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

#pragma once

// Cache-read harness for the FileCache wrapper microbenchmark. It compares
// three BufferedInput read paths over an identical synthetic working set:
//   * dbi  = DirectBufferedInput (no cache layer; OS-page-cache baseline).
//   * cbi  = CachedBufferedInput + native AsyncDataCache (RAM) + SsdCache.
//   * fcbi = ch::FileCacheBufferedInput + ch::FileCache (on-disk segments),
//            constructed CH-faithfully through a real FileCacheManager /
//            FileCacheFactory (never bare `new FileCache`).
//
// Structure (dedupe decision from Task 015 post-MVP contract): an abstract
// base `CacheHarnessBase` owns ALL shared logic once — the WorkloadDriver
// (sequential / zipfian / uniform offset generation), the timed sweep loop,
// the multi-threaded concurrent driver (--num_threads), and result / delta
// printing. Three thin subclasses override only buildCache() + readBatch().
// The reference implementation (ch-filecache, velox/dwio/common/benchmarks)
// copied the sweep/driver/stats logic into each of the three harnesses; we
// lift it into the base class instead.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <folly/executors/IOThreadPoolExecutor.h>

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox {
namespace memory {
class MemoryPool;
class MmapAllocator;
} // namespace memory
namespace cache {
class AsyncDataCache;
} // namespace cache
} // namespace facebook::velox

namespace facebook::velox::ch::bench {

// ---------------------------------------------------------------------------
// KeyGenerator + WorkloadDriver (ported from the ch-filecache reference,
// KeyGenerator.h / WorkloadDriver.h). Kept header-only and inside the task's
// declared file scope (velox/ch/benchmarks/**) rather than a new shared header.
// ---------------------------------------------------------------------------

// Key-distribution shape selector.
enum class Workload { kSequential, kZipfian, kUniform };

// Per-driver key index generator. Pure logic, no IO. next() returns an index
// in [0, n). Sequential walks [seqStart, seqStart + n) modulo n; zipfian and
// uniform draw from [0, n).
class KeyGenerator {
 public:
  KeyGenerator(
      Workload workload,
      uint64_t n,
      uint64_t seed,
      uint64_t seqStart = 0,
      double zipfTheta = 1.0);

  uint64_t next();

 private:
  void buildZipfCdf(double theta);

  Workload workload_;
  uint64_t n_;
  uint64_t seqPos_;
  std::mt19937_64 rng_;
  std::vector<double> cdf_;
};

// Generates a stream of fixed-size read regions over a synthetic working set of
// `workingSetKeys` aligned blocks of `readSizeBytes`, starting at `baseOffset`.
class WorkloadDriver {
 public:
  WorkloadDriver(
      Workload workload,
      uint64_t workingSetKeys,
      uint64_t readSizeBytes,
      uint64_t seed,
      uint64_t baseOffset = 0);

  velox::common::Region nextRegion();

 private:
  const uint64_t readSizeBytes_;
  const uint64_t baseOffset_;
  KeyGenerator keyGen_;
};

// ---------------------------------------------------------------------------
// Working set + layout
// ---------------------------------------------------------------------------

// One readable source file in the working set.
struct SourceFile {
  std::string path;
  uint64_t size;
};

// Default synthetic-blob path.
inline constexpr const char* kSyntheticBlobPath =
    "/tmp/velox_ch_wrapper_bench_remote.bin";

// Tier-aware byte counters for one measured sweep.
struct TierBytes {
  uint64_t ramBytes{0};
  uint64_t ssdBytes{0}; // local cache (SSD for cbi / disk for fcbi)
  uint64_t sourceBytes{0}; // remote/source reads
};

struct PassResult {
  uint64_t wallNs{0};
  uint64_t requestedBytes{0};
  TierBytes tiers;
};

// Maps a flat block-key space onto the (possibly multi-file) working set.
class DataLayout {
 public:
  DataLayout(
      const std::vector<SourceFile>& files,
      uint64_t readSize,
      uint64_t maxBytes);

  uint64_t totalKeys() const {
    return total_;
  }

  struct Loc {
    uint32_t fileIdx;
    uint64_t offset;
  };

  Loc resolve(uint64_t key) const;

 private:
  const uint64_t readSize_;
  uint64_t total_{0};
  std::vector<uint64_t> prefix_;
  std::vector<uint32_t> fileIndices_;
};

// Describes the working set to resolve (synthetic blob only for this bench).
struct WorkingSetConfig {
  std::string remotePath;
  uint64_t targetBytes{0};
  uint64_t remoteBytesOverride{0};
  bool rebuildRemote{false};
};

// Resolves the synthetic source blob for a run.
class WorkingSet {
 public:
  static WorkingSet create(const WorkingSetConfig& config);

  void materialize() const;

  const std::vector<SourceFile>& files() const {
    return files_;
  }

  uint64_t rawTotalBytes() const;

  uint64_t effectiveTargetBytes() const {
    return targetBytes_;
  }

 private:
  std::vector<SourceFile> files_;
  uint64_t targetBytes_{0};
  std::string remotePath_;
  uint64_t blobBytes_{0};
  bool rebuildRemote_{false};
};

// Cache sizing/placement for the harnesses. All sizes are bytes.
struct HarnessConfig {
  // cbi: AsyncDataCache (RAM) + SsdCache.
  std::string ssdPath;
  uint64_t ssdCacheBytes{0};
  int32_t ssdNumShards{1};
  uint64_t ramCacheBytes{0};
  int32_t ramNumShards{4};
  int32_t cbiReadQuantumBytes{0};

  // fcbi: ch::FileCache (on-disk segments, no RAM tier).
  std::string filecacheRoot;
  uint64_t filecacheDiskBytes{0};
  uint64_t fcbiSegmentBytes{0};

  // Shared.
  uint64_t batch{64};
};

// ---------------------------------------------------------------------------
// Base harness (all shared logic lives here, once)
// ---------------------------------------------------------------------------

uint64_t drain(dwio::common::SeekableInputStream& stream, uint64_t readSize);

// Consumes one fully-loaded stream for a region. drainConsumer discards bytes.
using StreamConsumer = std::function<void(
    dwio::common::SeekableInputStream& stream,
    uint64_t readSize,
    uint32_t fileIdx,
    uint64_t offset)>;

void drainConsumer(
    dwio::common::SeekableInputStream& stream,
    uint64_t readSize,
    uint32_t fileIdx,
    uint64_t offset);

// Abstract harness base. Owns the single-threaded sweep, the multi-threaded
// concurrent driver, and the shared config/source-file plumbing. Subclasses
// override only buildCache() and readBatch().
class CacheHarnessBase {
 public:
  CacheHarnessBase(
      const HarnessConfig& config,
      const std::vector<SourceFile>& files);
  virtual ~CacheHarnessBase() = default;

  // One single-threaded enqueue/load/drain sweep of `driver` for `ops`
  // operations. Timing wraps the enqueue+load+drain work only.
  PassResult sweep(
      WorkloadDriver& driver,
      uint64_t ops,
      uint64_t readSize,
      const DataLayout& layout);

  // Multi-threaded variant: `numThreads` workers each run an independent
  // per-thread driver over the whole key space. Returns the aggregate result
  // (wall = the slowest thread, requested/tier bytes summed).
  PassResult sweepConcurrent(
      Workload workload,
      uint64_t opsPerThread,
      uint64_t readSize,
      const DataLayout& layout,
      int32_t numThreads,
      uint64_t seed);

 protected:
  // Reads the given offsets (grouped by source-file index) and feeds each
  // loaded stream to `consume`. Per-subclass wrapper construction.
  virtual void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume) = 0;

  // Populates the per-tier byte counters of `r` from the sweep's IoStatistics.
  virtual void fillTiers(
      PassResult& r,
      io::IoStatistics& ioStats) = 0;

  const HarnessConfig config_;
  std::vector<SourceFile> sourceFiles_;

 private:
  // Shared core of one sweep against a caller-provided driver + ioStats.
  PassResult
  runSweep(WorkloadDriver& driver, uint64_t ops, uint64_t readSize,
      const DataLayout& layout, const std::shared_ptr<io::IoStatistics>& ioStats);
};

// ---------------------------------------------------------------------------
// dbi: DirectBufferedInput, no local cache (OS-page-cache baseline).
// ---------------------------------------------------------------------------
class DbiHarness : public CacheHarnessBase {
 public:
  DbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);

 protected:
  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume) override;

  void fillTiers(PassResult& r, io::IoStatistics& ioStats) override;

 private:
  void buildCache();

  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::shared_ptr<ReadFile>> files_;
  std::vector<StringIdLease> fileIds_;
};

// ---------------------------------------------------------------------------
// cbi: CachedBufferedInput + AsyncDataCache(RAM) + SsdCache.
// ---------------------------------------------------------------------------
class CbiHarness : public CacheHarnessBase {
 public:
  CbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);
  ~CbiHarness() override;

 protected:
  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume) override;

  void fillTiers(PassResult& r, io::IoStatistics& ioStats) override;

 private:
  void buildCache();

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> loadExecutor_;
  std::shared_ptr<memory::MmapAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  cache::SsdCache* ssdCache_{nullptr};
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::shared_ptr<ReadFile>> files_;
  std::vector<StringIdLease> fileIds_;
};

// ---------------------------------------------------------------------------
// fcbi: ch::FileCacheBufferedInput + ch::FileCache, constructed CH-faithfully
// through a real FileCacheManager (NEVER bare `new FileCache`).
// ---------------------------------------------------------------------------
class FcbiHarness : public CacheHarnessBase {
 public:
  FcbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);
  ~FcbiHarness() override;

 protected:
  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume) override;

  void fillTiers(PassResult& r, io::IoStatistics& ioStats) override;

 private:
  void buildCache();

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<FileCacheManager> manager_;
  FileCachePtr cache_;
  std::vector<std::shared_ptr<ReadFile>> files_;
  std::vector<FileCacheKey> keys_;
};

} // namespace facebook::velox::ch::bench
