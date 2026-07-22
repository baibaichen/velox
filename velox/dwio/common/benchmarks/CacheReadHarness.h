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

// Reusable cache-read harness shared by the BufferedInput wrapper microbench
// and the standalone velox_cache_verify tool. It owns the working-set
// resolution (synthetic blob or a real --data_dir), the flat-key DataLayout,
// and the per-wrapper read harnesses (cbi = CachedBufferedInput +
// AsyncDataCache + SsdCache; fcbi = FileCacheBufferedInput + ch::FileCache).
//
// The harnesses are decoupled from gflags: each binary parses its own flags and
// fills a HarnessConfig / WorkingSetConfig. Cleanup policy is explicit in the
// config so a persisted/reused cache (verify, two-phase runs) can opt out of
// the benchmark's wipe-on-start/destroy behavior. The actual read is driven
// through a StreamConsumer so callers can either discard bytes (benchmark) or
// capture them (verify) without duplicating the wrapper-construction logic.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <folly/Executor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

// folly::Timekeeper is forward-declared by FileCacheManager.h (included below);
// a shared_ptr member only needs the incomplete type, and the concrete
// ThreadWheelTimekeeper header is pulled in by CacheReadHarness.cpp.

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/StringIdMap.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/benchmarks/WorkloadDriver.h"

namespace facebook::velox {
namespace memory {
class MemoryPool;
class MmapAllocator;
} // namespace memory
namespace cache {
class AsyncDataCache;
} // namespace cache
namespace filesystems {
class FileSystem;
} // namespace filesystems
} // namespace facebook::velox

namespace facebook::velox::dwio::common::bench {

// One readable source file in the working set.
struct SourceFile {
  std::string path;
  uint64_t size;
};

// Default synthetic-blob path. Shared by the wrapper microbench and the
// velox_cache_verify tool so both resolve the exact same source bytes when run
// in synthetic mode (no --data_dir).
inline constexpr const char* kSyntheticBlobPath =
    "tmp/velox_wrapper_bench_remote.bin";

// Sentinel file that marks a directory as a benchmark-owned cache root. Task
// 018-D's cache-reset trap authenticates a run directory by the presence of
// this exact file before removing it, so every destructive reset the benchmark
// performs must preserve it. The name is part of the 018-D protocol contract;
// do not rename without updating that task.
inline constexpr const char* kCacheSentinelName =
    ".velox_benchmark_cache_sentinel";

// Destructively clears a benchmark cache root, fail-close and compatible with
// Task 018-D's sentinel protocol. This is the single shared implementation
// reused by every task-owned cache-root wipe (CacheReadHarness, the wrapper
// microbench and the A/B benchmark) so the safety policy lives in one place.
//
// `root` is normalized to an absolute, lexically-normal path with any trailing
// separator or `.` component stripped (libstdc++'s `lexically_normal` leaves a
// trailing empty filename for inputs like "tmp/" or "tmp/.", so this is done
// explicitly before any equality/subpath check). The clear is then governed by:
//   * Dangerous roots are always rejected: empty, the filesystem root, the
//     current working directory, and the `tmp/` directory directly under the
//     cwd (its parent). Trailing-separator spellings ("tmp/", "tmp//", "tmp/.",
//     "./tmp/") normalize to the same path and are rejected identically.
//     Rejection throws before anything is deleted.
//   * If the normalized root is strictly under the cwd's `tmp/` subtree, it may
//     be cleared without a sentinel (this keeps the default synthetic gate
//     self-contained).
//   * Otherwise an existing sentinel that is a *regular file* at
//     `<root>/.velox_benchmark_cache_sentinel` is required; without it the call
//     throws before deleting anything (fail-close for external roots). The
//     sentinel is authenticated by its symlink status, so a directory, symlink,
//     fifo or other non-regular entry with that name does NOT authorize a clear.
//   * When a sentinel is present, every child of the root except the sentinel
//     is removed and the sentinel is preserved, so an 018-D trap can still
//     authenticate and remove the run directory afterwards. When no sentinel is
//     present (only reachable for a `tmp/` subtree root) the whole root is
//     removed.
// Filesystem errors are never swallowed: the call throws with the normalized
// path in the message.
void clearBenchmarkCacheRoot(const std::string& root);

// Tier-aware byte counters for one measured sweep. sourceBytes should be ~0
// when the working set is fully cache-resident.
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

// Maps a flat block-key space onto the (possibly multi-file) working set. Each
// file contributes floor(size / readSize) fixed-size blocks (tail remainder
// dropped so no read straddles a file). A flat key in [0, totalKeys()) resolves
// to the original source-file index holding it and the byte offset within that
// file.
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
    uint32_t fileIdx; // index into the original `files` vector
    uint64_t offset;
  };

  Loc resolve(uint64_t key) const;

 private:
  const uint64_t readSize_;
  uint64_t total_{0};
  // prefix_[i] = cumulative blocks before the i-th *included* file;
  // prefix_.back() == total_. fileIndices_[i] is the original index of that
  // included file, so files that contribute zero blocks don't shift the
  // mapping.
  std::vector<uint64_t> prefix_;
  std::vector<uint32_t> fileIndices_;
};

// Describes the working set to resolve.
struct WorkingSetConfig {
  // Empty => single synthetic blob at `remotePath`; otherwise scan this
  // directory's *.parquet files (sorted) as the source set.
  std::string dataDir;
  std::string remotePath;
  // Working-set cap in bytes. Synthetic mode uses it directly; data_dir mode
  // caps the union of files to it (0 = all of it).
  uint64_t targetBytes{0};
  // Extra disjoint bytes the synthetic blob must cover for an optional RAM
  // scrub range past the target (data_dir mode ignores it).
  uint64_t scrubBytes{0};
  // Explicit synthetic blob size in bytes; 0 => targetBytes + scrubBytes + 1
  // GiB (the headroom the scrub range needs past the target).
  uint64_t remoteBytesOverride{0};
  // Force rebuilding the synthetic blob even if a same-size file exists.
  bool rebuildRemote{false};
};

// Resolves the source files for a run. Synthetic mode records the deterministic
// blob's path/size without writing it; data_dir mode scans the directory. Call
// materialize() after config validation to actually build the synthetic blob.
class WorkingSet {
 public:
  static WorkingSet create(const WorkingSetConfig& config);

  // Builds the synthetic blob on disk (no-op in data_dir mode). Separated from
  // create() so callers can validate config before paying a multi-GiB write.
  void materialize() const;

  const std::vector<SourceFile>& files() const {
    return files_;
  }

  uint64_t rawTotalBytes() const;

  // Working-set size in bytes, independent of read size (synthetic: the
  // requested target; data_dir: min(target cap, raw)).
  uint64_t effectiveTargetBytes() const {
    return targetBytes_;
  }

 private:
  std::vector<SourceFile> files_;
  uint64_t targetBytes_{0};
  // Synthetic-blob materialization state (unused in data_dir mode).
  bool synthetic_{false};
  std::string remotePath_;
  uint64_t blobBytes_{0};
  bool rebuildRemote_{false};
};

// Cache sizing/placement for the harnesses. All sizes are bytes (each binary
// converts its own gb/mb flags). Cleanup policy is explicit so persisted/reused
// caches can opt out of the wipe-on-start/destroy behavior.
struct HarnessConfig {
  // cbi: AsyncDataCache (RAM) + SsdCache.
  std::string ssdPath;
  uint64_t ssdCacheBytes{0};
  int32_t ssdNumShards{1};
  uint64_t ramCacheBytes{0};
  int32_t ramNumShards{4};
  // cbi read (load) IO thread pool size. 0 means "use ssdNumShards" (legacy
  // behavior). Set to 1 to force a serial read path while keeping the SSD
  // cache sharded for checkpoint reload.
  int32_t loadThreads{0};
  int32_t cbiReadQuantumBytes{0};
  // Non-zero makes the SsdCache durable across restarts (checkpoint). The
  // benchmark leaves it 0 (no checkpoint); two-phase/verify runs set it.
  uint64_t ssdCheckpointIntervalBytes{0};

  // fcbi: ch::FileCache (on-disk segments, no RAM tier).
  std::string filecacheRoot;
  uint64_t filecacheDiskBytes{0};
  uint64_t fcbiSegmentBytes{0};

  // Shared.
  uint64_t batch{64};
  bool clearCacheOnStart{true};
  bool cleanupOnDestroy{true};
  // hot-only (--phase=hot): the harness must self-check at construction
  // that the persisted cache actually reloaded, and throw rather than silently
  // running cold against an empty/missing cache dir.
  bool requireResidentCache{false};
};

// Reads and discards `readSize` bytes from `stream`; returns the count copied.
uint64_t drain(SeekableInputStream& stream, uint64_t readSize);

// Consumes one fully-loaded stream for the region [offset, offset+readSize) of
// the source file `fileIdx`. The consumer drives the stream's Next() loop. The
// benchmark passes `drainConsumer` (discard); verify passes a byte-capturing
// consumer.
using StreamConsumer = std::function<void(
    SeekableInputStream& stream,
    uint64_t readSize,
    uint32_t fileIdx,
    uint64_t offset)>;

// Default StreamConsumer: drains (discards) the stream's bytes.
void drainConsumer(
    SeekableInputStream& stream,
    uint64_t readSize,
    uint32_t fileIdx,
    uint64_t offset);

// cbi: CachedBufferedInput + AsyncDataCache(RAM) + SsdCache.
class CbiHarness {
 public:
  CbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);
  ~CbiHarness();

  // Runs one enqueue/load/drain sweep of `driver` for `ops` operations,
  // constructing a fresh CachedBufferedInput per `config.batch` regions.
  // Returns wall time, requested bytes and tier-byte deltas.
  PassResult sweep(
      WorkloadDriver& driver,
      uint64_t ops,
      uint64_t readSize,
      const DataLayout& layout);

  // Reads the given offsets (grouped by source-file index) through a fresh
  // CachedBufferedInput per file and feeds each loaded stream to `consume`.
  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume);

  // Forces RAM-resident, ssd-savable entries out to SSD and blocks until done.
  void flush();

  // Tears down and rebuilds the RAM+SSD cache stack so the next sweep starts
  // cold (empty local cache). Used by --cold_each_pass to measure the
  // cache-populate path on every measure pass. The source files, scan tracker
  // and memory pool are preserved; only the cache tiers are rebuilt.
  void clearCache();

  // Logs SSD residency after warming so a failed warm is visible.
  void logWarmState() const;

 private:
  // Builds the RAM+SSD cache stack (wipes ssdPath, then constructs the
  // executors, SsdCache, allocator and AsyncDataCache). Shared by the
  // constructor and clearCache().
  void buildCache();

  const HarnessConfig config_;
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

// fcbi: FileCacheBufferedInput + ch::FileCache(disk).
class FcbiHarness {
 public:
  FcbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);
  ~FcbiHarness();

  PassResult sweep(
      WorkloadDriver& driver,
      uint64_t ops,
      uint64_t readSize,
      const DataLayout& layout);

  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume);

  // ch::FileCache writes synchronously during load; no RAM->disk flush needed.
  void flush() {}

  // Tears down and rebuilds the ch::FileCache so the next sweep starts cold
  // (empty on-disk segment cache). Used by --cold_each_pass. The source files,
  // segment keys and memory pool are preserved.
  void clearCache();

  void logWarmState() const {}

 private:
  // Builds the ch::FileCacheManager (wipes filecacheRoot, then constructs the
  // Manager and resolves its default cache). Shared by the constructor and
  // clearCache(). The Manager owns the runtime services the cache needs; the
  // harness owns the Manager so verify/microbench run without installing a
  // process-global singleton.
  void buildCache();

  // Shuts down and drops the Manager (and the default cache handle) in the
  // documented order. Shared by clearCache() and the destructor.
  void teardownCache();

  // Sum of used bytes across the Manager's caches (diagnostics + the
  // requireResidentCache check).
  uint64_t residentBytes() const;

  const HarnessConfig config_;
  // Process resources the Manager references, declared before manager_ so they
  // outlive it (members are destroyed in reverse order; teardown() drops
  // manager_ first, then these).
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<filesystems::FileSystem> fileSystem_;
  std::shared_ptr<folly::Timekeeper> timekeeper_;
  std::shared_ptr<folly::Executor> executor_;
  std::shared_ptr<ch::FileCacheManager> manager_;
  ch::FileCachePtr cache_;
  std::vector<std::shared_ptr<ReadFile>> files_;
  std::vector<ch::FileCacheKey> keys_;
};

// dbi: DirectBufferedInput, no local cache. Reads go straight to the file and
// are served by the OS page cache when hot, so this is the "no cache layer"
// baseline. All read bytes are counted as source bytes (there is no cache
// tier to attribute them to).
class DbiHarness {
 public:
  DbiHarness(const HarnessConfig& config, const std::vector<SourceFile>& files);

  PassResult sweep(
      WorkloadDriver& driver,
      uint64_t ops,
      uint64_t readSize,
      const DataLayout& layout);

  void readBatch(
      const std::map<uint32_t, std::vector<uint64_t>>& byFile,
      uint64_t readSize,
      const std::shared_ptr<io::IoStatistics>& ioStats,
      const StreamConsumer& consume);

  // Warm reads already populate the OS page cache; nothing to flush.
  void flush() {}

  // dbi has no local cache tier, so a cold reset is a no-op (kept for a uniform
  // harness interface; --cold_each_pass only targets cbi/fcbi).
  void clearCache() {}

  void logWarmState() const {}

 private:
  const HarnessConfig config_;
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::shared_ptr<ReadFile>> files_;
  std::vector<StringIdLease> fileIds_;
};

} // namespace facebook::velox::dwio::common::bench
