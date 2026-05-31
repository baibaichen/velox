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

// One-time cache-correctness gate for the two-phase persistent workflow. After
// a cold --phase=prime run of the wrapper microbench has populated and persisted
// a backend's on-disk cache, this tool reloads that exact cache and re-reads the
// whole working set through the matching BufferedInput wrapper, byte-comparing
// every segment against a fresh pread of the source file. It exits 0 iff every
// segment matched (and the working set was non-empty), non-zero on any mismatch
// or if the persisted cache did not reload (fail loud, never silently cold).
//
// Backends mirror the microbench:
//   * cbi       = CachedBufferedInput + AsyncDataCache + SsdCache (reloaded from
//                 the SsdCache checkpoint).
//   * filecache = FileCacheBufferedInput + ch::FileCache (reloaded metadata).
//   * direct    = DirectBufferedInput, no cache layer: verify degenerates to a
//                 source-vs-source sanity read (no persisted cache required).
//
// Run it with the SAME working-set / cache-shape flags as the prime run so the
// keys and source bytes line up, e.g.:
//   velox_cache_verify --backend=filecache --target_ws_gb=4 \
//       --filecache_root=/tmp/... --filecache_disk_gb=50 --read_size_kib=1024

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"
#include "velox/dwio/common/benchmarks/CacheVerify.h"

// Defined in velox/flag_definitions/flags.cpp; global default is O_DIRECT.
DECLARE_bool(velox_ssd_odirect);

DEFINE_string(backend, "filecache",
    "Which cache backend to verify: 'cbi' (AsyncDataCache + SsdCache), "
    "'filecache' (ch::FileCache) or 'direct' (DirectBufferedInput, no cache "
    "layer -> source-vs-source sanity).");
DEFINE_string(data_dir, "",
    "If set, verify the real *.parquet files in this directory instead of the "
    "synthetic blob (must match the prime run's --data_dir).");
DEFINE_double(target_ws_gb, 32.0,
    "Working set to verify (GiB). Must match the prime run.");
DEFINE_double(remote_gb, 0.0,
    "Synthetic remote blob size (GiB); 0 = target_ws_gb + 1 margin. Must match "
    "the prime run.");
DEFINE_double(read_size_kib, 1024.0,
    "Segment size for the verify sweep, KiB. Byte correctness is independent of "
    "this, so a single size suffices.");
DEFINE_uint64(batch, 64, "Regions enqueued per BufferedInput before load().");

DEFINE_string(ssd_path, "/tmp/velox_wrapper_bench_ssd", "SsdCache root for cbi.");
DEFINE_double(ssd_cache_gb, 80.0, "SsdCache size (GiB) for cbi.");
DEFINE_int32(ssd_num_shards, 1, "SsdCache shard count for cbi.");
DEFINE_double(ram_cache_gb, 4.0, "AsyncDataCache RAM size (GiB) for cbi.");
DEFINE_int32(ram_num_shards, 4, "AsyncDataCache shard count for cbi.");
DEFINE_double(cbi_read_quantum_mb, 8.0, "cbi load quantum (MiB).");
DEFINE_double(ssd_checkpoint_mb, 0.0,
    "cbi SsdCache checkpoint interval (MiB); 0 defaults to 256 MiB so the "
    "reloaded checkpoint is found.");

DEFINE_string(filecache_root, "/tmp/velox_wrapper_bench_fc",
    "ch::FileCache root for filecache.");
DEFINE_double(filecache_disk_gb, 80.0, "ch::FileCache disk size (GiB).");
DEFINE_double(fcbi_segment_mb, 4.0, "ch::FileCache segment size (MiB).");

namespace facebook::velox {
namespace {

using dwio::common::bench::CacheVerifier;
using dwio::common::bench::CbiHarness;
using dwio::common::bench::DataLayout;
using dwio::common::bench::DbiHarness;
using dwio::common::bench::FcbiHarness;
using dwio::common::bench::HarnessConfig;
using dwio::common::bench::VerifyResult;
using dwio::common::bench::WorkingSet;
using dwio::common::bench::WorkingSetConfig;

uint64_t gbToBytes(double gb) {
  return static_cast<uint64_t>(gb * static_cast<double>(1ULL << 30));
}

// The verify tool always reads a persisted cache, so it never wipes the on-disk
// state and (for cbi) gives a non-zero checkpoint interval so the reloaded
// checkpoint is actually located. 'direct' has no managed cache.
HarnessConfig buildConfig(bool requireResidentCache) {
  HarnessConfig config;
  config.ssdPath = FLAGS_ssd_path;
  config.ssdCacheBytes = gbToBytes(FLAGS_ssd_cache_gb);
  config.ssdNumShards = FLAGS_ssd_num_shards;
  config.ramCacheBytes = gbToBytes(FLAGS_ram_cache_gb);
  config.ramNumShards = FLAGS_ram_num_shards;
  config.cbiReadQuantumBytes =
      static_cast<int32_t>(FLAGS_cbi_read_quantum_mb * (1 << 20));
  const double checkpointMb =
      FLAGS_ssd_checkpoint_mb <= 0.0 ? 256.0 : FLAGS_ssd_checkpoint_mb;
  config.ssdCheckpointIntervalBytes =
      static_cast<uint64_t>(checkpointMb * (1 << 20));
  config.filecacheRoot = FLAGS_filecache_root;
  config.filecacheDiskBytes = gbToBytes(FLAGS_filecache_disk_gb);
  config.fcbiSegmentBytes =
      static_cast<uint64_t>(FLAGS_fcbi_segment_mb * (1 << 20));
  config.batch = FLAGS_batch;
  config.clearCacheOnStart = false;
  config.cleanupOnDestroy = false;
  config.requireResidentCache = requireResidentCache;
  return config;
}

template <typename Harness>
VerifyResult runVerify(
    CacheVerifier& verifier,
    const WorkingSet& workingSet,
    const HarnessConfig& config,
    uint64_t readSize) {
  Harness harness(config, workingSet.files());
  const DataLayout layout{
      workingSet.files(), readSize, workingSet.effectiveTargetBytes()};
  return verifier.verifyWorkingSet(harness, layout, readSize, config.batch);
}

int run() {
  WorkingSetConfig wsConfig;
  wsConfig.dataDir = FLAGS_data_dir;
  wsConfig.remotePath = dwio::common::bench::kSyntheticBlobPath;
  wsConfig.targetBytes = gbToBytes(FLAGS_target_ws_gb);
  wsConfig.remoteBytesOverride =
      FLAGS_remote_gb > 0.0 ? gbToBytes(FLAGS_remote_gb) : 0;
  // The prime run already materialized the source; verify only reads it.
  const WorkingSet workingSet = WorkingSet::create(wsConfig);
  VELOX_USER_CHECK_GT(
      workingSet.effectiveTargetBytes(), 0, "working set is empty");

  const auto readSize = static_cast<uint64_t>(FLAGS_read_size_kib * 1024.0);
  CacheVerifier verifier(workingSet.files());

  VerifyResult result;
  if (FLAGS_backend == "cbi") {
    result = runVerify<CbiHarness>(
        verifier, workingSet, buildConfig(/*requireResidentCache=*/true),
        readSize);
  } else if (FLAGS_backend == "filecache") {
    result = runVerify<FcbiHarness>(
        verifier, workingSet, buildConfig(/*requireResidentCache=*/true),
        readSize);
  } else if (FLAGS_backend == "direct") {
    result = runVerify<DbiHarness>(
        verifier, workingSet, buildConfig(/*requireResidentCache=*/false),
        readSize);
  } else {
    VELOX_USER_FAIL(
        "--backend must be 'cbi', 'filecache' or 'direct' (got '{}')",
        FLAGS_backend);
  }

  LOG(INFO) << "[verify] backend=" << FLAGS_backend
            << " segments=" << result.segmentsChecked
            << " bytes=" << result.bytesChecked
            << " mismatches=" << result.mismatches << " checksum=0x"
            << std::hex << result.checksum << std::dec;
  if (!result.ok()) {
    LOG(ERROR) << "[verify] FAIL: " << result.mismatches
               << " mismatching segment(s); first at fileIdx="
               << result.firstMismatchFileIdx
               << " offset=" << result.firstMismatchOffset;
    return 1;
  }
  LOG(INFO) << "[verify] PASS: all " << result.segmentsChecked
            << " segments match the source.";
  return 0;
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

  // Match the microbench / e2e: SsdCache O_DIRECT off (reads through the OS page
  // cache) unless the user set it explicitly. Must run before the cbi harness
  // constructs its SsdCache.
  if (gflags::GetCommandLineFlagInfoOrDie("velox_ssd_odirect").is_default) {
    FLAGS_velox_ssd_odirect = false;
  }

  return facebook::velox::run();
}
