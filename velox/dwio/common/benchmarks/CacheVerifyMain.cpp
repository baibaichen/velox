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

// Byte-level cache-correctness gate for the FileCache benchmark suite. It
// materializes a small synthetic working set (or scans a real --data_dir),
// then for each selected backend reads the whole working set twice through the
// matching BufferedInput wrapper -- a cold pass that populates the cache and a
// hot pass that re-serves it from the cache tier -- byte-comparing every
// segment against a fresh pread of the source file. It exits 0 iff every
// segment of every selected backend matched (and the cold and hot passes agree
// byte-for-byte), non-zero on any mismatch.
//
// Backends:
//   * direct    = DirectBufferedInput, no cache layer (source-vs-source sanity).
//   * filecache = FileCacheBufferedInput + ch::FileCacheManager (on-disk cache).
//   * cbi       = CachedBufferedInput + AsyncDataCache + SsdCache.
//
// Unlike a persisted two-phase verify, this tool is self-contained: it wipes and
// re-primes each backend's cache in-process, so a single invocation with the
// working-set / cache-shape flags certifies byte correctness end to end, e.g.:
//   velox_cache_verify --target_ws_mb=64 --read_size_kib=1024 \
//       --filecache_root=tmp/fc_verify --filecache_disk_gib=1

#include <filesystem>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"
#include "velox/dwio/common/benchmarks/CacheVerify.h"

// Defined in velox/flag_definitions/flags.cpp; global default is O_DIRECT.
DECLARE_bool(velox_ssd_odirect);

DEFINE_string(
    backend,
    "all",
    "Which cache backend(s) to verify: 'all', 'direct' (DirectBufferedInput, "
    "no cache), 'filecache' (ch::FileCacheManager) or 'cbi' (AsyncDataCache + "
    "SsdCache).");
DEFINE_string(
    data_dir,
    "",
    "If set, verify the real *.parquet files in this directory instead of the "
    "synthetic blob.");
DEFINE_string(
    source_path,
    "tmp/velox_cache_verify_source.bin",
    "Synthetic source blob path (used when --data_dir is empty). Kept under a "
    "CWD-relative tmp/ dir, never /tmp.");
DEFINE_double(
    target_ws_mb,
    64.0,
    "Working set to verify (MiB). Small by design: correctness is independent "
    "of size.");
DEFINE_double(
    read_size_kib,
    1024.0,
    "Segment size for the verify sweep, KiB.");
DEFINE_uint64(batch, 64, "Regions enqueued per BufferedInput before load().");

// filecache backend.
DEFINE_string(
    filecache_root,
    "tmp/velox_cache_verify_fc",
    "ch::FileCache root for the filecache backend (CWD-relative, never /tmp).");
DEFINE_double(filecache_disk_gib, 1.0, "ch::FileCache disk size (GiB).");
DEFINE_double(fcbi_segment_mb, 4.0, "ch::FileCache segment size (MiB).");

// cbi backend.
DEFINE_string(
    ssd_root,
    "tmp/velox_cache_verify_ssd",
    "SsdCache root for the cbi backend (CWD-relative, never /tmp).");
DEFINE_double(ssd_cache_mb, 512.0, "SsdCache size (MiB) for cbi.");
DEFINE_int32(ssd_num_shards, 1, "SsdCache shard count for cbi.");
DEFINE_double(ram_cache_mb, 512.0, "AsyncDataCache RAM size (MiB) for cbi.");
DEFINE_int32(ram_num_shards, 4, "AsyncDataCache shard count for cbi.");
DEFINE_double(cbi_read_quantum_mb, 8.0, "cbi load quantum (MiB).");

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

uint64_t mbToBytes(double mb) {
  return static_cast<uint64_t>(mb * static_cast<double>(1ULL << 20));
}

uint64_t gibToBytes(double gib) {
  return static_cast<uint64_t>(gib * static_cast<double>(1ULL << 30));
}

// Self-contained (prime + reuse) config: wipe on start, clean up on destroy,
// and never require a pre-resident cache -- the cold pass primes it.
HarnessConfig buildConfig() {
  HarnessConfig config;
  // SsdCache requires an absolute path (SsdCache.cpp asserts the prefix starts
  // with '/'); the --ssd_root default is CWD-relative (never /tmp), so resolve
  // it to absolute before the cbi harness hands it to SsdCache.
  config.ssdPath = std::filesystem::absolute(FLAGS_ssd_root).string();
  config.ssdCacheBytes = mbToBytes(FLAGS_ssd_cache_mb);
  config.ssdNumShards = FLAGS_ssd_num_shards;
  config.ramCacheBytes = mbToBytes(FLAGS_ram_cache_mb);
  config.ramNumShards = FLAGS_ram_num_shards;
  config.cbiReadQuantumBytes =
      static_cast<int32_t>(FLAGS_cbi_read_quantum_mb * (1 << 20));
  config.filecacheRoot = FLAGS_filecache_root;
  config.filecacheDiskBytes = gibToBytes(FLAGS_filecache_disk_gib);
  config.fcbiSegmentBytes =
      static_cast<uint64_t>(FLAGS_fcbi_segment_mb * (1 << 20));
  config.batch = FLAGS_batch;
  config.clearCacheOnStart = true;
  config.cleanupOnDestroy = true;
  config.requireResidentCache = false;
  return config;
}

// Reads the whole working set through `Harness` twice: a cold pass that
// populates the cache and a hot pass that re-serves it. Both passes byte-check
// every segment against the source, and the two passes must agree byte-for-byte
// (identical FNV checksum) so a cache that serves stale/corrupt bytes on the hot
// path fails even if the cold path matched. Returns true iff the backend passes.
template <typename Harness>
bool runMode(
    const std::string& name,
    const WorkingSet& workingSet,
    uint64_t readSize) {
  const auto config = buildConfig();
  CacheVerifier verifier(workingSet.files());
  Harness harness(config, workingSet.files());
  const DataLayout layout{
      workingSet.files(), readSize, workingSet.effectiveTargetBytes()};

  const VerifyResult cold =
      verifier.verifyWorkingSet(harness, layout, readSize, config.batch);
  const VerifyResult hot =
      verifier.verifyWorkingSet(harness, layout, readSize, config.batch);

  LOG(INFO) << "[verify] backend=" << name << " cold{segments="
            << cold.segmentsChecked << " bytes=" << cold.bytesChecked
            << " mismatches=" << cold.mismatches << " checksum=0x" << std::hex
            << cold.checksum << std::dec << "}"
            << " hot{segments=" << hot.segmentsChecked
            << " bytes=" << hot.bytesChecked << " mismatches=" << hot.mismatches
            << " checksum=0x" << std::hex << hot.checksum << std::dec << "}";

  const bool ok =
      cold.ok() && hot.ok() && cold.checksum == hot.checksum;
  if (!ok) {
    LOG(ERROR) << "[verify] backend=" << name
               << " FAIL: cold.mismatches=" << cold.mismatches
               << " hot.mismatches=" << hot.mismatches
               << " checksumMatch=" << (cold.checksum == hot.checksum)
               << " first hot mismatch fileIdx=" << hot.firstMismatchFileIdx
               << " offset=" << hot.firstMismatchOffset;
    return false;
  }
  LOG(INFO) << "[verify] backend=" << name << " PASS: all "
            << hot.segmentsChecked
            << " segments match the source (cold+hot, checksum 0x" << std::hex
            << hot.checksum << std::dec << ").";
  return true;
}

bool runBackend(
    const std::string& name,
    const WorkingSet& workingSet,
    uint64_t readSize) {
  if (name == "direct") {
    return runMode<DbiHarness>(name, workingSet, readSize);
  }
  if (name == "filecache") {
    return runMode<FcbiHarness>(name, workingSet, readSize);
  }
  if (name == "cbi") {
    return runMode<CbiHarness>(name, workingSet, readSize);
  }
  VELOX_USER_FAIL(
      "--backend must be 'all', 'direct', 'filecache' or 'cbi' (got '{}')",
      name);
}

int run() {
  WorkingSetConfig wsConfig;
  wsConfig.dataDir = FLAGS_data_dir;
  wsConfig.remotePath = FLAGS_source_path;
  wsConfig.targetBytes = mbToBytes(FLAGS_target_ws_mb);
  // Blob exactly covers the working set (no scrub headroom needed for verify).
  wsConfig.remoteBytesOverride = mbToBytes(FLAGS_target_ws_mb);
  const WorkingSet workingSet = WorkingSet::create(wsConfig);
  workingSet.materialize();
  VELOX_USER_CHECK_GT(
      workingSet.effectiveTargetBytes(), 0, "working set is empty");

  const auto readSize = static_cast<uint64_t>(FLAGS_read_size_kib * 1024.0);

  std::vector<std::string> backends;
  if (FLAGS_backend == "all") {
    backends = {"direct", "filecache", "cbi"};
  } else {
    backends = {FLAGS_backend};
  }

  bool allOk = true;
  for (const auto& backend : backends) {
    allOk &= runBackend(backend, workingSet, readSize);
  }

  if (!allOk) {
    LOG(ERROR) << "[verify] FAIL: one or more backends did not match the "
                  "source.";
    return 1;
  }
  LOG(INFO) << "[verify] all backends PASS.";
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
