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

#include "velox/benchmarks/AbBenchmarkMain.h"

#include <filesystem>
#include <memory>
#include <system_error>

#include <folly/ScopeGuard.h>
#include <gflags/gflags.h>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"

DECLARE_string(input_source);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_int32(cache_gb);

DEFINE_bool(
    enable_slru,
    false,
    "If true and --input_source=filecache, construct the FileCache with "
    "FileCachePolicy::SLRU (SLRU eviction policy).");
DEFINE_double(
    slru_protected_ratio,
    0.6,
    "Protected-list fraction when --enable_slru. Forwarded into "
    "FileCacheSettings::slruSizeRatio.");

namespace facebook::velox::benchmarks {
namespace {

// Wipes FLAGS_filecache_root, recreates it, builds a ch::FileCache sized to
// FLAGS_filecache_disk_gib, and installs it as the process-wide singleton.
// Returns the owning handle so the caller can drop it after the sweep
// finishes (the destructor tears the singleton down).
std::unique_ptr<ch::FileCache> installFileCache() {
  // Wipe cache root so round 1 is always a true cold miss.
  std::error_code ec;
  std::filesystem::remove_all(FLAGS_filecache_root, ec);
  // remove_all failure is benign when the dir simply did not exist; the next
  // create_directories call is the authoritative check. Clear ec so any error
  // it reports refers to create_directories, not the prior remove_all.
  ec.clear();
  std::filesystem::create_directories(FLAGS_filecache_root, ec);
  VELOX_USER_CHECK(
      !ec,
      "Failed to create --filecache_root: {} ({})",
      FLAGS_filecache_root,
      ec.message());

  ch::FileCacheSettings settings;
  settings.path = FLAGS_filecache_root;
  settings.maxSize = static_cast<uint64_t>(FLAGS_filecache_disk_gib) << 30;
  settings.cachePolicy =
      FLAGS_enable_slru ? ch::FileCachePolicy::SLRU : ch::FileCachePolicy::LRU;
  settings.slruSizeRatio = FLAGS_slru_protected_ratio;
  settings.validate();

  auto cache = std::make_unique<ch::FileCache>("ab_benchmark", settings);
  cache->initialize();
  ch::FileCache::setInstance(cache.get());
  return cache;
}

} // namespace

int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy) {
  if (FLAGS_input_source.empty()) {
    runLegacy();
    return 0;
  }

  // Kept alive until end of dispatch; destructor tears the singleton down.
  std::unique_ptr<ch::FileCache> ownedFileCache;
  // Clear the global instance pointer before ownedFileCache is destroyed,
  // even if runAb() throws -- the FileCache destructor contract requires
  // setInstance(nullptr) to precede teardown.
  auto instanceGuard = folly::makeGuard([&ownedFileCache] {
    if (ownedFileCache != nullptr) {
      ch::FileCache::setInstance(nullptr);
    }
  });
  if (FLAGS_input_source == "filecache") {
    // QueryBenchmarkBase::initialize skips AsyncDataCache construction when
    // FLAGS_cache_gb == 0; force it so the CBI tier stays nullptr.
    FLAGS_cache_gb = 0;
    ownedFileCache = installFileCache();
  } else if (FLAGS_input_source == "cbi") {
    VELOX_USER_CHECK_GT(
        FLAGS_cache_gb, 0, "--input_source=cbi requires --cache_gb > 0");
  } else {
    VELOX_USER_FAIL(
        "Unknown --input_source: {} (expected cbi or filecache)",
        FLAGS_input_source);
  }

  ab.initialize();

  // Wire the per-backend cold-reset used by --cold_each_round. filecache tears
  // down and reinstalls its singleton (re-wiping disk + metadata via the same
  // installFileCache() path used at startup); cbi clears its AsyncDataCache.
  if (FLAGS_input_source == "filecache") {
    ab.setColdResetFn([&ownedFileCache]() {
      ch::FileCache::setInstance(nullptr);
      ownedFileCache = installFileCache();
    });
  } else {
    ab.setColdResetFn([&ab]() { ab.clearCbiCache(); });
  }

  const int32_t failed = ab.runAb();
  ab.shutdown();

  // Soft-cap: only signal systemic failure to the shell when more than 10
  // queries failed. Sweep results are still in --out.
  return failed > 10 ? 1 : 0;
}

} // namespace facebook::velox::benchmarks
