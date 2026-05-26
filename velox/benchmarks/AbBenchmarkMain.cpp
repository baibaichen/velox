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

#include <gflags/gflags.h>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"

DECLARE_string(input_source);
DECLARE_int32(fscache_disk_gib);
DECLARE_string(fscache_root);
DECLARE_int32(cache_gb);

namespace facebook::velox::benchmarks {
namespace {

// Wipes FLAGS_fscache_root, recreates it, builds an FsCache sized to
// FLAGS_fscache_disk_gib, and installs it as the process-wide singleton.
// Returns the owning handle so the caller can drop it after the sweep
// finishes (the destructor tears the singleton down).
std::unique_ptr<facebook::velox::cache::fs::FsCache> installFsCache() {
  using facebook::velox::cache::fs::FsCache;
  using facebook::velox::cache::fs::FsCacheConfig;
  // Wipe cache root so round 1 is always a true cold miss (spec 4.4).
  std::error_code ec;
  std::filesystem::remove_all(FLAGS_fscache_root, ec);
  // remove_all failure is benign when the dir simply did not exist; the next
  // create_directories call is the authoritative check. Clear ec so any error
  // it reports refers to create_directories, not the prior remove_all.
  ec.clear();
  std::filesystem::create_directories(FLAGS_fscache_root, ec);
  VELOX_USER_CHECK(
      !ec,
      "Failed to create --fscache_root: {} ({})",
      FLAGS_fscache_root,
      ec.message());

  FsCacheConfig cfg;
  cfg.cacheRoot = FLAGS_fscache_root;
  cfg.maxBytes = static_cast<uint64_t>(FLAGS_fscache_disk_gib) << 30;
  auto cache = std::make_unique<FsCache>(cfg);
  FsCache::setInstance(cache.get());
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
  std::unique_ptr<facebook::velox::cache::fs::FsCache> ownedFsCache;
  if (FLAGS_input_source == "fscache") {
    // QueryBenchmarkBase::initialize skips AsyncDataCache construction when
    // FLAGS_cache_gb == 0; force it so the CBI tier stays nullptr (spec 2.6).
    FLAGS_cache_gb = 0;
    ownedFsCache = installFsCache();
  } else if (FLAGS_input_source == "cbi") {
    VELOX_USER_CHECK_GT(
        FLAGS_cache_gb, 0, "--input_source=cbi requires --cache_gb > 0");
  } else {
    VELOX_USER_FAIL(
        "Unknown --input_source: {} (expected cbi or fscache)",
        FLAGS_input_source);
  }

  ab.initialize();
  const int32_t failed = ab.runAb();
  ab.shutdown();

  if (ownedFsCache != nullptr) {
    facebook::velox::cache::fs::FsCache::setInstance(nullptr);
  }
  // Soft-cap: only signal systemic failure to the shell when more than 10%
  // of queries failed (spec 4.2). Sweep results are still in --out.
  return failed > 10 ? 1 : 0;
}

} // namespace facebook::velox::benchmarks
