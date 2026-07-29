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

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace facebook::velox::benchmarks {

class AbBenchmarkBase;

/// Selects which I/O backend the A/B benchmark uses.
enum class AbInputSource
{
  kDirect,             ///< Pure Velox DirectBufferedInput; no caches.
  kFileCachePassthrough, ///< FileCacheBufferedInput in passthrough mode (no
                         ///< disk state); measures replacement overhead only.
  kFileCache,          ///< FileCacheBufferedInput with live on-disk FileCache.
  kCbi,                ///< CBI (AsyncDataCache SSD tier).
};

/// Parses a string token to `AbInputSource`. Throws `VeloxUserError` for an
/// unrecognized value so the error reaches the user without a stack trace.
AbInputSource parseAbInputSource(std::string_view token);

/// Maps a query-failure count (as returned by AbBenchmarkBase::runAb()) to a
/// process exit code. Any failed query (failed > 0) must be surfaced as a
/// nonzero exit code so shell scripts/orchestrators can detect it; only a
/// clean sweep (failed == 0) returns 0. Pure function, safe to unit test
/// directly.
int32_t abExitCode(int32_t failed);

/// Benchmark-only FileCache root lifecycle selected by --filecache_root_mode:
///   kReset -> wipe/recreate --filecache_root before every (re)install (the
///             pre-Task-022 default behavior).
///   kReuse -> require and reload an existing, already-populated root without
///             ever clearing it.
enum class FileCacheRootMode : uint8_t
{
  kReset,
  kReuse,
};

/// Parses/validates the benchmark-only FileCache root lifecycle mode. Pure
/// function, safe to unit test directly.
///
/// @param inputSource The resolved --input_source value.
/// @param rootMode The raw --filecache_root_mode value ("reset" or "reuse").
/// @param coldEachRound The resolved --cold_each_round value.
/// @return FileCacheRootMode::kReset when rootMode == "reset"; otherwise
///     FileCacheRootMode::kReuse.
/// @throws VeloxUserError when rootMode is neither "reset" nor "reuse", when
///     rootMode == "reuse" and inputSource != "filecache", or when rootMode ==
///     "reuse" and coldEachRound is true.
FileCacheRootMode parseFileCacheRootMode(
    const std::string& inputSource,
    const std::string& rootMode,
    bool coldEachRound);

/// Common --input_source dispatch for any AbBenchmarkBase-derived suite:
///   empty                -> runLegacy() (the suite's existing folly::runBenchmarks path)
///   "direct"             -> no cache, DirectBufferedInput
///   "filecache_passthrough" -> FileCacheBufferedInput passthrough mode
///   "filecache"          -> install ch::FileCache, call ab.runAb()
///   "cbi"                -> require --cache_gb>0, call ab.runAb()
/// Returns the process exit code from abExitCode(failed): 0 if no queries
/// failed, 1 otherwise.
int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy);

} // namespace facebook::velox::benchmarks
