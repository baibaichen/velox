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

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/benchmarks/KeyGenerator.h"
#include "velox/common/file/Region.h"

namespace facebook::velox::dwio::common::bench {

/// Generates a stream of fixed-size read regions over a synthetic working
/// set, for the BufferedInput wrapper microbench. The working set is
/// `workingSetKeys * readSizeBytes` contiguous bytes starting at
/// `baseOffset`, partitioned into `workingSetKeys` aligned blocks of
/// `readSizeBytes` each. `nextRegion()` returns the block selected by an
/// underlying KeyGenerator driven by the chosen Workload.
///
/// Reuses `velox::ch::bench::KeyGenerator` (header-only) so the Sequential,
/// Zipfian and Uniform shapes match the existing FileCache benchmarks.
class WorkloadDriver {
 public:
  /// Builds a driver over a working set of `workingSetKeys` aligned blocks
  /// of `readSizeBytes` each, starting at `baseOffset`. `seed` selects the
  /// KeyGenerator RNG stream.
  WorkloadDriver(
      ch::bench::Workload workload,
      uint64_t workingSetKeys,
      uint64_t readSizeBytes,
      uint64_t seed,
      uint64_t baseOffset = 0)
      : readSizeBytes_{readSizeBytes},
        baseOffset_{baseOffset},
        keyGen_{workload, workingSetKeys, seed} {
    VELOX_CHECK_GT(readSizeBytes, 0, "readSizeBytes must be positive");
  }

  /// Returns the region for the next key selected by the underlying
  /// KeyGenerator: `{offset = baseOffset + key * readSizeBytes, length =
  /// readSizeBytes}`.
  velox::common::Region nextRegion() {
    const uint64_t key = keyGen_.next();
    return velox::common::Region{
        baseOffset_ + key * readSizeBytes_, readSizeBytes_};
  }

 private:
  const uint64_t readSizeBytes_;
  const uint64_t baseOffset_;
  ch::bench::KeyGenerator keyGen_;
};

} // namespace facebook::velox::dwio::common::bench
