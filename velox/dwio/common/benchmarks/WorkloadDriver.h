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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/Region.h"

namespace facebook::velox::dwio::common::bench {

// Access-pattern shape for the WorkloadDriver key stream. This is header-only
// and self-contained: the former `ch::bench::KeyGenerator` was removed when the
// FileCache moved under `velox/ch`, so the three shapes are reproduced inline.
enum class Workload {
  Sequential,
  Uniform,
  Zipfian,
};

// Deterministic key generator over a flat key space [0, keys). Sequential wraps
// a monotonically increasing counter; Uniform draws i.i.d. uniform keys;
// Zipfian draws skewed keys (a few hot keys dominate) via inverse-CDF sampling
// of a zeta(theta) distribution. Every stream is seeded so a given
// (workload, keys, seed) triple is reproducible across runs and backends.
class KeyGenerator {
 public:
  KeyGenerator(Workload workload, uint64_t keys, uint64_t seed)
      : workload_{workload},
        keys_{keys},
        rng_{seed},
        uniform_{0, keys > 0 ? keys - 1 : 0} {
    VELOX_CHECK_GT(keys_, 0, "KeyGenerator needs a non-empty key space");
    if (workload_ == Workload::Zipfian) {
      buildZipfCdf();
    }
  }

  uint64_t next() {
    switch (workload_) {
      case Workload::Sequential:
        return sequential_++ % keys_;
      case Workload::Uniform:
        return uniform_(rng_);
      case Workload::Zipfian:
        return sampleZipf();
    }
    return 0;
  }

 private:
  void buildZipfCdf() {
    // Precompute the zeta(theta) normalization and the cumulative distribution
    // over the key space so next() is an O(log keys) inverse-CDF lookup.
    constexpr double kTheta = 0.99;
    cdf_.resize(keys_);
    double sum = 0.0;
    for (uint64_t i = 0; i < keys_; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i + 1), kTheta);
      cdf_[i] = sum;
    }
    const double norm = cdf_.back();
    for (auto& c : cdf_) {
      c /= norm;
    }
  }

  uint64_t sampleZipf() {
    const double u = unit_(rng_);
    const auto it = std::lower_bound(cdf_.begin(), cdf_.end(), u);
    const auto idx = static_cast<uint64_t>(std::distance(cdf_.begin(), it));
    return idx < keys_ ? idx : keys_ - 1;
  }

  const Workload workload_;
  const uint64_t keys_;
  std::mt19937_64 rng_;
  std::uniform_int_distribution<uint64_t> uniform_;
  std::uniform_real_distribution<double> unit_{0.0, 1.0};
  uint64_t sequential_{0};
  std::vector<double> cdf_;
};

/// Generates a stream of fixed-size read regions over a synthetic working
/// set, for the BufferedInput wrapper microbench. The working set is
/// `workingSetKeys * readSizeBytes` contiguous bytes starting at
/// `baseOffset`, partitioned into `workingSetKeys` aligned blocks of
/// `readSizeBytes` each. `nextRegion()` returns the block selected by an
/// underlying KeyGenerator driven by the chosen Workload.
class WorkloadDriver {
 public:
  /// Builds a driver over a working set of `workingSetKeys` aligned blocks
  /// of `readSizeBytes` each, starting at `baseOffset`. `seed` selects the
  /// KeyGenerator RNG stream.
  WorkloadDriver(
      Workload workload,
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
  KeyGenerator keyGen_;
};

} // namespace facebook::velox::dwio::common::bench
