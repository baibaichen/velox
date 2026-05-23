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

namespace facebook::velox::cache::fs::bench {

/// Key-distribution shape selector for KeyGenerator.
enum class Workload { kSequential, kZipfian, kUniform };

/// Per-thread key index generator. Pure logic, no IO. next() returns an index
/// in [0, n). Sequential walks [seqStart, seqStart + n) modulo n: giving each
/// thread a distinct seqStart with the same n yields partitioned scans;
/// giving every thread the same start yields aligned shared scans. Zipfian
/// and uniform draw from [0, n).
class KeyGenerator {
 public:
  KeyGenerator(
      Workload workload,
      uint64_t n,
      uint64_t seed,
      uint64_t seqStart = 0,
      double zipfTheta = 1.0)
      : workload_(workload), n_(n), seqPos_(seqStart % n), rng_(seed) {
    VELOX_CHECK_GT(n, 0, "KeyGenerator universe must be non-empty");
    if (workload_ == Workload::kZipfian) {
      buildZipfCdf(zipfTheta);
    }
  }

  uint64_t next() {
    switch (workload_) {
      case Workload::kSequential: {
        const auto k = seqPos_;
        seqPos_ = (seqPos_ + 1) % n_;
        return k;
      }
      case Workload::kZipfian: {
        std::uniform_real_distribution<double> u(0.0, 1.0);
        const double r = u(rng_);
        auto it = std::lower_bound(cdf_.begin(), cdf_.end(), r);
        return static_cast<uint64_t>(std::distance(cdf_.begin(), it));
      }
      case Workload::kUniform: {
        std::uniform_int_distribution<uint64_t> d(0, n_ - 1);
        return d(rng_);
      }
    }
    VELOX_UNREACHABLE();
  }

 private:
  void buildZipfCdf(double theta) {
    cdf_.resize(n_);
    double sum = 0.0;
    for (uint64_t i = 1; i <= n_; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i), theta);
      cdf_[i - 1] = sum;
    }
    for (auto& c : cdf_) {
      c /= sum;
    }
  }

  Workload workload_;
  uint64_t n_;
  uint64_t seqPos_;
  std::mt19937_64 rng_;
  std::vector<double> cdf_;
};

} // namespace facebook::velox::cache::fs::bench
