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
#include <chrono>

namespace facebook::velox::exec {

/// Adaptive prefetch look-ahead for loops with random memory accesses where
/// the working set exceeds CPU cache. Measures per-iteration latency during
/// initial iterations, then returns a fixed look-ahead for the remainder.
class AdaptivePrefetch {
 public:
  explicit AdaptivePrefetch(int32_t numIterations)
      : numIterations_(numIterations),
        start_(std::chrono::steady_clock::now()) {}

  /// Returns look-ahead distance, or 0 when too close to the end.
  int32_t lookAhead() {
    if (iteration_ == kMeasurementIterations) {
      computeLookAhead();
    }
    ++iteration_;
    if (iteration_ + lookAhead_ > numIterations_) {
      return 0;
    }
    return lookAhead_;
  }

  /// Computes the look-ahead for a measured elapsed duration (in
  /// nanoseconds) covering kMeasurementIterations iterations. Exposed as a
  /// pure, deterministic calculation so it can be unit tested directly
  /// without depending on clock resolution or timing behavior. An elapsed
  /// value of zero or less means the measured loop completed faster than
  /// the clock can resolve (observed in practice on clocksources with
  /// coarse ticks, e.g. Hyper-V's hyperv_clocksource_tsc_page), so the
  /// maximum look-ahead is selected and division by zero is avoided.
  static int32_t computeLookAheadForElapsedNs(int64_t elapsedNs) {
    if (elapsedNs <= 0) {
      return kMaxLookAhead;
    }
    return std::clamp(
        static_cast<int32_t>(
            kCoefficient * kAssumedDramLatencyNs * kMeasurementIterations /
            elapsedNs),
        kMinLookAhead,
        kMaxLookAhead);
  }

 private:
  static constexpr int32_t kMeasurementIterations = 16;
  static constexpr int32_t kMinLookAhead = 4;
  static constexpr int32_t kMaxLookAhead = 32;
  static constexpr int64_t kAssumedDramLatencyNs = 100;
  static constexpr int64_t kCoefficient = 4;

  void computeLookAhead() {
    auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now() - start_)
                         .count();
    lookAhead_ = computeLookAheadForElapsedNs(elapsedNs);
  }

  int32_t numIterations_;
  int32_t iteration_{0};
  int32_t lookAhead_{kMinLookAhead};
  std::chrono::steady_clock::time_point start_;
};

} // namespace facebook::velox::exec
