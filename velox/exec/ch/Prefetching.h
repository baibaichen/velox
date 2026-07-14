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
#include <cstddef>
#include <utility>

namespace facebook::velox::exec::ch {

class PrefetchingHelper {
 public:
  PrefetchingHelper() : start_(Clock::now()) {}

  size_t calcPrefetchLookAhead() const {
    return calcPrefetchLookAhead(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now() - start_));
  }

  static constexpr size_t calcPrefetchLookAhead(
      std::chrono::nanoseconds elapsed) {
    if (elapsed.count() <= 0) {
      return kMaxLookAhead;
    }
    const auto elapsedNanos = static_cast<size_t>(elapsed.count());
    const auto lookAhead = (kNumerator + elapsedNanos - 1) / elapsedNanos;
    return std::clamp(lookAhead, kMinLookAhead, kMaxLookAhead);
  }

  static constexpr size_t getInitialLookAheadValue() {
    return kMinLookAhead;
  }

  static constexpr size_t iterationsToMeasure() {
    return kIterationsToMeasure;
  }

 private:
  using Clock = std::chrono::steady_clock;

  static constexpr size_t kIterationsToMeasure = 100;
  static constexpr size_t kMinLookAhead = 4;
  static constexpr size_t kMaxLookAhead = 32;
  static constexpr size_t kAssumedLoadLatencyNanos = 100;
  static constexpr size_t kCoefficient = 4;
  static constexpr size_t kNumerator =
      kCoefficient * kAssumedLoadLatencyNanos * kIterationsToMeasure;

  Clock::time_point start_;
};

size_t minTableBytesForPrefetch();

template <typename PrefetchAction>
class JoinPrefetcher {
 public:
  JoinPrefetcher(bool usePrefetch, size_t total, PrefetchAction prefetchAction)
      : usePrefetch_(usePrefetch),
        total_(total),
        prefetchAction_(std::move(prefetchAction)) {}

  void prefetchAt(size_t row) {
    if (!usePrefetch_) {
      return;
    }
    if (row == PrefetchingHelper::iterationsToMeasure()) {
      lookAhead_ = helper_.calcPrefetchLookAhead();
    }
    const auto prefetchRow = row + lookAhead_;
    if (prefetchRow < total_) {
      prefetchAction_(prefetchRow);
    }
  }

 private:
  bool usePrefetch_;
  size_t total_;
  PrefetchAction prefetchAction_;
  PrefetchingHelper helper_;
  size_t lookAhead_{PrefetchingHelper::getInitialLookAheadValue()};
};

template <typename PrefetchAction>
auto makeJoinPrefetcher(
    bool usePrefetch,
    size_t total,
    PrefetchAction&& prefetchAction) {
  return JoinPrefetcher<std::decay_t<PrefetchAction>>(
      usePrefetch, total, std::forward<PrefetchAction>(prefetchAction));
}

} // namespace facebook::velox::exec::ch
