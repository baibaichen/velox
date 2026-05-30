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

#include <functional>

namespace facebook::velox::benchmarks {

class AbBenchmarkBase;

/// Common --input_source dispatch for any AbBenchmarkBase-derived suite:
///   empty       -> runLegacy() (the suite's existing folly::runBenchmarks path)
///   "filecache" -> install ch::FileCache, force --cache_gb=0, call ab.runAb()
///   "cbi"       -> require --cache_gb>0, call ab.runAb()
/// Returns the process exit code (0 unless more than 10 query failures, in
/// which case 1).
int32_t dispatchAbMain(
    AbBenchmarkBase& ab,
    const std::function<void()>& runLegacy);

} // namespace facebook::velox::benchmarks
