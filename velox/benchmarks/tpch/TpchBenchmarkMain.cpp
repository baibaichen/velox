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

#include <memory>

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/benchmarks/AbBenchmarkMain.h"
#include "velox/benchmarks/tpch/TpchBenchmark.h"

int main(int argc, char** argv) {
  std::string kUsage(
      "This program benchmarks TPC-H queries. With --input_source={cbi,fscache} "
      "runs the FsCache-vs-CBI A/B sweep (spec 2026-05-23-fscache-vs-cbi-tpcds). "
      "Without it, runs the legacy folly::runBenchmarks() flow.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init{&argc, &argv, false};

  benchmark = std::make_unique<TpchBenchmark>();
  return facebook::velox::benchmarks::dispatchAbMain(
      *benchmark, tpchBenchmarkMain);
}
