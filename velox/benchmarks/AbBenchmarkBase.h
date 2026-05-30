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

#include "velox/benchmarks/QueryBenchmarkBase.h"

DECLARE_string(input_source);
DECLARE_int32(rounds);
DECLARE_int32(filecache_disk_gib);
DECLARE_string(filecache_root);
DECLARE_string(out);

namespace facebook::velox::benchmarks {

/// Base class for benchmarks that need the FileCache-vs-CBI A/B sweep harness:
/// snapshot backend stats, run one query, snapshot again, emit one CSV row,
/// repeat for FLAGS_rounds x numQueries() iterations. Derived classes plug
/// in the suite-specific query count and per-query plan construction;
/// everything else is shared.
class AbBenchmarkBase : public facebook::velox::QueryBenchmarkBase {
 public:
  /// Drives the A/B sweep: FLAGS_rounds outer iterations x numQueries()
  /// queries, plan construction hoisted via buildPlan() once before the
  /// round loop. Per-query wall_ms is measured by std::chrono::steady_clock
  /// around QueryBenchmarkBase::run(). Writes one CSV row per (round, query)
  /// to FLAGS_out. Returns the number of failed queries (rows with non-empty
  /// error column) so the caller can set a non-zero exit code without
  /// re-reading the CSV.
  int32_t runAb();

 protected:
  /// Total number of queries in this suite (TPC-H: 22).
  virtual int32_t numQueries() const = 0;

  /// Builds the plan for queryId in [1, numQueries()]. Called once per query
  /// before the round loop, then the result is reused across rounds.
  virtual facebook::velox::exec::test::TpchPlan buildPlan(int32_t queryId) = 0;

  std::unordered_map<std::string, std::string> queryConfigs_;
};

} // namespace facebook::velox::benchmarks
