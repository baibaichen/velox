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

// Task 018b: three-engine (direct / cbi / filecache) TPCH A/B benchmark. Ported
// from the ch-filecache branch's velox/benchmarks TPCH A/B suite, adapted so the
// `filecache` engine routes through the Task 018a FileCacheBufferedInputBuilder
// (a FileCacheManager-built default cache), NOT a bare ch::FileCache singleton.
//
// EVERY TPCH command MUST pass --num_splits_per_file=1 (the upstream default of
// 10 causes false read amplification that hurts fcbi).

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <iostream>

#include "velox/ch/benchmarks/AbBenchmarkBase.h"
#include "velox/ch/benchmarks/AbBenchmarkMain.h"
#include "velox/exec/OperatorType.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

DEFINE_string(
    data_path,
    "",
    "Root path of TPC-H data. Data layout must follow Hive-style partitioning. "
    "Each table (customer, lineitem, ...) is a subdirectory of --data_path.");

namespace
{
bool notEmpty(const char * /*flagName*/, const std::string & value)
{
    return !value.empty();
}
} // namespace

DEFINE_validator(data_path, &notEmpty);

DEFINE_int32(run_query_verbose, -1, "Run a given query and print execution statistics");
DEFINE_int32(
    io_meter_column_pct,
    0,
    "Percentage of lineitem columns to include in IO meter query.");

namespace facebook::velox::ch::benchmarks
{

/// TPCH suite over the three-engine A/B harness. Reuses the velox
/// TpchQueryBuilder for plan construction; only the cache backend wiring differs
/// per --input_source (handled by dispatchAbMain).
class TpchAbBenchmark : public AbBenchmarkBase
{
public:
    void initialize() override
    {
        QueryBenchmarkBase::initialize();
        queryBuilder_ = std::make_shared<TpchQueryBuilder>(toFileFormat(FLAGS_data_format));
        queryBuilder_->initialize(FLAGS_data_path);
    }

    void shutdown() override
    {
        QueryBenchmarkBase::shutdown();
        queryBuilder_.reset();
    }

    void runMain(std::ostream & out, facebook::velox::RunStats & runStats) override
    {
        if (FLAGS_run_query_verbose == -1 && FLAGS_io_meter_column_pct == 0)
        {
            folly::runBenchmarks();
        }
        else
        {
            auto queryPlan = FLAGS_io_meter_column_pct > 0
                ? queryBuilder_->getIoMeterPlan(FLAGS_io_meter_column_pct)
                : queryBuilder_->getQueryPlan(FLAGS_run_query_verbose);
            auto [cursor, actualResults] = run(queryPlan, queryConfigs_);
            if (!cursor)
            {
                LOG(ERROR) << "Query terminated with error. Exiting";
                exit(1);
            }
            auto task = cursor->task();
            ensureTaskCompletion(task.get());
            const auto stats = task->taskStats();
            int64_t rawInputBytes = 0;
            for (auto & pipeline : stats.pipelineStats)
            {
                auto & first = pipeline.operatorStats[0];
                if (first.operatorType == OperatorType::kTableScan)
                {
                    rawInputBytes += first.rawInputBytes;
                }
            }
            runStats.rawInputBytes = rawInputBytes;
            out << fmt::format(
                       "Execution time: {}",
                       facebook::velox::succinctMillis(stats.executionEndTimeMs - stats.executionStartTimeMs))
                << std::endl;
            out << printPlanWithStats(*queryPlan.plan, stats, FLAGS_include_custom_stats) << std::endl;
        }
    }

    int32_t numQueries() const override { return 22; }

    facebook::velox::exec::test::TpchPlan buildPlan(int32_t queryId) override
    {
        return queryBuilder_->getQueryPlan(queryId);
    }

private:
    std::shared_ptr<TpchQueryBuilder> queryBuilder_;
};

} // namespace facebook::velox::ch::benchmarks

namespace
{
std::unique_ptr<facebook::velox::ch::benchmarks::TpchAbBenchmark> gBenchmark;

void tpchBenchmarkMain()
{
    VELOX_CHECK_NOT_NULL(gBenchmark);
    gBenchmark->initialize();
    if (FLAGS_test_flags_file.empty())
    {
        facebook::velox::RunStats ignore;
        gBenchmark->runMain(std::cout, ignore);
    }
    else
    {
        gBenchmark->runAllCombinations();
    }
    gBenchmark->shutdown();
}
} // namespace

int main(int argc, char ** argv)
{
    std::string kUsage("Three-engine TPC-H A/B benchmark (direct/cbi/filecache). "
                       "Always pass --num_splits_per_file=1.\n");
    gflags::SetUsageMessage(kUsage);
    folly::Init init{&argc, &argv, false};
    gBenchmark = std::make_unique<facebook::velox::ch::benchmarks::TpchAbBenchmark>();
    return facebook::velox::ch::benchmarks::dispatchAbMain(*gBenchmark, tpchBenchmarkMain);
}
