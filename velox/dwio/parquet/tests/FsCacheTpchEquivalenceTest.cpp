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

#include <folly/init/Init.h>
#include <filesystem>
#include <vector>

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FsCacheConfig.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

// Task 15 / issue #178: end-to-end equivalence check between the
// CachedBufferedInput (CBI) path and the FsCacheBufferedInput path on the
// full TPC-H q1-q22 workload.
//
// Approach: the existing HiveConnectorUtil::createBufferedInput already
// dispatches based on whether `QueryCtx::fsCache()` returns non-null
// (HiveConnectorUtil.cpp around line 654). And QueryCtx::fsCache_ defaults
// to FsCache::getInstance() at construction time. So the mode switch
// reduces to: install (or unset) the FsCache singleton before running the
// query. No BufferedInputFactory / session-property hook is required.
//
// For each query the test runs twice -- once with FsCache::setInstance
// pointing at a fresh per-query FsCache (so dispatch picks
// FsCacheBufferedInput), and once with FsCache::setInstance(nullptr) (so
// dispatch falls through to CachedBufferedInput / DirectBufferedInput).
// Both runs go through exec::test::assertQuery against DuckDB, which is the
// canonical reference and already handles unordered TPC-H result sets via
// sortingKeys. If both runs pass DuckDB comparison, the FsCache and CBI
// outputs are equivalent by transitivity.
class FsCacheTpchEquivalenceTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    duckDb_ = std::make_shared<DuckDbQueryRunner>();
    tempDirectory_ = TempDirectoryPath::create();
    tpchBuilder_ =
        std::make_shared<TpchQueryBuilder>(dwio::common::FileFormat::PARQUET);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();

    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    dwio::common::registerFileSinks();

    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();

    connector::hive::HiveConnectorFactory hiveFactory;
    auto hiveConnector = hiveFactory.newConnector(
        kHiveConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::ConnectorRegistry::global().insert(
        hiveConnector->connectorId(), hiveConnector);

    connector::tpch::TpchConnectorFactory tpchFactory;
    auto tpchConnector = tpchFactory.newConnector(
        kTpchConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::ConnectorRegistry::global().insert(
        tpchConnector->connectorId(), tpchConnector);

    saveTpchTablesAsParquet();
    tpchBuilder_->initialize(tempDirectory_->getPath());
  }

  static void TearDownTestSuite() {
    cache::fs::FsCache::setInstance(nullptr);
    connector::ConnectorRegistry::global().erase(kHiveConnectorId);
    connector::ConnectorRegistry::global().erase(kTpchConnectorId);
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
  }

  // Generates SF=0.01 TPC-H data under tempDirectory_, one Parquet file per
  // table. Mirrors ParquetTpchTest::saveTpchTablesAsParquet verbatim --
  // intentionally so the equivalence test consumes the exact same fixture
  // as the existing TPC-H smoke test.
  static void saveTpchTablesAsParquet() {
    std::shared_ptr<memory::MemoryPool> rootPool{
        memory::memoryManager()->addRootPool()};
    std::shared_ptr<memory::MemoryPool> pool{rootPool->addLeafChild("leaf")};

    for (const auto& table : tpch::tables) {
      auto tableName = toTableName(table);
      auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->getPath(), tableName);
      auto tableSchema = tpch::getTableSchema(table);
      auto columnNames = tableSchema->names();
      auto plan = PlanBuilder()
                      .tpchTableScan(table, std::move(columnNames), 0.01)
                      .planNode();
      auto split = exec::Split(
          std::make_shared<connector::tpch::TpchConnectorSplit>(
              kTpchConnectorId, /*cacheable=*/true, 1, 0));

      auto rows =
          AssertQueryBuilder(plan).splits({split}).copyResults(pool.get());
      duckDb_->createTable(tableName.data(), {rows});

      plan = PlanBuilder()
                 .values({rows})
                 .tableWrite(tableDirectory, dwio::common::FileFormat::PARQUET)
                 .planNode();

      AssertQueryBuilder(plan).copyResults(pool.get());
    }
  }

  std::shared_ptr<Task> runOnce(
      int queryId,
      const std::optional<std::vector<uint32_t>>& sortingKeys) const {
    auto tpchPlan = tpchBuilder_->getQueryPlan(queryId);
    auto duckDbSql = tpch::getQuery(queryId);
    constexpr int kNumSplits = 10;
    constexpr int kNumDrivers = 4;
    auto addSplits = [&, tpchPlan](TaskCursor* taskCursor) {
      if (taskCursor->noMoreSplits()) {
        return;
      }
      auto& task = taskCursor->task();
      for (const auto& entry : tpchPlan.dataFiles) {
        for (const auto& path : entry.second) {
          auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
              path, kNumSplits, tpchPlan.dataFileFormat);
          for (const auto& split : splits) {
            task->addSplit(entry.first, Split(split));
          }
        }
        task->noMoreSplits(entry.first);
      }
      taskCursor->setNoMoreSplits();
    };
    CursorParameters params;
    params.maxDrivers = kNumDrivers;
    params.planNode = tpchPlan.plan;
    return exec::test::assertQuery(
        params, addSplits, duckDbSql, *duckDb_, sortingKeys);
  }

  // Runs `queryId` twice -- first under the default (no-FsCache, CBI) path,
  // then with a fresh FsCache singleton installed so HiveConnectorUtil
  // dispatches to FsCacheBufferedInput. Both runs assert against DuckDB
  // inside assertQuery, so passing both proves CBI <-> FsCache equivalence
  // by transitivity. sortingKeys is forwarded to assertQuery's DuckDB
  // comparator the same way ParquetTpchTest does.
  void assertQueryInBothModes(
      int queryId,
      const std::optional<std::vector<uint32_t>>& sortingKeys = {}) {
    {
      SCOPED_TRACE(fmt::format("q{} mode=CBI (FsCache off)", queryId));
      cache::fs::FsCache::setInstance(nullptr);
      runOnce(queryId, sortingKeys);
    }
    {
      SCOPED_TRACE(fmt::format("q{} mode=FsCache", queryId));
      cache::fs::FsCacheConfig cfg;
      cfg.cacheRoot = fmt::format(
          "{}/fscache-q{}", tempDirectory_->getPath(), queryId);
      cfg.maxBytes = 256ULL * 1'024 * 1'024;
      std::filesystem::create_directories(cfg.cacheRoot);
      auto cache = std::make_unique<cache::fs::FsCache>(cfg);
      cache::fs::FsCache::setInstance(cache.get());
      try {
        runOnce(queryId, sortingKeys);
      } catch (...) {
        cache::fs::FsCache::setInstance(nullptr);
        throw;
      }
      // Unbind the singleton before `cache` is destroyed so any subsequent
      // QueryCtx construction does not capture a dangling pointer.
      cache::fs::FsCache::setInstance(nullptr);
    }
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<TempDirectoryPath> tempDirectory_;
  static std::shared_ptr<TpchQueryBuilder> tpchBuilder_;

  static constexpr char const* kTpchConnectorId{"test-tpch"};
};

std::shared_ptr<DuckDbQueryRunner> FsCacheTpchEquivalenceTest::duckDb_ =
    nullptr;
std::shared_ptr<TempDirectoryPath> FsCacheTpchEquivalenceTest::tempDirectory_ =
    nullptr;
std::shared_ptr<TpchQueryBuilder> FsCacheTpchEquivalenceTest::tpchBuilder_ =
    nullptr;

TEST_F(FsCacheTpchEquivalenceTest, Q1) {
  assertQueryInBothModes(1);
}

TEST_F(FsCacheTpchEquivalenceTest, Q2) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQueryInBothModes(2, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q3) {
  std::vector<uint32_t> sortingKeys{1, 2};
  assertQueryInBothModes(3, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q4) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(4, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q5) {
  std::vector<uint32_t> sortingKeys{1};
  assertQueryInBothModes(5, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q6) {
  assertQueryInBothModes(6);
}

TEST_F(FsCacheTpchEquivalenceTest, Q7) {
  std::vector<uint32_t> sortingKeys{0, 1, 2};
  assertQueryInBothModes(7, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q8) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(8, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q9) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQueryInBothModes(9, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q10) {
  std::vector<uint32_t> sortingKeys{2};
  assertQueryInBothModes(10, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q11) {
  std::vector<uint32_t> sortingKeys{1};
  assertQueryInBothModes(11, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q12) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(12, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q13) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQueryInBothModes(13, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q14) {
  assertQueryInBothModes(14);
}

TEST_F(FsCacheTpchEquivalenceTest, Q15) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(15, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q16) {
  std::vector<uint32_t> sortingKeys{0, 1, 2, 3};
  assertQueryInBothModes(16, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q17) {
  assertQueryInBothModes(17);
}

TEST_F(FsCacheTpchEquivalenceTest, Q18) {
  assertQueryInBothModes(18);
}

TEST_F(FsCacheTpchEquivalenceTest, Q19) {
  assertQueryInBothModes(19);
}

TEST_F(FsCacheTpchEquivalenceTest, Q20) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(20, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q21) {
  std::vector<uint32_t> sortingKeys{0, 1};
  assertQueryInBothModes(21, std::move(sortingKeys));
}

TEST_F(FsCacheTpchEquivalenceTest, Q22) {
  std::vector<uint32_t> sortingKeys{0};
  assertQueryInBothModes(22, std::move(sortingKeys));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
