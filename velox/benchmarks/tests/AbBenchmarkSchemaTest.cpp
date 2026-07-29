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

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <vector>

#include <folly/Singleton.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/benchmarks/AbBenchmarkMain.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::benchmarks {
namespace {

using namespace facebook::velox::test;
using velox::common::testutil::TempDirectoryPath;

// ---------------------------------------------------------------------------
// 35-field CSV schema (Task-4 expansion + Task-022 exact counters)
// ---------------------------------------------------------------------------

// The exact 35-field header: 18 base + 4 rusage + 13 scan I/O.
constexpr const char* kExpectedHeader =
    "round,query_id,wall_ms,rows,result_hash,result_match,bytes_read,hit_pct,"
    "miss_count,source_read_bytes,cache_write_bytes,"
    "cache_read_mib,predownload_mib,evict_mib,evict_count,"
    "op_p50_us,op_p95_us,error,"
    "user_ns,system_ns,voluntary_csw,involuntary_csw,"
    "storage_read_ops,storage_read_bytes,"
    "local_read_ops,local_read_bytes,"
    "prefetch_ops,prefetch_bytes,"
    "enqueue_count,enqueue_bytes,"
    "next_count,returned_bytes,"
    "seek_count,max_chunk_bytes,"
    "passthrough_read_bytes";

// NOTE: The header/row field-count tests below have been updated from 15→35
// fields as part of Task-4 CSV expansion. The ResultMatch serialization tests
// validate field indices which remain stable at position 5.

TEST(AbBenchmarkSchemaTest, CsvHeaderHasExactly32Fields)
{
  std::ostringstream oss;
  writeCsvHeader(oss);
  std::string header = oss.str();
  if (!header.empty() && header.back() == '\n')
  {
    header.pop_back();
  }
  EXPECT_EQ(header, kExpectedHeader)
      << "CSV header must match the 35-field schema exactly";
  int commas = 0;
  for (char c : header)
  {
    if (c == ',')
      ++commas;
  }
  EXPECT_EQ(commas, 34) << "35 fields require exactly 34 commas";
}

TEST(AbBenchmarkSchemaTest, CsvRowHas35Fields)
{
  AbCsvRow row{};
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 123.456;
  row.rows = 100;
  row.resultHash = 999;
  row.resultMatch = std::nullopt;
  row.bytesRead = 5000;
  row.hitPct = 95.0;
  row.missCount = 0;
  row.sourceReadBytes = 0;
  row.cacheWriteBytes = 0;
  row.cacheReadMib = 4.5;
  row.predownloadMib = 1.2;
  row.evictMib = 0.3;
  row.evictCount = 7;
  row.opP50Us = 10.0;
  row.opP95Us = 50.0;
  row.error = "";

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  if (!line.empty() && line.back() == '\n')
  {
    line.pop_back();
  }
  int commas = 0;
  for (char c : line)
  {
    if (c == ',')
      ++commas;
  }
  EXPECT_EQ(commas, 34) << "35 fields require exactly 34 commas";
}

TEST(AbBenchmarkSchemaTest, ResultMatchSerializesEmpty)
{
  AbCsvRow row{};
  row.round = 1;
  row.queryId = 1;
  row.resultMatch = std::nullopt;

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  // result_match field is after result_hash (field 5, 0-indexed),
  // and must be empty when nullopt.
  // Split by commas and check field 5.
  std::vector<std::string> fields;
  std::istringstream ss(line);
  std::string field;
  while (std::getline(ss, field, ','))
  {
    fields.push_back(field);
  }
  // Last field may have a newline
  if (!fields.empty() && !fields.back().empty() && fields.back().back() == '\n')
  {
    fields.back().pop_back();
  }
  ASSERT_EQ(fields.size(), 35) << "Must have exactly 35 fields";
  EXPECT_EQ(fields[5], "") << "result_match must be empty for nullopt";
}

TEST(AbBenchmarkSchemaTest, ResultMatchSerializesTrue)
{
  AbCsvRow row{};
  row.round = 1;
  row.queryId = 1;
  row.resultMatch = true;

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  std::vector<std::string> fields;
  std::istringstream ss(line);
  std::string field;
  while (std::getline(ss, field, ','))
  {
    fields.push_back(field);
  }
  if (!fields.empty() && !fields.back().empty() && fields.back().back() == '\n')
  {
    fields.back().pop_back();
  }
  ASSERT_EQ(fields.size(), 35) << "Must have exactly 35 fields";
  EXPECT_EQ(fields[5], "1") << "result_match must be 1 for true";
}

TEST(AbBenchmarkSchemaTest, ResultMatchSerializesFalse)
{
  AbCsvRow row{};
  row.round = 1;
  row.queryId = 1;
  row.resultMatch = false;

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  std::vector<std::string> fields;
  std::istringstream ss(line);
  std::string field;
  while (std::getline(ss, field, ','))
  {
    fields.push_back(field);
  }
  if (!fields.empty() && !fields.back().empty() && fields.back().back() == '\n')
  {
    fields.back().pop_back();
  }
  ASSERT_EQ(fields.size(), 35) << "Must have exactly 35 fields";
  EXPECT_EQ(fields[5], "0") << "result_match must be 0 for false";
}

class AbBenchmarkHelpersTest : public VectorTestBase,
                               public testing::Test
{
 protected:
  static void SetUpTestSuite()
  {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(AbBenchmarkHelpersTest, CountResultRowsSkipsNullptr)
{
  auto rows2 = makeRowVector(
      {"a"}, {makeFlatVector<int64_t>({1, 2})});
  auto rows3 = makeRowVector(
      {"a"}, {makeFlatVector<int64_t>({3, 4, 5})});
  std::vector<RowVectorPtr> results = {rows2, nullptr, rows3};
  EXPECT_EQ(countResultRows(results), 5);
}

TEST_F(AbBenchmarkHelpersTest, CountResultRowsEmptyVector)
{
  std::vector<RowVectorPtr> results = {};
  EXPECT_EQ(countResultRows(results), 0);
}

TEST_F(AbBenchmarkHelpersTest, ComputeResultHashDeterministic)
{
  auto rows = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int64_t>({10, 20}),
       makeFlatVector<double>({1.5, 2.5})});
  uint64_t expected = 0;
  for (vector_size_t r = 0; r < rows->size(); ++r)
  {
    expected += rows->hashValueAt(r);
  }
  std::vector<RowVectorPtr> results = {rows};
  EXPECT_EQ(computeResultHash(results), expected);
}

TEST_F(AbBenchmarkHelpersTest, ComputeResultHashSkipsNullptr)
{
  auto rows = makeRowVector(
      {"a"}, {makeFlatVector<int64_t>({42})});
  uint64_t expected = rows->hashValueAt(0);
  std::vector<RowVectorPtr> results = {nullptr, rows, nullptr};
  EXPECT_EQ(computeResultHash(results), expected);
}

// --- Flag validation tests ---

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversAllowsZero)
{
  EXPECT_NO_THROW(validateReferenceDrivers(0, 4));
}

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversAllowsOne)
{
  EXPECT_NO_THROW(validateReferenceDrivers(1, 4));
}

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversAllowsEqualToRequested)
{
  EXPECT_NO_THROW(validateReferenceDrivers(4, 4));
}

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversRejectsNegative)
{
  EXPECT_THROW(validateReferenceDrivers(-1, 4), VeloxUserError);
}

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversRejectsGreaterThanRequested)
{
  EXPECT_THROW(validateReferenceDrivers(5, 4), VeloxUserError);
}

TEST(AbBenchmarkFlagTest, ValidateReferenceDriversRejectsZeroRequested)
{
  EXPECT_THROW(validateReferenceDrivers(1, 0), VeloxUserError);
}

// --- Epsilon comparator tests ---

TEST_F(AbBenchmarkHelpersTest, EpsilonComparatorMatchesNearDoubles)
{
  // Reference: large double sum from one-driver aggregation
  auto reference = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({5.660776097195746e12})});
  // Actual: slightly different from four-driver aggregation
  auto actual = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({5.660776097193966e12})});

  EXPECT_TRUE(exec::test::assertEqualResults({reference}, {actual}));
}

TEST_F(AbBenchmarkHelpersTest, EpsilonComparatorRejectsChangedKey)
{
  auto reference = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({100.0})});
  auto actual = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({2}),
       makeFlatVector<double>({100.0})});

  bool result = true;
  EXPECT_NONFATAL_FAILURE(
      { result = exec::test::assertEqualResults({reference}, {actual}); },
      "");
  EXPECT_FALSE(result);
}

TEST_F(AbBenchmarkHelpersTest, EpsilonComparatorRejectsMissingRow)
{
  auto reference = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1, 2}),
       makeFlatVector<double>({100.0, 200.0})});
  auto actual = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({100.0})});

  bool result = true;
  EXPECT_NONFATAL_FAILURE(
      { result = exec::test::assertEqualResults({reference}, {actual}); },
      "");
  EXPECT_FALSE(result);
}

TEST_F(AbBenchmarkHelpersTest, EpsilonComparatorRejectsMateriallyDifferent)
{
  auto reference = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({100.0})});
  auto actual = makeRowVector(
      {"key", "val"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<double>({200.0})});

  bool result = true;
  EXPECT_NONFATAL_FAILURE(
      { result = exec::test::assertEqualResults({reference}, {actual}); },
      "");
  EXPECT_FALSE(result);
}

TEST(AbBenchmarkSchemaTest, BackendSnapshotFileCacheMapping)
{
  BackendSnapshot snap;
  snap.lookups = 100;
  snap.hits = 80;
  snap.misses = 20;
  snap.sourceReadBytes = 1234567;
  snap.cacheWriteBytes = 7654321;
  snap.cacheReadBytes = 1024 * 1024 * 10;   // 10 MiB
  snap.predownloadBytes = 1024 * 1024 * 2;   // 2 MiB
  snap.evictedBytes = 1024 * 1024 * 3;       // 3 MiB
  snap.evictionCount = 42;

  BackendSnapshot before;
  before.lookups = 0;
  before.hits = 0;
  before.misses = 0;
  before.sourceReadBytes = 0;
  before.cacheWriteBytes = 0;
  before.cacheReadBytes = 0;
  before.predownloadBytes = 0;
  before.evictedBytes = 0;
  before.evictionCount = 0;

  AbCsvRow row{};
  populateBackendDelta(row, before, snap);
  EXPECT_DOUBLE_EQ(row.hitPct, 80.0);
  EXPECT_EQ(row.missCount, 20);
  EXPECT_EQ(row.sourceReadBytes, 1234567);
  EXPECT_EQ(row.cacheWriteBytes, 7654321);
  EXPECT_DOUBLE_EQ(row.cacheReadMib, 10.0);
  EXPECT_DOUBLE_EQ(row.predownloadMib, 2.0);
  EXPECT_DOUBLE_EQ(row.evictMib, 3.0);
  EXPECT_EQ(row.evictCount, 42);
}

TEST(AbBenchmarkSchemaTest, BackendSnapshotCbiMapping)
{
  BackendSnapshot snap;
  snap.lookups = 50;
  snap.hits = 40;
  snap.misses = 10;
  snap.cacheReadBytes = 1024 * 1024 * 5;  // 5 MiB (hitBytes)
  snap.predownloadBytes = 0;
  snap.evictedBytes = 0;
  snap.evictionCount = 12;

  BackendSnapshot before;
  before.lookups = 0;
  before.hits = 0;
  before.misses = 0;
  before.cacheReadBytes = 0;
  before.predownloadBytes = 0;
  before.evictedBytes = 0;
  before.evictionCount = 0;

  AbCsvRow row{};
  populateBackendDelta(row, before, snap);
  EXPECT_DOUBLE_EQ(row.hitPct, 80.0);
  EXPECT_EQ(row.missCount, 10);
  EXPECT_EQ(row.sourceReadBytes, 0);
  EXPECT_EQ(row.cacheWriteBytes, 0);
  EXPECT_DOUBLE_EQ(row.cacheReadMib, 5.0);
  EXPECT_DOUBLE_EQ(row.predownloadMib, 0.0);
  EXPECT_DOUBLE_EQ(row.evictMib, 0.0);
  EXPECT_EQ(row.evictCount, 12);
}

TEST(AbBenchmarkSchemaTest, MutationSwapBytesCountFails)
{
  BackendSnapshot snap;
  snap.lookups = 10;
  snap.hits = 10;
  snap.cacheReadBytes = 0;
  snap.predownloadBytes = 0;
  snap.evictedBytes = 1024 * 1024 * 7;  // 7 MiB in bytes
  snap.evictionCount = 7;               // 7 segments

  BackendSnapshot before{};
  AbCsvRow row{};
  populateBackendDelta(row, before, snap);
  EXPECT_DOUBLE_EQ(row.evictMib, 7.0);
  EXPECT_EQ(row.evictCount, 7);
  EXPECT_GT(row.evictMib, 1.0);
  EXPECT_LT(row.evictCount, 100);
}

TEST(AbBenchmarkSchemaTest, CsvRowHas35FieldsForFailure)
{
  AbCsvRow row;
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 12.0;
  row.rows = 0;
  row.resultHash = 0;
  row.resultMatch = std::nullopt;
  row.bytesRead = 0;
  row.hitPct = 0.0;
  row.missCount = 0;
  row.sourceReadBytes = 0;
  row.cacheWriteBytes = 0;
  row.cacheReadMib = 0.0;
  row.predownloadMib = 0.0;
  row.evictMib = 0.0;
  row.evictCount = 0;
  row.opP50Us = 0.0;
  row.opP95Us = 0.0;
  row.error = "task failed (see ERROR log)";

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  if (!line.empty() && line.back() == '\n')
  {
    line.pop_back();
  }
  EXPECT_NE(line.find("task failed (see ERROR log)"), std::string::npos)
      << "Row must carry the exact fixed failure error string";
  int commas = 0;
  for (char c : line)
  {
    if (c == ',')
      ++commas;
  }
  EXPECT_EQ(commas, 34) << "35 fields require exactly 34 commas";
}

TEST(AbBenchmarkSchemaTest, FailedReferenceCheckedRowSerializesResultMatchFalse)
{
  // Simulates AbBenchmarkBase::runAb() when cursor == nullptr (task failure)
  // while reference verification is enabled: resultMatch must be set to
  // false (not left as nullopt) alongside the fixed failure error string, so
  // downstream analysis can distinguish "failed and unverifiable" rows from
  // "failed, not reference-checked" rows.
  AbCsvRow row;
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 12.0;
  row.rows = 0;
  row.resultHash = 0;
  row.resultMatch = false;
  row.bytesRead = 0;
  row.hitPct = 0.0;
  row.missCount = 0;
  row.sourceReadBytes = 0;
  row.cacheWriteBytes = 0;
  row.cacheReadMib = 0.0;
  row.predownloadMib = 0.0;
  row.evictMib = 0.0;
  row.evictCount = 0;
  row.opP50Us = 0.0;
  row.opP95Us = 0.0;
  row.error = "task failed (see ERROR log)";

  std::ostringstream oss;
  writeCsvRow(oss, row);
  std::string line = oss.str();
  if (!line.empty() && line.back() == '\n')
  {
    line.pop_back();
  }
  std::vector<std::string> fields;
  std::istringstream ss(line);
  std::string field;
  while (std::getline(ss, field, ','))
  {
    fields.push_back(field);
  }
  ASSERT_EQ(fields.size(), 35) << "Must have exactly 35 fields";
  EXPECT_EQ(fields[5], "0")
      << "result_match must serialize as 0 for a failed, reference-checked row";
  EXPECT_FALSE(fields.back().empty())
      << "error column must be nonempty for a failed row";
}

TEST(AbBenchmarkSchemaTest, ExitCodeIsZeroWhenNoQueriesFailed)
{
  EXPECT_EQ(abExitCode(0), 0);
}

TEST(AbBenchmarkSchemaTest, ExitCodeIsNonzeroForAnyFailedQuery)
{
  EXPECT_NE(abExitCode(1), 0);
  EXPECT_NE(abExitCode(10), 0);
}

// ---------------------------------------------------------------------------
// Task-1: input-source parser and passthrough RAII override
// ---------------------------------------------------------------------------

TEST(AbBenchmarkMainTest, ParsesInputSources)
{
  EXPECT_EQ(parseAbInputSource("direct"), AbInputSource::kDirect);
  EXPECT_EQ(
      parseAbInputSource("filecache_passthrough"),
      AbInputSource::kFileCachePassthrough);
  EXPECT_EQ(parseAbInputSource("filecache"), AbInputSource::kFileCache);
  EXPECT_EQ(parseAbInputSource("cbi"), AbInputSource::kCbi);
  EXPECT_THROW(parseAbInputSource("other"), VeloxUserError);
}

TEST(AbBenchmarkMainTest, PassthroughOverrideIsScopedAndExclusive)
{
  EXPECT_FALSE(connector::hive::fileCachePassthroughForBenchmarkEnabled());
  {
    connector::hive::ScopedFileCachePassthroughForBenchmark guard;
    EXPECT_TRUE(connector::hive::fileCachePassthroughForBenchmarkEnabled());
    EXPECT_THROW(
        { connector::hive::ScopedFileCachePassthroughForBenchmark secondGuard; },
        VeloxException);
  }
  EXPECT_FALSE(connector::hive::fileCachePassthroughForBenchmarkEnabled());
}

// ---------------------------------------------------------------------------
// Task-1: connector session propagation helper
// ---------------------------------------------------------------------------

class QueryBenchmarkBaseSessionTest : public testing::Test
{
 protected:
  static void SetUpTestSuite()
  {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(QueryBenchmarkBaseSessionTest, ApplyConnectorSessionPropertiesSetsBoolProperty)
{
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  auto queryCtx =
      core::QueryCtx::Builder()
          .executor(executor.get())
          .pool(memory::memoryManager()->addRootPool("018s-session-test"))
          .queryId("018s-session-test")
          .build();

  ConnectorSessionProperties properties{
      {exec::test::kHiveConnectorId,
       {{"buffered_input_perf_probe", "true"}}}};

  QueryBenchmarkBase::applyConnectorSessionProperties(*queryCtx, properties);
  const auto* session =
      queryCtx->connectorSessionProperties(exec::test::kHiveConnectorId);
  ASSERT_NE(session, nullptr);
  EXPECT_TRUE(session->get<bool>("buffered_input_perf_probe", false));
}

// ---------------------------------------------------------------------------
// Task-2: BufferedInput probe statistics
// ---------------------------------------------------------------------------

TEST(BufferedInputProbeStatsTest, DisabledIsZeroCostAndEmpty)
{
  io::IoStatistics stats;
  stats.recordBufferedInputEnqueue(100);
  stats.recordBufferedInputNext(40);
  stats.recordBufferedInputSeek();
  EXPECT_EQ(stats.bufferedInputProbeSnapshot(), io::BufferedInputProbeSnapshot{});
}

TEST(BufferedInputProbeStatsTest, RecordsCountsBytesAndMaximum)
{
  io::IoStatistics stats;
  stats.enableBufferedInputProbe();
  stats.recordBufferedInputEnqueue(100);
  stats.recordBufferedInputEnqueue(60);
  stats.recordBufferedInputNext(32);
  stats.recordBufferedInputNext(80);
  stats.recordBufferedInputSeek();
  const auto snapshot = stats.bufferedInputProbeSnapshot();
  EXPECT_EQ(snapshot.enqueueCount, 2);
  EXPECT_EQ(snapshot.enqueueBytes, 160);
  EXPECT_EQ(snapshot.nextCount, 2);
  EXPECT_EQ(snapshot.returnedBytes, 112);
  EXPECT_EQ(snapshot.seekCount, 1);
  EXPECT_EQ(snapshot.maxChunkBytes, 80);
}

TEST(BufferedInputProbeStatsTest, MergePreservesProbeCounters)
{
  io::IoStatistics left;
  io::IoStatistics right;
  left.enableBufferedInputProbe();
  right.enableBufferedInputProbe();
  left.recordBufferedInputEnqueue(10);
  left.recordBufferedInputNext(16);
  right.recordBufferedInputEnqueue(64);
  right.recordBufferedInputNext(32);
  right.recordBufferedInputNext(80);
  right.recordBufferedInputSeek();
  left.merge(right);
  const auto snapshot = left.bufferedInputProbeSnapshot();
  EXPECT_EQ(snapshot.enqueueCount, 2);
  EXPECT_EQ(snapshot.enqueueBytes, 74);
  EXPECT_EQ(snapshot.nextCount, 3);
  EXPECT_EQ(snapshot.returnedBytes, 128);
  EXPECT_EQ(snapshot.seekCount, 1);
  EXPECT_EQ(snapshot.maxChunkBytes, 80);
}

TEST(BufferedInputProbeStatsTest, MergeEnablesDisabledDestination)
{
  io::IoStatistics left;
  io::IoStatistics right;
  right.enableBufferedInputProbe();
  right.recordBufferedInputEnqueue(64);
  right.recordBufferedInputNext(80);
  left.merge(right);
  const auto snapshot = left.bufferedInputProbeSnapshot();
  EXPECT_EQ(snapshot.enqueueCount, 1);
  EXPECT_EQ(snapshot.enqueueBytes, 64);
  EXPECT_EQ(snapshot.nextCount, 1);
  EXPECT_EQ(snapshot.returnedBytes, 80);
  EXPECT_EQ(snapshot.maxChunkBytes, 80);
}

// ---------------------------------------------------------------------------
// Task-4: getrusage delta and scan-stat aggregation
// ---------------------------------------------------------------------------

TEST(AbBenchmarkStatsTest, ComputesRusageDelta)
{
  rusage before{};
  before.ru_utime = {.tv_sec = 3, .tv_usec = 900000};
  before.ru_stime = {.tv_sec = 1, .tv_usec = 100000};
  before.ru_nvcsw = 10;
  before.ru_nivcsw = 20;

  rusage after{};
  after.ru_utime = {.tv_sec = 5, .tv_usec = 100000};
  after.ru_stime = {.tv_sec = 1, .tv_usec = 600000};
  after.ru_nvcsw = 14;
  after.ru_nivcsw = 27;

  const auto delta = computeRusageDelta(before, after);
  EXPECT_EQ(delta.userNanos, 1'200'000'000);
  EXPECT_EQ(delta.systemNanos, 500'000'000);
  EXPECT_EQ(delta.voluntaryCsw, 4);
  EXPECT_EQ(delta.involuntaryCsw, 7);
}

TEST(AbBenchmarkStatsTest, CollectScanIoStatsAggregatesBothTableScans)
{
  using exec::OperatorStats;
  using exec::PipelineStats;
  using exec::TaskStats;

  auto makeMetric = [](int64_t val,
                       RuntimeCounter::Unit unit =
                           RuntimeCounter::Unit::kNone) -> RuntimeMetric
  {
    return RuntimeMetric(val, unit);
  };

  TaskStats taskStats;

  // Pipeline 0: two TableScan operators with probe stats
  PipelineStats p0(true, false);
  {
    OperatorStats op0;
    op0.operatorType = "TableScan";
    op0.runtimeStats["storageReadBytes"] =
        makeMetric(300, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["storageReadOps"] = makeMetric(3);
    op0.runtimeStats["localReadBytes"] =
        makeMetric(500, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["localReadOps"] = makeMetric(4);
    op0.runtimeStats["prefetchBytes"] =
        makeMetric(100, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["prefetchOps"] = makeMetric(2);
    op0.runtimeStats["bufferedInputEnqueueCount"] = makeMetric(5);
    op0.runtimeStats["bufferedInputEnqueueBytes"] =
        makeMetric(700, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["bufferedInputNextCount"] = makeMetric(9);
    op0.runtimeStats["bufferedInputReturnedBytes"] =
        makeMetric(800, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["bufferedInputSeekCount"] = makeMetric(2);
    op0.runtimeStats["bufferedInputMaxChunkBytes"] =
        makeMetric(128, RuntimeCounter::Unit::kBytes);
    op0.runtimeStats["fileCachePassthroughReadBytes"] =
        makeMetric(600, RuntimeCounter::Unit::kBytes);
    p0.operatorStats.push_back(op0);

    // Non-scan operator — must be ignored by collectScanIoStats.
    OperatorStats nonScan;
    nonScan.operatorType = "HashBuild";
    nonScan.runtimeStats["storageReadBytes"] =
        makeMetric(9999, RuntimeCounter::Unit::kBytes);
    p0.operatorStats.push_back(nonScan);
  }

  // Pipeline 1: second TableScan with different values
  PipelineStats p1(false, true);
  {
    OperatorStats op1;
    op1.operatorType = "TableScan";
    op1.runtimeStats["storageReadBytes"] =
        makeMetric(200, RuntimeCounter::Unit::kBytes);
    op1.runtimeStats["storageReadOps"] = makeMetric(2);
    op1.runtimeStats["localReadBytes"] =
        makeMetric(100, RuntimeCounter::Unit::kBytes);
    op1.runtimeStats["localReadOps"] = makeMetric(1);
    op1.runtimeStats["bufferedInputEnqueueCount"] = makeMetric(3);
    op1.runtimeStats["bufferedInputEnqueueBytes"] =
        makeMetric(400, RuntimeCounter::Unit::kBytes);
    op1.runtimeStats["bufferedInputNextCount"] = makeMetric(4);
    op1.runtimeStats["bufferedInputReturnedBytes"] =
        makeMetric(350, RuntimeCounter::Unit::kBytes);
    op1.runtimeStats["bufferedInputMaxChunkBytes"] =
        makeMetric(256, RuntimeCounter::Unit::kBytes);
    p1.operatorStats.push_back(op1);
  }

  taskStats.pipelineStats.push_back(p0);
  taskStats.pipelineStats.push_back(p1);

  const auto snap = collectScanIoStats(taskStats);

  // Sums across both TableScans, ignoring the HashBuild.
  EXPECT_EQ(snap.storageReadOps, 5);
  EXPECT_EQ(snap.storageReadBytes, 500);
  EXPECT_EQ(snap.localReadOps, 5);
  EXPECT_EQ(snap.localReadBytes, 600);
  EXPECT_EQ(snap.prefetchOps, 2);
  EXPECT_EQ(snap.prefetchBytes, 100);
  EXPECT_EQ(snap.enqueueCount, 8);
  EXPECT_EQ(snap.enqueueBytes, 1100);
  EXPECT_EQ(snap.nextCount, 13);
  EXPECT_EQ(snap.returnedBytes, 1150);
  EXPECT_EQ(snap.seekCount, 2);
  // Max chunk: max(128, 256) = 256
  EXPECT_EQ(snap.maxChunkBytes, 256);
  EXPECT_EQ(snap.passthroughReadBytes, 600);
}

// ---------------------------------------------------------------------------
// Task-3: BufferedInput trace-capture config validation
// ---------------------------------------------------------------------------

/// Returns a `BufferedInputTraceRunConfig` template with fake/placeholder
/// paths.  Only use for tests whose gate fires BEFORE path validation
/// (steps 1–14 in validateBufferedInputTraceConfig), so the fake
/// datasetRoot/traceRoot are never canonicalized or stat-checked.
static BufferedInputTraceRunConfig makeValidTraceConfig(int32_t queryId = 4)
{
    BufferedInputTraceRunConfig cfg;
    cfg.traceRoot = "/tmp/trace_out";
    cfg.datasetRoot = "/data/tpch-sf100";
    cfg.queryId = queryId;
    cfg.round = 1;
    cfg.veloxHead = "velox-abc123";
    cfg.glutenHead = "gluten-abc123";
    cfg.clickhouseHead = "ch-abc123";
    cfg.binaryBuildId = "build-id-abc";
    cfg.maxEvents = 5'000'000;
    return cfg;
}

TEST(AbBenchmarkSchemaTest, TraceConfigEmptyRootIsDisabledNoThrow)
{
    auto cfg = makeValidTraceConfig();
    cfg.traceRoot.clear();
    // Empty traceRoot means capture disabled; no gate should fire.
    EXPECT_NO_THROW(validateBufferedInputTraceConfig(
        cfg, AbInputSource::kFileCache, /*probeEnabled=*/true,
        /*queryIdFlag=*/0, /*numDrivers=*/99, /*rounds=*/99));
}

TEST(AbBenchmarkSchemaTest, TraceConfigValidConfigPasses)
{
    // Use real RAII temp dirs so path validation (steps 15–16) can pass.
    auto datasetDir = TempDirectoryPath::create();
    auto traceParentDir = TempDirectoryPath::create();
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    cfg.traceRoot = traceParentDir->getPath() + "/fresh_trace";
    EXPECT_NO_THROW(validateBufferedInputTraceConfig(
        cfg, AbInputSource::kDirect, /*probeEnabled=*/false,
        /*queryIdFlag=*/4, /*numDrivers=*/1, /*rounds=*/1));
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsNonDirectSource)
{
    const auto cfg = makeValidTraceConfig(4);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kFileCache, false, 4, 1, 1),
        VeloxException);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kFileCachePassthrough, false, 4, 1, 1),
        VeloxException);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kCbi, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsProbeEnabled)
{
    const auto cfg = makeValidTraceConfig(4);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, /*probeEnabled=*/true, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsQueryIdZero)
{
    auto cfg = makeValidTraceConfig(0);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, /*queryIdFlag=*/0, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsQueryIdOutOfRange)
{
    auto cfg = makeValidTraceConfig(25);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, /*queryIdFlag=*/25, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsMultiDriver)
{
    const auto cfg = makeValidTraceConfig(4);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, /*numDrivers=*/2, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsMultiRound)
{
    const auto cfg = makeValidTraceConfig(4);
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, /*rounds=*/2),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsTraceRoundNotOne)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.round = 2;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsEmptyDatasetRoot)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot.clear();
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsEmptyVeloxHead)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.veloxHead.clear();
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsEmptyGlutenHead)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.glutenHead.clear();
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsEmptyClickhouseHead)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.clickhouseHead.clear();
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsEmptyBinaryBuildId)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.binaryBuildId.clear();
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

TEST(AbBenchmarkSchemaTest, TraceConfigRejectsZeroMaxEvents)
{
    auto cfg = makeValidTraceConfig(4);
    cfg.maxEvents = 0;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// ---------------------------------------------------------------------------
// Task-3 review fixes: Finding 1 — queryId/queryIdFlag mismatch
// ---------------------------------------------------------------------------

// config.queryId must equal queryIdFlag exactly; a mismatched config would
// silently skip capture for the only query (doCapture stays false).
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsQueryMismatch)
{
    // config.queryId=5 but queryIdFlag=4 → mismatch detected.
    auto cfg = makeValidTraceConfig(5);
    // traceRoot/datasetRoot remain fake: mismatch fires at step 5, before paths.
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, /*queryIdFlag=*/4, 1, 1),
        VeloxException);
}

// ---------------------------------------------------------------------------
// Task-3 review fixes: Finding 2 — datasetRoot canonical check
// ---------------------------------------------------------------------------

// A nonexistent datasetRoot must be rejected.
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsNonexistentDatasetRoot)
{
    auto traceParentDir = TempDirectoryPath::create();
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = traceParentDir->getPath() + "/does_not_exist_subdir";
    cfg.traceRoot = traceParentDir->getPath() + "/fresh_trace";
    cfg.queryId = 4;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// A regular file used as datasetRoot must be rejected (must be a directory).
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsFileAsDatasetRoot)
{
    auto baseDir = TempDirectoryPath::create();
    const std::string filePath = baseDir->getPath() + "/not_a_dir.bin";
    { std::ofstream f(filePath); f << "x"; }
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = filePath;
    cfg.traceRoot = baseDir->getPath() + "/fresh_trace";
    cfg.queryId = 4;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// A real existing directory as datasetRoot must pass.
TEST(AbBenchmarkSchemaTest, TraceConfigDatasetRootValidDirectoryPasses)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceParentDir = TempDirectoryPath::create();
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    cfg.traceRoot = traceParentDir->getPath() + "/fresh_trace";
    cfg.queryId = 4;
    EXPECT_NO_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1));
}

// ---------------------------------------------------------------------------
// Task-3 review fixes: Finding 3 — traceRoot freshness preflight
// ---------------------------------------------------------------------------

// An already-existing traceRoot must be rejected.
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsExistingTraceRoot)
{
    auto datasetDir = TempDirectoryPath::create();
    auto existingTrace = TempDirectoryPath::create(); // already exists
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    cfg.traceRoot = existingTrace->getPath(); // exists → rejected
    cfg.queryId = 4;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// A traceRoot whose parent directory does not exist must be rejected.
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsMissingTraceRootParent)
{
    auto datasetDir = TempDirectoryPath::create();
    auto baseDir = TempDirectoryPath::create();
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    // Two levels deep: baseDir/nonexistent/fresh_trace — parent doesn't exist.
    cfg.traceRoot = baseDir->getPath() + "/nonexistent_parent/fresh_trace";
    cfg.queryId = 4;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// A traceRoot whose parent is a regular file (not a directory) must be rejected.
TEST(AbBenchmarkSchemaTest, TraceConfigRejectsFileAsTraceRootParent)
{
    auto datasetDir = TempDirectoryPath::create();
    auto baseDir = TempDirectoryPath::create();
    const std::string filePath = baseDir->getPath() + "/a_regular_file";
    { std::ofstream f(filePath); f << "x"; }
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    cfg.traceRoot = filePath + "/fresh_trace"; // parent is a file
    cfg.queryId = 4;
    EXPECT_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1),
        VeloxException);
}

// A fresh traceRoot name under an existing parent directory must pass.
TEST(AbBenchmarkSchemaTest, TraceConfigFreshTraceRootWithValidParentPasses)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceParentDir = TempDirectoryPath::create();
    auto cfg = makeValidTraceConfig(4);
    cfg.datasetRoot = datasetDir->getPath();
    cfg.traceRoot = traceParentDir->getPath() + "/fresh_trace_child";
    cfg.queryId = 4;
    EXPECT_NO_THROW(
        validateBufferedInputTraceConfig(
            cfg, AbInputSource::kDirect, false, 4, 1, 1));
}

// ---------------------------------------------------------------------------
// Task 018S q04 trace-capture crash: cleanup order + finish() error handling
// ---------------------------------------------------------------------------

// Lifetime probe for the borrowed-memory contract enforced by
// releaseResultsThenCursor. A "cursor" owns a token that "results" reference
// through a weak_ptr (mirroring how a RowVector's Buffer holds a raw
// MemoryPool*). If the cursor is destroyed before the results, a result
// observes a dead owner at destruction time -- the deterministic, non-UB signal
// standing in for the heap-use-after-free that crashed q04 capture cleanup.
TEST(AbBenchmarkCaptureCrashTest, ReleasesResultsBeforeCursor)
{
    struct Owner
    {
        std::shared_ptr<int> token{std::make_shared<int>(0)};
    };

    struct Borrower
    {
        std::weak_ptr<int> ownerToken;
        bool* sawDeadOwner{nullptr};
        ~Borrower()
        {
            if (ownerToken.expired())
            {
                *sawDeadOwner = true;
            }
        }
    };

    bool sawDeadOwner = false;
    auto cursor = std::make_unique<Owner>();
    std::vector<std::shared_ptr<Borrower>> results;
    results.push_back(
        std::make_shared<Borrower>(Borrower{cursor->token, &sawDeadOwner}));

    releaseResultsThenCursor(results, cursor);

    EXPECT_FALSE(sawDeadOwner)
        << "borrowed results were destroyed after their owning cursor: the "
           "borrowed-memory lifetime contract was violated (heap-use-after-free)";
}

// Post-fix integration confirmation that releaseResultsThenCursor safely tears
// down REAL borrowed Velox result vectors before the TaskCursor whose internal
// memory pool backs them. Where ReleasesResultsBeforeCursor above is a synthetic
// lifetime probe, this drives an actual in-memory Values plan through the same
// readCursor mechanism used by QueryBenchmarkBase::run, whose contract states
// results borrow cursor memory (readCursorAsync: "'result' borrows memory from
// cursor so the life cycle must be shorter"). With copyResult=true the cursor
// copies each output batch into a leaf pool it owns, so destroying the cursor
// before the results would free that pool out from under them -- exactly the
// heap-use-after-free that crashed q04 capture cleanup. This test asserts the
// production release order leaves results empty and the cursor null and the
// process exits cleanly. It is a confirmation of the fix, not a deliberate
// old-order UAF reproduction: do not reorder the release calls and do not run
// this under ASan expecting a fault.
TEST(AbBenchmarkCaptureCrashTest, ReleasesRealBorrowedResultsBeforeTaskCursor)
{
    // The crash-test suite links GTest::gtest_main with no Velox fixture, so
    // neither folly nor the process-wide memory manager is set up for us. A real
    // Task uses folly futures (e.g. the Timekeeper singleton), which abort if
    // requested before folly's singleton registration is marked complete -- the
    // job normally done by folly::Init in a task-running test's main. Complete
    // registration and initialize the memory manager exactly once per process
    // before building vectors or running a Task. A Values plan needs no
    // connectors, functions, or serdes, so default options suffice.
    static const bool runtimeInitialized = []()
    {
        folly::SingletonVault::singleton()->registrationComplete();
        memory::MemoryManager::initialize(memory::MemoryManager::Options{});
        return true;
    }();
    (void)runtimeInitialized;

    auto pool = memory::memoryManager()->addLeafPool();
    velox::test::VectorMaker maker(pool.get());

    // Two tiny in-memory batches; 8 rows total with a predictable sum.
    auto batch1 = maker.rowVector({maker.flatVector<int64_t>({0, 1, 2, 3})});
    auto batch2 = maker.rowVector({maker.flatVector<int64_t>({4, 5, 6, 7})});

    exec::CursorParameters params;
    params.planNode =
        exec::test::PlanBuilder().values({batch1, batch2}).planNode();
    params.maxDrivers = 1;

    // Same result mechanism as QueryBenchmarkBase::run: a real unique_ptr
    // TaskCursor plus a vector of RowVectorPtr that borrow the cursor's memory.
    std::unique_ptr<exec::TaskCursor> cursor;
    std::vector<RowVectorPtr> results;
    std::tie(cursor, results) = exec::test::readCursor(params);

    ASSERT_NE(cursor, nullptr);
    ASSERT_FALSE(results.empty()) << "Values plan produced no result batches";

    int64_t totalRows = 0;
    int64_t sum = 0;
    for (const auto& batch : results)
    {
        ASSERT_NE(batch, nullptr);
        totalRows += batch->size();
        auto* col = batch->childAt(0)->asFlatVector<int64_t>();
        ASSERT_NE(col, nullptr);
        for (vector_size_t i = 0; i < col->size(); ++i)
        {
            sum += col->valueAt(i);
        }
    }
    EXPECT_EQ(totalRows, 8);
    EXPECT_EQ(sum, 0 + 1 + 2 + 3 + 4 + 5 + 6 + 7);

    // Production release path: borrowed results are dropped before the owning
    // cursor, so the cursor's result pool outlives every vector allocated from
    // it. Destroying the cursor first would be a heap-use-after-free.
    releaseResultsThenCursor(results, cursor);

    EXPECT_TRUE(results.empty());
    EXPECT_EQ(cursor, nullptr);
}

// finish() may throw (lifecycle/overflow/I/O). finishTraceCaptureOrError must
// turn that into a diagnostic string instead of letting it escape to
// std::terminate. An already-existing trace root makes finish() fail
// deterministically.
TEST(AbBenchmarkCaptureCrashTest, FinishFailureBecomesErrorNotTerminate)
{
    auto datasetDir = TempDirectoryPath::create();
    auto existingTraceDir = TempDirectoryPath::create();

    dwio::common::BufferedInputTraceCaptureConfig cfg;
    cfg.traceRoot = existingTraceDir->getPath(); // already exists -> finish fails
    cfg.datasetRoot = datasetDir->getPath();
    cfg.queryId = 4;
    cfg.drivers = 1;
    cfg.binaryRealPath = "/proc/self/exe";
    cfg.binaryBuildId = "test-build";
    cfg.veloxHead = "velox-head";
    cfg.glutenHead = "gluten-head";
    cfg.clickhouseHead = "clickhouse-head";
    cfg.maxEvents = 16;

    dwio::common::ScopedBufferedInputTraceCapture guard(std::move(cfg));

    std::string error;
    EXPECT_NO_THROW({ error = finishTraceCaptureOrError(guard); });
    EXPECT_FALSE(error.empty());
    EXPECT_NE(error.find("trace capture failed"), std::string::npos);
    EXPECT_NE(error.find("trace root already exists"), std::string::npos);
}

// A successful finish() over an empty (no-I/O) capture returns no error and
// writes the trace artifacts.
TEST(AbBenchmarkCaptureCrashTest, FinishSuccessReturnsNoError)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceParentDir = TempDirectoryPath::create();
    const std::string traceRoot = traceParentDir->getPath() + "/trace_child";

    dwio::common::BufferedInputTraceCaptureConfig cfg;
    cfg.traceRoot = traceRoot;
    cfg.datasetRoot = datasetDir->getPath();
    cfg.queryId = 4;
    cfg.drivers = 1;
    cfg.binaryRealPath = "/proc/self/exe";
    cfg.binaryBuildId = "test-build";
    cfg.veloxHead = "velox-head";
    cfg.glutenHead = "gluten-head";
    cfg.clickhouseHead = "clickhouse-head";
    cfg.maxEvents = 16;

    dwio::common::ScopedBufferedInputTraceCapture guard(std::move(cfg));

    std::string error;
    EXPECT_NO_THROW({ error = finishTraceCaptureOrError(guard); });
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_TRUE(std::filesystem::exists(traceRoot + "/manifest.json"));
    EXPECT_TRUE(std::filesystem::exists(traceRoot + "/events.jsonl"));
}

// A finish() failure must produce a CSV-safe single-field diagnostic: the
// benchmark writes row.error into an unquoted 35-field CSV, so the reason must
// contain no ',', '\n', or '\r' that could split the field or the row. Uses a
// trace root path containing commas to prove the interpolated path text cannot
// break the CSV field, while still retaining a useful reason such as
// "trace root already exists".
TEST(AbBenchmarkCaptureCrashTest, FinishFailureDiagnosticIsCsvSafe)
{
    auto datasetDir = TempDirectoryPath::create();
    auto traceParentDir = TempDirectoryPath::create();
    // Comma-laden, already-existing trace root -> finish() fails and the
    // exception message interpolates this path (with its commas).
    const std::string traceRoot =
        traceParentDir->getPath() + "/trace,root,with,commas";
    std::filesystem::create_directories(traceRoot);

    dwio::common::BufferedInputTraceCaptureConfig cfg;
    cfg.traceRoot = traceRoot; // already exists -> finish fails
    cfg.datasetRoot = datasetDir->getPath();
    cfg.queryId = 4;
    cfg.drivers = 1;
    cfg.binaryRealPath = "/proc/self/exe";
    cfg.binaryBuildId = "test-build";
    cfg.veloxHead = "velox-head";
    cfg.glutenHead = "gluten-head";
    cfg.clickhouseHead = "clickhouse-head";
    cfg.maxEvents = 16;

    dwio::common::ScopedBufferedInputTraceCapture guard(std::move(cfg));

    std::string error;
    EXPECT_NO_THROW({ error = finishTraceCaptureOrError(guard); });

    // A useful, non-empty reason is retained.
    EXPECT_FALSE(error.empty());
    EXPECT_NE(error.find("trace capture failed"), std::string::npos);
    EXPECT_NE(error.find("trace root already exists"), std::string::npos);

    // The diagnostic must be a single CSV-safe field: no delimiter or row
    // terminator characters, even though the failing path contains commas.
    EXPECT_EQ(error.find(','), std::string::npos)
        << "capture diagnostic contains a comma and would split the CSV field: "
        << error;
    EXPECT_EQ(error.find('\n'), std::string::npos)
        << "capture diagnostic contains a newline and would break the CSV row: "
        << error;
    EXPECT_EQ(error.find('\r'), std::string::npos)
        << "capture diagnostic contains a carriage return: " << error;
}

// --- --filecache_root_mode validation tests (Task 022-4) ---

TEST(AbBenchmarkFlagTest, FileCacheRootModeResetIsDefaultCompatible)
{
  EXPECT_EQ(
      parseFileCacheRootMode("filecache", "reset", false),
      FileCacheRootMode::kReset);
  EXPECT_EQ(
      parseFileCacheRootMode("direct", "reset", false),
      FileCacheRootMode::kReset);
  EXPECT_EQ(
      parseFileCacheRootMode("cbi", "reset", false),
      FileCacheRootMode::kReset);
  EXPECT_EQ(
      parseFileCacheRootMode("", "reset", false),
      FileCacheRootMode::kReset);
}

TEST(AbBenchmarkFlagTest, FileCacheRootModeReuseIsFileCacheOnly)
{
  EXPECT_EQ(
      parseFileCacheRootMode("filecache", "reuse", false),
      FileCacheRootMode::kReuse);
  EXPECT_THROW(
      parseFileCacheRootMode("direct", "reuse", false),
      VeloxUserError);
  EXPECT_THROW(
      parseFileCacheRootMode("cbi", "reuse", false),
      VeloxUserError);
  EXPECT_THROW(
      parseFileCacheRootMode("", "reuse", false),
      VeloxUserError);
}

TEST(AbBenchmarkFlagTest, FileCacheRootModeRejectsUnknownAndColdEachRound)
{
  EXPECT_THROW(
      parseFileCacheRootMode("filecache", "keep", false),
      VeloxUserError);
  EXPECT_THROW(
      parseFileCacheRootMode("filecache", "reuse", true),
      VeloxUserError);
}

} // namespace
} // namespace facebook::velox::benchmarks
