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

#include <sstream>
#include <string>
#include <vector>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/benchmarks/AbBenchmarkMain.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::benchmarks {
namespace {

using namespace facebook::velox::test;

// The exact 15-field header required by Task 018 parallel verification.
constexpr const char* kExpectedHeader =
    "round,query_id,wall_ms,rows,result_hash,result_match,bytes_read,hit_pct,"
    "cache_read_mib,predownload_mib,evict_mib,evict_count,"
    "op_p50_us,op_p95_us,error";

TEST(AbBenchmarkSchemaTest, CsvHeaderHasExactly15Fields)
{
  std::ostringstream oss;
  writeCsvHeader(oss);
  std::string header = oss.str();
  if (!header.empty() && header.back() == '\n')
  {
    header.pop_back();
  }
  EXPECT_EQ(header, kExpectedHeader)
      << "CSV header must match the 15-field schema exactly";
  int commas = 0;
  for (char c : header)
  {
    if (c == ',')
      ++commas;
  }
  EXPECT_EQ(commas, 14) << "15 fields require exactly 14 commas";
}

TEST(AbBenchmarkSchemaTest, CsvRowHas15FieldsForSuccess)
{
  AbCsvRow row;
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 123.456;
  row.rows = 100;
  row.resultHash = 999;
  row.resultMatch = std::nullopt;
  row.bytesRead = 5000;
  row.hitPct = 95.0;
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
  EXPECT_EQ(commas, 14) << "15 fields require exactly 14 commas";
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
  ASSERT_EQ(fields.size(), 15) << "Must have exactly 15 fields";
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
  ASSERT_EQ(fields.size(), 15) << "Must have exactly 15 fields";
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
  ASSERT_EQ(fields.size(), 15) << "Must have exactly 15 fields";
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
  snap.cacheReadBytes = 1024 * 1024 * 10;   // 10 MiB
  snap.predownloadBytes = 1024 * 1024 * 2;   // 2 MiB
  snap.evictedBytes = 1024 * 1024 * 3;       // 3 MiB
  snap.evictionCount = 42;

  BackendSnapshot before;
  before.lookups = 0;
  before.hits = 0;
  before.cacheReadBytes = 0;
  before.predownloadBytes = 0;
  before.evictedBytes = 0;
  before.evictionCount = 0;

  AbCsvRow row{};
  populateBackendDelta(row, before, snap);
  EXPECT_DOUBLE_EQ(row.hitPct, 80.0);
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
  snap.cacheReadBytes = 1024 * 1024 * 5;  // 5 MiB (hitBytes)
  snap.predownloadBytes = 0;
  snap.evictedBytes = 0;
  snap.evictionCount = 12;

  BackendSnapshot before;
  before.lookups = 0;
  before.hits = 0;
  before.cacheReadBytes = 0;
  before.predownloadBytes = 0;
  before.evictedBytes = 0;
  before.evictionCount = 0;

  AbCsvRow row{};
  populateBackendDelta(row, before, snap);
  EXPECT_DOUBLE_EQ(row.hitPct, 80.0);
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

TEST(AbBenchmarkSchemaTest, CsvRowHas15FieldsForFailure)
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
  EXPECT_EQ(commas, 14) << "15 fields require exactly 14 commas, even on failure";
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
  ASSERT_EQ(fields.size(), 15) << "Must have exactly 15 fields";
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

} // namespace
} // namespace facebook::velox::benchmarks
