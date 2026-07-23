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

#include <sstream>
#include <string>
#include <vector>

#include "velox/benchmarks/AbBenchmarkBase.h"
#include "velox/benchmarks/AbBenchmarkMain.h"

namespace facebook::velox::benchmarks {
namespace {

// The exact 14-field header required by Task 018-C Step 0c.
constexpr const char* kExpectedHeader =
    "round,query_id,wall_ms,rows,result_hash,bytes_read,hit_pct,"
    "cache_read_mib,predownload_mib,evict_mib,evict_count,"
    "op_p50_us,op_p95_us,error";

TEST(AbBenchmarkSchemaTest, CsvHeaderHasExactly14Fields)
{
  std::ostringstream oss;
  writeCsvHeader(oss);
  std::string header = oss.str();
  // Remove trailing newline for comparison.
  if (!header.empty() && header.back() == '\n')
  {
    header.pop_back();
  }
  EXPECT_EQ(header, kExpectedHeader)
      << "CSV header must match the 14-field schema exactly";
  // Count commas (N-1 commas for N fields).
  int commas = 0;
  for (char c : header)
  {
    if (c == ',')
      ++commas;
  }
  EXPECT_EQ(commas, 13) << "14 fields require exactly 13 commas";
}

TEST(AbBenchmarkSchemaTest, CsvRowHas14FieldsForSuccess)
{
  AbCsvRow row;
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 123.456;
  row.rows = 100;
  row.resultHash = 999;
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
  EXPECT_EQ(commas, 13) << "14 fields require exactly 13 commas";
}

TEST(AbBenchmarkSchemaTest, BackendSnapshotFileCacheMapping)
{
  // Verify that FileCache backend populates cacheReadBytes, predownloadBytes,
  // evictedBytes, evictionCount as separate quantities.
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
  // CBI: cacheReadBytes is hitBytes, predownload is zero, evictedBytes is zero,
  // evictionCount is numEvict.
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
  // If someone accidentally puts evictionCount into evictMib, the values would
  // not make sense. This test ensures the numeric distinction is maintained:
  // evict_mib should be in MiB (bytes / 2^20), evict_count should be an integer.
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
  // evict_mib must be 7.0 (from bytes), NOT 7 (from count)
  EXPECT_DOUBLE_EQ(row.evictMib, 7.0);
  // evict_count must be integer 7 (from segments), NOT bytes
  EXPECT_EQ(row.evictCount, 7);
  // Key assertion: if bytes and count were swapped, evictMib would be
  // 7 / (1024*1024) ≈ 0.0000067, not 7.0, and evictCount would be
  // 1024*1024*7 = 7340032, not 7.
  EXPECT_GT(row.evictMib, 1.0);
  EXPECT_LT(row.evictCount, 100);
}

TEST(AbBenchmarkSchemaTest, CsvRowHas14FieldsForFailure)
{
  // The actual fixed error string emitted by AbBenchmarkBase::runAb() when
  // cursor == nullptr (task failure). The row must still round-trip through
  // exactly 14 fields even with a non-empty error column.
  AbCsvRow row;
  row.round = 1;
  row.queryId = 1;
  row.wallMs = 12.0;
  row.rows = 0;
  row.resultHash = 0;
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
  EXPECT_EQ(commas, 13) << "14 fields require exactly 13 commas, even on failure";
}

TEST(AbBenchmarkSchemaTest, ExitCodeIsZeroWhenNoQueriesFailed)
{
  EXPECT_EQ(abExitCode(0), 0);
}

TEST(AbBenchmarkSchemaTest, ExitCodeIsNonzeroForAnyFailedQuery)
{
  // Any failed query (however few) must produce a nonzero process exit code;
  // there must be no soft cap that tolerates a handful of failures.
  EXPECT_NE(abExitCode(1), 0);
  EXPECT_NE(abExitCode(10), 0);
}

} // namespace
} // namespace facebook::velox::benchmarks
