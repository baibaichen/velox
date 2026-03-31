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

/// Expression-level benchmark comparing VARIANT extraction through the Velox
/// expression evaluation framework against native column access.
///
/// Uses TPC-H lineitem data with three formats:
///   Format A (columnar 4-col): VARIANT data via variant_extract_* UDFs
///   Format B (row-based 2-col): VARIANT data via variant_extract_* UDFs
///   Format C (native shredded): direct FlatVector column reads
///
/// Benchmark cases (Q6-inspired):
///   SingleField: extract l_shipdate from each row
///   Q6Filter:    l_shipdate >= 8766 AND l_shipdate < 9131
///                AND l_discount >= 0.05 AND l_discount <= 0.07
///                AND l_quantity < 24
///   Q6Project:   l_extendedprice * l_discount

#include "velox/benchmarks/basic/VariantBenchEncoding.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/types/VariantType.h"
#include "velox/functions/prestosql/types/VariantRegistration.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

DEFINE_int64(num_rows, 100'000, "Number of lineitem rows");

using namespace facebook::velox;
using namespace facebook::velox::variant_encoding;
using namespace facebook::velox::variant_bench;

namespace {

// ---------------------------------------------------------------------------
// Data generation (same as VariantFormatBenchmark.cpp, but inline here for
// self-containment).
// ---------------------------------------------------------------------------

/// Build raw VARIANT blobs from TPC-H lineitem RowVector.
FlatVectorPtr<StringView> buildRawBlobs(
    const RowVector* lineitem,
    memory::MemoryPool* pool) {
  auto numRows = lineitem->size();
  auto vec =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);

  auto* orderkey = lineitem->childAt(0)->as<FlatVector<int64_t>>();
  auto* partkey = lineitem->childAt(1)->as<FlatVector<int64_t>>();
  auto* suppkey = lineitem->childAt(2)->as<FlatVector<int64_t>>();
  auto* linenumber = lineitem->childAt(3)->as<FlatVector<int32_t>>();
  auto* quantity = lineitem->childAt(4)->as<FlatVector<double>>();
  auto* extendedprice = lineitem->childAt(5)->as<FlatVector<double>>();
  auto* discount = lineitem->childAt(6)->as<FlatVector<double>>();
  auto* tax = lineitem->childAt(7)->as<FlatVector<double>>();
  auto* returnflag = lineitem->childAt(8)->as<FlatVector<StringView>>();
  auto* linestatus = lineitem->childAt(9)->as<FlatVector<StringView>>();
  auto* shipdate = lineitem->childAt(10)->as<FlatVector<int32_t>>();
  auto* commitdate = lineitem->childAt(11)->as<FlatVector<int32_t>>();
  auto* receiptdate = lineitem->childAt(12)->as<FlatVector<int32_t>>();
  auto* shipinstruct = lineitem->childAt(13)->as<FlatVector<StringView>>();
  auto* shipmode = lineitem->childAt(14)->as<FlatVector<StringView>>();
  auto* comment = lineitem->childAt(15)->as<FlatVector<StringView>>();

  std::string metadata = buildLineitemMetadata();

  std::vector<std::string> blobs(numRows);
  size_t totalBytes = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto rfSv = returnflag->valueAt(i);
    auto lsSv = linestatus->valueAt(i);
    auto siSv = shipinstruct->valueAt(i);
    auto smSv = shipmode->valueAt(i);
    auto cmSv = comment->valueAt(i);

    blobs[i] = buildLineitemBlob(
        metadata,
        orderkey->valueAt(i),
        partkey->valueAt(i),
        suppkey->valueAt(i),
        linenumber->valueAt(i),
        quantity->valueAt(i),
        extendedprice->valueAt(i),
        discount->valueAt(i),
        tax->valueAt(i),
        std::string_view(rfSv.data(), rfSv.size()),
        std::string_view(lsSv.data(), lsSv.size()),
        shipdate->valueAt(i),
        commitdate->valueAt(i),
        receiptdate->valueAt(i),
        std::string_view(siSv.data(), siSv.size()),
        std::string_view(smSv.data(), smSv.size()),
        std::string_view(cmSv.data(), cmSv.size()));
    totalBytes += blobs[i].size();
  }

  auto buf = AlignedBuffer::allocate<char>(totalBytes, pool);
  auto* dest = buf->asMutable<char>();
  size_t offset = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    std::memcpy(dest + offset, blobs[i].data(), blobs[i].size());
    vec->set(i, StringView(dest + offset, blobs[i].size()));
    offset += blobs[i].size();
  }

  return vec;
}

/// Decode raw blobs to Format A (columnar 4-col).
RowVectorPtr decodeToFormatA(
    const FlatVector<StringView>* rawBlobs,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  constexpr uint32_t kFieldsPerRow = field_id::kNumFields;

  vector_size_t totalKeys =
      static_cast<vector_size_t>(numRows) * kFieldsPerRow;
  vector_size_t totalChildren = totalKeys;
  vector_size_t totalValues =
      static_cast<vector_size_t>(numRows) * (1 + kFieldsPerRow);

  size_t totalDataBytes = 0;
  std::vector<size_t> metaLens(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    metaLens[i] = metadataSize(sv.data(), static_cast<size_t>(sv.size()));
    totalDataBytes += static_cast<size_t>(sv.size()) - metaLens[i];
  }

  auto keysChild =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalKeys, pool);
  auto keysOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto keysSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* keysOffsetsPtr = keysOffsets->asMutable<vector_size_t>();
  auto* keysSizesPtr = keysSizes->asMutable<vector_size_t>();

  auto childrenKeysIndex =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalChildren, pool);
  auto childrenValuesIndex =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalChildren, pool);
  auto childrenStruct = std::make_shared<RowVector>(
      pool,
      ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()}),
      nullptr,
      totalChildren,
      std::vector<VectorPtr>{childrenKeysIndex, childrenValuesIndex});
  auto childrenOffsets =
      AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto childrenSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* childrenOffsetsPtr = childrenOffsets->asMutable<vector_size_t>();
  auto* childrenSizesPtr = childrenSizes->asMutable<vector_size_t>();

  auto valuesTypeId =
      BaseVector::create<FlatVector<int8_t>>(TINYINT(), totalValues, pool);
  auto valuesByteOffset =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalValues, pool);
  auto valuesStruct = std::make_shared<RowVector>(
      pool,
      ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()}),
      nullptr,
      totalValues,
      std::vector<VectorPtr>{valuesTypeId, valuesByteOffset});
  auto valuesOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto valuesSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* valuesOffsetsPtr = valuesOffsets->asMutable<vector_size_t>();
  auto* valuesSizesPtr = valuesSizes->asMutable<vector_size_t>();

  auto dataVec =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);
  auto dataBuf = AlignedBuffer::allocate<char>(totalDataBytes, pool);
  auto* dataDestPtr = dataBuf->asMutable<char>();

  vector_size_t keyIdx = 0;
  vector_size_t childIdx = 0;
  vector_size_t valIdx = 0;
  size_t dataOffset = 0;

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    auto metaLen = metaLens[i];
    const char* value = sv.data() + metaLen;
    auto valueLen = static_cast<size_t>(sv.size()) - metaLen;

    std::memcpy(dataDestPtr + dataOffset, value, valueLen);
    dataVec->set(
        i,
        StringView(
            dataDestPtr + dataOffset, static_cast<int32_t>(valueLen)));

    uint32_t numFields = 0;
    uint8_t fieldIdSize = 0;
    uint8_t offsetSize = 0;
    const char* fieldIdsArr = nullptr;
    const char* offsetsArr = nullptr;
    const char* childData = parseObjectHeader(
        value, valueLen, numFields, fieldIdSize, offsetSize,
        fieldIdsArr, offsetsArr);

    uint8_t metaHeader = static_cast<uint8_t>(sv.data()[0]);
    uint8_t metaOffsetSize = ((metaHeader >> 6) & 0x03) + 1;
    uint32_t dictSize = readLE32(sv.data() + 1);

    keysOffsetsPtr[i] = keyIdx;
    keysSizesPtr[i] = static_cast<vector_size_t>(dictSize);

    size_t dictOffsetsStart = 5;
    size_t dictDataStart =
        dictOffsetsStart + (dictSize + 1) * metaOffsetSize;
    for (uint32_t d = 0; d < dictSize; ++d) {
      const char* offBase = sv.data() + dictOffsetsStart;
      uint32_t strStart = readOffset(offBase, metaOffsetSize, d);
      uint32_t strEnd = readOffset(offBase, metaOffsetSize, d + 1);
      const char* keyStr = sv.data() + dictDataStart + strStart;
      auto keyLen = static_cast<int32_t>(strEnd - strStart);
      keysChild->set(keyIdx + d, StringView(keyStr, keyLen));
    }
    keyIdx += dictSize;

    childrenOffsetsPtr[i] = childIdx;
    childrenSizesPtr[i] = static_cast<vector_size_t>(numFields);

    for (uint32_t f = 0; f < numFields; ++f) {
      uint32_t fid = readFieldId(fieldIdsArr, fieldIdSize, f);
      childrenKeysIndex->set(childIdx + f, static_cast<int32_t>(fid));
      childrenValuesIndex->set(
          childIdx + f, static_cast<int32_t>(valIdx + 1 + f));
    }
    childIdx += numFields;

    valuesOffsetsPtr[i] = valIdx;
    valuesSizesPtr[i] = 1 + static_cast<vector_size_t>(numFields);

    valuesTypeId->set(
        valIdx, static_cast<int8_t>(static_cast<uint8_t>(value[0])));
    valuesByteOffset->set(valIdx, 0);
    ++valIdx;

    if (childData) {
      size_t childDataStart = static_cast<size_t>(childData - value);
      for (uint32_t f = 0; f < numFields; ++f) {
        uint32_t childOff = readOffset(offsetsArr, offsetSize, f);
        size_t childPos = childDataStart + childOff;
        uint8_t childHdr = (childPos < valueLen)
            ? static_cast<uint8_t>(value[childPos])
            : header_byte::kNull;
        valuesTypeId->set(valIdx, static_cast<int8_t>(childHdr));
        valuesByteOffset->set(valIdx, static_cast<int32_t>(childPos));
        ++valIdx;
      }
    } else {
      for (uint32_t f = 0; f < numFields; ++f) {
        valuesTypeId->set(valIdx, static_cast<int8_t>(header_byte::kNull));
        valuesByteOffset->set(valIdx, 0);
        ++valIdx;
      }
    }

    dataOffset += valueLen;
  }

  dataVec->addStringBuffer(dataBuf);

  auto keysArray = std::make_shared<ArrayVector>(
      pool, ARRAY(VARCHAR()), nullptr, numRows,
      keysOffsets, keysSizes, keysChild);

  auto childrenArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
      nullptr, numRows,
      childrenOffsets, childrenSizes, childrenStruct);

  auto valuesArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
      nullptr, numRows,
      valuesOffsets, valuesSizes, valuesStruct);

  return std::make_shared<RowVector>(
      pool,
      VARIANT_COLUMNAR(),
      nullptr,
      numRows,
      std::vector<VectorPtr>{keysArray, childrenArray, valuesArray, dataVec});
}

/// Decode raw blobs to Format B (row-based 2-col).
RowVectorPtr decodeToFormatB(
    const FlatVector<StringView>* rawBlobs,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  std::string metadata = buildLineitemMetadata();

  auto metaVec =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);
  auto valueVec =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);

  auto metaBuf =
      AlignedBuffer::allocate<char>(metadata.size() * numRows, pool);
  auto* metaDest = metaBuf->asMutable<char>();

  size_t totalValueBytes = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    totalValueBytes += rawBlobs->valueAt(i).size();
  }
  auto valBuf = AlignedBuffer::allocate<char>(totalValueBytes, pool);
  auto* valDest = valBuf->asMutable<char>();

  size_t valOff = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    std::memcpy(
        metaDest + i * metadata.size(), metadata.data(), metadata.size());
    metaVec->set(
        i, StringView(metaDest + i * metadata.size(), metadata.size()));

    auto blob = rawBlobs->valueAt(i);
    std::memcpy(valDest + valOff, blob.data(), blob.size());
    valueVec->set(i, StringView(valDest + valOff, blob.size()));
    valOff += blob.size();
  }

  return std::make_shared<RowVector>(
      pool,
      VARIANT_ROW_BASED(),
      nullptr,
      numRows,
      std::vector<VectorPtr>{metaVec, valueVec});
}

// ---------------------------------------------------------------------------
// Benchmark state.
// ---------------------------------------------------------------------------

struct BenchState {
  vector_size_t numRows{0};
  RowVectorPtr formatA;
  RowVectorPtr formatB;
  RowVectorPtr nativeData; // Original lineitem (Format C).

  // Pre-resolved UDFs (row-based).
  std::shared_ptr<exec::VectorFunction> extractDateRowBased;
  std::shared_ptr<exec::VectorFunction> extractDiscountRowBased;
  std::shared_ptr<exec::VectorFunction> extractQuantityRowBased;
  std::shared_ptr<exec::VectorFunction> extractPriceRowBased;

  // Pre-resolved UDFs (columnar).
  std::shared_ptr<exec::VectorFunction> extractDateColumnar;
  std::shared_ptr<exec::VectorFunction> extractDiscountColumnar;
  std::shared_ptr<exec::VectorFunction> extractQuantityColumnar;
  std::shared_ptr<exec::VectorFunction> extractPriceColumnar;

  // Execution context.
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<memory::MemoryPool> dataPool;
  std::shared_ptr<memory::MemoryPool> execPool;
  std::unique_ptr<core::ExecCtx> execCtx;
};

std::unique_ptr<BenchState> gState;

/// Resolve a variant_extract UDF with the given input type signature.
std::shared_ptr<exec::VectorFunction> resolveExtractFunc(
    const std::string& udfName,
    const std::string& fieldName,
    const TypePtr& variantType,
    const core::QueryConfig& queryConfig,
    memory::MemoryPool* pool) {
  std::vector<TypePtr> inputTypes = {variantType, VARCHAR()};

  auto fieldNameConst = std::make_shared<ConstantVector<StringView>>(
      pool, 1, false, VARCHAR(), StringView(fieldName));
  std::vector<VectorPtr> constantInputs = {nullptr, fieldNameConst};

  return exec::getVectorFunction(
      udfName, inputTypes, constantInputs, queryConfig);
}

void initState() {
  gState = std::make_unique<BenchState>();

  // Register custom VARIANT types so signature resolution can match them.
  registerVariantTypes();

  // Register UDFs.
  registerVariantExtractFunctions();

  memory::initializeMemoryManager({});
  gState->pool = memory::memoryManager()->addRootPool("variantExprBench");
  gState->dataPool = gState->pool->addLeafChild("data");
  auto* pool = gState->dataPool.get();

  // Generate TPC-H lineitem data.
  auto numRows = static_cast<vector_size_t>(FLAGS_num_rows);
  size_t maxOrders = static_cast<size_t>(numRows) / 4 + 1;
  auto lineitem = tpch::genTpchLineItem(pool, maxOrders);
  if (lineitem->size() > numRows) {
    lineitem->resize(numRows);
  }
  gState->numRows = lineitem->size();
  gState->nativeData = lineitem;

  LOG(INFO) << "Generated " << gState->numRows << " lineitem rows";

  // Build Format A and Format B.
  auto rawBlobs = buildRawBlobs(lineitem.get(), pool);
  gState->formatA = decodeToFormatA(rawBlobs.get(), gState->numRows, pool);
  gState->formatB = decodeToFormatB(rawBlobs.get(), gState->numRows, pool);

  // Set up execution context.
  gState->queryCtx = core::QueryCtx::create();
  gState->execPool = gState->pool->addLeafChild("exec");
  gState->execCtx = std::make_unique<core::ExecCtx>(
      gState->execPool.get(), gState->queryCtx.get());

  const auto& qConfig = gState->queryCtx->queryConfig();
  auto rowBasedType = VARIANT_ROW_BASED();
  auto columnarType = VARIANT_COLUMNAR();

  // Resolve row-based UDFs.
  gState->extractDateRowBased = resolveExtractFunc(
      "variant_extract_date", "l_shipdate", rowBasedType, qConfig, pool);
  gState->extractDiscountRowBased = resolveExtractFunc(
      "variant_extract_double", "l_discount", rowBasedType, qConfig, pool);
  gState->extractQuantityRowBased = resolveExtractFunc(
      "variant_extract_double", "l_quantity", rowBasedType, qConfig, pool);
  gState->extractPriceRowBased = resolveExtractFunc(
      "variant_extract_double", "l_extendedprice", rowBasedType, qConfig, pool);

  // Resolve columnar UDFs.
  gState->extractDateColumnar = resolveExtractFunc(
      "variant_extract_date", "l_shipdate", columnarType, qConfig, pool);
  gState->extractDiscountColumnar = resolveExtractFunc(
      "variant_extract_double", "l_discount", columnarType, qConfig, pool);
  gState->extractQuantityColumnar = resolveExtractFunc(
      "variant_extract_double", "l_quantity", columnarType, qConfig, pool);
  gState->extractPriceColumnar = resolveExtractFunc(
      "variant_extract_double", "l_extendedprice", columnarType, qConfig, pool);
}

// ---------------------------------------------------------------------------
// Benchmark: SingleField — extract l_shipdate (DATE) from each row.
// ---------------------------------------------------------------------------

/// Format A: variant_extract_date(v, 'l_shipdate') via columnar UDF apply().
void singleFieldFormatA() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatA};
  VectorPtr result;

  gState->extractDateColumnar->apply(rows, args, INTEGER(), evalCtx, result);

  int64_t sum = 0;
  auto* flat = result->as<FlatVector<int32_t>>();
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += flat->valueAt(i);
  }
  folly::doNotOptimizeAway(sum);
}

/// Format B: variant_extract_date(v, 'l_shipdate') via UDF apply().
void singleFieldFormatB() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatB};
  VectorPtr result;

  gState->extractDateRowBased->apply(rows, args, INTEGER(), evalCtx, result);

  int64_t sum = 0;
  auto* flat = result->as<FlatVector<int32_t>>();
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += flat->valueAt(i);
  }
  folly::doNotOptimizeAway(sum);
}

/// Format C: direct read from shredded FlatVector<int32_t>.
void singleFieldFormatC() {
  auto* shipdate =
      gState->nativeData->childAt(10)->as<FlatVector<int32_t>>()->rawValues();
  int64_t sum = 0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += shipdate[i];
  }
  folly::doNotOptimizeAway(sum);
}

// ---------------------------------------------------------------------------
// Benchmark: Q6Filter — multi-field filter predicate.
// ---------------------------------------------------------------------------

/// Format A: columnar extraction for Q6 filter.
void q6FilterFormatA() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatA};
  VectorPtr shipdateResult;
  gState->extractDateColumnar->apply(
      rows, args, INTEGER(), evalCtx, shipdateResult);

  VectorPtr discountResult;
  gState->extractDiscountColumnar->apply(
      rows, args, DOUBLE(), evalCtx, discountResult);

  VectorPtr quantityResult;
  gState->extractQuantityColumnar->apply(
      rows, args, DOUBLE(), evalCtx, quantityResult);

  auto* sd = shipdateResult->as<FlatVector<int32_t>>()->rawValues();
  auto* disc = discountResult->as<FlatVector<double>>()->rawValues();
  auto* qty = quantityResult->as<FlatVector<double>>()->rawValues();

  size_t matchCount = 0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    if (sd[i] >= 8'766 && sd[i] < 9'131 && disc[i] >= 0.05 &&
        disc[i] <= 0.07 && qty[i] < 24.0) {
      ++matchCount;
    }
  }
  folly::doNotOptimizeAway(matchCount);
}

/// Format B: extract multiple fields and apply the Q6 filter predicate.
/// l_shipdate >= 8766 (1994-01-01) AND l_shipdate < 9131 (1995-01-01)
/// AND l_discount >= 0.05 AND l_discount <= 0.07
/// AND l_quantity < 24.0
void q6FilterFormatB() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatB};
  VectorPtr shipdateResult;
  gState->extractDateRowBased->apply(
      rows, args, INTEGER(), evalCtx, shipdateResult);

  VectorPtr discountResult;
  gState->extractDiscountRowBased->apply(
      rows, args, DOUBLE(), evalCtx, discountResult);

  VectorPtr quantityResult;
  gState->extractQuantityRowBased->apply(
      rows, args, DOUBLE(), evalCtx, quantityResult);

  auto* sd = shipdateResult->as<FlatVector<int32_t>>()->rawValues();
  auto* disc = discountResult->as<FlatVector<double>>()->rawValues();
  auto* qty = quantityResult->as<FlatVector<double>>()->rawValues();

  size_t matchCount = 0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    if (sd[i] >= 8'766 && sd[i] < 9'131 && disc[i] >= 0.05 &&
        disc[i] <= 0.07 && qty[i] < 24.0) {
      ++matchCount;
    }
  }
  folly::doNotOptimizeAway(matchCount);
}

/// Format C: native column access.
void q6FilterFormatC() {
  auto* sd =
      gState->nativeData->childAt(10)->as<FlatVector<int32_t>>()->rawValues();
  auto* disc =
      gState->nativeData->childAt(6)->as<FlatVector<double>>()->rawValues();
  auto* qty =
      gState->nativeData->childAt(4)->as<FlatVector<double>>()->rawValues();

  size_t matchCount = 0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    if (sd[i] >= 8'766 && sd[i] < 9'131 && disc[i] >= 0.05 &&
        disc[i] <= 0.07 && qty[i] < 24.0) {
      ++matchCount;
    }
  }
  folly::doNotOptimizeAway(matchCount);
}

// ---------------------------------------------------------------------------
// Benchmark: Q6Project — l_extendedprice * l_discount.
// ---------------------------------------------------------------------------

/// Format A: columnar extraction for l_extendedprice * l_discount.
void q6ProjectFormatA() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatA};

  VectorPtr priceResult;
  gState->extractPriceColumnar->apply(
      rows, args, DOUBLE(), evalCtx, priceResult);

  VectorPtr discountResult;
  gState->extractDiscountColumnar->apply(
      rows, args, DOUBLE(), evalCtx, discountResult);

  auto* price = priceResult->as<FlatVector<double>>()->rawValues();
  auto* disc = discountResult->as<FlatVector<double>>()->rawValues();

  double sum = 0.0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += price[i] * disc[i];
  }
  folly::doNotOptimizeAway(sum);
}

/// Format B: extract two doubles and multiply.
void q6ProjectFormatB() {
  SelectivityVector rows(gState->numRows);
  exec::EvalCtx evalCtx(gState->execCtx.get());

  std::vector<VectorPtr> args = {gState->formatB};

  VectorPtr priceResult;
  gState->extractPriceRowBased->apply(
      rows, args, DOUBLE(), evalCtx, priceResult);

  VectorPtr discountResult;
  gState->extractDiscountRowBased->apply(
      rows, args, DOUBLE(), evalCtx, discountResult);

  auto* price = priceResult->as<FlatVector<double>>()->rawValues();
  auto* disc = discountResult->as<FlatVector<double>>()->rawValues();

  double sum = 0.0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += price[i] * disc[i];
  }
  folly::doNotOptimizeAway(sum);
}

/// Format C: native column access.
void q6ProjectFormatC() {
  auto* price =
      gState->nativeData->childAt(5)->as<FlatVector<double>>()->rawValues();
  auto* disc =
      gState->nativeData->childAt(6)->as<FlatVector<double>>()->rawValues();

  double sum = 0.0;
  for (vector_size_t i = 0; i < gState->numRows; ++i) {
    sum += price[i] * disc[i];
  }
  folly::doNotOptimizeAway(sum);
}

} // namespace

// ---------------------------------------------------------------------------
// Benchmark registration.
// ---------------------------------------------------------------------------

BENCHMARK(SingleField_FormatA_Columnar) {
  singleFieldFormatA();
}

BENCHMARK_RELATIVE(SingleField_FormatB_RowBased) {
  singleFieldFormatB();
}

BENCHMARK_RELATIVE(SingleField_FormatC_NativeShredded) {
  singleFieldFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(Q6Filter_FormatA_Columnar) {
  q6FilterFormatA();
}

BENCHMARK_RELATIVE(Q6Filter_FormatB_RowBased) {
  q6FilterFormatB();
}

BENCHMARK_RELATIVE(Q6Filter_FormatC_NativeShredded) {
  q6FilterFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(Q6Project_FormatA_Columnar) {
  q6ProjectFormatA();
}

BENCHMARK_RELATIVE(Q6Project_FormatB_RowBased) {
  q6ProjectFormatB();
}

BENCHMARK_RELATIVE(Q6Project_FormatC_NativeShredded) {
  q6ProjectFormatC();
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  initState();
  folly::runBenchmarks();
  // Release vectors before pools to avoid memory leak assertions.
  gState->formatA.reset();
  gState->formatB.reset();
  gState->nativeData.reset();
  gState->extractDateRowBased.reset();
  gState->extractDiscountRowBased.reset();
  gState->extractQuantityRowBased.reset();
  gState->extractPriceRowBased.reset();
  gState->extractDateColumnar.reset();
  gState->extractDiscountColumnar.reset();
  gState->extractQuantityColumnar.reset();
  gState->extractPriceColumnar.reset();
  gState->execCtx.reset();
  gState.reset();
  return 0;
}
