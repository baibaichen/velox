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

/// Micro-benchmark comparing three VARIANT in-memory formats using TPC-H
/// lineitem data (16 fields, mixed types):
///   Format A (DuckDB 4-col): keys + children + values + data
///   Format B (StarRocks 2-col): metadata + value as separate VARBINARY columns
///   Format C (shredded): pre-extracted typed columns (upper bound)
///
/// Benchmark kernels:
///   FieldExtract:       extract l_shipdate (DATE) from each row
///   MultiFieldExtract:  extract l_extendedprice, l_discount, l_tax (3 DOUBLEs)
///   FieldExtractFilter: extract l_shipdate + filter <= threshold
///   ParquetDecode:      decode raw blob to Format A / Format B
///   RandomAccess:       random index read of data/value blob

#include "velox/benchmarks/basic/VariantBenchEncoding.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/functions/prestosql/types/VariantType.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

DEFINE_int64(num_rows, 1'000'000, "Target number of lineitem rows");

using namespace facebook::velox;
using namespace facebook::velox::variant_encoding;
using namespace facebook::velox::variant_bench;

namespace {

// ---------------------------------------------------------------------------
// Dataset holding all format representations.
// ---------------------------------------------------------------------------

struct Dataset {
  vector_size_t numRows{0};

  /// Raw Parquet VARIANT blobs: metadata + Object value per row.
  FlatVectorPtr<StringView> rawBlobs;

  /// Format A (DuckDB 4-col): RowVector with keys/children/values/data.
  RowVectorPtr formatA;

  /// Format B (StarRocks 2-col): ROW(metadata, value) as VARIANT_ROW_BASED.
  RowVectorPtr formatB;

  /// Accessor for the value column of Format B.
  FlatVector<StringView>* valueColB() const {
    return formatB->childAt(1)->as<FlatVector<StringView>>();
  }

  /// Format C (shredded): pre-extracted typed columns.
  FlatVectorPtr<int32_t> shreddedShipdate;
  FlatVectorPtr<double> shreddedExtendedprice;
  FlatVectorPtr<double> shreddedDiscount;
  FlatVectorPtr<double> shreddedTax;

  /// Pre-shuffled random indices for random access benchmarks.
  std::vector<vector_size_t> randomIndices;
};

// ---------------------------------------------------------------------------
// Build raw VARIANT blobs from TPC-H lineitem RowVector.
// ---------------------------------------------------------------------------

FlatVectorPtr<StringView> buildRawBlobs(
    const RowVector* lineitem,
    memory::MemoryPool* pool) {
  auto numRows = lineitem->size();
  auto vec = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);

  // Extract typed column pointers.
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

  // Pre-build metadata once (shared by all rows).
  std::string metadata = buildLineitemMetadata();

  // First pass: build all blobs and compute total size.
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

  // Copy into a single contiguous buffer.
  auto buf = AlignedBuffer::allocate<char>(totalBytes, pool);
  auto* dest = buf->asMutable<char>();
  size_t offset = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    std::memcpy(dest + offset, blobs[i].data(), blobs[i].size());
    vec->set(
        i, StringView(dest + offset, static_cast<int32_t>(blobs[i].size())));
    offset += blobs[i].size();
  }
  vec->addStringBuffer(buf);

  return vec;
}

// ---------------------------------------------------------------------------
// Decode raw blobs to Format A (DuckDB 4-col).
//
// For lineitem, every row is an Object with 16 fields.
// ---------------------------------------------------------------------------

RowVectorPtr decodeToFormatA(
    const FlatVector<StringView>* rawBlobs,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  // All rows are Objects with 16 fields.
  constexpr uint32_t kFieldsPerRow = field_id::kNumFields;

  // Count list elements.
  // keys: each row has 16 dictionary entries.
  vector_size_t totalKeys =
      static_cast<vector_size_t>(numRows) * kFieldsPerRow;
  // children: 16 per row.
  vector_size_t totalChildren = totalKeys;
  // values: 1 (root) + 16 (children) = 17 per row.
  vector_size_t totalValues =
      static_cast<vector_size_t>(numRows) * (1 + kFieldsPerRow);

  // Compute total data bytes and cache per-row metadata lengths.
  size_t totalDataBytes = 0;
  std::vector<size_t> metaLens(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    metaLens[i] = metadataSize(sv.data(), static_cast<size_t>(sv.size()));
    totalDataBytes += static_cast<size_t>(sv.size()) - metaLens[i];
  }

  // --- Allocate keys ArrayVector ---
  auto keysChild =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), totalKeys, pool);
  auto keysOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto keysSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* keysOffsetsPtr = keysOffsets->asMutable<vector_size_t>();
  auto* keysSizesPtr = keysSizes->asMutable<vector_size_t>();

  // --- Allocate children ArrayVector ---
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

  // --- Allocate values ArrayVector ---
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

  // --- Allocate data FlatVector ---
  auto dataVec =
      BaseVector::create<FlatVector<StringView>>(VARBINARY(), numRows, pool);
  auto dataBuf = AlignedBuffer::allocate<char>(totalDataBytes, pool);
  auto* dataDestPtr = dataBuf->asMutable<char>();

  // --- Populate ---
  vector_size_t keyIdx = 0;
  vector_size_t childIdx = 0;
  vector_size_t valIdx = 0;
  size_t dataOffset = 0;

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    auto metaLen = metaLens[i];
    const char* value = sv.data() + metaLen;
    auto valueLen = static_cast<size_t>(sv.size()) - metaLen;

    // Copy value portion into data buffer.
    std::memcpy(dataDestPtr + dataOffset, value, valueLen);
    dataVec->set(
        i,
        StringView(
            dataDestPtr + dataOffset, static_cast<int32_t>(valueLen)));

    // Parse the Object header.
    uint32_t numFields = 0;
    uint8_t fieldIdSize = 0;
    uint8_t offsetSize = 0;
    const char* fieldIdsArr = nullptr;
    const char* offsetsArr = nullptr;
    const char* childData = parseObjectHeader(
        value, valueLen, numFields, fieldIdSize, offsetSize,
        fieldIdsArr, offsetsArr);

    // Parse metadata dictionary.
    uint8_t metaHeader = static_cast<uint8_t>(sv.data()[0]);
    uint8_t metaOffsetSize = ((metaHeader >> 6) & 0x03) + 1;
    uint32_t dictSize = readLE32(sv.data() + 1);

    keysOffsetsPtr[i] = keyIdx;
    keysSizesPtr[i] = static_cast<vector_size_t>(dictSize);

    // Extract dictionary strings.
    size_t dictOffsetsStart = 5;
    size_t dictDataStart =
        dictOffsetsStart + (dictSize + 1) * metaOffsetSize;
    for (uint32_t d = 0; d < dictSize; ++d) {
      uint32_t strStart = 0;
      uint32_t strEnd = 0;
      const char* offBase = sv.data() + dictOffsetsStart;
      strStart = readOffset(offBase, metaOffsetSize, d);
      strEnd = readOffset(offBase, metaOffsetSize, d + 1);
      const char* keyStr = sv.data() + dictDataStart + strStart;
      auto keyLen = static_cast<int32_t>(strEnd - strStart);
      keysChild->set(keyIdx + d, StringView(keyStr, keyLen));
    }
    keyIdx += dictSize;

    // Children: field_id → keys_index, values_index.
    childrenOffsetsPtr[i] = childIdx;
    childrenSizesPtr[i] = static_cast<vector_size_t>(numFields);

    for (uint32_t f = 0; f < numFields; ++f) {
      uint32_t fid = readFieldId(fieldIdsArr, fieldIdSize, f);
      childrenKeysIndex->set(
          childIdx + f, static_cast<int32_t>(fid));
      childrenValuesIndex->set(
          childIdx + f, static_cast<int32_t>(valIdx + 1 + f));
    }
    childIdx += numFields;

    // Values: root entry + one entry per child.
    valuesOffsetsPtr[i] = valIdx;
    valuesSizesPtr[i] = 1 + static_cast<vector_size_t>(numFields);

    // Root value entry (Object type).
    uint8_t rootBasicType = valueBasicType(static_cast<uint8_t>(value[0]));
    valuesTypeId->set(
        valIdx, static_cast<int8_t>(static_cast<uint8_t>(value[0])));
    valuesByteOffset->set(valIdx, 0);
    ++valIdx;

    // Child value entries.
    if (childData) {
      size_t childDataStart =
          static_cast<size_t>(childData - value);
      for (uint32_t f = 0; f < numFields; ++f) {
        uint32_t childOff = readOffset(offsetsArr, offsetSize, f);
        size_t childPos = childDataStart + childOff;
        uint8_t childHdr = (childPos < valueLen)
            ? static_cast<uint8_t>(value[childPos])
            : header_byte::kNull;
        valuesTypeId->set(valIdx, static_cast<int8_t>(childHdr));
        valuesByteOffset->set(
            valIdx, static_cast<int32_t>(childPos));
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

  // Build ArrayVectors.
  auto keysArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(VARCHAR()),
      nullptr,
      numRows,
      keysOffsets,
      keysSizes,
      keysChild);

  auto childrenArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
      nullptr,
      numRows,
      childrenOffsets,
      childrenSizes,
      childrenStruct);

  auto valuesArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
      nullptr,
      numRows,
      valuesOffsets,
      valuesSizes,
      valuesStruct);

  return std::make_shared<VariantVector>(
      pool,
      VARIANT_COLUMNAR(),
      nullptr,
      numRows,
      std::vector<VectorPtr>{keysArray, childrenArray, valuesArray, dataVec});
}

// ---------------------------------------------------------------------------
// Decode raw blobs to Format B (StarRocks 2-col).
// ---------------------------------------------------------------------------

RowVectorPtr decodeToFormatB(
    const FlatVector<StringView>* rawBlobs,
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  auto metadataOut = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);
  auto valueOut = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);

  size_t totalMetaBytes = 0;
  size_t totalValueBytes = 0;
  std::vector<size_t> metaLensB(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    metaLensB[i] =
        metadataSize(sv.data(), static_cast<size_t>(sv.size()));
    totalMetaBytes += metaLensB[i];
    totalValueBytes += static_cast<size_t>(sv.size()) - metaLensB[i];
  }

  auto metaBuf = AlignedBuffer::allocate<char>(totalMetaBytes, pool);
  auto valBuf = AlignedBuffer::allocate<char>(totalValueBytes, pool);
  auto* metaDest = metaBuf->asMutable<char>();
  auto* valDest = valBuf->asMutable<char>();

  size_t metaOff = 0;
  size_t valOff = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    auto sv = rawBlobs->valueAt(i);
    auto metaLen = metaLensB[i];
    auto valueLen = static_cast<size_t>(sv.size()) - metaLen;

    std::memcpy(metaDest + metaOff, sv.data(), metaLen);
    metadataOut->set(
        i,
        StringView(metaDest + metaOff, static_cast<int32_t>(metaLen)));
    metaOff += metaLen;

    std::memcpy(valDest + valOff, sv.data() + metaLen, valueLen);
    valueOut->set(
        i,
        StringView(valDest + valOff, static_cast<int32_t>(valueLen)));
    valOff += valueLen;
  }

  metadataOut->addStringBuffer(metaBuf);
  valueOut->addStringBuffer(valBuf);

  return std::make_shared<VariantVector>(
      pool,
      VARIANT_ROW_BASED(),
      nullptr,
      numRows,
      std::vector<VectorPtr>{metadataOut, valueOut});
}

// ---------------------------------------------------------------------------
// Build the full dataset.
// ---------------------------------------------------------------------------

Dataset makeDataset(vector_size_t numRows, memory::MemoryPool* pool) {
  Dataset ds;

  // Generate TPC-H lineitem data. maxOrdersRows ≈ numRows/4 since each
  // order has ~4 lineitems on average.
  size_t maxOrders = static_cast<size_t>(numRows) / 4 + 1;
  auto lineitem = tpch::genTpchLineItem(pool, maxOrders);

  // Trim to requested numRows if we got more.
  auto actualRows = lineitem->size();
  if (actualRows > static_cast<vector_size_t>(numRows)) {
    lineitem->resize(numRows);
    actualRows = numRows;
  }
  ds.numRows = actualRows;

  LOG(INFO) << "Generated " << actualRows << " lineitem rows (requested "
            << numRows << ", maxOrders=" << maxOrders << ")";

  // Build raw VARIANT blobs.
  ds.rawBlobs = buildRawBlobs(lineitem.get(), pool);

  // Decode to Format A.
  ds.formatA = decodeToFormatA(ds.rawBlobs.get(), ds.numRows, pool);

  // Decode to Format B.
  ds.formatB = decodeToFormatB(ds.rawBlobs.get(), ds.numRows, pool);

  // Build Format C (shredded): extract columns directly from lineitem.
  // l_shipdate (col 10), l_extendedprice (col 5), l_discount (col 6),
  // l_tax (col 7).
  ds.shreddedShipdate = std::dynamic_pointer_cast<FlatVector<int32_t>>(
      lineitem->childAt(10));
  ds.shreddedExtendedprice = std::dynamic_pointer_cast<FlatVector<double>>(
      lineitem->childAt(5));
  ds.shreddedDiscount = std::dynamic_pointer_cast<FlatVector<double>>(
      lineitem->childAt(6));
  ds.shreddedTax = std::dynamic_pointer_cast<FlatVector<double>>(
      lineitem->childAt(7));

  // Build random indices.
  ds.randomIndices.resize(ds.numRows);
  std::iota(ds.randomIndices.begin(), ds.randomIndices.end(), 0);
  std::mt19937 rng(123);
  std::shuffle(ds.randomIndices.begin(), ds.randomIndices.end(), rng);

  return ds;
}

// Global state.
std::shared_ptr<memory::MemoryPool> gPool;
std::unique_ptr<Dataset> gDataset;

// ---------------------------------------------------------------------------
// Format A accessor helpers.
// ---------------------------------------------------------------------------

const ArrayVector* formatAValues() {
  return gDataset->formatA->childAt(2)->as<ArrayVector>();
}

const FlatVector<int8_t>* formatATypeIds() {
  return formatAValues()
      ->elements()
      ->as<RowVector>()
      ->childAt(0)
      ->as<FlatVector<int8_t>>();
}

const FlatVector<int32_t>* formatAByteOffsets() {
  return formatAValues()
      ->elements()
      ->as<RowVector>()
      ->childAt(1)
      ->as<FlatVector<int32_t>>();
}

const FlatVector<StringView>* formatAData() {
  return gDataset->formatA->childAt(3)->as<FlatVector<StringView>>();
}

// TPC-H Q1 filter threshold: 1998-09-02 as days since epoch.
// 1998-09-02 = 10471 days since 1970-01-01.
constexpr int32_t kShipdateThreshold = 10'471;

// ---------------------------------------------------------------------------
// FieldExtract: extract l_shipdate (DATE, field_id=11) from each row.
// ---------------------------------------------------------------------------

/// Format A: navigate values[row] to find the child with the right header,
/// then read 4B date from data blob.
size_t fieldExtractFormatA() {
  int64_t sum = 0;
  auto* valuesArr = formatAValues();
  auto* byteOffsetVec = formatAByteOffsets();
  auto* typeIdVec = formatATypeIds();
  auto* dataVec = formatAData();
  constexpr uint8_t kTargetFieldIdx = field_id::kShipdate;

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valOffset = valuesArr->offsetAt(i);
    // Child values start at valOffset + 1. The shipdate field is at sorted
    // index kTargetFieldIdx within the children.
    auto childValIdx = valOffset + 1 + kTargetFieldIdx;
    auto byteOff = byteOffsetVec->valueAt(childValIdx);

    auto dataSv = dataVec->valueAt(i);
    // Read DATE: header(1B) + 4B LE.
    auto pos = static_cast<size_t>(byteOff);
    if (pos + 5 <= static_cast<size_t>(dataSv.size())) {
      int32_t days = static_cast<int32_t>(readLE32(dataSv.data() + pos + 1));
      sum += days;
    }
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

/// Format B: parse Object header from value blob, locate field_id=11, read
/// DATE.
size_t fieldExtractFormatB() {
  int64_t sum = 0;
  auto* valueCol = gDataset->valueColB();

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    int32_t days;
    if (extractDateField(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            field_id::kShipdate,
            days)) {
      sum += days;
    }
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

/// Format C: direct read from shredded FlatVector<int32_t>.
size_t fieldExtractFormatC() {
  int64_t sum = 0;
  auto* shipdate = gDataset->shreddedShipdate->rawValues();
  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    sum += shipdate[i];
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

// ---------------------------------------------------------------------------
// MultiFieldExtract: extract l_extendedprice, l_discount, l_tax (3 DOUBLEs).
// ---------------------------------------------------------------------------

/// Format A: read 3 DOUBLE children from data blob.
size_t multiFieldExtractFormatA() {
  double total = 0;
  auto* valuesArr = formatAValues();
  auto* byteOffsetVec = formatAByteOffsets();
  auto* dataVec = formatAData();

  constexpr uint8_t kFields[] = {
      field_id::kExtendedprice,
      field_id::kDiscount,
      field_id::kTax,
  };

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valOffset = valuesArr->offsetAt(i);
    auto dataSv = dataVec->valueAt(i);

    for (auto fid : kFields) {
      auto childValIdx = valOffset + 1 + fid;
      auto byteOff = byteOffsetVec->valueAt(childValIdx);
      auto pos = static_cast<size_t>(byteOff);
      if (pos + 9 <= static_cast<size_t>(dataSv.size())) {
        total += readLEDouble(dataSv.data() + pos + 1);
      }
    }
  }
  folly::doNotOptimizeAway(total);
  return gDataset->numRows;
}

/// Format B: parse Object header once per row, extract 3 DOUBLEs.
size_t multiFieldExtractFormatB() {
  double total = 0;
  auto* valueCol = gDataset->valueColB();

  constexpr uint8_t kFields[] = {
      field_id::kExtendedprice,
      field_id::kDiscount,
      field_id::kTax,
  };

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    double vals[3];
    if (extractMultipleDoubleFields(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            kFields,
            3,
            vals)) {
      total += vals[0] + vals[1] + vals[2];
    }
  }
  folly::doNotOptimizeAway(total);
  return gDataset->numRows;
}

/// Format C: direct read from 3 shredded FlatVector<double>.
size_t multiFieldExtractFormatC() {
  double total = 0;
  auto* ep = gDataset->shreddedExtendedprice->rawValues();
  auto* disc = gDataset->shreddedDiscount->rawValues();
  auto* tax = gDataset->shreddedTax->rawValues();
  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    total += ep[i] + disc[i] + tax[i];
  }
  folly::doNotOptimizeAway(total);
  return gDataset->numRows;
}

// ---------------------------------------------------------------------------
// FieldExtractFilter: extract l_shipdate + filter <= threshold.
// ---------------------------------------------------------------------------

/// Format A.
size_t fieldExtractFilterFormatA() {
  size_t count = 0;
  auto* valuesArr = formatAValues();
  auto* byteOffsetVec = formatAByteOffsets();
  auto* dataVec = formatAData();

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valOffset = valuesArr->offsetAt(i);
    auto childValIdx = valOffset + 1 + field_id::kShipdate;
    auto byteOff = byteOffsetVec->valueAt(childValIdx);
    auto dataSv = dataVec->valueAt(i);
    auto pos = static_cast<size_t>(byteOff);
    if (pos + 5 <= static_cast<size_t>(dataSv.size())) {
      int32_t days = static_cast<int32_t>(readLE32(dataSv.data() + pos + 1));
      if (days <= kShipdateThreshold) {
        ++count;
      }
    }
  }
  folly::doNotOptimizeAway(count);
  return gDataset->numRows;
}

/// Format B.
size_t fieldExtractFilterFormatB() {
  size_t count = 0;
  auto* valueCol = gDataset->valueColB();

  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    int32_t days;
    if (extractDateField(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            field_id::kShipdate,
            days)) {
      if (days <= kShipdateThreshold) {
        ++count;
      }
    }
  }
  folly::doNotOptimizeAway(count);
  return gDataset->numRows;
}

/// Format C.
size_t fieldExtractFilterFormatC() {
  size_t count = 0;
  auto* shipdate = gDataset->shreddedShipdate->rawValues();
  for (vector_size_t i = 0; i < gDataset->numRows; ++i) {
    if (shipdate[i] <= kShipdateThreshold) {
      ++count;
    }
  }
  folly::doNotOptimizeAway(count);
  return gDataset->numRows;
}

// ---------------------------------------------------------------------------
// ParquetDecode benchmarks.
// ---------------------------------------------------------------------------

size_t benchDecodeToFormatA() {
  auto result = decodeToFormatA(
      gDataset->rawBlobs.get(), gDataset->numRows, gPool.get());
  folly::doNotOptimizeAway(result.get());
  return gDataset->numRows;
}

size_t benchDecodeToFormatB() {
  auto result = decodeToFormatB(
      gDataset->rawBlobs.get(), gDataset->numRows, gPool.get());
  folly::doNotOptimizeAway(result.get());
  return gDataset->numRows;
}

// ---------------------------------------------------------------------------
// RandomAccess: random-order field extraction (l_shipdate) to stress cache.
// ---------------------------------------------------------------------------

size_t randomAccessFormatA() {
  int64_t sum = 0;
  auto* valuesArr = formatAValues();
  auto* byteOffsetVec = formatAByteOffsets();
  auto* dataVec = formatAData();
  const auto& indices = gDataset->randomIndices;

  for (auto idx : indices) {
    auto valOffset = valuesArr->offsetAt(idx);
    auto childValIdx = valOffset + 1 + field_id::kShipdate;
    auto byteOff = byteOffsetVec->valueAt(childValIdx);
    auto dataSv = dataVec->valueAt(idx);
    auto pos = static_cast<size_t>(byteOff);
    if (pos + 5 <= static_cast<size_t>(dataSv.size())) {
      sum += static_cast<int32_t>(readLE32(dataSv.data() + pos + 1));
    }
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

size_t randomAccessFormatB() {
  int64_t sum = 0;
  auto* valueCol = gDataset->valueColB();
  const auto& indices = gDataset->randomIndices;

  for (auto idx : indices) {
    auto valSv = valueCol->valueAt(idx);
    int32_t days;
    if (extractDateField(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            field_id::kShipdate,
            days)) {
      sum += days;
    }
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

size_t randomAccessFormatC() {
  int64_t sum = 0;
  auto* shipdate = gDataset->shreddedShipdate->rawValues();
  const auto& indices = gDataset->randomIndices;
  for (auto idx : indices) {
    sum += shipdate[idx];
  }
  folly::doNotOptimizeAway(sum);
  return gDataset->numRows;
}

// ---------------------------------------------------------------------------
// ShredOnRead: batch-extract a field from Format B value blobs into a
// FlatVector, then scan the materialized column. Measures the end-to-end
// cost of "shred at read time" vs direct blob access.
// ---------------------------------------------------------------------------

/// Batch-extract l_shipdate from Format B into FlatVector<int32_t>, then
/// filter <= threshold.
size_t shredOnReadShipdateFilter() {
  auto* valueCol = gDataset->valueColB();
  auto numRows = gDataset->numRows;

  // Phase 1: extract l_shipdate into a flat column.
  auto shipdateCol =
      BaseVector::create<FlatVector<int32_t>>(INTEGER(), numRows, gPool.get());
  auto* shipdateRaw = shipdateCol->mutableRawValues();

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    int32_t days = 0;
    extractDateField(
        valSv.data(),
        static_cast<size_t>(valSv.size()),
        field_id::kShipdate,
        days);
    shipdateRaw[i] = days;
  }

  // Phase 2: scan the materialized column (same as Format C).
  size_t count = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (shipdateRaw[i] <= kShipdateThreshold) {
      ++count;
    }
  }
  folly::doNotOptimizeAway(count);
  return numRows;
}

/// Batch-extract l_extendedprice, l_discount, l_tax from Format B into 3
/// FlatVector<double>, then sum.
size_t shredOnReadMultiFieldSum() {
  auto* valueCol = gDataset->valueColB();
  auto numRows = gDataset->numRows;

  constexpr uint8_t kFields[] = {
      field_id::kExtendedprice,
      field_id::kDiscount,
      field_id::kTax,
  };

  // Phase 1: extract 3 doubles into flat columns.
  auto epCol =
      BaseVector::create<FlatVector<double>>(DOUBLE(), numRows, gPool.get());
  auto discCol =
      BaseVector::create<FlatVector<double>>(DOUBLE(), numRows, gPool.get());
  auto taxCol =
      BaseVector::create<FlatVector<double>>(DOUBLE(), numRows, gPool.get());
  auto* epRaw = epCol->mutableRawValues();
  auto* discRaw = discCol->mutableRawValues();
  auto* taxRaw = taxCol->mutableRawValues();

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    double vals[3] = {0, 0, 0};
    extractMultipleDoubleFields(
        valSv.data(),
        static_cast<size_t>(valSv.size()),
        kFields,
        3,
        vals);
    epRaw[i] = vals[0];
    discRaw[i] = vals[1];
    taxRaw[i] = vals[2];
  }

  // Phase 2: scan the materialized columns (same as Format C).
  double total = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    total += epRaw[i] + discRaw[i] + taxRaw[i];
  }
  folly::doNotOptimizeAway(total);
  return numRows;
}

// ---------------------------------------------------------------------------
// Benchmark registration.
// ---------------------------------------------------------------------------

BENCHMARK(FieldExtract_FormatA_4col) {
  fieldExtractFormatA();
}

BENCHMARK_RELATIVE(FieldExtract_FormatB_2col) {
  fieldExtractFormatB();
}

BENCHMARK_RELATIVE(FieldExtract_FormatC_shredded) {
  fieldExtractFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(MultiFieldExtract_FormatA_4col) {
  multiFieldExtractFormatA();
}

BENCHMARK_RELATIVE(MultiFieldExtract_FormatB_2col) {
  multiFieldExtractFormatB();
}

BENCHMARK_RELATIVE(MultiFieldExtract_FormatC_shredded) {
  multiFieldExtractFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(FieldExtractFilter_FormatA_4col) {
  fieldExtractFilterFormatA();
}

BENCHMARK_RELATIVE(FieldExtractFilter_FormatB_2col) {
  fieldExtractFilterFormatB();
}

BENCHMARK_RELATIVE(FieldExtractFilter_FormatC_shredded) {
  fieldExtractFilterFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ParquetDecode_ToFormatA_4col) {
  benchDecodeToFormatA();
}

BENCHMARK_RELATIVE(ParquetDecode_ToFormatB_2col) {
  benchDecodeToFormatB();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(RandomAccess_FormatA_4col) {
  randomAccessFormatA();
}

BENCHMARK_RELATIVE(RandomAccess_FormatB_2col) {
  randomAccessFormatB();
}

BENCHMARK_RELATIVE(RandomAccess_FormatC_shredded) {
  randomAccessFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ShredOnRead_ShipdateFilter_FormatB) {
  shredOnReadShipdateFilter();
}

BENCHMARK_RELATIVE(ShredOnRead_ShipdateFilter_FormatC) {
  fieldExtractFilterFormatC();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ShredOnRead_MultiFieldSum_FormatB) {
  shredOnReadMultiFieldSum();
}

BENCHMARK_RELATIVE(ShredOnRead_MultiFieldSum_FormatC) {
  multiFieldExtractFormatC();
}

// ===========================================================================
// AddressBook nested type benchmarks (Format A and Format B).
// ===========================================================================

// Format A data blob layout for OBJECT/ARRAY nodes:
//   child_count (4B LE) + children_idx (4B LE) = 8 bytes.
// For primitive nodes, the raw Parquet VARIANT value bytes are stored.
// type_id stores the Parquet VARIANT header byte for all node types.
constexpr size_t kNestedDataSize = 8; // child_count + children_idx

struct AddressBookDataset {
  vector_size_t numRows{0};

  /// Format A (DuckDB 4-col): RowVector with keys/children/values/data.
  RowVectorPtr formatA;

  /// Format B (StarRocks 2-col): ROW(metadata, value) as VARIANT_ROW_BASED.
  RowVectorPtr formatB;

  FlatVector<StringView>* valueColB() const {
    return formatB->childAt(1)->as<FlatVector<StringView>>();
  }
};

// Per-row builder state for recursive Format A construction.
struct FormatARowBuilder {
  std::vector<StringView> keys;
  std::vector<int32_t> childKeysIndex;
  std::vector<int32_t> childValuesIndex;
  std::vector<int8_t> typeIds;
  std::vector<int32_t> byteOffsets;
  std::string data;

  // Keys dictionary for this row (sorted, deduplicated).
  std::vector<std::string> keyDict;
  std::unordered_map<std::string, int32_t> keyToIndex;

  int32_t internKey(std::string_view key) {
    auto it = keyToIndex.find(std::string(key));
    if (it != keyToIndex.end()) {
      return it->second;
    }
    auto idx = static_cast<int32_t>(keyDict.size());
    keyDict.emplace_back(key);
    keyToIndex[std::string(key)] = idx;
    return idx;
  }

  /// Recursively decompose a Parquet VARIANT value blob into Format A arrays.
  /// metadata is needed to look up field names for Object fields.
  /// Returns the values index of the decomposed node.
  int32_t decomposeValue(
      const char* value,
      size_t valueLen,
      const char* metadata,
      size_t metadataLen) {
    if (valueLen < 1) {
      return -1;
    }

    uint8_t hdr = static_cast<uint8_t>(value[0]);
    uint8_t bt = hdr & 0x03;

    auto myValIdx = static_cast<int32_t>(typeIds.size());
    typeIds.push_back(static_cast<int8_t>(hdr));
    byteOffsets.push_back(0); // placeholder

    if (bt == basic_type::kObject) {
      uint32_t numFields;
      uint8_t fieldIdSize;
      uint8_t offsetSize;
      const char* fieldIdsArr;
      const char* offsetsArr;
      const char* childData = parseObjectHeader(
          value, valueLen, numFields, fieldIdSize, offsetSize,
          fieldIdsArr, offsetsArr);
      if (!childData) {
        return myValIdx;
      }

      // Write nested data: {child_count, children_idx}.
      auto childrenStart = static_cast<uint32_t>(childKeysIndex.size());
      auto dataOffset = static_cast<int32_t>(data.size());
      byteOffsets[myValIdx] = dataOffset;
      appendLE32(data, numFields);
      appendLE32(data, childrenStart);

      // Reserve children slots.
      for (uint32_t f = 0; f < numFields; ++f) {
        childKeysIndex.push_back(0); // placeholder
        childValuesIndex.push_back(0); // placeholder
      }

      // Decompose each child.
      // Parse metadata to look up field names.
      uint8_t metaHeader = static_cast<uint8_t>(metadata[0]);
      uint8_t metaOffsetSize = ((metaHeader >> 6) & 0x03) + 1;
      uint32_t dictSize = readLE32(metadata + 1);
      size_t dictOffsetsStart = 5;
      size_t dictDataStart = dictOffsetsStart + (dictSize + 1) * metaOffsetSize;

      for (uint32_t f = 0; f < numFields; ++f) {
        uint32_t fid = readFieldId(fieldIdsArr, fieldIdSize, f);

        // Skip fields whose id exceeds the metadata dictionary size.
        if (fid >= dictSize) {
          constexpr int32_t kInvalidIndex = -1;
          childKeysIndex[childrenStart + f] = kInvalidIndex;
          childValuesIndex[childrenStart + f] = kInvalidIndex;
          continue;
        }

        // Look up field name from metadata dictionary.
        uint32_t strStart = 0;
        uint32_t strEnd = 0;
        const char* metaOffBase = metadata + dictOffsetsStart;
        strStart = readOffset(metaOffBase, metaOffsetSize, fid);
        strEnd = readOffset(metaOffBase, metaOffsetSize, fid + 1);
        std::string_view fieldName(
            metadata + dictDataStart + strStart, strEnd - strStart);
        auto keyIdx = internKey(fieldName);

        uint32_t childStart = readOffset(offsetsArr, offsetSize, f);
        uint32_t childEnd = readOffset(offsetsArr, offsetSize, f + 1);
        const char* childValue = childData + childStart;
        size_t childLen = childEnd - childStart;

        // Determine which metadata to pass for nested Objects.
        // Nested Objects in AddressBook use their own metadata dictionaries
        // embedded in the blob, but in our Parquet VARIANT encoding the
        // metadata is separate. For nested objects, we need to figure out
        // their metadata. Since we share one metadata dict per row in
        // Format B, we'll reuse the same metadata for all levels.
        // This works because all field names across all levels are in
        // the same global dictionary.
        auto childValIdx =
            decomposeValue(childValue, childLen, metadata, metadataLen);

        childKeysIndex[childrenStart + f] = keyIdx;
        childValuesIndex[childrenStart + f] = childValIdx;
      }

      return myValIdx;
    }

    if (bt == basic_type::kArray) {
      uint32_t numElements;
      uint8_t offsetSize;
      const char* offsetsArr;
      const char* elemData = parseArrayHeader(
          value, valueLen, numElements, offsetSize, offsetsArr);
      if (!elemData) {
        return myValIdx;
      }

      auto childrenStart = static_cast<uint32_t>(childKeysIndex.size());
      auto dataOffset = static_cast<int32_t>(data.size());
      byteOffsets[myValIdx] = dataOffset;
      appendLE32(data, numElements);
      appendLE32(data, childrenStart);

      // Reserve children slots (Array elements have no key).
      for (uint32_t e = 0; e < numElements; ++e) {
        childKeysIndex.push_back(-1); // invalid key for array elements
        childValuesIndex.push_back(0); // placeholder
      }

      for (uint32_t e = 0; e < numElements; ++e) {
        uint32_t elemStart = readOffset(offsetsArr, offsetSize, e);
        uint32_t elemEnd = readOffset(offsetsArr, offsetSize, e + 1);
        const char* elemValue = elemData + elemStart;
        size_t elemLen = elemEnd - elemStart;

        auto elemValIdx =
            decomposeValue(elemValue, elemLen, metadata, metadataLen);
        childValuesIndex[childrenStart + e] = elemValIdx;
      }

      return myValIdx;
    }

    // Primitive: store the raw bytes in data blob.
    auto dataOffset = static_cast<int32_t>(data.size());
    byteOffsets[myValIdx] = dataOffset;
    data.append(value, valueLen);
    return myValIdx;
  }
};

// Build the AddressBook dataset with both Format A and Format B.
// Since nested Objects in separate metadata dictionaries would be complex
// for Format A (DuckDB uses a single per-row dictionary), we build a
// combined metadata dictionary with all field names across all nesting levels.
AddressBookDataset makeAddressBookDataset(
    vector_size_t numRows,
    memory::MemoryPool* pool) {
  using namespace addressbook;

  AddressBookDataset ds;
  ds.numRows = numRows;

  std::mt19937 rng(42);
  std::uniform_int_distribution<int32_t> numContactsDist(1, 5);
  std::uniform_int_distribution<int32_t> numPhonesDist(1, 3);
  std::uniform_int_distribution<int32_t> ageDist(18, 80);

  auto makeName = [&](int idx) {
    return "Person_" + std::to_string(idx);
  };
  auto makePhoneNum = [&](int idx) {
    return "+1-555-" + std::to_string(1'000 + idx % 9'000);
  };
  static const char* kPhoneTypes[] = {"home", "work", "mobile"};

  // Build a combined metadata dictionary with all field names from all levels
  // sorted alphabetically: age, contacts, name, number, owner, phones, type.
  auto combinedMetadata = buildMetadata(
      {"age", "contacts", "name", "number", "owner", "phones", "type"});
  size_t combinedMetaLen = combinedMetadata.size();

  // We also need the per-level metadata for Format B encoding.
  auto abMetadata = buildAddressBookMetadata();

  // First pass: build value blobs for both formats.
  // For Format B: use per-level metadata (AddressBook/Contact/Phone each have
  // their own metadata). The value blob is self-contained.
  // For Format A: we'll parse the value blob using the combined metadata.
  // But wait -- the Parquet VARIANT blob's Object values use field_ids that
  // reference their own level's metadata. So for Format A decomposition we
  // need to decode using the same metadata that was used for encoding.
  //
  // Solution: Build value blobs encoded with the combined metadata so Format A
  // can use a single dictionary. This means re-encoding with combined field_ids.

  // Combined field_ids (sorted alphabetically):
  //   age=0, contacts=1, name=2, number=3, owner=4, phones=5, type=6
  constexpr uint8_t kCombAge = 0;
  constexpr uint8_t kCombContacts = 1;
  constexpr uint8_t kCombName = 2;
  constexpr uint8_t kCombNumber = 3;
  constexpr uint8_t kCombOwner = 4;
  constexpr uint8_t kCombPhones = 5;
  constexpr uint8_t kCombType = 6;

  // Encode functions using combined field_ids.
  auto encodeCombinedPhone = [](std::string_view number, std::string_view type) {
    return encodeObjectWithFieldIds(
        {kCombNumber, kCombType},
        {encodeStringValue(number), encodeStringValue(type)});
  };

  auto encodeCombinedContact = [&](std::string_view name, int32_t age,
                                   const std::vector<std::string>& phones) {
    return encodeObjectWithFieldIds(
        {kCombAge, kCombName, kCombPhones},
        {encodeInt32Value(age), encodeStringValue(name), encodeArray(phones)});
  };

  auto encodeCombinedAddressBook =
      [&](std::string_view owner, const std::vector<std::string>& contacts) {
        return encodeObjectWithFieldIds(
            {kCombContacts, kCombOwner},
            {encodeArray(contacts), encodeStringValue(owner)});
      };

  // Build all value blobs (encoded with combined metadata field_ids).
  std::vector<std::string> combinedValueBlobs(numRows);

  // Also build Format B blobs (encoded with per-level metadata field_ids).
  std::vector<std::string> formatBValueBlobs(numRows);

  int nameCounter = 0;
  int phoneCounter = 0;

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto numContacts = numContactsDist(rng);
    std::vector<std::string> combContacts;
    std::vector<std::string> origContacts;
    combContacts.reserve(numContacts);
    origContacts.reserve(numContacts);

    for (int c = 0; c < numContacts; ++c) {
      auto numPhones = numPhonesDist(rng);
      std::vector<std::string> combPhones;
      std::vector<std::string> origPhones;
      combPhones.reserve(numPhones);
      origPhones.reserve(numPhones);

      for (int p = 0; p < numPhones; ++p) {
        auto phoneNum = makePhoneNum(phoneCounter++);
        auto phoneType = kPhoneTypes[p % 3];
        combPhones.push_back(encodeCombinedPhone(phoneNum, phoneType));
        origPhones.push_back(encodePhone(phoneNum, phoneType));
      }
      auto contactName = makeName(nameCounter++);
      auto contactAge = ageDist(rng);
      combContacts.push_back(
          encodeCombinedContact(contactName, contactAge, combPhones));
      origContacts.push_back(
          encodeContact(contactName, contactAge, origPhones));
    }
    auto ownerName = "Owner_" + std::to_string(i);
    combinedValueBlobs[i] =
        encodeCombinedAddressBook(ownerName, combContacts);
    formatBValueBlobs[i] = encodeAddressBook(ownerName, origContacts);
  }

  // --- Build Format B ---
  size_t totalBMetaBytes = 0;
  size_t totalBValueBytes = 0;
  auto abMeta = buildAddressBookMetadata();
  size_t abMetaLen = abMeta.size();
  for (vector_size_t i = 0; i < numRows; ++i) {
    totalBMetaBytes += abMetaLen;
    totalBValueBytes += formatBValueBlobs[i].size();
  }

  auto bMetadataOut = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);
  auto bValueOut = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);
  auto bMetaBuf = AlignedBuffer::allocate<char>(totalBMetaBytes, pool);
  auto bValBuf = AlignedBuffer::allocate<char>(totalBValueBytes, pool);
  auto* bMetaDest = bMetaBuf->asMutable<char>();
  auto* bValDest = bValBuf->asMutable<char>();

  size_t bMetaOff = 0;
  size_t bValOff = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    std::memcpy(bMetaDest + bMetaOff, abMeta.data(), abMetaLen);
    bMetadataOut->set(
        i, StringView(bMetaDest + bMetaOff, static_cast<int32_t>(abMetaLen)));
    bMetaOff += abMetaLen;

    auto& vb = formatBValueBlobs[i];
    std::memcpy(bValDest + bValOff, vb.data(), vb.size());
    bValueOut->set(
        i, StringView(bValDest + bValOff, static_cast<int32_t>(vb.size())));
    bValOff += vb.size();
  }
  bMetadataOut->addStringBuffer(bMetaBuf);
  bValueOut->addStringBuffer(bValBuf);

  ds.formatB = std::make_shared<VariantVector>(
      pool, VARIANT_ROW_BASED(), nullptr, numRows,
      std::vector<VectorPtr>{bMetadataOut, bValueOut});

  // --- Build Format A via recursive decomposition ---
  // First pass: decompose all rows to gather total sizes.
  std::vector<FormatARowBuilder> builders(numRows);
  size_t totalKeys = 0;
  size_t totalChildren = 0;
  size_t totalValues = 0;
  size_t totalDataBytes = 0;

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto& b = builders[i];
    b.decomposeValue(
        combinedValueBlobs[i].data(),
        combinedValueBlobs[i].size(),
        combinedMetadata.data(),
        combinedMetaLen);
    totalKeys += b.keyDict.size();
    totalChildren += b.childKeysIndex.size();
    totalValues += b.typeIds.size();
    totalDataBytes += b.data.size();
  }

  // Allocate Format A vectors.
  auto keysChild = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), totalKeys, pool);
  auto keysOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto keysSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* keysOffsetsPtr = keysOffsets->asMutable<vector_size_t>();
  auto* keysSizesPtr = keysSizes->asMutable<vector_size_t>();

  auto childrenKeysIndex = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalChildren, pool);
  auto childrenValuesIndex = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalChildren, pool);
  auto childrenStruct = std::make_shared<RowVector>(
      pool,
      ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()}),
      nullptr, totalChildren,
      std::vector<VectorPtr>{childrenKeysIndex, childrenValuesIndex});
  auto childrenOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto childrenSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* childrenOffsetsPtr = childrenOffsets->asMutable<vector_size_t>();
  auto* childrenSizesPtr = childrenSizes->asMutable<vector_size_t>();

  auto valuesTypeId = BaseVector::create<FlatVector<int8_t>>(
      TINYINT(), totalValues, pool);
  auto valuesByteOffset = BaseVector::create<FlatVector<int32_t>>(
      INTEGER(), totalValues, pool);
  auto valuesStruct = std::make_shared<RowVector>(
      pool,
      ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()}),
      nullptr, totalValues,
      std::vector<VectorPtr>{valuesTypeId, valuesByteOffset});
  auto valuesOffsets = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto valuesSizes = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
  auto* valuesOffsetsPtr = valuesOffsets->asMutable<vector_size_t>();
  auto* valuesSizesPtr = valuesSizes->asMutable<vector_size_t>();

  auto dataVec = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), numRows, pool);
  auto dataBuf = AlignedBuffer::allocate<char>(totalDataBytes, pool);
  auto* dataDestPtr = dataBuf->asMutable<char>();

  // Populate from builders.
  vector_size_t keyIdx = 0;
  vector_size_t childIdx = 0;
  vector_size_t valIdx = 0;
  size_t dataOffset = 0;

  // We need to store key strings somewhere stable. Collect all unique strings.
  // Build a contiguous buffer for key strings.
  size_t totalKeyStringBytes = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    for (const auto& k : builders[i].keyDict) {
      totalKeyStringBytes += k.size();
    }
  }
  auto keyStringBuf =
      AlignedBuffer::allocate<char>(totalKeyStringBytes, pool);
  auto* keyStringDest = keyStringBuf->asMutable<char>();
  size_t keyStringOff = 0;

  for (vector_size_t i = 0; i < numRows; ++i) {
    auto& b = builders[i];

    // Keys.
    keysOffsetsPtr[i] = keyIdx;
    keysSizesPtr[i] = static_cast<vector_size_t>(b.keyDict.size());
    for (const auto& k : b.keyDict) {
      std::memcpy(keyStringDest + keyStringOff, k.data(), k.size());
      keysChild->set(
          keyIdx,
          StringView(
              keyStringDest + keyStringOff, static_cast<int32_t>(k.size())));
      keyStringOff += k.size();
      ++keyIdx;
    }

    // Children.
    childrenOffsetsPtr[i] = childIdx;
    childrenSizesPtr[i] = static_cast<vector_size_t>(b.childKeysIndex.size());
    for (size_t c = 0; c < b.childKeysIndex.size(); ++c) {
      childrenKeysIndex->set(childIdx, b.childKeysIndex[c]);
      childrenValuesIndex->set(childIdx, b.childValuesIndex[c]);
      ++childIdx;
    }

    // Values.
    valuesOffsetsPtr[i] = valIdx;
    valuesSizesPtr[i] = static_cast<vector_size_t>(b.typeIds.size());
    for (size_t v = 0; v < b.typeIds.size(); ++v) {
      valuesTypeId->set(valIdx, b.typeIds[v]);
      valuesByteOffset->set(valIdx, b.byteOffsets[v]);
      ++valIdx;
    }

    // Data.
    std::memcpy(dataDestPtr + dataOffset, b.data.data(), b.data.size());
    dataVec->set(
        i,
        StringView(
            dataDestPtr + dataOffset, static_cast<int32_t>(b.data.size())));
    dataOffset += b.data.size();
  }

  keysChild->addStringBuffer(keyStringBuf);
  dataVec->addStringBuffer(dataBuf);

  auto keysArray = std::make_shared<ArrayVector>(
      pool, ARRAY(VARCHAR()), nullptr, numRows,
      keysOffsets, keysSizes, keysChild);
  auto childrenArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
      nullptr, numRows, childrenOffsets, childrenSizes, childrenStruct);
  auto valuesArray = std::make_shared<ArrayVector>(
      pool,
      ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
      nullptr, numRows, valuesOffsets, valuesSizes, valuesStruct);

  ds.formatA = std::make_shared<VariantVector>(
      pool, VARIANT_COLUMNAR(), nullptr, numRows,
      std::vector<VectorPtr>{keysArray, childrenArray, valuesArray, dataVec});

  return ds;
}

std::unique_ptr<AddressBookDataset> gAddressBook;

// ---------------------------------------------------------------------------
// AddressBook Format A accessor helpers.
// ---------------------------------------------------------------------------

const ArrayVector* abFormatAKeys() {
  return gAddressBook->formatA->childAt(0)->as<ArrayVector>();
}

const ArrayVector* abFormatAChildren() {
  return gAddressBook->formatA->childAt(1)->as<ArrayVector>();
}

const ArrayVector* abFormatAValues() {
  return gAddressBook->formatA->childAt(2)->as<ArrayVector>();
}

const FlatVector<int8_t>* abFormatATypeIds() {
  return abFormatAValues()
      ->elements()
      ->as<RowVector>()
      ->childAt(0)
      ->as<FlatVector<int8_t>>();
}

const FlatVector<int32_t>* abFormatAByteOffsets() {
  return abFormatAValues()
      ->elements()
      ->as<RowVector>()
      ->childAt(1)
      ->as<FlatVector<int32_t>>();
}

const FlatVector<int32_t>* abFormatAChildKeysIndex() {
  return abFormatAChildren()
      ->elements()
      ->as<RowVector>()
      ->childAt(0)
      ->as<FlatVector<int32_t>>();
}

const FlatVector<int32_t>* abFormatAChildValuesIndex() {
  return abFormatAChildren()
      ->elements()
      ->as<RowVector>()
      ->childAt(1)
      ->as<FlatVector<int32_t>>();
}

const FlatVector<StringView>* abFormatAKeys_flat() {
  return abFormatAKeys()->elements()->as<FlatVector<StringView>>();
}

const FlatVector<StringView>* abFormatAData() {
  return gAddressBook->formatA->childAt(3)->as<FlatVector<StringView>>();
}

/// Navigate a Format A decomposed tree: given a value node that is an OBJECT,
/// find the child with the given key name. Returns the child's values_index,
/// or -1 if not found.
int32_t formatAFindChild(
    vector_size_t row,
    int32_t valuesIndex,
    std::string_view targetKey,
    const ArrayVector* valuesArr,
    const FlatVector<int32_t>* byteOffsetVec,
    const FlatVector<StringView>* dataVec,
    const ArrayVector* keysArr,
    const FlatVector<StringView>* keysFlat,
    const ArrayVector* childrenArr,
    const FlatVector<int32_t>* childKeysIdx,
    const FlatVector<int32_t>* childValsIdx) {
  auto globalValIdx =
      static_cast<vector_size_t>(valuesArr->offsetAt(row)) + valuesIndex;
  auto byteOff = byteOffsetVec->valueAt(globalValIdx);
  auto dataSv = dataVec->valueAt(row);

  // Read child_count and children_idx from data blob.
  auto pos = static_cast<size_t>(byteOff);
  if (pos + 8 > static_cast<size_t>(dataSv.size())) {
    return -1;
  }
  uint32_t childCount = readLE32(dataSv.data() + pos);
  uint32_t childrenIdx = readLE32(dataSv.data() + pos + 4);

  auto keysOffset = keysArr->offsetAt(row);

  // Scan children to find the target key.
  auto childrenArrOffset = childrenArr->offsetAt(row);
  for (uint32_t c = 0; c < childCount; ++c) {
    auto globalChildIdx =
        static_cast<vector_size_t>(childrenArrOffset + childrenIdx + c);
    auto keyIdx = childKeysIdx->valueAt(globalChildIdx);
    if (keyIdx >= 0) {
      auto keySv = keysFlat->valueAt(keysOffset + keyIdx);
      if (std::string_view(keySv.data(), keySv.size()) == targetKey) {
        return childValsIdx->valueAt(globalChildIdx);
      }
    }
  }
  return -1;
}

/// Navigate a Format A decomposed tree: given a value node that is an ARRAY,
/// return the values_index of the Nth element. Returns -1 if out of bounds.
int32_t formatAArrayElement(
    vector_size_t row,
    int32_t valuesIndex,
    uint32_t elementIdx,
    const ArrayVector* valuesArr,
    const FlatVector<int32_t>* byteOffsetVec,
    const FlatVector<StringView>* dataVec,
    const ArrayVector* childrenArr,
    const FlatVector<int32_t>* childValsIdx) {
  auto globalValIdx =
      static_cast<vector_size_t>(valuesArr->offsetAt(row)) + valuesIndex;
  auto byteOff = byteOffsetVec->valueAt(globalValIdx);
  auto dataSv = dataVec->valueAt(row);

  auto pos = static_cast<size_t>(byteOff);
  if (pos + 8 > static_cast<size_t>(dataSv.size())) {
    return -1;
  }
  uint32_t childCount = readLE32(dataSv.data() + pos);
  uint32_t childrenIdx = readLE32(dataSv.data() + pos + 4);

  if (elementIdx >= childCount) {
    return -1;
  }

  auto childrenArrOffset = childrenArr->offsetAt(row);
  auto globalChildIdx = static_cast<vector_size_t>(
      childrenArrOffset + childrenIdx + elementIdx);
  return childValsIdx->valueAt(globalChildIdx);
}

/// Read a string value from a Format A primitive node.
std::string_view formatAReadString(
    vector_size_t row,
    int32_t valuesIndex,
    const ArrayVector* valuesArr,
    const FlatVector<int32_t>* byteOffsetVec,
    const FlatVector<StringView>* dataVec) {
  auto globalValIdx =
      static_cast<vector_size_t>(valuesArr->offsetAt(row)) + valuesIndex;
  auto byteOff = byteOffsetVec->valueAt(globalValIdx);
  auto dataSv = dataVec->valueAt(row);
  auto pos = static_cast<size_t>(byteOff);

  if (pos >= static_cast<size_t>(dataSv.size())) {
    return {};
  }

  const char* p = dataSv.data() + pos;
  size_t remaining = static_cast<size_t>(dataSv.size()) - pos;

  uint8_t hdr = static_cast<uint8_t>(p[0]);
  uint8_t bt = hdr & 0x03;

  if (bt == basic_type::kShortString) {
    uint8_t strLen = hdr >> 2;
    if (1 + strLen > remaining) {
      return {};
    }
    return {p + 1, strLen};
  }

  if (bt == basic_type::kPrimitive && (hdr >> 2) == primitive_type::kString) {
    if (remaining < 5) {
      return {};
    }
    uint32_t strLen = readLE32(p + 1);
    if (5 + strLen > remaining) {
      return {};
    }
    return {p + 5, strLen};
  }

  return {};
}

// ---------------------------------------------------------------------------
// NestedExtract_Owner: extract top-level 'owner' (VARCHAR) from AddressBook.
// ---------------------------------------------------------------------------

size_t nestedExtractOwnerFormatA() {
  int64_t totalLen = 0;
  auto* valuesArr = abFormatAValues();
  auto* byteOffsetVec = abFormatAByteOffsets();
  auto* dataVec = abFormatAData();
  auto* keysArr = abFormatAKeys();
  auto* keysFlat = abFormatAKeys_flat();
  auto* childrenArr = abFormatAChildren();
  auto* childKeysIdx = abFormatAChildKeysIndex();
  auto* childValsIdx = abFormatAChildValuesIndex();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto ownerValIdx = formatAFindChild(
        i, 0, "owner", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (ownerValIdx >= 0) {
      auto sv = formatAReadString(
          i, ownerValIdx, valuesArr, byteOffsetVec, dataVec);
      totalLen += sv.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

size_t nestedExtractOwnerFormatB() {
  int64_t totalLen = 0;
  auto* valueCol = gAddressBook->valueColB();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    std::string_view owner;
    if (extractStringField(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            addressbook::field_id::kOwner,
            owner)) {
      totalLen += owner.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

// ---------------------------------------------------------------------------
// NestedExtract_Contact0Name: root -> contacts[0] -> name (2-level nesting).
// ---------------------------------------------------------------------------

size_t nestedExtractContact0NameFormatA() {
  int64_t totalLen = 0;
  auto* valuesArr = abFormatAValues();
  auto* byteOffsetVec = abFormatAByteOffsets();
  auto* dataVec = abFormatAData();
  auto* keysArr = abFormatAKeys();
  auto* keysFlat = abFormatAKeys_flat();
  auto* childrenArr = abFormatAChildren();
  auto* childKeysIdx = abFormatAChildKeysIndex();
  auto* childValsIdx = abFormatAChildValuesIndex();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    // Step 1: find 'contacts' in root Object.
    auto contactsValIdx = formatAFindChild(
        i, 0, "contacts", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (contactsValIdx < 0) {
      continue;
    }

    // Step 2: get element[0] from contacts Array.
    auto contact0ValIdx = formatAArrayElement(
        i, contactsValIdx, 0, valuesArr, byteOffsetVec, dataVec,
        childrenArr, childValsIdx);
    if (contact0ValIdx < 0) {
      continue;
    }

    // Step 3: find 'name' in contacts[0] Object.
    auto nameValIdx = formatAFindChild(
        i, contact0ValIdx, "name", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (nameValIdx >= 0) {
      auto sv = formatAReadString(
          i, nameValIdx, valuesArr, byteOffsetVec, dataVec);
      totalLen += sv.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

size_t nestedExtractContact0NameFormatB() {
  int64_t totalLen = 0;
  auto* valueCol = gAddressBook->valueColB();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    const char* value = valSv.data();
    size_t valueLen = static_cast<size_t>(valSv.size());

    const char* contactsArr;
    size_t contactsLen;
    if (!extractObjectField(
            value, valueLen, addressbook::field_id::kContacts,
            contactsArr, contactsLen)) {
      continue;
    }

    const char* contact0;
    size_t contact0Len;
    if (!extractArrayElement(
            contactsArr, contactsLen, 0, contact0, contact0Len)) {
      continue;
    }

    std::string_view name;
    if (extractStringField(
            contact0, contact0Len, addressbook::field_id::kName, name)) {
      totalLen += name.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

// ---------------------------------------------------------------------------
// NestedExtract_ContactsArray: extract 'contacts' field sub-document size.
// For Format A, sum the data sizes of all children under the contacts node.
// For Format B, extract the byte range from the blob.
// ---------------------------------------------------------------------------

size_t nestedExtractContactsArrayFormatA() {
  int64_t totalBytes = 0;
  auto* valuesArr = abFormatAValues();
  auto* byteOffsetVec = abFormatAByteOffsets();
  auto* dataVec = abFormatAData();
  auto* keysArr = abFormatAKeys();
  auto* keysFlat = abFormatAKeys_flat();
  auto* childrenArr = abFormatAChildren();
  auto* childKeysIdx = abFormatAChildKeysIndex();
  auto* childValsIdx = abFormatAChildValuesIndex();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto contactsValIdx = formatAFindChild(
        i, 0, "contacts", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (contactsValIdx >= 0) {
      // Just read the nested data header to get child_count as a proxy
      // for "sub-document size" — comparable to Format B's byte range.
      auto globalValIdx = static_cast<vector_size_t>(
          valuesArr->offsetAt(i) + contactsValIdx);
      auto byteOff = byteOffsetVec->valueAt(globalValIdx);
      auto dataSv = dataVec->valueAt(i);
      auto pos = static_cast<size_t>(byteOff);
      if (pos + 8 <= static_cast<size_t>(dataSv.size())) {
        uint32_t childCount = readLE32(dataSv.data() + pos);
        totalBytes += childCount;
      }
    }
  }
  folly::doNotOptimizeAway(totalBytes);
  return gAddressBook->numRows;
}

size_t nestedExtractContactsArrayFormatB() {
  int64_t totalBytes = 0;
  auto* valueCol = gAddressBook->valueColB();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    const char* contactsArr;
    size_t contactsLen;
    if (extractObjectField(
            valSv.data(),
            static_cast<size_t>(valSv.size()),
            addressbook::field_id::kContacts,
            contactsArr,
            contactsLen)) {
      totalBytes += contactsLen;
    }
  }
  folly::doNotOptimizeAway(totalBytes);
  return gAddressBook->numRows;
}

// ---------------------------------------------------------------------------
// NestedExtract_Deep3: root -> contacts[0] -> phones[0] -> number (3-level).
// ---------------------------------------------------------------------------

size_t nestedExtractDeep3FormatA() {
  int64_t totalLen = 0;
  auto* valuesArr = abFormatAValues();
  auto* byteOffsetVec = abFormatAByteOffsets();
  auto* dataVec = abFormatAData();
  auto* keysArr = abFormatAKeys();
  auto* keysFlat = abFormatAKeys_flat();
  auto* childrenArr = abFormatAChildren();
  auto* childKeysIdx = abFormatAChildKeysIndex();
  auto* childValsIdx = abFormatAChildValuesIndex();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    // root -> contacts
    auto contactsValIdx = formatAFindChild(
        i, 0, "contacts", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (contactsValIdx < 0) {
      continue;
    }

    // contacts[0]
    auto contact0ValIdx = formatAArrayElement(
        i, contactsValIdx, 0, valuesArr, byteOffsetVec, dataVec,
        childrenArr, childValsIdx);
    if (contact0ValIdx < 0) {
      continue;
    }

    // contacts[0].phones
    auto phonesValIdx = formatAFindChild(
        i, contact0ValIdx, "phones", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (phonesValIdx < 0) {
      continue;
    }

    // phones[0]
    auto phone0ValIdx = formatAArrayElement(
        i, phonesValIdx, 0, valuesArr, byteOffsetVec, dataVec,
        childrenArr, childValsIdx);
    if (phone0ValIdx < 0) {
      continue;
    }

    // phones[0].number
    auto numberValIdx = formatAFindChild(
        i, phone0ValIdx, "number", valuesArr, byteOffsetVec, dataVec,
        keysArr, keysFlat, childrenArr, childKeysIdx, childValsIdx);
    if (numberValIdx >= 0) {
      auto sv = formatAReadString(
          i, numberValIdx, valuesArr, byteOffsetVec, dataVec);
      totalLen += sv.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

size_t nestedExtractDeep3FormatB() {
  int64_t totalLen = 0;
  auto* valueCol = gAddressBook->valueColB();

  for (vector_size_t i = 0; i < gAddressBook->numRows; ++i) {
    auto valSv = valueCol->valueAt(i);
    const char* value = valSv.data();
    size_t valueLen = static_cast<size_t>(valSv.size());

    const char* contactsArr;
    size_t contactsLen;
    if (!extractObjectField(
            value, valueLen, addressbook::field_id::kContacts,
            contactsArr, contactsLen)) {
      continue;
    }

    const char* contact0;
    size_t contact0Len;
    if (!extractArrayElement(
            contactsArr, contactsLen, 0, contact0, contact0Len)) {
      continue;
    }

    const char* phonesArr;
    size_t phonesLen;
    if (!extractObjectField(
            contact0, contact0Len, addressbook::field_id::kPhones,
            phonesArr, phonesLen)) {
      continue;
    }

    const char* phone0;
    size_t phone0Len;
    if (!extractArrayElement(phonesArr, phonesLen, 0, phone0, phone0Len)) {
      continue;
    }

    std::string_view number;
    if (extractStringField(
            phone0, phone0Len, addressbook::field_id::kNumber, number)) {
      totalLen += number.size();
    }
  }
  folly::doNotOptimizeAway(totalLen);
  return gAddressBook->numRows;
}

// ---------------------------------------------------------------------------
// AddressBook benchmark registration.
// ---------------------------------------------------------------------------

BENCHMARK_DRAW_LINE();

BENCHMARK(NestedExtract_Owner_FormatA) {
  nestedExtractOwnerFormatA();
}

BENCHMARK_RELATIVE(NestedExtract_Owner_FormatB) {
  nestedExtractOwnerFormatB();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(NestedExtract_Contact0Name_FormatA) {
  nestedExtractContact0NameFormatA();
}

BENCHMARK_RELATIVE(NestedExtract_Contact0Name_FormatB) {
  nestedExtractContact0NameFormatB();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(NestedExtract_ContactsArray_FormatA) {
  nestedExtractContactsArrayFormatA();
}

BENCHMARK_RELATIVE(NestedExtract_ContactsArray_FormatB) {
  nestedExtractContactsArrayFormatB();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(NestedExtract_Deep3_FormatA) {
  nestedExtractDeep3FormatA();
}

BENCHMARK_RELATIVE(NestedExtract_Deep3_FormatB) {
  nestedExtractDeep3FormatB();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  gPool = memory::deprecatedAddDefaultLeafMemoryPool("variant_benchmark");
  gDataset = std::make_unique<Dataset>(makeDataset(FLAGS_num_rows, gPool.get()));

  LOG(INFO) << "Dataset: " << gDataset->numRows << " rows";
  LOG(INFO) << "Format A: "
            << gDataset->formatA->estimateFlatSize() << " bytes";
  LOG(INFO) << "Format B: "
            << gDataset->formatB->estimateFlatSize() << " bytes";
  LOG(INFO) << "Format C: shipdate="
            << gDataset->shreddedShipdate->estimateFlatSize()
            << " bytes, extendedprice="
            << gDataset->shreddedExtendedprice->estimateFlatSize()
            << " bytes, discount="
            << gDataset->shreddedDiscount->estimateFlatSize()
            << " bytes, tax="
            << gDataset->shreddedTax->estimateFlatSize() << " bytes";
  LOG(INFO) << "Filter threshold: l_shipdate <= " << kShipdateThreshold
            << " (1998-09-02)";

  gAddressBook = std::make_unique<AddressBookDataset>(
      makeAddressBookDataset(FLAGS_num_rows, gPool.get()));
  LOG(INFO) << "AddressBook: " << gAddressBook->numRows << " rows";
  LOG(INFO) << "  Format A: "
            << gAddressBook->formatA->estimateFlatSize() << " bytes";
  LOG(INFO) << "  Format B: "
            << gAddressBook->formatB->estimateFlatSize() << " bytes";

  folly::runBenchmarks();
  gAddressBook.reset();
  gDataset.reset();
  gPool.reset();
  return 0;
}
