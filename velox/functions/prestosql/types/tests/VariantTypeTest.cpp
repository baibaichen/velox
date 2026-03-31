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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/types/VariantEncoding.h"
#include "velox/functions/prestosql/types/VariantRegistration.h"
#include "velox/functions/prestosql/types/VariantType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {

using namespace variant_encoding;

class VariantTypeTest : public testing::Test, public TypeTestBase {
 public:
  VariantTypeTest() {
    memory::MemoryManager::testingSetInstance(
        memory::MemoryManager::Options{});
    registerVariantTypes();
    pool_ = memory::memoryManager()->addLeafPool();
  }

 protected:
  std::shared_ptr<memory::MemoryPool> pool_;

  /// Build a 3-field metadata dictionary: l_discount, l_linestatus, l_shipdate.
  std::string buildTestMetadata() {
    std::vector<std::string_view> fieldNames = {
        "l_discount", "l_linestatus", "l_shipdate"};
    return buildMetadata(fieldNames);
  }

  /// Encode a 3-field Object value blob with discount(DOUBLE),
  /// linestatus(VARCHAR), shipdate(DATE).
  std::string encodeTestObject(
      double discount,
      std::string_view linestatus,
      int32_t shipdate) {
    std::string childValues[3];
    childValues[0] = encodeDoubleValue(discount);
    childValues[1] = encodeStringValue(linestatus);
    childValues[2] = encodeDateValue(shipdate);

    size_t totalChild = 0;
    for (auto& cv : childValues) {
      totalChild += cv.size();
    }

    uint8_t objectHeader =
        (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;

    std::string val;
    val.push_back(static_cast<char>(objectHeader));
    val.push_back(static_cast<char>(3));

    for (uint8_t fid = 0; fid < 3; ++fid) {
      val.push_back(static_cast<char>(fid));
    }

    uint8_t offset = 0;
    for (int f = 0; f < 3; ++f) {
      val.push_back(static_cast<char>(offset));
      offset += static_cast<uint8_t>(childValues[f].size());
    }
    val.push_back(static_cast<char>(offset));

    for (auto& cv : childValues) {
      val.append(cv);
    }

    return val;
  }

  /// Build a simple 3-row row-based VARIANT vector with known values.
  /// Each row is an Object with fields: l_discount(DOUBLE), l_shipdate(DATE),
  /// l_linestatus(VARCHAR).
  RowVectorPtr buildRowBasedVariant() {
    std::string metadata = buildTestMetadata();
    // field_ids: l_discount=0, l_linestatus=1, l_shipdate=2

    // Row 0: discount=0.04, shipdate=9204 (1995-03-15), linestatus="O"
    // Row 1: discount=0.09, shipdate=9374 (1995-09-01), linestatus="F"
    // Row 2: discount=0.01, shipdate=8766 (1994-01-01), linestatus="O"
    struct RowData {
      double discount;
      int32_t shipdate;
      std::string_view linestatus;
    };
    RowData rows[] = {
        {0.04, 9'204, "O"},
        {0.09, 9'374, "F"},
        {0.01, 8'766, "O"},
    };

    auto numRows = 3;
    auto metadataVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
    auto valueVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
    auto* metadataFlat = metadataVec->as<FlatVector<StringView>>();
    auto* valueFlat = valueVec->as<FlatVector<StringView>>();

    for (int i = 0; i < numRows; ++i) {
      metadataFlat->set(
          i, StringView(metadata.data(), metadata.size()));

      std::string val =
          encodeTestObject(rows[i].discount, rows[i].linestatus, rows[i].shipdate);
      valueFlat->set(i, StringView(val.data(), val.size()));
    }

    return std::make_shared<VariantVector>(
        pool_.get(),
        VARIANT_ROW_BASED(),
        nullptr,
        numRows,
        std::vector<VectorPtr>{metadataVec, valueVec});
  }

  /// Build a simple 3-row columnar VARIANT vector with known values.
  /// Same data as buildRowBasedVariant.
  RowVectorPtr buildColumnarVariant() {
    struct RowData {
      double discount;
      int32_t shipdate;
      std::string_view linestatus;
    };
    RowData rows[] = {
        {0.04, 9'204, "O"},
        {0.09, 9'374, "F"},
        {0.01, 8'766, "O"},
    };

    int32_t numRows = 3;
    int32_t numFields = 3; // discount, linestatus, shipdate

    // Build keys array: each row has 3 keys.
    auto keysElementsVec =
        BaseVector::create(VARCHAR(), numRows * numFields, pool_.get());
    auto* keysElements = keysElementsVec->as<FlatVector<StringView>>();
    std::string_view keyNames[] = {"l_discount", "l_linestatus", "l_shipdate"};
    for (int r = 0; r < numRows; ++r) {
      for (int f = 0; f < numFields; ++f) {
        keysElements->set(
            r * numFields + f,
            StringView(keyNames[f].data(), keyNames[f].size()));
      }
    }
    auto keysOffsets = allocateOffsets(numRows, pool_.get());
    auto keysSizes = allocateSizes(numRows, pool_.get());
    auto* keysOffsetsPtr = keysOffsets->asMutable<vector_size_t>();
    auto* keysSizesPtr = keysSizes->asMutable<vector_size_t>();
    for (int r = 0; r < numRows; ++r) {
      keysOffsetsPtr[r] = r * numFields;
      keysSizesPtr[r] = numFields;
    }
    auto keysArray = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(VARCHAR()),
        nullptr,
        numRows,
        keysOffsets,
        keysSizes,
        keysElementsVec);

    // Build children array.
    int32_t totalChildren = numRows * numFields;
    auto childrenKeysIdx =
        BaseVector::create(INTEGER(), totalChildren, pool_.get());
    auto childrenValsIdx =
        BaseVector::create(INTEGER(), totalChildren, pool_.get());
    auto* childrenKeysIdxFlat = childrenKeysIdx->as<FlatVector<int32_t>>();
    auto* childrenValsIdxFlat = childrenValsIdx->as<FlatVector<int32_t>>();
    for (int r = 0; r < numRows; ++r) {
      for (int f = 0; f < numFields; ++f) {
        int idx = r * numFields + f;
        childrenKeysIdxFlat->set(idx, f);
        childrenValsIdxFlat->set(idx, r * (numFields + 1) + 1 + f);
      }
    }
    auto childrenElementsVec = std::make_shared<RowVector>(
        pool_.get(),
        ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()}),
        nullptr,
        totalChildren,
        std::vector<VectorPtr>{childrenKeysIdx, childrenValsIdx});
    auto childrenOffsets = allocateOffsets(numRows, pool_.get());
    auto childrenSizes = allocateSizes(numRows, pool_.get());
    auto* childrenOffsetsPtr = childrenOffsets->asMutable<vector_size_t>();
    auto* childrenSizesPtr = childrenSizes->asMutable<vector_size_t>();
    for (int r = 0; r < numRows; ++r) {
      childrenOffsetsPtr[r] = r * numFields;
      childrenSizesPtr[r] = numFields;
    }
    auto childrenArray = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
        nullptr,
        numRows,
        childrenOffsets,
        childrenSizes,
        childrenElementsVec);

    // Build values array: each row has numFields+1 values (root + children).
    int32_t totalValues = numRows * (numFields + 1);
    auto valTypeId = BaseVector::create(TINYINT(), totalValues, pool_.get());
    auto valByteOff = BaseVector::create(INTEGER(), totalValues, pool_.get());
    auto* typeIdFlat = valTypeId->as<FlatVector<int8_t>>();
    auto* byteOffFlat = valByteOff->as<FlatVector<int32_t>>();

    auto dataVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
    auto* dataFlat = dataVec->as<FlatVector<StringView>>();

    for (int r = 0; r < numRows; ++r) {
      std::string blob;
      int32_t baseValIdx = r * (numFields + 1);

      // Root value.
      typeIdFlat->set(baseValIdx, static_cast<int8_t>(basic_type::kObject));
      byteOffFlat->set(baseValIdx, 0);

      // Child 0: discount (DOUBLE).
      int32_t off0 = static_cast<int32_t>(blob.size());
      typeIdFlat->set(
          baseValIdx + 1, static_cast<int8_t>(header_byte::kDouble));
      byteOffFlat->set(baseValIdx + 1, off0);
      uint64_t bits;
      std::memcpy(&bits, &rows[r].discount, 8);
      appendLE64(blob, bits);

      // Child 1: linestatus (SHORT_STRING).
      int32_t off1 = static_cast<int32_t>(blob.size());
      typeIdFlat->set(
          baseValIdx + 2,
          static_cast<int8_t>(
              (rows[r].linestatus.size() << 2) | basic_type::kShortString));
      byteOffFlat->set(baseValIdx + 2, off1);
      blob.append(rows[r].linestatus.data(), rows[r].linestatus.size());

      // Child 2: shipdate (DATE).
      int32_t off2 = static_cast<int32_t>(blob.size());
      typeIdFlat->set(
          baseValIdx + 3, static_cast<int8_t>(header_byte::kDate));
      byteOffFlat->set(baseValIdx + 3, off2);
      appendLE32(blob, static_cast<uint32_t>(rows[r].shipdate));

      dataFlat->set(r, StringView(blob.data(), blob.size()));
    }

    auto valuesElementsVec = std::make_shared<RowVector>(
        pool_.get(),
        ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()}),
        nullptr,
        totalValues,
        std::vector<VectorPtr>{valTypeId, valByteOff});
    auto valuesOffsets = allocateOffsets(numRows, pool_.get());
    auto valuesSizes = allocateSizes(numRows, pool_.get());
    auto* valuesOffsetsPtr = valuesOffsets->asMutable<vector_size_t>();
    auto* valuesSizesPtr = valuesSizes->asMutable<vector_size_t>();
    for (int r = 0; r < numRows; ++r) {
      valuesOffsetsPtr[r] = r * (numFields + 1);
      valuesSizesPtr[r] = numFields + 1;
    }
    auto valuesArray = std::make_shared<ArrayVector>(
        pool_.get(),
        ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
        nullptr,
        numRows,
        valuesOffsets,
        valuesSizes,
        valuesElementsVec);

    return std::make_shared<VariantVector>(
        pool_.get(),
        VARIANT_COLUMNAR(),
        nullptr,
        numRows,
        std::vector<VectorPtr>{
            keysArray, childrenArray, valuesArray, dataVec});
  }
};

// ---- Type system tests ----

TEST_F(VariantTypeTest, columnarTypeSingleton) {
  auto t1 = VARIANT_COLUMNAR();
  auto t2 = VARIANT_COLUMNAR();
  ASSERT_EQ(t1.get(), t2.get());
  ASSERT_STREQ(t1->name(), "VARIANT_COLUMNAR");
  ASSERT_STREQ(t1->kindName(), "VARIANT");
  ASSERT_EQ(t1->size(), 4);
  ASSERT_TRUE(t1->parameters().empty());
}

TEST_F(VariantTypeTest, rowBasedTypeSingleton) {
  auto t1 = VARIANT_ROW_BASED();
  auto t2 = VARIANT_ROW_BASED();
  ASSERT_EQ(t1.get(), t2.get());
  ASSERT_STREQ(t1->name(), "VARIANT_ROW_BASED");
  ASSERT_STREQ(t1->kindName(), "VARIANT");
  ASSERT_EQ(t1->size(), 2);
  ASSERT_TRUE(t1->parameters().empty());
}

TEST_F(VariantTypeTest, typeDetection) {
  ASSERT_TRUE(isVariantColumnarType(VARIANT_COLUMNAR()));
  ASSERT_FALSE(isVariantColumnarType(VARIANT_ROW_BASED()));
  ASSERT_TRUE(isVariantRowBasedType(VARIANT_ROW_BASED()));
  ASSERT_FALSE(isVariantRowBasedType(VARIANT_COLUMNAR()));
  ASSERT_TRUE(isVariantType(VARIANT_COLUMNAR()));
  ASSERT_TRUE(isVariantType(VARIANT_ROW_BASED()));
  ASSERT_FALSE(isVariantType(VARCHAR()));

  // Verify both VARIANT types report TypeKind::VARIANT.
  ASSERT_EQ(VARIANT_COLUMNAR()->kind(), TypeKind::VARIANT);
  ASSERT_EQ(VARIANT_ROW_BASED()->kind(), TypeKind::VARIANT);
  ASSERT_TRUE(VARIANT_COLUMNAR()->isVariant());
  ASSERT_TRUE(VARIANT_ROW_BASED()->isVariant());
  ASSERT_FALSE(VARIANT_COLUMNAR()->isRow());
  ASSERT_FALSE(VARIANT_ROW_BASED()->isRow());
}

TEST_F(VariantTypeTest, typeRegistration) {
  ASSERT_TRUE(hasType("VARIANT_COLUMNAR"));
  ASSERT_TRUE(hasType("VARIANT_ROW_BASED"));
  ASSERT_EQ(*getType("VARIANT_COLUMNAR", {}), *VARIANT_COLUMNAR());
  ASSERT_EQ(*getType("VARIANT_ROW_BASED", {}), *VARIANT_ROW_BASED());
}

TEST_F(VariantTypeTest, toRowType) {
  auto variantType = VARIANT_ROW_BASED();
  auto rowType = variantType->toRowType();
  ASSERT_TRUE(rowType->isRow());
  ASSERT_EQ(rowType->size(), 2);
  ASSERT_EQ(rowType->nameOf(0), "metadata");
  ASSERT_EQ(rowType->nameOf(1), "value");
  ASSERT_TRUE(rowType->childAt(0)->isVarbinary());
  ASSERT_TRUE(rowType->childAt(1)->isVarbinary());

  auto columnarType = VARIANT_COLUMNAR();
  auto columnarRowType = columnarType->toRowType();
  ASSERT_TRUE(columnarRowType->isRow());
  ASSERT_EQ(columnarRowType->size(), 4);
  ASSERT_EQ(columnarRowType->nameOf(0), "keys");
}

TEST_F(VariantTypeTest, variantFieldAccess) {
  auto type = VARIANT_ROW_BASED();

  // asVariantType works.
  auto variantPtr = asVariantType(type);
  ASSERT_NE(variantPtr, nullptr);

  // asRowType returns nullptr (no longer inherits RowType).
  auto rowPtr = asRowType(type);
  ASSERT_EQ(rowPtr, nullptr);

  // Field access methods.
  ASSERT_EQ(type->nameOf(0), "metadata");
  ASSERT_EQ(type->nameOf(1), "value");
  ASSERT_EQ(type->getChildIdx("metadata"), 0);
  ASSERT_EQ(type->getChildIdx("value"), 1);
  ASSERT_TRUE(type->containsChild("metadata"));
  ASSERT_FALSE(type->containsChild("nonexistent"));
  ASSERT_EQ(type->getChildIdxIfExists("nonexistent"), std::nullopt);
  ASSERT_TRUE(type->findChild("value")->isVarbinary());
}

TEST_F(VariantTypeTest, columnarTypeSerde) {
  testTypeSerde(VARIANT_COLUMNAR());
}

TEST_F(VariantTypeTest, rowBasedTypeSerde) {
  testTypeSerde(VARIANT_ROW_BASED());
}

// ---- Vector creation tests ----

TEST_F(VariantTypeTest, createRowBasedVector) {
  auto vec = buildRowBasedVariant();
  ASSERT_EQ(vec->size(), 3);
  ASSERT_TRUE(isVariantRowBasedType(vec->type()));
  ASSERT_EQ(vec->childrenSize(), 2);

  auto* metadataCol = vec->childAt(0)->as<FlatVector<StringView>>();
  auto* valueCol = vec->childAt(1)->as<FlatVector<StringView>>();
  ASSERT_NE(metadataCol, nullptr);
  ASSERT_NE(valueCol, nullptr);
  ASSERT_GT(metadataCol->valueAt(0).size(), 0);
  ASSERT_GT(valueCol->valueAt(0).size(), 0);
}

TEST_F(VariantTypeTest, createColumnarVector) {
  auto vec = buildColumnarVariant();
  ASSERT_EQ(vec->size(), 3);
  ASSERT_TRUE(isVariantColumnarType(vec->type()));
  ASSERT_EQ(vec->childrenSize(), 4);
}

// ---- Encoding function tests ----

TEST_F(VariantTypeTest, encodingHeaderByteConstants) {
  EXPECT_EQ(header_byte::kNull, (primitive_type::kNull << 2) | 0);
  EXPECT_EQ(header_byte::kInt32, (primitive_type::kInt32 << 2) | 0);
  EXPECT_EQ(header_byte::kInt64, (primitive_type::kInt64 << 2) | 0);
  EXPECT_EQ(header_byte::kDouble, (primitive_type::kDouble << 2) | 0);
  EXPECT_EQ(header_byte::kDate, (primitive_type::kDate << 2) | 0);
  EXPECT_EQ(header_byte::kString, (primitive_type::kString << 2) | 0);

  EXPECT_EQ(header_byte::kNull, 0x00);
  EXPECT_EQ(header_byte::kInt32, 0x14);
  EXPECT_EQ(header_byte::kInt64, 0x18);
  EXPECT_EQ(header_byte::kDouble, 0x1C);
  EXPECT_EQ(header_byte::kDate, 0x2C);
  EXPECT_EQ(header_byte::kString, 0x40);
}

TEST_F(VariantTypeTest, encodingInt32) {
  auto val = encodeInt32Value(42);
  EXPECT_EQ(val.size(), 5);
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kInt32);
  EXPECT_EQ(readLE32(val.data() + 1), 42u);
}

TEST_F(VariantTypeTest, encodingInt64) {
  auto val = encodeInt64Value(123'456'789'012LL);
  EXPECT_EQ(val.size(), 9);
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kInt64);
  EXPECT_EQ(readLE64(val.data() + 1), 123'456'789'012LL);
}

TEST_F(VariantTypeTest, encodingDouble) {
  auto val = encodeDoubleValue(3.14);
  EXPECT_EQ(val.size(), 9);
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kDouble);
  EXPECT_DOUBLE_EQ(readLEDouble(val.data() + 1), 3.14);
}

TEST_F(VariantTypeTest, encodingDate) {
  auto val = encodeDateValue(10'471);
  EXPECT_EQ(val.size(), 5);
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kDate);
  EXPECT_EQ(static_cast<int32_t>(readLE32(val.data() + 1)), 10'471);
}

TEST_F(VariantTypeTest, encodingShortString) {
  auto val = encodeStringValue("hello");
  EXPECT_EQ(val.size(), 6);
  uint8_t hdr = static_cast<uint8_t>(val[0]);
  EXPECT_EQ(hdr & 0x03, basic_type::kShortString);
  EXPECT_EQ(hdr >> 2, 5u);
  EXPECT_EQ(std::string_view(val.data() + 1, 5), "hello");
}

TEST_F(VariantTypeTest, encodingLongString) {
  std::string longStr(100, 'x');
  auto val = encodeStringValue(longStr);
  EXPECT_EQ(val.size(), 105);
  uint8_t hdr = static_cast<uint8_t>(val[0]);
  EXPECT_EQ(hdr, header_byte::kString);
  EXPECT_EQ(readLE32(val.data() + 1), 100u);
  EXPECT_EQ(std::string_view(val.data() + 5, 100), longStr);
}

TEST_F(VariantTypeTest, encodingNullValue) {
  auto val = encodeNullValue();
  EXPECT_EQ(val.size(), 1);
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kNull);
}

TEST_F(VariantTypeTest, encodingEmptyString) {
  auto val = encodeStringValue("");
  EXPECT_EQ(val.size(), 1);
  uint8_t hdr = static_cast<uint8_t>(val[0]);
  EXPECT_EQ(hdr & 0x03, basic_type::kShortString);
  EXPECT_EQ(hdr >> 2, 0u);
}

TEST_F(VariantTypeTest, encodingMaxShortString) {
  std::string str63(63, 'a');
  auto val = encodeStringValue(str63);
  EXPECT_EQ(val.size(), 64);
  uint8_t hdr = static_cast<uint8_t>(val[0]);
  EXPECT_EQ(hdr & 0x03, basic_type::kShortString);
  EXPECT_EQ(hdr >> 2, 63u);
}

TEST_F(VariantTypeTest, encodingMinLongString) {
  std::string str64(64, 'b');
  auto val = encodeStringValue(str64);
  EXPECT_EQ(val.size(), 69); // 1 + 4 + 64
  EXPECT_EQ(static_cast<uint8_t>(val[0]), header_byte::kString);
  EXPECT_EQ(readLE32(val.data() + 1), 64u);
}

// Verify encodeShortString rejects strings longer than 63 bytes.
TEST_F(VariantTypeTest, encodeShortStringRejectsLongInput) {
  std::string str64(64, 'x');
  VELOX_ASSERT_THROW(
      encodeShortString(str64), "ShortString maximum length is 63");
}

// Verify encodeStringValue correctly routes long strings to long encoding.
TEST_F(VariantTypeTest, encodeStringValueLongStringRoundTrip) {
  std::string str64(64, 'x');
  auto val = encodeStringValue(str64);
  std::string_view decoded;
  ASSERT_TRUE(decodeStringValue(val.data(), val.size(), decoded));
  EXPECT_EQ(decoded, str64);

  std::string str100(100, 'y');
  auto val2 = encodeStringValue(str100);
  std::string_view decoded2;
  ASSERT_TRUE(decodeStringValue(val2.data(), val2.size(), decoded2));
  EXPECT_EQ(decoded2, str100);
}

// ---- Metadata tests ----

TEST_F(VariantTypeTest, metadataBuildAndSize) {
  auto meta = buildTestMetadata();
  ASSERT_GE(meta.size(), 5u);

  uint8_t header = static_cast<uint8_t>(meta[0]);
  uint32_t dictSize = readLE32(meta.data() + 1);
  EXPECT_EQ(dictSize, 3u);
  EXPECT_EQ((header >> 1) & 0x03, 1u); // sorted ascending

  size_t computed = metadataSize(meta.data(), meta.size());
  EXPECT_EQ(computed, meta.size());
}

TEST_F(VariantTypeTest, metadataLookup) {
  std::string metadata = buildTestMetadata();

  ASSERT_EQ(
      lookupFieldIdByName(metadata.data(), metadata.size(), "l_discount"), 0);
  ASSERT_EQ(
      lookupFieldIdByName(metadata.data(), metadata.size(), "l_linestatus"), 1);
  ASSERT_EQ(
      lookupFieldIdByName(metadata.data(), metadata.size(), "l_shipdate"), 2);
  ASSERT_EQ(
      lookupFieldIdByName(metadata.data(), metadata.size(), "not_a_field"), -1);
}

TEST_F(VariantTypeTest, metadataLookupManyKeys) {
  std::vector<std::string_view> keys = {
      "alpha", "bravo", "charlie", "delta", "echo",
      "foxtrot", "golf", "hotel", "india", "juliet"};
  auto meta = buildMetadata(keys);

  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_EQ(
        lookupFieldIdByName(meta.data(), meta.size(), keys[i]),
        static_cast<int32_t>(i));
  }
  EXPECT_EQ(
      lookupFieldIdByName(meta.data(), meta.size(), "zzz"), -1);
  EXPECT_EQ(
      lookupFieldIdByName(meta.data(), meta.size(), "aaa"), -1);
}

TEST_F(VariantTypeTest, metadataLookupTruncatedInput) {
  // Metadata too short to parse.
  EXPECT_EQ(lookupFieldIdByName("ab", 2, "foo"), -1);
}

// ---- Row-based extraction tests ----

TEST_F(VariantTypeTest, rowBasedExtractDouble) {
  auto vec = buildRowBasedVariant();
  auto* valueCol = vec->childAt(1)->as<FlatVector<StringView>>();

  double discounts[] = {0.04, 0.09, 0.01};
  for (int row = 0; row < 3; ++row) {
    auto sv = valueCol->valueAt(row);
    double val;
    ASSERT_TRUE(extractDoubleField(sv.data(), sv.size(), 0, val));
    ASSERT_DOUBLE_EQ(val, discounts[row]);
  }
}

TEST_F(VariantTypeTest, rowBasedExtractDate) {
  auto vec = buildRowBasedVariant();
  auto* valueCol = vec->childAt(1)->as<FlatVector<StringView>>();

  int32_t shipdates[] = {9'204, 9'374, 8'766};
  for (int row = 0; row < 3; ++row) {
    auto sv = valueCol->valueAt(row);
    int32_t val;
    ASSERT_TRUE(extractDateField(sv.data(), sv.size(), 2, val));
    ASSERT_EQ(val, shipdates[row]);
  }
}

TEST_F(VariantTypeTest, rowBasedExtractString) {
  auto vec = buildRowBasedVariant();
  auto* valueCol = vec->childAt(1)->as<FlatVector<StringView>>();

  std::string_view linestatuses[] = {"O", "F", "O"};
  for (int row = 0; row < 3; ++row) {
    auto sv = valueCol->valueAt(row);
    std::string_view val;
    ASSERT_TRUE(extractStringField(sv.data(), sv.size(), 1, val));
    ASSERT_EQ(val, linestatuses[row]);
  }
}

// ---- extractInt32Field tests ----

TEST_F(VariantTypeTest, extractInt32FieldFromObject) {
  // Build Object with 2 fields: field_id=0 INT32(42), field_id=1 INT32(99).
  std::vector<std::string_view> keys = {"field_a", "field_b"};
  auto metadata = buildMetadata(keys);

  std::string childValues[2];
  childValues[0] = encodeInt32Value(42);
  childValues[1] = encodeInt32Value(99);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(2));
  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(1));

  uint8_t offset = 0;
  for (int f = 0; f < 2; ++f) {
    val.push_back(static_cast<char>(offset));
    offset += static_cast<uint8_t>(childValues[f].size());
  }
  val.push_back(static_cast<char>(offset));

  for (auto& cv : childValues) {
    val.append(cv);
  }

  int32_t result;
  ASSERT_TRUE(extractInt32Field(val.data(), val.size(), 0, result));
  EXPECT_EQ(result, 42);

  ASSERT_TRUE(extractInt32Field(val.data(), val.size(), 1, result));
  EXPECT_EQ(result, 99);

  // Field that doesn't exist.
  EXPECT_FALSE(extractInt32Field(val.data(), val.size(), 5, result));
}

// ---- extractInt64Field tests ----

TEST_F(VariantTypeTest, extractInt64FieldFromObject) {
  std::vector<std::string_view> keys = {"field_a", "field_b"};
  auto metadata = buildMetadata(keys);

  std::string childValues[2];
  childValues[0] = encodeInt64Value(1'000'000'000'000LL);
  childValues[1] = encodeInt64Value(-42LL);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(2));
  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(1));

  uint8_t offset = 0;
  for (int f = 0; f < 2; ++f) {
    val.push_back(static_cast<char>(offset));
    offset += static_cast<uint8_t>(childValues[f].size());
  }
  val.push_back(static_cast<char>(offset));

  for (auto& cv : childValues) {
    val.append(cv);
  }

  int64_t result;
  ASSERT_TRUE(extractInt64Field(val.data(), val.size(), 0, result));
  EXPECT_EQ(result, 1'000'000'000'000LL);

  ASSERT_TRUE(extractInt64Field(val.data(), val.size(), 1, result));
  EXPECT_EQ(result, -42LL);

  EXPECT_FALSE(extractInt64Field(val.data(), val.size(), 5, result));
}

// ---- extractStringField tests ----

TEST_F(VariantTypeTest, extractStringFieldShort) {
  auto vec = buildRowBasedVariant();
  auto* valueCol = vec->childAt(1)->as<FlatVector<StringView>>();

  // Row 0 linestatus = "O" (short string).
  auto sv = valueCol->valueAt(0);
  std::string_view val;
  ASSERT_TRUE(extractStringField(sv.data(), sv.size(), 1, val));
  EXPECT_EQ(val, "O");
}

TEST_F(VariantTypeTest, extractStringFieldLong) {
  // Build Object with 1 field: a long string (>63 bytes).
  std::vector<std::string_view> keys = {"long_field"};
  auto metadata = buildMetadata(keys);

  std::string longStr(100, 'z');
  std::string childValues[1];
  childValues[0] = encodeLongString(longStr);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(1));
  val.push_back(static_cast<char>(0));

  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(childValues[0].size()));

  val.append(childValues[0]);

  std::string_view result;
  ASSERT_TRUE(extractStringField(val.data(), val.size(), 0, result));
  EXPECT_EQ(result, longStr);
}

// ---- extractMultipleDoubleFields tests ----

TEST_F(VariantTypeTest, extractMultipleDoubleFields) {
  // Build Object with 4 fields: 0=DOUBLE(1.1), 1=DOUBLE(2.2), 2=DOUBLE(3.3),
  // 3=DOUBLE(4.4).
  std::string childValues[4];
  childValues[0] = encodeDoubleValue(1.1);
  childValues[1] = encodeDoubleValue(2.2);
  childValues[2] = encodeDoubleValue(3.3);
  childValues[3] = encodeDoubleValue(4.4);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(4));
  for (uint8_t fid = 0; fid < 4; ++fid) {
    val.push_back(static_cast<char>(fid));
  }
  uint8_t offset = 0;
  for (int f = 0; f < 4; ++f) {
    val.push_back(static_cast<char>(offset));
    offset += static_cast<uint8_t>(childValues[f].size());
  }
  val.push_back(static_cast<char>(offset));
  for (auto& cv : childValues) {
    val.append(cv);
  }

  // Extract fields 0, 2, 3 (must be ascending).
  uint8_t targets[] = {0, 2, 3};
  double outputs[3];
  ASSERT_TRUE(
      extractMultipleDoubleFields(val.data(), val.size(), targets, 3, outputs));
  EXPECT_DOUBLE_EQ(outputs[0], 1.1);
  EXPECT_DOUBLE_EQ(outputs[1], 3.3);
  EXPECT_DOUBLE_EQ(outputs[2], 4.4);
}

TEST_F(VariantTypeTest, extractMultipleDoubleFieldsMissingField) {
  // Build Object with 2 DOUBLE fields, try to extract 3.
  std::string childValues[2];
  childValues[0] = encodeDoubleValue(1.0);
  childValues[1] = encodeDoubleValue(2.0);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(2));
  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(1));

  uint8_t offset = 0;
  for (int f = 0; f < 2; ++f) {
    val.push_back(static_cast<char>(offset));
    offset += static_cast<uint8_t>(childValues[f].size());
  }
  val.push_back(static_cast<char>(offset));
  for (auto& cv : childValues) {
    val.append(cv);
  }

  // Field 5 doesn't exist.
  uint8_t targets[] = {0, 1, 5};
  double outputs[3];
  EXPECT_FALSE(
      extractMultipleDoubleFields(val.data(), val.size(), targets, 3, outputs));
}

// ---- Malformed data tests ----

TEST_F(VariantTypeTest, extractFromEmptyBlob) {
  double doubleVal;
  EXPECT_FALSE(extractDoubleField("", 0, 0, doubleVal));

  int32_t dateVal;
  EXPECT_FALSE(extractDateField("", 0, 0, dateVal));

  int32_t int32Val;
  EXPECT_FALSE(extractInt32Field("", 0, 0, int32Val));

  std::string_view strVal;
  EXPECT_FALSE(extractStringField("", 0, 0, strVal));
}

TEST_F(VariantTypeTest, extractFromTruncatedBlob) {
  // A valid Object header but truncated before child data.
  std::string val;
  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(1)); // 1 field
  // Truncated: no field_ids or offsets.

  double doubleVal;
  EXPECT_FALSE(extractDoubleField(val.data(), val.size(), 0, doubleVal));
}

TEST_F(VariantTypeTest, extractFromNonObjectBlob) {
  // A primitive value (not an Object).
  auto val = encodeInt32Value(42);

  double doubleVal;
  EXPECT_FALSE(extractDoubleField(val.data(), val.size(), 0, doubleVal));

  int32_t dateVal;
  EXPECT_FALSE(extractDateField(val.data(), val.size(), 0, dateVal));
}

TEST_F(VariantTypeTest, extractWrongTypeField) {
  // Build Object with INT32 field, try to extract as DOUBLE.
  std::string childValues[1];
  childValues[0] = encodeInt32Value(42);

  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  std::string val;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(1));
  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(0));
  val.push_back(static_cast<char>(childValues[0].size()));
  val.append(childValues[0]);

  double doubleVal;
  EXPECT_FALSE(extractDoubleField(val.data(), val.size(), 0, doubleVal));

  // Extracting as INT32 should succeed.
  int32_t int32Val;
  ASSERT_TRUE(extractInt32Field(val.data(), val.size(), 0, int32Val));
  EXPECT_EQ(int32Val, 42);
}

// ---- Columnar extraction tests ----

TEST_F(VariantTypeTest, columnarExtractDouble) {
  auto vec = buildColumnarVariant();
  auto* childrenArray = vec->childAt(1)->as<ArrayVector>();
  auto* childrenElements = childrenArray->elements()->as<RowVector>();
  auto* valuesIndexCol =
      childrenElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* valuesArray = vec->childAt(2)->as<ArrayVector>();
  auto* valuesElements = valuesArray->elements()->as<RowVector>();
  auto* byteOffsetCol =
      valuesElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* dataCol = vec->childAt(3)->as<FlatVector<StringView>>();

  double discounts[] = {0.04, 0.09, 0.01};
  int32_t fieldId = 0; // l_discount

  for (int row = 0; row < 3; ++row) {
    auto childrenOffset = childrenArray->offsetAt(row);
    auto valIdx = valuesIndexCol->valueAt(childrenOffset + fieldId);
    auto byteOff = byteOffsetCol->valueAt(valIdx);
    auto data = dataCol->valueAt(row);
    double val = readLEDouble(data.data() + byteOff);
    ASSERT_DOUBLE_EQ(val, discounts[row]);
  }
}

TEST_F(VariantTypeTest, columnarExtractDate) {
  auto vec = buildColumnarVariant();
  auto* childrenArray = vec->childAt(1)->as<ArrayVector>();
  auto* childrenElements = childrenArray->elements()->as<RowVector>();
  auto* valuesIndexCol =
      childrenElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* valuesArray = vec->childAt(2)->as<ArrayVector>();
  auto* valuesElements = valuesArray->elements()->as<RowVector>();
  auto* byteOffsetCol =
      valuesElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* dataCol = vec->childAt(3)->as<FlatVector<StringView>>();

  int32_t shipdates[] = {9'204, 9'374, 8'766};
  int32_t fieldId = 2; // l_shipdate

  for (int row = 0; row < 3; ++row) {
    auto childrenOffset = childrenArray->offsetAt(row);
    auto valIdx = valuesIndexCol->valueAt(childrenOffset + fieldId);
    auto byteOff = byteOffsetCol->valueAt(valIdx);
    auto data = dataCol->valueAt(row);
    int32_t val = static_cast<int32_t>(readLE32(data.data() + byteOff));
    ASSERT_EQ(val, shipdates[row]);
  }
}

// ---- Cross-format consistency test ----

TEST_F(VariantTypeTest, crossFormatConsistency) {
  auto rowBased = buildRowBasedVariant();
  auto columnar = buildColumnarVariant();

  auto* valueCol = rowBased->childAt(1)->as<FlatVector<StringView>>();
  auto* childrenArray = columnar->childAt(1)->as<ArrayVector>();
  auto* childrenElements = childrenArray->elements()->as<RowVector>();
  auto* valuesIndexCol =
      childrenElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* valuesArray = columnar->childAt(2)->as<ArrayVector>();
  auto* valuesElements = valuesArray->elements()->as<RowVector>();
  auto* byteOffsetCol =
      valuesElements->childAt(1)->as<FlatVector<int32_t>>();
  auto* dataCol = columnar->childAt(3)->as<FlatVector<StringView>>();

  // Verify l_discount extraction produces same results from both formats.
  for (int row = 0; row < 3; ++row) {
    auto sv = valueCol->valueAt(row);
    double rowBasedVal;
    ASSERT_TRUE(extractDoubleField(sv.data(), sv.size(), 0, rowBasedVal));

    auto childrenOffset = childrenArray->offsetAt(row);
    auto valIdx = valuesIndexCol->valueAt(childrenOffset + 0);
    auto byteOff = byteOffsetCol->valueAt(valIdx);
    auto data = dataCol->valueAt(row);
    double columnarVal = readLEDouble(data.data() + byteOff);

    ASSERT_DOUBLE_EQ(rowBasedVal, columnarVal);
  }

  // Verify l_shipdate extraction from both formats.
  for (int row = 0; row < 3; ++row) {
    auto sv = valueCol->valueAt(row);
    int32_t rowBasedVal;
    ASSERT_TRUE(extractDateField(sv.data(), sv.size(), 2, rowBasedVal));

    auto childrenOffset = childrenArray->offsetAt(row);
    auto valIdx = valuesIndexCol->valueAt(childrenOffset + 2);
    auto byteOff = byteOffsetCol->valueAt(valIdx);
    auto data = dataCol->valueAt(row);
    int32_t columnarVal = static_cast<int32_t>(readLE32(data.data() + byteOff));

    ASSERT_EQ(rowBasedVal, columnarVal);
  }
}

// ---- Object parsing edge cases ----

TEST_F(VariantTypeTest, parseObjectHeaderBasic) {
  // Build a minimal valid Object with 0 fields.
  std::string val;
  uint8_t objectHeader =
      (0 << 6) | (0 << 4) | (0 << 2) | basic_type::kObject;
  val.push_back(static_cast<char>(objectHeader));
  val.push_back(static_cast<char>(0)); // 0 fields

  // 1 offset (sentinel).
  val.push_back(static_cast<char>(0));

  uint32_t numFields;
  uint8_t fieldIdSize;
  uint8_t offsetSize;
  const char* fieldIds;
  const char* offsets;
  auto* childData = parseObjectHeader(
      val.data(), val.size(), numFields, fieldIdSize, offsetSize, fieldIds,
      offsets);
  ASSERT_NE(childData, nullptr);
  EXPECT_EQ(numFields, 0u);
  EXPECT_EQ(fieldIdSize, 1);
  EXPECT_EQ(offsetSize, 1);
}

TEST_F(VariantTypeTest, findFieldOffsetBinarySearch) {
  // Build sorted field_ids: [2, 5, 8, 12].
  char fieldIdsArr[] = {2, 5, 8, 12};
  // Offsets (1-byte): [0, 9, 18, 27, 36].
  char offsetsArr[] = {0, 9, 18, 27, 36};

  uint32_t startOut, endOut;

  // Find field 5 -> should return [9, 18).
  ASSERT_TRUE(findFieldOffset(
      fieldIdsArr, 1, offsetsArr, 1, 4, 5, startOut, endOut));
  EXPECT_EQ(startOut, 9u);
  EXPECT_EQ(endOut, 18u);

  // Find field 12 -> should return [27, 36).
  ASSERT_TRUE(findFieldOffset(
      fieldIdsArr, 1, offsetsArr, 1, 4, 12, startOut, endOut));
  EXPECT_EQ(startOut, 27u);
  EXPECT_EQ(endOut, 36u);

  // Find field 7 -> not present.
  EXPECT_FALSE(findFieldOffset(
      fieldIdsArr, 1, offsetsArr, 1, 4, 7, startOut, endOut));
}

// ---- Corrupted blob offset validation tests ----

// Build a minimal Object blob with corrupted offsets that extend past the
// actual child data. locateObjectField / extractObjectField must return false
// instead of producing an out-of-bounds pointer.
TEST_F(VariantTypeTest, locateObjectFieldRejectsCorruptedOffsets) {
  // Hand-craft a 1-field Object with child data = 5 bytes ("hello" without
  // header, but doesn't matter — we test offset validation, not value parsing).
  // Then set the sentinel offset to a value larger than the actual child data.

  // Header: is_large=0, fieldIdSizeMinus1=0, offsetSizeMinus1=0, type=kObject
  // => (0<<6)|(0<<4)|(0<<2)|0x02 = 0x02
  std::string blob;
  blob.push_back(0x02); // header
  blob.push_back(0x01); // num_elements = 1
  blob.push_back(0x00); // field_id[0] = 0
  blob.push_back(0x00); // offset[0] = 0
  blob.push_back(0x63); // offset[1] = 99 (sentinel — way past actual data!)
  // Child data: only 5 bytes.
  blob.append("hello");

  const char* fieldOut{};
  size_t fieldLen{};

  // Without validation, this would return a pointer with fieldLen = 99,
  // extending far past the blob.
  EXPECT_FALSE(
      extractObjectField(blob.data(), blob.size(), 0, fieldOut, fieldLen));
}

// Same idea for extractArrayElement with a corrupted array offset.
TEST_F(VariantTypeTest, extractArrayElementRejectsCorruptedOffsets) {
  // Header: is_large=0, offsetSizeMinus1=0, type=kArray
  // => (0<<4)|(0<<2)|0x03 = 0x03
  std::string blob;
  blob.push_back(0x03); // header
  blob.push_back(0x01); // num_elements = 1
  blob.push_back(0x00); // offset[0] = 0
  blob.push_back(0x50); // offset[1] = 80 (sentinel — past actual data!)
  // Element data: only 3 bytes.
  blob.append("abc");

  const char* elemOut{};
  size_t elemLen{};

  EXPECT_FALSE(
      extractArrayElement(blob.data(), blob.size(), 0, elemOut, elemLen));
}

// extractObjectField should reject start > end (reverse offsets).
TEST_F(VariantTypeTest, locateObjectFieldRejectsReverseOffsets) {
  // 1-field Object where offset[0] > offset[1] (reversed).
  std::string blob;
  blob.push_back(0x02); // header: Object, !large, 1B fid, 1B offset
  blob.push_back(0x01); // num_elements = 1
  blob.push_back(0x00); // field_id[0] = 0
  blob.push_back(0x05); // offset[0] = 5  (start)
  blob.push_back(0x02); // offset[1] = 2  (end < start!)
  blob.append("hello"); // 5 bytes child data

  const char* fieldOut{};
  size_t fieldLen{};
  EXPECT_FALSE(
      extractObjectField(blob.data(), blob.size(), 0, fieldOut, fieldLen));
}

// extractMultipleDoubleFields should reject corrupted offsets too.
TEST_F(VariantTypeTest, extractMultipleDoubleFieldsRejectsCorruptedOffsets) {
  // Build a 2-field Object. Field 0 is a valid double (9 bytes).
  // Field 1's sentinel offset is corrupted (too large).
  std::string childVal0 = encodeDoubleValue(3.14);
  std::string childVal1 = encodeDoubleValue(2.72);

  // Use encodeObjectWithFieldIds to build a valid base, then corrupt it.
  auto validBlob = encodeObjectWithFieldIds({0, 1}, {childVal0, childVal1});

  // Corrupt the sentinel offset (last offset entry) to be huge.
  // Header: 1B header + 1B count + 2*1B field_ids + 3*1B offsets + child data
  // Offsets start at byte 4, sentinel at byte 6.
  // But the offset size may vary. Let's just test via the valid extract first,
  // then build a manual blob.

  // Manual 2-field Object with corrupted sentinel:
  std::string blob;
  blob.push_back(0x02); // header: Object, !large, 1B fid, 1B offset
  blob.push_back(0x02); // num_elements = 2
  blob.push_back(0x00); // field_id[0] = 0
  blob.push_back(0x01); // field_id[1] = 1
  blob.push_back(0x00); // offset[0] = 0
  blob.push_back(0x09); // offset[1] = 9
  blob.push_back(0xFF); // offset[2] = 255 (sentinel — corrupted!)
  blob.append(childVal0);
  blob.append(childVal1);

  uint8_t targets[] = {0, 1};
  double outputs[2]{};
  EXPECT_FALSE(extractMultipleDoubleFields(
      blob.data(), blob.size(), targets, 2, outputs));
}

// ---- LE read/write round-trip tests ----

TEST_F(VariantTypeTest, leRoundTrip16) {
  std::string buf;
  appendLE16(buf, 0xABCD);
  EXPECT_EQ(readLE16(buf.data()), 0xABCD);
}

TEST_F(VariantTypeTest, leRoundTrip32) {
  std::string buf;
  appendLE32(buf, 0xDEADBEEF);
  EXPECT_EQ(readLE32(buf.data()), 0xDEADBEEF);
}

TEST_F(VariantTypeTest, leRoundTrip64) {
  std::string buf;
  appendLE64(buf, 0x0102030405060708ULL);
  EXPECT_EQ(
      static_cast<uint64_t>(readLE64(buf.data())), 0x0102030405060708ULL);
}

TEST_F(VariantTypeTest, leDoubleRoundTrip) {
  std::string buf;
  double original = -1.23456789e-100;
  uint64_t bits;
  std::memcpy(&bits, &original, 8);
  appendLE64(buf, bits);
  EXPECT_DOUBLE_EQ(readLEDouble(buf.data()), original);
}

// Verify encodeObjectWithFieldIds handles large field_ids (> 65535).
TEST_F(VariantTypeTest, encodeObjectLargeFieldId) {
  // Field id 70000 exceeds 16-bit range, requiring 4-byte field_ids.
  std::vector<uint32_t> fieldIds = {100, 70'000};
  std::vector<std::string> children = {
      encodeStringValue("hello"),
      encodeInt32Value(42),
  };
  auto encoded = encodeObjectWithFieldIds(fieldIds, children);

  // Parse header and verify it round-trips.
  uint32_t numFields{};
  uint8_t fieldIdSize{};
  uint8_t offsetSize{};
  const char* fieldIdsPtr{};
  const char* offsetsPtr{};
  auto* childData = parseObjectHeader(
      encoded.data(),
      encoded.size(),
      numFields,
      fieldIdSize,
      offsetSize,
      fieldIdsPtr,
      offsetsPtr);
  ASSERT_NE(childData, nullptr);
  EXPECT_EQ(numFields, 2);
  // field_id 70000 needs 4-byte field_ids (field_id_size_minus1 = 3).
  EXPECT_EQ(fieldIdSize, 4);

  // Verify we can extract the second field (field_id 70000).
  const char* fieldOut{};
  size_t fieldLen{};
  ASSERT_TRUE(extractObjectField(
      encoded.data(), encoded.size(), 70'000, fieldOut, fieldLen));
  // Decode as int32.
  EXPECT_EQ(
      static_cast<int32_t>(readLE32(fieldOut + 1)),
      42);
}

// Verify encodeObjectWithFieldIds rejects unsorted field_ids in debug mode.
#ifndef NDEBUG
TEST_F(VariantTypeTest, encodeObjectWithFieldIdsRejectsUnsorted) {
  std::vector<uint32_t> unsortedIds = {5, 2};
  std::vector<std::string> children = {
      encodeStringValue("a"),
      encodeStringValue("b"),
  };
  VELOX_ASSERT_THROW(
      encodeObjectWithFieldIds(unsortedIds, children),
      "fieldIds must be sorted");
}
#endif

// ---- VariantExtract UDF bug: applyColumnar crashes on missing field ----

} // namespace facebook::velox::test

namespace facebook::velox {
// The VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA macro in
// VariantExtract.cpp creates this registration function in namespace
// facebook::velox.
extern void registerVectorFunction_udf_variant_extract_double(
    const std::string&);
} // namespace facebook::velox

namespace facebook::velox::test {

// Verify that variant_extract on a columnar VARIANT returns null (not crash)
// when the requested field does not exist.
TEST_F(VariantTypeTest, columnarExtractMissingFieldReturnsNull) {
  facebook::velox::registerVectorFunction_udf_variant_extract_double(
      "variant_extract_double_test");

  auto columnar = buildColumnarVariant();

  // Create a constant vector holding a non-existent field name.
  auto fieldNameConstant = std::make_shared<ConstantVector<StringView>>(
      pool_.get(), 3, false, VARCHAR(), StringView("nonexistent"));

  // Use the raw ROW type matching the UDF signature so signature resolution
  // succeeds. The factory detects columnar layout by child count (4).
  auto columnarRowType = ROW(
      {"keys", "children", "values", "data"},
      {ARRAY(VARCHAR()),
       ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
       ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
       VARBINARY()});
  std::vector<TypePtr> inputTypes = {columnarRowType, VARCHAR()};
  std::vector<VectorPtr> constantInputs = {nullptr, fieldNameConstant};
  auto queryCtx = core::QueryCtx::create();
  auto func = exec::getVectorFunction(
      "variant_extract_double_test",
      inputTypes,
      constantInputs,
      queryCtx->queryConfig());
  ASSERT_NE(func, nullptr);

  // Build a minimal EvalCtx.
  auto execCtxPool = memory::memoryManager()->addLeafPool("execCtx");
  core::ExecCtx execCtx{execCtxPool.get(), queryCtx.get()};
  exec::EvalCtx evalCtx(&execCtx);

  SelectivityVector rows(3);
  std::vector<VectorPtr> args = {columnar};
  VectorPtr result; // nullptr — same as what the expression engine passes in.

  func->apply(rows, args, DOUBLE(), evalCtx, result);
  ASSERT_NE(result, nullptr);
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->isNullAt(i));
  }
}

// Verify that columnar extract returns null (not OOB crash) when a row has
// fewer children than the fieldId found from another row.
TEST_F(VariantTypeTest, columnarExtractVaryingChildCount) {
  facebook::velox::registerVectorFunction_udf_variant_extract_double(
      "variant_extract_double_oob_test");

  // Build a 2-row columnar VARIANT where:
  //   Row 0: 3 fields (l_discount=0, l_linestatus=1, l_shipdate=2)
  //   Row 1: 1 field  (l_discount=0)   <-- fewer children than fieldId=2
  // Row 1's children start at offset 3 in the flat array of total size 4.
  // When fieldId=2, childrenOffset(1)+2 = 3+2 = 5, but the flat vector
  // only has 4 elements — this is a genuine out-of-bounds access.
  int32_t numRows = 2;

  // Keys array.
  int32_t totalKeys = 4; // 3 + 1
  auto keysElementsVec =
      BaseVector::create(VARCHAR(), totalKeys, pool_.get());
  auto* keysElements = keysElementsVec->as<FlatVector<StringView>>();
  std::string_view allKeys[] = {"l_discount", "l_linestatus", "l_shipdate"};
  // Row 0: all 3 keys.
  for (int f = 0; f < 3; ++f) {
    keysElements->set(f, StringView(allKeys[f].data(), allKeys[f].size()));
  }
  // Row 1: only l_discount.
  keysElements->set(3, StringView(allKeys[0].data(), allKeys[0].size()));

  auto keysOffsets = allocateOffsets(numRows, pool_.get());
  auto keysSizes = allocateSizes(numRows, pool_.get());
  auto* keysOffsetsPtr = keysOffsets->asMutable<vector_size_t>();
  auto* keysSizesPtr = keysSizes->asMutable<vector_size_t>();
  keysOffsetsPtr[0] = 0;
  keysSizesPtr[0] = 3;
  keysOffsetsPtr[1] = 3;
  keysSizesPtr[1] = 1;
  auto keysArray = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(VARCHAR()),
      nullptr,
      numRows,
      keysOffsets,
      keysSizes,
      keysElementsVec);

  // Children array: 4 elements total (3 for row 0, 1 for row 1).
  int32_t totalChildren = 4;
  auto childrenKeysIdx =
      BaseVector::create(INTEGER(), totalChildren, pool_.get());
  auto childrenValsIdx =
      BaseVector::create(INTEGER(), totalChildren, pool_.get());
  auto* childrenKeysIdxFlat = childrenKeysIdx->as<FlatVector<int32_t>>();
  auto* childrenValsIdxFlat = childrenValsIdx->as<FlatVector<int32_t>>();
  // Row 0: children[0..2] → values 1,2,3 (root at 0).
  childrenKeysIdxFlat->set(0, 0);
  childrenValsIdxFlat->set(0, 1);
  childrenKeysIdxFlat->set(1, 1);
  childrenValsIdxFlat->set(1, 2);
  childrenKeysIdxFlat->set(2, 2);
  childrenValsIdxFlat->set(2, 3);
  // Row 1: children[3] → value 5 (root at 4).
  childrenKeysIdxFlat->set(3, 0);
  childrenValsIdxFlat->set(3, 5);

  auto childrenElementsVec = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()}),
      nullptr,
      totalChildren,
      std::vector<VectorPtr>{childrenKeysIdx, childrenValsIdx});
  auto childrenOffsets = allocateOffsets(numRows, pool_.get());
  auto childrenSizes = allocateSizes(numRows, pool_.get());
  auto* childrenOffsetsPtr = childrenOffsets->asMutable<vector_size_t>();
  auto* childrenSizesPtr = childrenSizes->asMutable<vector_size_t>();
  childrenOffsetsPtr[0] = 0;
  childrenSizesPtr[0] = 3;
  childrenOffsetsPtr[1] = 3;
  childrenSizesPtr[1] = 1;
  auto childrenArray = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
      nullptr,
      numRows,
      childrenOffsets,
      childrenSizes,
      childrenElementsVec);

  // Values: row 0 has 4 vals (root + 3 children), row 1 has 2 (root + 1).
  int32_t totalValues = 6;
  auto valTypeId = BaseVector::create(TINYINT(), totalValues, pool_.get());
  auto valByteOff = BaseVector::create(INTEGER(), totalValues, pool_.get());
  auto* typeIdFlat = valTypeId->as<FlatVector<int8_t>>();
  auto* byteOffFlat = valByteOff->as<FlatVector<int32_t>>();

  auto dataVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
  auto* dataFlat = dataVec->as<FlatVector<StringView>>();

  // Row 0: root(0), discount(1), linestatus(2), shipdate(3).
  {
    std::string blob;
    typeIdFlat->set(0, static_cast<int8_t>(basic_type::kObject));
    byteOffFlat->set(0, 0);
    typeIdFlat->set(1, static_cast<int8_t>(header_byte::kDouble));
    byteOffFlat->set(1, static_cast<int32_t>(blob.size()));
    double discount = 0.04;
    uint64_t bits;
    std::memcpy(&bits, &discount, 8);
    appendLE64(blob, bits);
    typeIdFlat->set(
        2, static_cast<int8_t>((1 << 2) | basic_type::kShortString));
    byteOffFlat->set(2, static_cast<int32_t>(blob.size()));
    blob.append("O");
    typeIdFlat->set(3, static_cast<int8_t>(header_byte::kDate));
    byteOffFlat->set(3, static_cast<int32_t>(blob.size()));
    appendLE32(blob, static_cast<uint32_t>(9'204));
    dataFlat->set(0, StringView(blob.data(), blob.size()));
  }
  // Row 1: root(4), discount(5).
  {
    std::string blob;
    typeIdFlat->set(4, static_cast<int8_t>(basic_type::kObject));
    byteOffFlat->set(4, 0);
    typeIdFlat->set(5, static_cast<int8_t>(header_byte::kDouble));
    byteOffFlat->set(5, static_cast<int32_t>(blob.size()));
    double discount = 0.09;
    uint64_t bits;
    std::memcpy(&bits, &discount, 8);
    appendLE64(blob, bits);
    dataFlat->set(1, StringView(blob.data(), blob.size()));
  }

  auto valuesElementsVec = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()}),
      nullptr,
      totalValues,
      std::vector<VectorPtr>{valTypeId, valByteOff});
  auto valuesOffsets = allocateOffsets(numRows, pool_.get());
  auto valuesSizes = allocateSizes(numRows, pool_.get());
  auto* valuesOffsetsPtr = valuesOffsets->asMutable<vector_size_t>();
  auto* valuesSizesPtr = valuesSizes->asMutable<vector_size_t>();
  valuesOffsetsPtr[0] = 0;
  valuesSizesPtr[0] = 4;
  valuesOffsetsPtr[1] = 4;
  valuesSizesPtr[1] = 2;
  auto valuesArray = std::make_shared<ArrayVector>(
      pool_.get(),
      ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
      nullptr,
      numRows,
      valuesOffsets,
      valuesSizes,
      valuesElementsVec);

  auto columnar = std::make_shared<VariantVector>(
      pool_.get(),
      VARIANT_COLUMNAR(),
      nullptr,
      numRows,
      std::vector<VectorPtr>{keysArray, childrenArray, valuesArray, dataVec});

  // Extract l_shipdate (fieldId=2). Row 1 has only 1 child (offset=3,
  // size=1). Without bounds check, childrenOffset+fieldId = 3+2 = 5 but
  // the flat children vector has only 4 elements → out-of-bounds.
  auto fieldNameConstant = std::make_shared<ConstantVector<StringView>>(
      pool_.get(), numRows, false, VARCHAR(), StringView("l_shipdate"));

  // Use the raw ROW type (not VARIANT_COLUMNAR) so signature resolution
  // matches the UDF's registered 4-column signature.
  auto columnarRowType = ROW(
      {"keys", "children", "values", "data"},
      {ARRAY(VARCHAR()),
       ARRAY(ROW({"keys_index", "values_index"}, {INTEGER(), INTEGER()})),
       ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
       VARBINARY()});
  std::vector<TypePtr> inputTypes = {columnarRowType, VARCHAR()};
  std::vector<VectorPtr> constantInputs = {nullptr, fieldNameConstant};
  auto queryCtx = core::QueryCtx::create();
  auto func = exec::getVectorFunction(
      "variant_extract_double_oob_test",
      inputTypes,
      constantInputs,
      queryCtx->queryConfig());
  ASSERT_NE(func, nullptr);

  auto execCtxPool = memory::memoryManager()->addLeafPool("execCtxOob");
  core::ExecCtx execCtx{execCtxPool.get(), queryCtx.get()};
  exec::EvalCtx evalCtx(&execCtx);

  SelectivityVector rows(numRows);
  std::vector<VectorPtr> args = {columnar};
  VectorPtr result;

  // Without the bounds check fix this would read past the children array.
  // With the fix, row 1 should be null.
  func->apply(rows, args, DOUBLE(), evalCtx, result);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->isNullAt(1))
      << "Row 1 should be null — it has only 1 child but fieldId=2";
}

// Verify that passing a NULL constant field name to variant_extract throws
// a user error instead of reading undefined memory.
TEST_F(VariantTypeTest, variantExtractRejectsNullFieldName) {
  facebook::velox::registerVectorFunction_udf_variant_extract_double(
      "variant_extract_double_null_name_test");

  // A null constant VARCHAR.
  auto nullFieldName = std::make_shared<ConstantVector<StringView>>(
      pool_.get(), 1, true /*isNull*/, VARCHAR(), StringView());

  auto rowBasedType = ROW({"metadata", "value"}, {VARBINARY(), VARBINARY()});
  std::vector<TypePtr> inputTypes = {rowBasedType, VARCHAR()};
  std::vector<VectorPtr> constantInputs = {nullptr, nullFieldName};
  auto queryCtx = core::QueryCtx::create();

  VELOX_ASSERT_USER_THROW(
      exec::getVectorFunction(
          "variant_extract_double_null_name_test",
          inputTypes,
          constantInputs,
          queryCtx->queryConfig()),
      "variant_extract field name must not be null");
}

// Verify that variant_extract handles null input rows gracefully (returns null,
// not undefined behavior) when defaultNullBehavior is false.
TEST_F(VariantTypeTest, variantExtractHandlesNullInputRow) {
  facebook::velox::registerVectorFunction_udf_variant_extract_double(
      "variant_extract_double_null_row_test");

  // Build a 3-row row-based VARIANT where row 1 is null.
  int32_t numRows = 3;
  std::string metadata = buildTestMetadata();

  auto metadataVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
  auto valueVec = BaseVector::create(VARBINARY(), numRows, pool_.get());
  auto* metadataFlat = metadataVec->as<FlatVector<StringView>>();
  auto* valueFlat = valueVec->as<FlatVector<StringView>>();

  // Row 0 and 2: valid data.
  double discounts[] = {0.04, 0.0, 0.01};
  int32_t shipdates[] = {9'204, 0, 8'766};
  std::string_view linestatuses[] = {"O", "", "O"};
  for (int i : {0, 2}) {
    metadataFlat->set(i, StringView(metadata.data(), metadata.size()));
    std::string val =
        encodeTestObject(discounts[i], linestatuses[i], shipdates[i]);
    valueFlat->set(i, StringView(val.data(), val.size()));
  }
  // Row 1: set metadata and value to something (won't be read if null check
  // works), but mark the RowVector row as null.
  metadataFlat->set(1, StringView(metadata.data(), metadata.size()));
  std::string dummyVal = encodeTestObject(0.0, "", 0);
  valueFlat->set(1, StringView(dummyVal.data(), dummyVal.size()));

  // Create nulls buffer for the RowVector: row 1 is null.
  auto nulls = allocateNulls(numRows, pool_.get());
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  bits::setNull(nullsPtr, 0, false);
  bits::setNull(nullsPtr, 1, true);
  bits::setNull(nullsPtr, 2, false);

  auto rowBased = std::make_shared<RowVector>(
      pool_.get(),
      ROW({"metadata", "value"}, {VARBINARY(), VARBINARY()}),
      nulls,
      numRows,
      std::vector<VectorPtr>{metadataVec, valueVec});

  auto fieldNameConstant = std::make_shared<ConstantVector<StringView>>(
      pool_.get(), numRows, false, VARCHAR(), StringView("l_discount"));

  auto rowBasedType = ROW({"metadata", "value"}, {VARBINARY(), VARBINARY()});
  std::vector<TypePtr> inputTypes = {rowBasedType, VARCHAR()};
  std::vector<VectorPtr> constantInputs = {nullptr, fieldNameConstant};
  auto queryCtx = core::QueryCtx::create();
  auto func = exec::getVectorFunction(
      "variant_extract_double_null_row_test",
      inputTypes,
      constantInputs,
      queryCtx->queryConfig());
  ASSERT_NE(func, nullptr);

  auto execCtxPool = memory::memoryManager()->addLeafPool("execCtxNull");
  core::ExecCtx execCtx{execCtxPool.get(), queryCtx.get()};
  exec::EvalCtx evalCtx(&execCtx);

  SelectivityVector rows(numRows);
  std::vector<VectorPtr> args = {rowBased};
  VectorPtr result;

  func->apply(rows, args, DOUBLE(), evalCtx, result);
  ASSERT_NE(result, nullptr);

  // Row 0 and 2 should have valid discount values.
  EXPECT_FALSE(result->isNullAt(0));
  EXPECT_DOUBLE_EQ(result->as<FlatVector<double>>()->valueAt(0), 0.04);
  EXPECT_FALSE(result->isNullAt(2));
  EXPECT_DOUBLE_EQ(result->as<FlatVector<double>>()->valueAt(2), 0.01);

  // Row 1 should be null (input row is null).
  EXPECT_TRUE(result->isNullAt(1))
      << "Null input row should produce null output";
}

} // namespace facebook::velox::test
