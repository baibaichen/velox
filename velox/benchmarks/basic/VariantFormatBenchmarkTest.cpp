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

#include "velox/benchmarks/basic/VariantBenchEncoding.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::variant_encoding;
using namespace facebook::velox::variant_bench;
using namespace facebook::velox;

class VariantBlobTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::deprecatedAddDefaultLeafMemoryPool("test");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

// Encoding primitive tests (headerByteConstants, encodeInt32, encodeInt64,
// encodeDouble, encodeDate, encodeShortString, encodeLongString) live in
// VariantTypeTest.cpp alongside the VariantEncoding.h library they exercise.

TEST_F(VariantBlobTest, lineitemMetadataHas16SortedKeys) {
  auto meta = buildLineitemMetadata();

  // Parse metadata header.
  ASSERT_GE(meta.size(), 5u);
  uint8_t header = static_cast<uint8_t>(meta[0]);
  uint32_t dictSize = readLE32(meta.data() + 1);
  EXPECT_EQ(dictSize, 16u);

  // Verify sorted flag.
  EXPECT_EQ((header >> 1) & 0x03, 1u); // sorted ascending

  // Parse and verify keys are sorted.
  uint8_t offsetSize = ((header >> 6) & 0x03) + 1;
  size_t offsetsStart = 5;
  std::vector<std::string> keys;
  for (uint32_t d = 0; d < dictSize; ++d) {
    uint32_t strStart = 0;
    uint32_t strEnd = 0;
    const char* offBase = meta.data() + offsetsStart;
    if (offsetSize == 1) {
      strStart = static_cast<uint8_t>(offBase[d]);
      strEnd = static_cast<uint8_t>(offBase[d + 1]);
    } else if (offsetSize == 2) {
      strStart = readLE16(offBase + d * 2);
      strEnd = readLE16(offBase + (d + 1) * 2);
    } else {
      strStart = readLE32(offBase + d * 4);
      strEnd = readLE32(offBase + (d + 1) * 4);
    }
    size_t dictDataStart = offsetsStart + (dictSize + 1) * offsetSize;
    keys.emplace_back(meta.data() + dictDataStart + strStart, strEnd - strStart);
  }

  EXPECT_EQ(keys.size(), 16u);
  // Verify sorted.
  for (size_t i = 1; i < keys.size(); ++i) {
    EXPECT_LT(keys[i - 1], keys[i]) << "Keys not sorted at index " << i;
  }
  // Verify expected first and last keys.
  EXPECT_EQ(keys[0], "l_comment");
  EXPECT_EQ(keys[15], "l_tax");
  // Verify shipdate is at index 11.
  EXPECT_EQ(keys[field_id::kShipdate], "l_shipdate");
}

TEST_F(VariantBlobTest, metadataSizeRoundTrip) {
  auto meta = buildLineitemMetadata();
  size_t computed = metadataSize(meta.data(), meta.size());
  EXPECT_EQ(computed, meta.size());
}

TEST_F(VariantBlobTest, lineitemBlobRootIsObject) {
  auto meta = buildLineitemMetadata();
  auto blob = buildLineitemBlob(
      meta,
      1, 2, 3, 4, // orderkey, partkey, suppkey, linenumber
      5.0, 6.0, 0.07, 0.08, // quantity, extendedprice, discount, tax
      "A", "F", // returnflag, linestatus
      8500, 8600, 8700, // shipdate, commitdate, receiptdate
      "DELIVER IN PERSON", "AIR", // shipinstruct, shipmode
      "test comment"); // comment

  EXPECT_TRUE(blobRootIsObject(blob.data(), blob.size()));
}

TEST_F(VariantBlobTest, extractShipdateFromLineitemBlob) {
  auto meta = buildLineitemMetadata();
  auto blob = buildLineitemBlob(
      meta,
      1, 2, 3, 4,
      5.0, 6.0, 0.07, 0.08,
      "A", "F",
      8500, 8600, 8700,
      "DELIVER IN PERSON", "AIR",
      "test comment");

  // Extract value portion.
  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  int32_t shipdate;
  EXPECT_TRUE(extractDateField(
      value, valueLen, field_id::kShipdate, shipdate));
  EXPECT_EQ(shipdate, 8500);

  int32_t commitdate;
  EXPECT_TRUE(extractDateField(
      value, valueLen, field_id::kCommitdate, commitdate));
  EXPECT_EQ(commitdate, 8600);

  int32_t receiptdate;
  EXPECT_TRUE(extractDateField(
      value, valueLen, field_id::kReceiptdate, receiptdate));
  EXPECT_EQ(receiptdate, 8700);
}

TEST_F(VariantBlobTest, extractDoublesFromLineitemBlob) {
  auto meta = buildLineitemMetadata();
  auto blob = buildLineitemBlob(
      meta,
      1, 2, 3, 4,
      25.0, 12345.67, 0.04, 0.02,
      "A", "F",
      8500, 8600, 8700,
      "DELIVER IN PERSON", "AIR",
      "test comment");

  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  double extendedprice;
  EXPECT_TRUE(extractDoubleField(
      value, valueLen, field_id::kExtendedprice, extendedprice));
  EXPECT_DOUBLE_EQ(extendedprice, 12345.67);

  double discount;
  EXPECT_TRUE(extractDoubleField(
      value, valueLen, field_id::kDiscount, discount));
  EXPECT_DOUBLE_EQ(discount, 0.04);

  double tax;
  EXPECT_TRUE(extractDoubleField(
      value, valueLen, field_id::kTax, tax));
  EXPECT_DOUBLE_EQ(tax, 0.02);

  double quantity;
  EXPECT_TRUE(extractDoubleField(
      value, valueLen, field_id::kQuantity, quantity));
  EXPECT_DOUBLE_EQ(quantity, 25.0);
}

TEST_F(VariantBlobTest, extractMultipleDoublesFromLineitemBlob) {
  auto meta = buildLineitemMetadata();
  auto blob = buildLineitemBlob(
      meta,
      1, 2, 3, 4,
      25.0, 12345.67, 0.04, 0.02,
      "A", "F",
      8500, 8600, 8700,
      "DELIVER IN PERSON", "AIR",
      "test comment");

  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  uint8_t targetFields[] = {
      field_id::kExtendedprice,
      field_id::kDiscount,
      field_id::kTax,
  };
  double outputs[3];
  EXPECT_TRUE(extractMultipleDoubleFields(
      value, valueLen, targetFields, 3, outputs));
  EXPECT_DOUBLE_EQ(outputs[0], 12345.67);
  EXPECT_DOUBLE_EQ(outputs[1], 0.04);
  EXPECT_DOUBLE_EQ(outputs[2], 0.02);
}

TEST_F(VariantBlobTest, tpchLineitemBlobRoundTrip) {
  // Generate a small batch of TPC-H lineitem data and encode/decode it.
  auto lineitem = tpch::genTpchLineItem(pool_.get(), 10);
  ASSERT_GT(lineitem->size(), 0);

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

  auto meta = buildLineitemMetadata();

  // Encode and verify first 5 rows.
  auto rowsToTest = std::min(static_cast<vector_size_t>(5), lineitem->size());
  for (vector_size_t i = 0; i < rowsToTest; ++i) {
    auto rfSv = returnflag->valueAt(i);
    auto lsSv = linestatus->valueAt(i);
    auto siSv = shipinstruct->valueAt(i);
    auto smSv = shipmode->valueAt(i);
    auto cmSv = comment->valueAt(i);

    auto blob = buildLineitemBlob(
        meta,
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

    EXPECT_TRUE(blobRootIsObject(blob.data(), blob.size()));

    // Extract value portion and verify fields.
    size_t metaLen = metadataSize(blob.data(), blob.size());
    const char* value = blob.data() + metaLen;
    size_t valueLen = blob.size() - metaLen;

    // Verify shipdate round-trip.
    int32_t extractedShipdate;
    ASSERT_TRUE(extractDateField(
        value, valueLen, field_id::kShipdate, extractedShipdate));
    EXPECT_EQ(extractedShipdate, shipdate->valueAt(i));

    // Verify extendedprice round-trip.
    double extractedPrice;
    ASSERT_TRUE(extractDoubleField(
        value, valueLen, field_id::kExtendedprice, extractedPrice));
    EXPECT_DOUBLE_EQ(extractedPrice, extendedprice->valueAt(i));

    // Verify discount round-trip.
    double extractedDiscount;
    ASSERT_TRUE(extractDoubleField(
        value, valueLen, field_id::kDiscount, extractedDiscount));
    EXPECT_DOUBLE_EQ(extractedDiscount, discount->valueAt(i));
  }
}

// ---------------------------------------------------------------------------
// AddressBook encoding / nested extraction tests.
// ---------------------------------------------------------------------------

TEST_F(VariantBlobTest, addressBookEncodeRoundTrip) {
  namespace ab = facebook::velox::variant_bench::addressbook;

  // Build a simple AddressBook blob and extract 'owner'.
  auto phone = ab::encodePhone("555-1234", "home");
  auto contact = ab::encodeContact("Alice", 30, {phone});
  auto blob = ab::buildAddressBookBlob("Bob", {contact});

  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  // Verify root is an Object.
  EXPECT_EQ(valueBasicType(static_cast<uint8_t>(value[0])), basic_type::kObject);

  // Extract 'owner' (field_id=1).
  std::string_view owner;
  EXPECT_TRUE(
      extractStringField(value, valueLen, ab::field_id::kOwner, owner));
  EXPECT_EQ(owner, "Bob");
}

TEST_F(VariantBlobTest, addressBookNestedExtract) {
  namespace ab = facebook::velox::variant_bench::addressbook;

  auto phone1 = ab::encodePhone("555-1111", "home");
  auto phone2 = ab::encodePhone("555-2222", "work");
  auto contact0 = ab::encodeContact("Alice", 30, {phone1, phone2});
  auto contact1 = ab::encodeContact("Charlie", 25, {phone1});
  auto blob = ab::buildAddressBookBlob("Bob", {contact0, contact1});

  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  // Extract contacts[0].name.
  const char* contactsArr;
  size_t contactsLen;
  ASSERT_TRUE(extractObjectField(
      value, valueLen, ab::field_id::kContacts, contactsArr, contactsLen));

  const char* contact0Data;
  size_t contact0Len;
  ASSERT_TRUE(
      extractArrayElement(contactsArr, contactsLen, 0, contact0Data, contact0Len));

  std::string_view name;
  ASSERT_TRUE(extractStringField(
      contact0Data, contact0Len, ab::field_id::kName, name));
  EXPECT_EQ(name, "Alice");

  // Extract contacts[0].age.
  int32_t age;
  ASSERT_TRUE(extractInt32Field(
      contact0Data, contact0Len, ab::field_id::kAge, age));
  EXPECT_EQ(age, 30);

  // Extract contacts[1].name.
  const char* contact1Data;
  size_t contact1Len;
  ASSERT_TRUE(
      extractArrayElement(contactsArr, contactsLen, 1, contact1Data, contact1Len));
  std::string_view name1;
  ASSERT_TRUE(extractStringField(
      contact1Data, contact1Len, ab::field_id::kName, name1));
  EXPECT_EQ(name1, "Charlie");
}

TEST_F(VariantBlobTest, addressBookDeepExtract) {
  namespace ab = facebook::velox::variant_bench::addressbook;

  auto phone0 = ab::encodePhone("555-DEEP", "mobile");
  auto phone1 = ab::encodePhone("555-OTHER", "home");
  auto contact = ab::encodeContact("Alice", 30, {phone0, phone1});
  auto blob = ab::buildAddressBookBlob("Bob", {contact});

  size_t metaLen = metadataSize(blob.data(), blob.size());
  const char* value = blob.data() + metaLen;
  size_t valueLen = blob.size() - metaLen;

  // Navigate: root -> contacts[0] -> phones[0] -> number.
  const char* contactsArr;
  size_t contactsLen;
  ASSERT_TRUE(extractObjectField(
      value, valueLen, ab::field_id::kContacts, contactsArr, contactsLen));

  const char* contact0;
  size_t contact0Len;
  ASSERT_TRUE(
      extractArrayElement(contactsArr, contactsLen, 0, contact0, contact0Len));

  const char* phonesArr;
  size_t phonesLen;
  ASSERT_TRUE(extractObjectField(
      contact0, contact0Len, ab::field_id::kPhones, phonesArr, phonesLen));

  const char* phone0Data;
  size_t phone0Len;
  ASSERT_TRUE(
      extractArrayElement(phonesArr, phonesLen, 0, phone0Data, phone0Len));

  std::string_view number;
  ASSERT_TRUE(extractStringField(
      phone0Data, phone0Len, ab::field_id::kNumber, number));
  EXPECT_EQ(number, "555-DEEP");

  // Also extract phones[1].type.
  const char* phone1Data;
  size_t phone1Len;
  ASSERT_TRUE(
      extractArrayElement(phonesArr, phonesLen, 1, phone1Data, phone1Len));

  std::string_view phoneType;
  ASSERT_TRUE(extractStringField(
      phone1Data, phone1Len, ab::field_id::kPhoneType, phoneType));
  EXPECT_EQ(phoneType, "home");
}

TEST_F(VariantBlobTest, arrayEncodingRoundTrip) {
  // Test standalone Array encoding.
  auto elem0 = encodeInt32Value(10);
  auto elem1 = encodeInt32Value(20);
  auto elem2 = encodeInt32Value(30);
  auto arr = encodeArray({elem0, elem1, elem2});

  // Parse header.
  uint32_t numElements;
  uint8_t offsetSize;
  const char* offsets;
  auto* elemData = parseArrayHeader(
      arr.data(), arr.size(), numElements, offsetSize, offsets);
  ASSERT_NE(elemData, nullptr);
  EXPECT_EQ(numElements, 3u);

  // Extract each element.
  for (uint32_t idx = 0; idx < 3; ++idx) {
    const char* elem;
    size_t elemLen;
    ASSERT_TRUE(extractArrayElement(arr.data(), arr.size(), idx, elem, elemLen));
    EXPECT_EQ(elemLen, 5u); // 1B header + 4B int32
    EXPECT_EQ(
        static_cast<int32_t>(readLE32(elem + 1)),
        static_cast<int32_t>(10 + idx * 10));
  }

  // Out-of-bounds access returns false.
  const char* elem;
  size_t elemLen;
  EXPECT_FALSE(extractArrayElement(arr.data(), arr.size(), 3, elem, elemLen));
}

TEST_F(VariantBlobTest, genericObjectEncodingRoundTrip) {
  // Encode a 3-field Object: field0=int32(42), field1=string("hello"),
  // field2=double(3.14).
  std::vector<std::string> children = {
      encodeInt32Value(42),
      encodeStringValue("hello"),
      encodeDoubleValue(3.14),
  };
  auto obj = encodeObject(children);

  // Extract each field.
  int32_t intVal;
  EXPECT_TRUE(extractInt32Field(obj.data(), obj.size(), 0, intVal));
  EXPECT_EQ(intVal, 42);

  std::string_view strVal;
  EXPECT_TRUE(extractStringField(obj.data(), obj.size(), 1, strVal));
  EXPECT_EQ(strVal, "hello");

  double dblVal;
  EXPECT_TRUE(extractDoubleField(obj.data(), obj.size(), 2, dblVal));
  EXPECT_DOUBLE_EQ(dblVal, 3.14);

  // Non-existent field_id.
  int32_t missing;
  EXPECT_FALSE(extractInt32Field(obj.data(), obj.size(), 99, missing));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
