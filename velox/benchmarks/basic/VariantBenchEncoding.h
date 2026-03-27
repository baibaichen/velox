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

// TPC-H lineitem helpers for the VARIANT format benchmark.
// Encoding/decoding primitives live in VariantEncoding.h; this file only
// provides lineitem-specific field definitions and blob builders.

#include "velox/functions/prestosql/types/VariantEncoding.h"

namespace facebook::velox::variant_bench {

using namespace variant_encoding;

// ---------------------------------------------------------------------------
// TPC-H lineitem field names (sorted alphabetically for the metadata dict).
// The sorted order determines field_id assignments.
// ---------------------------------------------------------------------------

// Lineitem field names sorted alphabetically.
inline const std::vector<std::string_view>& lineitemFieldNamesSorted() {
  static const std::vector<std::string_view> names = {
      "l_comment",       // 0
      "l_commitdate",    // 1
      "l_discount",      // 2
      "l_extendedprice", // 3
      "l_linenumber",    // 4
      "l_linestatus",    // 5
      "l_orderkey",      // 6
      "l_partkey",       // 7
      "l_quantity",      // 8
      "l_receiptdate",   // 9
      "l_returnflag",    // 10
      "l_shipdate",      // 11
      "l_shipinstruct",  // 12
      "l_shipmode",      // 13
      "l_suppkey",       // 14
      "l_tax",           // 15
  };
  return names;
}

// Field IDs for lineitem fields (index into sorted dictionary).
namespace field_id {
constexpr uint8_t kComment = 0;
constexpr uint8_t kCommitdate = 1;
constexpr uint8_t kDiscount = 2;
constexpr uint8_t kExtendedprice = 3;
constexpr uint8_t kLinenumber = 4;
constexpr uint8_t kLinestatus = 5;
constexpr uint8_t kOrderkey = 6;
constexpr uint8_t kPartkey = 7;
constexpr uint8_t kQuantity = 8;
constexpr uint8_t kReceiptdate = 9;
constexpr uint8_t kReturnflag = 10;
constexpr uint8_t kShipdate = 11;
constexpr uint8_t kShipinstruct = 12;
constexpr uint8_t kShipmode = 13;
constexpr uint8_t kSuppkey = 14;
constexpr uint8_t kTax = 15;
constexpr uint8_t kNumFields = 16;
} // namespace field_id

// Build the lineitem metadata dictionary.
inline std::string buildLineitemMetadata() {
  return buildMetadata(lineitemFieldNamesSorted());
}

// ---------------------------------------------------------------------------
// Object encoding for lineitem rows.
// ---------------------------------------------------------------------------

// Encode a lineitem row as a VARIANT Object value blob.
// Fields are passed in column order (matching genTpchLineItem schema).
// The Object's field_ids are emitted in sorted order (ascending).
//
// Column order from genTpchLineItem:
//   0: l_orderkey(BIGINT), 1: l_partkey(BIGINT), 2: l_suppkey(BIGINT),
//   3: l_linenumber(INTEGER), 4: l_quantity(DOUBLE),
//   5: l_extendedprice(DOUBLE), 6: l_discount(DOUBLE), 7: l_tax(DOUBLE),
//   8: l_returnflag(VARCHAR), 9: l_linestatus(VARCHAR),
//   10: l_shipdate(DATE), 11: l_commitdate(DATE), 12: l_receiptdate(DATE),
//   13: l_shipinstruct(VARCHAR), 14: l_shipmode(VARCHAR),
//   15: l_comment(VARCHAR)
//
// Sorted field_id order (for Object encoding):
//   0: l_comment(col 15), 1: l_commitdate(col 11), 2: l_discount(col 6),
//   3: l_extendedprice(col 5), 4: l_linenumber(col 3),
//   5: l_linestatus(col 9), 6: l_orderkey(col 0), 7: l_partkey(col 1),
//   8: l_quantity(col 4), 9: l_receiptdate(col 12),
//   10: l_returnflag(col 8), 11: l_shipdate(col 10),
//   12: l_shipinstruct(col 13), 13: l_shipmode(col 14),
//   14: l_suppkey(col 2), 15: l_tax(col 7)
inline std::string encodeLineitemObject(
    int64_t orderkey,
    int64_t partkey,
    int64_t suppkey,
    int32_t linenumber,
    double quantity,
    double extendedprice,
    double discount,
    double tax,
    std::string_view returnflag,
    std::string_view linestatus,
    int32_t shipdate,
    int32_t commitdate,
    int32_t receiptdate,
    std::string_view shipinstruct,
    std::string_view shipmode,
    std::string_view comment) {
  // Encode each child value in sorted field_id order.
  std::string childValues[field_id::kNumFields];
  childValues[field_id::kComment] = encodeStringValue(comment);
  childValues[field_id::kCommitdate] = encodeDateValue(commitdate);
  childValues[field_id::kDiscount] = encodeDoubleValue(discount);
  childValues[field_id::kExtendedprice] = encodeDoubleValue(extendedprice);
  childValues[field_id::kLinenumber] = encodeInt32Value(linenumber);
  childValues[field_id::kLinestatus] = encodeStringValue(linestatus);
  childValues[field_id::kOrderkey] = encodeInt64Value(orderkey);
  childValues[field_id::kPartkey] = encodeInt64Value(partkey);
  childValues[field_id::kQuantity] = encodeDoubleValue(quantity);
  childValues[field_id::kReceiptdate] = encodeDateValue(receiptdate);
  childValues[field_id::kReturnflag] = encodeStringValue(returnflag);
  childValues[field_id::kShipdate] = encodeDateValue(shipdate);
  childValues[field_id::kShipinstruct] = encodeStringValue(shipinstruct);
  childValues[field_id::kShipmode] = encodeStringValue(shipmode);
  childValues[field_id::kSuppkey] = encodeInt64Value(suppkey);
  childValues[field_id::kTax] = encodeDoubleValue(tax);

  // Compute total child data size.
  size_t totalChildBytes = 0;
  for (const auto& cv : childValues) {
    totalChildBytes += cv.size();
  }

  // Choose offset_size: need to address totalChildBytes range.
  // Use 2B offsets (up to 65535) which is enough for lineitem (~200B).
  uint8_t offsetSizeMinus1 = 1; // 2-byte offsets
  uint8_t fieldIdSizeMinus1 = 0; // 1-byte field IDs (16 < 256)
  // Use is_large=1 (4-byte count) unconditionally for simplicity even
  // though 16 fields would fit in 1 byte. This intentionally diverges from
  // encodeObjectWithFieldIds (which selects dynamically) to keep the
  // hand-rolled benchmark encoder straightforward.
  uint8_t isLarge = 1;

  // Object header byte.
  uint8_t objectHeader = (isLarge << 6) | (fieldIdSizeMinus1 << 4) |
      (offsetSizeMinus1 << 2) | basic_type::kObject;

  std::string val;
  val.push_back(static_cast<char>(objectHeader));

  // num_elements (4B LE because is_large=1).
  appendLE32(val, field_id::kNumFields);

  // field_ids: 16 x 1B, sorted 0..15.
  for (uint8_t fid = 0; fid < field_id::kNumFields; ++fid) {
    val.push_back(static_cast<char>(fid));
  }

  // offsets: 17 x 2B LE (num_elements + 1).
  uint16_t offset = 0;
  for (uint8_t fid = 0; fid < field_id::kNumFields; ++fid) {
    appendLE16(val, offset);
    offset += static_cast<uint16_t>(childValues[fid].size());
  }
  appendLE16(val, offset); // sentinel offset

  // child values.
  for (const auto& cv : childValues) {
    val.append(cv);
  }

  return val;
}

// Build a complete lineitem blob: metadata + Object value.
inline std::string buildLineitemBlob(
    const std::string& metadata,
    int64_t orderkey,
    int64_t partkey,
    int64_t suppkey,
    int32_t linenumber,
    double quantity,
    double extendedprice,
    double discount,
    double tax,
    std::string_view returnflag,
    std::string_view linestatus,
    int32_t shipdate,
    int32_t commitdate,
    int32_t receiptdate,
    std::string_view shipinstruct,
    std::string_view shipmode,
    std::string_view comment) {
  std::string blob = metadata;
  blob.append(encodeLineitemObject(
      orderkey,
      partkey,
      suppkey,
      linenumber,
      quantity,
      extendedprice,
      discount,
      tax,
      returnflag,
      linestatus,
      shipdate,
      commitdate,
      receiptdate,
      shipinstruct,
      shipmode,
      comment));
  return blob;
}

// ---------------------------------------------------------------------------
// Blob helpers.
// ---------------------------------------------------------------------------

// Check if the root value is an Object.
inline bool blobRootIsObject(const char* data, size_t size) {
  size_t metaLen = metadataSize(data, size);
  if (metaLen >= size) {
    return false;
  }
  return (static_cast<uint8_t>(data[metaLen]) & 0x03) == basic_type::kObject;
}

// Decode a child value from a value blob as a string. Returns empty string
// on failure. Delegates to variant_encoding::decodeStringValue.
inline std::string decodeChildValueAsString(
    const char* childValue,
    size_t childLen) {
  std::string_view sv;
  if (!decodeStringValue(childValue, childLen, sv)) {
    return {};
  }
  return std::string(sv);
}

// ---------------------------------------------------------------------------
// AddressBook nested type helpers.
// ---------------------------------------------------------------------------

namespace addressbook {

// Field IDs (sorted alphabetically within each Object type).
// AddressBook: contacts=0, owner=1
// Contact: age=0, name=1, phones=2
// Phone: number=0, type=1
namespace field_id {
constexpr uint8_t kContacts = 0;
constexpr uint8_t kOwner = 1;
constexpr uint8_t kAge = 0;
constexpr uint8_t kName = 1;
constexpr uint8_t kPhones = 2;
constexpr uint8_t kNumber = 0;
constexpr uint8_t kPhoneType = 1;
} // namespace field_id

// Build metadata for AddressBook (contacts, owner).
inline std::string buildAddressBookMetadata() {
  return buildMetadata({"contacts", "owner"});
}

// Build metadata for Contact (age, name, phones).
inline std::string buildContactMetadata() {
  return buildMetadata({"age", "name", "phones"});
}

// Build metadata for Phone (number, type).
inline std::string buildPhoneMetadata() {
  return buildMetadata({"number", "type"});
}

// Encode a Phone Object value: {number: VARCHAR, type: VARCHAR}.
inline std::string encodePhone(
    std::string_view number,
    std::string_view type) {
  std::vector<std::string> children = {
      encodeStringValue(number),
      encodeStringValue(type),
  };
  return encodeObject(children);
}

// Encode a Contact Object: {age: INT32, name: VARCHAR, phones: ARRAY<Phone>}.
// phones should be pre-encoded Phone Object values.
inline std::string encodeContact(
    std::string_view name,
    int32_t age,
    const std::vector<std::string>& phones) {
  std::vector<std::string> children = {
      encodeInt32Value(age),
      encodeStringValue(name),
      encodeArray(phones),
  };
  return encodeObject(children);
}

// Encode an AddressBook Object: {contacts: ARRAY<Contact>, owner: VARCHAR}.
// contacts should be pre-encoded Contact Object values.
inline std::string encodeAddressBook(
    std::string_view owner,
    const std::vector<std::string>& contacts) {
  std::vector<std::string> children = {
      encodeArray(contacts),
      encodeStringValue(owner),
  };
  return encodeObject(children);
}

// Build a complete AddressBook blob: metadata + Object value.
inline std::string buildAddressBookBlob(
    std::string_view owner,
    const std::vector<std::string>& contacts) {
  std::string blob = buildAddressBookMetadata();
  blob.append(encodeAddressBook(owner, contacts));
  return blob;
}

} // namespace addressbook

} // namespace facebook::velox::variant_bench
