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

/// Encode and decode Parquet VARIANT binary blobs following the spec.
/// Shared by the VARIANT type prototype and the format benchmark.

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::variant_encoding {

// ---------------------------------------------------------------------------
// Parquet VARIANT header byte layout.
// ---------------------------------------------------------------------------

namespace basic_type {
constexpr uint8_t kPrimitive = 0;
constexpr uint8_t kShortString = 1;
constexpr uint8_t kObject = 2;
constexpr uint8_t kArray = 3;
} // namespace basic_type

namespace primitive_type {
constexpr uint8_t kNull = 0;
constexpr uint8_t kTrue = 1;
constexpr uint8_t kFalse = 2;
constexpr uint8_t kInt8 = 3;
constexpr uint8_t kInt16 = 4;
constexpr uint8_t kInt32 = 5;
constexpr uint8_t kInt64 = 6;
constexpr uint8_t kDouble = 7;
constexpr uint8_t kDecimal4 = 8;
constexpr uint8_t kDecimal8 = 9;
constexpr uint8_t kDecimal16 = 10;
constexpr uint8_t kDate = 11;
constexpr uint8_t kTimestamp = 12;
constexpr uint8_t kTimestampNtz = 13;
constexpr uint8_t kFloat = 14;
constexpr uint8_t kBinary = 15;
constexpr uint8_t kString = 16;
} // namespace primitive_type

namespace header_byte {
constexpr uint8_t kNull =
    (primitive_type::kNull << 2) | basic_type::kPrimitive;
constexpr uint8_t kInt32 =
    (primitive_type::kInt32 << 2) | basic_type::kPrimitive;
constexpr uint8_t kInt64 =
    (primitive_type::kInt64 << 2) | basic_type::kPrimitive;
constexpr uint8_t kDouble =
    (primitive_type::kDouble << 2) | basic_type::kPrimitive;
constexpr uint8_t kDate =
    (primitive_type::kDate << 2) | basic_type::kPrimitive;
constexpr uint8_t kString =
    (primitive_type::kString << 2) | basic_type::kPrimitive;
} // namespace header_byte

// ---------------------------------------------------------------------------
// Little-endian helpers.
// ---------------------------------------------------------------------------

inline void appendLE16(std::string& buf, uint16_t v) {
  buf.push_back(static_cast<char>(v & 0xFF));
  buf.push_back(static_cast<char>((v >> 8) & 0xFF));
}

inline void appendLE32(std::string& buf, uint32_t v) {
  buf.push_back(static_cast<char>(v & 0xFF));
  buf.push_back(static_cast<char>((v >> 8) & 0xFF));
  buf.push_back(static_cast<char>((v >> 16) & 0xFF));
  buf.push_back(static_cast<char>((v >> 24) & 0xFF));
}

inline void appendLE64(std::string& buf, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    buf.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

/// Append a value using 1, 2, or 4 bytes in little-endian order.
inline void appendLEOffset(std::string& buf, uint32_t v, uint8_t offsetSize) {
  if (offsetSize == 1) {
    buf.push_back(static_cast<char>(v & 0xFF));
  } else if (offsetSize == 2) {
    appendLE16(buf, static_cast<uint16_t>(v));
  } else {
    appendLE32(buf, v);
  }
}

inline uint16_t readLE16(const char* p) {
  uint16_t v;
  std::memcpy(&v, p, 2);
  return v;
}

inline uint32_t readLE32(const char* p) {
  uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}

inline int64_t readLE64(const char* p) {
  int64_t v;
  std::memcpy(&v, p, 8);
  return v;
}

inline double readLEDouble(const char* p) {
  double v;
  std::memcpy(&v, p, 8);
  return v;
}

// ---------------------------------------------------------------------------
// Value encoding.
// ---------------------------------------------------------------------------

inline std::string encodeNullValue() {
  return {static_cast<char>(header_byte::kNull)};
}

inline std::string encodeInt32Value(int32_t v) {
  std::string val;
  val.reserve(5);
  val.push_back(static_cast<char>(header_byte::kInt32));
  appendLE32(val, static_cast<uint32_t>(v));
  return val;
}

inline std::string encodeInt64Value(int64_t v) {
  std::string val;
  val.reserve(9);
  val.push_back(static_cast<char>(header_byte::kInt64));
  appendLE64(val, static_cast<uint64_t>(v));
  return val;
}

inline std::string encodeDoubleValue(double v) {
  std::string val;
  val.reserve(9);
  val.push_back(static_cast<char>(header_byte::kDouble));
  uint64_t bits;
  std::memcpy(&bits, &v, 8);
  appendLE64(val, bits);
  return val;
}

inline std::string encodeDateValue(int32_t days) {
  std::string val;
  val.reserve(5);
  val.push_back(static_cast<char>(header_byte::kDate));
  appendLE32(val, static_cast<uint32_t>(days));
  return val;
}

inline std::string encodeShortString(std::string_view s) {
  VELOX_CHECK_LE(s.size(), 63, "ShortString maximum length is 63 bytes");
  std::string val;
  val.reserve(1 + s.size());
  val.push_back(static_cast<char>((s.size() << 2) | basic_type::kShortString));
  val.append(s.data(), s.size());
  return val;
}

inline std::string encodeLongString(std::string_view s) {
  std::string val;
  val.reserve(5 + s.size());
  val.push_back(static_cast<char>(header_byte::kString));
  appendLE32(val, static_cast<uint32_t>(s.size()));
  val.append(s.data(), s.size());
  return val;
}

inline std::string encodeStringValue(std::string_view s) {
  if (s.size() <= 63) {
    return encodeShortString(s);
  }
  return encodeLongString(s);
}

// ---------------------------------------------------------------------------
// Metadata encoding and parsing.
// ---------------------------------------------------------------------------

/// Build metadata for a sorted list of field names.
std::string buildMetadata(const std::vector<std::string_view>& keys);

/// Compute metadata size from a blob.
size_t metadataSize(const char* data, size_t size);

/// Look up a field name in a metadata dictionary. Returns the field_id
/// (0-based index) or -1 if not found. Assumes sorted dictionary.
int32_t lookupFieldIdByName(
    const char* metadata,
    size_t metadataLen,
    std::string_view fieldName);

// ---------------------------------------------------------------------------
// Object value parsing.
// ---------------------------------------------------------------------------

inline uint8_t valueBasicType(uint8_t headerByte) {
  return headerByte & 0x03;
}

inline uint8_t valuePrimitiveType(uint8_t headerByte) {
  return headerByte >> 2;
}

/// Parse an Object value header. Returns pointer to child values data,
/// or nullptr on failure.
const char* parseObjectHeader(
    const char* value,
    size_t valueLen,
    uint32_t& numFieldsOut,
    uint8_t& fieldIdSizeOut,
    uint8_t& offsetSizeOut,
    const char*& fieldIdsOut,
    const char*& offsetsOut);

inline uint32_t readFieldId(
    const char* fieldIds,
    uint8_t fieldIdSize,
    uint32_t idx) {
  if (fieldIdSize == 1) {
    return static_cast<uint8_t>(fieldIds[idx]);
  } else if (fieldIdSize == 2) {
    return readLE16(fieldIds + idx * 2);
  }
  return readLE32(fieldIds + idx * 4);
}

inline uint32_t readOffset(
    const char* offsets,
    uint8_t offsetSize,
    uint32_t idx) {
  if (offsetSize == 1) {
    return static_cast<uint8_t>(offsets[idx]);
  } else if (offsetSize == 2) {
    return readLE16(offsets + idx * 2);
  }
  return readLE32(offsets + idx * 4);
}

/// Find the byte range [startOut, endOut) within child values data for a
/// given field_id in a sorted Object. Uses binary search.
bool findFieldOffset(
    const char* fieldIds,
    uint8_t fieldIdSize,
    const char* offsets,
    uint8_t offsetSize,
    uint32_t numFields,
    uint32_t targetFieldId,
    uint32_t& startOut,
    uint32_t& endOut);

// ---------------------------------------------------------------------------
// Typed field extraction from an Object value blob.
// ---------------------------------------------------------------------------

/// Locate a field within an Object value blob. Returns a pointer to the child
/// value and its length, or false if the field is not found or the blob is
/// malformed. This is the shared preamble for all typed extract* functions.
bool locateObjectField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    const char*& childValueOut,
    size_t& childLenOut);

/// Extract a DATE (int32) field.
bool extractDateField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    int32_t& out);

/// Extract a DOUBLE field.
bool extractDoubleField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    double& out);

/// Extract an INT32 field.
bool extractInt32Field(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    int32_t& out);

/// Extract an INT64 field.
bool extractInt64Field(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    int64_t& out);

/// Decode a string from a value blob (short or long encoding).
bool decodeStringValue(
    const char* childValue,
    size_t childLen,
    std::string_view& out);

/// Extract a string field (short or long encoding). Returns false on failure.
bool extractStringField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    std::string_view& out);

/// Extract multiple DOUBLE fields from a value blob with a single header
/// parse. fieldIds must be in ascending order. Returns false on any failure.
bool extractMultipleDoubleFields(
    const char* value,
    size_t valueLen,
    const uint8_t* targetFieldIds,
    size_t numTargets,
    double* outputs);

// ---------------------------------------------------------------------------
// Generic Object encoding.
// ---------------------------------------------------------------------------

/// Encode a VARIANT Object from pre-encoded child values whose field_ids are
/// 0, 1, ..., n-1 (sorted ascending). Used when the caller has already
/// encoded each child in field_id order.
std::string encodeObject(const std::vector<std::string>& childValues);

/// Encode a VARIANT Object from pre-encoded child values with explicit
/// field_ids. The fieldIds and childValues vectors must be the same size, and
/// fieldIds must already be sorted ascending.
std::string encodeObjectWithFieldIds(
    const std::vector<uint32_t>& fieldIds,
    const std::vector<std::string>& childValues);

// ---------------------------------------------------------------------------
// Array encoding and parsing.
// ---------------------------------------------------------------------------

/// Encode a VARIANT Array from pre-encoded element values.
std::string encodeArray(const std::vector<std::string>& elementValues);

/// Parse an Array value header. Returns pointer to element values data,
/// or nullptr on failure.
const char* parseArrayHeader(
    const char* value,
    size_t valueLen,
    uint32_t& numElementsOut,
    uint8_t& offsetSizeOut,
    const char*& offsetsOut);

/// Extract the Nth element from an Array value as a sub-blob.
bool extractArrayElement(
    const char* value,
    size_t valueLen,
    uint32_t index,
    const char*& elementOut,
    size_t& elementLenOut);

/// Extract an Object field as a raw sub-blob (works for any value type).
bool extractObjectField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    const char*& fieldOut,
    size_t& fieldLenOut);

} // namespace facebook::velox::variant_encoding
