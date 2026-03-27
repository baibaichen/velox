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

#include "velox/functions/prestosql/types/VariantEncoding.h"

#include <algorithm>
#include <numeric>

namespace facebook::velox::variant_encoding {

namespace {
// Compute the offset-size-minus-1 value per the Parquet VARIANT spec:
// 0 → 1-byte offsets (max 255), 1 → 2-byte (max 65535), 3 → 4-byte.
// Note: value 2 is not used (no 3-byte offsets in the spec).
uint8_t computeOffsetSizeMinus1(size_t totalBytes) {
  if (totalBytes > 65'535) {
    return 3;
  }
  if (totalBytes > 255) {
    return 1;
  }
  return 0;
}
} // namespace

std::string buildMetadata(const std::vector<std::string_view>& keys) {
  size_t dictDataLen = 0;
  for (auto& k : keys) {
    dictDataLen += k.size();
  }

  uint8_t offsetSizeMinus1 = computeOffsetSizeMinus1(dictDataLen);
  uint8_t offsetSize = offsetSizeMinus1 + 1;

  uint8_t header = 0;
  header |= (1 << 1); // sorted ascending
  header |= (offsetSizeMinus1 << 6);

  std::string meta;
  meta.reserve(1 + 4 + (keys.size() + 1) * offsetSize + dictDataLen);
  meta.push_back(static_cast<char>(header));
  appendLE32(meta, static_cast<uint32_t>(keys.size()));

  uint32_t offset = 0;
  for (size_t i = 0; i <= keys.size(); ++i) {
    appendLEOffset(meta, offset, offsetSize);
    if (i < keys.size()) {
      offset += static_cast<uint32_t>(keys[i].size());
    }
  }

  for (auto& k : keys) {
    meta.append(k.data(), k.size());
  }

  return meta;
}

size_t metadataSize(const char* data, size_t size) {
  if (size < 5) {
    return size;
  }
  uint8_t header = static_cast<uint8_t>(data[0]);
  uint8_t offsetSize = ((header >> 6) & 0x03) + 1;
  uint32_t dictSize = readLE32(data + 1);

  size_t offsetsEnd = 5 + static_cast<size_t>(dictSize + 1) * offsetSize;
  if (offsetsEnd > size) {
    return size;
  }

  const char* offsetsPtr = data + 5;
  uint32_t dictDataLen = readOffset(offsetsPtr, offsetSize, dictSize);

  return offsetsEnd + dictDataLen;
}

int32_t lookupFieldIdByName(
    const char* metadata,
    size_t metadataLen,
    std::string_view fieldName) {
  if (metadataLen < 5) {
    return -1;
  }
  uint8_t header = static_cast<uint8_t>(metadata[0]);
  uint8_t offsetSize = ((header >> 6) & 0x03) + 1;
  uint32_t dictSize = readLE32(metadata + 1);

  size_t offsetsStart = 5;
  size_t offsetsEnd =
      offsetsStart + static_cast<size_t>(dictSize + 1) * offsetSize;
  if (offsetsEnd > metadataLen) {
    return -1;
  }

  const char* offsetsPtr = metadata + offsetsStart;
  const char* dictData = metadata + offsetsEnd;

  uint32_t lo = 0;
  uint32_t hi = dictSize;
  while (lo < hi) {
    uint32_t mid = lo + (hi - lo) / 2;
    uint32_t keyStart = readOffset(offsetsPtr, offsetSize, mid);
    uint32_t keyEnd = readOffset(offsetsPtr, offsetSize, mid + 1);
    std::string_view key(dictData + keyStart, keyEnd - keyStart);
    int cmp = key.compare(fieldName);
    if (cmp < 0) {
      lo = mid + 1;
    } else if (cmp > 0) {
      hi = mid;
    } else {
      return static_cast<int32_t>(mid);
    }
  }
  return -1;
}

const char* parseObjectHeader(
    const char* value,
    size_t valueLen,
    uint32_t& numFieldsOut,
    uint8_t& fieldIdSizeOut,
    uint8_t& offsetSizeOut,
    const char*& fieldIdsOut,
    const char*& offsetsOut) {
  if (valueLen < 1) {
    return nullptr;
  }
  uint8_t hdr = static_cast<uint8_t>(value[0]);
  if ((hdr & 0x03) != basic_type::kObject) {
    return nullptr;
  }

  offsetSizeOut = ((hdr >> 2) & 0x03) + 1;
  fieldIdSizeOut = ((hdr >> 4) & 0x03) + 1;
  bool isLarge = (hdr >> 6) & 0x01;

  size_t pos = 1;
  if (isLarge) {
    if (pos + 4 > valueLen) {
      return nullptr;
    }
    numFieldsOut = readLE32(value + pos);
    pos += 4;
  } else {
    if (pos + 1 > valueLen) {
      return nullptr;
    }
    numFieldsOut = static_cast<uint8_t>(value[pos]);
    pos += 1;
  }

  fieldIdsOut = value + pos;
  pos += static_cast<size_t>(numFieldsOut) * fieldIdSizeOut;

  offsetsOut = value + pos;
  pos += static_cast<size_t>(numFieldsOut + 1) * offsetSizeOut;

  if (pos > valueLen) {
    return nullptr;
  }
  return value + pos;
}

bool findFieldOffset(
    const char* fieldIds,
    uint8_t fieldIdSize,
    const char* offsets,
    uint8_t offsetSize,
    uint32_t numFields,
    uint32_t targetFieldId,
    uint32_t& startOut,
    uint32_t& endOut) {
  uint32_t lo = 0;
  uint32_t hi = numFields;
  while (lo < hi) {
    uint32_t mid = lo + (hi - lo) / 2;
    uint32_t fid = readFieldId(fieldIds, fieldIdSize, mid);
    if (fid < targetFieldId) {
      lo = mid + 1;
    } else if (fid > targetFieldId) {
      hi = mid;
    } else {
      startOut = readOffset(offsets, offsetSize, mid);
      endOut = readOffset(offsets, offsetSize, mid + 1);
      return true;
    }
  }
  return false;
}

bool locateObjectField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    const char*& childValueOut,
    size_t& childLenOut) {
  uint32_t numFields;
  uint8_t fieldIdSize;
  uint8_t offsetSize;
  const char* fieldIdsPtr;
  const char* offsets;
  const char* childData = parseObjectHeader(
      value, valueLen, numFields, fieldIdSize, offsetSize, fieldIdsPtr, offsets);
  if (!childData) {
    return false;
  }

  uint32_t start;
  uint32_t end;
  if (!findFieldOffset(
          fieldIdsPtr,
          fieldIdSize,
          offsets,
          offsetSize,
          numFields,
          targetFieldId,
          start,
          end)) {
    return false;
  }

  size_t payloadSize = valueLen - static_cast<size_t>(childData - value);
  if (start > end || end > payloadSize) {
    return false;
  }

  childValueOut = childData + start;
  childLenOut = end - start;
  return true;
}

bool extractDateField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    int32_t& out) {
  const char* childValue;
  size_t childLen;
  if (!locateObjectField(value, valueLen, targetFieldId, childValue, childLen)) {
    return false;
  }
  if (childLen < 5) {
    return false;
  }
  if (static_cast<uint8_t>(childValue[0]) != header_byte::kDate) {
    return false;
  }
  out = static_cast<int32_t>(readLE32(childValue + 1));
  return true;
}

bool extractDoubleField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    double& out) {
  const char* childValue;
  size_t childLen;
  if (!locateObjectField(value, valueLen, targetFieldId, childValue, childLen)) {
    return false;
  }
  if (childLen < 9) {
    return false;
  }
  if (static_cast<uint8_t>(childValue[0]) != header_byte::kDouble) {
    return false;
  }
  out = readLEDouble(childValue + 1);
  return true;
}

bool extractInt32Field(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    int32_t& out) {
  const char* childValue;
  size_t childLen;
  if (!locateObjectField(value, valueLen, targetFieldId, childValue, childLen)) {
    return false;
  }
  if (childLen < 5) {
    return false;
  }
  if (static_cast<uint8_t>(childValue[0]) != header_byte::kInt32) {
    return false;
  }
  out = static_cast<int32_t>(readLE32(childValue + 1));
  return true;
}

bool decodeStringValue(
    const char* childValue,
    size_t childLen,
    std::string_view& out) {
  if (childLen < 1) {
    return false;
  }
  uint8_t hdr = static_cast<uint8_t>(childValue[0]);
  uint8_t bt = hdr & 0x03;

  if (bt == basic_type::kShortString) {
    uint8_t strLen = hdr >> 2;
    if (1 + strLen > childLen) {
      return false;
    }
    out = std::string_view(childValue + 1, strLen);
    return true;
  }

  if (bt == basic_type::kPrimitive && (hdr >> 2) == primitive_type::kString) {
    if (childLen < 5) {
      return false;
    }
    uint32_t strLen = readLE32(childValue + 1);
    if (5 + strLen > childLen) {
      return false;
    }
    out = std::string_view(childValue + 5, strLen);
    return true;
  }

  return false;
}

bool extractStringField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    std::string_view& out) {
  const char* childValue;
  size_t childLen;
  if (!locateObjectField(value, valueLen, targetFieldId, childValue, childLen)) {
    return false;
  }
  return decodeStringValue(childValue, childLen, out);
}

bool extractMultipleDoubleFields(
    const char* value,
    size_t valueLen,
    const uint8_t* targetFieldIds,
    size_t numTargets,
    double* outputs) {
  uint32_t numFields;
  uint8_t fieldIdSize;
  uint8_t offsetSize;
  const char* fieldIdsArr;
  const char* offsets;
  const char* childData = parseObjectHeader(
      value,
      valueLen,
      numFields,
      fieldIdSize,
      offsetSize,
      fieldIdsArr,
      offsets);
  if (!childData) {
    return false;
  }

  size_t payloadSize = valueLen - static_cast<size_t>(childData - value);

  for (size_t t = 0; t < numTargets; ++t) {
    uint32_t start;
    uint32_t end;
    if (!findFieldOffset(
            fieldIdsArr,
            fieldIdSize,
            offsets,
            offsetSize,
            numFields,
            targetFieldIds[t],
            start,
            end)) {
      return false;
    }
    if (start > end || end > payloadSize) {
      return false;
    }
    const char* childValue = childData + start;
    size_t childLen = end - start;
    if (childLen < 9) {
      return false;
    }
    if (static_cast<uint8_t>(childValue[0]) != header_byte::kDouble) {
      return false;
    }
    outputs[t] = readLEDouble(childValue + 1);
  }
  return true;
}

std::string encodeObject(const std::vector<std::string>& childValues) {
  std::vector<uint32_t> fieldIds(childValues.size());
  std::iota(fieldIds.begin(), fieldIds.end(), 0);
  return encodeObjectWithFieldIds(fieldIds, childValues);
}

std::string encodeObjectWithFieldIds(
    const std::vector<uint32_t>& fieldIds,
    const std::vector<std::string>& childValues) {
  VELOX_CHECK_EQ(fieldIds.size(), childValues.size());
  VELOX_DCHECK(
      std::is_sorted(fieldIds.begin(), fieldIds.end()),
      "fieldIds must be sorted ascending");
  uint32_t numFields = static_cast<uint32_t>(childValues.size());

  size_t totalChildBytes = 0;
  for (const auto& cv : childValues) {
    totalChildBytes += cv.size();
  }

  uint8_t offsetSizeMinus1 = computeOffsetSizeMinus1(totalChildBytes);
  uint8_t offsetSize = offsetSizeMinus1 + 1;

  uint32_t maxFieldId = fieldIds.empty() ? 0 : fieldIds.back();
  uint8_t fieldIdSizeMinus1 = 0;
  if (maxFieldId > 65'535) {
    fieldIdSizeMinus1 = 3;
  } else if (maxFieldId > 255) {
    fieldIdSizeMinus1 = 1;
  }

  uint8_t isLarge = numFields > 255 ? 1 : 0;

  uint8_t objectHeader = (isLarge << 6) | (fieldIdSizeMinus1 << 4) |
      (offsetSizeMinus1 << 2) | basic_type::kObject;

  uint8_t fieldIdSize = fieldIdSizeMinus1 + 1;
  size_t countSize = isLarge ? 4 : 1;
  size_t totalSize = 1 + countSize + numFields * fieldIdSize +
      (numFields + 1) * offsetSize + totalChildBytes;

  std::string val;
  val.reserve(totalSize);
  val.push_back(static_cast<char>(objectHeader));

  if (isLarge) {
    appendLE32(val, numFields);
  } else {
    val.push_back(static_cast<char>(numFields & 0xFF));
  }

  for (auto fid : fieldIds) {
    appendLEOffset(val, fid, fieldIdSize);
  }

  uint32_t offset = 0;
  for (uint32_t i = 0; i < numFields; ++i) {
    appendLEOffset(val, offset, offsetSize);
    offset += static_cast<uint32_t>(childValues[i].size());
  }
  appendLEOffset(val, offset, offsetSize);

  for (const auto& cv : childValues) {
    val.append(cv);
  }

  return val;
}

std::string encodeArray(const std::vector<std::string>& elementValues) {
  uint32_t numElements = static_cast<uint32_t>(elementValues.size());

  size_t totalBytes = 0;
  for (const auto& ev : elementValues) {
    totalBytes += ev.size();
  }

  uint8_t offsetSizeMinus1 = computeOffsetSizeMinus1(totalBytes);
  uint8_t offsetSize = offsetSizeMinus1 + 1;

  uint8_t isLarge = numElements > 255 ? 1 : 0;

  uint8_t arrayHeader =
      (isLarge << 4) | (offsetSizeMinus1 << 2) | basic_type::kArray;

  size_t countSize = isLarge ? 4 : 1;
  size_t totalSize =
      1 + countSize + (numElements + 1) * offsetSize + totalBytes;

  std::string val;
  val.reserve(totalSize);
  val.push_back(static_cast<char>(arrayHeader));

  if (isLarge) {
    appendLE32(val, numElements);
  } else {
    val.push_back(static_cast<char>(numElements & 0xFF));
  }

  uint32_t offset = 0;
  for (uint32_t i = 0; i < numElements; ++i) {
    appendLEOffset(val, offset, offsetSize);
    offset += static_cast<uint32_t>(elementValues[i].size());
  }
  appendLEOffset(val, offset, offsetSize);

  for (const auto& ev : elementValues) {
    val.append(ev);
  }

  return val;
}

const char* parseArrayHeader(
    const char* value,
    size_t valueLen,
    uint32_t& numElementsOut,
    uint8_t& offsetSizeOut,
    const char*& offsetsOut) {
  if (valueLen < 1) {
    return nullptr;
  }
  uint8_t hdr = static_cast<uint8_t>(value[0]);
  if ((hdr & 0x03) != basic_type::kArray) {
    return nullptr;
  }

  offsetSizeOut = ((hdr >> 2) & 0x03) + 1;
  bool isLarge = (hdr >> 4) & 0x01;

  size_t pos = 1;
  if (isLarge) {
    if (pos + 4 > valueLen) {
      return nullptr;
    }
    numElementsOut = readLE32(value + pos);
    pos += 4;
  } else {
    if (pos + 1 > valueLen) {
      return nullptr;
    }
    numElementsOut = static_cast<uint8_t>(value[pos]);
    pos += 1;
  }

  offsetsOut = value + pos;
  pos += static_cast<size_t>(numElementsOut + 1) * offsetSizeOut;

  if (pos > valueLen) {
    return nullptr;
  }
  return value + pos;
}

bool extractArrayElement(
    const char* value,
    size_t valueLen,
    uint32_t index,
    const char*& elementOut,
    size_t& elementLenOut) {
  uint32_t numElements;
  uint8_t offsetSize;
  const char* offsets;
  const char* elemData =
      parseArrayHeader(value, valueLen, numElements, offsetSize, offsets);
  if (!elemData || index >= numElements) {
    return false;
  }

  uint32_t start = readOffset(offsets, offsetSize, index);
  uint32_t end = readOffset(offsets, offsetSize, index + 1);
  size_t payloadSize = valueLen - static_cast<size_t>(elemData - value);
  if (start > end || end > payloadSize) {
    return false;
  }
  elementOut = elemData + start;
  elementLenOut = end - start;
  return true;
}

bool extractObjectField(
    const char* value,
    size_t valueLen,
    uint32_t targetFieldId,
    const char*& fieldOut,
    size_t& fieldLenOut) {
  return locateObjectField(value, valueLen, targetFieldId, fieldOut, fieldLenOut);
}

} // namespace facebook::velox::variant_encoding
