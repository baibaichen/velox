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

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

#include <array>
#include <cstring>
#include <memory>
#include <variant>
#include <vector>

namespace facebook::velox::exec::ch {

struct UInt128 {
  std::array<uint64_t, 2> words{};
  bool operator==(const UInt128&) const = default;
};

struct UInt256 {
  std::array<uint64_t, 4> words{};
  bool operator==(const UInt256&) const = default;
};

static_assert(std::is_trivially_copyable_v<UInt128>);
static_assert(std::is_trivially_copyable_v<UInt256>);
static_assert(sizeof(UInt128) == 16);
static_assert(sizeof(UInt256) == 32);

enum class FixedKeyWidth : uint8_t {
  k8 = 1,
  k16 = 2,
  k32 = 4,
  k64 = 8,
  k128 = 16,
  k256 = 32,
};

inline size_t fixedKeyTypeSize(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
      return sizeof(int8_t);
    case TypeKind::SMALLINT:
      return sizeof(int16_t);
    case TypeKind::INTEGER:
      return sizeof(int32_t);
    case TypeKind::BIGINT:
      return sizeof(int64_t);
    default:
      VELOX_USER_FAIL(
          "packFixed supports TINYINT, SMALLINT, INTEGER, and BIGINT");
  }
}

inline FixedKeyWidth fixedKeyWidth(const std::vector<TypePtr>& types) {
  VELOX_USER_CHECK(!types.empty(), "packFixed requires at least one key");
  size_t bytes = 0;
  for (const auto& type : types) {
    VELOX_USER_CHECK_NOT_NULL(type);
    bytes += fixedKeyTypeSize(type->kind());
  }
  VELOX_USER_CHECK_LE(bytes, sizeof(UInt256), "packFixed key exceeds 32 bytes");
  // A single 1- or 2-byte integer key routes to the direct-address map, so its
  // packed width is the narrow k8/k16. Multi-column packs never narrow below
  // k32 even when their total is 1 or 2 bytes, matching FixedKeyMap::chooseType
  // which reserves key8/key16 for single-column keys.
  if (types.size() == 1) {
    if (bytes == sizeof(uint8_t)) {
      return FixedKeyWidth::k8;
    }
    if (bytes == sizeof(uint16_t)) {
      return FixedKeyWidth::k16;
    }
  }
  if (bytes <= sizeof(uint32_t)) {
    return FixedKeyWidth::k32;
  }
  return bytes <= sizeof(uint64_t)
      ? FixedKeyWidth::k64
      : bytes <= sizeof(UInt128) ? FixedKeyWidth::k128 : FixedKeyWidth::k256;
}

} // namespace facebook::velox::exec::ch
