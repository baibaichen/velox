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

#include "velox/type/Type.h"

namespace facebook::velox {

inline constexpr const char* kVariantColumnarTypeName = "variant_columnar";
inline constexpr const char* kVariantRowBasedTypeName = "variant_row_based";

/// Columnar VARIANT layout (DuckDB-style 4-column decomposition).
///
/// Physical schema: ROW<
///   keys: ARRAY(VARCHAR),
///   children: ARRAY(ROW(keys_index INTEGER, values_index INTEGER)),
///   values: ARRAY(ROW(type_id TINYINT, byte_offset INTEGER)),
///   data: VARBINARY
/// >
///
/// At scan time the Parquet binary blob is fully decomposed into these four
/// columns. Field extraction uses index-based lookups through the arrays.
class VariantColumnarType final : public VariantType {
  VariantColumnarType()
      : VariantType(
            {"keys", "children", "values", "data"},
            {ARRAY(VARCHAR()),
             ARRAY(ROW(
                 {"keys_index", "values_index"},
                 {INTEGER(), INTEGER()})),
             ARRAY(ROW({"type_id", "byte_offset"}, {TINYINT(), INTEGER()})),
             VARBINARY()}) {}

 public:
  static std::shared_ptr<const VariantColumnarType> get() {
    static const VariantColumnarType kInstance;
    return {std::shared_ptr<const VariantColumnarType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const char* name() const override {
    return "VARIANT_COLUMNAR";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  std::span<const TypeParameter> parameters() const override {
    return {};
  }
};

/// Row-based VARIANT layout (StarRocks/Spark-style 2-column blob).
///
/// Physical schema: ROW<metadata: VARBINARY, value: VARBINARY>
///
/// Each row stores the Parquet VARIANT binary encoding directly: a metadata
/// blob (field name dictionary) and a value blob (binary-encoded object).
/// Decode cost is a single memcpy per column. Field extraction parses the
/// blob on demand.
class VariantRowBasedType final : public VariantType {
  VariantRowBasedType()
      : VariantType({"metadata", "value"}, {VARBINARY(), VARBINARY()}) {}

 public:
  static std::shared_ptr<const VariantRowBasedType> get() {
    static const VariantRowBasedType kInstance;
    return {std::shared_ptr<const VariantRowBasedType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const char* name() const override {
    return "VARIANT_ROW_BASED";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  std::span<const TypeParameter> parameters() const override {
    return {};
  }
};

FOLLY_ALWAYS_INLINE bool isVariantColumnarType(const TypePtr& type) {
  return VariantColumnarType::get() == type;
}

FOLLY_ALWAYS_INLINE bool isVariantRowBasedType(const TypePtr& type) {
  return VariantRowBasedType::get() == type;
}

FOLLY_ALWAYS_INLINE bool isVariantType(const TypePtr& type) {
  return isVariantColumnarType(type) || isVariantRowBasedType(type);
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const VariantColumnarType>
VARIANT_COLUMNAR() {
  return VariantColumnarType::get();
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const VariantRowBasedType>
VARIANT_ROW_BASED() {
  return VariantRowBasedType::get();
}

} // namespace facebook::velox
