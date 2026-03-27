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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/types/VariantEncoding.h"
#include "velox/functions/prestosql/types/VariantType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {
namespace {

using namespace variant_encoding;

enum class ExtractKind {
  kDouble,
  kInt32,
  kDate,
  kString,
};

template <ExtractKind KIND>
class VariantExtractFunction : public exec::VectorFunction {
 public:
  VariantExtractFunction(bool isColumnar, std::string fieldName)
      : isColumnar_(isColumnar), fieldName_(std::move(fieldName)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    if (isColumnar_) {
      applyColumnar(rows, args, outputType, context, result);
    } else {
      applyRowBased(rows, args, outputType, context, result);
    }
  }

 private:
  /// Row-based extraction. Each row carries its own metadata + value blob.
  /// Resolve field_id from the first row's metadata (all rows share the same
  /// dictionary for lineitem data) then extract from each row's value blob.
  void applyRowBased(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto* input = args[0]->as<RowVector>();
    auto* metadataCol = input->childAt(0)->as<FlatVector<StringView>>();
    auto* valueCol = input->childAt(1)->as<FlatVector<StringView>>();

    context.ensureWritable(rows, outputType, result);

    // Resolve field_id from first non-null row's metadata.
    int32_t fieldId = -1;
    if (rows.hasSelections()) {
      rows.testSelected([&](auto row) {
        if (input->isNullAt(row)) {
          return true;
        }
        auto meta = metadataCol->valueAt(row);
        fieldId = lookupFieldIdByName(meta.data(), meta.size(), fieldName_);
        return false;
      });
    }

    if (fieldId < 0) {
      // Field not found — set all rows to null.
      rows.applyToSelected(
          [&](auto row) { result->setNull(row, true); });
      return;
    }

    if constexpr (KIND == ExtractKind::kDouble) {
      auto* flat = result->as<FlatVector<double>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto sv = valueCol->valueAt(row);
        double val;
        if (extractDoubleField(sv.data(), sv.size(), fieldId, val)) {
          flat->set(row, val);
        } else {
          flat->setNull(row, true);
        }
      });
    } else if constexpr (KIND == ExtractKind::kInt32) {
      auto* flat = result->as<FlatVector<int32_t>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto sv = valueCol->valueAt(row);
        int32_t val;
        if (extractInt32Field(sv.data(), sv.size(), fieldId, val)) {
          flat->set(row, val);
        } else {
          flat->setNull(row, true);
        }
      });
    } else if constexpr (KIND == ExtractKind::kDate) {
      auto* flat = result->as<FlatVector<int32_t>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto sv = valueCol->valueAt(row);
        int32_t val;
        if (extractDateField(sv.data(), sv.size(), fieldId, val)) {
          flat->set(row, val);
        } else {
          flat->setNull(row, true);
        }
      });
    } else if constexpr (KIND == ExtractKind::kString) {
      auto* flat = result->as<FlatVector<StringView>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto sv = valueCol->valueAt(row);
        std::string_view val;
        if (extractStringField(sv.data(), sv.size(), fieldId, val)) {
          flat->set(row, StringView(val.data(), val.size()));
        } else {
          flat->setNull(row, true);
        }
      });
    }
  }

  /// Columnar (4-column) extraction via array index lookups.
  void applyColumnar(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto* input = args[0]->as<RowVector>();

    // Resolve field_id from keys array (child 0).
    auto* keysArray = input->childAt(0)->as<ArrayVector>();
    auto* keysElements = keysArray->elements()->as<FlatVector<StringView>>();

    // Find field_id by linear scan of the keys array of the first non-null row.
    int32_t fieldId = -1;
    if (rows.hasSelections()) {
      rows.testSelected([&](auto row) {
        if (input->isNullAt(row)) {
          return true;
        }
        auto keysOffset = keysArray->offsetAt(row);
        auto keysSize = keysArray->sizeAt(row);
        for (vector_size_t i = 0; i < keysSize; ++i) {
          auto key = keysElements->valueAt(keysOffset + i);
          if (std::string_view(key.data(), key.size()) == fieldName_) {
            fieldId = i;
            return false;
          }
        }
        return false;
      });
    }

    if (fieldId < 0) {
      context.ensureWritable(rows, outputType, result);
      rows.applyToSelected(
          [&](auto row) { result->setNull(row, true); });
      return;
    }

    // children[1] = ARRAY(ROW(keys_index, values_index))
    auto* childrenArray = input->childAt(1)->as<ArrayVector>();
    auto* childrenElements = childrenArray->elements()->as<RowVector>();
    auto* valuesIndexCol =
        childrenElements->childAt(1)->as<FlatVector<int32_t>>();

    // values[2] = ARRAY(ROW(type_id, byte_offset))
    auto* valuesArray = input->childAt(2)->as<ArrayVector>();
    auto* valuesElements = valuesArray->elements()->as<RowVector>();
    auto* byteOffsetCol =
        valuesElements->childAt(1)->as<FlatVector<int32_t>>();

    // data[3] = VARBINARY
    auto* dataCol = input->childAt(3)->as<FlatVector<StringView>>();

    context.ensureWritable(rows, outputType, result);

    if constexpr (KIND == ExtractKind::kDouble) {
      auto* flat = result->as<FlatVector<double>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row) ||
            fieldId >= childrenArray->sizeAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto childrenOffset = childrenArray->offsetAt(row);
        auto valIdx = valuesIndexCol->valueAt(childrenOffset + fieldId);
        auto byteOff = byteOffsetCol->valueAt(valIdx);
        auto data = dataCol->valueAt(row);
        if (byteOff + 9 <= static_cast<int32_t>(data.size())) {
          flat->set(row, readLEDouble(data.data() + byteOff + 1));
        } else {
          flat->setNull(row, true);
        }
      });
    } else if constexpr (
        KIND == ExtractKind::kInt32 || KIND == ExtractKind::kDate) {
      auto* flat = result->as<FlatVector<int32_t>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row) ||
            fieldId >= childrenArray->sizeAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto childrenOffset = childrenArray->offsetAt(row);
        auto valIdx = valuesIndexCol->valueAt(childrenOffset + fieldId);
        auto byteOff = byteOffsetCol->valueAt(valIdx);
        auto data = dataCol->valueAt(row);
        if (byteOff + 5 <= static_cast<int32_t>(data.size())) {
          flat->set(
              row, static_cast<int32_t>(readLE32(data.data() + byteOff + 1)));
        } else {
          flat->setNull(row, true);
        }
      });
    } else if constexpr (KIND == ExtractKind::kString) {
      auto* flat = result->as<FlatVector<StringView>>();
      rows.applyToSelected([&](auto row) {
        if (input->isNullAt(row) ||
            fieldId >= childrenArray->sizeAt(row)) {
          flat->setNull(row, true);
          return;
        }
        auto childrenOffset = childrenArray->offsetAt(row);
        auto valIdx = valuesIndexCol->valueAt(childrenOffset + fieldId);
        auto byteOff = byteOffsetCol->valueAt(valIdx);
        auto data = dataCol->valueAt(row);
        const char* p = data.data() + byteOff;
        size_t remaining = data.size() - byteOff;
        std::string_view decoded;
        if (decodeStringValue(p, remaining, decoded)) {
          flat->set(row, StringView(decoded.data(), decoded.size()));
        } else {
          flat->setNull(row, true);
        }
      });
    }
  }

  const bool isColumnar_;
  const std::string fieldName_;
};

template <ExtractKind KIND>
class VariantExtractFunctionFactory {
 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::string returnType;
    if constexpr (KIND == ExtractKind::kDouble) {
      returnType = "double";
    } else if constexpr (KIND == ExtractKind::kInt32) {
      returnType = "integer";
    } else if constexpr (KIND == ExtractKind::kDate) {
      returnType = "integer";
    } else if constexpr (KIND == ExtractKind::kString) {
      returnType = "varchar";
    }

    // Accept both 2-column (row-based) and 4-column (columnar) ROW types.
    return {exec::FunctionSignatureBuilder()
                .returnType(returnType)
                .argumentType("row(varbinary,varbinary)")
                .constantArgumentType("varchar")
                .build(),
            exec::FunctionSignatureBuilder()
                .returnType(returnType)
                .argumentType(
                    "row(array(varchar),array(row(integer,integer)),"
                    "array(row(tinyint,integer)),varbinary)")
                .constantArgumentType("varchar")
                .build()};
  }

  static std::shared_ptr<exec::VectorFunction> create(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const core::QueryConfig& /*config*/) {
    VELOX_CHECK_EQ(inputArgs.size(), 2);

    // Determine columnar vs row-based by checking child count:
    // columnar = 4 children (keys, children, values, data),
    // row-based = 2 children (metadata, value).
    // The size==4 fallback handles the case where signature resolution passes
    // a structural ROW type (not the VARIANT_COLUMNAR singleton). This is safe
    // because the 4-column signature already constrains the type structure to
    // row(array(varchar),array(row(int,int)),array(row(tinyint,int)),varbinary).
    bool isColumnar = isVariantColumnarType(inputArgs[0].type) ||
        (inputArgs[0].type->isRow() && inputArgs[0].type->size() == 4);

    auto fieldNameVector = inputArgs[1].constantValue;
    VELOX_USER_CHECK_NOT_NULL(
        fieldNameVector,
        "variant_extract requires a constant field name argument");
    VELOX_USER_CHECK(
        !fieldNameVector->isNullAt(0),
        "variant_extract field name must not be null");
    auto fieldName =
        fieldNameVector->as<ConstantVector<StringView>>()->valueAt(0).str();

    return std::make_shared<VariantExtractFunction<KIND>>(
        isColumnar, std::move(fieldName));
  }
};

} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_variant_extract_double,
    (VariantExtractFunctionFactory<ExtractKind::kDouble>::signatures()),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    VariantExtractFunctionFactory<ExtractKind::kDouble>::create);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_variant_extract_int32,
    (VariantExtractFunctionFactory<ExtractKind::kInt32>::signatures()),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    VariantExtractFunctionFactory<ExtractKind::kInt32>::create);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_variant_extract_date,
    (VariantExtractFunctionFactory<ExtractKind::kDate>::signatures()),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    VariantExtractFunctionFactory<ExtractKind::kDate>::create);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_variant_extract_string,
    (VariantExtractFunctionFactory<ExtractKind::kString>::signatures()),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    VariantExtractFunctionFactory<ExtractKind::kString>::create);

} // namespace facebook::velox
