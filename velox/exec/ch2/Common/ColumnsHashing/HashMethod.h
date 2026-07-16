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
#include "velox/exec/ch2/Common/ColumnsHashing/ColumnsHashingImpl.h"
#include "velox/exec/ch2/Common/SipHash.h"
#include "velox/vector/FlatVector.h"

#include <folly/Portability.h>

#include <cstddef>
#include <cstring>
#include <vector>

namespace facebook::velox::exec::ch2 {

using Sizes = std::vector<size_t>;
using ColumnRawPtrs = std::vector<VectorPtr>;

template <typename T>
FOLLY_ALWAYS_INLINE T unalignedLoad(const void* address) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value;
  std::memcpy(&value, address, sizeof(value));
  return value;
}

template <
    typename Value,
    typename Mapped,
    typename FieldType,
    bool use_cache = true,
    bool need_offset = false,
    bool nullable = false>
struct HashMethodOneNumber : public columns_hashing_impl::HashMethodBase<
                                 HashMethodOneNumber<
                                     Value,
                                     Mapped,
                                     FieldType,
                                     use_cache,
                                     need_offset,
                                     nullable>,
                                 Value,
                                 Mapped,
                                 use_cache,
                                 need_offset,
                                 nullable> {
  using Self = HashMethodOneNumber<
      Value,
      Mapped,
      FieldType,
      use_cache,
      need_offset,
      nullable>;
  using Base = columns_hashing_impl::
      HashMethodBase<Self, Value, Mapped, use_cache, need_offset, nullable>;

  static constexpr bool has_cheap_key_calculation = true;
  static constexpr bool has_pre_computed_hashes = false;

  const char* vec;

  HashMethodOneNumber(
      const ColumnRawPtrs& key_columns,
      const Sizes&,
      const HashMethodContextPtr&)
      : Base(key_columns.empty() ? nullptr : key_columns[0]) {
    VELOX_CHECK_EQ(key_columns.size(), 1);
    const auto column = key_columns[0]->loadedVector();
    VELOX_CHECK(
        column->isFlatEncoding(),
        "HashMethodOneNumber task1 requires a flat key vector");
    VELOX_CHECK(
        !column->mayHaveNulls(),
        "HashMethodOneNumber task1 requires non-null keys");
    const auto* flat = column->template asFlatVector<FieldType>();
    VELOX_CHECK_NOT_NULL(
        flat, "HashMethodOneNumber key type does not match FieldType");
    vec = reinterpret_cast<const char*>(flat->rawValues());
  }

  using Base::createContext;
  using Base::emplaceKey;
  using Base::findKey;
  using Base::getHash;

  FieldType getKeyHolder(size_t row, ch::Arena&) const {
    return unalignedLoad<FieldType>(vec + row * sizeof(FieldType));
  }

  const FieldType* getKeyData() const {
    return reinterpret_cast<const FieldType*>(vec);
  }
};

} // namespace facebook::velox::exec::ch2
