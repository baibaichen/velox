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
#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/HashTable/HashTableKeyHolder.h"
#include "velox/vector/BaseVector.h"

#include <folly/Portability.h>

#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <vector>

namespace facebook::velox::exec::ch {

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

// infra 承载:CH key_columns[i]->getRawData().data()(定长列裸数据
// 基址)逐列换成一个 const char*。整批就是 std::vector<const char*>。
using ColumnRawData = std::vector<const char*>;
using Sizes = std::vector<size_t>;

struct HashMethodContextSettings {
  size_t max_threads{};
  bool serialize_string_with_zero_byte{false};
  bool enable_prefetch{true};
  size_t min_bytes_for_prefetch{0};
};

class HashMethodContext {
 public:
  virtual ~HashMethodContext() = default;
  using Settings = HashMethodContextSettings;
};

using HashMethodContextPtr = std::shared_ptr<HashMethodContext>;

namespace columns_hashing_impl {

// ============================================================================
// BaseStateKeysFixed - exactly 搬自 CH src/Common/ColumnsHashingImpl.h:525-608.
// 支撑 HashMethodKeysFixed 的 nullable。铁律:算法搬 CH,只换 infra 承载。
//
// infra 边界:CH getActualColumns() 返回 ColumnRawPtrs(nullable 取 nested,否则
//   列本身);ch pack 从 Velox flat 列取好 rawValues() 基址装成 ColumnRawData
//   (std::vector<const char*>),actual_columns 承载换成 ColumnRawData。
//
// nullable 分支照抄保留、本 task flat non-null 不走(has_nullable_keys=false;
// createBitmap VELOX_NYI/TODO,承载待后续 task 补)。
// ============================================================================
template <typename Key>
using KeysNullMap = std::vector<UInt8>;

template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
// CH 原文 (ColumnsHashingImpl.h:531-589)。infra:actual_columns/null_maps 承载换
// ColumnRawData。本 task 不实例化,照抄保留 + VELOX_NYI 折走。
template <typename Key>
class BaseStateKeysFixed<Key, true> {
 protected:
  explicit BaseStateKeysFixed(const ColumnRawData& key_columns)
      : actual_columns(key_columns) {
    VELOX_NYI(
        "nullable BaseStateKeysFixed<Key,true> is not supported in task3");
  }

  const ColumnRawData& getActualColumns() const {
    return actual_columns;
  }

  KeysNullMap<Key> createBitmap(size_t /*row*/) const {
    VELOX_NYI("createBitmap for nullable keys is not supported in task3");
  }

 private:
  ColumnRawData actual_columns;
  ColumnRawData null_maps;
};

/// Case where nullable keys are not supported.
// CH 原文 (ColumnsHashingImpl.h:592-608) 逐字。infra:actual_columns 承载换
// ColumnRawData。
template <typename Key>
class BaseStateKeysFixed<Key, false> {
 protected:
  explicit BaseStateKeysFixed(const ColumnRawData& columns)
      : actual_columns(columns) {}

  const ColumnRawData& getActualColumns() const {
    return actual_columns;
  }

  KeysNullMap<Key> createBitmap(size_t) const {
    VELOX_FAIL(
        "Internal error: calling createBitmap() for non-nullable keys is "
        "forbidden");
  }

 private:
  ColumnRawData actual_columns;
};

struct LastElementCacheBase {
  bool empty = true;
  bool found = false;
  UInt64 misses = 0;

  void onNewValue(bool isFound) {
    empty = false;
    found = isFound;
    ++misses;
  }

  bool hasOnlyOneValue() const {
    return found && misses == 1;
  }
};

template <typename Value, bool nullable>
struct LastElementCache;

template <typename Value>
struct LastElementCache<Value, true> : public LastElementCacheBase {
  Value value{};
  bool is_null = false;

  template <typename Key>
  bool check(const Key& key) const {
    return !is_null && value.first == key;
  }

  bool check(const Value& rhs) const {
    return !is_null && value == rhs;
  }
};

template <typename Value>
struct LastElementCache<Value, false> : public LastElementCacheBase {
  Value value{};

  template <typename Key>
  bool check(const Key& key) const {
    return value.first == key;
  }

  bool check(const Value& rhs) const {
    return value == rhs;
  }
};

template <typename Mapped>
class EmplaceResultImpl {
  Mapped& value;
  Mapped& cached_value;
  bool inserted;

 public:
  EmplaceResultImpl(Mapped& valueValue, Mapped& cachedValue, bool wasInserted)
      : value(valueValue), cached_value(cachedValue), inserted(wasInserted) {}

  bool isInserted() const {
    return inserted;
  }

  auto& getMapped() const {
    return value;
  }

  void setMapped(const Mapped& mapped) {
    cached_value = mapped;
    value = mapped;
  }
};

template <>
class EmplaceResultImpl<void> {
  bool inserted;

 public:
  explicit EmplaceResultImpl(bool wasInserted) : inserted(wasInserted) {}

  bool isInserted() const {
    return inserted;
  }
};

class FindResultImplBase {
  bool found;

 public:
  explicit FindResultImplBase(bool wasFound) : found(wasFound) {}

  bool isFound() const {
    return found;
  }
};

template <bool need_offset = false>
class FindResultImplOffsetBase {
 public:
  static constexpr bool has_offset = need_offset;
  explicit FindResultImplOffsetBase(size_t) {}
};

template <>
class FindResultImplOffsetBase<true> {
  size_t offset;

 public:
  static constexpr bool has_offset = true;

  explicit FindResultImplOffsetBase(size_t offsetValue) : offset(offsetValue) {}

  FOLLY_ALWAYS_INLINE size_t getOffset() const {
    return offset;
  }
};

template <typename Mapped, bool need_offset = false>
class FindResultImpl : public FindResultImplBase,
                       public FindResultImplOffsetBase<need_offset> {
  Mapped* value;

 public:
  FindResultImpl()
      : FindResultImplBase(false), FindResultImplOffsetBase<need_offset>(0) {}

  FindResultImpl(Mapped* mapped, bool wasFound, size_t offset)
      : FindResultImplBase(wasFound),
        FindResultImplOffsetBase<need_offset>(offset),
        value(mapped) {}

  Mapped& getMapped() const {
    return *value;
  }
};

template <bool need_offset>
class FindResultImpl<void, need_offset>
    : public FindResultImplBase, public FindResultImplOffsetBase<need_offset> {
 public:
  FindResultImpl(bool wasFound, size_t offset)
      : FindResultImplBase(wasFound),
        FindResultImplOffsetBase<need_offset>(offset) {}
};

template <
    typename Derived,
    typename Value,
    typename Mapped,
    bool consecutive_keys_optimization,
    bool need_offset = false,
    bool nullable = false>
class HashMethodBase {
 public:
  using EmplaceResult = EmplaceResultImpl<Mapped>;
  using FindResult = FindResultImpl<Mapped, need_offset>;
  static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
  using Cache = LastElementCache<Value, nullable>;
  static constexpr bool has_range_check = false;
  static constexpr bool has_pre_computed_hashes = false;

  static HashMethodContextPtr createContext(const HashMethodContextSettings&) {
    return nullptr;
  }

  template <typename Data>
  FOLLY_ALWAYS_INLINE EmplaceResult
  emplaceKey(Data& data, size_t row, ch::Arena& pool) {
    if constexpr (nullable) {
      if (!block_has_no_nulls && null_map_data[row]) [[unlikely]] {
        if constexpr (consecutive_keys_optimization) {
          if (!cache.is_null) {
            cache.onNewValue(true);
            cache.is_null = true;
          }
        }

        bool has_null_key = data.hasNullKeyData();
        data.hasNullKeyData() = true;

        if constexpr (has_mapped) {
          return EmplaceResult(
              data.getNullKeyData(), data.getNullKeyData(), !has_null_key);
        } else {
          return EmplaceResult(!has_null_key);
        }
      }
    }

    auto& derived = static_cast<Derived&>(*this);
    auto key_holder = derived.getKeyHolder(row, pool);
    if constexpr (Derived::has_pre_computed_hashes) {
      if (!derived.precomputed_hashes_initialized) [[unlikely]] {
        derived.initPrecomputedHashes(data, row);
      }

      if (derived.can_precompute_hashes) {
        if (row == derived.calibration_row) {
          derived.prefetch_look_ahead =
              derived.prefetching->calcPrefetchLookAhead();
        }
        const auto& hashes = derived.precomputed_hashes;
        if (row + derived.prefetch_look_ahead < hashes.size()) {
          data.prefetchByHash(hashes[row + derived.prefetch_look_ahead]);
        }
        return emplaceImpl<false>(key_holder, data, hashes[row]);
      }
    }
    return emplaceImpl<true>(key_holder, data, 0);
  }

  template <typename Data>
  FOLLY_ALWAYS_INLINE FindResult
  findKey(Data& data, size_t row, ch::Arena& pool) {
    if constexpr (nullable) {
      if (!block_has_no_nulls && null_map_data[row]) [[unlikely]] {
        bool has_null_key = data.hasNullKeyData();

        if constexpr (consecutive_keys_optimization) {
          if (!cache.is_null) {
            cache.onNewValue(has_null_key);
            cache.is_null = true;
          }
        }

        if constexpr (has_mapped) {
          return FindResult(&data.getNullKeyData(), has_null_key, 0);
        } else {
          return FindResult(has_null_key, 0);
        }
      }
    }

    auto& derived = static_cast<Derived&>(*this);
    if constexpr (Derived::has_pre_computed_hashes) {
      if (!derived.precomputed_hashes_initialized) [[unlikely]] {
        derived.initPrecomputedHashes(data, row);
      }

      if (derived.can_precompute_hashes) {
        if (row == derived.calibration_row) {
          derived.prefetch_look_ahead =
              derived.prefetching->calcPrefetchLookAhead();
        }
        const auto& hashes = derived.precomputed_hashes;
        if (row + derived.prefetch_look_ahead < hashes.size()) {
          data.prefetchByHash(hashes[row + derived.prefetch_look_ahead]);
        }

        if (data.isEmptyCell(hashes[row])) {
          if constexpr (has_mapped) {
            return FindResult(nullptr, false, 0);
          } else {
            return FindResult(false, 0);
          }
        }
      }
    }

    if constexpr (Derived::has_range_check) {
      auto [key_holder, in_range] =
          static_cast<const Derived&>(*this).getKeyHolderInRange(row, pool);
      if (!in_range) {
        if constexpr (has_mapped) {
          return FindResult(nullptr, false, 0);
        } else {
          return FindResult(false, 0);
        }
      }
      return findKeyImpl(keyHolderGetKey(key_holder), data);
    } else {
      auto key_holder = static_cast<Derived&>(*this).getKeyHolder(row, pool);
      return findKeyImpl(keyHolderGetKey(key_holder), data);
    }
  }

  template <typename Data>
  FOLLY_ALWAYS_INLINE size_t
  getHash(const Data& data, size_t row, ch::Arena& pool) {
    auto key_holder = static_cast<Derived&>(*this).getKeyHolder(row, pool);
    return data.hash(keyHolderGetKey(key_holder));
  }

  FOLLY_ALWAYS_INLINE void resetCache() {
    if constexpr (consecutive_keys_optimization) {
      cache.empty = true;
      cache.found = false;
      cache.misses = 0;
    }
  }

  FOLLY_ALWAYS_INLINE bool hasOnlyOneValueSinceLastReset() const {
    if constexpr (consecutive_keys_optimization) {
      return cache.hasOnlyOneValue();
    }
    return false;
  }

  FOLLY_ALWAYS_INLINE UInt64 getCacheMissesSinceLastReset() const {
    if constexpr (consecutive_keys_optimization) {
      return cache.misses;
    }
    return 0;
  }

  FOLLY_ALWAYS_INLINE bool isNullAt(size_t row) const {
    if constexpr (nullable) {
      return !block_has_no_nulls && null_map_data[row];
    } else {
      return false;
    }
  }

 protected:
  Cache cache;
  const UInt8* null_map_data = nullptr;
  bool block_has_no_nulls = true;
  bool has_null_data = false;

  explicit HashMethodBase(const VectorPtr& column = nullptr) {
    if constexpr (consecutive_keys_optimization) {
      if constexpr (has_mapped) {
        cache.value.second = Mapped();
        cache.value.first = {};
      } else {
        cache.value = Value();
      }
    }

    if constexpr (nullable) {
      VELOX_CHECK_NOT_NULL(column);
      VELOX_NYI("nullable HashMethod keys are not supported in task1");
    }
  }

  template <bool compute_hash, typename Data, typename KeyHolder>
  FOLLY_ALWAYS_INLINE EmplaceResult emplaceImpl(
      KeyHolder& key_holder,
      Data& data,
      [[maybe_unused]] size_t hash_value) {
    if constexpr (consecutive_keys_optimization) {
      if (cache.found && cache.check(keyHolderGetKey(key_holder))) {
        if constexpr (has_mapped) {
          return EmplaceResult(cache.value.second, cache.value.second, false);
        } else {
          return EmplaceResult(false);
        }
      }
    }

    typename Data::LookupResult it;
    bool inserted = false;

    if constexpr (compute_hash) {
      data.emplace(key_holder, it, inserted);
    } else {
      data.emplace(key_holder, it, inserted, hash_value);
    }

    [[maybe_unused]] Mapped* cached = nullptr;
    if constexpr (has_mapped) {
      cached = &it->getMapped();
    }

    if constexpr (has_mapped) {
      if (inserted) {
        new (&it->getMapped()) Mapped();
      }
    }

    if constexpr (consecutive_keys_optimization) {
      cache.onNewValue(true);

      if constexpr (nullable) {
        cache.is_null = false;
      }

      if constexpr (has_mapped) {
        cache.value.first = it->getKey();
        cache.value.second = it->getMapped();
        cached = &cache.value.second;
      } else {
        cache.value = it->getKey();
      }
    }

    if constexpr (has_mapped) {
      return EmplaceResult(it->getMapped(), *cached, inserted);
    } else {
      return EmplaceResult(inserted);
    }
  }

  template <typename Data, typename Key>
  FOLLY_ALWAYS_INLINE FindResult findKeyImpl(Key key, Data& data) {
    if constexpr (consecutive_keys_optimization) {
      static_assert(
          !FindResult::has_offset,
          "`consecutive_keys_optimization` and `has_offset` conflict");
      if (FOLLY_LIKELY(!cache.empty) && cache.check(key)) {
        if constexpr (has_mapped) {
          return FindResult(&cache.value.second, cache.found, 0);
        } else {
          return FindResult(cache.found, 0);
        }
      }
    }

    auto it = data.find(key);

    if constexpr (consecutive_keys_optimization) {
      cache.onNewValue(it != nullptr);

      if constexpr (nullable) {
        cache.is_null = false;
      }

      if constexpr (has_mapped) {
        cache.value.first = key;
        if (it) {
          cache.value.second = it->getMapped();
        }
      } else {
        cache.value = key;
      }
    }

    size_t offset = 0;
    if constexpr (FindResult::has_offset) {
      offset = it ? data.offsetInternal(it) : 0;
    }

    if constexpr (has_mapped) {
      return FindResult(it ? &it->getMapped() : nullptr, it != nullptr, offset);
    } else {
      return FindResult(it != nullptr, offset);
    }
  }
};

} // namespace columns_hashing_impl
} // namespace facebook::velox::exec::ch
