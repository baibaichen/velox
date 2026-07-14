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

#include "velox/exec/ch/FixedHashMap.h"
#include "velox/exec/ch/HashMap.h"
#include "velox/exec/ch/HashedKey.h"

#include <algorithm>
#include <variant>

namespace facebook::velox::exec::ch {

class FixedKeyMap {
 public:
  /// Identifies the concrete hash-map variant chosen for the join keys,
  /// mirroring ClickHouse's HashJoin method selection. The value is decided
  /// once via chooseType and then dispatched on.
  enum class Type {
    // key64/keys64 use the 64-bit map; key32/keys32 use a dedicated 32-bit
    // Map32. Single 1-/2-byte keys use a direct-address Map8/Map16.
    key8,
    key16,
    key32,
    key64,
    keys32,
    keys64,
    keys128,
    keys256,
    key_string,
    hashed,
  };

  /// Chooses the map type for the given key columns, faithfully porting
  /// ClickHouse's chooseMethod decision order. Fails for key kinds outside the
  /// supported set (integer fixed-width and VARCHAR).
  static Type chooseType(const std::vector<TypePtr>& keyTypes);

  using Map8 = FixedDirectMap_key8;
  using Map16 = FixedDirectMap_key16;
  using Map32 = HashMapAll_key32;
  using Map64 = HashMapAll_key64;
  using Map128 = HashMapAll_keys128;
  using Map256 = HashMapAll_keys256;
  using KeyStringMap = HashMapAll_key_string;
  using HashedMap = HashMapAll_hashed;

  FixedKeyMap(memory::MemoryPool* pool, std::vector<TypePtr> keyTypes)
      : keyTypes_(std::move(keyTypes)),
        type_(chooseType(keyTypes_)),
        maps_(makeMap(pool, type_)) {}

  /// Returns the concrete map variant chosen for the join keys. All dispatch is
  /// driven off this authoritative value, mirroring ClickHouse's chooseMethod.
  Type type() const {
    return type_;
  }

  /// Returns the packed-key width for a fixed-width integer map. Only valid
  /// when type() is one of the packed integer families; used by the
  /// FixedKeyDecoder pack path to size the packed key.
  FixedKeyWidth width() const {
    switch (type_) {
      case Type::key8:
        return FixedKeyWidth::k8;
      case Type::key16:
        return FixedKeyWidth::k16;
      case Type::key32:
      case Type::keys32:
        return FixedKeyWidth::k32;
      case Type::keys64:
      case Type::key64:
        return FixedKeyWidth::k64;
      case Type::keys128:
        return FixedKeyWidth::k128;
      case Type::keys256:
        return FixedKeyWidth::k256;
      default:
        VELOX_FAIL("Map has no fixed key width: {}", static_cast<int>(type_));
    }
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

  void reserve(size_t keys) {
    std::visit([&](auto& map) { map.reserve(keys); }, maps_);
  }

  bool empty() const {
    return std::visit([](const auto& map) { return map.empty(); }, maps_);
  }

  size_t size() const {
    return std::visit([](const auto& map) { return map.size(); }, maps_);
  }

  size_t getBufferSizeInCells() const {
    return std::visit(
        [](const auto& map) { return map.getBufferSizeInCells(); }, maps_);
  }

  size_t getBufferSizeInBytes() const {
    return std::visit(
        [](const auto& map) { return map.getBufferSizeInBytes(); }, maps_);
  }

  RowRefList& emplace(uint8_t key) {
    return map8().emplace(key);
  }

  RowRefList& emplace(uint16_t key) {
    return map16().emplace(key);
  }

  RowRefList& emplace(uint32_t key) {
    return map32().emplace(key);
  }

  RowRefList& emplace(uint64_t key) {
    return map64().emplace(key);
  }

  RowRefList& emplace(const UInt128& key) {
    return map128().emplace(key);
  }

  RowRefList& emplace(const UInt256& key) {
    return map256().emplace(key);
  }

  RowRefList& emplace(const StringRef& key) {
    return keyStringMap().emplace(key);
  }

  // Emplaces a string key using a precomputed hash, avoiding a rehash. Returns
  // the cell (existing or newly inserted) and sets inserted accordingly. On
  // insert the cell holds the caller-supplied key bytes, which must be
  // persisted to outlive the map; the caller is expected to swap in an
  // arena-owned copy via cell->setKey. Mirrors ClickHouse emplaceKey +
  // ArenaKeyHolder: a single probe both looks up and inserts.
  KeyStringMap::LookupResult
  emplace(const StringRef& key, size_t hashValue, bool& inserted) {
    KeyStringMap::LookupResult result;
    keyStringMap().emplace(key, result, inserted, hashValue);
    return result;
  }

  RowRefList& emplaceHashed(const UInt128& key) {
    return hashedMap().emplace(key);
  }

  Map8::LookupResult find(uint8_t key) {
    return map8().find(key);
  }

  Map8::ConstLookupResult find(uint8_t key) const {
    return map8().find(key);
  }

  Map16::LookupResult find(uint16_t key) {
    return map16().find(key);
  }

  Map16::ConstLookupResult find(uint16_t key) const {
    return map16().find(key);
  }

  Map32::LookupResult find(uint32_t key) {
    return map32().find(key);
  }

  Map32::ConstLookupResult find(uint32_t key) const {
    return map32().find(key);
  }

  Map64::LookupResult find(uint64_t key) {
    return map64().find(key);
  }

  Map64::ConstLookupResult find(uint64_t key) const {
    return map64().find(key);
  }

  Map128::LookupResult find(const UInt128& key) {
    return map128().find(key);
  }

  Map128::ConstLookupResult find(const UInt128& key) const {
    return map128().find(key);
  }

  Map256::LookupResult find(const UInt256& key) {
    return map256().find(key);
  }

  Map256::ConstLookupResult find(const UInt256& key) const {
    return map256().find(key);
  }

  KeyStringMap::LookupResult find(const StringRef& key) {
    return keyStringMap().find(key);
  }

  KeyStringMap::ConstLookupResult find(const StringRef& key) const {
    return keyStringMap().find(key);
  }

  // Looks up a string key using a precomputed hash, avoiding a rehash.
  KeyStringMap::LookupResult find(const StringRef& key, size_t hashValue) {
    return keyStringMap().find(key, hashValue);
  }

  KeyStringMap::ConstLookupResult find(const StringRef& key, size_t hashValue)
      const {
    return keyStringMap().find(key, hashValue);
  }

  HashedMap::LookupResult findHashed(const UInt128& key) {
    return hashedMap().find(key);
  }

  HashedMap::ConstLookupResult findHashed(const UInt128& key) const {
    return hashedMap().find(key);
  }

  auto begin() const {
    return map64().begin();
  }

  auto end() const {
    return map64().end();
  }

  template <typename Key>
  size_t hash(const Key& key) const {
    return map<Key>().hash(key);
  }

  template <typename Key>
  void prefetch(const Key& key) const {
    const auto& typedMap = map<Key>();
    typedMap.prefetchByHash(typedMap.hash(key));
  }

  // Returns the hash of a string key for the key_string map, letting callers
  // compute it once and reuse it across find/emplace/prefetch.
  size_t hashString(const StringRef& key) const {
    return keyStringMap().hash(key);
  }

  // Prefetches the string map bucket for a precomputed hash.
  void prefetchString(size_t hashValue) const {
    keyStringMap().prefetchByHash(hashValue);
  }

 private:
  using Maps = std::variant<
      Map8,
      Map16,
      Map32,
      Map64,
      Map128,
      Map256,
      KeyStringMap,
      HashedMap>;

  static Maps makeMap(memory::MemoryPool* pool, Type type) {
    switch (type) {
      // Single 1-/2-byte keys use a direct-address Map8/Map16 (key == index).
      case Type::key8:
        return Maps(std::in_place_type<Map8>, pool);
      case Type::key16:
        return Maps(std::in_place_type<Map16>, pool);
      // Single 4-byte or packs totaling <= 4 bytes use the 32-bit Map32; packs
      // totaling <= 8 bytes use the 64-bit map. The FixedKeyDecoder packs into
      // uint32_t or uint64_t respectively.
      case Type::key32:
      case Type::keys32:
        return Maps(std::in_place_type<Map32>, pool);
      case Type::keys64:
      case Type::key64:
        return Maps(std::in_place_type<Map64>, pool);
      case Type::keys128:
        return Maps(std::in_place_type<Map128>, pool);
      case Type::keys256:
        return Maps(std::in_place_type<Map256>, pool);
      case Type::key_string:
        return Maps(std::in_place_type<KeyStringMap>, pool);
      case Type::hashed:
        return Maps(std::in_place_type<HashedMap>, pool);
    }
    VELOX_UNREACHABLE();
  }

  template <typename Key>
  const auto& map() const {
    if constexpr (std::is_same_v<Key, uint8_t>) {
      return map8();
    } else if constexpr (std::is_same_v<Key, uint16_t>) {
      return map16();
    } else if constexpr (std::is_same_v<Key, uint32_t>) {
      return map32();
    } else if constexpr (std::is_same_v<Key, uint64_t>) {
      return map64();
    } else if constexpr (std::is_same_v<Key, UInt128>) {
      return map128();
    } else if constexpr (std::is_same_v<Key, UInt256>) {
      return map256();
    } else {
      static_assert(std::is_same_v<Key, StringRef>);
      return keyStringMap();
    }
  }

  Map8& map8() {
    return std::get<Map8>(maps_);
  }
  const Map8& map8() const {
    return std::get<Map8>(maps_);
  }
  Map16& map16() {
    return std::get<Map16>(maps_);
  }
  const Map16& map16() const {
    return std::get<Map16>(maps_);
  }
  Map32& map32() {
    return std::get<Map32>(maps_);
  }
  const Map32& map32() const {
    return std::get<Map32>(maps_);
  }
  Map64& map64() {
    return std::get<Map64>(maps_);
  }
  const Map64& map64() const {
    return std::get<Map64>(maps_);
  }
  Map128& map128() {
    return std::get<Map128>(maps_);
  }
  const Map128& map128() const {
    return std::get<Map128>(maps_);
  }
  Map256& map256() {
    return std::get<Map256>(maps_);
  }
  const Map256& map256() const {
    return std::get<Map256>(maps_);
  }
  KeyStringMap& keyStringMap() {
    return std::get<KeyStringMap>(maps_);
  }
  const KeyStringMap& keyStringMap() const {
    return std::get<KeyStringMap>(maps_);
  }
  HashedMap& hashedMap() {
    return std::get<HashedMap>(maps_);
  }
  const HashedMap& hashedMap() const {
    return std::get<HashedMap>(maps_);
  }

  std::vector<TypePtr> keyTypes_;
  Type type_;
  Maps maps_;
};

namespace detail {

// Returns true when the kind is one of the fixed-width integer kinds supported
// as a hash-join key.
inline bool isFixedIntegerKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return true;
    default:
      return false;
  }
}

} // namespace detail

inline FixedKeyMap::Type FixedKeyMap::chooseType(
    const std::vector<TypePtr>& keyTypes) {
  VELOX_USER_CHECK(!keyTypes.empty(), "hash join requires at least one key");
  for (const auto& type : keyTypes) {
    VELOX_USER_CHECK_NOT_NULL(type);
    VELOX_USER_CHECK(
        detail::isFixedIntegerKind(type->kind()) ||
            type->kind() == TypeKind::VARCHAR,
        "hash join key supports integer fixed-width and VARCHAR keys, got {}",
        type->toString());
  }

  // Single numeric key: route to the scalar keyN family by its byte width. The
  // composite keysN family (below) is reserved for multi-column keys, so this
  // width test does not overlap with the packing branch — matching ClickHouse's
  // chooseMethod, which keeps single-key and packed-key paths distinct.
  if (keyTypes.size() == 1 && detail::isFixedIntegerKind(keyTypes[0]->kind())) {
    switch (fixedKeyTypeSize(keyTypes[0]->kind())) {
      case 1:
        return Type::key8;
      case 2:
        return Type::key16;
      case 4:
        return Type::key32;
      case 8:
        return Type::key64;
    }
    VELOX_UNREACHABLE();
  }

  // All keys are fixed-width integers: pack by total bytes.
  const bool allFixedInteger =
      std::all_of(keyTypes.begin(), keyTypes.end(), [](const TypePtr& type) {
        return detail::isFixedIntegerKind(type->kind());
      });
  if (allFixedInteger) {
    size_t bytes = 0;
    for (const auto& type : keyTypes) {
      bytes += fixedKeyTypeSize(type->kind());
    }
    if (bytes <= 4) {
      return Type::keys32;
    }
    if (bytes <= 8) {
      return Type::keys64;
    }
    if (bytes <= 16) {
      return Type::keys128;
    }
    if (bytes <= 32) {
      return Type::keys256;
    }
    // Fixed keys totaling more than 32 bytes fall back to hashed.
    return Type::hashed;
  }

  // Single VARCHAR key.
  if (keyTypes.size() == 1 && keyTypes[0]->kind() == TypeKind::VARCHAR) {
    return Type::key_string;
  }

  // Multiple strings, a string+int mix, or any other combination.
  return Type::hashed;
}

} // namespace facebook::velox::exec::ch
