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

#include "velox/exec/ch/HashedKey.h"
#include "velox/exec/ch/HashMap.h"

#include <optional>
#include <variant>

namespace facebook::velox::exec::ch {

class FixedKeyMap {
 public:
  using Map64 = HashMapAll_key64;
  using Map128 = HashMapAll_keys128;
  using Map256 = HashMapAll_keys256;
  using SerializedMap = HashMapAll_serialized;
  using HashedMap = HashMapAll_hashed;

  explicit FixedKeyMap(memory::MemoryPool* pool)
      : FixedKeyMap(pool, FixedKeyWidth::k64) {}

  FixedKeyMap(memory::MemoryPool* pool, FixedKeyWidth width)
      : width_(width), maps_(makeFixedMap(pool, width)) {}

  FixedKeyMap(memory::MemoryPool* pool, std::vector<TypePtr> keyTypes)
      : FixedKeyMap(
            pool, std::move(keyTypes), ArbitraryKeyMode::kSerialized) {}

  FixedKeyMap(
      memory::MemoryPool* pool,
      std::vector<TypePtr> keyTypes,
      ArbitraryKeyMode arbitraryMode)
      : keyTypes_(std::move(keyTypes)),
        width_(useSerializedKey(keyTypes_)
                   ? std::nullopt
                   : std::optional<FixedKeyWidth>(fixedKeyWidth(keyTypes_))),
        arbitraryMode_(arbitraryMode),
        maps_(makeMap(pool, width_, arbitraryMode_)) {}

  bool serialized() const {
    return !width_.has_value() &&
        arbitraryMode_ == ArbitraryKeyMode::kSerialized;
  }

  bool hashed() const {
    return !width_.has_value() && arbitraryMode_ == ArbitraryKeyMode::kHashed;
  }

  FixedKeyWidth width() const {
    VELOX_CHECK(width_.has_value(), "Arbitrary map has no fixed key width");
    return *width_;
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
    return serializedMap().emplace(key);
  }

  RowRefList& emplaceHashed(const UInt128& key) {
    return hashedMap().emplace(key);
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

  SerializedMap::LookupResult find(const StringRef& key) {
    return serializedMap().find(key);
  }

  SerializedMap::ConstLookupResult find(const StringRef& key) const {
    return serializedMap().find(key);
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

 private:
  using Maps =
      std::variant<Map64, Map128, Map256, SerializedMap, HashedMap>;

  static Maps makeFixedMap(memory::MemoryPool* pool, FixedKeyWidth width) {
    switch (width) {
      case FixedKeyWidth::k64:
        return Maps(std::in_place_type<Map64>, pool);
      case FixedKeyWidth::k128:
        return Maps(std::in_place_type<Map128>, pool);
      case FixedKeyWidth::k256:
        return Maps(std::in_place_type<Map256>, pool);
    }
    VELOX_UNREACHABLE();
  }

  static Maps makeMap(
      memory::MemoryPool* pool,
      const std::optional<FixedKeyWidth>& width,
      ArbitraryKeyMode arbitraryMode) {
    if (width.has_value()) {
      return makeFixedMap(pool, *width);
    }
    return arbitraryMode == ArbitraryKeyMode::kHashed
        ? Maps(std::in_place_type<HashedMap>, pool)
        : Maps(std::in_place_type<SerializedMap>, pool);
  }

  template <typename Key>
  const auto& map() const {
    if constexpr (std::is_same_v<Key, uint64_t>) {
      return map64();
    } else if constexpr (std::is_same_v<Key, UInt128>) {
      return map128();
    } else if constexpr (std::is_same_v<Key, UInt256>) {
      return map256();
    } else {
      static_assert(std::is_same_v<Key, StringRef>);
      return serializedMap();
    }
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
  SerializedMap& serializedMap() {
    return std::get<SerializedMap>(maps_);
  }
  const SerializedMap& serializedMap() const {
    return std::get<SerializedMap>(maps_);
  }
  HashedMap& hashedMap() {
    return std::get<HashedMap>(maps_);
  }
  const HashedMap& hashedMap() const {
    return std::get<HashedMap>(maps_);
  }

  std::vector<TypePtr> keyTypes_;
  std::optional<FixedKeyWidth> width_;
  ArbitraryKeyMode arbitraryMode_{ArbitraryKeyMode::kSerialized};
  Maps maps_;
};

} // namespace facebook::velox::exec::ch
