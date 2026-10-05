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

#include <boost/unordered/concurrent_flat_map.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace facebook::velox {

/// Bounded object cache. Full caches reject new keys; replacing existing keys
/// is allowed. Expiration uses steady_clock and is checked lazily on get().
/// Map must provide Boost-compatible visit, cvisit, cvisit_all, try_emplace,
/// erase_if(key, predicate), size and clear with concurrent-reader safety.
/// Cache destruction requires all operations to have completed. Returned values
/// remain valid independently of the cache. Null values are not accepted.
template <
    typename Key,
    typename Value,
    template <typename...> class Map = boost::concurrent_flat_map>
class MemoryCache {
 public:
  using Clock = std::chrono::steady_clock;
  using ValuePtr = std::shared_ptr<const Value>;

  explicit MemoryCache(size_t maxEntries) : maxEntries_(maxEntries) {}

  /// nullopt means no expiration; nonpositive TTL expires immediately.
  /// TTL starts when put() is called, including time waiting for a writer.
  bool put(
      const Key& key,
      ValuePtr value,
      std::optional<Clock::duration> ttl = std::nullopt) {
    if (!value) {
      throw std::invalid_argument("MemoryCache does not accept null values");
    }
    auto deadline = Clock::time_point::max();
    if (ttl) {
      const auto now = Clock::now();
      if (*ttl <= Clock::duration::zero()) {
        deadline = now;
      } else if (*ttl < Clock::time_point::max() - now) {
        deadline = now + *ttl;
      }
    }
    auto entry = std::make_shared<const Entry>(Entry{std::move(value), deadline});
    // Declared before the lock so user value destructors run after unlocking.
    EntryPtr retired;
    std::lock_guard lock(writeMutex_);
    if (entries_.visit(key, [&](auto& item) {
          retired = std::exchange(item.second, entry);
        })) {
      return true;
    }
    if (entries_.size() >= maxEntries_) {
      return false;
    }
    return entries_.try_emplace(key, entry);
  }

  ValuePtr get(const Key& key) {
    EntryPtr entry;
    entries_.cvisit(key, [&](const auto& item) { entry = item.second; });
    if (!entry) {
      return nullptr;
    }
    if (entry->expiresAt <= Clock::now()) {
      std::lock_guard lock(writeMutex_);
      entries_.erase_if(key, [&](const auto& item) {
        return item.second == entry;
      });
      return nullptr;
    }
    return entry->value;
  }

  bool erase(const Key& key) {
    EntryPtr retired;
    std::lock_guard lock(writeMutex_);
    return entries_.erase_if(key, [&](const auto& item) {
      retired = item.second;
      return true;
    }) != 0;
  }

  /// Reclaims expired entries. Expired entries consume slots until reclaimed.
  size_t pruneExpired() {
    std::vector<std::pair<Key, EntryPtr>> retired;
    std::lock_guard lock(writeMutex_);
    const auto now = Clock::now();
    entries_.cvisit_all([&](const auto& item) {
      if (item.second->expiresAt <= now) {
        retired.emplace_back(item.first, item.second);
      }
    });
    for (const auto& [key, entry] : retired) {
      entries_.erase_if(key, [&](const auto& item) {
        return item.second == entry;
      });
    }
    return retired.size();
  }

  void clear() {
    std::vector<EntryPtr> retired;
    std::lock_guard lock(writeMutex_);
    retired.reserve(entries_.size());
    entries_.cvisit_all([&](const auto& item) { retired.push_back(item.second); });
    entries_.clear();
  }

  /// Includes expired entries not yet reclaimed; concurrent diagnostic snapshot.
  size_t size() const {
    return entries_.size();
  }

 private:
  struct Entry {
    ValuePtr value;
    Clock::time_point expiresAt;
  };
  using EntryPtr = std::shared_ptr<const Entry>;
  const size_t maxEntries_;
  // ponytail: serialize mutations for exact capacity and clear semantics;
  // shard capacity accounting only if measured write contention warrants it.
  std::mutex writeMutex_;
  Map<Key, EntryPtr> entries_;
};

} // namespace facebook::velox
