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

#include "velox/exec/ch/Common/Arena.h"

#include <memory>
#include <string_view>
#include <utility>

namespace facebook::velox::exec::ch2 {

template <typename Key>
FOLLY_ALWAYS_INLINE Key& keyHolderGetKey(Key&& key) {
  return key;
}

template <typename Key>
FOLLY_ALWAYS_INLINE void keyHolderPersistKey(Key&&) {}

template <typename Key>
FOLLY_ALWAYS_INLINE void keyHolderDiscardKey(Key&&) {}

struct ArenaKeyHolder {
  std::string_view key;
  ch::Arena& pool;
  std::unique_ptr<char[]> holder{};

  ArenaKeyHolder(
      std::string_view keyValue,
      ch::Arena& arena,
      std::unique_ptr<char[]> owned = {})
      : key(keyValue), pool(arena), holder(std::move(owned)) {}
};

FOLLY_ALWAYS_INLINE std::string_view& keyHolderGetKey(ArenaKeyHolder& holder) {
  return holder.key;
}

FOLLY_ALWAYS_INLINE void keyHolderPersistKey(ArenaKeyHolder& holder) {
  holder.key = std::string_view{
      holder.pool.insert(holder.key.data(), holder.key.size()),
      holder.key.size()};
}

FOLLY_ALWAYS_INLINE void keyHolderDiscardKey(ArenaKeyHolder&) {}

inline void keyPrefetch(std::string_view key) {
  constexpr size_t kCacheLineSize = 64;
  constexpr size_t kMaxPrefetch = kCacheLineSize * 4;
  const auto size = std::min(key.size(), kMaxPrefetch);
  for (const char* ptr = key.data(); ptr < key.data() + size;
       ptr += kCacheLineSize) {
    __builtin_prefetch(ptr);
  }
}

} // namespace facebook::velox::exec::ch2
