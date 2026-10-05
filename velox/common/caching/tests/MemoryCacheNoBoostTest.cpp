// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0
#include <map>
#include <mutex>
#include "velox/common/caching/MemoryCache.h"

// Caller-owned adapter: only stdlib storage, independent of Boost.Unordered.
template <class K, class V>
class StandardMap {
  mutable std::mutex mutex_;
  std::map<K, V> values_;

 public:
  template <class F>
  size_t cvisit(const K& key, F&& f) const {
    std::lock_guard lock(mutex_);
    auto it = values_.find(key);
    if (it == values_.end())
      return 0;
    f(*it);
    return 1;
  }
  template <class F>
  size_t visit(const K& key, F&& f) {
    std::lock_guard lock(mutex_);
    auto it = values_.find(key);
    if (it == values_.end())
      return 0;
    f(*it);
    return 1;
  }
  template <class F>
  void cvisit_all(F&& f) const {
    std::lock_guard lock(mutex_);
    for (auto& item : values_)
      f(item);
  }
  template <class F>
  size_t erase_if(const K& key, F&& f) {
    std::lock_guard lock(mutex_);
    auto it = values_.find(key);
    if (it == values_.end() || !f(*it))
      return 0;
    values_.erase(it);
    return 1;
  }
  bool try_emplace(const K& key, const V& value) {
    std::lock_guard lock(mutex_);
    return values_.try_emplace(key, value).second;
  }
  size_t size() const {
    std::lock_guard lock(mutex_);
    return values_.size();
  }
};
int main() {
  using C = facebook::velox::MemoryCache<int, int, StandardMap>;
  C::Options o;
  o.sizeLimit = 10;
  o.trackStatistics = true;
  C c(o);
  C::EntryOptions e;
  e.size = 1;
  auto value = std::make_shared<int>(42);
  c.set(1, value, e);
  C::ValuePtr result;
  if (!c.tryGetValue(1, result) || result != value)
    return 1;
  c.compact(1);
  return c.count() != 0;
}
