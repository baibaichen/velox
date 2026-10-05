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

#include "velox/common/caching/MemoryCache.h"
#include <gtest/gtest.h>
#include <atomic>
#include <thread>

using namespace facebook::velox;
using namespace std::chrono_literals;
template <class... T>
using AlternateMap = boost::concurrent_flat_map<T...>;

TEST(MemoryCacheTest, capacityExpirationAndLifetime) {
  MemoryCache<std::string, std::string, AlternateMap> cache(1);
  auto old = std::make_shared<const std::string>("old");
  auto fresh = std::make_shared<const std::string>("new");
  EXPECT_TRUE(cache.put("key", old));
  auto held = cache.get("key");
  EXPECT_FALSE(cache.put("other", fresh));
  EXPECT_TRUE(cache.put("key", fresh));
  EXPECT_EQ(cache.get("key"), fresh);
  EXPECT_EQ(*held, "old");
  EXPECT_TRUE(cache.put("key", fresh, 0ns));
  EXPECT_EQ(cache.get("key"), nullptr);
  EXPECT_EQ(cache.size(), 0);
  EXPECT_TRUE(cache.put("key", fresh, -1ns));
  EXPECT_EQ(cache.pruneExpired(), 1);
  EXPECT_TRUE(cache.put("key", fresh, 1h));
  EXPECT_EQ(cache.pruneExpired(), 0);
  cache.clear();
  EXPECT_EQ(cache.size(), 0);
  EXPECT_EQ(*held, "old");
  EXPECT_FALSE(cache.erase("missing"));
  EXPECT_THROW(cache.put("key", nullptr), std::invalid_argument);
  MemoryCache<int, int> zero(0);
  EXPECT_FALSE(zero.put(1, std::make_shared<const int>(1)));
}

TEST(MemoryCacheTest, concurrentMutationAndClear) {
  MemoryCache<int, int> cache(32);
  std::atomic<bool> valid{true};
  std::vector<std::thread> threads;
  for (int w = 0; w < 8; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < 3000; ++i) {
        int key = (i + w) % 64;
        cache.put(key, std::make_shared<const int>(key));
        auto value = cache.get(key);
        if ((value && *value != key) || cache.size() > 32) valid = false;
        if (i % 7 == 0) cache.erase(key);
        if (i % 43 == 0) cache.clear();
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_TRUE(valid);
  cache.clear();
  for (int i = 0; i < 32; ++i) EXPECT_TRUE(cache.put(i, std::make_shared<const int>(i)));
  EXPECT_FALSE(cache.put(33, std::make_shared<const int>(33)));
}

TEST(MemoryCacheTest, valueDestructionOutsideLocks) {
  MemoryCache<int, int> cache(2);
  int destroyed = 0;
  auto make = [&] {
    return std::shared_ptr<const int>(new int(1), [&](const int* p) {
      cache.erase(99); // Would deadlock if invoked under the writer lock.
      ++destroyed;
      delete p;
    });
  };
  cache.put(1, make());
  cache.put(1, make());
  EXPECT_EQ(destroyed, 1);
  cache.erase(1);
  EXPECT_EQ(destroyed, 2);
  cache.put(1, make(), 0ns);
  cache.pruneExpired();
  EXPECT_EQ(destroyed, 3);
  cache.put(1, make());
  cache.clear();
  EXPECT_EQ(destroyed, 4);
}
