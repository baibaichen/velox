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

#include <boost/unordered/concurrent_flat_map.hpp>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/LifoSemMPMCQueue.h>
#include <gtest/gtest.h>
#include <atomic>
#include <future>
#include <memory>
#include <string>

namespace {
std::atomic<bool> failAllocation{false};
template <typename T>
struct Allocator : std::allocator<T> {
  using value_type = T;
  template <typename U>
  struct rebind {
    using other = Allocator<U>;
  };
  Allocator() = default;
  template <typename U>
  Allocator(const Allocator<U>&) {}
  T* allocate(size_t n) {
    if (failAllocation) {
      throw std::bad_alloc();
    }
    return std::allocator<T>::allocate(n);
  }
};
using Ptr = std::shared_ptr<int>;
using Map = boost::concurrent_flat_map<
    std::string,
    Ptr,
    boost::hash<std::string>,
    std::equal_to<std::string>,
    Allocator<std::pair<const std::string, Ptr>>>;

TEST(MemoryCacheInfrastructureTest, growthAllocationFailurePreservesMapping) {
  Map map;
  auto value = std::make_shared<int>(42);
  size_t failures = 0;
  for (int i = 0; i < 1024; ++i) {
    const auto key = std::to_string(i);
    const auto before = map.size();
    failAllocation = true;
    bool failed = false;
    try {
      map.try_emplace(key, value);
    } catch (const std::bad_alloc&) {
      failed = true;
    }
    failAllocation = false;
    if (failed) {
      ++failures;
      ASSERT_EQ(map.size(), before);
      EXPECT_EQ(map.cvisit(key, [](const auto&) {}), 0);
      map.try_emplace(key, value);
    }
    for (int j = 0; j <= i; ++j) {
      ASSERT_EQ(
          map.cvisit(
              std::to_string(j),
              [&](const auto& item) { EXPECT_EQ(item.second, value); }),
          1);
    }
  }
  EXPECT_GT(failures, 1);
}

TEST(MemoryCacheInfrastructureTest, conditionalIdentityAndVisitorFailure) {
  Map map;
  auto old = std::make_shared<int>(1);
  auto replacement = std::make_shared<int>(2);
  map.try_emplace("key", old);
  EXPECT_THROW(
      map.visit("key", [](auto&) { throw std::bad_alloc(); }), std::bad_alloc);
  EXPECT_THROW(
      map.erase_if("key", [](const auto&) -> bool { throw std::bad_alloc(); }),
      std::bad_alloc);
  EXPECT_EQ(
      map.erase_if(
          "key", [&](const auto& item) { return item.second == replacement; }),
      0);
  Ptr retired;
  EXPECT_EQ(
      map.visit(
          "key",
          [&](auto& item) noexcept {
            if (item.second == old) {
              retired = std::exchange(item.second, replacement);
            }
          }),
      1);
  EXPECT_EQ(retired, old);
  EXPECT_EQ(
      map.erase_if("key", [&](const auto& item) { return item.second == old; }),
      0);
  EXPECT_EQ(
      map.erase_if(
          "key", [&](const auto& item) { return item.second == replacement; }),
      1);
}

TEST(MemoryCacheInfrastructureTest, boundedExecutorRejectsBeforePublication) {
  using Pool = folly::CPUThreadPoolExecutor;
  auto queue = std::make_unique<folly::LifoSemMPMCQueue<Pool::CPUTask>>(2);
  const auto capacity = queue->capacity();
  Pool pool(std::pair<size_t, size_t>{1, 1}, std::move(queue));
  std::promise<void> started;
  std::promise<void> release;
  auto released = release.get_future().share();
  pool.add([&] {
    started.set_value();
    released.wait();
  });
  started.get_future().wait();
  std::atomic<size_t> executed{0};
  std::promise<void> drained;
  for (size_t i = 0; i < capacity; ++i) {
    pool.add([&] {
      if (++executed == capacity)
        drained.set_value();
    });
  }
  bool rejected = false;
  try {
    pool.add([&] { executed += 10000; });
  } catch (const folly::QueueFullException&) {
    rejected = true;
  }
  // Unblock before any assertion can end the test or destruction joins workers.
  release.set_value();
  drained.get_future().wait();
  pool.join();
  EXPECT_TRUE(rejected);
  EXPECT_EQ(executed, capacity);
}
} // namespace

namespace {
struct CopyKey {
  std::string value;
  static inline bool failCopy = false;
  CopyKey(std::string v) : value(std::move(v)) {}
  CopyKey(const CopyKey& other) : value(other.value) {
    if (failCopy)
      throw std::bad_alloc();
  }
  CopyKey(CopyKey&&) noexcept = default;
  bool operator==(const CopyKey& other) const noexcept {
    return value == other.value;
  }
};
struct KeyHash {
  size_t operator()(const CopyKey& key) const noexcept {
    return std::hash<std::string>{}(key.value);
  }
};
TEST(MemoryCacheInfrastructureTest, keyCopyFailureAndNoThrowMigration) {
  static_assert(std::is_nothrow_move_constructible_v<std::string>);
  static_assert(std::is_nothrow_move_constructible_v<std::shared_ptr<int>>);
  boost::concurrent_flat_map<CopyKey, Ptr, KeyHash> map;
  auto held = std::make_shared<int>(42);
  CopyKey first("first"), next("next");
  map.try_emplace(first, held);
  CopyKey::failCopy = true;
  bool failed = false;
  try {
    map.try_emplace(next, held);
  } catch (const std::bad_alloc&) {
    failed = true;
  }
  CopyKey::failCopy = false;
  EXPECT_TRUE(failed);
  EXPECT_EQ(map.size(), 1);
  EXPECT_EQ(
      map.cvisit(
          first, [&](const auto& item) { EXPECT_EQ(item.second, held); }),
      1);
  EXPECT_TRUE(map.try_emplace(next, held));
  for (int i = 0; i < 1000; ++i)
    map.try_emplace(CopyKey(std::to_string(i)), held);
  EXPECT_EQ(map.size(), 1002);
}
} // namespace

namespace {
TEST(MemoryCacheInfrastructureTest, acceptedKeepAliveDrainsBeforePoolShutdown) {
  using Pool = folly::CPUThreadPoolExecutor;
  auto pool = std::make_unique<Pool>(
      std::pair<size_t, size_t>{1, 1},
      std::make_unique<folly::LifoSemMPMCQueue<Pool::CPUTask>>(2));
  std::promise<void> started, release;
  auto released = release.get_future().share();
  pool->add([&, keep = folly::getKeepAliveToken(pool.get())] {
    started.set_value();
    released.wait();
  });
  started.get_future().wait();
  std::atomic<int> executed{0};
  for (int i = 0; i < 2; ++i)
    pool->add([&, keep = folly::getKeepAliveToken(pool.get())] { ++executed; });
  std::thread close([&] { pool.reset(); });
  release.set_value();
  close.join();
  EXPECT_EQ(executed, 2);
}
} // namespace
