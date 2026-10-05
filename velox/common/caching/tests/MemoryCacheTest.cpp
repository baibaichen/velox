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

// Assertions adapted from .NET Foundation and Contributors (MIT);
// see ../MemoryCache.LICENSE.
#include "velox/common/caching/MemoryCache.h"
#include <boost/unordered/concurrent_flat_map.hpp>
#include <gtest/gtest.h>
#include <atomic>
#include <barrier>
#include <filesystem>
#include <fstream>
#include <future>
#include <random>
#include <sstream>
#include <thread>
#include "velox/common/base/tests/StatsReporterUtils.h"
using namespace facebook::velox;
using namespace std::chrono_literals;
using Cache = MemoryCache<std::string, int, boost::concurrent_flat_map>;
using Duration = Cache::Duration;
using TimePoint = Cache::TimePoint;

struct MemoryCacheTest : testing::Test {
  std::atomic<int64_t> time{100000000};
  Cache::Options options(bool limit = false) {
    Cache::Options o;
    o.clock = [this] { return TimePoint(Duration(time.load())); };
    o.trackStatistics = true;
    if (limit)
      o.sizeLimit = 10;
    return o;
  }
};

TEST_F(MemoryCacheTest, basicInterfacesAndOwnership) {
  Cache cache(options());
  auto value = std::make_shared<int>(1);
  auto result = value;
  EXPECT_FALSE(cache.tryGetValue("missing", result));
  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(cache.set("key", value), value);
  EXPECT_TRUE(cache.tryGetValue("key", result));
  EXPECT_EQ(result, value);
  EXPECT_FALSE(cache.tryGetValue("KEY", result));
  cache.set("key", nullptr);
  EXPECT_TRUE(cache.tryGetValue("key", result));
  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"key"});
  cache.remove("key");
  cache.remove("missing");
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(*value, 1);
  EXPECT_EQ(cache.getCurrentStatistics()->totalHits, 2);
  EXPECT_EQ(cache.getCurrentStatistics()->totalMisses, 2);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 0);
  EXPECT_FALSE(cache.getCurrentStatistics()->currentEstimatedSize);
}

TEST_F(MemoryCacheTest, validationAndDefaults) {
  EXPECT_EQ(Cache::Options{}.name, "Default");
  Cache defaults;
  EXPECT_FALSE(defaults.getCurrentStatistics());
  auto o = options(true);
  o.sizeLimit = -1;
  EXPECT_THROW(Cache{o}, std::invalid_argument);
  o.sizeLimit = 0;
  Cache zero(o);
  Cache::EntryOptions e;
  e.size = 0;
  zero.set("k", nullptr, e);
  EXPECT_EQ(zero.count(), 1);
  EXPECT_EQ(zero.getCurrentStatistics()->currentEstimatedSize, 0);
  Cache cache(options(true));
  EXPECT_THROW(cache.set("k", nullptr), std::logic_error);
  e.size = -1;
  EXPECT_THROW(cache.set("k", nullptr, e), std::invalid_argument);
  for (auto duration : {Duration(-1), Duration(0)}) {
    e = {};
    e.slidingExpiration = duration;
    EXPECT_THROW(defaults.set("k", nullptr, e), std::invalid_argument);
    e = {};
    e.absoluteExpirationRelativeToNow = duration;
    EXPECT_THROW(defaults.set("k", nullptr, e), std::invalid_argument);
  }
  for (double p : {-0.1, 1.1, double(INFINITY), -double(INFINITY)}) {
    o = options();
    o.compactionPercentage = p;
    EXPECT_THROW(Cache{o}, std::invalid_argument);
  }
}

TEST_F(MemoryCacheTest, capacityReplacementAndClear) {
  for (int size : {6, 5, 2}) {
    Cache cache(options(true));
    Cache::EntryOptions e;
    e.size = 6;
    cache.set("k", nullptr, e);
    e.size = 4;
    cache.set("q", nullptr, e);
    e.size = size;
    cache.set("k", nullptr, e);
    EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, size + 4);
    EXPECT_EQ(cache.count(), 2);
    cache.remove("k");
    EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 4);
    cache.clear();
    EXPECT_EQ(cache.count(), 0);
    EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  }
}

TEST_F(MemoryCacheTest, absoluteRelativeSlidingAndOptionsCopy) {
  Cache cache(options());
  Cache::ValuePtr result;
  Cache::EntryOptions e;
  e.absoluteExpirationRelativeToNow = Duration(20);
  e.absoluteExpiration = TimePoint(Duration(time + 10));
  e.slidingExpiration = Duration(5);
  cache.set("k", nullptr, e);
  e.absoluteExpiration = TimePoint::max();
  time += 4;
  EXPECT_TRUE(cache.tryGetValue("k", result));
  time += 4;
  EXPECT_TRUE(cache.tryGetValue("k", result));
  time += 2;
  EXPECT_FALSE(cache.tryGetValue("k", result));
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
  e = {};
  e.slidingExpiration = Duration(5);
  cache.set("k", nullptr, e);
  time += 5;
  EXPECT_FALSE(cache.tryGetValue("k", result));
  e = {};
  e.absoluteExpiration = TimePoint(Duration(time));
  cache.set("past", nullptr, e);
  EXPECT_EQ(cache.count(), 0);
}

TEST_F(MemoryCacheTest, priorityLruAndExpiredNeverRemove) {
  Cache cache(options());
  Cache::EntryOptions e;
  cache.set("a", nullptr);
  ++time;
  cache.set("b", nullptr);
  ++time;
  Cache::ValuePtr result;
  EXPECT_TRUE(cache.tryGetValue("a", result));
  cache.compact(0.5);
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"a"});
  e.priority = Cache::Priority::NeverRemove;
  e.absoluteExpiration = TimePoint(Duration(time + 1));
  cache.set("never", nullptr, e);
  cache.compact(1);
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"never"});
  ++time;
  cache.compact(0);
  EXPECT_EQ(cache.count(), 0);
}

TEST_F(MemoryCacheTest, concurrentSetRemoveClear) {
  Cache cache(options());
  std::atomic<bool> valid{true};
  std::vector<std::thread> threads;
  for (int w = 0; w < 8; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < 10000; ++i) {
        auto k = std::to_string((i + w) % 16);
        cache.set(k, std::make_shared<int>(1));
        Cache::ValuePtr value;
        if (cache.tryGetValue(k, value) && (!value || *value != 1))
          valid = false;
        if (i % 7 == 0)
          cache.remove(k);
        if (i % 101 == 0)
          cache.clear();
      }
    });
  }
  for (auto& t : threads)
    t.join();
  EXPECT_TRUE(valid);
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, testValueEnabledAndStalePriorCapacityEvidence) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 6;
  cache.set("K", nullptr, e);
  bool called = false;
  {
    ScopedTestValue hook(
        "MemoryCache::set::prior", std::function<void(void*)>([&](void*) {
          if (called)
            return;
          called = true;
          cache.remove("K");
          Cache::EntryOptions q;
          q.size = 10;
          cache.set("Q", nullptr, q);
        }));
    cache.set("K", nullptr, e);
  }
  EXPECT_TRUE(called);
  EXPECT_EQ(cache.count(), 2);
  // Fixed C# source admits the stale replacement as an insert; no drift.
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 16);
  cache.remove("K");
  cache.remove("Q");
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
}

TEST_F(MemoryCacheTest, reservedFailureAndClearDoNotUndoOtherOperations) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 3;
  bool called = false;
  {
    ScopedTestValue hook(
        "MemoryCache::set::reserved", std::function<void(void*)>([&](void*) {
          if (called)
            return;
          called = true;
          cache.set("other", nullptr, e);
          throw std::bad_alloc();
        }));
    EXPECT_THROW(cache.set("failed", nullptr, e), std::bad_alloc);
  }
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"other"});
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 3);
  called = false;
  {
    ScopedTestValue hook(
        "MemoryCache::set::reserved", std::function<void(void*)>([&](void*) {
          if (called)
            return;
          called = true;
          cache.clear();
          cache.set("new", nullptr, e);
          throw std::bad_alloc();
        }));
    EXPECT_THROW(cache.set("old", nullptr, e), std::bad_alloc);
  }
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"new"});
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 3);
}

TEST_F(MemoryCacheTest, candidateRemovalDoesNotDoubleCountEvictions) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 3;
  cache.set("a", nullptr, e);
  {
    ScopedTestValue hook(
        "MemoryCache::compact::candidates",
        std::function<void(void*)>([&](void*) { cache.remove("a"); }));
    cache.compact(1);
  }
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 0);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
}
#endif

namespace {
// Single shared worker: a FIFO fence completes after all earlier tasks released
// their task bodies. No sleeps and no public cache drain API.
void drainMaintenance() {
  std::promise<void> done;
  for (;;) {
    try {
      detail::memoryCacheExecutor().add([&] { done.set_value(); });
      break;
    } catch (const folly::QueueFullException&) {
      std::this_thread::yield();
    }
  }
  done.get_future().wait();
}
} // namespace

TEST_F(MemoryCacheTest, scanBoundaryAndMissingOperations) {
  auto o = options();
  o.expirationScanFrequency = Duration(10);
  Cache cache(o);
  Cache::EntryOptions e;
  e.absoluteExpirationRelativeToNow = Duration(1);
  cache.set("expired", nullptr, e);
  time += 10;
  cache.remove("missing");
  drainMaintenance();
  EXPECT_EQ(cache.count(), 1);
  ++time;
  cache.count();
  cache.keys();
  cache.getCurrentStatistics();
  drainMaintenance();
  EXPECT_EQ(cache.count(), 1);
  cache.remove("missing");
  drainMaintenance();
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
}

TEST_F(MemoryCacheTest, capacityRejectionAndBackgroundLowWatermark) {
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 6;
  cache.set("old", nullptr, e);
  e.size = 5;
  cache.set("other", nullptr, e);
  drainMaintenance();
  EXPECT_EQ(cache.count(), 1);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 6);
  e.size = 11;
  cache.set("old", nullptr, e);
  drainMaintenance();
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  cache.clear();
  for (int size : {3, 3, 4}) {
    e.size = size;
    cache.set(std::to_string(time++), nullptr, e);
  }
  e.size = 1;
  cache.set("rejected", nullptr, e);
  drainMaintenance();
  // 5% of 10 rounds down to zero: low watermark remains 10.
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 10);
}

TEST_F(MemoryCacheTest, extremesAndRelativeOverflow) {
  auto o = options(true);
  o.sizeLimit = INT64_MAX;
  Cache cache(o);
  Cache::EntryOptions e;
  e.size = INT64_MAX;
  cache.set("k", nullptr, e);
  cache.set("k", nullptr, e);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, INT64_MAX);
  cache.remove("k");
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  e = {};
  e.size = 0;
  e.absoluteExpirationRelativeToNow = Duration(1);
  time = 2534023007999999999LL;
  EXPECT_THROW(cache.set("overflow", nullptr, e), std::out_of_range);
  EXPECT_EQ(cache.count(), 0);
  for (double p :
       {-1.0, 0.0, 2.0, double(NAN), double(INFINITY), -double(INFINITY)}) {
    Cache c(options());
    c.set("k", nullptr);
    c.compact(p);
    EXPECT_EQ(c.count(), p > 0 ? 0 : 1);
  }
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, destructionDoesNotWaitForAcceptedMaintenance) {
  using namespace common::testutil;
  TestValue::enable();
  std::promise<void> started, release;
  auto released = release.get_future().share();
  std::weak_ptr<int> weak;
  {
    ScopedTestValue hook(
        "MemoryCache::maintenance::start",
        std::function<void(void*)>([&](void*) {
          started.set_value();
          released.wait();
        }));
    auto o = options();
    o.expirationScanFrequency = Duration(1);
    auto cache = std::make_unique<Cache>(o);
    auto value = std::make_shared<int>(42);
    weak = value;
    cache->set("k", std::move(value));
    time += 2;
    cache->remove("missing");
    started.get_future().wait();
    cache.reset();
    bool held = !weak.expired();
    release.set_value();
    drainMaintenance();
    EXPECT_TRUE(held);
  }
  drainMaintenance();
  EXPECT_TRUE(weak.expired());
}

TEST_F(MemoryCacheTest, keysCollectionOverlapsMutation) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options());
  cache.set("old", nullptr);
  std::promise<void> started, release;
  auto released = release.get_future().share();
  std::atomic<bool> first{true};
  std::vector<std::string> keys;
  {
    ScopedTestValue hook(
        "MemoryCache::keys::collect", std::function<void(void*)>([&](void*) {
          if (first.exchange(false)) {
            started.set_value();
            released.wait();
          }
        }));
    std::thread collector([&] { keys = cache.keys(); });
    started.get_future().wait();
    std::promise<void> mutating;
    std::thread writer([&] {
      mutating.set_value();
      cache.remove("old");
      cache.set("new", nullptr);
    });
    mutating.get_future().wait();
    release.set_value();
    collector.join();
    writer.join();
  }
  EXPECT_EQ(keys, std::vector<std::string>{"old"});
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"new"});
}
#endif

TEST_F(MemoryCacheTest, fullConcurrentSizeTrackingOuterLoop) {
  auto o = options(true);
  o.sizeLimit = 200LL * 1024 * 1024;
  o.clock = [] {
    return std::chrono::time_point_cast<Duration>(
        std::chrono::system_clock::now());
  };
  Cache cache(o);
  std::atomic<bool> valid{true};
  std::vector<std::thread> threads;
  const auto n =
      std::min(16u, std::max(4u, std::thread::hardware_concurrency()));
  auto payload = std::make_shared<int>(1);
  for (unsigned w = 0; w < n; ++w)
    threads.emplace_back([&, w] {
      std::mt19937 rng(w + 1);
      for (int i = 0; i < 200000; ++i) {
        auto key = "k" + std::to_string(rng() % 16);
        auto op = rng() % 100;
        if (op < 65) {
          Cache::EntryOptions e;
          e.size = 4096;
          e.absoluteExpirationRelativeToNow = 15ms;
          cache.set(key, payload, e);
        } else if (op < 85) {
          Cache::ValuePtr value;
          if (cache.tryGetValue(key, value) && value != payload)
            valid = false;
        } else
          cache.remove(key);
        if ((i & 1023) == 0 &&
            *cache.getCurrentStatistics()->currentEstimatedSize < 0)
          valid = false;
      }
    });
  for (auto& t : threads)
    t.join();
  for (int k = 0; k < 16; ++k)
    cache.remove("k" + std::to_string(k));
  drainMaintenance();
  EXPECT_TRUE(valid);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  Cache::EntryOptions e;
  e.size = 4096;
  int retained = 0;
  for (int i = 0; i < 512; ++i) {
    auto key = "fresh-" + std::to_string(i);
    cache.set(key, payload, e);
    Cache::ValuePtr value;
    retained += cache.tryGetValue(key, value);
  }
  EXPECT_EQ(retained, 512);
}

namespace {
class ThrowingReporter : public test::TestReporter {
 public:
  mutable int fail{0};
  void addMetricValue(const std::string& key, size_t value) const override {
    if (key.ends_with(".hits") && fail == 1)
      throw std::runtime_error("before");
    test::TestReporter::addMetricValue(key, value);
    if (key.ends_with(".hits") && fail == 2)
      throw std::runtime_error("after");
  }
};
folly::Singleton<BaseStatsReporter> cacheTestReporter([] {
  return new ThrowingReporter;
});
class ReporterEnvironment : public testing::Environment {
 public:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
  }
};
auto* reporterEnvironment =
    testing::AddGlobalTestEnvironment(new ReporterEnvironment);
} // namespace

TEST_F(MemoryCacheTest, reportAttemptsEachIntervalOnce) {
  auto reporter = std::dynamic_pointer_cast<ThrowingReporter>(
      folly::Singleton<BaseStatsReporter>::try_get());
  ASSERT_NE(reporter, nullptr);
  reporter->clear();
  auto cleanup = folly::makeGuard([&] {
    BaseStatsReporter::registered = false;
    reporter->fail = 0;
  });
  Cache cache(options());
  cache.set("k", nullptr);
  Cache::ValuePtr value;
  cache.tryGetValue("k", value);
  BaseStatsReporter::registered = false;
  cache.report();
  BaseStatsReporter::registered = true;
  cache.report();
  EXPECT_EQ(reporter->counterMap["velox.memory_cache.Default.hits"], 0);
  reporter->fail = 1;
  cache.tryGetValue("k", value);
  cache.tryGetValue("missing", value);
  cache.report();
  EXPECT_EQ(reporter->counterMap["velox.memory_cache.Default.misses"], 1);
  reporter->fail = 0;
  cache.report();
  EXPECT_EQ(reporter->counterMap["velox.memory_cache.Default.hits"], 0);
  reporter->fail = 2;
  cache.tryGetValue("k", value);
  cache.report();
  reporter->fail = 0;
  cache.report();
  EXPECT_EQ(reporter->counterMap["velox.memory_cache.Default.hits"], 1);
  EXPECT_EQ(
      reporter->statTypeMap["velox.memory_cache.Default.hits"], StatType::SUM);
  EXPECT_EQ(
      reporter->statTypeMap["velox.memory_cache.Default.entries"],
      StatType::AVG);
  EXPECT_FALSE(reporter->statTypeMap.contains(
      "velox.memory_cache.Default.estimated_size"));
  EXPECT_EQ(cache.getCurrentStatistics()->totalHits, 3);
}

TEST_F(MemoryCacheTest, addAndReplaceEntriesAreThreadSafe) {
  auto o = options(true);
  o.sizeLimit = 20;
  o.compactionPercentage = 0.5;
  o.expirationScanFrequency = Duration::zero();
  o.clock = [] {
    return std::chrono::time_point_cast<Duration>(
        std::chrono::system_clock::now());
  };
  Cache cache(o);
  std::barrier start(3);
  std::atomic<int> failures{0};
  std::vector<std::thread> workers;
  for (int w = 0; w < 3; ++w)
    workers.emplace_back([&, w] {
      std::mt19937 rng(w);
      start.arrive_and_wait();
      for (int i = 0; i < 10000; ++i) {
        int size = rng() % 5;
        Cache::EntryOptions e;
        e.size = size;
        try {
          cache.set(std::to_string(rng() % 10), std::make_shared<int>(size), e);
        } catch (...) {
          ++failures;
        }
      }
    });
  for (auto& w : workers)
    w.join();
  drainMaintenance();
  // No expired entries remain; later scans cannot change this size sum.
  int64_t sum = 0;
  for (int k = 0; k < 10; ++k) {
    Cache::ValuePtr v;
    if (cache.tryGetValue(std::to_string(k), v))
      sum += *v;
  }
  drainMaintenance();
  EXPECT_EQ(failures, 0);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, sum);
  EXPECT_LE(cache.count(), 10);
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, publishedEntrySurvivesSubmissionRejection) {
  using namespace common::testutil;
  TestValue::enable();
  std::promise<void> started, release;
  auto released = release.get_future().share();
  auto& executor = detail::memoryCacheExecutor();
  executor.add([&] {
    started.set_value();
    released.wait();
  });
  started.get_future().wait();
  auto unblock = folly::makeGuard([&] {
    release.set_value();
    drainMaintenance();
  });
  size_t accepted = 0;
  try {
    for (;;) {
      executor.add([] {});
      ++accepted;
    }
  } catch (const folly::QueueFullException&) {
  }
  EXPECT_GT(accepted, 0);
  auto o = options(true);
  o.expirationScanFrequency = Duration(1);
  Cache cache(o);
  time += 2;
  Cache::EntryOptions e;
  e.size = 3;
  EXPECT_THROW(cache.set("published", nullptr, e), folly::QueueFullException);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 3);
  EXPECT_EQ(cache.count(), 1);
  unblock.dismiss();
  release.set_value();
  drainMaintenance();
  cache.remove("missing");
  drainMaintenance();
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 3);
}

TEST_F(MemoryCacheTest, backgroundFailureReleasesGateAndOwnership) {
  using namespace common::testutil;
  TestValue::enable();
  auto o = options();
  o.expirationScanFrequency = Duration(1);
  Cache cache(o);
  Cache::EntryOptions e;
  e.absoluteExpirationRelativeToNow = Duration(1);
  cache.set("expired", nullptr, e);
  int attempts = 0;
  {
    ScopedTestValue hook(
        "MemoryCache::maintenance::start",
        std::function<void(void*)>([&](void*) {
          ++attempts;
          throw std::bad_alloc();
        }));
    time += 2;
    cache.remove("missing");
    drainMaintenance();
  }
  EXPECT_EQ(attempts, 1);
  EXPECT_EQ(cache.count(), 1);
  time += 2;
  cache.remove("missing");
  drainMaintenance();
  EXPECT_EQ(cache.count(), 0);
}
#endif

namespace {
struct MapFaults {
  static inline bool construct = false, insert = false, readSize = false;
  static inline int deletes = -1;
};
template <class K, class V>
class FaultMap : public boost::concurrent_flat_map<K, V> {
 public:
  size_t size() const {
    if (MapFaults::readSize)
      throw std::bad_alloc();
    return boost::concurrent_flat_map<K, V>::size();
  }
  FaultMap() {
    if (MapFaults::construct)
      throw std::bad_alloc();
  }
  template <class... A>
  bool try_emplace(A&&... args) {
    if (MapFaults::insert)
      throw std::bad_alloc();
    return boost::concurrent_flat_map<K, V>::try_emplace(
        std::forward<A>(args)...);
  }
  template <class P>
  size_t erase_if(const K& key, P&& predicate) {
    if (MapFaults::deletes == 0)
      throw std::bad_alloc();
    if (MapFaults::deletes > 0)
      --MapFaults::deletes;
    return boost::concurrent_flat_map<K, V>::erase_if(
        key, std::forward<P>(predicate));
  }
};
} // namespace
TEST(MemoryCacheFaultTest, combinedPublishClearAndPartialCompactionFailures) {
  using C = MemoryCache<int, int, FaultMap>;
  C::Options o;
  o.sizeLimit = 10;
  o.trackStatistics = true;
  C cache(o);
  C::EntryOptions e;
  e.size = 2;
  auto value = std::make_shared<int>(42);
  cache.set(1, value, e);
  auto cleanup = folly::makeGuard([] {
    MapFaults::construct = false;
    MapFaults::insert = false;
    MapFaults::deletes = -1;
  });
  MapFaults::insert = true;
  EXPECT_THROW(cache.set(2, value, e), std::bad_alloc);
  MapFaults::insert = false;
  EXPECT_EQ(cache.count(), 1);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 2);
  MapFaults::construct = true;
  EXPECT_THROW(cache.clear(), std::bad_alloc);
  MapFaults::construct = false;
  EXPECT_EQ(cache.count(), 1);
  cache.set(2, value, e);
  cache.set(3, value, e);
  MapFaults::deletes = 1;
  EXPECT_THROW(cache.compact(1), std::bad_alloc);
  MapFaults::deletes = -1;
  EXPECT_EQ(cache.count(), 2);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 4);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
  cache.compact(0);
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 3);
  EXPECT_EQ(*value, 42);
}

namespace facebook::velox {
struct MemoryCacheTestAccess {
  template <class C>
  static std::string reasons(const C& cache) {
    std::vector<std::pair<int, std::string>> values;
    cache.core_->state.load()->map.cvisit_all([&](const auto& item) {
      values.emplace_back(
          item.first,
          item.second->reason == C::Reason::None ? "None" : "Invalid");
    });
    std::sort(values.begin(), values.end());
    std::string result;
    for (auto& [key, reason] : values) {
      if (!result.empty())
        result += ",";
      result += std::to_string(key) + ":" + reason;
    }
    return result;
  }
};
} // namespace facebook::velox
TEST(MemoryCacheDifferentialTest, fixedSourceTrace) {
  std::ifstream oracle(
      std::filesystem::path(__FILE__).parent_path() /
      "data/memory-cache-trace.txt");
  ASSERT_TRUE(oracle.good());
  using C = MemoryCache<int, int, boost::concurrent_flat_map>;
  int64_t ticks = 0;
  C::Options o;
  o.trackStatistics = true;
  o.sizeLimit = 1000;
  o.clock = [&] { return C::TimePoint(C::Duration(ticks)); };
  C cache(o);
  uint32_t rng = 123;
  auto next = [&] {
    rng = rng * 1664525 + 1013904223;
    return rng;
  };
  for (int i = 0; i < 1000; ++i) {
    ++ticks;
    int op = (next() >> 16) % 8, key = (next() >> 16) % 8,
        v = (next() >> 16) % 1000;
    std::string result = "-";
    if (op < 3) {
      C::EntryOptions e;
      e.size = v % 4;
      e.absoluteExpirationRelativeToNow = C::Duration(1 + v % 20);
      e.priority = C::Priority(v % 4);
      cache.set(key, std::make_shared<int>(v), e);
    } else if (op == 3) {
      C::ValuePtr value;
      result = cache.tryGetValue(key, value) ? std::to_string(*value) : "miss";
    } else if (op == 4)
      cache.remove(key);
    else if (op == 5)
      cache.compact(0.5);
    else if (op == 6)
      ticks += 3;
    else
      cache.clear();
    auto keys = cache.keys();
    std::sort(keys.begin(), keys.end());
    auto stats = cache.getCurrentStatistics();
    std::ostringstream line;
    line << i << "|" << result << "|";
    for (size_t k = 0; k < keys.size(); ++k)
      line << (k ? "," : "") << keys[k];
    line << "|" << stats->totalHits << "|" << stats->totalMisses << "|"
         << stats->totalEvictions << "|" << *stats->currentEstimatedSize << "|"
         << MemoryCacheTestAccess::reasons(cache);
    std::string expected;
    ASSERT_TRUE(bool(std::getline(oracle, expected)));
    ASSERT_EQ(line.str(), expected);
  }
}

TEST_F(MemoryCacheTest, statisticsTheoryWithAndWithoutLimit) {
  for (bool limit : {true, false}) {
    Cache cache(options(limit));
    Cache::EntryOptions e;
    e.size = 2;
    cache.set("key", std::make_shared<int>(1), e);
    Cache::ValuePtr result;
    for (int i = 0; i < 100; ++i) {
      ASSERT_TRUE(cache.tryGetValue("key", result));
      EXPECT_FALSE(cache.tryGetValue("missing1", result));
      EXPECT_FALSE(cache.tryGetValue("missing2", result));
    }
    auto stats = cache.getCurrentStatistics();
    EXPECT_EQ(stats->totalHits, 100);
    EXPECT_EQ(stats->totalMisses, 200);
    EXPECT_EQ(stats->currentEntryCount, 1);
    EXPECT_EQ(
        stats->currentEstimatedSize,
        limit ? std::optional<int64_t>(2) : std::nullopt);
    e.size = 3;
    cache.set("key", std::make_shared<int>(2), e);
    ASSERT_TRUE(cache.tryGetValue("key", result));
    EXPECT_EQ(*result, 2);
    EXPECT_EQ(
        cache.getCurrentStatistics()->currentEstimatedSize,
        limit ? std::optional<int64_t>(3) : std::nullopt);
    // Time adaptation of the source token already invalid at second set.
    e.absoluteExpiration = TimePoint(Duration(time.load()));
    cache.set("key", nullptr, e);
    EXPECT_EQ(cache.count(), 0);
    EXPECT_EQ(
        cache.getCurrentStatistics()->currentEstimatedSize,
        limit ? std::optional<int64_t>(0) : std::nullopt);
  }
}

TEST_F(MemoryCacheTest, expiryAndPrioritySourceScenarios) {
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 2;
  e.absoluteExpiration = TimePoint(Duration(time + 10));
  cache.set("absolute", nullptr, e);
  time += 9;
  Cache::ValuePtr result;
  EXPECT_TRUE(cache.tryGetValue("absolute", result));
  ++time;
  EXPECT_FALSE(cache.tryGetValue("absolute", result));
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  e.absoluteExpiration.reset();
  e.absoluteExpirationRelativeToNow = Duration(10);
  cache.set("relative", nullptr, e);
  time += 10;
  EXPECT_FALSE(cache.tryGetValue("relative", result));
  e = {};
  e.size = 2;
  e.priority = Cache::Priority::High;
  cache.set("high", nullptr, e);
  e.priority = Cache::Priority::Normal;
  cache.set("normal", nullptr, e);
  e.priority = Cache::Priority::Low;
  cache.set("low", nullptr, e);
  cache.compact(1.0 / 3);
  auto keys = cache.keys();
  std::sort(keys.begin(), keys.end());
  EXPECT_EQ(keys, (std::vector<std::string>{"high", "normal"}));
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 4);
}

TEST_F(MemoryCacheTest, lowWatermarkUsesWeightsAndLru) {
  auto o = options(true);
  o.compactionPercentage = 0.3;
  Cache cache(o);
  for (int i = 0; i < 5; ++i) {
    Cache::EntryOptions e;
    e.size = i;
    cache.set(std::to_string(i), nullptr, e);
    ++time;
  }
  EXPECT_EQ(cache.count(), 5);
  Cache::EntryOptions e;
  e.size = 1;
  cache.set("5", nullptr, e);
  drainMaintenance();
  auto keys = cache.keys();
  std::sort(keys.begin(), keys.end());
  EXPECT_EQ(keys, (std::vector<std::string>{"3", "4"}));
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 7);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 3);
}

TEST_F(MemoryCacheTest, overcapacityPurgeAreThreadSafe) {
  auto o = options(true);
  o.compactionPercentage = 0.5;
  o.expirationScanFrequency = Duration::zero();
  o.clock = [] {
    return std::chrono::time_point_cast<Duration>(
        std::chrono::system_clock::now());
  };
  Cache cache(o);
  std::barrier start(3);
  std::atomic<bool> valid{true};
  std::vector<std::thread> workers;
  for (int w = 0; w < 3; ++w)
    workers.emplace_back([&, w] {
      start.arrive_and_wait();
      Cache::EntryOptions e;
      e.size = 1;
      for (int i = 0; i < 10000; ++i) {
        if (*cache.getCurrentStatistics()->currentEstimatedSize > 10)
          valid = false;
        cache.set(std::to_string(w * 10000 + i), nullptr, e);
      }
    });
  for (auto& t : workers)
    t.join();
  drainMaintenance();
  EXPECT_TRUE(valid);
  EXPECT_LE(cache.count(), 10);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, cache.count());
}

TEST_F(MemoryCacheTest, compactLastAccessedRaceCondition) {
  auto o = options();
  o.clock = [] {
    return std::chrono::time_point_cast<Duration>(
        std::chrono::system_clock::now());
  };
  Cache cache(o);
  std::atomic<bool> done{false};
  std::vector<std::thread> readers;
  for (unsigned w = 0; w < std::max(1u, std::thread::hardware_concurrency());
       ++w)
    readers.emplace_back([&, w] {
      std::mt19937 rng(w);
      Cache::ValuePtr value;
      while (!done) {
        cache.tryGetValue(std::to_string(rng() % 100), value);
        std::this_thread::yield();
      }
    });
  auto cleanup = folly::makeGuard([&] {
    done = true;
    for (auto& t : readers)
      t.join();
    drainMaintenance();
  });
  for (int i = 0; i < 1000; ++i) {
    for (int k = 0; k < 100; ++k)
      cache.set(std::to_string(k), nullptr);
    cache.compact(1);
  }
}

TEST_F(MemoryCacheTest, overflowAndCapacityCompactionSourceCases) {
  for (int64_t limit : {int64_t(10), INT64_MAX}) {
    auto o = options(true);
    o.sizeLimit = limit;
    o.compactionPercentage = 0.5;
    Cache cache(o);
    Cache::EntryOptions e;
    e.size = limit == 10 ? 6 : INT64_MAX;
    cache.set("old", nullptr, e);
    e.size = limit == 10 ? 5 : INT64_MAX;
    cache.set("rejected", nullptr, e);
    drainMaintenance();
    EXPECT_EQ(cache.count(), 0);
    EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
  }
  auto o = options(true);
  o.sizeLimit = 5;
  o.compactionPercentage = 0.5;
  Cache cache(o);
  Cache::EntryOptions e;
  e.size = 5;
  cache.set("key", nullptr, e);
  e.size = 6;
  cache.set("key", nullptr, e);
  drainMaintenance();
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
}

TEST_F(MemoryCacheTest, valueDestructionAndMutationRemainOutsideMapLocks) {
  Cache cache(options());
  int destroyed = 0;
  auto make = [&] {
    return std::shared_ptr<int>(new int(1), [&](int* p) {
      cache.remove("reentrant");
      ++destroyed;
      delete p;
    });
  };
  cache.set("key", make());
  cache.set("key", make());
  EXPECT_EQ(destroyed, 1);
  cache.remove("key");
  EXPECT_EQ(destroyed, 2);
  auto external = make();
  cache.set("key", external);
  *external = 42;
  Cache::ValuePtr read;
  ASSERT_TRUE(cache.tryGetValue("key", read));
  EXPECT_EQ(*read, 42);
  cache.clear();
  EXPECT_EQ(destroyed, 2);
  read.reset();
  external.reset();
  EXPECT_EQ(destroyed, 3);
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, clearAfterSwapFailureDoesNotRestoreOldState) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options(true));
  Cache::EntryOptions e;
  e.size = 3;
  cache.set("old", std::make_shared<int>(1), e);
  {
    ScopedTestValue hook(
        "MemoryCache::clear::exchanged", std::function<void(void*)>([&](void*) {
          cache.set("new", nullptr, e);
          throw std::bad_alloc();
        }));
    EXPECT_THROW(cache.clear(), std::bad_alloc);
  }
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"new"});
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 3);
}
TEST_F(MemoryCacheTest, expiredCandidateQueryCountsOnlyOnce) {
  using namespace common::testutil;
  TestValue::enable();
  Cache cache(options());
  Cache::EntryOptions e;
  e.absoluteExpirationRelativeToNow = Duration(1);
  cache.set("expired", nullptr, e);
  ++time;
  {
    ScopedTestValue hook(
        "MemoryCache::compact::candidates",
        std::function<void(void*)>([&](void*) {
          Cache::ValuePtr value;
          EXPECT_FALSE(cache.tryGetValue("expired", value));
        }));
    cache.compact(1);
  }
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
}
#endif

TEST_F(MemoryCacheTest, invalidPriorityDoesNotBreakExpirationScan) {
  auto o = options();
  o.expirationScanFrequency = Duration(1);
  Cache cache(o);
  Cache::EntryOptions e;
  e.priority = Cache::Priority(100);
  cache.set("invalid", nullptr, e);
  e = {};
  e.absoluteExpirationRelativeToNow = Duration(1);
  cache.set("expired", nullptr, e);
  time += 2;
  cache.remove("missing");
  drainMaintenance();
  EXPECT_EQ(cache.keys(), std::vector<std::string>{"invalid"});
  EXPECT_THROW(cache.compact(0), std::invalid_argument);
}

TEST_F(MemoryCacheTest, replacingNeverLeavesNullValue) {
  Cache cache(options());
  cache.set("key", std::make_shared<int>(1));
  std::atomic<int> active{2};
  std::atomic<bool> valid{true};
  std::barrier start(3);
  std::vector<std::thread> workers;
  for (int w = 0; w < 2; ++w)
    workers.emplace_back([&] {
      start.arrive_and_wait();
      for (int i = 0; i < 20000; ++i)
        cache.set("key", std::make_shared<int>(i));
      --active;
    });
  workers.emplace_back([&] {
    start.arrive_and_wait();
    while (active) {
      Cache::ValuePtr value;
      if (!cache.tryGetValue("key", value) || !value)
        valid = false;
    }
  });
  for (auto& w : workers)
    w.join();
  EXPECT_TRUE(valid);
}

TEST_F(MemoryCacheTest, concurrentReportsAndInstanceNames) {
  auto reporter = std::dynamic_pointer_cast<ThrowingReporter>(
      folly::Singleton<BaseStatsReporter>::try_get());
  ASSERT_NE(reporter, nullptr);
  reporter->clear();
  BaseStatsReporter::registered = true;
  auto cleanup =
      folly::makeGuard([] { BaseStatsReporter::registered = false; });
  auto o = options(true);
  o.name = "custom";
  Cache cache(o);
  Cache::EntryOptions e;
  e.size = 2;
  cache.set("k", nullptr, e);
  Cache::ValuePtr value;
  for (int i = 0; i < 100; ++i)
    cache.tryGetValue("k", value);
  std::vector<std::thread> reporters;
  for (int w = 0; w < 8; ++w)
    reporters.emplace_back([&] {
      for (int i = 0; i < 100; ++i)
        cache.report();
    });
  for (auto& t : reporters)
    t.join();
  EXPECT_EQ(reporter->counterMap["velox.memory_cache.custom.hits"], 100);
  EXPECT_EQ(
      reporter->statTypeMap["velox.memory_cache.custom.estimated_size"],
      StatType::AVG);
  EXPECT_EQ(cache.getCurrentStatistics()->totalHits, 100);
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, queuedMaintenanceKeepsCoreAndClockAlive) {
  auto& executor = detail::memoryCacheExecutor();
  std::promise<void> started, release;
  auto released = release.get_future().share();
  executor.add([&] {
    started.set_value();
    released.wait();
  });
  started.get_future().wait();
  auto cleanup = folly::makeGuard([&] {
    release.set_value();
    drainMaintenance();
  });
  auto clock = std::make_shared<std::atomic<int64_t>>(0);
  std::weak_ptr<std::atomic<int64_t>> weak = clock;
  {
    Cache::Options o;
    o.clock = [clock] { return TimePoint(Duration(clock->load())); };
    o.expirationScanFrequency = Duration(1);
    auto cache = std::make_unique<Cache>(o);
    *clock = 2;
    cache->remove("missing");
    cache.reset();
  }
  clock.reset();
  EXPECT_FALSE(weak.expired());
  cleanup.dismiss();
  release.set_value();
  drainMaintenance();
  EXPECT_TRUE(weak.expired());
}
#endif

TEST_F(MemoryCacheTest, sourceEvictionMetricsAndExplicitRemoveVariant) {
  for (bool explicitRemove : {false, true}) {
    Cache cache(options());
    Cache::EntryOptions e;
    e.absoluteExpirationRelativeToNow = Duration(10);
    for (int i = 0; i < 3; ++i)
      cache.set(std::to_string(i), nullptr, e);
    time += 20;
    Cache::ValuePtr value;
    if (explicitRemove)
      cache.remove("0");
    else
      EXPECT_FALSE(cache.tryGetValue("0", value));
    cache.compact(1);
    EXPECT_EQ(
        cache.getCurrentStatistics()->totalEvictions, explicitRemove ? 2 : 3);
  }
}

#ifndef NDEBUG
TEST(MemoryCacheFaultTest, backgroundPartialFailureKeepsCompletedAccounting) {
  using C = MemoryCache<int, int, FaultMap>;
  int64_t time = 0;
  C::Options o;
  o.sizeLimit = 10;
  o.trackStatistics = true;
  o.expirationScanFrequency = C::Duration(1);
  o.clock = [&] { return C::TimePoint(C::Duration(time)); };
  C cache(o);
  C::EntryOptions e;
  e.size = 2;
  e.absoluteExpirationRelativeToNow = C::Duration(1);
  cache.set(1, nullptr, e);
  cache.set(2, nullptr, e);
  cache.set(3, nullptr, e);
  MapFaults::deletes = 1;
  time = 2;
  // A miss get schedules scanning without consuming a delete fault itself.
  C::ValuePtr value;
  cache.tryGetValue(99, value);
  drainMaintenance();
  MapFaults::deletes = -1;
  EXPECT_EQ(cache.count(), 2);
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 4);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
  time = 4;
  cache.tryGetValue(99, value);
  drainMaintenance();
  EXPECT_EQ(cache.count(), 0);
  EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 3);
}
#endif

#ifndef NDEBUG
TEST_F(MemoryCacheTest, concurrentExtremeReservationsDoNotWrapAdmission) {
  using namespace common::testutil;
  TestValue::enable();
  auto o = options(true);
  o.sizeLimit = INT64_MAX;
  Cache cache(o);
  Cache::EntryOptions e;
  e.size = INT64_MAX;
  cache.set("K", nullptr, e);
  bool once = false;
  {
    ScopedTestValue hook(
        "MemoryCache::set::reserved", std::function<void(void*)>([&](void*) {
          if (once)
            return;
          once = true;
          cache.set("Q", nullptr, e);
        }));
    cache.set("K", nullptr, e);
  }
  drainMaintenance();
  Cache::ValuePtr result;
  EXPECT_FALSE(cache.tryGetValue("Q", result));
  EXPECT_LE(*cache.getCurrentStatistics()->currentEstimatedSize, INT64_MAX);
  cache.clear();
  EXPECT_EQ(cache.getCurrentStatistics()->currentEstimatedSize, 0);
}
#endif

TEST_F(MemoryCacheTest, relativeExpiryDoesNotMaskInvalidAbsoluteExpiry) {
  Cache cache(options());
  for (auto absolute : {TimePoint::min(), TimePoint::max()}) {
    for (bool relative : {false, true}) {
      Cache::EntryOptions e;
      e.absoluteExpiration = absolute;
      if (relative)
        e.absoluteExpirationRelativeToNow = Duration(1);
      EXPECT_THROW(cache.set("invalid", nullptr, e), std::out_of_range);
      EXPECT_EQ(cache.count(), 0);
    }
  }
}

TEST(MemoryCacheFaultTest, reportContainsSnapshotFailure) {
  using C = MemoryCache<int, int, FaultMap>;
  C::Options o;
  o.trackStatistics = true;
  C cache(o);
  cache.set(1, nullptr);
  C::ValuePtr value;
  EXPECT_TRUE(cache.tryGetValue(1, value));
  auto cleanup = folly::makeGuard([] { MapFaults::readSize = false; });
  MapFaults::readSize = true;
  EXPECT_NO_THROW(cache.report());
  MapFaults::readSize = false;
  EXPECT_EQ(cache.getCurrentStatistics()->totalHits, 1);
  EXPECT_EQ(cache.count(), 1);
}

#ifndef NDEBUG
TEST_F(MemoryCacheTest, scanDuringCandidateProcessingRunsAgain) {
  using namespace common::testutil;
  TestValue::enable();
  for (bool failScan : {false, true}) {
    auto o = options();
    o.expirationScanFrequency = Duration(10);
    Cache cache(o);
    Cache::EntryOptions e;
    e.absoluteExpirationRelativeToNow = Duration(30);
    cache.set("expiresDuringScan", nullptr, e);
    std::promise<void> collected, release;
    auto released = release.get_future().share();
    std::atomic<int> scans{0};
    ScopedTestValue hook(
        "MemoryCache::compact::candidates",
        std::function<void(void*)>([&](void*) {
          if (++scans != 1)
            return;
          collected.set_value();
          released.wait();
          if (failScan)
            throw std::bad_alloc();
        }));
    auto cleanup = folly::makeGuard([&] {
      release.set_value();
      drainMaintenance();
    });
    time += 11;
    cache.remove("missing");
    ASSERT_EQ(collected.get_future().wait_for(10s), std::future_status::ready);
    time += 29;
    cache.remove("missing");
    cleanup.dismiss();
    release.set_value();
    drainMaintenance();
    EXPECT_EQ(scans, 2);
    EXPECT_EQ(cache.count(), 0);
    EXPECT_EQ(cache.getCurrentStatistics()->totalEvictions, 1);
  }
}
#endif

#ifndef NDEBUG
TEST_F(MemoryCacheTest, retiredStateReleasedBeforeCoreTeardownCompletes) {
  using namespace common::testutil;
  TestValue::enable();
  std::weak_ptr<int> retiredValue;
  {
    Cache cache(options());
    auto value = std::make_shared<int>(42);
    retiredValue = value;
    cache.set("old", std::move(value));
    ScopedTestValue hook(
        "MemoryCache::clear::exchanged",
        std::function<void(void*)>([](void*) { throw std::bad_alloc(); }));
    EXPECT_THROW(cache.clear(), std::bad_alloc);
    EXPECT_EQ(cache.count(), 0);
  }
  EXPECT_TRUE(retiredValue.expired());
}
#endif
