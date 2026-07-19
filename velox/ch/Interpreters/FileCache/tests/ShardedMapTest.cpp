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

#include "velox/ch/Interpreters/FileCache/ShardedMap.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Common/ProfileEvents.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <latch>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using TestMap = FileCacheUtils::ShardedMap<std::string, int>;

// ── Basic operations ──────────────────────────────────────────────────────────

TEST(ShardedMapTest, InsertAndLookup)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    m.withShard("key", [](auto & map) { map["key"] = 42; });
    int result = 0;
    m.withShard("key", [&](const auto & map)
    {
        auto it = map.find("key");
        if (it != map.end())
            result = it->second;
    });
    EXPECT_EQ(result, 42);
}

TEST(ShardedMapTest, EraseUpdatesSize)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    m.withShard("a", [](auto & map) { map["a"] = 1; });
    EXPECT_EQ(m.size(), 1u);
    m.withShard("a", [](auto & map) { map.erase("a"); });
    EXPECT_EQ(m.size(), 0u);
}

TEST(ShardedMapTest, InsertUpdatesSize)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    EXPECT_EQ(m.size(), 0u);
    m.withShard("x", [](auto & map) { map["x"] = 1; });
    EXPECT_EQ(m.size(), 1u);
    m.withShard("y", [](auto & map) { map["y"] = 2; });
    EXPECT_EQ(m.size(), 2u);
}

TEST(ShardedMapTest, ForEachShardVisitsAll)
{
    // Insert 32 keys, one per shard slot using known hash distribution.
    // We can't control which shard a key lands on, so instead we insert
    // enough distinct keys and verify forEachShard sees all of them.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    for (int i = 0; i < 64; ++i)
        m.withShard(std::to_string(i), [i](auto & map)
        {
            map[std::to_string(i)] = i;
        });

    size_t total = 0;
    m.forEachShard([&](const auto & map) { total += map.size(); });
    EXPECT_EQ(total, 64u);
    EXPECT_EQ(m.size(), 64u);
}

TEST(ShardedMapTest, ForEachShardEraseUpdatesSize)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    for (int i = 0; i < 10; ++i)
        m.withShard(std::to_string(i), [i](auto & map)
        {
            map[std::to_string(i)] = i;
        });
    EXPECT_EQ(m.size(), 10u);

    m.forEachShard([](auto & map) { map.clear(); });
    EXPECT_EQ(m.size(), 0u);
}

// ── Same shard serializes callbacks ──────────────────────────────────────────

TEST(ShardedMapTest, SameKeyAlwaysSameShard)
{
    // Two callbacks on the same key target the same shard, so the shard lock
    // must serialize them: they can never be inside the callback at the same
    // time. Record the peak concurrent occupancy and assert it never exceeds 1.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    std::atomic<int> active{0};
    std::atomic<int> max_active{0};
    std::latch start{2};

    auto worker = [&]()
    {
        start.arrive_and_wait(); // both threads begin racing together
        m.withShard("shared", [&](auto &)
        {
            const int now = active.fetch_add(1, std::memory_order_seq_cst) + 1;
            int seen = max_active.load(std::memory_order_seq_cst);
            while (now > seen
                   && !max_active.compare_exchange_weak(
                          seen, now, std::memory_order_seq_cst))
            {
            }
            // Give the peer a chance to (wrongly) enter if the lock is missing.
            std::this_thread::yield();
            active.fetch_sub(1, std::memory_order_seq_cst);
        });
    };

    auto f1 = std::async(std::launch::async, worker);
    auto f2 = std::async(std::launch::async, worker);

    f1.get();
    f2.get();
    EXPECT_EQ(max_active.load(std::memory_order_seq_cst), 1)
        << "same-key callbacks must be serialized by the shard lock";
}

// ── Different shards execute concurrently ────────────────────────────────────

TEST(ShardedMapTest, DifferentShardsConcurrent)
{
    // Find two keys that land on different shards by trying combinations.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    std::hash<std::string> h;
    std::string key1 = "shard_a_key_0";
    std::string key2;
    for (int i = 0; i < 1000; ++i)
    {
        key2 = "shard_b_key_" + std::to_string(i);
        if ((h(key1) % 32) != (h(key2) % 32))
            break;
    }
    ASSERT_NE(h(key1) % 32, h(key2) % 32)
        << "Could not find two keys on different shards";

    std::latch barrier{2};
    std::atomic<bool> both_inside{false};

    auto probe = [&](const std::string & key)
    {
        m.withShard(key, [&](auto &)
        {
            // Both callbacks hold their (different) shard locks here. The latch
            // only releases once BOTH threads have arrived while inside their
            // callbacks, which is possible solely because the two shards use
            // independent mutexes. Reaching past the latch therefore proves the
            // callbacks overlapped. (A single-lock implementation would deadlock
            // here: the second thread could never enter its callback to arrive.)
            barrier.arrive_and_wait();
            both_inside.store(true, std::memory_order_seq_cst);
        });
    };

    auto f1 = std::async(std::launch::async, probe, key1);
    auto f2 = std::async(std::launch::async, probe, key2);

    f1.get();
    f2.get();
    EXPECT_TRUE(both_inside.load(std::memory_order_seq_cst))
        << "Different-shard callbacks should overlap";
}

// ── Exception-safe size accounting ───────────────────────────────────────────

TEST(ShardedMapTest, ExceptionAfterInsertUpdatesSize)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    EXPECT_THROW(
        m.withShard("ex", [](auto & map)
        {
            map["ex"] = 99;
            throw std::runtime_error("oops");
        }),
        std::runtime_error);
    // The insert happened before the throw; size must reflect it.
    EXPECT_EQ(m.size(), 1u);
}

TEST(ShardedMapTest, ExceptionAfterEraseUpdatesSize)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    m.withShard("e", [](auto & map) { map["e"] = 1; });
    EXPECT_EQ(m.size(), 1u);

    EXPECT_THROW(
        m.withShard("e", [](auto & map)
        {
            map.erase("e");
            throw std::runtime_error("oops");
        }),
        std::runtime_error);
    // The erase happened before the throw.
    EXPECT_EQ(m.size(), 0u);
}

// ── OriginPoolKeyHash shard compatibility ─────────────────────────────────────

TEST(ShardedMapTest, OriginPoolKeyHashSameUserSameShard)
{
    using OriginMap =
        FileCacheUtils::ShardedMap<OriginPoolKey, int, 32, OriginPoolKeyHash>;
    OriginMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    // Two keys for the same user, different weight/type, must land on the same
    // shard (hash is user_id only).
    OriginPoolKey k1{"u", uint64_t{1}, FileSegmentKeyType::Data};
    OriginPoolKey k2{"u", uint64_t{2}, FileSegmentKeyType::System};

    OriginPoolKeyHash hasher;
    EXPECT_EQ(hasher(k1) % 32, hasher(k2) % 32)
        << "Same user must select the same shard";

    // Insert one key, then look up the other from the same shard callback.
    m.withShard(k1, [&](auto & map) { map[k1] = 1; });
    int found = 0;
    m.withShard(k2, [&](auto & map)
    {
        // k1 and k2 are in the same shard; k1 must be visible here.
        auto it = map.find(k1);
        if (it != map.end())
            found = it->second;
    });
    EXPECT_EQ(found, 1);
}

// ── Static assertions ─────────────────────────────────────────────────────────

TEST(ShardedMapTest, CopyDeleted)
{
    static_assert(!std::is_copy_constructible_v<TestMap>);
    static_assert(!std::is_copy_assignable_v<TestMap>);
}

TEST(ShardedMapTest, SizeAfterConcurrentInserts)
{
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    constexpr int N = 200;
    std::vector<std::thread> threads;
    threads.reserve(N);
    for (int i = 0; i < N; ++i)
        threads.emplace_back([&, i]()
        {
            m.withShard(std::to_string(i), [&, i](auto & map)
            {
                map[std::to_string(i)] = i;
            });
        });
    for (auto & t : threads)
        t.join();
    EXPECT_EQ(m.size(), static_cast<size_t>(N));
}

// ── Ref-qualified functor: callback must be invoked as lvalue ─────────────────

TEST(ShardedMapTest, WithShardRefQualifiedUsesLvalueOverload)
{
    // A functor with distinct lvalue- and rvalue-qualified operator() overloads.
    // Passed as a temporary (prvalue): F is deduced as RefQualFunctor, so
    // std::forward<F>(f) casts f to rvalue and invokes operator()&&, while the
    // named-parameter invocation `f(map)` treats f as a lvalue and invokes
    // operator()& — exactly as CH.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    struct RefQualFunctor
    {
        int * lvalue_calls;
        int * rvalue_calls;
        void operator()(TestMap::Map &) & { ++(*lvalue_calls); }
        void operator()(TestMap::Map &) && { ++(*rvalue_calls); }
    };

    int lv = 0, rv = 0;
    m.withShard("k", RefQualFunctor{&lv, &rv});
    EXPECT_EQ(lv, 1) << "withShard must invoke the functor as an lvalue (named f(map))";
    EXPECT_EQ(rv, 0) << "withShard must not forward-cast the functor to an rvalue";
}

TEST(ShardedMapTest, ForEachShardRefQualifiedUsesLvalueOverload)
{
    // forEachShard visits all num_shards (32) shards, even empty ones.
    // std::forward<F>(f) in a loop casts f to rvalue on every iteration, invoking
    // operator()&&; named `f(map)` in the loop uses f as a lvalue every time,
    // invoking operator()&.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);

    struct RefQualFunctor
    {
        int * lvalue_calls;
        int * rvalue_calls;
        void operator()(TestMap::Map &) & { ++(*lvalue_calls); }
        void operator()(TestMap::Map &) && { ++(*rvalue_calls); }
    };

    int lv = 0, rv = 0;
    m.forEachShard(RefQualFunctor{&lv, &rv});
    EXPECT_EQ(lv, 32) << "forEachShard must invoke the functor as an lvalue on every shard";
    EXPECT_EQ(rv, 0) << "forEachShard must not forward-cast the functor to an rvalue";
}

// ── Return-by-value: withShard copies the callback result ─────────────────────

TEST(ShardedMapTest, WithShardReturnCopiesValue)
{
    // Callback returns int& (a live reference to a map element). withShard's
    // 'auto' return type decays that reference to a value copy before the lock
    // is released. Using decltype(auto) at the call site preserves whatever
    // reference category withShard actually returns: 'auto' → int (copy);
    // 'decltype(auto)' → int& (live reference). Mutating the returned value must
    // NOT change the stored map element.
    TestMap m(ProfileEvents::FilesystemCacheGetOrSetMicroseconds);
    m.withShard("v", [](auto & map) { map["v"] = 99; });

    decltype(auto) copy = m.withShard("v", [](auto & map) -> int & { return map["v"]; });
    copy = 0; // if copy is int (value) this is harmless; if int& it mutates the map element

    int stored = -1;
    m.withShard("v", [&](const auto & map) { stored = map.at("v"); });
    EXPECT_EQ(stored, 99)
        << "withShard 'auto' return must copy the value; "
           "mutating the copy must not affect the stored element";
}

} // namespace
} // namespace facebook::velox::ch
