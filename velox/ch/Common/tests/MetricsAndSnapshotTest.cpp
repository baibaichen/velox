#include "velox/ch/Common/CurrentMetrics.h"
#include "velox/ch/Common/FileCacheStats.h"
#include "velox/ch/Common/ProfileEvents.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

using namespace facebook::velox::ch;

TEST(CurrentMetricsTest, AddSubGetRoundTrip)
{
    auto m = CurrentMetrics::CacheFileSegments;
    auto before = CurrentMetrics::get(m);
    CurrentMetrics::add(m, 5);
    EXPECT_EQ(CurrentMetrics::get(m), before + 5);
    CurrentMetrics::sub(m, 3);
    EXPECT_EQ(CurrentMetrics::get(m), before + 2);
    CurrentMetrics::sub(m, 2);
    EXPECT_EQ(CurrentMetrics::get(m), before);
}

TEST(CurrentMetricsTest, SetOverwrites)
{
    auto m = CurrentMetrics::FilesystemCacheSize;
    CurrentMetrics::set(m, 42);
    EXPECT_EQ(CurrentMetrics::get(m), 42);
    CurrentMetrics::set(m, 0);
    EXPECT_EQ(CurrentMetrics::get(m), 0);
}

TEST(CurrentMetricsTest, IncrementRAII)
{
    auto m = CurrentMetrics::FilesystemCacheElements;
    auto before = CurrentMetrics::get(m);
    {
        CurrentMetrics::Increment inc(m, 7);
        EXPECT_EQ(CurrentMetrics::get(m), before + 7);
    }
    EXPECT_EQ(CurrentMetrics::get(m), before);
}

TEST(ProfileEventsTest, IncrementAccumulates)
{
    auto e = ProfileEvents::FilesystemCacheReserveAttempts;
    auto before = ProfileEvents::get(e);
    ProfileEvents::increment(e, 10);
    ProfileEvents::increment(e, 3);
    EXPECT_EQ(ProfileEvents::get(e), before + 13);
}

TEST(ProfileEventsTest, TimeIncrementRecordsNonzero)
{
    auto e = ProfileEvents::FilesystemCacheGetMicroseconds;
    auto before = ProfileEvents::get(e);
    {
        ProfileEventTimeIncrement<Microseconds> timer(e);
        // Busy spin for at least 1 microsecond to guarantee nonzero elapsed
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < std::chrono::microseconds(50))
        {
        }
    }
    EXPECT_GT(ProfileEvents::get(e), before);
}

TEST(ProfileEventsTest, AllExistingEnumNamesCompile)
{
    // Verify index correctness: last existing event is at index 49
    EXPECT_EQ(
        static_cast<size_t>(ProfileEvents::OpenedFileCacheMicroseconds), 49u);
    // First new event is at index 50
    EXPECT_EQ(
        static_cast<size_t>(ProfileEvents::CachedReadBufferWaitReadBufferMicroseconds), 50u);
    // Total count: 50 existing + 10 new = 60
    EXPECT_EQ(ProfileEvents::kNumEvents, 60u);
}

TEST(FileCacheStatsSnapshotTest, ReflectsCurrentValues)
{
    CurrentMetrics::set(CurrentMetrics::FilesystemCacheSize, 1024);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheBytes, 100);
    auto snap = takeFileCacheStatsSnapshot();
    EXPECT_EQ(snap.cacheSize, 1024);
    EXPECT_GE(snap.cacheReadBytes, 100u);
}

TEST(FileCacheStatsSnapshotTest, SubtractionProducesDeltas)
{
    auto before = takeFileCacheStatsSnapshot();
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceBytes, 500);
    CurrentMetrics::set(CurrentMetrics::FilesystemCacheKeys, 7);
    auto after = takeFileCacheStatsSnapshot();
    auto delta = after - before;
    EXPECT_EQ(delta.sourceReadBytes, 500u);
    EXPECT_EQ(delta.cacheKeys, 7); // gauge: from `after`
}

TEST(ProfileEventsTest, NewReaderEventsPresent)
{
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheHits);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheMisses);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromCacheMicroseconds, 10);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferReadFromSourceMicroseconds, 20);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedFromSourceMicroseconds, 30);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferCacheWriteMicroseconds, 40);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedFromSourceBytes, 50);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferPredownloadedBytes, 60);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferCreateBufferMicroseconds, 70);
    ProfileEvents::increment(ProfileEvents::CachedReadBufferWaitReadBufferMicroseconds, 80);
    EXPECT_GE(ProfileEvents::get(ProfileEvents::CachedReadBufferReadFromCacheHits), 1u);
    EXPECT_GE(ProfileEvents::get(ProfileEvents::CachedReadBufferWaitReadBufferMicroseconds), 80u);
}
