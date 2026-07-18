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

#include "velox/ch/Common/SipHash128.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentKeyType.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd_internal.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

#include <climits>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

// ── SipHash128 ────────────────────────────────────────────────────────────────

TEST(SipHash128Test, IncrementalEqualsOneShot)
{
    const char * data = "hello world";
    const size_t n = strlen(data);
    auto one = sipHash128(data, n);
    SipHash128 inc;
    inc.update(data, 5);
    inc.update(data + 5, n - 5);
    EXPECT_EQ(one, inc.get128());
}

TEST(FileCacheKeyTest, GoldenEmpty)
{
    EXPECT_EQ(
        FileCacheKey::fromPath("").toString(),
        "f711edcba8b6b5e5e983a656dbc1b532");
}

TEST(FileCacheKeyTest, GoldenAbc)
{
    EXPECT_EQ(
        FileCacheKey::fromPath("abc").toString(),
        "53a3124ce5655a686c6b96daa215b4b6");
}

TEST(FileCacheKeyTest, GoldenS3Path)
{
    EXPECT_EQ(
        FileCacheKey::fromPath("s3://bucket/key").toString(),
        "6ba3177b6fbaa4c9f65873033e35aeaa");
}

TEST(FileCacheKeyTest, GoldenLong)
{
    EXPECT_EQ(
        FileCacheKey::fromPath("0123456789abcdef0123456789abcdef").toString(),
        "77dd7dd78fa45ef0b93cc3b8df847cbd");
}

TEST(FileCacheKeyTest, RoundTrip)
{
    const auto key = FileCacheKey::fromPath("some/path/to/data.parquet");
    EXPECT_EQ(FileCacheKey::fromKeyString(key.toString()), key);
}

TEST(FileCacheKeyTest, ToStringLength)
{
    EXPECT_EQ(FileCacheKey::fromPath("x").toString().size(), 32u);
}

TEST(FileCacheKeyTest, DefaultConstructibleZero)
{
    static_assert(std::is_default_constructible_v<FileCacheKey>);
    EXPECT_EQ(FileCacheKey{}.key, static_cast<FileCacheKey::KeyHash>(0));
    EXPECT_EQ(
        FileCacheKey{}.toString(), "00000000000000000000000000000000");
}

TEST(FileCacheKeyTest, FromKeyStringBadLength)
{
    EXPECT_THROW(FileCacheKey::fromKeyString("abc"), VeloxRuntimeError);
    EXPECT_THROW(
        FileCacheKey::fromKeyString(std::string(31, '0')), VeloxRuntimeError);
    EXPECT_THROW(
        FileCacheKey::fromKeyString(std::string(33, '0')), VeloxRuntimeError);
}

TEST(FileCacheKeyTest, FromKeyStringInvalidHexChar)
{
    // Correct 32-character length but contains a non-hex character 'g'.
    EXPECT_THROW(
        FileCacheKey::fromKeyString("g0000000000000000000000000000000"),
        VeloxRuntimeError);
}

TEST(FileCacheKeyTest, OrderHighFirst)
{
    auto a = FileCacheKey::fromKey(
        (static_cast<uint128_t>(1) << 64) | uint64_t{0xFFFFFFFFFFFFFFFFULL});
    auto b = FileCacheKey::fromKey(
        (static_cast<uint128_t>(2) << 64) | uint64_t{0});
    EXPECT_LT(a, b);
}

TEST(FileCacheKeyTest, UsableInF14FastMap)
{
    folly::F14FastMap<FileCacheKey, int, FileCacheKeyHash> map;
    auto k1 = FileCacheKey::fromPath("p1");
    auto k2 = FileCacheKey::fromPath("p2");
    map[k1] = 1;
    map[k2] = 2;
    EXPECT_EQ(map[k1], 1);
    EXPECT_EQ(map[k2], 2);
}

// ── FileSegmentKeyType ────────────────────────────────────────────────────────

TEST(FileSegmentKeyTypeTest, PrefixGeneral)
{
    EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::General), "");
}

TEST(FileSegmentKeyTypeTest, PrefixSystem)
{
    EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::System), "System");
}

TEST(FileSegmentKeyTypeTest, PrefixData)
{
    EXPECT_EQ(getKeyTypePrefix(FileSegmentKeyType::Data), "Data");
}

TEST(FileSegmentKeyTypeTest, ToStringAll)
{
    EXPECT_EQ(toString(FileSegmentKeyType::General), "General");
    EXPECT_EQ(toString(FileSegmentKeyType::System), "System");
    EXPECT_EQ(toString(FileSegmentKeyType::Data), "Data");
}

TEST(FileSegmentKeyTypeTest, UnderlyingValues)
{
    static_assert(static_cast<uint8_t>(FileSegmentKeyType::General) == 0);
    static_assert(static_cast<uint8_t>(FileSegmentKeyType::System) == 1);
    static_assert(static_cast<uint8_t>(FileSegmentKeyType::Data) == 2);
}

// ── FileCacheOriginInfo ───────────────────────────────────────────────────────

TEST(FileCacheOriginInfoTest, EqualityUserIdOnly)
{
    FileCacheOriginInfo a{"user1", uint64_t{10}, FileSegmentKeyType::Data};
    FileCacheOriginInfo b{"user1", uint64_t{20}, FileSegmentKeyType::System};
    EXPECT_EQ(a, b);
}

TEST(FileCacheOriginInfoTest, InequalityDifferentUser)
{
    EXPECT_NE(FileCacheOriginInfo{"user1"}, FileCacheOriginInfo{"user2"});
}

TEST(FileCacheOriginInfoTest, OriginPoolKeyFullEquality)
{
    OriginPoolKey x{"user1", uint64_t{10}, FileSegmentKeyType::Data};
    OriginPoolKey y{"user1", uint64_t{10}, FileSegmentKeyType::Data};
    OriginPoolKey z{"user1", uint64_t{10}, FileSegmentKeyType::System};
    EXPECT_EQ(x, y);
    EXPECT_NE(x, z);
}

TEST(FileCacheOriginInfoTest, OriginPoolKeyHashSameUserSameBucket)
{
    OriginPoolKeyHash h;
    OriginPoolKey k1{"u", uint64_t{1}, FileSegmentKeyType::Data};
    OriginPoolKey k2{"u", uint64_t{2}, FileSegmentKeyType::System};
    EXPECT_EQ(h(k1), h(k2));
}

TEST(FileCacheOriginInfoTest, UsableInF14FastMap)
{
    folly::F14FastMap<OriginPoolKey, int, OriginPoolKeyHash> map;
    OriginPoolKey k{"u", uint64_t{1}, FileSegmentKeyType::General};
    map[k] = 42;
    EXPECT_EQ(map[k], 42);
}

// ── FileCache_fwd.h ───────────────────────────────────────────────────────────

TEST(FileCacheFwdTest, PolicyEnumValues)
{
    static_assert(static_cast<uint8_t>(FileCachePolicy::LRU) == 0);
    static_assert(static_cast<uint8_t>(FileCachePolicy::SLRU) == 1);
    static_assert(static_cast<uint8_t>(FileCachePolicy::SLRU_OVERCOMMIT) == 2);
    static_assert(static_cast<uint8_t>(FileCachePolicy::LRU_OVERCOMMIT) == 3);
}

TEST(FileCacheFwdTest, DefaultConstants)
{
    EXPECT_EQ(FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE, 32ULL * 1024 * 1024);
    EXPECT_EQ(FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT, 4ULL * 1024 * 1024);
    EXPECT_EQ(FILECACHE_DEFAULT_RESERVE_GRANULARITY, 4ULL * 1024 * 1024);
    EXPECT_EQ(FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS, 5u);
    EXPECT_EQ(FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT, 5000u);
    EXPECT_EQ(FILECACHE_DEFAULT_LOAD_METADATA_THREADS, 16u);
    EXPECT_EQ(FILECACHE_DEFAULT_MAX_ELEMENTS, 10'000'000u);
    EXPECT_EQ(FILECACHE_BYPASS_THRESHOLD, 256ULL * 1024 * 1024);
    EXPECT_DOUBLE_EQ(FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO, 0.0);
    EXPECT_DOUBLE_EQ(FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO, 0.0);
    EXPECT_EQ(FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH, 250u);
    EXPECT_EQ(FILECACHE_DEFAULT_FREE_SPACE_EVICTION_THREADS, 1u);
    EXPECT_EQ(FILECACHE_DEFAULT_CACHE_POLICY, FileCachePolicy::SLRU);
    EXPECT_DOUBLE_EQ(FILECACHE_DEFAULT_SLRU_RATIO, 0.6);
}

TEST(FileCacheFwdTest, FileCachePtrIsShared)
{
    static_assert(std::is_same_v<FileCachePtr, std::shared_ptr<FileCache>>);
}

TEST(FileCacheFwdTest, FileCacheSettingsAliasesConfig)
{
    static_assert(std::is_same_v<FileCacheSettings, FileCacheConfig>);
}

// ── FileCache_fwd_internal.h ──────────────────────────────────────────────────

TEST(FileCacheFwdInternalTest, FileSegmentsIsList)
{
    static_assert(
        std::is_same_v<FileSegments, std::list<std::shared_ptr<FileSegment>>>);
}

TEST(FileCacheFwdInternalTest, KeyMetadataWeakPtrIsWeak)
{
    static_assert(
        std::is_same_v<KeyMetadataWeakPtr, std::weak_ptr<KeyMetadata>>);
}

// ── FileCacheUtils ────────────────────────────────────────────────────────────

TEST(FileCacheUtilsTest, RoundDownZeroMultiple)
{
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(0, 0), 0u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(123, 0), 123u);
}

TEST(FileCacheUtilsTest, RoundDownBoundaries)
{
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(7, 8), 0u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(8, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundDownToMultiple(9, 8), 8u);
}

TEST(FileCacheUtilsTest, RoundUpZeroMultiple)
{
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 0), 0u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(123, 0), 123u);
}

TEST(FileCacheUtilsTest, RoundUpBoundaries)
{
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(0, 8), 0u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(1, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(8, 8), 8u);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(9, 8), 16u);
}

TEST(FileCacheUtilsTest, RoundUpRepresentableBoundary)
{
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(2, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX - 1, SIZE_MAX), SIZE_MAX);
    EXPECT_EQ(FileCacheUtils::roundUpToMultiple(SIZE_MAX, 1), SIZE_MAX);
}

TEST(FileCacheUtilsTest, RoundUpActualOverflow)
{
    EXPECT_THROW(
        FileCacheUtils::roundUpToMultiple(SIZE_MAX, SIZE_MAX - 1),
        std::overflow_error);
    EXPECT_THROW(
        FileCacheUtils::roundUpToMultiple(SIZE_MAX - 1, 4),
        std::overflow_error);
}

} // namespace
} // namespace facebook::velox::ch
