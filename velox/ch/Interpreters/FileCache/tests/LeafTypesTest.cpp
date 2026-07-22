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

// Streaming vs one-shot parity across many split points, including splits that
// land mid-word (not on an 8-byte boundary) so the partial-buffer carry path in
// SipHash128::update is exercised. CH sipHash128 supports streaming (see the CH
// SipHash.h header comment "done streaming"), so on-disk keys must be identical
// whether the path is hashed whole or in chunks.
TEST(SipHash128Test, IncrementalEqualsOneShotMultiChunk)
{
    // 37 bytes: not a multiple of 8, so tail-handling is covered too.
    const std::string data = "s3://bucket/some/long/object/key.parquet";
    const size_t n = data.size();
    const auto oneShot = sipHash128(data.data(), n);

    for (size_t split1 = 0; split1 <= n; ++split1)
    {
        for (size_t split2 = split1; split2 <= n; ++split2)
        {
            SipHash128 inc;
            inc.update(data.data(), split1);
            inc.update(data.data() + split1, split2 - split1);
            inc.update(data.data() + split2, n - split2);
            EXPECT_EQ(oneShot, inc.get128())
                << "split1=" << split1 << " split2=" << split2;
        }
    }
}

// Golden hashes independently derived from authoritative ClickHouse.
//
// Oracle method: compiled a standalone program (clang++-19, -std=c++23) that
// includes ClickHouse's real src/Common/SipHash.h and base/base/hex.h, computing
//   getHexUIntLowercase(DB::sipHash128(x.data(), x.size()))
// for each input. This is CH's exact FileCacheKey::fromPath -> toString path:
//   FileCacheKey.cpp:33  FileCacheKey(sipHash128(path.data(), path.size()))
//   FileCacheKey.cpp:23  getHexUIntLowercase(key)
// Producing (CH master):
//   ""                                 -> f711edcba8b6b5e5e983a656dbc1b532
//   "abc"                              -> 53a3124ce5655a686c6b96daa215b4b6
//   "s3://bucket/key"                  -> 6ba3177b6fbaa4c9f65873033e35aeaa
//   "0123456789abcdef0123456789abcdef" -> 77dd7dd78fa45ef0b93cc3b8df847cbd
// These match our impl below, so the goldens are provably CH-anchored (byte
// identical), not self-referential. If any of these ever diverge, our
// SipHash128/toString has a real CH-parity bug — do not adjust the golden.

TEST(FileCacheKeyTest, GoldenEmpty)
{
    // CH oracle: DB::sipHash128("", 0) -> f711edcba8b6b5e5e983a656dbc1b532
    EXPECT_EQ(
        FileCacheKey::fromPath("").toString(),
        "f711edcba8b6b5e5e983a656dbc1b532");
}

TEST(FileCacheKeyTest, GoldenAbc)
{
    // CH oracle: DB::sipHash128("abc", 3) -> 53a3124ce5655a686c6b96daa215b4b6
    EXPECT_EQ(
        FileCacheKey::fromPath("abc").toString(),
        "53a3124ce5655a686c6b96daa215b4b6");
}

TEST(FileCacheKeyTest, GoldenS3Path)
{
    // CH oracle: DB::sipHash128("s3://bucket/key") -> 6ba3177b6fbaa4c9f65873033e35aeaa
    EXPECT_EQ(
        FileCacheKey::fromPath("s3://bucket/key").toString(),
        "6ba3177b6fbaa4c9f65873033e35aeaa");
}

TEST(FileCacheKeyTest, GoldenLong)
{
    // CH oracle: DB::sipHash128("0123...abcdef") -> 77dd7dd78fa45ef0b93cc3b8df847cbd
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

TEST(FileCacheKeyTest, FromKeyStringMalformedCharCompatibility)
{
    // CH-oracle verified: DB::unhexUInt<UInt128>("g0...0") -> f0...0 (the exact
    // path CH FileCacheKey::fromKeyString takes for 32-char input, FileCacheKey.cpp:45).
    // CH FileCacheKey::fromKeyString delegates all 32-byte input to unhexUInt
    // without per-character validation. Non-hex 'g' maps to nibble 0xFF via the
    // lookup table; accumulation via addition (not OR) with natural uint64_t
    // overflow: 0xFF after 15 left 4-bit shifts yields 0xF000000000000000 for
    // the high word. So g0...0 must not throw and must stringify as f0...0.
    FileCacheKey key;
    ASSERT_NO_THROW(key = FileCacheKey::fromKeyString("g0000000000000000000000000000000"));
    EXPECT_EQ(key.toString(), "f0000000000000000000000000000000");
}

TEST(FileCacheKeyTest, UppercaseParserRoundTrip)
{
    // CH-oracle verified: DB::unhexUInt<UInt128>("AABBCCDD...") == the lowercase
    // parse, and getHexUIntLowercase emits lowercase "aabbccdd...".
    // CH unhexUInt accepts both upper- and lower-case hex via hex_char_to_digit_table.
    // Parse the same 128-bit value as lowercase and uppercase; results must be equal.
    // toString must emit the exact lowercase numeric form (fmt {:016x} format).
    const std::string lower = "aabbccdd11223344aabbccdd11223344";
    std::string upper = lower;
    for (auto & c : upper)
    {
        if (c >= 'a' && c <= 'f')
            c = static_cast<char>(c - 'a' + 'A');
    }
    // upper == "AABBCCDD11223344AABBCCDD11223344"
    const auto fromLower = FileCacheKey::fromKeyString(lower);
    FileCacheKey fromUpper;
    ASSERT_NO_THROW(fromUpper = FileCacheKey::fromKeyString(upper));
    EXPECT_EQ(fromUpper, fromLower);
    EXPECT_EQ(fromUpper.toString(), lower); // toString always emits lowercase
}

TEST(FileCacheKeyTest, MalformedCarryHighWord)
{
    // CH-oracle verified: DB::unhexUInt<UInt128>("fg0...0") -> ef0...0.
    // 'f'=15, 'g'=0xFF (invalid). Using addition (not OR), the high word accumulates:
    //   i=0: hi = 0x0F
    //   i=1: hi = (0x0F << 4) + 0xFF = 0xF0 + 0xFF = 0x1EF
    //   i=2..15: hi = hi << 4  (14 more shifts, each ×16)
    // Final hi = 0x1EF << 56 (mod 2^64) = 0xEF00000000000000.
    // With OR instead of +, i=1 gives 0xF0 | 0xFF = 0xFF, yielding 0xFF00000000000000
    // (result "ff000000000000000000000000000000"), so this test distinguishes the two.
    FileCacheKey key;
    ASSERT_NO_THROW(key = FileCacheKey::fromKeyString("fg000000000000000000000000000000"));
    EXPECT_EQ(key.toString(), "ef000000000000000000000000000000");
}

TEST(FileCacheKeyTest, MalformedCarryLowWord)
{
    // CH-oracle verified: DB::unhexUInt<UInt128>("0..fg..0") -> 0..ef..0.
    // Same carry arithmetic as MalformedCarryHighWord but exercised in the low
    // 64-bit accumulation loop (chars 16..31). High word is all '0' so hi = 0.
    // With addition: lo = 0xEF00000000000000. With OR: lo = 0xFF00000000000000.
    FileCacheKey key;
    ASSERT_NO_THROW(key = FileCacheKey::fromKeyString("0000000000000000fg00000000000000"));
    EXPECT_EQ(key.toString(), "0000000000000000ef00000000000000");
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

// ── FileCacheUtils::checkedAdd ────────────────────────────────────────────────

TEST(FileCacheUtilsTest, CheckedAddZero)
{
    EXPECT_EQ(FileCacheUtils::checkedAdd(uint64_t{0}, uint64_t{0}, "zero_op"), uint64_t{0});
    EXPECT_EQ(FileCacheUtils::checkedAdd(uint64_t{0}, uint64_t{5}, "zero_op"), uint64_t{5});
    EXPECT_EQ(FileCacheUtils::checkedAdd(uint64_t{5}, uint64_t{0}, "zero_op"), uint64_t{5});
}

TEST(FileCacheUtilsTest, CheckedAddNormal)
{
    EXPECT_EQ(
        FileCacheUtils::checkedAdd(uint64_t{3}, uint64_t{4}, "normal_op"),
        uint64_t{7});
}

TEST(FileCacheUtilsTest, CheckedAddMaxNoOverflow)
{
    EXPECT_EQ(
        FileCacheUtils::checkedAdd(UINT64_MAX, uint64_t{0}, "max_op"),
        UINT64_MAX);
}

TEST(FileCacheUtilsTest, CheckedAddOverflow)
{
    EXPECT_THROW(
        FileCacheUtils::checkedAdd(UINT64_MAX, uint64_t{1}, "overflow_op"),
        VeloxRuntimeError);
}

TEST(FileCacheUtilsTest, CheckedAddOperationInMessage)
{
    try {
        FileCacheUtils::checkedAdd(UINT64_MAX, uint64_t{1}, "budget_overflow_test");
        FAIL() << "Expected VeloxRuntimeError to be thrown";
    } catch (const VeloxRuntimeError & e) {
        EXPECT_NE(
            std::string(e.what()).find("budget_overflow_test"),
            std::string::npos)
            << "VeloxRuntimeError message must contain the operation name";
    }
}

} // namespace
} // namespace facebook::velox::ch
