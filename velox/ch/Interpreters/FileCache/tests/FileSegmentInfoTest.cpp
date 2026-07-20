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
#include "velox/ch/Interpreters/FileCache/FileSegmentInfo.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

/// The enum-layout checks are `static_assert`s: they are verified at compile
/// time, so a successful compilation of this translation unit is itself the
/// evidence that the ported `FileSegmentState`/`FileSegmentKind` values and
/// order match ClickHouse. Any drift makes this file fail to compile.
TEST(FileSegmentInfoTest, StateEnumLayout)
{
    // Order and underlying values preserved from ClickHouse.
    static_assert(static_cast<uint8_t>(FileSegmentState::DOWNLOADED) == 0);
    static_assert(static_cast<uint8_t>(FileSegmentState::EMPTY) == 1);
    static_assert(static_cast<uint8_t>(FileSegmentState::DOWNLOADING) == 2);
    static_assert(
        static_cast<uint8_t>(FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION) == 3);
    static_assert(static_cast<uint8_t>(FileSegmentState::PARTIALLY_DOWNLOADED) == 4);
    static_assert(static_cast<uint8_t>(FileSegmentState::DETACHED) == 5);

    static_assert(std::is_same_v<std::underlying_type_t<FileSegmentState>, uint8_t>);

    // Also assert at runtime so the case is a real executed test, not only a
    // compile-time predicate.
    EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::DOWNLOADED), 0);
    EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::DETACHED), 5);
}

TEST(FileSegmentInfoTest, KindEnumLayout)
{
    static_assert(static_cast<uint8_t>(FileSegmentKind::Regular) == 0);
    static_assert(static_cast<uint8_t>(FileSegmentKind::Ephemeral) == 1);
    static_assert(std::is_same_v<std::underlying_type_t<FileSegmentKind>, uint8_t>);

    EXPECT_EQ(static_cast<uint8_t>(FileSegmentKind::Regular), 0);
    EXPECT_EQ(static_cast<uint8_t>(FileSegmentKind::Ephemeral), 1);
}

TEST(FileSegmentInfoTest, InfoSnapshotDefaults)
{
    // The snapshot struct is an aggregate; its default field values and types
    // must match the ClickHouse-authoritative shape (time_t wall-clock finish
    // time, uint64_t references, QueueEntryType, embedded OriginInfo).
    FileSegmentInfo info;

    EXPECT_EQ(info.offset, 0u);
    EXPECT_TRUE(info.path.empty());
    EXPECT_EQ(info.range_left, 0u);
    EXPECT_EQ(info.range_right, 0u);
    EXPECT_EQ(info.kind, FileSegmentKind::Regular);
    EXPECT_EQ(info.state, FileSegmentState::EMPTY);
    EXPECT_EQ(info.size, 0u);
    EXPECT_EQ(info.downloaded_size, 0u);
    EXPECT_EQ(info.download_finished_time, static_cast<time_t>(0));
    EXPECT_EQ(info.cache_hits, 0u);
    EXPECT_EQ(info.references, 0u);
    EXPECT_FALSE(info.is_unbound);
    EXPECT_EQ(info.queue_entry_type, IFileCachePriority::QueueEntryType::None);

    static_assert(std::is_same_v<decltype(info.download_finished_time), time_t>);
    static_assert(std::is_same_v<decltype(info.references), uint64_t>);
    static_assert(std::is_same_v<decltype(info.key), FileCacheKey>);
    static_assert(std::is_same_v<decltype(info.origin), FileCacheOriginInfo>);
}

TEST(FileSegmentInfoTest, InfoSnapshotIsAssignable)
{
    // Fields can be populated exactly as `FileSegment::getInfo` does (designated
    // initialization order and types).
    FileSegmentInfo info;
    info.offset = 100;
    info.path = "/cache/000/abc/100";
    info.range_left = 100;
    info.range_right = 199;
    info.kind = FileSegmentKind::Ephemeral;
    info.state = FileSegmentState::DOWNLOADED;
    info.size = 100;
    info.downloaded_size = 100;
    info.download_finished_time = 1700000000;
    info.cache_hits = 3;
    info.references = 2;
    info.is_unbound = true;
    info.queue_entry_type = IFileCachePriority::QueueEntryType::LRU;

    EXPECT_EQ(info.range_right - info.range_left + 1, info.size);
    EXPECT_EQ(info.state, FileSegmentState::DOWNLOADED);
    EXPECT_TRUE(info.is_unbound);
    EXPECT_EQ(info.queue_entry_type, IFileCachePriority::QueueEntryType::LRU);
}

// NOTE: `toString(FileSegmentKind)` is defined in FileSegment.cpp, whose object
// only links once the center SCC (Metadata.cpp + FileCache.cpp) is present.
// This case therefore runs at the final SCC link; it compiles now against the
// real declaration.
TEST(FileSegmentInfoTest, KindToString)
{
    EXPECT_EQ(toString(FileSegmentKind::Regular), "Regular");
    EXPECT_EQ(toString(FileSegmentKind::Ephemeral), "Ephemeral");
}

} // namespace
} // namespace facebook::velox::ch
