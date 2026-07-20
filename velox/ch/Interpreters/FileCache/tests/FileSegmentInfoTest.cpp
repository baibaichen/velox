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
#include <ctime>
#include <type_traits>

namespace facebook::velox::ch
{
namespace
{

/// RED oracle: the on-disk/system-table layout requires this exact enum order and
/// underlying values, ported verbatim from ClickHouse `FileSegmentInfo.h`. Any
/// reorder of the pre-implementation/broken enum fails these static_asserts.
TEST(FileSegmentInfoTest, StateEnumLayout)
{
    static_assert(static_cast<uint8_t>(FileSegmentState::DOWNLOADED) == 0);
    static_assert(static_cast<uint8_t>(FileSegmentState::EMPTY) == 1);
    static_assert(static_cast<uint8_t>(FileSegmentState::DOWNLOADING) == 2);
    static_assert(static_cast<uint8_t>(FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION) == 3);
    static_assert(static_cast<uint8_t>(FileSegmentState::PARTIALLY_DOWNLOADED) == 4);
    static_assert(static_cast<uint8_t>(FileSegmentState::DETACHED) == 5);
    SUCCEED();
}

TEST(FileSegmentInfoTest, KindEnumLayout)
{
    static_assert(static_cast<uint8_t>(FileSegmentKind::Regular) == 0);
    static_assert(static_cast<uint8_t>(FileSegmentKind::Ephemeral) == 1);
    SUCCEED();
}

/// CH-source authority: `download_finished_time` is `time_t` (CH `FileSegmentInfo.h:74`),
/// not a chrono time_point. A regression to chrono would fail this compile-time check.
TEST(FileSegmentInfoTest, DownloadFinishedTimeIsTimeT)
{
    static_assert(std::is_same_v<decltype(FileSegmentInfo::download_finished_time), time_t>);
    SUCCEED();
}

TEST(FileSegmentInfoTest, InfoSnapshotFieldsPresent)
{
    FileSegmentInfo info{};
    info.offset = 100;
    info.range_left = 100;
    info.range_right = 199;
    info.kind = FileSegmentKind::Regular;
    info.state = FileSegmentState::DOWNLOADED;
    info.size = 100;
    info.downloaded_size = 100;
    info.download_finished_time = 0;
    info.cache_hits = 0;
    info.references = 1;
    info.is_unbound = false;
    info.queue_entry_type = IFileCachePriority::QueueEntryType::LRU;

    EXPECT_EQ(info.offset, 100u);
    EXPECT_EQ(info.range_right - info.range_left + 1, 100u);
    EXPECT_EQ(info.state, FileSegmentState::DOWNLOADED);
    EXPECT_EQ(info.queue_entry_type, IFileCachePriority::QueueEntryType::LRU);
}

TEST(FileSegmentInfoTest, KindToString)
{
    EXPECT_EQ(toString(FileSegmentKind::Regular), "Regular");
    EXPECT_EQ(toString(FileSegmentKind::Ephemeral), "Ephemeral");
}

} // namespace
} // namespace facebook::velox::ch
