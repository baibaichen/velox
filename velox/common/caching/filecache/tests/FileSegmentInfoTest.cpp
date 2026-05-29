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
#include "velox/common/caching/filecache/FileSegmentInfo.h"

#include <gtest/gtest.h>

using namespace facebook::velox::ch;

TEST(FileSegmentInfoTest, enumUnderlyingValues) {
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::DOWNLOADED), 0);
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::EMPTY), 1);
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::DOWNLOADING), 2);
  EXPECT_EQ(
      static_cast<uint8_t>(
          FileSegmentState::PARTIALLY_DOWNLOADED_NO_CONTINUATION),
      3);
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::PARTIALLY_DOWNLOADED), 4);
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentState::DETACHED), 5);

  EXPECT_EQ(static_cast<uint8_t>(FileSegmentKind::Regular), 0);
  EXPECT_EQ(static_cast<uint8_t>(FileSegmentKind::Ephemeral), 1);

  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::None), 0);
  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::LRU), 1);
  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::SLRU_Protected), 2);
  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::SLRU_Probationary), 3);
  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::SplitCache_Data), 4);
  EXPECT_EQ(static_cast<uint8_t>(FileCacheQueueEntryType::SplitCache_System), 5);
}

TEST(FileSegmentInfoTest, aggregateHoldsFields) {
  FileSegmentInfo info;
  info.offset = 1024;
  info.path = "/cache/file";
  info.rangeLeft = 0;
  info.rangeRight = 4095;
  info.kind = FileSegmentKind::Regular;
  info.state = FileSegmentState::DOWNLOADED;
  info.size = 4096;
  info.downloadedSize = 4096;
  info.cacheHits = 3;
  info.references = 1;
  info.isUnbound = false;
  info.queueEntryType = FileCacheQueueEntryType::LRU;

  EXPECT_EQ(info.offset, 1024);
  EXPECT_EQ(info.path, "/cache/file");
  EXPECT_EQ(info.rangeRight, 4095);
  EXPECT_EQ(info.kind, FileSegmentKind::Regular);
  EXPECT_EQ(info.state, FileSegmentState::DOWNLOADED);
  EXPECT_EQ(info.downloadedSize, 4096);
  EXPECT_EQ(info.queueEntryType, FileCacheQueueEntryType::LRU);
}
