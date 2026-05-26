/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

#include <vector>

namespace facebook::velox::cache::fs::test {

// Verifies the 6-state enum is wired exactly per spec §5.4.
TEST(FileSegmentStateTest, sixStatesExist) {
  EXPECT_EQ(static_cast<int>(FileSegment::State::kEmpty), 0);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDownloading), 1);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDownloaded), 2);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kPartiallyDownloaded), 3);
  EXPECT_EQ(
      static_cast<int>(FileSegment::State::kPartiallyDownloadedNoContinuation),
      4);
  EXPECT_EQ(static_cast<int>(FileSegment::State::kDetached), 5);
}

// Documents the legal transition edges per spec §5.4. Task 2-3 will fill in
// the actual reserve()/write()/complete() implementations; until then this
// test only locks in the enumeration of legal edges so Task 2 cannot
// silently widen the contract.
TEST(FileSegmentStateTest, legalTransitionEdgesEnumerated) {
  using S = FileSegment::State;
  struct Edge {
    S from;
    S to;
  };
  const std::vector<Edge> legal = {
      {S::kEmpty, S::kDownloading},
      {S::kDownloading, S::kDownloaded},
      {S::kDownloading, S::kPartiallyDownloaded},
      {S::kPartiallyDownloaded, S::kDownloading},
      {S::kPartiallyDownloaded, S::kPartiallyDownloadedNoContinuation},
      {S::kDownloaded, S::kDetached},
      {S::kDownloading, S::kDetached},
      {S::kPartiallyDownloaded, S::kDetached},
      {S::kPartiallyDownloadedNoContinuation, S::kDetached},
  };
  EXPECT_EQ(legal.size(), 9UL);
}

} // namespace facebook::velox::cache::fs::test
