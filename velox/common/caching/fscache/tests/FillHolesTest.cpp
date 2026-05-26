/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "velox/common/caching/fscache/FsCache.h"
#include "velox/common/caching/fscache/FileSegment.h"

#include <gtest/gtest.h>

namespace facebook::velox::cache::fs::test {

namespace {
FsCacheConfig defaultCfg() {
  FsCacheConfig cfg;
  cfg.alignment = 4UL << 20; // 4 MiB
  cfg.maxSegmentSize = 32UL << 20; // 32 MiB
  return cfg;
}

FileSegmentPtr makeSeg(uint64_t off, uint64_t size, const std::string& path) {
  return std::make_shared<FileSegment>(
      FsCacheKey{PathKey::fromPath(path), off, size}, path);
}
} // namespace

TEST(FillHolesTest, emptyFoundProducesSingleHoleSlicedByMaxSize) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {}, 0, 4'096, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 4UL);
  EXPECT_EQ(result[0]->key().offset, 0UL);
  EXPECT_EQ(result[0]->key().size, 1'024UL);
  EXPECT_EQ(result[3]->key().offset, 3'072UL);
  EXPECT_EQ(result[3]->key().size, 1'024UL);
  for (const auto& s : result) {
    EXPECT_EQ(s->state(), FileSegment::State::kEmpty);
  }
}

TEST(FillHolesTest, leadingHoleOnly) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto existing = makeSeg(2'048, 1'024, "/r/x");
  ASSERT_TRUE(md.insert(existing));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {existing}, 0, 3'072, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0]->key().offset, 0UL);
  EXPECT_EQ(result[1]->key().offset, 1'024UL);
  EXPECT_EQ(result[2]->key().offset, 2'048UL);
  EXPECT_EQ(result[2], existing);
}

TEST(FillHolesTest, trailingHoleOnly) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto existing = makeSeg(0, 1'024, "/r/x");
  ASSERT_TRUE(md.insert(existing));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {existing}, 0, 3'072, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0], existing);
  EXPECT_EQ(result[1]->key().offset, 1'024UL);
  EXPECT_EQ(result[2]->key().offset, 2'048UL);
}

TEST(FillHolesTest, middleHole) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto left = makeSeg(0, 1'024, "/r/x");
  auto right = makeSeg(2'048, 1'024, "/r/x");
  ASSERT_TRUE(md.insert(left));
  ASSERT_TRUE(md.insert(right));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {left, right}, 0, 3'072, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 3UL);
  EXPECT_EQ(result[0], left);
  EXPECT_EQ(result[1]->key().offset, 1'024UL);
  EXPECT_EQ(result[1]->state(), FileSegment::State::kEmpty);
  EXPECT_EQ(result[2], right);
}

TEST(FillHolesTest, noHolesReturnsFoundUnchanged) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto a = makeSeg(0, 1'024, "/r/x");
  auto b = makeSeg(1'024, 1'024, "/r/x");
  ASSERT_TRUE(md.insert(a));
  ASSERT_TRUE(md.insert(b));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {a, b}, 0, 2'048, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 2UL);
  EXPECT_EQ(result[0], a);
  EXPECT_EQ(result[1], b);
}

TEST(FillHolesTest, holeLargerThanMaxSegmentSizeSplits) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 512;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {}, 0, 2'048, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 4UL);
  for (size_t i{0}; i < 4; ++i) {
    EXPECT_EQ(result[i]->key().offset, i * 512UL);
    EXPECT_EQ(result[i]->key().size, 512UL);
  }
}

TEST(FillHolesTest, emptyRangeProducesEmptyResult) {
  // Zero-size [lo, hi) with lo == hi must yield empty result and must not
  // insert any segment into the per-key map.
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {}, 100, 100, p, "/r/x", locked, md, cfg);
  EXPECT_TRUE(result.empty());
  EXPECT_EQ(locked.get()->numSegments, 0UL);
}

TEST(FillHolesTest, singleSegmentExactlyCoversRangeReturnsItUnchanged) {
  auto cfg = defaultCfg();
  cfg.maxSegmentSize = 1'024;
  FsCacheMetadata md{4};
  PathKey p = PathKey::fromPath("/r/x");
  auto seg = makeSeg(0, 1'024, "/r/x");
  ASSERT_TRUE(md.insert(seg));
  auto locked = md.lockKeyMetadata(p, KeyNotFoundPolicy::kCreateEmpty);
  auto result = FsCache::fillHolesWithEmptyFileSegments(
      {seg}, 0, 1'024, p, "/r/x", locked, md, cfg);
  ASSERT_EQ(result.size(), 1UL);
  EXPECT_EQ(result[0], seg);
}

} // namespace facebook::velox::cache::fs::test
