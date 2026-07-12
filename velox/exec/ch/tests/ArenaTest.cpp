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

#include "velox/exec/ch/Arena.h"
#include "velox/common/memory/Memory.h"

#include <cstdint>
#include <cstring>

#include <gtest/gtest.h>

namespace facebook::velox::exec::ch {
namespace {

class ArenaTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Idempotent: sibling suites in this binary (e.g. RowRefListTest) may have
    // already initialized the process-wide manager, and initialize() throws on
    // a second call. Guard on testInstance() so suite order does not matter.
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("ch-arena-test");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(ArenaTest, accountsForGrowingChunksAndReleasesThem) {
  EXPECT_EQ(pool_->usedBytes(), 0);

  {
    Arena arena(pool_.get());
    arena.alloc(1);
    const auto firstChunkBytes = pool_->usedBytes();
    EXPECT_GT(firstChunkBytes, 0);

    arena.alloc(4096);
    EXPECT_GT(pool_->usedBytes(), firstChunkBytes);
  }

  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(ArenaTest, returnsAlignedLowAddressesWithWritableSimdPadding) {
  Arena arena(pool_.get());

  constexpr size_t kInitialUsableBytes = 4096 - 63;
  auto* bytes = arena.alloc(kInitialUsableBytes);
  std::memset(bytes + kInitialUsableBytes, 0x5a, 63);

  auto* word = arena.alloc<uint64_t>();
  EXPECT_EQ(reinterpret_cast<uintptr_t>(word) % alignof(uint64_t), 0);
  EXPECT_LT(reinterpret_cast<uintptr_t>(word), 1ull << 48);
  *word = 0x0123456789abcdefULL;
  EXPECT_EQ(*word, 0x0123456789abcdefULL);
}

} // namespace
} // namespace facebook::velox::exec::ch
