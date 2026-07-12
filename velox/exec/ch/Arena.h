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

#pragma once

#include "velox/common/memory/MemoryPool.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace facebook::velox::exec::ch {

class Arena {
 public:
  explicit Arena(
      memory::MemoryPool* pool,
      size_t initialSize = 4096,
      size_t growthFactor = 2,
      size_t linearGrowthThreshold = 128 * 1024 * 1024);

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  char* alloc(size_t size);
  char* alignedAlloc(size_t size, size_t alignment);

  template <typename T>
  T* alloc() {
    return reinterpret_cast<T*>(alignedAlloc(sizeof(T), alignof(T)));
  }

  void* rollback(size_t size);

  char* allocContinue(
      size_t additionalBytes,
      const char*& rangeStart,
      size_t startAlignment = 0);

  char* realloc(const char* oldData, size_t oldSize, size_t newSize);
  char* alignedRealloc(
      const char* oldData,
      size_t oldSize,
      size_t newSize,
      size_t alignment);

  const char* insert(const char* data, size_t size);
  const char* alignedInsert(
      const char* data,
      size_t size,
      size_t alignment);

  size_t allocatedBytes() const {
    return allocatedBytes_;
  }

  size_t usedBytes() const {
    return usedBytes_;
  }

  size_t remainingSpaceInCurrentMemoryChunk() const;

 private:
  static constexpr size_t kPaddingForSimd = 64;
  static constexpr size_t kPadRight = kPaddingForSimd - 1;

  struct alignas(16) MemoryChunk {
    explicit MemoryChunk(memory::MemoryPool* pool) : pool(pool) {}
    MemoryChunk(memory::MemoryPool* pool, size_t size);
    ~MemoryChunk();

    MemoryChunk(const MemoryChunk&) = delete;
    MemoryChunk& operator=(const MemoryChunk&) = delete;
    MemoryChunk(MemoryChunk&& other) noexcept;
    MemoryChunk& operator=(MemoryChunk&& other) noexcept;

    void swap(MemoryChunk& other) noexcept;
    bool empty() const {
      return begin == nullptr;
    }
    size_t size() const {
      return allocationSize;
    }
    size_t remaining() const {
      return empty() ? 0 : static_cast<size_t>(end - pos);
    }

    memory::MemoryPool* pool;
    char* begin{nullptr};
    char* pos{nullptr};
    char* end{nullptr};
    size_t allocationSize{0};
    std::unique_ptr<MemoryChunk> prev;
  };

  static size_t roundUpToPageSize(size_t size, size_t pageSize);
  size_t nextSize(size_t minNextSize) const;
  void addMemoryChunk(size_t minSize);

  memory::MemoryPool* pool_;
  size_t initialSize_;
  size_t growthFactor_;
  size_t linearGrowthThreshold_;
  MemoryChunk head_;
  size_t allocatedBytes_{0};
  size_t usedBytes_{0};
  size_t pageSize_;
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;

} // namespace facebook::velox::exec::ch
