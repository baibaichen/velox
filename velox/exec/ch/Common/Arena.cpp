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

#include "velox/exec/ch/Common/Arena.h"

#include "velox/common/base/Exceptions.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <utility>

#include <unistd.h>

#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#include <sanitizer/asan_interface.h>
#else
#define ASAN_POISON_MEMORY_REGION(address, size) ((void)0)
#define ASAN_UNPOISON_MEMORY_REGION(address, size) ((void)0)
#endif

namespace facebook::velox::exec::ch {

Arena::MemoryChunk::MemoryChunk(memory::MemoryPool* pool, size_t size)
    : pool(pool),
      begin(static_cast<char*>(pool->allocate(size))),
      pos(begin),
      end(begin + size - kPadRight),
      allocationSize(size) {}

Arena::MemoryChunk::~MemoryChunk() {
  if (empty()) {
    return;
  }
  ASAN_UNPOISON_MEMORY_REGION(begin, allocationSize);
  pool->free(begin, allocationSize);
}

Arena::MemoryChunk::MemoryChunk(MemoryChunk&& other) noexcept
    : pool(other.pool) {
  swap(other);
}

Arena::MemoryChunk& Arena::MemoryChunk::operator=(
    MemoryChunk&& other) noexcept {
  swap(other);
  return *this;
}

void Arena::MemoryChunk::swap(MemoryChunk& other) noexcept {
  std::swap(pool, other.pool);
  std::swap(begin, other.begin);
  std::swap(pos, other.pos);
  std::swap(end, other.end);
  std::swap(allocationSize, other.allocationSize);
  prev.swap(other.prev);
}

Arena::Arena(
    memory::MemoryPool* pool,
    size_t initialSize,
    size_t growthFactor,
    size_t linearGrowthThreshold)
    : pool_(pool),
      initialSize_(initialSize),
      growthFactor_(growthFactor),
      linearGrowthThreshold_(linearGrowthThreshold),
      head_(pool),
      pageSize_(static_cast<size_t>(::sysconf(_SC_PAGESIZE))) {
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_NE(pageSize_, static_cast<size_t>(-1));
}

size_t Arena::roundUpToPageSize(size_t size, size_t pageSize) {
  return (size + pageSize - 1) / pageSize * pageSize;
}

size_t Arena::nextSize(size_t minNextSize) const {
  size_t sizeAfterGrow;
  if (head_.empty()) {
    sizeAfterGrow = std::max(minNextSize, initialSize_);
  } else if (head_.size() < linearGrowthThreshold_) {
    sizeAfterGrow = std::max(minNextSize, head_.size() * growthFactor_);
  } else {
    sizeAfterGrow =
        (minNextSize + linearGrowthThreshold_ - 1) /
        linearGrowthThreshold_ * linearGrowthThreshold_;
  }
  assert(sizeAfterGrow >= minNextSize);
  return roundUpToPageSize(sizeAfterGrow, pageSize_);
}

void Arena::addMemoryChunk(size_t minSize) {
  const auto nextSize = this->nextSize(minSize + kPadRight);
  if (head_.empty()) {
    head_ = MemoryChunk(pool_, nextSize);
  } else {
    auto chunk = std::make_unique<MemoryChunk>(pool_, nextSize);
    head_.swap(*chunk);
    head_.prev = std::move(chunk);
  }
  allocatedBytes_ += head_.size();
}

char* Arena::alloc(size_t size) {
  usedBytes_ += size;
  if (head_.empty() || size > head_.remaining()) {
    addMemoryChunk(size);
  }

  char* result = head_.pos;
  head_.pos += size;
  ASAN_UNPOISON_MEMORY_REGION(result, size + kPadRight);
  return result;
}

char* Arena::alignedAlloc(size_t size, size_t alignment) {
  usedBytes_ += size;
  if (head_.empty() || size > head_.remaining()) {
    addMemoryChunk(size + alignment);
  }

  for (;;) {
    void* headPosition = head_.pos;
    size_t space = static_cast<size_t>(head_.end - head_.pos);
    auto* result =
        static_cast<char*>(std::align(alignment, size, headPosition, space));
    if (result != nullptr) {
      head_.pos = static_cast<char*>(headPosition) + size;
      ASAN_UNPOISON_MEMORY_REGION(result, size + kPadRight);
      return result;
    }
    addMemoryChunk(size + alignment);
  }
}

void* Arena::rollback(size_t size) {
  assert(size <= usedBytes_);
  assert(size <= static_cast<size_t>(head_.pos - head_.begin));
  usedBytes_ -= size;
  head_.pos -= size;
  ASAN_POISON_MEMORY_REGION(head_.pos, size + kPadRight);
  return head_.pos;
}

char* Arena::allocContinue(
    size_t additionalBytes,
    const char*& rangeStart,
    size_t startAlignment) {
  assert(additionalBytes > 0);
  if (rangeStart == nullptr) {
    char* result = startAlignment
        ? alignedAlloc(additionalBytes, startAlignment)
        : alloc(additionalBytes);
    rangeStart = result;
    return result;
  }

  assert(rangeStart >= head_.begin);
  assert(rangeStart < head_.end);
  if (head_.pos + additionalBytes <= head_.end) {
    return alloc(additionalBytes);
  }

  const size_t existingBytes = head_.pos - rangeStart;
  const size_t newBytes = existingBytes + additionalBytes;
  const char* oldRange = rangeStart;
  char* newRange = startAlignment ? alignedAlloc(newBytes, startAlignment)
                                  : alloc(newBytes);
  std::memcpy(newRange, oldRange, existingBytes);
  rangeStart = newRange;
  return newRange + existingBytes;
}

char* Arena::realloc(
    const char* oldData,
    size_t oldSize,
    size_t newSize) {
  char* result = alloc(newSize);
  if (oldData != nullptr) {
    std::memcpy(result, oldData, oldSize);
    ASAN_POISON_MEMORY_REGION(oldData, oldSize);
  }
  return result;
}

char* Arena::alignedRealloc(
    const char* oldData,
    size_t oldSize,
    size_t newSize,
    size_t alignment) {
  char* result = alignedAlloc(newSize, alignment);
  if (oldData != nullptr) {
    std::memcpy(result, oldData, oldSize);
    ASAN_POISON_MEMORY_REGION(oldData, oldSize);
  }
  return result;
}

const char* Arena::insert(const char* data, size_t size) {
  char* result = alloc(size);
  std::memcpy(result, data, size);
  return result;
}

const char* Arena::alignedInsert(
    const char* data,
    size_t size,
    size_t alignment) {
  char* result = alignedAlloc(size, alignment);
  std::memcpy(result, data, size);
  return result;
}

size_t Arena::remainingSpaceInCurrentMemoryChunk() const {
  return head_.remaining();
}

} // namespace facebook::velox::exec::ch
