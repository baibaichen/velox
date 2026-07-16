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

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace facebook::velox::exec::ch {

class HashTableAllocatorAdapter {
 public:
  explicit HashTableAllocatorAdapter(memory::MemoryPool* pool) : pool_(pool) {
    VELOX_CHECK_NOT_NULL(pool_);
  }

  void* alloc(size_t size, size_t alignment = 0) {
    if (size == 0) {
      return nullptr;
    }

    void* buffer = alignment == 0
        ? pool_->allocateZeroFilled(1, checkedSize(size))
        : pool_->allocateAligned(checkedSize(size), checkedAlignment(alignment));
    if (alignment != 0) {
      std::memset(buffer, 0, size);
    }
    return buffer;
  }

  void free(void* buffer, size_t size, size_t alignment = 0) {
    if (buffer == nullptr) {
      return;
    }

    if (alignment == 0) {
      pool_->free(buffer, checkedSize(size));
    } else {
      pool_->freeAligned(
          buffer, checkedSize(size), checkedAlignment(alignment));
    }
  }

  void* realloc(
      void* buffer,
      size_t oldSize,
      size_t newSize,
      size_t alignment = 0) {
    if (buffer == nullptr) {
      return alloc(newSize, alignment);
    }
    if (newSize == 0) {
      free(buffer, oldSize, alignment);
      return nullptr;
    }

    void* newBuffer;
    if (alignment == 0) {
      newBuffer =
          pool_->reallocate(buffer, checkedSize(oldSize), checkedSize(newSize));
    } else {
      newBuffer = pool_->allocateAligned(
          checkedSize(newSize), checkedAlignment(alignment));
      std::memcpy(newBuffer, buffer, std::min(oldSize, newSize));
      pool_->freeAligned(
          buffer, checkedSize(oldSize), checkedAlignment(alignment));
    }
    if (newSize > oldSize) {
      std::memset(
          static_cast<uint8_t*>(newBuffer) + oldSize, 0, newSize - oldSize);
    }
    return newBuffer;
  }

 private:
  static int64_t checkedSize(size_t size) {
    VELOX_CHECK_LE(size, static_cast<size_t>(INT64_MAX));
    return static_cast<int64_t>(size);
  }

  static uint32_t checkedAlignment(size_t alignment) {
    VELOX_CHECK_LE(alignment, static_cast<size_t>(UINT32_MAX));
    return static_cast<uint32_t>(alignment);
  }

  memory::MemoryPool* pool_;
};

} // namespace facebook::velox::exec::ch
