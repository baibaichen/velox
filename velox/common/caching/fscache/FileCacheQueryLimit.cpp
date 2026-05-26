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

#include "velox/common/caching/fscache/FileCacheQueryLimit.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::cache::fs {

QueryLimitToken::QueryLimitToken(
    FileCacheQueryLimit* parent,
    uint64_t maxBytes)
    : parent_{parent}, maxBytes_{maxBytes} {}

QueryLimitToken::~QueryLimitToken() {
  const auto held = reserved_.load(std::memory_order_acquire);
  if (held > 0) {
    parent_->onTokenRelease(held);
  }
}

bool QueryLimitToken::tryReserve(uint64_t bytes) {
  auto cur = reserved_.load(std::memory_order_acquire);
  while (true) {
    if (cur + bytes > maxBytes_) {
      return false;
    }
    if (reserved_.compare_exchange_weak(
            cur,
            cur + bytes,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      parent_->onTokenReserve(bytes);
      return true;
    }
  }
}

void QueryLimitToken::release(uint64_t bytes) {
  // fetch_sub returns the value before subtraction. Underflow check on
  // the pre-decrement value catches releases larger than the live
  // reservation.
  const auto prev = reserved_.fetch_sub(bytes, std::memory_order_acq_rel);
  VELOX_CHECK_GE(
      prev,
      bytes,
      "FileCacheQueryLimit underflow: prev={} bytes={}",
      prev,
      bytes);
  parent_->onTokenRelease(bytes);
}

std::unique_ptr<QueryLimitToken> FileCacheQueryLimit::reserveQuery(
    uint64_t maxBytesPerQuery) {
  return std::make_unique<QueryLimitToken>(this, maxBytesPerQuery);
}

} // namespace facebook::velox::cache::fs
