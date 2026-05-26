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

#include <atomic>
#include <cstdint>
#include <memory>

namespace facebook::velox::cache::fs {

class FileCacheQueryLimit;

/// Per-query reservation handle. Held by QueryCtx (or any caller scoping a
/// budget); callers invoke tryReserve() BEFORE getOrSet and read directly
/// from remote on `false`. release() is invoked when bytes belonging to the
/// query leave the cache (eviction, holder dtor on partial download).
///
/// Thread-safe: backed by a single atomic counter with CAS reservation.
/// The token destructor drops the token's contribution from the parent
/// FileCacheQueryLimit's totalReserved counter, used later for cluster-
/// wide caps. The token itself does NOT release cached bytes on dtor;
/// that is the caller's responsibility (a holder kept alive past query
/// end is a caller bug).
class QueryLimitToken {
 public:
  QueryLimitToken(FileCacheQueryLimit* parent, uint64_t maxBytes);
  ~QueryLimitToken();

  QueryLimitToken(const QueryLimitToken&) = delete;
  QueryLimitToken& operator=(const QueryLimitToken&) = delete;
  QueryLimitToken(QueryLimitToken&&) = delete;
  QueryLimitToken& operator=(QueryLimitToken&&) = delete;

  /// Attempts to reserve `bytes` against this token's budget. Returns true
  /// and bumps the counter on success; returns false unchanged if it would
  /// exceed maxBytes.
  bool tryReserve(uint64_t bytes);

  /// Releases `bytes` previously reserved. Throws via VELOX_CHECK_GE if
  /// this would underflow the live reservation.
  void release(uint64_t bytes);

  uint64_t reserved() const {
    return reserved_.load(std::memory_order_acquire);
  }

  uint64_t maxBytes() const {
    return maxBytes_;
  }

 private:
  FileCacheQueryLimit* const parent_;
  const uint64_t maxBytes_;
  std::atomic<uint64_t> reserved_{0};
};

/// Factory + cluster-wide accounting for QueryLimitToken. One instance
/// lives on FsCache; QueryCtx calls reserveQuery() once per query and
/// holds the returned token.
class FileCacheQueryLimit {
 public:
  /// Mints a new token with a per-query budget of `maxBytesPerQuery`.
  std::unique_ptr<QueryLimitToken> reserveQuery(uint64_t maxBytesPerQuery);

  /// Sum of `reserved()` across all live tokens. Used by future cluster
  /// limits; exposed now to keep the dtor-release invariant testable.
  uint64_t totalReserved() const {
    return totalReserved_.load(std::memory_order_acquire);
  }

  /// Called by QueryLimitToken on each successful tryReserve(). Public
  /// because the project style prohibits `friend`; the contract is
  /// "tokens own the counter, FileCacheQueryLimit owns the sum". Tests
  /// must not call these directly.
  void onTokenReserve(uint64_t bytes) {
    totalReserved_.fetch_add(bytes, std::memory_order_acq_rel);
  }
  void onTokenRelease(uint64_t bytes) {
    totalReserved_.fetch_sub(bytes, std::memory_order_acq_rel);
  }

 private:
  std::atomic<uint64_t> totalReserved_{0};
};

} // namespace facebook::velox::cache::fs
