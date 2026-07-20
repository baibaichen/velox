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

#include <folly/CancellationToken.h>

#include <memory>
#include <utility>

namespace facebook::velox::ch {

/// Task 017: real cancellation wrapper over `folly::CancellationToken`.
///
/// A default-constructed `QueryStatus` holds a token that is never cancelled, so
/// `throwIfKilled()` is a no-op (preserving the accepted first-phase call-point
/// behaviour for callers that do not supply a token). When constructed from a
/// live token, `throwIfKilled()` raises `VeloxRuntimeError` once cancellation is
/// requested.
class QueryStatus {
 public:
  QueryStatus() = default;

  explicit QueryStatus(folly::CancellationToken token)
      : token_(std::move(token)) {}

  void throwIfKilled() const {
    if (token_.isCancellationRequested()) {
      VELOX_FAIL("FileCache query cancelled");
    }
  }

  bool isCancelled() const {
    return token_.isCancellationRequested();
  }

 private:
  folly::CancellationToken token_;
};

using QueryStatusPtr = std::shared_ptr<QueryStatus>;

} // namespace facebook::velox::ch
