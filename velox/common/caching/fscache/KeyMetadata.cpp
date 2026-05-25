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

#include "velox/common/caching/fscache/KeyMetadata.h"

namespace facebook::velox::cache::fs {

LockedKey::LockedKey(KeyMetadata* meta, KeyMutex& mutex)
    : meta_{meta}, mutex_{&mutex} {
  mutex_->lock();
}

LockedKey::LockedKey(LockedKey&& other) noexcept
    : meta_{other.meta_}, mutex_{other.mutex_} {
  other.meta_ = nullptr;
  other.mutex_ = nullptr;
}

LockedKey& LockedKey::operator=(LockedKey&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (mutex_ != nullptr) {
    mutex_->unlock();
  }
  meta_ = other.meta_;
  mutex_ = other.mutex_;
  other.meta_ = nullptr;
  other.mutex_ = nullptr;
  return *this;
}

LockedKey::~LockedKey() {
  if (mutex_ != nullptr) {
    mutex_->unlock();
  }
}

LockedKey KeyMetadata::lock() {
  return LockedKey{this, mutex_};
}

} // namespace facebook::velox::cache::fs
