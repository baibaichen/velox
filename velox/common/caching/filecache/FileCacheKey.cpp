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
#include "velox/common/caching/filecache/FileCacheKey.h"

#include <folly/Random.h>
#include <folly/hash/SpookyHashV2.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch {

namespace {
uint64_t lowHalf(UInt128 v) {
  return static_cast<uint64_t>(v);
}
uint64_t highHalf(UInt128 v) {
  return static_cast<uint64_t>(v >> 64);
}

uint8_t unhexDigit(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return 0xFF;
}
} // namespace

FileCacheKey::FileCacheKey(const UInt128& key_) : key(key_) {}

std::string FileCacheKey::toString() const {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string result(32, '0');
  UInt128 v = key;
  for (int i = 31; i >= 0; --i) {
    result[i] = kDigits[static_cast<size_t>(v & 0xF)];
    v >>= 4;
  }
  return result;
}

FileCacheKey FileCacheKey::random() {
  const auto hi = static_cast<UInt128>(folly::Random::rand64());
  const auto lo = static_cast<UInt128>(folly::Random::rand64());
  return FileCacheKey((hi << 64) | lo);
}

FileCacheKey FileCacheKey::fromPath(const std::string& path) {
  uint64_t h1 = 0;
  uint64_t h2 = 0;
  // SpookyHashV2 with a fixed zero seed: keys are deterministic across process
  // restarts AND across builds (no runtime salt, no version/date macros). The
  // on-disk cache directory is therefore reusable after a restart as long as the
  // file paths are unchanged. The only incompatibility is with real ClickHouse,
  // which keys with sipHash128 (no folly SipHash available here). Port CH's
  // SipHash128 only if cache-directory interop with real ClickHouse is required.
  folly::hash::SpookyHashV2::Hash128(path.data(), path.size(), &h1, &h2);
  return FileCacheKey((static_cast<UInt128>(h1) << 64) | h2);
}

FileCacheKey FileCacheKey::fromKey(const UInt128& key) {
  return FileCacheKey(key);
}

FileCacheKey FileCacheKey::fromKeyString(const std::string& keyStr) {
  VELOX_CHECK_EQ(keyStr.size(), 32, "Invalid cache key hex: {}", keyStr);
  UInt128 v = 0;
  for (char c : keyStr) {
    v <<= 4;
    v += unhexDigit(c);
  }
  return FileCacheKey(v);
}

std::size_t FileCacheKeyAndOffsetHash::operator()(
    const FileCacheKeyAndOffset& key) const {
  return std::hash<FileCacheKey>()(key.first) ^
      std::hash<uint64_t>()(static_cast<uint64_t>(key.second));
}

} // namespace facebook::velox::ch

namespace std {
std::size_t hash<facebook::velox::ch::FileCacheKey>::operator()(
    const facebook::velox::ch::FileCacheKey& k) const {
  return facebook::velox::ch::lowHalf(k.key) ^
      facebook::velox::ch::highHalf(k.key);
}
} // namespace std
