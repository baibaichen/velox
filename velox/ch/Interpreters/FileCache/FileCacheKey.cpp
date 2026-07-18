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

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Common/FileCacheException.h"

#include <folly/Random.h>
#include <fmt/format.h>

namespace facebook::velox::ch
{

std::string FileCacheKey::toString() const
{
    return fmt::format(
        "{:016x}{:016x}",
        static_cast<uint64_t>(key >> 64),
        static_cast<uint64_t>(key));
}

FileCacheKey FileCacheKey::random()
{
    return FileCacheKey(
        (static_cast<uint128_t>(folly::Random::rand64()) << 64)
        | folly::Random::rand64());
}

FileCacheKey FileCacheKey::fromPath(std::string_view path)
{
    return FileCacheKey(sipHash128(path.data(), path.size()));
}

FileCacheKey FileCacheKey::fromKey(KeyHash k)
{
    return FileCacheKey(k);
}

FileCacheKey FileCacheKey::fromKeyString(std::string_view key_str)
{
    if (key_str.size() != 32)
        throwFileCacheException(
            "Invalid cache key hex string: expected 32 characters, got {}",
            key_str.size());

    auto hexDigit = [&](char c) -> uint8_t
    {
        if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
        if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
        if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
        throwFileCacheException(
            "Invalid hex character '{}' in cache key string", c);
    };

    uint64_t hi = 0, lo = 0;
    for (size_t i = 0;  i < 16; ++i) hi = (hi << 4) | hexDigit(key_str[i]);
    for (size_t i = 16; i < 32; ++i) lo = (lo << 4) | hexDigit(key_str[i]);
    return FileCacheKey((static_cast<uint128_t>(hi) << 64) | lo);
}

} // namespace facebook::velox::ch
