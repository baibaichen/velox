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

    // Nibble lookup matching ClickHouse unhexDigit / hex_char_to_digit_table.
    // Non-hex characters map to 0xFF and are silently accepted — exactly matching
    // CH FileCacheKey::fromKeyString -> unhexUInt<UInt128> behavior.
    // Accumulation uses addition (not OR) to reproduce CH's natural uint64_t
    // overflow for non-hex nibbles (e.g. 'g' -> 0xFF in position 0 yields
    // 0xF000000000000000 in the high word after 15 left 4-bit shifts).
    auto nibble = [](char c) noexcept -> uint64_t
    {
        const auto u = static_cast<unsigned char>(c);
        if (u >= '0' && u <= '9') return static_cast<uint64_t>(u - '0');
        if (u >= 'a' && u <= 'f') return static_cast<uint64_t>(u - 'a' + 10);
        if (u >= 'A' && u <= 'F') return static_cast<uint64_t>(u - 'A' + 10);
        return uint64_t{0xFF};
    };

    uint64_t hi = 0, lo = 0;
    for (size_t i = 0;  i < 16; ++i) hi = (hi << 4) + nibble(key_str[i]);
    for (size_t i = 16; i < 32; ++i) lo = (lo << 4) + nibble(key_str[i]);
    return FileCacheKey((static_cast<uint128_t>(hi) << 64) | lo);
}

} // namespace facebook::velox::ch
