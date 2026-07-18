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

#include <cstddef>
#include <cstdint>

namespace facebook::velox::ch
{

/// Minimal SipHash 2-4 helper — CH variant (key0=key1=0, v2^=0xff).
///
/// Result layout matches ClickHouse sipHash128:
///   lo = v0 ^ v1
///   hi = v2 ^ v3
///   uint128_t = (hi << 64) | lo
///
/// Do NOT replace with the reference SipHash-2-4 (v2^=0xee); the CH
/// variant is used in FileCacheKey::fromPath for persistent cache paths.
using uint128_t = __uint128_t;

class SipHash128
{
public:
    SipHash128();

    void update(const char * data, uint64_t size);

    /// Finalises and returns the 128-bit digest.
    /// ATTENTION: call at most once per instance.
    uint128_t get128();

private:
    uint64_t v0_, v1_, v2_, v3_;
    uint64_t cnt_;
    union
    {
        uint64_t current_word_;
        uint8_t current_bytes_[8];
    };
};

/// One-shot helper.
uint128_t sipHash128(const char * data, size_t size);

} // namespace facebook::velox::ch
