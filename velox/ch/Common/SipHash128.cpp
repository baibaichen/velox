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

#include "velox/ch/Common/SipHash128.h"
#include <cstring>

namespace facebook::velox::ch
{

namespace
{
inline uint64_t rotl64(uint64_t x, int r) noexcept
{
    return (x << r) | (x >> (64 - r));
}
} // namespace

#define SIPROUND                           \
    do                                     \
    {                                      \
        v0 += v1;                          \
        v1 = rotl64(v1, 13);               \
        v1 ^= v0;                          \
        v0 = rotl64(v0, 32);               \
        v2 += v3;                          \
        v3 = rotl64(v3, 16);               \
        v3 ^= v2;                          \
        v0 += v3;                          \
        v3 = rotl64(v3, 21);               \
        v3 ^= v0;                          \
        v2 += v1;                          \
        v1 = rotl64(v1, 17);               \
        v1 ^= v2;                          \
        v2 = rotl64(v2, 32);               \
    } while (false)

SipHash128::SipHash128()
    : v0_(0x736f6d6570736575ULL)
    , v1_(0x646f72616e646f6dULL)
    , v2_(0x6c7967656e657261ULL)
    , v3_(0x7465646279746573ULL)
    , cnt_(0)
    , current_word_(0)
{
}

void SipHash128::update(const char * data, uint64_t size)
{
    const char * end = data + size;

    if (cnt_ & 7)
    {
        while ((cnt_ & 7) && data < end)
        {
            current_bytes_[cnt_ & 7] = static_cast<uint8_t>(*data++);
            ++cnt_;
        }
        if (cnt_ & 7)
            return;

        uint64_t v0 = v0_, v1 = v1_, v2 = v2_, v3 = v3_;
        v3 ^= current_word_;
        SIPROUND; SIPROUND;
        v0 ^= current_word_;
        v0_ = v0; v1_ = v1; v2_ = v2; v3_ = v3;
    }

    cnt_ += static_cast<uint64_t>(end - data);

    while (end - data >= 8)
    {
        uint64_t word;
        std::memcpy(&word, data, 8);
        uint64_t v0 = v0_, v1 = v1_, v2 = v2_, v3 = v3_;
        v3 ^= word;
        SIPROUND; SIPROUND;
        v0 ^= word;
        v0_ = v0; v1_ = v1; v2_ = v2; v3_ = v3;
        data += 8;
    }

    current_word_ = 0;
    switch (end - data)
    {
        case 7: current_bytes_[6] = static_cast<uint8_t>(data[6]); [[fallthrough]];
        case 6: current_bytes_[5] = static_cast<uint8_t>(data[5]); [[fallthrough]];
        case 5: current_bytes_[4] = static_cast<uint8_t>(data[4]); [[fallthrough]];
        case 4: current_bytes_[3] = static_cast<uint8_t>(data[3]); [[fallthrough]];
        case 3: current_bytes_[2] = static_cast<uint8_t>(data[2]); [[fallthrough]];
        case 2: current_bytes_[1] = static_cast<uint8_t>(data[1]); [[fallthrough]];
        case 1: current_bytes_[0] = static_cast<uint8_t>(data[0]); [[fallthrough]];
        case 0: break;
    }
}

uint128_t SipHash128::get128()
{
    current_bytes_[7] = static_cast<uint8_t>(cnt_);

    uint64_t v0 = v0_, v1 = v1_, v2 = v2_, v3 = v3_;
    v3 ^= current_word_;
    SIPROUND; SIPROUND;
    v0 ^= current_word_;

    v2 ^= 0xff; // CH variant (reference uses 0xee)
    SIPROUND; SIPROUND; SIPROUND; SIPROUND;

    const uint64_t lo = v0 ^ v1;
    const uint64_t hi = v2 ^ v3;
    return (static_cast<uint128_t>(hi) << 64) | lo;
}

#undef SIPROUND

uint128_t sipHash128(const char * data, size_t size)
{
    SipHash128 h;
    h.update(data, size);
    return h.get128();
}

} // namespace facebook::velox::ch
