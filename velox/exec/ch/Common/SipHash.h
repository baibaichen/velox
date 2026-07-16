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

#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h"

#include <folly/Portability.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

namespace facebook::velox::exec::ch {

using UInt8 = uint8_t;
using UInt64 = uint64_t;
using UInt128 = ch::UInt128;

template <typename T>
FOLLY_ALWAYS_INLINE T unalignedLoadLittleEndian(const void* address) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value;
  std::memcpy(&value, address, sizeof(value));
  if constexpr (std::endian::native == std::endian::big) {
    static_assert(std::is_integral_v<T>);
    if constexpr (sizeof(T) == sizeof(uint64_t)) {
      value = static_cast<T>(__builtin_bswap64(static_cast<uint64_t>(value)));
    } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
      value = static_cast<T>(__builtin_bswap32(static_cast<uint32_t>(value)));
    } else if constexpr (sizeof(T) == sizeof(uint16_t)) {
      value = static_cast<T>(__builtin_bswap16(static_cast<uint16_t>(value)));
    }
  }
  return value;
}

template <typename T>
FOLLY_ALWAYS_INLINE T byteSwap(T value) {
  static_assert(std::is_integral_v<T>);
  using Unsigned = std::make_unsigned_t<T>;
  const auto unsignedValue = static_cast<Unsigned>(value);
  if constexpr (sizeof(T) == sizeof(uint64_t)) {
    return static_cast<T>(
        __builtin_bswap64(static_cast<uint64_t>(unsignedValue)));
  } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
    return static_cast<T>(
        __builtin_bswap32(static_cast<uint32_t>(unsignedValue)));
  } else if constexpr (sizeof(T) == sizeof(uint16_t)) {
    return static_cast<T>(
        __builtin_bswap16(static_cast<uint16_t>(unsignedValue)));
  } else {
    return value;
  }
}

template <typename T>
  requires std::is_integral_v<T>
FOLLY_ALWAYS_INLINE void transformEndiannessToLittle(T& value) {
  value = byteSwap(value);
}

template <typename T>
  requires std::is_floating_point_v<T>
FOLLY_ALWAYS_INLINE void transformEndiannessToLittle(T& value) {
  auto* first = reinterpret_cast<std::byte*>(&value);
  std::reverse(first, first + sizeof(T));
}

template <typename T>
  requires std::is_enum_v<T>
FOLLY_ALWAYS_INLINE void transformEndiannessToLittle(T& value) {
  using Underlying = std::underlying_type_t<T>;
  auto underlying = static_cast<Underlying>(value);
  transformEndiannessToLittle(underlying);
  value = static_cast<T>(underlying);
}

FOLLY_ALWAYS_INLINE void transformEndiannessToLittle(UInt128& value) {
  value.words[0] = byteSwap(value.words[0]);
  value.words[1] = byteSwap(value.words[1]);
  std::swap(value.words[0], value.words[1]);
}

#define SIPROUND            \
  do {                      \
    v0 += v1;               \
    v1 = std::rotl(v1, 13); \
    v1 ^= v0;               \
    v0 = std::rotl(v0, 32); \
    v2 += v3;               \
    v3 = std::rotl(v3, 16); \
    v3 ^= v2;               \
    v0 += v3;               \
    v3 = std::rotl(v3, 21); \
    v3 ^= v0;               \
    v2 += v1;               \
    v1 = std::rotl(v1, 17); \
    v1 ^= v2;               \
    v2 = std::rotl(v2, 32); \
  } while (0)

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define CURRENT_BYTES_IDX(i) (7 - (i))
#else
#define CURRENT_BYTES_IDX(i) (i)
#endif

class SipHash {
 private:
  UInt64 v0;
  UInt64 v1;
  UInt64 v2;
  UInt64 v3;
  UInt64 cnt;
  bool is_reference_128;

  union {
    UInt64 current_word;
    UInt8 current_bytes[8];
  };

  FOLLY_ALWAYS_INLINE void finalize() {
    current_bytes[CURRENT_BYTES_IDX(7)] = static_cast<UInt8>(cnt);

    v3 ^= current_word;
    SIPROUND;
    SIPROUND;
    v0 ^= current_word;

    if (is_reference_128) {
      v2 ^= 0xee;
    } else {
      v2 ^= 0xff;
    }
    SIPROUND;
    SIPROUND;
    SIPROUND;
    SIPROUND;
  }

 public:
  SipHash(UInt64 key0 = 0, UInt64 key1 = 0, bool isReference128 = false) {
    v0 = 0x736f6d6570736575ULL ^ key0;
    v1 = 0x646f72616e646f6dULL ^ key1;
    v2 = 0x6c7967656e657261ULL ^ key0;
    v3 = 0x7465646279746573ULL ^ key1;
    is_reference_128 = isReference128;

    if (is_reference_128) {
      v1 ^= 0xee;
    }

    cnt = 0;
    current_word = 0;
  }

  FOLLY_ALWAYS_INLINE void update(const char* data, UInt64 size) {
    const char* end = data + size;

    if (cnt & 7) {
      while (cnt & 7 && data < end) {
        current_bytes[CURRENT_BYTES_IDX(cnt & 7)] = *data;
        ++data;
        ++cnt;
      }

      if (cnt & 7) {
        return;
      }

      v3 ^= current_word;
      SIPROUND;
      SIPROUND;
      v0 ^= current_word;
    }

    cnt += end - data;

    while (end - data >= 8) {
      current_word = unalignedLoadLittleEndian<UInt64>(data);

      v3 ^= current_word;
      SIPROUND;
      SIPROUND;
      v0 ^= current_word;

      data += 8;
    }

    current_word = 0;
    switch (end - data) {
      case 7:
        current_bytes[CURRENT_BYTES_IDX(6)] = data[6];
        [[fallthrough]];
      case 6:
        current_bytes[CURRENT_BYTES_IDX(5)] = data[5];
        [[fallthrough]];
      case 5:
        current_bytes[CURRENT_BYTES_IDX(4)] = data[4];
        [[fallthrough]];
      case 4:
        current_bytes[CURRENT_BYTES_IDX(3)] = data[3];
        [[fallthrough]];
      case 3:
        current_bytes[CURRENT_BYTES_IDX(2)] = data[2];
        [[fallthrough]];
      case 2:
        current_bytes[CURRENT_BYTES_IDX(1)] = data[1];
        [[fallthrough]];
      case 1:
        current_bytes[CURRENT_BYTES_IDX(0)] = data[0];
        [[fallthrough]];
      case 0:
        break;
    }
  }

  template <typename Transform = void, typename T>
  FOLLY_ALWAYS_INLINE void update(const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    if constexpr (std::endian::native == std::endian::big) {
      auto transformed = value;
      if constexpr (!std::is_same_v<Transform, void>) {
        transformed = Transform()(value);
      } else {
        transformEndiannessToLittle(transformed);
      }
      update(reinterpret_cast<const char*>(&transformed), sizeof(transformed));
    } else {
      update(reinterpret_cast<const char*>(&value), sizeof(value));
    }
  }

  FOLLY_ALWAYS_INLINE void update(const std::string& value) {
    update(value.data(), value.length());
  }

  FOLLY_ALWAYS_INLINE void update(std::string_view value) {
    update(value.data(), value.size());
  }

  FOLLY_ALWAYS_INLINE void update(const char* value) {
    update(std::string_view(value));
  }

  FOLLY_ALWAYS_INLINE UInt64 get64() {
    finalize();
    return v0 ^ v1 ^ v2 ^ v3;
  }

  template <typename T>
    requires(sizeof(T) == 8)
  FOLLY_ALWAYS_INLINE void get128(T& lo, T& hi) {
    finalize();
    lo = v0 ^ v1;
    hi = v2 ^ v3;
  }

  FOLLY_ALWAYS_INLINE UInt128 get128() {
    UInt128 result;
    get128(result.words[0], result.words[1]);
    return result;
  }
};

#undef SIPROUND
#undef CURRENT_BYTES_IDX

} // namespace facebook::velox::exec::ch
