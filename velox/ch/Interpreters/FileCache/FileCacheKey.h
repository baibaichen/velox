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

#include "velox/ch/Common/SipHash128.h"

#include <folly/container/F14Map.h>
#include <velox/common/base/BitUtil.h>
#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace facebook::velox::ch
{

struct FileCacheKey
{
    using KeyHash = uint128_t;

    KeyHash key{};

    /// Zero-initialized default key (matches ClickHouse `FileCacheKey() = default`).
    /// Required by downstream default-constructed members (Task 012/015).
    FileCacheKey() = default;

    std::string toString() const;

    static FileCacheKey random();
    static FileCacheKey fromPath(std::string_view path);
    static FileCacheKey fromKey(KeyHash k);
    static FileCacheKey fromKeyString(std::string_view key_str);

    bool operator==(const FileCacheKey & other) const = default;
    bool operator<(const FileCacheKey & other) const { return key < other.key; }

private:
    explicit FileCacheKey(KeyHash k) : key(k) {}
};

using FileCacheKeyAndOffset = std::pair<FileCacheKey, size_t>;

struct FileCacheKeyHash
{
    size_t operator()(const FileCacheKey & k) const noexcept
    {
        return bits::hashMix(
            static_cast<uint64_t>(k.key >> 64),
            static_cast<uint64_t>(k.key));
    }
};

struct FileCacheKeyAndOffsetHash
{
    size_t operator()(const FileCacheKeyAndOffset & v) const noexcept
    {
        return bits::hashMix(
            FileCacheKeyHash{}(v.first), v.second);
    }
};

} // namespace facebook::velox::ch

template <>
struct fmt::formatter<facebook::velox::ch::FileCacheKey>
    : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(
        const facebook::velox::ch::FileCacheKey & key,
        FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(key.toString(), ctx);
    }
};
