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

#include "velox/ch/Interpreters/FileCache/FileSegmentKeyType.h"

#include <folly/container/F14Map.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace facebook::velox::ch
{

struct FileCacheOriginInfo
{
    using UserID = std::string;
    using Weight = uint64_t;
    using SegmentKeyType = FileSegmentKeyType;

    UserID user_id;
    std::optional<Weight> weight = std::nullopt;
    SegmentKeyType segment_type = SegmentKeyType::General;

    FileCacheOriginInfo() = default;

    explicit FileCacheOriginInfo(const UserID & user_id_)
        : user_id(user_id_)
    {
    }

    FileCacheOriginInfo(
        const UserID & user_id_,
        const Weight & weight_,
        SegmentKeyType segment_type_ = SegmentKeyType::General)
        : user_id(user_id_), weight(weight_), segment_type(segment_type_)
    {
    }

    /// Equality compares only user_id (matches ClickHouse semantics).
    bool operator==(const FileCacheOriginInfo & other) const
    {
        return user_id == other.user_id;
    }
};

using OriginInfoPtr = std::shared_ptr<const FileCacheOriginInfo>;

struct OriginPoolKey
{
    FileCacheOriginInfo::UserID user_id;
    std::optional<FileCacheOriginInfo::Weight> weight;
    FileCacheOriginInfo::SegmentKeyType segment_type;

    bool operator==(const OriginPoolKey & other) const = default;
};

/// Hash on user_id only — mirrors ClickHouse behaviour (user count is small
/// so collision on weight/type is acceptable).
struct OriginPoolKeyHash
{
    size_t operator()(const OriginPoolKey & key) const noexcept
    {
        return std::hash<FileCacheOriginInfo::UserID>{}(key.user_id);
    }
};

} // namespace facebook::velox::ch
