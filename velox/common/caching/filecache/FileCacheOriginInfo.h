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

#include "velox/common/caching/filecache/FileSegmentKeyType.h"

#include <cstdint>
#include <optional>
#include <string>
#include <sys/types.h>

namespace facebook::velox::ch
{

struct FileCacheOriginInfo
{
    using UserID = std::string;
    using Weight = uint64_t;
    using SegmentKeyType = FileSegmentKeyType;

    UserID userId;
    std::optional<Weight> weight = std::nullopt;
    SegmentKeyType segmentType = SegmentKeyType::General;

    FileCacheOriginInfo() = default;

    explicit FileCacheOriginInfo(const UserID & userId_)
        : userId(userId_)
    {
    }

    FileCacheOriginInfo(const UserID & userId_, const Weight & weight_, SegmentKeyType segmentType_ = SegmentKeyType::General)
        : userId(userId_)
        , weight(weight_)
        , segmentType(segmentType_)
    {
    }

    bool operator==(const FileCacheOriginInfo & other) const { return userId == other.userId; }
};

}
