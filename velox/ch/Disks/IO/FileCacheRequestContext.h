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

#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentKeyType.h"

#include <cstdint>
#include <string>

namespace facebook::velox::ch
{

/// Query/user/origin identity for a single scan read. Owned by the caller and
/// copied into each `FileCacheInputStream` (never a stream pointer). The default
/// Hive builder mapping is:
///   queryId     <- ConnectorQueryCtx::queryId
///   userId      <- FileCacheManager::commonUserId (stable process identity)
///   userWeight  <- 0 (default weight until per-user eviction is enabled)
///   cacheable   <- ReaderOptions::cacheable
///   segmentType <- FileSegmentKeyType::Data (default)
struct FileCacheRequestContext
{
    std::string queryId;
    std::string userId;
    uint64_t userWeight = 0;
    bool cacheable = true;
    FileSegmentKeyType segmentType = FileSegmentKeyType::Data;
};

} // namespace facebook::velox::ch
