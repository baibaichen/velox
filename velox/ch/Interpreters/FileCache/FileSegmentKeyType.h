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

#include <cstdint>
#include <string>

namespace facebook::velox::ch
{

enum class FileSegmentKeyType : uint8_t
{
    General = 0,
    System,
    Data,
};

/// Returns the directory prefix for a key type.
/// General returns "" — this is a cache-path invariant; do not change.
std::string getKeyTypePrefix(FileSegmentKeyType type);

/// Returns the enum name string ("General", "System", "Data").
std::string toString(FileSegmentKeyType type);

} // namespace facebook::velox::ch
