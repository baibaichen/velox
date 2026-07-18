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

#include "velox/ch/Interpreters/FileCache/FileSegmentKeyType.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch
{

namespace
{

std::string_view toStringView(FileSegmentKeyType type)
{
    switch (type)
    {
        case FileSegmentKeyType::General: return "General";
        case FileSegmentKeyType::System:  return "System";
        case FileSegmentKeyType::Data:    return "Data";
    }
    VELOX_FAIL("Unknown FileSegmentKeyType: {}", static_cast<uint8_t>(type));
}

} // namespace

std::string getKeyTypePrefix(FileSegmentKeyType type)
{
    if (type == FileSegmentKeyType::General)
        return "";
    return std::string(toStringView(type));
}

std::string toString(FileSegmentKeyType type)
{
    return std::string(toStringView(type));
}

} // namespace facebook::velox::ch
