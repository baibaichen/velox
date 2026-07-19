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

#include "velox/common/base/Exceptions.h"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace facebook::velox::ch::FileCacheUtils
{

inline size_t roundDownToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;
    return (num / multiple) * multiple;
}

/// Rounds `num` up to the nearest multiple of `multiple`.
/// Throws std::overflow_error when the result exceeds SIZE_MAX.
/// Uses remainder-based formula to avoid false overflow.
inline size_t roundUpToMultiple(size_t num, size_t multiple)
{
    if (!multiple)
        return num;

    const size_t remainder = num % multiple;
    if (remainder == 0)
        return num;

    size_t result = 0;
    if (__builtin_add_overflow(num, multiple - remainder, &result))
        throw std::overflow_error(
            "FileCacheUtils::roundUpToMultiple: "
            "rounded-up value does not fit in size_t");
    return result;
}

/// Returns lhs + rhs, or throws VeloxRuntimeError containing @p operation if
/// the sum would overflow uint64_t. No wrap, no saturation, no fallback.
/// Used by Tasks 013 and 014 for checked offset/budget arithmetic.
inline uint64_t checkedAdd(uint64_t lhs, uint64_t rhs, std::string_view operation)
{
    uint64_t result{};
    if (__builtin_add_overflow(lhs, rhs, &result))
        VELOX_FAIL("{}: {} + {} overflows uint64_t", operation, lhs, rhs);
    return result;
}

} // namespace facebook::velox::ch::FileCacheUtils
