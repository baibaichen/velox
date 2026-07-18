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

#include <folly/CPortability.h>

#include <cstdio>
#include <cstdlib>
#include <string_view>

// chassert(expression) / chassert(expression, message) mirror ClickHouse's
// base/base/defines.h chassert(): in debug and sanitizer builds the
// expression is evaluated exactly once and, on failure, aborts after
// printing the expression text (or the explicit message) to stderr. In an
// ordinary Release build (NDEBUG defined, no sanitizer) neither the
// expression nor the message is evaluated at runtime; only compile-time
// expression checking through sizeof() is preserved.

namespace facebook::velox::ch::detail
{

// Prints the failed assertion description and aborts. Never returns.
[[noreturn]] inline void chassertFail(std::string_view description)
{
    std::fprintf(
        stderr,
        "Assertion failed: %.*s\n",
        static_cast<int>(description.size()),
        description.data());
    std::fflush(stderr);
    std::abort();
}

}

#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)

#define CH_CHASSERT_1(expression, ...)                        \
    do                                                         \
    {                                                          \
        static_cast<bool>(expression)                          \
            ? void(0)                                           \
            : ::facebook::velox::ch::detail::chassertFail(#expression); \
    } while (false)

#define CH_CHASSERT_2(expression, message, ...)                \
    do                                                         \
    {                                                          \
        static_cast<bool>(expression)                          \
            ? void(0)                                           \
            : ::facebook::velox::ch::detail::chassertFail(message); \
    } while (false)

#else

// The sizeof() trick suppresses "unused expression" warnings without
// evaluating the expression (and, for the two-argument form, without even
// referencing the message argument) at runtime.
#define CH_CHASSERT_1(expression, ...) (void)sizeof(!(expression))
#define CH_CHASSERT_2(expression, message, ...) (void)sizeof(!(expression))

#endif

#define CH_CHASSERT_DISPATCH(_1, _2, N, ...) N(_1, _2)
#define CH_CHASSERT_INVOKE(tuple) CH_CHASSERT_DISPATCH tuple
#define chassert(...) \
    CH_CHASSERT_INVOKE((__VA_ARGS__, CH_CHASSERT_2, CH_CHASSERT_1))
