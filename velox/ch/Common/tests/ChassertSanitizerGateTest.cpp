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
#include "velox/ch/Common/ClickHouseAssert.h"

#include <gtest/gtest.h>

namespace facebook::velox::ch
{
namespace
{

// This target is compiled with NDEBUG and FOLLY_SANITIZE=1, simulating a
// sanitizer build without requiring an actual sanitizer-instrumented binary.
// chassert() must still abort under these compile definitions.
TEST(ClickHouseAssertSanitizerGateTest, AbortsEvenWithNdebugWhenSanitizerActive)
{
    EXPECT_DEATH(chassert(false), "");
}

}
}
