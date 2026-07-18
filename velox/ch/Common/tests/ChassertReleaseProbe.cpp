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

#include <string>

namespace
{

// Counts how many times the expression or the diagnostic message were
// evaluated. This target is compiled with NDEBUG only (no sanitizer), so
// ordinary Release chassert() must not evaluate either one: the process exit
// status (this counter) must be zero.
int sideEffectCount = 0;

bool expressionWithSideEffect()
{
    ++sideEffectCount;
    return false;
}

std::string diagnosticWithSideEffect()
{
    ++sideEffectCount;
    return "diagnostic constructed";
}

}

int main()
{
    chassert(expressionWithSideEffect(), diagnosticWithSideEffect());
    return sideEffectCount;
}
