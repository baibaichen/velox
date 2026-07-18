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

#include "velox/ch/Common/FileCacheQueryIdScope.h"

#include <folly/system/ThreadId.h>

#include <string>
#include <string_view>

namespace facebook::velox::ch
{

namespace
{
// Thread-local storage for the current query id.
thread_local std::string tCurrentQueryId;
} // namespace

FileCacheQueryIdScope::FileCacheQueryIdScope(std::string_view queryId)
    : previousQueryId_(tCurrentQueryId)
{
    tCurrentQueryId.assign(queryId);
}

FileCacheQueryIdScope::~FileCacheQueryIdScope()
{
    tCurrentQueryId = std::move(previousQueryId_);
}

std::string_view FileCacheQueryIdScope::currentQueryId()
{
    return tCurrentQueryId;
}

std::string FileCacheQueryIdScope::getCallerId()
{
    const auto tid = std::to_string(folly::getOSThreadID());
    const auto & qid = tCurrentQueryId;
    if (qid.empty())
        return "None:" + tid;
    return qid + ":" + tid;
}

} // namespace facebook::velox::ch
