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

#include <string>
#include <string_view>

namespace facebook::velox::ch
{

/// RAII scope that sets a thread-local query id for the duration of a
/// synchronous operation.
///
/// `FileSegment::getCallerId` composes `"<query-id>:<os-tid>"` by reading
/// `currentQueryId()`.  This mirrors ClickHouse's `CurrentThread` query-id
/// mechanism without requiring explicit parameter threading through all
/// FileSegment APIs.
///
/// Nesting is supported: the destructor restores the previous query id.
/// The scope must not cross an OS-thread scheduling boundary; it only
/// stabilises the identity for one synchronous execution window.
class FileCacheQueryIdScope
{
public:
    explicit FileCacheQueryIdScope(std::string_view queryId);
    ~FileCacheQueryIdScope();

    FileCacheQueryIdScope(const FileCacheQueryIdScope &) = delete;
    FileCacheQueryIdScope & operator=(const FileCacheQueryIdScope &) = delete;

    /// Returns the current thread-local query id, or an empty string_view if
    /// no scope is active.
    static std::string_view currentQueryId();

    /// Returns the caller identity string used by `FileSegment::getCallerId`:
    ///   "<query-id>:<os-tid>"        when a scope is active
    ///   "None:<threadname>:<os-tid>" otherwise (CH diagnostic format; the
    ///                                threadname is diagnostic only and may be
    ///                                empty when the platform reports no name)
    static std::string getCallerId();

private:
    std::string previousQueryId_;
};

} // namespace facebook::velox::ch
