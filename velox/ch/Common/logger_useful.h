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

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace facebook::velox::ch
{

// First-phase, name-only logger shim. FileCache.cpp directly calls
// log->name(), so getLogger() must return a non-null object that remembers
// the name it was constructed with. Actual logging is deferred to Task 017.
class FileCacheLogger
{
public:
    explicit FileCacheLogger(std::string name)
        : name_(std::move(name))
    {
    }

    const std::string & name() const
    {
        return name_;
    }

private:
    std::string name_;
};

using LoggerPtr = std::shared_ptr<FileCacheLogger>;

inline LoggerPtr getLogger(std::string_view name)
{
    return std::make_shared<FileCacheLogger>(std::string(name));
}

inline std::string getCurrentExceptionMessage(bool /*withStackTrace*/ = false)
{
    return {};
}

inline void tryLogCurrentException(...) noexcept
{
}

}

#define LOG_TEST(...) \
    do                \
    {                 \
    } while (false)
#define LOG_TRACE(...) LOG_TEST(__VA_ARGS__)
#define LOG_DEBUG(...) LOG_TEST(__VA_ARGS__)
#define LOG_INFO(...) LOG_TEST(__VA_ARGS__)
#define LOG_WARNING(...) LOG_TEST(__VA_ARGS__)
#define LOG_ERROR(...) LOG_TEST(__VA_ARGS__)
