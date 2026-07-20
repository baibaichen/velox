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

#include <folly/ExceptionString.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace facebook::velox::ch
{

// Name-only logger object. FileCache.cpp calls log->name() outside the logging
// macros (to name its background schedule task), so getLogger() must return a
// non-null object that remembers the name it was constructed with. This public
// shape (FileCacheLogger, name(), LoggerPtr, getLogger) is preserved from the
// corrected Task 003 shim; Task 017 only adds real logging behind the macros.
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

// Task 017: real current-exception formatting. Returns an empty string when
// there is no in-flight exception, otherwise a non-empty diagnostic string
// produced by Folly's exception stringifier. `folly::exceptionStr` handles
// standard, Velox, nested, and non-standard exceptions without a rethrow/catch
// ladder. `withStackTrace` is kept only for CH call-shape compatibility: a
// VeloxException string already embeds its Velox-managed stack-trace state when
// enabled, so no second stack trace is synthesized here.
inline std::string getCurrentExceptionMessage(bool /*withStackTrace*/ = false)
{
    const auto exception = std::current_exception();
    if (!exception)
        return {};

    const auto message = folly::exceptionStr(exception);
    return std::string(message.data(), message.size());
}

// Task 017: log the current exception text via glog. This diagnostic helper is
// noexcept and must never let a logging failure replace the exception being
// handled.
inline void tryLogCurrentException(const LoggerPtr & logger, std::string_view context = {}) noexcept
{
    try
    {
        const auto msg = getCurrentExceptionMessage(/*withStackTrace=*/true);
        if (logger)
        {
            if (context.empty())
                LOG(WARNING) << "[" << logger->name() << "] exception: " << msg;
            else
                LOG(WARNING) << "[" << logger->name() << "] " << context
                             << ": " << msg;
        }
        else if (context.empty())
        {
            LOG(WARNING) << "exception: " << msg;
        }
        else
        {
            LOG(WARNING) << context << ": exception: " << msg;
        }
    }
    catch (...)
    {
        // This diagnostic helper must not replace the exception being handled.
    }
}

// Context-only overload (e.g. tryLogCurrentException(__PRETTY_FUNCTION__)).
inline void tryLogCurrentException(std::string_view context) noexcept
{
    tryLogCurrentException(LoggerPtr{}, context);
}

}

// Lazy log macros: evaluate the logger expression once per enabled log call and
// format arguments only when the level is enabled (so a disabled level performs
// no fmt::format work). The logger tag uses name(); a null logger prints the
// message with no tag.
#define FILECACHE_LOG_IMPL(level, logger_ptr, ...)                            \
    do                                                                        \
    {                                                                         \
        if (VLOG_IS_ON(level))                                                \
        {                                                                     \
            const auto & _fc_logger = (logger_ptr);                           \
            const std::string _fc_msg = fmt::format(__VA_ARGS__);             \
            if (_fc_logger != nullptr)                                        \
                VLOG(level) << "[" << _fc_logger->name() << "] " << _fc_msg;  \
            else                                                              \
                VLOG(level) << _fc_msg;                                       \
        }                                                                     \
    } while (false)

#define LOG_TRACE(logger_ptr, ...) FILECACHE_LOG_IMPL(3, logger_ptr, __VA_ARGS__)
#define LOG_DEBUG(logger_ptr, ...) FILECACHE_LOG_IMPL(2, logger_ptr, __VA_ARGS__)
#define LOG_INFO(logger_ptr, ...) FILECACHE_LOG_IMPL(1, logger_ptr, __VA_ARGS__)

#define LOG_WARNING(logger_ptr, ...)                                          \
    do                                                                        \
    {                                                                         \
        (void)sizeof(logger_ptr);                                             \
        LOG(WARNING) << fmt::format(__VA_ARGS__);                             \
    } while (false)

#define LOG_ERROR(logger_ptr, ...)                                            \
    do                                                                        \
    {                                                                         \
        (void)sizeof(logger_ptr);                                             \
        LOG(ERROR) << fmt::format(__VA_ARGS__);                               \
    } while (false)

// LOG_TEST stays no-op and must NOT eagerly evaluate its arguments (the accepted
// Task 003 non-evaluation contract). Do not map it to VLOG(...) << fmt::format,
// which would format even when the level is disabled.
#define LOG_TEST(...) \
    do                \
    {                 \
    } while (false)
