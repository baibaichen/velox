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

#include "velox/ch/Common/ProfileEvents.h"
#include "velox/ch/Common/SharedMutex.h"

#include <chrono>
#include <mutex>
#include <shared_mutex>

namespace facebook::velox::ch
{

/**
 * Lock ordering (outermost to innermost):
 *
 *   CachePriorityGuard
 *     > CacheMetadataGuard
 *       > KeyGuard
 *         > FileSegmentGuard
 *
 * CacheStateGuard (total size / element counters) is independent of the
 * priority chain; it is taken after CachePriorityGuard when both are needed.
 *
 * Nested Lock types are intentionally distinct structs so that a
 * KeyGuard::Lock cannot be passed where a FileSegmentGuard::Lock is expected,
 * enforcing the ordering contract at compile time.
 */

/// Priority queue guard.
/// WriteLock for structural mutations; ReadLock for read-only iteration.
struct CachePriorityGuard
{
    using WriteLock = std::unique_lock<SharedMutex>;
    using ReadLock = std::shared_lock<SharedMutex>;

    ReadLock tryReadLock()
    {
        return ReadLock(mutex, std::try_to_lock);
    }

    WriteLock tryWriteLock()
    {
        return WriteLock(mutex, std::try_to_lock);
    }

    ReadLock readLock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(
            ProfileEvents::FilesystemCachePriorityReadLockMicroseconds);
        return ReadLock(mutex);
    }

    WriteLock writeLock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(
            ProfileEvents::FilesystemCachePriorityWriteLockMicroseconds);
        return WriteLock(mutex);
    }

    CachePriorityGuard() = default;
    CachePriorityGuard(const CachePriorityGuard &) = delete;
    CachePriorityGuard & operator=(const CachePriorityGuard &) = delete;

private:
    SharedMutex mutex;
};

/// State guard protecting total-size / element counters.
struct CacheStateGuard
{
    using Mutex = std::timed_mutex;

    struct Lock : public std::unique_lock<Mutex>
    {
        using Base = std::unique_lock<Mutex>;
        using Base::Base;
    };

    Lock tryLock()
    {
        return Lock(mutex, std::try_to_lock);
    }

    Lock lock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(
            ProfileEvents::FilesystemCacheStateLockMicroseconds);
        return Lock(mutex);
    }

    Lock tryLockFor(const std::chrono::milliseconds & acquireTimeout)
    {
        ProfileEventTimeIncrement<Microseconds> watch(
            ProfileEvents::FilesystemCacheStateLockMicroseconds);
        return Lock(mutex, acquireTimeout);
    }

    CacheStateGuard() = default;
    CacheStateGuard(const CacheStateGuard &) = delete;
    CacheStateGuard & operator=(const CacheStateGuard &) = delete;

private:
    Mutex mutex;
};

/// Metadata guard.  One instance per CacheMetadata object.
struct CacheMetadataGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & m) : std::unique_lock<std::mutex>(m) {}
    };

    Lock lock()
    {
        return Lock(mutex);
    }

    CacheMetadataGuard() = default;
    CacheMetadataGuard(const CacheMetadataGuard &) = delete;
    CacheMetadataGuard & operator=(const CacheMetadataGuard &) = delete;

    std::mutex mutex;
};

/// Key guard.  One instance per cache key entry.
struct KeyGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & m) : std::unique_lock<std::mutex>(m) {}
    };

    Lock lock()
    {
        return Lock(mutex);
    }

    KeyGuard() = default;
    KeyGuard(const KeyGuard &) = delete;
    KeyGuard & operator=(const KeyGuard &) = delete;

    std::mutex mutex;
};

/// File-segment guard.  One instance per FileSegment.
struct FileSegmentGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & m) : std::unique_lock<std::mutex>(m) {}
    };

    Lock lock()
    {
        return Lock(mutex);
    }

    FileSegmentGuard() = default;
    FileSegmentGuard(const FileSegmentGuard &) = delete;
    FileSegmentGuard & operator=(const FileSegmentGuard &) = delete;

    std::mutex mutex;
};

} // namespace facebook::velox::ch
