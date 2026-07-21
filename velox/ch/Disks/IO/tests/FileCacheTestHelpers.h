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

/// Shared header-only helpers for FileCacheE2ETest and FileCacheSeekBenchmark.
/// Provides deterministic data generation, counting/direct-IO ReadFile mocks,
/// manager/cache/input construction, and stream reading utilities.

#pragma once

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheFactory.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/dwio/common/Options.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/ManualTimekeeper.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace facebook::velox::ch::test
{

using velox::common::testutil::TempDirectoryPath;

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

/// Generates a deterministic byte pattern where byte[i] = i % 251.
/// 251 is prime, so the pattern is unique over a longer period.
inline std::vector<char> makeDeterministicData(size_t n)
{
    std::vector<char> d(n);
    for (size_t i = 0; i < n; ++i)
        d[i] = static_cast<char>(i % 251);
    return d;
}

// ---------------------------------------------------------------------------
// CountingReadFile: in-memory source with pread call/byte counting
// ---------------------------------------------------------------------------

class CountingReadFile : public ReadFile
{
public:
    explicit CountingReadFile(std::vector<char> data)
        : data_(std::move(data))
    {
    }

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const FileIoContext & = {}) const override
    {
        ++preadCalls_;
        if (offset >= data_.size())
            return {};
        const uint64_t n = std::min<uint64_t>(length, data_.size() - offset);
        std::memcpy(buf, data_.data() + offset, n);
        preadBytes_ += n;
        return std::string_view(static_cast<const char *>(buf), n);
    }

    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    bool shouldCoalesce() const override { return false; }
    std::string getName() const override { return "<CountingReadFile>"; }
    uint64_t getNaturalReadSize() const override { return 1024; }

    uint64_t preadBytes() const { return preadBytes_.load(); }
    uint64_t preadCalls() const { return preadCalls_.load(); }

    const std::vector<char> & data() const { return data_; }

private:
    std::vector<char> data_;
    mutable std::atomic<uint64_t> preadBytes_{0};
    mutable std::atomic<uint64_t> preadCalls_{0};
};

// ---------------------------------------------------------------------------
// DirectIoReadFile: reports a direct-IO alignment requirement
// ---------------------------------------------------------------------------

class DirectIoReadFile : public CountingReadFile
{
public:
    DirectIoReadFile(std::vector<char> data, uint64_t alignment)
        : CountingReadFile(std::move(data)), alignment_(alignment)
    {
    }

    bool directIo(uint64_t & alignment) const override
    {
        alignment = alignment_;
        return true;
    }

private:
    uint64_t alignment_;
};

// ---------------------------------------------------------------------------
// Bounded spin utility
// ---------------------------------------------------------------------------

inline bool spinUntil(
    const std::function<bool()> & pred,
    std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
            return true;
        std::this_thread::yield();
    }
    return pred();
}

// ---------------------------------------------------------------------------
// Manager / Cache / Input construction helpers
// ---------------------------------------------------------------------------

struct FileCacheTestOptions
{
    uint64_t maxSize = 16ull << 20;
    uint64_t maxFileSegmentSize = 1ull << 20;
    uint64_t backgroundDownloadThreads = 0;
    bool skipCacheOnDiskFailure = false;
    bool enableBypassCacheWithThreshold = false;
    uint64_t bypassCacheThreshold = 256ull << 20;
    bool enableFilesystemQueryCacheLimit = false;
};

/// Creates a FileCacheManager with a single default cache configured from opts.
inline std::shared_ptr<FileCacheManager> makeManager(
    const std::string & cacheDir,
    memory::MemoryPool * pool,
    const std::shared_ptr<filesystems::FileSystem> & fs,
    const std::shared_ptr<folly::Timekeeper> & timekeeper,
    const FileCacheTestOptions & testOpts = {})
{
    FileCacheConfig config;
    config.path = cacheDir;
    config.maxSize = testOpts.maxSize;
    config.maxFileSegmentSize = testOpts.maxFileSegmentSize;
    config.boundaryAlignment = 1;
    config.reserveGranularity = 0;
    config.loadMetadataThreads = 1;
    config.backgroundDownloadThreads = testOpts.backgroundDownloadThreads;
    config.cachePolicy = FileCachePolicy::LRU;
    config.skipCacheOnDiskFailure = testOpts.skipCacheOnDiskFailure;
    config.enableBypassCacheWithThreshold = testOpts.enableBypassCacheWithThreshold;
    config.bypassCacheThreshold = testOpts.bypassCacheThreshold;
    config.enableFilesystemQueryCacheLimit = testOpts.enableFilesystemQueryCacheLimit;

    FileCacheManager::NamedFileCacheConfig named;
    named.name = "default";
    named.config = config;
    named.configPath = cacheDir + "/default.cfg";

    FileCacheManager::Options options;
    options.caches = {named};
    options.defaultCacheName = "default";
    options.commonUserId = "e2e-user";
    options.cachePathPrefix = cacheDir;
    options.allowedCacheRoot = cacheDir;
    options.localFileSystem = fs;
    options.memoryPool = pool;
    options.timekeeper = timekeeper;
    options.initializeOnCreate = true;
    return FileCacheManager::create(std::move(options));
}

/// Creates a FileCacheBufferedInput for the given ReadFile.
inline std::unique_ptr<FileCacheBufferedInput> makeInput(
    FileCacheManager & manager,
    FileCachePtr cache,
    std::shared_ptr<ReadFile> source,
    FileCacheKey key,
    memory::MemoryPool * pool,
    folly::Executor * executor,
    FileCacheReadOptions opts = {},
    const std::string & queryId = "q")
{
    dwio::common::ReaderOptions readerOptions(pool);
    FileCacheRequestContext context;
    context.queryId = queryId;
    context.userId = manager.commonUserId();
    FileCacheOriginInfo origin(manager.commonUserId(), context.userWeight);
    return std::make_unique<FileCacheBufferedInput>(
        std::move(source),
        std::move(cache),
        std::move(key),
        origin,
        std::move(opts),
        context,
        dwio::common::MetricsLog::voidLog(),
        /*ioStatistics*/ nullptr,
        /*ioStats*/ nullptr,
        executor,
        readerOptions);
}

// ---------------------------------------------------------------------------
// Stream reading helpers
// ---------------------------------------------------------------------------

/// Reads all remaining bytes from a stream via Next().
inline std::vector<char> readAll(dwio::common::SeekableInputStream & stream)
{
    std::vector<char> out;
    const void * data = nullptr;
    int size = 0;
    while (stream.Next(&data, &size))
        out.insert(out.end(), static_cast<const char *>(data),
                   static_cast<const char *>(data) + size);
    return out;
}

/// Reads exactly `n` bytes from a stream; asserts enough data is available.
inline std::vector<char> readN(dwio::common::SeekableInputStream & stream, size_t n)
{
    std::vector<char> out;
    out.reserve(n);
    const void * data = nullptr;
    int size = 0;
    while (out.size() < n && stream.Next(&data, &size))
    {
        size_t take = std::min<size_t>(size, n - out.size());
        out.insert(out.end(), static_cast<const char *>(data),
                   static_cast<const char *>(data) + take);
        if (static_cast<size_t>(size) > take)
            stream.BackUp(size - static_cast<int>(take));
    }
    return out;
}

} // namespace facebook::velox::ch::test
