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
#include "velox/common/caching/filecache/FileCache.h"

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>
#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/EvictionCandidates.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/caching/filecache/FileCacheUtils.h"
#include "velox/common/caching/filecache/FileCacheDownloadExecutor.h"
#include "velox/common/caching/filecache/FileSegmentInfo.h"
#include "velox/common/caching/filecache/IFileCachePriority.h"
#include "velox/common/caching/filecache/LRUFileCachePriority.h"
#include "velox/common/caching/filecache/SLRUFileCachePriority.h"
#include "velox/common/caching/filecache/SplitFileCachePriority.h"
#include "velox/common/file/File.h"

namespace fs = std::filesystem;

namespace facebook::velox::ch {

#define LOG_TEST(logger, ...) VLOG(1) << fmt::format(__VA_ARGS__)
#define LOG_TRACE(logger, ...) VLOG(1) << fmt::format(__VA_ARGS__)
#define LOG_DEBUG(logger, ...) VLOG(1) << fmt::format(__VA_ARGS__)
#define LOG_INFO(logger, ...) LOG(INFO) << fmt::format(__VA_ARGS__)
#define LOG_WARNING(logger, ...) LOG(WARNING) << fmt::format(__VA_ARGS__)
#define LOG_ERROR(logger, ...) LOG(ERROR) << fmt::format(__VA_ARGS__)

namespace {
std::string getCommonUserID() {
    static const std::string user = FileCacheKey::random().toString();
    return user;
}

size_t roundDownToMultiple(size_t num, size_t multiple) {
    if (!multiple)
        return num;
    return (num / multiple) * multiple;
}

bool tryParseUInt64(uint64_t & out, const std::string & text) {
    try {
        size_t pos = 0;
        unsigned long long value = std::stoull(text, &pos);
        if (pos != text.size())
            return false;
        out = static_cast<uint64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

uint64_t parseUInt64(const std::string & text) {
    uint64_t result = 0;
    VELOX_CHECK(tryParseUInt64(result, text), "Cannot parse uint64_t from {}", text);
    return result;
}

bool fileCacheSettingsEqual(const FileCacheSettings & lhs, const FileCacheSettings & rhs) {
    return lhs.path.value == rhs.path.value
        && lhs.maxSize.value == rhs.maxSize.value
        && lhs.maxElements.value == rhs.maxElements.value
        && lhs.maxFileSegmentSize.value == rhs.maxFileSegmentSize.value
        && lhs.backgroundDownloadThreads.value == rhs.backgroundDownloadThreads.value
        && lhs.backgroundDownloadQueueSizeLimit.value == rhs.backgroundDownloadQueueSizeLimit.value
        && lhs.backgroundDownloadMaxFileSegmentSize.value == rhs.backgroundDownloadMaxFileSegmentSize.value;
}

class Stopwatch {
public:
    Stopwatch() : start_(std::chrono::steady_clock::now()), end_(start_) {}
    void stop() { end_ = std::chrono::steady_clock::now(); }
    uint64_t elapsedMilliseconds() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(end_ - start_).count();
    }
private:
    std::chrono::steady_clock::time_point start_;
    std::chrono::steady_clock::time_point end_;
};

template <typename T>
class ConcurrentBoundedQueue {
public:
    explicit ConcurrentBoundedQueue(size_t capacity) : capacity_(capacity) {}
    bool tryPush(T value) {
        std::lock_guard lock(mutex_);
        if (finished_ || queue_.size() >= capacity_)
            return false;
        queue_.push_back(std::move(value));
        cv_.notify_one();
        return true;
    }
    bool pop(T & value) {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [&] { return finished_ || !queue_.empty(); });
        if (queue_.empty())
            return false;
        value = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }
    void finish() {
        std::lock_guard lock(mutex_);
        finished_ = true;
        cv_.notify_all();
    }
private:
    size_t capacity_;
    std::deque<T> queue_;
    bool finished_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
};
} // namespace

class StatusFile {
public:
    static constexpr bool write_full_info = true;
    StatusFile(const fs::path & path, bool) {
        facebook::velox::LocalWriteFile file(path.string(), true, false);
        file.append("ok");
        file.close();
    }
};

class ThreadFromGlobalPool {
public:
    template <typename F>
    explicit ThreadFromGlobalPool(F && f) : thread_(std::forward<F>(f)) {}
    bool joinable() const { return thread_.joinable(); }
    void join() { thread_.join(); }
private:
    std::thread thread_;
};

class BackgroundSchedulePoolTaskHolder {
public:
    template <typename F>
    explicit BackgroundSchedulePoolTaskHolder(F && f) : func_(std::forward<F>(f)) {}
    bool schedule() { VELOX_NYI("TODO(threading): Velox background scheduler integration for FileCache free-space task"); }
    bool scheduleAfter(uint64_t) { VELOX_NYI("TODO(threading): Velox delayed background scheduler integration for FileCache free-space task"); }
    void deactivate() {}
private:
    std::function<void()> func_;
};

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, State state)
{
    auto & local_stat = getStatByKind(kind);
    switch (state)
    {
        case State::Releasable:
        {
            totalStat.releasableSize += size;
            ++totalStat.releasableCount;

            local_stat.releasableSize += size;
            ++local_stat.releasableCount;
            break;
        }
        case State::NonReleasable:
        {
            totalStat.nonReleasableSize += size;
            ++totalStat.nonReleasableCount;

            local_stat.nonReleasableSize += size;
            ++local_stat.nonReleasableCount;
            break;
        }
        case State::Evicting:
        {
            ++totalStat.evictingCount;
            ++local_stat.evictingCount;
            break;
        }
        case State::Moving:
        {
            ++totalStat.movingCount;
            ++local_stat.movingCount;
            break;
        }
        case State::Invalidated:
        {
            ++totalStat.invalidatedCount;
            ++local_stat.invalidatedCount;
            break;
        }
    }
}

std::string FileCacheReserveStat::Stat::toString() const
{
    return fmt::format(
        "releasable size: {}, releasable count: {}, non-releasable size: {}, non-releasable count: {}, evicting count: {}, moving count: {}, invalidated count: {}",
        releasableSize,
        releasableCount,
        nonReleasableSize,
        nonReleasableCount,
        evictingCount,
        movingCount,
        invalidatedCount);
}

double normalizeProbability(double probability)
{
    if (probability < 0.0)
        probability = .0;
    else if (probability > 1.0)
        probability = 1.0;
    return probability;
}

FileCache::CheckCacheProbability::CheckCacheProbability(double probability, uint64_t seed)
    : rndgen(seed == 0 ? folly::Random::rand64() : seed)
    , distribution(normalizeProbability(probability))
{
}

bool FileCache::CheckCacheProbability::doCheck()
{
    std::lock_guard lock(mutex);
    return distribution(rndgen);
}

FileCache::FileCache(const std::string & cache_name, const FileCacheSettings & settings)
    : maxFileSegmentSize(settings.maxFileSegmentSize.value)
    , bypassCacheThreshold(settings.enableBypassCacheWithThreshold.value ? settings.bypassCacheThreshold.value : 0)
    , boundaryAlignment(settings.boundaryAlignment.value)
    , backgroundDownloadMaxFileSegmentSize(settings.backgroundDownloadMaxFileSegmentSize.value)
    , loadMetadataThreads(settings.loadMetadataThreads.value)
    , loadMetadataAsynchronously(settings.loadMetadataAsynchronously.value)
    , writeCachePerUserDirectory(settings.writeCachePerUserIdDirectory.value)
    , allowDynamicCacheResize(settings.allowDynamicCacheResize.value)
    , dynamicResizeLockWaitMs(settings.dynamicResizeLockWaitMs.value)
    , keepCurrentSizeToMaxRatio(1 - settings.keepFreeSpaceSizeRatio.value)
    , keepCurrentElementsToMaxRatio(1 - settings.keepFreeSpaceElementsRatio.value)
    , keepUpFreeSpaceRemoveBatch(settings.keepFreeSpaceRemoveBatch.value)
    , useSplitCache(settings.useSplitCache.value)
    , splitCacheRatio(settings.splitCacheRatio.value)
    , skipCacheOnDiskFailure_(settings.skipCacheOnDiskFailure.value)
    , name(cache_name)
    , backgroundDownloadThreads_(settings.backgroundDownloadThreads.value)
    , metadata(settings.path.value,
               settings.backgroundDownloadQueueSizeLimit.value,
               settings.backgroundDownloadThreads.value,
               writeCachePerUserDirectory)
    , checkCacheProbability(settings.checkCacheProbability.value)
{
    CachePriorityCreatorFunction creator_function;
    switch (settings.cachePolicy.value)
    {
        case FileCachePolicy::LRU:
        {
            creator_function = [](size_t max_size, size_t max_elements, double /*size_ratio*/, size_t /*overcommit_eviction_evict_step*/, std::string description) -> IFileCachePriorityPtr
            {
                return std::make_unique<LRUFileCachePriority>(
                    max_size,
                    max_elements,
                    description);
            };
            break;
        }
        case FileCachePolicy::SLRU:
        {
            creator_function = [](size_t max_size, size_t max_elements, double size_ratio, size_t /*overcommit_eviction_evict_step*/, std::string description) -> IFileCachePriorityPtr
            {
                return std::make_unique<SLRUFileCachePriority>(
                    max_size,
                    max_elements,
                    size_ratio,
                    description);
            };
            break;
        }
#if 0
        case FileCachePolicy::LRU_OVERCOMMIT:
        {
            creator_function = [](size_t max_size, size_t max_elements, double /*size_ratio*/, size_t overcommit_eviction_evict_step, std::string /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<LRUFileCachePriority>>(
                    overcommit_eviction_evict_step,
                    max_size,
                    max_elements,
                    "overcommit");
            };
            break;
        }
        case FileCachePolicy::SLRU_OVERCOMMIT:
        {
            creator_function = [](size_t max_size, size_t max_elements, double size_ratio, size_t overcommit_eviction_evict_step, std::string /*description*/) -> IFileCachePriorityPtr
            {
                return std::make_unique<OvercommitFileCachePriority<SLRUFileCachePriority>>(
                    overcommit_eviction_evict_step,
                    max_size,
                    max_elements,
                    size_ratio,
                    "overcommit");
            };
            break;
        }
#else
        case FileCachePolicy::LRU_OVERCOMMIT:
        case FileCachePolicy::SLRU_OVERCOMMIT:
            VELOX_USER_FAIL("Overcommit cache policies are not supported without distributed cache");
#endif
    }
    if (useSplitCache)
    {
        mainPriority = std::make_unique<SplitFileCachePriority>(
            creator_function,
            settings.maxSize.value,
            settings.maxElements.value,
            settings.slruSizeRatio.value,
            settings.splitCacheRatio.value,
            cache_name
        );
    }
    else
    {
        mainPriority = creator_function(
            settings.maxSize.value,
            settings.maxElements.value,
            settings.slruSizeRatio.value,
            settings.overcommitEvictionEvictStep.value,
            cache_name
        );
    }
    LOG_DEBUG(log, "Using {} cache policy", static_cast<int>(settings.cachePolicy.value));

    if (settings.enableFilesystemQueryCacheLimit.value)
        queryLimit = std::make_unique<FileCacheQueryLimit>();

    // TODO(metric): CH observation removed.
}

const FileCache::OriginInfo & FileCache::getCommonOrigin()
{
    static OriginInfo origin(getCommonUserID(), 0, FileSegmentKeyType::General);
    return origin;
}

FileCache::OriginInfo FileCache::getCommonOriginWithSegmentKeyType(const fs::path & filename) const
{
    auto origin = FileCache::getCommonOrigin();
    if (!useSplitCache)
        return origin;

    const static std::set<std::string> system_cache_type = {".txt", ".json", ".idx", ".cidx", ".dat"};
    origin.segmentType = system_cache_type.contains(filename.extension().string()) ? FileSegmentKeyType::System : FileSegmentKeyType::Data;
    return origin;
}

const FileCache::OriginInfo & FileCache::getInternalOrigin()
{
    static OriginInfo origin("internal");
    return origin;
}

bool FileCache::isInitialized() const
{
    return isInitialized_;
}

void FileCache::throwInitExceptionIfNeeded()
{
    if (loadMetadataAsynchronously)
        return;

    std::lock_guard lock(initMutex);
    if (initException)
        std::rethrow_exception(initException);
}

const std::string & FileCache::getBasePath() const
{
    return metadata.getBaseDirectory();
}

bool FileCache::skipCacheOnDiskFailure() const
{
    return skipCacheOnDiskFailure_;
}

std::string FileCache::getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin) const
{
    return metadata.getFileSegmentPath(key, offset, segment_kind, origin);
}

std::string FileCache::getKeyPath(const Key & key, const OriginInfo & origin) const
{
    return metadata.getKeyPath(key, origin);
}

void FileCache::assertInitialized() const
{
    if (isInitialized_)
        return;

    std::unique_lock lock(initMutex);
    if (isInitialized_)
        return;

    if (initException)
        std::rethrow_exception(initException);
    if (!isInitialized_)
        VELOX_FAIL("Cache not initialized");
}

void FileCache::initialize()
{
    // Prevent initialize() from running twice. This may be caused by two cache disks being created with the same path (see integration/test_filesystem_cache).
    folly::call_once(initializeCalled, [&] {
        bool need_to_load_metadata = fs::exists(getBasePath());
        try
        {
            if (!need_to_load_metadata)
                fs::create_directories(getBasePath());

            auto fs_info = std::filesystem::space(getBasePath());
            const size_t size_limit = mainPriority->getSizeLimit(cacheStateGuard.lock());
            if (fs_info.capacity < size_limit)
                VELOX_USER_FAIL("The total capacity of the disk containing cache path {} is less than the specified max_size {} bytes",
                                getBasePath(), std::to_string(size_limit));

            statusFile = std::make_unique<StatusFile>(fs::path(getBasePath()) / "status", StatusFile::write_full_info);
        }
        catch (const std::filesystem::filesystem_error & e)
        {
            initException = std::current_exception();
            VELOX_USER_FAIL("Failed to retrieve filesystem information for cache path {}. Error: {}",
                            getBasePath(), e.what());
        }
        catch (...)
        {
            initException = std::current_exception();
            LOG(ERROR) << "Exception in " << __PRETTY_FUNCTION__;
            throw;
        }

        if (loadMetadataAsynchronously)
        {
            loadMetadataMainThread = std::make_unique<ThreadFromGlobalPool>([this, need_to_load_metadata] { initializeImpl(need_to_load_metadata); });
        }
        else
        {
            initializeImpl(need_to_load_metadata);
        }

        if (backgroundDownloadThreads_ > 0)
            downloadExecutor_ = std::make_unique<FileCacheDownloadExecutor>(backgroundDownloadThreads_);
    });
}

void FileCache::initializeImpl(bool load_metadata)
{
    std::lock_guard lock(initMutex);

    if (isInitialized_)
        return;

    try
    {
        if (load_metadata)
            loadMetadata();

        metadata.startup();
    }
    catch (...)
    {
        initException = std::current_exception();
        LOG(ERROR) << "Exception in " << __PRETTY_FUNCTION__;
        throw;
    }

    if (keepCurrentSizeToMaxRatio != 1 || keepCurrentElementsToMaxRatio != 1)
    {
        keepUpFreeSpaceRatioTask = std::make_unique<BackgroundSchedulePoolTaskHolder>([this] { freeSpaceRatioKeepingThreadFunc(); });
        keepUpFreeSpaceRatioTask->schedule();
    }

    isInitialized_ = true;
    LOG_TEST(log, "Initialized cache from {}", metadata.getBaseDirectory());
}

CachePriorityGuard::WriteLock FileCache::lockCache() const
{
    return cacheGuard.writeLock();
}

FileSegments FileCache::getImpl(const LockedKey & locked_key, const FileSegment::Range & range, size_t file_segments_limit) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (bypassCacheThreshold && range.size() > bypassCacheThreshold)
    {
        auto file_segment = std::make_shared<FileSegment>(
            locked_key.getKey(), range.left, range.size(), FileSegment::State::DETACHED);
        return { file_segment };
    }

    if (locked_key.empty())
        return {};

    FileSegments result;
    auto add_to_result = [&](const FileSegmentMetadata & file_segment_metadata)
    {
        if (file_segments_limit && result.size() == file_segments_limit)
            return false;

        FileSegmentPtr file_segment;
        if (file_segment_metadata.isEvictingOrRemoved(locked_key))
        {
            file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(),
                file_segment_metadata.fileSegment->offset(),
                file_segment_metadata.fileSegment->range().size(),
                FileSegment::State::DETACHED);
        }
        else
        {
            file_segment = file_segment_metadata.fileSegment;
        }

        result.push_back(file_segment);
        return true;
    };

    const auto & file_segments = locked_key;
    auto segment_it = file_segments.lower_bound(range.left);
    if (segment_it == file_segments.end())
    {
        /// N - last cached segment for given file key, segment{N}.offset < range.left:
        ///   segment{N}                       segment{N}
        /// [________                         [_______]
        ///     [__________]         OR                  [________]
        ///     ^                                        ^
        ///     range.left                               range.left

        const auto & file_segment_metadata = *file_segments.rbegin()->second;
        if (file_segment_metadata.fileSegment->range().right < range.left)
            return {};

        if (!add_to_result(file_segment_metadata))
            return result;
    }
    else /// segment_it <-- segmment{k}
    {
        if (segment_it != file_segments.begin())
        {
            const auto & prev_file_segment_metadata = *std::prev(segment_it)->second;
            const auto & prev_range = prev_file_segment_metadata.fileSegment->range();

            if (range.left <= prev_range.right)
            {
                ///   segment{k-1}  segment{k}
                ///   [________]   [_____
                ///       [___________
                ///       ^
                ///       range.left
                if (!add_to_result(prev_file_segment_metadata))
                    return result;
            }
        }

        ///  segment{k} ...       segment{k-1}  segment{k}                      segment{k}
        ///  [______              [______]     [____                        [________
        ///  [_________     OR              [________      OR    [______]   ^
        ///  ^                              ^                           ^   segment{k}.offset
        ///  range.left                     range.left                  range.right

        while (segment_it != file_segments.end())
        {
            const auto & file_segment_metadata = *segment_it->second;
            if (range.right < file_segment_metadata.fileSegment->range().left)
                break;

            if (!add_to_result(file_segment_metadata))
                return result;

            ++segment_it;
        }
    }

    return result;
}

std::vector<FileSegment::Range> FileCache::splitRange(size_t offset, size_t size, size_t aligned_size)
{
    VELOX_DCHECK(size > 0);
    VELOX_DCHECK(size <= aligned_size);

    /// Consider this example to understand why we need to account here for both `size` and `aligned_size`.
    /// [________________]__________________] <-- requested range
    ///                  ^                  ^
    ///                right offset         aligned_right_offset
    /// [_________]                           <-- last cached file segment, e.g. we have uncovered suffix of the requested range
    ///           ^
    ///           last_file_segment_right_offset
    /// [________________]
    ///        size
    /// [____________________________________]
    ///        aligned_size
    ///
    /// So it is possible that we split this hole range into sub-segments by `maxFileSegmentSize`
    /// and get something like this:
    ///
    /// [________________________]
    ///          ^               ^
    ///          |               last_file_segment_right_offset + maxFileSegmentSize
    ///          last_file_segment_right_offset
    /// e.g. there is no need to create sub-segment for range (last_file_segment_right_offset + maxFileSegmentSize, aligned_right_offset].
    /// Because its left offset would be bigger than right_offset.
    /// Therefore, we set end_pos_non_included as offset+size, but remaining_size as aligned_size.

    std::vector<FileSegment::Range> ranges;

    size_t current_pos = offset;
    size_t end_pos_non_included = offset + size;
    size_t remaining_size = aligned_size;

    const size_t max_size = maxFileSegmentSize.load();
    while (current_pos < end_pos_non_included)
    {
        auto current_file_segment_size = std::min(remaining_size, max_size);
        ranges.emplace_back(current_pos, current_pos + current_file_segment_size - 1);

        remaining_size -= current_file_segment_size;
        current_pos += current_file_segment_size;
    }

    return ranges;
}

FileSegments FileCache::createFileSegmentsFromRanges(
    LockedKey & locked_key,
    const std::vector<FileSegment::Range> & ranges,
    size_t & file_segments_count,
    size_t file_segments_limit,
    const CreateFileSegmentSettings & create_settings)
{
    FileSegments result;
    for (const auto & r : ranges)
    {
        if (file_segments_limit && file_segments_count >= file_segments_limit)
            break;
        auto metadata_it = addFileSegment(locked_key, r.left, r.size(), FileSegment::State::EMPTY, create_settings);
        result.push_back(metadata_it->second->fileSegment);
        ++file_segments_count;
    }
    return result;
}

void FileCache::fillHolesWithEmptyFileSegments(
    LockedKey & locked_key,
    FileSegments & file_segments,
    const FileSegment::Range & range,
    size_t non_aligned_right_offset,
    size_t file_segments_limit,
    bool fill_with_detached_file_segments,
    const CreateFileSegmentSettings & create_settings)
{
    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a file_segment_metadata with file segment state EMPTY.

    VELOX_DCHECK(!file_segments.empty());

    auto it = file_segments.begin();
    size_t processed_count = 0;
    auto segment_range = (*it)->range();

    size_t current_pos;
    if (segment_range.left < range.left)
    {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// segment1

        current_pos = segment_range.right + 1;
        ++it;
        ++processed_count;
    }
    else
        current_pos = range.left;

    auto is_limit_reached = [&]() -> bool
    {
        return file_segments_limit && processed_count >= file_segments_limit;
    };

    while (current_pos <= range.right && it != file_segments.end() && !is_limit_reached())
    {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left)
        {
            current_pos = segment_range.right + 1;
            ++it;
            ++processed_count;
            continue;
        }

        VELOX_DCHECK(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(), current_pos, hole_size, FileSegment::State::DETACHED, create_settings);

            file_segments.insert(it, file_segment);
            ++processed_count;
        }
        else
        {
            const auto ranges = splitRange(current_pos, hole_size, hole_size);
            auto hole_segments = createFileSegmentsFromRanges(locked_key, ranges, processed_count, file_segments_limit, create_settings);
            file_segments.splice(it, std::move(hole_segments));
        }

        if (is_limit_reached())
            break;

        current_pos = segment_range.right + 1;
        ++it;
        ++processed_count;
    }

    auto erase_unprocessed = [&]()
    {
        VELOX_DCHECK(file_segments.size() >= file_segments_limit);
        file_segments.erase(it, file_segments.end());
        VELOX_DCHECK(file_segments.size() == file_segments_limit);
    };

    if (is_limit_reached())
    {
        erase_unprocessed();
        return;
    }

    VELOX_DCHECK(!file_segments_limit || file_segments.size() < file_segments_limit);

    if (current_pos <= non_aligned_right_offset)
    {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;
        auto non_aligned_hole_size = non_aligned_right_offset - current_pos + 1;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(), current_pos, non_aligned_hole_size, FileSegment::State::DETACHED, create_settings);

            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            const auto ranges = splitRange(current_pos, non_aligned_hole_size, hole_size);
            auto hole_segments = createFileSegmentsFromRanges(locked_key, ranges, processed_count, file_segments_limit, create_settings);
            file_segments.splice(it, std::move(hole_segments));

            if (is_limit_reached())
                erase_unprocessed();
        }
    }
}

FileSegmentsHolderPtr FileCache::trySet(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings,
    const OriginInfo & origin)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin);
    FileSegment::Range range(offset, offset + size - 1);

    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0);
    if (!file_segments.empty())
        return nullptr;

    if (create_settings.unbounded)
    {
        /// If the file is unbounded, we can create a single file_segment_metadata for it.
        auto file_segment_metadata_it = addFileSegment(
            *locked_key, offset, size, FileSegment::State::EMPTY, create_settings);
        file_segments = {file_segment_metadata_it->second->fileSegment};
    }
    else
    {
        const auto ranges = splitRange(offset, size, size);
        size_t file_segments_count = 0;
        file_segments = createFileSegmentsFromRanges(*locked_key, ranges, file_segments_count, /* file_segments_limit */0, create_settings);
    }

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::set(
    const Key & key,
    size_t offset,
    size_t size,
    const CreateFileSegmentSettings & create_settings,
    const OriginInfo & origin)
{
    if (auto holder = trySet(key, offset, size, create_settings, origin))
        return holder;

    VELOX_FAIL("Having intersection with already existing cache");
}

FileSegmentsHolderPtr
FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_size,
    const CreateFileSegmentSettings & create_settings,
    size_t file_segments_limit,
    const OriginInfo & origin_info,
    std::optional<size_t> boundary_alignment_)
{
    // TODO(metric): CH observation removed.

    assertInitialized();

    size_t initial_range_right_offset = (file_size ? std::min(offset + size, file_size) : offset + size) - 1;
    FileSegment::Range initial_range(offset, initial_range_right_offset);
    /// result_range is initial range, which will be adjusted according to
    /// 1. aligned_offset, aligned_end_offset
    /// 2. max_file_segments_limit
    FileSegment::Range result_range = initial_range;

    const size_t alignment = boundary_alignment_.value_or(boundaryAlignment);
    const auto aligned_offset = roundDownToMultiple(initial_range.left, alignment);
    auto aligned_end_offset = (file_size
        ? std::min(FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment), file_size)
        : FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment)) - 1;

    VELOX_DCHECK(aligned_offset <= initial_range.left);
    VELOX_DCHECK(aligned_end_offset >= initial_range.right);

    auto locked_key = metadata.lockKeyMetadata(
        key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin_info);

    /// Get all segments which intersect with the given range.
    auto file_segments = getImpl(*locked_key, initial_range, file_segments_limit);

    if (file_segments_limit)
    {
        VELOX_DCHECK(file_segments.size() <= file_segments_limit);
        if (file_segments.size() == file_segments_limit)
            result_range.right = aligned_end_offset = file_segments.back()->range().right;
    }

    /// Check case if we have uncovered prefix, e.g.
    ///
    ///   [_______________]
    ///   ^               ^
    ///   range.left      range.right
    ///         [___] [__________]        <-- current cache (example)
    ///   [    ]
    ///   ^----^
    ///   uncovered prefix.
    const bool has_uncovered_prefix = file_segments.empty() || result_range.left < file_segments.front()->range().left;

    if (aligned_offset < result_range.left && has_uncovered_prefix)
    {
        auto prefix_range = FileSegment::Range(
            aligned_offset,
            file_segments.empty() ? result_range.left - 1 : file_segments.front()->range().left - 1);

        auto prefix_file_segments = getImpl(*locked_key, prefix_range, /* file_segments_limit */0);

        if (prefix_file_segments.empty())
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left      range.right
            ///                             [___] [__________]         <-- current cache (example)
            result_range.left = aligned_offset;
        }
        else
        {
            ///   [____________________][_______________]
            ///   ^                     ^               ^
            ///   aligned_offset        range.left          range.right
            ///   ____]     [____]           [___] [__________]        <-- current cache (example)
            ///                  ^
            ///                  prefix_file_segments.back().right

            VELOX_DCHECK(prefix_file_segments.back()->range().right < result_range.left);
            VELOX_DCHECK(prefix_file_segments.back()->range().right >= aligned_offset);

            result_range.left = prefix_file_segments.back()->range().right + 1;
        }
    }

    /// Check case if we have uncovered suffix.
    ///
    ///   [___________________]
    ///   ^                   ^
    ///   range.left          range.right
    ///      [___]   [___]                  <-- current cache (example)
    ///                   [___]
    ///                   ^---^
    ///                    uncovered_suffix
    const bool has_uncovered_suffix = file_segments.empty() || file_segments.back()->range().right < result_range.right;

    if (result_range.right < aligned_end_offset && has_uncovered_suffix)
    {
        auto suffix_range = FileSegment::Range(result_range.right, aligned_end_offset);
        /// We need to get 1 file segment, so file_segments_limit = 1 here.
        auto suffix_file_segments = getImpl(*locked_key, suffix_range, /* file_segments_limit */1);

        if (suffix_file_segments.empty())
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]                                    <-- current cache (example)

            result_range.right = aligned_end_offset;
        }
        else
        {
            ///   [__________________][                       ]
            ///   ^                  ^                        ^
            ///   range.left         range.right              aligned_end_offset
            ///      [___]   [___]          [_________]               <-- current cache (example)
            ///                             ^
            ///                             suffix_file_segments.front().left
            result_range.right = suffix_file_segments.front()->range().left - 1;
        }
    }

    if (file_segments.empty())
    {
        auto ranges = splitRange(result_range.left, initial_range.size() + (initial_range.left - result_range.left), result_range.size());
        size_t file_segments_count = file_segments.size();
        file_segments.splice(
            file_segments.end(),
            createFileSegmentsFromRanges(*locked_key, ranges, file_segments_count, file_segments_limit, create_settings));
    }
    else
    {
        VELOX_DCHECK(file_segments.front()->range().right >= result_range.left);
        VELOX_DCHECK(file_segments.back()->range().left <= result_range.right);

        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, result_range, offset + size - 1, file_segments_limit, /* fill_with_detached */false, create_settings);

        if (!file_segments.front()->range().contains(result_range.left))
        {
            VELOX_FAIL("Expected {} to include {} "
                "(end offset: {}, aligned offset: {}, aligned end offset: {})",
                file_segments.front()->range().toString(), offset,
                result_range.right, aligned_offset, aligned_end_offset);
        }
    }

    /// Compare with initial_range and not result_range,
    /// See comment in splitRange for explanation.
    VELOX_DCHECK(file_segments_limit
             ? file_segments.back()->range().left <= initial_range.right
             : file_segments.back()->range().contains(initial_range.right),
             fmt::format(
                 "Unexpected state. Back: {}, result range: {}, "
                 "limit: {}, initial offset: {}, initial size: {}, file size: {}",
                 file_segments.back()->range().toString(), result_range.toString(),
                 file_segments_limit, offset, size, file_size));

    VELOX_DCHECK(!file_segments_limit || file_segments.size() <= file_segments_limit);

    locked_key.reset();
    assertCacheCorrectnessWithProbability();
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_segments_limit,
    const UserID & userId)
{
    // TODO(metric): CH observation removed.

    assertInitialized();

    std::unique_ptr<FileSegmentsHolder> holder;
    if (auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(userId));
        locked_key != nullptr)
    {
        FileSegment::Range range(offset, offset + size - 1);

        /// Get all segments which intersect with the given range.
        auto file_segments = getImpl(*locked_key, range, file_segments_limit);
        if (!file_segments.empty())
        {
            if (file_segments_limit)
            {
                VELOX_DCHECK(file_segments.size() <= file_segments_limit);
                if (file_segments.size() == file_segments_limit)
                    range.right = file_segments.back()->range().right;
            }

            fillHolesWithEmptyFileSegments(
                *locked_key, file_segments, range, offset + size - 1, file_segments_limit, /* fill_with_detached */true, CreateFileSegmentSettings{});

            VELOX_DCHECK(!file_segments_limit || file_segments.size() <= file_segments_limit);
            holder = std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    if (!holder)
        holder = std::make_unique<FileSegmentsHolder>(FileSegments{std::make_shared<FileSegment>(key, offset, size, FileSegment::State::DETACHED)});

    assertCacheCorrectnessWithProbability();
    return holder;
}

KeyMetadata::iterator FileCache::addFileSegment(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & create_settings)
{
    /// Create a file_segment_metadata and put it in `files` map by [key][offset].

    VELOX_DCHECK(size > 0); /// Empty file segments in cache are not allowed.

    const auto & key = locked_key.getKey();
    const FileSegment::Range range(offset, offset + size - 1);

    if (auto intersecting_range = locked_key.hasIntersectingRange(range))
    {
        VELOX_FAIL("Attempt to add intersecting file segment in cache ({} intersects {})",
            range.toString(), intersecting_range->toString());
    }

    FileSegment::State result_state = state;

    auto file_segment = std::make_shared<FileSegment>(
        key,
        offset,
        size,
        result_state,
        create_settings,
        metadata.isBackgroundDownloadEnabled(),
        this,
        locked_key.getKeyMetadata());

    auto file_segment_metadata = std::make_shared<FileSegmentMetadata>(std::move(file_segment));

    auto [file_segment_metadata_it, inserted] = locked_key.emplace(offset, file_segment_metadata);
    if (!inserted)
    {
        VELOX_FAIL("Failed to insert {}:{}: entry already exists", key, offset);
    }

    return file_segment_metadata_it;
}

bool FileCache::tryIncreasePriority(FileSegment & file_segment)
{
    std::shared_lock lock(dynamicResizeLock, std::try_to_lock);
    /// Skip priority increase if cache resize is currently in progress.
    /// We cannot do queue moves during dynamic resize.
    if (!lock.owns_lock())
        return false;
    return mainPriority->tryIncreasePriority(
        *file_segment.getQueueIterator(), file_segment.isCompleted(), cacheGuard, cacheStateGuard);
}

bool FileCache::tryReserve(
    FileSegment & file_segment,
    size_t size,
    FileCacheReserveStat & reserve_stat,
    const OriginInfo & origin_info,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason)
{
    // TODO(metric): CH observation removed.
    // TODO(metric): CH observation removed.
    // TODO(metric): CH observation removed.

    assertInitialized();

    /// Skip space reservation if dynamic cache resize is currently in progress.
    /// We cannot do both at the same time.
    std::shared_lock resize_shared_lock(dynamicResizeLock, std::try_to_lock);
    if (!resize_shared_lock.owns_lock())
    {
        // TODO(metric): CH observation removed.
        failure_reason = "cache is being resized";
        return false;
    }

    cacheReserveActiveThreads.fetch_add(1, std::memory_order_relaxed);
    auto cacheReserveActiveThreadsGuard = folly::makeGuard([&] {
        cacheReserveActiveThreads.fetch_sub(1, std::memory_order_relaxed);
    });

    LOG_TEST(log, "Trying to reserve space ({} bytes) for {}:{}", size, file_segment.key(), file_segment.offset());

    return doTryReserve(
        file_segment, size, reserve_stat, origin_info, lock_wait_timeout_milliseconds,
        failure_reason);
}

bool FileCache::doTryReserve(
    FileSegment & file_segment,
    size_t size,
    FileCacheReserveStat & reserve_stat,
    const OriginInfo & origin_info,
    size_t /* lock_wait_timeout_milliseconds */,
    std::string & failure_reason)
{
    auto main_priority_iterator = file_segment.getQueueIterator();
#ifdef DEBUG_OR_SANITIZER_BUILD
    /// A file_segment_metadata acquires a priority iterator
    /// on first successful space reservation attempt,
    /// so main_priority_iterator == nullptr, if no space reservation took place yet.
    if (main_priority_iterator)
        VELOX_DCHECK(file_segment.getReservedSize() > 0);
    else
        VELOX_DCHECK(file_segment.getReservedSize() == 0);
#endif

    /// In case of per query cache limit (by default disabled),
    /// we add/remove entries from both (mainPriority and query_priority) priority queues,
    /// but iterate entries in order of query_priority, while checking the limits in both.
    Priority * query_priority = nullptr;
    FileCacheQueryLimit::QueryContextPtr query_context;

    std::unique_ptr<EvictionInfo> main_eviction_info; /// Server scoped cache limits eviction info.
    std::unique_ptr<EvictionInfo> query_eviction_info; /// Query scoped cache limits eviction info.

    /// First collect "eviction info" under cache state lock, which will tell us
    /// how much do we need to evict in order to make sure we have enough space in cache.
    /// Also "eviction info" includes `HoldSpace` holders in case all/subset of cache size
    /// is already available, making sure current thread "locks" that part of the cache
    /// before starting to evict remaining space.
    {
        size_t required_elements_num = main_priority_iterator ? 0 : 1;

        auto lock = cacheStateGuard.lock();

        /// Check per-query cache limits.
        if (queryLimit)
        {
            query_context = queryLimit->tryGetQueryContext(lock);
            if (query_context)
            {
                query_priority = &query_context->getPriority();
                if (!query_priority->canFit(size, required_elements_num, lock, /* reservee */nullptr, origin_info)
                    && !query_context->recacheOnFileCacheQueryLimitExceeded())
                {
                    LOG_TEST(
                        log, "Query limit exceeded, space reservation failed, "
                        "recache_on_query_limit_exceeded is disabled (while reserving for {}:{} with size {}): {}",
                        file_segment.key(), file_segment.offset(), size, query_priority->getStateInfoForLog(lock));

                    failure_reason = "query limit exceeded";
                    return false;
                }
                query_eviction_info = query_priority->collectEvictionInfo(
                    size,
                    required_elements_num,
                    main_priority_iterator.get(),
                    /* is_total_space_cleanup */false,
                    origin_info,
                    lock);
            }
        }

        /// Check server-wide cache limits.
        main_eviction_info = mainPriority->collectEvictionInfo(
            size,
            required_elements_num,
            main_priority_iterator.get(),
            /* is_total_space_cleanup */false,
            origin_info,
            lock);

        /// Can we already just increment size for the queue entry and quit?
        /// TODO: allow to quit here if query_context != nullptr.
        if (main_priority_iterator && !main_eviction_info->requiresEviction() && !query_context)
        {
            main_eviction_info->releaseHoldSpace(lock);
            main_priority_iterator->incrementSize(size, lock);

            file_segment.reservedSize += size;
            VELOX_DCHECK(file_segment.reservedSize == main_priority_iterator->getEntry()->size);
            return true;
        }
    }

    EvictionCandidates eviction_candidates;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;

    /// Collect candidates for eviction and
    /// evict them from in-memory state and from filesystem.
    if (!doEviction(
        *main_eviction_info, query_eviction_info.get(), file_segment, origin_info,
        main_priority_iterator, reserve_stat, eviction_candidates,
        invalidated_entries, query_priority, failure_reason))
    {
        VELOX_DCHECK(!failure_reason.empty());
        return false;
    }

    bool added_new_main_entry = !main_priority_iterator;
    Priority::IteratorPtr query_priority_iterator;

    if (!main_priority_iterator || eviction_candidates.requiresAfterEvictWrite())
    {
        auto lock = cacheGuard.writeLock();
        eviction_candidates.afterEvictWrite(lock);
        IFileCachePriority::removeEntries(invalidated_entries, lock);

        if (!main_priority_iterator)
        {
            /// Create a new queue entry with size 0.
            main_priority_iterator = mainPriority->add(
                file_segment.getKeyMetadata(),
                file_segment.offset(),
                /* size */0,
                lock,
                nullptr);

            if (query_context)
            {
                query_priority_iterator = query_context->tryGet(file_segment.key(), file_segment.offset(), lock);
                if (!query_priority_iterator)
                    query_context->add(file_segment.getKeyMetadata(), file_segment.offset(), /* size */0, lock);
            }
        }
    }
    else if (!invalidated_entries.empty())
    {
        if (auto lock = cacheGuard.tryWriteLock(); lock.owns_lock())
            IFileCachePriority::removeEntries(invalidated_entries, lock);
    }

    try
    {
        auto lock = cacheStateGuard.lock();
        main_eviction_info->releaseHoldSpace(lock);
        if (query_eviction_info)
            query_eviction_info->releaseHoldSpace(lock);

        eviction_candidates.afterEvictState(lock);
        main_priority_iterator->incrementSize(size, lock);

        if (query_priority_iterator)
            query_priority_iterator->incrementSize(size, lock);
    }
    catch (...)
    {
        /// Protect against zombie queue entries which are not assigned to any file segment
        /// and are not "invalidated" (which makes them non-removable).
        if (main_priority_iterator && added_new_main_entry)
            main_priority_iterator->invalidate();

        throw;
    }

    /// Mark that size was successfully updated.
    if (added_new_main_entry)
        file_segment.setQueueIterator(main_priority_iterator);

    file_segment.reservedSize += size;
    VELOX_DCHECK(file_segment.reservedSize == main_priority_iterator->getEntry()->size);

    if (!file_segment.getKeyMetadata()->createBaseDirectory())
    {
        failure_reason = "not enough space on device";
        return false;
    }

    return true;
}

bool FileCache::doEviction(
    const EvictionInfo & main_eviction_info,
    const EvictionInfo * query_eviction_info,
    FileSegment & file_segment,
    const OriginInfo & origin_info,
    const IFileCachePriority::IteratorPtr & main_priority_iterator,
    FileCacheReserveStat & reserve_stat,
    EvictionCandidates & eviction_candidates,
    IFileCachePriority::InvalidatedEntriesInfos & invalidated_entries,
    Priority * query_priority,
    std::string & failure_reason)
{
    LOG_TEST(log, "Main eviction info {}", main_eviction_info.toString());
    if (query_priority)
        LOG_TEST(log, "Query eviction info {}", query_eviction_info->toString());

    if (!main_eviction_info.requiresEviction()
        && (!query_eviction_info || !query_eviction_info->requiresEviction()))
        return true;

    /// If there is at least something we need to evict,
    /// we need to collect "eviction candidates".
    /// This is done under "read (shared) lock" for query priority queue.
    {
        auto on_cannot_evict_enough_space_message = [&](const IFileCachePriority & priority)
        {
            const auto & stat = reserve_stat.totalStat;
            return fmt::format(
                "cannot evict enough space "
                "(stat: {}, total size: {}/{}, total elements: {}/{}, "
                "background download elements: {})",
                stat.toString(),
                priority.getSizeApprox(), priority.getSizeLimitApprox(),
                priority.getElementsCountApprox(), priority.getElementsLimitApprox(),
                0 /* TODO(metric): FilesystemCacheDownloadQueueElements */);
        };

        bool continue_from_last_eviction_pos = cacheReserveActiveThreads.load(std::memory_order_relaxed) > 1;
        if (!continue_from_last_eviction_pos)
            mainPriority->resetEvictionPos();

        if (query_eviction_info && query_eviction_info->requiresEviction())
        {
            VELOX_DCHECK(query_priority);
            if (!query_priority->collectCandidatesForEviction(
                    *query_eviction_info,
                    reserve_stat,
                    eviction_candidates,
                    invalidated_entries,
                    /* reservee */{},
                    continue_from_last_eviction_pos,
                    /* max_candidates_size */0,
                    /* is_total_space_cleanup */false,
                    origin_info,
                    cacheGuard,
                    cacheStateGuard))
            {
                failure_reason = on_cannot_evict_enough_space_message(*query_priority);
                return false;
            }

            LOG_TEST(log, "Query limits satisfied (while reserving for {}:{})",
                     file_segment.key(), file_segment.offset());
        }

        if (!mainPriority->collectCandidatesForEviction(
                main_eviction_info,
                reserve_stat,
                eviction_candidates,
                invalidated_entries,
                main_priority_iterator,
                continue_from_last_eviction_pos,
                /* max_candidates_size */0,
                /* is_total_space_cleanup */false,
                origin_info,
                cacheGuard,
                cacheStateGuard))
        {
            failure_reason = on_cannot_evict_enough_space_message(*mainPriority);
            return false;
        }
    }

    /// Remove eviction candidates from filesystem.
    /// This is done without any lock.
    if (eviction_candidates.size() > 0)
    {
        auto on_failed_evict = [&]()
        {
            {
                auto lock = cacheGuard.writeLock();
                eviction_candidates.afterEvictWrite(lock);
                IFileCachePriority::removeEntries(invalidated_entries, lock);
            }
            eviction_candidates.afterEvictState(cacheStateGuard.lock());
        };
        try
        {
            eviction_candidates.evict();
            evictions_.fetch_add(
                eviction_candidates.getNumEvicted(), std::memory_order_relaxed);
        }
        catch (...)
        {
            evictions_.fetch_add(
                eviction_candidates.getNumEvicted(), std::memory_order_relaxed);
            on_failed_evict();
            throw;
        }

        const auto & failed_candidates = eviction_candidates.getFailedCandidates();
        if (failed_candidates.size() > 0)
        {
            on_failed_evict();
            VELOX_FAIL("Failed to evict {} file segments (first error: {})",
                failed_candidates.size(), failed_candidates.getFirstErrorMessage());
        }
    }
    return true;
}

void FileCache::freeSpaceRatioKeepingThreadFunc()
{
    static constexpr auto lock_failed_reschedule_ms = 1000;
    static constexpr auto space_ratio_satisfied_reschedule_ms = 5000;
    static constexpr auto general_reschedule_ms = 5000;

    if (shutdown)
        return;
    // TODO(failpoint): CH file_cache_stall_free_space_ratio_keeping_thread failpoint omitted.

    Stopwatch watch;

    std::unique_ptr<EvictionInfo> eviction_info;
    {
        auto lock = cacheStateGuard.tryLock();

        /// To avoid deteriorating contention on cache,
        /// proceed only if cache is not heavily used.
        if (!lock)
        {
            keepUpFreeSpaceRatioTask->scheduleAfter(lock_failed_reschedule_ms);
            return;
        }

        const size_t size_limit = mainPriority->getSizeLimit(lock);
        const size_t elements_limit = mainPriority->getElementsLimit(lock);

        const size_t desired_size = std::lround(keepCurrentSizeToMaxRatio * static_cast<double>(size_limit));
        const size_t desired_elements_num = std::lround(keepCurrentElementsToMaxRatio * static_cast<double>(elements_limit));

        const size_t current_size = mainPriority->getSize(lock);
        const size_t current_elements_num = mainPriority->getElementsCount(lock);

        const size_t size_to_evict = current_size && (current_size > desired_size)
            ? current_size - desired_size
            : 0;
        const size_t elements_to_evict = current_elements_num && (current_elements_num > desired_elements_num)
            ? current_elements_num - desired_elements_num
            : 0;

        if (!size_to_evict && !elements_to_evict)
        {
            /// Nothing to free - all limits are satisfied.
            keepUpFreeSpaceRatioTask->scheduleAfter(space_ratio_satisfied_reschedule_ms);
            return;
        }

        LOG_TRACE(
            log, "Starting an iteration to maintain desired size. "
            "Current size {}/{}, elements {}/{}. Desired size: {}, desired elements: {}",
            mainPriority->getSize(lock), size_limit,
            mainPriority->getElementsCount(lock), elements_limit,
            desired_size, desired_elements_num);

        eviction_info = mainPriority->collectEvictionInfo(
            size_to_evict,
            elements_to_evict,
            /* reservee */nullptr,
            /* is_total_space_cleanup */true,
            getInternalOrigin(),
            lock);
    }

    VELOX_DCHECK(!eviction_info->hasHoldSpace());
    VELOX_DCHECK(eviction_info->requiresEviction());

    // TODO(metric): CH observation removed.

    FileCacheReserveStat stat;
    EvictionCandidates eviction_candidates;

    IFileCachePriority::CollectStatus desired_size_status =  IFileCachePriority::CollectStatus::CANNOT_EVICT;
    /// Collect at most `keepUpFreeSpaceRemoveBatch` elements to evict,
    /// (we use batches to make sure we do not block cache for too long,
    /// by default the batch size is quite small).

    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    mainPriority->collectCandidatesForEviction(
        *eviction_info,
        stat,
        eviction_candidates,
        invalidated_entries,
        /* reservee */nullptr,
        /* continue_from_last_eviction_pos */false,
        /* max_candidates_size */keepUpFreeSpaceRemoveBatch,
        /* is_total_space_cleanup */true,
        getInternalOrigin(),
        cacheGuard,
        cacheStateGuard);

    if (eviction_candidates.size() > 0)
    {
        desired_size_status = IFileCachePriority::CollectStatus::SUCCESS;
        eviction_candidates.evict();
        evictions_.fetch_add(
            eviction_candidates.getNumEvicted(), std::memory_order_relaxed);
    }

    /// Take lock again to finalize eviction,
    /// e.g. to update the in-memory state.
    {
        auto lock = cacheGuard.writeLock();
        eviction_candidates.afterEvictWrite(lock);
        IFileCachePriority::removeEntries(invalidated_entries, lock);
    }
    eviction_candidates.afterEvictState(cacheStateGuard.lock());

    // TODO(metric): CH observation removed.
    // TODO(metric): CH observation removed.

    watch.stop();
    // TODO(metric): CH observation removed.

    LOG_TRACE(log, "Free space ratio keeping thread finished with status `{}` in {} ms",
              static_cast<int>(desired_size_status), watch.elapsedMilliseconds());

    [[maybe_unused]] bool scheduled = false;
    switch (desired_size_status)
    {
        case IFileCachePriority::CollectStatus::SUCCESS: [[fallthrough]];
        case IFileCachePriority::CollectStatus::CANNOT_EVICT:
        {
            scheduled = keepUpFreeSpaceRatioTask->scheduleAfter(general_reschedule_ms);
            break;
        }
        case IFileCachePriority::CollectStatus::REACHED_MAX_CANDIDATES_LIMIT:
        {
            scheduled = keepUpFreeSpaceRatioTask->schedule();
            break;
        }
    }
    VELOX_DCHECK(scheduled);
}

void FileCache::iterate(IterateFunc && func, const UserID & userId)
{
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & file_segment_metadata : locked_key)
            func(FileSegment::getInfo(file_segment_metadata.second->fileSegment));
    }, userId);
}

FileCache::CacheIteratorPtr FileCache::getCacheIterator(const UserID & userId)
{
    return metadata.getIterator(userId);
}

void FileCache::removeKey(const Key & key, const UserID & userId)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */false, userId);
}

void FileCache::removeKeyIfExists(const Key & key, const UserID & userId)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */true, userId);
}

void FileCache::removeFileSegment(const Key & key, size_t offset, const UserID & userId)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, OriginInfo(userId));
    locked_key->removeFileSegment(offset);
}

void FileCache::removeFileSegmentIfExists(const Key & key, size_t offset, const UserID & userId)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(userId));
    if (locked_key)
        locked_key->removeFileSegmentIfExists(offset);
}

void FileCache::removePathIfExists(const std::string & path, const UserID & userId)
{
    removeKeyIfExists(Key::fromPath(path), userId);
}

void FileCache::removeAllReleasable(const UserID & userId)
{
    assertInitialized();
    assertCacheCorrectness();

    metadata.removeAllKeys(userId);
}

void FileCache::loadMetadata()
{
    // TODO(metric): CH observation removed.

    if (!metadata.isEmpty())
    {
        VELOX_FAIL("Cache initialization is partially made. "
            "This can be a result of a failed first attempt to initialize cache. "
            "Please, check log for error messages");
    }

    loadMetadataImpl();

    /// Shuffle file_segment_metadatas to have random order in LRUQueue
    /// as at startup all file_segment_metadatas have the same priority.
    mainPriority->shuffle(cacheGuard.writeLock());
}

void FileCache::loadMetadataImpl()
{
    auto parse_user = [&](const fs::path & path) -> std::optional<OriginInfo>
    {
        auto filename = path.filename().string();

        auto pos = filename.find_last_of('.');
        if (pos == std::string::npos)
            return std::nullopt;

        return OriginInfo(filename.substr(0, pos), parseUInt64(filename.substr(pos + 1)));
    };

    auto get_keys_dir_to_process_with_user_dir = [
        &,
        initialized = false,
        user_it = fs::directory_iterator{},
        origin = OriginInfo{},
        key_prefix_it = fs::directory_iterator{},
        get_key_mutex = std::mutex()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(get_key_mutex);
        while (true)
        {
            if (key_prefix_it == fs::directory_iterator{})
            {
                if (initialized)
                {
                    if (user_it == fs::directory_iterator{})
                    {
                        /// No more user directories.
                        return std::nullopt;
                    }

                    /// Go to next user.
                    ++user_it;
                }
                else
                {
                    user_it = fs::directory_iterator{metadata.getBaseDirectory()};
                    initialized = true;
                }

                if (user_it == fs::directory_iterator{})
                {
                    /// No more user directories.
                    return std::nullopt;
                }

                if (user_it->path().filename() == "status")
                    continue;

                key_prefix_it = fs::directory_iterator{user_it->path()};
                if (key_prefix_it == fs::directory_iterator())
                {
                    /// User directory is empty.
                    fs::remove(user_it->path());
                    continue;
                }

                auto parsed_result = parse_user(user_it->path());
                if (parsed_result.has_value())
                {
                    origin = parsed_result.value();
                }
                else
                {
                    LOG_WARNING(log, "Unexpected file format: {}", user_it->path().string());
                    continue;
                }
            }

            auto path = key_prefix_it->path();
            if (key_prefix_it->is_directory())
            {
                ++key_prefix_it;
                return std::pair{path, origin};
            }
            ++key_prefix_it;
        }
    };

    std::vector<FileSegmentKeyType> key_types_to_load{FileSegmentKeyType::Data, FileSegmentKeyType::System};
    if (!useSplitCache)
        key_types_to_load.push_back(FileSegmentKeyType::General);

    auto get_keys_dir_to_process = [
        &,
        key_prefix_it = fs::directory_iterator{},
        key_type_index = 0ull,
        origin = getCommonOrigin(),
        get_key_mutex = std::mutex()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(get_key_mutex);

        while (true)
        {
            if (key_prefix_it == fs::directory_iterator())
            {
                if (key_type_index == key_types_to_load.size())
                    return std::nullopt;

                auto type = key_types_to_load[key_type_index];
                auto dir_path = fs::path(metadata.getBaseDirectory()).append(getKeyTypePrefix(type));
                origin.segmentType = type;
                key_type_index++;

                if (!fs::exists(dir_path))
                    continue;
                key_prefix_it = fs::directory_iterator{dir_path};
                continue;
            }

            auto path = key_prefix_it->path();

            const std::string key_prefix_dir_name = path.filename().string();
            if (key_prefix_it->is_directory() &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::Data) &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::System)
            )
            {
                key_prefix_it++;
                return std::pair{path, origin};
            }

            if (!key_prefix_it->is_directory() && key_prefix_it->path().filename() != "status")
            {
                LOG_WARNING(log, "Unexpected file {} (not a directory), will skip it", path.string());
            }
            key_prefix_it++;
        }
    };

    const uint64_t num_listing_threads = std::max(uint64_t(1), loadMetadataThreads / 2);
    const uint64_t num_loading_threads = loadMetadataThreads - num_listing_threads;

    LOG_INFO(log, "Loading filesystem cache from {} using {} listing thread(s) and {} loading thread(s)",
             metadata.getBaseDirectory(), num_listing_threads, num_loading_threads);

    if (writeCachePerUserDirectory && useSplitCache)
        LOG_WARNING(log, "useSplitCache currently unsupported with writeCachePerUserDirectory. Will ignore useSplitCache.");

    /// Bounded queue of individual key directories fed by listing threads, drained by loading threads.
    /// Size 0 when there are no loading threads: tryPush always fails immediately,
    /// so listing threads load all keys directly.
    ConcurrentBoundedQueue<std::pair<fs::path, OriginInfo>> key_dirs_queue(num_loading_threads == 0 ? 0 : 1000);

    std::exception_ptr first_exception;
    std::mutex exception_mutex;
    /// Tracks how many listing threads are still running.
    std::atomic<uint64_t> listing_threads_remaining{num_listing_threads};

    auto handle_exception = [&]()
    {
        std::lock_guard lock(exception_mutex);
        if (!first_exception)
            first_exception = std::current_exception();
        stopLoadingMetadata = true;
        key_dirs_queue.finish();
    };

    /// Listing threads: each picks up key_prefix_dirs in parallel and enqueues individual key dirs.
    /// The last listing thread to finish calls finish() on the queue.
    std::vector<ThreadFromGlobalPool> listing_threads;
    for (uint64_t i = 0; i < num_listing_threads; ++i)
    {
        try
        {
            listing_threads.emplace_back([&]
            {
                while (!stopLoadingMetadata)
                {
                    try
                    {
                        std::optional<std::pair<fs::path, OriginInfo>> prefix_result;
                        if (writeCachePerUserDirectory)
                            prefix_result = get_keys_dir_to_process_with_user_dir();
                        else
                            prefix_result = get_keys_dir_to_process();

                        if (!prefix_result.has_value())
                            break;

                        const auto & [key_prefix_dir, origin] = prefix_result.value();

                        fs::directory_iterator key_it{key_prefix_dir};
                        if (key_it == fs::directory_iterator{})
                        {
                            LOG_DEBUG(log, "Removing empty key prefix directory: {}", key_prefix_dir.string());
                            fs::remove(key_prefix_dir);
                            continue;
                        }

                        for (; key_it != fs::directory_iterator(); ++key_it)
                        {
                            if (stopLoadingMetadata)
                                break;
                            if (!key_it->is_directory())
                            {
                                LOG_DEBUG(log, "Unexpected file: {} (not a directory). Expected a directory", key_it->path().string());
                                continue;
                            }
                            const auto key_dir_path = key_it->path();
                            /// tryPush returns false if the queue is full (timeout=0) or finish() was called.
                            /// Distinguish the two by checking stopLoadingMetadata.
                            /// If the queue is full, load directly instead of waiting.
                            if (!key_dirs_queue.tryPush({key_dir_path, origin}))
                            {
                                if (stopLoadingMetadata)
                                    break;
                                loadMetadataForKey(key_dir_path, origin);
                            }
                        }
                    }
                    catch (...)
                    {
                        handle_exception();
                    }
                }

                if (listing_threads_remaining.fetch_sub(1) == 1)
                    key_dirs_queue.finish();
            });
        }
        catch (...)
        {
            handle_exception();
            break;
        }
    }

    /// Loading threads: drain the queue and load individual key dirs.
    std::vector<ThreadFromGlobalPool> loading_threads;
    for (uint64_t i = 0; i < num_loading_threads; ++i)
    {
        try
        {
            loading_threads.emplace_back([&]
            {
                /// pop() blocks when the queue is empty until either a new item is pushed
                /// by a listing thread or finish() is called (after all listing threads exit).
                std::pair<fs::path, OriginInfo> item;
                while (key_dirs_queue.pop(item))
                {
                    if (stopLoadingMetadata)
                        return;
                    try
                    {
                        loadMetadataForKey(item.first, item.second);
                    }
                    catch (...)
                    {
                        handle_exception();
                        return;
                    }
                }
            });
        }
        catch (...)
        {
            handle_exception();
            break;
        }
    }

    for (auto & thread : listing_threads)
        if (thread.joinable())
            thread.join();
    for (auto & thread : loading_threads)
        if (thread.joinable())
            thread.join();

    if (first_exception)
        std::rethrow_exception(first_exception);

    mainPriority->check(cacheStateGuard.lock());

    assertCacheCorrectness();
}

void FileCache::loadMetadataForKey(const fs::path & key_directory, const OriginInfo & origin_info)
{
    if (fs::is_empty(key_directory))
    {
        LOG_DEBUG(log, "Removing empty key directory: {}", key_directory.string());
        fs::remove(key_directory);
        return;
    }

    const auto key = Key::fromKeyString(key_directory.filename().string());
    auto key_metadata = metadata.getKeyMetadata(
        key,
        CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY,
        origin_info,
        /* is_initial_load */true);

    /// Phase 1: scan and parse all segment files for this key (no lock held).
    struct SegmentToLoad
    {
        uint64_t offset;
        uint64_t size;
        FileSegmentKind kind;
        fs::path path;
        IFileCachePriority::IteratorPtr cache_it; /// filled in phase 2
    };
    std::vector<SegmentToLoad> segments;

    for (fs::directory_iterator offset_it{key_directory}; offset_it != fs::directory_iterator(); ++offset_it)
    {
        auto offset_with_suffix = offset_it->path().filename().string();
        bool parsed;
        uint64_t offset = 0;

        auto delim_pos = offset_with_suffix.find('_');
        if (delim_pos == std::string::npos)
        {
            parsed = tryParseUInt64(offset, offset_with_suffix);
        }
        else
        {
            parsed = tryParseUInt64(offset, offset_with_suffix.substr(0, delim_pos));

            if (offset_with_suffix.substr(delim_pos + 1) == "persistent")
            {
                /// For compatibility. Persistent files are no longer supported.
                fs::remove(offset_it->path());
                continue;
            }
            if (offset_with_suffix.substr(delim_pos + 1) == "temporary")
            {
                fs::remove(offset_it->path());
                continue;
            }
        }

        if (!parsed)
        {
            LOG_WARNING(log, "Unexpected file: {}", offset_it->path().string());
            continue;
        }

        auto size = offset_it->file_size();
        if (!size)
        {
            fs::remove(offset_it->path());
            continue;
        }

        segments.push_back({offset, size, FileSegmentKind::Regular, offset_it->path(), nullptr});
    }

    /// Phase 2: add all segments for the key under a single write lock acquisition.
    /// TODO: we can get rid of this lockCache() if we first load everything in parallel
    /// without any mutual lock between loading threads, and only after do removeOverflow().
    /// This will be better because overflow here may
    /// happen only if cache configuration changed and max_size became less than it was.
    size_t size_limit = 0;
    {
        auto lock = cacheGuard.writeLock();
        auto state_lock = cacheStateGuard.lock();
        size_limit = mainPriority->getSizeLimit(state_lock);

        for (auto & segment : segments)
        {
            if (mainPriority->canFit(
                    segment.size,
                    /* elements */1,
                    state_lock,
                    /* reservee */nullptr,
                    origin_info,
                    /* is_initial_load */true))
            {
                segment.cache_it = mainPriority->add(
                    key_metadata,
                    segment.offset,
                    segment.size,
                    lock,
                    &state_lock,
                    /* is_initial_load */true);
            }
        }
    }

    /// Phase 3: construct FileSegment objects and emplace
    /// (no lock held, because a single key is loaded by a single thread).
    size_t failed_to_fit = 0;
    for (auto & segment : segments)
    {
        if (segment.cache_it)
        {
            bool inserted = false;
            try
            {
                auto file_segment = std::make_shared<FileSegment>(
                    key,
                    segment.offset,
                    segment.size,
                    FileSegment::State::DOWNLOADED,
                    CreateFileSegmentSettings(segment.kind),
                    /* background_download_enabled */false,
                    this,
                    key_metadata,
                    segment.cache_it);

                inserted = key_metadata->emplaceUnlocked(segment.offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment))).second;
            }
            catch (...)
            {
                LOG(ERROR) << "Exception in " << __PRETTY_FUNCTION__;
                VELOX_DCHECK(false);
            }

            if (inserted)
            {
                LOG_TEST(log, "Added file segment {}:{} (size: {}) with path: {}", key, segment.offset, segment.size, segment.path.string());
            }
            else
            {
                segment.cache_it->remove(cacheGuard.writeLock());
                fs::remove(segment.path);
                VELOX_DCHECK(false);
            }
        }
        else
        {
            ++failed_to_fit;
            fs::remove(segment.path);
        }
    }

    if (failed_to_fit)
    {
        LOG_WARNING(
            log,
            "Cache capacity changed (max size: {}), "
            "{} file(s) for key {} do not fit in cache anymore",
            size_limit, failed_to_fit, key);
    }

    if (key_metadata->sizeUnlocked() == 0)
    {
        metadata.removeKey(key, /* if_exists */false, origin_info.userId);
    }
}

FileCache::~FileCache()
{
    // Join all in-flight download tasks (they capture FileCache/segment state)
    // before any cache members are torn down.
    downloadExecutor_.reset();
    deactivateBackgroundOperations();
    assertCacheCorrectness();
}

FileCache * & FileCache::instanceRef()
{
    static FileCache * instance = nullptr;
    return instance;
}

FileCache * FileCache::getInstance()
{
    return instanceRef();
}

void FileCache::setInstance(FileCache * instance)
{
    instanceRef() = instance;
}

void FileCache::deactivateBackgroundOperations()
{
    shutdown.store(true);

    stopLoadingMetadata = true;
    if (loadMetadataMainThread && loadMetadataMainThread->joinable())
        loadMetadataMainThread->join();

    metadata.shutdown();
    if (keepUpFreeSpaceRatioTask)
        keepUpFreeSpaceRatioTask->deactivate();
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const UserID & userId)
{
    assertInitialized();
    assertCacheCorrectness();

    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
            file_segments.push_back(FileSegment::getInfo(file_segment_metadata->fileSegment));
    }, userId);
    return file_segments;
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const Key & key, const UserID & userId)
{
    std::vector<FileSegment::Info> file_segments;
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL, OriginInfo(userId));
    for (const auto & [_, file_segment_metadata] : *locked_key)
        file_segments.push_back(FileSegment::getInfo(file_segment_metadata->fileSegment));
    return file_segments;
}

IFileCachePriority::PriorityDumpPtr FileCache::dumpQueue()
{
    assertInitialized();
    return mainPriority->dump(cacheGuard.readLock());
}

IFileCachePriority::Type FileCache::getEvictionPolicyType()
{
    assertInitialized();
    return mainPriority->getType();
}

std::unordered_map<std::string, FileCache::UsageStat> FileCache::getUsageStatPerClient()
{
    assertInitialized();
    return mainPriority->getUsageStatPerClient();
}

std::vector<std::string> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, getInternalOrigin());
    if (!locked_key)
        return {};

    std::vector<std::string> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key)
    {
        const auto & file_segment = *file_segment_metadata->fileSegment;
        if (file_segment.state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(locked_key->getKeyMetadata()->getFileSegmentPath(file_segment));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    /// We use this method for metrics, so it is ok to get approximate result.
    return mainPriority->getSizeApprox();
}

FileCacheStats FileCache::stats() const
{
    FileCacheStats result;
    result.hits = hits_.load(std::memory_order_relaxed);
    result.misses = misses_.load(std::memory_order_relaxed);
    result.downloadedBytes = downloadedBytes_.load(std::memory_order_relaxed);
    result.evictions = evictions_.load(std::memory_order_relaxed);
    result.bytesOnDisk = getUsedCacheSize();
    return result;
}

size_t FileCache::getMaxCacheSize() const
{
    return mainPriority->getSizeLimitApprox();
}

size_t FileCache::getFileSegmentsNum() const
{
    /// We use this method for metrics, so it is ok to get approximate result.
    return mainPriority->getElementsCountApprox();
}

void FileCache::assertCacheCorrectnessWithProbability()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    if (checkCacheProbability.doCheck())
    {
        assertCacheCorrectness();
    }
#endif
}

void FileCache::assertCacheCorrectness()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    LOG_TEST(log, "Checking cache correctness");
    // TODO(metric): CH observation removed.
    // TODO(metric): CH observation removed.

    metadata.iterate([&](LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
        {
            VELOX_DCHECK(file_segment_metadata->fileSegment->assertCorrectness());
        }
    }, getInternalOrigin().userId);

    FileCacheReserveStat stat;
    mainPriority->iterate([](LockedKey &, const FileSegmentMetadataPtr & file_segment_metadata)
    {
        VELOX_DCHECK(file_segment_metadata->fileSegment->assertCorrectness());
        return IFileCachePriority::IterationResult::CONTINUE;
    }, stat, cacheGuard.readLock());

    mainPriority->check(cacheStateGuard.lock());
#endif
}

void FileCache::applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings)
{
    if (!isInitialized_ || shutdown || fileCacheSettingsEqual(new_settings, actual_settings))
        return;

    std::lock_guard lock(applySettingsMutex);

    if (new_settings.backgroundDownloadQueueSizeLimit.value != actual_settings.backgroundDownloadQueueSizeLimit.value
        && metadata.setBackgroundDownloadQueueSizeLimit(new_settings.backgroundDownloadQueueSizeLimit.value))
    {
        LOG_INFO(log, "Changed background_download_queue_size from {} to {}",
                 actual_settings.backgroundDownloadQueueSizeLimit.value,
                 new_settings.backgroundDownloadQueueSizeLimit.value);

        actual_settings.backgroundDownloadQueueSizeLimit.value = new_settings.backgroundDownloadQueueSizeLimit.value;
    }

    if (new_settings.backgroundDownloadThreads.value != actual_settings.backgroundDownloadThreads.value)
    {
        bool updated = false;
        try
        {
            updated = metadata.setBackgroundDownloadThreads(new_settings.backgroundDownloadThreads.value);
        }
        catch (...)
        {
            actual_settings.backgroundDownloadThreads.value = metadata.getBackgroundDownloadThreads();
            throw;
        }

        if (updated)
        {
            LOG_INFO(log, "Changed background_download_threads from {} to {}",
                    actual_settings.backgroundDownloadThreads.value,
                    new_settings.backgroundDownloadThreads.value);

            actual_settings.backgroundDownloadThreads.value = new_settings.backgroundDownloadThreads.value;
        }
    }

    if (new_settings.backgroundDownloadMaxFileSegmentSize.value != actual_settings.backgroundDownloadMaxFileSegmentSize.value)
    {
        backgroundDownloadMaxFileSegmentSize = new_settings.backgroundDownloadMaxFileSegmentSize.value;

        LOG_INFO(log, "Changed backgroundDownloadMaxFileSegmentSize from {} to {}",
                actual_settings.backgroundDownloadMaxFileSegmentSize.value,
                new_settings.backgroundDownloadMaxFileSegmentSize.value);

        actual_settings.backgroundDownloadMaxFileSegmentSize.value = new_settings.backgroundDownloadMaxFileSegmentSize.value;
    }

    {
        SizeLimits desired_limits{
            .maxSize = new_settings.maxSize.value,
            .maxElements = new_settings.maxElements.value,
            .slruSizeRatio = new_settings.slruSizeRatio.value
        };
        SizeLimits current_limits{
            .maxSize = actual_settings.maxSize.value,
            .maxElements = actual_settings.maxElements.value,
            .slruSizeRatio = actual_settings.slruSizeRatio.value
        };

        const bool max_size_changed = desired_limits.maxSize != current_limits.maxSize;
        const bool max_elements_changed = desired_limits.maxElements != current_limits.maxElements;
        const bool slru_ratio_changed = desired_limits.slruSizeRatio != current_limits.slruSizeRatio;

        const bool do_dynamic_resize = (max_size_changed || max_elements_changed) && !slru_ratio_changed;

        if (allowDynamicCacheResize && do_dynamic_resize && !mainPriority->isOvercommitEviction())
        {
            auto result_limits = doDynamicResize(current_limits, desired_limits);

            actual_settings.maxSize.value = result_limits.maxSize;
            actual_settings.maxElements.value = result_limits.maxElements;
        }
        else if (do_dynamic_resize)
        {
            LOG_WARNING(
                log, "Filesystem cache size was modified, "
                "but dynamic cache resize is disabled, "
                "therefore cache size will not be changed without server restart. "
                "To enable dynamic cache resize, "
                "add `allowDynamicCacheResize` to cache configuration");
        }
        else
        {
            LOG_DEBUG(
                log, "Nothing to resize in filesystem cache. "
                "Current max size: {}, max_elements: {}, slru ratio: {}",
                current_limits.maxSize, current_limits.maxElements, current_limits.slruSizeRatio);
        }
    }

    if (new_settings.maxFileSegmentSize.value != actual_settings.maxFileSegmentSize.value)
    {
        maxFileSegmentSize = actual_settings.maxFileSegmentSize.value = new_settings.maxFileSegmentSize.value;
    }
}

FileCache::SizeLimits FileCache::doDynamicResize(const SizeLimits & prev_limits, const SizeLimits & desired_limits)
{
    if (prev_limits.slruSizeRatio != desired_limits.slruSizeRatio)
        VELOX_USER_FAIL("Dynamic resize of size ratio is not allowed");

    std::unique_lock resize_lock(dynamicResizeLock, std::defer_lock);
    if (!resize_lock.try_lock_for(std::chrono::milliseconds(dynamicResizeLockWaitMs)))
    {
        LOG_WARNING(log, "Dynamic resize skipped: could not acquire resize lock within {}ms",
                    dynamicResizeLockWaitMs);
        return prev_limits;
    }

    SizeLimits result_limits;
    bool modified_size_limit = false;
    {
        auto cache_lock = cacheStateGuard.lock();

        if (prev_limits.maxSize != mainPriority->getSizeLimit(cache_lock))
            VELOX_USER_FAIL("Current limits inconsistency in size");

        if (prev_limits.maxElements != mainPriority->getElementsLimit(cache_lock))
            VELOX_USER_FAIL("Current limits inconsistency in elements number");

        try
        {
            modified_size_limit = doDynamicResizeImpl(prev_limits, desired_limits, result_limits, cache_lock);
            VELOX_DCHECK(result_limits.maxSize && result_limits.maxElements);
        }
        catch (...)
        {
            LOG_ERROR(
                log, "Unexpected error during dynamic cache resize: {}",
                std::current_exception() ? "exception" : "");

            if (!cache_lock.owns_lock())
                cache_lock.lock();

            size_t max_size = mainPriority->getSizeLimit(cache_lock);
            size_t max_elements = mainPriority->getElementsLimit(cache_lock);

            if (result_limits.maxSize != max_size || result_limits.maxElements != max_elements)
            {
                LOG_DEBUG(
                    log, "Resetting max size from {} to {}, max elements from {} to {}",
                    result_limits.maxSize, max_size, result_limits.maxElements, max_elements);

                result_limits.maxSize = max_size;
                result_limits.maxElements = max_elements;
            }

            cache_lock.unlock();
            assertCacheCorrectness();
            throw;
        }
    }

    if (modified_size_limit)
    {
        LOG_INFO(log, "Changed max_size from {} to {}, max_elements from {} to {}",
                 prev_limits.maxSize, result_limits.maxSize,
                 prev_limits.maxElements, result_limits.maxElements);

        VELOX_DCHECK(result_limits.maxSize == desired_limits.maxSize);
        VELOX_DCHECK(result_limits.maxElements == desired_limits.maxElements);
    }
    else
    {
        LOG_WARNING(
            log, "Unable to modify size limit from {} to {}, elements limit from {} to {}. "
            "`max_size` and `max_elements` settings will remain inconsistent with config.xml. "
            "Next attempt to update them will happen on the next config reload. "
            "You can trigger it with SYSTEM RELOAD CONFIG.",
            prev_limits.maxSize, desired_limits.maxSize,
            prev_limits.maxElements, desired_limits.maxElements);
    }

    VELOX_DCHECK(mainPriority->getSizeApprox() <= result_limits.maxSize);
    VELOX_DCHECK(mainPriority->getElementsCountApprox() <= result_limits.maxElements);

    VELOX_DCHECK(mainPriority->getSizeLimit(cacheStateGuard.lock()) == result_limits.maxSize);
    VELOX_DCHECK(mainPriority->getElementsLimit(cacheStateGuard.lock()) == result_limits.maxElements);

    assertCacheCorrectness();
    return result_limits;
}

bool FileCache::doDynamicResizeImpl(
    const SizeLimits & prev_limits,
    const SizeLimits & desired_limits,
    SizeLimits & result_limits,
    CacheStateGuard::Lock & state_lock)
{
    /// In order to not block cache for the duration of cache resize,
    /// we do:
    /// a. Take a cache lock.
    ///     1. Collect eviction candidates,
    ///     2. If eviction candidates size is non-zero,
    ///        remove their queue entries.
    ///        This will release space we consider to be hold for them,
    ///        so that we can safely modify size limits.
    ///     3. Modify size limits of cache.
    /// b. Release a cache lock.
    ///     1. Do actual eviction from filesystem.

    auto eviction_info = mainPriority->collectEvictionInfoForResize(
        desired_limits.maxSize,
        desired_limits.maxElements,
        getInternalOrigin(),
        state_lock);

    VELOX_DCHECK(!eviction_info->hasHoldSpace());

    EvictionCandidates eviction_candidates;
    if (!eviction_info->requiresEviction())
    {
        /// Nothing needs to be evicted,
        /// just modify the limits and we are done.

        mainPriority->modifySizeLimits(
            desired_limits.maxSize,
            desired_limits.maxElements,
            desired_limits.slruSizeRatio,
            state_lock);

        result_limits = desired_limits;

        LOG_INFO(
            log, "Nothing needs to be evicted for new size limits ({})",
            mainPriority->getStateInfoForLog(state_lock));
        return true;
    }

    state_lock.unlock();

    FileCacheReserveStat stat;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    if (!mainPriority->collectCandidatesForEviction(
            *eviction_info,
            stat,
            eviction_candidates,
            invalidated_entries,
            /* reservee */nullptr,
            /* continue_from_last_eviction_pos */false,
            /* max_candidates_size */0,
            /* is_total_space_cleanup */true,
            getInternalOrigin(),
            cacheGuard,
            cacheStateGuard))
    {
        result_limits = prev_limits;
        LOG_INFO(log, "Dynamic cache resize is not possible at the moment");
        return false;
    }

    /// Remove only queue entries of eviction candidates.
    /// Hold the write lock until after modifying size limits,
    /// to prevent concurrent `tryIncreasePriority` from promoting entries
    /// into the protected queue between removal and limit modification.
    auto write_lock = cacheGuard.writeLock();
    eviction_candidates.removeQueueEntries(write_lock);

    /// Note that (in-memory) metadata about corresponding file segments
    /// (e.g. file segment info in CacheMetadata) will be removed
    /// only after eviction from filesystem. This is needed to avoid
    /// a race on removal of file from filesystsem and
    /// addition of the same file as part of a newly cached file segment.

    state_lock.lock();

    /// Modify cache size limits.
    /// From this point cache eviction will follow them.
    mainPriority->modifySizeLimits(
        desired_limits.maxSize,
        desired_limits.maxElements,
        desired_limits.slruSizeRatio,
        state_lock);

    state_lock.unlock();
    write_lock.unlock();

    /// Do actual eviction from filesystem.
    eviction_candidates.evict();
    evictions_.fetch_add(
        eviction_candidates.getNumEvicted(), std::memory_order_relaxed);

    VELOX_DCHECK(!eviction_candidates.requiresAfterEvictWrite());
    IFileCachePriority::removeEntries(invalidated_entries, cacheGuard.writeLock());

    auto failed_candidates = eviction_candidates.getFailedCandidates();
    if (failed_candidates.size() == 0)
    {
        LOG_INFO(
            log, "Successfully evicted {} cache elements needed for resize",
            eviction_candidates.size());

        result_limits = desired_limits;
        return true;
    }

    /// Restore to previous limits. Using prev_limits is safe because
    /// the entries existed under those limits before the resize attempt,
    /// so each sub-queue had enough room. Computing a tighter bound
    /// (desired + failed) would be incorrect for SLRU: when all failed
    /// entries belong to one sub-queue (e.g. protected with ratio 0.6),
    /// the total might not translate into enough per-sub-queue space
    /// after the ratio split.
    result_limits = prev_limits;

    LOG_INFO(
        log, "Having {} failed candidates with total size {}. "
        "Will set current limits as {} in size and {} in elements number",
        failed_candidates.totalCacheElements, failed_candidates.totalCacheSize,
        result_limits.maxSize, result_limits.maxElements);

    /// The below code corresponds to the case where
    /// we failed to execute eviction from filesystem.
    /// So here we need to restore removed queue entries to avoid broken cache state.
    /// As this case should be very rare (as it can only happen because of a bug)
    /// we allow ourselves to be suboptional here in favour of being robust
    /// and take two locks at the same time to do the restore in the most straightforward way.
    auto cache_write_lock = cacheGuard.writeLock();
    state_lock.lock();

    /// Increase the max size and max elements
    /// to the size and number of failed candidates.
    mainPriority->modifySizeLimits(
        result_limits.maxSize,
        result_limits.maxElements,
        result_limits.slruSizeRatio,
        state_lock);

    /// Add failed candidates back to queue.
    for (const auto & key_candidates_entry : failed_candidates.failedCandidatesPerKey)
    {
        const auto & key_metadata = key_candidates_entry.keyMetadata;
        const auto & key_candidates = key_candidates_entry.candidates;
        VELOX_DCHECK(!key_candidates.empty());

        auto locked_key = key_metadata->tryLock();
        if (!locked_key)
        {
            /// Key cannot be removed,
            /// because if we failed to remove something from it above,
            /// then we did not remove it from key metadata,
            /// so key lock must remain valid.
            LOG_ERROR(log, "Unexpected state: key {} does not exist", key_metadata->key);
            VELOX_DCHECK(false);
            continue;
        }

        for (const auto & candidate : key_candidates)
        {
            const auto & file_segment = candidate->fileSegment;
            /// Restore the original queue entry size. For partial segments it is reserved size.
            const auto restored_size = candidate->size();

            LOG_DEBUG(
                log, "Adding back file segment after failed eviction: {}:{}, restored size: {}, downloaded size: {}",
                file_segment->key(), file_segment->offset(), restored_size, file_segment->getDownloadedSize());

            auto original_queue_type = eviction_candidates.getOriginalQueueType(candidate.get());

            auto main_priority_iterator = mainPriority->addForRestore(
                key_metadata,
                file_segment->offset(),
                restored_size,
                original_queue_type,
                cache_write_lock,
                &state_lock);

            candidate->setRemovedFlag(*locked_key, /* value */false);
            file_segment->restoreQueueIteratorAfterDelayedRemoval(main_priority_iterator);
        }
    }

    return false;
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const std::string & /* query_id */, const FilesystemCacheSettings & /* cache_settings */)
{
    VELOX_NYI("TODO(query-context): FilesystemCacheSettings is still forward-declared in the port; wire query-limit settings before enabling this path");
}

std::vector<FileSegment::Info> FileCache::sync()
{
    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](LockedKey & locked_key)
    {
        auto broken = locked_key.sync();
        file_segments.insert(file_segments.end(), broken.begin(), broken.end());
    }, getInternalOrigin().userId);
    return file_segments;
}

} // namespace facebook::velox::ch
