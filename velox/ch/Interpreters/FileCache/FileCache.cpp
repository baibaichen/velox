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
#include "velox/ch/Interpreters/FileCache/FileCache.h"

#include "velox/ch/Common/ClickHouseAssert.h"
#include "velox/ch/Common/FileCacheBoundedQueue.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Interpreters/FileCache/EvictionCandidates.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/LRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SLRUFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/SplitFileCachePriority.h"

#include "velox/common/time/CpuWallTimer.h"

#include <folly/Random.h>
#include <folly/ScopeGuard.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/system/HardwareConcurrency.h>

#include <charconv>
#include <cmath>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;

namespace facebook::velox::ch
{

/// D11: CH `Stopwatch` maps to the wall-time snapshot of `DeltaCpuWallTimeStopWatch`.
using Stopwatch = facebook::velox::DeltaCpuWallTimeStopWatch;

namespace
{
/// CH `parse<UInt64>` / `tryParse<UInt64>` on a decimal filename component.
/// Uses `std::from_chars` (no locale, no allocation).
bool tryParseUInt64(uint64_t & out, std::string_view text)
{
    if (text.empty())
        return false;
    const char * first = text.data();
    const char * last = text.data() + text.size();
    auto [ptr, ec] = std::from_chars(first, last, out);
    return ec == std::errc() && ptr == last;
}

uint64_t parseUInt64(std::string_view text)
{
    uint64_t value = 0;
    if (!tryParseUInt64(value, text))
        throwFileCacheException("Cannot parse UInt64 from '{}'", std::string(text));
    return value;
}

double normalizeProbability(double probability)
{
    if (probability < 0.0)
        probability = .0;
    else if (probability > 1.0)
        probability = 1.0;
    return probability;
}

/// Reschedule delays (ms) and the state-lock acquire timeout for background free-space keeping.
constexpr size_t free_space_keeping_reschedule_ms = 5000;
constexpr size_t free_space_keeping_retry_reschedule_ms = 1000;
constexpr size_t free_space_keeping_state_lock_timeout_ms = 1000;

/// A batch handed to a remover: file segments to delete, plus zero-size queue entries to drop.
struct EvictionBatch
{
    EvictionCandidatesPtr candidates;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
};
using EvictionBatchPtr = std::shared_ptr<EvictionBatch>;

/// Key directory work item processed by the metadata-loading pipeline.
using KeyDirectoryWork = std::pair<fs::path, FileCacheOriginInfo>;
}

void FileCacheReserveStat::update(size_t size, FileSegmentKind kind, State state)
{
    auto & local_stat = getStatByKind(kind);
    switch (state)
    {
        case State::Releasable:
        {
            total_stat.releasable_size += size;
            ++total_stat.releasable_count;

            local_stat.releasable_size += size;
            ++local_stat.releasable_count;
            break;
        }
        case State::NonReleasable:
        {
            total_stat.non_releasable_size += size;
            ++total_stat.non_releasable_count;

            local_stat.non_releasable_size += size;
            ++local_stat.non_releasable_count;
            break;
        }
        case State::Evicting:
        {
            ++total_stat.evicting_count;
            ++local_stat.evicting_count;
            break;
        }
        case State::Moving:
        {
            ++total_stat.moving_count;
            ++local_stat.moving_count;
            break;
        }
        case State::Invalidated:
        {
            ++total_stat.invalidated_count;
            ++local_stat.invalidated_count;
            break;
        }
    }
}

std::string FileCacheReserveStat::Stat::toString() const
{
    return fmt::format(
        "releasable size: {}, releasable count: {}, "
        "non-releasable size: {}, non-releasable count: {}, "
        "evicting count: {}, moving count: {}, invalidated count: {}, "
        "candidates iteration steps: {}, clients iterated: {}",
        releasable_size, releasable_count,
        non_releasable_size, non_releasable_count,
        evicting_count, moving_count, invalidated_count,
        candidates_iteration_steps, clients_iterated);
}

FileCache::CheckCacheProbability::CheckCacheProbability(double probability, UInt64 seed_)
    : distribution(normalizeProbability(probability))
    , seed(seed_)
{
}

bool FileCache::CheckCacheProbability::doCheck()
{
    std::lock_guard lock(mutex);
    /// D-011-2: CH `pcg64_fast`/`randomSeed()` maps to `folly::Random`, drawn at the call site.
    const double sample = folly::Random::randDouble01();
    return sample < distribution.p();
}

FileCache::FileCache(
    const std::string & cache_name,
    const FileCacheSettings & settings,
    FileCacheWorkerPool & worker_pool_,
    FileCacheScheduler & scheduler_,
    OpenedFileCache & opened_file_cache_,
    filesystems::FileSystem & local_file_system_,
    const std::string & common_user_id_)
    : common_user_id(common_user_id_)
    , common_origin(common_user_id, 0, FileSegmentKeyType::General)
    /// D3: manager-owned runtime services, injected by reference (design 02:373-382). The worker
    /// pool backs every `FileCacheWorker` (metadata download/cleanup threads, the eviction pool,
    /// and the load-metadata helper threads); the scheduler replaces CH's `BackgroundSchedulePool`
    /// for the background maintenance tasks. `FileCache` no longer owns these.
    , worker_pool(worker_pool_)
    , scheduler(scheduler_)
    , opened_file_cache(opened_file_cache_)
    , local_file_system(local_file_system_)
    , max_file_segment_size(settings.maxFileSegmentSize)
    , bypass_cache_threshold(settings.enableBypassCacheWithThreshold ? settings.bypassCacheThreshold : 0)
    , boundary_alignment(settings.boundaryAlignment)
    , reserve_granularity(settings.reserveGranularity)
    , background_download_max_file_segment_size(settings.backgroundDownloadMaxFileSegmentSize)
    , load_metadata_threads(settings.loadMetadataThreads)
    , load_metadata_asynchronously(settings.loadMetadataAsynchronously)
    , write_cache_per_user_directory(settings.writeCachePerUserIdDirectory)
    , allow_dynamic_cache_resize(settings.allowDynamicCacheResize)
    , dynamic_resize_lock_wait_ms(settings.dynamicResizeLockWaitMs)
    , keep_current_size_to_max_ratio(1 - settings.keepFreeSpaceSizeRatio)
    , keep_current_elements_to_max_ratio(1 - settings.keepFreeSpaceElementsRatio)
    , keep_up_free_space_remove_batch(settings.keepFreeSpaceRemoveBatch)
    , keep_up_free_space_eviction_threads(settings.keepFreeSpaceEvictionThreads)
    , invalidated_entries_cleanup_threshold(settings.invalidatedEntriesCleanupThreshold)
    , invalidated_entries_cleanup_interval_ms(settings.invalidatedEntriesCleanupIntervalMs)
    , invalidated_entries_cleanup_remove_batch(settings.invalidatedEntriesCleanupRemoveBatch)
    , idle_client_ttl_sec(settings.idleClientTtlSec)
    , idle_client_check_interval_sec(settings.idleClientCheckIntervalSec)
    , idle_client_eviction_threads(settings.idleClientEvictionThreads)
    , use_split_cache(settings.useSplitCache)
    , split_cache_ratio(settings.splitCacheRatio)
    , skip_cache_on_disk_failure(settings.skipCacheOnDiskFailure)
    , expose_eviction_metrics(settings.exposePrometheusEvictionMetrics)
    , expose_eviction_metrics_per_user(settings.exposePrometheusEvictionMetricsPerUser)
    , name(cache_name)
    , log(getLogger("FileCache(" + cache_name + ")"))
    , metadata(
          settings.path,
          settings.backgroundDownloadQueueSizeLimit,
          settings.backgroundDownloadThreads,
          write_cache_per_user_directory,
          worker_pool,
          opened_file_cache,
          settings.reserveSpaceWaitLockTimeoutMilliseconds)
    , check_cache_probability(settings.checkCacheProbability)
{
    CachePriorityCreatorFunction creator_function;
    switch (settings.cachePolicy)
    {
        case FileCachePolicy::LRU:
        {
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, double /*size_ratio*/, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<LRUFileCachePriority>(queue_type, max_size, max_elements, description);
            };
            break;
        }
        case FileCachePolicy::SLRU:
        {
            creator_function = [](IFileCachePriority::QueueType queue_type, size_t max_size, size_t max_elements, double size_ratio, size_t /*overcommit_eviction_evict_step*/, String description) -> IFileCachePriorityPtr
            {
                return std::make_unique<SLRUFileCachePriority>(queue_type, max_size, max_elements, size_ratio, description);
            };
            break;
        }
        case FileCachePolicy::LRU_OVERCOMMIT:
        case FileCachePolicy::SLRU_OVERCOMMIT:
            /// R7: reject the overcommit policies explicitly (distributed-cache only; excluded here).
            throwFileCacheException("Overcommit cache policies are not supported");
    }

    if (use_split_cache)
    {
        if (settings.cachePolicy == FileCachePolicy::LRU_OVERCOMMIT
            || settings.cachePolicy == FileCachePolicy::SLRU_OVERCOMMIT)
            throwFileCacheException("`use_split_cache` is not supported with overcommit cache policies");

        main_priority = std::make_unique<SplitFileCachePriority>(
            IFileCachePriority::QueueType::Main,
            creator_function,
            settings.maxSize,
            settings.maxElements,
            settings.slruSizeRatio,
            settings.splitCacheRatio,
            cache_name);
    }
    else
    {
        main_priority = creator_function(
            IFileCachePriority::QueueType::Main,
            settings.maxSize,
            settings.maxElements,
            settings.slruSizeRatio,
            settings.overcommitEvictionEvictStep,
            cache_name);
    }

    LOG_DEBUG(log, "Using {} cache policy", static_cast<int>(settings.cachePolicy));

    main_priority->setOnEvictCallback([this](const FileSegment & segment, const String & user_id)
    {
        onSegmentEvicted(segment, user_id);
    });

    /// Idle-client eviction needs per-client usage, which only overcommit policies keep.
    const bool is_overcommit_policy =
        settings.cachePolicy == FileCachePolicy::LRU_OVERCOMMIT || settings.cachePolicy == FileCachePolicy::SLRU_OVERCOMMIT;
    client_tracking_possible = is_overcommit_policy && write_cache_per_user_directory;
    if (write_cache_per_user_directory && idle_client_ttl_sec.load() > 0 && !is_overcommit_policy)
        LOG_WARNING(log, "idle_client_ttl_sec is set but the cache policy does not track "
                         "per-client usage; idle-client eviction is disabled");

    if (settings.enableFilesystemQueryCacheLimit)
        query_limit = std::make_unique<FileCacheQueryLimit>();
}

const FileCache::OriginInfo & FileCache::getCommonOrigin() const
{
    /// B5: host-injected stable commonUserId (replaces CH's ServerUUID-derived static origin).
    return common_origin;
}

FileCache::OriginInfo FileCache::getCommonOriginWithSegmentKeyType(const fs::path & filename) const
{
    auto origin = getCommonOrigin();
    if (!use_split_cache)
        return origin;

    static const std::set<std::string> system_cache_type = {".txt", ".json", ".idx", ".cidx", ".dat"};
    origin.segment_type = system_cache_type.contains(filename.extension().string()) ? FileSegmentKeyType::System : FileSegmentKeyType::Data;
    return origin;
}

const FileCache::OriginInfo & FileCache::getInternalOrigin()
{
    static const OriginInfo origin("internal");
    return origin;
}

FileCache::~FileCache()
{
    deactivateBackgroundOperations();
    assertCacheCorrectness();
}

bool FileCache::isInitialized() const
{
    return is_initialized;
}

void FileCache::throwInitExceptionIfNeeded()
{
    if (load_metadata_asynchronously)
        return;

    std::lock_guard lock(init_mutex);
    if (init_exception)
        std::rethrow_exception(init_exception);
}

const String & FileCache::getBasePath() const
{
    return metadata.getBaseDirectory();
}

bool FileCache::skipCacheOnDiskFailure() const
{
    return skip_cache_on_disk_failure;
}

String FileCache::getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin, std::optional<size_t> size) const
{
    return metadata.getFileSegmentPath(key, offset, segment_kind, origin, size);
}

String FileCache::getKeyPath(const Key & key, const OriginInfo & origin) const
{
    return metadata.getKeyPath(key, origin);
}

void FileCache::assertInitialized() const
{
    if (is_initialized)
        return;

    std::unique_lock lock(init_mutex);
    if (is_initialized)
        return;

    if (init_exception)
        std::rethrow_exception(init_exception);
    if (!is_initialized)
        throwFileCacheException("Cache not initialized");
}

void FileCache::initialize()
{
    /// Prevent initialize() from running twice (two disks may share a path).
    /// D8: CH `callOnce` maps to `std::call_once` (retry-on-exception semantics preserved).
    std::call_once(initialize_called, [&]
    {
        bool need_to_load_metadata = fs::exists(getBasePath());
        try
        {
            if (!need_to_load_metadata)
                fs::create_directories(getBasePath());

            auto fs_info = std::filesystem::space(getBasePath());
            const size_t size_limit = main_priority->getSizeLimit(cache_state_guard.lock());
            if (fs_info.capacity < size_limit)
                throwFileCacheException(
                    "The total capacity of the disk containing cache path {} is less than the specified max_size {} bytes",
                    getBasePath(), std::to_string(size_limit));

            status_file = std::make_unique<StatusFile>(fs::path(getBasePath()) / "status", StatusFile::writeFullInfo());
        }
        catch (const std::filesystem::filesystem_error & e)
        {
            init_exception = std::current_exception();
            throwFileCacheException(
                "Failed to retrieve filesystem information for cache path {}. Error: {}",
                getBasePath(), std::string(e.what()));
        }
        catch (...)
        {
            init_exception = std::current_exception();
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }

        if (load_metadata_asynchronously)
        {
            load_metadata_main_thread = std::make_unique<ThreadFromGlobalPool>(
                worker_pool, [this, need_to_load_metadata] { initializeImpl(need_to_load_metadata); });
        }
        else
        {
            initializeImpl(need_to_load_metadata);
        }
    });
}

void FileCache::initializeImpl(bool load_metadata)
{
    std::lock_guard lock(init_mutex);

    if (is_initialized)
        return;

    try
    {
        /// Record per-client accesses for idle-client eviction. Installed whenever
        /// tracking is possible (not gated on the TTL), so the TTL can be toggled live.
        if (client_tracking_possible)
            metadata.setClientAccessCallback([this](const std::string & user_id) { main_priority->touchClientAccess(user_id); });

        /// The single maintenance task (invalidated-entries cleanup + idle eviction).
        /// B6: created on the owned FileCacheScheduler instead of CH's global BackgroundSchedulePool.
        /// Publish the wake notifier before loadMetadata can invalidate entries.
        background_cleanup_task = scheduler.createTask(
            "FileCache:" + name + ":background-cleanup", [this] { backgroundCleanupTaskFunc(); });
        main_priority->setInvalidateNotifier(
            invalidated_entries_cleanup_threshold, [this] { background_cleanup_task->schedule(); });
        background_cleanup_task->scheduleAfter(backgroundCleanupIntervalMs());

        if (load_metadata)
            loadMetadata();

        metadata.startup();
    }
    catch (...)
    {
        /// The cleanup task may already be scheduled; stop it so a retried
        /// initialization does not leave an orphaned task rescheduling on `this`.
        if (background_cleanup_task)
            background_cleanup_task->deactivate();
        init_exception = std::current_exception();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    if (keep_current_size_to_max_ratio != 1 || keep_current_elements_to_max_ratio != 1)
    {
        /// The eviction pool uses the velox FileCacheThreadPool ctor (worker pool + max threads +
        /// queue size); CH's CurrentMetrics thread enumerators are dropped (no-op metrics).
        eviction_pool = std::make_unique<ThreadPool>(
            worker_pool,
            /* max_threads */ keep_up_free_space_eviction_threads,
            /* queue_size */ keep_up_free_space_eviction_threads);

        keep_up_free_space_ratio_task = scheduler.createTask(
            "FileCache:" + name + ":free-space", [this] { freeSpaceRatioKeepingThreadFunc(); });
        keep_up_free_space_ratio_task->schedule();
    }

    is_initialized = true;
    LOG_TEST(log, "Initialized cache from {}", metadata.getBaseDirectory());
}

CachePriorityGuard::WriteLock FileCache::lockCache() const
{
    return cache_guard.writeLock();
}

FileSegments FileCache::getImpl(
    const LockedKey & locked_key, const FileSegment::Range & range, size_t file_segments_limit, bool ignore_bypass_threshold) const
{
    /// Given range = [left, right] and non-overlapping ordered set of file segments,
    /// find list [segment1, ..., segmentN] of segments which intersect with given range.

    if (!ignore_bypass_threshold && bypass_cache_threshold && range.size() > bypass_cache_threshold)
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

        const bool evicting_or_removed = file_segment_metadata.isEvictingOrRemoved(locked_key);

        FileSegmentPtr file_segment;
        if (evicting_or_removed)
        {
            file_segment = std::make_shared<FileSegment>(
                locked_key.getKey(),
                file_segment_metadata.file_segment->offset(),
                file_segment_metadata.file_segment->range().size(),
                FileSegment::State::DETACHED);
        }
        else
        {
            file_segment = file_segment_metadata.file_segment;
        }

        result.push_back(file_segment);
        return true;
    };

    const auto & file_segments = locked_key;
    auto segment_it = file_segments.lower_bound(range.left);
    if (segment_it == file_segments.end())
    {
        const auto & file_segment_metadata = *file_segments.rbegin()->second;
        if (file_segment_metadata.file_segment->range().right < range.left)
            return {};

        if (!add_to_result(file_segment_metadata))
            return result;
    }
    else
    {
        if (segment_it != file_segments.begin())
        {
            const auto & prev_file_segment_metadata = *std::prev(segment_it)->second;
            const auto & prev_range = prev_file_segment_metadata.file_segment->range();

            if (range.left <= prev_range.right)
            {
                if (!add_to_result(prev_file_segment_metadata))
                    return result;
            }
        }

        while (segment_it != file_segments.end())
        {
            const auto & file_segment_metadata = *segment_it->second;
            if (range.right < file_segment_metadata.file_segment->range().left)
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
    chassert(size > 0);
    chassert(size <= aligned_size);

    std::vector<FileSegment::Range> ranges;

    size_t current_pos = offset;
    size_t end_pos_non_included = offset + size;
    size_t remaining_size = aligned_size;

    const size_t max_size = max_file_segment_size.load();
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
        result.push_back(metadata_it->second->file_segment);
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
    chassert(!file_segments.empty());

    auto it = file_segments.begin();
    size_t processed_count = 0;
    auto segment_range = (*it)->range();

    size_t current_pos = 0;
    if (segment_range.left < range.left)
    {
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

        chassert(current_pos < segment_range.left);

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
        chassert(file_segments.size() >= file_segments_limit);
        file_segments.erase(it, file_segments.end());
        chassert(file_segments.size() == file_segments_limit);
    };

    if (is_limit_reached())
    {
        erase_unprocessed();
        return;
    }

    chassert(!file_segments_limit || file_segments.size() < file_segments_limit);

    if (current_pos <= non_aligned_right_offset)
    {
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

KeyMetadata::iterator FileCache::addFileSegment(
    LockedKey & locked_key,
    size_t offset,
    size_t size,
    FileSegment::State state,
    const CreateFileSegmentSettings & create_settings)
{
    chassert(size > 0); /// Empty file segments in cache are not allowed.

    const auto & key = locked_key.getKey();
    const FileSegment::Range range(offset, offset + size - 1);

    if (auto intersecting_range = locked_key.hasIntersectingRange(range))
    {
        throwFileCacheException(
            "Attempt to add intersecting file segment in cache ({} intersects {})",
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
        throwFileCacheException("Failed to insert {}:{}: entry already exists", key.toString(), offset);

    return file_segment_metadata_it;
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

    /// The bypass-threshold shortcut is a read-time optimization; on a write ignore it.
    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0, /* ignore_bypass_threshold */true);
    if (!file_segments.empty())
        return nullptr;

    if (create_settings.unbounded)
    {
        auto file_segment_metadata_it = addFileSegment(
            *locked_key, offset, size, FileSegment::State::EMPTY, create_settings);
        file_segments = {file_segment_metadata_it->second->file_segment};
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

    throwFileCacheException("Having intersection with already existing cache");
}

FileSegmentsHolderPtr FileCache::getOrSet(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_size,
    const CreateFileSegmentSettings & create_settings,
    size_t file_segments_limit,
    const OriginInfo & origin_info,
    std::optional<size_t> boundary_alignment_)
{
    assertInitialized();

    size_t initial_range_right_offset = (file_size ? std::min(offset + size, file_size) : offset + size) - 1;
    FileSegment::Range initial_range(offset, initial_range_right_offset);
    FileSegment::Range result_range = initial_range;

    const size_t alignment = boundary_alignment_.value_or(boundary_alignment);
    const auto aligned_offset = FileCacheUtils::roundDownToMultiple(initial_range.left, alignment);
    auto aligned_end_offset = (file_size
        ? std::min(FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment), file_size)
        : FileCacheUtils::roundUpToMultiple(initial_range.right + 1, alignment)) - 1;

    chassert(aligned_offset <= initial_range.left);
    chassert(aligned_end_offset >= initial_range.right);

    auto locked_key = metadata.lockKeyMetadata(
        key, CacheMetadata::KeyNotFoundPolicy::CREATE_EMPTY, origin_info);

    auto file_segments = getImpl(*locked_key, initial_range, file_segments_limit);

    if (file_segments_limit)
    {
        chassert(file_segments.size() <= file_segments_limit);
        if (file_segments.size() == file_segments_limit)
            result_range.right = aligned_end_offset = file_segments.back()->range().right;
    }

    const bool has_uncovered_prefix = file_segments.empty() || result_range.left < file_segments.front()->range().left;

    if (aligned_offset < result_range.left && has_uncovered_prefix)
    {
        auto prefix_range = FileSegment::Range(
            aligned_offset,
            file_segments.empty() ? result_range.left - 1 : file_segments.front()->range().left - 1);

        auto prefix_file_segments = getImpl(*locked_key, prefix_range, /* file_segments_limit */0);

        if (prefix_file_segments.empty())
        {
            result_range.left = aligned_offset;
        }
        else
        {
            chassert(prefix_file_segments.back()->range().right < result_range.left);
            chassert(prefix_file_segments.back()->range().right >= aligned_offset);

            result_range.left = prefix_file_segments.back()->range().right + 1;
        }
    }

    const bool has_uncovered_suffix = file_segments.empty() || file_segments.back()->range().right < result_range.right;

    if (result_range.right < aligned_end_offset && has_uncovered_suffix)
    {
        auto suffix_range = FileSegment::Range(result_range.right, aligned_end_offset);
        auto suffix_file_segments = getImpl(*locked_key, suffix_range, /* file_segments_limit */1);

        if (suffix_file_segments.empty())
        {
            result_range.right = aligned_end_offset;
        }
        else
        {
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
        chassert(file_segments.front()->range().right >= result_range.left);
        chassert(file_segments.back()->range().left <= result_range.right);

        fillHolesWithEmptyFileSegments(
            *locked_key, file_segments, result_range, offset + size - 1, file_segments_limit, /* fill_with_detached */false, create_settings);

        if (!file_segments.front()->range().contains(result_range.left))
        {
            throwFileCacheException(
                "Expected {} to include {} (end offset: {}, aligned offset: {}, aligned end offset: {})",
                file_segments.front()->range().toString(), offset,
                result_range.right, aligned_offset, aligned_end_offset);
        }
    }

    chassert(file_segments_limit
             ? file_segments.back()->range().left <= initial_range.right
             : file_segments.back()->range().contains(initial_range.right));

    chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);

    locked_key.reset();
    assertCacheCorrectnessWithProbability();
    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

FileSegmentsHolderPtr FileCache::get(
    const Key & key,
    size_t offset,
    size_t size,
    size_t file_segments_limit,
    const UserID & user_id)
{
    assertInitialized();

    std::unique_ptr<FileSegmentsHolder> holder;
    if (auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
        locked_key != nullptr)
    {
        FileSegment::Range range(offset, offset + size - 1);

        auto file_segments = getImpl(*locked_key, range, file_segments_limit);
        if (!file_segments.empty())
        {
            if (file_segments_limit)
            {
                chassert(file_segments.size() <= file_segments_limit);
                if (file_segments.size() == file_segments_limit)
                    range.right = file_segments.back()->range().right;
            }

            fillHolesWithEmptyFileSegments(
                *locked_key, file_segments, range, offset + size - 1, file_segments_limit, /* fill_with_detached */true, CreateFileSegmentSettings{});

            chassert(!file_segments_limit || file_segments.size() <= file_segments_limit);
            holder = std::make_unique<FileSegmentsHolder>(std::move(file_segments));
        }
    }

    if (!holder)
        holder = std::make_unique<FileSegmentsHolder>(FileSegments{std::make_shared<FileSegment>(key, offset, size, FileSegment::State::DETACHED)});

    assertCacheCorrectnessWithProbability();
    return holder;
}

FileSegmentsHolderPtr FileCache::getDownloadedContiguousOrEmpty(
    const Key & key,
    size_t offset,
    size_t size,
    const UserID & user_id)
{
    assertInitialized();
    chassert(size);

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
    if (!locked_key)
        return std::make_unique<FileSegmentsHolder>();

    const FileSegment::Range range(offset, offset + size - 1);
    auto file_segments = getImpl(*locked_key, range, /* file_segments_limit */0, /* ignore_bypass_threshold */true);

    if (file_segments.empty()
        || file_segments.front()->range().left > offset
        || file_segments.back()->range().right < range.right)
        return std::make_unique<FileSegmentsHolder>();

    size_t expected_left = file_segments.front()->range().left;
    for (const auto & file_segment : file_segments)
    {
        if (file_segment->range().left != expected_left)
            return std::make_unique<FileSegmentsHolder>();

        if (file_segment->getCurrentWriteOffset() < std::min(file_segment->range().right + 1, offset + size))
            return std::make_unique<FileSegmentsHolder>();

        expected_left = file_segment->range().right + 1;
    }

    return std::make_unique<FileSegmentsHolder>(std::move(file_segments));
}

bool FileCache::tryIncreasePriority(FileSegment & file_segment)
{
    std::shared_lock lock(dynamic_resize_lock, std::try_to_lock);
    /// Skip priority increase if cache resize is currently in progress.
    if (!lock.owns_lock())
        return false;
    return main_priority->tryIncreasePriority(
        *file_segment.getQueueIterator(), file_segment.isCompleted(), cache_guard, cache_state_guard);
}

bool FileCache::tryReserve(
    FileSegment & file_segment,
    size_t size,
    FileCacheReserveStat & reserve_stat,
    const OriginInfo & origin_info,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason)
{
    assertInitialized();

    /// Skip space reservation if dynamic cache resize is currently in progress.
    std::shared_lock resize_shared_lock(dynamic_resize_lock, std::try_to_lock);
    if (!resize_shared_lock.owns_lock())
    {
        failure_reason = "cache is being resized";
        return false;
    }

    cache_reserve_active_threads.fetch_add(1, std::memory_order_relaxed);
    SCOPE_EXIT
    {
        cache_reserve_active_threads.fetch_sub(1, std::memory_order_relaxed);
    };

    LOG_TEST(log, "Trying to reserve space ({} bytes) for {}:{}", size, file_segment.key().toString(), file_segment.offset());

    const bool success = doTryReserve(
        file_segment, size, reserve_stat, origin_info, lock_wait_timeout_milliseconds, failure_reason);
    return success;
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
#ifndef NDEBUG
    if (main_priority_iterator)
        chassert(file_segment.getReservedSize() > 0);
    else
        chassert(file_segment.getReservedSize() == 0);
#endif

    Priority * query_priority = nullptr;
    FileCacheQueryLimit::QueryContextPtr query_context;

    std::unique_ptr<EvictionInfo> main_eviction_info;
    std::unique_ptr<EvictionInfo> query_eviction_info;

    {
        size_t required_elements_num = main_priority_iterator ? 0 : 1;

        auto lock = cache_state_guard.lock();

        if (query_limit)
        {
            query_context = query_limit->tryGetQueryContext(lock);
            if (query_context)
            {
                query_priority = &query_context->getPriority();
                if (!query_priority->canFit(size, required_elements_num, lock, /* reservee */nullptr, origin_info)
                    && !query_context->recacheOnFileCacheQueryLimitExceeded())
                {
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

        main_eviction_info = main_priority->collectEvictionInfo(
            size,
            required_elements_num,
            main_priority_iterator.get(),
            /* is_total_space_cleanup */false,
            origin_info,
            lock);

        /// Fast path: increment the existing entry's size and quit.
        if (main_priority_iterator && !main_eviction_info->requiresEviction() && !query_context)
        {
            main_eviction_info->releaseHoldSpace(lock);
            main_priority_iterator->incrementSize(size, lock);

            file_segment.reserved_size += size;
            chassert(file_segment.reserved_size == main_priority_iterator->getEntry()->size);
            return true;
        }
    }

    EvictionCandidates eviction_candidates(main_priority->getOnEvictCallback());
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;

    if (!doEviction(
        *main_eviction_info, query_eviction_info.get(), file_segment, origin_info,
        main_priority_iterator, reserve_stat, eviction_candidates,
        invalidated_entries, query_priority, failure_reason))
    {
        chassert(!failure_reason.empty());
        return false;
    }

    bool added_new_main_entry = !main_priority_iterator;
    Priority::IteratorPtr query_priority_iterator;

    if (!main_priority_iterator || eviction_candidates.requiresAfterEvictWrite())
    {
        auto lock = cache_guard.writeLock();
        eviction_candidates.afterEvictWrite(lock);
        IFileCachePriority::removeEntries(invalidated_entries, lock);

        if (!main_priority_iterator)
        {
            main_priority_iterator = main_priority->add(
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
        if (auto lock = cache_guard.tryWriteLock(); lock.owns_lock())
            IFileCachePriority::removeEntries(invalidated_entries, lock);
    }

    try
    {
        auto lock = cache_state_guard.lock();
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
        /// Protect against zombie queue entries.
        if (main_priority_iterator && added_new_main_entry)
            main_priority_iterator->invalidate();

        throw;
    }

    if (added_new_main_entry)
        file_segment.setQueueIterator(main_priority_iterator);

    file_segment.reserved_size += size;
    chassert(file_segment.reserved_size == main_priority_iterator->getEntry()->size);

    if (auto ec = file_segment.getKeyMetadata()->createBaseDirectory(); ec)
    {
        failure_reason = "Failed to create base directory for key, error: " + ec.message();
        return false;
    }

    return true;
}

bool FileCache::doEviction(
    EvictionInfo & main_eviction_info,
    EvictionInfo * query_eviction_info,
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

    {
        auto on_cannot_evict_enough_space_message = [&](const IFileCachePriority & priority)
        {
            const auto & stat = reserve_stat.total_stat;
            return fmt::format(
                "cannot evict enough space "
                "(stat: {}, total size: {}/{}, total elements: {}/{})",
                stat.toString(),
                priority.getSizeApprox(), priority.getSizeLimitApprox(),
                priority.getElementsCountApprox(), priority.getElementsLimitApprox());
        };

        const bool resume_reserve_cursor = cache_reserve_active_threads.load(std::memory_order_relaxed) > 1;
        const auto eviction_cursor = resume_reserve_cursor
            ? IFileCachePriority::EvictionCursor::Reserve
            : IFileCachePriority::EvictionCursor::FromHead;
        if (!resume_reserve_cursor)
            main_priority->resetEvictionPos(IFileCachePriority::EvictionCursor::Reserve);

        if (query_eviction_info && query_eviction_info->requiresEviction())
        {
            chassert(query_priority);
            if (!query_priority->collectCandidatesForEviction(
                    *query_eviction_info,
                    reserve_stat,
                    eviction_candidates,
                    invalidated_entries,
                    /* reservee */{},
                    eviction_cursor,
                    /* max_candidates_size */0,
                    /* is_total_space_cleanup */false,
                    origin_info,
                    cache_guard,
                    cache_state_guard))
            {
                failure_reason = on_cannot_evict_enough_space_message(*query_priority);
                return false;
            }

            LOG_TEST(log, "Query limits satisfied (while reserving for {}:{})",
                     file_segment.key().toString(), file_segment.offset());
        }

        if (!main_priority->collectCandidatesForEviction(
                main_eviction_info,
                reserve_stat,
                eviction_candidates,
                invalidated_entries,
                main_priority_iterator,
                eviction_cursor,
                /* max_candidates_size */0,
                /* is_total_space_cleanup */false,
                origin_info,
                cache_guard,
                cache_state_guard))
        {
            failure_reason = on_cannot_evict_enough_space_message(*main_priority);
            return false;
        }
    }

    /// Remove eviction candidates from filesystem (no lock held).
    if (eviction_candidates.size() > 0)
    {
        auto on_failed_evict = [&]()
        {
            {
                auto lock = cache_guard.writeLock();
                eviction_candidates.afterEvictWrite(lock);
                IFileCachePriority::removeEntries(invalidated_entries, lock);
            }
            eviction_candidates.afterEvictState(cache_state_guard.lock());
        };
        try
        {
            eviction_candidates.evict();
        }
        catch (...)
        {
            on_failed_evict();
            throw;
        }

        const auto & failed_candidates = eviction_candidates.getFailedCandidates();
        if (failed_candidates.size() > 0)
        {
            on_failed_evict();
            throwFileCacheException(
                "Failed to evict {} file segments (first error: {})",
                failed_candidates.size(), failed_candidates.getFirstErrorMessage());
        }
    }
    return true;
}

void FileCache::iterate(IterateFunc && func, const UserID & user_id)
{
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & file_segment_metadata : locked_key)
            func(FileSegment::getInfo(file_segment_metadata.second->file_segment));
    }, user_id);
}

FileCache::CacheIteratorPtr FileCache::getCacheIterator(const UserID & user_id)
{
    return metadata.getIterator(user_id);
}

void FileCache::removeKey(const Key & key, const UserID & user_id)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */false, user_id);
}

void FileCache::removeKeyIfExists(const Key & key, const UserID & user_id)
{
    assertInitialized();
    metadata.removeKey(key, /* if_exists */true, user_id);
}

void FileCache::removeFileSegment(const Key & key, size_t offset, const UserID & user_id)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW, OriginInfo(user_id));
    locked_key->removeFileSegment(offset);
}

void FileCache::removeFileSegmentIfExists(const Key & key, size_t offset, const UserID & user_id)
{
    assertInitialized();
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, OriginInfo(user_id));
    if (locked_key)
        locked_key->removeFileSegmentIfExists(offset);
}

void FileCache::removePathIfExists(const String & path, const UserID & user_id)
{
    removeKeyIfExists(Key::fromPath(path), user_id);
}

void FileCache::removeAllReleasable(const UserID & user_id)
{
    assertInitialized();
    assertCacheCorrectness();

    metadata.removeAllKeys(user_id);
}

void FileCache::loadMetadata()
{
    if (!metadata.isEmpty())
    {
        throwFileCacheException(
            "Cache initialization is partially made. "
            "This can be a result of a failed first attempt to initialize cache. "
            "Please, check log for error messages");
    }

    loadMetadataImpl();

    /// Shuffle file_segment_metadatas to have random order in LRUQueue.
    main_priority->shuffle(cache_guard.writeLock());
}

void FileCache::loadMetadataImpl()
{
    auto parse_user = [&](const fs::path & path) -> std::optional<OriginInfo>
    {
        auto filename = path.filename().string();

        auto pos = filename.find_last_of('.');
        if (pos == std::string::npos)
            return std::nullopt;

        uint64_t weight = 0;
        if (!tryParseUInt64(weight, filename.substr(pos + 1)))
            return std::nullopt;
        return OriginInfo(filename.substr(0, pos), weight);
    };

    auto get_keys_dir_to_process_with_user_dir = [
        &,
        initialized = false,
        user_it = fs::directory_iterator{},
        origin = OriginInfo{},
        key_prefix_it = fs::directory_iterator{},
        get_key_mutex = std::make_shared<std::mutex>()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(*get_key_mutex);
        while (true)
        {
            if (key_prefix_it == fs::directory_iterator{})
            {
                if (initialized)
                {
                    if (user_it == fs::directory_iterator{})
                        return std::nullopt;
                    ++user_it;
                }
                else
                {
                    user_it = fs::directory_iterator{metadata.getBaseDirectory()};
                    initialized = true;
                }

                if (user_it == fs::directory_iterator{})
                    return std::nullopt;

                if (user_it->path().filename() == "status")
                    continue;

                key_prefix_it = fs::directory_iterator{user_it->path()};
                if (key_prefix_it == fs::directory_iterator())
                {
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
    if (!use_split_cache)
        key_types_to_load.push_back(FileSegmentKeyType::General);

    auto get_keys_dir_to_process = [
        &,
        key_prefix_it = fs::directory_iterator{},
        key_type_index = 0ull,
        origin = getCommonOrigin(),
        get_key_mutex = std::make_shared<std::mutex>()]
        () mutable -> std::optional<std::pair<fs::path, OriginInfo>>
    {
        std::lock_guard lk(*get_key_mutex);

        while (true)
        {
            if (key_prefix_it == fs::directory_iterator())
            {
                if (key_type_index == key_types_to_load.size())
                    return std::nullopt;

                auto type = key_types_to_load[key_type_index];
                auto dir_path = fs::path(metadata.getBaseDirectory()).append(getKeyTypePrefix(type));
                origin.segment_type = type;
                key_type_index++;

                if (!fs::exists(dir_path))
                    continue;
                key_prefix_it = fs::directory_iterator{dir_path};
                continue;
            }

            auto path = key_prefix_it->path();

            const std::string key_prefix_dir_name = path.filename();
            if (key_prefix_it->is_directory() &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::Data) &&
                key_prefix_dir_name != getKeyTypePrefix(FileSegmentKeyType::System))
            {
                key_prefix_it++;
                return std::pair{path, origin};
            }

            if (!key_prefix_it->is_directory() && key_prefix_it->path().filename() != "status")
                LOG_WARNING(log, "Unexpected file {} (not a directory), will skip it", path.string());
            key_prefix_it++;
        }
    };

    const UInt64 num_listing_threads = std::max(UInt64(1), load_metadata_threads / 2);
    const UInt64 num_loading_threads = load_metadata_threads - num_listing_threads;

    LOG_INFO(log, "Loading filesystem cache from {} using {} listing thread(s) and {} loading thread(s)",
             metadata.getBaseDirectory(), num_listing_threads, num_loading_threads);

    if (write_cache_per_user_directory && use_split_cache)
        LOG_WARNING(log, "use_split_cache currently unsupported with write_cache_per_user_directory. Will ignore use_split_cache.");

    /// Bounded queue of individual key directories fed by listing threads, drained by loading threads.
    /// Capacity 1000 when there are loading threads; 0 disables buffering (listing threads load directly).
    FileCacheBoundedQueue<KeyDirectoryWork> key_dirs_queue(num_loading_threads == 0 ? 0 : 1000);

    std::exception_ptr first_exception;
    std::mutex exception_mutex;
    std::atomic<UInt64> listing_threads_remaining{num_listing_threads};

    auto handle_exception = [&]()
    {
        std::lock_guard lock(exception_mutex);
        if (!first_exception)
            first_exception = std::current_exception();
        stop_loading_metadata = true;
        key_dirs_queue.finish();
    };

    auto drain_queue = [&]()
    {
        KeyDirectoryWork item;
        while (key_dirs_queue.pop(item))
        {
            if (stop_loading_metadata)
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
    };

    std::vector<ThreadFromGlobalPool> listing_threads;
    for (UInt64 i = 0; i < num_listing_threads; ++i)
    {
        try
        {
            listing_threads.emplace_back(worker_pool, [&]
            {
                while (!stop_loading_metadata)
                {
                    try
                    {
                        std::optional<KeyDirectoryWork> prefix_result;
                        if (write_cache_per_user_directory)
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
                            if (stop_loading_metadata)
                                break;
                            if (!key_it->is_directory())
                            {
                                LOG_DEBUG(log, "Unexpected file: {} (not a directory). Expected a directory", key_it->path().string());
                                continue;
                            }
                            const auto key_dir_path = key_it->path();
                            /// tryPush fails if the queue is full (timeout=0) or finish() was called.
                            /// Distinguish via stop_loading_metadata; if full, load directly.
                            if (!key_dirs_queue.tryPush(KeyDirectoryWork{key_dir_path, origin}))
                            {
                                if (stop_loading_metadata)
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

                drain_queue();
            });
        }
        catch (...)
        {
            handle_exception();
            break;
        }
    }

    std::vector<ThreadFromGlobalPool> loading_threads;
    for (UInt64 i = 0; i < num_loading_threads; ++i)
    {
        try
        {
            loading_threads.emplace_back(worker_pool, [&] { drain_queue(); });
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

    main_priority->check(cache_state_guard.lock());

    assertCacheCorrectness();
}

void FileCache::loadMetadataForKey(const fs::path & key_directory, const OriginInfo & origin_info)
{
    fs::directory_iterator offset_it{key_directory};
    if (offset_it == fs::directory_iterator())
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

    struct SegmentToLoad
    {
        UInt64 offset;
        UInt64 size;
        FileSegmentKind kind;
        fs::path path;
        IFileCachePriority::IteratorPtr cache_it;
        bool size_in_filename;
    };
    std::vector<SegmentToLoad> segments;
    std::unordered_map<UInt64, size_t> offset_to_index;

    for (; offset_it != fs::directory_iterator(); ++offset_it)
    {
        if (!offset_it->is_regular_file())
        {
            LOG_WARNING(log, "Unexpected non-regular entry in cache directory: {}", offset_it->path().string());
            continue;
        }

        auto offset_with_suffix = offset_it->path().filename().string();
        bool parsed = false;
        UInt64 offset = 0;
        std::optional<UInt64> size_from_name;

        auto delim_pos = offset_with_suffix.find('_');
        if (delim_pos == std::string::npos)
        {
            parsed = tryParseUInt64(offset, offset_with_suffix);
        }
        else
        {
            parsed = tryParseUInt64(offset, offset_with_suffix.substr(0, delim_pos));

            const auto suffix = offset_with_suffix.substr(delim_pos + 1);
            if (suffix == "persistent")
            {
                fs::remove(offset_it->path());
                continue;
            }
            if (suffix == "temporary")
            {
                fs::remove(offset_it->path());
                continue;
            }

            UInt64 size_value = 0;
            if (parsed && tryParseUInt64(size_value, suffix))
                size_from_name = size_value;
            else
                parsed = false;
        }

        if (!parsed)
        {
            LOG_WARNING(log, "Unexpected file: {}", offset_it->path().string());
            continue;
        }

        UInt64 size = size_from_name.has_value() ? *size_from_name : offset_it->file_size();
        if (!size)
        {
            fs::remove(offset_it->path());
            continue;
        }

        if (auto [it, is_new] = offset_to_index.try_emplace(offset, segments.size()); !is_new)
        {
            auto & existing = segments[it->second];
            const bool replace_existing = !size_from_name.has_value() && existing.size_in_filename;
            LOG_WARNING(
                log,
                "Duplicate cache files for offset {}: keeping '{}', ignoring '{}'",
                offset,
                replace_existing ? offset_it->path().string() : existing.path.string(),
                replace_existing ? existing.path.string() : offset_it->path().string());
            if (replace_existing)
                existing = {offset, size, FileSegmentKind::Regular, offset_it->path(), nullptr, /* size_in_filename */false};
            continue;
        }

        segments.push_back({offset, size, FileSegmentKind::Regular, offset_it->path(), nullptr, size_from_name.has_value()});
    }

    size_t size_limit = 0;
    {
        auto lock = cache_guard.writeLock();
        auto state_lock = cache_state_guard.lock();
        size_limit = main_priority->getSizeLimit(state_lock);

        for (auto & segment : segments)
        {
            if (main_priority->canFit(
                    segment.size,
                    /* elements */1,
                    state_lock,
                    /* reservee */nullptr,
                    origin_info,
                    /* is_initial_load */true))
            {
                segment.cache_it = main_priority->add(
                    key_metadata,
                    segment.offset,
                    segment.size,
                    lock,
                    &state_lock,
                    /* is_initial_load */true);
            }
        }
    }

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
                    segment.cache_it,
                    /* size_in_filename */segment.size_in_filename);

                inserted = key_metadata->emplaceUnlocked(segment.offset, std::make_shared<FileSegmentMetadata>(std::move(file_segment))).second;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
            }

            if (inserted)
            {
                LOG_TEST(log, "Added file segment {}:{} (size: {}) with path: {}", key.toString(), segment.offset, segment.size, segment.path.string());
            }
            else
            {
                segment.cache_it->remove(cache_guard.writeLock());
                fs::remove(segment.path);
                chassert(false);
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
            size_limit, failed_to_fit, key.toString());
    }

    if (key_metadata->sizeUnlocked() == 0)
        metadata.removeKey(key, /* if_exists */false, origin_info.user_id);
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const UserID & user_id)
{
    assertInitialized();
    assertCacheCorrectness();

    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](const LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
            file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    }, user_id);
    return file_segments;
}

std::vector<FileSegment::Info> FileCache::getFileSegmentInfos(const Key & key, const UserID & user_id)
{
    std::vector<FileSegment::Info> file_segments;
    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::THROW_LOGICAL, OriginInfo(user_id));
    for (const auto & [_, file_segment_metadata] : *locked_key)
        file_segments.push_back(FileSegment::getInfo(file_segment_metadata->file_segment));
    return file_segments;
}

IFileCachePriority::PriorityDumpPtr FileCache::dumpQueue()
{
    assertInitialized();
    return main_priority->dump(cache_guard.readLock());
}

IFileCachePriority::Type FileCache::getEvictionPolicyType()
{
    assertInitialized();
    return main_priority->getType();
}

std::unordered_map<std::string, FileCache::UsageStat> FileCache::getUsageStatPerClient()
{
    assertInitialized();
    return main_priority->getUsageStatPerClient();
}

std::vector<String> FileCache::tryGetCachePaths(const Key & key)
{
    assertInitialized();

    auto locked_key = metadata.lockKeyMetadata(key, CacheMetadata::KeyNotFoundPolicy::RETURN_NULL, getInternalOrigin());
    if (!locked_key)
        return {};

    std::vector<String> cache_paths;

    for (const auto & [offset, file_segment_metadata] : *locked_key)
    {
        const auto & file_segment = *file_segment_metadata->file_segment;
        if (file_segment.state() == FileSegment::State::DOWNLOADED)
            cache_paths.push_back(locked_key->getKeyMetadata()->getFileSegmentPath(file_segment));
    }
    return cache_paths;
}

size_t FileCache::getUsedCacheSize() const
{
    return main_priority->getSizeApprox();
}

size_t FileCache::getMaxCacheSize() const
{
    return main_priority->getSizeLimitApprox();
}

size_t FileCache::getFileSegmentsNum() const
{
    return main_priority->getElementsCountApprox();
}

void FileCache::onSegmentEvicted(const FileSegment & /* segment */, const String & /* user_id */) const
{
    /// Prometheus/per-user eviction metrics are excluded (kept as no-op metric shims);
    /// CH's ProfileEvents::increment calls collapse to no-ops here.
}

IFileCachePriority::OnEvictCallback FileCache::getOnBackgroundEvictCallback() const
{
    return std::bind_front(&FileCache::onSegmentEvictedInTheBackground, this);
}

void FileCache::onSegmentEvictedInTheBackground(const FileSegment & segment, const String & user_id) const
{
    onSegmentEvicted(segment, user_id);
}

void FileCache::deactivateBackgroundOperations()
{
    shutdown.store(true);

    stop_loading_metadata = true;
    if (load_metadata_main_thread && load_metadata_main_thread->joinable())
        load_metadata_main_thread->join();

    if (keep_up_free_space_ratio_task)
        keep_up_free_space_ratio_task->deactivate();
    /// The single maintenance task: invalidated-entries cleanup + idle-client eviction.
    if (background_cleanup_task)
        background_cleanup_task->deactivate();

    /// The task is stopped, so no new eviction jobs can be scheduled - drain the rest.
    if (eviction_pool)
        eviction_pool->wait();

    metadata.shutdown();
}

std::vector<FileSegment::Info> FileCache::sync()
{
    std::vector<FileSegment::Info> file_segments;
    metadata.iterate([&](LockedKey & locked_key)
    {
        auto broken = locked_key.sync();
        file_segments.insert(file_segments.end(), broken.begin(), broken.end());
    }, getInternalOrigin().user_id);
    return file_segments;
}

void FileCache::assertCacheCorrectnessWithProbability()
{
#ifndef NDEBUG
    if (check_cache_probability.doCheck())
        assertCacheCorrectness();
#endif
}

void FileCache::assertCacheCorrectness()
{
#ifndef NDEBUG
    LOG_TEST(log, "Checking cache correctness");

    metadata.iterate([&](LockedKey & locked_key)
    {
        for (const auto & [_, file_segment_metadata] : locked_key)
            chassert(file_segment_metadata->file_segment->assertCorrectness());
    }, getInternalOrigin().user_id);

    FileCacheReserveStat stat;
    main_priority->iterate([](LockedKey &, const FileSegmentMetadataPtr & file_segment_metadata)
    {
        chassert(file_segment_metadata->file_segment->assertCorrectness());
        return IFileCachePriority::IterationResult::CONTINUE;
    }, stat, cache_guard.readLock());

    main_priority->check(cache_state_guard.lock());
#endif
}

FileCache::QueryContextHolderPtr FileCache::getQueryContextHolder(
    const String & query_id, const FileCacheReadOptions & settings)
{
    if (!query_limit || settings.maxDownloadSizePerQuery == 0)
        return {};

    auto lock = cache_guard.writeLock();
    auto context = query_limit->getOrSetQueryContext(query_id, settings, lock);
    return std::make_unique<QueryContextHolder>(query_id, this, query_limit.get(), std::move(context));
}

void FileCache::freeSpaceRatioKeepingThreadFunc()
{
    if (shutdown)
        return;

    Stopwatch watch;
    size_t reschedule_ms = free_space_keeping_reschedule_ms;

    /// The task must never throw out: otherwise it stops being rescheduled.
    try
    {
        freeSpaceRatioImpl(reschedule_ms);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in free space ratio keeping thread");
    }

    /// D11: single wall-time snapshot (elapsedMilliseconds); feeds only a no-op metric.
    [[maybe_unused]] const size_t elapsed_ms = watch.elapsed().wallNanos / 1'000'000;

    [[maybe_unused]] bool scheduled = keep_up_free_space_ratio_task->scheduleAfter(reschedule_ms);
    chassert(scheduled);
}

std::unique_ptr<EvictionInfo> FileCache::collectFreeSpaceEvictionInfo(
    const CacheStateGuard::Lock & lock, size_t in_flight_size, size_t in_flight_elements)
{
    const size_t size_limit = main_priority->getSizeLimit(lock);
    const size_t elements_limit = main_priority->getElementsLimit(lock);

    const size_t desired_size = std::lround(keep_current_size_to_max_ratio * static_cast<double>(size_limit));
    const size_t desired_elements = std::lround(keep_current_elements_to_max_ratio * static_cast<double>(elements_limit));

    const size_t current_size = main_priority->getSize(lock);
    const size_t current_elements = main_priority->getElementsCount(lock);
    const size_t projected_size = current_size > in_flight_size ? current_size - in_flight_size : 0;
    const size_t projected_elements = current_elements > in_flight_elements ? current_elements - in_flight_elements : 0;

    const size_t size_to_evict = projected_size > desired_size ? projected_size - desired_size : 0;
    const size_t elements_to_evict = projected_elements > desired_elements ? projected_elements - desired_elements : 0;

    if (!size_to_evict && !elements_to_evict)
        return nullptr;

    auto eviction_info = main_priority->collectEvictionInfo(
        size_to_evict, elements_to_evict, /* reservee */nullptr,
        /* is_total_space_cleanup */true, getInternalOrigin(), lock);

    chassert(eviction_info);
    chassert(!eviction_info->hasHoldSpace());
    chassert(eviction_info->requiresEviction());
    return eviction_info;
}

void FileCache::freeSpaceRatioImpl(size_t & reschedule_ms)
{
    std::unique_ptr<EvictionInfo> eviction_info;
    {
        auto lock = cache_state_guard.tryLockFor(std::chrono::milliseconds(free_space_keeping_state_lock_timeout_ms));
        if (!lock)
        {
            reschedule_ms = free_space_keeping_retry_reschedule_ms;
            return;
        }
        eviction_info = collectFreeSpaceEvictionInfo(lock, 0, 0);
    }
    if (!eviction_info)
        return;

    auto status = IFileCachePriority::CollectStatus::SUCCESS;

    const size_t queue_capacity = std::max<size_t>(2 * keep_up_free_space_eviction_threads, 2);
    FileCacheBoundedQueue<EvictionBatchPtr> pending_eviction_queue(queue_capacity);
    FileCacheBoundedQueue<EvictionBatchPtr> pending_finalization_queue(queue_capacity);

    std::atomic<size_t> running_removers = 0;
    bool removers_scheduled = false;

    size_t in_flight_size = 0;
    size_t in_flight_elements = 0;
    size_t evicted_size = 0;
    size_t evicted_elements = 0;

    auto finalize_removed = [&](bool blocking)
    {
        EvictionBatchPtr batch;
        while (blocking ? pending_finalization_queue.pop(batch) : pending_finalization_queue.tryPop(batch))
        {
            in_flight_size -= batch->candidates->bytes();
            in_flight_elements -= batch->candidates->size();
            try
            {
                {
                    auto lock = cache_guard.writeLock();
                    batch->candidates->afterEvictWrite(lock);
                    IFileCachePriority::removeEntries(batch->invalidated_entries, lock);
                }
                batch->candidates->afterEvictState(cache_state_guard.lock());

                const auto failed = batch->candidates->getFailedCandidates();
                evicted_size += batch->candidates->bytes() - failed.total_cache_size;
                evicted_elements += batch->candidates->size() - failed.total_cache_elements;
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to finalize evicted file segments");
                chassert(false);
            }
        }
    };

    try
    {
        for (size_t i = 0; i < keep_up_free_space_eviction_threads; ++i)
        {
            eviction_pool->scheduleOrThrowOnError([&]
            {
                try
                {
                    EvictionBatchPtr batch;
                    while (pending_eviction_queue.pop(batch))
                    {
                        try
                        {
                            batch->candidates->evict();
                        }
                        catch (...)
                        {
                            tryLogCurrentException(log, "Failed to evict file segments in background");
                        }
                        if (!pending_finalization_queue.push(std::move(batch)))
                        {
                            chassert(false);
                            break;
                        }
                    }
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Background eviction remover failed");
                }

                if (running_removers.fetch_sub(1) == 1)
                    pending_finalization_queue.finish();
            });
            ++running_removers;
            removers_scheduled = true;
        }

        main_priority->resetEvictionPos(IFileCachePriority::EvictionCursor::Background);

        while (!shutdown)
        {
            if (!eviction_info)
            {
                finalize_removed(/* blocking */false);

                auto lock = cache_state_guard.tryLockFor(std::chrono::milliseconds(free_space_keeping_state_lock_timeout_ms));
                if (!lock)
                {
                    reschedule_ms = free_space_keeping_retry_reschedule_ms;
                    status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                    break;
                }
                eviction_info = collectFreeSpaceEvictionInfo(lock, in_flight_size, in_flight_elements);
                if (!eviction_info)
                    break;
            }

            LOG_TRACE(log, "Collecting eviction candidates to keep free space ({}), in flight: {}/{} (size/elements)",
                      eviction_info->toString(), in_flight_size, in_flight_elements);

            auto batch = std::make_shared<EvictionBatch>();
            batch->candidates = std::make_unique<EvictionCandidates>(getOnBackgroundEvictCallback());
            FileCacheReserveStat stat;
            main_priority->collectCandidatesForEviction(
                *eviction_info, stat, *batch->candidates, batch->invalidated_entries,
                /* reservee */nullptr, IFileCachePriority::EvictionCursor::Background,
                /* max_candidates_size */keep_up_free_space_remove_batch,
                /* is_total_space_cleanup */true, getInternalOrigin(),
                cache_guard, cache_state_guard);

            eviction_info.reset();

            const size_t batch_size = batch->candidates->size();
            if (batch_size == 0)
            {
                if (!batch->invalidated_entries.empty())
                {
                    IFileCachePriority::removeEntries(batch->invalidated_entries, cache_guard.writeLock());
                    continue;
                }
                status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                break;
            }

            const size_t batch_bytes = batch->candidates->bytes();

            constexpr size_t push_timeout_ms = 10;
            constexpr size_t max_push_attempts = 1000;
            bool pushed = false;
            for (size_t attempt = 0; !pushed && attempt < max_push_attempts; ++attempt)
            {
                pushed = pending_eviction_queue.tryPush(batch, push_timeout_ms);
                if (!pushed)
                    finalize_removed(/* blocking */false);
            }
            if (!pushed)
            {
                LOG_WARNING(
                    log, "Background eviction workers take too much time to evict "
                    "(max_push_attempts: {}, push_timeout_ms: {})",
                    max_push_attempts, push_timeout_ms);

                status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
                reschedule_ms = free_space_keeping_retry_reschedule_ms;
                break;
            }

            in_flight_size += batch_bytes;
            in_flight_elements += batch_size;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error while collecting background eviction candidates");
        status = IFileCachePriority::CollectStatus::CANNOT_EVICT;
    }

    if (shutdown)
        status = IFileCachePriority::CollectStatus::CANNOT_EVICT;

    pending_eviction_queue.finish();
    if (!removers_scheduled)
        pending_finalization_queue.finish();
    finalize_removed(/* blocking */true);
    eviction_pool->wait();

    LOG_TRACE(log, "Free space ratio keeping thread finished with status `{}`, evicted {} file segments ({} bytes)",
              static_cast<int>(status), evicted_elements, evicted_size);

    assertCacheCorrectness();
}

UInt64 FileCache::backgroundCleanupIntervalMs() const
{
    const auto ttl_sec = idle_client_ttl_sec.load();
    if (ttl_sec == 0)
        return invalidated_entries_cleanup_interval_ms;
    const auto check_sec = idle_client_check_interval_sec.load();
    const auto interval_sec = check_sec > 0 ? check_sec : std::max<UInt64>(1, ttl_sec / 10);
    return std::min<UInt64>(invalidated_entries_cleanup_interval_ms, interval_sec * 1000);
}

void FileCache::backgroundCleanupTaskFunc()
{
    size_t removed = 0;
    {
        Stopwatch watch;
        try
        {
            removed = main_priority->removeInvalidatedEntries(invalidated_entries_cleanup_remove_batch, cache_guard);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        [[maybe_unused]] const size_t elapsed_ms = watch.elapsed().wallNanos / 1'000'000;
    }

    /// Idle-client eviction shares this thread; self-throttled to its own interval.
    if (client_tracking_possible)
    {
        try
        {
            evictIdleClients();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (shutdown || !background_cleanup_task)
        return;

    if (removed == invalidated_entries_cleanup_remove_batch)
        background_cleanup_task->schedule();
    else
        background_cleanup_task->scheduleAfter(backgroundCleanupIntervalMs());
}

void FileCache::evictIdleClients()
{
    if (!is_initialized || shutdown)
        return;

    const auto ttl_sec = idle_client_ttl_sec.load();
    if (ttl_sec == 0)
        return;

    const auto now = std::chrono::steady_clock::now();
    const auto configured = idle_client_check_interval_sec.load();
    const std::chrono::seconds check_interval(configured > 0 ? configured : std::max<UInt64>(1, ttl_sec / 10));
    if (last_idle_eviction.time_since_epoch().count() != 0 && now - last_idle_eviction < check_interval)
        return;
    last_idle_eviction = now;

    const std::chrono::seconds ttl(ttl_sec);
    const auto idle_clients = main_priority->collectIdleClients(ttl);
    if (idle_clients.empty())
        return;

    const auto check_now = std::chrono::steady_clock::now();
    std::atomic<size_t> next_index = 0;
    auto purge_worker = [&]
    {
        for (size_t i = next_index.fetch_add(1, std::memory_order_relaxed);
             i < idle_clients.size() && !shutdown;
             i = next_index.fetch_add(1, std::memory_order_relaxed))
        {
            const auto & [user_id, usage] = idle_clients[i];
            try
            {
                if (!usage->idleFor(ttl, check_now))
                    continue;

                if (usage->total_size == 0)
                    continue;

                if (metadata.removeAllKeys(user_id))
                    LOG_INFO(log, "Purged cache for idle client '{}' (idle for >= {} sec)", user_id, ttl_sec);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to purge idle client cache");
            }
        }
    };

    const size_t num_extra_threads = std::min<size_t>(idle_clients.size(), idle_client_eviction_threads) - 1;
    std::vector<ThreadFromGlobalPool> threads;
    try
    {
        threads.reserve(num_extra_threads);
        for (size_t t = 0; t < num_extra_threads; ++t)
            threads.emplace_back(worker_pool, purge_worker);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to spawn idle client eviction threads");
    }
    purge_worker();
    for (auto & thread : threads)
        thread.join();
}

void FileCache::applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings)
{
    /// R2: decide per field by value comparison new!=actual (matching CH FileCache.cpp:2800),
    /// not per-field `.changed` tracking.
    if (!is_initialized || shutdown || new_settings == actual_settings)
        return;

    std::lock_guard lock(apply_settings_mutex);

    if (new_settings.backgroundDownloadQueueSizeLimit != actual_settings.backgroundDownloadQueueSizeLimit
        && metadata.setBackgroundDownloadQueueSizeLimit(new_settings.backgroundDownloadQueueSizeLimit))
    {
        actual_settings.backgroundDownloadQueueSizeLimit = new_settings.backgroundDownloadQueueSizeLimit;
    }

    if (new_settings.backgroundDownloadThreads != actual_settings.backgroundDownloadThreads)
    {
        bool updated = false;
        try
        {
            updated = metadata.setBackgroundDownloadThreads(new_settings.backgroundDownloadThreads);
        }
        catch (...)
        {
            actual_settings.backgroundDownloadThreads = metadata.getBackgroundDownloadThreads();
            throw;
        }

        if (updated)
            actual_settings.backgroundDownloadThreads = new_settings.backgroundDownloadThreads;
    }

    if (new_settings.backgroundDownloadMaxFileSegmentSize != actual_settings.backgroundDownloadMaxFileSegmentSize)
    {
        background_download_max_file_segment_size = new_settings.backgroundDownloadMaxFileSegmentSize;
        actual_settings.backgroundDownloadMaxFileSegmentSize = new_settings.backgroundDownloadMaxFileSegmentSize;
    }

    if (new_settings.reserveGranularity != actual_settings.reserveGranularity)
    {
        reserve_granularity.store(new_settings.reserveGranularity, std::memory_order_relaxed);
        actual_settings.reserveGranularity = new_settings.reserveGranularity;
    }

    {
        SizeLimits desired_limits{
            .max_size = new_settings.maxSize,
            .max_elements = new_settings.maxElements,
            .slru_size_ratio = new_settings.slruSizeRatio
        };
        SizeLimits current_limits{
            .max_size = actual_settings.maxSize,
            .max_elements = actual_settings.maxElements,
            .slru_size_ratio = actual_settings.slruSizeRatio
        };

        const bool max_size_changed = desired_limits.max_size != current_limits.max_size;
        const bool max_elements_changed = desired_limits.max_elements != current_limits.max_elements;
        const bool slru_ratio_changed = desired_limits.slru_size_ratio != current_limits.slru_size_ratio;

        const bool do_dynamic_resize = (max_size_changed || max_elements_changed) && !slru_ratio_changed;

        if (allow_dynamic_cache_resize && do_dynamic_resize && !main_priority->isOvercommitEviction())
        {
            auto result_limits = doDynamicResize(current_limits, desired_limits);

            actual_settings.maxSize = result_limits.max_size;
            actual_settings.maxElements = result_limits.max_elements;
        }
        else if (do_dynamic_resize)
        {
            LOG_WARNING(
                log, "Filesystem cache size was modified, but dynamic cache resize is disabled, "
                "therefore cache size will not be changed without server restart.");
        }
    }

    if (new_settings.maxFileSegmentSize != actual_settings.maxFileSegmentSize)
    {
        max_file_segment_size = actual_settings.maxFileSegmentSize = new_settings.maxFileSegmentSize;
    }

    if (new_settings.idleClientTtlSec != actual_settings.idleClientTtlSec)
    {
        const UInt64 new_ttl = new_settings.idleClientTtlSec;
        const bool was_enabled = idle_client_ttl_sec.load() > 0;
        idle_client_ttl_sec.store(new_ttl);
        actual_settings.idleClientTtlSec = new_settings.idleClientTtlSec;

        if (new_ttl > 0 && !client_tracking_possible)
            LOG_WARNING(log, "idle_client_ttl_sec is set but the cache policy does not track "
                             "per-client usage; idle-client eviction is disabled");
        else if (new_ttl > 0 && !was_enabled && client_tracking_possible && background_cleanup_task)
            background_cleanup_task->schedule();
    }

    if (new_settings.idleClientCheckIntervalSec != actual_settings.idleClientCheckIntervalSec)
    {
        idle_client_check_interval_sec.store(new_settings.idleClientCheckIntervalSec);
        actual_settings.idleClientCheckIntervalSec = new_settings.idleClientCheckIntervalSec;
    }

    if (new_settings.exposePrometheusEvictionMetrics != actual_settings.exposePrometheusEvictionMetrics)
    {
        expose_eviction_metrics.store(new_settings.exposePrometheusEvictionMetrics, std::memory_order_relaxed);
        actual_settings.exposePrometheusEvictionMetrics = new_settings.exposePrometheusEvictionMetrics;
    }

    if (new_settings.exposePrometheusEvictionMetricsPerUser != actual_settings.exposePrometheusEvictionMetricsPerUser)
    {
        expose_eviction_metrics_per_user.store(new_settings.exposePrometheusEvictionMetricsPerUser, std::memory_order_relaxed);
        actual_settings.exposePrometheusEvictionMetricsPerUser = new_settings.exposePrometheusEvictionMetricsPerUser;
    }
}

FileCache::SizeLimits FileCache::doDynamicResize(const SizeLimits & prev_limits, const SizeLimits & desired_limits)
{
    if (prev_limits.slru_size_ratio != desired_limits.slru_size_ratio)
        throwFileCacheException("Dynamic resize of size ratio is not allowed");

    std::unique_lock resize_lock(dynamic_resize_lock, std::defer_lock);
    if (!resize_lock.try_lock_for(std::chrono::milliseconds(dynamic_resize_lock_wait_ms)))
    {
        LOG_WARNING(log, "Dynamic resize skipped: could not acquire resize lock within {}ms", dynamic_resize_lock_wait_ms);
        return prev_limits;
    }

    SizeLimits result_limits;
    bool modified_size_limit = false;
    {
        auto cache_lock = cache_state_guard.lock();

        if (prev_limits.max_size != main_priority->getSizeLimit(cache_lock))
            throwFileCacheException("Current limits inconsistency in size");

        if (prev_limits.max_elements != main_priority->getElementsLimit(cache_lock))
            throwFileCacheException("Current limits inconsistency in elements number");

        try
        {
            modified_size_limit = doDynamicResizeImpl(prev_limits, desired_limits, result_limits, cache_lock);
            chassert(result_limits.max_size && result_limits.max_elements);
        }
        catch (...)
        {
            LOG_ERROR(log, "Unexpected error during dynamic cache resize: {}", getCurrentExceptionMessage(true));

            if (!cache_lock.owns_lock())
                cache_lock.lock();

            size_t max_size = main_priority->getSizeLimit(cache_lock);
            size_t max_elements = main_priority->getElementsLimit(cache_lock);

            if (result_limits.max_size != max_size || result_limits.max_elements != max_elements)
            {
                result_limits.max_size = max_size;
                result_limits.max_elements = max_elements;
            }

            cache_lock.unlock();
            assertCacheCorrectness();
            throw;
        }
    }

    if (!modified_size_limit)
    {
        LOG_WARNING(
            log, "Unable to modify size limit from {} to {}, elements limit from {} to {}.",
            prev_limits.max_size, desired_limits.max_size,
            prev_limits.max_elements, desired_limits.max_elements);
    }

    chassert(main_priority->getSizeApprox() <= result_limits.max_size);
    chassert(main_priority->getElementsCountApprox() <= result_limits.max_elements);

    assertCacheCorrectness();
    return result_limits;
}

bool FileCache::doDynamicResizeImpl(
    const SizeLimits & prev_limits,
    const SizeLimits & desired_limits,
    SizeLimits & result_limits,
    CacheStateGuard::Lock & state_lock)
{
    auto eviction_info = main_priority->collectEvictionInfoForResize(
        desired_limits.max_size,
        desired_limits.max_elements,
        getInternalOrigin(),
        state_lock);

    chassert(!eviction_info->hasHoldSpace());

    EvictionCandidates eviction_candidates(main_priority->getOnEvictCallback());
    if (!eviction_info->requiresEviction())
    {
        main_priority->modifySizeLimits(
            desired_limits.max_size,
            desired_limits.max_elements,
            desired_limits.slru_size_ratio,
            state_lock);

        result_limits = desired_limits;
        return true;
    }

    state_lock.unlock();

    FileCacheReserveStat stat;
    IFileCachePriority::InvalidatedEntriesInfos invalidated_entries;
    if (!main_priority->collectCandidatesForEviction(
            *eviction_info,
            stat,
            eviction_candidates,
            invalidated_entries,
            /* reservee */nullptr,
            IFileCachePriority::EvictionCursor::FromHead,
            /* max_candidates_size */0,
            /* is_total_space_cleanup */true,
            getInternalOrigin(),
            cache_guard,
            cache_state_guard))
    {
        result_limits = prev_limits;
        return false;
    }

    auto write_lock = cache_guard.writeLock();
    eviction_candidates.removeQueueEntries(write_lock);

    state_lock.lock();

    main_priority->modifySizeLimits(
        desired_limits.max_size,
        desired_limits.max_elements,
        desired_limits.slru_size_ratio,
        state_lock);

    state_lock.unlock();
    write_lock.unlock();

    eviction_candidates.evict();

    chassert(!eviction_candidates.requiresAfterEvictWrite());
    IFileCachePriority::removeEntries(invalidated_entries, cache_guard.writeLock());

    auto failed_candidates = eviction_candidates.getFailedCandidates();
    if (failed_candidates.size() == 0)
    {
        result_limits = desired_limits;
        return true;
    }

    result_limits = prev_limits;

    auto cache_write_lock = cache_guard.writeLock();
    state_lock.lock();

    main_priority->modifySizeLimits(
        result_limits.max_size,
        result_limits.max_elements,
        result_limits.slru_size_ratio,
        state_lock);

    for (const auto & [key_metadata, key_candidates, _] : failed_candidates.failed_candidates_per_key)
    {
        chassert(!key_candidates.empty());

        auto locked_key = key_metadata->tryLock();
        if (!locked_key)
        {
            LOG_ERROR(log, "Unexpected state: key {} does not exist", key_metadata->key.toString());
            chassert(false);
            continue;
        }

        for (const auto & candidate : key_candidates)
        {
            const auto & file_segment = candidate->file_segment;
            const auto restored_size = candidate->size();

            auto original_queue_type = eviction_candidates.getOriginalQueueType(candidate.get());

            auto main_priority_iterator = main_priority->addForRestore(
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

}
