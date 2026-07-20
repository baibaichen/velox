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

#include "velox/ch/Common/ClickHouseAliases.h"
#include "velox/ch/Common/FileCacheScheduler.h"
#include "velox/ch/Common/StatusFile.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/ch/Interpreters/FileCache/EvictionCandidates.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd_internal.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/Metadata.h"
#include "velox/ch/Interpreters/FileCache/QueryLimit.h"
#include "velox/ch/Interpreters/FileCache/SplitFileCachePriority.h"

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace facebook::velox
{
class ReadFile;
class WriteFile;
namespace memory
{
class MemoryPool;
}
} // namespace facebook::velox

namespace facebook::velox::ch
{

/// Track acquired space in cache during reservation to make error messages when
/// no space is left more informative. Ported exactly from ClickHouse; the
/// `magic_enum::enum_count<FileSegmentKind>()` array bound is replaced with the
/// literal kind count (2).
struct FileCacheReserveStat
{
    struct Stat
    {
        size_t releasable_size = 0;
        size_t releasable_count = 0;

        size_t non_releasable_size = 0;
        size_t non_releasable_count = 0;

        size_t evicting_count = 0;
        size_t moving_count = 0;
        size_t invalidated_count = 0;

        size_t candidates_iteration_steps = 0;
        size_t clients_iterated = 0;

        Stat & operator+=(const Stat & other)
        {
            releasable_size += other.releasable_size;
            releasable_count += other.releasable_count;
            non_releasable_size += other.non_releasable_size;
            non_releasable_count += other.non_releasable_count;
            evicting_count += other.evicting_count;
            moving_count += other.moving_count;
            invalidated_count += other.invalidated_count;
            candidates_iteration_steps += other.candidates_iteration_steps;
            clients_iterated += other.clients_iterated;
            return *this;
        }

        std::string toString() const;
    };

    Stat total_stat;
    static constexpr size_t kind_count = 2;
    std::array<Stat, kind_count> stat_by_kind{};

    Stat & getStatByKind(FileSegmentKind kind) { return stat_by_kind[static_cast<uint8_t>(kind)]; }
    const Stat & getStatByKind(FileSegmentKind kind) const { return stat_by_kind[static_cast<uint8_t>(kind)]; }

    enum class State
    {
        Releasable,
        NonReleasable,
        Evicting,
        Moving,
        Invalidated,
    };
    void update(size_t size, FileSegmentKind kind, State state);

    FileCacheReserveStat & operator+=(const FileCacheReserveStat & other)
    {
        total_stat += other.total_stat;
        for (size_t i = 0; i < stat_by_kind.size(); ++i)
            stat_by_kind[i] += other.stat_by_kind[i];
        return *this;
    }
};

/// Local cache for remote filesystem files, represented as a set of
/// non-overlapping non-empty file segments.
class FileCache
{
public:
    FileCache(const FileCache &) = delete;
    FileCache & operator=(const FileCache &) = delete;

    using Key = FileCacheKey;
    using QueryLimit = FileCacheQueryLimit;
    using Priority = IFileCachePriority;
    using PriorityEntry = IFileCachePriority::Entry;
    using QueryContextHolder = FileCacheQueryLimit::QueryContextHolder;
    using OriginInfo = FileCacheOriginInfo;
    using UserID = FileCacheOriginInfo::UserID;
    using Type = FileSegmentKeyType;
    using CachePriorityCreatorFunction = SplitFileCachePriority::CachePriorityCreatorFunction;

    /// Manager-injected local-cache file factories and opened-file invalidation
    /// (Task 013 owns these; replaces global FileSystem / OpenedFileCache
    /// singleton). `append=true` opens an existing partial file positioned at
    /// its end; `append=false` creates a new file (error if it already exists).
    using CacheWriteFileFactory =
        std::function<std::unique_ptr<velox::WriteFile>(const std::string & path, bool append)>;
    using CacheReadFileFactory =
        std::function<std::shared_ptr<velox::ReadFile>(const std::string & path)>;
    using OpenedFileInvalidator = std::function<void(const std::string & path)>;

    FileCache(
        const std::string & cache_name,
        const FileCacheConfig & settings,
        FileCacheScheduler & scheduler_,
        FileCacheWorkerPool & worker_pool_,
        velox::memory::MemoryPool * memory_pool_,
        const OriginInfo & common_origin_,
        CacheWriteFileFactory create_write_file_,
        CacheReadFileFactory open_read_file_,
        OpenedFileInvalidator invalidate_opened_file_);

    ~FileCache();

    void initialize();

    bool isInitialized() const;

    void throwInitExceptionIfNeeded();

    const String & getBasePath() const;

    bool skipCacheOnDiskFailure() const { return skip_cache_on_disk_failure; }

    const OriginInfo & getCommonOrigin() const { return common_origin; }

    static const OriginInfo & getInternalOrigin();

    OriginInfo getCommonOriginWithSegmentKeyType(const std::filesystem::path & filename) const;

    String getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin, std::optional<size_t> size = std::nullopt) const;

    String getKeyPath(const Key & key, const OriginInfo & origin) const;

    FileSegmentsHolderPtr getOrSet(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_size,
        const CreateFileSegmentSettings & settings,
        size_t file_segments_limit,
        const OriginInfo & origin,
        std::optional<size_t> boundary_alignment_ = std::nullopt);

    FileSegmentsHolderPtr get(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_segments_limit,
        const UserID & user_id);

    FileSegmentsHolderPtr getDownloadedContiguousOrEmpty(
        const Key & key,
        size_t offset,
        size_t size,
        const UserID & user_id);

    FileSegmentsHolderPtr set(
        const Key & key,
        size_t offset,
        size_t size,
        const CreateFileSegmentSettings & settings,
        const OriginInfo & origin);

    FileSegmentsHolderPtr trySet(
        const Key & key,
        size_t offset,
        size_t size,
        const CreateFileSegmentSettings & settings,
        const OriginInfo & origin);

    void removeFileSegment(const Key & key, size_t offset, const UserID & user_id);

    void removeFileSegmentIfExists(const Key & key, size_t offset, const UserID & user_id);

    void removeKey(const Key & key, const UserID & user_id);

    void removeKeyIfExists(const Key & key, const UserID & user_id);

    void removePathIfExists(const String & path, const UserID & user_id);

    void removeAllReleasable(const UserID & user_id);

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;
    size_t getMaxCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return max_file_segment_size; }

    size_t getBackgroundDownloadMaxFileSegmentSize() const { return background_download_max_file_segment_size.load(); }

    size_t getBoundaryAlignment() const { return boundary_alignment; }

    size_t getReserveGranularity() const { return reserve_granularity.load(std::memory_order_relaxed); }

    bool tryReserve(
        FileSegment & file_segment,
        size_t size,
        FileCacheReserveStat & stat,
        const OriginInfo & origin,
        size_t lock_wait_timeout_milliseconds,
        std::string & failure_reason);

    bool tryIncreasePriority(FileSegment & file_segment);

    std::vector<FileSegment::Info> getFileSegmentInfos(const UserID & user_id);

    std::vector<FileSegment::Info> getFileSegmentInfos(const Key & key, const UserID & user_id);

    IFileCachePriority::PriorityDumpPtr dumpQueue();

    IFileCachePriority::Type getEvictionPolicyType();

    using UsageStat = IFileCachePriority::UsageStat;
    std::unordered_map<std::string, UsageStat> getUsageStatPerClient();

    void deactivateBackgroundOperations();

    CachePriorityGuard::WriteLock lockCache() const;

    std::vector<FileSegment::Info> sync();

    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr getQueryContextHolder(const String & query_id, const FileCacheReadOptions & options);

    using IterateFunc = std::function<void(const FileSegmentInfo &)>;
    void iterate(IterateFunc && func, const UserID & user_id);

    using CacheIteratorPtr = CacheMetadata::IteratorPtr;
    CacheIteratorPtr getCacheIterator(const UserID & user_id);

    void applySettingsIfPossible(const FileCacheConfig & new_settings, FileCacheConfig & actual_settings);

    void freeSpaceRatioKeepingThreadFunc();

    void backgroundCleanupTaskFunc();

    void evictIdleClients();

    UInt64 backgroundCleanupIntervalMs() const;

    const String & getName() const { return name; }

    /// Manager-injected file-factory access used by FileSegment/CacheMetadata
    /// to open local cache files (replaces the CH global FileSystem).
    FileSegment::LocalCacheWriterPtr createCacheWriteBuffer(const std::string & path, size_t existing_size) const;
    FileSegment::RemoteFileReaderPtr createCacheReadBuffer(const std::string & path) const;
    void invalidateOpenedFile(const std::string & path) const;
    velox::memory::MemoryPool * getMemoryPool() const { return memory_pool; }

private:
    void onSegmentEvicted(const FileSegment & segment, const String & user_id) const;
    IFileCachePriority::OnEvictCallback getOnBackgroundEvictCallback() const;
    void onSegmentEvictedInTheBackground(const FileSegment & segment, const String & user_id) const;

    using KeyAndOffset = FileCacheKeyAndOffset;

    std::atomic<size_t> max_file_segment_size;
    const size_t bypass_cache_threshold;
    const size_t boundary_alignment;
    std::atomic<size_t> reserve_granularity;
    std::atomic<size_t> background_download_max_file_segment_size;
    UInt64 load_metadata_threads;
    const bool load_metadata_asynchronously;
    std::atomic<bool> stop_loading_metadata = false;
    std::unique_ptr<FileCacheWorker> load_metadata_main_thread;
    const bool write_cache_per_user_directory;
    const bool allow_dynamic_cache_resize;
    const size_t dynamic_resize_lock_wait_ms;

    /// Manager-injected runtime dependencies.
    FileCacheScheduler & scheduler;
    FileCacheWorkerPool & worker_pool;
    velox::memory::MemoryPool * memory_pool;
    const OriginInfo common_origin;
    const CacheWriteFileFactory create_write_file;
    const CacheReadFileFactory open_read_file;
    const OpenedFileInvalidator invalidate_opened_file_callback;

    FileCacheScheduledTaskHolder keep_up_free_space_ratio_task;
    const double keep_current_size_to_max_ratio;
    const double keep_current_elements_to_max_ratio;
    const size_t keep_up_free_space_remove_batch;
    const size_t keep_up_free_space_eviction_threads;

    std::unique_ptr<FileCacheThreadPool> eviction_pool;

    FileCacheScheduledTaskHolder background_cleanup_task;
    const UInt64 invalidated_entries_cleanup_threshold;
    const UInt64 invalidated_entries_cleanup_interval_ms;
    const UInt64 invalidated_entries_cleanup_remove_batch;

    std::atomic<UInt64> idle_client_ttl_sec{0};
    std::atomic<UInt64> idle_client_check_interval_sec{0};
    const UInt64 idle_client_eviction_threads;
    std::chrono::steady_clock::time_point last_idle_eviction;
    bool client_tracking_possible = false;

    const bool use_split_cache;
    const double split_cache_ratio;

    const bool skip_cache_on_disk_failure;
    std::atomic<bool> expose_eviction_metrics;
    std::atomic<bool> expose_eviction_metrics_per_user;

    String name;
    LoggerPtr log;

    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    /// Exception-safe once guard for initialize(). std::call_once cannot be used
    /// here: this build statically links libstdc++/libgcc, so an exception thrown
    /// by the callable unwinds through glibc's pthread_once (no unwind tables) and
    /// aborts instead of retrying. A plain mutex+flag preserves ClickHouse's
    /// callOnce retry-on-exception semantics (a throw leaves the flag unset).
    std::mutex initialize_mutex;
    bool initialize_completed = false;
    mutable std::mutex init_mutex;
    std::unique_ptr<StatusFile> status_file;
    std::atomic<bool> shutdown = false;
    std::shared_timed_mutex dynamic_resize_lock;

    std::atomic<size_t> cache_reserve_active_threads = 0;

    std::mutex apply_settings_mutex;

    FileCachePriorityPtr main_priority;

    /// Must be declared after main_priority: metadata holds iterators that
    /// reference the priority's internal state, so metadata is destroyed first.
    CacheMetadata metadata;
    mutable CachePriorityGuard cache_guard;
    mutable CachePriorityGuard queue_guard;
    mutable CacheStateGuard cache_state_guard;

    struct CheckCacheProbability
    {
        explicit CheckCacheProbability(double probability, UInt64 seed = 0);

        bool doCheck();

    private:
        std::mt19937_64 rndgen;
        std::bernoulli_distribution distribution;
        std::mutex mutex;
    };
    CheckCacheProbability check_cache_probability;

    FileCacheQueryLimitPtr query_limit;

    void initializeImpl(bool load_metadata);

    void assertInitialized() const;
    void assertCacheCorrectness();
    void assertCacheCorrectnessWithProbability();

    void loadMetadata();
    void loadMetadataImpl();
    void loadMetadataForKey(const std::filesystem::path & key_dir, const OriginInfo & origin);

    FileSegments getImpl(
        const LockedKey & locked_key,
        const FileSegment::Range & range,
        size_t file_segments_limit,
        bool ignore_bypass_threshold = false) const;

    std::vector<FileSegment::Range> splitRange(size_t offset, size_t size, size_t aligned_size);

    FileSegments createFileSegmentsFromRanges(
        LockedKey & locked_key,
        const std::vector<FileSegment::Range> & ranges,
        size_t & file_segments_count,
        size_t file_segments_limit,
        const CreateFileSegmentSettings & create_settings);

    void fillHolesWithEmptyFileSegments(
        LockedKey & locked_key,
        FileSegments & file_segments,
        const FileSegment::Range & range,
        size_t non_aligned_right_offset,
        size_t file_segments_limit,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings);

    KeyMetadata::iterator addFileSegment(
        LockedKey & locked_key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings);

    struct SizeLimits
    {
        size_t max_size = 0;
        size_t max_elements = 0;
        double slru_size_ratio = 0;
    };
    SizeLimits doDynamicResize(const SizeLimits & prev_limits, const SizeLimits & desired_limits);
    bool doDynamicResizeImpl(
        const SizeLimits & prev_limits,
        const SizeLimits & desired_limits,
        SizeLimits & result_limits,
        CacheStateGuard::Lock &);

    bool doTryReserve(
        FileSegment & file_segment,
        size_t size,
        FileCacheReserveStat & stat,
        const OriginInfo & origin_info,
        size_t lock_wait_timeout_milliseconds,
        std::string & failure_reason);

    bool doEviction(
        EvictionInfo & main_eviction_info,
        EvictionInfo * query_eviction_info,
        FileSegment & file_segment,
        const OriginInfo & origin_info,
        const IFileCachePriority::IteratorPtr & main_priority_iterator,
        FileCacheReserveStat & reserve_stat,
        EvictionCandidates & eviction_candidates,
        IFileCachePriority::InvalidatedEntriesInfos & invalidated_entries,
        Priority * query_priority,
        std::string & failure_reason);

    std::unique_ptr<EvictionInfo> collectFreeSpaceEvictionInfo(
        const CacheStateGuard::Lock & lock, size_t in_flight_size, size_t in_flight_elements);

    void freeSpaceRatioImpl(size_t & reschedule_ms);
};

} // namespace facebook::velox::ch
