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

#include <array>
#include <cstdint>
#include <functional>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <boost/functional/hash.hpp>

#include <folly/synchronization/CallOnce.h>
#include "velox/common/caching/filecache/FileCache_fwd.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/caching/filecache/Metadata.h"
#include "velox/common/caching/filecache/QueryLimit.h"
#include "velox/common/caching/filecache/FileCache_fwd_internal.h"
#include "velox/common/caching/filecache/FileCacheSettings.h"
#include "velox/common/caching/filecache/FileCacheOriginInfo.h"
#include "velox/common/caching/filecache/SplitFileCachePriority.h"
#include <filesystem>
#include <random>
#include <folly/Random.h>

#include "velox/common/base/Exceptions.h"


namespace facebook::velox::ch {

class BackgroundSchedulePoolTaskHolder; // TODO(threading): CH background scheduler holder.
class StatusFile; // TODO(status-file): CH StatusFile marker.
class ThreadFromGlobalPool; // TODO(threading): CH global thread pool thread.

struct ReadSettings;
struct FilesystemCacheSettings;

/// Track acquired space in cache during reservation
/// to make error messages when no space left more informative.
struct FileCacheReserveStat
{
    struct Stat
    {
        size_t releasableSize = 0;
        size_t releasableCount = 0;

        size_t nonReleasableSize = 0;
        size_t nonReleasableCount = 0;

        size_t evictingCount = 0;
        size_t movingCount = 0;
        size_t invalidatedCount = 0;

        Stat & operator +=(const Stat & other)
        {
            releasableSize += other.releasableSize;
            releasableCount += other.releasableCount;
            nonReleasableSize += other.nonReleasableSize;
            nonReleasableCount += other.nonReleasableCount;
            evictingCount += other.evictingCount;
            movingCount += other.movingCount;
            invalidatedCount += other.invalidatedCount;
            return *this;
        }

        std::string toString() const;
    };

    Stat totalStat;
    // TODO(enum-reflection): CH uses magic_enum::enum_count<FileSegmentKind>();
    // keep this in sync with FileSegmentKind in FileSegmentInfo.h.
    static constexpr size_t kFileSegmentKindCount = 2;
    std::array<Stat, kFileSegmentKindCount> statByKind{};

    Stat & getStatByKind(FileSegmentKind kind) { return statByKind[static_cast<uint8_t>(kind)]; }
    const Stat & getStatByKind(FileSegmentKind kind) const { return statByKind[static_cast<uint8_t>(kind)]; }

    enum class State
    {
        Releasable,
        NonReleasable,
        Evicting,
        Moving,
        Invalidated,
    };
    void update(size_t size, FileSegmentKind kind, State state);

    FileCacheReserveStat & operator +=(const FileCacheReserveStat & other)
    {
        totalStat += other.totalStat;
        for (size_t i = 0; i < statByKind.size(); ++i)
            statByKind[i] += other.statByKind[i];
        return *this;
    }
};

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using QueryLimit = FileCacheQueryLimit;
    using Priority = IFileCachePriority;
    using PriorityEntry = IFileCachePriority::Entry;
    using QueryContextHolder = FileCacheQueryLimit::QueryContextHolder;
    using OriginInfo = FileCacheOriginInfo;
    using UserID = FileCacheOriginInfo::UserID;
    using Type = FileSegmentKeyType;
    using CachePriorityCreatorFunction = SplitFileCachePriority::CachePriorityCreatorFunction;

    FileCache(const std::string & cache_name, const FileCacheSettings & settings);

    ~FileCache();

    void initialize();

    bool isInitialized() const;

    /// Throws if `!loadMetadataAsynchronously` and there is an exception in `initException`
    void throwInitExceptionIfNeeded();

    const std::string & getBasePath() const;

    bool skipCacheOnDiskFailure() const;

    static const FileCacheOriginInfo & getCommonOrigin();

    static const OriginInfo & getInternalOrigin();

    OriginInfo getCommonOriginWithSegmentKeyType(const std::filesystem::path & filename) const;

    std::string getFileSegmentPath(const Key & key, size_t offset, FileSegmentKind segment_kind, const OriginInfo & origin) const;

    std::string getKeyPath(const Key & key, const OriginInfo & origin) const;

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are held
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolderPtr getOrSet(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_size,
        const CreateFileSegmentSettings & settings,
        size_t file_segments_limit,
        const OriginInfo & origin,
        std::optional<size_t> boundary_alignment_ = std::nullopt);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolderPtr get(
        const Key & key,
        size_t offset,
        size_t size,
        size_t file_segments_limit,
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

    /// Remove file segment by `key` and `offset`. Throws if file segment does not exist.
    void removeFileSegment(const Key & key, size_t offset, const UserID & user_id);

    /// Remove file segment by `key` and `offset`. Does nothing if file segment does not exist.
    void removeFileSegmentIfExists(const Key & key, size_t offset, const UserID & user_id);

    /// Remove files by `key`. Throws if key does not exist.
    void removeKey(const Key & key, const UserID & user_id);

    /// Remove files by `key`.
    void removeKeyIfExists(const Key & key, const UserID & user_id);

    /// Removes files by `path`.
    void removePathIfExists(const std::string & path, const UserID & user_id);

    /// Remove files by `key`.
    void removeAllReleasable(const UserID & user_id);

    std::vector<std::string> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;
    size_t getMaxCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return maxFileSegmentSize; }

    size_t getBackgroundDownloadMaxFileSegmentSize() const { return backgroundDownloadMaxFileSegmentSize.load(); }

    size_t getBoundaryAlignment() const { return boundaryAlignment; }

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
    QueryContextHolderPtr getQueryContextHolder(const std::string & query_id, const FilesystemCacheSettings & settings);

    using IterateFunc = std::function<void(const FileSegmentInfo &)>;
    void iterate(IterateFunc && func, const UserID & user_id);

    using CacheIteratorPtr = CacheMetadata::IteratorPtr;
    CacheIteratorPtr getCacheIterator(const UserID & user_id);

    void applySettingsIfPossible(const FileCacheSettings & new_settings, FileCacheSettings & actual_settings);

    void freeSpaceRatioKeepingThreadFunc();

    const std::string & getName() const { return name; }

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    std::atomic<size_t> maxFileSegmentSize;
    const size_t bypassCacheThreshold;
    const size_t boundaryAlignment;
    std::atomic<size_t> backgroundDownloadMaxFileSegmentSize;
    uint64_t loadMetadataThreads;
    const bool loadMetadataAsynchronously;
    std::atomic<bool> stopLoadingMetadata = false;
    std::unique_ptr<ThreadFromGlobalPool> loadMetadataMainThread; // TODO(threading): CH ThreadFromGlobalPool retained until Velox executor wiring exists.
    const bool writeCachePerUserDirectory;
    const bool allowDynamicCacheResize;
    const size_t dynamicResizeLockWaitMs;

    std::unique_ptr<BackgroundSchedulePoolTaskHolder> keepUpFreeSpaceRatioTask; // TODO(threading): CH 原为按值持有，.cpp 阶段映射到 folly 调度器.
    const double keepCurrentSizeToMaxRatio;
    const double keepCurrentElementsToMaxRatio;
    const size_t keepUpFreeSpaceRemoveBatch;

    // Use IFileCachePriority wrapper in order to separate data/system files into different segments.
    const bool useSplitCache;
    const double splitCacheRatio;

    const bool skipCacheOnDiskFailure_;

    std::string name;

    std::exception_ptr initException;
    std::atomic<bool> isInitialized_ = false;
    folly::once_flag initializeCalled; // TODO(call-once): CH OnceFlag mapped to Velox/folly once_flag.
    mutable std::mutex initMutex;
    std::unique_ptr<StatusFile> statusFile; // TODO(status-file): CH StatusFile retained until Velox status marker exists.
    std::atomic<bool> shutdown = false;
    std::shared_timed_mutex dynamicResizeLock;

    std::atomic<size_t> cacheReserveActiveThreads = 0;

    std::mutex applySettingsMutex;

    CacheMetadata metadata;

    FileCachePriorityPtr mainPriority;
    mutable CachePriorityGuard cacheGuard;
    mutable CachePriorityGuard queueGuard;
    mutable CacheStateGuard cacheStateGuard;

    /// Random checks for cache correctness.
    /// They are heavy, so cannot be done on each cache access.
    struct CheckCacheProbability
    {
        explicit CheckCacheProbability(double probability, uint64_t seed = 0);

        bool doCheck();

    private:
        folly::Random::DefaultGenerator rndgen; // TODO(random): CH uses pcg64_fast; Velox uses folly::Random.
        std::bernoulli_distribution distribution;
        std::mutex mutex;
    };
    CheckCacheProbability checkCacheProbability;

    /**
     * A QueryLimit allows to control cache write limit per query.
     * E.g. if a query needs n bytes from cache, but it has only k bytes, where 0 <= k <= n
     * then allowed loaded cache size is std::min(n - k, max_query_cache_size).
     */
    FileCacheQueryLimitPtr queryLimit;

    void initializeImpl(bool load_metadata);

    void assertInitialized() const;
    void assertCacheCorrectness();
    void assertCacheCorrectnessWithProbability();

    void loadMetadata();
    void loadMetadataImpl();
    void loadMetadataForKey(const std::filesystem::path & key_dir, const OriginInfo & origin);

    /// Get all file segments from cache which intersect with `range`.
    /// If `file_segments_limit` > 0, return no more than first file_segments_limit
    /// file segments.
    FileSegments getImpl(
        const LockedKey & locked_key,
        const FileSegment::Range & range,
        size_t file_segments_limit) const;

    /// Split range into subranges by maxFileSegmentSize,
    /// each subrange size must be less or equal to maxFileSegmentSize.
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
        size_t maxSize = 0;
        size_t maxElements = 0;
        double slruSizeRatio = 0;
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
        const EvictionInfo & main_eviction_info,
        const EvictionInfo * query_eviction_info,
        FileSegment & file_segment,
        const OriginInfo & origin_info,
        const IFileCachePriority::IteratorPtr & main_priority_iterator,
        FileCacheReserveStat & reserve_stat,
        EvictionCandidates & eviction_candidates,
        IFileCachePriority::InvalidatedEntriesInfos & invalidated_entries,
        Priority * query_priority,
        std::string & failure_reason);
};

} // namespace facebook::velox::ch
