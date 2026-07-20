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

#include "velox/buffer/Buffer.h"
#include "velox/ch/Common/ClickHouseAliases.h"
#include "velox/ch/Common/ClickHouseAssert.h"
#include "velox/ch/Common/SharedMutex.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheOriginInfo.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd_internal.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/ShardedMap.h"

#include <folly/container/F14Map.h>

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <system_error>
#include <variant>
#include <vector>

namespace facebook::velox::memory
{
class MemoryPool;
}

namespace facebook::velox::ch
{

class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;
class DownloadQueue;
using DownloadQueuePtr = std::shared_ptr<DownloadQueue>;

class CacheMetadata;

struct FileSegmentMetadata
{
    using Priority = IFileCachePriority;

    FileSegmentMetadata(const FileSegmentMetadata &) = delete;
    FileSegmentMetadata & operator=(const FileSegmentMetadata &) = delete;

    explicit FileSegmentMetadata(FileSegmentPtr && file_segment_);

    /// CH uses isSharedPtrUnique; the reference-count expectation is that the
    /// metadata bucket holds the only strong reference.
    bool releasable() const { return file_segment.use_count() == 1; }

    size_t size() const;

    bool isEvictingOrRemoved(const LockedKey & lock) const { return isRemoved(lock) || isEvicting(lock); }

    bool isRemoved(const LockedKey &) const { return removed; }

    bool isEvicting(const LockedKey &) const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            return false;
        const auto entry_state = iterator->getEntry()->getState();
        return entry_state == Priority::Entry::State::Evicting;
    }

    void setRemovedFlag(const LockedKey &, bool value = true)
    {
        removed = value;
        chassert(!getQueueIterator());
    }

    void setEvictingFlag(const LockedKey & lock) const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            VELOX_FAIL("Iterator is not set");
        iterator->getEntry()->setEvictingFlag(lock);
    }

    void resetEvictingFlag() const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            VELOX_FAIL("Iterator is not set");

        const auto & entry = iterator->getEntry();
        chassert(size() == entry->size);
        entry->resetFlag(Priority::Entry::State::Evicting);
    }

    Priority::IteratorPtr getQueueIterator() const { return file_segment->getQueueIterator(); }

    FileSegmentPtr file_segment;

private:
    /// If removed=true, then iterator is invalid.
    bool removed = false;
};

using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;


struct KeyMetadata : private std::map<size_t, FileSegmentMetadataPtr>,
                     public std::enable_shared_from_this<KeyMetadata>
{
    friend class CacheMetadata;
    friend struct LockedKey;

    using Key = FileCacheKey;
    using iterator = std::map<size_t, FileSegmentMetadataPtr>::iterator;
    using OriginInfo = FileCacheOriginInfo;
    using OriginInfoPtr = std::shared_ptr<const OriginInfo>;
    using UserID = OriginInfo::UserID;

    KeyMetadata(const KeyMetadata &) = delete;
    KeyMetadata & operator=(const KeyMetadata &) = delete;

    KeyMetadata(
        const Key & key_,
        OriginInfoPtr origin_,
        const CacheMetadata * cache_metadata_,
        bool created_base_directory_ = false);

    enum class KeyState : uint8_t
    {
        ACTIVE,
        REMOVING,
        REMOVED,
    };

    const Key key;
    /// Shared across all keys with the same origin.
    const OriginInfoPtr origin;

    LockedKeyPtr lock();

    /// Fails if key is not in ACTIVE state (REMOVING or REMOVED).
    LockedKeyPtr tryLock();

    [[nodiscard]] std::error_code createBaseDirectory();

    std::string getPath() const;

    std::string getFileSegmentPath(const FileSegment & file_segment) const;

    std::string getFileSegmentPath(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size) const;

    bool checkAccess(const UserID & user_id_) const;

    void assertAccess(const UserID & user_id_) const;

    /// Used only during loadMetadata() on server startup where there is no
    /// concurrency on the Key; this Unlocked version avoids a KeyGuard::Lock.
    template <class... Args>
    auto emplaceUnlocked(Args &&... args) { return emplace(std::forward<Args>(args)...); }
    size_t sizeUnlocked() const { return size(); }

    KeyState getState();

private:
    const CacheMetadata * cache_metadata;

    KeyState key_state = KeyState::ACTIVE;
    KeyGuard guard;

    std::atomic<bool> created_base_directory = false;

    LockedKeyPtr lockNoStateCheck();
    LoggerPtr logger() const;
    bool addToDownloadQueue(FileSegmentPtr file_segment);
    void addToCleanupQueue();
    /// Forwards to the CacheMetadata-injected opened-file invalidation callback
    /// (replaces CH's OpenedFileCache::instance().remove()); used by
    /// LockedKey::removeFileSegmentImpl after deleting a segment file.
    void invalidateOpenedFile(const std::string & path) const;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;


class CacheMetadata
{
    friend struct KeyMetadata;
    class IteratorImpl;
    class BatchedIteratorImpl;
    using IteratorImplPtr = std::shared_ptr<IteratorImpl>;
    using BatchedIteratorImplPtr = std::shared_ptr<BatchedIteratorImpl>;

public:
    using Key = FileCacheKey;
    using IterateFunc = std::function<void(LockedKey &)>;
    using OriginInfo = FileCacheOriginInfo;
    using UserID = OriginInfo::UserID;

    /// Manager-injected runtime dependencies (Task 013 owns these): the shared
    /// physical worker pool for download/cleanup workers, the MemoryPool that
    /// charges the reusable background-download buffer, the reserve-space lock
    /// timeout (from FileCacheConfig, not global Context), and the opened-file
    /// invalidation callback (replaces OpenedFileCache::instance().remove()).
    CacheMetadata(
        const std::string & path_,
        size_t background_download_queue_size_limit_,
        size_t background_download_threads_,
        bool write_cache_per_user_directory_,
        FileCacheWorkerPool & worker_pool_,
        velox::memory::MemoryPool * memory_pool_,
        size_t reserve_space_wait_lock_timeout_milliseconds_,
        std::function<void(const std::string &)> invalidate_opened_file_,
        const UserID & common_user_id_);

    CacheMetadata(const CacheMetadata &) = delete;
    CacheMetadata & operator=(const CacheMetadata &) = delete;

    virtual ~CacheMetadata();

    void startup();

    bool isEmpty() const;

    const String & getBaseDirectory() const { return path; }

    String getKeyPath(const Key & key, const OriginInfo & origin) const;

    String getFileSegmentPath(
        const Key & key,
        size_t offset,
        FileSegmentKind segment_kind,
        const OriginInfo & origin,
        std::optional<size_t> size = std::nullopt) const;

    void iterate(IterateFunc && func, const UserID & user_id);

    class Iterator;
    using IteratorPtr = std::unique_ptr<Iterator>;
    IteratorPtr getIterator(const UserID & user_id);

    enum class KeyNotFoundPolicy : uint8_t
    {
        THROW,
        THROW_LOGICAL,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    KeyMetadataPtr getKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        const OriginInfo & origin,
        bool is_initial_load = false);

    LockedKeyPtr lockKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        const OriginInfo & origin,
        bool is_initial_load = false);

    void removeKey(const Key & key, bool if_exists, const UserID & user_id);

    /// Returns true if the client was fully purged; false if any key kept
    /// non-releasable segments and survived.
    bool removeAllKeys(const UserID & user_id);

    void shutdown();

    bool setBackgroundDownloadThreads(size_t threads_num);
    size_t getBackgroundDownloadThreads() const { return download_threads.size(); }

    bool setBackgroundDownloadQueueSizeLimit(size_t size);

    bool isBackgroundDownloadEnabled();

    void setClientAccessCallback(std::function<void(const UserID &)> callback) { on_client_access = std::move(callback); }

    static String getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size = std::nullopt);

private:
    static constexpr size_t buckets_num = 1024;

    const std::string path;
    const CleanupQueuePtr cleanup_queue;
    const DownloadQueuePtr download_queue;
    const bool write_cache_per_user_directory;
    /// Manager-injected runtime dependencies.
    FileCacheWorkerPool & worker_pool;
    velox::memory::MemoryPool * memory_pool;
    const size_t reserve_space_wait_lock_timeout_milliseconds;
    const std::function<void(const std::string &)> invalidate_opened_file;
    const UserID common_user_id;
    std::function<void(const UserID &)> on_client_access;

    LoggerPtr log;
    mutable SharedMutex key_prefix_directory_mutex;

    using OriginInfoPtr = KeyMetadata::OriginInfoPtr;

    /// SD4: F14 bucket accepted only under the no-reference-across-mutation
    /// proof: every bucket accessor copies the KeyMetadataPtr (a shared_ptr,
    /// stable across rehash) out of the bucket before releasing the per-bucket
    /// guard; no raw reference/iterator into a bucket may cross a mutating call.
    struct MetadataBucket : public folly::F14FastMap<FileCacheKey, KeyMetadataPtr, FileCacheKeyHash>
    {
        CacheMetadataGuard::Lock lock() const;

    private:
        mutable CacheMetadataGuard guard;
    };
    using MetadataBuckets = std::vector<MetadataBucket>;
    MetadataBuckets metadata_buckets{buckets_num};

    OriginInfoPtr getOrCreateSharedOrigin(const OriginInfo & origin);

    void removeSharedOrigins(const UserID & user_id);

    /// SD1: origin dedup pool. Callbacks copy out shared_ptr values, never
    /// map-slot references.
    mutable FileCacheUtils::ShardedMap<OriginPoolKey, OriginInfoPtr, 32, OriginPoolKeyHash> origins;

    struct DownloadThread
    {
        std::unique_ptr<FileCacheWorker> thread;
        bool stop_flag{false};
    };

    std::atomic<size_t> download_threads_num;
    std::vector<std::shared_ptr<DownloadThread>> download_threads;
    std::unique_ptr<FileCacheWorker> cleanup_thread;

    MetadataBucket & getMetadataBucket(const Key & key);
    /// CH uses std::optional<Memory<>>; mapped to a reusable pool-charged
    /// BufferPtr (empty until first allocation).
    void downloadImpl(FileSegment & file_segment, BufferPtr & memory) const;
    MetadataBucket::iterator removeEmptyKey(
        MetadataBucket & bucket,
        MetadataBucket::iterator it,
        LockedKey &,
        const CacheMetadataGuard::Lock &);

    void downloadThreadFunc(const bool & stop_flag);

    void cleanupThreadFunc();
};

class CacheMetadata::Iterator
{
public:
    using Impl = std::variant<CacheMetadata::IteratorImplPtr, CacheMetadata::BatchedIteratorImplPtr>;
    explicit Iterator(const UserID & user_id_, MetadataBuckets & metadata_buckets_);

    using OnFileSegmentFunc = std::function<void(const FileSegmentInfo &)>;
    /// Execute func for one more file segment. Not thread-safe.
    bool next(OnFileSegmentFunc func);
    /// Execute func for a batch of file segments. Sequential calls may run on
    /// different threads (no concurrent calls).
    bool nextBatch(OnFileSegmentFunc func);

protected:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    std::optional<Impl> impl;
};

/// A download-queue work item. Carries a weak_ptr<FileSegment> in addition to
/// key+offset: key+offset alone would accept a new segment created at the same
/// offset after the original was deleted.
struct DownloadInfo
{
    FileCacheKey key;
    uint64_t offset = 0;
    std::weak_ptr<FileSegment> segment;
};

struct LockedKey
{
    using Key = FileCacheKey;

    LockedKey(const LockedKey &) = delete;
    LockedKey & operator=(const LockedKey &) = delete;

    explicit LockedKey(std::shared_ptr<KeyMetadata> key_metadata_);

    ~LockedKey();

    const Key & getKey() const { return key_metadata->key; }

    auto begin() const { return key_metadata->begin(); }
    auto rbegin() const { return key_metadata->rbegin(); }

    auto end() const { return key_metadata->end(); }
    auto rend() const { return key_metadata->rend(); }

    bool empty() const { return key_metadata->empty(); }
    auto lower_bound(size_t size) const { return key_metadata->lower_bound(size); }
    template <class... Args>
    auto emplace(Args &&... args) { return key_metadata->emplace(std::forward<Args>(args)...); }

    std::shared_ptr<const FileSegmentMetadata> getByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> getByOffset(size_t offset);

    std::shared_ptr<const FileSegmentMetadata> tryGetByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> tryGetByOffset(size_t offset);

    KeyMetadata::KeyState getKeyState() const { return key_metadata->key_state; }

    std::shared_ptr<const KeyMetadata> getKeyMetadata() const { return key_metadata; }
    std::shared_ptr<KeyMetadata> getKeyMetadata() { return key_metadata; }

    bool removeAllFileSegments();

    KeyMetadata::iterator removeFileSegment(
        size_t offset,
        const FileSegmentGuard::Lock &,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    KeyMetadata::iterator removeFileSegment(
        size_t offset,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    KeyMetadata::iterator removeFileSegmentIfExists(
        size_t offset,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    bool addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    std::optional<FileSegment::Range> hasIntersectingRange(const FileSegment::Range & range) const;

    void removeFromCleanupQueue();

    void markAsRemoved();

    std::vector<FileSegment::Info> sync();

    std::string toString() const;

private:
    KeyMetadata::iterator removeFileSegmentImpl(
        KeyMetadata::iterator it,
        const FileSegmentGuard::Lock &,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    /// Declaration order determines destruction order: `lock` must be
    /// destroyed BEFORE `key_metadata` drops its shared reference.
    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
};

} // namespace facebook::velox::ch

/// `KeyMetadata::KeyState` is formatted in log/assert messages (e.g.
/// `EvictionCandidates.cpp`). ClickHouse relies on a `magic_enum`-backed fmt
/// formatter; this explicit specialization keeps the same names without that
/// dependency (mirrors the `keyStateName` switch used inside `Metadata.cpp`).
template <>
struct fmt::formatter<facebook::velox::ch::KeyMetadata::KeyState>
    : fmt::formatter<std::string_view>
{
    template <typename FormatCtx>
    auto format(facebook::velox::ch::KeyMetadata::KeyState state, FormatCtx & ctx) const
    {
        using KeyState = facebook::velox::ch::KeyMetadata::KeyState;
        std::string_view name = "";
        switch (state)
        {
            case KeyState::ACTIVE:
                name = "ACTIVE";
                break;
            case KeyState::REMOVING:
                name = "REMOVING";
                break;
            case KeyState::REMOVED:
                name = "REMOVED";
                break;
        }
        return fmt::formatter<std::string_view>::format(name, ctx);
    }
};
