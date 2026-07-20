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

#include "velox/ch/Common/ClickHouseAssert.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/SharedMutex.h"
#include "velox/ch/Common/ThreadPool.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd_internal.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
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

namespace facebook::velox::ch
{

/// Internal metadata worker queues. Their concrete definitions live in `Metadata.cpp`; they are
/// handwritten types (NOT `FileCacheBoundedQueue`).
class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;
class DownloadQueue;
using DownloadQueuePtr = std::shared_ptr<DownloadQueue>;

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;
class CacheMetadata;

struct FileSegmentMetadata
{
    using Priority = IFileCachePriority;

    /// Copy/move are disabled (CH `boost::noncopyable`).
    FileSegmentMetadata(const FileSegmentMetadata &) = delete;
    FileSegmentMetadata & operator=(const FileSegmentMetadata &) = delete;

    explicit FileSegmentMetadata(FileSegmentPtr && file_segment_);

    /// Releasable iff this metadata holds the only reference to the file segment.
    bool releasable() const { return file_segment.use_count() == 1; }

    size_t size() const;

    bool isEvictingOrRemoved(const LockedKey & lock) const { return isRemoved(lock) || isEvicting(lock); }

    /// Whether queue entry is removed/evicted.
    bool isRemoved(const LockedKey &) const { return removed; }

    /// Whether queue entry is in evicting state.
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
            throwFileCacheException("Iterator is not set");
        iterator->getEntry()->setEvictingFlag(lock);
    }

    void resetEvictingFlag() const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            throwFileCacheException("Iterator is not set");

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

    /// Copy/move are disabled (CH `boost::noncopyable`).
    KeyMetadata(const KeyMetadata &) = delete;
    KeyMetadata & operator=(const KeyMetadata &) = delete;

    using Key = FileCacheKey;
    using iterator = std::map<size_t, FileSegmentMetadataPtr>::iterator;
    using OriginInfo = FileCacheOriginInfo;
    using OriginInfoPtr = std::shared_ptr<const OriginInfo>;
    using UserID = OriginInfo::UserID;

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
    /// Shared across all keys with the same origin, since OriginInfo is immutable
    /// and distinct origins are very few. See CacheMetadata::getOrCreateSharedOrigin.
    const OriginInfoPtr origin;

    LockedKeyPtr lock();

    /// Will only fail if key is not in ACTIVE state, e.g. REMOVING or REMOVED.
    LockedKeyPtr tryLock();

    [[nodiscard]] std::error_code createBaseDirectory();

    std::string getPath() const;

    std::string getFileSegmentPath(const FileSegment & file_segment) const;

    /// Build the path for a segment file directly from its components.
    /// When `size` is set, the size is encoded into the file name (`<offset>_<size>`),
    /// which lets startup metadata loading avoid a `stat` per file.
    std::string getFileSegmentPath(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size) const;

    bool checkAccess(const UserID & user_id_) const;

    void assertAccess(const UserID & user_id_) const;

    /// This method is used for loadMetadata() on server startup,
    /// where we know there is no concurrency on Key and we do not want therefore taking a KeyGuard::Lock,
    /// therefore we use this Unlocked version. This method should not be used anywhere else.
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
    /// Copy/move are disabled (CH `boost::noncopyable`).
    CacheMetadata(const CacheMetadata &) = delete;
    CacheMetadata & operator=(const CacheMetadata &) = delete;

    using Key = FileCacheKey;
    using IterateFunc = std::function<void(LockedKey &)>;
    using OriginInfo = FileCacheOriginInfo;
    using UserID = OriginInfo::UserID;

    explicit CacheMetadata(
        const std::string & path_,
        size_t background_download_queue_size_limit_,
        size_t background_download_threads_,
        bool write_cache_per_user_directory_);

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
    /// non-releasable (held) segments and survived — retry on a later sweep.
    bool removeAllKeys(const UserID & user_id);

    void shutdown();

    bool setBackgroundDownloadThreads(size_t threads_num);
    size_t getBackgroundDownloadThreads() const { return download_threads.size(); }

    bool setBackgroundDownloadQueueSizeLimit(size_t size);

    bool isBackgroundDownloadEnabled();

    void setClientAccessCallback(std::function<void(const UserID &)> callback) { on_client_access = std::move(callback); }

private:
    static constexpr size_t buckets_num = 1024;

    const std::string path;
    const CleanupQueuePtr cleanup_queue;
    const DownloadQueuePtr download_queue;
    const bool write_cache_per_user_directory;
    std::function<void(const UserID &)> on_client_access;

    LoggerPtr log;
    mutable SharedMutex key_prefix_directory_mutex;

    using OriginInfoPtr = KeyMetadata::OriginInfoPtr;

    /// F14 metadata bucket (SD4): iterators/mapped-value references never escape while holding a
    /// bucket lock, and the stored `shared_ptr<KeyMetadata>` keeps the pointee stable across
    /// bucket mutations.
    struct MetadataBucket : public folly::F14FastMap<FileCacheKey, KeyMetadataPtr, FileCacheKeyHash>
    {
        CacheMetadataGuard::Lock lock() const;

    private:
        mutable CacheMetadataGuard guard;
    };
    using MetadataBuckets = std::vector<MetadataBucket>;
    MetadataBuckets metadata_buckets{buckets_num};

    /// Return a deduplicated immutable origin, shared by all keys with the same OriginPoolKey.
    /// The pool is sharded by a hash of the origin's identity (i.e. by client), not by the
    /// cache key, so each distinct origin is stored exactly once instead of being duplicated
    /// across the (much more numerous) key buckets.
    OriginInfoPtr getOrCreateSharedOrigin(const OriginInfo & origin);

    /// Drop every shared origin owned by this client from the dedup pool. Called once all of the
    /// client's keys are removed (see removeAllKeys), so the pool does not leak entries for clients
    /// that come and go, in particular under idle-client TTL eviction.
    void removeSharedOrigins(const UserID & user_id);

    mutable FileCacheUtils::ShardedMap<OriginPoolKey, OriginInfoPtr, 32, OriginPoolKeyHash> origins;

    struct DownloadThread
    {
        std::unique_ptr<ThreadFromGlobalPool> thread;
        bool stop_flag{false};
    };

    std::atomic<size_t> download_threads_num;
    std::vector<std::shared_ptr<DownloadThread>> download_threads;
    std::unique_ptr<ThreadFromGlobalPool> cleanup_thread;

    static String getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size = std::nullopt);

    MetadataBucket & getMetadataBucket(const Key & key);
    /// D3: CH `std::optional<Memory<>>` (owned, `DBMS_DEFAULT_BUFFER_SIZE`) maps to a
    /// MemoryPool-charged `CacheBuffer` reused across background downloads.
    void downloadImpl(FileSegment & file_segment, std::optional<CacheBuffer> & memory) const;
    MetadataBucket::iterator removeEmptyKey(
        MetadataBucket & bucket,
        MetadataBucket::iterator it,
        LockedKey &,
        const CacheMetadataGuard::Lock &);

    void downloadThreadFunc(const bool & stop_flag);

    /// Firstly, this cleanup does not delete cache files,
    /// but only empty keys from cache_metadata_map and key (prefix) directories from fs.
    /// Secondly, it deletes those only if arose as a result of
    /// (1) eviction in FileCache::tryReserve();
    /// (2) removal of cancelled non-downloaded file segments after FileSegment::complete().
    /// which does not include removal of cache files because of FileCache::removeKey/removeAllKeys,
    /// triggered by removal of source files from objects storage.
    /// E.g. number of elements submitted to background cleanup should remain low.
    void cleanupThreadFunc();
};

class CacheMetadata::Iterator
{
public:
    using Impl = std::variant<CacheMetadata::IteratorImplPtr, CacheMetadata::BatchedIteratorImplPtr>;
    explicit Iterator(const UserID & user_id_, MetadataBuckets & metadata_buckets_);

    using OnFileSegmentFunc = std::function<void(const FileSegmentInfo &)>;
    /// Execute func for one more file segment.
    /// Cannot be used from different threads.
    bool next(OnFileSegmentFunc func);
    /// Execute func for a batch of file segments.
    /// Safe to be used from different threads.
    bool nextBatch(OnFileSegmentFunc func);

protected:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    std::optional<Impl> impl;
};

/**
 * `LockedKey` is an object which makes sure that as long as it exists the following is true:
 * 1. the key cannot be removed from cache
 *    (Why: this LockedKey locks key metadata mutex in ctor, unlocks it in dtor, and so
 *    when key is going to be deleted, key mutex is also locked.
 *    Why it cannot be the other way round? E.g. that ctor of LockedKey locks the key
 *    right after it was deleted? This case it taken into consideration in createLockedKey())
 * 2. the key cannot be modified, e.g. new offsets cannot be added to key; already existing
 *    offsets cannot be deleted from the key
 * And also provides some methods which allow the owner of this LockedKey object to do such
 * modification of the key (adding/deleting offsets) and deleting the key from cache.
 */
struct LockedKey
{
    using Key = FileCacheKey;

    /// Copy/move are disabled (CH `boost::noncopyable`).
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
    auto lower_bound(size_t size) const { return key_metadata->lower_bound(size); } /// NOLINT
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

    /// Declaration order determines destruction order:
    /// `lock` must be destructed before `key_metadata` drops its shared reference.
    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock;
};

}
