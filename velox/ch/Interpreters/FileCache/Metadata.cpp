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
#include "velox/ch/Interpreters/FileCache/Metadata.h"

#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentInfo.h"

#include <folly/ScopeGuard.h>

#include <filesystem>
#include <queue>
#include <unordered_set>

namespace fs = std::filesystem;

namespace facebook::velox::ch
{

namespace
{
std::string_view keyStateName(KeyMetadata::KeyState state)
{
    switch (state)
    {
        case KeyMetadata::KeyState::ACTIVE: return "ACTIVE";
        case KeyMetadata::KeyState::REMOVING: return "REMOVING";
        case KeyMetadata::KeyState::REMOVED: return "REMOVED";
    }
    return "UNKNOWN";
}
}

FileSegmentMetadata::FileSegmentMetadata(FileSegmentPtr && file_segment_)
    : file_segment(std::move(file_segment_))
{
    switch (file_segment->state())
    {
        case FileSegment::State::DOWNLOADED:
        {
            chassert(file_segment->getQueueIterator());
            break;
        }
        case FileSegment::State::EMPTY:
        case FileSegment::State::DOWNLOADING:
        {
            break;
        }
        default:
            throwFileCacheException(
                "Can create file segment with either EMPTY, DOWNLOADED, DOWNLOADING state, got: {}",
                FileSegment::stateToString(file_segment->state()));
    }
}

size_t FileSegmentMetadata::size() const
{
    return file_segment->getReservedSize();
}

KeyMetadata::KeyMetadata(
    const Key & key_,
    OriginInfoPtr origin_,
    const CacheMetadata * cache_metadata_,
    bool created_base_directory_)
    : key(key_)
    , origin(std::move(origin_))
    , cache_metadata(cache_metadata_)
    , created_base_directory(created_base_directory_)
{
    chassert(origin);

    if (*origin == FileCache::getInternalOrigin())
        throwFileCacheException("Cannot create key metadata with internal user id");

    if (!origin->weight.has_value())
        throwFileCacheException("Cannot create key metadata without user weight");

    chassert(!created_base_directory || fs::exists(getPath()));
}

bool KeyMetadata::checkAccess(const UserID & user_id_) const
{
    return user_id_ == origin->user_id || user_id_ == FileCache::getInternalOrigin().user_id;
}

void KeyMetadata::assertAccess(const UserID & user_id_) const
{
    if (!checkAccess(user_id_))
    {
        throwFileCacheException("Metadata for key {} belongs to another user", key.toString());
    }
}

CacheMetadata::OriginInfoPtr CacheMetadata::getOrCreateSharedOrigin(const OriginInfo & origin)
{
    OriginPoolKey pool_key{origin.user_id, origin.weight, origin.segment_type};
    return origins.withShard(pool_key, [&](auto & map) -> OriginInfoPtr
    {
        auto it = map.find(pool_key);
        if (it == map.end())
            it = map.emplace(pool_key, std::make_shared<const OriginInfo>(origin)).first;
        return it->second;
    });
}

void CacheMetadata::removeSharedOrigins(const UserID & user_id)
{
    origins.forEachShard([&](auto & map)
    {
        for (auto it = map.begin(); it != map.end();)
        {
            if (it->first.user_id == user_id)
                it = map.erase(it);
            else
                ++it;
        }
    });
}

LockedKeyPtr KeyMetadata::lock()
{
    auto locked = tryLock();
    if (locked)
        return locked;

    throwFileCacheException("Cannot lock key {} (state: {})", key.toString(), keyStateName(key_state));
}

LockedKeyPtr KeyMetadata::tryLock()
{
    auto locked = lockNoStateCheck();
    if (key_state == KeyMetadata::KeyState::ACTIVE)
        return locked;

    return nullptr;
}

LockedKeyPtr KeyMetadata::lockNoStateCheck()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);
    return std::make_shared<LockedKey>(shared_from_this());
}

KeyMetadata::KeyState KeyMetadata::getState()
{
    auto locked = lockNoStateCheck();
    return key_state;
}

std::error_code KeyMetadata::createBaseDirectory()
{
    if (created_base_directory.load())
        return {};

    std::shared_lock lock(cache_metadata->key_prefix_directory_mutex);

    if (created_base_directory.load(std::memory_order_relaxed))
        return {};

    std::error_code ec;
    fs::create_directories(getPath(), ec);

    if (!ec)
    {
        created_base_directory.store(true);
        ProfileEvents::increment(ProfileEvents::FilesystemCacheCreatedKeyDirectories);
    }
    else if (ec != std::errc::no_space_on_device && ec != std::errc::too_many_files_open)
        LOG_TRACE(cache_metadata->log, "Failed to create base directory for key {}, {}", key, ec.message());

    return ec;
}

std::string KeyMetadata::getPath() const
{
    return cache_metadata->getKeyPath(key, *origin);
}

std::string KeyMetadata::getFileSegmentPath(const FileSegment & file_segment) const
{
    std::optional<size_t> size;
    if (file_segment.hasSizeInFileName())
        size = file_segment.range().size();
    return cache_metadata->getFileSegmentPath(key, file_segment.offset(), file_segment.getKind(), *origin, size);
}

std::string KeyMetadata::getFileSegmentPath(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size) const
{
    return cache_metadata->getFileSegmentPath(key, offset, segment_kind, *origin, size);
}

LoggerPtr KeyMetadata::logger() const
{
    return cache_metadata->log;
}

CacheMetadata::CacheMetadata(
    const std::string & path_,
    size_t background_download_queue_size_limit_,
    size_t background_download_threads_,
    bool write_cache_per_user_directory_,
    FileCacheWorkerPool & worker_pool_,
    size_t reserve_space_wait_lock_timeout_milliseconds_)
    : path(path_)
    , cleanup_queue(std::make_shared<CleanupQueue>())
    , download_queue(std::make_shared<DownloadQueue>(background_download_queue_size_limit_))
    , write_cache_per_user_directory(write_cache_per_user_directory_)
    , worker_pool(worker_pool_)
    , reserve_space_wait_lock_timeout_milliseconds(reserve_space_wait_lock_timeout_milliseconds_)
    , log(getLogger("CacheMetadata"))
    , origins(ProfileEvents::FilesystemCacheLockOriginPoolMicroseconds)
    , download_threads_num(background_download_threads_)
{
}

CacheMetadata::~CacheMetadata() = default;

String CacheMetadata::getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size)
{
    switch (segment_kind)
    {
        case FileSegmentKind::Ephemeral:
            return std::to_string(offset) + "_temporary";
        case FileSegmentKind::Regular:
            if (size.has_value())
                return std::to_string(offset) + "_" + std::to_string(*size);
            return std::to_string(offset);
    }
    return std::to_string(offset);
}

String CacheMetadata::getFileSegmentPath(
    const Key & key,
    size_t offset,
    FileSegmentKind segment_kind,
    const OriginInfo & origin,
    std::optional<size_t> size) const
{
    return fs::path(getKeyPath(key, origin)) / getFileNameForFileSegment(offset, segment_kind, size);
}

String CacheMetadata::getKeyPath(const Key & key, const OriginInfo & origin) const
{
    const auto key_str = key.toString();
    const auto key_type_prefix = getKeyTypePrefix(origin.segment_type);
    if (write_cache_per_user_directory)
        return fs::path(path) / key_type_prefix / fmt::format("{}.{}", origin.user_id, origin.weight.value()) / key_str.substr(0, 3) / key_str;

    return fs::path(path) / key_type_prefix / key_str.substr(0, 3) / key_str;
}

CacheMetadataGuard::Lock CacheMetadata::MetadataBucket::lock() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockMetadataMicroseconds);
    return guard.lock();
}

CacheMetadata::MetadataBucket & CacheMetadata::getMetadataBucket(const Key & key)
{
    const auto bucket = static_cast<size_t>(key.key % buckets_num);
    return metadata_buckets[bucket];
}

LockedKeyPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey & key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo & origin,
    bool is_initial_load)
{
    auto key_metadata = getKeyMetadata(key, key_not_found_policy, origin, is_initial_load);
    if (!key_metadata)
        return nullptr;

    {
        auto locked_metadata = key_metadata->lockNoStateCheck();
        const auto key_state = locked_metadata->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
            return locked_metadata;

        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throwFileCacheException("No such key `{}` in cache", key);
        if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL)
            throwFileCacheException("No such key `{}` in cache", key);

        if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        if (key_state == KeyMetadata::KeyState::REMOVING)
        {
            locked_metadata->removeFromCleanupQueue();
            LOG_DEBUG(log, "Removal of key {} is cancelled", key);
            return locked_metadata;
        }

        chassert(key_state == KeyMetadata::KeyState::REMOVED);
        chassert(key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY);
    }

    /// Now we are at the case when the key was removed but we need to return empty key. Retry.
    return lockKeyMetadata(key, key_not_found_policy, origin);
}

KeyMetadataPtr CacheMetadata::getKeyMetadata(
    const Key & key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo & origin,
    bool is_initial_load)
{
    KeyMetadataPtr result;
    {
        auto & bucket = getMetadataBucket(key);
        auto lock = bucket.lock();

        auto it = bucket.find(key);
        if (it == bucket.end())
        {
            if (key_not_found_policy == KeyNotFoundPolicy::THROW)
                throwFileCacheException("No such key `{}` in cache", key);
            if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL)
                throwFileCacheException("No such key `{}` in cache", key);
            if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
                return nullptr;

            it = bucket.emplace(
                key, std::make_shared<KeyMetadata>(key, getOrCreateSharedOrigin(origin), this, is_initial_load)).first;
        }

        it->second->assertAccess(origin.user_id);
        result = it->second;
    }

    /// Refresh idle-client TTL after releasing the bucket lock. Skip internal and empty ids.
    /// Common-id filtering is applied by the injected callback itself (the SCC phase has no
    /// FileCache back-reference here; `getCommonOrigin` is an instance method on FileCache).
    if (result && on_client_access)
    {
        const auto & user_id = origin.user_id;
        if (!user_id.empty() && user_id != FileCache::getInternalOrigin().user_id)
        {
            on_client_access(user_id);
        }
    }

    return result;
}

bool CacheMetadata::isEmpty() const
{
    for (const auto & bucket : metadata_buckets)
        if (!bucket.empty())
            return false;
    return true;
}

void CacheMetadata::iterate(IterateFunc && func, const KeyMetadata::UserID & user_id)
{
    for (auto & bucket : metadata_buckets)
    {
        auto lk = bucket.lock();
        for (auto & [key, key_metadata] : bucket)
        {
            if (!key_metadata->checkAccess(user_id))
                continue;

            auto locked_key = key_metadata->lockNoStateCheck();
            const auto key_state = locked_key->getKeyState();

            if (key_state == KeyMetadata::KeyState::ACTIVE)
            {
                func(*locked_key);
                continue;
            }
            if (key_state == KeyMetadata::KeyState::REMOVING)
                continue;

            throwFileCacheException("Cannot lock key {}: key does not exist", key_metadata->key);
        }
    }
}

class CacheMetadata::IteratorImpl
{
public:
    IteratorImpl(MetadataBuckets & metadata_buckets_, const UserID & user_id_)
        : user_id(user_id_)
        , metadata_buckets(metadata_buckets_)
        , bucket_it(metadata_buckets_.begin())
    {
    }

    bool next(Iterator::OnFileSegmentFunc func)
    {
        while (true)
        {
            if (bucket_it == metadata_buckets.end())
                return false;

            if (!bucket_lock)
                bucket_lock = bucket_it->lock();

            if (!key_it.has_value())
                key_it = bucket_it->begin();

            if (key_it.value() == bucket_it->end())
            {
                ++bucket_it;
                bucket_lock.reset();

                key_it.reset();
                key_lock.reset();

                file_segment_it.reset();
                continue;
            }

            const auto & key = key_it.value()->second;

            if (!key_lock)
            {
                if (!key->checkAccess(user_id))
                {
                    ++key_it.value();
                    continue;
                }

                key_lock = key->tryLock();
                if (!key_lock)
                {
                    ++key_it.value();
                    continue;
                }
            }

            if (!file_segment_it.has_value())
                file_segment_it = key->begin();

            if (file_segment_it.value() == key->end())
            {
                ++key_it.value();
                key_lock.reset();

                file_segment_it.reset();
                continue;
            }

            func(FileSegment::getInfo(file_segment_it.value()->second->file_segment));
            ++(file_segment_it.value());
            return true;
        }
    }

private:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    MetadataBuckets::iterator bucket_it;
    std::optional<MetadataBucket::iterator> key_it;
    std::optional<KeyMetadata::iterator> file_segment_it;

    std::optional<CacheMetadataGuard::Lock> bucket_lock;
    LockedKeyPtr key_lock;
};

class CacheMetadata::BatchedIteratorImpl
{
public:
    BatchedIteratorImpl(MetadataBuckets & metadata_buckets_, const UserID & user_id_)
        : user_id(user_id_)
        , metadata_buckets(metadata_buckets_)
        , bucket_it(metadata_buckets_.begin())
    {
    }

    bool next(Iterator::OnFileSegmentFunc func)
    {
        bool result = false;
        while (bucket_it != metadata_buckets.end())
        {
            auto bucket_lock = bucket_it->lock();
            for (const auto & [unused_key, key_metadata] : *bucket_it)
            {
                if (!key_metadata->checkAccess(user_id))
                    continue;

                auto key_lock = key_metadata->tryLock();
                if (!key_lock)
                    continue;

                result |= key_metadata->sizeUnlocked();
                for (const auto & [unused_offset, file_segment_metadata] : *key_metadata)
                    func(FileSegment::getInfo(file_segment_metadata->file_segment));
            }
            ++bucket_it;
            if (result)
                break;
        }
        return result;
    }

private:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    MetadataBuckets::iterator bucket_it;
};

CacheMetadata::Iterator::Iterator(const UserID & user_id_, MetadataBuckets & metadata_buckets_)
    : user_id(user_id_), metadata_buckets(metadata_buckets_)
{
}

bool CacheMetadata::Iterator::next(OnFileSegmentFunc func)
{
    if (!impl.has_value())
        impl = std::make_shared<IteratorImpl>(metadata_buckets, user_id);

    if (auto * iterator = std::get_if<CacheMetadata::IteratorImplPtr>(&impl.value()); iterator)
        return (*iterator)->next(func);

    throwFileCacheException("Expected IteratorImplPtr");
}

bool CacheMetadata::Iterator::nextBatch(OnFileSegmentFunc func)
{
    if (!impl)
        impl = std::make_shared<BatchedIteratorImpl>(metadata_buckets, user_id);

    if (auto * iterator = std::get_if<CacheMetadata::BatchedIteratorImplPtr>(&impl.value()); iterator)
        return (*iterator)->next(func);

    throwFileCacheException("Expected BatchedIteratorImplPtr");
}

CacheMetadata::IteratorPtr CacheMetadata::getIterator(const UserID & user_id)
{
    return std::make_unique<Iterator>(user_id, metadata_buckets);
}

bool CacheMetadata::removeAllKeys(const UserID & user_id)
{
    bool fully_removed = true;
    for (auto & bucket : metadata_buckets)
    {
        auto lock = bucket.lock();
        for (auto it = bucket.begin(); it != bucket.end();)
        {
            if (!it->second->checkAccess(user_id))
            {
                ++it;
                continue;
            }

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::ACTIVE)
            {
                bool removed_all = locked_key->removeAllFileSegments();
                if (removed_all)
                {
                    it = removeEmptyKey(bucket, it, *locked_key, lock);
                    continue;
                }
                fully_removed = false;
            }
            ++it;
        }
    }

    if (fully_removed)
        removeSharedOrigins(user_id);

    return fully_removed;
}

void CacheMetadata::removeKey(const Key & key, bool if_exists, const UserID & user_id)
{
    auto & bucket = getMetadataBucket(key);
    auto lock = bucket.lock();
    auto it = bucket.find(key);
    if (it == bucket.end())
    {
        if (if_exists)
            return;
        throwFileCacheException("No such key: {}", key);
    }

    it->second->assertAccess(user_id);
    auto locked_key = it->second->lockNoStateCheck();
    auto state = locked_key->getKeyState();
    if (state != KeyMetadata::KeyState::ACTIVE)
    {
        if (if_exists)
            return;
        throwFileCacheException("No such key: {} (state: {})", key, keyStateName(state));
    }

    bool removed_all = locked_key->removeAllFileSegments();
    if (removed_all)
        removeEmptyKey(bucket, it, *locked_key, lock);
}

CacheMetadata::MetadataBucket::iterator
CacheMetadata::removeEmptyKey(
    MetadataBucket & bucket,
    MetadataBucket::iterator it,
    LockedKey & locked_key,
    const CacheMetadataGuard::Lock &)
{
    const auto & key = locked_key.getKey();

    if (!locked_key.empty())
        throwFileCacheException("Cannot remove non-empty key: {}", key);

    locked_key.markAsRemoved();
    auto next_it = bucket.erase(it);

    LOG_TEST(log, "Key {} is removed from metadata", key);

    const fs::path key_directory = getKeyPath(key, *locked_key.getKeyMetadata()->origin);
    const fs::path key_prefix_directory = key_directory.parent_path();

    try
    {
        if (fs::exists(key_directory))
        {
            fs::remove_all(key_directory);
            LOG_TEST(log, "Directory ({}) for key {} removed", key_directory.string(), key);
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while removing key {}: {}", key, getCurrentExceptionMessage(true));
        chassert(false);
        return next_it;
    }

    try
    {
        std::unique_lock mutex(key_prefix_directory_mutex);
        if (fs::exists(key_prefix_directory) && fs::is_empty(key_prefix_directory))
        {
            fs::remove(key_prefix_directory);
            LOG_TEST(log, "Prefix directory ({}) for key {} removed", key_prefix_directory.string(), key);

            if (write_cache_per_user_directory)
            {
                const fs::path user_directory = key_prefix_directory.parent_path();
                if (fs::exists(user_directory) && fs::is_empty(user_directory))
                {
                    fs::remove(user_directory);
                    LOG_TEST(log, "User directory ({}) for key {} removed", user_directory.string(), key);
                }
            }
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while removing key {}: {}", key, getCurrentExceptionMessage(true));
        chassert(false);
    }
    return next_it;
}

class CleanupQueue
{
    friend class CacheMetadata;
public:
    void add(const FileCacheKey & key)
    {
        bool inserted = false;
        {
            std::lock_guard lock(mutex);
            if (cancelled)
                return;
            inserted = keys.insert(key).second;
        }
        if (inserted)
            cv.notify_one();
    }

    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;
        }
        cv.notify_all();
    }

private:
    std::unordered_set<FileCacheKey, FileCacheKeyHash> keys;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;
};

void CacheMetadata::cleanupThreadFunc()
{
    while (true)
    {
        Key key;
        {
            std::unique_lock lock(cleanup_queue->mutex);
            if (cleanup_queue->cancelled)
                return;

            auto & keys = cleanup_queue->keys;
            if (keys.empty())
            {
                cleanup_queue->cv.wait(lock, [&](){ return cleanup_queue->cancelled || !keys.empty(); });
                if (cleanup_queue->cancelled)
                    return;
            }

            auto it = keys.begin();
            key = *it;
            keys.erase(it);
        }

        try
        {
            auto & bucket = getMetadataBucket(key);
            auto lock = bucket.lock();

            auto it = bucket.find(key);
            if (it == bucket.end())
                continue;

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::REMOVING)
            {
                removeEmptyKey(bucket, it, *locked_key, lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

class DownloadQueue
{
friend class CacheMetadata;
public:
    explicit DownloadQueue(size_t queue_size_limit_) : queue_size_limit(queue_size_limit_) {}

    bool add(FileSegmentPtr file_segment)
    {
        {
            std::lock_guard lock(mutex);
            if (cancelled || (queue_size_limit && queue.size() >= queue_size_limit))
                return false;
            queue.push(DownloadInfo{file_segment->key(), file_segment->offset(), file_segment});
        }

        cv.notify_one();
        return true;
    }

    bool setQueueLimit(size_t size) { return queue_size_limit.exchange(size) != size; }

private:
    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;
        }
        cv.notify_all();
    }

    std::atomic<size_t> queue_size_limit;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;

    struct DownloadInfo
    {
        DownloadInfo(
            const FileCacheKey & key_,
            const size_t & offset_,
            const std::weak_ptr<FileSegment> & file_segment_)
            : key(key_), offset(offset_), file_segment(file_segment_) {}

        FileCacheKey key;
        size_t offset;
        /// Weak pointer to file segment: the segment at key:offset can be removed and added back to
        /// metadata before background download actually starts; identity via key+offset alone would
        /// accept a different segment.
        std::weak_ptr<FileSegment> file_segment;
    };

    std::queue<DownloadInfo> queue;
};

void CacheMetadata::downloadThreadFunc(const bool & stop_flag)
{
    std::optional<CacheBuffer> memory;
    while (true)
    {
        Key key;
        size_t offset = 0;
        std::weak_ptr<FileSegment> file_segment_weak;

        {
            std::unique_lock lock(download_queue->mutex);
            if (download_queue->cancelled || stop_flag)
                return;

            if (download_queue->queue.empty())
            {
                download_queue->cv.wait(lock, [&](){ return download_queue->cancelled || !download_queue->queue.empty() || stop_flag; });
                if (download_queue->cancelled || stop_flag)
                    return;
            }

            auto entry = download_queue->queue.front();
            key = entry.key;
            offset = entry.offset;
            file_segment_weak = entry.file_segment;

            download_queue->queue.pop();
        }

        try
        {
            FileSegmentsHolderPtr holder;
            try
            {
                {
                    auto locked_key = lockKeyMetadata(key, KeyNotFoundPolicy::RETURN_NULL, FileCache::getInternalOrigin());
                    if (!locked_key)
                        continue;

                    auto file_segment_metadata = locked_key->tryGetByOffset(offset);
                    if (!file_segment_metadata || file_segment_metadata->isEvictingOrRemoved(*locked_key))
                        continue;

                    auto file_segment = file_segment_weak.lock();

                    if (!file_segment
                        || file_segment != file_segment_metadata->file_segment
                        || file_segment->state() != FileSegment::State::PARTIALLY_DOWNLOADED)
                        continue;

                    holder = std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
                }

                auto & file_segment = holder->front();

                if (file_segment.getOrSetDownloader() != FileSegment::getCallerId())
                    continue;

                chassert(file_segment.getDownloadedSize() != file_segment.range().size());
                chassert(file_segment.assertCorrectness());

                downloadImpl(file_segment, memory);
                holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            }
            catch (...)
            {
                if (holder)
                {
                    auto & file_segment = holder->front();
                    file_segment.setDownloadFailed();

                    LOG_ERROR(
                        log, "Error during background download of {}:{} ({}): {}",
                        file_segment.key(), file_segment.offset(),
                        file_segment.getInfoForLog(), getCurrentExceptionMessage(true));
                }
                else
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    chassert(false);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
        }
    }
}

bool CacheMetadata::setBackgroundDownloadQueueSizeLimit(size_t size)
{
    return download_queue->setQueueLimit(size);
}

void CacheMetadata::downloadImpl(FileSegment & file_segment, std::optional<CacheBuffer> & memory) const
{
    /// D3: CH reused an owned `Memory<>` (`DBMS_DEFAULT_BUFFER_SIZE`) as an external read target.
    /// The Velox `ReadBufferFromVeloxReadFile` already owns a MemoryPool-charged internal buffer
    /// (SD9), so we read through the reader's own buffer instead of maintaining a separate reused
    /// allocation; `memory` is retained for signature fidelity but not used for a redundant owner.
    (void)memory;

    LOG_TEST(
        log, "Downloading {} bytes for file segment {}",
        file_segment.range().size() - file_segment.getDownloadedSize(), file_segment.getInfoForLog());

    size_t size_to_download = file_segment.getSizeForBackgroundDownload();
    if (!size_to_download)
        return;

    auto buf = file_segment.getRemoteFileReader();
    if (!buf)
    {
        LOG_TEST(log, "No reader in {}:{} (range: {}, downloaded size: {})",
                 file_segment.key(), file_segment.offset(),
                 file_segment.range().toString(), file_segment.getDownloadedSize());
        return;
    }

    /// Read through the reader's own (pool-charged) buffer: detach any external target so the
    /// reader republishes its internal window on the next `next()`.
    buf->set(nullptr, 0);

    const auto reserve_space_lock_wait_timeout_ms = reserve_space_wait_lock_timeout_milliseconds;

    size_t offset = file_segment.getCurrentWriteOffset();
    if (offset != static_cast<size_t>(buf->getPosition()))
        buf->seek(offset, SEEK_SET);

    while (size_to_download && !buf->eof())
    {
        const auto available = buf->available();
        chassert(available);

        const auto size = std::min(available, size_to_download);
        size_to_download -= size;

        std::string failure_reason;
        if (!file_segment.reserve(size, reserve_space_lock_wait_timeout_ms, failure_reason))
        {
            LOG_TEST(
                log, "Failed to reserve space during background download for {}:{} (downloaded size: {}/{})",
                file_segment.key(), file_segment.offset(),
                file_segment.getDownloadedSize(), file_segment.range().size());
            break;
        }

        try
        {
            file_segment.write(buf->position(), size, offset);
            offset += size;
            buf->position() += size;
        }
        catch (const FileCacheErrnoException & e)
        {
            int code = e.getErrno();
            if (code == /* ENOSPC */ 28 || code == /* EDQUOT */ 122)
            {
                LOG_INFO(log, "Insert into cache is skipped due to insufficient disk space. ({})", e.what());
                break;
            }
            throw;
        }
    }

    /// Reset buffer to avoid a stale buffer-end offset assertion on the reader.
    file_segment.resetRemoteFileReader();
    file_segment.completePartAndResetDownloader();

    LOG_TEST(log, "Downloaded file segment: {}", file_segment.getInfoForLog());
}

void CacheMetadata::startup()
{
    download_threads.reserve(download_threads_num);
    for (size_t i = 0; i < download_threads_num; ++i)
    {
        download_threads.emplace_back(std::make_shared<DownloadThread>());
        download_threads.back()->thread = std::make_unique<ThreadFromGlobalPool>(
            worker_pool, [this, thread = download_threads.back()] { downloadThreadFunc(thread->stop_flag); });
    }
    cleanup_thread = std::make_unique<ThreadFromGlobalPool>(worker_pool, [this]{ cleanupThreadFunc(); });
}

void CacheMetadata::shutdown()
{
    /// Cancel the queues first so that any in-flight and future waiters observe the cancel and stop
    /// (cancel-before-join): joining without cancelling first would deadlock on the condition vars.
    download_queue->cancel();
    cleanup_queue->cancel();

    for (auto & download_thread : download_threads)
    {
        if (download_thread->thread && download_thread->thread->joinable())
            download_thread->thread->join();
    }
    if (cleanup_thread && cleanup_thread->joinable())
        cleanup_thread->join();
}

bool CacheMetadata::isBackgroundDownloadEnabled()
{
    return download_threads_num;
}

bool CacheMetadata::setBackgroundDownloadThreads(size_t threads_num)
{
    if (threads_num == download_threads_num)
        return false;

    SCOPE_EXIT { download_threads_num = download_threads.size(); };

    if (threads_num > download_threads_num)
    {
        size_t add_threads = threads_num - download_threads_num;
        for (size_t i = 0; i < add_threads; ++i)
        {
            download_threads.emplace_back(std::make_shared<DownloadThread>());
            try
            {
                download_threads.back()->thread = std::make_unique<ThreadFromGlobalPool>(
                    worker_pool, [this, thread = download_threads.back()] { downloadThreadFunc(thread->stop_flag); });
            }
            catch (...)
            {
                download_threads.pop_back();
                throw;
            }
        }
    }
    else if (threads_num < download_threads_num)
    {
        size_t remove_threads = download_threads_num - threads_num;

        {
            std::lock_guard lock(download_queue->mutex);
            for (size_t i = 0; i < remove_threads; ++i)
                download_threads[download_threads.size() - 1 - i]->stop_flag = true;
        }

        download_queue->cv.notify_all();

        for (size_t i = 0; i < remove_threads; ++i)
        {
            chassert(download_threads.back()->stop_flag);

            auto & thread = download_threads.back()->thread;
            if (thread && thread->joinable())
                thread->join();

            download_threads.pop_back();
        }
    }
    return true;
}

bool KeyMetadata::addToDownloadQueue(FileSegmentPtr file_segment)
{
    return cache_metadata->download_queue->add(file_segment);
}

void KeyMetadata::addToCleanupQueue()
{
    cache_metadata->cleanup_queue->add(key);
}

LockedKey::LockedKey(std::shared_ptr<KeyMetadata> key_metadata_)
    : key_metadata(key_metadata_)
    , lock(key_metadata->guard.lock())
{
}

LockedKey::~LockedKey()
{
    if (!key_metadata->empty() || getKeyState() != KeyMetadata::KeyState::ACTIVE)
        return;

    key_metadata->key_state = KeyMetadata::KeyState::REMOVING;
    LOG_TEST(key_metadata->logger(), "Submitting key {} for removal", getKey());
    key_metadata->addToCleanupQueue();
}

void LockedKey::removeFromCleanupQueue()
{
    if (key_metadata->key_state != KeyMetadata::KeyState::REMOVING)
        throwFileCacheException("Cannot remove non-removing");

    key_metadata->key_state = KeyMetadata::KeyState::ACTIVE;
}

void LockedKey::markAsRemoved()
{
    key_metadata->key_state = KeyMetadata::KeyState::REMOVED;
}

bool LockedKey::isLastOwnerOfFileSegment(size_t offset) const
{
    const auto file_segment_metadata = getByOffset(offset);
    return file_segment_metadata->file_segment.use_count() == 2;
}

bool LockedKey::removeAllFileSegments()
{
    bool removed_all = true;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (!it->second->releasable())
        {
            ++it;
            removed_all = false;
            continue;
        }
        if (it->second->isEvictingOrRemoved(*this))
        {
            ++it;
            removed_all = false;
            continue;
        }

        auto file_segment = it->second->file_segment;
        it = removeFileSegment(file_segment->offset(), file_segment->lock());
    }
    return removed_all;
}

KeyMetadata::iterator LockedKey::removeFileSegmentIfExists(size_t offset, bool can_be_broken, bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return {};

    if (!can_be_broken && !it->second->releasable())
        return {};

    auto file_segment = it->second->file_segment;
    return removeFileSegmentImpl(it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, bool can_be_broken, bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throwFileCacheException("There is no offset {}", offset);

    if (!can_be_broken && !it->second->releasable())
        return {};

    auto file_segment = it->second->file_segment;
    return removeFileSegmentImpl(it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throwFileCacheException("There is no offset {} in key {}", offset, getKey());

    return removeFileSegmentImpl(it, segment_lock, can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegmentImpl(
    KeyMetadata::iterator it,
    const FileSegmentGuard::Lock & segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry)
{
    auto file_segment = it->second->file_segment;

    LOG_TEST(
        key_metadata->logger(), "Remove from cache. Key: {}, offset: {}, size: {}",
        getKey(), file_segment->offset(), file_segment->getReservedSize());

    chassert(can_be_broken || file_segment->assertCorrectnessUnlocked(segment_lock));

    /// Access `queue_iterator` directly (LockedKey is a friend of FileSegment): the public
    /// `getQueueIterator()` re-acquires the segment lock, which we already hold here, and would
    /// self-deadlock the non-recursive `FileSegmentGuard`. CH also touches the member directly.
    if (file_segment->queue_iterator && invalidate_queue_entry)
        file_segment->queue_iterator->invalidate();

    try
    {
        file_segment->detach(segment_lock, *this);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
        /// Do not rethrow, we must delete the file below.
    }

    /// In CH the removal site does two adjacent but INDEPENDENT things
    /// (`Metadata.cpp:1261,1267`): (1) `fs::remove(path)` — the actual file deletion, which is the
    /// core of eviction and MUST run; and (2) `OpenedFileCache::instance().remove(path, flags)` —
    /// invalidating cached open handles, a Task-013 Manager concept. We perform (1) normally; (2)
    /// becomes a no-op here (see below). Per the Task-012 amendment (B2b CORRECTION / B7, user
    /// decision 2026-07-20) only the opened-handle invalidation is deferred, never the removal.
    bool opened_handle_invalidation_required = false;
    std::string removed_path;

    try
    {
        const auto path = key_metadata->getFileSegmentPath(*file_segment);
        if (file_segment->getDownloadedSize() == 0)
        {
            chassert(!fs::exists(path));
        }
        else if (fs::exists(path))
        {
            fs::remove(path);
            opened_handle_invalidation_required = true;
            removed_path = path;
        }
        else if (!can_be_broken)
        {
#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)
            throwFileCacheException("Expected path {} to exist", path);
#else
            LOG_WARNING(key_metadata->logger(), "Expected path {} to exist, while removing {}:{}",
                        path, getKey(), file_segment->offset());
#endif
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
    }

    if (opened_handle_invalidation_required)
    {
        /// TODO(Task 013): invalidate opened file handles via the manager-owned OpenedFileCache.
        /// No-op in the SCC phase: no `OpenedFileCache` exists yet (it is manager-owned, introduced
        /// in Task 013), so there are no cached handles for `removed_path` to go stale. Task 013
        /// wires the real Manager-backed invalidation into this same seam.
        (void)removed_path;
        return key_metadata->erase(it);
    }

    return key_metadata->erase(it);
}

bool LockedKey::addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throwFileCacheException("There is not offset {}", offset);
    return key_metadata->addToDownloadQueue(it->second->file_segment);
}

std::optional<FileSegment::Range> LockedKey::hasIntersectingRange(const FileSegment::Range & range) const
{
    if (key_metadata->empty())
        return {};

    auto it = key_metadata->lower_bound(range.left);
    if (it != key_metadata->end()) /// has next range
    {
        auto next_range = it->second->file_segment->range();
        if (!(range < next_range))
            return next_range;

        if (it == key_metadata->begin())
            return {};
    }

    auto prev_range = std::prev(it)->second->file_segment->range();
    if (!(prev_range < range))
        return prev_range;

    return {};
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::getByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throwFileCacheException("There is not offset {}", offset);
    return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::getByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throwFileCacheException("There is not offset {}", offset);
    return it->second;
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::tryGetByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::tryGetByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return it->second;
}

std::string LockedKey::toString() const
{
    std::string result;
    for (auto it = key_metadata->begin(); it != key_metadata->end(); ++it)
    {
        if (it != key_metadata->begin())
            result += ", ";
        result += std::to_string(it->first);
    }
    return result;
}

std::vector<FileSegment::Info> LockedKey::sync()
{
    std::vector<FileSegment::Info> broken;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (it->second->isEvictingOrRemoved(*this) || !it->second->releasable())
        {
            ++it;
            continue;
        }

        auto file_segment = it->second->file_segment;
        if (file_segment->isDetached())
        {
            throwFileCacheException("File segment has unexpected state: DETACHED ({})", file_segment->getInfoForLog());
        }

        if (file_segment->getDownloadedSize() == 0)
        {
            ++it;
            continue;
        }

        const auto & path = key_metadata->getFileSegmentPath(*file_segment);
        if (!fs::exists(path))
        {
            LOG_WARNING(
                key_metadata->logger(),
                "File segment has DOWNLOADED state, but file does not exist ({})",
                file_segment->getInfoForLog());

            broken.push_back(FileSegment::getInfo(file_segment));
            it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */true);
            continue;
        }

        const size_t actual_size = fs::file_size(path);
        const size_t expected_size = file_segment->getDownloadedSize();

        if (actual_size == expected_size)
        {
            ++it;
            continue;
        }

        LOG_WARNING(
            key_metadata->logger(),
            "File segment has unexpected size. Having {}, expected {} ({})",
            actual_size, expected_size, file_segment->getInfoForLog());

        broken.push_back(FileSegment::getInfo(file_segment));
        it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */true);
    }
    return broken;
}

}
