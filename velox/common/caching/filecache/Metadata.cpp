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
#include "velox/common/caching/filecache/Metadata.h"

#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/caching/filecache/FileSegmentInfo.h"

#include <folly/ScopeGuard.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <condition_variable>
#include <exception>
#include <filesystem>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "velox/common/base/Exceptions.h"

namespace fs = std::filesystem;

namespace facebook::velox::ch {

namespace {

std::string_view keyStateName(KeyMetadata::KeyState state) {
  switch (state) {
    case KeyMetadata::KeyState::ACTIVE:
      return "ACTIVE";
    case KeyMetadata::KeyState::REMOVING:
      return "REMOVING";
    case KeyMetadata::KeyState::REMOVED:
      return "REMOVED";
  }
  return {};
}

std::string getCurrentExceptionMessage(bool /*with_stacktrace*/) {
  try {
    if (auto exception = std::current_exception()) {
      std::rethrow_exception(exception);
    }
  } catch (const VeloxException& e) {
    return e.message();
  } catch (const std::exception& e) {
    return e.what();
  } catch (...) {
    return "Unknown exception";
  }
  return "";
}

void tryLogCurrentException(const char* function) {
  LOG(ERROR) << fmt::format(
      "{}: {}", function, getCurrentExceptionMessage(true));
}

} // namespace

FileSegmentMetadata::FileSegmentMetadata(FileSegmentPtr&& file_segment_)
    : fileSegment(std::move(file_segment_)) {
  switch (fileSegment->state()) {
    case FileSegment::State::DOWNLOADED: {
      VELOX_DCHECK(fileSegment->getQueueIterator());
      break;
    }
    case FileSegment::State::EMPTY:
    case FileSegment::State::DOWNLOADING: {
      break;
    }
    default:
      VELOX_FAIL(
          "Can create file segment with either EMPTY, DOWNLOADED, DOWNLOADING state, got: {}",
          FileSegment::stateToString(fileSegment->state()));
  }
}

size_t FileSegmentMetadata::size() const {
  return fileSegment->getReservedSize();
}

KeyMetadata::KeyMetadata(
    const Key& key_,
    const OriginInfo& origin_,
    const CacheMetadata* cache_metadata_,
    bool created_base_directory_)
    : key(key_),
      origin(origin_),
      cacheMetadata(cache_metadata_),
      createdBaseDirectory(created_base_directory_) {
  if (origin_ == FileCache::getInternalOrigin()) {
    VELOX_FAIL("Cannot create key metadata with internal user id");
  }

  if (!origin_.weight.has_value()) {
    VELOX_FAIL("Cannot create key metadata without user weight");
  }

  VELOX_DCHECK(!createdBaseDirectory || fs::exists(getPath()));
}

bool KeyMetadata::checkAccess(const UserID& user_id_) const {
  return user_id_ == origin.userId ||
      user_id_ == FileCache::getInternalOrigin().userId;
}

void KeyMetadata::assertAccess(const UserID& user_id_) const {
  if (!checkAccess(user_id_)) {
    VELOX_FAIL("Metadata for key {} belongs to another user", key.toString());
  }
}

LockedKeyPtr KeyMetadata::lock() {
  auto locked = tryLock();
  if (locked) {
    return locked;
  }

  VELOX_FAIL(
      "Cannot lock key {} (state: {})", key, keyStateName(keyState));
}

LockedKeyPtr KeyMetadata::tryLock() {
  auto locked = lockNoStateCheck();
  if (keyState == KeyMetadata::KeyState::ACTIVE) {
    return locked;
  }

  return nullptr;
}

LockedKeyPtr KeyMetadata::lockNoStateCheck() {
  // TODO(metric): CH ProfileEventTimeIncrement FilesystemCacheLockKeyMicroseconds.
  return std::make_unique<LockedKey>(shared_from_this());
}

KeyMetadata::KeyState KeyMetadata::getState() {
  auto locked = lockNoStateCheck();
  return keyState;
}

bool KeyMetadata::createBaseDirectory(bool throw_if_failed) {
  if (createdBaseDirectory.load()) {
    return true;
  }

  std::shared_lock lock(cacheMetadata->keyPrefixDirectoryMutex);

  if (createdBaseDirectory.load(std::memory_order_relaxed)) {
    return true;
  }

  try {
    fs::create_directories(getPath());
    createdBaseDirectory.store(true);
    // TODO(metric): CH ProfileEvents::FilesystemCacheCreatedKeyDirectories.
  } catch (const fs::filesystem_error& e) {
    createdBaseDirectory = false;

    if (!throw_if_failed &&
        (e.code() == std::errc::no_space_on_device ||
         e.code() == std::errc::read_only_file_system ||
         e.code() == std::errc::permission_denied ||
         e.code() == std::errc::too_many_files_open ||
         e.code() == std::errc::operation_not_permitted)) {
      VLOG(1) << fmt::format(
          "Failed to create base directory for key {}, because no space left on device",
          key);

      return false;
    }
    throw;
  }

  return true;
}

std::string KeyMetadata::getPath() const {
  return cacheMetadata->getKeyPath(key, origin);
}

std::string KeyMetadata::getFileSegmentPath(
    const FileSegment& file_segment) const {
  return cacheMetadata->getFileSegmentPath(
      key, file_segment.offset(), file_segment.getKind(), origin);
}

CacheMetadata::CacheMetadata(
    const std::string& path_,
    size_t background_download_queue_size_limit_,
    size_t background_download_threads_,
    bool write_cache_per_user_directory_)
    : path(path_),
      cleanupQueue(std::make_shared<CleanupQueue>()),
      downloadQueue(
          std::make_shared<DownloadQueue>(background_download_queue_size_limit_)),
      writeCachePerUserDirectory(write_cache_per_user_directory_),
      downloadThreadsNum(background_download_threads_) {}

CacheMetadata::~CacheMetadata() = default;

std::string CacheMetadata::getFileNameForFileSegment(
    size_t offset,
    FileSegmentKind segment_kind) {
  std::string file_suffix;
  switch (segment_kind) {
    case FileSegmentKind::Ephemeral:
      file_suffix = "_temporary";
      break;
    case FileSegmentKind::Regular:
      break;
  }
  return std::to_string(offset) + file_suffix;
}

std::string CacheMetadata::getFileSegmentPath(
    const Key& key,
    size_t offset,
    FileSegmentKind segment_kind,
    const OriginInfo& origin) const {
  return (fs::path(getKeyPath(key, origin)) /
          getFileNameForFileSegment(offset, segment_kind))
      .string();
}

std::string CacheMetadata::getKeyPath(
    const Key& key,
    const OriginInfo& origin) const {
  const auto key_str = key.toString();
  const auto key_type_prefix = getKeyTypePrefix(origin.segmentType);
  if (writeCachePerUserDirectory) {
    return (fs::path(path) / key_type_prefix /
            fmt::format("{}.{}", origin.userId, origin.weight.value()) /
            key_str.substr(0, 3) / key_str)
        .string();
  }

  return (fs::path(path) / key_type_prefix / key_str.substr(0, 3) / key_str)
      .string();
}

CacheMetadataGuard::Lock CacheMetadata::MetadataBucket::lock() const {
  // TODO(metric): CH ProfileEventTimeIncrement FilesystemCacheLockMetadataMicroseconds.
  return guard.lock();
}

CacheMetadata::MetadataBucket& CacheMetadata::getMetadataBucket(
    const Key& key) {
  const auto bucket = static_cast<size_t>(key.key % bucketsNum);
  return metadataBuckets[bucket];
}

LockedKeyPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey& key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo& origin,
    bool is_initial_load) {
  auto key_metadata =
      getKeyMetadata(key, key_not_found_policy, origin, is_initial_load);
  if (!key_metadata) {
    return nullptr;
  }

  {
    auto locked_metadata = key_metadata->lockNoStateCheck();
    const auto key_state = locked_metadata->getKeyState();

    if (key_state == KeyMetadata::KeyState::ACTIVE) {
      return locked_metadata;
    }

    if (key_not_found_policy == KeyNotFoundPolicy::THROW) {
      VELOX_FAIL("No such key `{}` in cache", key);
    }
    if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL) {
      VELOX_FAIL("No such key `{}` in cache", key);
    }

    if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL) {
      return nullptr;
    }

    if (key_state == KeyMetadata::KeyState::REMOVING) {
      locked_metadata->removeFromCleanupQueue();
      VLOG(1) << fmt::format("Removal of key {} is cancelled", key);
      return locked_metadata;
    }

    VELOX_DCHECK(key_state == KeyMetadata::KeyState::REMOVED);
    VELOX_DCHECK(key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY);
  }

  /// Now we are at the case when the key was removed (key_state == KeyMetadata::KeyState::REMOVED)
  /// but we need to return empty key (key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY)
  /// Retry
  return lockKeyMetadata(key, key_not_found_policy, origin);
}

KeyMetadataPtr CacheMetadata::getKeyMetadata(
    const Key& key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo& origin,
    bool is_initial_load) {
  auto& bucket = getMetadataBucket(key);
  auto lock = bucket.lock();

  auto it = bucket.find(key);
  if (it == bucket.end()) {
    if (key_not_found_policy == KeyNotFoundPolicy::THROW) {
      VELOX_FAIL("No such key `{}` in cache", key);
    }
    if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL) {
      VELOX_FAIL("No such key `{}` in cache", key);
    }
    if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL) {
      return nullptr;
    }

    it = bucket
             .emplace(
                 key,
                 std::make_shared<KeyMetadata>(
                     key, origin, this, is_initial_load))
             .first;

    // TODO(metric): CH CurrentMetrics::FilesystemCacheKeys add.
  }

  it->second->assertAccess(origin.userId);
  return it->second;
}

bool CacheMetadata::isEmpty() const {
  for (const auto& bucket : metadataBuckets) {
    if (!bucket.empty()) {
      return false;
    }
  }
  return true;
}

void CacheMetadata::iterate(IterateFunc&& func, const UserID& user_id) {
  for (auto& bucket : metadataBuckets) {
    auto lk = bucket.lock();
    for (auto& [key, key_metadata] : bucket) {
      if (!key_metadata->checkAccess(user_id)) {
        continue;
      }

      auto locked_key = key_metadata->lockNoStateCheck();
      const auto key_state = locked_key->getKeyState();

      if (key_state == KeyMetadata::KeyState::ACTIVE) {
        func(*locked_key);
        continue;
      }
      if (key_state == KeyMetadata::KeyState::REMOVING) {
        continue;
      }

      VELOX_FAIL(
          "Cannot lock key {}: key does not exist", key_metadata->key);
    }
  }
}

class CacheMetadata::IteratorImpl {
 public:
  IteratorImpl(MetadataBuckets& metadata_buckets_, const UserID& user_id_)
      : user_id(user_id_),
        metadata_buckets(metadata_buckets_),
        bucket_it(metadata_buckets_.begin()) {}

  bool next(Iterator::OnFileSegmentFunc func) {
    while (true) {
      if (bucket_it == metadata_buckets.end()) {
        return false;
      }

      if (!bucket_lock) {
        bucket_lock = bucket_it->lock();
      }

      if (!key_it.has_value()) {
        key_it = bucket_it->begin();
      }

      if (key_it.value() == bucket_it->end()) {
        ++bucket_it;
        bucket_lock.reset();

        key_it.reset();
        key_lock.reset();

        file_segment_it.reset();
        continue;
      }

      const auto& key = key_it.value()->second;

      if (!key_lock) {
        if (!key->checkAccess(user_id)) {
          ++key_it.value();
          continue;
        }

        /// Will lock only if key is in state ACTIVE.
        key_lock = key->tryLock();
        if (!key_lock) {
          ++key_it.value();
          continue;
        }
      }

      if (!file_segment_it.has_value()) {
        file_segment_it = key->begin();
      }

      if (file_segment_it.value() == key->end()) {
        ++key_it.value();
        key_lock.reset();

        file_segment_it.reset();
        continue;
      }

      func(FileSegment::getInfo(file_segment_it.value()->second->fileSegment));
      ++(file_segment_it.value());
      return true;
    }
  }

 private:
  const UserID user_id;
  MetadataBuckets& metadata_buckets;
  MetadataBuckets::iterator bucket_it;
  std::optional<MetadataBucket::iterator> key_it;
  std::optional<KeyMetadata::iterator> file_segment_it;

  std::optional<CacheMetadataGuard::Lock> bucket_lock;
  LockedKeyPtr key_lock;
};

class CacheMetadata::BatchedIteratorImpl {
 public:
  BatchedIteratorImpl(
      MetadataBuckets& metadata_buckets_,
      const UserID& user_id_)
      : user_id(user_id_),
        metadata_buckets(metadata_buckets_),
        bucket_it(metadata_buckets_.begin()) {}

  bool next(Iterator::OnFileSegmentFunc func) {
    bool result = false;
    while (bucket_it != metadata_buckets.end()) {
      auto bucket_lock = bucket_it->lock();
      for (const auto& [_, key_metadata] : *bucket_it) {
        if (!key_metadata->checkAccess(user_id)) {
          continue;
        }

        /// Will lock only if key is in state ACTIVE.
        auto key_lock = key_metadata->tryLock();
        if (!key_lock) {
          continue;
        }

        result |= key_metadata->size();
        for (const auto& [_, file_segment_metadata] : *key_metadata) {
          func(FileSegment::getInfo(file_segment_metadata->fileSegment));
        }
      }
      ++bucket_it;
      if (result) {
        break;
      }
    }
    return result;
  }

 private:
  const UserID user_id;
  MetadataBuckets& metadata_buckets;
  MetadataBuckets::iterator bucket_it;
};

CacheMetadata::Iterator::Iterator(
    const UserID& user_id_,
    MetadataBuckets& metadata_buckets_)
    : userId(user_id_), metadataBuckets(metadata_buckets_) {}

bool CacheMetadata::Iterator::next(OnFileSegmentFunc func) {
  if (!impl.has_value()) {
    impl = std::make_shared<IteratorImpl>(metadataBuckets, userId);
  }

  if (auto* iterator =
          std::get_if<CacheMetadata::IteratorImplPtr>(&impl.value());
      iterator) {
    return (*iterator)->next(func);
  }

  VELOX_FAIL("Expected IteratorImplPtr");
}

bool CacheMetadata::Iterator::nextBatch(OnFileSegmentFunc func) {
  if (!impl) {
    impl = std::make_shared<BatchedIteratorImpl>(metadataBuckets, userId);
  }

  if (auto* iterator =
          std::get_if<CacheMetadata::BatchedIteratorImplPtr>(&impl.value());
      iterator) {
    return (*iterator)->next(func);
  }

  VELOX_FAIL("Expected BatchedIteratorImplPtr");
}

CacheMetadata::IteratorPtr CacheMetadata::getIterator(const UserID& user_id) {
  return std::make_unique<Iterator>(user_id, metadataBuckets);
}

void CacheMetadata::removeAllKeys(const UserID& user_id) {
  for (auto& bucket : metadataBuckets) {
    auto lock = bucket.lock();
    for (auto it = bucket.begin(); it != bucket.end();) {
      if (!it->second->checkAccess(user_id)) {
        ++it;
        continue;
      }

      auto locked_key = it->second->lockNoStateCheck();
      if (locked_key->getKeyState() == KeyMetadata::KeyState::ACTIVE) {
        bool removed_all = locked_key->removeAllFileSegments();
        if (removed_all) {
          it = removeEmptyKey(bucket, it, *locked_key, lock);
          continue;
        }
      }
      ++it;
    }
  }
}

void CacheMetadata::removeKey(
    const Key& key,
    bool if_exists,
    const UserID& user_id) {
  auto& bucket = getMetadataBucket(key);
  auto lock = bucket.lock();
  auto it = bucket.find(key);
  if (it == bucket.end()) {
    if (if_exists) {
      return;
    }
    VELOX_FAIL("No such key: {}", key);
  }

  it->second->assertAccess(user_id);
  auto locked_key = it->second->lockNoStateCheck();
  auto state = locked_key->getKeyState();
  if (state != KeyMetadata::KeyState::ACTIVE) {
    if (if_exists) {
      return;
    }
    VELOX_FAIL("No such key: {} (state: {})", key, keyStateName(state));
  }

  bool removed_all = locked_key->removeAllFileSegments();
  if (removed_all) {
    removeEmptyKey(bucket, it, *locked_key, lock);
  }
}

CacheMetadata::MetadataBucket::iterator CacheMetadata::removeEmptyKey(
    MetadataBucket& bucket,
    MetadataBucket::iterator it,
    LockedKey& locked_key,
    const CacheMetadataGuard::Lock&) {
  const auto& key = locked_key.getKey();

  if (!locked_key.empty()) {
    VELOX_FAIL("Cannot remove non-empty key: {}", key);
  }

  locked_key.markAsRemoved();
  auto next_it = bucket.erase(it);

  // TODO(metric): CH CurrentMetrics::FilesystemCacheKeys sub.

  VLOG(1) << fmt::format("Key {} is removed from metadata", key);

  const fs::path key_directory =
      getKeyPath(key, locked_key.getKeyMetadata()->origin);
  const fs::path key_prefix_directory = key_directory.parent_path();

  try {
    if (fs::exists(key_directory)) {
      fs::remove_all(key_directory);
      VLOG(1) << fmt::format(
          "Directory ({}) for key {} removed", key_directory.string(), key);
    }
  } catch (...) {
    LOG(ERROR) << fmt::format(
        "Error while removing key {}: {}",
        key,
        getCurrentExceptionMessage(true));
    VELOX_DCHECK(false);
    return next_it;
  }

  try {
    std::unique_lock mutex(keyPrefixDirectoryMutex);
    if (fs::exists(key_prefix_directory) && fs::is_empty(key_prefix_directory)) {
      fs::remove(key_prefix_directory);
      VLOG(1) << fmt::format(
          "Prefix directory ({}) for key {} removed",
          key_prefix_directory.string(),
          key);
    }

    /// TODO: Remove empty user directories.
  } catch (...) {
    LOG(ERROR) << fmt::format(
        "Error while removing key {}: {}",
        key,
        getCurrentExceptionMessage(true));
    VELOX_DCHECK(false);
  }
  return next_it;
}

class CleanupQueue {
  friend class CacheMetadata;

 public:
  void add(const FileCacheKey& key) {
    bool inserted;
    {
      std::lock_guard lock(mutex);
      if (cancelled) {
        return;
      }
      inserted = keys.insert(key).second;
    }
    if (inserted) {
      // TODO(metric): CH CurrentMetrics::FilesystemCacheDelayedCleanupElements add.
      cv.notify_one();
    }
  }

  void cancel() {
    {
      std::lock_guard lock(mutex);
      cancelled = true;
    }
    cv.notify_all();
  }

 private:
  std::unordered_set<FileCacheKey> keys;
  mutable std::mutex mutex;
  std::condition_variable cv;
  bool cancelled = false;
};

void CacheMetadata::cleanupThreadFunc() {
  while (true) {
    Key key;
    {
      std::unique_lock lock(cleanupQueue->mutex);
      if (cleanupQueue->cancelled) {
        return;
      }

      auto& keys = cleanupQueue->keys;
      if (keys.empty()) {
        cleanupQueue->cv.wait(
            lock, [&]() { return cleanupQueue->cancelled || !keys.empty(); });
        if (cleanupQueue->cancelled) {
          return;
        }
      }

      auto it = keys.begin();
      key = *it;
      keys.erase(it);
    }

    // TODO(metric): CH CurrentMetrics::FilesystemCacheDelayedCleanupElements sub.

    try {
      auto& bucket = getMetadataBucket(key);
      auto lock = bucket.lock();

      auto it = bucket.find(key);
      if (it == bucket.end()) {
        continue;
      }

      auto locked_key = it->second->lockNoStateCheck();
      if (locked_key->getKeyState() == KeyMetadata::KeyState::REMOVING) {
        removeEmptyKey(bucket, it, *locked_key, lock);
      }
    } catch (...) {
      tryLogCurrentException(__PRETTY_FUNCTION__);
    }
  }
}

class DownloadQueue {
  friend class CacheMetadata;

 public:
  explicit DownloadQueue(size_t queue_size_limit_)
      : queueSizeLimit(queue_size_limit_) {}

  bool add(FileSegmentPtr file_segment) {
    {
      std::lock_guard lock(mutex);
      if (cancelled || (queueSizeLimit && queue.size() >= queueSizeLimit)) {
        return false;
      }
      queue.push(DownloadInfo{
          file_segment->key(), file_segment->offset(), file_segment});
    }

    // TODO(metric): CH CurrentMetrics::FilesystemCacheDownloadQueueElements add.
    cv.notify_one();
    return true;
  }

  bool setQueueLimit(size_t size) {
    return queueSizeLimit.exchange(size) != size;
  }

 private:
  void cancel() {
    {
      std::lock_guard lock(mutex);
      cancelled = true;
    }
    cv.notify_all();
  }

  std::atomic<size_t> queueSizeLimit;
  mutable std::mutex mutex;
  std::condition_variable cv;
  bool cancelled = false;

  struct DownloadInfo {
    DownloadInfo(
        const CacheMetadata::Key& key_,
        const size_t& offset_,
        const std::weak_ptr<FileSegment>& file_segment_)
        : key(key_), offset(offset_), fileSegment(file_segment_) {}

    CacheMetadata::Key key;
    size_t offset;
    /// We keep weak pointer to file segment
    /// instead of just getting it from file_segment_metadata,
    /// because file segment at key:offset count be removed and added back to metadata
    /// before we actually started background download.
    std::weak_ptr<FileSegment> fileSegment;
  };

  std::queue<DownloadInfo> queue;
};

void CacheMetadata::downloadThreadFunc(const bool& stop_flag) {
  std::optional<std::vector<char>> memory;
  while (true) {
    Key key;
    size_t offset;
    std::weak_ptr<FileSegment> file_segment_weak;

    {
      std::unique_lock lock(downloadQueue->mutex);
      if (downloadQueue->cancelled || stop_flag) {
        return;
      }

      if (downloadQueue->queue.empty()) {
        downloadQueue->cv.wait(lock, [&]() {
          return downloadQueue->cancelled || !downloadQueue->queue.empty() ||
              stop_flag;
        });
        if (downloadQueue->cancelled || stop_flag) {
          return;
        }
      }

      auto entry = downloadQueue->queue.front();
      key = entry.key;
      offset = entry.offset;
      file_segment_weak = entry.fileSegment;

      downloadQueue->queue.pop();
    }

    // TODO(metric): CH CurrentMetrics::FilesystemCacheDownloadQueueElements sub.

    try {
      FileSegmentsHolderPtr holder;
      try {
        {
          auto locked_key = lockKeyMetadata(
              key, KeyNotFoundPolicy::RETURN_NULL, FileCache::getInternalOrigin());
          if (!locked_key) {
            continue;
          }

          auto file_segment_metadata = locked_key->tryGetByOffset(offset);
          if (!file_segment_metadata ||
              file_segment_metadata->isEvictingOrRemoved(*locked_key)) {
            continue;
          }

          auto file_segment = file_segment_weak.lock();

          if (!file_segment ||
              file_segment != file_segment_metadata->fileSegment ||
              file_segment->state() !=
                  FileSegment::State::PARTIALLY_DOWNLOADED) {
            continue;
          }

          holder = std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
        }

        auto& file_segment = holder->front();

        if (file_segment.getOrSetDownloader() != FileSegment::getCallerId()) {
          continue;
        }

        VELOX_DCHECK(file_segment.getDownloadedSize() != file_segment.range().size());
        VELOX_DCHECK(file_segment.assertCorrectness());

        downloadImpl(file_segment, memory);
        holder->completeAndPopFront(
            /*allow_background_download=*/false,
            /*force_shrink_to_downloaded_size=*/false);
      } catch (...) {
        if (holder) {
          auto& file_segment = holder->front();
          file_segment.setDownloadFailed();

          LOG(ERROR) << fmt::format(
              "Error during background download of {}:{} ({}): {}",
              file_segment.key(),
              file_segment.offset(),
              file_segment.getInfoForLog(),
              getCurrentExceptionMessage(true));
        } else {
          tryLogCurrentException(__PRETTY_FUNCTION__);
          VELOX_DCHECK(false);
        }
      }
    } catch (...) {
      tryLogCurrentException(__PRETTY_FUNCTION__);
      VELOX_DCHECK(false);
    }
  }
}

bool CacheMetadata::setBackgroundDownloadQueueSizeLimit(size_t size) {
  return downloadQueue->setQueueLimit(size);
}

void CacheMetadata::downloadImpl(
    FileSegment& file_segment,
    std::optional<std::vector<char>>& memory) const {
  VLOG(1) << fmt::format(
      "Downloading {} bytes for file segment {}",
      file_segment.range().size() - file_segment.getDownloadedSize(),
      file_segment.getInfoForLog());

  size_t size_to_download = file_segment.getSizeForBackgroundDownload();
  if (!size_to_download) {
    return;
  }

  auto buf = file_segment.getRemoteFileReader();
  if (!buf) {
    VLOG(1) << fmt::format(
        "No reader in {}:{} (state: {}, range: {}, downloaded size: {})",
        file_segment.key(),
        file_segment.offset(),
        FileSegment::stateToString(file_segment.state()),
        file_segment.range().toString(),
        file_segment.getDownloadedSize());
    return;
  }

  if (!memory) {
    memory.emplace(std::min(size_t(1024 * 1024), size_to_download));
  }

  VELOX_NYI(
      "TODO(io): ReadBufferFromFileBase is still only forward-declared in the Velox port; mapping CH buf->set/available/position/seek/eof to Velox ReadFile/LocalWriteFile requires the remote-reader interface decision");
}

void CacheMetadata::startup() {
  downloadThreads.reserve(downloadThreadsNum);
  for (size_t i = 0; i < downloadThreadsNum; ++i) {
    downloadThreads.emplace_back(std::make_shared<DownloadThread>());
    downloadThreads.back()->thread =
        std::make_unique<folly::CPUThreadPoolExecutor>(1);
    downloadThreads.back()->thread->add(
        [this, thread = downloadThreads.back()] {
          downloadThreadFunc(thread->stopFlag);
        });
  }
  cleanupThread = std::make_unique<folly::CPUThreadPoolExecutor>(1);
  cleanupThread->add([this] { cleanupThreadFunc(); });
}

void CacheMetadata::shutdown() {
  downloadQueue->cancel();
  cleanupQueue->cancel();

  for (auto& download_thread : downloadThreads) {
    if (download_thread->thread) {
      download_thread->thread->join();
    }
  }
  if (cleanupThread) {
    cleanupThread->join();
  }
}

bool CacheMetadata::isBackgroundDownloadEnabled() {
  return downloadThreadsNum;
}

bool CacheMetadata::setBackgroundDownloadThreads(size_t threads_num) {
  if (threads_num == downloadThreadsNum) {
    return false;
  }

  auto guard = folly::makeGuard(
      [&]() { downloadThreadsNum = downloadThreads.size(); });

  if (threads_num > downloadThreadsNum) {
    size_t add_threads = threads_num - downloadThreadsNum;
    for (size_t i = 0; i < add_threads; ++i) {
      downloadThreads.emplace_back(std::make_shared<DownloadThread>());
      try {
        downloadThreads.back()->thread =
            std::make_unique<folly::CPUThreadPoolExecutor>(1);
        downloadThreads.back()->thread->add(
            [this, thread = downloadThreads.back()] {
              downloadThreadFunc(thread->stopFlag);
            });
      } catch (...) {
        downloadThreads.pop_back();
        throw;
      }
    }
  } else if (threads_num < downloadThreadsNum) {
    size_t remove_threads = downloadThreadsNum - threads_num;

    {
      std::lock_guard lock(downloadQueue->mutex);
      for (size_t i = 0; i < remove_threads; ++i) {
        downloadThreads[downloadThreads.size() - 1 - i]->stopFlag = true;
      }
    }

    downloadQueue->cv.notify_all();

    for (size_t i = 0; i < remove_threads; ++i) {
      VELOX_DCHECK(downloadThreads.back()->stopFlag);

      auto& thread = downloadThreads.back()->thread;
      if (thread) {
        thread->join();
      }

      downloadThreads.pop_back();
    }
  }
  return true;
}

bool KeyMetadata::addToDownloadQueue(FileSegmentPtr file_segment) {
  return cacheMetadata->downloadQueue->add(file_segment);
}

void KeyMetadata::addToCleanupQueue() {
  cacheMetadata->cleanupQueue->add(key);
}

LockedKey::LockedKey(std::shared_ptr<KeyMetadata> key_metadata_)
    : keyMetadata(key_metadata_), lock(keyMetadata->guard.lock()) {}

LockedKey::~LockedKey() {
  if (!keyMetadata->empty() || getKeyState() != KeyMetadata::KeyState::ACTIVE) {
    return;
  }

  /// If state if ACTIVE and key turns out empty - we submit it for delayed removal.
  /// Because we do not want to always lock all cache metadata lock, when we remove files segments.
  /// but sometimes we do - we remove the empty key without delay - then key state
  /// will be REMOVED here and we will return in the check above.
  /// See comment near cleanupThreadFunc() for more details.

  keyMetadata->keyState = KeyMetadata::KeyState::REMOVING;
  VLOG(1) << fmt::format("Submitting key {} for removal", getKey());
  keyMetadata->addToCleanupQueue();
}

void LockedKey::removeFromCleanupQueue() {
  if (keyMetadata->keyState != KeyMetadata::KeyState::REMOVING) {
    VELOX_FAIL("Cannot remove non-removing");
  }

  /// Just mark key_state as "not to be removed", the cleanup thread will check it and skip the key.
  keyMetadata->keyState = KeyMetadata::KeyState::ACTIVE;
}

void LockedKey::markAsRemoved() {
  keyMetadata->keyState = KeyMetadata::KeyState::REMOVED;
}

bool LockedKey::isLastOwnerOfFileSegment(size_t offset) const {
  const auto file_segment_metadata = getByOffset(offset);
  return file_segment_metadata->fileSegment.use_count() == 2;
}

bool LockedKey::removeAllFileSegments() {
  bool removed_all = true;
  for (auto it = keyMetadata->begin(); it != keyMetadata->end();) {
    if (!it->second->releasable()) {
      ++it;
      removed_all = false;
      continue;
    }
    if (it->second->isEvictingOrRemoved(*this)) {
      /// File segment is currently a removal candidate,
      /// we do not know if it will be removed or not yet,
      /// but its size is currently accounted as potentially removed,
      /// so if we remove file segment now, we break the freeable_count
      /// calculation in tryReserve.
      ++it;
      removed_all = false;
      continue;
    }

    auto file_segment = it->second->fileSegment;
    it = removeFileSegment(file_segment->offset(), file_segment->lock());
  }
  return removed_all;
}

KeyMetadata::iterator LockedKey::removeFileSegmentIfExists(
    size_t offset,
    bool can_be_broken,
    bool invalidate_queue_entry) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    return {};
  }

  auto file_segment = it->second->fileSegment;
  return removeFileSegmentImpl(
      it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(
    size_t offset,
    bool can_be_broken,
    bool invalidate_queue_entry) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    VELOX_FAIL("There is no offset {}", offset);
  }

  auto file_segment = it->second->fileSegment;
  return removeFileSegmentImpl(
      it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(
    size_t offset,
    const FileSegmentGuard::Lock& segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    VELOX_FAIL("There is no offset {} in key {}", offset, getKey());
  }

  return removeFileSegmentImpl(
      it, segment_lock, can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegmentImpl(
    KeyMetadata::iterator it,
    const FileSegmentGuard::Lock& segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry) {
  auto file_segment = it->second->fileSegment;

  VLOG(1) << fmt::format(
      "Remove from cache. Key: {}, offset: {}, size: {}",
      getKey(),
      file_segment->offset(),
      file_segment->reservedSize.load());

  VELOX_DCHECK(
      can_be_broken || file_segment->assertCorrectnessUnlocked(segment_lock));

  if (file_segment->queueIterator && invalidate_queue_entry) {
    file_segment->queueIterator->invalidate();
  }

  try {
    file_segment->detach(segment_lock, *this);
  } catch (...) {
    tryLogCurrentException(__PRETTY_FUNCTION__);
    VELOX_DCHECK(false);
    /// Do not rethrow, we must delete the file below.
  }

  try {
    const auto path = keyMetadata->getFileSegmentPath(*file_segment);
    if (file_segment->downloadedSize == 0) {
      VELOX_DCHECK(!fs::exists(path));
    } else if (fs::exists(path)) {
      fs::remove(path);

      /// Clear OpenedFileCache to avoid reading from incorrect file descriptor.
      /// TODO(io): CH OpenedFileCache has no Velox filecache counterpart in this port.

      VLOG(1) << fmt::format("Removed file segment at path: {}", path);
    } else if (!can_be_broken) {
#ifdef DEBUG_OR_SANITIZER_BUILD
      VELOX_FAIL("Expected path {} to exist", path);
#else
      LOG(WARNING) << fmt::format(
          "Expected path {} to exist, while removing {}:{}",
          path,
          getKey(),
          file_segment->offset());
#endif
    }
  } catch (...) {
    tryLogCurrentException(__PRETTY_FUNCTION__);
    VELOX_DCHECK(false);
  }

  return keyMetadata->erase(it);
}

bool LockedKey::addToDownloadQueue(
    size_t offset,
    const FileSegmentGuard::Lock&) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    VELOX_FAIL("There is not offset {}", offset);
  }
  return keyMetadata->addToDownloadQueue(it->second->fileSegment);
}

std::optional<FileSegment::Range> LockedKey::hasIntersectingRange(
    const FileSegment::Range& range) const {
  if (keyMetadata->empty()) {
    return {};
  }

  auto it = keyMetadata->lower_bound(range.left);
  if (it != keyMetadata->end()) /// has next range
  {
    auto next_range = it->second->fileSegment->range();
    if (!(range < next_range)) {
      return next_range;
    }

    if (it == keyMetadata->begin()) {
      return {};
    }
  }

  auto prev_range = std::prev(it)->second->fileSegment->range();
  if (!(prev_range < range)) {
    return prev_range;
  }

  return {};
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::getByOffset(
    size_t offset) const {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    VELOX_FAIL("There is not offset {}", offset);
  }
  return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::getByOffset(size_t offset) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    VELOX_FAIL("There is not offset {}", offset);
  }
  return it->second;
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::tryGetByOffset(
    size_t offset) const {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::tryGetByOffset(size_t offset) {
  auto it = keyMetadata->find(offset);
  if (it == keyMetadata->end()) {
    return nullptr;
  }
  return it->second;
}

std::string LockedKey::toString() const {
  std::string result;
  for (auto it = keyMetadata->begin(); it != keyMetadata->end(); ++it) {
    if (it != keyMetadata->begin()) {
      result += ", ";
    }
    result += std::to_string(it->first);
  }
  return result;
}

std::vector<FileSegment::Info> LockedKey::sync() {
  std::vector<FileSegment::Info> broken;
  for (auto it = keyMetadata->begin(); it != keyMetadata->end();) {
    if (it->second->isEvictingOrRemoved(*this) || !it->second->releasable()) {
      ++it;
      continue;
    }

    auto file_segment = it->second->fileSegment;
    if (file_segment->isDetached()) {
      VELOX_FAIL(
          "File segment has unexpected state: DETACHED ({})",
          file_segment->getInfoForLog());
    }

    if (file_segment->getDownloadedSize() == 0) {
      ++it;
      continue;
    }

    const auto& path = keyMetadata->getFileSegmentPath(*file_segment);
    if (!fs::exists(path)) {
      LOG(WARNING) << fmt::format(
          "File segment has DOWNLOADED state, but file does not exist ({})",
          file_segment->getInfoForLog());

      broken.push_back(FileSegment::getInfo(file_segment));
      it = removeFileSegment(
          file_segment->offset(), file_segment->lock(), /* can_be_broken */ true);
      continue;
    }

    const size_t actual_size = fs::file_size(path);
    const size_t expected_size = file_segment->getDownloadedSize();

    if (actual_size == expected_size) {
      ++it;
      continue;
    }

    LOG(WARNING) << fmt::format(
        "File segment has unexpected size. Having {}, expected {} ({})",
        actual_size,
        expected_size,
        file_segment->getInfoForLog());

    broken.push_back(FileSegment::getInfo(file_segment));
    it = removeFileSegment(
        file_segment->offset(), file_segment->lock(), /* can_be_broken */ true);
  }
  return broken;
}

} // namespace facebook::velox::ch
