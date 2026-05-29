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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <boost/noncopyable.hpp>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/FileCacheKey.h"
#include "velox/common/caching/filecache/FileCache_fwd_internal.h"
#include "velox/common/caching/filecache/FileSegment.h"
#include "velox/common/caching/filecache/Guards.h"
#include "velox/common/caching/filecache/IFileCachePriority.h"

namespace facebook::velox::ch {

class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;
class DownloadQueue;
using DownloadQueuePtr = std::shared_ptr<DownloadQueue>;

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;
class CacheMetadata;

namespace ErrorCodes {
extern const int LOGICAL_ERROR;
}

struct FileSegmentMetadata : private boost::noncopyable {
  using Priority = IFileCachePriority;

  explicit FileSegmentMetadata(FileSegmentPtr&& file_segment_);

  bool releasable() const {
    return fileSegment.use_count() == 1;
  }

  size_t size() const;

  bool isEvictingOrRemoved(const LockedKey& lock) const {
    return isRemoved(lock) || isEvicting(lock);
  }

  /// Whether queue entry is removed/evicted.
  bool isRemoved(const LockedKey&) const {
    return removed;
  }

  /// Whether queue entry is in evicting state.
  bool isEvicting(const LockedKey&) const {
    auto iterator = getQueueIterator();
    if (!iterator) {
      return false;
    }
    const auto entry_state = iterator->getEntry()->getState();
    return entry_state == Priority::Entry::State::Evicting;
  }

  void setRemovedFlag(const LockedKey&, bool value = true) {
    removed = value;
    VELOX_DCHECK(!getQueueIterator());
  }

  void setEvictingFlag(const LockedKey& lock) const {
    auto iterator = getQueueIterator();
    if (!iterator) {
      VELOX_FAIL("Iterator is not set");
    }
    iterator->getEntry()->setEvictingFlag(lock);
  }

  void resetEvictingFlag() const {
    auto iterator = getQueueIterator();
    if (!iterator) {
      VELOX_FAIL("Iterator is not set");
    }

    const auto& entry = iterator->getEntry();
    VELOX_DCHECK(size() == entry->size);
    entry->resetFlag(Priority::Entry::State::Evicting);
  }

  Priority::IteratorPtr getQueueIterator() const {
    return fileSegment->getQueueIterator();
  }

  FileSegmentPtr fileSegment;

 private:
  /// If removed=true, then iterator is invalid.
  bool removed = false;
};

using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;

struct KeyMetadata : private std::map<size_t, FileSegmentMetadataPtr>,
                     private boost::noncopyable,
                     public std::enable_shared_from_this<KeyMetadata> {
  friend class CacheMetadata;
  friend struct LockedKey;

  using Key = FileCacheKey;
  using iterator = iterator;
  using OriginInfo = FileCacheOriginInfo;
  using UserID = OriginInfo::UserID;

  KeyMetadata(
      const Key& key_,
      const OriginInfo& origin_,
      const CacheMetadata* cache_metadata_,
      bool created_base_directory_ = false);

  enum class KeyState : uint8_t {
    ACTIVE,
    REMOVING,
    REMOVED,
  };

  const Key key;
  const OriginInfo origin;

  LockedKeyPtr lock();

  /// Will only fail if key is not in ACTIVE state, e.g. REMOVING or REMOVED.
  LockedKeyPtr tryLock();

  bool createBaseDirectory(bool throw_if_failed = false);

  std::string getPath() const;

  std::string getFileSegmentPath(const FileSegment& file_segment) const;

  bool checkAccess(const UserID& user_id_) const;

  void assertAccess(const UserID& user_id_) const;

  /// This method is used for loadMetadata() on server startup,
  /// where we know there is no concurrency on Key and we do not want therefore taking a KeyGuard::Lock,
  /// therefore we use this Unlocked version. This method should not be used anywhere else.
  template <class... Args>
  auto emplaceUnlocked(Args&&... args) {
    return emplace(std::forward<Args>(args)...);
  }
  size_t sizeUnlocked() const {
    return size();
  }

  KeyState getState();

 private:
  const CacheMetadata* cacheMetadata;

  KeyState keyState = KeyState::ACTIVE;
  KeyGuard guard;

  std::atomic<bool> createdBaseDirectory = false;

  LockedKeyPtr lockNoStateCheck();
  // TODO(logging): CH logger() was used only for LOG_* call sites; map .cpp logs to glog.
  bool addToDownloadQueue(FileSegmentPtr file_segment);
  void addToCleanupQueue();
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

class CacheMetadata : private boost::noncopyable {
  friend struct KeyMetadata;
  class IteratorImpl;
  class BatchedIteratorImpl;
  using IteratorImplPtr = std::shared_ptr<IteratorImpl>;
  using BatchedIteratorImplPtr = std::shared_ptr<BatchedIteratorImpl>;

 public:
  using Key = FileCacheKey;
  using IterateFunc = std::function<void(LockedKey&)>;
  using OriginInfo = FileCacheOriginInfo;
  using UserID = OriginInfo::UserID;

  explicit CacheMetadata(
      const std::string& path_,
      size_t background_download_queue_size_limit_,
      size_t background_download_threads_,
      bool write_cache_per_user_directory_);

  virtual ~CacheMetadata();

  void startup();

  bool isEmpty() const;

  const std::string& getBaseDirectory() const { return path; }

  std::string getKeyPath(const Key& key, const OriginInfo& origin) const;

  std::string getFileSegmentPath(
      const Key& key,
      size_t offset,
      FileSegmentKind segment_kind,
      const OriginInfo& origin) const;

  void iterate(IterateFunc&& func, const UserID& user_id);

  class Iterator;
  using IteratorPtr = std::unique_ptr<Iterator>;
  IteratorPtr getIterator(const UserID& user_id);

  enum class KeyNotFoundPolicy : uint8_t {
    THROW,
    THROW_LOGICAL,
    CREATE_EMPTY,
    RETURN_NULL,
  };

  KeyMetadataPtr getKeyMetadata(
      const Key& key,
      KeyNotFoundPolicy key_not_found_policy,
      const OriginInfo& origin,
      bool is_initial_load = false);

  LockedKeyPtr lockKeyMetadata(
      const Key& key,
      KeyNotFoundPolicy key_not_found_policy,
      const OriginInfo& origin,
      bool is_initial_load = false);

  void removeKey(const Key& key, bool if_exists, const UserID& user_id);
  void removeAllKeys(const UserID& user_id);

  void shutdown();

  bool setBackgroundDownloadThreads(size_t threads_num);
  size_t getBackgroundDownloadThreads() const {
    return downloadThreads.size();
  }

  bool setBackgroundDownloadQueueSizeLimit(size_t size);

  bool isBackgroundDownloadEnabled();

 private:
  static constexpr size_t bucketsNum = 1024;

  const std::string path;
  const CleanupQueuePtr cleanupQueue;
  const DownloadQueuePtr downloadQueue;
  const bool writeCachePerUserDirectory;

  mutable std::shared_mutex keyPrefixDirectoryMutex; // TODO(ch-port): CH Common/SharedMutex.h -> std::shared_mutex per Velox port convention.

  struct MetadataBucket : public std::unordered_map<FileCacheKey, KeyMetadataPtr> {
    CacheMetadataGuard::Lock lock() const;

   private:
    mutable CacheMetadataGuard guard;
  };
  using MetadataBuckets = std::vector<MetadataBucket>;
  MetadataBuckets metadataBuckets{bucketsNum};

  struct DownloadThread {
    std::unique_ptr<folly::CPUThreadPoolExecutor> thread; // TODO(threading): CH ThreadFromGlobalPool per download worker -> folly CPUThreadPoolExecutor.
    bool stopFlag{false};
  };

  std::atomic<size_t> downloadThreadsNum;
  std::vector<std::shared_ptr<DownloadThread>> downloadThreads;
  std::unique_ptr<folly::CPUThreadPoolExecutor> cleanupThread; // TODO(threading): CH ThreadFromGlobalPool cleanup worker -> folly CPUThreadPoolExecutor.

  static std::string getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind);

  MetadataBucket& getMetadataBucket(const Key& key);
  void downloadImpl(FileSegment& file_segment, std::optional<std::vector<char>>& memory) const;
  MetadataBucket::iterator removeEmptyKey(
      MetadataBucket& bucket,
      MetadataBucket::iterator it,
      LockedKey&,
      const CacheMetadataGuard::Lock&);

  void downloadThreadFunc(const bool& stop_flag);

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

class CacheMetadata::Iterator {
 public:
  using Impl = std::variant<CacheMetadata::IteratorImplPtr, CacheMetadata::BatchedIteratorImplPtr>;
  explicit Iterator(const UserID& user_id_, MetadataBuckets& metadata_buckets_);

  using OnFileSegmentFunc = std::function<void(const FileSegmentInfo&)>;
  /// Execute func for one more file segment.
  /// Cannot be used from different threads.
  bool next(OnFileSegmentFunc func);
  /// Execute func for a batch of file segments.
  /// Safe to be used from different threads.
  bool nextBatch(OnFileSegmentFunc func);

 protected:
  const UserID userId;
  MetadataBuckets& metadataBuckets;
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
struct LockedKey : private boost::noncopyable {
  using Key = FileCacheKey;

  explicit LockedKey(std::shared_ptr<KeyMetadata> key_metadata_);

  ~LockedKey();

  const Key& getKey() const {
    return keyMetadata->key;
  }

  auto begin() const {
    return keyMetadata->begin();
  }
  auto rbegin() const {
    return keyMetadata->rbegin();
  }

  auto end() const {
    return keyMetadata->end();
  }
  auto rend() const {
    return keyMetadata->rend();
  }

  bool empty() const {
    return keyMetadata->empty();
  }
  auto lower_bound(size_t size) const {
    return keyMetadata->lower_bound(size);
  } /// NOLINT
  template <class... Args>
  auto emplace(Args&&... args) {
    return keyMetadata->emplace(std::forward<Args>(args)...);
  }

  std::shared_ptr<const FileSegmentMetadata> getByOffset(size_t offset) const;
  std::shared_ptr<FileSegmentMetadata> getByOffset(size_t offset);

  std::shared_ptr<const FileSegmentMetadata> tryGetByOffset(size_t offset) const;
  std::shared_ptr<FileSegmentMetadata> tryGetByOffset(size_t offset);

  KeyMetadata::KeyState getKeyState() const {
    return keyMetadata->keyState;
  }

  std::shared_ptr<const KeyMetadata> getKeyMetadata() const {
    return keyMetadata;
  }
  std::shared_ptr<KeyMetadata> getKeyMetadata() {
    return keyMetadata;
  }

  bool removeAllFileSegments();

  KeyMetadata::iterator removeFileSegment(
      size_t offset,
      const FileSegmentGuard::Lock&,
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

  bool addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock&);

  bool isLastOwnerOfFileSegment(size_t offset) const;

  std::optional<FileSegment::Range> hasIntersectingRange(const FileSegment::Range& range) const;

  void removeFromCleanupQueue();

  void markAsRemoved();

  std::vector<FileSegment::Info> sync();

  std::string toString() const;

 private:
  KeyMetadata::iterator removeFileSegmentImpl(
      KeyMetadata::iterator it,
      const FileSegmentGuard::Lock&,
      bool can_be_broken = false,
      bool invalidate_queue_entry = true);

  const std::shared_ptr<KeyMetadata> keyMetadata;
  KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
};

} // namespace facebook::velox::ch
