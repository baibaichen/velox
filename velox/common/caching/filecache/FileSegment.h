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

#include <fcntl.h>
#include <atomic>
#include <condition_variable>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>

#include <boost/noncopyable.hpp>
#include <fmt/format.h>

#include "velox/common/caching/filecache/FileCacheKey.h"
#include "velox/common/caching/filecache/Guards.h"
#include "velox/common/caching/filecache/IFileCachePriority.h"
#include "velox/common/caching/filecache/FileSegmentInfo.h"
#include "velox/common/caching/filecache/FileCache_fwd_internal.h"

namespace facebook::velox {
class ByteInputStream;
}

namespace facebook::velox::ch {

class WriteBufferFromFile; // TODO(filecache-io): Port ClickHouse IO WriteBufferFromFile.
struct FileCacheReserveStat;

struct CreateFileSegmentSettings {
  FileSegmentKind kind = FileSegmentKind::Regular;
  bool unbounded = false;

  CreateFileSegmentSettings() = default;

  explicit CreateFileSegmentSettings(FileSegmentKind kind_)
      : kind(kind_), unbounded(kind == FileSegmentKind::Ephemeral) {}
};

class FileSegment : private boost::noncopyable {
  friend struct LockedKey;
  friend class FileCache; /// Because of reserved_size in tryReserve().

 public:
  using Key = FileCacheKey;
  using RemoteFileReaderPtr = std::shared_ptr<velox::ByteInputStream>;
  using LocalCacheWriterPtr = std::shared_ptr<WriteBufferFromFile>;
  using Downloader = std::string;
  using DownloaderId = std::string;
  using Priority = IFileCachePriority;
  using State = FileSegmentState;
  using Info = FileSegmentInfo;
  using QueueEntryType = FileCacheQueueEntryType;
  using KeyType = FileSegmentKeyType;

  FileSegment(
      const Key& key_,
      size_t offset_,
      size_t size_,
      State download_state_,
      const CreateFileSegmentSettings& create_settings = {},
      bool background_download_enabled_ = false,
      FileCache* cache_ = nullptr,
      std::weak_ptr<KeyMetadata> key_metadata_ = std::weak_ptr<KeyMetadata>(),
      Priority::IteratorPtr queue_iterator_ = nullptr);

  ~FileSegment();

  State state() const;

  static std::string stateToString(FileSegment::State state);

  /// Represents an interval [left, right] including both boundaries.
  struct Range {
    size_t left;
    size_t right;

    Range(size_t left_, size_t right_);

    bool operator==(const Range& other) const {
      return left == other.left && right == other.right;
    }

    bool operator<(const Range& other) const {
      return right < other.left;
    }

    size_t size() const {
      return right - left + 1;
    }

    bool contains(size_t point) const {
      return left <= point && point <= right;
    }

    bool contains(const Range& other) const {
      return contains(other.left) && contains(other.right);
    }

    std::string toString() const {
      return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right));
    }
  };

  static std::string getCallerId();

  std::string getInfoForLog() const;

  /**
   * ========== Methods to get file segment's constant state ==================
   */

  const Range& range() const {
    return segmentRange;
  }

  const Key& key() const {
    return fileKey;
  }

  size_t offset() const {
    return range().left;
  }

  FileSegmentKind getKind() const {
    return segmentKind;
  }

  bool isUnbound() const {
    return isUnbound_;
  }

  std::string getPath() const;

  int getFlagsForLocalRead() const {
    return O_RDONLY | O_CLOEXEC;
  }

  /**
   * ========== Methods for _any_ file segment's owner ========================
   */

  std::string getOrSetDownloader();

  bool isDownloader() const;

  DownloaderId getDownloader() const;

  /// Wait for the change of state from DOWNLOADING to any other.
  State wait(size_t offset);

  bool isDownloaded() const;

  time_t getFinishedDownloadTime() const;

  size_t getHitsCount() const {
    return hitsCount;
  }

  size_t getRefCount() const {
    return refCount;
  }

  size_t getCurrentWriteOffset() const;

  size_t getDownloadedSize() const;

  size_t getReservedSize() const;

  /// Now detached status can be used in the following cases:
  /// 1. there is only 1 remaining file segment holder
  ///    && it does not need this segment anymore
  ///    && this file segment was in cache and needs to be removed
  /// 2. in read_from_cache_if_exists_otherwise_bypass_cache case to create NOOP file segments.
  /// 3. removeIfExists - method which removes file segments from cache even though
  ///    it might be used at the moment.

  /// If file segment is detached it means the following:
  /// 1. It is not present in FileCache, e.g. will not be visible to any cache user apart from
  /// those who acquired shared pointer to this file segment before it was detached.
  /// 2. Detached file segment can still be hold by some cache users, but it's state became
  /// immutable at the point it was detached, any non-const / stateful method will throw an
  /// exception.
  void detach(const FileSegmentGuard::Lock&, const LockedKey&);

  static FileSegmentInfo getInfo(const FileSegmentPtr& file_segment);

  bool isDetached() const;

  /// File segment has a completed state, if this state is final and
  /// is not going to be changed. Completed states: DOWNALODED, DETACHED.
  bool isCompleted(bool sync = false) const;

  void increasePriority();

  /**
   * ========== Methods used by `cache` ========================
   */

  FileSegmentGuard::Lock lock() const;

  Priority::IteratorPtr getQueueIterator() const;

  void setQueueIterator(Priority::IteratorPtr iterator);

  void markDelayedRemovalAndResetQueueIterator();

  /// Restore a queue iterator after dynamic-resize eviction failed.
  /// The segment must have been marked by `markDelayedRemovalAndResetQueueIterator`.
  void restoreQueueIteratorAfterDelayedRemoval(Priority::IteratorPtr iterator);

  KeyMetadataPtr tryGetKeyMetadata() const;

  KeyMetadataPtr getKeyMetadata() const;

  bool assertCorrectness() const;

  size_t getSizeForBackgroundDownload() const;

  /**
   * ========== Methods that must do cv.notify() ==================
   */

  static void complete(
      FileSegmentPtr&& file_segment,
      bool allow_background_download,
      bool force_shrink_to_downloaded_size);

  void completePartAndResetDownloader();

  void resetDownloader();

  /**
   * ========== Methods for _only_ file segment's `downloader` ==================
   */

  /// Try to reserve exactly `size` bytes (in addition to the getDownloadedSize() bytes already downloaded).
  /// Returns true if reservation was successful, false otherwise.
  bool reserve(
      size_t size_to_reserve,
      size_t lock_wait_timeout_milliseconds,
      std::string& failure_reason,
      FileCacheReserveStat* reserve_stat = nullptr);

  /// Write data into reserved space.
  void write(char* from, size_t size, size_t offset_in_file);

  /// Streams up to `num_bytes` from `reader` into `segment`, starting at the
  /// segment's current write offset, and returns the number of bytes written.
  ///
  /// Reserves cache space *before* consuming each chunk from `reader`. This
  /// ordering is required for correctness: `ByteInputStream` advances its read
  /// cursor as bytes are consumed and cannot seek backward (e.g.
  /// FileInputStream), so consuming before a failed reservation would lose
  /// bytes irrecoverably. On reservation failure `reserve()` already moves the
  /// segment to PARTIALLY_DOWNLOADED_NO_CONTINUATION; streaming then stops and
  /// the partial result is returned. Streaming also stops at reader EOF, which
  /// leaves the segment partially downloaded.
  ///
  /// Precondition: the caller is the segment's downloader and `num_bytes` does
  /// not exceed the bytes remaining in the segment's range. `reader` operates
  /// in absolute file coordinates: position 0 is the start of the file, so the
  /// reader is seeked to the segment's absolute write offset
  /// (`getCurrentWriteOffset()`), not a range-relative offset. `scratch` is a
  /// caller-owned, reusable buffer (grown as needed) used to copy each chunk.
  static size_t downloadFromReader(
      FileSegment& segment,
      velox::ByteInputStream& reader,
      size_t num_bytes,
      std::vector<char>& scratch,
      size_t lock_wait_timeout_milliseconds);

  // Invariant: if state() != DOWNLOADING and remote file reader is present, the reader's
  // available() == 0, and getFileOffsetOfBufferEnd() == our getCurrentWriteOffset().
  //
  // The reader typically requires its internal_buffer to be assigned from the outside before
  // calling next().
  RemoteFileReaderPtr getRemoteFileReader();
  LocalCacheWriterPtr getLocalCacheWriter();

  RemoteFileReaderPtr extractRemoteFileReader();

  void resetRemoteFileReader();

  void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

  void setDownloadFailed();

  /// Mark that no more data will be written to this segment (e.g. the remote object turned out
  /// to be smaller than expected), without treating it as a failure.
  /// The segment will be shrunk to the actually downloaded size during completion.
  void setDownloadFinishedWithoutContinuation();

  bool isBackgroundDownloadEnabled() const {
    return backgroundDownloadEnabled;
  }

 private:
  std::string getDownloaderUnlocked(const FileSegmentGuard::Lock&) const;
  bool isDownloaderUnlocked(const FileSegmentGuard::Lock& segment_lock) const;
  void resetDownloaderUnlocked(const FileSegmentGuard::Lock&);
  size_t getSizeForBackgroundDownloadUnlocked(const FileSegmentGuard::Lock&) const;

  void setDownloadState(State state, const FileSegmentGuard::Lock&);
  void resetDownloadingStateUnlocked(const FileSegmentGuard::Lock&);
  void setDetachedState(const FileSegmentGuard::Lock&);

  std::string getInfoForLogUnlocked(const FileSegmentGuard::Lock&) const;

  void setDownloadedUnlocked(const FileSegmentGuard::Lock&);
  void setDownloadFailedUnlocked(const FileSegmentGuard::Lock&);
  void shrinkFileSegmentToDownloadedSize(
      const LockedKey&,
      const FileSegmentGuard::Lock&,
      bool force_shrink_to_downloaded_size);

  void assertNotDetached() const;
  void assertNotDetachedUnlocked(const FileSegmentGuard::Lock&) const;
  void assertIsDownloaderUnlocked(
      const std::string& operation,
      const FileSegmentGuard::Lock&) const;
  bool assertCorrectnessUnlocked(const FileSegmentGuard::Lock&) const;

  LockedKeyPtr lockKeyMetadata(bool assert_exists = true) const;

  std::string tryGetPath() const;

  void complete(
      const LockedKeyPtr& locked_key,
      bool allow_background_download,
      bool force_shrink_to_downloaded_size);

  const Key fileKey;
  Range segmentRange;
  const FileSegmentKind segmentKind;
  /// Size of the segment is not known until it is downloaded and
  /// can be bigger than max_file_segment_size.
  /// is_unbound == true for temporary data in cache.
  const bool isUnbound_;
  const bool backgroundDownloadEnabled;

  std::atomic<State> downloadState;
  DownloaderId downloaderId; /// The one who prepares the download
  time_t downloadFinishedTime = 0;

  RemoteFileReaderPtr remoteFileReader;
  LocalCacheWriterPtr cacheWriter;

  /// downloaded_size should always be less or equal to reserved_size
  std::atomic<size_t> downloadedSize = 0;
  std::atomic<size_t> reservedSize = 0;
  mutable std::mutex writeMutex;

  mutable FileSegmentGuard segmentGuard;
  std::weak_ptr<KeyMetadata> keyMetadata;
  mutable Priority::IteratorPtr queueIterator; /// Iterator is put here on first reservation attempt, if successful.
  FileCache* cache;
  std::condition_variable cv;
  std::mutex increasePriorityMutex;

  std::atomic<size_t> hitsCount = 0; /// cache hits.
  std::atomic<size_t> refCount = 0; /// Used for getting snapshot state

  /// Guarded by `segment_guard`. Set while dynamic-resize eviction is pending.
  bool onDelayedRemoval = false;

  // TODO(metric): CH CurrentMetrics::Increment is a pure observation counter; no-op in header port.
};

struct FileSegmentsHolder final : private boost::noncopyable {
  FileSegmentsHolder() = default;

  explicit FileSegmentsHolder(FileSegments&& file_segments_);

  ~FileSegmentsHolder();

  bool empty() const {
    return fileSegments.empty();
  }

  size_t size() const {
    return fileSegments.size();
  }

  std::string toString(bool with_state = false) const;

  void completeAndPopFront(
      bool allow_background_download,
      bool force_shrink_to_downloaded_size) {
    completeAndPopFrontImpl(allow_background_download, force_shrink_to_downloaded_size);
  }

  FileSegment& front() {
    return *fileSegments.front();
  }
  const FileSegment& front() const {
    return *fileSegments.front();
  }

  FileSegment& back() {
    return *fileSegments.back();
  }
  const FileSegment& back() const {
    return *fileSegments.back();
  }

  FileSegment& add(FileSegmentPtr&& file_segment);

  FileSegments::iterator begin() {
    return fileSegments.begin();
  }
  FileSegments::iterator end() {
    return fileSegments.end();
  }

  FileSegments::const_iterator begin() const {
    return fileSegments.begin();
  }
  FileSegments::const_iterator end() const {
    return fileSegments.end();
  }
  FileSegmentPtr getSingleFileSegment() const;

  void reset();

 private:
  FileSegments fileSegments{};

  FileSegments::iterator completeAndPopFrontImpl(
      bool allow_background_download,
      bool force_shrink_to_downloaded_size);
};

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

std::string toString(const FileSegments& file_segments, bool with_state = false);

} // namespace facebook::velox::ch
