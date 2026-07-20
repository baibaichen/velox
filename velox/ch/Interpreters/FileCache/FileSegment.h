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
#include "velox/ch/Common/ClickHouseAssert.h"
#include "velox/ch/Common/logger_useful.h"
#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/IO/WriteBufferFromVeloxWriteFile.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCache_fwd_internal.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentInfo.h"
#include "velox/ch/Interpreters/FileCache/Guards.h"
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"

#include <folly/CancellationToken.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace facebook::velox::ch
{

class FileCache;
struct FileCacheReserveStat;

struct CreateFileSegmentSettings
{
    FileSegmentKind kind = FileSegmentKind::Regular;
    bool unbounded = false;

    CreateFileSegmentSettings() = default;

    explicit CreateFileSegmentSettings(FileSegmentKind kind_)
        : kind(kind_), unbounded(kind == FileSegmentKind::Ephemeral)
    {
    }
};

class FileSegment
{
    friend struct LockedKey;
    friend class FileCache; /// Because of reserved_size in tryReserve().

public:
    FileSegment(const FileSegment &) = delete;
    FileSegment & operator=(const FileSegment &) = delete;
    FileSegment(FileSegment &&) = delete;
    FileSegment & operator=(FileSegment &&) = delete;

    using Key = FileCacheKey;
    /// Two-layer ownership preserved from CH (see design 09): the wrapper is
    /// shared, and it exclusively owns the underlying Velox file.
    using RemoteFileReaderPtr = std::shared_ptr<ReadBufferFromVeloxReadFile>;
    using LocalCacheWriterPtr = std::shared_ptr<WriteBufferFromVeloxWriteFile>;
    using Downloader = std::string;
    using DownloaderId = std::string;
    using Priority = IFileCachePriority;
    using State = FileSegmentState;
    using Info = FileSegmentInfo;
    using QueueEntryType = IFileCachePriority::QueueEntryType;
    using KeyType = FileSegmentKeyType;

    FileSegment(
        const Key & key_,
        size_t offset_,
        size_t size_,
        State download_state_,
        const CreateFileSegmentSettings & create_settings = {},
        bool background_download_enabled_ = false,
        FileCache * cache_ = nullptr,
        std::weak_ptr<KeyMetadata> key_metadata_ = std::weak_ptr<KeyMetadata>(),
        Priority::IteratorPtr queue_iterator_ = nullptr,
        bool size_in_filename_ = false);

    ~FileSegment();

    State state() const;

    static String stateToString(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range
    {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_);

        bool operator==(const Range & other) const { return left == other.left && right == other.right; }

        bool operator<(const Range & other) const { return right < other.left; }

        size_t size() const { return right - left + 1; }

        bool contains(size_t point) const { return left <= point && point <= right; }

        bool contains(const Range & other) const { return contains(other.left) && contains(other.right); }

        String toString() const { return fmt::format("[{}, {}]", left, right); }
    };

    static String getCallerId();

    String getInfoForLog() const;

    /**
     * ========== Methods to get file segment's constant state ==================
     */

    const Range & range() const { return segment_range; }

    const Key & key() const { return file_key; }

    size_t offset() const { return range().left; }

    FileSegmentKind getKind() const { return segment_kind; }

    bool isUnbound() const { return is_unbound; }

    /// Whether the segment's file on disk currently has its size encoded in the
    /// name (`<offset>_<size>`).
    bool hasSizeInFileName() const { return size_in_filename; }

    String getPath() const;

    /**
     * ========== Methods for _any_ file segment's owner ========================
     */

    String getOrSetDownloader();

    bool isDownloader() const;

    DownloaderId getDownloader() const;

    /// Wait for the change of state from DOWNLOADING to any other. Observes the
    /// injected cancellation token while blocked (CH uses CurrentThread's
    /// QueryStatus); see design 09.
    State wait(size_t offset, const folly::CancellationToken & cancellation_token);

    bool isDownloaded() const;

    time_t getFinishedDownloadTime() const;

    size_t getHitsCount() const { return hits_count; }

    size_t getCurrentWriteOffset() const;

    size_t getDownloadedSize() const;

    size_t getReservedSize() const;

    void detach(const FileSegmentGuard::Lock &, const LockedKey &);

    static FileSegmentInfo getInfo(const FileSegmentPtr & file_segment);

    bool isDetached() const;

    /// Completed states are final: DOWNLOADED, DETACHED.
    bool isCompleted(bool sync = false) const;

    void increasePriority();

    /**
     * ========== Methods used by `cache` ========================
     */

    FileSegmentGuard::Lock lock() const;

    Priority::IteratorPtr getQueueIterator() const;

    void setQueueIterator(Priority::IteratorPtr iterator);

    void markDelayedRemovalAndResetQueueIterator();

    void restoreQueueIteratorAfterDelayedRemoval(Priority::IteratorPtr iterator);

    KeyMetadataPtr tryGetKeyMetadata() const;

    KeyMetadataPtr getKeyMetadata() const;

    bool assertCorrectness() const;

    size_t getSizeForBackgroundDownload() const;

    /**
     * ========== Methods that must do cv.notify() ==================
     */

    static void complete(FileSegmentPtr && file_segment, bool allow_background_download, bool force_shrink_to_downloaded_size);

    void completePartAndResetDownloader();

    void resetDownloader();

    /**
     * ========== Methods for _only_ file segment's `downloader` ==================
     */

    bool reserve(
        size_t size_to_reserve,
        size_t lock_wait_timeout_milliseconds,
        std::string & failure_reason,
        FileCacheReserveStat * reserve_stat = nullptr,
        size_t reserve_hint = 0);

    /// Write data into reserved space.
    void write(char * from, size_t size, size_t offset_in_file);

    RemoteFileReaderPtr getRemoteFileReader();
    LocalCacheWriterPtr getLocalCacheWriter();

    RemoteFileReaderPtr extractRemoteFileReader();

    void resetRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void setDownloadFailed();

    void setDownloadFinishedWithoutContinuation();

    bool isBackgroundDownloadEnabled() const { return background_download_enabled; }

private:
    struct DownloadState;

    String getDownloaderUnlocked(const FileSegmentGuard::Lock &) const;
    DownloadState & getOrCreateDownloadDataUnlocked(const FileSegmentGuard::Lock &);
    void resetDownloadDataUnlocked(const FileSegmentGuard::Lock &);

    const LoggerPtr & getLog() const;
    bool isDownloaderUnlocked(const FileSegmentGuard::Lock & segment_lock) const;
    void resetDownloaderUnlocked(const FileSegmentGuard::Lock &);
    size_t getSizeForBackgroundDownloadUnlocked(const FileSegmentGuard::Lock &) const;

    void setDownloadState(State state, const FileSegmentGuard::Lock &);
    void resetDownloadingStateUnlocked(const FileSegmentGuard::Lock &);
    void setDetachedState(const FileSegmentGuard::Lock &);

    String getInfoForLogUnlocked(const FileSegmentGuard::Lock &) const;

    void setDownloadedUnlocked(const FileSegmentGuard::Lock &);
    void setDownloadFailedUnlocked(const FileSegmentGuard::Lock &);

    void renameToIncludeSizeInNameUnlocked(const FileSegmentGuard::Lock &);
    void shrinkFileSegmentToDownloadedSize(const LockedKey &, const FileSegmentGuard::Lock &, bool force_shrink_to_downloaded_size);

    void assertNotDetached() const;
    void assertNotDetachedUnlocked(const FileSegmentGuard::Lock &) const;
    void assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock &) const;
    bool assertCorrectnessUnlocked(const FileSegmentGuard::Lock &) const;

    LockedKeyPtr lockKeyMetadata(bool assert_exists = true) const;

    String tryGetPath() const;

    void complete(const LockedKeyPtr & locked_key, bool allow_background_download, bool force_shrink_to_downloaded_size);

    const Key file_key;
    Range segment_range;
    const FileSegmentKind segment_kind;
    /// is_unbound == true for temporary data in cache.
    const bool is_unbound;
    const bool background_download_enabled;

    /// Whether the on-disk file is named `<offset>_<size>`. Only transitions
    /// false -> true, under `segment_guard`; reads in `getPath` are lock-free.
    std::atomic<bool> size_in_filename;

    std::atomic<State> download_state;
    time_t download_finished_time = 0;

    /// downloaded_size should always be less or equal to reserved_size
    std::atomic<size_t> downloaded_size = 0;
    std::atomic<size_t> reserved_size = 0;

    struct DownloadState
    {
        DownloaderId downloader_id;
        RemoteFileReaderPtr remote_file_reader;
        LocalCacheWriterPtr cache_writer;
        /// Only used for an assertion in debug/sanitizer builds.
        mutable std::mutex write_mutex;
    };
    std::unique_ptr<DownloadState> download_data;

    mutable FileSegmentGuard segment_guard;
    std::weak_ptr<KeyMetadata> key_metadata;
    mutable Priority::IteratorPtr queue_iterator;
    FileCache * cache;
    std::condition_variable cv;
    /// Dedups concurrent increasePriority() calls; a pure try-lock.
    std::atomic_flag increasing_priority;

#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)
    LoggerPtr log;
#endif

    std::atomic<size_t> hits_count = 0;

    /// Guarded by `segment_guard`. Set while dynamic-resize eviction is pending.
    bool on_delayed_removal = false;
};


struct FileSegmentsHolder final
{
    FileSegmentsHolder() = default;

    FileSegmentsHolder(const FileSegmentsHolder &) = delete;
    FileSegmentsHolder & operator=(const FileSegmentsHolder &) = delete;

    explicit FileSegmentsHolder(FileSegments && file_segments_);

    ~FileSegmentsHolder();

    bool empty() const { return file_segments.empty(); }

    size_t size() const { return file_segments.size(); }

    String toString(bool with_state = false) const;

    void completeAndPopFront(bool allow_background_download, bool force_shrink_to_downloaded_size)
    {
        completeAndPopFrontImpl(allow_background_download, force_shrink_to_downloaded_size);
    }

    FileSegment & front() { return *file_segments.front(); }
    const FileSegment & front() const { return *file_segments.front(); }

    FileSegment & back() { return *file_segments.back(); }
    const FileSegment & back() const { return *file_segments.back(); }

    FileSegment & add(FileSegmentPtr && file_segment);

    FileSegments::iterator begin() { return file_segments.begin(); }
    FileSegments::iterator end() { return file_segments.end(); }

    FileSegments::const_iterator begin() const { return file_segments.begin(); }
    FileSegments::const_iterator end() const { return file_segments.end(); }
    FileSegmentPtr getSingleFileSegment() const;

    void reset();

private:
    FileSegments file_segments{};

    FileSegments::iterator completeAndPopFrontImpl(bool allow_background_download, bool force_shrink_to_downloaded_size);
};

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

String toString(const FileSegments & file_segments, bool with_state = false);

} // namespace facebook::velox::ch
