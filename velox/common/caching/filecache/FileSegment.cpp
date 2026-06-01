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
#include "velox/common/caching/filecache/FileSegment.h"

#include <algorithm>
#include <filesystem>
#include <sstream>
#include <utility>

#include <folly/ScopeGuard.h>
#include <folly/Conv.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/caching/filecache/FileCache.h"
#include "velox/common/caching/filecache/FileCacheUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/process/ProcessBase.h"

namespace fs = std::filesystem;

namespace facebook::velox::ch {

time_t timeInSeconds(std::chrono::system_clock::time_point tp) {
  return std::chrono::system_clock::to_time_t(tp);
}

template <typename T>
void unused(const T&) {}

class WriteBufferFromFile {
 public:
  explicit WriteBufferFromFile(const std::string& path)
      : file_(path, false, false) {}

  void append(std::string_view data) {
    file_.append(data);
  }

  void finalize() {
    // ClickHouse-faithful: do NOT fsync on segment completion. Upstream CH's
    // WriteBufferFromFileDescriptor::finalizeImpl() only flushes the userspace
    // buffer to the fd (::write); fsync lives in a separate sync() that the
    // download path never calls. Durability is intentionally not required: a
    // crash drops only the unwritten tail, and startup recovery treats the
    // on-disk file_size() as the authoritative downloaded size (see
    // FileCache::loadMetadataForKey), re-downloading any missing tail on demand.
    // append() already does a direct ::write into the page cache, so readers'
    // pread hits the data without an fsync. The previously-present file_.flush()
    // (=::fsync) was a port-introduced deviation that made cold writes ~10x
    // slower.
    file_.close();
  }

  void cancel() {
    file_.close();
  }

  uint64_t size() const {
    return file_.size();
  }

 private:
  velox::LocalWriteFile file_;
};

std::string toString(FileSegmentKind kind)
{
    return std::string(kind == FileSegmentKind::Regular ? "Regular" : "Ephemeral");
}

FileSegment::FileSegment(
        const Key & key_,
        size_t offset_,
        size_t size_,
        State downloadState_,
        const CreateFileSegmentSettings & settings,
        bool backgroundDownloadEnabled_,
        FileCache * cache_,
        std::weak_ptr<KeyMetadata> keyMetadata_,
        Priority::IteratorPtr queueIterator_)
    : fileKey(key_)
    , segmentRange(offset_, offset_ + size_ - 1)
    , segmentKind(settings.kind)
    , isUnbound_(settings.unbounded)
    , backgroundDownloadEnabled(backgroundDownloadEnabled_)
    , downloadState(downloadState_)
    , keyMetadata(keyMetadata_)
    , queueIterator(queueIterator_)
    , cache(cache_)
{
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (downloadState.load())
    {
        /// EMPTY is used when file segment is not in cache and
        /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
        case (State::EMPTY):
        {
            VELOX_DCHECK(keyMetadata.lock());
            break;
        }
        /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
        case (State::DOWNLOADED):
        {
            reservedSize = downloadedSize = size_;
            VELOX_DCHECK(fs::file_size(getPath()) == size_);
            VELOX_DCHECK(queueIterator);
            VELOX_DCHECK(keyMetadata.lock());
            break;
        }
        case (State::DETACHED):
        {
            break;
        }
        default:
        {
            VELOX_FAIL("Can only create file segment with either EMPTY, DOWNLOADED or DETACHED state");
        }
    }
}

FileSegment::Range::Range(size_t left_, size_t right_) : left(left_), right(right_)
{
    if (left > right)
        VELOX_FAIL("Attempt to create incorrect range: [{}, {}]", left, right);
}

FileSegment::State FileSegment::state() const
{
    auto lk = lock();
    return downloadState;
}

std::string FileSegment::getPath() const
{
    return getKeyMetadata()->getFileSegmentPath(*this);
}

std::string FileSegment::tryGetPath() const
{
    auto metadata = tryGetKeyMetadata();
    if (!metadata)
        return "";
    return metadata->getFileSegmentPath(*this);
}

FileSegmentGuard::Lock FileSegment::lock() const
{
    // TODO(metric): CH ProfileEvents::FileSegmentLockMicroseconds
    return segmentGuard.lock();
}

void FileSegment::setDownloadState(State state, const FileSegmentGuard::Lock & lock)
{
    if (isCompleted(false) && state != State::DETACHED)
    {
        VELOX_FAIL("Updating state to {} of file segment is not allowed, because it is already completed ({})",
            stateToString(state), getInfoForLogUnlocked(lock));
    }

    VLOG(1) << fmt::format("Updated state from {} to {}", stateToString(downloadState.load()), stateToString(state));
    downloadState = state;
}

size_t FileSegment::getReservedSize() const
{
    auto lk = lock();
    return reservedSize;
}

FileSegment::Priority::IteratorPtr FileSegment::getQueueIterator() const
{
    auto lk = lock();
    return queueIterator;
}

void FileSegment::setQueueIterator(Priority::IteratorPtr iterator)
{
    auto lk = lock();
    if (queueIterator)
        VELOX_FAIL("Queue iterator cannot be set twice");
    VELOX_DCHECK(!onDelayedRemoval);
    queueIterator = iterator;
}

void FileSegment::markDelayedRemovalAndResetQueueIterator()
{
    auto lk = lock();
    onDelayedRemoval = true;
    queueIterator = {};
}

void FileSegment::restoreQueueIteratorAfterDelayedRemoval(
    Priority::IteratorPtr iterator)
{
    auto lk = lock();
    VELOX_DCHECK(iterator);
    VELOX_DCHECK(onDelayedRemoval);
    VELOX_DCHECK(!queueIterator);
    queueIterator = std::move(iterator);
    onDelayedRemoval = false;
}

size_t FileSegment::getCurrentWriteOffset() const
{
    return range().left + downloadedSize;
}

size_t FileSegment::getDownloadedSize() const
{
    return downloadedSize;
}

bool FileSegment::isDownloaded() const
{
    auto lk = lock();
    return downloadState == State::DOWNLOADED;
}

time_t FileSegment::getFinishedDownloadTime() const
{
    auto lk = lock();
    return downloadFinishedTime;
}

std::string FileSegment::getCallerId()
{
    return fmt::format(
        "None:{}",
        folly::to<std::string>(reinterpret_cast<uintptr_t>(process::getThreadId())));
}

std::string FileSegment::getDownloader() const
{
    return getDownloaderUnlocked(lock());
}

std::string FileSegment::getDownloaderUnlocked(const FileSegmentGuard::Lock &) const
{
    return downloaderId;
}

std::string FileSegment::getOrSetDownloader()
{
    auto lk = lock();

    assertNotDetachedUnlocked(lk);

    auto current_downloader = getDownloaderUnlocked(lk);

    if (current_downloader.empty())
    {
        const auto caller_id = getCallerId();
        bool allow_new_downloader = downloadState == State::EMPTY || downloadState == State::PARTIALLY_DOWNLOADED;
        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(downloadState.load());

        current_downloader = downloaderId = caller_id;
        setDownloadState(State::DOWNLOADING, lk);
        VELOX_DCHECK(keyMetadata.lock());
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked(const FileSegmentGuard::Lock & lock)
{
    VELOX_DCHECK(isDownloaderUnlocked(lock));
    VELOX_DCHECK(downloadState == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSize();
    /// range().size() can equal 0 in case of write-though cache.
    if (!isUnbound_ && current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(lock);
    else if (current_downloaded_size)
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);
    else
        setDownloadState(State::EMPTY, lock);
}

void FileSegment::resetDownloader()
{
    auto lk = lock();

    auto notifyAllGuard = folly::makeGuard([&] { cv.notify_all(); });

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("resetDownloader", lk);

    resetDownloadingStateUnlocked(lk);
    resetDownloaderUnlocked(lk);
}

void FileSegment::resetDownloaderUnlocked(const FileSegmentGuard::Lock &)
{
    if (downloaderId.empty())
        return;

    VLOG(1) << fmt::format("Resetting downloader from {}", downloaderId);
    downloaderId.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock & lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(lock);

    if (caller != current_downloader)
    {
        VELOX_FAIL("Operation `{}` can be done only by downloader. "
            "(CallerId: {}, downloader id: {})",
            operation, caller, downloaderId);
    }
}

bool FileSegment::isDownloader() const
{
    auto lk = lock();
    return isDownloaderUnlocked(lk);
}

bool FileSegment::isDownloaderUnlocked(const FileSegmentGuard::Lock & lock) const
{
    return getCallerId() == getDownloaderUnlocked(lock);
}

FileSegment::RemoteFileReaderPtr FileSegment::getRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("getRemoteFileReader", lk);
    return remoteFileReader;
}

FileSegment::LocalCacheWriterPtr FileSegment::getLocalCacheWriter()
{
    return cacheWriter;
}

void FileSegment::resetRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("resetRemoteFileReader", lk);
    remoteFileReader.reset();
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    auto lk = lock();
    if (remoteFileReader
        && (downloadState == State::DOWNLOADED
            || downloadState == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION))
    {
        return std::move(remoteFileReader);
    }
    return nullptr;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remoteFileReader_)
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setRemoteFileReader", lk);

    if (remoteFileReader)
        VELOX_FAIL("Remote file reader already exists");

    remoteFileReader = remoteFileReader_;
}

void FileSegment::write(char * from, size_t size, size_t offset_in_file)
{
    // TODO(metric): CH ProfileEvents::FileSegmentWriteMicroseconds
    auto file_segment_path = getPath();
    {
        if (!size)
            VELOX_FAIL("Writing zero size is not allowed");

        {
            auto lk = lock();
            assertIsDownloaderUnlocked("write", lk);
            assertNotDetachedUnlocked(lk);
        }

        if (downloadState != State::DOWNLOADING)
            VELOX_FAIL("Expected DOWNLOADING state, got {}", stateToString(downloadState.load()));

        const size_t first_non_downloaded_offset = getCurrentWriteOffset();

        if (offset_in_file != first_non_downloaded_offset)
        {
            VELOX_FAIL("Attempt to write {} bytes to offset: {}, but current write offset is {}",
                size, offset_in_file, first_non_downloaded_offset);
        }

        const size_t current_downloaded_size = getDownloadedSize();
        VELOX_DCHECK(reservedSize >= current_downloaded_size);

        const size_t free_reserved_size = reservedSize - current_downloaded_size;
        if (free_reserved_size < size)
            VELOX_FAIL("Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (!isUnbound_)
        {
            if (current_downloaded_size == range().size())
                VELOX_FAIL("File segment is already fully downloaded");

            if (current_downloaded_size + size > range().size())
            {
                VELOX_FAIL("Cannot download beyond file segment boundaries: {}. Write offset: {}, size: {}, downloaded size: {}",
                    range().size(), first_non_downloaded_offset, size, current_downloaded_size);
            }
        }
    }

    try
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        /// This mutex is only needed to have a valid assertion in assertCacheCorrectness(),
        /// which is only executed in debug/sanitizer builds (under DEBUG_OR_SANITIZER_BUILD).
        std::lock_guard lock(writeMutex);
#endif

        if (!cacheWriter)
        {
            int flags = -1;
            if (downloadedSize > 0)
                flags = O_WRONLY | O_APPEND | O_CLOEXEC;
            unused(flags);
            cacheWriter = std::make_shared<WriteBufferFromFile>(getPath());
        }

        cacheWriter->append(std::string_view(from, size));

        downloadedSize += size;
        VELOX_DCHECK(std::filesystem::file_size(file_segment_path) == downloadedSize);
    }
    catch (const std::exception & e)
    {
        auto lk = lock();
        setDownloadFailedUnlocked(lk);
        VELOX_FAIL(
            "{}, current cache state: {}",
            e.what(),
            getInfoForLogUnlocked(lk));
    }

    VELOX_DCHECK(getCurrentWriteOffset() == offset_in_file + size);
}

size_t FileSegment::downloadFromReader(
    FileSegment& segment,
    velox::ByteInputStream& reader,
    size_t num_bytes,
    std::vector<char>& scratch,
    size_t lock_wait_timeout_milliseconds)
{
    // Per-write chunk cap. Bounds the scratch buffer and keeps each reserve()
    // small so a tight cache can satisfy it incrementally.
    // TODO(io): this fixed 1 MiB step is the streaming download granularity that
    // CH exposes as max_read_buffer_size (the benchmark's --fcbi_read_buffer_mb
    // knob). Making it configurable means threading a chunk size through this
    // shared static (called by both the foreground glue and the background pool)
    // plus a FileCacheSettings field; deferred to avoid changing production
    // cache behavior, so the knob currently validates == 1 MiB.
    constexpr size_t kDownloadChunk = 1ULL << 20;

    if (num_bytes == 0)
        return 0;

    // Align the reader's read cursor to where the segment expects to be
    // written next. ByteInputStream cannot seek backward, so a reader that is
    // already past the write offset is a programming error rather than a
    // recoverable state.
    size_t offset = segment.getCurrentWriteOffset();
    const auto reader_pos = static_cast<size_t>(reader.tellp());
    if (reader_pos < offset)
        reader.seekp(static_cast<std::streampos>(offset));
    else if (reader_pos > offset)
        VELOX_FAIL(
            "Remote reader position {} is ahead of segment write offset {}",
            reader_pos,
            offset);

    size_t written = 0;
    while (num_bytes > 0 && !reader.atEnd())
    {
        const size_t to_read =
            std::min({num_bytes, reader.remainingSize(), kDownloadChunk});
        if (to_read == 0)
            break;

        std::string failure_reason;
        if (!segment.reserve(to_read, lock_wait_timeout_milliseconds, failure_reason))
        {
            // reserve() has already moved the segment to
            // PARTIALLY_DOWNLOADED_NO_CONTINUATION. Report what was written so
            // far; do not consume from the reader.
            LOG(INFO) << fmt::format(
                "Stopping download of {} at {} bytes: failed to reserve {} bytes: {}",
                segment.getInfoForLog(),
                written,
                to_read,
                failure_reason);
            break;
        }

        if (scratch.size() < to_read)
            scratch.resize(to_read);
        reader.readBytes(
            reinterpret_cast<uint8_t*>(scratch.data()),
            static_cast<int32_t>(to_read));
        segment.write(scratch.data(), to_read, offset);

        offset += to_read;
        num_bytes -= to_read;
        written += to_read;
    }

    return written;
}

FileSegment::State FileSegment::wait(size_t offset)
{
    auto lk = lock();

    if (downloaderId.empty() || offset < getCurrentWriteOffset())
        return downloadState;

    if (downloadState == State::EMPTY)
        VELOX_FAIL("Cannot wait on a file segment with empty state");

    if (downloadState == State::DOWNLOADING)
    {
        VLOG(1) << fmt::format("{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), downloaderId);
        // TODO(metric): CH ProfileEvents::FileSegmentWaitMicroseconds

        VELOX_DCHECK(!getDownloaderUnlocked(lk).empty());
        VELOX_DCHECK(!isDownloaderUnlocked(lk));

        [[maybe_unused]] const auto ok = cv.wait_for(lk, std::chrono::seconds(60), [&, this]()
        {
            return downloadState != State::DOWNLOADING || offset < getCurrentWriteOffset();
        });
        /// VELOX_DCHECK(ok);
    }

    return downloadState;
}

KeyMetadataPtr FileSegment::getKeyMetadata() const
{
    auto metadata = tryGetKeyMetadata();
    if (metadata)
        return metadata;
    VELOX_FAIL("Cannot lock key, key metadata is not set ({})", stateToString(downloadState.load()));
}

KeyMetadataPtr FileSegment::tryGetKeyMetadata() const
{
    auto metadata = keyMetadata.lock();
    if (metadata)
        return metadata;
    return nullptr;
}

LockedKeyPtr FileSegment::lockKeyMetadata(bool assert_exists) const
{
    if (assert_exists)
        return getKeyMetadata()->lock();

    auto metadata = tryGetKeyMetadata();
    if (!metadata)
        return nullptr;
    return metadata->tryLock();
}

bool FileSegment::reserve(
    size_t size_to_reserve,
    size_t lock_wait_timeout_milliseconds,
    std::string & failure_reason,
    FileCacheReserveStat * reserve_stat)
{
    if (!size_to_reserve)
        VELOX_FAIL("Zero space reservation is not allowed");

    size_t current_downloaded_size;

    bool is_file_segment_size_exceeded;
    {
        auto lk = lock();

        assertNotDetachedUnlocked(lk);
        assertIsDownloaderUnlocked("reserve", lk);

        current_downloaded_size = getDownloadedSize();

        is_file_segment_size_exceeded = current_downloaded_size + size_to_reserve > range().size();
        if (is_file_segment_size_exceeded && !isUnbound_)
        {
            VELOX_FAIL("Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                size_to_reserve, range().toString(), downloadedSize.load());
        }

        VELOX_DCHECK(reservedSize >= current_downloaded_size);
    }

    /**
     * It is possible to have downloadedSize < reservedSize when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */

    size_t already_reserved_size = reservedSize - current_downloaded_size;

    if (already_reserved_size >= size_to_reserve)
        return true;

    size_to_reserve = size_to_reserve - already_reserved_size;

    /// This (resizable file segments) is allowed only for single threaded use of file segment.
    /// Currently it is used only for temporary files through cache.
    if (isUnbound_ && is_file_segment_size_exceeded)
        /// Note: segmentRange.right is inclusive.
        segmentRange.right = range().left + current_downloaded_size + size_to_reserve - 1;

    /// if reserve_stat is not passed then use dummy stat and discard the result.
    FileCacheReserveStat dummy_stat;
    if (!reserve_stat)
        reserve_stat = &dummy_stat;

    bool reserved = cache->tryReserve(
        *this, size_to_reserve, *reserve_stat, getKeyMetadata()->origin, lock_wait_timeout_milliseconds, failure_reason);

    if (!reserved)
        setDownloadFailedUnlocked(lock());

    return reserved;
}

void FileSegment::setDownloadedUnlocked(const FileSegmentGuard::Lock &)
{
    if (downloadState == State::DOWNLOADED)
        return;

    downloadState = State::DOWNLOADED;
    downloadFinishedTime = timeInSeconds(std::chrono::system_clock::now());

    if (cacheWriter)
    {
        cacheWriter->finalize();
        cacheWriter.reset();
    }

    remoteFileReader.reset();

    VELOX_DCHECK(downloadedSize > 0);
    VELOX_DCHECK(fs::file_size(getPath()) == downloadedSize);
}

void FileSegment::setDownloadFailed()
{
    auto lk = lock();
    setDownloadFailedUnlocked(lk);
}

void FileSegment::setDownloadFinishedWithoutContinuation()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setDownloadFinishedWithoutContinuation", lk);
    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lk);
    cv.notify_all();
}

void FileSegment::setDownloadFailedUnlocked(const FileSegmentGuard::Lock & lock)
{
    LOG(INFO) << fmt::format("Setting download as failed: {}", getInfoForLogUnlocked(lock));

    auto notifyAllGuard = folly::makeGuard([&] { cv.notify_all(); });

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lock);

    if (cacheWriter)
    {
        cacheWriter->cancel();
        cacheWriter.reset();
    }

    remoteFileReader.reset();
}

void FileSegment::completePartAndResetDownloader()
{
    auto lk = lock();

    auto notifyAllGuard = folly::makeGuard([&] { cv.notify_all(); });

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", lk);

    VELOX_DCHECK(downloadState == State::DOWNLOADING
             || downloadState == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

    if (downloadState == State::DOWNLOADING)
        resetDownloadingStateUnlocked(lk);

    resetDownloaderUnlocked(lk);

    VLOG(1) << fmt::format("Complete batch. ({})", getInfoForLogUnlocked(lk));
}

void FileSegment::shrinkFileSegmentToDownloadedSize(const LockedKey & locked_key, const FileSegmentGuard::Lock & lock, bool force_shrink_to_downloaded_size)
{
    VELOX_DCHECK(downloadedSize);
    VELOX_DCHECK(fs::file_size(getPath()) > 0);

    if (downloadedSize == range().size())
    {
        /// Nothing to resize;
        return;
    }

    if (!locked_key.isLastOwnerOfFileSegment(offset()))
    {
        VELOX_FAIL("Shrinking of file segment can be done only by the last holder: {}",
            getInfoForLog());
    }

    size_t result_size = downloadedSize;
    if (!force_shrink_to_downloaded_size)
    {
        size_t aligned_downloaded_size = FileCacheUtils::roundUpToMultiple(downloadedSize, cache->getBoundaryAlignment());
        result_size = std::min(aligned_downloaded_size, range().size());
    }

    VELOX_DCHECK(result_size <= range().size());
    VELOX_DCHECK(result_size >= downloadedSize);

    if (result_size == range().size())
    {
        /// Nothing to resize;
        return;
    }

    VLOG(1) << fmt::format(
        "Shrinking file segment {} -> {} (downloaded size: {})",
        range().size(), result_size, downloadedSize.load());

    if (downloadedSize == result_size)
        setDownloadState(State::DOWNLOADED, lock);
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);

    segmentRange.right = segmentRange.left + result_size - 1;

    if (reservedSize > result_size)
    {
        queueIterator->decrementSize(reservedSize - result_size);
        reservedSize = result_size;
    }
}

size_t FileSegment::getSizeForBackgroundDownload() const
{
    auto lk = lock();
    return getSizeForBackgroundDownloadUnlocked(lk);
}

size_t FileSegment::getSizeForBackgroundDownloadUnlocked(const FileSegmentGuard::Lock &) const
{
    if (!backgroundDownloadEnabled
        || !downloadedSize
        || !remoteFileReader)
    {
        return 0;
    }

    VELOX_DCHECK(downloadedSize <= range().size());

    const size_t background_download_max_file_segment_size = cache->getBackgroundDownloadMaxFileSegmentSize();
    size_t desired_size;
    if (downloadedSize >= background_download_max_file_segment_size)
        desired_size = FileCacheUtils::roundUpToMultiple(downloadedSize, cache->getBoundaryAlignment());
    else
        desired_size = FileCacheUtils::roundUpToMultiple(background_download_max_file_segment_size, cache->getBoundaryAlignment());

    desired_size = std::min(desired_size, range().size());
    VELOX_DCHECK(desired_size >= downloadedSize);

    return desired_size - downloadedSize;
}

void FileSegment::complete(FileSegmentPtr && file_segment, bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    if (!file_segment)
        VELOX_FAIL("File segment is nullptr");

    // TODO(metric): CH ProfileEvents::FileSegmentCompleteMicroseconds

    if (file_segment->isCompleted())
        return;

    auto locked_key = file_segment->lockKeyMetadata(false);
    if (!locked_key)
    {
        /// If we failed to lock a key, it must be in detached state.
        if (file_segment->isDetached())
            return;

        VELOX_FAIL("Cannot complete file segment: {}", file_segment->getInfoForLog());
    }

    auto resetFileSegmentGuard = folly::makeGuard([&] { file_segment.reset(); });

    file_segment->complete(locked_key, allow_background_download, force_shrink_to_downloaded_size);
}

void FileSegment::complete(const LockedKeyPtr & locked_key, bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    auto segment_lock = lock();

    if (isCompleted(false))
        return;

    const bool is_downloader = isDownloaderUnlocked(segment_lock);
    const bool is_last_holder = locked_key->isLastOwnerOfFileSegment(offset());
    const size_t current_downloaded_size = getDownloadedSize();

    auto notifyDownloaderGuard = folly::makeGuard([&] {
        if (is_downloader)
            cv.notify_all();
    });

    VLOG(1) << fmt::format(
        "Complete based on current state (is_last_holder: {}, force shrink: {}, {})",
        is_last_holder, force_shrink_to_downloaded_size, getInfoForLogUnlocked(segment_lock));

    if (is_downloader)
    {
        if (downloadState == State::DOWNLOADING)
            resetDownloadingStateUnlocked(segment_lock);
        resetDownloaderUnlocked(segment_lock);
    }

    if (segmentKind == FileSegmentKind::Ephemeral && is_last_holder)
    {
        VLOG(1) << fmt::format("Removing temporary file segment: {}", getInfoForLogUnlocked(segment_lock));
        locked_key->removeFileSegment(offset(), segment_lock);
        return;
    }

    switch (downloadState.load())
    {
        case State::DOWNLOADED:
        {
            VELOX_DCHECK(current_downloaded_size == range().size());
            VELOX_DCHECK(current_downloaded_size == fs::file_size(getPath()));
            VELOX_DCHECK(!cacheWriter);
            VELOX_DCHECK(!remoteFileReader);
            break;
        }
        case State::DOWNLOADING:
        {
            VELOX_DCHECK(!is_last_holder);
            break;
        }
        case State::EMPTY:
        {
            if (is_last_holder)
                locked_key->removeFileSegment(offset(), segment_lock);
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        {
            VELOX_DCHECK(current_downloaded_size > 0);
            VELOX_DCHECK(fs::exists(getPath()));
            VELOX_DCHECK(fs::file_size(getPath()) > 0);

            if (is_last_holder)
            {
                bool added_to_download_queue = false;
                size_t background_download_size = allow_background_download ? getSizeForBackgroundDownloadUnlocked(segment_lock) : 0;
                if (background_download_size)
                {
                    // TODO(metric): CH ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundDownloadQueuePush);
                    added_to_download_queue = locked_key->addToDownloadQueue(offset(), segment_lock); /// Finish download in background.
                }

                if (!added_to_download_queue)
                {
                    /// Reset the writer to reduce memory usage,
                    /// because we do not know when download will be continued next time.
                    if (cacheWriter)
                    {
                        cacheWriter->finalize();
                        cacheWriter.reset();
                    }

                    /// Reset the reader so request is not kept alive and with that
                    /// preventing other operations on the same objects
                    remoteFileReader.reset();

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            VELOX_DCHECK(current_downloaded_size != range().size());

            if (is_last_holder)
            {
                if (current_downloaded_size == 0)
                {
                    locked_key->removeFileSegment(offset(), segment_lock);
                }
                else
                {
                    VLOG(1) << fmt::format("Resize file segment {} to downloaded: {}", range().toString(), current_downloaded_size);

                    /// Reset the writer to reduce memory usage,
                    /// because we do not know when download will be continued next time.
                    if (cacheWriter)
                    {
                        cacheWriter->finalize();
                        cacheWriter.reset();
                    }

                    remoteFileReader.reset();

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        default:
            VELOX_FAIL("Unexpected state while completing file segment");
    }

    VLOG(1) << fmt::format("Completed file segment: {}", getInfoForLogUnlocked(segment_lock));

    if (downloadState != State::DETACHED)
        VELOX_DCHECK(assertCorrectnessUnlocked(segment_lock));
}

std::string FileSegment::getInfoForLog() const
{
    auto lk = lock();
    return getInfoForLogUnlocked(lk);
}

std::string FileSegment::getInfoForLogUnlocked(const FileSegmentGuard::Lock &) const
{
    std::ostringstream info;
    info << "File segment: " << range().toString() << ", ";
    info << "key: " << key().toString() << ", ";
    info << "state: " << stateToString(downloadState.load()) << ", ";
    info << "downloaded size: " << getDownloadedSize() << ", ";
    info << "reserved size: " << reservedSize.load() << ", ";
    info << "downloader id: " << (downloaderId.empty() ? "None" : downloaderId) << ", ";
    info << "current write offset: " << getCurrentWriteOffset() << ", ";
    info << "caller id: " << getCallerId() << ", ";
    info << "kind: " << toString(segmentKind) << ", ";
    info << "unbound: " << isUnbound_ << ", ";
    info << "background download: " << backgroundDownloadEnabled;

    return info.str();
}

std::string FileSegment::stateToString(FileSegment::State state)
{
    switch (state)
    {
        case FileSegment::State::DOWNLOADED:
            return "DOWNLOADED";
        case FileSegment::State::EMPTY:
            return "EMPTY";
        case FileSegment::State::DOWNLOADING:
            return "DOWNLOADING";
        case FileSegment::State::PARTIALLY_DOWNLOADED:
            return "PARTIALLY DOWNLOADED";
        case FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
            return "PARTIALLY DOWNLOADED NO CONTINUATION";
        case FileSegment::State::DETACHED:
            return "DETACHED";
    }
    VELOX_FAIL("Unexpected file segment state: {}", static_cast<uint8_t>(state));
}

bool FileSegment::assertCorrectness() const
{
    return assertCorrectnessUnlocked(lock());
}

bool FileSegment::assertCorrectnessUnlocked(const FileSegmentGuard::Lock & lock) const
{
    auto throw_logical = [&](const std::string & error)
    {
        VELOX_FAIL("{}. File segment info: {}", error, getInfoForLogUnlocked(lock));
    };

    auto check_iterator = [&](const Priority::IteratorPtr & it)
    {
        unused(this);
        if (!it)
            return;

        auto entry = it->getEntry();
        auto entry_size = entry->size.load(std::memory_order_relaxed);
        if (entry_size == 0)
        {
            /// A race in case of SLRU eviction is possible here
            /// when we do setIterator during downgrade.
            /// Then as entry is invalidated right after we set a new iterator
            /// - just fetch entry once more.
            entry = it->getEntry();
            entry_size = entry->size;
        }
        if (downloadState != State::DOWNLOADING && entry_size != reservedSize)
            throw_logical(
                fmt::format("Expected entry.size == reservedSize ({} == {}, entry: {})",
                            entry_size, reservedSize.load(), entry->toString()));

        VELOX_DCHECK(entry->key == key());
        VELOX_DCHECK(entry->offset == offset());
    };

    const auto file_path = getPath();

    {
        std::lock_guard lk(writeMutex);
        if (downloadedSize == 0)
        {
            if (downloadState != State::DOWNLOADING && fs::exists(file_path))
                throw_logical("Expected file " + file_path + " not to exist");
        }
        else if (!fs::exists(file_path))
        {
            throw_logical("Expected file " + file_path + " to exist");
        }
    }

    /// A restored queue iterator must clear the delayed-removal state.
    if (queueIterator)
        VELOX_DCHECK(!onDelayedRemoval);

    switch (downloadState.load())
    {
        case State::EMPTY:
        {
            VELOX_DCHECK(downloaderId.empty());
            VELOX_DCHECK(!fs::exists(getPath()));
            VELOX_DCHECK(!queueIterator);
            break;
        }
        case State::DOWNLOADED:
        {
            VELOX_DCHECK(downloaderId.empty());

            VELOX_DCHECK(downloadedSize == reservedSize);
            VELOX_DCHECK(downloadedSize == range().size());
            VELOX_DCHECK(downloadedSize > 0);

            VELOX_DCHECK(!remoteFileReader);
            VELOX_DCHECK(!cacheWriter);

            auto file_size = fs::file_size(getPath());
            unused(file_size);

            VELOX_DCHECK(file_size == range().size());
            VELOX_DCHECK(downloadedSize == range().size());

            VELOX_DCHECK(queueIterator || onDelayedRemoval);
            check_iterator(queueIterator);
            break;
        }
        case State::DOWNLOADING:
        {
            VELOX_DCHECK(!downloaderId.empty());
            if (downloadedSize)
            {
                VELOX_DCHECK(queueIterator);
                VELOX_DCHECK(fs::file_size(getPath()) > 0);
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        {
            VELOX_DCHECK(downloaderId.empty());

            VELOX_DCHECK(reservedSize >= downloadedSize);
            VELOX_DCHECK(downloadedSize > 0);

            auto file_size = fs::file_size(getPath());
            unused(file_size);

            VELOX_DCHECK(file_size > 0);
            VELOX_DCHECK(file_size <= range().size());
            VELOX_DCHECK(downloadedSize <= range().size());

            VELOX_DCHECK(queueIterator || onDelayedRemoval);
            check_iterator(queueIterator);
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            VELOX_DCHECK(reservedSize >= downloadedSize);
            check_iterator(queueIterator);
            break;
        }
        case State::DETACHED:
        {
            break;
        }
    }

    return true;
}

void FileSegment::assertNotDetached() const
{
    auto lk = lock();
    assertNotDetachedUnlocked(lk);
}

void FileSegment::assertNotDetachedUnlocked(const FileSegmentGuard::Lock & lock) const
{
    if (downloadState == State::DETACHED)
    {
        VELOX_FAIL("Cache file segment is in detached state, operation not allowed. "
            "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
            "Please, retry. File segment info: {}", getInfoForLogUnlocked(lock));
    }
}

FileSegment::Info FileSegment::getInfo(const FileSegmentPtr & file_segment)
{
    auto lock = file_segment->lock();
    auto keyMetadata = file_segment->tryGetKeyMetadata();
    return Info{
        .key = file_segment->key(),
        .offset = file_segment->offset(),
        .path = file_segment->tryGetPath(),
        .rangeLeft = file_segment->range().left,
        .rangeRight = file_segment->range().right,
        .kind = file_segment->segmentKind,
        .state = file_segment->downloadState,
        .size = file_segment->range().size(),
        .downloadedSize = file_segment->downloadedSize,
        .downloadFinishedTime = file_segment->downloadFinishedTime,
        .cacheHits = file_segment->hitsCount,
        .references = static_cast<uint64_t>(file_segment.use_count()),
        .isUnbound = file_segment->isUnbound_,
        .queueEntryType = file_segment->queueIterator ? file_segment->queueIterator->getType() : QueueEntryType::None,
        .origin = keyMetadata->origin,
    };
}

bool FileSegment::isDetached() const
{
    auto lk = lock();
    return downloadState == State::DETACHED;
}

bool FileSegment::isCompleted(bool sync) const
{
    auto is_completed_state = [this]() -> bool
    {
        return downloadState == State::DOWNLOADED || downloadState == State::DETACHED;
    };

    if (sync)
    {
        if (is_completed_state())
            return true;

        auto lk = lock();
        return is_completed_state();
    }

    return is_completed_state();
}

void FileSegment::setDetachedState(const FileSegmentGuard::Lock & lock)
{
    setDownloadState(State::DETACHED, lock);
    keyMetadata.reset();
    queueIterator = nullptr;
    if (cacheWriter)
        cacheWriter->cancel();
    cacheWriter.reset();
    remoteFileReader.reset();
}

void FileSegment::detach(const FileSegmentGuard::Lock & lock, const LockedKey &)
{
    if (downloadState == State::DETACHED)
        return;

    if (!downloaderId.empty())
        resetDownloaderUnlocked(lock);
    setDetachedState(lock);
}

void FileSegment::increasePriority()
{
    if (!cache)
    {
        VELOX_DCHECK(isDetached());
        return;
    }

    // TODO(metric): CH ProfileEvents::FileSegmentIncreasePriorityMicroseconds

    /// In case of concurrently called increasePriority()
    /// we want to increase a priority only once
    /// (because it does not really make any sense
    /// to do it immediately again after we've just done it)
    std::unique_lock<std::mutex> lock(increasePriorityMutex, std::defer_lock);
    if (lock.try_lock())
    {
        auto it = getQueueIterator();
        if (it)
        {
            if (!cache->tryIncreasePriority(*this))
            {
                // TODO(metric): CH ProfileEvents::increment(ProfileEvents::FileSegmentFailToIncreasePriority);
            }

            /// Used only for system.filesystem_cache.
            ++hitsCount;
        }
    }
}

FileSegment::~FileSegment()
{
    try
    {
        /// Can be non-finalized in case it was push to background download
        /// but not executed before server shutdown.
        if (cacheWriter)
            cacheWriter->finalize();
    }
    catch (...)
    {
        LOG(ERROR) << "Exception in FileSegment destructor";
    }
}

FileSegmentsHolder::FileSegmentsHolder(FileSegments && file_segments_)
    : fileSegments(std::move(file_segments_))
{
    // TODO(metric): CH CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments, fileSegments.size());
    // TODO(metric): CH ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments, fileSegments.size());
}

FileSegmentPtr FileSegmentsHolder::getSingleFileSegment() const
{
    if (fileSegments.size() != 1)
    {
        VELOX_FAIL("Expected single file segment, got: {} in holder {}",
            fileSegments.size(), toString());
    }
    return fileSegments.front();
}

void FileSegmentsHolder::reset()
{
    // TODO(metric): CH ProfileEvents::FileSegmentHolderCompleteMicroseconds

    // TODO(metric): CH ProfileEvents::increment(ProfileEvents::FilesystemCacheUnusedHoldFileSegments, fileSegments.size());
    for (auto file_segment_it = fileSegments.begin(); file_segment_it != fileSegments.end();)
    {
        try
        {
            /// One might think it would have been more correct to do `false` here,
            /// not to allow background download for file segments that we actually did not start reading.
            /// But actually we would only do that, if those file segments were already read partially by some other thread/query
            /// but they were not put to the download queue, because current thread was holding them in Holder.
            /// So as a culprit, we need to allow to happen what would have happened if we did not exist.
            file_segment_it = completeAndPopFrontImpl(/*allow_background_download=*/true, /*force_shrink_to_downloaded_size=*/false);
        }
        catch (...)
        {
            LOG(ERROR) << fmt::format("Exception in {}", __PRETTY_FUNCTION__);
            VELOX_DCHECK(false);
            continue;
        }
    }
    fileSegments.clear();
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    reset();
}

FileSegments::iterator FileSegmentsHolder::completeAndPopFrontImpl(bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    auto file_segment_it = fileSegments.begin();
    FileSegment::complete(std::move(*file_segment_it), allow_background_download, force_shrink_to_downloaded_size);
    // TODO(metric): CH CurrentMetrics::sub(CurrentMetrics::FilesystemCacheHoldFileSegments);
    return fileSegments.erase(file_segment_it);
}

FileSegment & FileSegmentsHolder::add(FileSegmentPtr && file_segment)
{
    fileSegments.push_back(file_segment);
    // TODO(metric): CH CurrentMetrics::add(CurrentMetrics::FilesystemCacheHoldFileSegments);
    // TODO(metric): CH ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments);
    return *fileSegments.back();
}

std::string FileSegmentsHolder::toString(bool with_state) const
{
    return facebook::velox::ch::toString(fileSegments, with_state);
}

std::string toString(const FileSegments & file_segments, bool with_state)
{
    std::string ranges;
    for (const auto & file_segment : file_segments)
    {
        if (!ranges.empty())
            ranges += ", ";
        ranges += file_segment->range().toString();
        if (file_segment->isUnbound())
            ranges += "(unbound)";
        if (with_state)
            ranges += "(" + FileSegment::stateToString(file_segment->state()) + ")";
    }
    return ranges;
}

} // namespace facebook::velox::ch
