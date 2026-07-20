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
#include "velox/ch/Interpreters/FileCache/FileSegment.h"

#include "velox/ch/Common/FileCacheQueryIdScope.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheErrnoException.h"
#include "velox/ch/Interpreters/FileCache/FileCacheUtils.h"
#include "velox/common/file/LocalFile.h"

#include <folly/ScopeGuard.h>

#include <chrono>
#include <filesystem>

namespace fs = std::filesystem;

namespace facebook::velox::ch
{

namespace
{
/// CH `timeInSeconds(system_clock::now())`.
time_t nowInSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}

String toString(FileSegmentKind kind)
{
    switch (kind)
    {
        case FileSegmentKind::Regular:
            return "Regular";
        case FileSegmentKind::Ephemeral:
            return "Ephemeral";
    }
    return "Unknown";
}

FileSegment::FileSegment(
        const Key & key_,
        size_t offset_,
        size_t size_,
        State download_state_,
        const CreateFileSegmentSettings & settings,
        bool background_download_enabled_,
        FileCache * cache_,
        std::weak_ptr<KeyMetadata> key_metadata_,
        Priority::IteratorPtr queue_iterator_,
        bool size_in_filename_)
    : file_key(key_)
    , segment_range(offset_, offset_ + size_ - 1)
    , segment_kind(settings.kind)
    , is_unbound(settings.unbounded)
    , background_download_enabled(background_download_enabled_)
    , size_in_filename(size_in_filename_)
    , download_state(download_state_)
    , key_metadata(key_metadata_)
    , queue_iterator(queue_iterator_)
    , cache(cache_)
#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)
    , log(getLogger(fmt::format("FileSegment({}) : {}", key_.toString(), range().toString())))
#endif
{
    /// The size is encoded into the file name only for fully downloaded regular segments
    /// (see `renameToIncludeSizeInNameUnlocked`), so on creation it can be set only together
    /// with the `DOWNLOADED` state (used when loading cache metadata on startup).
    chassert(!size_in_filename || download_state == State::DOWNLOADED);

    switch (download_state)
    {
        case (State::EMPTY):
        {
            chassert(key_metadata.lock());
            break;
        }
        case (State::DOWNLOADED):
        {
            reserved_size = downloaded_size = size_;
            chassert(size_in_filename || fs::file_size(getPath()) == size_);
            chassert(queue_iterator);
            chassert(key_metadata.lock());
            break;
        }
        case (State::DETACHED):
        {
            break;
        }
        default:
        {
            throwFileCacheException(
                "Can only create file segment with either EMPTY, DOWNLOADED or DETACHED state");
        }
    }
}

FileSegment::Range::Range(size_t left_, size_t right_) : left(left_), right(right_)
{
    if (left > right)
        throwFileCacheException("Attempt to create incorrect range: [{}, {}]", left, right);
}

const LoggerPtr & FileSegment::getLog() const
{
#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)
    return log;
#else
    static const LoggerPtr log = getLogger("FileSegment");
    return log;
#endif
}

FileSegment::State FileSegment::state() const
{
    return download_state.load();
}

String FileSegment::getPath() const
{
    return getKeyMetadata()->getFileSegmentPath(*this);
}

String FileSegment::tryGetPath() const
{
    auto metadata = tryGetKeyMetadata();
    if (!metadata)
        return "";
    return metadata->getFileSegmentPath(*this);
}

FileSegmentGuard::Lock FileSegment::lock() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentLockMicroseconds);
    return segment_guard.lock();
}

void FileSegment::setDownloadState(State state, const FileSegmentGuard::Lock & lock)
{
    if (isCompleted(false) && state != State::DETACHED)
    {
        throwFileCacheException(
            "Updating state to {} of file segment is not allowed, because it is already completed ({})",
            stateToString(state), getInfoForLogUnlocked(lock));
    }

    LOG_TEST(getLog(), "Updated state from {} to {}", stateToString(download_state), stateToString(state));
    download_state = state;
}

size_t FileSegment::getReservedSize() const
{
    return reserved_size.load();
}

FileSegment::Priority::IteratorPtr FileSegment::getQueueIterator() const
{
    auto lk = lock();
    return queue_iterator;
}

void FileSegment::setQueueIterator(Priority::IteratorPtr iterator)
{
    auto lk = lock();
    if (queue_iterator)
        throwFileCacheException("Queue iterator cannot be set twice");
    chassert(!on_delayed_removal);
    queue_iterator = iterator;
}

void FileSegment::markDelayedRemovalAndResetQueueIterator()
{
    auto lk = lock();
    on_delayed_removal = true;
    queue_iterator = {};
}

void FileSegment::restoreQueueIteratorAfterDelayedRemoval(Priority::IteratorPtr iterator)
{
    auto lk = lock();
    chassert(iterator);
    chassert(on_delayed_removal);
    chassert(!queue_iterator);
    queue_iterator = std::move(iterator);
    on_delayed_removal = false;
}

size_t FileSegment::getCurrentWriteOffset() const
{
    return range().left + downloaded_size;
}

size_t FileSegment::getDownloadedSize() const
{
    return downloaded_size;
}

bool FileSegment::isDownloaded() const
{
    return download_state.load() == State::DOWNLOADED;
}

time_t FileSegment::getFinishedDownloadTime() const
{
    auto lk = lock();
    return download_finished_time;
}

String FileSegment::getCallerId()
{
    return FileCacheQueryIdScope::getCallerId();
}

namespace
{
/// Default local cache writer file: the exact `LocalWriteFile` construction production uses
/// (seek-to-end append, create-if-absent, buffered). A test may replace this via
/// `setWriteFileFactoryForTesting` to inject a fault-injecting `velox::WriteFile`.
FileSegment::WriteFileFactory & writeFileFactoryStorage()
{
    static FileSegment::WriteFileFactory factory = [](const std::string & path) -> std::unique_ptr<velox::WriteFile>
    {
        return std::make_unique<velox::LocalWriteFile>(
            path,
            /* shouldCreateParentDirectories */ false,
            /* shouldThrowOnFileAlreadyExists */ false,
            /* bufferIo */ true);
    };
    return factory;
}
}

void FileSegment::setWriteFileFactoryForTesting(WriteFileFactory factory)
{
    writeFileFactoryStorage() = std::move(factory);
}

std::unique_ptr<velox::WriteFile> FileSegment::createWriteFile(const std::string & path)
{
    return writeFileFactoryStorage()(path);
}

String FileSegment::getDownloader() const
{
    return getDownloaderUnlocked(lock());
}

String FileSegment::getDownloaderUnlocked(const FileSegmentGuard::Lock &) const
{
    return download_data ? download_data->downloader_id : "";
}

FileSegment::DownloadState & FileSegment::getOrCreateDownloadDataUnlocked(const FileSegmentGuard::Lock &)
{
    if (!download_data)
        download_data = std::make_unique<DownloadState>();
    return *download_data;
}

void FileSegment::resetDownloadDataUnlocked(const FileSegmentGuard::Lock &)
{
    download_data.reset();
}

String FileSegment::getOrSetDownloader()
{
    auto lk = lock();

    assertNotDetachedUnlocked(lk);

    auto current_downloader = getDownloaderUnlocked(lk);

    if (current_downloader.empty())
    {
        const auto caller_id = getCallerId();
        bool allow_new_downloader = download_state == State::EMPTY || download_state == State::PARTIALLY_DOWNLOADED;
        if (!allow_new_downloader)
            return "notAllowed:" + stateToString(download_state);

        current_downloader = getOrCreateDownloadDataUnlocked(lk).downloader_id = caller_id;
        setDownloadState(State::DOWNLOADING, lk);
        chassert(key_metadata.lock());
    }

    return current_downloader;
}

void FileSegment::resetDownloadingStateUnlocked(const FileSegmentGuard::Lock & lock)
{
    chassert(isDownloaderUnlocked(lock));
    chassert(download_state == State::DOWNLOADING);

    size_t current_downloaded_size = getDownloadedSize();
    /// range().size() can equal 0 in case of write-though cache.
    if (!is_unbound && current_downloaded_size != 0 && current_downloaded_size == range().size())
        setDownloadedUnlocked(lock);
    else if (current_downloaded_size)
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);
    else
        setDownloadState(State::EMPTY, lock);
}

void FileSegment::resetDownloader()
{
    auto lk = lock();

    SCOPE_EXIT { cv.notify_all(); };

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("resetDownloader", lk);

    resetDownloadingStateUnlocked(lk);
    resetDownloaderUnlocked(lk);
}

void FileSegment::resetDownloaderUnlocked(const FileSegmentGuard::Lock &)
{
    if (!download_data || download_data->downloader_id.empty())
        return;

    LOG_TEST(getLog(), "Resetting downloader from {}", download_data->downloader_id);
    download_data->downloader_id.clear();
}

void FileSegment::assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock & lock) const
{
    auto caller = getCallerId();
    auto current_downloader = getDownloaderUnlocked(lock);

    if (caller != current_downloader)
    {
        throwFileCacheException(
            "Operation `{}` can be done only by downloader. (CallerId: {}, downloader id: {})",
            operation, caller, current_downloader);
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
    return download_data ? download_data->remote_file_reader : nullptr;
}

FileSegment::LocalCacheWriterPtr FileSegment::getLocalCacheWriter()
{
    auto lk = lock();
    return download_data ? download_data->cache_writer : nullptr;
}

void FileSegment::resetRemoteFileReader()
{
    auto lk = lock();
    assertIsDownloaderUnlocked("resetRemoteFileReader", lk);
    if (download_data)
        download_data->remote_file_reader.reset();
}

FileSegment::RemoteFileReaderPtr FileSegment::extractRemoteFileReader()
{
    auto lk = lock();
    if (download_data && download_data->remote_file_reader
        && (download_state == State::DOWNLOADED
            || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION))
    {
        return std::move(download_data->remote_file_reader);
    }
    return nullptr;
}

void FileSegment::setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_)
{
    auto lk = lock();
    assertIsDownloaderUnlocked("setRemoteFileReader", lk);

    auto & download = getOrCreateDownloadDataUnlocked(lk);
    if (download.remote_file_reader)
        throwFileCacheException("Remote file reader already exists");

    download.remote_file_reader = remote_file_reader_;
}

void FileSegment::write(char * from, size_t size, size_t offset_in_file)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWriteMicroseconds);
    auto file_segment_path = getPath();
    DownloadState * download = nullptr;
    {
        if (!size)
            throwFileCacheException("Writing zero size is not allowed");

        {
            auto lk = lock();
            assertIsDownloaderUnlocked("write", lk);
            assertNotDetachedUnlocked(lk);
            download = &getOrCreateDownloadDataUnlocked(lk);
        }

        if (download_state != State::DOWNLOADING)
            throwFileCacheException("Expected DOWNLOADING state, got {}", stateToString(download_state));

        const size_t first_non_downloaded_offset = getCurrentWriteOffset();

        if (offset_in_file != first_non_downloaded_offset)
        {
            throwFileCacheException(
                "Attempt to write {} bytes to offset: {}, but current write offset is {} ({})",
                size, offset_in_file, first_non_downloaded_offset, getInfoForLog());
        }

        const size_t current_downloaded_size = getDownloadedSize();
        chassert(reserved_size >= current_downloaded_size);

        const size_t free_reserved_size = reserved_size - current_downloaded_size;
        if (free_reserved_size < size)
            throwFileCacheException("Not enough space is reserved. Available: {}, expected: {}", free_reserved_size, size);

        if (!is_unbound)
        {
            if (current_downloaded_size == range().size())
                throwFileCacheException("File segment is already fully downloaded");

            if (current_downloaded_size + size > range().size())
            {
                throwFileCacheException(
                    "Cannot download beyond file segment boundaries: {}. Write offset: {}, size: {}, downloaded size: {}",
                    range().size(), first_non_downloaded_offset, size, current_downloaded_size);
            }
        }
    }

    try
    {
#if !defined(NDEBUG) || defined(FOLLY_SANITIZE)
        std::lock_guard write_lock(download->write_mutex);
#endif

        if (!download->cache_writer)
        {
            /// CH creates a `WriteBufferFromFile` with append flags once the segment already has
            /// bytes on disk; here `LocalWriteFile` seeks to end and, with
            /// `shouldThrowOnFileAlreadyExists=false`, appends to an existing file or creates it.
            /// The underlying `velox::WriteFile` is built through `createWriteFile` so a test can
            /// inject a fault-injecting file (partial-physical-append-failure contract); the
            /// default factory reproduces this exact construction.
            download->cache_writer = std::make_shared<WriteBufferFromVeloxWriteFile>(createWriteFile(file_segment_path));
        }

        /// Size is equal to offset as offset for write buffer points to data end.
        download->cache_writer->set(from, /* size */ size, /* offset */ size);
        /// Reset the buffer when finished.
        SCOPE_EXIT { download->cache_writer->set(nullptr, 0); };
        /// Flush the buffer.
        download->cache_writer->next();

        downloaded_size += size;
        chassert(std::filesystem::file_size(file_segment_path) == downloaded_size);
    }
    catch (const FileCacheErrnoException & e)
    {
        const int code = e.getErrno();
        const bool is_no_space_left_error = code == /* ENOSPC */ 28 || code == /* EDQUOT */ 122;

        auto lk = lock();
        setDownloadFailedUnlocked(lk);

        if (fs::exists(file_segment_path))
        {
            if (downloaded_size == 0)
            {
                fs::remove(file_segment_path);
            }
            else if (is_no_space_left_error)
            {
                const auto physical_size = fs::file_size(file_segment_path);

                LOG_TRACE(getLog(), "Failed to write to file: no space left on device "
                          "(file size: {}, downloaded size: {}, reserved size: {})",
                          physical_size, downloaded_size.load(), reserved_size.load());

                chassert(downloaded_size <= physical_size && physical_size <= reserved_size);
                if (downloaded_size != physical_size)
                    downloaded_size = physical_size;
            }
        }

        throw;
    }
    catch (...)
    {
        auto lk = lock();
        setDownloadFailedUnlocked(lk);
        throw;
    }

    chassert(getCurrentWriteOffset() == offset_in_file + size);
}

FileSegment::State FileSegment::wait(size_t offset)
{
    auto lk = lock();

    if (getDownloaderUnlocked(lk).empty() || offset < getCurrentWriteOffset())
        return download_state;

    if (download_state == State::EMPTY)
        throwFileCacheException("Cannot wait on a file segment with empty state");

    if (download_state == State::DOWNLOADING)
    {
        LOG_TEST(getLog(), "{} waiting on: {}, current downloader: {}", getCallerId(), range().toString(), getDownloaderUnlocked(lk));
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentWaitMicroseconds);

        chassert(!getDownloaderUnlocked(lk).empty());
        chassert(!isDownloaderUnlocked(lk));

        /// Wait for the download in short slices. The condition variable is only notified on
        /// download progress, so a stalled or dead downloader would otherwise pin this thread until
        /// the full timeout. Query cancellation is not wired into this MVP (the accepted S1 header
        /// takes no cancellation token); the bounded 60s deadline still prevents an indefinite hang.
        auto downloaded = [&, this]()
        {
            return download_state != State::DOWNLOADING || offset < getCurrentWriteOffset();
        };
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (true)
        {
            if (cv.wait_for(lk, std::chrono::seconds(1), downloaded))
                break;
            if (std::chrono::steady_clock::now() >= deadline)
                break;
        }
    }

    return download_state;
}

KeyMetadataPtr FileSegment::getKeyMetadata() const
{
    auto metadata = tryGetKeyMetadata();
    if (metadata)
        return metadata;
    throwFileCacheException("Cannot lock key, key metadata is not set ({})", stateToString(download_state));
}

KeyMetadataPtr FileSegment::tryGetKeyMetadata() const
{
    auto metadata = key_metadata.lock();
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
    FileCacheReserveStat * reserve_stat,
    size_t reserve_hint)
{
    if (!size_to_reserve)
        throwFileCacheException("Zero space reservation is not allowed");

    size_t current_downloaded_size = 0;

    bool is_file_segment_size_exceeded = false;
    {
        auto lk = lock();

        assertNotDetachedUnlocked(lk);
        assertIsDownloaderUnlocked("reserve", lk);

        current_downloaded_size = getDownloadedSize();

        is_file_segment_size_exceeded = current_downloaded_size + size_to_reserve > range().size();
        if (is_file_segment_size_exceeded && !is_unbound)
        {
            throwFileCacheException(
                "Attempt to reserve space too much space ({}) for file segment with range: {} (downloaded size: {})",
                size_to_reserve, range().toString(), downloaded_size.load());
        }

        chassert(reserved_size >= current_downloaded_size);
    }

    chassert(range().size() >= reserved_size);

    if (reserved_size > current_downloaded_size)
    {
        const size_t available_reserved = reserved_size - current_downloaded_size;
        if (available_reserved >= size_to_reserve)
            return true;
        size_to_reserve -= available_reserved;
    }

    const size_t minimum_reserve_size = size_to_reserve;

    if (!is_unbound)
    {
        const auto reserve_granularity = cache->getReserveGranularity();
        if (reserve_granularity && reserve_granularity > size_to_reserve)
        {
            size_to_reserve = reserved_size + reserve_granularity > range().size()
                ? range().size() - reserved_size
                : reserve_granularity;

            const size_t read_horizon = current_downloaded_size + reserve_hint;
            if (reserve_hint
                && read_horizon > reserved_size
                && read_horizon < reserved_size + size_to_reserve)
                size_to_reserve = read_horizon - reserved_size;
        }
    }

    size_to_reserve = std::max(size_to_reserve, minimum_reserve_size);

    /// This (resizable file segments) is allowed only for single threaded use of file segment.
    /// Currently it is used only for temporary files through cache.
    if (is_unbound && is_file_segment_size_exceeded)
        /// Note: segment_range.right is inclusive.
        segment_range.right = range().left + current_downloaded_size + size_to_reserve - 1;

    /// if reserve_stat is not passed then use dummy stat and discard the result.
    FileCacheReserveStat dummy_stat;
    if (!reserve_stat)
        reserve_stat = &dummy_stat;

    bool reserved = cache->tryReserve(
        *this, size_to_reserve, *reserve_stat, *getKeyMetadata()->origin, lock_wait_timeout_milliseconds, failure_reason);

    if (!reserved)
        setDownloadFailedUnlocked(lock());

    return reserved;
}

void FileSegment::setDownloadedUnlocked(const FileSegmentGuard::Lock & lock)
{
    if (download_state == State::DOWNLOADED)
        return;

    if (download_data && download_data->cache_writer)
    {
        try
        {
            download_data->cache_writer->finalize();
        }
        catch (...)
        {
            tryLogCurrentException(getLog(), "Failed to finalize cache writer while marking file segment as downloaded");
            setDownloadFailedUnlocked(lock);
            return;
        }
    }

    resetDownloadDataUnlocked(lock);

    renameToIncludeSizeInNameUnlocked(lock);

    download_state = State::DOWNLOADED;
    download_finished_time = nowInSeconds();

    chassert(downloaded_size > 0);
    chassert(fs::file_size(getPath()) == downloaded_size);
}

void FileSegment::renameToIncludeSizeInNameUnlocked(const FileSegmentGuard::Lock &)
{
    if (segment_kind != FileSegmentKind::Regular || size_in_filename)
        return;

    chassert(!download_data);

    auto key_metadata_ptr = getKeyMetadata();
    const auto old_path = key_metadata_ptr->getFileSegmentPath(offset(), segment_kind, /* size */ std::nullopt);
    const auto new_path = key_metadata_ptr->getFileSegmentPath(offset(), segment_kind, range().size());

    if (old_path == new_path)
    {
        size_in_filename = true;
        return;
    }

    /// Encoding the size in the name is only a startup optimization, so the rename is best-effort.
    /// The segment is already fully downloaded and valid under its legacy `<offset>` name; if the
    /// rename fails we keep that name (`size_in_filename` stays false and the loader falls back to a
    /// `stat`) and do not propagate the error.
    bool renamed = false;
    try
    {
        fs::rename(old_path, new_path);
        size_in_filename = true;
        renamed = true;
    }
    catch (...)
    {
        tryLogCurrentException(
            getLog(),
            fmt::format("Failed to rename cache file '{}' to encode its size in the name; keeping the legacy name", old_path));
    }

    /// TODO(Task 013): invalidate opened file handles via the manager-owned OpenedFileCache.
    /// A reader that opened this segment while it was still named `old_path` left an entry in the
    /// opened-file cache keyed by `old_path`; a future segment created at the same key/offset is
    /// again named `old_path`, so opening it could reuse the stale descriptor. CH drops the
    /// `old_path` entry here via `OpenedFileCache::instance().remove`. This is a no-op in the SCC
    /// phase: no `OpenedFileCache` exists yet (it is manager-owned, introduced in Task 013), so
    /// there are no cached handles to go stale. The rename itself (the core operation) already ran
    /// above per the Task-012 amendment (B2b CORRECTION / B7, user decision 2026-07-20). Task 013
    /// wires the real Manager-backed invalidation into this same seam.
    (void)renamed;
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
    chassert(!download_data || !download_data->remote_file_reader);
    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lk);
    cv.notify_all();
}

void FileSegment::setDownloadFailedUnlocked(const FileSegmentGuard::Lock & lock)
{
    LOG_INFO(getLog(), "Setting download as failed: {}", getInfoForLogUnlocked(lock));

    SCOPE_EXIT { cv.notify_all(); };

    setDownloadState(State::PARTIALLY_DOWNLOADED_NO_CONTINUATION, lock);

    /// Keep `download_data` (downloader_id) so the same downloader can still complete the segment.
    if (download_data)
    {
        if (download_data->cache_writer)
        {
            download_data->cache_writer->cancel();
            download_data->cache_writer.reset();
        }
        download_data->remote_file_reader.reset();
    }
}

void FileSegment::completePartAndResetDownloader()
{
    auto lk = lock();

    SCOPE_EXIT { cv.notify_all(); };

    assertNotDetachedUnlocked(lk);
    assertIsDownloaderUnlocked("completePartAndResetDownloader", lk);

    chassert(download_state == State::DOWNLOADING
             || download_state == State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);

    if (download_state == State::DOWNLOADING)
        resetDownloadingStateUnlocked(lk);

    resetDownloaderUnlocked(lk);

    LOG_TEST(getLog(), "Complete batch. ({})", getInfoForLogUnlocked(lk));
}

void FileSegment::shrinkFileSegmentToDownloadedSize(const LockedKey & locked_key, const FileSegmentGuard::Lock & lock, bool force_shrink_to_downloaded_size)
{
    chassert(downloaded_size);
    chassert(fs::file_size(getPath()) > 0);

    if (downloaded_size == range().size())
    {
        /// Nothing to resize;
        return;
    }

    if (!locked_key.isLastOwnerOfFileSegment(offset()))
    {
        throwFileCacheException(
            "Shrinking of file segment can be done only by the last holder: {}", getInfoForLog());
    }

    size_t result_size = downloaded_size;
    if (!force_shrink_to_downloaded_size)
    {
        size_t aligned_downloaded_size = FileCacheUtils::roundUpToMultiple(downloaded_size, cache->getBoundaryAlignment());
        result_size = std::min(aligned_downloaded_size, range().size());
    }

    chassert(result_size <= range().size());
    chassert(result_size >= downloaded_size);

    /// Return the reserve-ahead surplus (reserved but not downloaded) to the cache.
    chassert(reserved_size >= downloaded_size);
    if (reserved_size > downloaded_size)
    {
        queue_iterator->decrementSize(reserved_size - downloaded_size);
        reserved_size = downloaded_size.load();
    }

    if (result_size == range().size())
    {
        /// Nothing to resize;
        return;
    }

    LOG_TEST(getLog(), "Shrinking file segment {} -> {} (downloaded size: {})",
             range().size(), result_size, downloaded_size.load());

    segment_range.right = segment_range.left + result_size - 1;

    if (downloaded_size == result_size)
    {
        resetDownloadDataUnlocked(lock);
        setDownloadState(State::DOWNLOADED, lock);
    }
    else
        setDownloadState(State::PARTIALLY_DOWNLOADED, lock);

    if (download_state == State::DOWNLOADED)
        renameToIncludeSizeInNameUnlocked(lock);
}

size_t FileSegment::getSizeForBackgroundDownload() const
{
    auto lk = lock();
    return getSizeForBackgroundDownloadUnlocked(lk);
}

size_t FileSegment::getSizeForBackgroundDownloadUnlocked(const FileSegmentGuard::Lock &) const
{
    if (!background_download_enabled
        || !downloaded_size
        || !download_data
        || !download_data->remote_file_reader)
    {
        return 0;
    }

    chassert(downloaded_size <= range().size());

    const size_t background_download_max_file_segment_size = cache->getBackgroundDownloadMaxFileSegmentSize();
    size_t desired_size = 0;
    if (downloaded_size >= background_download_max_file_segment_size)
        desired_size = FileCacheUtils::roundUpToMultiple(downloaded_size, cache->getBoundaryAlignment());
    else
        desired_size = FileCacheUtils::roundUpToMultiple(background_download_max_file_segment_size, cache->getBoundaryAlignment());

    desired_size = std::min(desired_size, range().size());
    chassert(desired_size >= downloaded_size);

    return desired_size - downloaded_size;
}

void FileSegment::complete(FileSegmentPtr && file_segment, bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    if (!file_segment)
        throwFileCacheException("File segment is nullptr");

    if (file_segment->isCompleted())
        return;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentCompleteMicroseconds);

    auto locked_key = file_segment->lockKeyMetadata(false);
    if (!locked_key)
    {
        /// If we failed to lock a key, it must be in detached state.
        if (file_segment->isDetached())
            return;

        throwFileCacheException("Cannot complete file segment: {}", file_segment->getInfoForLog());
    }

    SCOPE_EXIT { file_segment.reset(); };

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

    SCOPE_EXIT {
        if (is_downloader)
            cv.notify_all();
    };

    LOG_TEST(
        getLog(), "Complete based on current state (is_last_holder: {}, force shrink: {}, {})",
        is_last_holder, force_shrink_to_downloaded_size, getInfoForLogUnlocked(segment_lock));

    if (is_downloader)
    {
        if (download_state == State::DOWNLOADING)
            resetDownloadingStateUnlocked(segment_lock);
        resetDownloaderUnlocked(segment_lock);
    }

    if (segment_kind == FileSegmentKind::Ephemeral && is_last_holder)
    {
        LOG_TEST(getLog(), "Removing temporary file segment: {}", getInfoForLogUnlocked(segment_lock));
        locked_key->removeFileSegment(offset(), segment_lock);
        return;
    }

    switch (download_state)
    {
        case State::DOWNLOADED:
        {
            chassert(current_downloaded_size == range().size());
            chassert(current_downloaded_size == fs::file_size(getPath()));
            chassert(!download_data);
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!is_last_holder);
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
            chassert(current_downloaded_size > 0);
            chassert(fs::exists(getPath()));
            chassert(fs::file_size(getPath()) > 0);

            if (is_last_holder)
            {
                bool added_to_download_queue = false;
                size_t background_download_size = allow_background_download ? getSizeForBackgroundDownloadUnlocked(segment_lock) : 0;
                if (background_download_size)
                {
                    ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundDownloadQueuePush);
                    added_to_download_queue = locked_key->addToDownloadQueue(offset(), segment_lock); /// Finish download in background.
                }

                if (!added_to_download_queue)
                {
                    if (download_data)
                    {
                        if (download_data->cache_writer)
                        {
                            try
                            {
                                download_data->cache_writer->finalize();
                            }
                            catch (...)
                            {
                                tryLogCurrentException(getLog(), "Failed to finalize cache writer on complete");
                            }
                            download_data->cache_writer.reset();
                        }
                        download_data->remote_file_reader.reset();
                    }

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            chassert(current_downloaded_size != range().size());

            if (is_last_holder)
            {
                if (current_downloaded_size == 0)
                {
                    locked_key->removeFileSegment(offset(), segment_lock);
                }
                else
                {
                    LOG_TEST(getLog(), "Resize file segment {} to downloaded: {}", range().toString(), current_downloaded_size);

                    if (download_data)
                    {
                        if (download_data->cache_writer)
                        {
                            try
                            {
                                download_data->cache_writer->finalize();
                            }
                            catch (...)
                            {
                                tryLogCurrentException(getLog(), "Failed to finalize cache writer on complete");
                            }
                            download_data->cache_writer.reset();
                        }
                        download_data->remote_file_reader.reset();
                    }

                    shrinkFileSegmentToDownloadedSize(*locked_key, segment_lock, force_shrink_to_downloaded_size);
                }
            }
            break;
        }
        default:
            throwFileCacheException("Unexpected state while completing file segment");
    }

    LOG_TEST(getLog(), "Completed file segment: {}", getInfoForLogUnlocked(segment_lock));

    if (download_state != State::DETACHED)
        chassert(assertCorrectnessUnlocked(segment_lock));
}

String FileSegment::getInfoForLog() const
{
    auto lk = lock();
    return getInfoForLogUnlocked(lk);
}

String FileSegment::getInfoForLogUnlocked(const FileSegmentGuard::Lock & lock) const
{
    const auto downloader_id = getDownloaderUnlocked(lock);
    return fmt::format(
        "File segment: {}, key: {}, state: {}, downloaded size: {}, reserved size: {}, "
        "downloader id: {}, current write offset: {}, caller id: {}, kind: {}, unbound: {}, "
        "background download: {}",
        range().toString(),
        key().toString(),
        stateToString(download_state.load()),
        getDownloadedSize(),
        reserved_size.load(),
        downloader_id.empty() ? "None" : downloader_id,
        getCurrentWriteOffset(),
        getCallerId(),
        toString(segment_kind),
        is_unbound,
        background_download_enabled);
}

String FileSegment::stateToString(FileSegment::State state)
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
    return "UNKNOWN";
}

bool FileSegment::assertCorrectness() const
{
    return assertCorrectnessUnlocked(lock());
}

bool FileSegment::assertCorrectnessUnlocked(const FileSegmentGuard::Lock & lock) const
{
    auto throw_logical = [&](const std::string & error)
    {
        throwFileCacheException("{}. File segment info: {}", error, getInfoForLogUnlocked(lock));
    };

    auto check_iterator = [&](const Priority::IteratorPtr & it)
    {
        if (!it)
            return;

        auto entry = it->getEntry();
        auto entry_size = entry->size.load(std::memory_order_relaxed);
        if (entry_size == 0)
        {
            entry = it->getEntry();
            entry_size = entry->size;
        }
        if (download_state != State::DOWNLOADING && entry_size != reserved_size)
            throw_logical(
                fmt::format("Expected entry.size == reserved_size ({} == {}, entry: {})",
                            entry_size, reserved_size.load(), entry->toString()));

        chassert(entry->key == key());
        chassert(entry->offset == offset());
    };

    const auto file_path = getPath();

    {
        std::unique_lock<std::mutex> write_lk;
        if (download_data)
            write_lk = std::unique_lock(download_data->write_mutex);

        if (downloaded_size == 0)
        {
            if (download_state != State::DOWNLOADING && fs::exists(file_path))
                throw_logical("Expected file " + file_path + " not to exist");
        }
        else if (!fs::exists(file_path))
        {
            throw_logical("Expected file " + file_path + " to exist");
        }
    }

    if (queue_iterator)
        chassert(!on_delayed_removal);

    switch (download_state.load())
    {
        case State::EMPTY:
        {
            chassert(getDownloaderUnlocked(lock).empty());
            chassert(!fs::exists(getPath()));
            chassert(!queue_iterator);
            break;
        }
        case State::DOWNLOADED:
        {
            chassert(!download_data);

            chassert(downloaded_size == reserved_size);
            chassert(downloaded_size == range().size());
            chassert(downloaded_size > 0);

            if (!size_in_filename)
            {
                auto file_size = fs::file_size(getPath());
                chassert(file_size == range().size());
            }
            chassert(downloaded_size == range().size());

            chassert(queue_iterator || on_delayed_removal);
            check_iterator(queue_iterator);
            break;
        }
        case State::DOWNLOADING:
        {
            chassert(!getDownloaderUnlocked(lock).empty());
            if (downloaded_size)
            {
                chassert(queue_iterator);
                chassert(fs::file_size(getPath()) > 0);
            }
            break;
        }
        case State::PARTIALLY_DOWNLOADED:
        {
            chassert(getDownloaderUnlocked(lock).empty());

            chassert(reserved_size >= downloaded_size);
            chassert(downloaded_size > 0);

            auto file_size = fs::file_size(getPath());

            chassert(file_size > 0);
            chassert(file_size <= range().size());
            chassert(downloaded_size <= range().size());

            chassert(queue_iterator || on_delayed_removal);
            check_iterator(queue_iterator);
            break;
        }
        case State::PARTIALLY_DOWNLOADED_NO_CONTINUATION:
        {
            chassert(reserved_size >= downloaded_size);
            check_iterator(queue_iterator);
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
    if (download_state == State::DETACHED)
    {
        throwFileCacheException(
            "Cache file segment is in detached state, operation not allowed. "
            "It can happen when cache was concurrently dropped with SYSTEM DROP FILESYSTEM CACHE FORCE. "
            "Please, retry. File segment info: {}", getInfoForLogUnlocked(lock));
    }
}

FileSegment::Info FileSegment::getInfo(const FileSegmentPtr & file_segment)
{
    auto lock = file_segment->lock();
    auto key_metadata = file_segment->tryGetKeyMetadata();
    return Info{
        .key = file_segment->key(),
        .offset = file_segment->offset(),
        .path = file_segment->tryGetPath(),
        .range_left = file_segment->range().left,
        .range_right = file_segment->range().right,
        .kind = file_segment->segment_kind,
        .state = file_segment->download_state,
        .size = file_segment->range().size(),
        .downloaded_size = file_segment->downloaded_size,
        .download_finished_time = file_segment->download_finished_time,
        .cache_hits = file_segment->hits_count,
        .references = static_cast<uint64_t>(file_segment.use_count()),
        .is_unbound = file_segment->is_unbound,
        .queue_entry_type = file_segment->queue_iterator ? file_segment->queue_iterator->getType() : QueueEntryType::None,
        .origin = *key_metadata->origin,
    };
}

bool FileSegment::isDetached() const
{
    auto lk = lock();
    return download_state == State::DETACHED;
}

bool FileSegment::isCompleted(bool sync) const
{
    auto is_completed_state = [this]() -> bool
    {
        return download_state == State::DOWNLOADED || download_state == State::DETACHED;
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
    key_metadata.reset();
    queue_iterator = nullptr;
    if (download_data && download_data->cache_writer)
        download_data->cache_writer->cancel();
    resetDownloadDataUnlocked(lock);
}

void FileSegment::detach(const FileSegmentGuard::Lock & lock, const LockedKey &)
{
    if (download_state == State::DETACHED)
        return;

    if (!getDownloaderUnlocked(lock).empty())
        resetDownloaderUnlocked(lock);
    setDetachedState(lock);
}

void FileSegment::increasePriority()
{
    if (!cache)
    {
        chassert(isDetached());
        return;
    }

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentIncreasePriorityMicroseconds);

    if (!increasing_priority.test_and_set(std::memory_order_acquire))
    {
        SCOPE_EXIT { increasing_priority.clear(std::memory_order_release); };

        auto it = getQueueIterator();
        if (it)
        {
            if (!cache->tryIncreasePriority(*this))
                ProfileEvents::increment(ProfileEvents::FileSegmentFailToIncreasePriority);

            /// Used only for system.filesystem_cache.
            ++hits_count;
        }
    }
}

FileSegment::~FileSegment()
{
    try
    {
        /// Can be non-finalized in case it was pushed to background download
        /// but not executed before server shutdown.
        if (download_data && download_data->cache_writer)
            download_data->cache_writer->finalize();
    }
    catch (...)
    {
        tryLogCurrentException(getLog());
    }
}

FileSegmentsHolder::FileSegmentsHolder(FileSegments && file_segments_)
    : file_segments(std::move(file_segments_))
{
}

FileSegmentPtr FileSegmentsHolder::getSingleFileSegment() const
{
    if (file_segments.size() != 1)
    {
        throwFileCacheException(
            "Expected single file segment, got: {} in holder {}", file_segments.size(), toString());
    }
    return file_segments.front();
}

void FileSegmentsHolder::reset()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FileSegmentHolderCompleteMicroseconds);

    ProfileEvents::increment(ProfileEvents::FilesystemCacheUnusedHoldFileSegments, file_segments.size());
    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();)
    {
        try
        {
            file_segment_it = completeAndPopFrontImpl(/*allow_background_download=*/true, /*force_shrink_to_downloaded_size=*/false);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }
    file_segments.clear();
}

FileSegmentsHolder::~FileSegmentsHolder()
{
    reset();
}

FileSegments::iterator FileSegmentsHolder::completeAndPopFrontImpl(bool allow_background_download, bool force_shrink_to_downloaded_size)
{
    auto file_segment_it = file_segments.begin();
    FileSegment::complete(std::move(*file_segment_it), allow_background_download, force_shrink_to_downloaded_size);
    return file_segments.erase(file_segment_it);
}

FileSegment & FileSegmentsHolder::add(FileSegmentPtr && file_segment)
{
    file_segments.push_back(file_segment);
    ProfileEvents::increment(ProfileEvents::FilesystemCacheHoldFileSegments);
    return *file_segments.back();
}

String FileSegmentsHolder::toString(bool with_state) const
{
    return ch::toString(file_segments, with_state);
}

String toString(const FileSegments & file_segments, bool with_state)
{
    String ranges;
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

}
