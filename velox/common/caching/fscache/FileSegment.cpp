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

#include "velox/common/caching/fscache/FileSegment.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <fmt/format.h>

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

namespace facebook::velox::cache::fs {

FileSegment::~FileSegment() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

std::string FileSegment::localPath(const std::string& cacheRoot) const {
  const std::string name = key_.fileName();
  return fmt::format(
      "{}/{}/{}/{}", cacheRoot, name.substr(0, 2), name.substr(2, 2), name);
}

bool FileSegment::beginDownload() {
  State expected = State::kEmpty;
  return state_.compare_exchange_strong(
      expected, State::kDownloading, std::memory_order_acq_rel);
}

void FileSegment::download(
    ::facebook::velox::ReadFile& remote,
    const std::string& cacheRoot) {
  VELOX_CHECK(
      state() == State::kDownloading,
      "FileSegment::download requires beginDownload() to have been called, state={}",
      static_cast<int>(state()));
  const std::string finalPath = localPath(cacheRoot);
  // Warm-restart short-circuit: a prior process already produced this exact
  // file (same hash + offset + size). Skip the remote re-download and publish
  // the existing bytes directly. FsCache::loadFromDisk() does not pre-credit
  // these to bytesOnDisk, so the writer path that called us still runs
  // onInsert + bytesOnDisk += size and the segment becomes evictable.
  std::error_code existCheck;
  if (std::filesystem::exists(finalPath, existCheck) && !existCheck &&
      std::filesystem::file_size(finalPath, existCheck) == key_.size &&
      !existCheck) {
    downloadedSize_.store(key_.size, std::memory_order_release);
    state_.store(State::kDownloaded, std::memory_order_release);
    return;
  }
  const std::string tmpPath = finalPath + ".tmp";

  std::filesystem::create_directories(
      std::filesystem::path{finalPath}.parent_path());

  try {
    std::ofstream out{tmpPath, std::ios::binary | std::ios::trunc};
    VELOX_CHECK(out.is_open(), "Cannot open .tmp for write: {}", tmpPath);

    constexpr uint64_t kChunk = 1UL * 1'024 * 1'024;
    std::vector<char> buffer(kChunk);
    uint64_t remaining = key_.size;
    uint64_t offset = key_.offset;
    while (remaining > 0) {
      const uint64_t toRead = std::min(kChunk, remaining);
      const std::string_view view = remote.pread(offset, toRead, buffer.data());
      VELOX_CHECK_EQ(
          view.size(),
          toRead,
          "Short read from remote: got {} expected {}",
          view.size(),
          toRead);
      out.write(view.data(), view.size());
      VELOX_CHECK(out.good(), "Write to .tmp failed: {}", tmpPath);
      offset += toRead;
      remaining -= toRead;
      downloadedSize_.fetch_add(toRead, std::memory_order_release);
    }
    out.close();

    std::filesystem::rename(tmpPath, finalPath);
    state_.store(State::kDownloaded, std::memory_order_release);
  } catch (...) {
    // Only remove the .tmp this writer was building -- never the finalPath.
    // The rename below is the publication point; if the catch fires the
    // rename hasn't happened, so a pre-existing finalPath (e.g. a valid
    // cache file left by a prior process during recovery) must be left
    // untouched.
    std::error_code ignore;
    std::filesystem::remove(tmpPath, ignore);
    downloadedSize_.store(0, std::memory_order_release);
    state_.store(State::kEmpty, std::memory_order_release);
    throw;
  }
}

bool FileSegment::reserve(
    uint64_t reservedBytes,
    const std::string& cacheRoot) {
  State expected = State::kEmpty;
  if (!state_.compare_exchange_strong(
          expected, State::kDownloading, std::memory_order_acq_rel)) {
    return false;
  }
  const std::string finalPath = localPath(cacheRoot);
  // Warm-restart short-circuit: a prior process already produced this exact
  // file (same hash + offset + size). Skip the remote re-download and publish
  // the existing bytes directly. FsCache::loadFromDisk() does not pre-credit
  // these to bytesOnDisk, so the writer path that called us still runs
  // onInsert + bytesOnDisk += size and the segment becomes evictable.
  std::error_code existCheck;
  if (std::filesystem::exists(finalPath, existCheck) && !existCheck &&
      std::filesystem::file_size(finalPath, existCheck) == key_.size &&
      !existCheck) {
    downloadedSize_.store(key_.size, std::memory_order_release);
    state_.store(State::kDownloaded, std::memory_order_release);
    cv_.notify_all();
    return false;
  }
  std::filesystem::create_directories(
      std::filesystem::path{finalPath}.parent_path());
  // Spec §5.4: open O_CREAT|O_WRONLY with mode 0644, NO ftruncate.
  // complete() ftruncates to key().size; abandon() leaves a short file
  // whose stat_size lets loadFromDisk recognise the partial.
  const int fd = ::open(finalPath.c_str(), O_CREAT | O_WRONLY, 0644);
  if (fd < 0) {
    const int err = errno;
    state_.store(State::kEmpty, std::memory_order_release);
    VELOX_FAIL("Failed to open cache file for write: {} errno={}", finalPath, err);
  }
  fd_ = fd;
  reservedBytes_ = reservedBytes;
  downloader_.store(std::this_thread::get_id(), std::memory_order_release);
  return true;
}

void FileSegment::write(const char* buf, uint64_t len) {
  VELOX_CHECK_EQ(
      static_cast<int>(state()),
      static_cast<int>(State::kDownloading),
      "FileSegment::write called outside kDownloading");
  const uint64_t already = downloadedSize_.load(std::memory_order_acquire);
  VELOX_CHECK_LE(
      already + len,
      reservedBytes_,
      "Write exceeds reserved bytes: already={} len={} reserved={}",
      already,
      len,
      reservedBytes_);
  uint64_t written{0};
  while (written < len) {
    const ssize_t n = ::pwrite(
        fd_,
        buf + written,
        len - written,
        static_cast<off_t>(already + written));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      const int err = errno;
      VELOX_FAIL("pwrite failed: errno={} fd={}", err, fd_);
    }
    if (n == 0) {
      VELOX_FAIL("pwrite returned 0: fd={}", fd_);
    }
    written += static_cast<uint64_t>(n);
    // Advance per chunk (not once at the end) so a partial-then-fail keeps
    // downloadedSize_ in sync with actual on-disk bytes. Phase-2 resume after
    // abandon() relies on this to skip already-written bytes.
    downloadedSize_.store(already + written, std::memory_order_release);
  }
  // Wake up any reader waiting on partial bytes / state changes.
  {
    std::lock_guard<FileSegmentMutex> lk{mutex_};
    cv_.notify_all();
  }
}

void FileSegment::complete() {
  State expected = State::kDownloading;
  VELOX_CHECK(
      state_.compare_exchange_strong(
          expected, State::kDownloaded, std::memory_order_acq_rel),
      "complete() requires kDownloading, found state={}",
      static_cast<int>(expected));
  if (::ftruncate(fd_, static_cast<off_t>(key_.size)) != 0) {
    const int err = errno;
    VELOX_FAIL("ftruncate failed: errno={} fd={} size={}", err, fd_, key_.size);
  }
  if (::fsync(fd_) != 0) {
    const int err = errno;
    VELOX_FAIL("fsync failed: errno={} fd={}", err, fd_);
  }
  if (::close(fd_) != 0) {
    const int err = errno;
    VELOX_FAIL("close failed: errno={} fd={}", err, fd_);
  }
  fd_ = -1;
  downloader_.store(std::thread::id{}, std::memory_order_release);
  {
    std::lock_guard<FileSegmentMutex> lk{mutex_};
    cv_.notify_all();
  }
}

void FileSegment::abandon() {
  // Best-effort close; per spec §5.5 a lost CAS (writer raced to complete())
  // is not an error.
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  State expected = State::kDownloading;
  state_.compare_exchange_strong(
      expected, State::kPartiallyDownloaded, std::memory_order_acq_rel);
  downloader_.store(std::thread::id{}, std::memory_order_release);
  {
    std::lock_guard<FileSegmentMutex> lk{mutex_};
    cv_.notify_all();
  }
}

std::thread::id FileSegment::getDownloader() const noexcept {
  return downloader_.load(std::memory_order_acquire);
}

void FileSegment::waitForDownloadedSize(uint64_t needed) {
  std::unique_lock<FileSegmentMutex> lk{mutex_};
  cv_.wait(lk, [&]() {
    return downloadedSize_.load(std::memory_order_acquire) >= needed ||
        state_.load(std::memory_order_acquire) != State::kDownloading;
  });
  const auto have = downloadedSize_.load(std::memory_order_acquire);
  if (have >= needed) {
    return;
  }
  // State exited kDownloading short of needed: writer abandoned. Reader
  // must surface the failure rather than reading garbage past the
  // partial boundary.
  const auto finalState = state_.load(std::memory_order_acquire);
  VELOX_FAIL(
      "FileSegment writer abandoned with downloadedSize={} < needed={}, finalState={}",
      have,
      needed,
      static_cast<int>(finalState));
}

void FileSegment::read(
    uint64_t offsetInSegment,
    uint64_t length,
    char* outBuf,
    const std::string& cacheRoot) const {
  // Spec §4.2 partial-readable: caller is expected to have called
  // waitForDownloadedSize(offsetInSegment + length) first, so the bytes
  // exist on disk even if the segment is still kDownloading. Allow read
  // from kDownloading + kDownloaded + kDetached; reject only states where
  // the file may have been removed (kEmpty, kPartiallyDownloaded* with
  // pending eviction).
  const auto currentState = state();
  VELOX_CHECK(
      currentState == State::kDownloaded || currentState == State::kDetached ||
          currentState == State::kDownloading,
      "FileSegment::read called in state {}",
      static_cast<int>(currentState));
  VELOX_CHECK_LE(offsetInSegment + length, key_.size);
  const auto downloaded =
      downloadedSize_.load(std::memory_order_acquire);
  VELOX_CHECK_LE(
      offsetInSegment + length,
      downloaded,
      "FileSegment::read past durable byte boundary; caller must waitForDownloadedSize first: end={} downloadedSize={}",
      offsetInSegment + length,
      downloaded);

  // Go through Velox's LocalReadFile rather than std::ifstream so the read
  // follows the same FileIoContext / pread semantics every other Velox path
  // uses; this also makes it cheap to swap in a different FileSystem later.
  const std::string path = localPath(cacheRoot);
  ::facebook::velox::LocalReadFile file{path};
  const auto view = file.pread(offsetInSegment, length, outBuf);
  VELOX_CHECK_EQ(
      view.size(), length, "Short read from cache file: {}", path);
}

} // namespace facebook::velox::cache::fs
