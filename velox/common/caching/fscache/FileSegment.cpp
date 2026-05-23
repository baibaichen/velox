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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <vector>

namespace facebook::velox::cache::fs {

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

void FileSegment::read(
    uint64_t offsetInSegment,
    uint64_t length,
    char* outBuf,
    const std::string& cacheRoot) const {
  VELOX_CHECK(
      state() == State::kDownloaded || state() == State::kDetached,
      "FileSegment::read called in state {}",
      static_cast<int>(state()));
  VELOX_CHECK_LE(offsetInSegment + length, key_.size);

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
