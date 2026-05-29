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
#include "velox/common/caching/filecache/FileCacheSettings.h"

#include <cerrno>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <sys/statvfs.h>

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ch {

namespace fs = std::filesystem;

void FileCacheSettings::validate() {
  auto& settings = *this;

  if (!settings.path.changed) {
    VELOX_USER_FAIL("`path` is required parameter of cache configuration");
  }

  if (fs::path(settings.path.value).is_relative()) {
    VELOX_CHECK(
        false,
        "`path` was not normalized to absolute: {}",
        settings.path.value);
  }

  if (!settings.maxSize.changed && !settings.maxSizeRatioToTotalSpace.changed) {
    VELOX_USER_FAIL(
        "Either `max_size` or `max_size_ratio_to_total_space` must be defined in cache configuration");
  }

  if (settings.maxSize.changed && settings.maxSizeRatioToTotalSpace.changed) {
    VELOX_USER_FAIL(
        "`max_size` and `max_size_ratio_to_total_space` cannot be specified at the same time");
  }

  if (settings.maxSize.changed && settings.maxSize == 0) {
    VELOX_USER_FAIL("`max_size` cannot be 0");
  }

  if (settings.overcommitEvictionEvictStep == 0) {
    VELOX_USER_FAIL("`overcommit_eviction_evict_step` cannot be zero");
  }

  if (settings.boundaryAlignment > settings.maxFileSegmentSize) {
    VELOX_USER_FAIL(
        "`boundary_alignment` ({}) must not exceed `max_file_segment_size` ({}): "
        "each file segment must be large enough to cover the alignment window",
        settings.boundaryAlignment.value,
        settings.maxFileSegmentSize.value);
  }

  if (settings.maxSizeRatioToTotalSpace.changed) {
    if (settings.maxSizeRatioToTotalSpace <= 0 ||
        settings.maxSizeRatioToTotalSpace > 1) {
      VELOX_USER_FAIL(
          "`max_size_ratio_to_total_space` must be in range (0, 1]");
    }

    try {
      fs::create_directories(settings.path.value);
    } catch (const fs::filesystem_error& e) {
      VELOX_USER_FAIL(
          "Failed to create cache directory {}: {}", settings.path.value, e.what());
    }

    struct statvfs stat {};
    if (::statvfs(settings.path.value.c_str(), &stat) != 0) {
      VELOX_USER_FAIL(
          "Failed to statvfs cache path {}: {}",
          settings.path.value,
          std::strerror(errno));
    }

    const auto totalSpace = stat.f_blocks * stat.f_frsize;
    settings.maxSize = static_cast<uint64_t>(std::floor(
        settings.maxSizeRatioToTotalSpace.value * static_cast<double>(totalSpace)));

    LOG(INFO) << "Using max_size as ratio "
              << settings.maxSizeRatioToTotalSpace.value
              << " to total disk space on path " << settings.path.value << ": "
              << settings.maxSize.value << " (total space: " << totalSpace << ")";
  }
}

// TODO(config): loadFromConfig/loadFromCollection and system-settings dump logic
// are CH configuration and system-table infrastructure from
// FileCacheSettings.cpp; Velox integration will construct FileCacheSettings
// programmatically in the later configuration unit.

} // namespace facebook::velox::ch
