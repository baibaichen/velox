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

#include <cstddef>
#include <optional>

namespace facebook::velox::ch {

/// Per-read consumer settings for the filesystem cache stage.
/// Ported from ClickHouse IO/ReadSettings.h FilesystemCacheSettings.
struct FilesystemCacheSettings {
  bool readIfExistsOtherwiseBypass = false;
  size_t segmentsBatchSize = 20;
  std::optional<size_t> boundaryAlignment;
  bool allowBackgroundDownload = true;
  bool allowBackgroundDownloadForMetadataFilesInPackedStorage = true;
  bool allowBackgroundDownloadDuringFetch = true;
  bool preferBiggerBufferSize = true;
  size_t reserveSpaceWaitLockTimeoutMilliseconds = 1000;
  size_t maxDownloadSizePerQuery = (128UL * 1024 * 1024 * 1024);
  bool skipDownloadIfExceedsPerQueryCacheWriteLimit = true;
  bool enableLog = false;
};

} // namespace facebook::velox::ch
