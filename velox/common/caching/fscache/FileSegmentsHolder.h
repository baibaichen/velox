/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#pragma once

#include "velox/common/caching/fscache/FileSegment.h"

#include <glog/logging.h>

#include <memory>
#include <thread>
#include <vector>

namespace facebook::velox::cache::fs {

// Re-declare the alias defined in KeyMetadata.h / FsCacheMetadata.h so this
// header does not need to pull in either of those larger dependencies.
// Identical typedef redeclarations across translation units are well-formed.
using FileSegmentPtr = std::shared_ptr<FileSegment>;

/// RAII owner for the segment list returned by FsCache::getOrSet. Callers
/// drive each segment from kEmpty -> kDownloaded; if the caller throws
/// mid-loop, the holder dtor invokes abandon() on any segment whose
/// writer is the current thread so other waiters wake instead of hanging.
class FileSegmentsHolder {
 public:
  explicit FileSegmentsHolder(std::vector<FileSegmentPtr> segments)
      : segments_{std::move(segments)} {}

  ~FileSegmentsHolder() {
    const auto self = std::this_thread::get_id();
    for (auto& seg : segments_) {
      if (seg == nullptr) {
        continue;
      }
      if (seg->state() == FileSegment::State::kDownloading &&
          seg->getDownloader() == self) {
        // Dtor must not propagate: throwing out of a dtor while another
        // dtor frame is already unwinding calls std::terminate. abandon()
        // is normally noexcept (it only flips state + drops the writer
        // slot), but log-and-continue here so a bug in a future version
        // can't crash the process — silently swallowing without a log
        // would be the actual silent-failure smell. Per-segment try/catch
        // so one bad segment doesn't prevent the rest from being
        // abandoned.
        try {
          seg->abandon();
        } catch (const std::exception& e) {
          LOG(ERROR) << "FileSegmentsHolder dtor: abandon() threw: "
                     << e.what();
        } catch (...) {
          LOG(ERROR) << "FileSegmentsHolder dtor: abandon() threw "
                        "non-std exception";
        }
      }
    }
  }

  /// Mutable access to the held segments.
  std::vector<FileSegmentPtr>& segments() {
    return segments_;
  }

  /// Read-only access to the held segments.
  const std::vector<FileSegmentPtr>& segments() const {
    return segments_;
  }

  /// True if no segments are held. Returned by `FsCache::getOrSet` when
  /// the request bypasses the cache (see spec §8.3 and Task 12
  /// shouldBypass) — callers must fall back to reading directly from
  /// the remote in that case.
  bool empty() const {
    return segments_.empty();
  }

  FileSegmentsHolder(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder& operator=(const FileSegmentsHolder&) = delete;
  FileSegmentsHolder(FileSegmentsHolder&&) noexcept = default;
  FileSegmentsHolder& operator=(FileSegmentsHolder&&) noexcept = default;

 private:
  std::vector<FileSegmentPtr> segments_;
};

/// Owning pointer alias used by FsCache::getOrSet return types.
using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

} // namespace facebook::velox::cache::fs
