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

#include <memory>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

namespace facebook::velox::ch {

/// A ReadFile that shares ownership of an underlying ReadFile and forwards all
/// calls to it. Consumers such as FileInputStream take ownership of their
/// reader through a unique_ptr<ReadFile>; this adapter lets such a consumer be
/// built over a shared_ptr<ReadFile> without transferring sole ownership. The
/// underlying file stays alive for as long as the adapter does, so a segment's
/// remote reader (used for background tail-fill) can safely outlive the
/// BufferedInput that created it.
class SharedReadFile final : public velox::ReadFile {
 public:
  explicit SharedReadFile(std::shared_ptr<velox::ReadFile> file)
      : file_(std::move(file)) {
    VELOX_CHECK_NOT_NULL(file_, "SharedReadFile requires a non-null ReadFile");
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override {
    return file_->pread(offset, length, buf, context);
  }

  std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& context = {}) const override {
    return file_->pread(offset, length, context);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const override {
    return file_->preadv(offset, buffers, context);
  }

  uint64_t preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      const FileIoContext& context = {}) const override {
    return file_->preadv(regions, iobufs, context);
  }

  folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const override {
    return file_->preadvAsync(offset, buffers, context);
  }

  bool hasPreadvAsync() const override {
    return file_->hasPreadvAsync();
  }

  bool shouldCoalesce() const override {
    return file_->shouldCoalesce();
  }

  uint64_t size() const override {
    return file_->size();
  }

  uint64_t memoryUsage() const override {
    return file_->memoryUsage();
  }

  uint64_t bytesRead() const override {
    return file_->bytesRead();
  }

  void resetBytesRead() override {
    file_->resetBytesRead();
  }

  std::string getName() const override {
    return file_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return file_->getNaturalReadSize();
  }

 private:
  const std::shared_ptr<velox::ReadFile> file_;
};

} // namespace facebook::velox::ch
