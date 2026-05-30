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

#include <fmt/format.h>
#include <memory>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/ByteStream.h"

namespace facebook::velox::ch {

/// A positioned, random-access ByteInputStream over a shared ReadFile. The
/// cursor is an ABSOLUTE file offset, so seekp() is a cheap pointer move and
/// readBytes() issues a single positioned pread at that offset. This is the key
/// difference from FileInputStream, whose seek advances by sequentially reading
/// (and thus fetching) every intervening byte -- prohibitive when downloading a
/// cache segment that starts at a large file offset.
///
/// Used to stream a cache segment's bytes into the cache, both for the
/// foreground prefix download and the background tail-fill. It holds a
/// shared_ptr to the underlying file so the reader can outlive the
/// BufferedInput that created it (the background tail-fill runs later, on the
/// FileCache's own download threads).
///
/// Only the methods exercised by the download path are meaningful:
/// size/atEnd/tellp/seekp/remainingSize/readBytes/readByte/skip. nextView() is
/// unsupported because there is no persistent backing buffer to view into.
/// Likewise, the inherited non-virtual template read<T>() must NOT be called: it
/// dereferences the base's `current_` byte range, which this stream never sets;
/// use readBytes()/readByte() instead.
class ReadFileByteInputStream final : public velox::ByteInputStream {
 public:
  explicit ReadFileByteInputStream(std::shared_ptr<velox::ReadFile> file)
      : file_{std::move(file)}, size_{file_ ? file_->size() : 0} {
    VELOX_CHECK_NOT_NULL(
        file_, "ReadFileByteInputStream requires a non-null ReadFile");
  }

  using velox::ByteInputStream::readBytes;

  size_t size() const override {
    return size_;
  }

  bool atEnd() const override {
    return pos_ >= size_;
  }

  std::streampos tellp() const override {
    return static_cast<std::streampos>(pos_);
  }

  void seekp(std::streampos pos) override {
    const int64_t target = pos;
    VELOX_CHECK_GE(target, 0, "ReadFileByteInputStream: negative seek position");
    VELOX_CHECK_LE(
        static_cast<uint64_t>(target),
        size_,
        "ReadFileByteInputStream: seek past end of file");
    pos_ = static_cast<uint64_t>(target);
  }

  size_t remainingSize() const override {
    return size_ - pos_;
  }

  uint8_t readByte() override {
    uint8_t byte;
    readBytes(&byte, 1);
    return byte;
  }

  void readBytes(uint8_t* bytes, int32_t size) override {
    VELOX_CHECK_GE(size, 0, "ReadFileByteInputStream: negative read size");
    if (size == 0) {
      return;
    }
    const auto numBytes = static_cast<uint64_t>(size);
    VELOX_CHECK_LE(
        numBytes,
        remainingSize(),
        "ReadFileByteInputStream: read past end of file");
    file_->pread(pos_, numBytes, bytes);
    pos_ += numBytes;
  }

  std::string_view nextView(int64_t /*size*/) override {
    VELOX_NYI(
        "ReadFileByteInputStream::nextView is not supported; use readBytes");
  }

  void skip(int32_t size) override {
    VELOX_CHECK_GE(size, 0, "ReadFileByteInputStream: negative skip size");
    seekp(static_cast<std::streampos>(pos_ + static_cast<uint64_t>(size)));
  }

  std::string toString() const override {
    return fmt::format(
        "ReadFileByteInputStream({}) pos {} size {}",
        file_->getName(),
        pos_,
        size_);
  }

 private:
  const std::shared_ptr<velox::ReadFile> file_;
  const uint64_t size_;
  uint64_t pos_{0};
};

} // namespace facebook::velox::ch
