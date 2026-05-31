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

#include "velox/dwio/common/benchmarks/CacheVerify.h"

#include <algorithm>
#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

namespace facebook::velox::dwio::common::bench {
namespace {

// Copies up to `readSize` bytes from `stream` into `out`. The harness hands the
// consumer a stream positioned at the segment start; like drain() we clamp the
// final Next() chunk to the requested size and stop, so `out` holds exactly the
// segment's [0, copied) bytes. Returns the number of bytes captured.
uint64_t copyStream(
    SeekableInputStream& stream,
    uint64_t readSize,
    std::string& out) {
  out.clear();
  out.reserve(readSize);
  uint64_t copied = 0;
  const void* data = nullptr;
  int32_t size = 0;
  while (copied < readSize && stream.Next(&data, &size)) {
    const auto take =
        std::min<uint64_t>(static_cast<uint64_t>(size), readSize - copied);
    out.append(static_cast<const char*>(data), take);
    copied += take;
  }
  return copied;
}

void foldChecksum(uint64_t& hash, const char* data, uint64_t size) {
  for (uint64_t i = 0; i < size; ++i) {
    hash ^= static_cast<uint8_t>(data[i]);
    hash *= 0x100000001b3ULL; // FNV-1a 64 prime.
  }
}

} // namespace

CacheVerifier::CacheVerifier(const std::vector<SourceFile>& sources) {
  sources_.reserve(sources.size());
  for (const auto& source : sources) {
    sources_.push_back(std::make_shared<LocalReadFile>(source.path));
  }
}

StreamConsumer CacheVerifier::makeConsumer(VerifyResult& result) const {
  return [this, &result](
             SeekableInputStream& stream,
             uint64_t readSize,
             uint32_t fileIdx,
             uint64_t offset) {
    std::string got;
    const uint64_t copied = copyStream(stream, readSize, got);

    VELOX_CHECK_LT(
        fileIdx, sources_.size(), "verify: source index out of range");
    // Compare against the full requested region: DataLayout only emits blocks
    // fully contained in the file, so pread(readSize) never over-reads EOF. A
    // wrapper that returns a short stream (copied < readSize) must fail loud
    // rather than have its truncated prefix silently match.
    const std::string want = sources_[fileIdx]->pread(offset, readSize);

    result.segmentsChecked++;
    result.bytesChecked += copied;
    foldChecksum(result.checksum, got.data(), got.size());

    const bool mismatch = got.size() != want.size() ||
        std::memcmp(got.data(), want.data(), got.size()) != 0;
    if (mismatch) {
      if (result.mismatches == 0) {
        result.firstMismatchFileIdx = fileIdx;
        result.firstMismatchOffset = offset;
      }
      result.mismatches++;
    }
  };
}

} // namespace facebook::velox::dwio::common::bench
