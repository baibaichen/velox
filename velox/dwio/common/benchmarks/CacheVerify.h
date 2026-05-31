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

// Byte-level cache-correctness gate shared by the velox_cache_verify tool. It
// re-reads a working set through one of the CacheReadHarness wrappers (cbi /
// fcbi / dbi) and compares every loaded segment against a fresh pread of the
// source file. The intent is a one-time gate after a cold prime so later hot
// runs can trust the persisted cache: any byte mismatch is recorded with the
// offending file/offset so a corrupt or stale cache fails loud instead of
// silently serving wrong data.
//
// The wrapper-specific read is reached through each harness's readBatch, so the
// sweep methods are templated over the harness type; the byte-compare consumer
// itself is type-erased (StreamConsumer) and lives in the .cpp.

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/benchmarks/CacheReadHarness.h"

namespace facebook::velox {
class ReadFile;
} // namespace facebook::velox

namespace facebook::velox::dwio::common::bench {

// Outcome of a verify sweep. `ok()` is the gate: zero mismatches over a
// non-empty working set. The checksum is a running FNV-1a hash of the
// wrapper-returned bytes in sweep order, useful for cross-backend / cross-run
// equality spot checks beyond the per-segment compare.
struct VerifyResult {
  uint64_t segmentsChecked{0};
  uint64_t bytesChecked{0};
  uint64_t mismatches{0};
  // Location of the first mismatching segment (valid only when mismatches > 0).
  uint32_t firstMismatchFileIdx{0};
  uint64_t firstMismatchOffset{0};
  // FNV-1a 64 over every verified wrapper byte, in sweep order.
  uint64_t checksum{0};

  bool ok() const {
    return mismatches == 0 && segmentsChecked > 0;
  }
};

// Compares cache-served bytes against the source files they were derived from.
// Construct with the same SourceFile set the harness was built from; the
// verifier opens an independent read-only handle per source for the reference
// preads (so it never shares the wrapper's cache path).
class CacheVerifier {
 public:
  explicit CacheVerifier(const std::vector<SourceFile>& sources);

  // Walks the whole flat key space [0, layout.totalKeys()) in `batch`-sized
  // groups, reading each segment of `readSize` bytes through `harness` and
  // comparing it to a source pread of the same [offset, readSize) range.
  // Accumulates counts/checksum into the returned result; the caller decides
  // what to do on mismatch (the tool exits non-zero).
  template <typename Harness>
  VerifyResult verifyWorkingSet(
      Harness& harness,
      const DataLayout& layout,
      uint64_t readSize,
      uint64_t batch);

 private:
  // Builds the byte-capturing/comparing consumer that drives one loaded stream,
  // preads the matching source range and folds the comparison into `result`.
  StreamConsumer makeConsumer(VerifyResult& result) const;

  std::vector<std::shared_ptr<ReadFile>> sources_;
};

template <typename Harness>
VerifyResult CacheVerifier::verifyWorkingSet(
    Harness& harness,
    const DataLayout& layout,
    uint64_t readSize,
    uint64_t batch) {
  VerifyResult result;
  const auto ioStats = std::make_shared<io::IoStatistics>();
  const StreamConsumer consume = makeConsumer(result);
  const uint64_t totalKeys = layout.totalKeys();
  std::map<uint32_t, std::vector<uint64_t>> byFile;
  uint64_t pending = 0;
  for (uint64_t key = 0; key < totalKeys; ++key) {
    const auto loc = layout.resolve(key);
    byFile[loc.fileIdx].push_back(loc.offset);
    if (++pending >= batch) {
      harness.readBatch(byFile, readSize, ioStats, consume);
      byFile.clear();
      pending = 0;
    }
  }
  if (pending > 0) {
    harness.readBatch(byFile, readSize, ioStats, consume);
  }
  return result;
}

} // namespace facebook::velox::dwio::common::bench
