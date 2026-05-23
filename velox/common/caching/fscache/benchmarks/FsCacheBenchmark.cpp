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

// Phase-1 microbenchmark for FsCache::getOrSet. See
// docs/superpowers/specs/2026-05-23-fscache-microbench-design.md for the
// design and the 36-cell sweep specification.

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace {

using namespace facebook::velox;

// ReadFile that delegates to an inner LocalReadFile and optionally sleeps
// `latencyUs_` microseconds before each pread to simulate remote IO.
// Composition is required because LocalReadFile's pread/size/preadv/
// memoryUsage/shouldCoalesce are `final`. Bumps the inherited
// ReadFile::bytesRead_ counter directly so the driver can read bytesRead()
// to measure how many bytes hit the remote.
class SleepyReadFile : public ReadFile {
 public:
  SleepyReadFile(const std::string& path, uint64_t latencyUs)
      : inner_(path), latencyUs_(latencyUs) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override {
    if (latencyUs_ != 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(latencyUs_));
    }
    auto out = inner_.pread(offset, length, buf, context);
    bytesRead_ += length;
    return out;
  }

  uint64_t size() const override {
    return inner_.size();
  }

  uint64_t memoryUsage() const override {
    return inner_.memoryUsage();
  }

  bool shouldCoalesce() const override {
    return inner_.shouldCoalesce();
  }

  std::string getName() const override {
    return inner_.getName();
  }

  uint64_t getNaturalReadSize() const override {
    return inner_.getNaturalReadSize();
  }

 private:
  mutable LocalReadFile inner_;
  const uint64_t latencyUs_;
};

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();
  LOG(INFO) << "velox_fscache_benchmark scaffold OK";
  return 0;
}
