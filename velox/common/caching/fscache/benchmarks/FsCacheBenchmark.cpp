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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "velox/common/file/FileSystems.h"

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  facebook::velox::filesystems::registerLocalFileSystem();
  LOG(INFO) << "velox_fscache_benchmark scaffold OK";
  return 0;
}
