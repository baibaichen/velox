# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Phase-1 FsCache::getOrSet self-cost microbenchmark. Gated on
# VELOX_ENABLE_BENCHMARKS_BASIC so the default OFF build is unaffected.
add_executable(velox_fscache_benchmark FsCacheBenchmark.cpp)
target_link_libraries(
  velox_fscache_benchmark
  PRIVATE
    velox_fscache
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)
