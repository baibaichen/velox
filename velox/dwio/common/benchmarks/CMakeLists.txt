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

# Wrapper microbench comparing CachedBufferedInput (AsyncDataCache + SsdCache)
# vs FileCacheBufferedInput (ch::FileCache), with cache config aligned to the
# e2e TPC-H A/B benchmark. Gated on VELOX_ENABLE_BENCHMARKS_BASIC so the
# default OFF build is unaffected.
add_executable(velox_bufferedinput_wrapper_benchmark
               BufferedInputWrapperBenchmark.cpp)
# Default directory for the per-run markdown report (--report_dir). Using the
# source dir keeps reports next to the benchmark regardless of the run cwd,
# without hardcoding an absolute path in the source.
target_compile_definitions(
  velox_bufferedinput_wrapper_benchmark
  PRIVATE WRAPPER_BENCH_REPORT_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(
  velox_bufferedinput_wrapper_benchmark
  PRIVATE
    velox_dwio_common
    velox_ch_file_cache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)
