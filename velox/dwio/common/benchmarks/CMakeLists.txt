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

# Shared cache-read harness: working-set resolution, flat-key DataLayout and the
# per-wrapper read harnesses (cbi / fcbi). Reused by the wrapper microbench and
# the standalone velox_cache_verify tool.
add_library(velox_cache_read_harness CacheReadHarness.cpp CacheVerify.cpp)
target_link_libraries(
  velox_cache_read_harness
  PUBLIC
    velox_dwio_common
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)

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
    velox_cache_read_harness
    velox_dwio_common
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)

# One-time cache-correctness gate: reloads a backend's persisted on-disk cache
# and byte-compares the whole working set against the source files. Used after a
# --phase=cold run to certify the persisted cache before hot re-runs.
add_executable(velox_cache_verify CacheVerifyMain.cpp)
target_link_libraries(
  velox_cache_verify
  PRIVATE
    velox_cache_read_harness
    velox_dwio_common
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

# Deterministic A/B/C BufferedInput trace replay library.
add_library(velox_buffered_input_trace_replay_lib
            BufferedInputTraceReplay.cpp)
target_link_libraries(
  velox_buffered_input_trace_replay_lib
  PUBLIC
    velox_dwio_common
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)

add_executable(velox_buffered_input_trace_replay
               BufferedInputTraceReplayMain.cpp)
target_link_libraries(
  velox_buffered_input_trace_replay
  PRIVATE
    velox_buffered_input_trace_replay_lib
    velox_dwio_common
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    Folly::folly
    gflags::gflags
    glog::glog)
