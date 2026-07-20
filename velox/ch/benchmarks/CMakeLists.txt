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

# Random-seek benchmark for the assembled FileCache read path. Links the mono
# velox_ch_filecache target (which contains the center SCC, Factory/Manager, and
# DWIO sources) once; a separate *_dwio/_manager/_core library set does not exist
# under VELOX_MONO_LIBRARY.
add_executable(velox_ch_filecache_seek_benchmark FileCacheSeekBenchmark.cpp)

target_link_libraries(
  velox_ch_filecache_seek_benchmark
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_file
    velox_exception
    velox_memory
    Folly::folly
    Folly::follybenchmark
    gflags::gflags
)

# FileCache wrapper microbenchmark: fcbi (our ch::FileCache, Manager-built) vs
# cbi (native AsyncDataCache + SsdCache) vs dbi (DirectBufferedInput). Links the
# mono velox_ch_filecache target once (center SCC + Factory/Manager + DWIO), the
# native caching library for cbi's AsyncDataCache/SsdCache, and velox_dwio_common
# for Cached/DirectBufferedInput.
add_executable(
  velox_ch_filecache_wrapper_benchmark
  FileCacheWrapperBenchmark.cpp
  CacheReadHarness.cpp)

target_link_libraries(
  velox_ch_filecache_wrapper_benchmark
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_caching
    velox_file
    velox_exception
    velox_memory
    velox_flag_definitions
    Folly::folly
    gflags::gflags
    glog::glog
)
