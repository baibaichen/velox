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

# Task 014 DWIO read path: the Velox BufferedInput/SeekableInputStream that
# drive the ClickHouse FileCache scan reads (ported from
# CachedOnDiskReadBufferFromFile). In mono builds velox_add_library appends
# these sources to the velox target and aliases velox_ch_filecache_dwio to it.
velox_add_library(
  velox_ch_filecache_dwio
  FileCacheBufferedInput.cpp
  FileCacheInputStream.cpp
  HEADERS
    FileCacheRequestContext.h
    FileCacheFileIdentity.h
    FileCacheBufferedInput.h
    FileCacheInputStream.h)

# In non-mono builds velox_ch_filecache_dwio is a real library; link its direct
# dependencies. In mono builds it is an alias to velox (which already links
# these) and target_link_libraries() on an alias is an error, so guard on the
# mode (mirrors velox_ch_filecache_core / velox_ch_filecache_manager).
if(NOT VELOX_MONO_LIBRARY)
  target_link_libraries(
    velox_ch_filecache_dwio
    PUBLIC
      velox_ch_filecache_manager
      velox_ch_filecache_core
      velox_ch_filecache
      velox_dwio_common
      velox_file
      velox_memory
      Folly::folly
      fmt::fmt)
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
