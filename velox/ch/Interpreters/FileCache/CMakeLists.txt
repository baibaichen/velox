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

# Compiled sources for velox_ch_filecache. In the default mono build
# velox_ch_filecache is an alias to the velox target and cannot be passed to
# target_sources() directly, so route through the velox_sources() helper which
# targets the real underlying library in both mono and non-mono builds.
velox_sources(
  velox_ch_filecache
  PRIVATE
  FileSegmentKeyType.cpp
  FileCacheKey.cpp
  FileCacheSettings.cpp
)

# Guards.h is a header-only addition to velox_ch_filecache.
# With VELOX_MONO_LIBRARY=ON the global include_directories(.) from the root
# CMakeLists already makes it findable; we register the FILE_SET only in
# non-mono builds so IDE navigation and install work there too. Since Task 004
# gives velox_ch_filecache a compiled source (StatusFile.cpp), the target is a
# real (non-INTERFACE) library in non-mono builds, and its "HEADERS" FILE_SET
# was already registered with PUBLIC scope by velox_add_library() in
# Common/CMakeLists.txt; a FILE_SET name must keep one consistent scope across
# all target_sources() calls on the same target, so this call must also use
# PUBLIC (not INTERFACE). Task 008 adds its public headers to the same set.
if(NOT VELOX_MONO_LIBRARY)
  target_sources(
    velox_ch_filecache
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${PROJECT_SOURCE_DIR}
    FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/Guards.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileSegmentKeyType.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheOriginInfo.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCache_fwd.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCache_fwd_internal.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheKey.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheUtils.h
      ${CMAKE_CURRENT_SOURCE_DIR}/ShardedMap.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheSettings.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheReadOptions.h
  )
endif()

# The center strongly connected component (SCC) of the FileCache implementation:
# the Task-011 priority/eviction sources plus the Task-012 center files. These
# nine translation units are mutually recursive (FileSegment <-> Metadata <->
# FileCache <-> priority/eviction) and must be compiled and linked together as a
# single unit; see port/task/012-filecache-core-scc.md "Why the Center SCC
# Cannot Be Split". In mono builds velox_add_library appends these sources to the
# velox target and aliases velox_ch_filecache_core to it.
velox_add_library(
  velox_ch_filecache_core
  IFileCachePriority.cpp
  LRUFileCachePriority.cpp
  SLRUFileCachePriority.cpp
  SplitFileCachePriority.cpp
  EvictionCandidates.cpp
  FileSegment.cpp
  Metadata.cpp
  FileCache.cpp
  QueryLimit.cpp
  HEADERS
    CacheUsage.h
    IFileCachePriority.h
    LRUFileCachePriority.h
    SLRUFileCachePriority.h
    SplitFileCachePriority.h
    EvictionCandidates.h
    FileSegmentInfo.h
    FileSegment.h
    Metadata.h
    FileCache.h
    QueryLimit.h
)

# In non-mono builds velox_ch_filecache_core is a real library; link its direct
# dependencies. In mono builds it is an alias to velox (which already links these)
# and target_link_libraries() on an alias is an error, so guard on the mode.
if(NOT VELOX_MONO_LIBRARY)
  target_link_libraries(
    velox_ch_filecache_core
    PUBLIC
      velox_ch_filecache
      velox_file
      velox_memory
      velox_test_util
      Folly::folly
      fmt::fmt
  )
endif()

# Task 013 Factory/Manager: the process-level runtime owner. FileCacheManager
# owns the shared worker pool, scheduler, and OpenedFileCache and contains one
# FileCacheFactory (the sole registry). In mono builds velox_add_library appends
# these sources to the velox target and aliases velox_ch_filecache_manager to it.
velox_add_library(
  velox_ch_filecache_manager
  FileCacheFactory.cpp
  FileCacheManager.cpp
  HEADERS
    FileCacheFactory.h
    FileCacheManager.h
    OpenedFileCache.h
)

# In non-mono builds velox_ch_filecache_manager is a real library; link its
# direct dependencies. In mono builds it is an alias to velox (which already
# links these) and target_link_libraries() on an alias is an error, so guard on
# the mode (mirrors velox_ch_filecache_core above).
if(NOT VELOX_MONO_LIBRARY)
  target_link_libraries(
    velox_ch_filecache_manager
    PUBLIC
      velox_ch_filecache_core
      velox_ch_filecache
      velox_file
      velox_memory
      Folly::folly
      fmt::fmt
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
