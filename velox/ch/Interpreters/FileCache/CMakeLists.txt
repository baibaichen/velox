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
  # Center-SCC sources (Task 012 sub-attempts S2/S3). Registered here so they are
  # compiled as part of velox_ch_filecache. The dedicated velox_ch_filecache_core
  # library and the SCC test target are added in S4; a full link is NOT claimed by
  # S3 (the SCC has no intermediate link step).
  IFileCachePriority.cpp
  LRUFileCachePriority.cpp
  SLRUFileCachePriority.cpp
  SplitFileCachePriority.cpp
  EvictionCandidates.cpp
  FileSegment.cpp
  Metadata.cpp
  FileCache.cpp
  QueryLimit.cpp
  # Task 013 Factory + Manager. Registered into velox_ch_filecache (the mono target)
  # for the same reason as the center-SCC sources: a separate library would
  # double-define the SCC symbols they link against and break the link (ODR). The
  # Task-013 test links velox_ch_filecache once.
  FileCacheFactory.cpp
  FileCacheManager.cpp
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
      ${CMAKE_CURRENT_SOURCE_DIR}/OpenedFileCache.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheFactory.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheManager.h
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
