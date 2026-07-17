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

# Guards.h is a header-only addition to velox_ch_filecache.
# With VELOX_MONO_LIBRARY=ON the global include_directories(.) from the root
# CMakeLists already makes it findable; we register the FILE_SET only in
# non-mono builds so IDE navigation and install work there too. Since Task 004
# gives velox_ch_filecache a compiled source (StatusFile.cpp), the target is a
# real (non-INTERFACE) library in non-mono builds, and its "HEADERS" FILE_SET
# was already registered with PUBLIC scope by velox_add_library() in
# Common/CMakeLists.txt; a FILE_SET name must keep one consistent scope across
# all target_sources() calls on the same target, so this call must also use
# PUBLIC (not INTERFACE).
if(NOT VELOX_MONO_LIBRARY)
  target_sources(
    velox_ch_filecache
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${PROJECT_SOURCE_DIR}
    FILES ${CMAKE_CURRENT_SOURCE_DIR}/Guards.h
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
