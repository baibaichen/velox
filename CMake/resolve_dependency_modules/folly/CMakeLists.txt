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
project(Folly)
cmake_minimum_required(VERSION 3.28)

velox_set_source(FastFloat)
velox_resolve_dependency(FastFloat CONFIG REQUIRED)

set(VELOX_FOLLY_BUILD_VERSION v2026.09.07.00)
set(
  VELOX_FOLLY_BUILD_SHA256_CHECKSUM
  f114713009738faa49961aeaa050592a09a4c8a7b46ba9a0780d4aaba468173b
)
set(
  VELOX_FOLLY_SOURCE_URL
  "https://github.com/facebook/folly/releases/download/${VELOX_FOLLY_BUILD_VERSION}/folly-${VELOX_FOLLY_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(FOLLY)

message(STATUS "Building Folly from source")

if(gflags_SOURCE STREQUAL "BUNDLED")
  # FetchContent redirects FindGflags without populating its legacy variables.
  set(LIBGFLAGS_FOUND TRUE)
  set(LIBGFLAGS_LIBRARY gflags::gflags)
  set(LIBGFLAGS_INCLUDE_DIR ${gflags_BINARY_DIR}/include)
  # Avoid probing the namespace by linking against gflags before it is built.
  set(FOLLY_UNUSUAL_GFLAGS_NAMESPACE OFF)
  set(FOLLY_GFLAGS_NAMESPACE gflags)
endif()

FetchContent_Declare(
  folly
  URL ${VELOX_FOLLY_SOURCE_URL}
  URL_HASH ${VELOX_FOLLY_BUILD_SHA256_CHECKSUM}
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/folly-no-export.patch
  OVERRIDE_FIND_PACKAGE
  SYSTEM
  EXCLUDE_FROM_ALL
)

set(BUILD_SHARED_LIBS ${VELOX_BUILD_SHARED})

# Enable INT128 support
set(FOLLY_HAVE_INT128_T ON)

FetchContent_MakeAvailable(folly)

if(gflags_SOURCE STREQUAL "BUNDLED")
  add_dependencies(folly glog::glog gflags::gflags fmt::fmt)
endif()
