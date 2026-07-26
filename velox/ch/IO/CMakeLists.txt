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

# Add the IO adapter sources to velox_ch_filecache. velox_sources() routes to
# the real target in both build modes: it appends to `velox` under
# VELOX_MONO_LIBRARY=ON (where velox_ch_filecache is an ALIAS to `velox`, and
# target_sources() cannot be called on an alias) and to velox_ch_filecache
# directly in non-mono builds.
velox_sources(
  velox_ch_filecache
  PRIVATE
    ReadBufferFromVeloxReadFile.cpp
    WriteBufferFromVeloxWriteFile.cpp
    FileCacheLocalWriteFile.cpp
)

# In non-mono builds velox_ch_filecache is a real library; register the public
# headers via FILE_SET so IDE navigation and install work there too. In mono
# builds the target is an ALIAS to `velox`, target_sources() cannot modify it,
# and the root include_directories(.) already makes the headers findable.
if(NOT VELOX_MONO_LIBRARY)
  target_sources(
    velox_ch_filecache
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${PROJECT_SOURCE_DIR}
    FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/ReadBufferFromVeloxReadFile.h
      ${CMAKE_CURRENT_SOURCE_DIR}/WriteBufferFromVeloxWriteFile.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheLocalWriteFile.h
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
