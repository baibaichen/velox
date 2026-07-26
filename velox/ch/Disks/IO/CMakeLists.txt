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

# The scan/DWIO read path (FileCacheBufferedInput + FileCacheInputStream).
#
# In the default mono build (VELOX_MONO_LIBRARY=ON) velox_ch_filecache is an
# ALIAS to the velox target, and a separate velox_ch_filecache_dwio library
# would double-define the center-SCC / Factory / Manager symbols these sources
# link against and break the link (the same ODR reason the SCC and Factory /
# Manager sources were compiled INTO velox_ch_filecache in Tasks 012/013).
# So the DWIO sources are registered into velox_ch_filecache via velox_sources(),
# which targets the real underlying library in both mono and non-mono builds.
velox_sources(
  velox_ch_filecache
  PRIVATE
    FileCacheBufferedInput.cpp
    FileCacheBufferedInputBuilder.cpp
    FileCacheCoalescedLoad.cpp
    FileCacheInputStream.cpp
)

# The connector integration (FileCacheBufferedInputBuilder) uses the Hive
# connector extension point BufferedInputBuilder / createBufferedInput, so link
# velox_hive_connector into the target that carries these sources.
velox_link_libraries(velox_ch_filecache PUBLIC velox_hive_connector)

# In non-mono builds velox_ch_filecache is a real library; register the public
# headers via FILE_SET so IDE navigation and install work there too. In mono
# builds the root include_directories(.) already makes the headers findable.
if(NOT VELOX_MONO_LIBRARY)
  target_sources(
    velox_ch_filecache
    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ${PROJECT_SOURCE_DIR}
    FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheRequestContext.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheFileIdentity.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheBufferedInput.h
      ${CMAKE_CURRENT_SOURCE_DIR}/FileCacheInputStream.h
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
