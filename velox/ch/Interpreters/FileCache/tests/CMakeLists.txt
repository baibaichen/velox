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

add_executable(velox_ch_guards_test StatusFileAndGuardsTest.cpp)
add_test(velox_ch_guards_test velox_ch_guards_test)

target_include_directories(
  velox_ch_guards_test
  PRIVATE
    ${PROJECT_BINARY_DIR}/velox/ch/Common
)

target_link_libraries(
  velox_ch_guards_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    velox_exception
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

add_executable(velox_ch_leaf_types_test LeafTypesTest.cpp)
add_test(velox_ch_leaf_types_test velox_ch_leaf_types_test)

target_link_libraries(
  velox_ch_leaf_types_test
  PRIVATE
    velox_ch_filecache
    GTest::gtest
    GTest::gtest_main
)

# ShardedMap.h is a public header of velox_ch_filecache. Link this focused test
# directly only to velox_ch_filecache and GTest so a missing public-interface
# dependency (Folly, fmt, velox_exception) surfaces here instead of being masked
# by direct links; the velox_ch_filecache PUBLIC interface must propagate them.
add_executable(velox_ch_sharded_map_test ShardedMapTest.cpp)
add_test(velox_ch_sharded_map_test velox_ch_sharded_map_test)

target_link_libraries(
  velox_ch_sharded_map_test
  PRIVATE
    velox_ch_filecache
    GTest::gtest
    GTest::gtest_main
)

# FileCacheSettings.h/.cpp and FileCacheReadOptions.h are part of
# velox_ch_filecache. FileCacheSettings.h publicly includes
# velox/common/config/Config.h, so velox_ch_filecache must PUBLIC-propagate
# velox_common_config (and its exception/fmt/Folly deps). Link this focused test
# directly only to velox_ch_filecache and GTest so any missing public-interface
# dependency surfaces here instead of being masked by direct links.
add_executable(velox_ch_settings_test FileCacheSettingsTest.cpp)
add_test(velox_ch_settings_test velox_ch_settings_test)

target_link_libraries(
  velox_ch_settings_test
  PRIVATE
    velox_ch_filecache
    GTest::gtest
    GTest::gtest_main
)

# Center-SCC (Task 012) test: exercises the compiled velox_ch_filecache_core
# library (priority/eviction + FileSegment + Metadata + FileCache + QueryLimit)
# through its production constructors and real temporary files.
add_executable(
  velox_ch_filecache_core_scc_test
  PriorityEvictionTest.cpp
  FileSegmentInfoTest.cpp
  FileSegmentTest.cpp
  MetadataTest.cpp
  FileCacheTest.cpp
  QueryLimitTest.cpp
)
add_test(velox_ch_filecache_core_scc_test velox_ch_filecache_core_scc_test)

target_link_libraries(
  velox_ch_filecache_core_scc_test
  PRIVATE
    velox_ch_filecache_core
    velox_ch_filecache
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)
