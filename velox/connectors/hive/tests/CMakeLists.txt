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
add_executable(
  velox_hive_connector_test
  FileColumnHandleTest.cpp
  FileConfigTest.cpp
  HiveIndexSourceUtilsTest.cpp
  FileConnectorSplitTest.cpp
  FileConnectorUtilTest.cpp
  FileHandleTest.cpp
  HiveConfigTest.cpp
  HiveDataSinkTest.cpp
  HiveConnectorTest.cpp
  HiveConnectorUtilTest.cpp
  HiveConnectorSerDeTest.cpp
  HivePartitionFunctionTest.cpp
  HivePartitionNameTest.cpp
  HiveSplitTest.cpp
  PartitionIdGeneratorTest.cpp
  TableHandleTest.cpp
)
add_test(velox_hive_connector_test velox_hive_connector_test)

target_link_libraries(
  velox_hive_connector_test
  velox_hive_connector
  velox_hive_partition_function
  velox_dwio_common_exception
  velox_dwio_orc_reader
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_exec
  velox_exec_test_lib
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)

if(VELOX_ENABLE_PARQUET)
  target_include_directories(velox_hive_connector_test PUBLIC ${ARROW_PREFIX}/install/include)
  target_link_libraries(
    velox_hive_connector_test
    velox_dwio_parquet_writer
    velox_dwio_parquet_reader
  )
endif()

# Dedicated executable (Task 018 Hive FCBI adapter): selection, mapping,
# mutual exclusion, and real miss-fill-hit tests for the current FileCache
# adapter in `createBufferedInput`. Kept separate from the monolithic
# `velox_hive_connector_test` so this focused suite builds/links/runs without
# compiling the rest of the (much larger) Hive connector test set, in both
# mono and non-mono trees.
add_executable(
  velox_hive_filecache_buffered_input_test
  HiveFileCacheBufferedInputTest.cpp
)
add_test(
  velox_hive_filecache_buffered_input_test
  velox_hive_filecache_buffered_input_test
)

target_link_libraries(
  velox_hive_filecache_buffered_input_test
  PRIVATE
    velox_hive_connector
    velox_connector
    velox_dwio_common
    velox_dwio_common_exception
    velox_ch_filecache_dwio
    velox_ch_filecache_manager
    velox_ch_filecache_core
    velox_ch_filecache
    velox_caching
    velox_file
    velox_memory
    velox_exception
    velox_test_util
    Folly::folly
    GTest::gtest
    GTest::gtest_main
)
