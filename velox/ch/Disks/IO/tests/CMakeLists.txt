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

add_executable(velox_ch_filecache_buffered_input_test FileCacheBufferedInputTest.cpp)
add_test(velox_ch_filecache_buffered_input_test velox_ch_filecache_buffered_input_test)

target_link_libraries(
  velox_ch_filecache_buffered_input_test
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

# Task 015: Velox-only end-to-end validation of the assembled public read path
# (FileCacheBufferedInput -> FileCacheInputStream -> FileCache) driven through a
# real FileCacheManager. Links the mono velox_ch_filecache target (which contains
# the center SCC, Factory/Manager, and DWIO sources) once; a separate
# *_dwio/_manager/_core library set does not exist under VELOX_MONO_LIBRARY.
add_executable(velox_ch_filecache_e2e_test FileCacheE2ETest.cpp)
add_test(velox_ch_filecache_e2e_test velox_ch_filecache_e2e_test)

target_link_libraries(
  velox_ch_filecache_e2e_test
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

# Task 017: cancellation wiring for FileCacheInputStream::Next. Drives the
# assembled read path through a real FileCacheManager (same fixture style as
# the E2E test) with a folly::CancellationSource and asserts the throw points.
add_executable(velox_ch_cancellation_test FileCacheCancellationTest.cpp)
add_test(velox_ch_cancellation_test velox_ch_cancellation_test)

target_link_libraries(
  velox_ch_cancellation_test
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

# Task 017 post-acceptance amendment 1: prove the three global ProfileEvents
# hit/source/write byte counters are driven from the assembled read path.
add_executable(velox_ch_filecache_hit_metrics_test FileCacheHitMetricsTest.cpp)
add_test(velox_ch_filecache_hit_metrics_test velox_ch_filecache_hit_metrics_test)

target_link_libraries(
  velox_ch_filecache_hit_metrics_test
  PRIVATE
    velox_ch_filecache
    velox_dwio_common
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

# Task 018a: connector integration — FileCacheBufferedInputBuilder routes the
# Hive connector read path through the FileCache. Drives the real
# BufferedInputBuilder::create(...) extension point (real FileHandle,
# ConnectorQueryCtx, and read) and asserts FileCache vs native selection.
add_executable(
  velox_ch_filecache_connector_test
  FileCacheBufferedInputBuilderTest.cpp)
add_test(velox_ch_filecache_connector_test velox_ch_filecache_connector_test)

target_link_libraries(
  velox_ch_filecache_connector_test
  PRIVATE
    velox_ch_filecache
    velox_hive_connector
    velox_dwio_common
    velox_test_util
    velox_exception
    velox_file
    velox_memory
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)
