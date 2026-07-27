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

add_executable(velox_cache_read_harness_test CacheReadHarnessTest.cpp)
add_test(velox_cache_read_harness_test velox_cache_read_harness_test)
target_link_libraries(
  velox_cache_read_harness_test
  PRIVATE
    velox_cache_read_harness
    velox_exception
    GTest::gtest
    GTest::gtest_main)

add_executable(velox_cache_verify_test CacheVerifyTest.cpp)
add_test(velox_cache_verify_test velox_cache_verify_test)
target_link_libraries(
  velox_cache_verify_test
  PRIVATE
    velox_cache_read_harness
    velox_exception
    GTest::gtest
    GTest::gtest_main)

add_executable(velox_buffered_input_trace_replay_test
               BufferedInputTraceReplayTest.cpp)
add_test(velox_buffered_input_trace_replay_test
         velox_buffered_input_trace_replay_test)
target_link_libraries(
  velox_buffered_input_trace_replay_test
  PRIVATE
    velox_buffered_input_trace_replay_lib
    velox_dwio_common
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
    GTest::gtest_main)
