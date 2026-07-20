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

add_executable(velox_ch_common_test BasicShimsTest.cpp)
add_test(velox_ch_common_test velox_ch_common_test)

target_link_libraries(
  velox_ch_common_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    velox_exception
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

add_executable(velox_ch_threadpool_test ThreadPoolTest.cpp)
add_test(velox_ch_threadpool_test velox_ch_threadpool_test)

target_link_libraries(
  velox_ch_threadpool_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    velox_exception
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

add_executable(velox_ch_scheduler_test SchedulerAndScopeTest.cpp)
add_test(velox_ch_scheduler_test velox_ch_scheduler_test)

target_link_libraries(
  velox_ch_scheduler_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    velox_exception
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)

# Plain executable (no gtest): the process exit status is the proof. It is
# compiled with NDEBUG only, so ClickHouseAssert.h must take the ordinary
# Release path and evaluate neither the asserted expression nor the
# diagnostic message. A zero exit status is the passing result.
add_executable(velox_ch_chassert_release_probe ChassertReleaseProbe.cpp)
add_test(velox_ch_chassert_release_probe velox_ch_chassert_release_probe)

target_compile_definitions(velox_ch_chassert_release_probe PRIVATE NDEBUG)

target_link_libraries(
  velox_ch_chassert_release_probe
  PRIVATE
    velox_ch_filecache
    Folly::folly
)

# Compiled with NDEBUG and FOLLY_SANITIZE=1 to simulate a sanitizer build:
# chassert() must still abort, unlike ordinary Release.
add_executable(velox_ch_chassert_sanitizer_gate_test ChassertSanitizerGateTest.cpp)
add_test(velox_ch_chassert_sanitizer_gate_test velox_ch_chassert_sanitizer_gate_test)

target_compile_definitions(
  velox_ch_chassert_sanitizer_gate_test
  PRIVATE
    NDEBUG
    FOLLY_SANITIZE=1
)

target_link_libraries(
  velox_ch_chassert_sanitizer_gate_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    Folly::folly
    GTest::gtest
    GTest::gtest_main
)

add_executable(velox_ch_observability_test ObservabilityTest.cpp)
add_test(velox_ch_observability_test velox_ch_observability_test)

target_link_libraries(
  velox_ch_observability_test
  PRIVATE
    velox_ch_filecache
    velox_test_util
    velox_exception
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
)
