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
