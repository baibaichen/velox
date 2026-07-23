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

add_executable(velox_ab_benchmark_schema_test AbBenchmarkSchemaTest.cpp)

target_link_libraries(
  velox_ab_benchmark_schema_test
  velox_benchmark_ab
  GTest::gtest
  GTest::gtest_main)

add_test(NAME velox_ab_benchmark_schema_test COMMAND velox_ab_benchmark_schema_test)
