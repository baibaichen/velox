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

add_executable(velox_exec_ch_hash_join_benchmark ChHashJoinBenchmark.cpp)

find_package(DuckDB CONFIG REQUIRED)
if(NOT TARGET duckdb_static)
  include("${DuckDB_DIR}/DuckDBExports.cmake")
endif()

get_target_property(
  CH_BENCHMARK_DUCKDB_STATIC
  duckdb_static
  IMPORTED_LOCATION_RELEASE
)
get_filename_component(
  CH_BENCHMARK_DUCKDB_LIB_DIR "${CH_BENCHMARK_DUCKDB_STATIC}" DIRECTORY
)
target_link_directories(
  velox_exec_ch_hash_join_benchmark
  PRIVATE "${CH_BENCHMARK_DUCKDB_LIB_DIR}"
)

target_link_libraries(
  velox_exec_ch_hash_join_benchmark
  velox_exec_ch
  velox_exec
  velox_cursor
  velox_functions_prestosql
  velox_parse_parser
  velox_parse_utils
  velox_vector_test_lib
  Folly::follybenchmark
  duckdb_static
)
