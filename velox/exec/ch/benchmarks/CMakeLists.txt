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
add_executable(
  velox_exec_ch_hashtable_layer_benchmark ChHashTableLayerBenchmark.cpp)

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

target_link_libraries(
  velox_exec_ch_hashtable_layer_benchmark
  velox_exec_ch
  velox_exec
  velox_vector_test_lib
  Folly::follybenchmark
  duckdb_static
)

# O5 multi-machine A/B micro-bench: SSSE3 packFixedShuffle vs scalar packFixed,
# same binary switches both paths (moved from ch in task9c-2).
add_executable(
  velox_exec_ch_keysfixed_ssse3_benchmark ChKeysFixedSsse3Benchmark.cpp)

target_link_libraries(
  velox_exec_ch_keysfixed_ssse3_benchmark
  velox_exec_ch
  velox_vector
  velox_vector_test_lib
  Folly::follybenchmark)
