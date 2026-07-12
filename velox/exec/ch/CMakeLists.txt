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

velox_add_library(
  velox_exec_ch
  Arena.cpp
  ChHashBuild.cpp
  ChHashJoinBridge.cpp
  ChHashProbe.cpp
  EmitGather.cpp
  RetainedVectorsIndex.cpp
  RowRefList.cpp
  HEADERS
    Arena.h
    ChHashBuild.h
    ChHashBuildOperator.h
    ChHashJoinBridge.h
    ChHashJoinNode.h
    ChHashProbe.h
    ChHashProbeOperator.h
    EmitGather.h
    HashMap.h
    HashTable.h
    HashTableAllocatorAdapter.h
    RetainedVectorsIndex.h
    RowRef.h
    RowRefList.h
)
velox_link_libraries(velox_exec_ch PUBLIC velox_exec velox_memory velox_vector)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

velox_install_library_headers()
