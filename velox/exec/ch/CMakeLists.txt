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
  Common/Arena.cpp
  ChHashBuild.cpp
  ChHashBuildOperator.cpp
  ChHashJoinBridge.cpp
  ChHashProbe.cpp
  ChHashProbeOperator.cpp
  EmitGather.cpp
  Common/HashTable/Prefetching.cpp
  RetainedVectorsIndex.cpp
  Interpreters/RowRefList.cpp
  HEADERS
    Common/Arena.h
    ChHashBuild.h
    ChHashBuildOperator.h
    ChHashJoinBridge.h
    ChHashJoinNode.h
    ChHashProbe.h
    ChHashProbeOperator.h
    EmitGather.h
    Common/HashTable/HashMap.h
    Common/ColumnsHashing/FixedKey.h
    Common/HashTable/HashTable.h
    Interpreters/HashJoin/FixedKeyMap.h
    Common/ColumnsHashing/SerializedKey.h
    Common/HashTable/HashTableAllocatorAdapter.h
    Common/HashTable/Prefetching.h
    RetainedVectorsIndex.h
    Interpreters/RowRef.h
    Interpreters/RowRefList.h
    Common/SipHash.h
    Common/ColumnsHashing/ColumnsHashingImpl.h
    Common/ColumnsHashing/HashMethod.h
    Common/HashTable/HashTableKeyHolder.h
    Common/HashTable/StringHashMapAdapter.h
    DataTypes/FixedStringType.h
    Interpreters/AggregationCommon.h
    Interpreters/HashJoin/ChHashRoute.h
    Interpreters/HashJoin/ChHashMethodDispatch.h
    Interpreters/HashJoin/ChHashJoinCh2Pipeline.h
    Interpreters/HashJoin/LowCardinalityKeyGetterForJoin.h
)
velox_link_libraries(velox_exec_ch PUBLIC velox_exec velox_memory velox_vector)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

velox_install_library_headers()
