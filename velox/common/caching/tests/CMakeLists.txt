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

add_executable(simple_lru_cache_test SimpleLRUCacheTest.cpp)
add_test(simple_lru_cache_test simple_lru_cache_test)
target_link_libraries(
  simple_lru_cache_test
  PRIVATE velox_common_base Folly::folly velox_time glog::glog GTest::gtest GTest::gtest_main
)

# Split velox_cache_test into individual test binaries for parallel execution.
set(
  VELOX_CACHE_TEST_SOURCES
  AsyncDataCacheTest.cpp
  CacheTTLControllerTest.cpp
  SsdFileTest.cpp
  SsdFileTrackerTest.cpp
  StringIdMapTest.cpp
)

set(
  VELOX_CACHE_TEST_DEPS
  velox_caching
  velox_file
  velox_file_test_utils
  velox_memory
  velox_test_util
  velox_flag_definitions
  Folly::folly
  glog::glog
  GTest::gtest
  GTest::gtest_main
)

velox_add_grouped_tests(
  PREFIX velox_cache_test
  SOURCES ${VELOX_CACHE_TEST_SOURCES}
  DEPS ${VELOX_CACHE_TEST_DEPS}
)

add_executable(cached_factory_test CachedFactoryTest.cpp)
add_test(cached_factory_test cached_factory_test)
target_link_libraries(
  cached_factory_test
  PRIVATE velox_common_base Folly::folly velox_time glog::glog GTest::gtest GTest::gtest_main
)

velox_add_library(velox_caching_test_util INTERFACE HEADERS CacheTestUtil.h)

add_executable(memory_cache_test MemoryCacheTest.cpp)
target_link_libraries(
  memory_cache_test
  PRIVATE velox_memory_cache Boost::unordered GTest::gtest GTest::gtest_main
)
add_test(NAME memory_cache_test COMMAND memory_cache_test)

add_executable(memory_cache_contract_test MemoryCacheInfrastructureTest.cpp)
target_link_libraries(
  memory_cache_contract_test
  PRIVATE Folly::folly Boost::unordered GTest::gtest GTest::gtest_main
)
add_test(NAME memory_cache_contract_test COMMAND memory_cache_contract_test)

add_executable(memory_cache_no_boost_test MemoryCacheNoBoostTest.cpp)
target_link_libraries(memory_cache_no_boost_test PRIVATE velox_memory_cache)
add_test(NAME memory_cache_no_boost_test COMMAND memory_cache_no_boost_test)

set(MEMORY_CACHE_SANITIZERS "" CACHE STRING "Sanitizers for MemoryCache template tests")
if(MEMORY_CACHE_SANITIZERS)
  foreach(target memory_cache_test memory_cache_contract_test memory_cache_no_boost_test)
    target_compile_options(
      ${target}
      PRIVATE -fsanitize=${MEMORY_CACHE_SANITIZERS} -fno-omit-frame-pointer
    )
    target_link_options(${target} PRIVATE -fsanitize=${MEMORY_CACHE_SANITIZERS})
  endforeach()
endif()

option(MEMORY_CACHE_TSAN_FOLLY "Instrument bundled Folly for MemoryCache TSAN" OFF)
if(MEMORY_CACHE_TSAN_FOLLY)
  if(NOT MEMORY_CACHE_SANITIZERS STREQUAL "thread")
    message(FATAL_ERROR "MEMORY_CACHE_TSAN_FOLLY requires MEMORY_CACHE_SANITIZERS=thread")
  endif()
  function(memory_cache_instrument_folly directory)
    get_property(targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(target IN LISTS targets)
      get_target_property(type ${target} TYPE)
      if(type STREQUAL "OBJECT_LIBRARY" OR type STREQUAL "STATIC_LIBRARY")
        target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
      endif()
    endforeach()
    get_property(children DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
    foreach(child IN LISTS children)
      memory_cache_instrument_folly("${child}")
    endforeach()
  endfunction()
  memory_cache_instrument_folly("${folly_SOURCE_DIR}")
endif()
