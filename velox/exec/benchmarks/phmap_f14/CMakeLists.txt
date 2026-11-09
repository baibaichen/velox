cmake_minimum_required(VERSION 3.28)
project(UniqueValueSetBenchmark LANGUAGES CXX)

# Standalone experiment: phmap is not a Velox production dependency.
set(PHMAP_ROOT "" CACHE PATH "Checkout of greg7mdp/parallel-hashmap")
if(NOT EXISTS "${PHMAP_ROOT}/parallel_hashmap/phmap.h")
  message(FATAL_ERROR "Set PHMAP_ROOT to the pinned phmap checkout")
endif()
find_package(Threads REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)
find_path(XSIMD_INCLUDE_DIR xsimd/xsimd.hpp REQUIRED)
get_filename_component(VELOX_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../.." ABSOLUTE)

add_executable(unique_value_set_benchmark UniqueValueSetBenchmark.cpp)
target_compile_features(unique_value_set_benchmark PRIVATE cxx_std_20)
target_include_directories(unique_value_set_benchmark PRIVATE
  "${VELOX_ROOT}" "${PHMAP_ROOT}" "${XSIMD_INCLUDE_DIR}"
  "${VELOX_ROOT}/velox/external/xxhash")
target_link_libraries(unique_value_set_benchmark PRIVATE Folly::follybenchmark Folly::folly)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
  target_compile_options(unique_value_set_benchmark PRIVATE
    -mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2)
endif()
target_compile_options(unique_value_set_benchmark PRIVATE -Wall -Wextra -Wno-unused-parameter)
