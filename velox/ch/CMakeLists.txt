add_subdirectory(Common)
add_subdirectory(Interpreters)
add_subdirectory(IO)
add_subdirectory(Disks)

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
