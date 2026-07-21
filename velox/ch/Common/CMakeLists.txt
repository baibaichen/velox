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

set(
  VELOX_BUILD_REVISION
  ""
  CACHE STRING
  "Full Velox source revision embedded in StatusFile diagnostics"
)

set(VELOX_BUILD_REVISION_VALUE "${VELOX_BUILD_REVISION}")
string(STRIP "${VELOX_BUILD_REVISION_VALUE}" VELOX_BUILD_REVISION_VALUE)

if(VELOX_BUILD_REVISION_VALUE STREQUAL "")
  find_package(Git QUIET)
  if(NOT Git_FOUND OR NOT GIT_EXECUTABLE)
    message(
      FATAL_ERROR
      "Cannot determine the full Velox source revision. Configure with -DVELOX_BUILD_REVISION=<full-source-revision>."
    )
  endif()

  execute_process(
    COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    RESULT_VARIABLE VELOX_BUILD_REVISION_GIT_RESULT
    OUTPUT_VARIABLE VELOX_BUILD_REVISION_VALUE
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )

  if(
    NOT VELOX_BUILD_REVISION_GIT_RESULT EQUAL 0
    OR VELOX_BUILD_REVISION_VALUE STREQUAL ""
  )
    message(
      FATAL_ERROR
      "Cannot determine the full Velox source revision. Configure with -DVELOX_BUILD_REVISION=<full-source-revision>."
    )
  endif()
endif()

string(LENGTH "${VELOX_BUILD_REVISION_VALUE}" VELOX_BUILD_REVISION_LENGTH)
if(
  NOT "${VELOX_BUILD_REVISION_VALUE}" MATCHES "^[0-9A-Fa-f]+$"
  OR (
    NOT VELOX_BUILD_REVISION_LENGTH EQUAL 40
    AND NOT VELOX_BUILD_REVISION_LENGTH EQUAL 64
  )
)
  message(
    FATAL_ERROR
    "VELOX_BUILD_REVISION must be a full source revision containing exactly 40 or 64 hexadecimal characters."
  )
endif()

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/VeloxBuildRevision.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/VeloxBuildRevision.h
  @ONLY
)

velox_add_library(
  velox_ch_filecache
  CurrentMetrics.cpp
  ProfileEvents.cpp
  FileCacheStats.cpp
  StatusFile.cpp
  ThreadPool.cpp
  FileCacheQueryIdScope.cpp
  FileCacheScheduler.cpp
  SipHash128.cpp
  HEADERS
    ClickHouseAliases.h
    ClickHouseAssert.h
    CurrentMetrics.h
    FailPoint.h
    FileCacheBoundedQueue.h
    FileCacheException.h
    FileCacheFilesystem.h
    FileCacheQueryIdScope.h
    FileCacheScheduler.h
    FileCacheStats.h
    FilesystemCacheLog.h
    logger_useful.h
    OpenTelemetryTraceContext.h
    ProfileEvents.h
    QueryStatus.h
    SharedMutex.h
    SipHash128.h
    StatusFile.h
    ThreadPool.h
)

velox_include_directories(
  velox_ch_filecache
  PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

# In non-mono builds velox_ch_filecache is a real library; link its direct deps.
# FileCacheKey.cpp uses fmt::format and the VELOX_FAIL-based exception helper, and
# the public headers include fmt and Folly F14 containers, so carry all three as
# PUBLIC to propagate their compile usage requirements to consumers of the headers.
# FileCacheSettings.h publicly includes velox/common/config/Config.h, so
# velox_common_config is likewise a PUBLIC dependency.
if(NOT VELOX_MONO_LIBRARY)
  target_link_libraries(
    velox_ch_filecache
    PUBLIC
      velox_common_config
      velox_exception
      Folly::folly
      fmt::fmt
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
