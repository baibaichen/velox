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
  velox_ch_filecache
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

# In non-mono builds velox_ch_filecache is a real library; link its direct deps.
# FileCacheKey.cpp uses fmt::format and the VELOX_FAIL-based exception helper, and
# the public headers include fmt and Folly F14 containers, so carry all three as
# PUBLIC to propagate their compile usage requirements to consumers of the headers.
if(NOT VELOX_MONO_LIBRARY)
  target_link_libraries(
    velox_ch_filecache
    PUBLIC
      velox_exception
      Folly::folly
      fmt::fmt
  )
endif()

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
