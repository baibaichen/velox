/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/ch/Disks/IO/FileCacheBufferedInputBuilder.h"

#include "velox/ch/Disks/IO/FileCacheBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheRequestContext.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"

namespace facebook::velox::ch
{

std::unique_ptr<dwio::common::BufferedInput> FileCacheBufferedInputBuilder::create(
    const FileHandle & fileHandle,
    const dwio::common::ReaderOptions & readerOpts,
    const connector::ConnectorQueryCtx * connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStatistics,
    std::shared_ptr<IoStats> ioStats,
    folly::Executor * executor,
    const folly::F14FastMap<std::string, std::string> & fileReadOps)
{
    // Mutual-exclusion guard: the native AsyncDataCache and the FileCache must not
    // both be active for the same read, or the data would be double-cached. This
    // mirrors the native selection which keys off connectorQueryCtx->cache().
    VELOX_CHECK_NULL(
        connectorQueryCtx->cache(),
        "FileCache and AsyncDataCache cannot both be installed");

    // Resolve the default cache per call. Install-time validation
    // (registerFileCacheBufferedInputBuilder) already proved the Manager is
    // FileCache-configured, so getDefault() cannot throw here for "no default".
    FileCachePtr cache = manager_.getDefault();

    FileCacheRequestContext requestContext;
    requestContext.queryId = connectorQueryCtx->queryId();
    requestContext.userId = manager_.commonUserId();

    return std::make_unique<FileCacheBufferedInput>(
        fileHandle.file,
        cache,
        FileCacheKey::fromPath(fileHandle.file->getName()),
        cache->getCommonOrigin(),
        FileCacheReadOptions{},
        requestContext,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid,
        fileHandle.groupId,
        connector::Connector::getTracker(connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        std::move(ioStatistics),
        std::move(ioStats),
        executor,
        readerOpts,
        fileReadOps);
}

void registerFileCacheBufferedInputBuilder(FileCacheManager & manager)
{
    // Fail-fast install-time validation: calling this function declares FileCache
    // is being installed, so a Manager without a default cache is a configuration
    // error. Use the non-throwing hasDefault() predicate (Task 013) rather than
    // the throwing getDefault() accessor as a validator.
    VELOX_CHECK(
        manager.hasDefault(),
        "registerFileCacheBufferedInputBuilder: FileCacheManager has no default cache configured");

    connector::hive::BufferedInputBuilder::registerBuilder(
        std::make_shared<FileCacheBufferedInputBuilder>(manager));
}

} // namespace facebook::velox::ch
