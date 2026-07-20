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
#pragma once

#include "velox/connectors/hive/BufferedInputBuilder.h"

namespace facebook::velox::ch
{

class FileCacheManager;

/// A `BufferedInputBuilder` that routes the Hive connector read path through the
/// ClickHouse `FileCache` (`FileCacheBufferedInput` -> `FileCacheInputStream` ->
/// `FileCache`) instead of the native `createBufferedInput` selection.
///
/// Registered via `registerFileCacheBufferedInputBuilder` (below). Deployments
/// that do NOT install FileCache simply never call that function and keep the
/// statically-registered `DefaultBufferInputBuilder`, which reads through the
/// native path. That registration boundary IS the fallback: this builder has no
/// internal "FileCache not installed" branch.
///
/// Ownership: the builder holds a `FileCacheManager&` captured at construction.
/// The Manager MUST outlive the builder. The default cache is resolved per call
/// in `create` via `manager_.getDefault()`; it is never cached in the builder.
class FileCacheBufferedInputBuilder final : public connector::hive::BufferedInputBuilder
{
public:
    /// `manager` must be FileCache-configured (a default cache resolvable via
    /// `getDefault`) and must outlive this builder.
    explicit FileCacheBufferedInputBuilder(FileCacheManager & manager) : manager_(manager) { }

    std::unique_ptr<dwio::common::BufferedInput> create(
        const FileHandle & fileHandle,
        const dwio::common::ReaderOptions & readerOpts,
        const connector::ConnectorQueryCtx * connectorQueryCtx,
        std::shared_ptr<io::IoStatistics> ioStatistics,
        std::shared_ptr<IoStats> ioStats,
        folly::Executor * executor,
        const folly::F14FastMap<std::string, std::string> & fileReadOps = {}) override;

private:
    FileCacheManager & manager_;
};

/// Install `FileCacheBufferedInputBuilder` as the process-wide connector buffered
/// input builder.
///
/// 1. Calls `manager.getDefault()` once to validate the Manager is configured for
///    FileCache. If it throws (no default cache configured), the throw
///    propagates: calling this function DECLARES "I am installing FileCache", so a
///    Manager without a default cache is a caller configuration error that must
///    surface immediately. It is NOT caught.
/// 2. Registers the builder via `BufferedInputBuilder::registerBuilder`.
///
/// `manager` MUST outlive the registered builder (i.e. until either the process
/// ends or another builder is registered).
void registerFileCacheBufferedInputBuilder(FileCacheManager & manager);

} // namespace facebook::velox::ch
