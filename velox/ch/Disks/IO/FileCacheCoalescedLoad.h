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

#include "velox/ch/Disks/IO/FileCacheInputStream.h"
#include "velox/ch/Interpreters/FileCache/FileCache.h"
#include "velox/ch/Interpreters/FileCache/FileSegment.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/file/Region.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace facebook::velox::ch
{

/// Load-private request model for a single planned chunk of a coalesced group.
/// `requestIndex` is the stable business request id (stable across the whole
/// input, not a 0..N-1 counter within the load). Not part of any public load
/// API and never leaks the `PlanChunk` type.
struct FileCacheLoadRequest
{
    size_t requestIndex;
    velox::common::Region region;
    velox::cache::TrackingId trackingId;
    std::vector<FileCachePreparedBuffer> buffers;
    bool ready{false};
    bool consumed{false};
};

/// Executes the group read for a coalesced set of miss chunks by reusing the
/// internal `FileCacheInputStream` model. Data lands in the CH `FileSegment`s
/// and in per-request RAM buffers; unlike a standard Velox `CoalescedLoad`, it
/// makes no cache entries (`loadData` returns `{}`) and delivers `BufferPtr`s
/// through `getData`.
class FileCacheCoalescedLoad final : public cache::CoalescedLoad
{
public:
    struct Context
    {
        std::shared_ptr<const FileCacheReadContext> readContext;
        FileCache::QueryContextHolderPtr queryContextHolder;
    };

    FileCacheCoalescedLoad(
        Context context,
        uint64_t groupOffset,
        uint64_t groupLength,
        std::vector<FileCacheLoadRequest> requests);

    std::vector<cache::CachePin> loadData(bool prefetch) override;

    bool isSsdLoad() const override { return false; }

    int64_t size() const override;

    /// Under `requestMutex_`, all-or-nothing ownership move of the prepared
    /// buffers for the requests whose `requestIndex` is in `requestIndices`.
    /// Returns `std::nullopt` when the requests are not all ready or already
    /// consumed. The signature is an intentional deviation from the standard
    /// `DirectCoalescedLoad::getData` (see design 12.4.2 / task 020).
    std::optional<std::vector<FileCachePreparedBuffer>> getData(const std::vector<size_t> & requestIndices);

private:
    Context context_;
    mutable std::mutex requestMutex_;
    FileSegmentsHolderPtr groupSegments_;
    std::vector<FileCacheLoadRequest> requests_;
    uint64_t groupOffset_;
    uint64_t groupLength_;
};

} // namespace facebook::velox::ch
