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
#include "velox/common/caching/filecache/IFileCachePriority.h"
#include "velox/common/caching/filecache/CacheUsage.h"
#include "velox/common/caching/filecache/FileCacheOriginInfo.h"
#include <absl/container/flat_hash_map.h>
#include <deque>

namespace facebook::velox::ch
{

/// Eviction info:
/// - contains information about how much size/elements is needed to be evicted
/// - holds "space holders", for space which was already available
///   and will now be "held" as reserved, while we are evicting remaining space.
/// If releaseHoldSpace() is not called,
/// held space will be automatically released in destructor of HoldSpacePtr.
struct QueueEvictionInfo;
using QueueEvictionInfoPtr = std::unique_ptr<QueueEvictionInfo>;

struct QueueEvictionInfo
{
    explicit QueueEvictionInfo(
        const std::string & description_,
        const FileCacheOriginInfo::UserID & userId_)
        : description(description_), userId(userId_) {}

    const std::string description;
    const FileCacheOriginInfo::UserID userId;

    size_t sizeToEvict = 0;
    size_t elementsToEvict = 0;
    IFileCachePriority::HoldSpacePtr holdSpace;

    void merge(QueueEvictionInfoPtr other);

    std::string toString() const;
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return sizeToEvict || elementsToEvict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const { return holdSpace != nullptr; }
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);
};
using QueueID = size_t;

class EvictionInfo;
using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;

/// Aggregated eviction info:
/// - contains QueueEvictionInfo per queue_id
/// - aggregates all methods among all QueueEvictionInfo's.
class EvictionInfo : public absl::flat_hash_map<QueueID, QueueEvictionInfoPtr>, private boost::noncopyable
{
public:
    EvictionInfo() = default;
    /// Creates eviction info from a single QueueEvictionInfo.
    /// More infos can be added via add() method.
    explicit EvictionInfo(QueueID queue_id, QueueEvictionInfoPtr info);

    /// Get eviction info by queue id.
    const QueueEvictionInfo & get(const QueueID & queue_id) const;
    /// Add eviction info under the queue_id.
    /// Throws exception if eviction info with the same queue_id already exists.
    void add(EvictionInfoPtr && info);
    void addOrUpdate(EvictionInfoPtr && info);

    size_t getSizeToEvict() const { return sizeToEvict; }
    size_t getElementsToEvict() const { return elementsToEvict; }
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return sizeToEvict || elementsToEvict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const;
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);

    std::string toString() const;

    void setCacheUsage(std::vector<CacheUsagePtr> && usage) { sortedCacheUsage = std::move(usage); }
    std::vector<CacheUsagePtr> getCacheUsage() const { return sortedCacheUsage; }

private:
    /// If `merge_if_exists` is true
    /// (meaning that eviction info by `queue_id` already exists),
    /// combine two eviction info's into one.
    void addImpl(const QueueID & queue_id, QueueEvictionInfoPtr info, bool merge_if_exists);

    size_t sizeToEvict = 0; /// Total size to evict among all eviction infos.
    size_t elementsToEvict = 0; /// Total elements to evict among all eviction infos.

    std::vector<CacheUsagePtr> sortedCacheUsage;
};

class EvictionCandidates : private boost::noncopyable
{
public:
    using AfterEvictWriteFunc = std::function<void(const CachePriorityGuard::WriteLock & lk)>;
    using AfterEvictStateFunc = std::function<void(const CacheStateGuard::Lock & lk)>;

    EvictionCandidates();
    ~EvictionCandidates();

    /// Total number of eviction candidates.
    size_t size() const { return candidatesSize; }
    /// Total size in bytes of all eviction candidates.
    size_t bytes() const { return candidatesBytes; }

    auto begin() { return candidates.begin(); }
    auto end() { return candidates.end(); }
    auto begin() const { return candidates.begin(); }
    auto end() const { return candidates.end(); }

    /// Add a new eviction candidate.
    void add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key);
    /// Set a callback to be executed after eviction is finished.
    /// "write" func modifies priority queue structure.
    /// "state" func modifies cache size/elements counters.
    void setAfterEvictWriteFunc(AfterEvictWriteFunc && func) { afterEvictWriteFunc = std::move(func); }
    void setAfterEvictStateFunc(AfterEvictStateFunc && func) { afterEvictStateFunc = std::move(func); }

    /// Evict all candidates, which were added before via add().
    void evict();
    /// Execute "after eviction callbacks".
    /// "write" callback must be executed before "state" callback.
    void afterEvictWrite(const CachePriorityGuard::WriteLock & lock);
    void afterEvictState(const CacheStateGuard::Lock & lock);

    /// Whether calling afterEvictWrite() is required.
    /// (Can be used to avoid taking write lock)
    bool requiresAfterEvictWrite() const { return bool(afterEvictWriteFunc); }
    /// Whether calling afterEvictState() is required.
    /// (Can be used to avoid taking state lock)
    bool requiresAfterEvictState() const { return bool(afterEvictStateFunc) || !queueEntriesToInvalidate.empty(); }

    /// Used only for dynamic cache resize,
    /// allows to remove queue entries in advance.
    void removeQueueEntries(const CachePriorityGuard::WriteLock &);

    struct KeyCandidates
    {
        KeyMetadataPtr keyMetadata;
        std::vector<FileSegmentMetadataPtr> candidates;
        std::vector<std::string> errorMessages;
    };
    /// Get eviction candidates which failed to be evicted during evict().
    struct FailedCandidates
    {
        std::vector<KeyCandidates> failedCandidatesPerKey;
        size_t totalCacheSize = 0;
        size_t totalCacheElements = 0;

        size_t size() const { return failedCandidatesPerKey.size(); }

        std::string getFirstErrorMessage() const;
    };

    FailedCandidates getFailedCandidates() const { return failedCandidates; }

    /// Count of file segments successfully evicted by evict() (Layer B metric;
    /// incremented at the CH FilesystemCacheEvictedFileSegments site). Monotonic
    /// for the lifetime of this single-use object; FileCache aggregates it into
    /// its cache-wide evictions_ counter after evict() returns.
    size_t getNumEvicted() const { return numEvicted; }

    /// Get the original queue type of a candidate saved during removeQueueEntries.
    /// Returns None if not found (e.g., if removeQueueEntries was not called).
    FileCacheQueueEntryType getOriginalQueueType(const FileSegmentMetadata * candidate) const
    {
        auto it = originalQueueTypes.find(candidate);
        return it != originalQueueTypes.end() ? it->second : FileCacheQueueEntryType::None;
    }

private:
    absl::flat_hash_map<FileCacheKey, KeyCandidates, std::hash<FileCacheKey>> candidates;
    size_t candidatesSize = 0;
    size_t candidatesBytes = 0;
    size_t numEvicted = 0;
    FailedCandidates failedCandidates;

    /// Saved original queue type per candidate, populated in removeQueueEntries.
    std::unordered_map<const FileSegmentMetadata *, FileCacheQueueEntryType> originalQueueTypes;

    AfterEvictWriteFunc afterEvictWriteFunc;
    AfterEvictStateFunc afterEvictStateFunc;

    std::vector<IFileCachePriority::IteratorPtr> queueEntriesToInvalidate;
    bool removedQueueEntries = false;

    IFileCachePriority::HoldSpacePtr holdSpace;

    // TODO(logging): CH LoggerPtr was used only by LOG_* call sites; map .cpp logs to glog.
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
