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
#include "velox/ch/Interpreters/FileCache/IFileCachePriority.h"
#include "velox/ch/Interpreters/FileCache/EvictionCandidates.h"
#include "velox/ch/Interpreters/FileCache/FileSegmentInfo.h"

#include "velox/common/base/Exceptions.h"

#include <fmt/format.h>

namespace facebook::velox::ch
{

namespace
{
    /// Explicit exhaustive `Type` name mapping (CH uses `magic_enum::enum_name`).
    std::string_view typeName(IFileCachePriority::Type type)
    {
        switch (type)
        {
            case IFileCachePriority::Type::LRU: return "LRU";
            case IFileCachePriority::Type::SLRU: return "SLRU";
            case IFileCachePriority::Type::LRU_OVERCOMMIT: return "LRU_OVERCOMMIT";
            case IFileCachePriority::Type::SLRU_OVERCOMMIT: return "SLRU_OVERCOMMIT";
        }
        return {};
    }
}

IFileCachePriority::IFileCachePriority(QueueType queue_type_, size_t max_size_, size_t max_elements_)
    : queue_type(queue_type_), max_size(max_size_), max_elements(max_elements_)
{
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    State initial_state)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
    , state(initial_state)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
{
}

std::string IFileCachePriority::Entry::toString(const std::string & prefix) const
{
    return fmt::format(
        "{}{}:{}:{} (state: {})",
        prefix, key, offset, size.load(),
        toStringView(state.load(std::memory_order_relaxed)));
}

KeyMetadataPtr IFileCachePriority::Entry::getKeyMetadata() const
{
    auto locked = key_metadata.lock();
    if (!locked)
        VELOX_FAIL("Key metadata is expired for entry {}", toString());
    return locked;
}

void IFileCachePriority::check(const CacheStateGuard::Lock & lock) const
{
    if ((max_size != 0 && getSize(lock) > max_size) || (max_elements != 0 && getElementsCount(lock) > max_elements))
    {
        VELOX_FAIL("Cache limits violated. "
                        "{}", getStateInfoForLog(lock));
    }

    if (getSize(lock) > (1ull << 63) || getElementsCount(lock) > (1ull << 63))
        VELOX_FAIL("Cache became inconsistent. There must be a bug");
}

std::unordered_map<std::string, IFileCachePriority::UsageStat> IFileCachePriority::getUsageStatPerClient()
{
    VELOX_FAIL(
        "getUsageStatPerClient() is not implemented for {} policy",
        typeName(getType()));
}

void IFileCachePriority::removeEntries(
    const std::vector<InvalidatedEntryInfo> & entries,
    const CachePriorityGuard::WriteLock & lock)
{
    if (entries.empty())
        return;

    for (const auto & [entry, it] : entries)
    {
        /// We store `entry` shared pointer in addition to `it`
        /// (which is an iterator pointing to the same entry)
        /// because `it` could become invalid,
        /// so we use `entry` to check validity of the iterator.
        const auto entry_state = entry->getState();
        chassert(entry_state == Entry::State::Invalidated || entry_state == Entry::State::Removed,
                 fmt::format("Unexpected state: {}", Entry::toStringView(entry_state)));
        if (entry_state != Entry::State::Removed)
            it->remove(lock);
    }
}

IFileCachePriority::IPriorityDump::IPriorityDump() = default;
IFileCachePriority::IPriorityDump::~IPriorityDump() = default;

IFileCachePriority::IPriorityDump::IPriorityDump(const std::vector<FileSegmentInfo> & infos_)
    : infos(infos_)
{
}

void IFileCachePriority::IPriorityDump::merge(const IPriorityDump & other)
{
    infos.insert(infos.end(), other.infos.begin(), other.infos.end());
}

}
