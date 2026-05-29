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

#include <list>
#include <memory>

namespace facebook::velox::ch {

class FileCache;
using FileCachePtr = std::shared_ptr<FileCache>;

class IFileCachePriority;
using FileCachePriorityPtr = std::shared_ptr<IFileCachePriority>;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;

struct FileSegmentMetadata;
using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;

struct LockedKey;
using LockedKeyPtr = std::shared_ptr<LockedKey>;

struct KeyMetadata;
using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

} // namespace facebook::velox::ch
