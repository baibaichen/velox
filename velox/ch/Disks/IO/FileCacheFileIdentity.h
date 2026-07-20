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

#include "velox/ch/Common/SipHash128.h"
#include "velox/ch/Interpreters/FileCache/FileCacheKey.h"

#include <string>

namespace facebook::velox::ch
{

/// File version identity used to derive a `FileCacheKey`. Kept separate from the
/// query/user request context so that object-storage versioning (etag) never
/// mixes into query identity.
///
/// Key derivation is the single source of truth for this task; any future
/// object-storage plumbing that supplies a real etag must go through `deriveKey`,
/// never bypass it. A path-only entry must not be reused once a real etag is
/// available, otherwise an overwritten object could hit stale cached content.
struct FileCacheFileIdentity
{
    std::string path;
    std::string etag;

    /// Derive the cache key:
    ///   etag empty     -> FileCacheKey::fromPath(path)
    ///   etag non-empty -> FileCacheKey::fromKey(SipHash128(path + etag))
    ///
    /// Two different non-empty etags for the same path produce different keys.
    static FileCacheKey deriveKey(const FileCacheFileIdentity & id);
};

inline FileCacheKey FileCacheFileIdentity::deriveKey(const FileCacheFileIdentity & id)
{
    if (id.etag.empty())
        return FileCacheKey::fromPath(id.path);

    // Concatenate path and etag, then hash with the CH SipHash128 variant so
    // that two different etags for the same path map to different keys.
    const std::string combined = id.path + id.etag;
    return FileCacheKey::fromKey(sipHash128(combined.data(), combined.size()));
}

} // namespace facebook::velox::ch
