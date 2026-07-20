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

/// Path + etag used to derive the `FileCache` key. This is the single source of
/// truth for key derivation on the scan read path; object-storage plumbing that
/// supplies a real etag must go through `deriveKey`, not bypass it.
struct FileCacheFileIdentity
{
    std::string path;
    std::string etag;

    /// Derive cache key:
    ///   etag empty     -> FileCacheKey::fromPath(path)
    ///   etag non-empty -> FileCacheKey::fromKey(SipHash128(path + etag))
    /// Two different non-empty etags for the same path produce different keys.
    static FileCacheKey deriveKey(const FileCacheFileIdentity & id)
    {
        if (id.etag.empty())
            return FileCacheKey::fromPath(id.path);

        SipHash128 hash;
        hash.update(id.path.data(), id.path.size());
        hash.update(id.etag.data(), id.etag.size());
        return FileCacheKey::fromKey(hash.get128());
    }
};

} // namespace facebook::velox::ch
