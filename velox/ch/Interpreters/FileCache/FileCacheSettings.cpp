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

#include "velox/ch/Interpreters/FileCache/FileCacheSettings.h"
#include "velox/ch/Common/FileCacheException.h"
#include "velox/ch/Common/FileCacheFilesystem.h"

#include <velox/common/base/Exceptions.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>

namespace facebook::velox::ch
{

namespace
{

// All config key names recognised under a cache prefix.
// An unknown key under the prefix is rejected.
const std::set<std::string> kKnownKeys = {
    "path",
    "max-size",
    "max-elements",
    "max-file-segment-size",
    "boundary-alignment",
    "reserve-granularity",
    "cache-on-write-operations",
    "cache-policy",
    "slru-size-ratio",
    "background-download-threads",
    "background-download-queue-size-limit",
    "background-download-max-file-segment-size",
    "load-metadata-threads",
    "load-metadata-asynchronously",
    "keep-free-space-size-ratio",
    "keep-free-space-elements-ratio",
    "keep-free-space-remove-batch",
    "keep-free-space-eviction-threads",
    "invalidated-entries-cleanup-interval-ms",
    "invalidated-entries-cleanup-threshold",
    "invalidated-entries-cleanup-remove-batch",
    "enable-filesystem-query-cache-limit",
    "cache-hits-threshold",
    "enable-bypass-cache-with-threshold",
    "bypass-cache-threshold",
    "write-cache-per-user-id-directory",
    "allow-dynamic-cache-resize",
    "dynamic-resize-lock-wait-ms",
    "max-size-ratio-to-total-space",
    "skip-cache-on-disk-failure",
    "use-split-cache",
    "split-cache-ratio",
    "overcommit-eviction-evict-step",
    "check-cache-probability",
    "idle-client-ttl-sec",
    "idle-client-check-interval-sec",
    "idle-client-eviction-threads",
    "expose-prometheus-eviction-metrics",
    "expose-prometheus-eviction-metrics-per-user",
};

std::string toLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s;
}

FileCachePolicy parsePolicy(const std::string & s)
{
    const auto lower = toLower(s);
    if (lower == "lru")
        return FileCachePolicy::LRU;
    if (lower == "slru")
        return FileCachePolicy::SLRU;
    if (lower == "lru_overcommit" || lower == "lru-overcommit")
        return FileCachePolicy::LRU_OVERCOMMIT;
    if (lower == "slru_overcommit" || lower == "slru-overcommit")
        return FileCachePolicy::SLRU_OVERCOMMIT;
    throwFileCacheException(
        "Unknown cache policy '{}'. Supported: lru, slru, "
        "lru_overcommit, slru_overcommit",
        s);
}

template <typename T>
T getOr(
    const std::unordered_map<std::string, std::string> & kv,
    const std::string & key,
    T defaultVal)
{
    auto it = kv.find(key);
    if (it == kv.end())
        return defaultVal;
    try
    {
        if constexpr (std::is_same_v<T, std::string>)
            return it->second;
        else if constexpr (std::is_same_v<T, bool>)
        {
            const auto v = toLower(it->second);
            if (v == "true" || v == "1")
                return true;
            if (v == "false" || v == "0")
                return false;
            throwFileCacheException(
                "Cannot parse boolean for key '{}': '{}'", key, it->second);
        }
        else if constexpr (std::is_floating_point_v<T>)
        {
            std::size_t pos = 0;
            const T parsed = static_cast<T>(std::stod(it->second, &pos));
            if (pos != it->second.size())
                throwFileCacheException(
                    "Cannot parse floating-point value for key '{}': '{}'",
                    key, it->second);
            return parsed;
        }
        else if constexpr (std::is_integral_v<T>)
        {
            // Strict unsigned parse: reject a sign, trailing characters, or
            // overflow so a misconfiguration (e.g. a negative size) fails fast
            // instead of silently wrapping around.
            const std::string & s = it->second;
            std::uint64_t parsed = 0;
            const auto * end = s.data() + s.size();
            const auto res = std::from_chars(s.data(), end, parsed);
            if (res.ec != std::errc() || res.ptr != end)
                throwFileCacheException(
                    "Cannot parse unsigned integer for key '{}': '{}'", key, s);
            return static_cast<T>(parsed);
        }
    }
    catch (const VeloxRuntimeError &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        throwFileCacheException(
            "Cannot parse value for key '{}': {} — {}", key, it->second, e.what());
    }
    return defaultVal;
}

} // namespace

FileCacheConfig FileCacheSettingsLoader::load(
    const config::ConfigBase & config,
    const std::string & cachePrefix,
    const std::string & cachePathPrefix,
    const std::string & allowedCacheRoot)
{
    // Strip the prefix and expose the sub-keys as a flat map.
    const auto prefixWithDot = cachePrefix + ".";
    auto rawKv = config.rawConfigsWithPrefix(prefixWithDot);

    // Reject unknown keys.
    for (const auto & [key, _] : rawKv)
    {
        if (kKnownKeys.find(key) == kKnownKeys.end())
            throwFileCacheException(
                "Unknown cache configuration key '{}' under prefix '{}'",
                key, cachePrefix);
    }

    // ── Presence tracking ─────────────────────────────────────────────────────
    const bool hasPath = rawKv.count("path") > 0;
    const bool hasMaxSize = rawKv.count("max-size") > 0;
    const bool hasRatio = rawKv.count("max-size-ratio-to-total-space") > 0;

    // ── Path resolution ───────────────────────────────────────────────────────
    if (!hasPath)
        throwFileCacheException(
            "`path` is required for cache configuration under prefix '{}'",
            cachePrefix);

    const std::string rawPath = rawKv.at("path");
    fs::path resolved = rawPath;
    if (resolved.is_relative())
    {
        if (cachePathPrefix.empty())
            throwFileCacheException(
                "Cache path '{}' is relative but no cachePathPrefix was provided",
                rawPath);
        resolved = fs::path(cachePathPrefix) / resolved;
    }
    resolved = resolved.lexically_normal();
    if (!resolved.is_absolute())
        throwFileCacheException(
            "Cache path '{}' did not resolve to an absolute path", rawPath);

    // ── Path authorization (allowed root) ─────────────────────────────────────
    // Reject any resolved path that does not lie under `allowedCacheRoot`, before
    // any filesystem side effect (directory creation / space query). Both sides
    // are canonicalized with `weakly_canonical` so a symlink cannot escape the
    // root and a symlinked common prefix is resolved consistently; the stored
    // `cfg.path` keeps the approved lexically-normal form (set below), while
    // canonicalization is used only for this comparison. The check is a
    // component-prefix loop (not a string prefix): every component of the root
    // must equal the corresponding leading component of the resolved path, and
    // the loop never advances either iterator past its end.
    {
        // Fail closed on a missing/relative root instead of silently allowing
        // every path (an empty root would otherwise match nothing and accept all).
        if (allowedCacheRoot.empty() || fs::path(allowedCacheRoot).is_relative())
            throwFileCacheException(
                "`allowedCacheRoot` must be a non-empty absolute path; got '{}'",
                allowedCacheRoot);

        // A trailing separator leaves an empty final component under iteration
        // (e.g. `weakly_canonical` of a not-yet-existing "/cache/"), which would
        // otherwise break the component comparison; drop it on both sides.
        const auto dropTrailingSeparator = [](fs::path p)
        {
            if (!p.has_filename() && p != p.root_path())
                p = p.parent_path();
            return p;
        };

        fs::path rootCanon;
        fs::path pathCanon;
        try
        {
            rootCanon = dropTrailingSeparator(
                fs::weakly_canonical(fs::path(allowedCacheRoot)));
            pathCanon = dropTrailingSeparator(fs::weakly_canonical(resolved));
        }
        catch (const fs::filesystem_error & e)
        {
            throwFileCacheExceptionFromFilesystemError(
                e, "Failed to canonicalize cache path for allowed-root check");
        }

        auto rootIt = rootCanon.begin();
        auto pathIt = pathCanon.begin();
        for (; rootIt != rootCanon.end(); ++rootIt, ++pathIt)
        {
            if (pathIt == pathCanon.end() || *rootIt != *pathIt)
                throwFileCacheException(
                    "Cache path '{}' (resolved to '{}') must lie under allowed "
                    "root '{}'",
                    rawPath, resolved.string(), allowedCacheRoot);
        }
    }

    // ── Max-size source ───────────────────────────────────────────────────────
    if (!hasMaxSize && !hasRatio)
        throwFileCacheException(
            "Either `max-size` or `max-size-ratio-to-total-space` must be "
            "defined under cache prefix '{}'",
            cachePrefix);
    if (hasMaxSize && hasRatio)
        throwFileCacheException(
            "`max-size` and `max-size-ratio-to-total-space` cannot both be "
            "specified under cache prefix '{}'",
            cachePrefix);

    // ── Populate config ───────────────────────────────────────────────────────
    FileCacheConfig cfg;
    cfg.path = resolved.string();

    if (hasMaxSize)
    {
        cfg.maxSize = getOr<uint64_t>(rawKv, "max-size", 0);
        if (cfg.maxSize == 0)
            throwFileCacheException(
                "`max-size` cannot be 0 under cache prefix '{}'", cachePrefix);
    }

    cfg.maxElements = getOr(rawKv, "max-elements", cfg.maxElements);
    cfg.maxFileSegmentSize =
        getOr(rawKv, "max-file-segment-size", cfg.maxFileSegmentSize);
    cfg.boundaryAlignment =
        getOr(rawKv, "boundary-alignment", cfg.boundaryAlignment);
    cfg.reserveGranularity =
        getOr(rawKv, "reserve-granularity", cfg.reserveGranularity);
    cfg.cacheOnWriteOperations =
        getOr(rawKv, "cache-on-write-operations", cfg.cacheOnWriteOperations);
    cfg.slruSizeRatio = getOr(rawKv, "slru-size-ratio", cfg.slruSizeRatio);
    cfg.backgroundDownloadThreads =
        getOr(rawKv, "background-download-threads", cfg.backgroundDownloadThreads);
    cfg.backgroundDownloadQueueSizeLimit =
        getOr(rawKv, "background-download-queue-size-limit",
              cfg.backgroundDownloadQueueSizeLimit);
    cfg.backgroundDownloadMaxFileSegmentSize =
        getOr(rawKv, "background-download-max-file-segment-size",
              cfg.backgroundDownloadMaxFileSegmentSize);
    cfg.loadMetadataThreads =
        getOr(rawKv, "load-metadata-threads", cfg.loadMetadataThreads);
    cfg.loadMetadataAsynchronously =
        getOr(rawKv, "load-metadata-asynchronously", cfg.loadMetadataAsynchronously);
    cfg.keepFreeSpaceSizeRatio =
        getOr(rawKv, "keep-free-space-size-ratio", cfg.keepFreeSpaceSizeRatio);
    cfg.keepFreeSpaceElementsRatio =
        getOr(rawKv, "keep-free-space-elements-ratio", cfg.keepFreeSpaceElementsRatio);
    cfg.keepFreeSpaceRemoveBatch =
        getOr(rawKv, "keep-free-space-remove-batch", cfg.keepFreeSpaceRemoveBatch);
    cfg.keepFreeSpaceEvictionThreads =
        getOr(rawKv, "keep-free-space-eviction-threads", cfg.keepFreeSpaceEvictionThreads);
    cfg.invalidatedEntriesCleanupIntervalMs =
        getOr(rawKv, "invalidated-entries-cleanup-interval-ms",
              cfg.invalidatedEntriesCleanupIntervalMs);
    cfg.invalidatedEntriesCleanupThreshold =
        getOr(rawKv, "invalidated-entries-cleanup-threshold",
              cfg.invalidatedEntriesCleanupThreshold);
    cfg.invalidatedEntriesCleanupRemoveBatch =
        getOr(rawKv, "invalidated-entries-cleanup-remove-batch",
              cfg.invalidatedEntriesCleanupRemoveBatch);
    cfg.enableFilesystemQueryCacheLimit =
        getOr(rawKv, "enable-filesystem-query-cache-limit",
              cfg.enableFilesystemQueryCacheLimit);
    cfg.cacheHitsThreshold =
        getOr(rawKv, "cache-hits-threshold", cfg.cacheHitsThreshold);
    cfg.enableBypassCacheWithThreshold =
        getOr(rawKv, "enable-bypass-cache-with-threshold",
              cfg.enableBypassCacheWithThreshold);
    cfg.bypassCacheThreshold =
        getOr(rawKv, "bypass-cache-threshold", cfg.bypassCacheThreshold);
    cfg.writeCachePerUserIdDirectory =
        getOr(rawKv, "write-cache-per-user-id-directory",
              cfg.writeCachePerUserIdDirectory);
    cfg.allowDynamicCacheResize =
        getOr(rawKv, "allow-dynamic-cache-resize", cfg.allowDynamicCacheResize);
    cfg.dynamicResizeLockWaitMs =
        getOr(rawKv, "dynamic-resize-lock-wait-ms", cfg.dynamicResizeLockWaitMs);
    cfg.maxSizeRatioToTotalSpace =
        getOr(rawKv, "max-size-ratio-to-total-space", cfg.maxSizeRatioToTotalSpace);
    cfg.skipCacheOnDiskFailure =
        getOr(rawKv, "skip-cache-on-disk-failure", cfg.skipCacheOnDiskFailure);
    cfg.useSplitCache = getOr(rawKv, "use-split-cache", cfg.useSplitCache);
    cfg.splitCacheRatio = getOr(rawKv, "split-cache-ratio", cfg.splitCacheRatio);
    cfg.overcommitEvictionEvictStep =
        getOr(rawKv, "overcommit-eviction-evict-step", cfg.overcommitEvictionEvictStep);
    cfg.checkCacheProbability =
        getOr(rawKv, "check-cache-probability", cfg.checkCacheProbability);
    cfg.idleClientTtlSec =
        getOr(rawKv, "idle-client-ttl-sec", cfg.idleClientTtlSec);
    cfg.idleClientCheckIntervalSec =
        getOr(rawKv, "idle-client-check-interval-sec", cfg.idleClientCheckIntervalSec);
    cfg.idleClientEvictionThreads =
        getOr(rawKv, "idle-client-eviction-threads", cfg.idleClientEvictionThreads);
    cfg.exposePrometheusEvictionMetrics =
        getOr(rawKv, "expose-prometheus-eviction-metrics",
              cfg.exposePrometheusEvictionMetrics);
    cfg.exposePrometheusEvictionMetricsPerUser =
        getOr(rawKv, "expose-prometheus-eviction-metrics-per-user",
              cfg.exposePrometheusEvictionMetricsPerUser);

    if (rawKv.count("cache-policy") > 0)
        cfg.cachePolicy = parsePolicy(rawKv.at("cache-policy"));

    // ── Ratio-derived max-size ────────────────────────────────────────────────
    if (hasRatio)
    {
        if (!std::isfinite(cfg.maxSizeRatioToTotalSpace)
            || cfg.maxSizeRatioToTotalSpace <= 0.0
            || cfg.maxSizeRatioToTotalSpace > 1.0)
            throwFileCacheException(
                "`max-size-ratio-to-total-space` must be in (0, 1]; got {}",
                cfg.maxSizeRatioToTotalSpace);

        std::uintmax_t capacity = 0;
        try
        {
            fs::create_directories(resolved);
            capacity = fs::space(resolved).capacity;
        }
        catch (const fs::filesystem_error & e)
        {
            throwFileCacheExceptionFromFilesystemError(
                e, "Failed to prepare cache directory for ratio-derived max-size");
        }

        cfg.maxSize = static_cast<uint64_t>(
            std::floor(cfg.maxSizeRatioToTotalSpace
                       * static_cast<double>(capacity)));
        if (cfg.maxSize == 0)
            throwFileCacheException(
                "Ratio-derived max-size is 0 (ratio={}, total_space={}); "
                "increase max-size-ratio-to-total-space",
                cfg.maxSizeRatioToTotalSpace, capacity);
    }

    // ── Validation ────────────────────────────────────────────────────────────

    // Phase-1 unsupported features: fail fast with a clear message.
    if (cfg.cacheOnWriteOperations)
        throwFileCacheException(
            "cache_on_write_operations is not supported in the Velox FileCache "
            "port (first phase). Remove it from the cache configuration.");

    if (cfg.cachePolicy == FileCachePolicy::LRU_OVERCOMMIT
        || cfg.cachePolicy == FileCachePolicy::SLRU_OVERCOMMIT)
        throwFileCacheException(
            "Overcommit cache policies (LRU_OVERCOMMIT, SLRU_OVERCOMMIT) are "
            "not supported in the Velox FileCache port (first phase). "
            "Use LRU or SLRU instead.");

    // Fail-fast safety checks.
    if (cfg.maxFileSegmentSize == 0)
        throwFileCacheException(
            "`max-file-segment-size` cannot be 0; it would cause splitRange() "
            "to make no progress");

    if (cfg.boundaryAlignment > cfg.maxFileSegmentSize)
        throwFileCacheException(
            "`boundary-alignment` ({}) must be <= `max-file-segment-size` ({})",
            cfg.boundaryAlignment, cfg.maxFileSegmentSize);

    if (cfg.overcommitEvictionEvictStep == 0)
        throwFileCacheException(
            "`overcommit-eviction-evict-step` cannot be zero");

    if (cfg.useSplitCache
        && (cfg.cachePolicy == FileCachePolicy::LRU_OVERCOMMIT
            || cfg.cachePolicy == FileCachePolicy::SLRU_OVERCOMMIT))
        throwFileCacheException(
            "`use-split-cache` cannot be combined with overcommit policies");

    if (cfg.loadMetadataThreads == 0)
        throwFileCacheException("`load-metadata-threads` cannot be zero");

    if (cfg.keepFreeSpaceEvictionThreads == 0)
        throwFileCacheException("`keep-free-space-eviction-threads` cannot be zero");

    if (cfg.invalidatedEntriesCleanupIntervalMs == 0)
        throwFileCacheException(
            "`invalidated-entries-cleanup-interval-ms` cannot be zero");

    if (cfg.invalidatedEntriesCleanupThreshold == 0)
        throwFileCacheException(
            "`invalidated-entries-cleanup-threshold` cannot be zero");

    if (cfg.invalidatedEntriesCleanupRemoveBatch == 0)
        throwFileCacheException(
            "`invalidated-entries-cleanup-remove-batch` cannot be zero");

    if (cfg.idleClientEvictionThreads == 0)
        throwFileCacheException("`idle-client-eviction-threads` cannot be zero");

    return cfg;
}

} // namespace facebook::velox::ch
