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
#include "velox/ch/Interpreters/FileCache/FileCacheReadOptions.h"
#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>

namespace facebook::velox::ch
{
namespace
{

namespace fs = std::filesystem;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a ConfigBase from a flat key-value map for testing.
static std::shared_ptr<config::ConfigBase> makeConfig(
    std::unordered_map<std::string, std::string> kv)
{
    return std::make_shared<config::ConfigBase>(std::move(kv));
}

/// Minimal valid config: one absolute path and explicit max-size.
[[maybe_unused]] static std::unordered_map<std::string, std::string> minimalKv(
    const std::string & path = "/tmp/test_cache",
    const std::string & maxSize = "1073741824")
{
    return {
        {"file-cache.path", path},
        {"file-cache.max-size", maxSize},
    };
}

/// A uniquely named temporary directory that is created on construction and
/// recursively removed on destruction. Used by the containment tests so that
/// symlink resolution and filesystem side effects are exercised against real
/// paths.
struct ScopedTempDir
{
    fs::path path;

    explicit ScopedTempDir(const std::string & name)
    {
        path = fs::temp_directory_path() / name;
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }

    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// ── Default value tests ───────────────────────────────────────────────────────

TEST(FileCacheConfigTest, DefaultValues)
{
    FileCacheConfig cfg;
    EXPECT_EQ(cfg.maxSize, 0u);
    EXPECT_EQ(cfg.maxElements, FILECACHE_DEFAULT_MAX_ELEMENTS);
    EXPECT_EQ(cfg.maxFileSegmentSize, FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE);
    EXPECT_EQ(cfg.boundaryAlignment, FILECACHE_DEFAULT_FILE_SEGMENT_ALIGNMENT);
    EXPECT_EQ(cfg.reserveGranularity, FILECACHE_DEFAULT_RESERVE_GRANULARITY);
    EXPECT_EQ(cfg.cacheOnWriteOperations, false);
    EXPECT_EQ(cfg.cachePolicy, FileCachePolicy::SLRU);
    EXPECT_DOUBLE_EQ(cfg.slruSizeRatio, FILECACHE_DEFAULT_SLRU_RATIO);
    EXPECT_EQ(cfg.backgroundDownloadThreads, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_THREADS);
    EXPECT_EQ(cfg.backgroundDownloadQueueSizeLimit, FILECACHE_DEFAULT_BACKGROUND_DOWNLOAD_QUEUE_SIZE_LIMIT);
    EXPECT_EQ(cfg.backgroundDownloadMaxFileSegmentSize,
              FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE_WITH_BACKGROUND_DOWLOAD);
    EXPECT_EQ(cfg.loadMetadataThreads, FILECACHE_DEFAULT_LOAD_METADATA_THREADS);
    EXPECT_EQ(cfg.loadMetadataAsynchronously, false);
    EXPECT_DOUBLE_EQ(cfg.keepFreeSpaceSizeRatio, FILECACHE_DEFAULT_FREE_SPACE_SIZE_RATIO);
    EXPECT_DOUBLE_EQ(cfg.keepFreeSpaceElementsRatio, FILECACHE_DEFAULT_FREE_SPACE_ELEMENTS_RATIO);
    EXPECT_EQ(cfg.keepFreeSpaceRemoveBatch, FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH);
    EXPECT_EQ(cfg.keepFreeSpaceEvictionThreads, FILECACHE_DEFAULT_FREE_SPACE_EVICTION_THREADS);
    EXPECT_EQ(cfg.enableFilesystemQueryCacheLimit, false);
    EXPECT_EQ(cfg.enableBypassCacheWithThreshold, false);
    EXPECT_EQ(cfg.bypassCacheThreshold, FILECACHE_BYPASS_THRESHOLD);
    EXPECT_EQ(cfg.writeCachePerUserIdDirectory, false);
    EXPECT_EQ(cfg.allowDynamicCacheResize, false);
    EXPECT_EQ(cfg.maxSizeRatioToTotalSpace, 0.0);
    EXPECT_DOUBLE_EQ(cfg.checkCacheProbability, 0.001);
    EXPECT_EQ(cfg.useSplitCache, false);
}

/// Covers every FileCacheConfig field not asserted by DefaultValues, so the full
/// struct contract is pinned to the CH defaults / FileCache_fwd.h constants.
TEST(FileCacheConfigTest, AllRemainingFieldDefaults)
{
    FileCacheConfig cfg;
    EXPECT_TRUE(cfg.path.empty());
    EXPECT_EQ(cfg.invalidatedEntriesCleanupIntervalMs, 10'000u);
    EXPECT_EQ(cfg.invalidatedEntriesCleanupThreshold, 1'000u);
    EXPECT_EQ(cfg.invalidatedEntriesCleanupRemoveBatch, FILECACHE_DEFAULT_FREE_SPACE_REMOVE_BATCH);
    EXPECT_EQ(cfg.cacheHitsThreshold, 0u);
    EXPECT_EQ(cfg.dynamicResizeLockWaitMs, 1'000u);
    EXPECT_EQ(cfg.skipCacheOnDiskFailure, false);
    EXPECT_DOUBLE_EQ(cfg.splitCacheRatio, 0.1);
    EXPECT_EQ(cfg.overcommitEvictionEvictStep, 10ULL * 1024 * 1024);
    EXPECT_EQ(cfg.idleClientTtlSec, 7ULL * 24 * 60 * 60);
    EXPECT_EQ(cfg.idleClientCheckIntervalSec, 0u);
    EXPECT_EQ(cfg.idleClientEvictionThreads, 4u);
    EXPECT_EQ(cfg.exposePrometheusEvictionMetrics, false);
    EXPECT_EQ(cfg.exposePrometheusEvictionMetricsPerUser, false);
}

TEST(FileCacheConfigTest, FreeSpaceRatiosDisabledByDefault)
{
    FileCacheConfig cfg;
    EXPECT_DOUBLE_EQ(cfg.keepFreeSpaceSizeRatio, 0.0);
    EXPECT_DOUBLE_EQ(cfg.keepFreeSpaceElementsRatio, 0.0);
}

TEST(FileCacheConfigTest, DefaultPolicySLRU)
{
    FileCacheConfig cfg;
    EXPECT_EQ(cfg.cachePolicy, FileCachePolicy::SLRU);
}

TEST(FileCacheConfigTest, DefaultSLRURatio)
{
    FileCacheConfig cfg;
    EXPECT_DOUBLE_EQ(cfg.slruSizeRatio, 0.6);
}

TEST(FileCacheConfigTest, DefaultMetadataThreads)
{
    FileCacheConfig cfg;
    EXPECT_EQ(cfg.loadMetadataThreads, 16u);
}

TEST(FileCacheConfigTest, DefaultBackgroundDownloadThreads)
{
    FileCacheConfig cfg;
    EXPECT_EQ(cfg.backgroundDownloadThreads, 5u);
}

TEST(FileCacheConfigTest, EqualityDefault)
{
    FileCacheConfig a, b;
    EXPECT_EQ(a, b);
}

/// Plain value type: copy is a deep, independent value and equality is by value,
/// matching the CH pimpl deep-copy / value-equality behavior.
TEST(FileCacheConfigTest, CopyProducesEqualIndependentValue)
{
    FileCacheConfig a;
    a.path = "/x/y";
    a.maxSize = 123;
    a.cachePolicy = FileCachePolicy::LRU;
    a.slruSizeRatio = 0.42;

    FileCacheConfig b = a;
    EXPECT_EQ(a, b);

    b.maxSize = 456;
    EXPECT_NE(a, b);
    EXPECT_EQ(a.maxSize, 123u);
    EXPECT_EQ(b.path, "/x/y");
    EXPECT_EQ(b.cachePolicy, FileCachePolicy::LRU);
    EXPECT_DOUBLE_EQ(b.slruSizeRatio, 0.42);
}

TEST(FileCacheConfigTest, FileCacheSettingsAliasesConfig)
{
    static_assert(std::is_same_v<FileCacheSettings, FileCacheConfig>);
}

TEST(FileCacheReadOptionsTest, ExactDefaults)
{
    FileCacheReadOptions options;
    EXPECT_FALSE(options.tempCacheOnly);
    EXPECT_FALSE(options.readIfExistsOtherwiseBypass);
    EXPECT_TRUE(options.allowBackgroundDownload);
    EXPECT_EQ(options.segmentsBatchSize, 20);
    EXPECT_EQ(options.maxDownloadSizePerQuery, 0);
    EXPECT_TRUE(options.skipDownloadIfExceedsPerQueryCacheWriteLimit);
}

/// Covers every FileCacheReadOptions field (the request-scoped contract from the
/// read-context design), including the ones ExactDefaults does not touch.
TEST(FileCacheReadOptionsTest, AllDefaults)
{
    FileCacheReadOptions o;
    EXPECT_FALSE(o.tempCacheOnly);
    EXPECT_FALSE(o.readIfExistsOtherwiseBypass);
    EXPECT_TRUE(o.allowBackgroundDownload);
    EXPECT_TRUE(o.allowBackgroundDownloadForMetadataFilesInPackedStorage);
    EXPECT_TRUE(o.allowBackgroundDownloadDuringFetch);
    EXPECT_TRUE(o.preferBiggerBufferSize);
    EXPECT_EQ(o.segmentsBatchSize, 20u);
    EXPECT_FALSE(o.boundaryAlignment.has_value());
    EXPECT_EQ(o.remoteFsBufferSize, 0u);
    EXPECT_EQ(o.localFsBufferSize, 0u);
    EXPECT_EQ(o.reserveSpaceWaitLockTimeoutMs, 0u);
    EXPECT_EQ(o.maxDownloadSizePerQuery, 0u);
    EXPECT_TRUE(o.skipDownloadIfExceedsPerQueryCacheWriteLimit);
    EXPECT_FALSE(o.enableFilesystemCacheLog);
}

/// Request-scoped value owned by the caller: copy is independent.
TEST(FileCacheReadOptionsTest, CopyIsIndependentValue)
{
    FileCacheReadOptions a;
    a.segmentsBatchSize = 7;
    a.boundaryAlignment = 4096;
    a.tempCacheOnly = true;

    FileCacheReadOptions b = a;
    EXPECT_EQ(b.segmentsBatchSize, 7u);
    ASSERT_TRUE(b.boundaryAlignment.has_value());
    EXPECT_EQ(*b.boundaryAlignment, 4096u);
    EXPECT_TRUE(b.tempCacheOnly);

    b.segmentsBatchSize = 9;
    EXPECT_EQ(a.segmentsBatchSize, 7u);
}

// ── Parsing tests ─────────────────────────────────────────────────────────────

TEST(FileCacheSettingsLoaderTest, ParseAbsolutePathAndMaxSize)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/data/cache"},
        {"file-cache.max-size", "10737418240"},
    });
    FileCacheConfig result = FileCacheSettingsLoader::load(
        *cfg, "file-cache", "/prefix", "/data");
    EXPECT_EQ(result.path, "/data/cache");
    EXPECT_EQ(result.maxSize, 10'737'418'240u);
}

TEST(FileCacheSettingsLoaderTest, ParseRelativePathUnderPrefix)
{
    auto cfg = makeConfig({
        {"file-cache.path", "my_cache"},
        {"file-cache.max-size", "1073741824"},
    });
    FileCacheConfig result = FileCacheSettingsLoader::load(
        *cfg, "file-cache", "/data/caches", "/data");
    // Relative path resolved under cachePathPrefix.
    EXPECT_EQ(result.path, "/data/caches/my_cache");
}

TEST(FileCacheSettingsLoaderTest, ParseNamedCachePrefix)
{
    auto cfg = makeConfig({
        {"file-cache.my_store.path", "/named/cache"},
        {"file-cache.my_store.max-size", "536870912"},
    });
    FileCacheConfig result = FileCacheSettingsLoader::load(
        *cfg, "file-cache.my_store", "/prefix", "/named");
    EXPECT_EQ(result.path, "/named/cache");
    EXPECT_EQ(result.maxSize, 536'870'912u);
}

TEST(FileCacheSettingsLoaderTest, ParsePolicyCaseInsensitive)
{
    for (const char * spelling : {"lru", "LRU"})
    {
        auto cfg = makeConfig({
            {"file-cache.path", "/c"},
            {"file-cache.max-size", "1073741824"},
            {"file-cache.cache-policy", spelling},
        });
        FileCacheConfig result = FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/c", "/");
        EXPECT_EQ(result.cachePolicy, FileCachePolicy::LRU);
    }
    for (const char * spelling : {"slru", "SLRU"})
    {
        auto cfg = makeConfig({
            {"file-cache.path", "/c"},
            {"file-cache.max-size", "1073741824"},
            {"file-cache.cache-policy", spelling},
        });
        FileCacheConfig result = FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/c", "/");
        EXPECT_EQ(result.cachePolicy, FileCachePolicy::SLRU);
    }
}

/// All supported scalar types (UInt64, Bool, Double) parse into their fields.
TEST(FileCacheSettingsLoaderTest, ParseScalarFields)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/data/cache"},
        {"file-cache.max-size", "2147483648"},
        {"file-cache.max-elements", "12345"},
        {"file-cache.max-file-segment-size", "16777216"},
        {"file-cache.boundary-alignment", "1048576"},
        {"file-cache.reserve-granularity", "2097152"},
        {"file-cache.background-download-threads", "7"},
        {"file-cache.background-download-queue-size-limit", "999"},
        {"file-cache.load-metadata-threads", "4"},
        {"file-cache.load-metadata-asynchronously", "true"},
        {"file-cache.slru-size-ratio", "0.75"},
        {"file-cache.keep-free-space-size-ratio", "0.25"},
        {"file-cache.use-split-cache", "1"},
        {"file-cache.enable-bypass-cache-with-threshold", "true"},
        {"file-cache.bypass-cache-threshold", "8388608"},
    });
    FileCacheConfig r = FileCacheSettingsLoader::load(
        *cfg, "file-cache", "/data", "/data");
    EXPECT_EQ(r.maxSize, 2'147'483'648u);
    EXPECT_EQ(r.maxElements, 12'345u);
    EXPECT_EQ(r.maxFileSegmentSize, 16'777'216u);
    EXPECT_EQ(r.boundaryAlignment, 1'048'576u);
    EXPECT_EQ(r.reserveGranularity, 2'097'152u);
    EXPECT_EQ(r.backgroundDownloadThreads, 7u);
    EXPECT_EQ(r.backgroundDownloadQueueSizeLimit, 999u);
    EXPECT_EQ(r.loadMetadataThreads, 4u);
    EXPECT_TRUE(r.loadMetadataAsynchronously);
    EXPECT_DOUBLE_EQ(r.slruSizeRatio, 0.75);
    EXPECT_DOUBLE_EQ(r.keepFreeSpaceSizeRatio, 0.25);
    EXPECT_TRUE(r.useSplitCache);
    EXPECT_TRUE(r.enableBypassCacheWithThreshold);
    EXPECT_EQ(r.bypassCacheThreshold, 8'388'608u);
}

TEST(FileCacheSettingsLoaderTest, ParseMaxSizeRatio)
{
    // Ratio-derived max-size test requires the cache directory to exist so that
    // std::filesystem::space() succeeds. Create a temp directory.
    auto tmpDir = fs::temp_directory_path() / "ch_cache_test_ratio";
    fs::create_directories(tmpDir);

    auto cfg = makeConfig({
        {"file-cache.path", tmpDir.string()},
        {"file-cache.max-size-ratio-to-total-space", "0.5"},
    });
    FileCacheConfig result = FileCacheSettingsLoader::load(
        *cfg, "file-cache", tmpDir.string(), tmpDir.string());
    auto totalSpace = fs::space(tmpDir).capacity;
    auto expectedMaxSize = static_cast<uint64_t>(
        std::floor(0.5 * static_cast<double>(totalSpace)));
    EXPECT_EQ(result.maxSize, expectedMaxSize);
    EXPECT_GT(result.maxSize, 0u);

    fs::remove_all(tmpDir);
}

// ── Validation error tests ────────────────────────────────────────────────────

TEST(FileCacheSettingsLoaderTest, MissingPath)
{
    auto cfg = makeConfig({{"file-cache.max-size", "1073741824"}});
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/p", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, RelativePathWithoutPrefixRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "relative_cache"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, NeitherMaxSizeNorRatio)
{
    auto cfg = makeConfig({{"file-cache.path", "/cache"}});
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, BothMaxSizeAndRatio)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.max-size-ratio-to-total-space", "0.5"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, ExplicitMaxSizeZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, RatioOutOfRange)
{
    for (const char * v : {"0", "1.1", "-0.1"})
    {
        auto cfg = makeConfig({
            {"file-cache.path", "/cache"},
            {"file-cache.max-size-ratio-to-total-space", v},
        });
        EXPECT_THROW(
            FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
            VeloxRuntimeError)
            << "ratio=" << v;
    }
}

/// Ratio in range but so small that the derived effective max-size floors to 0:
/// this is the phase-1 fail-fast enhancement over CH.
TEST(FileCacheSettingsLoaderTest, RatioDerivedMaxSizeZero)
{
    auto tmpDir = fs::temp_directory_path() / "ch_cache_test_ratio_zero";
    fs::create_directories(tmpDir);

    // 1e-18 is > 0 and <= 1, but floor(1e-18 * capacity) == 0 for any realistic
    // disk (capacity < 1 exabyte), so the effective max-size is 0 and rejected.
    auto cfg = makeConfig({
        {"file-cache.path", tmpDir.string()},
        {"file-cache.max-size-ratio-to-total-space", "1e-18"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", tmpDir.string(), tmpDir.string()),
        VeloxRuntimeError);

    fs::remove_all(tmpDir);
}

TEST(FileCacheSettingsLoaderTest, MaxFileSegmentSizeZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.max-file-segment-size", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, BoundaryAlignmentExceedsMaxSegmentSize)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        // boundary (8 MiB) > max-file-segment-size (4 MiB)
        {"file-cache.boundary-alignment", "8388608"},
        {"file-cache.max-file-segment-size", "4194304"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, LoadMetadataThreadsZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.load-metadata-threads", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, KeepFreeSpaceEvictionThreadsZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.keep-free-space-eviction-threads", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, InvalidatedEntriesCleanupIntervalZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.invalidated-entries-cleanup-interval-ms", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, InvalidatedEntriesCleanupThresholdZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.invalidated-entries-cleanup-threshold", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, InvalidatedEntriesCleanupRemoveBatchZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.invalidated-entries-cleanup-remove-batch", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, IdleClientEvictionThreadsZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.idle-client-eviction-threads", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, OvercommitEvictionEvictStepZero)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.overcommit-eviction-evict-step", "0"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, UnknownKeyRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.unknown-setting-xyz", "value"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, UnknownPolicyRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.cache-policy", "bogus"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, WriteThoughUnsupported)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.cache-on-write-operations", "true"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, OvercommitPoliciesUnsupported)
{
    for (const char * policy : {"lru_overcommit", "LRU_OVERCOMMIT",
                                 "slru_overcommit", "SLRU_OVERCOMMIT"})
    {
        auto cfg = makeConfig({
            {"file-cache.path", "/cache"},
            {"file-cache.max-size", "1073741824"},
            {"file-cache.cache-policy", policy},
        });
        EXPECT_THROW(
            FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
            VeloxRuntimeError)
            << "policy=" << policy;
    }
}

TEST(FileCacheSettingsLoaderTest, SplitCacheWithOvercommitRejected)
{
    // useSplitCache cannot combine with overcommit policy. Even if the overcommit
    // rejection fires first, the intent of this test is the combination.
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.use-split-cache", "true"},
        {"file-cache.cache-policy", "lru_overcommit"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

// ── Path containment tests (component-prefix, canonicalizing) ──────────────────

TEST(FileCacheContainmentTest, ExactRootAccepted)
{
    ScopedTempDir root("ch_cache_contain_exact");
    auto cfg = makeConfig({
        {"file-cache.path", root.path.string()},
        {"file-cache.max-size", "1073741824"},
    });
    FileCacheConfig r;
    ASSERT_NO_THROW(
        r = FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()));
    EXPECT_EQ(r.path, fs::path(root.path).lexically_normal().string());
}

TEST(FileCacheContainmentTest, DescendantAccepted)
{
    ScopedTempDir root("ch_cache_contain_descend");
    auto sub = root.path / "a" / "b";
    auto cfg = makeConfig({
        {"file-cache.path", sub.string()},
        {"file-cache.max-size", "1073741824"},
    });
    FileCacheConfig r;
    ASSERT_NO_THROW(
        r = FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()));
    EXPECT_EQ(r.path, sub.lexically_normal().string());
}

TEST(FileCacheContainmentTest, ShorterPathRejectedSafely)
{
    // allowedRoot is DEEPER than the resolved path. The component-prefix loop
    // must reject this without ever advancing the resolved-path iterator past
    // its end (the pre-amendment std::mismatch snippet had UB here).
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/cache/sub/deeper", "/cache/sub/deeper"),
        VeloxRuntimeError);
}

TEST(FileCacheContainmentTest, SiblingPrefixRejected)
{
    // "/cache-other" shares a string prefix with "/cache" but is a different
    // path component; a string-prefix check would wrongly accept it.
    auto cfg = makeConfig({
        {"file-cache.path", "/cache-other/data"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/cache"),
        VeloxRuntimeError);
}

TEST(FileCacheContainmentTest, DotDotEscapeRejected)
{
    // Relative path whose ".." escapes the prefix after lexical normalization.
    auto cfg = makeConfig({
        {"file-cache.path", "../evil"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/data/caches", "/data/caches"),
        VeloxRuntimeError);
}

TEST(FileCacheContainmentTest, SymlinkEscapeRejected)
{
    // A symlink inside the allowed root that points outside must not let the
    // resolved path escape. Only canonicalization (not lexical normalization)
    // catches this, so a lexical-only containment check would wrongly accept.
    ScopedTempDir root("ch_cache_contain_symroot");
    ScopedTempDir outside("ch_cache_contain_symout");

    auto link = root.path / "escape";
    std::error_code ec;
    fs::create_directory_symlink(outside.path, link, ec);
    ASSERT_FALSE(ec) << "failed to create symlink: " << ec.message();

    auto viaLink = link / "cache"; // resolves through the symlink to outside/cache
    auto cfg = makeConfig({
        {"file-cache.path", viaLink.string()},
        // Valid max-size, so the ONLY reason to reject is the containment check.
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()),
        VeloxRuntimeError);
}

TEST(FileCacheContainmentTest, OutsideRootRatioCreatesNoSideEffect)
{
    // Path authorization (allowed-root containment) must happen BEFORE any
    // create_directories()/space() side effect, so an out-of-root ratio config
    // is rejected without ever creating the directory.
    ScopedTempDir root("ch_cache_contain_sideeffect");
    auto outsidePath =
        fs::temp_directory_path() / "ch_cache_contain_should_not_exist";
    std::error_code ec;
    fs::remove_all(outsidePath, ec);

    auto cfg = makeConfig({
        {"file-cache.path", outsidePath.string()},
        {"file-cache.max-size-ratio-to-total-space", "0.5"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()),
        VeloxRuntimeError);
    EXPECT_FALSE(fs::exists(outsidePath))
        << "path outside allowed root must not be created before validation";

    fs::remove_all(outsidePath, ec);
}

TEST(FileCacheSettingsLoaderTest, FilesystemErrorSurfacesOriginalDiagnostics)
{
    // Make create_directories() fail (a regular file stands where a parent
    // directory should be) and assert the original filesystem diagnostics (the
    // failing path) are surfaced in the thrown exception.
    ScopedTempDir base("ch_cache_fs_diag");
    auto filePath = base.path / "not_a_dir";
    {
        std::ofstream ofs(filePath);
        ofs << "x";
    }
    ASSERT_TRUE(fs::exists(filePath));

    auto badPath = filePath / "cache"; // parent component is a regular file
    auto cfg = makeConfig({
        {"file-cache.path", badPath.string()},
        {"file-cache.max-size-ratio-to-total-space", "0.5"},
    });
    try
    {
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", base.path.string(), base.path.string());
        FAIL() << "expected a VeloxRuntimeError";
    }
    catch (const VeloxRuntimeError & e)
    {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("not_a_dir"), std::string::npos) << msg;
    }
}

// ── Strict scalar parsing ─────────────────────────────────────────────────────

TEST(FileCacheSettingsLoaderTest, NegativeMaxSizeRejected)
{
    // A negative value must fail fast, not silently wrap around to a huge size.
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "-1"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, TrailingGarbageInIntegerRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.max-elements", "123abc"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

TEST(FileCacheSettingsLoaderTest, TrailingGarbageInDoubleRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.slru-size-ratio", "0.5xyz"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/cache", "/"),
        VeloxRuntimeError);
}

// ── Allowed-root precondition and trailing-separator handling ──────────────────

TEST(FileCacheContainmentTest, TrailingSlashNonExistentRootAcceptsDescendant)
{
    // A not-yet-existing allowed root supplied with a trailing separator must
    // still accept a descendant path (the trailing empty component must not
    // cause a false rejection).
    auto base = fs::temp_directory_path() / "ch_cache_trailing_root_nonexist";
    std::error_code ec;
    fs::remove_all(base, ec); // ensure it does not exist during containment
    const std::string rootWithSlash = base.string() + "/";
    auto sub = base / "sub";

    auto cfg = makeConfig({
        {"file-cache.path", sub.string()},
        {"file-cache.max-size", "1073741824"}, // explicit size => no side effect
    });
    FileCacheConfig r;
    ASSERT_NO_THROW(
        r = FileCacheSettingsLoader::load(
            *cfg, "file-cache", rootWithSlash, rootWithSlash));
    EXPECT_EQ(r.path, sub.lexically_normal().string());
    EXPECT_FALSE(fs::exists(base)); // no side effect for an explicit max-size

    fs::remove_all(base, ec);
}

TEST(FileCacheContainmentTest, EmptyAllowedRootRejected)
{
    // An empty allowed root must fail closed, not accept every absolute path.
    auto cfg = makeConfig({
        {"file-cache.path", "/anywhere/cache"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(*cfg, "file-cache", "/prefix", ""),
        VeloxRuntimeError);
}

TEST(FileCacheContainmentTest, RelativeAllowedRootRejected)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/anywhere/cache"},
        {"file-cache.max-size", "1073741824"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/prefix", "relative/root"),
        VeloxRuntimeError);
}

// ── Non-finite ratio rejection ────────────────────────────────────────────────
// Each non-finite value must be rejected with a VeloxRuntimeError BEFORE any
// directory creation, filesystem::space, floor, or integer conversion side
// effect. The NaN path is a genuine RED against the attempt-2 implementation:
// `NaN <= 0.0` and `NaN > 1.0` are both false, so the ordered-comparison range
// guard does not fire and NaN falls through to UB. The `!std::isfinite` guard
// (minimal fix) precedes the ordered comparisons and closes this gap.

TEST(FileCacheSettingsLoaderTest, NonFiniteRatioNaN)
{
    // Use a real subdirectory so create_directories() would have a real effect.
    ScopedTempDir root("ch_cache_ratio_nan_root");
    auto cachePath = root.path / "nan_cache";
    std::error_code ec;
    fs::remove_all(cachePath, ec); // ensure the subdir does not exist initially

    auto cfg = makeConfig({
        {"file-cache.path", cachePath.string()},
        {"file-cache.max-size-ratio-to-total-space", "nan"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()),
        VeloxRuntimeError);
    // After the fix the guard fires before create_directories; no side effect.
    EXPECT_FALSE(fs::exists(cachePath))
        << "nan ratio must not trigger directory creation before rejection";
}

TEST(FileCacheSettingsLoaderTest, NonFiniteRatioPosInf)
{
    ScopedTempDir root("ch_cache_ratio_posinf_root");
    auto cachePath = root.path / "posinf_cache";
    std::error_code ec;
    fs::remove_all(cachePath, ec);

    auto cfg = makeConfig({
        {"file-cache.path", cachePath.string()},
        {"file-cache.max-size-ratio-to-total-space", "inf"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()),
        VeloxRuntimeError);
    EXPECT_FALSE(fs::exists(cachePath))
        << "+inf ratio must not trigger directory creation before rejection";
}

TEST(FileCacheSettingsLoaderTest, NonFiniteRatioNegInf)
{
    ScopedTempDir root("ch_cache_ratio_neginf_root");
    auto cachePath = root.path / "neginf_cache";
    std::error_code ec;
    fs::remove_all(cachePath, ec);

    auto cfg = makeConfig({
        {"file-cache.path", cachePath.string()},
        {"file-cache.max-size-ratio-to-total-space", "-inf"},
    });
    EXPECT_THROW(
        FileCacheSettingsLoader::load(
            *cfg, "file-cache", root.path.string(), root.path.string()),
        VeloxRuntimeError);
    EXPECT_FALSE(fs::exists(cachePath))
        << "-inf ratio must not trigger directory creation before rejection";
}

// ── One-hot boolean loader cases ─────────────────────────────────────────────
// For each of the six loader-controlled boolean keys: set only that key to
// true in a minimal valid config, assert its target field is true, and assert
// all other five target fields are false. Any key/field swap — including a
// swap inside a same-valued group — makes at least two assertions fail,
// proving each key maps to a unique and correct field.

TEST(FileCacheSettingsLoaderTest, OneHotBooleanMappings)
{
    struct BoolCase
    {
        const char * key;
        bool FileCacheConfig::*field;
    };

    const BoolCase cases[] = {
        {"allow-dynamic-cache-resize",
         &FileCacheConfig::allowDynamicCacheResize},
        {"enable-filesystem-query-cache-limit",
         &FileCacheConfig::enableFilesystemQueryCacheLimit},
        {"expose-prometheus-eviction-metrics",
         &FileCacheConfig::exposePrometheusEvictionMetrics},
        {"expose-prometheus-eviction-metrics-per-user",
         &FileCacheConfig::exposePrometheusEvictionMetricsPerUser},
        {"skip-cache-on-disk-failure",
         &FileCacheConfig::skipCacheOnDiskFailure},
        {"write-cache-per-user-id-directory",
         &FileCacheConfig::writeCachePerUserIdDirectory},
    };
    constexpr std::size_t N = std::size(cases);

    for (std::size_t i = 0; i < N; ++i)
    {
        SCOPED_TRACE(cases[i].key);
        // Minimal config with only the i-th boolean key set to true;
        // all other five boolean keys are absent and default to false.
        auto kv = minimalKv();
        kv[std::string("file-cache.") + cases[i].key] = "true";
        auto cfg = makeConfig(std::move(kv));
        FileCacheConfig r = FileCacheSettingsLoader::load(
            *cfg, "file-cache", "/tmp/test_cache", "/");

        EXPECT_TRUE(r.*(cases[i].field))
            << "key " << cases[i].key << " must set its own field to true";
        for (std::size_t j = 0; j < N; ++j)
        {
            if (j != i)
            {
                EXPECT_FALSE(r.*(cases[j].field))
                    << "field for " << cases[j].key
                    << " must remain false when only " << cases[i].key
                    << " is set";
            }
        }
    }
}

// ── Non-boolean previously-uncovered key/field pairs ─────────────────────────
// Each of the nine non-boolean keys is set to a distinct non-default value.
// A key/field misrouting leaves the incorrectly written field at the non-default
// value while the intended field stays at its default — both visible below.

TEST(FileCacheSettingsLoaderTest, ParseUncoveredNonBoolKeys)
{
    auto cfg = makeConfig({
        {"file-cache.path", "/data/cache"},
        {"file-cache.max-size", "1073741824"},
        {"file-cache.background-download-max-file-segment-size", "8388608"},
        {"file-cache.cache-hits-threshold", "42"},
        {"file-cache.check-cache-probability", "0.05"},
        {"file-cache.dynamic-resize-lock-wait-ms", "2500"},
        {"file-cache.idle-client-check-interval-sec", "300"},
        {"file-cache.idle-client-ttl-sec", "3600"},
        {"file-cache.keep-free-space-elements-ratio", "0.15"},
        {"file-cache.keep-free-space-remove-batch", "500"},
        {"file-cache.split-cache-ratio", "0.3"},
    });
    FileCacheConfig r = FileCacheSettingsLoader::load(
        *cfg, "file-cache", "/data", "/");

    EXPECT_EQ(r.backgroundDownloadMaxFileSegmentSize, 8'388'608u);
    EXPECT_EQ(r.cacheHitsThreshold, 42u);
    EXPECT_DOUBLE_EQ(r.checkCacheProbability, 0.05);
    EXPECT_EQ(r.dynamicResizeLockWaitMs, 2'500u);
    EXPECT_EQ(r.idleClientCheckIntervalSec, 300u);
    EXPECT_EQ(r.idleClientTtlSec, 3'600u);
    EXPECT_DOUBLE_EQ(r.keepFreeSpaceElementsRatio, 0.15);
    EXPECT_EQ(r.keepFreeSpaceRemoveBatch, 500u);
    EXPECT_DOUBLE_EQ(r.splitCacheRatio, 0.3);
}

} // namespace
} // namespace facebook::velox::ch
