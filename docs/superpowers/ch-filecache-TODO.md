# ch::FileCache 端口 —— TODO 索引

> 索引性文档，只列条目 + 引用，详情看引用处。范围：`velox/common/caching/filecache/`
> + `velox/dwio/common/FileCache*`（排除 tests/benchmarks）。核对：2026-06-01。
> 代码 TODO：103 = 53 `TODO(metric)` + 50 非-metric。来源以 `grep -rn TODO` 为准。

## 主线：FCBI 改 pull
- ✅ `pull-core`（阶段1）—— **已实现**，见 `plans/2026-06-01-pull-core.md` + `specs/2026-06-01-pull-core-design.md`；提交 `cace38488`+`7c2f8c00a`，`velox_ch_file_cache_test` 全绿。
- `pull-prefetch`（阶段2，依赖 pull-core，未开始）
- 详见 `specs/2026-06-01-f16-serve-from-memory-design.md` 顶部决策横幅。

## 性能优化 deferred
- `async-ssd-writeback`：冷 miss 时 `downloadFromReader`（`FileSegment.cpp` ~463-533）现在在消费线程上**同步**逐 1MB 写盘后才放行 reader。配合 serve-from-memory，可先把字节喂给 reader，SSD 写回放后台线程，把 SSD 写（及 fsync）移出读关键路径。约束：`downloadedSize` 必须等异步写完才前进（否则别的 reader pread 盘会读到未落盘字节）。关联 checkpoints 029-031。

## 功能性 deferred（会落代码）
- `fcbi-coalesce-io` → `specs/2026-05-31-ch-filecache-step18-design.md:226`
- `TODO(bypass-on-reserve-failure)` → 同上 `:216`
- `TODO(hash-stability)` → `FileCacheKey.cpp::fromPath`、`specs/2026-05-30-ch-filecache-port-design.md:58`
- `TODO(io)` → `FileSegment.cpp:472`、`Metadata.cpp:918,1156`、`FileSegment.h:41`
- 杂项裸 TODO → `Metadata.cpp:638`、`FileCache.cpp:2020`、`SLRUFileCachePriority.cpp:430`

## 关联组
- `TODO(query-context)` → `QueryLimit.{h:31,34,40,81,cpp:27}`、`FileCache.cpp:1241,2657`；审计 `reviews/2026-05-31-ch-filecache-port-audit.md:127`
- `TODO(threading)` → `FileCache.cpp:176,177`、`FileCache.h:46,48,347,352`、`Metadata.h:287,293`

## 移植占位（收尾批量，低优先级）
- `TODO(metric)` ×53（全局，审计 F15）、`TODO(profile-events)` → `Guards.h:96,100,122,127`
- `TODO(config)` → `FileCacheSettings.{h:84,cpp:105}`；`specs/2026-05-30-…port-design.md:280,282`
- `TODO(logging)` → `Metadata.h:186` 等 5 处；`TODO(ch-port)` → `EvictionCandidates.cpp:33,47,73,109` 等 7 处
- `TODO(failpoint)` → `FileCache.cpp:1468`、`EvictionCandidates.cpp:327`
- `TODO(random)` → `LRUFileCachePriority.cpp:79,710`、`FileCache.h:402`
- `TODO(status-file)`、`TODO(thread-safety)`、`TODO(enum-reflection)`、`TODO(call-once)` → `FileCache.{h,cpp}`、`LRUFileCachePriority.cpp:704`

## 审计缺口
- `reviews/2026-05-31-ch-filecache-port-audit.md`：F15（metrics，LOW）、query-context、fd 缓存清理（无风险）、free-space 线程。
