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

#include "velox/dwio/common/BufferedInputTrace.h"

#include <atomic>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <folly/json.h>
#include <folly/system/ThreadId.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

namespace
{

// ---------------------------------------------------------------------------
// Op name tables
// ---------------------------------------------------------------------------

const std::unordered_map<std::string, BufferedInputTraceOp>& opByName()
{
    static const std::unordered_map<std::string, BufferedInputTraceOp> kTable = {
        {"input_create", BufferedInputTraceOp::kInputCreate},
        {"input_clone", BufferedInputTraceOp::kInputClone},
        {"input_reset", BufferedInputTraceOp::kInputReset},
        {"input_preload", BufferedInputTraceOp::kInputPreload},
        {"input_set_num_stripes", BufferedInputTraceOp::kInputSetNumStripes},
        {"input_close", BufferedInputTraceOp::kInputClose},
        {"stream_enqueue", BufferedInputTraceOp::kStreamEnqueue},
        {"stream_read", BufferedInputTraceOp::kStreamRead},
        {"load", BufferedInputTraceOp::kLoad},
        {"consume", BufferedInputTraceOp::kConsume},
        {"skip", BufferedInputTraceOp::kSkip},
        {"seek", BufferedInputTraceOp::kSeek},
        {"stream_eof", BufferedInputTraceOp::kStreamEof},
        {"stream_close", BufferedInputTraceOp::kStreamClose},
    };
    return kTable;
}

const std::unordered_map<BufferedInputTraceOp, std::string>& opName()
{
    static const std::unordered_map<BufferedInputTraceOp, std::string> kTable = {
        {BufferedInputTraceOp::kInputCreate, "input_create"},
        {BufferedInputTraceOp::kInputClone, "input_clone"},
        {BufferedInputTraceOp::kInputReset, "input_reset"},
        {BufferedInputTraceOp::kInputPreload, "input_preload"},
        {BufferedInputTraceOp::kInputSetNumStripes, "input_set_num_stripes"},
        {BufferedInputTraceOp::kInputClose, "input_close"},
        {BufferedInputTraceOp::kStreamEnqueue, "stream_enqueue"},
        {BufferedInputTraceOp::kStreamRead, "stream_read"},
        {BufferedInputTraceOp::kLoad, "load"},
        {BufferedInputTraceOp::kConsume, "consume"},
        {BufferedInputTraceOp::kSkip, "skip"},
        {BufferedInputTraceOp::kSeek, "seek"},
        {BufferedInputTraceOp::kStreamEof, "stream_eof"},
        {BufferedInputTraceOp::kStreamClose, "stream_close"},
    };
    return kTable;
}

// ---------------------------------------------------------------------------
// Strict key validation helpers
// ---------------------------------------------------------------------------

/// Verify that 'obj' contains exactly the keys in 'required'.
/// Throws on any unknown key or missing required key.
void checkExactKeys(
    const folly::dynamic& obj,
    std::initializer_list<const char*> required)
{
    std::unordered_set<std::string> allowed;
    for (const char* k : required)
    {
        allowed.insert(k);
    }
    for (const auto& [k, v] : obj.items())
    {
        const auto& ks = k.getString();
        VELOX_CHECK(
            allowed.count(ks) > 0,
            "unknown key '{}' in JSON object",
            ks);
    }
    for (const auto& k : allowed)
    {
        VELOX_CHECK(
            obj.count(k) > 0,
            "missing required key '{}' in JSON object",
            k);
    }
}

/// Extract an int64_t from a folly::dynamic int, throwing on overflow when
/// coercing to a narrower type.
template <typename T>
T getInt(const folly::dynamic& obj, const char* key)
{
    const auto& v = obj[key];
    VELOX_CHECK(
        v.isInt(),
        "key '{}' must be an integer, got type {}",
        key,
        v.typeName());
    const int64_t raw = v.asInt();
    if constexpr (std::is_same_v<T, int64_t>)
    {
        return raw;
    }
    else if constexpr (std::is_unsigned_v<T>)
    {
        VELOX_CHECK(
            raw >= 0,
            "key '{}' must be non-negative, got {}",
            key,
            raw);
        const auto uraw = static_cast<uint64_t>(raw);
        if constexpr (sizeof(T) < sizeof(uint64_t))
        {
            VELOX_CHECK(
                uraw <= static_cast<uint64_t>(std::numeric_limits<T>::max()),
                "key '{}' value {} overflows target type",
                key,
                uraw);
        }
        return static_cast<T>(uraw);
    }
    else
    {
        // signed narrower than int64_t
        VELOX_CHECK(
            raw >= static_cast<int64_t>(std::numeric_limits<T>::min()) &&
                raw <= static_cast<int64_t>(std::numeric_limits<T>::max()),
            "key '{}' value {} overflows target type",
            key,
            raw);
        return static_cast<T>(raw);
    }
}

std::string getString(const folly::dynamic& obj, const char* key)
{
    const auto& v = obj[key];
    VELOX_CHECK(
        v.isString(),
        "key '{}' must be a string, got type {}",
        key,
        v.typeName());
    return v.getString();
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Checked conversion from uint64_t to int64_t for JSON encoding.
/// Values above INT64_MAX cannot be losslessly represented as JSON integers
/// and must be rejected rather than silently wrapped or truncated.
int64_t checkedToInt64(uint64_t v, const char* fieldName)
{
    VELOX_CHECK(
        v <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
        "value {} in field '{}' overflows int64 and cannot be serialized to JSON",
        v,
        fieldName);
    return static_cast<int64_t>(v);
}

std::string opToString(BufferedInputTraceOp op)
{
    const auto it = opName().find(op);
    VELOX_CHECK(
        it != opName().end(),
        "unknown BufferedInputTraceOp value {}",
        static_cast<int>(op));
    return it->second;
}

BufferedInputTraceOp opFromString(const std::string& name)
{
    const auto it = opByName().find(name);
    VELOX_CHECK(
        it != opByName().end(),
        "unknown op name '{}'",
        name);
    return it->second;
}

folly::dynamic serializeFile(const BufferedInputTraceFile& f)
{
    folly::dynamic obj = folly::dynamic::object;
    obj["file_id"] = checkedToInt64(f.fileId, "file_id");
    obj["relative_path"] = f.relativePath;
    obj["size"] = checkedToInt64(f.size, "size");
    return obj;
}

folly::dynamic serializeManifest(
    const BufferedInputTraceManifest& m,
    const std::vector<BufferedInputTraceFile>& files)
{
    folly::dynamic obj = folly::dynamic::object;
    obj["schema_version"] = checkedToInt64(m.schemaVersion, "schema_version");
    obj["query_id"] = static_cast<int64_t>(m.queryId);
    obj["drivers"] = static_cast<int64_t>(m.drivers);
    obj["dataset_root"] = m.datasetRoot;
    obj["binary_real_path"] = m.binaryRealPath;
    obj["binary_build_id"] = m.binaryBuildId;
    obj["velox_head"] = m.veloxHead;
    obj["gluten_head"] = m.glutenHead;
    obj["clickhouse_head"] = m.clickhouseHead;
    obj["event_count"] = checkedToInt64(m.eventCount, "event_count");
    folly::dynamic filesArr = folly::dynamic::array;
    for (const auto& f : files)
    {
        filesArr.push_back(serializeFile(f));
    }
    obj["files"] = std::move(filesArr);
    return obj;
}

folly::dynamic serializeEvent(const BufferedInputTraceEvent& e)
{
    folly::dynamic obj = folly::dynamic::object;
    obj["seq"] = checkedToInt64(e.seq, "seq");
    obj["capture_tid"] = checkedToInt64(e.captureTid, "capture_tid");
    obj["thread_index"] = checkedToInt64(e.threadIndex, "thread_index");
    obj["op"] = opToString(e.op);
    obj["file_id"] = checkedToInt64(e.fileId, "file_id");
    obj["input_id"] = checkedToInt64(e.inputId, "input_id");
    obj["parent_input_id"] = checkedToInt64(e.parentInputId, "parent_input_id");
    obj["stream_id"] = checkedToInt64(e.streamId, "stream_id");
    obj["offset"] = checkedToInt64(e.offset, "offset");
    obj["length"] = checkedToInt64(e.length, "length");
    obj["argument"] = e.argument;
    obj["result"] = e.result;
    obj["log_type"] = static_cast<int64_t>(e.logType);
    return obj;
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

BufferedInputTraceFile parseFile(const folly::dynamic& obj)
{
    checkExactKeys(obj, {"file_id", "relative_path", "size"});
    BufferedInputTraceFile f;
    f.fileId = getInt<uint64_t>(obj, "file_id");
    f.relativePath = getString(obj, "relative_path");
    f.size = getInt<uint64_t>(obj, "size");
    return f;
}

BufferedInputTraceManifest parseManifest(
    const folly::dynamic& obj,
    std::vector<BufferedInputTraceFile>& filesOut)
{
    checkExactKeys(
        obj,
        {"schema_version",
         "query_id",
         "drivers",
         "dataset_root",
         "binary_real_path",
         "binary_build_id",
         "velox_head",
         "gluten_head",
         "clickhouse_head",
         "event_count",
         "files"});

    BufferedInputTraceManifest m;
    m.schemaVersion = getInt<uint32_t>(obj, "schema_version");
    m.queryId = getInt<int32_t>(obj, "query_id");
    m.drivers = getInt<int32_t>(obj, "drivers");
    m.datasetRoot = getString(obj, "dataset_root");
    m.binaryRealPath = getString(obj, "binary_real_path");
    m.binaryBuildId = getString(obj, "binary_build_id");
    m.veloxHead = getString(obj, "velox_head");
    m.glutenHead = getString(obj, "gluten_head");
    m.clickhouseHead = getString(obj, "clickhouse_head");
    m.eventCount = getInt<uint64_t>(obj, "event_count");

    const auto& filesArr = obj["files"];
    VELOX_CHECK(
        filesArr.isArray(),
        "key 'files' must be an array, got type {}",
        filesArr.typeName());
    filesOut.clear();
    for (const auto& f : filesArr)
    {
        filesOut.push_back(parseFile(f));
    }
    return m;
}

BufferedInputTraceEvent parseEvent(const folly::dynamic& obj)
{
    checkExactKeys(
        obj,
        {"seq",
         "capture_tid",
         "thread_index",
         "op",
         "file_id",
         "input_id",
         "parent_input_id",
         "stream_id",
         "offset",
         "length",
         "argument",
         "result",
         "log_type"});

    BufferedInputTraceEvent e;
    e.seq = getInt<uint64_t>(obj, "seq");
    e.captureTid = getInt<uint64_t>(obj, "capture_tid");
    e.threadIndex = getInt<uint32_t>(obj, "thread_index");
    e.op = opFromString(getString(obj, "op"));
    e.fileId = getInt<uint64_t>(obj, "file_id");
    e.inputId = getInt<uint64_t>(obj, "input_id");
    e.parentInputId = getInt<uint64_t>(obj, "parent_input_id");
    e.streamId = getInt<uint64_t>(obj, "stream_id");
    e.offset = getInt<uint64_t>(obj, "offset");
    e.length = getInt<uint64_t>(obj, "length");
    e.argument = getInt<int64_t>(obj, "argument");
    e.result = getInt<int64_t>(obj, "result");
    e.logType = getInt<int32_t>(obj, "log_type");
    return e;
}

// ---------------------------------------------------------------------------
// Path escape detection
// ---------------------------------------------------------------------------

bool hasPathEscape(const std::string& relativePath)
{
    if (!relativePath.empty() && relativePath[0] == '/')
    {
        return true;
    }
    const std::filesystem::path p(relativePath);
    for (const auto& component : p)
    {
        if (component == "..")
        {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Lifecycle state machine
// ---------------------------------------------------------------------------

struct InputState
{
    uint64_t fileId{0};
    bool live{true};
    std::unordered_set<uint64_t> liveStreams;
};

struct StreamState
{
    uint64_t ownerInputId{0};
    bool live{true};
};

} // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

SerializedBufferedInputTrace serializeBufferedInputTrace(
    const BufferedInputTraceDocument& trace)
{
    const folly::json::serialization_opts opts;
    SerializedBufferedInputTrace out;
    out.manifest = folly::toJson(serializeManifest(trace.manifest, trace.files));
    std::ostringstream ss;
    for (const auto& e : trace.events)
    {
        ss << folly::toJson(serializeEvent(e)) << '\n';
    }
    out.events = ss.str();
    return out;
}

BufferedInputTraceDocument parseBufferedInputTrace(
    std::string_view manifestJson,
    std::string_view eventsJsonl)
{
    BufferedInputTraceDocument doc;

    // Parse manifest
    folly::dynamic manifestObj = folly::parseJson(manifestJson);
    VELOX_CHECK(
        manifestObj.isObject(),
        "manifest JSON must be an object, got type {}",
        manifestObj.typeName());
    doc.manifest = parseManifest(manifestObj, doc.files);

    // Parse events (JSONL)
    std::string_view remaining = eventsJsonl;
    while (!remaining.empty())
    {
        // Skip blank lines
        const size_t nlPos = remaining.find('\n');
        const std::string_view line = (nlPos == std::string_view::npos)
            ? remaining
            : remaining.substr(0, nlPos);
        remaining = (nlPos == std::string_view::npos)
            ? std::string_view{}
            : remaining.substr(nlPos + 1);

        if (line.empty())
        {
            continue;
        }
        folly::dynamic eventObj = folly::parseJson(line);
        VELOX_CHECK(
            eventObj.isObject(),
            "event JSON must be an object, got type {}",
            eventObj.typeName());
        doc.events.push_back(parseEvent(eventObj));
    }

    return doc;
}

BufferedInputTraceDocument loadBufferedInputTrace(const std::string& traceRoot)
{
    namespace fs = std::filesystem;
    const fs::path root(traceRoot);

    const fs::path manifestPath = root / "manifest.json";
    VELOX_CHECK(
        fs::exists(manifestPath),
        "manifest.json not found in {}",
        traceRoot);
    std::string manifestJson;
    {
        std::ifstream f(manifestPath);
        VELOX_CHECK(f.is_open(), "failed to open {}", manifestPath.string());
        std::ostringstream ss;
        ss << f.rdbuf();
        manifestJson = ss.str();
    }

    const fs::path eventsPath = root / "events.jsonl";
    VELOX_CHECK(
        fs::exists(eventsPath),
        "events.jsonl not found in {}",
        traceRoot);
    std::string eventsJsonl;
    {
        std::ifstream f(eventsPath);
        VELOX_CHECK(f.is_open(), "failed to open {}", eventsPath.string());
        std::ostringstream ss;
        ss << f.rdbuf();
        eventsJsonl = ss.str();
    }

    return parseBufferedInputTrace(manifestJson, eventsJsonl);
}

void validateBufferedInputTrace(
    const BufferedInputTraceDocument& trace,
    const std::string& expectedDatasetRoot)
{
    // 1. Schema version
    VELOX_CHECK(
        trace.manifest.schemaVersion == 1,
        "unsupported schema version {}; only version 1 is supported",
        trace.manifest.schemaVersion);

    // 2. File ID validation: non-zero and unique across the files table.
    {
        std::unordered_set<uint64_t> seenFileIds;
        for (const auto& file : trace.files)
        {
            VELOX_CHECK(
                file.fileId != 0,
                "file_id=0 is reserved; all file IDs must be non-zero");
            VELOX_CHECK(
                seenFileIds.insert(file.fileId).second,
                "duplicate file_id={} in files table",
                file.fileId);
        }
    }

    // 3. File path safety (lexical: no absolute path, no '..' component).
    for (const auto& file : trace.files)
    {
        VELOX_CHECK(
            !hasPathEscape(file.relativePath),
            "file_id={} path escapes dataset root: '{}'",
            file.fileId,
            file.relativePath);
    }

    // 4. Dataset root and canonical containment (when expectedDatasetRoot is
    //    provided).  Every file's resolved path must be a strict descendant of
    //    the canonical root.  Unresolvable paths fail closed.
    if (!expectedDatasetRoot.empty())
    {
        namespace fs = std::filesystem;
        std::error_code ec;

        const fs::path canonRoot = fs::canonical(expectedDatasetRoot, ec);
        VELOX_CHECK(
            !ec,
            "expectedDatasetRoot '{}' cannot be canonicalized: {}",
            expectedDatasetRoot,
            ec.message());

        const fs::path manifestRoot =
            fs::canonical(trace.manifest.datasetRoot, ec);
        VELOX_CHECK(
            !ec,
            "manifest datasetRoot '{}' cannot be canonicalized: {}",
            trace.manifest.datasetRoot,
            ec.message());
        VELOX_CHECK(
            manifestRoot == canonRoot,
            "manifest datasetRoot '{}' (canonical: '{}') does not match "
            "expectedDatasetRoot '{}' (canonical: '{}')",
            trace.manifest.datasetRoot,
            manifestRoot.string(),
            expectedDatasetRoot,
            canonRoot.string());

        const std::string rootStr = canonRoot.string();
        for (const auto& file : trace.files)
        {
            const fs::path filePath = canonRoot / file.relativePath;
            const fs::path canonFile = fs::canonical(filePath, ec);
            VELOX_CHECK(
                !ec,
                "file_id={} path '{}' cannot be resolved: {}",
                file.fileId,
                filePath.string(),
                ec.message());

            const std::string canonFileStr = canonFile.string();
            VELOX_CHECK(
                canonFileStr.size() > rootStr.size() &&
                    canonFileStr.compare(0, rootStr.size(), rootStr) == 0 &&
                    canonFileStr[rootStr.size()] == '/',
                "file_id={} path '{}' resolves to '{}' which is not a strict "
                "descendant of dataset root '{}'",
                file.fileId,
                file.relativePath,
                canonFileStr,
                rootStr);
        }
    }

    // Build file ID lookup set for lifecycle checks (IDs already validated).
    std::unordered_set<uint64_t> fileIds;
    for (const auto& file : trace.files)
    {
        fileIds.insert(file.fileId);
    }

    // 4. Seq monotonicity + 5. Lifecycle state machine
    std::unordered_map<uint64_t, InputState> inputs;
    std::unordered_map<uint64_t, StreamState> streams;
    uint64_t prevSeq = 0;

    for (const auto& ev : trace.events)
    {
        // Seq must be strictly increasing
        VELOX_CHECK(
            ev.seq > prevSeq,
            "event seq must be strictly increasing: got {} after {}",
            ev.seq,
            prevSeq);
        prevSeq = ev.seq;

        switch (ev.op)
        {
            case BufferedInputTraceOp::kInputCreate:
            {
                VELOX_CHECK(
                    ev.inputId != 0,
                    "input_id=0 is reserved; input_id must be non-zero on input_create (seq={})",
                    ev.seq);
                VELOX_CHECK(
                    fileIds.count(ev.fileId) > 0,
                    "file_id={} not found in files table (seq={})",
                    ev.fileId,
                    ev.seq);
                VELOX_CHECK(
                    inputs.find(ev.inputId) == inputs.end(),
                    "input_id={} is already live (seq={})",
                    ev.inputId,
                    ev.seq);
                InputState s;
                s.fileId = ev.fileId;
                inputs[ev.inputId] = std::move(s);
                break;
            }

            case BufferedInputTraceOp::kInputClone:
            {
                VELOX_CHECK(
                    ev.inputId != 0,
                    "input_id=0 is reserved; input_id must be non-zero on input_clone (seq={})",
                    ev.seq);
                VELOX_CHECK(
                    inputs.count(ev.parentInputId) > 0 &&
                        inputs.at(ev.parentInputId).live,
                    "parent input_id={} is not live for clone (seq={})",
                    ev.parentInputId,
                    ev.seq);
                VELOX_CHECK(
                    inputs.find(ev.inputId) == inputs.end(),
                    "input_id={} is already live (seq={})",
                    ev.inputId,
                    ev.seq);
                InputState s;
                s.fileId = inputs.at(ev.parentInputId).fileId;
                inputs[ev.inputId] = std::move(s);
                break;
            }

            case BufferedInputTraceOp::kInputReset:
            case BufferedInputTraceOp::kInputPreload:
            case BufferedInputTraceOp::kInputSetNumStripes:
            case BufferedInputTraceOp::kLoad:
            {
                VELOX_CHECK(
                    inputs.count(ev.inputId) > 0 &&
                        inputs.at(ev.inputId).live,
                    "input_id={} is not live (seq={})",
                    ev.inputId,
                    ev.seq);
                break;
            }

            case BufferedInputTraceOp::kInputClose:
            {
                const auto it = inputs.find(ev.inputId);
                VELOX_CHECK(
                    it != inputs.end() && it->second.live,
                    "input_id={} is not live (seq={})",
                    ev.inputId,
                    ev.seq);
                VELOX_CHECK(
                    it->second.liveStreams.empty(),
                    "stream_id={} is still live when input_id={} closes (seq={})",
                    *it->second.liveStreams.begin(),
                    ev.inputId,
                    ev.seq);
                it->second.live = false;
                break;
            }

            case BufferedInputTraceOp::kStreamEnqueue:
            case BufferedInputTraceOp::kStreamRead:
            {
                VELOX_CHECK(
                    ev.streamId != 0,
                    "stream_id=0 is reserved; stream_id must be non-zero on stream create (seq={})",
                    ev.seq);
                VELOX_CHECK(
                    inputs.count(ev.inputId) > 0 &&
                        inputs.at(ev.inputId).live,
                    "input_id={} is not live for stream create (seq={})",
                    ev.inputId,
                    ev.seq);
                VELOX_CHECK(
                    streams.find(ev.streamId) == streams.end(),
                    "stream_id={} is already live (seq={})",
                    ev.streamId,
                    ev.seq);
                streams[ev.streamId] = StreamState{ev.inputId, true};
                inputs.at(ev.inputId).liveStreams.insert(ev.streamId);
                break;
            }

            case BufferedInputTraceOp::kConsume:
            case BufferedInputTraceOp::kSkip:
            case BufferedInputTraceOp::kSeek:
            case BufferedInputTraceOp::kStreamEof:
            {
                const auto sit = streams.find(ev.streamId);
                VELOX_CHECK(
                    sit != streams.end() && sit->second.live,
                    "stream_id={} is not live (seq={})",
                    ev.streamId,
                    ev.seq);
                const uint64_t ownerInputId = sit->second.ownerInputId;
                VELOX_CHECK(
                    inputs.count(ownerInputId) > 0 &&
                        inputs.at(ownerInputId).live,
                    "stream_id={} owner input_id={} is not live (seq={})",
                    ev.streamId,
                    ownerInputId,
                    ev.seq);
                break;
            }

            case BufferedInputTraceOp::kStreamClose:
            {
                const auto sit = streams.find(ev.streamId);
                VELOX_CHECK(
                    sit != streams.end() && sit->second.live,
                    "stream_id={} is not live (seq={})",
                    ev.streamId,
                    ev.seq);
                const uint64_t ownerInputId = sit->second.ownerInputId;
                // If the event carries an explicit inputId, it must match.
                if (ev.inputId != 0)
                {
                    VELOX_CHECK(
                        ev.inputId == ownerInputId,
                        "stream_id={} owner mismatch: recorded owner is "
                        "input_id={}, but event says input_id={} (seq={})",
                        ev.streamId,
                        ownerInputId,
                        ev.inputId,
                        ev.seq);
                }
                VELOX_CHECK(
                    inputs.count(ownerInputId) > 0 &&
                        inputs.at(ownerInputId).live,
                    "stream_id={} owner input_id={} is not live at stream_close (seq={})",
                    ev.streamId,
                    ownerInputId,
                    ev.seq);
                sit->second.live = false;
                inputs.at(ownerInputId).liveStreams.erase(ev.streamId);
                break;
            }
        }
    }

    // 6. No live objects after the final event
    for (const auto& [id, state] : inputs)
    {
        VELOX_CHECK(
            !state.live,
            "live inputs remain after final event: input_id={}",
            id);
    }
    for (const auto& [id, state] : streams)
    {
        VELOX_CHECK(
            !state.live,
            "live streams remain after final event: stream_id={}",
            id);
    }

    // 7. Event count
    VELOX_CHECK(
        trace.manifest.eventCount == trace.events.size(),
        "manifest.eventCount={} does not match events.size()={}",
        trace.manifest.eventCount,
        trace.events.size());
}

} // namespace facebook::velox::dwio::common

// =============================================================================
// Task-2: BufferedInputTraceSession, capture guard, decorators
// =============================================================================

namespace facebook::velox::dwio::common {

// ---------------------------------------------------------------------------
// BufferedInputTraceSession
// ---------------------------------------------------------------------------

class BufferedInputTraceSession
{
public:
    explicit BufferedInputTraceSession(BufferedInputTraceCaptureConfig config)
        : config_(std::move(config))
    {
        // Canonicalize datasetRoot once; fail closed if not resolvable.
        if (!config_.datasetRoot.empty())
        {
            namespace fs = std::filesystem;
            std::error_code ec;
            const auto canonRoot = fs::canonical(config_.datasetRoot, ec);
            VELOX_CHECK(
                !ec,
                "datasetRoot '{}' cannot be canonicalized: {}",
                config_.datasetRoot,
                ec.message());
            canonicalDatasetRoot_ = canonRoot.string();
        }
    }

    // -----------------------------------------------------------------------
    // ID allocation (always under mutex)
    // -----------------------------------------------------------------------

    /// Register a ReadFile by its absolute on-disk path.  Returns a stable
    /// file_id.  Canonicalizes the path and requires strict descendant
    /// containment under canonicalDatasetRoot_.  Rejects size mismatches for
    /// already-registered canonical paths.
    uint64_t getOrAllocFileId(
        const std::string& absolutePath,
        uint64_t fileSize)
    {
        namespace fs = std::filesystem;

        // Canonicalize source file path — fail closed if not resolvable.
        std::error_code ec;
        const auto canonFile = fs::canonical(absolutePath, ec);
        VELOX_CHECK(
            !ec,
            "cannot canonicalize source file '{}': {}",
            absolutePath,
            ec.message());
        const std::string canonFileStr = canonFile.string();

        // Require strict descendant containment.
        VELOX_CHECK(
            !canonicalDatasetRoot_.empty() &&
                canonFileStr.size() > canonicalDatasetRoot_.size() &&
                canonFileStr.compare(
                    0, canonicalDatasetRoot_.size(), canonicalDatasetRoot_) ==
                    0 &&
                canonFileStr[canonicalDatasetRoot_.size()] == '/',
            "source file '{}' (canonical: '{}') is not inside datasetRoot '{}'",
            absolutePath,
            canonFileStr,
            canonicalDatasetRoot_);

        std::lock_guard<std::mutex> lk(mutex_);

        const auto it = fileIdMap_.find(canonFileStr);
        if (it != fileIdMap_.end())
        {
            const size_t idx = it->second;
            VELOX_CHECK(
                files_[idx].size == fileSize,
                "size mismatch for canonical path '{}': "
                "registered {} bytes but got {}",
                canonFileStr,
                files_[idx].size,
                fileSize);
            return files_[idx].fileId;
        }

        // Derive relative path from canonical root.
        const std::string relPath =
            canonFileStr.substr(canonicalDatasetRoot_.size() + 1);

        const uint64_t fid = nextFileId_++;
        fileIdMap_[canonFileStr] = files_.size();
        files_.push_back({fid, relPath, fileSize});
        return fid;
    }

    uint64_t allocInputId()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return nextInputId_++;
    }

    uint64_t allocStreamId()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return nextStreamId_++;
    }

    // -----------------------------------------------------------------------
    // Event recording
    // -----------------------------------------------------------------------

    /// Append an event, assigning seq/captureTid/threadIndex under the lock.
    /// On any error (finished, overflow) stores firstError_ without throwing.
    void tryAppendEvent(BufferedInputTraceEvent ev)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (finished_)
        {
            return; // drop events after finish
        }
        if (!firstError_.empty())
        {
            return; // already in error — drop
        }
        if (events_.size() >= config_.maxEvents)
        {
            firstError_ = "maxEvents limit exceeded";
            return;
        }
        ev.seq = nextSeq_++;
        ev.captureTid = folly::getOSThreadID();
        ev.threadIndex = threadIndex(ev.captureTid);
        events_.push_back(ev);
    }

    /// Append an event from a non-destructor path; throws on error.
    void appendEvent(BufferedInputTraceEvent ev)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        VELOX_CHECK(
            !finished_,
            "BufferedInputTraceSession: use after finish");
        if (!firstError_.empty())
        {
            VELOX_FAIL("BufferedInputTraceSession error: {}", firstError_);
        }
        if (events_.size() >= config_.maxEvents)
        {
            firstError_ = "maxEvents limit exceeded";
            VELOX_FAIL("BufferedInputTraceSession: maxEvents limit exceeded");
        }
        ev.seq = nextSeq_++;
        ev.captureTid = folly::getOSThreadID();
        ev.threadIndex = threadIndex(ev.captureTid);
        events_.push_back(ev);
    }

    /// Record a lifecycle error from a destructor (non-throwing).
    void setError(std::string msg)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (firstError_.empty())
        {
            firstError_ = std::move(msg);
        }
    }

    bool isFinished() const
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return finished_;
    }

    // -----------------------------------------------------------------------
    // finish(): validate + write
    // -----------------------------------------------------------------------

    void finish()
    {
        std::lock_guard<std::mutex> lk(mutex_);

        if (finished_)
        {
            return; // idempotent after success
        }

        // Propagate any stored error first.
        if (!firstError_.empty())
        {
            VELOX_FAIL(
                "BufferedInputTraceSession cannot finish: {}", firstError_);
        }

        // Build the trace document.
        BufferedInputTraceDocument doc;
        doc.manifest.schemaVersion = 1;
        doc.manifest.queryId = config_.queryId;
        doc.manifest.drivers = config_.drivers;
        doc.manifest.datasetRoot = config_.datasetRoot;
        doc.manifest.binaryRealPath = config_.binaryRealPath;
        doc.manifest.binaryBuildId = config_.binaryBuildId;
        doc.manifest.veloxHead = config_.veloxHead;
        doc.manifest.glutenHead = config_.glutenHead;
        doc.manifest.clickhouseHead = config_.clickhouseHead;
        doc.manifest.eventCount = events_.size();
        doc.files = files_;
        doc.events = events_;

        // Reject unresolvable or empty dataset root before writing.
        VELOX_CHECK(
            !canonicalDatasetRoot_.empty(),
            "datasetRoot is required and must be resolvable for trace finish");

        // Validate lifecycle (throws on error); pass canonical root for
        // canonical containment check (Finding 3).
        validateBufferedInputTrace(doc, canonicalDatasetRoot_);

        // Reject existing trace root.
        namespace fs = std::filesystem;
        std::error_code ec;
        VELOX_CHECK(
            !fs::exists(config_.traceRoot, ec),
            "trace root already exists: {}",
            config_.traceRoot);
        VELOX_CHECK(!ec, "stat of traceRoot failed: {}", ec.message());

        fs::create_directories(config_.traceRoot, ec);
        VELOX_CHECK(
            !ec,
            "failed to create trace root '{}': {}",
            config_.traceRoot,
            ec.message());

        // Serialize.
        const auto serial = serializeBufferedInputTrace(doc);

        // Write manifest.json.
        {
            const std::string mpath = config_.traceRoot + "/manifest.json";
            std::ofstream mf(mpath, std::ios::out | std::ios::trunc);
            VELOX_CHECK(
                mf.is_open(),
                "failed to open manifest '{}' for writing",
                mpath);
            mf << serial.manifest;
            VELOX_CHECK(mf.good(), "failed to write manifest '{}'", mpath);
        }

        // Write events.jsonl.
        {
            const std::string epath = config_.traceRoot + "/events.jsonl";
            std::ofstream ef(epath, std::ios::out | std::ios::trunc);
            VELOX_CHECK(
                ef.is_open(),
                "failed to open events file '{}' for writing",
                epath);
            ef << serial.events;
            VELOX_CHECK(
                ef.good(), "failed to write events file '{}'", epath);
        }

        finished_ = true;
    }

private:
    // Caller must hold mutex_.
    uint32_t threadIndex(uint64_t tid)
    {
        auto it = tidToThreadIdx_.find(tid);
        if (it != tidToThreadIdx_.end())
        {
            return it->second;
        }
        const uint32_t idx = nextThreadIdx_++;
        tidToThreadIdx_[tid] = idx;
        return idx;
    }

    BufferedInputTraceCaptureConfig config_;
    std::string canonicalDatasetRoot_;
    mutable std::mutex mutex_;

    std::vector<BufferedInputTraceFile> files_;
    std::unordered_map<std::string, size_t> fileIdMap_; // canonical path → files_ index
    std::vector<BufferedInputTraceEvent> events_;
    std::unordered_map<uint64_t, uint32_t> tidToThreadIdx_;

    uint32_t nextThreadIdx_{0};
    uint64_t nextSeq_{1};
    uint64_t nextFileId_{1};
    uint64_t nextInputId_{1};
    uint64_t nextStreamId_{1};

    bool finished_{false};
    std::string firstError_;
};

// ---------------------------------------------------------------------------
// Global exclusive capture guard
// ---------------------------------------------------------------------------

namespace
{
// Mutex + weak_ptr so wrappers can hold a strong reference that outlives the
// guard; maybeWrap acquires a strong reference under the lock before leaving
// the synchronization boundary (Finding 1).
std::mutex gSessionMutex;
std::weak_ptr<BufferedInputTraceSession> gWeakSession;
} // namespace

// ---------------------------------------------------------------------------
// ScopedBufferedInputTraceCapture
// ---------------------------------------------------------------------------

ScopedBufferedInputTraceCapture::ScopedBufferedInputTraceCapture(
    BufferedInputTraceCaptureConfig config)
    : session_(
          std::make_shared<BufferedInputTraceSession>(std::move(config)))
{
    std::lock_guard<std::mutex> lk(gSessionMutex);
    VELOX_CHECK(
        gWeakSession.expired(),
        "another BufferedInput trace capture is already active");
    gWeakSession = session_;
}

ScopedBufferedInputTraceCapture::~ScopedBufferedInputTraceCapture()
{
    // Clear global only if it still points to our session (Finding 4).
    {
        std::lock_guard<std::mutex> lk(gSessionMutex);
        if (gWeakSession.lock().get() == session_.get())
        {
            gWeakSession.reset();
        }
    }
    if (!session_->isFinished())
    {
        try
        {
            session_->finish();
        }
        catch (const std::exception& ex)
        {
            // Finding 5: log but never throw from a destructor.
            LOG(ERROR)
                << "ScopedBufferedInputTraceCapture destructor: "
                   "finalization failed: "
                << ex.what();
        }
        catch (...)
        {
            LOG(ERROR)
                << "ScopedBufferedInputTraceCapture destructor: "
                   "finalization failed with unknown exception";
        }
    }
}

void ScopedBufferedInputTraceCapture::finish()
{
    session_->finish(); // throws on error

    // Deactivate immediately after a successful finish (Finding 4).
    std::lock_guard<std::mutex> lk(gSessionMutex);
    if (gWeakSession.lock().get() == session_.get())
    {
        gWeakSession.reset();
    }
}

bool bufferedInputTraceCaptureEnabled()
{
    std::lock_guard<std::mutex> lk(gSessionMutex);
    return !gWeakSession.expired();
}

std::unique_ptr<BufferedInput> maybeWrapBufferedInputForTrace(
    std::unique_ptr<BufferedInput> input,
    memory::MemoryPool& pool,
    const std::shared_ptr<velox::ReadFile>& sourceFile)
{
    // Acquire a strong reference under the lock so the session cannot be
    // destroyed between the check and the wrap (Finding 1).
    std::shared_ptr<BufferedInputTraceSession> session;
    {
        std::lock_guard<std::mutex> lk(gSessionMutex);
        session = gWeakSession.lock();
    }
    if (!session)
    {
        return input;
    }

    const std::string absPath = sourceFile->getName();
    const uint64_t fileSize = sourceFile->size();
    // getOrAllocFileId canonicalizes and validates containment (Finding 2);
    // throws on error — no fallback.
    const uint64_t fileId = session->getOrAllocFileId(absPath, fileSize);

    return std::make_unique<TracingBufferedInput>(
        std::move(input), pool, fileId, std::move(session));
}

// ---------------------------------------------------------------------------
// TracingSeekableInputStream
// ---------------------------------------------------------------------------

TracingSeekableInputStream::TracingSeekableInputStream(
    std::unique_ptr<SeekableInputStream> inner,
    std::shared_ptr<BufferedInputTraceSession> session,
    uint64_t inputId,
    uint64_t streamId)
    : inner_(std::move(inner)),
      session_(std::move(session)),
      inputId_(inputId),
      streamId_(streamId)
{
}

TracingSeekableInputStream::~TracingSeekableInputStream()
{
    if (closed_)
    {
        return;
    }
    flushPendingConsume(/*throwing=*/false);
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kStreamClose;
    ev.inputId = inputId_;
    ev.streamId = streamId_;
    session_->tryAppendEvent(ev);
    closed_ = true;
}

bool TracingSeekableInputStream::Next(const void** data, int32_t* size)
{
    VELOX_CHECK(!closed_, "Next called on closed TracingSeekableInputStream");
    flushPendingConsume(/*throwing=*/true);

    const bool ok = inner_->Next(data, size);
    if (!ok)
    {
        // EOF
        BufferedInputTraceEvent ev;
        ev.op = BufferedInputTraceOp::kStreamEof;
        ev.inputId = inputId_;
        ev.streamId = streamId_;
        session_->appendEvent(ev);
        return false;
    }

    // Defensive: inner must not return a negative size (Finding 7).
    VELOX_CHECK(
        *size >= 0,
        "stream_id={} Next() returned negative size {}; "
        "inner stream contract violated",
        streamId_,
        *size);
    // ByteCount() must have advanced by at least *size (Finding 7).
    VELOX_CHECK(
        inner_->ByteCount() >= static_cast<int64_t>(*size),
        "stream_id={} ByteCount()={} < size={} after Next; "
        "inner stream ByteCount contract violated",
        streamId_,
        inner_->ByteCount(),
        *size);

    // Record pending: the caller will consume up to *size bytes (may BackUp).
    pendingActive_ = true;
    pendingOffset_ = static_cast<uint64_t>(inner_->ByteCount() - *size);
    pendingSize_ = static_cast<uint64_t>(*size);
    return true;
}

void TracingSeekableInputStream::BackUp(int32_t count)
{
    VELOX_CHECK(!closed_, "BackUp called on closed TracingSeekableInputStream");
    // Finding 7: reject negative count before the unsigned cast.
    VELOX_CHECK(
        count >= 0,
        "stream_id={} BackUp count must be non-negative, got {}",
        streamId_,
        count);
    VELOX_CHECK(
        pendingActive_,
        "BackUp({}) called with no pending Next on stream_id={}",
        count,
        streamId_);
    VELOX_CHECK(
        static_cast<uint64_t>(count) <= pendingSize_,
        "BackUp({}) exceeds pending size {} on stream_id={}",
        count,
        pendingSize_,
        streamId_);
    inner_->BackUp(count);
    pendingSize_ -= static_cast<uint64_t>(count);
}

bool TracingSeekableInputStream::SkipInt64(int64_t count)
{
    VELOX_CHECK(
        !closed_, "SkipInt64 called on closed TracingSeekableInputStream");
    flushPendingConsume(/*throwing=*/true);
    const bool ok = inner_->SkipInt64(count);
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kSkip;
    ev.inputId = inputId_;
    ev.streamId = streamId_;
    ev.argument = count;
    ev.result = ok ? 1 : 0;
    session_->appendEvent(ev);
    return ok;
}

int64_t TracingSeekableInputStream::ByteCount() const
{
    return inner_->ByteCount();
}

void TracingSeekableInputStream::seekToPosition(PositionProvider& position)
{
    VELOX_CHECK(
        !closed_,
        "seekToPosition called on closed TracingSeekableInputStream");
    VELOX_CHECK(
        inner_->positionSize() == 1,
        "stream_id={} positionSize()={} != 1; seek not supported in phase-1 trace",
        streamId_,
        inner_->positionSize());
    flushPendingConsume(/*throwing=*/true);
    inner_->seekToPosition(position);
    const int64_t byteCount = inner_->ByteCount();
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kSeek;
    ev.inputId = inputId_;
    ev.streamId = streamId_;
    ev.result = byteCount;
    session_->appendEvent(ev);
}

std::string TracingSeekableInputStream::getName() const
{
    return inner_->getName();
}

size_t TracingSeekableInputStream::positionSize() const
{
    return inner_->positionSize();
}

void TracingSeekableInputStream::flushPendingConsume(bool throwing)
{
    if (!pendingActive_)
    {
        return;
    }
    pendingActive_ = false;
    if (pendingSize_ == 0)
    {
        return; // zero-length consume; nothing to record
    }
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kConsume;
    ev.inputId = inputId_;
    ev.streamId = streamId_;
    ev.offset = pendingOffset_;
    ev.length = pendingSize_;
    if (throwing)
    {
        session_->appendEvent(ev);
    }
    else
    {
        session_->tryAppendEvent(ev);
    }
}

// ---------------------------------------------------------------------------
// TracingBufferedInput
// ---------------------------------------------------------------------------

TracingBufferedInput::TracingBufferedInput(
    std::unique_ptr<BufferedInput> inner,
    memory::MemoryPool& pool,
    uint64_t fileId,
    std::shared_ptr<BufferedInputTraceSession> session)
    : BufferedInput(inner->getInputStream(), pool),
      inner_(std::move(inner)),
      session_(std::move(session)),
      fileId_(fileId),
      inputId_(session_->allocInputId())
{
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputCreate;
    ev.fileId = fileId_;
    ev.inputId = inputId_;
    session_->appendEvent(ev);
}

TracingBufferedInput::TracingBufferedInput(
    std::unique_ptr<BufferedInput> inner,
    memory::MemoryPool& pool,
    uint64_t fileId,
    std::shared_ptr<BufferedInputTraceSession> session,
    uint64_t inputId)
    : BufferedInput(inner->getInputStream(), pool),
      inner_(std::move(inner)),
      session_(std::move(session)),
      fileId_(fileId),
      inputId_(inputId)
{
    // input_clone event already emitted by parent; do not emit input_create.
}

TracingBufferedInput::~TracingBufferedInput()
{
    // Detect live streams: they should have been closed before their owner.
    // We cannot iterate session internals directly, but the validator will
    // catch it.  Just record input_close; finish() will validate.
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputClose;
    ev.inputId = inputId_;
    session_->tryAppendEvent(ev);
}

const std::string& TracingBufferedInput::getName() const
{
    return inner_->getName();
}

std::unique_ptr<SeekableInputStream> TracingBufferedInput::enqueue(
    velox::common::Region region,
    const StreamIdentifier* sid)
{
    auto innerStream = inner_->enqueue(region, sid);
    const uint64_t streamId = session_->allocStreamId();
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kStreamEnqueue;
    ev.inputId = inputId_;
    ev.streamId = streamId;
    ev.offset = region.offset;
    ev.length = region.length;
    session_->appendEvent(ev);
    return std::make_unique<TracingSeekableInputStream>(
        std::move(innerStream), session_, inputId_, streamId);
}

void TracingBufferedInput::preload()
{
    inner_->preload();
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputPreload;
    ev.inputId = inputId_;
    session_->appendEvent(ev); // throwing path (Finding 6)
}

bool TracingBufferedInput::preloaded() const
{
    return inner_->preloaded();
}

void TracingBufferedInput::load(const LogType logType)
{
    inner_->load(logType);
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kLoad;
    ev.inputId = inputId_;
    ev.logType = static_cast<int32_t>(logType);
    session_->appendEvent(ev);
}

bool TracingBufferedInput::isBuffered(
    uint64_t offset,
    uint64_t length) const
{
    return inner_->isBuffered(offset, length);
}

std::unique_ptr<SeekableInputStream> TracingBufferedInput::read(
    uint64_t offset,
    uint64_t length,
    LogType logType) const
{
    auto innerStream = inner_->read(offset, length, logType);
    const uint64_t streamId = session_->allocStreamId();
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kStreamRead;
    ev.inputId = inputId_;
    ev.streamId = streamId;
    ev.offset = offset;
    ev.length = length;
    ev.logType = static_cast<int32_t>(logType);
    session_->appendEvent(ev);
    return std::make_unique<TracingSeekableInputStream>(
        std::move(innerStream), session_, inputId_, streamId);
}

bool TracingBufferedInput::shouldPreload(int32_t numPages)
{
    return inner_->shouldPreload(numPages);
}

bool TracingBufferedInput::shouldPrefetchStripes() const
{
    return inner_->shouldPrefetchStripes();
}

void TracingBufferedInput::setNumStripes(int32_t numStripes)
{
    inner_->setNumStripes(numStripes);
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputSetNumStripes;
    ev.inputId = inputId_;
    ev.argument = numStripes;
    session_->appendEvent(ev);
}

std::unique_ptr<BufferedInput> TracingBufferedInput::clone() const
{
    auto clonedInner = inner_->clone();
    const uint64_t childId = session_->allocInputId();

    // Record clone event (parent → child).
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputClone;
    ev.inputId = childId;
    ev.parentInputId = inputId_;
    session_->appendEvent(ev);

    return std::unique_ptr<TracingBufferedInput>(
        new TracingBufferedInput(
            std::move(clonedInner), *pool_, fileId_, session_, childId));
}

folly::Executor* TracingBufferedInput::executor() const
{
    return inner_->executor();
}

bool TracingBufferedInput::hasCache() const
{
    return inner_->hasCache();
}

void TracingBufferedInput::cacheRegion(
    uint64_t offset,
    uint64_t length,
    std::string_view data)
{
    inner_->cacheRegion(offset, length, data);
}

void TracingBufferedInput::cacheRegion(
    uint64_t offset,
    uint64_t length,
    const folly::IOBuf& buffer,
    uint64_t bufferOffset)
{
    inner_->cacheRegion(offset, length, buffer, bufferOffset);
}

std::optional<CachedRegion> TracingBufferedInput::findCachedRegion(
    uint64_t offset) const
{
    return inner_->findCachedRegion(offset);
}

uint64_t TracingBufferedInput::nextFetchSize() const
{
    return inner_->nextFetchSize();
}

void TracingBufferedInput::reset()
{
    inner_->reset();
    BufferedInputTraceEvent ev;
    ev.op = BufferedInputTraceOp::kInputReset;
    ev.inputId = inputId_;
    session_->appendEvent(ev);
}

} // namespace facebook::velox::dwio::common
