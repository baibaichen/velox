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

#include "velox/ch/Common/StatusFile.h"

#include "velox/common/testutil/TempDirectoryPath.h"

#include <folly/FileUtil.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

using common::testutil::TempDirectoryPath;

/// Captures glog VLOG(1)/LOG(INFO) messages so the unclean-restart diagnostic
/// emitted by the StatusFile constructor can be asserted verbatim.
class LogCapture : public google::LogSink
{
public:
    LogCapture()
    {
        google::AddLogSink(this);
    }

    ~LogCapture() override
    {
        google::RemoveLogSink(this);
    }

    void send(
        google::LogSeverity /*severity*/,
        const char * /*full_filename*/,
        const char * /*base_filename*/,
        int /*line*/,
        const google::LogMessageTime & /*time*/,
        const char * message,
        size_t message_len) override
    {
        messages_.emplace_back(message, message_len);
    }

    bool contains(const std::string & needle) const
    {
        for (const auto & msg : messages_)
        {
            if (msg.find(needle) != std::string::npos)
                return true;
        }
        return false;
    }

private:
    std::vector<std::string> messages_;
};

/// RAII helper to temporarily raise glog verbosity so VLOG(1) (== LOG_INFO) is
/// enabled, then restore it. LOG_INFO maps to VLOG(1) in logger_useful.h.
class VerbosityGuard
{
public:
    explicit VerbosityGuard(int level) : previous_(FLAGS_v)
    {
        FLAGS_v = level;
    }

    ~VerbosityGuard()
    {
        FLAGS_v = previous_;
    }

private:
    int previous_;
};

void writeFileContents(const std::string & path, const std::string & contents)
{
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    ASSERT_NE(fd, -1);
    ASSERT_EQ(
        folly::writeFull(fd, contents.data(), contents.size()),
        static_cast<ssize_t>(contents.size()));
    ASSERT_EQ(::close(fd), 0);
}

std::string readFileContents(const std::string & path)
{
    std::string out;
    const bool ok = folly::readFile(path.c_str(), out);
    EXPECT_TRUE(ok);
    return out;
}

TEST(StatusFileTest, UncleanRestartLogsOldContents)
{
    auto dir = TempDirectoryPath::create();
    const std::string path = dir->getPath() + "/status";

    // Pre-create a non-empty status file, as if left over by a previous
    // instance that did not shut down cleanly.
    writeFileContents(path, "PID: 12345\n");

    LogCapture capture;
    VerbosityGuard verbosity(1);

    StatusFile status(path, StatusFile::writePid());

    EXPECT_TRUE(capture.contains(
        "Status file " + path + " already exists - unclean restart. Contents:"));
    EXPECT_TRUE(capture.contains("PID: 12345"));

    // The ctor must still truncate and rewrite the file with the new PID.
    const std::string rewritten = readFileContents(path);
    EXPECT_EQ(rewritten, std::to_string(static_cast<long>(::getpid())));
}

TEST(StatusFileTest, UncleanRestartEmptyFile)
{
    auto dir = TempDirectoryPath::create();
    const std::string path = dir->getPath() + "/status";

    // Pre-create an EMPTY status file (probable unclean hardware restart).
    writeFileContents(path, "");

    LogCapture capture;
    VerbosityGuard verbosity(1);

    StatusFile status(path, StatusFile::writePid());

    EXPECT_TRUE(capture.contains(
        "Status file " + path
        + " already exists and is empty - probably unclean hardware restart."));
    EXPECT_FALSE(capture.contains("unclean restart. Contents:"));
}

TEST(StatusFileTest, FreshFileEmitsNoUncleanRestartLog)
{
    auto dir = TempDirectoryPath::create();
    const std::string path = dir->getPath() + "/status";

    LogCapture capture;
    VerbosityGuard verbosity(1);

    // No pre-existing file: neither unclean-restart variant should be logged.
    StatusFile status(path, StatusFile::writePid());

    EXPECT_FALSE(capture.contains("unclean restart. Contents:"));
    EXPECT_FALSE(capture.contains("probably unclean hardware restart."));
}

} // namespace
} // namespace facebook::velox::ch
