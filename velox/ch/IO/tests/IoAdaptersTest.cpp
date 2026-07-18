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

#include "velox/ch/IO/ReadBufferFromVeloxReadFile.h"
#include "velox/ch/IO/WriteBufferFromVeloxWriteFile.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace facebook::velox::ch
{
namespace
{

// ---------------------------------------------------------------------------
// In-memory mock ReadFile
// ---------------------------------------------------------------------------

class MockReadFile : public velox::ReadFile
{
public:
    explicit MockReadFile(std::string data) : data_(std::move(data)) {}

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const velox::FileIoContext &) const override
    {
        const uint64_t available =
            offset < data_.size() ? data_.size() - offset : 0;
        const uint64_t toRead = std::min(length, available);
        if (toRead > 0)
            std::memcpy(buf, data_.data() + offset, toRead);
        bytesRead_ += toRead;
        return {static_cast<const char *>(buf), toRead};
    }

    bool shouldCoalesce() const override { return false; }
    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    std::string getName() const override { return "MockReadFile"; }
    uint64_t getNaturalReadSize() const override { return 4096; }

private:
    std::string data_;
};

// A ReadFile whose pread throws on demand, used to exercise next()'s exception
// path and prove the external-buffer lifetime ends even when a physical read
// fails.
class ThrowingReadFile : public velox::ReadFile
{
public:
    explicit ThrowingReadFile(std::string data) : data_(std::move(data)) {}

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const velox::FileIoContext &) const override
    {
        if (throwOnNextPread_)
        {
            throwOnNextPread_ = false;
            VELOX_FAIL("simulated pread failure");
        }
        const uint64_t available =
            offset < data_.size() ? data_.size() - offset : 0;
        const uint64_t toRead = std::min(length, available);
        if (toRead > 0)
            std::memcpy(buf, data_.data() + offset, toRead);
        return {static_cast<const char *>(buf), toRead};
    }

    // Arms the next pread call to throw exactly once.
    void throwOnNextPread() { throwOnNextPread_ = true; }

    bool shouldCoalesce() const override { return false; }
    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    std::string getName() const override { return "ThrowingReadFile"; }
    uint64_t getNaturalReadSize() const override { return 4096; }

private:
    std::string data_;
    mutable bool throwOnNextPread_{false};
};

// ---------------------------------------------------------------------------
// In-memory mock WriteFile
// ---------------------------------------------------------------------------

class MockWriteFile : public velox::WriteFile
{
public:
    void append(std::string_view data) override
    {
        VELOX_CHECK(!closed_, "WriteFile already closed");
        VELOX_CHECK(!cancelled_, "WriteFile already cancelled");
        content_.append(data);
    }

    void flush() override
    {
        flushed_ = true;
    }

    void close() override
    {
        closed_ = true;
    }

    uint64_t size() const override { return content_.size(); }

    const std::string getName() const override
    {
        return "MockWriteFile";
    }

    const std::string & content() const { return content_; }
    bool isClosed() const { return closed_; }
    bool isFlushed() const { return flushed_; }
    bool isCancelled() const { return cancelled_; }

    void setCancelled() { cancelled_ = true; }

private:
    std::string content_;
    bool closed_{false};
    bool flushed_{false};
    bool cancelled_{false};
};

// ---------------------------------------------------------------------------
// ReadBufferFromVeloxReadFile tests
// ---------------------------------------------------------------------------

TEST(ReadBufferFromVeloxReadFileTest, NextReadsDataInChunks)
{
    const std::string data(8192, 'A');
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, /*bufferSize=*/4096);

    std::string result;
    while (reader.next())
    {
        result.append(reader.position(), reader.bufferEnd() - reader.position());
        reader.advance(reader.bufferEnd() - reader.position());
    }
    EXPECT_EQ(result, data);
}

TEST(ReadBufferFromVeloxReadFileTest, NextReturnsFalseAtEof)
{
    auto rf = std::make_shared<MockReadFile>("hello");
    ReadBufferFromVeloxReadFile reader(rf);

    ASSERT_TRUE(reader.next());
    reader.advance(reader.bufferEnd() - reader.position());
    EXPECT_FALSE(reader.next());
    EXPECT_TRUE(reader.eof());
}

TEST(ReadBufferFromVeloxReadFileTest, GetPositionTracksConsumption)
{
    const std::string data(1024, 'B');
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 512);

    EXPECT_EQ(reader.getPosition(), 0);
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 512);

    reader.advance(256);
    EXPECT_EQ(reader.getPosition(), 256);
}

TEST(ReadBufferFromVeloxReadFileTest, SetReadUntilPositionLimitsReads)
{
    const std::string data(4096, 'C');
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4096);

    reader.setReadUntilPosition(512);

    ASSERT_TRUE(reader.next());
    // Must not read past the readUntil boundary.
    EXPECT_LE(
        static_cast<size_t>(reader.bufferEnd() - reader.position()), 512u);
}

TEST(ReadBufferFromVeloxReadFileTest, SeekRepositionsToAbsoluteOffset)
{
    const std::string data = "0123456789";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf);

    reader.seek(5);
    EXPECT_EQ(reader.getPosition(), 5);

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "56789");
}

TEST(ReadBufferFromVeloxReadFileTest, ExternalBufferIsUsedForReads)
{
    const std::string data = "external-buffer-test";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf);

    // Provide an external buffer; next() should write into it directly.
    std::vector<char> externalBuf(data.size());
    reader.set(externalBuf.data(), externalBuf.size());

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(
        std::string(externalBuf.data(), data.size()),
        data);
}

TEST(ReadBufferFromVeloxReadFileTest, ExternalBufferIsSingleUseThenRevertsToInternal)
{
    const std::string data = "ABCDEFGH";
    auto rf = std::make_shared<MockReadFile>(data);
    // Small internal buffer so the file is read in 4-byte chunks.
    ReadBufferFromVeloxReadFile reader(rf, 4);

    // Arm an external buffer for exactly one read cycle.
    std::vector<char> externalBuf(4, '\0');
    reader.set(externalBuf.data(), externalBuf.size());

    // First next() reads directly into the external buffer.
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "ABCD");
    EXPECT_EQ(std::string(externalBuf.data(), externalBuf.size()), "ABCD");
    reader.advance(reader.bufferEnd() - reader.position());

    // The external buffer is single-use: the second next() must read into the
    // reader's internal buffer and must NOT touch the caller's external buffer
    // (which the caller is free to release once the first next() returned).
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "EFGH");
    EXPECT_EQ(std::string(externalBuf.data(), externalBuf.size()), "ABCD")
        << "second next() must not write into the consumed external buffer";
}

TEST(ReadBufferFromVeloxReadFileTest, SeekCancelsArmedExternalBuffer)
{
    const std::string data = "ABCDEFGHIJ";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    // Arm an external buffer but seek before consuming it. Per set()'s contract
    // a seek ends the external buffer's validity window, so the caller may
    // release it; the next read must land in the internal buffer and must not
    // write through the released external storage.
    std::vector<char> externalBuf(4, '\0');
    reader.set(externalBuf.data(), externalBuf.size());
    reader.seek(4, SEEK_SET);

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "EFGH");
    EXPECT_EQ(
        std::string(externalBuf.data(), externalBuf.size()),
        std::string(4, '\0'))
        << "next() after set()+seek() must not write into the released external "
           "buffer";
}

TEST(ReadBufferFromVeloxReadFileTest, NonOwningConstructorDoesNotDelete)
{
    const std::string data = "non-owning";
    MockReadFile rawFile(data);

    {
        ReadBufferFromVeloxReadFile reader(&rawFile);
        ASSERT_TRUE(reader.next());
    }
    // rawFile must still be usable after reader is destroyed.
    EXPECT_EQ(rawFile.size(), data.size());
}

TEST(ReadBufferFromVeloxReadFileTest, GetFileNameDelegates)
{
    auto rf = std::make_shared<MockReadFile>("x");
    ReadBufferFromVeloxReadFile reader(rf);
    EXPECT_EQ(reader.getFileName(), "MockReadFile");
}

// ---------------------------------------------------------------------------
// ReadBufferFromVeloxReadFile: offset and boundary safety
// ---------------------------------------------------------------------------

TEST(ReadBufferFromVeloxReadFileTest, AdvanceRejectsNegativeCount)
{
    auto rf = std::make_shared<MockReadFile>("hello");
    ReadBufferFromVeloxReadFile reader(rf);

    ASSERT_TRUE(reader.next());
    // A negative advance must be rejected before any pointer arithmetic.
    EXPECT_THROW(reader.advance(-1), VeloxException);
    // The rejected advance must not have moved the cursor.
    EXPECT_EQ(reader.getPosition(), 0);
}

TEST(ReadBufferFromVeloxReadFileTest, AdvanceRejectsOversizedCount)
{
    const std::string data = "ABCDEFGH";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    ASSERT_TRUE(reader.next());
    const ptrdiff_t available = reader.bufferEnd() - reader.position();
    // One byte past the loaded window must be rejected.
    EXPECT_THROW(reader.advance(available + 1), VeloxException);
    // An extreme count must be rejected without out-of-range pointer
    // arithmetic.
    EXPECT_THROW(
        reader.advance(std::numeric_limits<ptrdiff_t>::max()), VeloxException);
    // The rejected advances must not have moved the cursor.
    EXPECT_EQ(reader.getPosition(), 0);
}

TEST(ReadBufferFromVeloxReadFileTest, SeekSetRejectsNegativeOffset)
{
    auto rf = std::make_shared<MockReadFile>("0123456789");
    ReadBufferFromVeloxReadFile reader(rf);

    EXPECT_THROW(reader.seek(-1, SEEK_SET), VeloxException);
}

TEST(ReadBufferFromVeloxReadFileTest, SeekCurSupportsValidNegativeOffset)
{
    const std::string data = "0123456789";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf);

    reader.seek(8, SEEK_SET);
    reader.seek(-3, SEEK_CUR);
    EXPECT_EQ(reader.getPosition(), 5);

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "56789");
}

TEST(ReadBufferFromVeloxReadFileTest, SeekCurRejectsPositionBeforeStart)
{
    auto rf = std::make_shared<MockReadFile>("0123456789");
    ReadBufferFromVeloxReadFile reader(rf);

    reader.seek(4, SEEK_SET);
    EXPECT_THROW(reader.seek(-10, SEEK_CUR), VeloxException);
}

TEST(ReadBufferFromVeloxReadFileTest, SeekCurRejectsPositiveOverflow)
{
    auto rf = std::make_shared<MockReadFile>("0123456789");
    ReadBufferFromVeloxReadFile reader(rf);

    reader.seek(std::numeric_limits<off_t>::max() - 4, SEEK_SET);
    // Adding this offset would overflow off_t; it must be rejected before the
    // addition rather than wrapping into a bogus position.
    EXPECT_THROW(reader.seek(100, SEEK_CUR), VeloxException);
}

TEST(ReadBufferFromVeloxReadFileTest, SetReadUntilPositionResumesAfterExtension)
{
    const std::string data = "ABCDEFGHIJ";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    reader.setReadUntilPosition(4);
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "ABCD");
    reader.advance(reader.bufferEnd() - reader.position());

    // At the boundary next() reports EOF.
    EXPECT_FALSE(reader.next());
    EXPECT_TRUE(reader.eof());

    // Extending the boundary beyond the current offset must clear EOF and let
    // next() resume.
    reader.setReadUntilPosition(8);
    EXPECT_FALSE(reader.eof());
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "EFGH");
}

TEST(ReadBufferFromVeloxReadFileTest, SetRejectsNullExternalBuffer)
{
    auto rf = std::make_shared<MockReadFile>("data");
    ReadBufferFromVeloxReadFile reader(rf);

    EXPECT_THROW(reader.set(nullptr, 16), VeloxException);
}

TEST(ReadBufferFromVeloxReadFileTest, SetRejectsZeroCapacityExternalBuffer)
{
    auto rf = std::make_shared<MockReadFile>("data");
    ReadBufferFromVeloxReadFile reader(rf);

    std::vector<char> buf(4);
    EXPECT_THROW(reader.set(buf.data(), 0), VeloxException);
}

// ---------------------------------------------------------------------------
// ReadBufferFromVeloxReadFile: external-buffer lifetime on every next() exit
// ---------------------------------------------------------------------------

TEST(ReadBufferFromVeloxReadFileTest, ExternalBufferReleasedAtBoundaryThenNextUsesInternal)
{
    const std::string data = "ABCDEFGH";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    // Consume up to a readUntil boundary so the reader sits exactly on it.
    reader.setReadUntilPosition(4);
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "ABCD");
    reader.advance(reader.bufferEnd() - reader.position());

    // Arm an external buffer, then hit the boundary: next() returns false and
    // must end the external buffer's lifetime so the caller may release it.
    std::vector<char> externalBuf(4, '\0');
    reader.set(externalBuf.data(), externalBuf.size());
    EXPECT_FALSE(reader.next());
    EXPECT_EQ(
        std::string(externalBuf.data(), externalBuf.size()), std::string(4, '\0'))
        << "a boundary next() must not fill the external buffer";

    // The window must be a coherent empty internal window at the current offset.
    EXPECT_EQ(reader.position(), reader.bufferEnd());
    EXPECT_EQ(reader.getPosition(), 4);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 4);

    // Extending the boundary and reading again must use internal memory and
    // leave the released external buffer untouched.
    reader.setReadUntilPosition(8);
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "EFGH");
    EXPECT_EQ(
        std::string(externalBuf.data(), externalBuf.size()), std::string(4, '\0'))
        << "the resumed read must not write through the released external buffer";
}

TEST(ReadBufferFromVeloxReadFileTest, ExternalBufferPreadThrowsThenRetryUsesInternal)
{
    const std::string data = "ABCDEFGH";
    auto rf = std::make_shared<ThrowingReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    std::vector<char> externalBuf(4, '\0');
    reader.set(externalBuf.data(), externalBuf.size());

    // A failed pread must propagate its original exception unchanged and end the
    // external buffer's lifetime.
    rf->throwOnNextPread();
    try
    {
        reader.next();
        FAIL() << "expected pread to throw";
    }
    catch (const VeloxException & e)
    {
        EXPECT_NE(
            std::string(e.what()).find("simulated pread failure"),
            std::string::npos);
    }

    EXPECT_EQ(
        std::string(externalBuf.data(), externalBuf.size()), std::string(4, '\0'))
        << "a failed read must not fill the external buffer";
    // The window must be a coherent empty internal window preserving the offset.
    EXPECT_EQ(reader.position(), reader.bufferEnd());
    EXPECT_EQ(reader.getPosition(), 0);

    // The retry must use internal memory and leave the released external buffer
    // untouched.
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(std::string(reader.position(), reader.bufferEnd()), "ABCD");
    EXPECT_EQ(
        std::string(externalBuf.data(), externalBuf.size()), std::string(4, '\0'))
        << "the retry must not write through the released external buffer";
}

TEST(ReadBufferFromVeloxReadFileTest, EmptyPreadAfterExternalReadKeepsCoherentInternalWindow)
{
    const std::string data = "ABCD";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 4);

    // Allow reads to run one buffer past physical EOF so the terminal next()
    // takes the empty-pread path rather than the readUntil boundary path.
    reader.setReadUntilPosition(8);

    std::vector<char> externalBuf(4, '\0');
    reader.set(externalBuf.data(), externalBuf.size());
    ASSERT_TRUE(reader.next());
    ASSERT_EQ(std::string(reader.position(), reader.bufferEnd()), "ABCD");
    reader.advance(reader.bufferEnd() - reader.position());

    // The next read hits physical EOF via an empty pread. It must restore a
    // coherent empty internal window: no stale external pointers and no
    // cross-allocation pointer arithmetic.
    EXPECT_FALSE(reader.next());
    EXPECT_TRUE(reader.eof());
    EXPECT_EQ(reader.position(), reader.bufferEnd());
    EXPECT_EQ(reader.getPosition(), 4);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), reader.getPosition())
        << "an empty read must leave bufferEnd coherent with position";
    EXPECT_NO_THROW(reader.advance(0));
}

// ---------------------------------------------------------------------------
// ReadBufferFromVeloxReadFile: dynamic readUntil boundary with buffered data
// ---------------------------------------------------------------------------

TEST(ReadBufferFromVeloxReadFileTest, ShrinkReadUntilConstrainsLoadedWindow)
{
    const std::string data = "ABCDEFGH";
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, 8);

    // Load a full 8-byte window.
    ASSERT_TRUE(reader.next());
    ASSERT_EQ(reader.bufferEnd() - reader.position(), 8);

    // Shrink the boundary below the loaded window: the window must be constrained
    // so bufferEnd exposes no bytes at or beyond the new exclusive limit.
    reader.setReadUntilPosition(4);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 4)
        << "bufferEnd must not expose bytes at or beyond the new boundary";
    EXPECT_LE(reader.bufferEnd() - reader.position(), 4);

    // advance must reject consuming past the new exclusive boundary.
    EXPECT_THROW(reader.advance(5), VeloxException);
}

// ---------------------------------------------------------------------------
// WriteBufferFromVeloxWriteFile tests
// ---------------------------------------------------------------------------

TEST(WriteBufferFromVeloxWriteFileTest, WriteAccumulatesAndFlushCommits)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 4096);

    writer.write("hello", 5);
    writer.write(" world", 6);

    EXPECT_TRUE(wf->content().empty()); // buffered, not yet flushed
    writer.flush();
    EXPECT_EQ(wf->content(), "hello world");
    EXPECT_TRUE(wf->isFlushed());
}

TEST(WriteBufferFromVeloxWriteFileTest, FinalizeFlushesAndCloses)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 4096);

    writer.write("data", 4);
    writer.finalize();

    EXPECT_EQ(wf->content(), "data");
    EXPECT_TRUE(wf->isClosed());
}

TEST(WriteBufferFromVeloxWriteFileTest, CancelAbandonsBufferedData)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 4096);

    writer.write("discard", 7);
    writer.cancel();

    EXPECT_TRUE(wf->content().empty());
    EXPECT_FALSE(wf->isClosed());
}

TEST(WriteBufferFromVeloxWriteFileTest, NextGetWritableChunkAndShortWrite)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 64);

    // next() provides a writable chunk.
    char * data = nullptr;
    int64_t size = 0;
    writer.next(data, size);
    ASSERT_NE(data, nullptr);
    ASSERT_GT(size, 0);

    // Write only 3 bytes (short write).
    std::memcpy(data, "abc", 3);
    writer.advance(3);

    // Flush should commit only the 3 bytes actually written.
    writer.flush();
    EXPECT_EQ(wf->content(), "abc");
}

TEST(WriteBufferFromVeloxWriteFileTest, BufferAutoFlushesWhenFull)
{
    auto wf = std::make_shared<MockWriteFile>();
    // Small buffer to force auto-flush.
    WriteBufferFromVeloxWriteFile writer(wf, 8);

    const std::string payload(16, 'Z');
    writer.write(payload.data(), payload.size());

    // At least the first 8 bytes must have been flushed already.
    EXPECT_GE(wf->content().size(), 8u);
}

TEST(WriteBufferFromVeloxWriteFileTest, GetPositionTracksWrittenBytes)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 4096);

    EXPECT_EQ(writer.getPosition(), 0u);
    writer.write("abcde", 5);
    EXPECT_EQ(writer.getPosition(), 5u);
}

TEST(WriteBufferFromVeloxWriteFileTest, OwnershipTransferSharedPtr)
{
    auto wf = std::make_shared<MockWriteFile>();
    std::weak_ptr<MockWriteFile> weak = wf;
    {
        // The writer copies the shared_ptr, so caller and writer both own it.
        WriteBufferFromVeloxWriteFile writer(wf);
        writer.write("x", 1);
        writer.finalize();
    }
    // Destroying the writer must not destroy the file while the caller still
    // holds wf on the stack: ownership is shared, not transferred.
    EXPECT_FALSE(weak.expired());

    // Once the caller drops its reference too, the file is released, proving
    // the writer did not leak its shared_ptr.
    wf.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(WriteBufferFromVeloxWriteFileTest, AdvanceRejectsOverflowingCount)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 16);

    writer.write("01234567", 8); // writePos_ = 8, 8 bytes of spare capacity

    // A normal oversized advance is rejected.
    EXPECT_THROW(writer.advance(9), VeloxException);
    // An advance whose addition would wrap past the buffer capacity must be
    // rejected by the validation itself, not silently accepted.
    EXPECT_THROW(
        writer.advance(std::numeric_limits<size_t>::max() - 4), VeloxException);
    // Position must be unchanged after the rejected advances.
    EXPECT_EQ(writer.getPosition(), 8u);
}

TEST(WriteBufferFromVeloxWriteFileTest, AdvanceAfterFinalizeThrows)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 64);

    // Stage a short write, then finalize the writer.
    char * data = nullptr;
    int64_t size = 0;
    writer.next(data, size);
    std::memcpy(data, "abc", 3);
    writer.advance(3);
    writer.finalize();

    // advance is a write operation: after finalize it must throw without
    // changing the write position, just like write() and next().
    const size_t positionBefore = writer.getPosition();
    EXPECT_THROW(writer.advance(1), VeloxException);
    EXPECT_EQ(writer.getPosition(), positionBefore)
        << "advance after finalize must not change the write position";
}

TEST(WriteBufferFromVeloxWriteFileTest, AdvanceAfterCancelThrows)
{
    auto wf = std::make_shared<MockWriteFile>();
    WriteBufferFromVeloxWriteFile writer(wf, 64);

    writer.write("abc", 3);
    writer.cancel();

    // advance after cancel must throw without changing the write position.
    const size_t positionBefore = writer.getPosition();
    EXPECT_THROW(writer.advance(1), VeloxException);
    EXPECT_EQ(writer.getPosition(), positionBefore)
        << "advance after cancel must not change the write position";
}

} // namespace
} // namespace facebook::velox::ch
