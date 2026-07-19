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

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

namespace facebook::velox::ch
{
namespace
{

// ---------------------------------------------------------------------------
// Mock ReadFile: serves in-memory data, counts preads, records the read
// destination, and can report a direct-IO alignment.
// ---------------------------------------------------------------------------
class MockReadFile : public velox::ReadFile
{
public:
    explicit MockReadFile(std::string data, uint64_t directIoAlignment = 1)
        : data_(std::move(data)), directIoAlignment_(directIoAlignment)
    {
    }

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const velox::FileIoContext &) const override
    {
        ++preadCalls_;
        lastPreadDest_ = buf;
        lastPreadOffset_ = offset;
        lastPreadLength_ = length;
        // Enforce O_DIRECT-style alignment on the actual read the adapter issues.
        // The call count is recorded above *first*, so a test can prove the
        // adapter fails closed before pread (call count unchanged) rather than
        // relying on this backstop to reject a misaligned read.
        if (directIoAlignment_ > 1)
        {
            VELOX_CHECK_EQ(offset % directIoAlignment_, 0u, "mock pread: unaligned offset");
            VELOX_CHECK_EQ(length % directIoAlignment_, 0u, "mock pread: unaligned length");
            VELOX_CHECK_EQ(
                reinterpret_cast<uintptr_t>(buf) % directIoAlignment_,
                0u,
                "mock pread: unaligned destination");
        }
        const uint64_t available =
            offset < data_.size() ? data_.size() - offset : 0;
        const uint64_t toRead = std::min(length, available);
        if (toRead > 0)
            std::memcpy(buf, data_.data() + offset, toRead);
        bytesRead_ += toRead;
        return {static_cast<const char *>(buf), toRead};
    }

    bool directIo(uint64_t & alignment) const override
    {
        alignment = directIoAlignment_;
        return directIoAlignment_ > 1;
    }

    bool shouldCoalesce() const override { return false; }
    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    std::string getName() const override { return "MockReadFile"; }
    uint64_t getNaturalReadSize() const override { return 4096; }

    int preadCalls() const { return preadCalls_; }
    const void * lastPreadDest() const { return lastPreadDest_; }
    uint64_t lastPreadOffset() const { return lastPreadOffset_; }
    uint64_t lastPreadLength() const { return lastPreadLength_; }

private:
    std::string data_;
    uint64_t directIoAlignment_;
    mutable int preadCalls_{0};
    mutable const void * lastPreadDest_{nullptr};
    mutable uint64_t lastPreadOffset_{0};
    mutable uint64_t lastPreadLength_{0};
};

// A ReadFile whose pread throws on the first call and counts invocations, used
// to prove the reader's exception state is terminal.
class CountingThrowReadFile : public velox::ReadFile
{
public:
    explicit CountingThrowReadFile(std::string data) : data_(std::move(data)) {}

    std::string_view pread(
        uint64_t offset,
        uint64_t length,
        void * buf,
        const velox::FileIoContext &) const override
    {
        ++preadCalls_;
        if (preadCalls_ == 1)
            VELOX_FAIL("simulated pread failure");
        const uint64_t available =
            offset < data_.size() ? data_.size() - offset : 0;
        const uint64_t toRead = std::min(length, available);
        if (toRead > 0)
            std::memcpy(buf, data_.data() + offset, toRead);
        return {static_cast<const char *>(buf), toRead};
    }

    int preadCalls() const { return preadCalls_; }

    bool shouldCoalesce() const override { return false; }
    uint64_t size() const override { return data_.size(); }
    uint64_t memoryUsage() const override { return data_.size(); }
    std::string getName() const override { return "CountingThrowReadFile"; }
    uint64_t getNaturalReadSize() const override { return 4096; }

private:
    std::string data_;
    mutable int preadCalls_{0};
};

// ---------------------------------------------------------------------------
// Mock WriteFile: reports every observable event to an external observer so the
// state survives the WriteFile being released by cancel().
// ---------------------------------------------------------------------------
struct WriteFileObserver
{
    std::string content;
    const char * lastAppendData{nullptr};
    size_t lastAppendSize{0};
    int appendCalls{0};
    int flushCalls{0};
    int closeCalls{0};
    size_t contentSizeAtLastFlush{0};
    bool closed{false};
    bool destroyed{false};
    // When >= 0, the append whose 0-based index equals this value throws.
    int throwOnAppendIndex{-1};
    // On the throwing append, physically commit this many bytes (a strict prefix
    // of the requested data) before failing, so a caller can observe a partial
    // physical write.
    size_t partialPrefixBytes{0};
    std::string name{"ObservableWriteFile"};
};

class ObservableWriteFile : public velox::WriteFile
{
public:
    explicit ObservableWriteFile(WriteFileObserver * observer) : observer_(observer)
    {
    }

    ~ObservableWriteFile() override { observer_->destroyed = true; }

    void append(std::string_view data) override
    {
        VELOX_CHECK(!observer_->closed, "append after close");
        if (observer_->throwOnAppendIndex == observer_->appendCalls)
        {
            observer_->throwOnAppendIndex = -1;
            // Physically commit a strict prefix (possibly empty) before failing,
            // so a caller can later observe the partial physical write. The
            // append itself does not complete: appendCalls is not incremented.
            const size_t prefix =
                std::min(observer_->partialPrefixBytes, data.size());
            if (prefix > 0)
                observer_->content.append(data.substr(0, prefix));
            observer_->lastAppendData = data.data();
            observer_->lastAppendSize = data.size();
            VELOX_FAIL("simulated append failure");
        }
        observer_->lastAppendData = data.data();
        observer_->lastAppendSize = data.size();
        ++observer_->appendCalls;
        observer_->content.append(data);
    }

    void flush() override
    {
        ++observer_->flushCalls;
        observer_->contentSizeAtLastFlush = observer_->content.size();
    }

    void close() override
    {
        ++observer_->closeCalls;
        observer_->closed = true;
    }

    uint64_t size() const override { return observer_->content.size(); }
    const std::string getName() const override { return observer_->name; }

private:
    WriteFileObserver * observer_;
};

// ---------------------------------------------------------------------------
// Fixture providing a leaf MemoryPool for the owned buffers.
// ---------------------------------------------------------------------------
class IoAdaptersTest : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
    }

    void SetUp() override
    {
        pool_ = memoryManager_.addLeafPool("io-adapters-test");
    }

    velox::memory::MemoryManager memoryManager_;
    std::shared_ptr<velox::memory::MemoryPool> pool_;
};

std::string toString(const CacheBuffer & buffer)
{
    return std::string(buffer.begin(), buffer.size());
}

// ===========================================================================
// ReadBufferFromVeloxReadFile
// ===========================================================================

// Basic streaming: read the whole file in chunks and track count()/position.
TEST_F(IoAdaptersTest, ReaderReadsWholeFileInChunks)
{
    const std::string data(8192, 'A');
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/4096);

    std::string result;
    while (!reader.eof())
    {
        const size_t available = reader.available();
        result.append(reader.position(), available);
        reader.position() += available;
    }
    EXPECT_EQ(result, data);
    // "settle consumed offset before the next nextImpl": count() accumulates the
    // consumed bytes across every next().
    EXPECT_EQ(reader.count(), data.size());
    EXPECT_EQ(reader.getPosition(), static_cast<off_t>(data.size()));
    EXPECT_TRUE(reader.eof());
}

// Reader 1: eof() fills an empty reader and reports false while bytes are
// available.
TEST_F(IoAdaptersTest, ReaderEofFillsBuffer)
{
    auto rf = std::make_shared<MockReadFile>("hello");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    EXPECT_FALSE(reader.hasPendingData());
    EXPECT_FALSE(reader.eof());
    EXPECT_TRUE(reader.hasPendingData());
    EXPECT_EQ(reader.available(), 5u);
    EXPECT_EQ(toString(reader.buffer()), "hello");
}

// Reader 2: next() settles the consumed offset (bytes) before the next read.
TEST_F(IoAdaptersTest, ReaderNextSettlesConsumedOffset)
{
    const std::string data(1024, 'B');
    auto rf = std::make_shared<MockReadFile>(data);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/512);

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 512u);
    EXPECT_EQ(reader.count(), 0u); // nothing consumed yet

    reader.position() += 256; // consume half
    EXPECT_EQ(reader.count(), 256u);
    EXPECT_EQ(reader.getPosition(), 256);

    reader.position() += 256; // consume the rest
    ASSERT_TRUE(reader.next()); // settles 512 -> bytes, then reads more
    EXPECT_EQ(reader.count(), 512u);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 1024u);
}

// Reader 3: a pread exception cancels the reader; the state is terminal and a
// second read is rejected without touching pread again.
TEST_F(IoAdaptersTest, ReaderExceptionIsTerminal)
{
    auto rf = std::make_shared<CountingThrowReadFile>("abcdefgh");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    EXPECT_THROW(reader.next(), VeloxException);
    EXPECT_TRUE(reader.isCanceled());
    EXPECT_THROW(reader.next(), VeloxException);
    EXPECT_EQ(rf->preadCalls(), 1)
        << "a canceled reader must not retry the physical read";
}

// Reader 4: external memory remains the read target across reads until an
// explicit detach.
TEST_F(IoAdaptersTest, ReaderExternalBufferPersistsAcrossReads)
{
    auto rf = std::make_shared<MockReadFile>("AAAABBBB");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    char ext[4] = {0, 0, 0, 0};
    reader.set(ext, sizeof(ext));

    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.buffer().begin(), ext);
    EXPECT_EQ(toString(reader.buffer()), "AAAA");

    reader.position() = reader.buffer().end(); // consume
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.buffer().begin(), ext)
        << "external buffer must remain the read target";
    EXPECT_EQ(std::string(ext, sizeof(ext)), "BBBB");
}

// Reader 5: set(nullptr, 0) removes every reference to caller memory.
TEST_F(IoAdaptersTest, ReaderSetNullDetaches)
{
    auto rf = std::make_shared<MockReadFile>("payload");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    char ext[4] = {0, 0, 0, 0};
    reader.set(ext, sizeof(ext));
    ASSERT_TRUE(reader.next());
    ASSERT_EQ(reader.internalBuffer().begin(), ext);

    EXPECT_NO_THROW(reader.set(nullptr, 0));
    EXPECT_NE(reader.internalBuffer().begin(), ext);
    EXPECT_NE(reader.position(), ext);
    EXPECT_EQ(reader.available(), 0u);
}

// Reader 6: the attach/read/detach handoff satisfies both FileSegment
// invariants (available()==0 and getFileOffsetOfBufferEnd()==currentWriteOffset,
// with no pointer left referencing the caller buffer).
TEST_F(IoAdaptersTest, ReaderHandoffSatisfiesFileSegmentInvariants)
{
    auto rf = std::make_shared<MockReadFile>(std::string(4096, 'Z'));
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    char queryBuffer[1024];
    reader.seek(0);
    reader.set(queryBuffer, sizeof(queryBuffer));
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.buffer().begin(), queryBuffer); // remote bytes landed in B

    // FileSegment writes B to cache; its currentWriteOffset advances to here.
    const size_t currentWriteOffset = reader.getFileOffsetOfBufferEnd();

    reader.set(nullptr, 0); // detach, then hand the reader to FileSegment
    EXPECT_EQ(reader.available(), 0u);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), currentWriteOffset);
    EXPECT_NE(reader.internalBuffer().begin(), queryBuffer);
    EXPECT_NE(reader.position(), queryBuffer);
}

// Reader 7a: setReadUntilPosition bounds a read and, when shrunk over an already
// loaded window, clamps it; extending resumes.
TEST_F(IoAdaptersTest, ReaderRightBoundShrinkAndExtend)
{
    auto rf = std::make_shared<MockReadFile>(std::string(4096, 'C'));
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/4096);

    reader.setReadUntilPosition(512);
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.buffer().size(), 512u);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 512u);

    // Shrink below the loaded window: it must not expose bytes at/after 256.
    reader.setReadUntilPosition(256);
    EXPECT_EQ(reader.buffer().size(), 256u);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 256u);

    // Extend and resume from where we stopped.
    reader.setReadUntilPosition(1024);
    reader.position() = reader.buffer().end();
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 1024u);
}

// Reader 7b: seek repositions and the buffer-end offset / pending-data track it.
TEST_F(IoAdaptersTest, ReaderSeekAndPendingData)
{
    auto rf = std::make_shared<MockReadFile>("0123456789");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());

    EXPECT_EQ(reader.seek(5), 5);
    EXPECT_FALSE(reader.hasPendingData());
    ASSERT_TRUE(reader.next());
    EXPECT_EQ(toString(reader.buffer()), "56789");
    EXPECT_EQ(reader.getPosition(), 5);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 10u);
    EXPECT_TRUE(reader.hasPendingData());

    reader.position() += reader.available();
    EXPECT_FALSE(reader.hasPendingData());
    EXPECT_TRUE(reader.eof());
}

// Reader 8: the owned buffer is charged to the injected pool and released when
// the reader is destroyed.
TEST_F(IoAdaptersTest, ReaderOwnedBufferChargedToPool)
{
    auto rf = std::make_shared<MockReadFile>("x");
    auto pool = memoryManager_.addLeafPool("reader-pool");
    ASSERT_EQ(pool->usedBytes(), 0);
    {
        ReadBufferFromVeloxReadFile reader(rf, pool.get(), /*bufferSize=*/64 * 1024);
        EXPECT_GT(pool->usedBytes(), 0)
            << "owned buffer must be allocated from the injected pool";
    }
    EXPECT_EQ(pool->usedBytes(), 0) << "owned buffer must be released";
}

// Reader 9: with direct IO enabled the owned buffer is aligned, aligned external
// buffers are accepted, and misaligned ones are rejected.
TEST_F(IoAdaptersTest, ReaderDirectIoAlignment)
{
    constexpr uint64_t kAlignment = 512;
    auto rf = std::make_shared<MockReadFile>(std::string(4096, 'D'), kAlignment);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/4096);

    // Owned buffer address is aligned.
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(reader.internalBuffer().begin()) % kAlignment,
        0u);

    alignas(kAlignment) char aligned[kAlignment];
    EXPECT_NO_THROW(reader.set(aligned, kAlignment));
    reader.set(nullptr, 0);

    // Misaligned address and misaligned length are rejected (no silent
    // buffered-IO fallback).
    EXPECT_THROW(reader.set(aligned + 1, kAlignment), VeloxException);
    EXPECT_THROW(reader.set(aligned, kAlignment - 1), VeloxException);
}

// Reader 9a (direct-IO): a fully aligned read succeeds and issues exactly one
// aligned pread; an aligned seek target is accepted.
TEST_F(IoAdaptersTest, ReaderDirectIoAlignedReadSucceeds)
{
    constexpr uint64_t kAlignment = 512;
    // File size is a multiple of the alignment so every read stays aligned.
    auto rf = std::make_shared<MockReadFile>(std::string(1024, 'D'), kAlignment);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/512);

    // An aligned seek target is accepted and issues no read on its own.
    EXPECT_EQ(reader.seek(512), 512);
    EXPECT_EQ(rf->preadCalls(), 0);
    EXPECT_EQ(reader.seek(0), 0);

    ASSERT_TRUE(reader.next()); // aligned read of [0, 512)
    EXPECT_EQ(reader.available(), 512u);
    EXPECT_EQ(rf->preadCalls(), 1);
    EXPECT_EQ(rf->lastPreadOffset() % kAlignment, 0u);
    EXPECT_EQ(rf->lastPreadLength() % kAlignment, 0u);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(rf->lastPreadDest()) % kAlignment, 0u);
}

// Reader 9b (direct-IO): an unaligned SEEK_SET target is rejected before it
// mutates the reader position, and no read is issued.
TEST_F(IoAdaptersTest, ReaderDirectIoUnalignedSeekRejected)
{
    constexpr uint64_t kAlignment = 512;
    auto rf = std::make_shared<MockReadFile>(std::string(1024, 'D'), kAlignment);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/512);

    EXPECT_THROW(reader.seek(100), VeloxException);
    EXPECT_EQ(reader.getFileOffsetOfBufferEnd(), 0u)
        << "a rejected unaligned seek must not move the reader position";
    EXPECT_EQ(rf->preadCalls(), 0);
}

// Reader 9c (direct-IO): an unaligned file tail is rejected *before* pread, so
// the mock's pread call count is unchanged (no over-read, no silent rounding).
TEST_F(IoAdaptersTest, ReaderDirectIoUnalignedTailRejectedBeforePread)
{
    constexpr uint64_t kAlignment = 512;
    // 512 aligned bytes followed by a 100-byte unaligned tail.
    auto rf = std::make_shared<MockReadFile>(std::string(612, 'D'), kAlignment);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/512);

    ASSERT_TRUE(reader.next()); // aligned read of [0, 512)
    EXPECT_EQ(rf->preadCalls(), 1);
    reader.position() = reader.buffer().end(); // consume the loaded window

    EXPECT_THROW(reader.next(), VeloxException)
        << "an unaligned file tail must fail closed";
    EXPECT_EQ(rf->preadCalls(), 1)
        << "the adapter must reject the unaligned tail before calling pread";
}

// Reader 9d (direct-IO): an unaligned right bound is rejected before pread.
TEST_F(IoAdaptersTest, ReaderDirectIoUnalignedRightBoundRejectedBeforePread)
{
    constexpr uint64_t kAlignment = 512;
    auto rf = std::make_shared<MockReadFile>(std::string(1024, 'D'), kAlignment);
    ReadBufferFromVeloxReadFile reader(rf, pool_.get(), /*bufferSize=*/1024);

    reader.setReadUntilPosition(600); // unaligned right bound
    EXPECT_THROW(reader.next(), VeloxException)
        << "an unaligned right bound must fail closed";
    EXPECT_EQ(rf->preadCalls(), 0)
        << "the adapter must reject the unaligned right bound before calling pread";
}

// The non-owning raw-pointer constructor does not take ownership of the file.
TEST_F(IoAdaptersTest, ReaderNonOwningConstructor)
{
    MockReadFile rawFile("non-owning");
    {
        ReadBufferFromVeloxReadFile reader(&rawFile, pool_.get());
        ASSERT_TRUE(reader.next());
    }
    EXPECT_EQ(rawFile.size(), std::string("non-owning").size());
}

// Capability and identity accessors.
TEST_F(IoAdaptersTest, ReaderCapabilitiesAndIdentity)
{
    auto rf = std::make_shared<MockReadFile>("abcdef");
    ReadBufferFromVeloxReadFile reader(rf, pool_.get());
    EXPECT_TRUE(reader.supportsExternalBufferMode());
    EXPECT_TRUE(reader.supportsRightBoundedReads());
    EXPECT_EQ(reader.getFileName(), "MockReadFile");
    ASSERT_TRUE(reader.tryGetFileSize().has_value());
    EXPECT_EQ(reader.tryGetFileSize().value(), 6u);

    // The stored base type is what FileSegment::RemoteFileReaderPtr holds.
    std::shared_ptr<ReadBufferFromFileBase> base =
        std::make_shared<ReadBufferFromVeloxReadFile>(rf, pool_.get());
    EXPECT_EQ(base->getFileName(), "MockReadFile");
}

// ===========================================================================
// WriteBufferFromVeloxWriteFile
// ===========================================================================

// Writer 1: a buffer-size-0 external attach + no-arg next appends without a
// staging copy; the address the WriteFile observes equals the caller buffer.
TEST_F(IoAdaptersTest, WriterExternalZeroCopyAppend)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf)); // bufferSize 0

    const std::string payload = "zero-copy-payload";
    auto external = AlignedBuffer::allocate<char>(payload.size(), pool_.get());
    char * externalData = external->asMutable<char>();
    std::memcpy(externalData, payload.data(), payload.size());

    writer.set(externalData, payload.size(), payload.size());
    writer.next();
    writer.set(nullptr, 0);

    EXPECT_EQ(observer.lastAppendData, externalData)
        << "append must receive the caller buffer directly (no staging copy)";
    EXPECT_EQ(observer.content, payload);
    EXPECT_EQ(observer.appendCalls, 1);
}

// Writer 2: a non-zero owned buffer is a BufferPtr charged to the injected pool.
TEST_F(IoAdaptersTest, WriterOwnedBufferChargedToPool)
{
    auto pool = memoryManager_.addLeafPool("writer-pool");
    ASSERT_EQ(pool->usedBytes(), 0);
    WriteFileObserver observer;
    {
        auto wf = std::make_unique<ObservableWriteFile>(&observer);
        WriteBufferFromVeloxWriteFile writer(
            std::move(wf), pool.get(), /*bufferSize=*/64 * 1024);
        EXPECT_GT(pool->usedBytes(), 0);
    }
    EXPECT_EQ(pool->usedBytes(), 0);
}

// Writer 3: set(nullptr, 0) detaches and leaves no caller pointer.
TEST_F(IoAdaptersTest, WriterSetNullDetaches)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char external[8] = {};
    writer.set(external, sizeof(external), sizeof(external));
    ASSERT_EQ(writer.buffer().begin(), external);

    writer.set(nullptr, 0);
    EXPECT_NE(writer.buffer().begin(), external);
    EXPECT_EQ(writer.offset(), 0u);
}

// Buffer-state safety 1: set() rejects a null buffer with a nonzero size or
// offset instead of performing null-pointer arithmetic; the canonical
// set(nullptr, 0) detach is still accepted and leaves a coherent empty state.
TEST_F(IoAdaptersTest, WriterSetRejectsNullWithNonzeroSizeOrOffset)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    EXPECT_THROW(writer.set(nullptr, 5), VeloxException);
    EXPECT_THROW(writer.set(nullptr, 0, 3), VeloxException);

    EXPECT_NO_THROW(writer.set(nullptr, 0));
    EXPECT_EQ(writer.buffer().begin(), nullptr);
    EXPECT_EQ(writer.offset(), 0u);
    EXPECT_EQ(writer.available(), 0u);
    EXPECT_FALSE(writer.hasPendingData());
}

// Buffer-state safety 2: set() rejects an offset past the buffer size before it
// forms an out-of-range cursor; an offset equal to the size (the FileSegment
// write shape) is accepted.
TEST_F(IoAdaptersTest, WriterSetRejectsOffsetPastSize)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char buf[4] = {};
    EXPECT_THROW(writer.set(buf, sizeof(buf), sizeof(buf) + 1), VeloxException);

    EXPECT_NO_THROW(writer.set(buf, sizeof(buf), sizeof(buf)));
    EXPECT_EQ(writer.offset(), sizeof(buf));
}

// Buffer-state safety 3: after a detach the accessors are a coherent empty
// state and finalize appends nothing and closes cleanly (no dangling caller
// pointer is dereferenced).
TEST_F(IoAdaptersTest, WriterDetachAccessorsAndFinalizeAreCoherent)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char external[8];
    std::memset(external, 'X', sizeof(external));
    writer.set(external, sizeof(external), sizeof(external));
    writer.set(nullptr, 0); // detach without appending

    EXPECT_EQ(writer.offset(), 0u);
    EXPECT_EQ(writer.available(), 0u);
    EXPECT_FALSE(writer.hasPendingData());
    EXPECT_NE(writer.buffer().begin(), external);

    EXPECT_NO_THROW(writer.finalize());
    EXPECT_EQ(observer.appendCalls, 0) << "a detached finalize must append nothing";
    EXPECT_TRUE(observer.closed);
}

// Writer 4: next appends exactly offset bytes, settles count, and resets
// position.
TEST_F(IoAdaptersTest, WriterNextAppendsExactlyOffset)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char external[5];
    std::memcpy(external, "abcde", 5);
    writer.set(external, sizeof(external), sizeof(external));
    EXPECT_EQ(writer.offset(), 5u);

    writer.next();
    EXPECT_EQ(observer.content, "abcde");
    EXPECT_EQ(observer.appendCalls, 1);
    EXPECT_EQ(writer.offset(), 0u) << "position must reset to the working begin";
    EXPECT_EQ(writer.count(), 5u) << "settled bytes must include the appended chunk";
}

// Writer 5: sync performs append then flush without close.
TEST_F(IoAdaptersTest, WriterSyncAppendsThenFlushesWithoutClose)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char external[4];
    std::memcpy(external, "data", 4);
    writer.set(external, sizeof(external), sizeof(external));
    writer.sync();

    EXPECT_EQ(observer.content, "data");
    EXPECT_EQ(observer.flushCalls, 1);
    EXPECT_EQ(observer.contentSizeAtLastFlush, 4u) << "append must precede flush";
    EXPECT_FALSE(observer.closed);
    // The writer stays active after sync.
    EXPECT_FALSE(writer.isFinalized());
    EXPECT_FALSE(writer.isCanceled());
}

// Writer 6: finalize appends then closes without an extra flush and is
// idempotent.
TEST_F(IoAdaptersTest, WriterFinalizeAppendsThenClosesIdempotent)
{
    WriteFileObserver observer;
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char external[4];
    std::memcpy(external, "data", 4);
    writer.set(external, sizeof(external), sizeof(external));
    writer.next();
    writer.set(nullptr, 0);

    writer.finalize();
    EXPECT_EQ(observer.content, "data");
    EXPECT_TRUE(observer.closed);
    EXPECT_EQ(observer.closeCalls, 1);
    EXPECT_EQ(observer.flushCalls, 0) << "finalize must not add an extra flush";

    EXPECT_NO_THROW(writer.finalize());
    EXPECT_EQ(observer.closeCalls, 1) << "repeated finalize must be a no-op";
}

// Writer 7: cancel is noexcept and idempotent, appends nothing, releases the
// file, and is a no-op after finalize.
TEST_F(IoAdaptersTest, WriterCancelIsNoexceptIdempotent)
{
    WriteFileObserver observer;
    {
        auto wf = std::make_unique<ObservableWriteFile>(&observer);
        WriteBufferFromVeloxWriteFile writer(std::move(wf));

        char external[7];
        std::memcpy(external, "discard", 7);
        writer.set(external, sizeof(external), sizeof(external));

        EXPECT_NO_THROW(writer.cancel());
        EXPECT_TRUE(writer.isCanceled());
        EXPECT_EQ(observer.appendCalls, 0) << "cancel must not append pending bytes";
        EXPECT_TRUE(observer.destroyed) << "cancel must release the WriteFile";
        // cancel must discard the pending cursor and detach the caller buffer.
        EXPECT_EQ(writer.offset(), 0u) << "cancel must discard the pending cursor";
        EXPECT_FALSE(writer.hasPendingData());
        EXPECT_NE(writer.buffer().begin(), external)
            << "cancel must leave no pointer into the caller buffer";
        EXPECT_NO_THROW(writer.cancel()); // idempotent
    }

    // cancel after finalize is a no-op.
    WriteFileObserver observer2;
    auto wf2 = std::make_unique<ObservableWriteFile>(&observer2);
    WriteBufferFromVeloxWriteFile writer2(std::move(wf2));
    writer2.finalize();
    EXPECT_NO_THROW(writer2.cancel());
    EXPECT_TRUE(writer2.isFinalized());
    EXPECT_FALSE(writer2.isCanceled());
}

// Writer 8: a next() failure leaves the writer canceled and prevents a second
// write; the original append exception propagates unchanged.
TEST_F(IoAdaptersTest, WriterNextFailureCancelsAndPreventsWrite)
{
    WriteFileObserver observer;
    observer.throwOnAppendIndex = 0; // first append throws
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(
        std::move(wf), pool_.get(), /*bufferSize=*/64);

    writer.write("hello", 5); // staged into the owned buffer, not yet appended
    try
    {
        writer.next();
        FAIL() << "next() must rethrow the append failure";
    }
    catch (const VeloxException & e)
    {
        EXPECT_NE(std::string(e.what()).find("simulated append failure"),
            std::string::npos);
    }
    EXPECT_TRUE(writer.isCanceled());
    // CH WriteBuffer::next settles bytes += bytes_in_buffer before cancel/rethrow
    // (WriteBuffer.h:69); count()/getPosition() must equal prior settled count
    // (0) + attempted bytes (5), matching the CH contract.
    EXPECT_EQ(writer.count(), 5u)
        << "writer must settle the attempted 5 bytes before cancel";
    EXPECT_EQ(writer.getPosition(), 5u)
        << "getPosition() must match settled count after failure";
    EXPECT_EQ(observer.content, "") << "no bytes may be committed on a failed append";
    // The pending cursor is discarded so no retry can re-append the abandoned bytes.
    EXPECT_EQ(writer.offset(), 0u) << "a failed append must discard the pending cursor";
    EXPECT_FALSE(writer.hasPendingData());
    EXPECT_THROW(writer.write("more", 4), VeloxException);
}

// Writer 9: getFileName delegates to the WriteFile.
TEST_F(IoAdaptersTest, WriterGetFileNameDelegates)
{
    WriteFileObserver observer;
    observer.name = "/cache/segment-42";
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));
    EXPECT_EQ(writer.getFileName(), "/cache/segment-42");
}

// Writer 10: given an already-open WriteFile that already contains a downloaded
// prefix, appending through the adapter preserves that prefix. This proves the
// adapter never truncates an already-open file; it does not claim to prove the
// file-opening mode (append/no-truncate opening is FileSegment's responsibility,
// Task 012).
TEST_F(IoAdaptersTest, WriterResumeAppendsWithoutTruncatingPrefix)
{
    WriteFileObserver observer;
    observer.content = "DOWNLOADED-PREFIX"; // an already-downloaded prefix
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char more[5];
    std::memcpy(more, "-more", 5);
    writer.set(more, sizeof(more), sizeof(more));
    writer.next();
    writer.set(nullptr, 0);

    EXPECT_EQ(observer.content, "DOWNLOADED-PREFIX-more")
        << "the downloaded prefix must be preserved and the new bytes appended";
}

// Writer 11: a WriteFile double physically commits a strict prefix of the
// requested append and then throws. The adapter must rethrow the original
// exception unchanged, become canceled, detach the caller buffer, perform no
// retry, and leave the committed physical prefix observable for a later
// FileSegment reconciliation. This adapter test does not itself reconcile
// filesystem size against downloaded/reserved sizes (that is FileSegment,
// Task 012).
TEST_F(IoAdaptersTest, WriterPartialWriteCommitsPrefixThenThrows)
{
    WriteFileObserver observer;
    observer.content = "COMMITTED";  // already-downloaded bytes on disk
    observer.throwOnAppendIndex = 0; // the first append fails mid-flight
    observer.partialPrefixBytes = 6; // after physically writing 6 of 16 bytes
    auto wf = std::make_unique<ObservableWriteFile>(&observer);
    WriteBufferFromVeloxWriteFile writer(std::move(wf));

    char reserved[16];
    std::memset(reserved, 'R', sizeof(reserved));
    writer.set(reserved, sizeof(reserved), sizeof(reserved));

    try
    {
        writer.next();
        FAIL() << "next() must rethrow the append failure";
    }
    catch (const VeloxException & e)
    {
        EXPECT_NE(std::string(e.what()).find("simulated append failure"),
            std::string::npos)
            << "the original append exception must propagate unchanged";
    }

    EXPECT_TRUE(writer.isCanceled());
    // CH WriteBuffer::next settles bytes += bytes_in_buffer before cancel/rethrow;
    // count()/getPosition() must equal prior settled count (0) + attempted (16),
    // regardless of how many bytes were physically committed.
    EXPECT_EQ(writer.count(), 16u)
        << "writer must settle the full attempted 16 bytes before cancel";
    EXPECT_EQ(writer.getPosition(), 16u)
        << "getPosition() must match settled count after partial-write failure";
    EXPECT_EQ(writer.offset(), 0u) << "the pending cursor must be discarded";
    EXPECT_NE(writer.buffer().begin(), reserved)
        << "a failed append must detach the caller buffer";
    // The strict physical prefix stays on disk for FileSegment to reconcile from
    // the filesystem size; the adapter adds neither the reserved-but-unwritten
    // tail nor performs the reconciliation itself.
    EXPECT_EQ(observer.content, "COMMITTEDRRRRRR")
        << "only the strict physical prefix is observable after the failure";
    EXPECT_EQ(observer.appendCalls, 0) << "the failed append did not complete";
    EXPECT_THROW(writer.write("more", 4), VeloxException)
        << "a canceled writer must not retry the write";
}

} // namespace
} // namespace facebook::velox::ch
